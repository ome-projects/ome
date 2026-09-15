package quota

import (
	"context"
	"errors"
	"time"

	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	api "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/exitcode"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/cli/quotacollection"
	"sigs.k8s.io/ome/pkg/cli/quotastatusprojection"
	"sigs.k8s.io/ome/pkg/cli/quotatreeprojection"
	"sigs.k8s.io/ome/pkg/cli/report"
	v "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func newDiagnosticCmd(f factory.Factory, streams genericiooptions.IOStreams, clock v.Clock, limits paging.Limits, validate bool) *cobra.Command {
	name, short := "status", "Inspect reported quota budgets, capacity and materialization"
	if validate {
		name, short = "validate", "Assert advisory quota topology against the complete snapshot"
	}
	var output string
	cmd := &cobra.Command{Use: name + " [ACCELERATORQUOTA]", Short: short,
		Long: `Read cluster-scoped AcceleratorQuota evidence without auxiliary or remote reads.
Validation always checks the whole complete snapshot, even with a selected name.
It writes one diagnostic report and exits 2 when advisory checks find violations.
Empty topology lacks the reserved root and therefore fails validation.
Status with a name performs one identity-bound GET; unnamed status lists at most
1000 objects in 2 pages within a 10-second deadline. Incomplete snapshots fail.
Status validates every admitted group before canonical display caps: 64 inspected
entries per group, 16 displayed; 64 displayed objects (use a named status GET).
Oversize/invalid groups expose no misleading valid prefix. Truncation is explicit.
Ready, Degraded and Materialized condition reasons use a closed vocabulary.
Raw messages, labels, annotations, UIDs and resource versions are never reported.
Zero optional nonpointer scalars lose wire presence and remain Unknown/Defaulted.
Source generation is not comparable to local generation; high-water is historical.
Sample timestamps do not establish a configured freshness SLA. No free-capacity,
admission, controller installation, Kueue or independent enforcement claim.
Compact output fits 80 columns; wide, JSON and YAML retain the same safe fields.
Use quota tree for Computed ancestry: the current API has no reported status path.`,
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) > 1 {
				return errors.New("quota diagnostic accepts at most one AcceleratorQuota name")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			target := ""
			if len(args) == 1 {
				target = args[0]
				if len(target) > 63 || len(validation.IsDNS1123Subdomain(target)) > 0 {
					return errors.New("AcceleratorQuota name is invalid")
				}
			}
			format := report.FormatTable
			if output != "wide" {
				var err error
				format, err = report.ParseFormat(output)
				if err != nil {
					return errors.New("unsupported output format (supported: table, wide, json, yaml)")
				}
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), limits.RequestTimeout)
			defer cancel()
			if err := ctx.Err(); err != nil {
				return safeReadError(err)
			}
			client, err := f.OMEClient()
			if err != nil || client == nil {
				return errors.New("AcceleratorQuota client unavailable")
			}
			quotas := client.OmeV1beta1().AcceleratorQuotas()
			if !validate && target != "" {
				q, readErr := quotas.Get(ctx, target, metav1.GetOptions{})
				if readErr != nil {
					if apierrors.IsNotFound(readErr) {
						return errors.New("AcceleratorQuota target unavailable: NotFound")
					}
					return safeReadError(readErr)
				}
				if err := ctx.Err(); err != nil {
					return safeReadError(err)
				}
				if q == nil || q.Name != target || q.Namespace != "" {
					return errors.New("AcceleratorQuota target response identity is invalid")
				}
				value, projectErr := quotastatusprojection.Project(quotastatusprojection.Input{Quotas: []api.AcceleratorQuota{*q}, Target: target, Scope: "Named", ObservedPages: 1, ObservedItems: 1}, clock)
				if projectErr != nil {
					return projectErr
				}
				if output == "wide" {
					return v.QuotaStatusWideTable(value).Write(streams.Out)
				}
				return report.Write(streams.Out, format, value)
			}
			snapshot, readErr := quotacollection.Collect(ctx, quotas, limits)
			if readErr != nil {
				return safeReadError(readErr)
			}
			if validate {
				topology, projectErr := quotatreeprojection.Project(quotatreeprojection.Input{Quotas: snapshot.Quotas, Completeness: snapshot.Completeness, Target: target}, clock)
				if projectErr != nil {
					return projectErr
				}
				value := v.NewEnvelope(v.QuotaValidationReportKind, topology.Metadata, v.QuotaValidationContent{Valid: len(topology.Content.Problems) == 0, Topology: topology.Content}, v.ClockFunc(func() time.Time { return topology.CollectedAt }))
				value.Sources, value.Warnings = topology.Sources, topology.Warnings
				if output == "wide" {
					err = v.QuotaValidationWideTable(value).Write(streams.Out)
				} else {
					err = report.Write(streams.Out, format, value)
				}
				if err != nil {
					return err
				}
				if !value.Content.Valid {
					return &exitcode.UnmetAssertionError{Err: errors.New("advisory quota validation found violations in the complete snapshot")}
				}
				return nil
			}
			value, projectErr := quotastatusprojection.Project(quotastatusprojection.Input{Quotas: snapshot.Quotas, Scope: "Cluster", ObservedPages: snapshot.Completeness.ObservedPages, ObservedItems: snapshot.Completeness.ObservedItems, Truncated: snapshot.Completeness.Truncated}, clock)
			if projectErr != nil {
				return projectErr
			}
			if output == "wide" {
				return v.QuotaStatusWideTable(value).Write(streams.Out)
			}
			return report.Write(streams.Out, format, value)
		},
	}
	cmd.Flags().StringVarP(&output, "output", "o", "table", "Output format: table, wide, json or yaml")
	return cmd
}
