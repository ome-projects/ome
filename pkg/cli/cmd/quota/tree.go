// Package quota implements read-only AcceleratorQuota diagnostics.
package quota

import (
	"context"
	"errors"
	"time"

	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/cli/quotacollection"
	"sigs.k8s.io/ome/pkg/cli/quotatreeprojection"
	"sigs.k8s.io/ome/pkg/cli/report"
	v "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

// NewCmd builds the read-only quota family. Validation/status are separate work.
func NewCmd(f factory.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	cmd := &cobra.Command{Use: "quota", Short: "Inspect declared accelerator quota topology"}
	cmd.AddCommand(newTreeCmd(f, streams, v.SystemClock{}, paging.Limits{PageSize: paging.ChunkSize, MaxItems: 1000, MaxPages: 2, RequestTimeout: 10 * time.Second}))
	return cmd
}

func newTreeCmd(f factory.Factory, streams genericiooptions.IOStreams, clock v.Clock, limits paging.Limits) *cobra.Command {
	var output string
	cmd := &cobra.Command{
		Use: "tree [ACCELERATORQUOTA]", Short: "Show declared quota ancestry and budget leaves",
		Long: `Render a complete bounded snapshot of cluster-scoped AcceleratorQuotas.
A ClusterQueue leaf is its own tenant (metadata.name); Cohorts group leaves.
An optional name selects its ancestors and descendants. Problems and sources
retain whole-snapshot context. Paths are Computed from Declared parent edges.

This advisory report does not prove admission, enforcement, controller
installation, Kueue integration, materialization, or available capacity.
Collection reads at most 1000 objects in 2 pages within a 10-second deadline.
Incomplete snapshots cannot establish topology and return an error.
Compact output fits 80 columns; -o wide retains complete safe fields.
Indentation is capped at depth 8; deeper nodes show their actual depth=N.
Use -o wide for full computed paths and depths.
It never prints labels, annotations, status messages, UIDs, or resource versions.`,
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) > 1 {
				return errors.New("quota tree accepts at most one AcceleratorQuota name")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			var target string
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
			snapshot, err := quotacollection.Collect(ctx, client.OmeV1beta1().AcceleratorQuotas(), limits)
			if err != nil {
				return safeReadError(err)
			}
			value, err := quotatreeprojection.Project(quotatreeprojection.Input{Quotas: snapshot.Quotas, Completeness: snapshot.Completeness, Target: target}, clock)
			if err != nil {
				return err
			}
			if output == "wide" {
				return v.QuotaTreeWideTable(value).Write(streams.Out)
			}
			return report.Write(streams.Out, format, value)
		},
	}
	cmd.Flags().StringVarP(&output, "output", "o", "table", "Output format: table, wide, json or yaml")
	return cmd
}

// safeReadError deliberately discards credential-bearing transport messages.
func safeReadError(err error) error {
	reason := "Unreadable"
	switch {
	case errors.Is(err, context.Canceled):
		reason = "Canceled"
	case errors.Is(err, context.DeadlineExceeded), apierrors.IsTimeout(err), apierrors.IsServerTimeout(err):
		reason = "TimedOut"
	case apierrors.IsForbidden(err), apierrors.IsUnauthorized(err):
		reason = "Forbidden"
	case apierrors.IsNotFound(err):
		reason = "UnsupportedAPI"
	case errors.Is(err, quotacollection.ErrEmptyResponse):
		reason = "MalformedPayload"
	}
	return errors.New("AcceleratorQuota snapshot unavailable: " + reason)
}
