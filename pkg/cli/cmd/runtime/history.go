package runtime

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/effective"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/namespace"
	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/runtimeprojection"
)

type historyProjector func(
	*v1beta1.InferenceService,
	*effective.RuntimeState,
	reportv1alpha1.Clock,
) (reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeHistoryContent], error)

type historyCommandDependencies struct {
	clock     reportv1alpha1.Clock
	limits    paging.Limits
	projector historyProjector
}

type historyOptions struct {
	genericiooptions.IOStreams
	namespaceOptions *namespace.Options
	dependencies     historyCommandDependencies
	output           string
	name             string
	format           report.Format
	wide             bool
}

func newHistoryCmd(f factory.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	return newHistoryCmdWithDependencies(f, streams, historyCommandDependencies{
		clock: reportv1alpha1.SystemClock{},
		limits: paging.Limits{
			PageSize:       paging.ChunkSize,
			MaxItems:       1000,
			MaxPages:       2,
			RequestTimeout: 10 * time.Second,
		},
		projector: runtimeprojection.ProjectHistory,
	})
}

func newHistoryCmdWithDependencies(
	f factory.Factory,
	streams genericiooptions.IOStreams,
	dependencies historyCommandDependencies,
) *cobra.Command {
	o := &historyOptions{
		IOStreams: streams, namespaceOptions: namespace.NewOptions(), dependencies: dependencies,
	}
	cmd := &cobra.Command{
		Use:   "history INFERENCESERVICE",
		Short: "Show bounded runtime revision history for an InferenceService",
		Long: `Shows allowlisted, retention-bounded ControllerRevision history for an
InferenceService's resolved runtime.

The command reads at most two pages and 1,000 revisions, and reports whether
the observed window is complete or truncated. Raw runtime specs,
ControllerRevision data, status messages, resource versions, and
synchronization tokens are never printed.

The compact table keeps each line within 80 columns. WINDOW is
OBS/BOUND/SEEN/ASKED: C=complete, P=partial, U=unavailable, N=not requested;
B=retention-bounded and I=incomplete. ROLES uses A=active, Q=requested,
R=reported, and H=history. CHECK uses OK, BAD, or ?; LIVE uses MATCH, DIFF,
AMB, or ?. ISSUES uses R/G for revision/global report counts. Long revision
names use PREFIX#DIGEST, with an eight-hex display-only digest of the complete
revision name. It is not a Kubernetes identity or runtime hash. Use -o wide
for full revision names, timestamps, hashes, sources, and exact issue codes.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			o.name = args[0]
			if err := o.validate(); err != nil {
				return err
			}
			return o.run(cmd.Context(), f)
		},
	}
	cmd.Flags().StringVarP(&o.output, "output", "o", "table", "Output format: table, wide, json or yaml")
	o.namespaceOptions.AddOMEFlags(cmd.Flags())
	return cmd
}

func (o *historyOptions) validate() error {
	format, wide, err := parseHistoryOutput(o.output)
	if err != nil {
		return err
	}
	o.format = format
	o.wide = wide
	if problems := validation.IsDNS1123Subdomain(o.name); len(problems) > 0 {
		return fmt.Errorf("InferenceService name %q is invalid: %s", o.name, strings.Join(problems, "; "))
	}
	return nil
}

func parseHistoryOutput(value string) (report.Format, bool, error) {
	if value == "wide" {
		return report.FormatTable, true, nil
	}
	format, err := report.ParseFormat(value)
	if err != nil {
		return "", false, fmt.Errorf(
			"unsupported output format %q (supported: table, wide, json, yaml)", value,
		)
	}
	return format, false, nil
}

func (o *historyOptions) run(ctx context.Context, f factory.Factory) error {
	evidence, err := collectRuntimeEvidence(
		ctx, f, o.namespaceOptions, o.name, o.dependencies.limits,
		runtimeEvidenceOptions{IncludeHistory: true},
	)
	if err != nil {
		return err
	}
	projected, err := o.dependencies.projector(
		evidence.inferenceService, evidence.state, o.dependencies.clock,
	)
	if err != nil {
		return fmt.Errorf("project runtime history evidence: %w", err)
	}
	if o.wide {
		if err := projected.Content.WideTable().Write(o.Out); err != nil {
			return fmt.Errorf("write runtime history report: %w", err)
		}
		return nil
	}
	if err := report.Write(o.Out, o.format, projected); err != nil {
		return fmt.Errorf("write runtime history report: %w", err)
	}
	return nil
}
