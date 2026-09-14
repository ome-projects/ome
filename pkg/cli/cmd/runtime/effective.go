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

type effectiveProjector func(
	*v1beta1.InferenceService,
	*effective.RuntimeState,
	reportv1alpha1.Clock,
) (reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeEffectiveContent], error)

type effectiveCommandDependencies struct {
	clock     reportv1alpha1.Clock
	limits    paging.Limits
	projector effectiveProjector
}

type effectiveOptions struct {
	genericiooptions.IOStreams
	namespaceOptions *namespace.Options
	dependencies     effectiveCommandDependencies
	output           string
	name             string
	format           report.Format
	wide             bool
}

func newEffectiveCmd(f factory.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	return newEffectiveCmdWithDependencies(f, streams, effectiveCommandDependencies{
		clock: reportv1alpha1.SystemClock{},
		limits: paging.Limits{
			PageSize:       paging.ChunkSize,
			MaxItems:       1000,
			MaxPages:       2,
			RequestTimeout: 10 * time.Second,
		},
		projector: runtimeprojection.ProjectEffective,
	})
}

func newEffectiveCmdWithDependencies(
	f factory.Factory,
	streams genericiooptions.IOStreams,
	dependencies effectiveCommandDependencies,
) *cobra.Command {
	o := &effectiveOptions{
		IOStreams: streams, namespaceOptions: namespace.NewOptions(), dependencies: dependencies,
	}
	cmd := &cobra.Command{
		Use:   "effective INFERENCESERVICE",
		Short: "Show effective runtime evidence for an InferenceService",
		Long: `Shows allowlisted runtime selection, inheritance, pin, status, drift,
and live-versus-controller-active evidence for an InferenceService.

Current only means status.observedGeneration == metadata.generation in the
fetched snapshot, never wall-clock freshness or rollout convergence. Raw
runtime specs, ControllerRevision data, status messages, resource versions,
and synchronization tokens are never printed.

The compact table uses SR for ServingRuntime and CSR for ClusterServingRuntime,
and renders components as MODE (SOURCE). Use -o wide for the complete legacy
table.`,
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

func (o *effectiveOptions) validate() error {
	format, wide, err := parseEffectiveOutput(o.output)
	if err != nil {
		return err
	}
	o.format = format
	o.wide = wide
	return o.validateName()
}

func parseEffectiveOutput(value string) (report.Format, bool, error) {
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

func (o *effectiveOptions) validateName() error {
	if problems := validation.IsDNS1123Subdomain(o.name); len(problems) > 0 {
		return fmt.Errorf("InferenceService name %q is invalid: %s", o.name, strings.Join(problems, "; "))
	}
	return nil
}

func (o *effectiveOptions) run(ctx context.Context, f factory.Factory) error {
	evidence, err := collectRuntimeEvidence(
		ctx, f, o.namespaceOptions, o.name, o.dependencies.limits,
		runtimeEvidenceOptions{IncludeHistory: false},
	)
	if err != nil {
		return err
	}
	projected, err := o.dependencies.projector(
		evidence.inferenceService, evidence.state, o.dependencies.clock,
	)
	if err != nil {
		return fmt.Errorf("project effective runtime evidence: %w", err)
	}
	if o.wide {
		if err := projected.Content.WideTable().Write(o.Out); err != nil {
			return fmt.Errorf("write effective runtime report: %w", err)
		}
		return nil
	}
	if err := report.Write(o.Out, o.format, projected); err != nil {
		return fmt.Errorf("write effective runtime report: %w", err)
	}
	return nil
}
