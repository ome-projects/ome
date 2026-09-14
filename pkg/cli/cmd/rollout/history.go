package rollout

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/apierror"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/rolloutprojection"
)

type historyProjector func(
	*omev1beta1.InferenceService,
	reportv1alpha1.Clock,
) (reportv1alpha1.RolloutHistoryReport, error)

type historyOptions struct {
	streams   genericiooptions.IOStreams
	output    string
	clock     reportv1alpha1.Clock
	projector historyProjector
}

func newHistoryCmd(
	f factory.Factory,
	streams genericiooptions.IOStreams,
	clock reportv1alpha1.Clock,
) *cobra.Command {
	return newHistoryCmdWithProjector(f, streams, clock, rolloutprojection.ProjectHistory)
}

func newHistoryCmdWithProjector(
	f factory.Factory,
	streams genericiooptions.IOStreams,
	clock reportv1alpha1.Clock,
	projector historyProjector,
) *cobra.Command {
	options := &historyOptions{
		streams: streams, clock: clock, projector: projector,
	}
	cmd := &cobra.Command{
		Use:   "history INFERENCESERVICE",
		Short: "Show bounded retained rollout evidence",
		Long: `Show bounded rollout evidence retained on one InferenceService.

This is not a durable audit trail. The retained window contains the active run and
single last-run slot, plus current revision and rollout provenance status.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			format, wide, err := parseRolloutOutput(options.output)
			if err != nil {
				return err
			}
			return options.run(cmd.Context(), f, args[0], format, wide)
		},
	}
	cmd.Flags().StringVarP(&options.output, "output", "o", "table", "Output format: table, wide, json, or yaml")
	return cmd
}

func (o *historyOptions) run(
	ctx context.Context,
	f factory.Factory,
	name string,
	format report.Format,
	wide bool,
) error {
	if problems := utilvalidation.IsDNS1123Subdomain(name); len(problems) > 0 {
		return ErrInvalidInferenceServiceName
	}
	namespace, _, err := f.Namespace()
	if err != nil {
		return err
	}
	if problems := utilvalidation.IsDNS1123Label(namespace); len(problems) > 0 {
		return ErrInvalidNamespace
	}
	client, err := f.OMEClient()
	if err != nil {
		return err
	}
	isvc, err := client.OmeV1beta1().InferenceServices(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return apierror.Friendly(err)
	}
	if isvc == nil {
		return rolloutprojection.ErrNilInferenceService
	}
	if isvc.Name != name {
		return ErrReturnedInferenceServiceNameMismatch
	}
	if isvc.Namespace != namespace {
		return ErrReturnedInferenceServiceNamespaceMismatch
	}
	reportValue, err := o.projector(isvc, o.clock)
	if err != nil {
		return err
	}
	if wide {
		if err := reportValue.WideTable().Write(o.streams.Out); err != nil {
			return fmt.Errorf("write rollout history table: %w", err)
		}
		return nil
	}
	return report.Write(o.streams.Out, format, reportValue)
}
