package rollout

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"sigs.k8s.io/ome/pkg/cli/apierror"
	"sigs.k8s.io/ome/pkg/cli/exitcode"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/rolloutprojection"
)

var (
	// ErrRolloutValidationInvalid is returned after an Invalid report is written.
	ErrRolloutValidationInvalid = errors.New("rollout validation found invalid configuration")
	// ErrRolloutValidationUnverifiable is returned after an Unverifiable report is written.
	ErrRolloutValidationUnverifiable = errors.New("rollout validation could not verify all prerequisites")
)

type validateOptions struct {
	streams genericiooptions.IOStreams
	output  string
	clock   reportv1alpha1.Clock
}

func newValidateCmd(
	f factory.Factory,
	streams genericiooptions.IOStreams,
	clock reportv1alpha1.Clock,
) *cobra.Command {
	options := &validateOptions{streams: streams, clock: clock}
	cmd := &cobra.Command{
		Use:   "validate INFERENCESERVICE",
		Short: "Validate rollout, traffic, and autoscaling configuration",
		Long: `Validate rollout, traffic, and autoscaling configuration.

The report is written before its assertion is evaluated. Valid returns exit
code 0. Invalid and Unverifiable return exit code 2. API, projection, and
output failures return exit code 1.`,
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

func (o *validateOptions) run(
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
	reportValue, err := rolloutprojection.ProjectValidation(isvc, o.clock)
	if err != nil {
		return err
	}
	if wide {
		if err := reportValue.WideTable().Write(o.streams.Out); err != nil {
			return fmt.Errorf("write validation report table: %w", err)
		}
	} else if err := report.Write(o.streams.Out, format, reportValue); err != nil {
		return fmt.Errorf("write validation report: %w", err)
	}

	switch reportValue.Content.Summary.State {
	case reportv1alpha1.RolloutValidationValid:
		return nil
	case reportv1alpha1.RolloutValidationInvalid:
		return &exitcode.UnmetAssertionError{Err: ErrRolloutValidationInvalid}
	default:
		return &exitcode.UnmetAssertionError{Err: ErrRolloutValidationUnverifiable}
	}
}
