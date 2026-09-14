// Package rollout implements read-only rollout inspection commands.
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
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/rolloutprojection"
)

var (
	// ErrReturnedInferenceServiceNameMismatch rejects a response that is not
	// bound to the requested resource name.
	ErrReturnedInferenceServiceNameMismatch = errors.New("returned inference service name does not match request")
	// ErrReturnedInferenceServiceNamespaceMismatch rejects a response that is
	// not bound to the resolved request namespace.
	ErrReturnedInferenceServiceNamespaceMismatch = errors.New("returned inference service namespace does not match request")
	// ErrInvalidInferenceServiceName rejects an invalid local resource identity
	// without copying the hostile value into user-visible output.
	ErrInvalidInferenceServiceName = errors.New("inference service name is invalid")
	// ErrInvalidNamespace rejects an empty or malformed resolved namespace.
	ErrInvalidNamespace = errors.New("namespace is invalid")
)

// NewCmd builds the rollout command family.
func NewCmd(f factory.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	return newCmdWithClock(f, streams, reportv1alpha1.SystemClock{})
}

func newCmdWithClock(
	f factory.Factory,
	streams genericiooptions.IOStreams,
	clock reportv1alpha1.Clock,
) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rollout",
		Short: "Inspect InferenceService rollouts",
	}
	cmd.AddCommand(newStatusCmd(f, streams, clock))
	cmd.AddCommand(newExplainCmd(f, streams, clock))
	cmd.AddCommand(newValidateCmd(f, streams, clock))
	return cmd
}

type statusOptions struct {
	streams genericiooptions.IOStreams
	output  string
	clock   reportv1alpha1.Clock
}

func newStatusCmd(
	f factory.Factory,
	streams genericiooptions.IOStreams,
	clock reportv1alpha1.Clock,
) *cobra.Command {
	options := &statusOptions{streams: streams, clock: clock}
	cmd := &cobra.Command{
		Use:   "status INFERENCESERVICE",
		Short: "Show rollout progress for an InferenceService",
		Args:  cobra.ExactArgs(1),
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

func parseRolloutOutput(value string) (report.Format, bool, error) {
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

func (o *statusOptions) run(
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
	reportValue, err := rolloutprojection.Project(isvc, o.clock)
	if err != nil {
		return err
	}
	if wide {
		if err := reportValue.WideTable().Write(o.streams.Out); err != nil {
			return fmt.Errorf("write report table: %w", err)
		}
		return nil
	}
	return report.Write(o.streams.Out, format, reportValue)
}
