package traffic

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/apierror"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/trafficprojection"
)

var (
	ErrReturnedInferenceServiceNameMismatch      = errors.New("returned inference service name does not match request")
	ErrReturnedInferenceServiceNamespaceMismatch = errors.New("returned inference service namespace does not match request")
	ErrReturnedInferenceServiceUIDMissing        = errors.New("returned inference service has no UID")
)

type statusProjector func(*omev1beta1.InferenceService, reportv1alpha1.Clock) (reportv1alpha1.TrafficStatusReport, error)

type statusDependencies struct {
	clock   reportv1alpha1.Clock
	project statusProjector
}

type statusOptions struct {
	genericiooptions.IOStreams
	output string
	deps   statusDependencies
}

func newStatusCmd(f factory.Factory, streams genericiooptions.IOStreams, deps statusDependencies) *cobra.Command {
	o := &statusOptions{IOStreams: streams, deps: deps}
	cmd := &cobra.Command{
		Use:   "status INFERENCESERVICE",
		Short: "Show controller-reported traffic status",
		Long: `Show bounded controller-reported traffic evidence from one InferenceService.
It does not query backend policies, HTTPRoutes, Services, or pods.
Reported routes, endpoints, and weights describe controller status.
It does not prove data-plane realization or live reachability.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return o.run(cmd.Context(), f, args[0])
		},
	}
	cmd.Flags().StringVarP(&o.output, "output", "o", "table", "Output format: table, wide, json or yaml")
	return cmd
}

func (o *statusOptions) run(ctx context.Context, f factory.Factory, name string) error {
	format, wide, err := parseStatusOutput(o.output)
	if err != nil {
		return err
	}
	if problems := utilvalidation.IsDNS1123Subdomain(name); len(problems) > 0 {
		return fmt.Errorf("invalid InferenceService name %q: %s", name, strings.Join(problems, "; "))
	}
	namespace, _, err := f.Namespace()
	if err != nil {
		return fmt.Errorf("resolve namespace: %w", err)
	}
	if namespace == "" {
		return errors.New("resolved namespace must not be empty")
	}
	if problems := utilvalidation.IsDNS1123Label(namespace); len(problems) > 0 {
		return fmt.Errorf("invalid resolved namespace: %s", strings.Join(problems, "; "))
	}
	client, err := f.OMEClient()
	if err != nil {
		return fmt.Errorf("create OME client: %w", err)
	}
	isvc, err := client.OmeV1beta1().InferenceServices(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get InferenceService %q: %w", namespace+"/"+name, apierror.Friendly(err))
	}
	if isvc == nil {
		return trafficprojection.ErrInferenceServiceRequired
	}
	if isvc.Name != name {
		return ErrReturnedInferenceServiceNameMismatch
	}
	if isvc.Namespace != namespace {
		return ErrReturnedInferenceServiceNamespaceMismatch
	}
	if isvc.UID == "" {
		return ErrReturnedInferenceServiceUIDMissing
	}
	reportValue, err := o.deps.project(isvc, o.deps.clock)
	if err != nil {
		return fmt.Errorf("project traffic status for InferenceService %q: %w", namespace+"/"+name, err)
	}
	if wide {
		if err := reportValue.WideTable().Write(o.Out); err != nil {
			return fmt.Errorf("write traffic status: %w", err)
		}
		return nil
	}
	if err := report.Write(o.Out, format, reportValue); err != nil {
		return fmt.Errorf("write traffic status: %w", err)
	}
	return nil
}

func parseStatusOutput(value string) (report.Format, bool, error) {
	if value == "wide" {
		return report.FormatTable, true, nil
	}
	switch value {
	case "", "table", "json", "yaml":
		format, err := report.ParseFormat(value)
		return format, false, err
	default:
		return "", false, fmt.Errorf("unsupported output format %q (supported: table, wide, json, yaml)", value)
	}
}
