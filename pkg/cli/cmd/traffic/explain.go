package traffic

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/trafficprojection"
)

type explainProjector func(
	*omev1beta1.InferenceService,
	reportv1alpha1.Clock,
) (reportv1alpha1.TrafficExplainReport, error)

type explainDependencies struct {
	clock   reportv1alpha1.Clock
	project explainProjector
}

type explainOptions struct {
	genericiooptions.IOStreams
	output string
	deps   explainDependencies
}

func newExplainCmd(
	f factory.Factory,
	streams genericiooptions.IOStreams,
	deps explainDependencies,
) *cobra.Command {
	o := &explainOptions{IOStreams: streams, deps: deps}
	cmd := &cobra.Command{
		Use:   "explain INFERENCESERVICE",
		Short: "Explain declared and reported traffic behavior",
		Long: `Compare declared traffic intent with controller-reported support and
controller-reported realization evidence for one InferenceService.

The command does not query emitted policies, HTTPRoutes, Services, endpoints,
or pods. Controller status does not prove data-plane realization or live
reachability. Raw annotations, headers, condition messages, credentials,
UIDs, and resource versions are never printed.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return o.run(cmd.Context(), f, args[0])
		},
	}
	cmd.Flags().StringVarP(
		&o.output, "output", "o", "table",
		"Output format: table, wide, json or yaml",
	)
	return cmd
}

func (o *explainOptions) run(
	ctx context.Context,
	f factory.Factory,
	name string,
) error {
	format, wide, err := parseStatusOutput(o.output)
	if err != nil {
		return err
	}
	if problems := utilvalidation.IsDNS1123Subdomain(name); len(problems) > 0 {
		return fmt.Errorf(
			"invalid InferenceService name %q: %s",
			name, strings.Join(problems, "; "),
		)
	}
	namespace, _, err := f.Namespace()
	if err != nil {
		return fmt.Errorf("resolve namespace: %w", err)
	}
	if namespace == "" {
		return errors.New("resolved namespace must not be empty")
	}
	if problems := utilvalidation.IsDNS1123Label(namespace); len(problems) > 0 {
		return fmt.Errorf(
			"invalid resolved namespace: %s", strings.Join(problems, "; "),
		)
	}
	client, err := f.OMEClient()
	if err != nil {
		return fmt.Errorf("create OME client: %w", err)
	}
	isvc, err := client.OmeV1beta1().InferenceServices(namespace).
		Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return sanitizeTrafficExplainAPIError(namespace, name, err)
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
	if o.deps.project == nil {
		return errors.New("traffic explain projector is not configured")
	}
	reportValue, err := o.deps.project(isvc, o.deps.clock)
	if err != nil {
		return fmt.Errorf(
			"project traffic explanation for InferenceService %q: %w",
			namespace+"/"+name, err,
		)
	}
	if wide {
		if err := reportValue.WideTable().Write(o.Out); err != nil {
			return fmt.Errorf("write traffic explanation: %w", err)
		}
		return nil
	}
	if err := report.Write(o.Out, format, reportValue); err != nil {
		return fmt.Errorf("write traffic explanation: %w", err)
	}
	return nil
}

// sanitizedTrafficAPIError preserves errors.Is/errors.As behavior without
// exposing arbitrary API-server or transport text through Error().
type sanitizedTrafficAPIError struct {
	message string
	cause   error
}

func (e *sanitizedTrafficAPIError) Error() string { return e.message }

func (e *sanitizedTrafficAPIError) GoString() string { return e.message }

func (e *sanitizedTrafficAPIError) Unwrap() error { return e.cause }

func sanitizeTrafficExplainAPIError(namespace, name string, cause error) error {
	suffix := "API request failed"
	switch {
	case apierrors.IsNotFound(cause):
		suffix = "not found"
	case apierrors.IsForbidden(cause):
		suffix = "forbidden"
	case apierrors.IsUnauthorized(cause):
		suffix = "unauthorized"
	case errors.Is(cause, context.Canceled):
		suffix = "request cancelled"
	case errors.Is(cause, context.DeadlineExceeded),
		apierrors.IsTimeout(cause), apierrors.IsServerTimeout(cause):
		suffix = "request timed out"
	case apierrors.IsTooManyRequests(cause):
		suffix = "request throttled"
	case apierrors.IsServiceUnavailable(cause):
		suffix = "service unavailable"
	case apierrors.IsInternalError(cause):
		suffix = "server error"
	}
	return &sanitizedTrafficAPIError{
		message: fmt.Sprintf(
			"get InferenceService %q: %s", namespace+"/"+name, suffix,
		),
		cause: cause,
	}
}
