package instance

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

	"sigs.k8s.io/ome/pkg/cli/apierror"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/instancecollection"
	"sigs.k8s.io/ome/pkg/cli/instanceprojection"
	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

var (
	ErrInvalidInferenceServiceName               = errors.New("inference service name is invalid")
	ErrInvalidNamespace                          = errors.New("namespace is invalid")
	ErrReturnedInferenceServiceNil               = errors.New("returned inference service is nil")
	ErrReturnedInferenceServiceNameMismatch      = errors.New("returned inference service name does not match request")
	ErrReturnedInferenceServiceNamespaceMismatch = errors.New("returned inference service namespace does not match request")
	ErrReturnedInferenceServiceUIDMissing        = errors.New("returned inference service has no UID")
	ErrReturnedInferenceServiceUIDInvalid        = errors.New("returned inference service has an unsafe UID")
)

type instanceProjector func(
	instanceprojection.Input,
	reportv1alpha1.Clock,
) (reportv1alpha1.InstanceListReport, error)

type listDependencies struct {
	clock        reportv1alpha1.Clock
	limits       paging.Limits
	maxInstances int
	project      instanceProjector
}

type listOptions struct {
	streams genericiooptions.IOStreams
	output  string
	deps    listDependencies
}

func newListCmd(f factory.Factory, streams genericiooptions.IOStreams, deps listDependencies) *cobra.Command {
	if deps.project == nil {
		deps.project = instanceprojection.Project
	}
	o := &listOptions{streams: streams, deps: deps}
	cmd := &cobra.Command{
		Use:   "list INFERENCESERVICE",
		Short: "List controller-reported logical instances",
		Long: `List logical OMENative instances from related InferenceReplica status.

PODS is serving/available/total.
AOF is admitted/operation/last-failure presence.

This command does not read Pods or infer live pod state. The API compatibility
field ReadyPodCount is not persisted by OMENative and is intentionally ignored.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return o.run(cmd.Context(), f, args[0])
		},
	}
	cmd.Flags().StringVarP(&o.output, "output", "o", "table", "Output format: table, json or yaml")
	return cmd
}

func (o *listOptions) run(ctx context.Context, f factory.Factory, name string) error {
	format, err := report.ParseFormat(o.output)
	if err != nil {
		return err
	}
	if problems := utilvalidation.IsDNS1123Subdomain(name); len(problems) > 0 {
		return ErrInvalidInferenceServiceName
	}
	namespace, _, err := f.Namespace()
	if err != nil {
		return fmt.Errorf("resolve namespace: %w", err)
	}
	if namespace == "" || len(utilvalidation.IsDNS1123Label(namespace)) > 0 {
		return ErrInvalidNamespace
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
		return ErrReturnedInferenceServiceNil
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
	if !validUID(string(isvc.UID)) {
		return ErrReturnedInferenceServiceUIDInvalid
	}

	collection, collectionErr := instancecollection.CollectRelated(
		ctx, client.OmeV1beta1().InferenceReplicas(namespace), isvc,
		instancecollection.Limits{Paging: o.deps.limits, MaxStatusRows: o.deps.maxInstances},
	)
	if collectionErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if errors.Is(collectionErr, context.Canceled) {
			return collectionErr
		}
	}
	input := instanceprojection.Input{
		InferenceService: isvc, Collection: collection,
		MaxInstances: o.deps.maxInstances,
	}
	if collectionErr != nil {
		input.CollectionUnavailable = collectionUnavailableReason(collectionErr)
	}
	reportValue, err := o.deps.project(input, o.deps.clock)
	if err != nil {
		return fmt.Errorf("project instance list for InferenceService %q: %w", namespace+"/"+name, err)
	}
	if err := report.Write(o.streams.Out, format, reportValue); err != nil {
		return fmt.Errorf("write instance list: %w", err)
	}
	return nil
}

func validUID(uid string) bool {
	if len(uid) > 128 {
		return false
	}
	for _, value := range []byte(uid) {
		if (value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z') ||
			(value >= '0' && value <= '9') || value == '-' || value == '_' || value == '.' {
			continue
		}
		return false
	}
	return true
}

func collectionUnavailableReason(err error) reportv1alpha1.UnavailableReason {
	switch {
	case apierrors.IsForbidden(err):
		return reportv1alpha1.UnavailableForbidden
	case apierrors.IsNotFound(err) || strings.Contains(err.Error(), "the server could not find the requested resource"):
		return reportv1alpha1.UnavailableUnsupportedAPI
	default:
		return reportv1alpha1.UnavailableUnreadable
	}
}
