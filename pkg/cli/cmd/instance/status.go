package instance

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/apierror"
	"sigs.k8s.io/ome/pkg/cli/effective"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/instancecollection"
	"sigs.k8s.io/ome/pkg/cli/instancestatusprojection"
	clinamespace "sigs.k8s.io/ome/pkg/cli/namespace"
	"sigs.k8s.io/ome/pkg/cli/observation"
	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/runtimeinheritance"
)

var (
	ErrStatusComponentRequired = errors.New("--component is required")
	ErrStatusComponentInvalid  = errors.New("component must be engine, decoder, or router")
	ErrStatusIndexInvalid      = errors.New("instance index must be an integer from 0 through 2147483647")
)

type statusDependencies struct {
	clock            reportv1alpha1.Clock
	irLimits         instancecollection.Limits
	podLimits        paging.Limits
	eventLimits      observation.EventLimits
	projectionLimits instancestatusprojection.Limits
	runtimeLimits    paging.Limits
}

func defaultInstanceStatusDependencies() statusDependencies {
	return statusDependencies{
		clock: reportv1alpha1.SystemClock{},
		irLimits: instancecollection.Limits{
			Paging:        paging.Limits{PageSize: 20, MaxItems: 60, MaxPages: 3, RequestTimeout: 10 * time.Second},
			MaxStatusRows: 4096,
			Details: instancecollection.DetailLimits{
				MaxConditions: 16, MaxScannedConditions: 64, MaxNodeHints: 16, MaxScannedNodeHints: 64,
				MaxMigrations: 16, MaxScannedMigrations: 64,
			},
		},
		podLimits:     paging.Limits{PageSize: 32, MaxItems: 64, MaxPages: 2, RequestTimeout: 10 * time.Second},
		runtimeLimits: paging.Limits{PageSize: 20, MaxItems: 60, MaxPages: 3, RequestTimeout: 10 * time.Second},
		eventLimits: observation.EventLimits{
			Paging:     paging.Limits{PageSize: 25, MaxItems: 50, MaxPages: 2, RequestTimeout: 10 * time.Second},
			MaxTargets: 17, MaxConcurrent: 4,
		},
		projectionLimits: instancestatusprojection.Limits{
			MaxInstances: 4096, MaxPods: 16, MaxContainerStatuses: 64, MaxPodConditions: 64, MaxEvents: 100,
		},
	}
}

type statusOptions struct {
	streams    genericiooptions.IOStreams
	output     string
	component  string
	format     report.Format
	wide       bool
	index      int32
	deps       statusDependencies
	namespaces *clinamespace.Options
}

func newStatusCmd(f factory.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	return newStatusCmdWithDependencies(f, streams, defaultInstanceStatusDependencies())
}

func newStatusCmdWithDependencies(f factory.Factory, streams genericiooptions.IOStreams, deps statusDependencies) *cobra.Command {
	o := &statusOptions{streams: streams, deps: deps, namespaces: clinamespace.NewOptions()}
	cmd := &cobra.Command{
		Use:   "status INFERENCESERVICE INDEX --component COMPONENT",
		Short: "Show one logical instance and bounded live evidence",
		Long: `Show one OMENative logical instance with bounded live Pod and Warning Event evidence.

IR status is authoritative for identity, phase, incarnation, revisions,
admission, persisted pod counts, conditions, operation, and last failure. Pods
never create a logical instance or upgrade stale, malformed, or duplicate IR
evidence. A RawDeployment component is reported as NotOMENative.

Effective deployment mode follows the controller's live-runtime or pinned
ControllerRevision snapshot; --ome-namespace selects the revision namespace.
The report includes ReadySince, active ordinal, and migrations involving the
selected source or surge instance. Status encoding provenance is reported as
DenseV1 or ColumnarV2 when a complete related InferenceReplica was collected.
It is unavailable when that evidence cannot be established.

POD Ready is the Kubernetes Ready condition. Serving is the controller-owned
ome.io/serving readiness gate. POD restarts total bounded init and regular
container restart counters. Deleting reports a deletion timestamp. Warning
Event messages are never shown; only safe target, reason, count, and time
fields are retained. Operation/failure and condition free-form text is
sanitized, credential-shaped text is redacted, and machine output is capped.

The command is read-only. Output formats are table, wide, json or yaml. The default
table uses a vertical FIELD/VALUE view whose physical lines are at most 80
display columns. Wide shows complete bounded identities, revisions, nodes,
operation details, and timestamps. JSON and YAML retain the same safe report.`,
		Example: `  kubectl ome instance status chat 0 --component engine -n prod
  kubectl ome instance status chat 2 --component decoder -o json`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.validate(args[0], args[1]); err != nil {
				return err
			}
			return o.run(cmd.Context(), f, args[0])
		},
	}
	cmd.Flags().StringVar(&o.component, "component", "", "Component: engine, decoder or router (required)")
	cmd.Flags().StringVarP(&o.output, "output", "o", "table", "Output format: table, wide, json or yaml")
	o.namespaces.AddOMEFlags(cmd.Flags())
	return cmd
}

func (o *statusOptions) validate(name, rawIndex string) error {
	requested := strings.ToLower(strings.TrimSpace(o.output))
	o.wide = requested == "wide"
	if o.wide {
		requested = "table"
	}
	format, err := report.ParseFormat(requested)
	if err != nil {
		return err
	}
	o.format = format
	if len(utilvalidation.IsDNS1123Subdomain(name)) > 0 {
		return ErrInvalidInferenceServiceName
	}
	if strings.TrimSpace(o.component) == "" {
		return ErrStatusComponentRequired
	}
	if !validStatusComponent(o.component) {
		return ErrStatusComponentInvalid
	}
	index, err := strconv.ParseInt(rawIndex, 10, 32)
	if err != nil || index < 0 {
		return ErrStatusIndexInvalid
	}
	o.index = int32(index)
	return nil
}

func (o *statusOptions) run(ctx context.Context, f factory.Factory, name string) error {
	namespace, _, err := f.Namespace()
	if err != nil {
		return fmt.Errorf("resolve namespace: %w", err)
	}
	if namespace == "" || len(utilvalidation.IsDNS1123Label(namespace)) > 0 {
		return ErrInvalidNamespace
	}
	resolvedNamespaces, err := o.namespaces.Resolve(namespace)
	if err != nil {
		return err
	}
	omeClient, err := f.OMEClient()
	if err != nil {
		return fmt.Errorf("create OME client: %w", err)
	}
	getCtx, cancel := context.WithTimeout(ctx, o.deps.irLimits.Paging.RequestTimeout)
	isvc, err := omeClient.OmeV1beta1().InferenceServices(namespace).Get(getCtx, name, metav1.GetOptions{})
	requestErr := getCtx.Err()
	cancel()
	if requestErr != nil {
		return requestErr
	}
	if err != nil {
		return fmt.Errorf("get InferenceService %q: %w", namespace+"/"+name, apierror.Friendly(err))
	}
	if err := validateReturnedISVC(isvc, namespace, name); err != nil {
		return err
	}
	component := omev1beta1.ComponentType(o.component)
	mode := resolveStatusDeployment(ctx, f, isvc, component, resolvedNamespaces.OMENamespace, o.deps.runtimeLimits)
	input := instancestatusprojection.Input{
		InferenceService: isvc, Component: component, Index: o.index,
		NotOMENative:          mode.resolved && mode.mode != constants.OMENative,
		DeploymentMode:        reportv1alpha1.DeploymentMode(mode.mode),
		DeploymentModeSource:  reportv1alpha1.DeploymentModeSource(mode.source),
		DeploymentModeOrigin:  mode.origin,
		DeploymentUnavailable: mode.unavailable,
		Pods:                  observation.Collection[corev1.Pod]{Items: []corev1.Pod{}},
		Events:                observation.EventCollection{Items: []corev1.Event{}, Failures: []observation.SourceFailure{}},
	}
	if input.NotOMENative {
		return o.projectAndWrite(input, namespace, name)
	}

	irLimits := o.deps.irLimits
	irLimits.Details.SelectedComponent = component
	irLimits.Details.SelectedIndex = o.index
	collection, collectionErr := instancecollection.CollectRelated(
		ctx, omeClient.OmeV1beta1().InferenceReplicas(namespace), isvc, irLimits,
	)
	input.Collection = collection
	if collectionErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if errors.Is(collectionErr, context.Canceled) {
			return collectionErr
		}
		input.CollectionUnavailable = collectionUnavailableReason(collectionErr)
	}
	preliminary, err := instancestatusprojection.Project(input, o.deps.projectionLimits, o.deps.clock)
	if err != nil {
		return fmt.Errorf("project instance status for InferenceService %q: %w", namespace+"/"+name, err)
	}
	if preliminary.Content.Instance == nil {
		return o.write(preliminary)
	}
	ir := exactComponentReplica(collection, component)
	if ir == nil {
		return o.write(preliminary)
	}

	kubeClient, kubeErr := f.KubeClient()
	if kubeErr != nil {
		input.PodsUnavailable = reportv1alpha1.UnavailableUnreadable
		input.Events.Failures = []observation.SourceFailure{{Err: kubeErr}}
		return o.projectAndWrite(input, namespace, name)
	}
	selector := labels.Set{
		constants.InferenceServicePodLabelKey: isvc.Name,
		constants.OMEComponentLabel:           string(component),
		query.LabelManagedBy:                  query.ManagedByOMENative,
		query.LabelInstanceIdx:                strconv.FormatInt(int64(o.index), 10),
	}.AsSelector().String()
	pods, podErr := observation.CollectPods(ctx, kubeClient.CoreV1(), namespace, selector, o.deps.podLimits)
	input.Pods = pods
	if podErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if errors.Is(podErr, context.Canceled) {
			return podErr
		}
		input.PodsUnavailable = unavailableReason(podErr)
	}
	maxEventTargets := min(o.deps.projectionLimits.MaxPods, o.deps.eventLimits.MaxTargets)
	targets, skippedEventTargets := instancestatusprojection.EventTargets(
		isvc, ir, o.index, pods.Items, o.deps.projectionLimits, maxEventTargets,
	)
	events, eventErr := observation.CollectWarningEvents(ctx, kubeClient.CoreV1(), targets, o.deps.eventLimits)
	input.Events = events
	input.Events.SkippedTargets += skippedEventTargets
	if eventErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if errors.Is(eventErr, context.Canceled) {
			return eventErr
		}
		input.Events.Failures = append(input.Events.Failures, observation.SourceFailure{Err: eventErr})
	}
	return o.projectAndWrite(input, namespace, name)
}

func (o *statusOptions) projectAndWrite(input instancestatusprojection.Input, namespace, name string) error {
	projected, err := instancestatusprojection.Project(input, o.deps.projectionLimits, o.deps.clock)
	if err != nil {
		return fmt.Errorf("project instance status for InferenceService %q: %w", namespace+"/"+name, err)
	}
	return o.write(projected)
}

func (o *statusOptions) write(projected reportv1alpha1.InstanceStatusReport) error {
	if o.wide {
		if err := projected.WideTable().Write(o.streams.Out); err != nil {
			return fmt.Errorf("write instance status: %w", err)
		}
		return nil
	}
	if err := report.Write(o.streams.Out, o.format, projected); err != nil {
		return fmt.Errorf("write instance status: %w", err)
	}
	return nil
}

type statusDeploymentResolution struct {
	mode        constants.DeploymentModeType
	source      effective.ComponentDeploymentModeSource
	origin      string
	resolved    bool
	unavailable reportv1alpha1.UnavailableReason
}

func resolveStatusDeployment(ctx context.Context, f factory.Factory, isvc *omev1beta1.InferenceService, component omev1beta1.ComponentType, omeNamespace string, limits paging.Limits) statusDeploymentResolution {
	if mode, source, ok := definitiveStatusDeployment(isvc, component); ok {
		return statusDeploymentResolution{mode: mode, source: source, origin: "InferenceService", resolved: true}
	}
	client, err := f.RuntimeClient()
	if err != nil {
		return statusDeploymentResolution{unavailable: reportv1alpha1.UnavailableUnreadable}
	}
	liveResolver, err := effective.NewBoundedRuntimeResolver(client, limits)
	if err != nil {
		return statusDeploymentResolution{unavailable: reportv1alpha1.UnavailableUnreadable}
	}
	kubeClient, err := f.KubeClient()
	if err != nil {
		return statusDeploymentResolution{unavailable: reportv1alpha1.UnavailableUnreadable}
	}
	resolver, err := effective.NewRuntimePinResolver(kubeClient.AppsV1(), liveResolver, omeNamespace, limits)
	if err != nil {
		return statusDeploymentResolution{unavailable: reportv1alpha1.UnavailableUnreadable}
	}
	resolveCtx, cancel := context.WithTimeout(ctx, limits.RequestTimeout)
	defer cancel()
	state, err := resolver.Resolve(resolveCtx, isvc, effective.RuntimeResolveOptions{})
	if err != nil {
		return statusDeploymentResolution{unavailable: unavailableReason(err)}
	}
	active, err := state.RequireActive()
	if err != nil {
		return statusDeploymentResolution{unavailable: deploymentUnavailableReason(state)}
	}
	for _, candidate := range active.Components() {
		if candidate.Type == component {
			return statusDeploymentResolution{mode: candidate.DeploymentMode, source: candidate.DeploymentModeSource, origin: string(active.Origin), resolved: true}
		}
	}
	return statusDeploymentResolution{unavailable: reportv1alpha1.UnavailableNotConfigured}
}

func deploymentUnavailableReason(state *effective.RuntimeState) reportv1alpha1.UnavailableReason {
	if state == nil {
		return reportv1alpha1.UnavailableUnreadable
	}
	switch state.LiveAvailability() {
	case effective.LiveRuntimeNotFound:
		return reportv1alpha1.UnavailableNotFound
	case effective.LiveRuntimeDisabled:
		return reportv1alpha1.UnavailableDisabled
	}
	for _, issue := range state.SourceIssues() {
		var cycle *runtimeinheritance.CycleError
		var depth *runtimeinheritance.MaxDepthExceededError
		switch {
		case errors.As(issue, &cycle):
			return reportv1alpha1.UnavailableCycle
		case errors.As(issue, &depth):
			return reportv1alpha1.UnavailableMaxDepthExceeded
		case apierrors.IsForbidden(issue):
			return reportv1alpha1.UnavailableForbidden
		}
	}
	switch state.PinState {
	case effective.RuntimePinStateRevisionMissing:
		return reportv1alpha1.UnavailableNotFound
	case effective.RuntimePinStateRevisionDisabled:
		return reportv1alpha1.UnavailableDisabled
	case effective.RuntimePinStateRevisionInvalid, effective.RuntimePinStateInvalidIntent:
		return reportv1alpha1.UnavailableMalformedPayload
	default:
		return reportv1alpha1.UnavailableUnreadable
	}
}

func definitiveStatusDeployment(isvc *omev1beta1.InferenceService, component omev1beta1.ComponentType) (constants.DeploymentModeType, effective.ComponentDeploymentModeSource, bool) {
	if value, present := isvc.Annotations[constants.DeploymentMode]; present && constants.DeploymentModeType(value) == constants.VirtualDeployment {
		return constants.VirtualDeployment, effective.DeploymentModeServiceAnnotation, true
	}
	var annotations map[string]string
	switch component {
	case omev1beta1.EngineComponent:
		if isvc.Spec.Engine != nil {
			annotations = isvc.Spec.Engine.Annotations
		}
	case omev1beta1.DecoderComponent:
		if isvc.Spec.Decoder != nil {
			annotations = isvc.Spec.Decoder.Annotations
		}
	case omev1beta1.RouterComponent:
		if isvc.Spec.Router != nil {
			annotations = isvc.Spec.Router.Annotations
		}
	}
	if value, present := annotations[constants.DeploymentMode]; present {
		mode := constants.DeploymentModeType(value)
		if mode.IsValid() {
			return mode, effective.DeploymentModeComponentAnnotation, true
		}
	}
	return "", "", false
}

func validateReturnedISVC(isvc *omev1beta1.InferenceService, namespace, name string) error {
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
	return nil
}

func validStatusComponent(value string) bool {
	return value == string(omev1beta1.EngineComponent) || value == string(omev1beta1.DecoderComponent) || value == string(omev1beta1.RouterComponent)
}

func exactComponentReplica(collection instancecollection.Result, component omev1beta1.ComponentType) *omev1beta1.InferenceReplica {
	var found *omev1beta1.InferenceReplica
	for i := range collection.Items {
		if collection.Items[i].Spec.Component != component {
			continue
		}
		if found != nil {
			return nil
		}
		found = &collection.Items[i]
	}
	return found
}

func unavailableReason(err error) reportv1alpha1.UnavailableReason {
	switch {
	case apierrors.IsForbidden(err):
		return reportv1alpha1.UnavailableForbidden
	case apierrors.IsNotFound(err):
		return reportv1alpha1.UnavailableNotFound
	default:
		return reportv1alpha1.UnavailableUnreadable
	}
}
