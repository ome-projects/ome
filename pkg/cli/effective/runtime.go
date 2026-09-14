// Package effective resolves the authoritative resources and effective
// configuration behind CLI service targets.
package effective

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"sigs.k8s.io/ome/pkg/runtimeinheritance"
	"sigs.k8s.io/ome/pkg/runtimeselector"
)

// ErrUnsafeRuntimeSerialization prevents internal, potentially secret-bearing
// runtime and component specs from becoming an accidental output contract.
// Callers must first project them into an allowlisted report type.
var ErrUnsafeRuntimeSerialization = errors.New("effective runtime configuration must be projected to an allowlisted report before serialization")

// RuntimeSelectionSource identifies whether the service named a runtime or
// the operator selector chose one from the referenced model.
type RuntimeSelectionSource string

const (
	RuntimeExplicit RuntimeSelectionSource = "Explicit"
	RuntimeSelected RuntimeSelectionSource = "Selected"
)

type runtimeSelector interface {
	GetRuntime(context.Context, string, string, string) (*v1beta1.ServingRuntimeSpec, bool, error)
	SelectRuntime(context.Context, *v1beta1.BaseModelSpec, *v1beta1.InferenceService) (*runtimeselector.RuntimeSelection, error)
	ValidateRuntime(context.Context, string, *v1beta1.BaseModelSpec, *v1beta1.InferenceService) error
}

// RuntimeAdvisoryCode identifies bounded, non-fatal evidence discovered while
// resolving the same runtime configuration path as the operator.
type RuntimeAdvisoryCode string

const (
	// RuntimeAdvisoryDeclaredCompatibilityMismatch matches the operator's
	// deliberate choice to continue with a named runtime for a non-sharded
	// model despite a declared compatibility mismatch.
	RuntimeAdvisoryDeclaredCompatibilityMismatch RuntimeAdvisoryCode = "DeclaredCompatibilityMismatch"
	// RuntimeAdvisoryInvalidDeclaredKind records a non-nil runtime Kind that is
	// not one of the two supported API values. Reads may continue through the
	// selector's fallback behavior, but pin-aware mutations must fail closed.
	RuntimeAdvisoryInvalidDeclaredKind RuntimeAdvisoryCode = "InvalidDeclaredKind"
)

// RuntimeAdvisory is intentionally code-only. In particular, it must not
// forward selector errors because their text can contain user-authored model
// and runtime values. A report layer can map each code to fixed presentation.
type RuntimeAdvisory struct {
	Code RuntimeAdvisoryCode
}

// InheritanceObservationState records whether the declared inheritance chain
// could be observed independently of the authoritative runtime snapshot.
type InheritanceObservationState string

const (
	InheritanceObserved    InheritanceObservationState = "Observed"
	InheritanceUnavailable InheritanceObservationState = "Unavailable"
)

// InheritanceUnavailableReason is a bounded classification of why declared
// inheritance provenance could not be observed.
type InheritanceUnavailableReason string

const (
	InheritanceNotFound         InheritanceUnavailableReason = "NotFound"
	InheritanceForbidden        InheritanceUnavailableReason = "Forbidden"
	InheritanceCycle            InheritanceUnavailableReason = "Cycle"
	InheritanceMaxDepthExceeded InheritanceUnavailableReason = "MaxDepthExceeded"
	InheritanceMalformed        InheritanceUnavailableReason = "Malformed"
	InheritanceUnreadable       InheritanceUnavailableReason = "Unreadable"
)

// RuntimeSourceReference is a safe identity-only observation of one declared
// runtime source. It never contains labels, annotations, or a runtime spec.
type RuntimeSourceReference struct {
	APIVersion      string
	Kind            string
	Namespace       string
	Name            string
	UID             types.UID
	Generation      int64
	ResourceVersion string
}

// InheritanceObservation records a same-traversal, root-first source chain or
// one bounded reason why it is unavailable. Its fields are private so callers
// cannot construct contradictory state/reason/chain combinations.
type InheritanceObservation struct {
	state             InheritanceObservationState
	chain             []RuntimeSourceReference
	unavailableReason InheritanceUnavailableReason
}

func observedInheritance(chain []RuntimeSourceReference) InheritanceObservation {
	if len(chain) == 0 {
		return unavailableInheritance(InheritanceMalformed)
	}
	return InheritanceObservation{state: InheritanceObserved, chain: append([]RuntimeSourceReference{}, chain...)}
}

func unavailableInheritance(reason InheritanceUnavailableReason) InheritanceObservation {
	if !knownInheritanceUnavailableReason(reason) {
		reason = InheritanceUnreadable
	}
	return InheritanceObservation{
		state:             InheritanceUnavailable,
		chain:             []RuntimeSourceReference{},
		unavailableReason: reason,
	}
}

// State returns whether the declared chain was observed.
func (o InheritanceObservation) State() InheritanceObservationState {
	return o.state
}

// Chain returns a defensive copy of the root-first source identities.
func (o InheritanceObservation) Chain() []RuntimeSourceReference {
	return append([]RuntimeSourceReference{}, o.chain...)
}

// UnavailableReason returns the bounded reason when State is Unavailable.
func (o InheritanceObservation) UnavailableReason() InheritanceUnavailableReason {
	return o.unavailableReason
}

// Validate rejects zero, unknown, and contradictory observations.
func (o InheritanceObservation) Validate() error {
	switch o.state {
	case InheritanceObserved:
		if len(o.chain) == 0 || o.unavailableReason != "" {
			return errors.New("observed inheritance requires a nonempty chain and no unavailable reason")
		}
		return nil
	case InheritanceUnavailable:
		if len(o.chain) != 0 || !knownInheritanceUnavailableReason(o.unavailableReason) {
			return errors.New("unavailable inheritance requires an empty chain and one known reason")
		}
		return nil
	default:
		return errors.New("inheritance observation state is invalid")
	}
}

func knownInheritanceUnavailableReason(reason InheritanceUnavailableReason) bool {
	switch reason {
	case InheritanceNotFound, InheritanceForbidden, InheritanceCycle,
		InheritanceMaxDepthExceeded, InheritanceMalformed, InheritanceUnreadable:
		return true
	default:
		return false
	}
}

// ModelResolution records the actual model scope selected by the same
// namespaced-first lookup used by the operator.
type ModelResolution struct {
	Name            string
	Kind            string
	Namespace       string
	UID             types.UID
	Generation      int64
	ResourceVersion string
	spec            *v1beta1.BaseModelSpec
}

// MarshalJSON rejects direct serialization because the private spec can contain
// user-authored model configuration that is not an allowlisted CLI schema.
func (ModelResolution) MarshalJSON() ([]byte, error) {
	return nil, ErrUnsafeRuntimeSerialization
}

// MarshalYAML rejects direct YAML serialization for the same reason as
// MarshalJSON.
func (ModelResolution) MarshalYAML() (any, error) {
	return nil, ErrUnsafeRuntimeSerialization
}

// RuntimePinSourceState records whether controller pin-source identity is
// relevant and safe to claim for this live resolution.
type RuntimePinSourceState string

const (
	RuntimePinSourceNotApplicable RuntimePinSourceState = "NotApplicable"
	RuntimePinSourceResolved      RuntimePinSourceState = "Resolved"
	RuntimePinSourceInvalid       RuntimePinSourceState = "Invalid"
)

// RuntimePinSource preserves the controller's revision naming/label scope.
// It is deliberately separate from the actual live runtime scope because a
// cluster-declared lookup can fall back to a namespaced live object.
type RuntimePinSource struct {
	State     RuntimePinSourceState
	Kind      string
	Namespace string
}

// Validate rejects unknown and contradictory pin-source state.
func (s RuntimePinSource) Validate() error {
	switch s.State {
	case RuntimePinSourceNotApplicable, RuntimePinSourceInvalid:
		if s.Kind != "" || s.Namespace != "" {
			return errors.New("non-resolved runtime pin source must not claim kind or namespace")
		}
		return nil
	case RuntimePinSourceResolved:
		switch s.Kind {
		case runtimeselector.KindClusterServingRuntime:
			if s.Namespace != "" {
				return errors.New("cluster runtime pin source must not have a namespace")
			}
			return nil
		case runtimeselector.KindServingRuntime:
			if s.Namespace == "" {
				return errors.New("namespaced runtime pin source requires a namespace")
			}
			return nil
		default:
			return errors.New("resolved runtime pin source kind is invalid")
		}
	default:
		return errors.New("runtime pin source state is invalid")
	}
}

// RuntimeResolution records the actual runtime scope, the separately observed
// declared inheritance chain, and the exact live spec snapshot the operator
// uses for its component merge. Namespace is empty for a
// ClusterServingRuntime.
//
// Auto-selected runtimes deliberately retain the selector's raw spec snapshot:
// the current operator does not re-resolve inheritance after selection.
// DeclaredInheritance is an independent object-identity observation and does
// not claim to be the snapshot that produced spec. When AutoSync is false,
// pin-aware consumers must resolve the ControllerRevision before presenting it
// as the active configuration; the separate pin-provenance layer owns that
// step.
//
// spec may contain literal credentials in container fields. It is private
// resolution state and must never be serialized directly; report layers use
// an allowlisted redacted summary.
type RuntimeResolution struct {
	Name                string
	Kind                string
	Namespace           string
	UID                 string
	Generation          int64
	IdentityObserved    bool
	SelectionSource     RuntimeSelectionSource
	RequestedKind       string
	RequestedKindSet    bool
	PinSource           RuntimePinSource
	DeclaredInheritance InheritanceObservation
	resourceVersion     string
	spec                *v1beta1.ServingRuntimeSpec
	AutoSync            bool
	RequestedRevision   string
}

// MarshalJSON rejects direct serialization because the private spec can
// contain literal credentials in pod-template fields.
func (RuntimeResolution) MarshalJSON() ([]byte, error) {
	return nil, ErrUnsafeRuntimeSerialization
}

// MarshalYAML rejects direct YAML serialization for the same reason as
// MarshalJSON.
func (RuntimeResolution) MarshalYAML() (any, error) {
	return nil, ErrUnsafeRuntimeSerialization
}

// ComponentDeploymentModeSource records the precedence rung that selected a
// component's effective deployment mode.
type ComponentDeploymentModeSource string

const (
	DeploymentModeComponentAnnotation ComponentDeploymentModeSource = "ComponentAnnotation"
	DeploymentModeServiceSpec         ComponentDeploymentModeSource = "ServiceSpec"
	DeploymentModeLeaderWorkerShape   ComponentDeploymentModeSource = "LeaderWorkerShape"
	DeploymentModeDefault             ComponentDeploymentModeSource = "Default"
)

// EffectiveComponent is one merged runtime/InferenceService component. Only
// the private spec matching Type is populated.
type EffectiveComponent struct {
	Type                 v1beta1.ComponentType
	DeploymentMode       constants.DeploymentModeType
	DeploymentModeSource ComponentDeploymentModeSource
	engine               *v1beta1.EngineSpec
	decoder              *v1beta1.DecoderSpec
	router               *v1beta1.RouterSpec
}

// MarshalJSON rejects direct serialization because merged component specs can
// contain literal credentials in pod-template fields.
func (EffectiveComponent) MarshalJSON() ([]byte, error) {
	return nil, ErrUnsafeRuntimeSerialization
}

// MarshalYAML rejects direct YAML serialization for the same reason as
// MarshalJSON.
func (EffectiveComponent) MarshalYAML() (any, error) {
	return nil, ErrUnsafeRuntimeSerialization
}

// LiveConfiguration is the operator-faithful merge against the authoritative
// live runtime snapshot. Components are ordered engine, decoder, router. Its
// full runtime and component specs are internal and must not be rendered
// without the separate redaction layer.
type LiveConfiguration struct {
	Model      *ModelResolution
	Runtime    RuntimeResolution
	Components []EffectiveComponent
	Advisories []RuntimeAdvisory
}

// MarshalJSON rejects direct serialization of the complete internal
// resolution. Report layers must copy reviewed fields into an allowlisted
// versioned schema instead.
func (LiveConfiguration) MarshalJSON() ([]byte, error) {
	return nil, ErrUnsafeRuntimeSerialization
}

// MarshalYAML rejects direct YAML serialization for the same reason as
// MarshalJSON.
func (LiveConfiguration) MarshalYAML() (any, error) {
	return nil, ErrUnsafeRuntimeSerialization
}

// RuntimeResolver resolves model/runtime references through the same selector
// used by the operator and observes declared inheritance as separate
// provenance.
type RuntimeResolver struct {
	client    ctrlclient.Client
	selector  runtimeSelector
	resolveMu sync.Mutex
}

func NewRuntimeResolver(client ctrlclient.Client) *RuntimeResolver {
	if client == nil {
		return &RuntimeResolver{}
	}
	tracked := newRuntimeSnapshotClient(client)
	return newRuntimeResolver(tracked, runtimeselector.New(tracked))
}

func newRuntimeResolver(client ctrlclient.Client, selector runtimeSelector) *RuntimeResolver {
	return &RuntimeResolver{client: client, selector: selector}
}

// ResolveLive resolves the exact live runtime snapshot used by the operator
// and merges it with the service. See RuntimeResolution for selection,
// redaction, and AutoSync=false caveats.
func (r *RuntimeResolver) ResolveLive(ctx context.Context, isvc *v1beta1.InferenceService) (*LiveConfiguration, error) {
	if r == nil || r.client == nil || r.selector == nil {
		return nil, errors.New("runtime resolver is not configured")
	}
	if isvc == nil {
		return nil, errors.New("InferenceService must not be nil")
	}
	if isvc.Namespace == "" {
		return nil, errors.New("InferenceService namespace must not be empty")
	}
	r.resolveMu.Lock()
	defer r.resolveMu.Unlock()
	if resetter, ok := r.client.(runtimeSnapshotResetter); ok {
		resetter.resetRuntimeSnapshots()
	}

	model, err := resolveModel(ctx, r.client, isvc)
	if err != nil {
		return nil, err
	}

	reference, err := r.resolveRuntimeReference(ctx, isvc, model)
	if err != nil {
		return nil, err
	}

	if reference.spec == nil {
		return nil, fmt.Errorf("resolve runtime %q: empty runtime spec", reference.name)
	}
	if reference.spec.IsDisabled() {
		return nil, &runtimeselector.RuntimeDisabledError{RuntimeName: reference.name, IsCluster: reference.cluster}
	}

	kind := runtimeselector.KindServingRuntime
	namespace := isvc.Namespace
	if reference.cluster {
		kind = runtimeselector.KindClusterServingRuntime
		namespace = ""
	}
	snapshot, snapshotFound := runtimeSnapshotFor(r.client, kind, namespace, reference.name)

	components, err := MergeEffectiveComponents(isvc, reference.spec)
	if err != nil {
		return nil, err
	}
	declaredInheritance, err := observeDeclaredInheritance(
		ctx, r.client, isvc.Namespace, reference.name, reference.cluster,
	)
	if err != nil {
		return nil, err
	}

	autoSync, requestedKind, requestedKindSet, requestedRevision, pinSource := runtimeReferenceIntent(isvc, reference.source)

	return &LiveConfiguration{
		Model: model,
		Runtime: RuntimeResolution{
			Name:                reference.name,
			Kind:                kind,
			Namespace:           namespace,
			UID:                 snapshot.uid,
			Generation:          snapshot.generation,
			IdentityObserved:    snapshotFound && snapshot.identityObserved(),
			SelectionSource:     reference.source,
			RequestedKind:       requestedKind,
			RequestedKindSet:    requestedKindSet,
			PinSource:           pinSource,
			DeclaredInheritance: declaredInheritance,
			resourceVersion:     snapshot.resourceVersion,
			spec:                reference.spec.DeepCopy(),
			AutoSync:            autoSync,
			RequestedRevision:   requestedRevision,
		},
		Components: components,
		Advisories: append([]RuntimeAdvisory{}, reference.advisories...),
	}, nil
}

func runtimeSnapshotFor(
	client ctrlclient.Client,
	kind, namespace, name string,
) (runtimeObjectSnapshot, bool) {
	provider, ok := client.(runtimeSnapshotProvider)
	if !ok {
		return runtimeObjectSnapshot{}, false
	}
	return provider.runtimeSnapshot(kind, namespace, name)
}

type resolvedRuntimeReference struct {
	name       string
	cluster    bool
	source     RuntimeSelectionSource
	spec       *v1beta1.ServingRuntimeSpec
	advisories []RuntimeAdvisory
}

func (r *RuntimeResolver) resolveRuntimeReference(
	ctx context.Context,
	isvc *v1beta1.InferenceService,
	model *ModelResolution,
) (*resolvedRuntimeReference, error) {
	if isvc.Spec.Runtime != nil && isvc.Spec.Runtime.Name != "" {
		name := isvc.Spec.Runtime.Name
		advisories := []RuntimeAdvisory{}
		if !validDeclaredRuntimeKind(isvc.Spec.Runtime.Kind) {
			advisories = append(advisories, RuntimeAdvisory{Code: RuntimeAdvisoryInvalidDeclaredKind})
		}
		if model != nil && runtimeAutoSyncEnabled(isvc.Spec.Runtime) {
			if err := r.selector.ValidateRuntime(ctx, name, model.spec, isvc); err != nil {
				var compatibilityErr *runtimeselector.RuntimeCompatibilityError
				if errors.As(err, &compatibilityErr) && !isvcutils.IsShardedBaseModel(model.spec) {
					advisories = append(advisories, RuntimeAdvisory{
						Code: RuntimeAdvisoryDeclaredCompatibilityMismatch,
					})
				} else {
					return nil, fmt.Errorf("validate explicit runtime %q: %w", name, err)
				}
			}
		}
		spec, cluster, err := r.selector.GetRuntime(ctx, name, isvc.Namespace, runtimeselector.RefKind(isvc.Spec.Runtime))
		if err != nil {
			return nil, fmt.Errorf("resolve explicit runtime %q: %w", name, err)
		}
		return &resolvedRuntimeReference{
			name: name, cluster: cluster, source: RuntimeExplicit, spec: spec, advisories: advisories,
		}, nil
	}
	if model == nil {
		return nil, errors.New("InferenceService must reference a model or runtime")
	}
	selection, err := r.selector.SelectRuntime(ctx, model.spec, isvc)
	if err != nil {
		return nil, fmt.Errorf("select runtime for model %q: %w", model.Name, err)
	}
	if selection == nil || selection.Name == "" || selection.Spec == nil {
		return nil, fmt.Errorf("select runtime for model %q: selector returned an empty result", model.Name)
	}
	return &resolvedRuntimeReference{
		name: selection.Name, cluster: selection.IsCluster, source: RuntimeSelected, spec: selection.Spec,
		advisories: []RuntimeAdvisory{},
	}, nil
}

func runtimeAutoSyncEnabled(reference *v1beta1.ServingRuntimeRef) bool {
	return reference.AutoSync == nil || *reference.AutoSync
}

func runtimeReferenceIntent(
	isvc *v1beta1.InferenceService,
	selectionSource RuntimeSelectionSource,
) (bool, string, bool, string, RuntimePinSource) {
	notApplicable := RuntimePinSource{State: RuntimePinSourceNotApplicable}
	if selectionSource != RuntimeExplicit || isvc.Spec.Runtime == nil || isvc.Spec.Runtime.Name == "" {
		return true, "", false, "", notApplicable
	}

	reference := isvc.Spec.Runtime
	autoSync := runtimeAutoSyncEnabled(reference)
	requestedKind := ""
	requestedKindSet := reference.Kind != nil
	if requestedKindSet {
		requestedKind = *reference.Kind
	}
	requestedRevision := ""
	if reference.Revision != nil {
		requestedRevision = *reference.Revision
	}
	if autoSync {
		return true, requestedKind, requestedKindSet, requestedRevision, notApplicable
	}
	if !validDeclaredRuntimeKind(reference.Kind) {
		return false, requestedKind, requestedKindSet, requestedRevision, RuntimePinSource{State: RuntimePinSourceInvalid}
	}
	if reference.Kind == nil || *reference.Kind == runtimeselector.KindClusterServingRuntime {
		return false, requestedKind, requestedKindSet, requestedRevision, RuntimePinSource{
			State: RuntimePinSourceResolved,
			Kind:  runtimeselector.KindClusterServingRuntime,
		}
	}
	return false, requestedKind, requestedKindSet, requestedRevision, RuntimePinSource{
		State:     RuntimePinSourceResolved,
		Kind:      runtimeselector.KindServingRuntime,
		Namespace: isvc.Namespace,
	}
}

func validDeclaredRuntimeKind(kind *string) bool {
	return kind == nil || *kind == runtimeselector.KindClusterServingRuntime || *kind == runtimeselector.KindServingRuntime
}

func resolveModel(ctx context.Context, client ctrlclient.Client, isvc *v1beta1.InferenceService) (*ModelResolution, error) {
	if isvc.Spec.Model == nil || isvc.Spec.Model.Name == "" {
		return nil, nil
	}
	name := isvc.Spec.Model.Name
	model := &v1beta1.BaseModel{}
	err := client.Get(ctx, types.NamespacedName{Name: name, Namespace: isvc.Namespace}, model)
	if err == nil {
		if model.Spec.Disabled != nil && *model.Spec.Disabled {
			return nil, fmt.Errorf("model %q is disabled", name)
		}
		return &ModelResolution{
			Name: name, Kind: "BaseModel", Namespace: isvc.Namespace,
			UID: model.UID, Generation: model.Generation, ResourceVersion: model.ResourceVersion,
			spec: model.Spec.DeepCopy(),
		}, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("resolve model %q: %w", name, err)
	}

	clusterModel := &v1beta1.ClusterBaseModel{}
	if err := client.Get(ctx, types.NamespacedName{Name: name}, clusterModel); err != nil {
		return nil, fmt.Errorf("resolve model %q: %w", name, err)
	}
	if clusterModel.Spec.Disabled != nil && *clusterModel.Spec.Disabled {
		return nil, fmt.Errorf("model %q is disabled", name)
	}
	return &ModelResolution{
		Name: name, Kind: "ClusterBaseModel",
		UID: clusterModel.UID, Generation: clusterModel.Generation, ResourceVersion: clusterModel.ResourceVersion,
		spec: clusterModel.Spec.DeepCopy(),
	}, nil
}

type inheritanceReadError struct {
	cause error
}

func (e *inheritanceReadError) Error() string {
	return "runtime inheritance source read failed"
}

func (e *inheritanceReadError) Unwrap() error {
	return e.cause
}

func observeDeclaredInheritance(
	ctx context.Context,
	client ctrlclient.Client,
	namespace, name string,
	cluster bool,
) (InheritanceObservation, error) {
	chain, err := observeRuntimeInheritance(ctx, client, namespace, name, cluster)
	if err == nil {
		return observedInheritance(chain), nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return InheritanceObservation{}, fmt.Errorf("observe declared runtime inheritance: %w", ctxErr)
	}
	for _, safetyError := range []error{
		ErrRuntimeObjectIdentityMismatch,
		ErrRuntimeSnapshotChanged,
		ErrRuntimeSnapshotUnbindable,
	} {
		if errors.Is(err, safetyError) {
			return InheritanceObservation{}, safetyError
		}
	}
	return unavailableInheritance(classifyInheritanceUnavailable(err)), nil
}

func observeRuntimeInheritance(
	ctx context.Context,
	client ctrlclient.Client,
	namespace, name string,
	cluster bool,
) ([]RuntimeSourceReference, error) {
	sources := map[string]RuntimeSourceReference{}
	var start *runtimeinheritance.RuntimeRef
	var fetch runtimeinheritance.Fetcher
	var err error
	if cluster {
		fetch = clusterRuntimeObserver(client, sources)
		start, err = fetch(ctx, name)
	} else {
		start, err = namespacedRuntimeHead(ctx, client, namespace, name, sources)
		fetch = namespacedRuntimeObserver(client, namespace, sources)
	}
	if err != nil {
		return nil, err
	}
	_, chain, err := runtimeinheritance.Resolve(ctx, start, fetch, constants.RuntimeInheritMaxDepth)
	if err != nil {
		return nil, err
	}
	references := make([]RuntimeSourceReference, 0, len(chain))
	for _, sourceName := range chain {
		reference, found := sources[sourceName]
		if !found {
			return nil, errors.New("runtime inheritance traversal omitted source identity")
		}
		references = append(references, reference)
	}
	return references, nil
}

func clusterRuntimeObserver(
	client ctrlclient.Client,
	sources map[string]RuntimeSourceReference,
) runtimeinheritance.Fetcher {
	return func(ctx context.Context, name string) (*runtimeinheritance.RuntimeRef, error) {
		runtimeObject := &v1beta1.ClusterServingRuntime{}
		if err := client.Get(ctx, types.NamespacedName{Name: name}, runtimeObject); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, errors.Join(runtimeinheritance.ErrParentNotFound, err)
			}
			return nil, &inheritanceReadError{cause: err}
		}
		sources[runtimeObject.Name] = runtimeSourceReference(runtimeselector.KindClusterServingRuntime, runtimeObject)
		return &runtimeinheritance.RuntimeRef{
			Name: runtimeObject.Name, Spec: &runtimeObject.Spec,
			ParentName: runtimeObject.Annotations[constants.RuntimeInheritFromAnnotationKey],
		}, nil
	}
}

func namespacedRuntimeHead(
	ctx context.Context,
	client ctrlclient.Client,
	namespace, name string,
	sources map[string]RuntimeSourceReference,
) (*runtimeinheritance.RuntimeRef, error) {
	runtimeObject := &v1beta1.ServingRuntime{}
	if err := client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, runtimeObject); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, errors.Join(runtimeinheritance.ErrParentNotFound, err)
		}
		return nil, &inheritanceReadError{cause: err}
	}
	sources[runtimeObject.Name] = runtimeSourceReference(runtimeselector.KindServingRuntime, runtimeObject)
	return &runtimeinheritance.RuntimeRef{
		Name: runtimeObject.Name, Spec: &runtimeObject.Spec,
		ParentName: runtimeObject.Annotations[constants.RuntimeInheritFromAnnotationKey],
	}, nil
}

func namespacedRuntimeObserver(
	client ctrlclient.Client,
	namespace string,
	sources map[string]RuntimeSourceReference,
) runtimeinheritance.Fetcher {
	clusterObserver := clusterRuntimeObserver(client, sources)
	return func(ctx context.Context, name string) (*runtimeinheritance.RuntimeRef, error) {
		runtimeObject := &v1beta1.ServingRuntime{}
		err := client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, runtimeObject)
		if err == nil {
			sources[runtimeObject.Name] = runtimeSourceReference(runtimeselector.KindServingRuntime, runtimeObject)
			return &runtimeinheritance.RuntimeRef{
				Name: runtimeObject.Name, Spec: &runtimeObject.Spec,
				ParentName: runtimeObject.Annotations[constants.RuntimeInheritFromAnnotationKey],
			}, nil
		}
		if !apierrors.IsNotFound(err) {
			return nil, &inheritanceReadError{cause: err}
		}
		return clusterObserver(ctx, name)
	}
}

func runtimeSourceReference(kind string, object metav1.Object) RuntimeSourceReference {
	return RuntimeSourceReference{
		APIVersion: v1beta1.SchemeGroupVersion.String(),
		Kind:       kind, Namespace: object.GetNamespace(), Name: object.GetName(),
		UID: object.GetUID(), Generation: object.GetGeneration(), ResourceVersion: object.GetResourceVersion(),
	}
}

func classifyInheritanceUnavailable(err error) InheritanceUnavailableReason {
	var cycle *runtimeinheritance.CycleError
	if errors.As(err, &cycle) {
		return InheritanceCycle
	}
	var depth *runtimeinheritance.MaxDepthExceededError
	if errors.As(err, &depth) {
		return InheritanceMaxDepthExceeded
	}
	var parentNotFound *runtimeinheritance.ParentNotFoundError
	if errors.As(err, &parentNotFound) || errors.Is(err, runtimeinheritance.ErrParentNotFound) || apierrors.IsNotFound(err) {
		return InheritanceNotFound
	}
	if apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err) {
		return InheritanceForbidden
	}
	var readErr *inheritanceReadError
	if errors.As(err, &readErr) {
		return InheritanceUnreadable
	}
	return InheritanceMalformed
}

// MergeEffectiveComponents applies an authoritative operator runtime snapshot
// to the service and resolves each resulting deployment mode. Inputs are not
// modified.
func MergeEffectiveComponents(isvc *v1beta1.InferenceService, runtimeSpec *v1beta1.ServingRuntimeSpec) ([]EffectiveComponent, error) {
	if isvc == nil {
		return nil, errors.New("InferenceService must not be nil")
	}
	if runtimeSpec == nil {
		return nil, errors.New("runtime spec must not be nil")
	}
	engine, decoder, router, err := isvcutils.MergeRuntimeSpecs(isvc.DeepCopy(), runtimeSpec.DeepCopy(), logr.Discard())
	if err != nil {
		return nil, fmt.Errorf("merge runtime and InferenceService components: %w", err)
	}
	engineMode, decoderMode, routerMode, err := isvcutils.DetermineDeploymentModes(
		engine,
		decoder,
		router,
		runtimeSpec,
		isvc.Spec.DeploymentMode,
	)
	if err != nil {
		return nil, fmt.Errorf("resolve component deployment modes: %w", err)
	}

	components := make([]EffectiveComponent, 0, 3)
	if engine != nil {
		components = append(components, EffectiveComponent{
			Type:                 v1beta1.EngineComponent,
			DeploymentMode:       engineMode,
			DeploymentModeSource: componentDeploymentModeSource(engine.Annotations, engine.Leader != nil || engine.Worker != nil, isvc.Spec.DeploymentMode),
			engine:               engine,
		})
	}
	if decoder != nil {
		components = append(components, EffectiveComponent{
			Type:                 v1beta1.DecoderComponent,
			DeploymentMode:       decoderMode,
			DeploymentModeSource: componentDeploymentModeSource(decoder.Annotations, decoder.Leader != nil || decoder.Worker != nil, isvc.Spec.DeploymentMode),
			decoder:              decoder,
		})
	}
	if router != nil {
		components = append(components, EffectiveComponent{
			Type:                 v1beta1.RouterComponent,
			DeploymentMode:       routerMode,
			DeploymentModeSource: componentDeploymentModeSource(router.Annotations, false, isvc.Spec.DeploymentMode),
			router:               router,
		})
	}
	return components, nil
}

func componentDeploymentModeSource(
	annotations map[string]string,
	hasLeaderWorkerShape bool,
	specMode *constants.DeploymentModeType,
) ComponentDeploymentModeSource {
	if _, found := isvcutils.GetDeploymentModeFromAnnotations(annotations); found {
		return DeploymentModeComponentAnnotation
	}
	if specMode != nil && specMode.IsValid() {
		return DeploymentModeServiceSpec
	}
	if hasLeaderWorkerShape {
		return DeploymentModeLeaderWorkerShape
	}
	return DeploymentModeDefault
}
