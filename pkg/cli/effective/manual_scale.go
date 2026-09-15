package effective

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

var (
	ErrManualScaleSource        = errors.New("manual scale source evidence is unavailable or inconsistent")
	ErrManualScalePolicy        = errors.New("manual scale requires locally verifiable autoscaling; winning policy resolution is unavailable")
	ErrManualScaleSourceChanged = errors.New("manual scale source changed; inspect current status and prepare a new request")
)

// ManualScaleIdentity is a whitelisted source identity, never a source payload
// or private runtime resourceVersion.
type ManualScaleIdentity struct {
	Kind       string
	Namespace  string
	Name       string
	UID        string
	Generation int64
}

// ManualScaleSummary presents only the selected source's verified ownership
// and bounds. Callers receive fresh copies of the source slice.
type ManualScaleSummary struct {
	Component  v1beta1.ComponentType
	Class      v1beta1.AutoscalerClass
	ManagedBy  AutoscalingManagedBy
	SpecSource AutoscalingSpecSource
	Minimum    int32
	Maximum    int32
	Parent     ManualScaleIdentity
	Sources    []ManualScaleIdentity
}

type manualScaleStamp struct {
	object          ctrlclient.Object
	identity        ManualScaleIdentity
	resourceVersion string
}

// ManualScaleSource is an opaque proof minted by pin-aware effective
// resolution. Its zero value cannot authorize an action.
type ManualScaleSource struct {
	parent     *v1beta1.InferenceService
	target     string
	summary    ManualScaleSummary
	autoscaler *v1beta1.ComponentAutoscaler
	stamps     []manualScaleStamp
}

func (ManualScaleSource) MarshalJSON() ([]byte, error) { return nil, ErrUnsafeRuntimeSerialization }
func (ManualScaleSource) MarshalYAML() (any, error)    { return nil, ErrUnsafeRuntimeSerialization }
func (ManualScaleSource) String() string               { return "<effective.ManualScaleSource redacted>" }
func (ManualScaleSource) GoString() string             { return "<effective.ManualScaleSource redacted>" }

// ResolveManualScaleSource uses the existing autoscale resolver and its
// privately retained controller-resolved block; it performs no policy reads.
func ResolveManualScaleSource(parent *v1beta1.InferenceService, state *RuntimeState, component v1beta1.ComponentType) (ManualScaleSource, error) {
	invalid := func() (ManualScaleSource, error) { return ManualScaleSource{}, ErrManualScaleSource }
	if parent == nil || state == nil || !state.MatchesInferenceService(parent) ||
		parent.Generation <= 0 || state.Generation != parent.Generation ||
		(state.SyncTokenState != SyncTokenStateAbsent && state.SyncTokenState != SyncTokenStateAcknowledged) ||
		(state.DriftState != RuntimeDriftStateNotReported && state.DriftState != RuntimeDriftStateReportedFalse && state.DriftState != RuntimeDriftStateReportedTrue) {
		return invalid()
	}
	active, err := state.RequireActive()
	if err != nil {
		return invalid()
	}
	switch active.Origin {
	case ConfigurationOriginControllerRevision:
		if _, err := state.RequireConsistentActive(); err != nil {
			return invalid()
		}
	case ConfigurationOriginLiveRuntime:
		live := state.LiveConfiguration()
		if state.PinMode != RuntimePinModeAutoSync || state.DriftState == RuntimeDriftStateReportedTrue || live == nil ||
			!live.Runtime.IdentityObserved || live.Runtime.Generation <= 0 ||
			live.Runtime.Name != active.RuntimeName || live.Runtime.Kind != active.RuntimeKind || live.Runtime.Namespace != active.RuntimeNamespace {
			return invalid()
		}
	default:
		return invalid()
	}
	resolution, err := ResolveAutoscaling(parent, state)
	if err != nil || resolution.ScalingPolicy.State != ScalingPolicyAvailable || resolution.ScalingPolicy.Mode != v1beta1.ScalingIndependent {
		return invalid()
	}
	var selected *EffectiveAutoscalingComponent
	for i := range resolution.Components {
		if resolution.Components[i].Type == component {
			if selected != nil {
				return invalid()
			}
			selected = &resolution.Components[i]
		}
	}
	if selected != nil && selected.State == AutoscalingComponentUnavailable && selected.SpecSource == AutoscalingSpecSourcePolicy {
		return ManualScaleSource{}, ErrManualScalePolicy
	}
	if selected == nil || selected.State != AutoscalingComponentAvailable || selected.DeploymentMode != constants.OMENative ||
		selected.Bounds.State != AutoscalingBoundsAvailable || selected.Bounds.MinReplicas == nil || selected.Bounds.MaxReplicas == nil ||
		*selected.Bounds.MinReplicas < 1 || *selected.Bounds.MaxReplicas < *selected.Bounds.MinReplicas || selected.autoscaler == nil {
		return invalid()
	}
	reported, found := parent.Status.Components[component]
	if !found || reported.ScaleTargetRef == nil || reported.ScaleTargetRef.APIVersion != "ome.io/v1beta1" ||
		reported.ScaleTargetRef.Kind != "InferenceReplica" || !manualScaleName(reported.ScaleTargetRef.Name) || reported.Autoscaler == nil ||
		reported.Autoscaler.Class != selected.Class || string(reported.Autoscaler.ManagedBy) != string(selected.ManagedBy) ||
		reported.Autoscaler.SpecSource != string(selected.SpecSource) || !manualScalePolicyEvidence(parent, component, reported.Autoscaler) {
		return invalid()
	}
	source := ManualScaleSource{parent: parent.DeepCopy(), target: reported.ScaleTargetRef.Name, autoscaler: selected.autoscaler.DeepCopy(),
		summary: ManualScaleSummary{Component: component, Class: selected.Class, ManagedBy: selected.ManagedBy, SpecSource: selected.SpecSource,
			Minimum: *selected.Bounds.MinReplicas, Maximum: *selected.Bounds.MaxReplicas,
			Parent: ManualScaleIdentity{Kind: "InferenceService", Namespace: parent.Namespace, Name: parent.Name, UID: string(parent.UID), Generation: parent.Generation}}}
	if !source.addStamp(&v1beta1.InferenceService{}, source.summary.Parent, parent.ResourceVersion) {
		return invalid()
	}
	if active.Origin == ConfigurationOriginLiveRuntime {
		live := state.LiveConfiguration()
		chain := live.Runtime.DeclaredInheritance.Chain()
		if live.Runtime.DeclaredInheritance.State() != InheritanceObserved || len(chain) == 0 || len(chain) > 32 {
			return invalid()
		}
		// The production inheritance resolver returns the declaration chain
		// root-first; the selected runtime is the last identity, not the root.
		head := chain[len(chain)-1]
		if head.Kind != live.Runtime.Kind || head.Namespace != live.Runtime.Namespace || head.Name != live.Runtime.Name ||
			string(head.UID) != live.Runtime.UID || head.Generation != live.Runtime.Generation || head.ResourceVersion != live.Runtime.resourceVersion {
			return invalid()
		}
		for _, ref := range chain {
			var object ctrlclient.Object
			switch ref.Kind {
			case "ServingRuntime":
				object = &v1beta1.ServingRuntime{}
			case "ClusterServingRuntime":
				object = &v1beta1.ClusterServingRuntime{}
			default:
				return invalid()
			}
			if !source.addStamp(object, ManualScaleIdentity{Kind: ref.Kind, Namespace: ref.Namespace, Name: ref.Name, UID: string(ref.UID), Generation: ref.Generation}, ref.ResourceVersion) {
				return invalid()
			}
		}
		if live.Runtime.SelectionSource == RuntimeSelected {
			if live.Model == nil {
				return invalid()
			}
			model := live.Model
			var object ctrlclient.Object
			switch model.Kind {
			case "BaseModel":
				object = &v1beta1.BaseModel{}
			case "ClusterBaseModel":
				object = &v1beta1.ClusterBaseModel{}
			default:
				return invalid()
			}
			if !source.addStamp(object, ManualScaleIdentity{Kind: model.Kind, Namespace: model.Namespace, Name: model.Name, UID: string(model.UID), Generation: model.Generation}, model.ResourceVersion) {
				return invalid()
			}
		}
	} else {
		found := false
		for _, revision := range state.RevisionObservations() {
			if revision.Name != active.RevisionName {
				continue
			}
			if found || !source.addStamp(&appsv1.ControllerRevision{}, ManualScaleIdentity{Kind: "ControllerRevision", Namespace: revision.Namespace, Name: revision.Name, UID: revision.UID}, revision.ResourceVersion) {
				return invalid()
			}
			found = true
		}
		if !found {
			return invalid()
		}
	}
	return source, nil
}

func manualScalePolicyEvidence(parent *v1beta1.InferenceService, component v1beta1.ComponentType, status *v1beta1.ComponentAutoscalerStatus) bool {
	inline := isvcComponentExtension(parent, component)
	var reference *v1beta1.AutoscalerPolicyRef
	if inline != nil {
		reference = inline.AutoscalerPolicyRef
	}
	count := 0
	for _, condition := range status.Conditions {
		if condition.Type != v1beta1.AutoscalerResolvedCondition {
			continue
		}
		count++
		if count > 1 || condition.Status != metav1.ConditionTrue || condition.ObservedGeneration != parent.Generation ||
			condition.Reason != v1beta1.AutoscalerResolvedReasonInlinePrecedence || reference == nil || inline.Autoscaler == nil {
			return false
		}
	}
	if status.Policy != nil {
		return false
	}
	if reference == nil {
		return status.ShadowedPolicyRef == nil && count == 0
	}
	return inline.Autoscaler != nil && count == 1 && status.ShadowedPolicyRef != nil && status.ShadowedPolicyRef.Name == reference.Name
}

func manualScaleName(name string) bool {
	return name != "" && len(validation.IsDNS1123Subdomain(name)) == 0
}

func (s *ManualScaleSource) addStamp(object ctrlclient.Object, identity ManualScaleIdentity, rv string) bool {
	if !manualScaleName(identity.Name) || identity.UID == "" || rv == "" || len(identity.UID) > 256 || len(rv) > 256 ||
		(identity.Kind != "ControllerRevision" && identity.Generation <= 0) {
		return false
	}
	if strings.IndexFunc(identity.UID, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.')
	}) >= 0 {
		return false
	}
	clusterScoped := identity.Kind == "ClusterServingRuntime" || identity.Kind == "ClusterBaseModel"
	if clusterScoped && identity.Namespace != "" || !clusterScoped && (identity.Namespace == "" || len(validation.IsDNS1123Label(identity.Namespace)) != 0) {
		return false
	}
	for _, prior := range s.stamps {
		if prior.identity.Kind == identity.Kind && prior.identity.Namespace == identity.Namespace && prior.identity.Name == identity.Name {
			return false
		}
	}
	s.stamps = append(s.stamps, manualScaleStamp{object: object, identity: identity, resourceVersion: rv})
	if identity.Kind != "InferenceService" {
		s.summary.Sources = append(s.summary.Sources, identity)
	}
	return true
}

func (s ManualScaleSource) Summary() ManualScaleSummary {
	summary := s.summary
	summary.Sources = append([]ManualScaleIdentity(nil), summary.Sources...)
	return summary
}

func (s ManualScaleSource) MatchesParent(parent *v1beta1.InferenceService) bool {
	return s.parent != nil && parent != nil && s.parent.Name == parent.Name && s.parent.Namespace == parent.Namespace &&
		s.parent.UID == parent.UID && s.parent.ResourceVersion == parent.ResourceVersion && s.parent.Generation == parent.Generation &&
		reflect.DeepEqual(s.parent.Spec, parent.Spec) && reflect.DeepEqual(s.parent.Status, parent.Status)
}

func (s ManualScaleSource) ValidateReplica(replica *v1beta1.InferenceReplica) error {
	if s.parent == nil || s.autoscaler == nil || replica == nil || replica.Name != s.target || replica.Namespace != s.parent.Namespace ||
		replica.Spec.Component != s.summary.Component || !reflect.DeepEqual(replica.Spec.Autoscaler, s.autoscaler) {
		return ErrManualScaleSource
	}
	return nil
}

// Revalidate performs bounded exact reads of the proof's own source stamps.
// It does not reselect a runtime, regenerate a plan or acquire live policies.
func (s ManualScaleSource) Revalidate(ctx context.Context, reader ctrlclient.Reader) error {
	if s.parent == nil || reader == nil || len(s.stamps) < 2 {
		return ErrManualScaleSource
	}
	for _, stamp := range s.stamps {
		if err := ctx.Err(); err != nil {
			return err
		}
		object := stamp.object.DeepCopyObject().(ctrlclient.Object)
		requestContext, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := reader.Get(requestContext, ctrlclient.ObjectKey{Namespace: stamp.identity.Namespace, Name: stamp.identity.Name}, object)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, context.Canceled) {
				return context.Canceled
			}
			if errors.Is(err, context.DeadlineExceeded) {
				return context.DeadlineExceeded
			}
			return ErrManualScaleSourceChanged
		}
		gvk := object.GetObjectKind().GroupVersionKind()
		expected := schema.GroupVersionKind{Group: "ome.io", Version: "v1beta1", Kind: stamp.identity.Kind}
		if stamp.identity.Kind == "ControllerRevision" {
			expected = appsv1.SchemeGroupVersion.WithKind("ControllerRevision")
		}
		if (!gvk.Empty() && gvk != expected) || object.GetName() != stamp.identity.Name || object.GetNamespace() != stamp.identity.Namespace ||
			string(object.GetUID()) != stamp.identity.UID || object.GetResourceVersion() != stamp.resourceVersion || object.GetGeneration() != stamp.identity.Generation || object.GetDeletionTimestamp() != nil {
			return ErrManualScaleSourceChanged
		}
		if parent, ok := object.(*v1beta1.InferenceService); ok && !s.MatchesParent(parent) {
			return ErrManualScaleSourceChanged
		}
	}
	return ctx.Err()
}
