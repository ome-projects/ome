package inferencereplica

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/validation"
)

// isvcGVK identifies the InferenceService kind in owner references.
var isvcGVK = v1beta1.SchemeGroupVersion.WithKind("InferenceService")

// projectingParent returns the replica's InferenceService controller owner
// reference, or nil when it has none. It matches group and kind, as the
// replica controller does, so a reference at any served version counts.
func projectingParent(ir *v1beta1.InferenceReplica) *metav1.OwnerReference {
	ref := metav1.GetControllerOf(ir)
	if ref == nil || schema.FromAPIVersionAndKind(ref.APIVersion, ref.Kind).GroupKind() != isvcGVK.GroupKind() {
		return nil
	}
	return ref
}

// controllerRefInstalled reports whether newObj carries a controller owner
// reference oldObj lacks: one added where there was none, or a replacement
// that changes its apiVersion, kind, name or UID. Keeping or removing the
// controller reference, or changing only its blockOwnerDeletion, installs
// nothing.
func controllerRefInstalled(oldObj, newObj *v1beta1.InferenceReplica) bool {
	newCtl := metav1.GetControllerOf(newObj)
	return newCtl != nil && !sameOwner(metav1.GetControllerOf(oldObj), newCtl)
}

// sameOwner reports whether two references carry the same apiVersion, kind,
// name and UID; a version change counts as a new reference. BlockOwnerDeletion
// is ignored: the garbage collector clears it during foreground deletion
// without changing the owner.
func sameOwner(a, b *metav1.OwnerReference) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.APIVersion == b.APIVersion && a.Kind == b.Kind && a.Name == b.Name && a.UID == b.UID
}

// projectedForm reports whether the replica is projected by an
// InferenceService and names it: the controller owner reference when
// present, else spec.parentRef, which only the controller sets and which
// is immutable, so it marks a projected replica even after an orphan
// delete removed the owner reference.
func projectedForm(ir *v1beta1.InferenceReplica) (parent string, projected bool) {
	if ref := projectingParent(ir); ref != nil {
		return ref.Name, true
	}
	if name := ir.ParentName(); name != "" {
		return name, true
	}
	return "", false
}

// controllerOnlyFields are the spec fields only the InferenceService
// controller may write on any replica.
type controllerOnlyFields struct {
	ParentRef             *v1beta1.ParentReference
	PlacementExecution    *v1beta1.PlacementExecutionPolicy
	PlacementReplicaLimit *int32
}

func controllerOnly(s v1beta1.InferenceReplicaSpec) controllerOnlyFields {
	return controllerOnlyFields{
		ParentRef:             s.ParentRef,
		PlacementExecution:    s.PlacementExecution,
		PlacementReplicaLimit: s.PlacementReplicaLimit,
	}
}

// placementFieldsSet reports whether the placement fields, which only the
// controller writes, are present.
func placementFieldsSet(s v1beta1.InferenceReplicaSpec) bool {
	return s.PlacementExecution != nil || s.PlacementReplicaLimit != nil
}

func controllerOnlyChanged(oldSpec, newSpec v1beta1.InferenceReplicaSpec) bool {
	return !equality.Semantic.DeepEqual(controllerOnly(oldSpec), controllerOnly(newSpec))
}

func specChanged(oldSpec, newSpec v1beta1.InferenceReplicaSpec) bool {
	return !equality.Semantic.DeepEqual(oldSpec, newSpec)
}

// rolloutControlFieldList names, for denial messages, the spec fields an
// InferenceService writes on a replica it references without projecting it;
// withoutComposerFields zeroes exactly these and the controller-only fields.
var rolloutControlFieldList = strings.Join(constants.InferenceReplicaComposedFieldNames(), ", ")

// withoutComposerFields returns s with every field the InferenceService
// controller may write on a replica it does not project zeroed: the
// rollout-control fields and the controller-only fields. A pacing block
// left empty collapses to nil, so writing the first knob into a replica
// without one is not a user-field change. pacing.maxUnavailable stays: it
// is a user field.
func withoutComposerFields(s v1beta1.InferenceReplicaSpec) v1beta1.InferenceReplicaSpec {
	out := *s.DeepCopy()
	out.Paused, out.PauseMode, out.PairingProtocol = false, "", nil
	if out.Pacing != nil {
		out.Pacing.Partition, out.Pacing.RollbackToRevision = nil, nil
		if equality.Semantic.DeepEqual(*out.Pacing, v1beta1.InferenceReplicaPacing{}) {
			out.Pacing = nil
		}
	}
	out.ParentRef, out.PlacementExecution, out.PlacementReplicaLimit = nil, nil, nil
	return out
}

// userFieldsChanged reports whether a spec change touches a field the
// InferenceService controller may not write on a replica it does not
// project.
func userFieldsChanged(oldSpec, newSpec v1beta1.InferenceReplicaSpec) bool {
	return !equality.Semantic.DeepEqual(withoutComposerFields(oldSpec), withoutComposerFields(newSpec))
}

// composedFieldsChanged reports whether newObj adds, changes or removes the
// composed-fields annotation relative to oldObj, which is nil on create.
func composedFieldsChanged(oldObj, newObj *v1beta1.InferenceReplica) bool {
	var was string
	var had bool
	if oldObj != nil {
		was, had = oldObj.Annotations[constants.InferenceReplicaComposedFieldsAnnotationKey]
	}
	now, has := newObj.Annotations[constants.InferenceReplicaComposedFieldsAnnotationKey]
	return had != has || was != now
}

// standaloneSpecError rejects a standalone spec that renders from more or
// fewer than one template source or pins its runtime, and settings a
// standalone replica accepts in shape but nothing acts on: only the
// InferenceService controller creates HPA or KEDA scalers, and a replica reads
// spec.minReadySeconds, never the nested lifecycle field.
func standaloneSpecError(s v1beta1.InferenceReplicaSpec) error {
	if err := validation.ValidateInferenceReplicaTemplateSource(len(s.Runners), s.ModelRef != nil, s.RuntimeRef != nil, pinnedRuntime(s.RuntimeRef)); err != nil {
		return err
	}
	if s.Autoscaler != nil && (s.Autoscaler.Class == v1beta1.AutoscalerHPA || s.Autoscaler.Class == v1beta1.AutoscalerKEDA) {
		return fmt.Errorf("spec.autoscaler.class %q creates no scaler on a standalone InferenceReplica; target the scale subresource with your own HorizontalPodAutoscaler or ScaledObject, or use class %q",
			s.Autoscaler.Class, v1beta1.AutoscalerExternal)
	}
	if s.Lifecycle != nil && s.Lifecycle.MinReadySeconds != nil {
		return fmt.Errorf("spec.lifecycle.minReadySeconds is not read on an InferenceReplica; set spec.minReadySeconds")
	}
	return nil
}

// pinnedRuntime reports whether ref asks for a runtime pin: autoSync off or a
// named revision. A replica renders the live runtime and keeps no
// ControllerRevision of it, so a pin is rejected rather than ignored.
func pinnedRuntime(ref *v1beta1.ServingRuntimeRef) bool {
	if ref == nil {
		return false
	}
	return (ref.AutoSync != nil && !*ref.AutoSync) || (ref.Revision != nil && *ref.Revision != "")
}

// runtimeRefChanged reports whether newSpec names a runtime oldSpec did not,
// or a different one.
func runtimeRefChanged(oldSpec, newSpec v1beta1.InferenceReplicaSpec) bool {
	return newSpec.RuntimeRef != nil && !equality.Semantic.DeepEqual(oldSpec.RuntimeRef, newSpec.RuntimeRef)
}

// migrationRequested reports whether newObj carries a migration-request
// annotation that oldObj (nil on create) did not. A manual migration surges
// through the per-revision Service an InferenceService creates, so a
// standalone replica cannot complete one.
func migrationRequested(oldObj, newObj *v1beta1.InferenceReplica) bool {
	for key := range newObj.Annotations {
		if !strings.HasPrefix(key, constants.MigrationRequestAnnotationPrefix) {
			continue
		}
		if oldObj == nil {
			return true
		}
		if _, had := oldObj.Annotations[key]; !had {
			return true
		}
	}
	return false
}

// collidingInferenceServiceNames lists the InferenceService names whose
// projected replicas would share pod, Service and revision names with a
// standalone replica called name: the name itself, and the name with a
// component suffix removed.
func collidingInferenceServiceNames(name string) []string {
	out := []string{name}
	for _, c := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent} {
		if trimmed := strings.TrimSuffix(name, "-"+string(c)); trimmed != name && trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
