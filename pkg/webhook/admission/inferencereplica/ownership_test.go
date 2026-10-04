package inferencereplica

import (
	"context"
	"net/http"
	"strings"
	"testing"

	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/runtimeselector"
)

const (
	controllerUser       = "system:serviceaccount:ome:ome-controller-manager"
	plainUser            = "example-user"
	garbageCollectorUser = "system:serviceaccount:kube-system:generic-garbage-collector"

	// ownerRefDenial is the denial for a controller owner reference only the
	// InferenceService controller may set.
	ownerRefDenial = "only the InferenceService controller may set an InferenceService controller owner reference or change a projected replica's"
	// composerDenial is the denial for a controller write to a user field of
	// a replica it does not project; it lists the fields the controller may
	// write.
	composerDenial = "the InferenceService controller writes only rollout-control fields (pacing.partition, paused, pauseMode, pacing.rollbackToRevision, pairingProtocol) on a replica it does not project"
	// composedFieldsDenial is the denial for a non-controller write to the
	// composed-fields annotation.
	composedFieldsDenial = "the ome.io/composed-fields annotation is written by the InferenceService controller"
)

// inferenceReplicaConfigMap is the inferenceservice-config ConfigMap holding
// only the given inferenceReplica block.
func inferenceReplicaConfigMap(block string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: constants.InferenceServiceConfigMapName, Namespace: constants.OMENamespace},
		Data:       map[string]string{controllerconfig.InferenceReplicaConfigName: block},
	}
}

// identityValidator returns a Validator whose configured controller identity
// is controllerUser and whose Reader holds objs.
func identityValidator(t *testing.T, objs ...client.Object) *Validator {
	t.Helper()
	cm := inferenceReplicaConfigMap(`{"controllerIdentity":{"usernames":["` + controllerUser + `"]}}`)
	scheme := runtime.NewScheme()
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	return &Validator{
		Decoder:   newDecoder(t),
		Clientset: k8sfake.NewSimpleClientset(cm),
		Reader:    ctrlfake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build(),
	}
}

func asUser(req admission.Request, username string) admission.Request {
	req.UserInfo = authenticationv1.UserInfo{Username: username, Groups: []string{"system:authenticated"}}
	return req
}

// projectedIR is a replica an InferenceService named "llama" projects:
// controller owner reference plus parentRef, as the projector stamps them.
func projectedIR() *v1beta1.InferenceReplica {
	ir := baselineIR(nil)
	ir.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: "InferenceService",
		Name: "llama", UID: "isvc-uid", Controller: ptr.To(true),
	}}
	return ir
}

// standaloneIR is a user-created replica: no owner reference, no parentRef.
func standaloneIR() *v1beta1.InferenceReplica {
	ir := baselineIR(nil)
	ir.Name = "pool-a"
	ir.OwnerReferences = nil
	ir.Spec.ParentRef = nil
	return ir
}

func withPlacementLimit(ir *v1beta1.InferenceReplica) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	out.Spec.PlacementReplicaLimit = ptr.To(int32(2))
	return out
}

func withAnnotation(ir *v1beta1.InferenceReplica, k, v string) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	if out.Annotations == nil {
		out.Annotations = map[string]string{}
	}
	out.Annotations[k] = v
	return out
}

func withoutAnnotation(ir *v1beta1.InferenceReplica, k string) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	delete(out.Annotations, k)
	return out
}

// controllerRef is a controller owner reference to the named object.
func controllerRef(apiVersion, kind, name string) metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion: apiVersion, Kind: kind, Name: name,
		UID: types.UID(kind + "-" + name), Controller: ptr.To(true),
	}
}

// withOwners returns a copy of ir whose owner references are exactly refs.
func withOwners(ir *v1beta1.InferenceReplica, refs ...metav1.OwnerReference) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	out.OwnerReferences = refs
	return out
}

// withBlockOwnerDeletion returns a copy of ir whose owner references carry
// the given blockOwnerDeletion, the field the garbage collector clears
// during foreground deletion.
func withBlockOwnerDeletion(ir *v1beta1.InferenceReplica, block bool) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	for i := range out.OwnerReferences {
		out.OwnerReferences[i].BlockOwnerDeletion = ptr.To(block)
	}
	return out
}

func withAutoscalerClass(ir *v1beta1.InferenceReplica, class v1beta1.AutoscalerClass) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	out.Spec.Autoscaler = &v1beta1.ComponentAutoscaler{Class: class}
	return out
}

func withLifecycleMinReady(ir *v1beta1.InferenceReplica, seconds int32) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	out.Spec.Lifecycle = &v1beta1.LifecycleSpec{MinReadySeconds: &seconds}
	return out
}

func withPaused(ir *v1beta1.InferenceReplica, mode v1beta1.PauseMode) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	out.Spec.Paused = true
	out.Spec.PauseMode = mode
	return out
}

func withRollbackToRevision(ir *v1beta1.InferenceReplica, revision string) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	if out.Spec.Pacing == nil {
		out.Spec.Pacing = &v1beta1.InferenceReplicaPacing{}
	}
	out.Spec.Pacing.RollbackToRevision = &revision
	return out
}

func withPacingMaxUnavailable(ir *v1beta1.InferenceReplica, n int32) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	if out.Spec.Pacing == nil {
		out.Spec.Pacing = &v1beta1.InferenceReplicaPacing{}
	}
	out.Spec.Pacing.MaxUnavailable = ptr.To(intstr.FromInt32(n))
	return out
}

func withPairingProtocol(ir *v1beta1.InferenceReplica, token string) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	out.Spec.PairingProtocol = &token
	return out
}

// withComposedFields returns a copy of ir carrying the composed-fields
// annotation with the given value.
func withComposedFields(ir *v1beta1.InferenceReplica, value string) *v1beta1.InferenceReplica {
	return withAnnotation(ir, constants.InferenceReplicaComposedFieldsAnnotationKey, value)
}

// refsIR is a standalone replica of component c that renders from the
// ClusterServingRuntime runtime-a instead of stored runners.
func refsIR(c v1beta1.ComponentType) *v1beta1.InferenceReplica {
	ir := standaloneIR()
	ir.Spec.Component = c
	ir.Spec.Runners = nil
	ir.Spec.RuntimeRef = &v1beta1.ServingRuntimeRef{Name: "runtime-a", Kind: ptr.To(runtimeselector.KindClusterServingRuntime)}
	return ir
}

func withModelRef(ir *v1beta1.InferenceReplica, name string) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	out.Spec.ModelRef = &v1beta1.ModelRef{Name: name}
	return out
}

func withRuntimeRef(ir *v1beta1.InferenceReplica, ref *v1beta1.ServingRuntimeRef) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	out.Spec.RuntimeRef = ref
	return out
}

func withoutRunners(ir *v1beta1.InferenceReplica) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	out.Spec.Runners = nil
	return out
}

// clusterRuntime is a ClusterServingRuntime declaring the roles of spec.
func clusterRuntime(name string, spec v1beta1.ServingRuntimeSpec, annotations map[string]string) *v1beta1.ClusterServingRuntime {
	return &v1beta1.ClusterServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: annotations}, Spec: spec}
}

func TestOwnershipByIdentity(t *testing.T) {
	collidingISVC := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "pool-a", Namespace: "prod-models"}}
	withParentRef := standaloneIR()
	withParentRef.Spec.ParentRef = &v1beta1.ParentReference{Name: "llama"}
	adopted := standaloneIR()
	adopted.OwnerReferences = projectedIR().OwnerReferences
	// orphan is a projected replica after an orphan delete removed its
	// owner reference; spec.parentRef still names llama.
	orphan := projectedIR()
	orphan.OwnerReferences = nil
	orphanWithHPA := withAutoscalerClass(orphan, v1beta1.AutoscalerHPA)
	parentless := projectedIR()
	parentless.Spec.ParentRef = nil
	stamped := withAnnotation(projectedIR(), constants.InferenceReplicaControllerWriteAnnotationKey, constants.InferenceReplicaControllerWriteAnnotationVal)
	deploymentRef := controllerRef("apps/v1", "Deployment", "llama-pool")
	otherVersion := schema.GroupVersion{Group: v1beta1.SchemeGroupVersion.Group, Version: "v1"}.String()
	otherGroupRef := controllerRef("serving.kserve.io/v1beta1", "InferenceService", "pool-a")
	otherVersionRef := controllerRef(otherVersion, "InferenceService", "llama")
	// editedController returns a projected replica whose InferenceService
	// controller reference has been changed by edit.
	editedController := func(edit func(ref *metav1.OwnerReference)) *v1beta1.InferenceReplica {
		ref := projectedIR().OwnerReferences[0]
		edit(&ref)
		return withOwners(projectedIR(), ref)
	}
	blocked := withBlockOwnerDeletion(projectedIR(), true)
	migrationKey := constants.MigrationRequestAnnotationPrefix + "0f3a"
	engineOnly := v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{}}
	engineRuntime := clusterRuntime("runtime-a", engineOnly, nil)
	namespacedEngineRuntime := &v1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "runtime-a", Namespace: "prod-models"}, Spec: engineOnly}
	decoderParent := clusterRuntime("runtime-base", v1beta1.ServingRuntimeSpec{DecoderConfig: &v1beta1.DecoderSpec{}}, nil)
	inheritingChild := clusterRuntime("runtime-a", engineOnly, map[string]string{constants.RuntimeInheritFromAnnotationKey: "runtime-base"})
	noDecoderPiece := `ClusterServingRuntime "runtime-a" has no decoder piece; a decoder replica cannot render from it`
	decoderFromRunners := standaloneIR()
	decoderFromRunners.Spec.Component = v1beta1.DecoderComponent

	cases := []struct {
		name     string
		objs     []client.Object
		req      func(t *testing.T) admission.Request
		allowed  bool
		contains string
	}{
		{"controller creates a projected replica", nil,
			func(t *testing.T) admission.Request { return asUser(createReq(t, projectedIR()), controllerUser) }, true, ""},
		{"a user cannot create a projected replica", nil,
			func(t *testing.T) admission.Request { return asUser(createReq(t, projectedIR()), plainUser) }, false, "projected by InferenceService"},
		{"a user creates a standalone replica", nil,
			func(t *testing.T) admission.Request { return asUser(createReq(t, standaloneIR()), plainUser) }, true, ""},
		{"a standalone create may not carry parentRef", nil,
			func(t *testing.T) admission.Request { return asUser(createReq(t, withParentRef), plainUser) }, false, "omit it on a standalone"},
		{"a user cannot set placement fields on create", nil,
			func(t *testing.T) admission.Request {
				return asUser(createReq(t, withPlacementLimit(standaloneIR())), plainUser)
			}, false, "written only by the InferenceService controller"},
		{"the controller may set placement fields on a standalone replica", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, standaloneIR(), withPlacementLimit(standaloneIR())), controllerUser)
			}, true, ""},
		{"the controller cannot create a standalone replica", nil,
			func(t *testing.T) admission.Request { return asUser(createReq(t, standaloneIR()), controllerUser) },
			false, "creates only the replicas it projects"},
		{"the controller cannot create a standalone replica with placement fields", nil,
			func(t *testing.T) admission.Request {
				return asUser(createReq(t, withPlacementLimit(standaloneIR())), controllerUser)
			}, false, "creates only the replicas it projects"},
		{"a standalone replica may not take an InferenceService name", []client.Object{collidingISVC},
			func(t *testing.T) admission.Request { return asUser(createReq(t, standaloneIR()), plainUser) }, false, "would share pod and Service names"},
		{"a user cannot change a projected replica's spec", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, projectedIR(), withReplicas(projectedIR(), 3)), plainUser)
			}, false, "edit the InferenceService instead (request from "},
		{"a user may annotate a projected replica", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, projectedIR(), withAnnotation(projectedIR(), "ome.io/reset-instances", "all")), plainUser)
			}, true, ""},
		{"the controller changes a projected replica's runners", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, projectedIR(), withImage(projectedIR(), "sgl:1.1")), controllerUser)
			}, true, ""},
		{"a user changes a standalone replica's runners", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, standaloneIR(), withImage(standaloneIR(), "sgl:1.1")), plainUser)
			}, true, ""},
		{"a user cannot set placement fields on update", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, standaloneIR(), withPlacementLimit(standaloneIR())), plainUser)
			}, false, "written only by the InferenceService controller"},
		{"a user cannot attach a standalone replica to an InferenceService", nil,
			func(t *testing.T) admission.Request { return asUser(updateReq(t, standaloneIR(), adopted), plainUser) }, false, ownerRefDenial},
		{"the controller may attach a standalone replica to an InferenceService", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, standaloneIR(), adopted), controllerUser)
			}, true, ""},
		{"a user cannot replace the InferenceService controller reference with a Deployment controller", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, projectedIR(), withOwners(projectedIR(), deploymentRef)), plainUser)
			}, false, ownerRefDenial},
		{"the controller may replace a projected replica's controller reference", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, projectedIR(), withOwners(projectedIR(), deploymentRef)), controllerUser)
			}, true, ""},
		{"a user cannot add a foreign controller reference to an orphaned replica", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, orphan, withOwners(orphan, deploymentRef)), plainUser)
			}, false, ownerRefDenial},
		{"a user cannot rename the controller reference", nil,
			func(t *testing.T) admission.Request {
				renamed := editedController(func(ref *metav1.OwnerReference) { ref.Name = "other-isvc" })
				return asUser(updateReq(t, projectedIR(), renamed), plainUser)
			}, false, ownerRefDenial},
		{"a user cannot change the controller reference's UID", nil,
			func(t *testing.T) admission.Request {
				reUIDed := editedController(func(ref *metav1.OwnerReference) { ref.UID = "other-isvc-uid" })
				return asUser(updateReq(t, projectedIR(), reUIDed), plainUser)
			}, false, ownerRefDenial},
		{"a user cannot change the controller reference's apiVersion", nil,
			func(t *testing.T) admission.Request {
				reVersioned := editedController(func(ref *metav1.OwnerReference) { ref.APIVersion = otherVersion })
				return asUser(updateReq(t, projectedIR(), reVersioned), plainUser)
			}, false, ownerRefDenial},
		{"garbage collection may clear blockOwnerDeletion on the controller reference", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, blocked, withBlockOwnerDeletion(blocked, false)), garbageCollectorUser)
			}, true, "metadata-only update"},
		{"a user may set their own controller reference on a standalone replica", nil,
			func(t *testing.T) admission.Request {
				owned := withOwners(standaloneIR(), controllerRef("apps.example.com/v1", "ReplicaPool", "pool-a"))
				return asUser(updateReq(t, standaloneIR(), owned), plainUser)
			}, true, ""},
		{"a same-kind controller reference from another group leaves a create standalone", nil,
			func(t *testing.T) admission.Request {
				return asUser(createReq(t, withOwners(standaloneIR(), otherGroupRef)), plainUser)
			}, true, ""},
		{"a same-kind controller reference from another group leaves an update standalone", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, standaloneIR(), withOwners(standaloneIR(), otherGroupRef)), plainUser)
			}, true, ""},
		{"an InferenceService controller reference at another version of the group marks a create projected", nil,
			func(t *testing.T) admission.Request {
				return asUser(createReq(t, withOwners(standaloneIR(), otherVersionRef)), plainUser)
			}, false, "projected by InferenceService"},
		{"a user cannot attach a standalone replica through another version of the group", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, standaloneIR(), withOwners(standaloneIR(), otherVersionRef)), plainUser)
			}, false, ownerRefDenial},
		{"a user cannot add spec.parentRef on update", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, standaloneIR(), withParentRef), plainUser)
			}, false, "spec.parentRef is immutable"},
		{"the controller cannot add spec.parentRef on update", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, standaloneIR(), withParentRef), controllerUser)
			}, false, "spec.parentRef is immutable"},
		{"a user cannot remove spec.parentRef on update", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, projectedIR(), parentless), plainUser)
			}, false, "spec.parentRef is immutable"},
		{"the controller cannot remove spec.parentRef on update", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, projectedIR(), parentless), controllerUser)
			}, false, "spec.parentRef is immutable"},
		{"a user cannot remove the owner reference and change the spec in one request", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, projectedIR(), withReplicas(orphan, 3)), plainUser)
			}, false, "projected by InferenceService"},
		{"the controller-write annotation does not make a user the controller on create", nil,
			func(t *testing.T) admission.Request { return asUser(createReq(t, stamped), plainUser) }, false, "projected by InferenceService"},
		{"the controller-write annotation does not make a user the controller on update", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, stamped, withReplicas(stamped, 3)), plainUser)
			}, false, "projected by InferenceService"},
		{"garbage collection may remove the InferenceService owner reference", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, projectedIR(), orphan), garbageCollectorUser)
			}, true, ""},
		{"an orphaned projected replica keeps rejecting user spec writes", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, orphan, withReplicas(orphan, 3)), plainUser)
			}, false, "projected by InferenceService"},
		{"an orphaned projected replica accepts controller writes despite its projected settings", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, orphanWithHPA, withImage(orphanWithHPA, "sgl:1.1")), controllerUser)
			}, true, ""},
		{"a user cannot attach an orphaned replica to an InferenceService", nil,
			func(t *testing.T) admission.Request { return asUser(updateReq(t, orphan, projectedIR()), plainUser) }, false, ownerRefDenial},
		{"the controller re-stamps the owner reference on an orphaned replica", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, orphan, projectedIR()), controllerUser)
			}, true, ""},
		{"a standalone replica may not ask for an hpa scaler", nil,
			func(t *testing.T) admission.Request {
				return asUser(createReq(t, withAutoscalerClass(standaloneIR(), v1beta1.AutoscalerHPA)), plainUser)
			}, false, "creates no scaler"},
		{"a standalone replica may not set lifecycle.minReadySeconds", nil,
			func(t *testing.T) admission.Request {
				return asUser(createReq(t, withLifecycleMinReady(standaloneIR(), 5)), plainUser)
			}, false, "set spec.minReadySeconds"},
		{"a projected replica keeps its hpa scaler", nil,
			func(t *testing.T) admission.Request {
				return asUser(createReq(t, withAutoscalerClass(projectedIR(), v1beta1.AutoscalerHPA)), controllerUser)
			}, true, ""},
		{"a standalone replica cannot request a migration", nil,
			func(t *testing.T) admission.Request {
				return asUser(createReq(t, withAnnotation(standaloneIR(), migrationKey, `{"instance":0}`)), plainUser)
			}, false, "manual migration is not available"},
		{"a standalone replica cannot gain a migration request on update", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, standaloneIR(), withAnnotation(standaloneIR(), migrationKey, `{"instance":0}`)), plainUser)
			}, false, "manual migration is not available"},
		{"a user may request a migration on a projected replica", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, projectedIR(), withAnnotation(projectedIR(), migrationKey, `{"instance":0}`)), plainUser)
			}, true, ""},
		{"a standalone replica may not take a projected replica's name", []client.Object{&v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod-models"}}},
			func(t *testing.T) admission.Request {
				ir := standaloneIR()
				ir.Name = "svc-engine"
				return asUser(createReq(t, ir), plainUser)
			}, false, "would share pod and Service names"},
		{"a standalone replica's name must be a valid Service-name prefix", nil,
			func(t *testing.T) admission.Request {
				ir := standaloneIR()
				ir.Name = "pool.a"
				return asUser(createReq(t, ir), plainUser)
			}, false, "prefix of its pod and Service names and must match"},
		// A standalone replica that names a runtime is checked against the
		// runtime the controller would resolve.
		{"a standalone decoder replica may not name a runtime without a decoder piece", []client.Object{engineRuntime},
			func(t *testing.T) admission.Request {
				return asUser(createReq(t, refsIR(v1beta1.DecoderComponent)), plainUser)
			}, false, noDecoderPiece},
		{"a standalone engine replica renders from a runtime with an engine piece", []client.Object{engineRuntime},
			func(t *testing.T) admission.Request {
				return asUser(createReq(t, refsIR(v1beta1.EngineComponent)), plainUser)
			}, true, ""},
		{"a standalone replica may name a runtime that is not found", nil,
			func(t *testing.T) admission.Request {
				return asUser(createReq(t, refsIR(v1beta1.DecoderComponent)), plainUser)
			}, true, ""},
		{"a reference to the ServingRuntime kind checks the namespaced runtime", []client.Object{namespacedEngineRuntime},
			func(t *testing.T) admission.Request {
				ref := &v1beta1.ServingRuntimeRef{Name: "runtime-a", Kind: ptr.To(runtimeselector.KindServingRuntime)}
				return asUser(createReq(t, withRuntimeRef(refsIR(v1beta1.DecoderComponent), ref)), plainUser)
			}, false, `ServingRuntime "runtime-a" has no decoder piece`},
		{"a reference to the ClusterServingRuntime kind falls back to the namespaced runtime as the controller does", []client.Object{namespacedEngineRuntime},
			func(t *testing.T) admission.Request {
				return asUser(createReq(t, refsIR(v1beta1.DecoderComponent)), plainUser)
			}, false, `ServingRuntime "runtime-a" has no decoder piece`},
		{"a runtime inheriting the piece from its parent is admitted", []client.Object{decoderParent, inheritingChild},
			func(t *testing.T) admission.Request {
				return asUser(createReq(t, refsIR(v1beta1.DecoderComponent)), plainUser)
			}, true, ""},
		{"a runtime whose inherit-from parent is missing is rejected", []client.Object{inheritingChild},
			func(t *testing.T) admission.Request {
				return asUser(createReq(t, refsIR(v1beta1.EngineComponent)), plainUser)
			}, false, `runtime "runtime-a" inherits from "runtime-base", which does not exist`},
		{"a standalone replica may not pin its runtime", []client.Object{engineRuntime},
			func(t *testing.T) admission.Request {
				pinned := refsIR(v1beta1.EngineComponent)
				pinned.Spec.RuntimeRef.AutoSync = ptr.To(false)
				return asUser(createReq(t, pinned), plainUser)
			}, false, "not honored on an InferenceReplica"},
		{"a standalone replica may not keep its runners when it gains a model reference", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, standaloneIR(), withModelRef(standaloneIR(), "model-a")), plainUser)
			}, false, "exclusive"},
		{"a standalone replica may not drop its last template source", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, standaloneIR(), withoutRunners(standaloneIR())), plainUser)
			}, false, "spec.runners is required unless spec.modelRef or spec.runtimeRef is set"},
		{"a standalone replica may move from runners to a runtime that is not found", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, standaloneIR(), refsIR(v1beta1.EngineComponent)), plainUser)
			}, true, ""},
		{"a standalone replica may not move to a runtime without its piece", []client.Object{engineRuntime},
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, decoderFromRunners, refsIR(v1beta1.DecoderComponent)), plainUser)
			}, false, noDecoderPiece},
		{"the controller may set placement fields on a refs replica", []client.Object{engineRuntime},
			func(t *testing.T) admission.Request {
				refs := refsIR(v1beta1.EngineComponent)
				return asUser(updateReq(t, refs, withPlacementLimit(refs)), controllerUser)
			}, true, ""},
		// On a replica it does not project the controller writes only the
		// rollout-control fields and the composed-fields annotation.
		{"the controller may set the partition on a standalone replica", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, standaloneIR(), withPacingPartition(standaloneIR(), 1)), controllerUser)
			}, true, ""},
		{"the controller may pause a standalone replica", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, standaloneIR(), withPaused(standaloneIR(), v1beta1.PauseModeFreeze)), controllerUser)
			}, true, ""},
		{"the controller may set the rollback target and pairing protocol on a standalone replica", nil,
			func(t *testing.T) admission.Request {
				steered := withPairingProtocol(withRollbackToRevision(standaloneIR(), "pool-a-0f3a"), "v2")
				return asUser(updateReq(t, standaloneIR(), steered), controllerUser)
			}, true, ""},
		{"the controller may clear the fields it held on a standalone replica", nil,
			func(t *testing.T) admission.Request {
				held := withComposedFields(withPacingPartition(withPaused(standaloneIR(), v1beta1.PauseModeRecover), 1), "svc:paused,pacing.partition")
				return asUser(updateReq(t, held, standaloneIR()), controllerUser)
			}, true, ""},
		{"the controller cannot scale a standalone replica", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, standaloneIR(), withReplicas(standaloneIR(), 3)), controllerUser)
			}, false, composerDenial},
		{"the controller cannot change a standalone replica's runners", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, standaloneIR(), withImage(standaloneIR(), "sgl:1.1")), controllerUser)
			}, false, composerDenial},
		{"the controller cannot set pacing.maxUnavailable on a standalone replica", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, standaloneIR(), withPacingMaxUnavailable(standaloneIR(), 1)), controllerUser)
			}, false, composerDenial},
		{"the controller cannot drop a user's pacing.maxUnavailable while setting the partition", nil,
			func(t *testing.T) admission.Request {
				budgeted := withPacingMaxUnavailable(standaloneIR(), 1)
				return asUser(updateReq(t, budgeted, withPacingPartition(standaloneIR(), 1)), controllerUser)
			}, false, composerDenial},
		{"the controller cannot scale a standalone replica while pausing it", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, standaloneIR(), withReplicas(withPaused(standaloneIR(), v1beta1.PauseModeFreeze), 3)), controllerUser)
			}, false, composerDenial},
		{"a user may set the partition on a standalone replica", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, standaloneIR(), withPacingPartition(standaloneIR(), 1)), plainUser)
			}, true, ""},
		{"a user cannot pause a projected replica", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, projectedIR(), withPaused(projectedIR(), v1beta1.PauseModeFreeze)), plainUser)
			}, false, "projected by InferenceService"},
		// Only the controller adds, changes or removes the composed-fields
		// annotation; any other annotation edit stays a metadata-only update.
		{"a user cannot add the composed-fields annotation to a standalone replica", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, standaloneIR(), withComposedFields(standaloneIR(), "svc:paused")), plainUser)
			}, false, composedFieldsDenial},
		{"a user cannot change the composed-fields annotation", nil,
			func(t *testing.T) admission.Request {
				held := withComposedFields(standaloneIR(), "svc:paused")
				return asUser(updateReq(t, held, withComposedFields(held, "svc:pacing.partition")), plainUser)
			}, false, composedFieldsDenial},
		{"a user cannot remove the composed-fields annotation", nil,
			func(t *testing.T) admission.Request {
				held := withComposedFields(standaloneIR(), "svc:paused")
				return asUser(updateReq(t, held, withoutAnnotation(held, constants.InferenceReplicaComposedFieldsAnnotationKey)), plainUser)
			}, false, composedFieldsDenial},
		{"a user cannot create a standalone replica carrying the composed-fields annotation", nil,
			func(t *testing.T) admission.Request {
				return asUser(createReq(t, withComposedFields(standaloneIR(), "svc:paused")), plainUser)
			}, false, composedFieldsDenial},
		{"a user cannot add the composed-fields annotation to a projected replica", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, projectedIR(), withComposedFields(projectedIR(), "svc:paused")), plainUser)
			}, false, composedFieldsDenial},
		{"a user may change another annotation beside the composed-fields annotation", nil,
			func(t *testing.T) admission.Request {
				held := withComposedFields(standaloneIR(), "svc:paused")
				return asUser(updateReq(t, held, withAnnotation(held, "example.com/note", "keep")), plainUser)
			}, true, "metadata-only update"},
		{"the controller may add the composed-fields annotation with no spec change", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, standaloneIR(), withComposedFields(standaloneIR(), "svc:paused")), controllerUser)
			}, true, ""},
		{"the controller may remove the composed-fields annotation with no spec change", nil,
			func(t *testing.T) admission.Request {
				held := withComposedFields(standaloneIR(), "svc:paused")
				return asUser(updateReq(t, held, withoutAnnotation(held, constants.InferenceReplicaComposedFieldsAnnotationKey)), controllerUser)
			}, true, ""},
		{"the controller may write the composed-fields annotation on a projected replica", nil,
			func(t *testing.T) admission.Request {
				return asUser(updateReq(t, projectedIR(), withComposedFields(projectedIR(), "svc:paused")), controllerUser)
			}, true, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := identityValidator(t, tc.objs...).Handle(context.Background(), tc.req(t))
			if resp.Allowed != tc.allowed {
				t.Fatalf("allowed = %v, want %v (message %q)", resp.Allowed, tc.allowed, resp.Result.Message)
			}
			if tc.contains != "" && !strings.Contains(resp.Result.Message, tc.contains) {
				t.Fatalf("message %q does not contain %q", resp.Result.Message, tc.contains)
			}
		})
	}
}

// TestTemplateSourceReadsTheRuntimeOnlyWhenNamed pins that a replica with
// modelRef alone, and an update that keeps its runtime reference, read no
// runtime.
func TestTemplateSourceReadsTheRuntimeOnlyWhenNamed(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	v := identityValidator(t)
	v.Reader = ctrlfake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			switch obj.(type) {
			case *v1beta1.ServingRuntime, *v1beta1.ClusterServingRuntime:
				t.Errorf("runtime %s was read", key.Name)
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}).Build()

	modelOnly := withModelRef(withoutRunners(standaloneIR()), "model-a")
	if resp := v.Handle(context.Background(), asUser(createReq(t, modelOnly), plainUser)); !resp.Allowed {
		t.Fatalf("modelRef alone denied: %s", resp.Result.Message)
	}
	refs := refsIR(v1beta1.EngineComponent)
	if resp := v.Handle(context.Background(), asUser(updateReq(t, refs, withReplicas(refs, 3)), plainUser)); !resp.Allowed {
		t.Fatalf("scale of a refs replica denied: %s", resp.Result.Message)
	}
	// The controller's rollout-control writes on a replica it does not
	// project read no runtime either.
	if resp := v.Handle(context.Background(), asUser(updateReq(t, refs, withPacingPartition(refs, 1)), controllerUser)); !resp.Allowed {
		t.Fatalf("partition of a refs replica by the controller denied: %s", resp.Result.Message)
	}
}

func TestOwnershipWithoutIdentityKeepsTheAnnotationConvention(t *testing.T) {
	v := &Validator{Decoder: newDecoder(t)}
	stamped := projectedIR()
	stamped.Annotations = withControllerWrite()

	if resp := v.Handle(context.Background(), createReq(t, projectedIR())); resp.Allowed || !strings.Contains(resp.Result.Message, constants.InferenceReplicaControllerWriteAnnotationKey) {
		t.Fatalf("projected create without the annotation: allowed=%v message=%q", resp.Allowed, resp.Result.Message)
	}
	if resp := v.Handle(context.Background(), createReq(t, stamped)); !resp.Allowed {
		t.Fatalf("projected create with the annotation denied: %q", resp.Result.Message)
	}
	if resp := v.Handle(context.Background(), createReq(t, standaloneIR())); !resp.Allowed {
		t.Fatalf("standalone create without the annotation denied: %q", resp.Result.Message)
	}
	if resp := v.Handle(context.Background(), updateReq(t, stamped, withReplicas(stamped, 3))); !resp.Allowed {
		t.Fatalf("annotated projected update denied: %q", resp.Result.Message)
	}
}

// Without a readable controller identity (an invalid inferenceReplica block
// or no inferenceservice-config ConfigMap) every decision that needs the
// identity fails with a server error, but no metadata-only update does:
// refusing a finalizer change, a label edit, a mailbox annotation removal or
// the garbage collector's owner reference patches would strand the replica
// behind this fail-closed webhook.
func TestOwnershipConfigErrorSparesMetadataOnlyUpdates(t *testing.T) {
	old := withAnnotation(withFinalizers(projectedIR(), "ome.io/ir-teardown"), constants.ResetInstancesAnnotationKey, constants.ResetInstancesAll)
	old = withBlockOwnerDeletion(old, true)
	type request struct {
		name string
		req  admission.Request
	}
	held := withComposedFields(standaloneIR(), "svc:paused")
	spared := []request{
		{"finalizer-only update", asUser(updateReq(t, old, withFinalizers(old)), plainUser)},
		{"label-only update", asUser(updateReq(t, old, withLabel(old, "team", "team-a")), plainUser)},
		{"mailbox annotation removal", asUser(updateReq(t, old, withoutAnnotation(old, constants.ResetInstancesAnnotationKey)), plainUser)},
		{"blockOwnerDeletion cleared", asUser(updateReq(t, old, withBlockOwnerDeletion(old, false)), garbageCollectorUser)},
		{"owner reference removal", asUser(updateReq(t, old, withOwners(old)), garbageCollectorUser)},
		{"label edit beside an unchanged composed-fields annotation", asUser(updateReq(t, held, withLabel(held, "team", "team-a")), plainUser)},
	}
	failing := []request{
		{"create", asUser(createReq(t, standaloneIR()), plainUser)},
		{"spec change", asUser(updateReq(t, old, withReplicas(old, 3)), plainUser)},
		{"composed-fields annotation change", asUser(updateReq(t, standaloneIR(), held), plainUser)},
	}
	configs := []struct {
		name      string
		clientset *k8sfake.Clientset
	}{
		{"invalid block", k8sfake.NewSimpleClientset(inferenceReplicaConfigMap(`{"controllerIdentity":{"user":"x"}}`))},
		{"missing ConfigMap", k8sfake.NewSimpleClientset()},
	}
	for _, config := range configs {
		v := &Validator{Decoder: newDecoder(t), Clientset: config.clientset}
		for _, tc := range spared {
			t.Run(config.name+"/"+tc.name, func(t *testing.T) {
				if resp := v.Handle(context.Background(), tc.req); !resp.Allowed || resp.Result.Message != "metadata-only update" {
					t.Fatalf("allowed=%v message=%q, want a metadata-only update", resp.Allowed, resp.Result.Message)
				}
			})
		}
		for _, tc := range failing {
			t.Run(config.name+"/"+tc.name, func(t *testing.T) {
				resp := v.Handle(context.Background(), tc.req)
				if resp.Allowed || resp.Result.Code != http.StatusInternalServerError {
					t.Fatalf("allowed=%v code=%d message=%q, want a server error", resp.Allowed, resp.Result.Code, resp.Result.Message)
				}
			})
		}
	}
}

// A group-only identity recognizes the controller by the caller's groups,
// whatever its username.
func TestOwnershipByGroupIdentity(t *testing.T) {
	v := &Validator{
		Decoder:   newDecoder(t),
		Clientset: k8sfake.NewSimpleClientset(inferenceReplicaConfigMap(`{"controllerIdentity":{"groups":["ome-controllers"]}}`)),
	}
	inGroup := createReq(t, projectedIR())
	inGroup.UserInfo = authenticationv1.UserInfo{Username: plainUser, Groups: []string{"system:authenticated", "ome-controllers"}}
	if resp := v.Handle(context.Background(), inGroup); !resp.Allowed {
		t.Fatalf("a caller in the controller group was denied: %q", resp.Result.Message)
	}
	outside := asUser(createReq(t, projectedIR()), plainUser)
	if resp := v.Handle(context.Background(), outside); resp.Allowed || !strings.Contains(resp.Result.Message, "projected by InferenceService") {
		t.Fatalf("a caller outside the controller group: allowed=%v message=%q", resp.Allowed, resp.Result.Message)
	}
}
