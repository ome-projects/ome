package canary

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/rollout"
)

// A prefill/decode unit pushed to version 2 stopped Failed on the ready
// timeout before any version 2 pod came up: every Instance of both members
// runs version 1, and both replicas still name version 2 as the revision
// they roll toward. The replica controller created version 2's routing and
// headless Service for each member ahead of the pods that would name them.
// The dispatch's sweep of unused per-revision Services must leave those
// four alone, or the two controllers create and delete them pass after
// pass; a revision no replica rolls toward and no pod runs is swept in the
// same pass.
func TestDispatch_KeepsTheRollTargetRevisionServicesWithNoPod(t *testing.T) {
	ns := "default"
	f := stoppedUnitWithoutNewPods(t, ns, "pd")
	if _, err := Dispatch(context.Background(), f.deps(t)); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	f.mustHave(t, v1beta1.EngineComponent, "new")
	f.mustHave(t, v1beta1.DecoderComponent, "decnew")
	f.mustNotHave(t, v1beta1.EngineComponent, "gone")
}

// The same unit rolled back by the operator: the rejected revision is on
// record and each replica is pointed at its version 1 ControllerRevision,
// so version 2 is no longer the revision either replica rolls toward. With
// no pod of it left, its Services go in the sweep.
func TestDispatch_SweepsTheRejectedRevisionServicesOnceNoReplicaRollsTowardIt(t *testing.T) {
	ns := "default"
	f := stoppedUnitWithoutNewPods(t, ns, "pd")
	cs := rollout.CanaryStatusFor(&f.isvc.Status, v1beta1.EngineComponent)
	cs.RolledBackRevisionHash = "new"
	cs.RollbackRequested = true
	cs.Failed = nil
	engine := f.isvc.Status.Components[v1beta1.EngineComponent]
	engine.RolloutPhase = v1beta1.RolloutPhaseRolledBack
	f.isvc.Status.Components[v1beta1.EngineComponent] = engine
	for comp, stable := range map[v1beta1.ComponentType]string{v1beta1.EngineComponent: "old", v1beta1.DecoderComponent: "decold"} {
		ir := &v1beta1.InferenceReplica{}
		key := types.NamespacedName{Namespace: ns, Name: "pd-" + string(comp)}
		if err := f.client.Get(context.Background(), key, ir); err != nil {
			t.Fatal(err)
		}
		name := "pd-" + string(comp) + "-" + stable
		ir.Spec.Pacing = &v1beta1.InferenceReplicaPacing{RollbackToRevision: &name}
		if err := f.client.Update(context.Background(), ir); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := Dispatch(context.Background(), f.deps(t)); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	f.mustNotHave(t, v1beta1.EngineComponent, "new")
	f.mustNotHave(t, v1beta1.DecoderComponent, "decnew")
	f.mustHave(t, v1beta1.EngineComponent, "old")
	f.mustHave(t, v1beta1.DecoderComponent, "decold")
}

// stoppedUnit is the fixture of a prefill/decode canary stopped Failed with
// no pod of the new revision.
type stoppedUnit struct {
	isvc   *v1beta1.InferenceService
	client client.Client
}

// stoppedUnitWithoutNewPods builds two engines and two decoders on version 1
// (engine old, decoder decold), both replicas rolling toward version 2
// (new, decnew) under a recorded two-step canary run that stopped Failed on the
// ready timeout at step 0. Version 2's per-revision Services exist for both
// members, as the replica controller leaves them, and the engine also has a
// pair for a revision gone from every replica and pod.
func stoppedUnitWithoutNewPods(t *testing.T, ns, name string) *stoppedUnit {
	t.Helper()
	n2 := 2
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, UID: types.UID(name + "-uid")}}
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n2}}
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n2}}
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("50%"), Traffic: 10, Pause: &v1beta1.RolloutPause{}},
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		}},
	}}}
	pinActiveRun(isvc)
	stopped := metav1.Now()
	rollout.SetCanaryStatusFor(&isvc.Status, v1beta1.EngineComponent, &v1beta1.CanaryStatus{
		CanaryRevisionHash: "new", StableRevisionHash: "old", CurrentStep: 0,
		CapacityWaitSince: &stopped,
		Failed:            &v1beta1.CanaryFailure{Reason: v1beta1.CanaryFailureCapacityTimeout, Time: &stopped},
	})
	engine := isvc.Status.Components[v1beta1.EngineComponent]
	engine.RolloutPhase = v1beta1.RolloutPhaseFailed
	isvc.Status.Components[v1beta1.EngineComponent] = engine

	engineIR := ir(ns, name, v1beta1.EngineComponent, "new")
	engineIR.Status.CurrentRevision = name + "-engine-old"
	decoderIR := ir(ns, name, v1beta1.DecoderComponent, "decnew")
	decoderIR.Status.CurrentRevision = name + "-decoder-decold"
	objs := []runtime.Object{isvc, engineIR, decoderIR,
		canaryPod(ns, name, "engine", "old", name+"-engine-0"),
		canaryPod(ns, name, "engine", "old", name+"-engine-1"),
		canaryPod(ns, name, "decoder", "decold", name+"-decoder-0"),
		canaryPod(ns, name, "decoder", "decold", name+"-decoder-1"),
		canaryControllerRevision(ns, name, "engine", "old", 1),
		canaryControllerRevision(ns, name, "engine", "new", 2),
		canaryControllerRevision(ns, name, "decoder", "decold", 1),
		canaryControllerRevision(ns, name, "decoder", "decnew", 2),
	}
	for _, pair := range []struct {
		comp v1beta1.ComponentType
		hash string
	}{
		{v1beta1.EngineComponent, "old"}, {v1beta1.EngineComponent, "new"}, {v1beta1.EngineComponent, "gone"},
		{v1beta1.DecoderComponent, "decold"}, {v1beta1.DecoderComponent, "decnew"},
	} {
		objs = append(objs, revisionServicePair(isvc, pair.comp, pair.hash)...)
	}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(objs...).Build()
	syncInstanceRows(t, c, isvc, v1beta1.EngineComponent, v1beta1.DecoderComponent)
	return &stoppedUnit{isvc: isvc, client: c}
}

func (f *stoppedUnit) deps(t *testing.T) DispatchDeps {
	t.Helper()
	return DispatchDeps{Client: f.client, Reader: f.client, ISVC: f.isvc,
		ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(f.isvc, rollout.Policies{})}
}

// revisionServicePair is a revision's routing and headless Service as the
// controller writes them: owned by the service, selecting the revision.
func revisionServicePair(isvc *v1beta1.InferenceService, comp v1beta1.ComponentType, hash string) []runtime.Object {
	names := coordination.PerRevisionServiceNames(isvc.Name, comp, hash)
	var out []runtime.Object
	for _, name := range []string{names.RoutingName, names.HeadlessName} {
		selector := map[string]string{
			constants.InferenceServicePodLabelKey: isvc.Name,
			constants.OMEComponentLabel:           string(comp),
			query.LabelRevisionHash:               hash,
			query.LabelManagedBy:                  query.ManagedByOMENative,
		}
		out = append(out, &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Namespace: isvc.Namespace, Name: name, Labels: selector,
				OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(isvc, v1beta1.SchemeGroupVersion.WithKind("InferenceService"))}},
			Spec: corev1.ServiceSpec{Selector: selector, Ports: []corev1.ServicePort{{Name: "http", Port: 8000}}},
		})
	}
	return out
}

func (f *stoppedUnit) mustHave(t *testing.T, comp v1beta1.ComponentType, hash string) {
	t.Helper()
	names := coordination.PerRevisionServiceNames(f.isvc.Name, comp, hash)
	for _, name := range []string{names.RoutingName, names.HeadlessName} {
		if err := f.client.Get(context.Background(), types.NamespacedName{Namespace: f.isvc.Namespace, Name: name}, &corev1.Service{}); err != nil {
			t.Errorf("%s revision %s: Service %s must survive the sweep while the replica rolls toward the revision: %v", comp, hash, name, err)
		}
	}
}

func (f *stoppedUnit) mustNotHave(t *testing.T, comp v1beta1.ComponentType, hash string) {
	t.Helper()
	names := coordination.PerRevisionServiceNames(f.isvc.Name, comp, hash)
	for _, name := range []string{names.RoutingName, names.HeadlessName} {
		err := f.client.Get(context.Background(), types.NamespacedName{Namespace: f.isvc.Namespace, Name: name}, &corev1.Service{})
		if !apierrors.IsNotFound(err) {
			t.Errorf("%s revision %s: Service %s must be swept once no pod runs the revision and no replica rolls toward it, Get returned %v", comp, hash, name, err)
		}
	}
}
