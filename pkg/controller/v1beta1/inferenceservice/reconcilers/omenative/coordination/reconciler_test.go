package coordination

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

func TestReconcile_NilISVCErrors(t *testing.T) {
	_, err := Reconcile(context.Background(), ReconcileInputs{Client: testClient()})
	if err == nil {
		t.Errorf("nil ISVC: want error")
	}
}

func TestReconcile_NilClientErrors(t *testing.T) {
	_, err := Reconcile(context.Background(), ReconcileInputs{ISVC: testReconcilerISVC()})
	if err == nil {
		t.Errorf("nil Client: want error")
	}
}

func TestReconcile_NoOpForNonOMENativeISVC(t *testing.T) {
	isvc := testReconcilerISVC()
	// No engine/decoder OMENative annotation.
	r, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC:   isvc,
		Client: testClient(),
		Reader: testClient(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.PerRevisionServicesEnsured != 0 {
		t.Errorf("should not emit Services for non-OMENative ISVC: got %d", r.PerRevisionServicesEnsured)
	}
}

func TestReconcile_UsesResolvedOMENativeMembership(t *testing.T) {
	isvc := testReconcilerISVC()
	isvc.Spec.Engine = &v1beta1.EngineSpec{}
	c := testClient(buildPod(isvc, v1beta1.EngineComponent, "hash1", 0))

	r, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC:   isvc,
		Client: c,
		Reader: c,
		ComponentDeploymentModes: map[v1beta1.ComponentType]constants.DeploymentModeType{
			v1beta1.EngineComponent: constants.OMENative,
		},
		ComponentRunnerPorts: testComponentRunnerPorts(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.PerRevisionServicesEnsured != 1 {
		t.Fatalf("resolved OMENative Engine should ensure one revision Service pair, got %d", r.PerRevisionServicesEnsured)
	}
	for _, name := range []string{
		PerRevisionServiceName(isvc.Name, v1beta1.EngineComponent, "hash1"),
		PerRevisionHeadlessServiceName(isvc.Name, v1beta1.EngineComponent, "hash1"),
	} {
		if err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: name}, &corev1.Service{}); err != nil {
			t.Errorf("resolved OMENative Engine Service %s missing: %v", name, err)
		}
	}
}

func TestReconcile_EmitsPerRevisionServiceForOMENativeComponent(t *testing.T) {
	isvc := testOMENativeISVC()
	isvc.Spec.Rollout = singleEngineGroup()
	pods := []runtime.Object{
		buildPod(isvc, v1beta1.EngineComponent, "hash1", 0),
		buildPod(isvc, v1beta1.EngineComponent, "hash1", 1),
	}
	c := testClient(pods...)
	r, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC:                     isvc,
		Client:                   c,
		Reader:                   c,
		Now:                      time.Now(),
		ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent),
		ComponentRunnerPorts:     testComponentRunnerPorts(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.PerRevisionServicesEnsured == 0 {
		t.Errorf("should ensure per-revision Services: got %+v", r)
	}
	// Verify the Service exists.
	svc := &corev1.Service{}
	key := client.ObjectKey{Namespace: isvc.Namespace, Name: PerRevisionServiceName(isvc.Name, v1beta1.EngineComponent, "hash1")}
	if err := c.Get(context.Background(), key, svc); err != nil {
		t.Errorf("per-revision Service missing: %v", err)
	}
	// The published port comes from the Component's effective serving
	// template, not from a builder-side default.
	if len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Port != 8000 {
		t.Errorf("per-revision Service ports: got %+v want a single port 8000", svc.Spec.Ports)
	}
}

func TestReconcile_UsesObservedRunnerShapeForRevisionServices(t *testing.T) {
	isvc := testOMENativeISVC()
	isvc.Spec.Rollout = singleEngineGroup()
	stablePod := buildRunnerPod(isvc, v1beta1.EngineComponent, "stable", 0, v1beta1.RunnerNameDefault, "0")
	multiLeader := buildRunnerPod(isvc, v1beta1.EngineComponent, "multi", 1, v1beta1.RunnerNameLeader, "0")
	multiWorker := buildRunnerPod(isvc, v1beta1.EngineComponent, "multi", 2, v1beta1.RunnerNameWorker, "0")
	c := testClient(stablePod, multiLeader, multiWorker)

	if _, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC:                     isvc,
		Client:                   c,
		Reader:                   c,
		Now:                      time.Now(),
		ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent),
		ComponentRunnerPorts:     testComponentRunnerPorts(),
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, tc := range []struct {
		hash     string
		multiPod bool
	}{
		{hash: "stable", multiPod: false},
		{hash: "multi", multiPod: true},
	} {
		routing := &corev1.Service{}
		routingKey := client.ObjectKey{
			Namespace: isvc.Namespace,
			Name:      PerRevisionServiceName(isvc.Name, v1beta1.EngineComponent, tc.hash),
		}
		if err := c.Get(context.Background(), routingKey, routing); err != nil {
			t.Fatalf("routing Service for %s missing: %v", tc.hash, err)
		}
		if tc.multiPod {
			if got := routing.Spec.Selector[query.LabelRunner]; got != string(v1beta1.RunnerNameLeader) {
				t.Errorf("multi-pod routing Service selector %s: got %q want %q", query.LabelRunner, got, v1beta1.RunnerNameLeader)
			}
			if got := routing.Spec.Selector[query.LabelPodOrdinal]; got != "0" {
				t.Errorf("multi-pod routing Service selector %s: got %q want 0", query.LabelPodOrdinal, got)
			}
			if !routingSelectorMatches(routing.Spec.Selector, multiLeader.Labels) {
				t.Errorf("multi-pod leader must match routing selector: selector=%v labels=%v", routing.Spec.Selector, multiLeader.Labels)
			}
			if routingSelectorMatches(routing.Spec.Selector, multiWorker.Labels) {
				t.Errorf("multi-pod worker must not match routing selector: selector=%v labels=%v", routing.Spec.Selector, multiWorker.Labels)
			}
		} else {
			if _, ok := routing.Spec.Selector[query.LabelRunner]; ok {
				t.Errorf("single-pod routing Service selector must not include %s: %v", query.LabelRunner, routing.Spec.Selector)
			}
			if _, ok := routing.Spec.Selector[query.LabelPodOrdinal]; ok {
				t.Errorf("single-pod routing Service selector must not include %s: %v", query.LabelPodOrdinal, routing.Spec.Selector)
			}
			if !routingSelectorMatches(routing.Spec.Selector, stablePod.Labels) {
				t.Errorf("single-pod revision must match broad routing selector: selector=%v labels=%v", routing.Spec.Selector, stablePod.Labels)
			}
		}

		headless := &corev1.Service{}
		headlessKey := client.ObjectKey{
			Namespace: isvc.Namespace,
			Name:      PerRevisionHeadlessServiceName(isvc.Name, v1beta1.EngineComponent, tc.hash),
		}
		if err := c.Get(context.Background(), headlessKey, headless); err != nil {
			t.Fatalf("headless Service for %s missing: %v", tc.hash, err)
		}
		if _, ok := headless.Spec.Selector[query.LabelRunner]; ok {
			t.Errorf("headless Service for %s selector must not include %s: %v", tc.hash, query.LabelRunner, headless.Spec.Selector)
		}
		if _, ok := headless.Spec.Selector[query.LabelPodOrdinal]; ok {
			t.Errorf("headless Service for %s selector must not include %s: %v", tc.hash, query.LabelPodOrdinal, headless.Spec.Selector)
		}
	}
}

// A Component whose serving template declares no port gets no routing Service:
// the reconcile still succeeds and the rest of the coordination pass runs.
func TestReconcile_SkipsRoutingServiceWithoutRunnerPort(t *testing.T) {
	isvc := testOMENativeISVC()
	isvc.Spec.Rollout = singleEngineGroup()
	c := testClient(buildPod(isvc, v1beta1.EngineComponent, "hash1", 0))
	if _, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(),
		ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent),
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	svc := &corev1.Service{}
	key := client.ObjectKey{Namespace: isvc.Namespace, Name: PerRevisionServiceName(isvc.Name, v1beta1.EngineComponent, "hash1")}
	if err := c.Get(context.Background(), key, svc); !apierrors.IsNotFound(err) {
		t.Errorf("routing Service must not be created without a declared port: err=%v svc=%+v", err, svc)
	}
	headless := &corev1.Service{}
	hkey := client.ObjectKey{Namespace: isvc.Namespace, Name: PerRevisionHeadlessServiceName(isvc.Name, v1beta1.EngineComponent, "hash1")}
	if err := c.Get(context.Background(), hkey, headless); err != nil {
		t.Errorf("headless Service should still be ensured: %v", err)
	}
}

func TestReconcile_DataPlaneRunsWithoutCoordinationGroup(t *testing.T) {
	// Updated contract: per-revision Services + Traffic[] are the
	// data plane for HTTPRoute weighted-backendRef consumers.
	// They emit for every OMENative-mode Component, with or without a
	// rolloutCoordination group declared. Only the per-group state
	// machine (Status.RolloutCoordination, RolloutCoordinationReady
	// condition, group events) stays opt-in via the rolloutCoordination
	// block.
	isvc := testOMENativeISVC()
	pods := []runtime.Object{
		buildPod(isvc, v1beta1.EngineComponent, "hash1", 0),
	}
	c := testClient(pods...)
	r, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(),
		ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.PerRevisionServicesEnsured == 0 {
		t.Errorf("no groups: per-revision Services should still emit for the data plane, got %d", r.PerRevisionServicesEnsured)
	}
	if isvc.Status.RolloutCoordination != nil {
		t.Errorf("no groups: should not write RolloutCoordination status, got %+v", isvc.Status.RolloutCoordination)
	}
	cs, ok := isvc.Status.Components[v1beta1.EngineComponent]
	if !ok || len(cs.Traffic) == 0 {
		t.Errorf("no groups: Traffic[] should still populate from observed pods, got %+v", cs)
	}
}

// steadyEngineStatus seeds the engine Component as fully settled onto a
// single revision with a converged 100% Traffic[0] target.
func steadyEngineStatus(isvc *v1beta1.InferenceService, hash string) {
	rev := isvc.Name + "-engine-" + hash
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{
				CurrentRevision: rev,
				UpdateRevision:  rev,
			},
			Traffic: []v1beta1.ComponentTrafficTarget{{
				RevisionName:   PerRevisionServiceName(isvc.Name, v1beta1.EngineComponent, hash),
				Percent:        100,
				LatestRevision: true,
			}},
		},
	}
}

// TestReconcile_GCsOrphanWhenRevisionSetLooksConverged verifies that GC runs
// when live and recorded revision sets agree. Traffic uses ready pods while GC
// uses total pods, so set equality does not prove no orphan is collectible.
func TestReconcile_GCsOrphanWhenRevisionSetLooksConverged(t *testing.T) {
	isvc := testOMENativeISVC()
	// Post-drain steady state: Traffic records only hash1, and only hash1 has
	// pods — the two sets agree.
	steadyEngineStatus(isvc, "hash1")

	orphanRouting := PerRevisionServiceName(isvc.Name, v1beta1.EngineComponent, "hash0")
	orphanHeadless := PerRevisionHeadlessServiceName(isvc.Name, v1beta1.EngineComponent, "hash0")
	objs := []runtime.Object{
		buildPod(isvc, v1beta1.EngineComponent, "hash1", 0),
		// Left behind by the previous revision; no pod carries hash0.
		perRevisionServiceFixture(isvc, v1beta1.EngineComponent, "hash0", orphanRouting),
		perRevisionServiceFixture(isvc, v1beta1.EngineComponent, "hash0", orphanHeadless),
	}
	c := &listCountingClient{Client: testClient(objs...)}
	r, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(),
		ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.PerRevisionServicesEnsured != 1 {
		t.Errorf("ensure must still run for the live hash, got %d want 1", r.PerRevisionServicesEnsured)
	}
	if c.serviceLists == 0 {
		t.Error("orphan sweep must run: got 0 Service LIST calls")
	}
	for _, name := range []string{orphanRouting, orphanHeadless} {
		err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: name}, &corev1.Service{})
		if !apierrors.IsNotFound(err) {
			t.Errorf("orphan Service %s must be deleted, Get returned %v", name, err)
		}
	}
}

// perRevisionServiceFixture builds a per-revision Service carrying the labels
// gcOrphanedPerRevisionServices selects on, so the sweep can find it.
func perRevisionServiceFixture(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, revisionHash, name string) runtime.Object {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: isvc.Namespace,
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: isvc.Name,
				constants.OMEComponentLabel:           string(component),
				query.LabelManagedBy:                  query.ManagedByOMENative,
				query.LabelRevisionHash:               revisionHash,
			},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				constants.InferenceServicePodLabelKey: isvc.Name,
				constants.OMEComponentLabel:           string(component),
				query.LabelManagedBy:                  query.ManagedByOMENative,
				query.LabelRevisionHash:               revisionHash,
			},
		},
	}
}

// TestReconcile_RecreatesDeletedServiceWhenConverged pins Service
// self-heal: a per-revision Service deleted out-of-band must be recreated
// on the next reconcile even when the live revision-hash set is unchanged
// (converged). Gating ensure on convergence would leave the deleted
// Service gone and the HTTPRoute routing to a dead backend.
func TestReconcile_RecreatesDeletedServiceWhenConverged(t *testing.T) {
	isvc := testOMENativeISVC()
	steadyEngineStatus(isvc, "hash1")
	pods := []runtime.Object{
		buildPod(isvc, v1beta1.EngineComponent, "hash1", 0),
	}
	c := testClient(pods...)
	ctx := context.Background()

	// First reconcile creates the per-revision Services for hash1.
	if _, err := Reconcile(ctx, ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(),
		ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent),
		ComponentRunnerPorts:     testComponentRunnerPorts(),
	}); err != nil {
		t.Fatalf("first reconcile: unexpected error: %v", err)
	}
	routingKey := client.ObjectKey{Namespace: isvc.Namespace, Name: PerRevisionServiceName(isvc.Name, v1beta1.EngineComponent, "hash1")}
	if err := c.Get(ctx, routingKey, &corev1.Service{}); err != nil {
		t.Fatalf("setup: routing Service should exist after first reconcile: %v", err)
	}

	// Delete the routing Service out-of-band (e.g. operator fat-fingered a
	// kubectl delete) while the revision set stays converged.
	if err := c.Delete(ctx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: routingKey.Namespace, Name: routingKey.Name}}); err != nil {
		t.Fatalf("delete out-of-band: %v", err)
	}
	if err := c.Get(ctx, routingKey, &corev1.Service{}); err == nil {
		t.Fatalf("setup: routing Service should be gone after delete")
	}

	// Second reconcile: revision set is unchanged (converged), but the
	// deleted Service must be recreated.
	if _, err := Reconcile(ctx, ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(),
		ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent),
		ComponentRunnerPorts:     testComponentRunnerPorts(),
	}); err != nil {
		t.Fatalf("second reconcile: unexpected error: %v", err)
	}
	if err := c.Get(ctx, routingKey, &corev1.Service{}); err != nil {
		t.Errorf("converged revision set: deleted per-revision Service must be recreated, got: %v", err)
	}
}

// TestReconcile_EnsuresServicesWhenRevisionSetDiverges pins the other
// branch of the change-detector: when the live pod-hash set differs from
// the recorded Traffic set (here no Traffic is recorded yet but a live pod
// exists), the full ensure path runs and creates the per-revision Service.
func TestReconcile_EnsuresServicesWhenRevisionSetDiverges(t *testing.T) {
	isvc := testOMENativeISVC()
	// No recorded Traffic: recorded set is empty, live set is {hash1}; they
	// diverge, so the ensure path must run and create the Service.
	pods := []runtime.Object{
		buildPod(isvc, v1beta1.EngineComponent, "hash1", 0),
	}
	c := testClient(pods...)
	r, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(), ComponentRunnerPorts: testComponentRunnerPorts(),
		ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.PerRevisionServicesEnsured == 0 {
		t.Errorf("diverged revision set: per-revision Services must be ensured, got %d", r.PerRevisionServicesEnsured)
	}
	svc := &corev1.Service{}
	key := client.ObjectKey{Namespace: isvc.Namespace, Name: PerRevisionServiceName(isvc.Name, v1beta1.EngineComponent, "hash1")}
	if err := c.Get(context.Background(), key, svc); err != nil {
		t.Errorf("diverged revision set: per-revision Service should be created, got: %v", err)
	}
}

// TestReconcile_ObservesPodsViaCachedClient confirms the coordination
// layer reads per-revision pods through the cached Client, not the live
// Reader: a nil Reader must not be dereferenced. The full observe / ensure
// path runs entirely off Client.
func TestReconcile_ObservesPodsViaCachedClient(t *testing.T) {
	isvc := testOMENativeISVC()
	pods := []runtime.Object{
		buildPod(isvc, v1beta1.EngineComponent, "hash1", 0),
	}
	c := testClient(pods...)
	// Reader deliberately nil: if coordination still read pods via Reader
	// this would panic. The cached Client must carry the observation.
	r, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: nil, Now: time.Now(),
		ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.PerRevisionServicesEnsured == 0 {
		t.Errorf("cached-read path: pods observed via Client must drive Service ensure, got %d", r.PerRevisionServicesEnsured)
	}
	cs, ok := isvc.Status.Components[v1beta1.EngineComponent]
	if !ok || len(cs.Traffic) == 0 {
		t.Errorf("cached-read path: Traffic[] must populate from pods read via Client, got %+v", cs)
	}
}

func TestReconcile_WritesTrafficStatus(t *testing.T) {
	isvc := testOMENativeISVC()
	isvc.Spec.Rollout = singleEngineGroup()
	pods := []runtime.Object{
		buildPod(isvc, v1beta1.EngineComponent, "hash1", 0),
		buildPod(isvc, v1beta1.EngineComponent, "hash1", 1),
	}
	c := testClient(pods...)
	if _, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(),
		ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent),
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cs, ok := isvc.Status.Components[v1beta1.EngineComponent]
	if !ok {
		t.Fatalf("Components[engine] not populated")
	}
	if len(cs.Traffic) != 1 {
		t.Fatalf("Traffic: got %d entries want 1", len(cs.Traffic))
	}
	if cs.Traffic[0].RevisionName != "llama-engine-rev-hash1" {
		t.Errorf("Traffic[0].RevisionName: got %q want llama-engine-rev-hash1", cs.Traffic[0].RevisionName)
	}
	if cs.Traffic[0].Percent != 100 {
		t.Errorf("Traffic[0].Percent: got %d want 100", cs.Traffic[0].Percent)
	}
}

// TestReconcile_TrafficLatestRevisionIdenticalShapeAcrossComponents pins
// the per-Component contract:
// for an ISVC running engine + decoder + router (all OMENative, each on
// a single ControllerRevision), every Component's traffic[0] must carry
// the same canonical shape — LatestRevision=true, Percent=100, and the
// per-revision Service name as RevisionName. The repro showed
// engine.traffic[0] missing the LatestRevision flag while
// decoder.traffic[0] + router.traffic[0] had it set correctly — caused
// by the no-op short-circuit in TrafficDiffersMeaningfully treating the
// flag as cosmetic, so an early reconcile that wrote LatestRevision=false
// (UpdateRevision not yet observed from the IR Status) stranded the
// stale value even after the rollout converged.
func TestReconcile_DoesNotClobberCanaryTraffic(t *testing.T) {
	isvc := testOMENativeISVC()
	// Canary owns the engine; canary.Dispatch is the authoritative traffic
	// producer for it (it sets the explicit step weight, decoupled from pod
	// count). coordination.Reconcile runs unconditionally in v2 — it must NOT
	// recompute the engine's traffic from pod proportions and clobber that weight.
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
					{Capacity: intstr.FromString("50%"), Traffic: 10},
					{Capacity: intstr.FromString("100%"), Traffic: 100},
				}},
			},
		},
	}
	// Canary set an explicit 10/90 split (the step weight), independent of pods.
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {Traffic: []v1beta1.ComponentTrafficTarget{
			{RevisionName: "llama-engine-rev-canaryhash", Percent: 10},
			{RevisionName: "llama-engine-rev-stablehash", Percent: 90},
		}},
	}
	// Pods are 50/50 across the two revisions, so coordination's pod-proportional
	// recompute (50/50) differs from the canary's 10/90 and would clobber it.
	pods := []runtime.Object{
		buildPod(isvc, v1beta1.EngineComponent, "canaryhash", 0),
		buildPod(isvc, v1beta1.EngineComponent, "stablehash", 1),
	}
	c := testClient(pods...)
	if _, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(),
		ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent),
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := isvc.Status.Components[v1beta1.EngineComponent].Traffic
	for _, target := range got {
		var want int32
		switch target.RevisionName {
		case "llama-engine-rev-canaryhash":
			want = 10
		case "llama-engine-rev-stablehash":
			want = 90
		default:
			t.Fatalf("coordination clobbered canary traffic with revision %q", target.RevisionName)
		}
		if target.Percent != want {
			t.Errorf("%s: got %d%% want %d%% — coordination clobbered the canary step weight", target.RevisionName, target.Percent, want)
		}
	}
}

func TestReconcile_TrafficLatestRevisionIdenticalShapeAcrossComponents(t *testing.T) {
	isvc := testOMENativeISVC()
	// Add decoder + router as OMENative-mode Components.
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{
				constants.DeploymentMode: string(constants.OMENative),
			},
		},
	}
	isvc.Spec.Router = &v1beta1.RouterSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{
				constants.DeploymentMode: string(constants.OMENative),
			},
		},
	}
	// Each Component IR identifies the revision that its traffic status
	// treats as latest.
	engineIR := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: isvc.Name + "-engine", Namespace: isvc.Namespace},
		Status: v1beta1.InferenceReplicaStatus{
			UpdateRevision: isvc.Name + "-engine-engineHash",
		},
	}
	decoderIR := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: isvc.Name + "-decoder", Namespace: isvc.Namespace},
		Status: v1beta1.InferenceReplicaStatus{
			UpdateRevision: isvc.Name + "-decoder-decoderHash",
		},
	}
	routerIR := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: isvc.Name + "-router", Namespace: isvc.Namespace},
		Status: v1beta1.InferenceReplicaStatus{
			UpdateRevision: isvc.Name + "-router-routerHash",
		},
	}
	pods := []runtime.Object{
		buildPod(isvc, v1beta1.EngineComponent, "engineHash", 0),
		buildPod(isvc, v1beta1.DecoderComponent, "decoderHash", 0),
		buildPod(isvc, v1beta1.RouterComponent, "routerHash", 0),
	}
	c := testClient(append(pods, engineIR, decoderIR, routerIR)...)
	if _, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(),
		ComponentDeploymentModes: testOMENativeModes(
			v1beta1.EngineComponent,
			v1beta1.DecoderComponent,
			v1beta1.RouterComponent,
		),
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, comp := range []v1beta1.ComponentType{
		v1beta1.EngineComponent,
		v1beta1.DecoderComponent,
		v1beta1.RouterComponent,
	} {
		cs, ok := isvc.Status.Components[comp]
		if !ok {
			t.Errorf("%s: status entry missing", comp)
			continue
		}
		if len(cs.Traffic) != 1 {
			t.Errorf("%s: Traffic len=%d want 1: %+v", comp, len(cs.Traffic), cs.Traffic)
			continue
		}
		if cs.Traffic[0].Percent != 100 {
			t.Errorf("%s: Percent=%d want 100", comp, cs.Traffic[0].Percent)
		}
		if !cs.Traffic[0].LatestRevision {
			t.Errorf("%s: LatestRevision=false want true (regression)", comp)
		}
	}
}

// TestReconcile_TrafficLatestRevisionFlipPersists is the focused
// reconcile-level check of the no-op short-circuit. On the first
// observation the UpdateRevision is empty (matches the first-reconcile
// race where AggregateIRStatus has not yet mirrored IR.Status). On the
// second observation UpdateRevision is populated. The writer MUST flip
// Traffic[0].LatestRevision from false to true on the second pass —
// TrafficDiffersMeaningfully must not treat the change as cosmetic and
// skip the write.
func TestReconcile_TrafficLatestRevisionFlipPersists(t *testing.T) {
	isvc := testOMENativeISVC()
	pod := buildPod(isvc, v1beta1.EngineComponent, "engineHash", 0)
	engineIR := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: isvc.Name + "-engine", Namespace: isvc.Namespace},
		Status:     v1beta1.InferenceReplicaStatus{
			// First pass: UpdateRevision empty (IR not yet reconciled).
		},
	}
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(pod, engineIR).WithStatusSubresource(&v1beta1.InferenceReplica{}).Build()

	// First reconcile: UpdateRevision empty → LatestRevision=false.
	if _, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(),
		ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent),
	}); err != nil {
		t.Fatalf("first reconcile: unexpected error: %v", err)
	}
	cs1, ok := isvc.Status.Components[v1beta1.EngineComponent]
	if !ok || len(cs1.Traffic) != 1 {
		t.Fatalf("first pass: Traffic missing or wrong len: %+v", cs1.Traffic)
	}
	if cs1.Traffic[0].LatestRevision {
		t.Fatalf("first pass: LatestRevision should be false when UpdateRevision is empty: %+v", cs1.Traffic[0])
	}

	// Mirror the IR controller committing UpdateRevision to IR status.
	ir := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: isvc.Namespace, Name: isvc.Name + "-engine",
	}, ir); err != nil {
		t.Fatalf("get IR: %v", err)
	}
	ir.Status.UpdateRevision = isvc.Name + "-engine-engineHash"
	if err := c.Status().Update(context.Background(), ir); err != nil {
		t.Fatalf("update IR status: %v", err)
	}

	// Second reconcile: same pod set, but UpdateRevision is now
	// observed — LatestRevision must flip to true on the next write.
	if _, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(),
		ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent),
	}); err != nil {
		t.Fatalf("second reconcile: unexpected error: %v", err)
	}
	cs2, ok := isvc.Status.Components[v1beta1.EngineComponent]
	if !ok || len(cs2.Traffic) != 1 {
		t.Fatalf("second pass: Traffic missing or wrong len: %+v", cs2.Traffic)
	}
	if !cs2.Traffic[0].LatestRevision {
		t.Errorf("second pass: LatestRevision did not flip to true (regression: engine traffic[].latestRevision stuck stale)")
	}
}

// TestReconcile_PreviousRolledoutRecordedOnAdvance pins the rolled-out
// identifier contract across two completed rollouts: when the traffic
// converges to 100% on a NEW revision, LatestRolledoutRevision advances
// to the new per-revision Service name and the prior value is demoted
// to PreviousRolledoutRevision. A repeat observation of the SAME
// converged revision must not shift Previous.
func TestReconcile_PreviousRolledoutRecordedOnAdvance(t *testing.T) {
	isvc := testOMENativeISVC()

	reconcileWith := func(hash string) {
		t.Helper()
		engineIR := &v1beta1.InferenceReplica{
			ObjectMeta: metav1.ObjectMeta{Name: isvc.Name + "-engine", Namespace: isvc.Namespace},
			Status: v1beta1.InferenceReplicaStatus{
				UpdateRevision: isvc.Name + "-engine-" + hash,
			},
		}
		c := testClient(buildPod(isvc, v1beta1.EngineComponent, hash, 0), engineIR)
		if _, err := Reconcile(context.Background(), ReconcileInputs{
			ISVC: isvc, Client: c, Reader: c, Now: time.Now(),
			ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent),
		}); err != nil {
			t.Fatalf("reconcile with hash %s: unexpected error: %v", hash, err)
		}
	}

	// First rollout completes on hashA.
	reconcileWith("hasha")
	cs := isvc.Status.Components[v1beta1.EngineComponent]
	if got, want := cs.LatestRolledoutRevision, "llama-engine-rev-hasha"; got != want {
		t.Fatalf("after first rollout: LatestRolledoutRevision=%q want %q", got, want)
	}
	if cs.PreviousRolledoutRevision != "" {
		t.Fatalf("after first rollout: PreviousRolledoutRevision=%q want empty (no prior rollout)", cs.PreviousRolledoutRevision)
	}

	// Second rollout completes on hashB: Latest advances, Previous
	// records the immediately-prior rolled-out revision.
	reconcileWith("hashb")
	cs = isvc.Status.Components[v1beta1.EngineComponent]
	if got, want := cs.LatestRolledoutRevision, "llama-engine-rev-hashb"; got != want {
		t.Fatalf("after second rollout: LatestRolledoutRevision=%q want %q", got, want)
	}
	if got, want := cs.PreviousRolledoutRevision, "llama-engine-rev-hasha"; got != want {
		t.Fatalf("after second rollout: PreviousRolledoutRevision=%q want %q", got, want)
	}

	// Re-observing the same converged revision must not shift Previous
	// onto the current Latest.
	reconcileWith("hashb")
	cs = isvc.Status.Components[v1beta1.EngineComponent]
	if got, want := cs.PreviousRolledoutRevision, "llama-engine-rev-hasha"; got != want {
		t.Fatalf("after repeat observation: PreviousRolledoutRevision=%q want %q (must not self-demote)", got, want)
	}
	if got, want := cs.LatestRolledoutRevision, "llama-engine-rev-hashb"; got != want {
		t.Fatalf("after repeat observation: LatestRolledoutRevision=%q want %q", got, want)
	}
}

func TestUpdateTrafficStatusRepairsRevisionMetadataWhenTrafficIsCurrent(t *testing.T) {
	isvc := testOMENativeISVC()
	traffic := BuildTrafficTargets(isvc.Name, v1beta1.EngineComponent, []RevisionWeight{
		{RevisionHash: "current", Percent: 100, LatestRevision: true},
	})
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			Traffic:                   traffic,
			LatestReadyRevision:       "llama-engine-rev-old-ready",
			LatestRolledoutRevision:   "llama-engine-rev-old",
			PreviousRolledoutRevision: "llama-engine-rev-older",
		},
	}
	engineIR := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: isvc.Name + "-engine", Namespace: isvc.Namespace},
		Status: v1beta1.InferenceReplicaStatus{
			UpdateRevision: isvc.Name + "-engine-current",
		},
	}
	c := testClient(engineIR)

	err := updateTrafficStatus(
		context.Background(),
		c,
		isvc,
		[]v1beta1.ComponentType{v1beta1.EngineComponent},
		map[v1beta1.ComponentType]map[string]int32{
			v1beta1.EngineComponent: {"current": 1},
		},
		1,
	)
	if err != nil {
		t.Fatalf("update traffic status: %v", err)
	}

	cs := isvc.Status.Components[v1beta1.EngineComponent]
	if !reflect.DeepEqual(cs.Traffic, traffic) {
		t.Errorf("Traffic=%+v want unchanged %+v", cs.Traffic, traffic)
	}
	if got, want := cs.LatestReadyRevision, "llama-engine-rev-current"; got != want {
		t.Errorf("LatestReadyRevision=%q want %q", got, want)
	}
	if got, want := cs.LatestRolledoutRevision, "llama-engine-rev-current"; got != want {
		t.Errorf("LatestRolledoutRevision=%q want %q", got, want)
	}
	if got, want := cs.PreviousRolledoutRevision, "llama-engine-rev-old"; got != want {
		t.Errorf("PreviousRolledoutRevision=%q want %q", got, want)
	}

	if err := updateTrafficStatus(
		context.Background(),
		c,
		isvc,
		[]v1beta1.ComponentType{v1beta1.EngineComponent},
		map[v1beta1.ComponentType]map[string]int32{
			v1beta1.EngineComponent: {"current": 1},
		},
		1,
	); err != nil {
		t.Fatalf("repeat update traffic status: %v", err)
	}
	cs = isvc.Status.Components[v1beta1.EngineComponent]
	if got, want := cs.PreviousRolledoutRevision, "llama-engine-rev-old"; got != want {
		t.Errorf("repeat PreviousRolledoutRevision=%q want %q", got, want)
	}
}

// singleEngineGroup returns a minimal RolloutSpec that opts the engine
// Component into a rollout group. (v1's Independent policy is gone in v2;
// a lone single-Component blueGreen group is the minimal opt-in that
// keeps the per-revision data plane + group status writing exercised.)
func singleEngineGroup() *v1beta1.RolloutSpec {
	return &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				BlueGreen:  &v1beta1.GroupBlueGreen{},
			},
		},
	}
}

func TestReconcile_WritesGroupStatus(t *testing.T) {
	isvc := testOMENativeISVC()
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
		},
	}
	pods := []runtime.Object{buildPod(isvc, v1beta1.EngineComponent, "hash1", 0)}
	c := testClient(pods...)
	if _, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(),
		ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent),
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if isvc.Status.RolloutCoordination == nil {
		t.Fatalf("RolloutCoordination status nil")
	}
	if len(isvc.Status.RolloutCoordination.Groups) != 1 {
		t.Fatalf("groups: got %d want 1", len(isvc.Status.RolloutCoordination.Groups))
	}
	g := isvc.Status.RolloutCoordination.Groups[0]
	if g.Name != "0" {
		t.Errorf("Name: got %q want 0", g.Name)
	}
	if g.Phase == "" {
		t.Errorf("Phase should be populated")
	}
}

func TestReconcile_InvalidGroupShapeErrors(t *testing.T) {
	isvc := testOMENativeISVC()
	// A rollingUpdate group with maxSurge=0 AND maxUnavailable=0 is shape-invalid:
	// no surge headroom and no drain headroom, so the roll can never make
	// progress. The webhook rejects it in production; ValidateGroupShape is the
	// runtime safety net the reconciler runs in case an object bypassed admission.
	// (Single-Component rollingUpdate is a valid progression; only the
	// deadlock-budget shape is rejected here. "Sequential without order" is not
	// expressible: Sequential is a run of single-Component blueGreen groups the
	// controller collapses with a derived Order, so it can never be order-less.)
	zero := intstr.FromInt(0)
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				RollingUpdate: &v1beta1.GroupRollingUpdate{
					MaxSurge:       &zero,
					MaxUnavailable: &zero,
				},
			},
		},
	}
	c := testClient()
	_, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(),
		ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent),
	})
	if err == nil {
		t.Errorf("zero-budget pacing: want error")
	}
}

func TestReconcile_ClearsStaleCoordinationStatusWhenGroupsGone(t *testing.T) {
	isvc := testOMENativeISVC()
	// Pre-seed status as if a prior blueGreen rollout ran and is mid-flight.
	isvc.Status.RolloutCoordination = &v1beta1.RolloutCoordinationStatus{
		Groups: []v1beta1.RolloutCoordinationGroupStatus{
			{Name: "0", Policy: v1beta1.CoordinationPolicyBlueGreen, Phase: v1beta1.CoordinationPhaseSurging},
		},
	}
	isvc.Status.SetCondition(apis.ConditionType(v1beta1.RolloutCoordinationReady), &apis.Condition{
		Type:   apis.ConditionType(v1beta1.RolloutCoordinationReady),
		Status: corev1.ConditionFalse,
		Reason: "InProgress",
	})
	// The ISVC now has NO coordination-style group (switched to canary-only).
	// coordination.Reconcile still runs (unconditional in v2) but ResolveGroups
	// returns empty — it must reconcile the stale status away, not leave it.
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Canary:     &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{{Capacity: intstr.FromString("100%"), Traffic: 100}}},
			},
		},
	}
	c := testClient()
	if _, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(),
		ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent),
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if isvc.Status.RolloutCoordination != nil {
		t.Errorf("stale RolloutCoordination not cleared: %+v", isvc.Status.RolloutCoordination)
	}
	cond := isvc.Status.GetCondition(apis.ConditionType(v1beta1.RolloutCoordinationReady))
	if cond == nil || cond.Status != corev1.ConditionTrue {
		t.Errorf("RolloutCoordinationReady should be resolved to True, got %+v", cond)
	}
}

func testOMENativeISVC() *v1beta1.InferenceService {
	isvc := testReconcilerISVC()
	isvc.Spec.Engine = &v1beta1.EngineSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{
				constants.DeploymentMode: string(constants.OMENative),
			},
		},
	}
	return isvc
}

func testReconcilerISVC() *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "llama",
			Namespace: "prod",
			UID:       types.UID("llama-uid"),
		},
	}
}

func testOMENativeModes(components ...v1beta1.ComponentType) map[v1beta1.ComponentType]constants.DeploymentModeType {
	modes := make(map[v1beta1.ComponentType]constants.DeploymentModeType, len(components))
	for _, component := range components {
		modes[component] = constants.OMENative
	}
	return modes
}

// buildPod returns a Running+Ready pod (the realistic steady state):
// the traffic writer weighs only READY pods, so a status-less fixture
// would carry zero weight.
func buildPod(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, revisionHash string, idx int) runtime.Object {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName(isvc.Name, component, idx),
			Namespace: isvc.Namespace,
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: isvc.Name,
				constants.OMEComponentLabel:           string(component),
				query.LabelManagedBy:                  query.ManagedByOMENative,
				query.LabelRevisionHash:               revisionHash,
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}
}

func buildRunnerPod(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, revisionHash string, idx int, runner v1beta1.RunnerName, ordinal string) *corev1.Pod {
	pod := buildPod(isvc, component, revisionHash, idx).(*corev1.Pod)
	pod.Labels[query.LabelRunner] = string(runner)
	if ordinal != "" {
		pod.Labels[query.LabelPodOrdinal] = ordinal
	}
	return pod
}

func podName(isvc string, component v1beta1.ComponentType, idx int) string {
	return isvc + "-" + string(component) + "-pod-" + strconv.Itoa(idx)
}

func testClient(objs ...runtime.Object) client.Client {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()
}

// listCountingClient wraps a client.Client and counts Service LIST calls so
// a test can assert the GC orphan sweep (the heavier namespace Service LIST)
// is skipped when the revision set is converged, while ensure still runs.
type listCountingClient struct {
	client.Client
	serviceLists int
}

func (c *listCountingClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*corev1.ServiceList); ok {
		c.serviceLists++
	}
	return c.Client.List(ctx, list, opts...)
}

func TestReconcile_ConsiderCoordinationGroupEventFiresForMultiOMENativeWithoutCoord(t *testing.T) {
	isvc := testOMENativeISVC()
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{
				constants.DeploymentMode: string(constants.OMENative),
			},
		},
	}
	rec := record.NewFakeRecorder(10)
	c := testClient()
	_, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Recorder: rec, Now: time.Now(),
		ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent, v1beta1.DecoderComponent),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	events := drainEvents(rec)
	var saw bool
	for _, e := range events {
		if contains(e, EventReasonConsiderCoordinationGroup) {
			saw = true
		}
	}
	if !saw {
		t.Errorf("expected %s event for multi-OMENative ISVC without coord block; got %v",
			EventReasonConsiderCoordinationGroup, events)
	}
}

func TestReconcile_ConsiderCoordinationGroupEventNotFiredForSingleComponent(t *testing.T) {
	isvc := testOMENativeISVC() // engine only
	rec := record.NewFakeRecorder(10)
	c := testClient()
	_, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Recorder: rec, Now: time.Now(),
		ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	events := drainEvents(rec)
	for _, e := range events {
		if contains(e, EventReasonConsiderCoordinationGroup) {
			t.Errorf("single-Component ISVC must not get the hint event; got %v", events)
		}
	}
}

func TestReconcile_ConsiderCoordinationGroupEventNotFiredWhenGroupDeclared(t *testing.T) {
	isvc := testOMENativeISVC()
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{
				constants.DeploymentMode: string(constants.OMENative),
			},
		},
	}
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			BlueGreen:  &v1beta1.GroupBlueGreen{},
		}},
	}
	rec := record.NewFakeRecorder(10)
	c := testClient()
	_, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Recorder: rec, Now: time.Now(),
		ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent, v1beta1.DecoderComponent),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	events := drainEvents(rec)
	for _, e := range events {
		if contains(e, EventReasonConsiderCoordinationGroup) {
			t.Errorf("ISVC with coord block must not get the hint event; got %v", events)
		}
	}
}

func TestReconcile_ConsiderCoordinationGroupEventFiresOnceAcrossReconciles(t *testing.T) {
	isvc := testOMENativeISVC()
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{
				constants.DeploymentMode: string(constants.OMENative),
			},
		},
	}
	rec := record.NewFakeRecorder(10)
	c := testClient()
	countAdvisory := func() int {
		n := 0
		for _, e := range drainEvents(rec) {
			if contains(e, EventReasonConsiderCoordinationGroup) {
				n++
			}
		}
		return n
	}
	// First reconcile: transition into the advisory state -> event fires once.
	if _, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Recorder: rec, Now: time.Now(),
		ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent, v1beta1.DecoderComponent),
	}); err != nil {
		t.Fatalf("reconcile 1: %v", err)
	}
	if got := countAdvisory(); got != 1 {
		t.Fatalf("expected exactly 1 %s event on first reconcile; got %d",
			EventReasonConsiderCoordinationGroup, got)
	}
	// Second reconcile, same ISVC (CoordinationAdvisory condition now set):
	// the one-shot event must NOT re-fire — a runaway per-reconcile event
	// count is exactly what the condition prevents.
	if _, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Recorder: rec, Now: time.Now(),
		ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent, v1beta1.DecoderComponent),
	}); err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	if got := countAdvisory(); got != 0 {
		t.Errorf("advisory event must not re-fire once the CoordinationAdvisory condition is set; got %d", got)
	}
}

// --- buildComponentObservation per-Instance failure rollup ---

func TestBuildComponentObservation_NoStatusReturnsZero(t *testing.T) {
	// No IR status available (nil summary) — observation must
	// report zero values, NOT Failed (otherwise a brand-new ISVC would
	// flip the group to Failed).
	obs := buildComponentObservation(nil, v1beta1.EngineComponent, nil, 0)
	if obs.Failed {
		t.Errorf("missing status must not report Failed; got %+v", obs)
	}
	if obs.FailureMessage != "" {
		t.Errorf("missing status must not carry a FailureMessage; got %q", obs.FailureMessage)
	}
}

func TestBuildComponentObservation_NoFailedInstancesIsNotFailed(t *testing.T) {
	// All Instances Ready/Updating/etc., none Failed — observation
	// reports Failed=false. Pins that the rollup only fires on the
	// explicit Failed phase, not on transient states.
	summary := &v1beta1.InferenceReplicaStatus{
		InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Phase: v1beta1.OMENativeInstanceReady},
			{Index: 1, Phase: v1beta1.OMENativeInstanceUpdating},
		},
	}
	obs := buildComponentObservation(summary, v1beta1.EngineComponent, nil, 0)
	if obs.Failed {
		t.Errorf("no Failed Instances should not flip Failed; got %+v", obs)
	}
}

func TestBuildComponentObservation_AnyFailedInstanceMarksComponentFailed(t *testing.T) {
	// One Instance in Phase=Failed — the rollup must mark the
	// Component Failed so the BlueGreen / Sequential / RollingUpdate
	// state machines transition the group to Failed.
	summary := &v1beta1.InferenceReplicaStatus{
		InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Phase: v1beta1.OMENativeInstanceReady},
			{Index: 1, Phase: v1beta1.OMENativeInstanceFailed},
		},
	}
	obs := buildComponentObservation(summary, v1beta1.EngineComponent, nil, 0)
	if !obs.Failed {
		t.Errorf("any Failed Instance must mark Component Failed; got %+v", obs)
	}
	if obs.FailureMessage == "" {
		t.Errorf("Failed Component must carry a FailureMessage; got empty")
	}
	if !contains(obs.FailureMessage, "1") {
		t.Errorf("FailureMessage should name the failed Instance index; got %q", obs.FailureMessage)
	}
}

func TestBuildComponentObservation_MultipleFailedInstancesListedInMessage(t *testing.T) {
	// Two Instances in Phase=Failed — message names both.
	summary := &v1beta1.InferenceReplicaStatus{
		InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Phase: v1beta1.OMENativeInstanceFailed},
			{Index: 2, Phase: v1beta1.OMENativeInstanceFailed},
			{Index: 1, Phase: v1beta1.OMENativeInstanceReady},
		},
	}
	obs := buildComponentObservation(summary, v1beta1.EngineComponent, nil, 0)
	if !obs.Failed {
		t.Errorf("two Failed Instances must mark Component Failed; got %+v", obs)
	}
	if !contains(obs.FailureMessage, "0") || !contains(obs.FailureMessage, "2") {
		t.Errorf("FailureMessage should name both failed indices (0, 2); got %q", obs.FailureMessage)
	}
}

func TestBuildComponentObservation_IRSourceMatchesLegacyCopy(t *testing.T) {
	// buildComponentObservation must produce identical
	// ComponentObservation values whether sourced from IR status or
	// the equivalent per-Component ISVC copy — the two representations
	// must agree.
	irStatus := &v1beta1.InferenceReplicaStatus{
		Replicas: 4, ReadyReplicas: 3, ServingReplicas: 3,
		UpdatedReplicas: 2, UpdatedReadyReplicas: 2,
		CurrentRevision: "svc-engine-aaaa", UpdateRevision: "svc-engine-bbbb",
		InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Phase: v1beta1.OMENativeInstanceReady},
			{Index: 1, Phase: v1beta1.OMENativeInstanceFailed},
		},
	}
	perRev := map[string]int32{"aaaa": 2, "bbbb": 2}
	got := buildComponentObservation(irStatus, v1beta1.EngineComponent, perRev, 0)
	if got.DesiredReplicas != 4 || got.ReadyPods != 3 || got.ServingPods != 3 ||
		got.NewRevisionPods != 2 || got.NewRevisionReadyPods != 2 ||
		got.TotalPods != 4 || !got.RolloutInFlight || !got.Failed {
		t.Fatalf("IR-sourced observation diverged from legacy copy semantics: %+v", got)
	}
	if got.CurrentRevisionHash != "aaaa" || got.TargetRevisionHash != "bbbb" {
		t.Fatalf("revision hashes diverged: %+v", got)
	}
}

func TestBuildComponentObservation_AtDesiredShapeWithStaticPartition(t *testing.T) {
	// Replicas=8, partition=2: 6 instances Ready on the target (update)
	// revision + 2 held instances Ready on the prior revision is the
	// desired staged shape. buildComponentObservation reports Partition=2
	// and AtDesiredShape=true.
	const target = "svc-engine-bbbb"
	const prior = "svc-engine-aaaa"
	instances := make([]v1beta1.OMENativeInstanceStatus, 0, 8)
	// Indices 0,1 are held on the prior revision (index < partition).
	for i := int32(0); i < 2; i++ {
		instances = append(instances, v1beta1.OMENativeInstanceStatus{
			Index: i, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: prior,
		})
	}
	// Indices 2..7 have rolled to the target revision.
	for i := int32(2); i < 8; i++ {
		instances = append(instances, v1beta1.OMENativeInstanceStatus{
			Index: i, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: target,
		})
	}
	summary := &v1beta1.InferenceReplicaStatus{
		Replicas:         8,
		UpdateRevision:   target,
		InstanceStatuses: instances,
	}
	obs := buildComponentObservation(summary, v1beta1.EngineComponent, nil, 2)
	if obs.Partition != 2 {
		t.Fatalf("Partition: got %d want 2", obs.Partition)
	}
	if !obs.AtDesiredShape {
		t.Fatalf("expected AtDesiredShape=true for the converged staged shape; got %+v", obs)
	}

	// Flip one held instance to a non-Ready phase — the staged shape is no
	// longer at rest (held instances must be Ready too), so AtDesiredShape
	// drops to false.
	instances[0].Phase = v1beta1.OMENativeInstanceUpdating
	obs = buildComponentObservation(summary, v1beta1.EngineComponent, nil, 2)
	if obs.AtDesiredShape {
		t.Fatalf("a non-Ready held instance must make AtDesiredShape=false; got %+v", obs)
	}
	if obs.Partition != 2 {
		t.Fatalf("Partition should stay 2 regardless of convergence; got %d", obs.Partition)
	}
}

func TestBuildGroupObservation_PartitionFromProjectedIRSpec(t *testing.T) {
	// The effective partition comes from the projected IR spec (the
	// merged ISVC↔runtime lifecycle). The raw ISVC here carries no
	// lifecycle at all — modeling a partition inherited from the
	// ServingRuntime — yet the observation must still stage the
	// Component at partition 2 and report AtDesiredShape once
	// (Replicas-2) instances are Ready on the target and 2 held
	// instances are Ready on the prior revision.
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		Policy:     v1beta1.CoordinationPolicyBlueGreen,
	}
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "isvc", Namespace: "default"},
	}
	const target = "isvc-engine-bbbb"
	const prior = "isvc-engine-aaaa"
	instances := []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: prior},
		{Index: 1, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: prior},
		{Index: 2, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: target},
		{Index: 3, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: target},
	}
	partition := int32(2)
	engineIR := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "isvc-engine", Namespace: "default"},
		Spec: v1beta1.InferenceReplicaSpec{
			Lifecycle: &v1beta1.LifecycleSpec{
				UpdateStrategy: &v1beta1.UpdateStrategy{
					RollingUpdate: &v1beta1.RollingUpdate{Partition: &partition},
				},
			},
		},
		Status: v1beta1.InferenceReplicaStatus{
			Replicas:         4,
			CurrentRevision:  prior,
			UpdateRevision:   target,
			InstanceStatuses: instances,
		},
	}
	c := testClient(engineIR)
	obs, err := buildGroupObservation(context.Background(), c, isvc, g, map[v1beta1.ComponentType]map[string]int32{})
	if err != nil {
		t.Fatalf("buildGroupObservation: %v", err)
	}
	engineObs := obs.Components[v1beta1.EngineComponent]
	if engineObs.Partition != 2 {
		t.Fatalf("Partition must come from the projected IR spec; got %d want 2", engineObs.Partition)
	}
	if !engineObs.AtDesiredShape {
		t.Fatalf("staged shape at the IR-spec partition must report AtDesiredShape=true; got %+v", engineObs)
	}
}

func TestBuildComponentObservation_FailedPropagatesThroughBlueGreen(t *testing.T) {
	// End-to-end: a Failed Instance flows through buildComponentObservation
	// into the BlueGreen state machine, which returns Phase=Failed for
	// the group. The bad_image_kind KIND spec pins the same path end to end.
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Policy:     v1beta1.CoordinationPolicyBlueGreen,
	}
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "isvc-test", Namespace: "default"},
	}
	engineIR := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "isvc-test-engine", Namespace: "default"},
		Status: v1beta1.InferenceReplicaStatus{
			InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
				{Index: 0, Phase: v1beta1.OMENativeInstanceFailed},
			},
		},
	}
	decoderIR := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "isvc-test-decoder", Namespace: "default"},
		Status: v1beta1.InferenceReplicaStatus{
			InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
				{Index: 0, Phase: v1beta1.OMENativeInstanceReady},
			},
		},
	}
	c := testClient(engineIR, decoderIR)
	obs, err := buildGroupObservation(context.Background(), c, isvc, g, map[v1beta1.ComponentType]map[string]int32{})
	if err != nil {
		t.Fatalf("buildGroupObservation: %v", err)
	}
	tr := ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhaseFailed {
		t.Errorf("BlueGreen with any Failed Component must report Failed; got %q", tr.Phase)
	}
}

func TestCollectFailedInstanceIndices_PreservesOrder(t *testing.T) {
	out := collectFailedInstanceIndices([]v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceReady},
		{Index: 2, Phase: v1beta1.OMENativeInstanceFailed},
		{Index: 1, Phase: v1beta1.OMENativeInstanceFailed},
		{Index: 3, Phase: v1beta1.OMENativeInstanceUpdating},
	})
	if len(out) != 2 {
		t.Fatalf("got %d failed indices want 2: %v", len(out), out)
	}
	// Slice order matches the input order (the InstanceStatuses
	// iteration order). This is intentional — the formatter doesn't
	// sort either, so operators see indices as they appear in status.
	if out[0] != 2 || out[1] != 1 {
		t.Errorf("collect order: got %v want [2 1] (slice order, not sorted)", out)
	}
}

func TestCollectFailedInstanceIndices_EmptyOnNoFailures(t *testing.T) {
	out := collectFailedInstanceIndices([]v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceReady},
	})
	if len(out) != 0 {
		t.Errorf("no failures should return nil/empty; got %v", out)
	}
}

// TestBuildRatioState_DefersSnapshotWhenStatusEmpty pins the snapshot
// deferral behind the BlueGreen+RatioBalanced anchor. The first reconcile
// of a freshly-created ISVC observes every Component with
// status.OMENative.Replicas == 0 (the OMENative status writer hasn't
// run yet). Snapshotting from that empty status would anchor Original to
// {Engine: 1, Decoder: 1} via SnapshotOriginal's zero-clamp, deadlocking
// rollouts whose live ratio disagrees with 1:1.
//
// The snapshot is deferred until every Component has non-zero
// observed Replicas. This test pins that contract: empty status →
// empty Original; populated status → {eng: 4, dec: 2} matches the
// cluster shape.
func TestBuildRatioState_DefersSnapshotWhenStatusEmpty(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Policy:     v1beta1.CoordinationPolicyBlueGreen,
		Pacing: v1beta1.CoordinationPacing{
			Type: v1beta1.CoordinationPacingRatioBalanced,
		},
	}

	// IR status not yet available — DesiredReplicas==0 for both Components.
	// buildComponentObservation returns zero-valued observations when
	// IR summary is nil.
	isvcEmpty := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "isvc", Namespace: "default"}}
	emptyClient := testClient()
	obsEmpty, err := buildGroupObservation(context.Background(), emptyClient, isvcEmpty, g, map[v1beta1.ComponentType]map[string]int32{})
	if err != nil {
		t.Fatalf("buildGroupObservation: %v", err)
	}
	stateEmpty := buildRatioState(isvcEmpty, g, obsEmpty, map[v1beta1.ComponentType]map[string]int32{})

	if len(stateEmpty.Original) != 0 {
		t.Fatalf("empty status MUST defer snapshot to avoid the SnapshotOriginal zero-clamp anchoring Original to {1, 1}; got Original=%v", stateEmpty.Original)
	}

	// Once IR status carries real Replicas counts, the snapshot should
	// reflect the cluster shape verbatim.
	isvcPopulated := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "isvc", Namespace: "default"}}
	engineIR := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "isvc-engine", Namespace: "default"},
		Status:     v1beta1.InferenceReplicaStatus{Replicas: 4},
	}
	decoderIR := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "isvc-decoder", Namespace: "default"},
		Status:     v1beta1.InferenceReplicaStatus{Replicas: 2},
	}
	popClient := testClient(engineIR, decoderIR)
	obsPopulated, err := buildGroupObservation(context.Background(), popClient, isvcPopulated, g, map[v1beta1.ComponentType]map[string]int32{})
	if err != nil {
		t.Fatalf("buildGroupObservation: %v", err)
	}
	statePopulated := buildRatioState(isvcPopulated, g, obsPopulated, map[v1beta1.ComponentType]map[string]int32{})

	if statePopulated.Original[v1beta1.EngineComponent] != 4 {
		t.Errorf("Original engine: got %d want 4 (matches status.OMENative.Replicas)", statePopulated.Original[v1beta1.EngineComponent])
	}
	if statePopulated.Original[v1beta1.DecoderComponent] != 2 {
		t.Errorf("Original decoder: got %d want 2 (matches status.OMENative.Replicas)", statePopulated.Original[v1beta1.DecoderComponent])
	}
}

// TestBuildRatioState_PartiallyPopulatedStatusDefersSnapshot covers the
// race where one Component has been reconciled (status.OMENative.Replicas
// populated) but the other hasn't yet. The snapshot anchor must reflect
// the WHOLE group's shape, so until every Component reports non-zero
// Replicas, defer. Otherwise the in-flight Component pegs the ratio
// against a still-zero peer and immediately rejects every surge.
func TestBuildRatioState_PartiallyPopulatedStatusDefersSnapshot(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Policy:     v1beta1.CoordinationPolicyBlueGreen,
		Pacing: v1beta1.CoordinationPacing{
			Type: v1beta1.CoordinationPacingRatioBalanced,
		},
	}
	// Engine IR populated; decoder IR not yet.
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "isvc", Namespace: "default"}}
	engineIR := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "isvc-engine", Namespace: "default"},
		Status:     v1beta1.InferenceReplicaStatus{Replicas: 4},
	}
	c := testClient(engineIR)
	obs, err := buildGroupObservation(context.Background(), c, isvc, g, map[v1beta1.ComponentType]map[string]int32{})
	if err != nil {
		t.Fatalf("buildGroupObservation: %v", err)
	}
	state := buildRatioState(isvc, g, obs, map[v1beta1.ComponentType]map[string]int32{})
	if len(state.Original) != 0 {
		t.Fatalf("partially populated status MUST defer snapshot; got Original=%v", state.Original)
	}
}

// TestBuildRatioState_PreservesAlreadyRecordedAnchor pins the
// already-recorded path: when ObservedRatio.Original is already
// persisted from a prior reconcile, buildRatioState reads it as-is
// (verbatim) and does NOT re-snapshot — the anchor must remain stable
// across the rollout even if MinReplicas changes mid-flight.
func TestBuildRatioState_PreservesAlreadyRecordedAnchor(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Policy:     v1beta1.CoordinationPolicyBlueGreen,
		Pacing: v1beta1.CoordinationPacing{
			Type: v1beta1.CoordinationPacingRatioBalanced,
		},
	}
	// IR status: scaled DOWN to 2 engine + 1 decoder mid-rollout.
	// ObservedRatio.Original captured at rollout start = 4:2 (the
	// anchor we want to preserve, NOT the current shape).
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "isvc", Namespace: "default"},
		Status: v1beta1.InferenceServiceStatus{
			RolloutCoordination: &v1beta1.RolloutCoordinationStatus{
				Groups: []v1beta1.RolloutCoordinationGroupStatus{{
					Name: "0",
					ObservedRatio: &v1beta1.RolloutCoordinationRatio{
						Original: map[v1beta1.ComponentType]int32{
							v1beta1.EngineComponent:  4,
							v1beta1.DecoderComponent: 2,
						},
					},
				}},
			},
		},
	}
	engineIR := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "isvc-engine", Namespace: "default"},
		Status:     v1beta1.InferenceReplicaStatus{Replicas: 2},
	}
	decoderIR := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "isvc-decoder", Namespace: "default"},
		Status:     v1beta1.InferenceReplicaStatus{Replicas: 1},
	}
	c := testClient(engineIR, decoderIR)
	obs, err := buildGroupObservation(context.Background(), c, isvc, g, map[v1beta1.ComponentType]map[string]int32{})
	if err != nil {
		t.Fatalf("buildGroupObservation: %v", err)
	}
	state := buildRatioState(isvc, g, obs, map[v1beta1.ComponentType]map[string]int32{})
	if state.Original[v1beta1.EngineComponent] != 4 {
		t.Errorf("anchor preservation: engine Original got %d want 4 (the snapshot from rollout start, NOT the current 2)", state.Original[v1beta1.EngineComponent])
	}
	if state.Original[v1beta1.DecoderComponent] != 2 {
		t.Errorf("anchor preservation: decoder Original got %d want 2 (the snapshot from rollout start, NOT the current 1)", state.Original[v1beta1.DecoderComponent])
	}
	if len(state.Original) != 2 {
		t.Errorf("stable membership must keep the anchor verbatim; got Original=%v", state.Original)
	}
}

// ratioMembershipISVC builds an ISVC whose group "0" carries a persisted
// {engine: 4, decoder: 2} anchor plus any extra anchor entries.
func ratioMembershipISVC(extra map[v1beta1.ComponentType]int32) *v1beta1.InferenceService {
	original := map[v1beta1.ComponentType]int32{
		v1beta1.EngineComponent:  4,
		v1beta1.DecoderComponent: 2,
	}
	for c, r := range extra {
		original[c] = r
	}
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "isvc", Namespace: "default"},
		Status: v1beta1.InferenceServiceStatus{
			RolloutCoordination: &v1beta1.RolloutCoordinationStatus{
				Groups: []v1beta1.RolloutCoordinationGroupStatus{{
					Name:          "0",
					ObservedRatio: &v1beta1.RolloutCoordinationRatio{Original: original},
				}},
			},
		},
	}
}

// TestBuildRatioState_LateMemberGainsAnchor: a Component added after the
// anchor was recorded gains an entry from its observed Replicas while
// recorded entries stay verbatim despite drifted live counts.
func TestBuildRatioState_LateMemberGainsAnchor(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent},
		Policy:     v1beta1.CoordinationPolicyBlueGreen,
		Pacing: v1beta1.CoordinationPacing{
			Type: v1beta1.CoordinationPacingRatioBalanced,
		},
	}
	isvc := ratioMembershipISVC(nil)
	// Live counts drifted since the snapshot (engine 4→2): the merge
	// must not rewrite existing anchors from live status.
	engineIR := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "isvc-engine", Namespace: "default"},
		Status:     v1beta1.InferenceReplicaStatus{Replicas: 2},
	}
	decoderIR := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "isvc-decoder", Namespace: "default"},
		Status:     v1beta1.InferenceReplicaStatus{Replicas: 2},
	}
	routerIR := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "isvc-router", Namespace: "default"},
		Status:     v1beta1.InferenceReplicaStatus{Replicas: 3},
	}
	c := testClient(engineIR, decoderIR, routerIR)
	obs, err := buildGroupObservation(context.Background(), c, isvc, g, map[v1beta1.ComponentType]map[string]int32{})
	if err != nil {
		t.Fatalf("buildGroupObservation: %v", err)
	}
	state := buildRatioState(isvc, g, obs, map[v1beta1.ComponentType]map[string]int32{})
	if state.Original[v1beta1.RouterComponent] != 3 {
		t.Errorf("late member anchor: router Original got %d want 3 (its observed Replicas at join)", state.Original[v1beta1.RouterComponent])
	}
	if state.Original[v1beta1.EngineComponent] != 4 {
		t.Errorf("existing anchor disturbed: engine Original got %d want 4 (must stay verbatim despite live drift to 2)", state.Original[v1beta1.EngineComponent])
	}
	if state.Original[v1beta1.DecoderComponent] != 2 {
		t.Errorf("existing anchor disturbed: decoder Original got %d want 2", state.Original[v1beta1.DecoderComponent])
	}
}

// TestBuildRatioState_LateMemberWithZeroStatusDeferred: a late member
// with zero observed Replicas (status unwritten) is not anchored on this
// pass, matching the initial snapshot's defer-on-zero rule.
func TestBuildRatioState_LateMemberWithZeroStatusDeferred(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent},
		Policy:     v1beta1.CoordinationPolicyBlueGreen,
		Pacing: v1beta1.CoordinationPacing{
			Type: v1beta1.CoordinationPacingRatioBalanced,
		},
	}
	isvc := ratioMembershipISVC(nil)
	engineIR := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "isvc-engine", Namespace: "default"},
		Status:     v1beta1.InferenceReplicaStatus{Replicas: 4},
	}
	decoderIR := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "isvc-decoder", Namespace: "default"},
		Status:     v1beta1.InferenceReplicaStatus{Replicas: 2},
	}
	// No router IR: its observation reads DesiredReplicas == 0.
	c := testClient(engineIR, decoderIR)
	obs, err := buildGroupObservation(context.Background(), c, isvc, g, map[v1beta1.ComponentType]map[string]int32{})
	if err != nil {
		t.Fatalf("buildGroupObservation: %v", err)
	}
	state := buildRatioState(isvc, g, obs, map[v1beta1.ComponentType]map[string]int32{})
	if _, ok := state.Original[v1beta1.RouterComponent]; ok {
		t.Errorf("zero-status late member must be deferred, not anchored; got router Original=%d", state.Original[v1beta1.RouterComponent])
	}
	if state.Original[v1beta1.EngineComponent] != 4 || state.Original[v1beta1.DecoderComponent] != 2 {
		t.Errorf("existing anchors must survive a deferred late member; got Original=%v", state.Original)
	}
}

// TestBuildRatioState_RemovedMemberAnchorDropped: an anchor entry for a
// Component no longer in the group is dropped; remaining anchors stay
// verbatim.
func TestBuildRatioState_RemovedMemberAnchorDropped(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Policy:     v1beta1.CoordinationPolicyBlueGreen,
		Pacing: v1beta1.CoordinationPacing{
			Type: v1beta1.CoordinationPacingRatioBalanced,
		},
	}
	isvc := ratioMembershipISVC(map[v1beta1.ComponentType]int32{v1beta1.RouterComponent: 3})
	engineIR := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "isvc-engine", Namespace: "default"},
		Status:     v1beta1.InferenceReplicaStatus{Replicas: 4},
	}
	decoderIR := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "isvc-decoder", Namespace: "default"},
		Status:     v1beta1.InferenceReplicaStatus{Replicas: 2},
	}
	c := testClient(engineIR, decoderIR)
	obs, err := buildGroupObservation(context.Background(), c, isvc, g, map[v1beta1.ComponentType]map[string]int32{})
	if err != nil {
		t.Fatalf("buildGroupObservation: %v", err)
	}
	state := buildRatioState(isvc, g, obs, map[v1beta1.ComponentType]map[string]int32{})
	if _, ok := state.Original[v1beta1.RouterComponent]; ok {
		t.Errorf("removed member's anchor must be dropped; got router Original=%d", state.Original[v1beta1.RouterComponent])
	}
	if state.Original[v1beta1.EngineComponent] != 4 || state.Original[v1beta1.DecoderComponent] != 2 {
		t.Errorf("remaining anchors must stay verbatim; got Original=%v", state.Original)
	}
}

// TestBuildRatioState_AllAnchorsStaleFallsBackToSnapshot: when every
// persisted anchor belongs to removed members, the group is treated as
// never-snapshotted and the initial snapshot path runs.
func TestBuildRatioState_AllAnchorsStaleFallsBackToSnapshot(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Policy:     v1beta1.CoordinationPolicyBlueGreen,
		Pacing: v1beta1.CoordinationPacing{
			Type: v1beta1.CoordinationPacingRatioBalanced,
		},
	}
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "isvc", Namespace: "default"},
		Status: v1beta1.InferenceServiceStatus{
			RolloutCoordination: &v1beta1.RolloutCoordinationStatus{
				Groups: []v1beta1.RolloutCoordinationGroupStatus{{
					Name: "0",
					ObservedRatio: &v1beta1.RolloutCoordinationRatio{
						Original: map[v1beta1.ComponentType]int32{v1beta1.RouterComponent: 3},
					},
				}},
			},
		},
	}
	engineIR := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "isvc-engine", Namespace: "default"},
		Status:     v1beta1.InferenceReplicaStatus{Replicas: 4},
	}
	decoderIR := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "isvc-decoder", Namespace: "default"},
		Status:     v1beta1.InferenceReplicaStatus{Replicas: 2},
	}
	c := testClient(engineIR, decoderIR)
	obs, err := buildGroupObservation(context.Background(), c, isvc, g, map[v1beta1.ComponentType]map[string]int32{})
	if err != nil {
		t.Fatalf("buildGroupObservation: %v", err)
	}
	state := buildRatioState(isvc, g, obs, map[v1beta1.ComponentType]map[string]int32{})
	if state.Original[v1beta1.EngineComponent] != 4 || state.Original[v1beta1.DecoderComponent] != 2 {
		t.Errorf("all-stale anchor must re-snapshot the current membership; got Original=%v", state.Original)
	}
	if _, ok := state.Original[v1beta1.RouterComponent]; ok {
		t.Errorf("stale anchor entry must not survive the re-snapshot; got Original=%v", state.Original)
	}
}

// TestPodReadyAndServing covers the serving-readiness predicate the canary
// capacity gate relies on: only Running, non-deleting pods reporting
// PodReady=True count as serving.
func TestPodReadyAndServing(t *testing.T) {
	condTrue, condFalse := corev1.ConditionTrue, corev1.ConditionFalse
	mk := func(phase corev1.PodPhase, cond *corev1.ConditionStatus, deleting bool) *corev1.Pod {
		p := &corev1.Pod{}
		p.Status.Phase = phase
		if cond != nil {
			p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: *cond}}
		}
		if deleting {
			p.DeletionTimestamp = &metav1.Time{}
		}
		return p
	}
	cases := []struct {
		name string
		pod  *corev1.Pod
		want bool
	}{
		{"running + ready", mk(corev1.PodRunning, &condTrue, false), true},
		{"running + ready=false", mk(corev1.PodRunning, &condFalse, false), false},
		{"running + no ready condition", mk(corev1.PodRunning, nil, false), false},
		{"pending (ContainerCreating)", mk(corev1.PodPending, nil, false), false},
		{"running + ready but terminating", mk(corev1.PodRunning, &condTrue, true), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := podReadyAndServing(tc.pod); got != tc.want {
				t.Errorf("podReadyAndServing(%s) = %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}

// TestObservePerRevisionPods_TotalVsReady is the regression guard for the canary
// premature-cutover bug: total counts every pod carrying a revision hash, but the
// READY map excludes pods that merely exist (ContainerCreating/Pending). The
// canary capacity gate consumes the ready map, so it never shifts traffic / cuts
// over before the new revision is actually serving.
func TestObservePerRevisionPods_TotalVsReady(t *testing.T) {
	isvc := testReconcilerISVC()
	const stable, canary = "stablehash", "canaryhash"
	objs := []runtime.Object{
		buildReadyPod(isvc, v1beta1.EngineComponent, stable, 0, true),
		buildReadyPod(isvc, v1beta1.EngineComponent, stable, 1, true),
		buildReadyPod(isvc, v1beta1.EngineComponent, canary, 2, true),  // serving
		buildReadyPod(isvc, v1beta1.EngineComponent, canary, 3, false), // exists, not serving yet
	}
	total, ready, _, _, err := observePerRevisionPods(context.Background(), testClient(objs...), isvc, []v1beta1.ComponentType{v1beta1.EngineComponent}, true)
	if err != nil {
		t.Fatalf("observePerRevisionPods: %v", err)
	}
	eng := v1beta1.EngineComponent
	if total[eng][canary] != 2 {
		t.Errorf("total[canary] = %d, want 2 (both canary pods exist)", total[eng][canary])
	}
	if ready[eng][canary] != 1 {
		t.Errorf("ready[canary] = %d, want 1 (the not-yet-serving pod must be excluded)", ready[eng][canary])
	}
	if total[eng][stable] != 2 || ready[eng][stable] != 2 {
		t.Errorf("stable total/ready = %d/%d, want 2/2", total[eng][stable], ready[eng][stable])
	}
}

func TestObservePerRevisionPods_DetectsRoutingSelectorPerRevision(t *testing.T) {
	isvc := testReconcilerISVC()
	objs := []runtime.Object{
		buildRunnerPod(isvc, v1beta1.EngineComponent, "default", 0, v1beta1.RunnerNameDefault, "0"),
		buildPod(isvc, v1beta1.EngineComponent, "missing", 1),
		buildRunnerPod(isvc, v1beta1.EngineComponent, "leader", 2, v1beta1.RunnerNameLeader, ""),
		buildRunnerPod(isvc, v1beta1.EngineComponent, "worker", 3, v1beta1.RunnerNameWorker, ""),
		buildRunnerPod(isvc, v1beta1.EngineComponent, "with-ordinal", 4, v1beta1.RunnerNameLeader, "0"),
		buildRunnerPod(isvc, v1beta1.EngineComponent, "with-ordinal", 5, v1beta1.RunnerNameWorker, "0"),
		buildRunnerPod(isvc, v1beta1.EngineComponent, "mixed-ordinal", 6, v1beta1.RunnerNameLeader, "0"),
		buildRunnerPod(isvc, v1beta1.EngineComponent, "mixed-ordinal", 7, v1beta1.RunnerNameWorker, ""),
	}

	_, _, routing, _, err := observePerRevisionPods(
		context.Background(),
		testClient(objs...),
		isvc,
		[]v1beta1.ComponentType{v1beta1.EngineComponent},
		true,
	)
	if err != nil {
		t.Fatalf("observePerRevisionPods: %v", err)
	}
	want := map[string]RevisionRoutingSelector{
		"default":       {},
		"missing":       {},
		"leader":        {LeaderOnly: true},
		"worker":        {LeaderOnly: true},
		"with-ordinal":  {LeaderOnly: true, PodOrdinal: true},
		"mixed-ordinal": {LeaderOnly: true},
	}
	for hash, selector := range want {
		if got := routing[v1beta1.EngineComponent][hash]; got != selector {
			t.Errorf("routing selector for %s: got %+v want %+v", hash, got, selector)
		}
	}
}

// TestObservePerRevisionPods_IndexParityWithLabelPath asserts the indexed read
// strategy (useIndex=true) buckets the EXACT same pods, by revision hash, as the
// label-selector strategy (useIndex=false) — including a pod with a missing
// revision-hash label, which must be dropped from both maps. The fake client has
// no field index, so useIndex=true exercises ListOMENativePodsByName's index
// fallback; this guards that the index path stays bucket-for-bucket identical to
// the old raw label List.
func TestObservePerRevisionPods_IndexParityWithLabelPath(t *testing.T) {
	isvc := testReconcilerISVC()
	const stable, canary = "stablehash", "canaryhash"
	objs := []runtime.Object{
		buildReadyPod(isvc, v1beta1.EngineComponent, stable, 0, true),
		buildReadyPod(isvc, v1beta1.EngineComponent, stable, 1, false),
		buildReadyPod(isvc, v1beta1.EngineComponent, canary, 2, true),
		buildReadyPod(isvc, v1beta1.EngineComponent, "", 3, true), // missing hash → dropped
	}
	comps := []v1beta1.ComponentType{v1beta1.EngineComponent}

	totalIdx, readyIdx, routingIdx, _, err := observePerRevisionPods(context.Background(), testClient(objs...), isvc, comps, true)
	if err != nil {
		t.Fatalf("observePerRevisionPods(useIndex=true): %v", err)
	}
	totalLbl, readyLbl, routingLbl, _, err := observePerRevisionPods(context.Background(), testClient(objs...), isvc, comps, false)
	if err != nil {
		t.Fatalf("observePerRevisionPods(useIndex=false): %v", err)
	}

	if !reflect.DeepEqual(totalIdx, totalLbl) {
		t.Errorf("total maps differ: index=%v label=%v", totalIdx, totalLbl)
	}
	if !reflect.DeepEqual(readyIdx, readyLbl) {
		t.Errorf("ready maps differ: index=%v label=%v", readyIdx, readyLbl)
	}
	if !reflect.DeepEqual(routingIdx, routingLbl) {
		t.Errorf("routing selector maps differ: index=%v label=%v", routingIdx, routingLbl)
	}
	eng := v1beta1.EngineComponent
	// Empty-hash pod must not appear under any bucket.
	if _, ok := totalIdx[eng][""]; ok {
		t.Errorf("empty-hash pod leaked into total bucket: %v", totalIdx[eng])
	}
	if totalIdx[eng][stable] != 2 || totalIdx[eng][canary] != 1 {
		t.Errorf("total buckets = %v, want stable=2 canary=1", totalIdx[eng])
	}
}

// buildReadyPod is buildPod with a serving status set (serving=true → Running +
// PodReady=True) or a not-yet-serving status (serving=false → Pending), for the
// total-vs-ready accounting test.
func buildReadyPod(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, revisionHash string, idx int, serving bool) runtime.Object {
	p := buildPod(isvc, component, revisionHash, idx).(*corev1.Pod)
	if serving {
		p.Status.Phase = corev1.PodRunning
		p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	} else {
		p.Status = corev1.PodStatus{Phase: corev1.PodPending}
	}
	return p
}

// failingIRClient wraps a client.Client and fails every InferenceReplica
// Get with a non-NotFound error, modeling a flaky apiserver / cache read.
// Everything else (pods, Services) passes through.
type failingIRClient struct {
	client.Client
}

func (c *failingIRClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*v1beta1.InferenceReplica); ok {
		return errors.New("simulated apiserver failure")
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

// TestReconcile_IRReadErrorFailsReconcile pins the fail-closed contract:
// an IR read error must fail the coordination reconcile (retry next
// pass), not degrade to a zero-valued observation that fakes an Idle
// group and rewrites traffic with a fabricated latest hash.
func TestReconcile_IRReadErrorFailsReconcile(t *testing.T) {
	isvc := testOMENativeISVC()
	isvc.Spec.Rollout = singleEngineGroup()
	pods := []runtime.Object{buildPod(isvc, v1beta1.EngineComponent, "hash1", 0)}
	c := &failingIRClient{Client: testClient(pods...)}
	_, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(),
		ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent),
	})
	if err == nil {
		t.Fatal("IR read error must fail the reconcile, not silently degrade")
	}
	if !strings.Contains(err.Error(), "simulated apiserver failure") {
		t.Errorf("error should carry the underlying read failure: %v", err)
	}
}

// TestReconcile_TrafficWeighsOnlyReadyPods: a revision whose only pod is
// Pending (e.g. ImagePullBackOff) must not receive a traffic percent —
// its per-revision Service has zero endpoints. Its Service must still be
// ensured (total map) so it is routable the moment the pod flips Ready.
func TestReconcile_TrafficWeighsOnlyReadyPods(t *testing.T) {
	isvc := testOMENativeISVC()
	pods := []runtime.Object{
		buildPod(isvc, v1beta1.EngineComponent, "hash1", 0),
		buildReadyPod(isvc, v1beta1.EngineComponent, "hash2", 1, false),
	}
	c := testClient(pods...)
	if _, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(), ComponentRunnerPorts: testComponentRunnerPorts(),
		ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent),
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	traffic := isvc.Status.Components[v1beta1.EngineComponent].Traffic
	if len(traffic) != 1 {
		t.Fatalf("only the ready revision may carry weight; got %+v", traffic)
	}
	if want := PerRevisionServiceName(isvc.Name, v1beta1.EngineComponent, "hash1"); traffic[0].RevisionName != want || traffic[0].Percent != 100 {
		t.Errorf("ready revision must hold 100%%: got %+v want %s@100", traffic[0], want)
	}
	// The pending revision's Service is still ensured from the total map.
	svc := &corev1.Service{}
	key := client.ObjectKey{Namespace: isvc.Namespace, Name: PerRevisionServiceName(isvc.Name, v1beta1.EngineComponent, "hash2")}
	if err := c.Get(context.Background(), key, svc); err != nil {
		t.Errorf("pending revision's per-revision Service must still exist: %v", err)
	}
}

// TestMergeAndPersistGroupStatuses_PrunesRemovedGroups: a group renamed
// or removed from spec.rollout must not leave its stale status entry
// behind; surviving groups keep their LastTransitionTime when the phase
// didn't change.
func TestMergeAndPersistGroupStatuses_PrunesRemovedGroups(t *testing.T) {
	then := metav1.NewTime(time.Now().Add(-time.Hour))
	isvc := &v1beta1.InferenceService{
		Status: v1beta1.InferenceServiceStatus{
			RolloutCoordination: &v1beta1.RolloutCoordinationStatus{
				Groups: []v1beta1.RolloutCoordinationGroupStatus{
					{Name: "0", Phase: v1beta1.CoordinationPhaseIdle, LastTransitionTime: &then},
					{Name: "legacy", Phase: v1beta1.CoordinationPhaseSurging, LastTransitionTime: &then},
				},
			},
		},
	}
	now := metav1.Now()
	mergeAndPersistGroupStatuses(isvc, []v1beta1.RolloutCoordinationGroupStatus{
		{Name: "0", Phase: v1beta1.CoordinationPhaseIdle, LastTransitionTime: &now},
	})
	groups := isvc.Status.RolloutCoordination.Groups
	if len(groups) != 1 || groups[0].Name != "0" {
		t.Fatalf("undeclared group must be pruned; got %+v", groups)
	}
	if !groups[0].LastTransitionTime.Equal(&then) {
		t.Errorf("unchanged phase must preserve LastTransitionTime; got %v want %v", groups[0].LastTransitionTime, then)
	}
}

// TestEmitPhaseEvents_OnlyOnTransition pins the spam guard: a settled
// group must not re-emit GroupCompleted every reconcile (that burns the
// per-object EventCorrelator budget and can suppress real Warnings).
func TestEmitPhaseEvents_OnlyOnTransition(t *testing.T) {
	isvc := testReconcilerISVC()
	g := ResolvedGroup{Name: "0"}
	idle := GroupTransition{Phase: v1beta1.CoordinationPhaseIdle, Message: "settled"}

	rec := record.NewFakeRecorder(10)
	emitPhaseEvents(rec, isvc, g, idle, v1beta1.CoordinationPhaseIdle, "")
	if got := drainEvents(rec); len(got) != 0 {
		t.Errorf("unchanged Idle phase must not emit; got %v", got)
	}

	emitPhaseEvents(rec, isvc, g, idle, "", "")
	if got := drainEvents(rec); len(got) != 0 {
		t.Errorf("first observation at rest must not emit GroupCompleted; got %v", got)
	}

	emitPhaseEvents(rec, isvc, g, idle, v1beta1.CoordinationPhaseSurging, "")
	got := drainEvents(rec)
	if len(got) != 1 || !strings.Contains(got[0], EventReasonGroupCompleted) {
		t.Errorf("Surging→Idle transition must emit GroupCompleted once; got %v", got)
	}

	surging := GroupTransition{Phase: v1beta1.CoordinationPhaseSurging, Message: "rolling"}
	emitPhaseEvents(rec, isvc, g, surging, "", "")
	got = drainEvents(rec)
	if len(got) != 1 || !strings.Contains(got[0], EventReasonGroupSurging) {
		t.Errorf("first observation mid-roll must emit GroupSurging; got %v", got)
	}
}

// TestEmitPhaseEvents_AwaitingGatedOnCompositeTransition: the soak
// window holds the base Phase at Idle while only CompositePhase
// advances, so the Awaiting event is guarded on the composite
// transition, not the base one.
func TestEmitPhaseEvents_AwaitingGatedOnCompositeTransition(t *testing.T) {
	isvc := testReconcilerISVC()
	g := ResolvedGroup{Name: "0"}
	tr := GroupTransition{
		Phase:          v1beta1.CoordinationPhaseIdle,
		CompositePhase: v1beta1.CompositePhaseSequentialAwaiting,
		Message:        "soaking",
	}

	rec := record.NewFakeRecorder(10)
	emitPhaseEvents(rec, isvc, g, tr, v1beta1.CoordinationPhaseIdle, v1beta1.CompositePhaseSequentialAwaiting)
	if got := drainEvents(rec); len(got) != 0 {
		t.Errorf("unchanged composite must not re-emit Awaiting; got %v", got)
	}

	emitPhaseEvents(rec, isvc, g, tr, v1beta1.CoordinationPhaseIdle, "decoder.Surging")
	got := drainEvents(rec)
	if len(got) != 1 || !strings.Contains(got[0], EventReasonSequentialAwaitingNext) {
		t.Errorf("composite transition into Awaiting must emit once; got %v", got)
	}
}

// TestReconcile_SteadyStateEmitsNoGroupEvents drives the real entry
// point twice over a settled single-revision group: neither pass may
// emit a group phase event.
func TestReconcile_SteadyStateEmitsNoGroupEvents(t *testing.T) {
	isvc := testOMENativeISVC()
	isvc.Spec.Rollout = singleEngineGroup()
	pods := []runtime.Object{buildPod(isvc, v1beta1.EngineComponent, "hash1", 0)}
	c := testClient(pods...)
	rec := record.NewFakeRecorder(10)
	for pass := 0; pass < 2; pass++ {
		if _, err := Reconcile(context.Background(), ReconcileInputs{
			ISVC: isvc, Client: c, Reader: c, Recorder: rec, Now: time.Now(),
			ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent),
		}); err != nil {
			t.Fatalf("pass %d: unexpected error: %v", pass, err)
		}
		for _, e := range drainEvents(rec) {
			if strings.Contains(e, EventReasonGroupCompleted) {
				t.Errorf("pass %d: settled group must not emit GroupCompleted: %v", pass, e)
			}
		}
	}
}
