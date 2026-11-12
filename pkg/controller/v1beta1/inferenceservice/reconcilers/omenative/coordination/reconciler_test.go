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
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
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
	c := testClient(endpointsListed(isvc, pods...)...)
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
	c := testClient(endpointsListed(isvc, pods...)...)
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
	c := testClient(endpointsListed(isvc, pods...)...)
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
	c := testClient(endpointsListed(isvc, append(pods, engineIR, decoderIR, routerIR)...)...)
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
	if err := discoveryv1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(endpointsListed(isvc, pod, engineIR)...).WithStatusSubresource(&v1beta1.InferenceReplica{}).Build()

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
		c := testClient(endpointsListed(isvc, buildPod(isvc, v1beta1.EngineComponent, hash, 0), engineIR)...)
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

// A service whose two sides are resized during a roll has its ratio
// reference rewritten to the new counts by the next pass, and the status
// shows them: 8 prefill and 4 decode recorded, the specs now asking for 7
// and 3.
func TestReconcile_RatioReferenceFollowsAScaleMidRoll(t *testing.T) {
	isvc := testOMENativeISVC()
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{
				constants.DeploymentMode: string(constants.OMENative),
			},
		},
	}
	tol := int32(20)
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{{
			Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			RollingUpdate: &v1beta1.GroupRollingUpdate{},
			MaintainRatio: &v1beta1.MaintainRatio{Tolerance: &tol},
		}},
	}
	isvc.Status.RolloutCoordination = &v1beta1.RolloutCoordinationStatus{
		Groups: []v1beta1.RolloutCoordinationGroupStatus{{
			Name:       "0",
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			Policy:     v1beta1.CoordinationPolicyRollingUpdate,
			Phase:      v1beta1.CoordinationPhaseSurging,
			ObservedRatio: &v1beta1.RolloutCoordinationRatio{Original: map[v1beta1.ComponentType]int32{
				v1beta1.EngineComponent: 8, v1beta1.DecoderComponent: 4,
			}},
		}},
	}
	engineIR := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: isvc.Name + "-engine", Namespace: isvc.Namespace},
		Spec:       v1beta1.InferenceReplicaSpec{Replicas: int32Ptr(7)},
		Status: v1beta1.InferenceReplicaStatus{
			Replicas: 7, ServingReplicas: 6,
			CurrentRevision: isvc.Name + "-engine-oldHash", UpdateRevision: isvc.Name + "-engine-engineHash",
		},
	}
	decoderIR := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: isvc.Name + "-decoder", Namespace: isvc.Namespace},
		Spec:       v1beta1.InferenceReplicaSpec{Replicas: int32Ptr(3)},
		Status: v1beta1.InferenceReplicaStatus{
			Replicas: 3, ServingReplicas: 3,
			CurrentRevision: isvc.Name + "-decoder-oldHash", UpdateRevision: isvc.Name + "-decoder-decoderHash",
		},
	}
	pods := []runtime.Object{
		buildPod(isvc, v1beta1.EngineComponent, "oldHash", 0),
		buildPod(isvc, v1beta1.DecoderComponent, "oldHash", 0),
	}
	c := testClient(append(pods, engineIR, decoderIR)...)
	if _, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(),
		ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent, v1beta1.DecoderComponent),
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	groups := isvc.Status.RolloutCoordination.Groups
	if len(groups) != 1 || groups[0].ObservedRatio == nil {
		t.Fatalf("expected one group with a ratio reference; got %+v", groups)
	}
	got := groups[0].ObservedRatio.Original
	if got[v1beta1.EngineComponent] != 7 || got[v1beta1.DecoderComponent] != 3 {
		t.Fatalf("the status must show the counts the specs ask for now; got original=%v want engine 7, decoder 3", got)
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
	if err := discoveryv1.AddToScheme(scheme); err != nil {
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
	// the group. The cluster-level suite pins the same path end to end.
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

// A parked roll attempt's phase is its pod set's, and the Component's
// failure rollup is the row's phase, so the group's phase follows the set:
// it enters Failed while the set sits in the kubelet's back-off, leaves it
// once the set serves, and holds still while the runner restarts in place
// inside the stuck-pod grace, however many times - the GroupFailed warning
// fires once for the back-off and not again until the set is unready past
// the grace. The rows are the workload layer's own readings of the set.
func TestGroupPhase_ParkedAttemptRestartingInsideTheGraceDoesNotReenterFailed(t *testing.T) {
	const grace = 90 * time.Second
	now := time.Now()
	const attemptRevision = "llama-engine-newhash"
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: attemptRevision}}
	row := workloadtypes.InstanceStatus{
		Index: 0, Incarnation: 2, Phase: workloadtypes.InstancePhaseFailed, PodCount: 1,
		RunningRevision: "llama-engine-oldhash", TargetRevision: attemptRevision,
		Operation: &workloadtypes.InstanceOperation{
			ID: "update-0-1", Type: workloadtypes.InstanceOperationUpdate, Step: workloadtypes.UpdateStepParked,
			TargetRevision: attemptRevision, Waiting: string(workloadtypes.RolloutHoldGateRetryBlock),
		},
		LastFailure: &workloadtypes.InstanceTermination{PodName: "llama-engine-0-default-0", Reason: "CrashLoopBackOff"},
	}
	pod := func(ready bool, unreadyFor time.Duration, waiting string) *corev1.Pod {
		status := corev1.ConditionFalse
		if ready {
			status = corev1.ConditionTrue
		}
		since := metav1.NewTime(now.Add(-unreadyFor))
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "llama-engine-0-default-0", CreationTimestamp: metav1.NewTime(now.Add(-time.Hour))},
			Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{
				{Type: podreadiness.ConditionType, Status: corev1.ConditionTrue},
				{Type: corev1.ContainersReady, Status: status, LastTransitionTime: since},
				{Type: corev1.PodReady, Status: status, LastTransitionTime: since},
			}},
		}
		cs := corev1.ContainerStatus{Name: "main", Ready: ready, RestartCount: 2, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}
		if waiting != "" {
			cs.State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: waiting}}
		}
		p.Status.ContainerStatuses = []corev1.ContainerStatus{cs}
		return p
	}
	serving := pod(true, 0, "")
	backoff := pod(false, 0, "CrashLoopBackOff")
	restarting := func(unreadyFor time.Duration) *corev1.Pod { return pod(false, unreadyFor, "") }

	input := workloadtypes.ReconcileInput{StuckPodGrace: grace, Clock: clocktesting.NewFakeClock(now)}
	input.MutateInstance = func(_ context.Context, _ int32, mutate func(*workloadtypes.InstanceStatus) bool) error {
		mutate(&row)
		return nil
	}
	plan := workloadtypes.ComponentPlan{Component: workloadtypes.ComponentEngine, Replicas: 1,
		Instances: []workloadtypes.InstancePlan{{Index: 0, Incarnation: 2, Runners: []workloadtypes.RunnerPlan{{Name: "default", Size: 1}}}}}
	follow := func(p *corev1.Pod) {
		t.Helper()
		input.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{row}
		err := escalation.FollowStoppedAttempts(context.Background(), escalation.PassInput{
			Input: input, Plan: plan, Target: target,
			Pods: func(context.Context) (map[int32][]*corev1.Pod, error) { return map[int32][]*corev1.Pod{0: {p}}, nil },
		})
		if err != nil {
			t.Fatalf("FollowStoppedAttempts: %v", err)
		}
	}

	isvc := testOMENativeISVC()
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Policy: v1beta1.CoordinationPolicyRollingUpdate}
	rec := record.NewFakeRecorder(20)
	var prevPhase v1beta1.CoordinationPhase
	var prevComposite string
	groupPass := func() v1beta1.CoordinationPhase {
		t.Helper()
		ir := &v1beta1.InferenceReplica{
			ObjectMeta: metav1.ObjectMeta{Name: irprojector.InferenceReplicaName(isvc.Name, v1beta1.EngineComponent), Namespace: isvc.Namespace},
			Status:     v1beta1.InferenceReplicaStatus{Replicas: 1, InstanceStatuses: v1beta1convert.InstanceStatusSliceFromWorkload([]workloadtypes.InstanceStatus{row})},
		}
		obs, err := buildGroupObservation(context.Background(), testClient(ir), isvc, g, map[v1beta1.ComponentType]map[string]int32{})
		if err != nil {
			t.Fatalf("buildGroupObservation: %v", err)
		}
		tr := ComputeTransition(obs)
		emitPhaseEvents(rec, isvc, g, tr, prevPhase, prevComposite)
		prevPhase, prevComposite = tr.Phase, tr.CompositePhase
		return tr.Phase
	}
	failedEvents := func() int {
		n := 0
		for _, e := range drainEvents(rec) {
			if contains(e, EventReasonGroupFailed) {
				n++
			}
		}
		return n
	}

	follow(backoff)
	if phase := groupPass(); phase != v1beta1.CoordinationPhaseFailed || failedEvents() != 1 {
		t.Fatalf("the set in the kubelet's back-off: group %s, want Failed announced once", phase)
	}
	follow(serving)
	if phase := groupPass(); phase == v1beta1.CoordinationPhaseFailed {
		t.Fatalf("the set serving again must take the group out of Failed")
	}
	for i, p := range []*corev1.Pod{restarting(5 * time.Second), serving, restarting(grace / 2), serving, restarting(grace - time.Second)} {
		follow(p)
		if phase := groupPass(); phase == v1beta1.CoordinationPhaseFailed || row.Phase != workloadtypes.InstancePhaseUpdating {
			t.Fatalf("pass %d: group %s with the row %s, want the restart in place inside the grace to move nothing", i, phase, row.Phase)
		}
	}
	if n := failedEvents(); n != 0 {
		t.Fatalf("%d GroupFailed warning(s) across restarts in place inside the grace, want none", n)
	}
	follow(restarting(grace + time.Second))
	if phase := groupPass(); phase != v1beta1.CoordinationPhaseFailed || failedEvents() != 1 {
		t.Fatalf("the set unready past the grace: group %s, want Failed announced once more", phase)
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

// ratioGroup is the two-member RatioBalanced group the reference tests read.
func ratioGroup(components ...v1beta1.ComponentType) ResolvedGroup {
	return ResolvedGroup{
		Name:       "0",
		Components: components,
		Policy:     v1beta1.CoordinationPolicyBlueGreen,
		Pacing: v1beta1.CoordinationPacing{
			Type: v1beta1.CoordinationPacingRatioBalanced,
		},
	}
}

// replicaWithCounts is a member's replica whose spec asks for `desired`
// Instances while its status reports `rows` of them.
func replicaWithCounts(c v1beta1.ComponentType, desired, rows int32) *v1beta1.InferenceReplica {
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "isvc-" + string(c), Namespace: "default"},
		Spec:       v1beta1.InferenceReplicaSpec{Replicas: int32Ptr(desired)},
		Status:     v1beta1.InferenceReplicaStatus{Replicas: rows},
	}
}

// recordedReferenceISVC is a service whose group "0" carries the given
// ratio reference from an earlier pass.
func recordedReferenceISVC(original map[v1beta1.ComponentType]int32) *v1beta1.InferenceService {
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

// ratioReferenceOf runs the observation and the reference build the ISVC
// pass runs for one group against the given replicas.
func ratioReferenceOf(t *testing.T, isvc *v1beta1.InferenceService, g ResolvedGroup, irs ...runtime.Object) map[v1beta1.ComponentType]int32 {
	t.Helper()
	obs, err := buildGroupObservation(context.Background(), testClient(irs...), isvc, g, map[v1beta1.ComponentType]map[string]int32{})
	if err != nil {
		t.Fatalf("buildGroupObservation: %v", err)
	}
	return buildRatioState(isvc, g, obs, map[v1beta1.ComponentType]map[string]int32{}).Original
}

// TestBuildRatioState_DefersSnapshotUntilEveryReplicaExists pins where the
// reference comes from and when it is first taken. It is read off each
// member's replica spec, the count the operator asked for, so a replica
// whose status is not yet written still anchors at its real shape; a group
// with no replicas at all has nothing to anchor to and defers, never
// recording a zero-clamped {1, 1} that would wedge every later band
// decision.
func TestBuildRatioState_DefersSnapshotUntilEveryReplicaExists(t *testing.T) {
	g := ratioGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent)
	empty := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "isvc", Namespace: "default"}}
	if got := ratioReferenceOf(t, empty, g); len(got) != 0 {
		t.Fatalf("no replica exists yet, so nothing can be anchored; got Original=%v", got)
	}

	fresh := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "isvc", Namespace: "default"}}
	got := ratioReferenceOf(t, fresh, g,
		replicaWithCounts(v1beta1.EngineComponent, 4, 0),
		replicaWithCounts(v1beta1.DecoderComponent, 2, 0))
	if got[v1beta1.EngineComponent] != 4 || got[v1beta1.DecoderComponent] != 2 {
		t.Errorf("the reference is the count each spec asks for, status written or not; got Original=%v want engine 4, decoder 2", got)
	}
}

// TestBuildRatioState_OneReplicaMissingDefersSnapshot covers the pass in
// which one member's replica exists and the other's does not yet. The
// reference describes the whole group's shape, so it waits for every
// replica; otherwise the first member would be measured against a peer
// with no count.
func TestBuildRatioState_OneReplicaMissingDefersSnapshot(t *testing.T) {
	g := ratioGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent)
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "isvc", Namespace: "default"}}
	if got := ratioReferenceOf(t, isvc, g, replicaWithCounts(v1beta1.EngineComponent, 4, 4)); len(got) != 0 {
		t.Fatalf("a group with a replica still missing must defer its reference; got Original=%v", got)
	}
}

// TestBuildRatioState_ReferenceFollowsTheSpec pins the rule the band and
// the lockstep measure against: the reference is the count each member's
// spec asks for now. A service recorded at 15 prefill and 15 decode whose
// operator resizes it to 30 and 10 is re-anchored to 30:10 on the next
// pass, whatever the rows still show, so the band keeps the ratio the
// operator asked for and the decode side is finished at 10. A reference
// that matches the spec is carried as it is, even while the rows drift
// below it (a replica mid-repair), since the rows are not the operator's
// intent.
func TestBuildRatioState_ReferenceFollowsTheSpec(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	g := ratioGroup(eng, dec)

	resized := recordedReferenceISVC(map[v1beta1.ComponentType]int32{eng: 15, dec: 15})
	got := ratioReferenceOf(t, resized, g, replicaWithCounts(eng, 30, 15), replicaWithCounts(dec, 10, 15))
	if got[eng] != 30 || got[dec] != 10 {
		t.Errorf("the reference must follow the spec's new counts; got Original=%v want engine 30, decoder 10", got)
	}
	if len(got) != 2 {
		t.Errorf("the whole group is re-anchored at once; got Original=%v", got)
	}

	steady := recordedReferenceISVC(map[v1beta1.ComponentType]int32{eng: 4, dec: 2})
	got = ratioReferenceOf(t, steady, g, replicaWithCounts(eng, 4, 2), replicaWithCounts(dec, 2, 1))
	if got[eng] != 4 || got[dec] != 2 {
		t.Errorf("a reference equal to the spec is kept while the rows drift; got Original=%v want engine 4, decoder 2", got)
	}
}

// ratioMembershipISVC builds an ISVC whose group "0" carries a persisted
// {engine: 4, decoder: 2} reference plus any extra entries.
func ratioMembershipISVC(extra map[v1beta1.ComponentType]int32) *v1beta1.InferenceService {
	original := map[v1beta1.ComponentType]int32{
		v1beta1.EngineComponent:  4,
		v1beta1.DecoderComponent: 2,
	}
	for c, r := range extra {
		original[c] = r
	}
	return recordedReferenceISVC(original)
}

// TestBuildRatioState_LateMemberGainsAnchor: a Component added after the
// reference was recorded gains an entry from its spec, and the recorded
// members keep the counts their specs ask for, whatever their rows show.
func TestBuildRatioState_LateMemberGainsAnchor(t *testing.T) {
	g := ratioGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent)
	// The engine's rows drifted to 2 since the reference was taken; its
	// spec still asks for 4, and the spec is what the reference reads.
	got := ratioReferenceOf(t, ratioMembershipISVC(nil), g,
		replicaWithCounts(v1beta1.EngineComponent, 4, 2),
		replicaWithCounts(v1beta1.DecoderComponent, 2, 2),
		replicaWithCounts(v1beta1.RouterComponent, 3, 3))
	if got[v1beta1.RouterComponent] != 3 {
		t.Errorf("late member anchor: router Original got %d want 3 (the count its spec asks for)", got[v1beta1.RouterComponent])
	}
	if got[v1beta1.EngineComponent] != 4 {
		t.Errorf("engine Original got %d want 4 (its spec, not its drifted rows)", got[v1beta1.EngineComponent])
	}
	if got[v1beta1.DecoderComponent] != 2 {
		t.Errorf("decoder Original got %d want 2", got[v1beta1.DecoderComponent])
	}
}

// TestBuildRatioState_LateMemberWithoutAReplicaDeferred: a late member
// whose replica does not exist yet is not anchored on this pass, and the
// recorded entries are carried unchanged until it is.
func TestBuildRatioState_LateMemberWithoutAReplicaDeferred(t *testing.T) {
	g := ratioGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent)
	got := ratioReferenceOf(t, ratioMembershipISVC(nil), g,
		replicaWithCounts(v1beta1.EngineComponent, 4, 4),
		replicaWithCounts(v1beta1.DecoderComponent, 2, 2))
	if _, ok := got[v1beta1.RouterComponent]; ok {
		t.Errorf("a late member without a replica must be deferred, not anchored; got router Original=%d", got[v1beta1.RouterComponent])
	}
	if got[v1beta1.EngineComponent] != 4 || got[v1beta1.DecoderComponent] != 2 {
		t.Errorf("recorded entries must survive a deferred late member; got Original=%v", got)
	}
}

// TestBuildRatioState_RemovedMemberAnchorDropped: an entry for a Component
// no longer in the group is dropped; the remaining entries stay.
func TestBuildRatioState_RemovedMemberAnchorDropped(t *testing.T) {
	g := ratioGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent)
	got := ratioReferenceOf(t, ratioMembershipISVC(map[v1beta1.ComponentType]int32{v1beta1.RouterComponent: 3}), g,
		replicaWithCounts(v1beta1.EngineComponent, 4, 4),
		replicaWithCounts(v1beta1.DecoderComponent, 2, 2))
	if _, ok := got[v1beta1.RouterComponent]; ok {
		t.Errorf("removed member's entry must be dropped; got router Original=%d", got[v1beta1.RouterComponent])
	}
	if got[v1beta1.EngineComponent] != 4 || got[v1beta1.DecoderComponent] != 2 {
		t.Errorf("remaining entries must stay; got Original=%v", got)
	}
}

// TestBuildRatioState_AllAnchorsStaleFallsBackToSnapshot: when every
// recorded entry belongs to removed members, the current membership is
// anchored from its specs.
func TestBuildRatioState_AllAnchorsStaleFallsBackToSnapshot(t *testing.T) {
	g := ratioGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent)
	got := ratioReferenceOf(t, recordedReferenceISVC(map[v1beta1.ComponentType]int32{v1beta1.RouterComponent: 3}), g,
		replicaWithCounts(v1beta1.EngineComponent, 4, 4),
		replicaWithCounts(v1beta1.DecoderComponent, 2, 2))
	if got[v1beta1.EngineComponent] != 4 || got[v1beta1.DecoderComponent] != 2 {
		t.Errorf("an all-stale reference must anchor the current membership; got Original=%v", got)
	}
	if _, ok := got[v1beta1.RouterComponent]; ok {
		t.Errorf("a stale entry must not survive; got Original=%v", got)
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
	gated := func(p *corev1.Pod, gate corev1.ConditionStatus) *corev1.Pod {
		p.Status.Conditions = append(p.Status.Conditions, corev1.PodCondition{Type: podreadiness.ConditionType, Status: gate})
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
		{"running + ready + serving gate true", gated(mk(corev1.PodRunning, &condTrue, false), corev1.ConditionTrue), true},
		{"running + ready but held out of rotation by the serving gate", gated(mk(corev1.PodRunning, &condTrue, false), corev1.ConditionFalse), false},
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
	c := testClient(endpointsListed(isvc, pods...)...)
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

// TestReconcile_GCRetainsTargetRevisionServicesWithoutPods pins that the
// orphan sweep keeps a Component's roll-target revision Services alive
// before that revision has any pod: a peer Component creates them ahead of
// the pods whose per-revision peer endpoint names them. A genuinely stale
// revision (no pods, not the target) is still swept in the same pass.
func TestReconcile_GCRetainsTargetRevisionServicesWithoutPods(t *testing.T) {
	isvc := testOMENativeISVC()
	steadyEngineStatus(isvc, "hash1")
	engineIR := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: isvc.Name + "-engine", Namespace: isvc.Namespace},
		Status: v1beta1.InferenceReplicaStatus{
			CurrentRevision: isvc.Name + "-engine-hash1",
			UpdateRevision:  isvc.Name + "-engine-hash2",
		},
	}
	targetRouting := PerRevisionServiceName(isvc.Name, v1beta1.EngineComponent, "hash2")
	targetHeadless := PerRevisionHeadlessServiceName(isvc.Name, v1beta1.EngineComponent, "hash2")
	staleRouting := PerRevisionServiceName(isvc.Name, v1beta1.EngineComponent, "hash0")
	staleHeadless := PerRevisionHeadlessServiceName(isvc.Name, v1beta1.EngineComponent, "hash0")
	objs := []runtime.Object{
		engineIR,
		buildPod(isvc, v1beta1.EngineComponent, "hash1", 0),
		perRevisionServiceFixture(isvc, v1beta1.EngineComponent, "hash2", targetRouting),
		perRevisionServiceFixture(isvc, v1beta1.EngineComponent, "hash2", targetHeadless),
		perRevisionServiceFixture(isvc, v1beta1.EngineComponent, "hash0", staleRouting),
		perRevisionServiceFixture(isvc, v1beta1.EngineComponent, "hash0", staleHeadless),
	}
	c := testClient(objs...)
	if _, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(),
		ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent),
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, name := range []string{targetRouting, targetHeadless} {
		if err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: name}, &corev1.Service{}); err != nil {
			t.Errorf("target-revision Service %s must survive the sweep before its pods exist: %v", name, err)
		}
	}
	for _, name := range []string{staleRouting, staleHeadless} {
		err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: name}, &corev1.Service{})
		if !apierrors.IsNotFound(err) {
			t.Errorf("stale Service %s must still be swept, Get returned %v", name, err)
		}
	}
}

// A canary group's Components stay the canary engine's while the run that
// pinned the group is open, even after the group is edited out of the spec:
// the run continues on the pinned plan, and two writers on one traffic field
// is the hazard this guards.
func TestReconcile_PinnedCanaryGroupKeepsOwnership(t *testing.T) {
	isvc := testOMENativeISVC()
	group := v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("50%"), Traffic: 10},
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		}},
	}
	// The spec does not carry the group; the open run still pins it.
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{}
	isvc.Status.Rollout = &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{
		RunID: "run-1",
		Plan:  v1beta1.RolloutRunPlan{Groups: []v1beta1.RolloutRunGroup{{Source: v1beta1.RolloutPlanSourceInline, Group: group}}},
	}}
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {Traffic: []v1beta1.ComponentTrafficTarget{
			{RevisionName: "llama-engine-rev-canaryhash", Percent: 10},
			{RevisionName: "llama-engine-rev-stablehash", Percent: 90},
		}},
	}
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
	for _, target := range isvc.Status.Components[v1beta1.EngineComponent].Traffic {
		var want int32
		switch target.RevisionName {
		case "llama-engine-rev-canaryhash":
			want = 10
		case "llama-engine-rev-stablehash":
			want = 90
		default:
			t.Fatalf("coordination wrote traffic for a pinned canary group: %q", target.RevisionName)
		}
		if target.Percent != want {
			t.Errorf("%s: got %d%% want %d%% — coordination overwrote the pinned canary's step weight", target.RevisionName, target.Percent, want)
		}
	}
}

// A canary group's secondary is the canary engine's as much as its primary:
// coordination writes neither's traffic, whatever the pod proportions say, so
// the step weight the canary engine published on the secondary stands.
func TestReconcile_LeavesCanarySecondaryTrafficToTheCanaryEngine(t *testing.T) {
	isvc := testOMENativeISVC()
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("50%"), Traffic: 10},
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		}},
	}}}
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.DecoderComponent: {Traffic: []v1beta1.ComponentTrafficTarget{
			{RevisionName: "llama-decoder-rev-canaryhash", Percent: 10},
			{RevisionName: "llama-decoder-rev-stablehash", Percent: 90},
		}},
	}
	pods := []runtime.Object{
		buildPod(isvc, v1beta1.DecoderComponent, "canaryhash", 0),
		buildPod(isvc, v1beta1.DecoderComponent, "stablehash", 1),
	}
	c := testClient(pods...)
	if _, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(),
		ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent, v1beta1.DecoderComponent),
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := isvc.Status.Components[v1beta1.DecoderComponent].Traffic
	if len(got) != 2 {
		t.Fatalf("coordination rewrote a canary secondary's traffic: %+v", got)
	}
	for _, target := range got {
		var want int32
		switch target.RevisionName {
		case "llama-decoder-rev-canaryhash":
			want = 10
		case "llama-decoder-rev-stablehash":
			want = 90
		default:
			t.Fatalf("coordination wrote traffic for a canary secondary: %q", target.RevisionName)
		}
		if target.Percent != want {
			t.Errorf("%s: got %d%% want %d%% — coordination overwrote the canary secondary's step weight", target.RevisionName, target.Percent, want)
		}
	}
}

// buildGangPods returns the leader and worker pods of one multi-pod Instance
// on one revision, Running and PodReady, with the controller's serving gate
// at the given status. A gate held False while PodReady still reads True is
// the drain window: the controller has taken the Instance out of rotation
// and the kubelet has not re-evaluated PodReady yet.
func buildGangPods(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, revisionHash string, instance int, gate corev1.ConditionStatus) []runtime.Object {
	var out []runtime.Object
	for ordinal, runner := range []v1beta1.RunnerName{v1beta1.RunnerNameLeader, v1beta1.RunnerNameWorker, v1beta1.RunnerNameWorker} {
		pod := buildRunnerPod(isvc, component, revisionHash, instance*10+ordinal, runner, strconv.Itoa(ordinal))
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: podreadiness.ConditionType, Status: gate})
		out = append(out, pod)
	}
	return out
}

// TestReconcile_TrafficDropsRevisionHeldOutOfRotation pins the end state of a
// pairing-protocol adoption on a multi-pod P/D Component: once the last
// Instance of the pre-adoption revision is held out of rotation, the traffic
// surface names only the adopted revision, with its protocol, and the routing
// Service it names carries the protocol label. The held gang still reads
// PodReady=True (the kubelet re-evaluates the gate asynchronously), and the
// pass that observes the hold is the one the InferenceReplica status write
// for it enqueues, so the traffic surface must follow the controller's own
// rotation decision rather than wait for the kubelet.
func TestReconcile_TrafficDropsRevisionHeldOutOfRotation(t *testing.T) {
	isvc := testOMENativeISVC()
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
		},
	}
	const (
		oldHash, newHash = "b4732dc5", "8c1d2e3f"
		protocol         = "pd-v2"
	)
	decoder := v1beta1.DecoderComponent
	oldRevision := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{
		Name: isvc.Name + "-decoder-" + oldHash, Namespace: isvc.Namespace,
	}}
	newRevision := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{
		Name: isvc.Name + "-decoder-" + newHash, Namespace: isvc.Namespace,
		Annotations: map[string]string{query.LabelPairingProtocol: protocol},
	}}
	decoderIR := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: isvc.Name + "-decoder", Namespace: isvc.Namespace},
		Status:     v1beta1.InferenceReplicaStatus{UpdateRevision: newRevision.Name},
	}
	// The adopted revision serves; the pre-adoption gang is held out of
	// rotation but not yet re-evaluated by the kubelet.
	objs := []runtime.Object{oldRevision, newRevision, decoderIR}
	objs = append(objs, buildGangPods(isvc, decoder, newHash, 0, corev1.ConditionTrue)...)
	objs = append(objs, buildGangPods(isvc, decoder, oldHash, 1, corev1.ConditionFalse)...)
	c := testClient(endpointsListed(isvc, objs...)...)
	if _, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(), ComponentRunnerPorts: testComponentRunnerPorts(),
		ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent, decoder),
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	traffic := isvc.Status.Components[decoder].Traffic
	if len(traffic) != 1 {
		t.Fatalf("a revision held out of rotation must not stay a live traffic target; got %+v", traffic)
	}
	want := PerRevisionServiceName(isvc.Name, decoder, newHash)
	if traffic[0].RevisionName != want || traffic[0].Percent != 100 || traffic[0].PairingProtocol != protocol {
		t.Errorf("live traffic must be the adopted revision at 100%% with its protocol: got %+v want %s@100 %q", traffic[0], want, protocol)
	}
	svc := &corev1.Service{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: want}, svc); err != nil {
		t.Fatalf("routing Service named by live traffic: %v", err)
	}
	if got := svc.Labels[query.LabelPairingProtocol]; got != protocol {
		t.Errorf("routing Service %s pairing label: got %q want %q", want, got, protocol)
	}
}

// Referenced roles' pods, per-revision Services, traffic entries and orphan
// sweep are keyed on each replica's own prefix while a projected role
// beside them stays on the service name; the service owns every role's
// Services.
func TestReconcile_ReferencedRoleKeysPerRevisionObjectsOnTheReplicaPrefix(t *testing.T) {
	isvc := testOMENativeISVC()
	isvc.Spec.Router = &v1beta1.RouterSpec{}
	isvc.Spec.ReplicaRefs = &v1beta1.ReplicaRefs{Engine: []string{"pool-a"}, Decoder: []string{"pool-d"}}
	referencedPod := func(prefix string, c v1beta1.ComponentType) *corev1.Pod {
		pod := buildPod(isvc, c, "hash1", 0).(*corev1.Pod)
		pod.Name = podName(prefix, c, 0)
		pod.Labels[constants.InferenceServicePodLabelKey] = prefix
		return pod
	}
	stale := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pool-d-decoder-rev-old",
			Namespace: isvc.Namespace,
			Labels:    perRevisionServiceSelector("pool-d", v1beta1.DecoderComponent, "old"),
		},
		Spec: corev1.ServiceSpec{Selector: perRevisionServiceSelector("pool-d", v1beta1.DecoderComponent, "old")},
	}
	c := testClient(endpointsListed(isvc, referencedPod("pool-a", v1beta1.EngineComponent), referencedPod("pool-d", v1beta1.DecoderComponent), buildPod(isvc, v1beta1.RouterComponent, "hash1", 0), stale)...)
	if _, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(),
		ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent),
		ComponentRunnerPorts:     testComponentRunnerPorts(),
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, name := range []string{"pool-a-engine-rev-hash1", "pool-d-decoder-rev-hash1", "pool-d-decoder-rev-hash1-headless", "llama-router-rev-hash1"} {
		svc := &corev1.Service{}
		if err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: name}, svc); err != nil {
			t.Fatalf("Service %s must exist: %v", name, err)
		}
		checkOwnerRef(t, svc, isvc)
	}
	decoderSvc := &corev1.Service{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: "pool-d-decoder-rev-hash1"}, decoderSvc); err != nil {
		t.Fatal(err)
	}
	checkSelector(t, decoderSvc.Spec.Selector, "pool-d", "decoder", "hash1")
	for _, name := range []string{"llama-engine-rev-hash1", "llama-decoder-rev-hash1"} {
		if err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: name}, &corev1.Service{}); !apierrors.IsNotFound(err) {
			t.Errorf("no per-revision Service may be named after the service for a referenced role (%s): err=%v", name, err)
		}
	}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: stale.Name}, &corev1.Service{}); !apierrors.IsNotFound(err) {
		t.Errorf("the orphan sweep must collect the referenced role's stale Service under its prefix: err=%v", err)
	}

	decoder := isvc.Status.Components[v1beta1.DecoderComponent]
	if len(decoder.Traffic) != 1 || decoder.Traffic[0].RevisionName != "pool-d-decoder-rev-hash1" {
		t.Errorf("decoder Traffic must name the replica-prefixed Service: %+v", decoder.Traffic)
	}
	if decoder.LatestRolledoutRevision != "pool-d-decoder-rev-hash1" {
		t.Errorf("decoder LatestRolledoutRevision: got %q want pool-d-decoder-rev-hash1", decoder.LatestRolledoutRevision)
	}
	router := isvc.Status.Components[v1beta1.RouterComponent]
	if len(router.Traffic) != 1 || router.Traffic[0].RevisionName != "llama-router-rev-hash1" {
		t.Errorf("router Traffic must keep the service-named Service: %+v", router.Traffic)
	}
}

// rollingEngineIR is the engine's replica mid-roll: one Instance still on
// revision aaaa, the update revision bbbb recorded and no pod on it yet.
func rollingEngineIR(isvc *v1beta1.InferenceService) *v1beta1.InferenceReplica {
	prior := isvc.Name + "-engine-aaaa"
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: irprojector.InferenceReplicaName(isvc.Name, v1beta1.EngineComponent), Namespace: isvc.Namespace},
		Status: v1beta1.InferenceReplicaStatus{
			Replicas: 1, ReadyReplicas: 1, ServingReplicas: 1, AvailableReplicas: 1,
			CurrentRevision: prior, UpdateRevision: isvc.Name + "-engine-bbbb",
			InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: prior}},
		},
	}
}

func groupPhaseOf(isvc *v1beta1.InferenceService) v1beta1.CoordinationPhase {
	if isvc.Status.RolloutCoordination == nil || len(isvc.Status.RolloutCoordination.Groups) == 0 {
		return ""
	}
	return isvc.Status.RolloutCoordination.Groups[0].Phase
}

// A member whose InferenceReplica is gone reads as a zero observation, not
// rolling and not failed, on the pass that first misses it; the pass before
// wrote the phase its own read computed.
func TestReconcile_MissingReplicaReadsIdleOnTheNextPass(t *testing.T) {
	ctx := context.Background()
	isvc := testOMENativeISVC()
	isvc.Spec.Rollout = singleEngineGroup()
	ir := rollingEngineIR(isvc)
	c := testClient(ir, buildPod(isvc, v1beta1.EngineComponent, "aaaa", 0))
	in := ReconcileInputs{ISVC: isvc, Client: c, Reader: c, Now: time.Now(), ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent)}
	if _, err := Reconcile(ctx, in); err != nil {
		t.Fatalf("first pass: %v", err)
	}
	if got := groupPhaseOf(isvc); got != v1beta1.CoordinationPhaseSurging {
		t.Fatalf("a rolling member with no new pod reads Surging; got %q", got)
	}
	if err := c.Delete(ctx, ir); err != nil {
		t.Fatalf("delete replica: %v", err)
	}
	if _, err := Reconcile(ctx, in); err != nil {
		t.Fatalf("second pass: %v", err)
	}
	if got := groupPhaseOf(isvc); got != v1beta1.CoordinationPhaseIdle {
		t.Fatalf("a member with no replica is not rolling, so the group reads Idle; got %q", got)
	}
	cond := isvc.Status.GetCondition(apis.ConditionType(v1beta1.RolloutCoordinationReady))
	if cond == nil || cond.Status != corev1.ConditionTrue {
		t.Fatalf("RolloutCoordinationReady should read True over a zero observation; got %+v", cond)
	}
}

// The counters the phase is computed from are the replica's published ones;
// the pod list only feeds the total. A replica whose counters trail the pods
// the pass lists reads the phase the counters say for that pass.
func TestBuildComponentObservation_CountersTrailingThePodsReadTheCounters(t *testing.T) {
	const prior, target = "svc-engine-aaaa", "svc-engine-bbbb"
	summary := &v1beta1.InferenceReplicaStatus{
		Replicas: 2, ReadyReplicas: 2, ServingReplicas: 2,
		CurrentRevision: prior, UpdateRevision: target,
		InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: prior},
			{Index: 1, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: prior},
		},
	}
	// The cache lists a target pod the replica has not counted yet.
	pods := map[string]int32{"aaaa": 2, "bbbb": 1}
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Policy: v1beta1.CoordinationPolicyBlueGreen}
	obs := GroupObservation{Group: g, Components: map[v1beta1.ComponentType]ComponentObservation{
		v1beta1.EngineComponent: buildComponentObservation(summary, v1beta1.EngineComponent, pods, 0),
	}}
	if got := obs.Components[v1beta1.EngineComponent]; got.TotalPods != 3 || got.NewRevisionPods != 0 {
		t.Fatalf("the total comes from the pods and the new-revision count from the counters; got %+v", got)
	}
	if tr := ComputeTransition(obs); tr.Phase != v1beta1.CoordinationPhaseSurging {
		t.Fatalf("trailing counters read Surging although a target pod is listed; got %q", tr.Phase)
	}
	// Once the replica publishes the pod, the same pods read Waiting.
	summary.UpdatedReplicas = 1
	obs.Components[v1beta1.EngineComponent] = buildComponentObservation(summary, v1beta1.EngineComponent, pods, 0)
	if tr := ComputeTransition(obs); tr.Phase != v1beta1.CoordinationPhaseWaiting {
		t.Fatalf("fresh counters read Waiting; got %q", tr.Phase)
	}
}

// A copy of the service with no anchor snapshots one from the desired counts
// the pass sees, whatever anchor the live object carries.
func TestBuildRatioState_CopyWithoutAnchorSnapshotsTodaysReplicas(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Policy:     v1beta1.CoordinationPolicyBlueGreen,
		Pacing:     v1beta1.CoordinationPacing{Type: v1beta1.CoordinationPacingRatioBalanced},
	}
	// The pass copy predates the flush that wrote the anchor.
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "isvc", Namespace: "default"}}
	obs := GroupObservation{Group: g, Components: map[v1beta1.ComponentType]ComponentObservation{
		v1beta1.EngineComponent:  {Component: v1beta1.EngineComponent, DesiredReplicas: 3, SpecReplicas: 3},
		v1beta1.DecoderComponent: {Component: v1beta1.DecoderComponent, DesiredReplicas: 1, SpecReplicas: 1},
	}}
	state := buildRatioState(isvc, g, obs, map[v1beta1.ComponentType]map[string]int32{})
	if state.Original[v1beta1.EngineComponent] != 3 || state.Original[v1beta1.DecoderComponent] != 1 {
		t.Fatalf("a copy without an anchor snapshots today's desired counts; got %v", state.Original)
	}
}

// refusingServiceClient fails every Service create with err and passes
// everything else through.
func refusingServiceClient(err error, objs ...runtime.Object) client.Client {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, appsv1.AddToScheme, v1beta1.AddToScheme} {
		if addErr := add(scheme); addErr != nil {
			panic(addErr)
		}
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).WithInterceptorFuncs(interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*corev1.Service); ok {
				return err
			}
			return cl.Create(ctx, obj, opts...)
		},
	}).Build()
}

// seededGroupEntry is a group entry a prior pass left behind, at a phase the
// current facts do not compute, so an untouched entry is observable.
func seededGroupEntry(isvc *v1beta1.InferenceService, then time.Time) {
	at := metav1.NewTime(then)
	isvc.Status.RolloutCoordination = &v1beta1.RolloutCoordinationStatus{Groups: []v1beta1.RolloutCoordinationGroupStatus{{
		Name: "0", Policy: v1beta1.CoordinationPolicyBlueGreen, Phase: v1beta1.CoordinationPhaseShifting,
		CompositePhase: string(v1beta1.CoordinationPhaseShifting), LastTransitionTime: &at,
	}}}
}

// A per-revision Service write that fails before the group is evaluated
// fails the pass and leaves the group entry as the previous pass wrote it,
// whatever phase the members would compute.
func TestReconcile_ServiceWriteFailureLeavesTheGroupEntry(t *testing.T) {
	isvc := testOMENativeISVC()
	isvc.Spec.Rollout = singleEngineGroup()
	then := time.Now().Add(-time.Hour)
	seededGroupEntry(isvc, then)
	c := refusingServiceClient(apierrors.NewInternalError(errors.New("etcdserver: request timed out")),
		rollingEngineIR(isvc), buildPod(isvc, v1beta1.EngineComponent, "aaaa", 0))
	_, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(), ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent),
	})
	if err == nil {
		t.Fatal("a failed Service write must fail the pass so it is retried")
	}
	g := isvc.Status.RolloutCoordination.Groups[0]
	if g.Phase != v1beta1.CoordinationPhaseShifting || !g.LastTransitionTime.Time.Equal(then) {
		t.Fatalf("the group entry must be untouched by a pass that failed before evaluating it; got %+v", g)
	}
}

// A per-revision Service create the apiserver refuses because the namespace
// is terminating ends the pass cleanly without evaluating the group: the
// entry stays as the previous pass wrote it and no error is returned.
func TestReconcile_NamespaceTerminatingStopsBeforeTheGroup(t *testing.T) {
	isvc := testOMENativeISVC()
	isvc.Spec.Rollout = singleEngineGroup()
	then := time.Now().Add(-time.Hour)
	seededGroupEntry(isvc, then)
	refusal := apierrors.NewForbidden(corev1.Resource("services"), "svc", errors.New("namespace is being terminated"))
	refusal.ErrStatus.Details.Causes = append(refusal.ErrStatus.Details.Causes, metav1.StatusCause{Type: corev1.NamespaceTerminatingCause})
	c := refusingServiceClient(refusal, rollingEngineIR(isvc), buildPod(isvc, v1beta1.EngineComponent, "aaaa", 0))
	result, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(), ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent),
	})
	if err != nil || result == nil {
		t.Fatalf("a terminating namespace ends the pass cleanly; got result=%v err=%v", result, err)
	}
	g := isvc.Status.RolloutCoordination.Groups[0]
	if g.Phase != v1beta1.CoordinationPhaseShifting || !g.LastTransitionTime.Time.Equal(then) {
		t.Fatalf("the group entry must be untouched by a pass that stopped before evaluating it; got %+v", g)
	}
}

// Both accepted values of the pause annotation read as the global pause, so
// a change between them is invisible to the group.
func TestBuildGroupObservation_EitherPauseValueReadsPaused(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Policy: v1beta1.CoordinationPolicyBlueGreen}
	for _, value := range []string{"true", constants.PausedRolloutFreezeValue} {
		isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
			Name: "isvc", Namespace: "default", Annotations: map[string]string{constants.PausedRolloutAnnotation: value},
		}}
		obs, err := buildGroupObservation(context.Background(), testClient(), isvc, g, map[v1beta1.ComponentType]map[string]int32{})
		if err != nil {
			t.Fatalf("%s: %v", value, err)
		}
		if !obs.PausedGlobal {
			t.Fatalf("%s: the pause must read as the global pause", value)
		}
		if tr := ComputeTransition(obs); tr.Phase != v1beta1.CoordinationPhasePaused {
			t.Fatalf("%s: got %q want Paused", value, tr.Phase)
		}
	}
}

// Each pass writes the phase its own read of the replica and the pods
// computed; a fact that lands after that read is observed by the next pass,
// which recomputes from it. The walk below changes the cluster between
// passes only, and every phase written is the one the facts before the pass
// compute.
func TestReconcile_EachPassWritesThePhaseItsOwnReadComputed(t *testing.T) {
	ctx := context.Background()
	isvc := testOMENativeISVC()
	isvc.Spec.Rollout = singleEngineGroup()
	prior, target := isvc.Name+"-engine-aaaa", isvc.Name+"-engine-bbbb"
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: irprojector.InferenceReplicaName(isvc.Name, v1beta1.EngineComponent), Namespace: isvc.Namespace},
		Status: v1beta1.InferenceReplicaStatus{
			Replicas: 1, ReadyReplicas: 1, ServingReplicas: 1, UpdatedReplicas: 1, UpdatedReadyReplicas: 1,
			CurrentRevision: prior, UpdateRevision: prior,
			InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: prior}},
		},
	}
	oldPod := buildPod(isvc, v1beta1.EngineComponent, "aaaa", 0).(*corev1.Pod)
	newPod := buildReadyPod(isvc, v1beta1.EngineComponent, "bbbb", 1, false).(*corev1.Pod)
	c := testClient(ir, oldPod)
	in := ReconcileInputs{ISVC: isvc, Client: c, Reader: c, Now: time.Now(), ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent)}
	publish := func(edit func(*v1beta1.InferenceReplicaStatus)) {
		t.Helper()
		live := &v1beta1.InferenceReplica{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(ir), live); err != nil {
			t.Fatalf("read replica: %v", err)
		}
		edit(&live.Status)
		if err := c.Update(ctx, live); err != nil {
			t.Fatalf("publish replica: %v", err)
		}
	}
	pass := func(step string, want v1beta1.CoordinationPhase) {
		t.Helper()
		if _, err := Reconcile(ctx, in); err != nil {
			t.Fatalf("%s: %v", step, err)
		}
		if got := groupPhaseOf(isvc); got != want {
			t.Fatalf("%s: phase got %q want %q", step, got, want)
		}
	}
	pass("settled", v1beta1.CoordinationPhaseIdle)

	// The bump lands after the Idle write; the next pass reads Surging.
	publish(func(s *v1beta1.InferenceReplicaStatus) {
		s.UpdateRevision = target
		s.UpdatedReplicas, s.UpdatedReadyReplicas = 0, 0
	})
	pass("bumped", v1beta1.CoordinationPhaseSurging)

	// A row counted on the target, its pod not Ready, lands after the Surging
	// write; the next pass reads Waiting.
	if err := c.Create(ctx, newPod); err != nil {
		t.Fatalf("create target pod: %v", err)
	}
	publish(func(s *v1beta1.InferenceReplicaStatus) { s.UpdatedReplicas = 1 })
	pass("surged", v1beta1.CoordinationPhaseWaiting)

	// The target pod turns Ready while the old one still serves; Shifting.
	if err := c.Get(ctx, client.ObjectKeyFromObject(newPod), newPod); err != nil {
		t.Fatalf("read target pod: %v", err)
	}
	newPod.Status = corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}
	if err := c.Update(ctx, newPod); err != nil {
		t.Fatalf("ready target pod: %v", err)
	}
	publish(func(s *v1beta1.InferenceReplicaStatus) { s.UpdatedReadyReplicas = 1 })
	pass("ready on target", v1beta1.CoordinationPhaseShifting)

	// The old pod leaves; ScalingDown until the replica promotes.
	if err := c.Delete(ctx, oldPod); err != nil {
		t.Fatalf("delete old pod: %v", err)
	}
	pass("old pods gone", v1beta1.CoordinationPhaseScalingDown)

	publish(func(s *v1beta1.InferenceReplicaStatus) { s.CurrentRevision = target })
	pass("converged", v1beta1.CoordinationPhaseIdle)

	// A Failed row lands after the Idle write; the next pass reads Failed,
	// and the pass after the row recovers reads Idle again.
	publish(func(s *v1beta1.InferenceReplicaStatus) { s.InstanceStatuses[0].Phase = v1beta1.OMENativeInstanceFailed })
	pass("row failed", v1beta1.CoordinationPhaseFailed)
	publish(func(s *v1beta1.InferenceReplicaStatus) { s.InstanceStatuses[0].Phase = v1beta1.OMENativeInstanceReady })
	pass("row recovered", v1beta1.CoordinationPhaseIdle)

	// The pause annotation is read on the pass after it is set, and its
	// removal on the pass after that.
	isvc.Annotations = map[string]string{constants.PausedRolloutAnnotation: "true"}
	pass("paused", v1beta1.CoordinationPhasePaused)
	delete(isvc.Annotations, constants.PausedRolloutAnnotation)
	pass("unpaused", v1beta1.CoordinationPhaseIdle)
}

// recordingServiceClient appends the name of every Service create to created,
// in the order the pass issued it, and passes every call through.
func recordingServiceClient(created *[]string, objs ...runtime.Object) client.Client {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, appsv1.AddToScheme, discoveryv1.AddToScheme, v1beta1.AddToScheme} {
		if addErr := add(scheme); addErr != nil {
			panic(addErr)
		}
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).WithInterceptorFuncs(interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if svc, ok := obj.(*corev1.Service); ok {
				*created = append(*created, svc.Name)
			}
			return cl.Create(ctx, obj, opts...)
		},
	}).Build()
}

// A pass that first sees a member on two revisions writes the two revisions'
// Service pairs in ascending revision order, so every pass issues the same
// create sequence and a create that fails always leaves the same revision
// waiting for the next pass. The pass is repeated because a pass that picked
// the order at random would still produce the sorted one about half the time.
func TestReconcile_PerRevisionServicesAreWrittenInSortedRevisionOrder(t *testing.T) {
	ctx := context.Background()
	const passes = 32
	want := strings.Join([]string{
		PerRevisionServiceName("llama", v1beta1.EngineComponent, "aaaa"),
		PerRevisionHeadlessServiceName("llama", v1beta1.EngineComponent, "aaaa"),
		PerRevisionServiceName("llama", v1beta1.EngineComponent, "bbbb"),
		PerRevisionHeadlessServiceName("llama", v1beta1.EngineComponent, "bbbb"),
	}, " ")
	orders := map[string]int{}
	for i := 0; i < passes; i++ {
		isvc := testOMENativeISVC()
		isvc.Spec.Rollout = singleEngineGroup()
		var created []string
		c := recordingServiceClient(&created, endpointsListed(isvc, rollingEngineIR(isvc),
			buildPod(isvc, v1beta1.EngineComponent, "aaaa", 0),
			buildPod(isvc, v1beta1.EngineComponent, "bbbb", 1))...)
		if _, err := Reconcile(ctx, ReconcileInputs{
			ISVC: isvc, Client: c, Reader: c, Now: time.Now(),
			ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent),
			ComponentRunnerPorts:     testComponentRunnerPorts(),
		}); err != nil {
			t.Fatalf("pass %d: %v", i, err)
		}
		orders[strings.Join(created, " ")]++
	}
	if len(orders) != 1 || orders[want] != passes {
		t.Fatalf("every pass must create the per-revision Services in ascending revision order\nwant %d x %q\ngot  %v", passes, want, orders)
	}
}

// SortedRevisionHashes returns the revision hashes of a per-revision pod-count
// map in ascending order, and nothing for an empty map.
func TestSortedRevisionHashes(t *testing.T) {
	if got := SortedRevisionHashes(map[string]int32{"cccc": 1, "aaaa": 3, "bbbb": 0}); !reflect.DeepEqual(got, []string{"aaaa", "bbbb", "cccc"}) {
		t.Fatalf("got %v want [aaaa bbbb cccc]", got)
	}
	if got := SortedRevisionHashes(nil); len(got) != 0 {
		t.Fatalf("empty map: got %v want nothing", got)
	}
}

// gangMemberPod builds one pod of a gang Instance: its runner, the ordinal
// 0 of that runner, the Instance index, the serving gate True and PodReady
// as serving says. The pod name carries the Instance and the runner's
// place in it so two members never collide.
func gangMemberPod(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, revisionHash string, instance int, runner v1beta1.RunnerName, serving bool) *corev1.Pod {
	place := 0
	if runner == v1beta1.RunnerNameWorker {
		place = 1
	}
	pod := buildRunnerPod(isvc, component, revisionHash, instance*10+place, runner, "0")
	pod.Labels[query.LabelInstanceIdx] = strconv.Itoa(instance)
	ready := corev1.ConditionTrue
	if !serving {
		ready = corev1.ConditionFalse
	}
	pod.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.PodReady, Status: ready},
		{Type: podreadiness.ConditionType, Status: corev1.ConditionTrue},
	}
	return pod
}

// setPodReady flips one pod's PodReady condition in the client, the way the
// node lifecycle controller or the kubelet does when a pod leaves or
// re-enters rotation.
func setPodReady(t *testing.T, c client.Client, name types.NamespacedName, ready bool) {
	t.Helper()
	pod := &corev1.Pod{}
	if err := c.Get(context.Background(), name, pod); err != nil {
		t.Fatalf("get pod %s: %v", name.Name, err)
	}
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == corev1.PodReady {
			pod.Status.Conditions[i].Status = status
		}
	}
	if err := c.Status().Update(context.Background(), pod); err != nil {
		t.Fatalf("update pod %s: %v", name.Name, err)
	}
}

// expectTrafficSplit checks one Component's targets by revision hash and
// percent: exactly the revisions in want, each at its share.
func expectTrafficSplit(t *testing.T, step string, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, want map[string]int32) {
	t.Helper()
	got := isvc.Status.Components[component].Traffic
	if len(got) != len(want) {
		t.Fatalf("%s: traffic = %+v, want shares on %v", step, got, want)
	}
	for _, target := range got {
		hash := query.RevisionFromName(target.RevisionName).Hash()
		percent, ok := want[hash]
		if !ok {
			t.Fatalf("%s: traffic names %q, want only %v: %+v", step, hash, want, got)
		}
		if target.Percent != percent {
			t.Fatalf("%s: %s carries %d%%, want %d%%", step, hash, target.Percent, percent)
		}
	}
}

// TestObservePerRevisionPods_GangServesThroughItsLeader: a routing Service
// publishes a gang's leader and never a worker, so an Instance serves
// through its leader. A gang whose leader left readiness counts no serving
// pod however its worker reads, a whole gang counts every member, and a
// gang with a worker out counts the rest: the count the Instance rows
// publish.
func TestObservePerRevisionPods_GangServesThroughItsLeader(t *testing.T) {
	isvc := testReconcilerISVC()
	eng := v1beta1.EngineComponent
	objs := []runtime.Object{
		// Instance 0 on old: the leader is out, the worker still passes readiness.
		gangMemberPod(isvc, eng, "old", 0, v1beta1.RunnerNameLeader, false),
		gangMemberPod(isvc, eng, "old", 0, v1beta1.RunnerNameWorker, true),
		// Instance 1 on new: the whole gang serves.
		gangMemberPod(isvc, eng, "new", 1, v1beta1.RunnerNameLeader, true),
		gangMemberPod(isvc, eng, "new", 1, v1beta1.RunnerNameWorker, true),
		// Instance 2 on new: the worker is out, the leader serves.
		gangMemberPod(isvc, eng, "new", 2, v1beta1.RunnerNameLeader, true),
		gangMemberPod(isvc, eng, "new", 2, v1beta1.RunnerNameWorker, false),
	}
	total, ready, _, _, err := observePerRevisionPods(context.Background(), testClient(objs...), isvc, []v1beta1.ComponentType{eng}, true)
	if err != nil {
		t.Fatalf("observePerRevisionPods: %v", err)
	}
	if total[eng]["old"] != 2 || total[eng]["new"] != 4 {
		t.Errorf("total = %v, want old 2 and new 4 (every pod counts)", total[eng])
	}
	if n, present := ready[eng]["old"]; present || n != 0 {
		t.Errorf("ready[old] = %d (present %v), want no entry: a gang whose leader is out serves nothing", n, present)
	}
	if got := ready[eng]["new"]; got != 3 {
		t.Errorf("ready[new] = %d, want 3: a whole gang of 2 and a gang serving through its leader alone", got)
	}
}

// TestReconcile_TrafficFollowsTheServingGangs: two gang Instances on two
// revisions share the traffic by their serving pods. While the old gang's
// leader is out of rotation, its worker still Ready, the old revision has
// no serving pod and the whole share rests on the new revision; the share
// returns on the pass that sees the leader serve again. With no serving
// pod on either revision there is nothing to move the share onto and the
// split is left as it was.
func TestReconcile_TrafficFollowsTheServingGangs(t *testing.T) {
	isvc := testOMENativeISVC()
	eng := v1beta1.EngineComponent
	engineIR := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: isvc.Name + "-engine", Namespace: isvc.Namespace},
		Status:     v1beta1.InferenceReplicaStatus{UpdateRevision: isvc.Name + "-engine-new"},
	}
	oldLeader := gangMemberPod(isvc, eng, "old", 0, v1beta1.RunnerNameLeader, true)
	newLeader := gangMemberPod(isvc, eng, "new", 1, v1beta1.RunnerNameLeader, true)
	c := testClient(endpointsListed(isvc, engineIR,
		oldLeader, gangMemberPod(isvc, eng, "old", 0, v1beta1.RunnerNameWorker, true),
		newLeader, gangMemberPod(isvc, eng, "new", 1, v1beta1.RunnerNameWorker, true))...)
	reconcile := func(step string) {
		t.Helper()
		if _, err := Reconcile(context.Background(), ReconcileInputs{
			ISVC: isvc, Client: c, Reader: c, Now: time.Now(), ComponentRunnerPorts: testComponentRunnerPorts(),
			ComponentDeploymentModes: testOMENativeModes(eng),
		}); err != nil {
			t.Fatalf("%s: %v", step, err)
		}
	}
	oldLeaderKey := types.NamespacedName{Namespace: oldLeader.Namespace, Name: oldLeader.Name}
	newLeaderKey := types.NamespacedName{Namespace: newLeader.Namespace, Name: newLeader.Name}

	reconcile("both gangs serve")
	expectTrafficSplit(t, "both gangs serve", isvc, eng, map[string]int32{"old": 50, "new": 50})

	setPodReady(t, c, oldLeaderKey, false)
	reconcile("the old gang's leader is out")
	expectTrafficSplit(t, "the old gang's leader is out", isvc, eng, map[string]int32{"new": 100})

	setPodReady(t, c, oldLeaderKey, true)
	reconcile("the old gang's leader serves again")
	expectTrafficSplit(t, "the old gang's leader serves again", isvc, eng, map[string]int32{"old": 50, "new": 50})

	setPodReady(t, c, oldLeaderKey, false)
	setPodReady(t, c, newLeaderKey, false)
	reconcile("no leader serves")
	expectTrafficSplit(t, "no leader serves", isvc, eng, map[string]int32{"old": 50, "new": 50})
}

// TestReconcile_TrafficStaysOnTheOnlyRevisionWhileItsGangIsDark: a
// Component running one revision keeps its whole share on that revision
// while its gang's leader is out; no other revision could carry it.
func TestReconcile_TrafficStaysOnTheOnlyRevisionWhileItsGangIsDark(t *testing.T) {
	isvc := testOMENativeISVC()
	eng := v1beta1.EngineComponent
	leader := gangMemberPod(isvc, eng, "only", 0, v1beta1.RunnerNameLeader, true)
	c := testClient(endpointsListed(isvc, leader, gangMemberPod(isvc, eng, "only", 0, v1beta1.RunnerNameWorker, true))...)
	reconcile := func(step string) {
		t.Helper()
		if _, err := Reconcile(context.Background(), ReconcileInputs{
			ISVC: isvc, Client: c, Reader: c, Now: time.Now(), ComponentRunnerPorts: testComponentRunnerPorts(),
			ComponentDeploymentModes: testOMENativeModes(eng),
		}); err != nil {
			t.Fatalf("%s: %v", step, err)
		}
	}
	reconcile("the gang serves")
	expectTrafficSplit(t, "the gang serves", isvc, eng, map[string]int32{"only": 100})

	setPodReady(t, c, types.NamespacedName{Namespace: leader.Namespace, Name: leader.Name}, false)
	reconcile("the leader is out")
	expectTrafficSplit(t, "the leader is out", isvc, eng, map[string]int32{"only": 100})
}

// A refusal the pass meets before it evaluates the group leaves the group
// entry exactly as the previous pass wrote it, whatever phase that was. A
// replica that cannot be read and a per-revision Service write the
// apiserver refuses fail the pass for a retry; a create refused because the
// namespace is terminating ends it cleanly. Each case seeds the entry at
// one phase and reads the entry back after the pass.
func TestReconcile_ARefusalBeforeTheGroupLeavesItsEntryInEveryPhase(t *testing.T) {
	then := metav1.NewTime(time.Now().Add(-time.Hour))
	entry := func(phase v1beta1.CoordinationPhase) v1beta1.RolloutCoordinationGroupStatus {
		return v1beta1.RolloutCoordinationGroupStatus{
			Name: "0", Policy: v1beta1.CoordinationPolicyBlueGreen, Phase: phase,
			CompositePhase: string(phase), LastTransitionTime: &then,
		}
	}
	awaiting := v1beta1.RolloutCoordinationGroupStatus{
		Name: "0", Policy: v1beta1.CoordinationPolicySequential, Phase: v1beta1.CoordinationPhaseIdle,
		CompositePhase: v1beta1.CompositePhaseSequentialAwaiting, CurrentComponent: v1beta1.EngineComponent,
		PreviousComponent: v1beta1.DecoderComponent, LastTransitionTime: &then,
	}
	phases := []struct {
		name string
		seed v1beta1.RolloutCoordinationGroupStatus
	}{
		{"idle", entry(v1beta1.CoordinationPhaseIdle)},
		{"seq-awaiting", awaiting},
		{"surging", entry(v1beta1.CoordinationPhaseSurging)},
		{"waiting", entry(v1beta1.CoordinationPhaseWaiting)},
		{"shifting", entry(v1beta1.CoordinationPhaseShifting)},
		{"scaling-down", entry(v1beta1.CoordinationPhaseScalingDown)},
		{"staged", entry(v1beta1.CoordinationPhaseStaged)},
		{"failed", entry(v1beta1.CoordinationPhaseFailed)},
		{"paused", entry(v1beta1.CoordinationPhasePaused)},
	}
	terminating := apierrors.NewForbidden(corev1.Resource("services"), "svc", errors.New("namespace is being terminated"))
	terminating.ErrStatus.Details.Causes = append(terminating.ErrStatus.Details.Causes, metav1.StatusCause{Type: corev1.NamespaceTerminatingCause})
	refusals := []struct {
		name    string
		client  func(objs ...runtime.Object) client.Client
		wantErr bool
	}{
		{"ir-read-error", func(objs ...runtime.Object) client.Client { return &failingIRClient{Client: testClient(objs...)} }, true},
		{"service-write-refused", func(objs ...runtime.Object) client.Client {
			return refusingServiceClient(apierrors.NewInternalError(errors.New("etcdserver: request timed out")), objs...)
		}, true},
		{"namespace-terminating", func(objs ...runtime.Object) client.Client { return refusingServiceClient(terminating, objs...) }, false},
	}
	for _, phase := range phases {
		for _, refusal := range refusals {
			t.Run(phase.name+"-"+refusal.name, func(t *testing.T) {
				isvc := testOMENativeISVC()
				isvc.Spec.Rollout = singleEngineGroup()
				isvc.Status.RolloutCoordination = &v1beta1.RolloutCoordinationStatus{Groups: []v1beta1.RolloutCoordinationGroupStatus{phase.seed}}
				c := refusal.client(rollingEngineIR(isvc), buildPod(isvc, v1beta1.EngineComponent, "aaaa", 0))
				result, err := Reconcile(context.Background(), ReconcileInputs{
					ISVC: isvc, Client: c, Reader: c, Now: time.Now(), ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent),
				})
				if refusal.wantErr && err == nil {
					t.Fatal("the refusal must fail the pass so it is retried")
				}
				if !refusal.wantErr && (err != nil || result == nil) {
					t.Fatalf("a terminating namespace ends the pass cleanly; got result=%v err=%v", result, err)
				}
				groups := isvc.Status.RolloutCoordination.Groups
				if len(groups) != 1 {
					t.Fatalf("the one group entry must survive the pass; got %+v", groups)
				}
				got := groups[0]
				if got.Phase != phase.seed.Phase || got.CompositePhase != phase.seed.CompositePhase ||
					got.CurrentComponent != phase.seed.CurrentComponent || got.PreviousComponent != phase.seed.PreviousComponent ||
					got.LastTransitionTime == nil || !got.LastTransitionTime.Time.Equal(then.Time) {
					t.Fatalf("a pass that stopped before evaluating the group must leave its entry as written; got %+v want %+v", got, phase.seed)
				}
			})
		}
	}
}

// A group that leaves the spec is pruned from status on the next pass,
// whatever phase its entry recorded; with no coordination-style group
// left the whole block goes and RolloutCoordinationReady reads True. Each
// case seeds the entry at one phase, switches the spec to a canary-only
// group, and reads the status back.
func TestReconcile_ARemovedGroupIsPrunedFromEveryPhase(t *testing.T) {
	then := metav1.NewTime(time.Now().Add(-time.Hour))
	entry := func(phase v1beta1.CoordinationPhase) v1beta1.RolloutCoordinationGroupStatus {
		return v1beta1.RolloutCoordinationGroupStatus{
			Name: "0", Policy: v1beta1.CoordinationPolicyBlueGreen, Phase: phase,
			CompositePhase: string(phase), LastTransitionTime: &then,
		}
	}
	awaiting := v1beta1.RolloutCoordinationGroupStatus{
		Name: "0", Policy: v1beta1.CoordinationPolicySequential, Phase: v1beta1.CoordinationPhaseIdle,
		CompositePhase: v1beta1.CompositePhaseSequentialAwaiting, CurrentComponent: v1beta1.EngineComponent,
		PreviousComponent: v1beta1.DecoderComponent, LastTransitionTime: &then,
	}
	phases := []struct {
		name string
		seed v1beta1.RolloutCoordinationGroupStatus
	}{
		{"idle", entry(v1beta1.CoordinationPhaseIdle)},
		{"seq-awaiting", awaiting},
		{"surging", entry(v1beta1.CoordinationPhaseSurging)},
		{"waiting", entry(v1beta1.CoordinationPhaseWaiting)},
		{"shifting", entry(v1beta1.CoordinationPhaseShifting)},
		{"scaling-down", entry(v1beta1.CoordinationPhaseScalingDown)},
		{"staged", entry(v1beta1.CoordinationPhaseStaged)},
		{"failed", entry(v1beta1.CoordinationPhaseFailed)},
		{"paused", entry(v1beta1.CoordinationPhasePaused)},
	}
	for _, phase := range phases {
		t.Run(phase.name, func(t *testing.T) {
			isvc := testOMENativeISVC()
			isvc.Status.RolloutCoordination = &v1beta1.RolloutCoordinationStatus{Groups: []v1beta1.RolloutCoordinationGroupStatus{phase.seed}}
			isvc.Status.SetCondition(apis.ConditionType(v1beta1.RolloutCoordinationReady), &apis.Condition{
				Type: apis.ConditionType(v1beta1.RolloutCoordinationReady), Status: corev1.ConditionFalse, Reason: "InProgress",
			})
			isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Canary:     &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{{Capacity: intstr.FromString("100%"), Traffic: 100}}},
			}}}
			c := testClient(rollingEngineIR(isvc), buildPod(isvc, v1beta1.EngineComponent, "aaaa", 0))
			if _, err := Reconcile(context.Background(), ReconcileInputs{
				ISVC: isvc, Client: c, Reader: c, Now: time.Now(), ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent),
			}); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if isvc.Status.RolloutCoordination != nil {
				t.Fatalf("the removed group's entry must be pruned with the block; got %+v", isvc.Status.RolloutCoordination)
			}
			cond := isvc.Status.GetCondition(apis.ConditionType(v1beta1.RolloutCoordinationReady))
			if cond == nil || cond.Status != corev1.ConditionTrue {
				t.Fatalf("RolloutCoordinationReady must read True once no group is left; got %+v", cond)
			}
		})
	}
}

// chainMemberIR is one chain member's replica with two Instances on the
// running revision, its current and update revisions by hash, and a Ready
// condition in the given state since the given instant. The rows and the
// counters follow the condition: Ready rows while it is True, Pending rows
// and no Ready pod otherwise. The parent-generation annotation matches the
// service, so the replica-side gate reads the replica as fresh.
func chainMemberIR(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, currentHash, updateHash, runningHash string, ready metav1.ConditionStatus, since time.Time) *v1beta1.InferenceReplica {
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name: irprojector.InferenceReplicaName(isvc.Name, c), Namespace: isvc.Namespace,
			Annotations: parentGenAnnotation(isvc.Generation),
		},
		Spec: v1beta1.InferenceReplicaSpec{Replicas: int32Ptr(2)},
		Status: v1beta1.InferenceReplicaStatus{
			Replicas:        2,
			CurrentRevision: isvc.Name + "-" + string(c) + "-" + currentHash,
			UpdateRevision:  isvc.Name + "-" + string(c) + "-" + updateHash,
		},
	}
	setChainMemberRows(ir, runningHash, ready, since)
	return ir
}

// setChainMemberRows writes a chain member's rows, counters and Ready
// condition for the given readiness: every Instance Ready on the running
// revision while the condition is True, Pending with no Ready pod otherwise.
func setChainMemberRows(ir *v1beta1.InferenceReplica, runningHash string, ready metav1.ConditionStatus, since time.Time) {
	running := ir.Name + "-" + runningHash
	phase, n := v1beta1.OMENativeInstanceReady, int32(2)
	if ready != metav1.ConditionTrue {
		phase, n = v1beta1.OMENativeInstancePending, 0
	}
	ir.Status.ReadyReplicas, ir.Status.ServingReplicas, ir.Status.AvailableReplicas = n, n, n
	ir.Status.UpdatedReplicas, ir.Status.UpdatedReadyReplicas = 0, 0
	if running == ir.Status.UpdateRevision {
		ir.Status.UpdatedReplicas, ir.Status.UpdatedReadyReplicas = 2, n
	}
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: phase, RunningRevision: running},
		{Index: 1, Phase: phase, RunningRevision: running},
	}
	reason := "AllInstancesReady"
	if ready != metav1.ConditionTrue {
		reason = "NoInstanceReady"
	}
	ir.Status.Conditions = []metav1.Condition{{Type: conditionReady, Status: ready, Reason: reason, LastTransitionTime: metav1.NewTime(since)}}
}

// setChainMemberReadiness rewrites a stored chain member's readiness: its
// rows, counters and Ready condition, as the replica controller publishes
// them when every pod leaves rotation or serves again.
func setChainMemberReadiness(ctx context.Context, t *testing.T, c client.Client, ir *v1beta1.InferenceReplica, runningHash string, ready metav1.ConditionStatus, since time.Time) {
	t.Helper()
	live := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(ir), live); err != nil {
		t.Fatalf("read replica %s: %v", ir.Name, err)
	}
	setChainMemberRows(live, runningHash, ready, since)
	if err := c.Update(ctx, live); err != nil {
		t.Fatalf("write replica %s: %v", ir.Name, err)
	}
}

// chainPods are the two pods of each chain member on the given revision.
func chainPods(isvc *v1beta1.InferenceService, decoderHash, engineHash string) []runtime.Object {
	return []runtime.Object{
		buildPod(isvc, v1beta1.DecoderComponent, decoderHash, 0), buildPod(isvc, v1beta1.DecoderComponent, decoderHash, 1),
		buildPod(isvc, v1beta1.EngineComponent, engineHash, 0), buildPod(isvc, v1beta1.EngineComponent, engineHash, 1),
	}
}

// sequentialChainPass runs one coordination pass over a chain at the given
// offset from t0 and checks the composite phase the group writes.
func sequentialChainPass(ctx context.Context, t *testing.T, isvc *v1beta1.InferenceService, c client.Client, t0 time.Time, at time.Duration, want string) {
	t.Helper()
	in := ReconcileInputs{ISVC: isvc, Client: c, Reader: c, Now: t0.Add(at), ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent, v1beta1.DecoderComponent)}
	if _, err := Reconcile(ctx, in); err != nil {
		t.Fatalf("pass at t+%s: %v", at, err)
	}
	if isvc.Status.RolloutCoordination == nil || len(isvc.Status.RolloutCoordination.Groups) != 1 {
		t.Fatalf("pass at t+%s: expected one group entry; got %+v", at, isvc.Status.RolloutCoordination)
	}
	if got := isvc.Status.RolloutCoordination.Groups[0].CompositePhase; got != want {
		t.Fatalf("at t+%s the group reads %q, want %q (message %q)", at, got, want, isvc.Status.RolloutCoordination.Groups[0].Message)
	}
}

// A Sequential chain's wait after a finished member runs from the instant
// that member first read done, and a readiness loss of the member afterwards
// neither cuts the wait short nor starts it over. Decoder then engine, both
// of 2 Instances, a five-minute wait after the decoder: the decoder finishes
// at t+0; every decoder pod leaves rotation at t+2m45s and is Ready again at
// t+3m; the group keeps Awaiting the engine until t+5m and reads the engine's
// own phase from then on.
func TestReconcile_TheWaitAfterAMemberRunsFromItsFinishThroughAReadinessLoss(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	isvc := sequentialChainISVC(5 * time.Minute)
	decoder := chainMemberIR(isvc, v1beta1.DecoderComponent, "bbbb", "bbbb", "bbbb", metav1.ConditionTrue, t0)
	engine := chainMemberIR(isvc, v1beta1.EngineComponent, "aaaa", "bbbb", "aaaa", metav1.ConditionUnknown, t0)
	c := testClient(append(chainPods(isvc, "bbbb", "aaaa"), decoder, engine)...)

	sequentialChainPass(ctx, t, isvc, c, t0, 0, v1beta1.CompositePhaseSequentialAwaiting)
	setChainMemberReadiness(ctx, t, c, decoder, "bbbb", metav1.ConditionFalse, t0.Add(2*time.Minute+45*time.Second))
	sequentialChainPass(ctx, t, isvc, c, t0, 2*time.Minute+45*time.Second, v1beta1.CompositePhaseSequentialAwaiting)
	if msg := isvc.Status.RolloutCoordination.Groups[0].Message; !strings.Contains(msg, "decoder not Ready") {
		t.Fatalf("at t+2m45s the group must say the decoder is not Ready; got %q", msg)
	}
	setChainMemberReadiness(ctx, t, c, decoder, "bbbb", metav1.ConditionTrue, t0.Add(3*time.Minute))
	sequentialChainPass(ctx, t, isvc, c, t0, 3*time.Minute, v1beta1.CompositePhaseSequentialAwaiting)
	if msg := isvc.Status.RolloutCoordination.Groups[0].Message; strings.Contains(msg, "not Ready") {
		t.Fatalf("at t+3m the decoder is Ready again and the group must say so; got %q", msg)
	}
	sequentialChainPass(ctx, t, isvc, c, t0, 4*time.Minute+59*time.Second, v1beta1.CompositePhaseSequentialAwaiting)
	sequentialChainPass(ctx, t, isvc, c, t0, 5*time.Minute+time.Second, "engine.Surging")
}

// While the finished member is not Ready the next member is held past the
// window too, and it starts on the member's recovery when the window has
// elapsed meanwhile: every decoder pod leaves rotation at t+2m45s and comes
// back at t+6m; at t+5m1s the group still awaits the engine, naming the
// decoder's readiness, and at t+6m the engine's own phase shows at once.
func TestReconcile_TheNextMemberStartsOnRecoveryWhenTheWaitElapsedDuringTheLoss(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	isvc := sequentialChainISVC(5 * time.Minute)
	decoder := chainMemberIR(isvc, v1beta1.DecoderComponent, "bbbb", "bbbb", "bbbb", metav1.ConditionTrue, t0)
	engine := chainMemberIR(isvc, v1beta1.EngineComponent, "aaaa", "bbbb", "aaaa", metav1.ConditionUnknown, t0)
	c := testClient(append(chainPods(isvc, "bbbb", "aaaa"), decoder, engine)...)

	sequentialChainPass(ctx, t, isvc, c, t0, 0, v1beta1.CompositePhaseSequentialAwaiting)
	setChainMemberReadiness(ctx, t, c, decoder, "bbbb", metav1.ConditionFalse, t0.Add(2*time.Minute+45*time.Second))
	sequentialChainPass(ctx, t, isvc, c, t0, 5*time.Minute+time.Second, v1beta1.CompositePhaseSequentialAwaiting)
	if msg := isvc.Status.RolloutCoordination.Groups[0].Message; !strings.Contains(msg, "decoder not Ready") {
		t.Fatalf("at t+5m1s the group must say the decoder is not Ready; got %q", msg)
	}
	setChainMemberReadiness(ctx, t, c, decoder, "bbbb", metav1.ConditionTrue, t0.Add(6*time.Minute))
	sequentialChainPass(ctx, t, isvc, c, t0, 6*time.Minute, "engine.Surging")
}

// The wait after a finished member also runs on while that member's replica
// object is deleted and created again: the fresh object publishes nothing
// for a pass and then a Ready condition that turned True on the new object,
// and neither releases the engine early nor delays it past t+5m.
func TestReconcile_TheWaitAfterAMemberRunsOnWhenItsReplicaIsRecreated(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	isvc := sequentialChainISVC(5 * time.Minute)
	decoder := chainMemberIR(isvc, v1beta1.DecoderComponent, "bbbb", "bbbb", "bbbb", metav1.ConditionTrue, t0)
	engine := chainMemberIR(isvc, v1beta1.EngineComponent, "aaaa", "bbbb", "aaaa", metav1.ConditionUnknown, t0)
	c := testClient(append(chainPods(isvc, "bbbb", "aaaa"), decoder, engine)...)

	sequentialChainPass(ctx, t, isvc, c, t0, 0, v1beta1.CompositePhaseSequentialAwaiting)
	if err := c.Delete(ctx, decoder); err != nil {
		t.Fatalf("delete replica: %v", err)
	}
	recreated := chainMemberIR(isvc, v1beta1.DecoderComponent, "bbbb", "bbbb", "bbbb", metav1.ConditionTrue, t0)
	recreated.Status = v1beta1.InferenceReplicaStatus{}
	if err := c.Create(ctx, recreated); err != nil {
		t.Fatalf("create replica again: %v", err)
	}
	sequentialChainPass(ctx, t, isvc, c, t0, 2*time.Minute+15*time.Second, v1beta1.CompositePhaseSequentialAwaiting)
	// The replica controller publishes the recreated object: the same
	// revisions, a Ready condition that turned True now.
	live := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(recreated), live); err != nil {
		t.Fatalf("read replica: %v", err)
	}
	live.Status = decoder.Status
	setChainMemberRows(live, "bbbb", metav1.ConditionTrue, t0.Add(2*time.Minute+16*time.Second))
	if err := c.Update(ctx, live); err != nil {
		t.Fatalf("publish replica: %v", err)
	}
	sequentialChainPass(ctx, t, isvc, c, t0, 2*time.Minute+30*time.Second, v1beta1.CompositePhaseSequentialAwaiting)
	sequentialChainPass(ctx, t, isvc, c, t0, 5*time.Minute+time.Second, "engine.Surging")
}

// TestGroupPhase_AHeldMemberKeepsThePhaseItsRowsCompute locks what the group
// reads while a rolling member's retry record for its target is Held: the
// phase its Instance rows compute, and nothing from the record. A push whose
// new image no pod can pull ends its attempt with the old pod still serving,
// so the row reads Ready on the old revision with the failed attempt on its
// record and the group reads Surging, a roll in flight and held, not Failed,
// under BlueGreen, RollingUpdate and Sequential alike; Sequential keeps the
// held member as the current one. A Failed row beside the record reads
// Failed with the rows' own summary. The replicas' own holds carry the Held
// reading, and a release of the record changes nothing the phase reads.
func TestGroupPhase_AHeldMemberKeepsThePhaseItsRowsCompute(t *testing.T) {
	isvc := testOMENativeISVC()
	engine, decoder := v1beta1.EngineComponent, v1beta1.DecoderComponent
	const v1, v2 = "llama-engine-v1", "llama-engine-v2"
	old := func(index int32) v1beta1.OMENativeInstanceStatus {
		return v1beta1.OMENativeInstanceStatus{Index: index, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: v1, TargetRevision: v1, PodCount: 1, ServingPodCount: 1}
	}
	// attempted is the row the replica publishes once a surge toward v2 has
	// ended without serving: Ready on v1, the pushed revision kept as its
	// target, the failure on its record and no operation open.
	attempted := func(index int32) v1beta1.OMENativeInstanceStatus {
		row := old(index)
		row.TargetRevision = v2
		row.LastFailure = &v1beta1.InstanceTermination{PodName: "pull-failed", Reason: "ImagePullBackOff"}
		return row
	}
	replica := func(c v1beta1.ComponentType, update string, rows []v1beta1.OMENativeInstanceStatus, held bool) *v1beta1.InferenceReplica {
		ir := &v1beta1.InferenceReplica{
			ObjectMeta: metav1.ObjectMeta{Name: irprojector.InferenceReplicaName(isvc.Name, c), Namespace: isvc.Namespace},
			Status: v1beta1.InferenceReplicaStatus{
				Replicas: int32(len(rows)), ReadyReplicas: int32(len(rows)), ServingReplicas: int32(len(rows)),
				CurrentRevision: v1, UpdateRevision: update, InstanceStatuses: rows,
			},
		}
		if held {
			ir.Status.RetryBlocks = []v1beta1.RetryBlock{{TargetRevision: v2, State: v1beta1.RetryBlockHeld, AttemptsStarted: 3, Reason: "ImagePullBackOff"}}
		}
		return ir
	}
	failedRow := func(index int32) v1beta1.OMENativeInstanceStatus {
		row := attempted(index)
		row.Phase = v1beta1.OMENativeInstanceFailed
		row.ServingPodCount = 0
		return row
	}
	cases := []struct {
		name          string
		group         ResolvedGroup
		replicas      []runtime.Object
		wantPhase     v1beta1.CoordinationPhase
		wantComposite string
		wantMessage   string
		wantCurrent   v1beta1.ComponentType
	}{
		{
			name:  "bluegreen: one engine attempt failed and the revision is Held while the other engine and the decoder serve",
			group: ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{engine, decoder}, Policy: v1beta1.CoordinationPolicyBlueGreen},
			replicas: []runtime.Object{
				replica(engine, v2, []v1beta1.OMENativeInstanceStatus{attempted(0), old(1)}, true),
				replica(decoder, v1, []v1beta1.OMENativeInstanceStatus{old(0)}, false),
			},
			wantComposite: "Surging",
		},
		{
			name:  "sequential: the decoder, first in Order, is Held and the engine waits its turn",
			group: ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{decoder, engine}, Order: []v1beta1.ComponentType{decoder, engine}, Policy: v1beta1.CoordinationPolicySequential},
			replicas: []runtime.Object{
				replica(decoder, v2, []v1beta1.OMENativeInstanceStatus{attempted(0)}, true),
				replica(engine, v2, []v1beta1.OMENativeInstanceStatus{old(0)}, false),
			},
			wantComposite: "decoder.Surging",
			wantCurrent:   decoder,
		},
		{
			name:  "rollingupdate: both sides roll, each with one failed attempt, and the decoder is Held",
			group: ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{engine, decoder}, Policy: v1beta1.CoordinationPolicyRollingUpdate},
			replicas: []runtime.Object{
				replica(engine, v2, []v1beta1.OMENativeInstanceStatus{attempted(0), old(1), old(2)}, false),
				replica(decoder, v2, []v1beta1.OMENativeInstanceStatus{attempted(0), old(1), old(2)}, true),
			},
			wantComposite: "Surging",
		},
		{
			name:  "rollingupdate: the same rows with the record released read the same",
			group: ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{engine, decoder}, Policy: v1beta1.CoordinationPolicyRollingUpdate},
			replicas: []runtime.Object{
				replica(engine, v2, []v1beta1.OMENativeInstanceStatus{attempted(0), old(1), old(2)}, false),
				replica(decoder, v2, []v1beta1.OMENativeInstanceStatus{attempted(0), old(1), old(2)}, false),
			},
			wantComposite: "Surging",
		},
		{
			name:  "rollingupdate: a Failed row beside the Held decoder reads Failed from the row",
			group: ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{engine, decoder}, Policy: v1beta1.CoordinationPolicyRollingUpdate},
			replicas: []runtime.Object{
				replica(engine, v2, []v1beta1.OMENativeInstanceStatus{failedRow(0), old(1), old(2)}, false),
				replica(decoder, v2, []v1beta1.OMENativeInstanceStatus{attempted(0), old(1), old(2)}, true),
			},
			wantPhase:     v1beta1.CoordinationPhaseFailed,
			wantComposite: "Failed",
			wantMessage:   "one or more Components in the RollingUpdate group failed",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obs, err := buildGroupObservation(context.Background(), testClient(tc.replicas...), isvc, tc.group, map[v1beta1.ComponentType]map[string]int32{})
			if err != nil {
				t.Fatalf("buildGroupObservation: %v", err)
			}
			tr := ComputeTransition(obs)
			wantPhase := tc.wantPhase
			if wantPhase == "" {
				wantPhase = v1beta1.CoordinationPhaseSurging
			}
			if tr.Phase != wantPhase {
				t.Fatalf("phase=%q (composite %q, message %q), want %q: the phase is the rows', the record never enters it", tr.Phase, tr.CompositePhase, tr.Message, wantPhase)
			}
			if tc.wantMessage != "" && tr.Message != tc.wantMessage {
				t.Errorf("message=%q, want %q", tr.Message, tc.wantMessage)
			}
			if tr.CompositePhase != tc.wantComposite {
				t.Errorf("composite=%q, want %q", tr.CompositePhase, tc.wantComposite)
			}
			if tr.CurrentComponent != tc.wantCurrent {
				t.Errorf("current=%q, want %q", tr.CurrentComponent, tc.wantCurrent)
			}
		})
	}
}
