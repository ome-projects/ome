package ops

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/gang"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// gangSchedClient is legacyNewFakeClient plus the scheduler-plugins
// scheduling.x-k8s.io scheme so the test can assert PodGroup creation.
func gangSchedClient(t *testing.T, initObjs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		corev1.AddToScheme,
		v1beta1.AddToScheme,
		discoveryv1.AddToScheme,
		appsv1.AddToScheme,
		schedulingv1alpha1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			t.Fatalf("add scheme: %v", err)
		}
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceService{}).
		WithObjects(initObjs...).
		Build()
}

// TestGangSurgeUpdate_EnsuresPodGroupBeforeSurgePods pins that
// gangSurgeUpdate announces the surge index's PodGroup to the gang
// scheduler BEFORE creating that gang's pods.
//
// Scenario: a multi-node engine (minReplicas=4, canary capacity 50%).
// The canary surges a new gang at a fresh instance index; on a
// coscheduler-style cluster, surge pods created before their PodGroup
// exists are rejected with "0/1 nodes are available: 1 PodGroup not
// found" and never scheduled, so the canary capacity gate never opens
// and the rollout stalls at phase=Pending.
//
// The top-level EnsurePodGroups pass keys off plan.Instances, and the
// surge index only lands in the plan once its GangSurgeTarget status
// round-trips into ObservedState. In the window before that round-trip,
// gangSurgeUpdate creates the surge pods carrying the pod-group label, so
// it must ensure the PodGroup inline before the create.
func TestGangSurgeUpdate_EnsuresPodGroupBeforeSurgePods(t *testing.T) {
	legacyResetExpectations(t)
	isvc, _ := surgeISVCReady("gang-a", "prod", 1)
	plan := gangSurgePlan()
	plan.TopologyKey = "network.example.com/fabric-domain"

	v1Name := "gang-a-engine-rev-v1hash"
	v2Name := "gang-a-engine-rev-v2hash"

	// In-flight gang surge, BEFORE the surge pods are created: the source
	// (idx=0) carries Op{Surge, SurgeIndex=1}; idx=1 carries the
	// GangSurgeTarget marker (on IR).
	ir := gangSurgeInFlightIR(isvc, v1Name, v2Name)

	c := gangSchedClient(t, isvc, ir)
	makeCR(t, c, isvc, v2Name)
	// Source gang still alive at idx=0 (capacity holds during the surge).
	for _, runner := range []string{"leader", "worker"} {
		hash := query.RevisionHashFromControllerRevisionName(v2Name)
		if err := c.Create(context.Background(), gangPodAt(isvc, 0, runner, hash, true, true)); err != nil {
			t.Fatalf("seed source pod (%s): %v", runner, err)
		}
	}

	input := gangInputWithRemove(isvc, c)
	// The cluster has the PodGroup CRD — same flag the IR reconciler threads.
	input.DesiredSpec.GangSchedulingAvailable = true

	v2 := &appsv1.ControllerRevision{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: v2Name}, v2); err != nil {
		t.Fatalf("get v2 CR: %v", err)
	}

	// Wire the inline surge-PodGroup ensure exactly as the IR reconciler
	// does (deps.EnsureGangPodGroup = gang.EnsureSurgePodGroup).
	deps := legacyTestDeps(c)
	deps.EnsureGangPodGroup = gang.EnsureSurgePodGroup(deps)

	// Create-surge-gang pass: this is where the surge gang's pods are
	// created at idx=1.
	if _, err := surgeUpdate(context.Background(), deps, input, plan, plan.Instances[0], v2, nil); err != nil {
		t.Fatalf("gang surge create pass: %v", err)
	}

	// The surge pods exist...
	surgePods, err := query.LiveListPodsForInstance(context.Background(), c, "prod", "gang-a", workload.ComponentEngine, 1)
	if err != nil {
		t.Fatalf("list surge gang pods: %v", err)
	}
	if len(surgePods) == 0 {
		t.Fatalf("expected surge gang pods at idx=1; found none")
	}

	// ...and the PodGroup they reference MUST exist (else the gang scheduler
	// rejects them with "PodGroup not found").
	wantPG := query.PodGroupName("gang-a", workload.ComponentEngine, 1)
	pg := &schedulingv1alpha1.PodGroup{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: wantPG}, pg); err != nil {
		t.Fatalf("surge gang PodGroup %q missing when its pods exist: %v", wantPG, err)
	}
	// MinMember must cover the whole gang (leader + worker = 2).
	if pg.Spec.MinMember != 2 {
		t.Errorf("PodGroup %q MinMember=%d, want 2 (leader+worker)", wantPG, pg.Spec.MinMember)
	}
	if got := pg.Annotations[query.AnnotationTopologyKey]; got != plan.TopologyKey {
		t.Errorf("surge PodGroup %q topology annotation=%q, want %q", wantPG, got, plan.TopologyKey)
	}
	// Every surge pod must carry the pod-group label naming that PodGroup.
	for _, pod := range surgePods {
		if got := pod.Labels[query.LabelPodGroup]; got != wantPG {
			t.Errorf("surge pod %s pod-group label=%q, want %q", pod.Name, got, wantPG)
		}
	}
}

// TestGangSurgeUpdate_HoldsSourceDrainUntilReplacementPodReady pins that
// a gang surge must NOT drain the source gang
// out of serving until the REPLACEMENT gang is PodReady (containers ready AND
// the ome.io/serving readiness gate AND'd in by kubelet) — not merely
// ContainersReady. ome.io/serving is itself a readiness gate, so a just-served
// pod is not PodReady (not in Service rotation) until kubelet re-evaluates the
// gate. Draining the source on ContainersReady alone drops it out
// of rotation while the replacement is still not in it — an N-1 capacity
// TROUGH. Across Components rolled on independent timelines those offset
// troughs skew the live RatioBalanced ratio.
func TestGangSurgeUpdate_HoldsSourceDrainUntilReplacementPodReady(t *testing.T) {
	legacyResetExpectations(t)
	isvc, _ := surgeISVCReady("gang-b", "prod", 1)
	plan := gangSurgePlan()

	v1Name := "gang-b-engine-rev-v1hash"
	v2Name := "gang-b-engine-rev-v2hash"
	ir := gangSurgeInFlightIR(isvc, v1Name, v2Name)

	c := gangSchedClient(t, isvc, ir)
	makeCR(t, c, isvc, v1Name)
	makeCR(t, c, isvc, v2Name)
	v1Hash := query.RevisionHashFromControllerRevisionName(v1Name)
	v2Hash := query.RevisionHashFromControllerRevisionName(v2Name)

	// Source gang (idx=0, old rev) fully in rotation.
	for _, runner := range []string{"leader", "worker"} {
		if err := c.Create(context.Background(), gangPodAt(isvc, 0, runner, v1Hash, true, true)); err != nil {
			t.Fatalf("seed source pod (%s): %v", runner, err)
		}
	}
	// Replacement gang (idx=1, new rev): ContainersReady + serving, but NOT
	// yet PodReady — the window in which the source must still be held.
	for _, runner := range []string{"leader", "worker"} {
		if err := c.Create(context.Background(), gangPodAt(isvc, 1, runner, v2Hash, true, true)); err != nil {
			t.Fatalf("seed replacement pod (%s): %v", runner, err)
		}
	}

	input := gangInputWithRemove(isvc, c)
	input.DesiredSpec.GangSchedulingAvailable = true
	deps := legacyTestDeps(c)
	deps.EnsureGangPodGroup = gang.EnsureSurgePodGroup(deps)

	v2 := &appsv1.ControllerRevision{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: v2Name}, v2); err != nil {
		t.Fatalf("get v2 CR: %v", err)
	}

	srcServing := func(when string) (present int, serving int) {
		pods, err := query.LiveListPodsForInstance(context.Background(), c, "prod", "gang-b", workload.ComponentEngine, 0)
		if err != nil {
			t.Fatalf("list source pods (%s): %v", when, err)
		}
		for _, p := range pods {
			present++
			if podreadiness.IsServing(p) {
				serving++
			}
		}
		return
	}

	// Pass 1: replacement NOT PodReady → the source must stay fully serving.
	if _, err := surgeUpdate(context.Background(), deps, input, plan, plan.Instances[0], v2, nil); err != nil {
		t.Fatalf("gang surge pass 1: %v", err)
	}
	if present, serving := srcServing("pass1"); present == 0 || serving != present {
		t.Fatalf("source drained before replacement PodReady (present=%d serving=%d) — capacity trough", present, serving)
	}

	// Flip the replacement gang to PodReady (kubelet AND'd the serving gate).
	replPods, err := query.LiveListPodsForInstance(context.Background(), c, "prod", "gang-b", workload.ComponentEngine, 1)
	if err != nil {
		t.Fatalf("list replacement pods: %v", err)
	}
	for _, p := range replPods {
		p.Status.Conditions = append(p.Status.Conditions, corev1.PodCondition{
			Type: corev1.PodReady, Status: corev1.ConditionTrue,
		})
		if err := c.Status().Update(context.Background(), p); err != nil {
			t.Fatalf("flip replacement PodReady (%s): %v", p.Name, err)
		}
	}

	// Pass 2: replacement PodReady → NOW the source drains out of serving.
	if _, err := surgeUpdate(context.Background(), deps, input, plan, plan.Instances[0], v2, nil); err != nil {
		t.Fatalf("gang surge pass 2: %v", err)
	}
	if present, serving := srcServing("pass2"); present > 0 && serving > 0 {
		t.Errorf("source still serving after replacement PodReady (present=%d serving=%d) — drain did not fire", present, serving)
	}
}
