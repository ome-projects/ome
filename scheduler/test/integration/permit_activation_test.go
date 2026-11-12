package integration

import (
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	schedutil "sigs.k8s.io/scheduler-plugins/test/util"
)

// roleLabel is the label a gang's leader pod carries so a worker's
// required podAffinity term can target it specifically — mirroring how OME's
// real multi-node gangs mark their leader Runner (see spread_tsc_test.go's
// "runner" convention).
const roleLabel = "runner"

// affinityToLeader is the worker side of the worker-follows-leader shape: a
// REQUIRED podAffinity term that matches only the gang's leader, within the
// same topology domain.
func affinityToLeader(pgName string) *v1.Affinity {
	return &v1.Affinity{PodAffinity: &v1.PodAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{{
			TopologyKey: domainLabelKey,
			LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
				podGroupLabel: pgName,
				roleLabel:     "leader",
			}},
		}},
	}}
}

// TestPermitActivationRescuesAffinityBlockedWorker locks prompt convergence of
// the worker-first ordering of a multi-pod gang: the worker carries a REQUIRED
// podAffinity term to its leader (worker-follows-leader, never mutual — a
// mutual requirement would block the very first pod scheduled forever), gated
// together by the same PodGroup. Created first, the worker's earliest attempt
// can only fail: its leader does not exist yet, and until the leader is
// assumed on a node no node can satisfy the term. Nothing in the cluster
// reports the leader's later arrival or assumption to the held worker, so
// the plugin's own activations (on the member set completing, and on a member
// reaching Permit) are what bring it back.
//
// Both members must bind well inside the gate's permit timeout: a pass proves
// the worker was woken and retried promptly, not that a timeout unwound and
// re-formed the gang.
func TestPermitActivationRescuesAffinityBlockedWorker(t *testing.T) {
	tc := startScheduler(t, globalKubeConfig, gangPackOptions(t)...)
	defer tc.teardown(t)

	const ns = "gang-sibling-wake"
	createNamespace(t, tc, ns)

	for _, n := range []string{"sw1", "sw2"} {
		if _, err := tc.ClientSet.CoreV1().Nodes().Create(tc.Ctx, makeGPUNode(n, "a", 1), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create node %s: %v", n, err)
		}
	}

	pg := schedutil.MakePG("gang", ns, 2, nil, nil)
	pg.Annotations = map[string]string{topologyKeyAnnotation: domainLabelKey}
	// Generous on purpose: the assertion below is a tight bind window, so a pass
	// must come from prompt convergence, never from this timeout firing an
	// unwind/retry that happens to land in time by coincidence.
	longTimeout := int32(120)
	pg.Spec.ScheduleTimeoutSeconds = &longTimeout
	if _, err := tc.SchedClient.SchedulingV1alpha1().PodGroups(ns).Create(tc.Ctx, pg, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create podgroup: %v", err)
	}

	leader := makeGangPod("sw-leader", ns, "gang")
	leader.Labels[roleLabel] = "leader"

	worker := makeGangPod("sw-worker", ns, "gang")
	worker.Labels[roleLabel] = "worker"
	worker.Spec.Affinity = affinityToLeader("gang")

	// Worker first: its earliest attempt(s) can only fail — the leader either
	// does not exist yet, or exists but has not been assumed anywhere the
	// required affinity term could match.
	if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, worker, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create worker: %v", err)
	}
	if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, leader, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create leader: %v", err)
	}

	const tight = 8 * time.Second
	for _, name := range []string{"sw-leader", "sw-worker"} {
		node := waitForPodBound(t, tc, ns, name, tight)
		if node != "sw1" && node != "sw2" {
			t.Errorf("pod %s bound to %s, want sw1/sw2", name, node)
		}
	}
}

// TestTemplateCompletionRescuesLeaderFirstGang locks the opposite creation order
// of the same worker-follows-leader shape, which is how a multi-node controller
// typically creates a gang: leader first, then the worker. The leader is popped
// before the worker exists and is held short of the full member template set.
// An unscheduled pod's creation is not a scheduling-queue move event, so the
// worker's arrival on its own wakes nobody; and the worker's own attempt cannot
// succeed, because its required affinity needs the leader assumed on a node
// first. Neither member reaches Permit, so Permit's sibling activation never
// runs either. The plugin must therefore treat "the member set became complete"
// as an explicit wake-up: the worker's PreFilter, the first to observe the full
// set, activates the held leader; the leader then takes the domain, waits at
// Permit, and its activation pulls the worker through.
//
// The tight bind window is the assertion: without that wake-up both pods sit in
// the unschedulable pool until the scheduler's periodic flush, minutes later.
func TestTemplateCompletionRescuesLeaderFirstGang(t *testing.T) {
	tc := startScheduler(t, globalKubeConfig, gangPackOptions(t)...)
	defer tc.teardown(t)

	const ns = "gang-leader-first"
	createNamespace(t, tc, ns)

	for _, n := range []string{"lf1", "lf2"} {
		if _, err := tc.ClientSet.CoreV1().Nodes().Create(tc.Ctx, makeGPUNode(n, "a", 1), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create node %s: %v", n, err)
		}
	}

	pg := schedutil.MakePG("gang", ns, 2, nil, nil)
	pg.Annotations = map[string]string{topologyKeyAnnotation: domainLabelKey}
	// Generous on purpose, as in the worker-first test: convergence must come
	// from the wake-up, never from a permit timeout unwinding and retrying.
	longTimeout := int32(120)
	pg.Spec.ScheduleTimeoutSeconds = &longTimeout
	if _, err := tc.SchedClient.SchedulingV1alpha1().PodGroups(ns).Create(tc.Ctx, pg, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create podgroup: %v", err)
	}

	leader := makeGangPod("lf-leader", ns, "gang")
	leader.Labels[roleLabel] = "leader"
	if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, leader, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create leader: %v", err)
	}
	// The leader must have been popped and held back on the incomplete member set
	// before the worker exists; creating the worker any earlier could let the
	// leader's first attempt see the full set and hide the ordering under test.
	waitForPodRejected(t, tc, ns, "lf-leader", "waiting for all PodGroup member templates", 10*time.Second)

	worker := makeGangPod("lf-worker", ns, "gang")
	worker.Labels[roleLabel] = "worker"
	worker.Spec.Affinity = affinityToLeader("gang")
	if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, worker, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create worker: %v", err)
	}

	const tight = 10 * time.Second
	for _, name := range []string{"lf-leader", "lf-worker"} {
		node := waitForPodBound(t, tc, ns, name, tight)
		if node != "lf1" && node != "lf2" {
			t.Errorf("pod %s bound to %s, want lf1/lf2", name, node)
		}
	}
}

// TestLeaderFirstGangKeepsBestFitDomain is the leader-first ordering on a
// cluster with two feasible domains: "a" (2 free nodes, the best fit for a
// 2-member gang) and "b" (8 free nodes). A worker whose required affinity
// targets a leader that is not yet assumed cannot pass Filter on any node, so
// if it planned and took a domain anyway, the failure would be recorded
// against that domain and the leader's own attempt would plan around it,
// landing the gang in the looser domain. The worker must instead yield without
// planning, leaving the leader to make the placement decision.
//
// Both members must bind promptly AND in domain "a".
func TestLeaderFirstGangKeepsBestFitDomain(t *testing.T) {
	tc := startScheduler(t, globalKubeConfig, gangPackOptions(t)...)
	defer tc.teardown(t)

	const ns = "gang-leader-first-bestfit"
	createNamespace(t, tc, ns)

	for _, n := range []string{"bf-a1", "bf-a2"} {
		if _, err := tc.ClientSet.CoreV1().Nodes().Create(tc.Ctx, makeGPUNode(n, "a", 1), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create node %s: %v", n, err)
		}
	}
	for _, n := range []string{"bf-b1", "bf-b2", "bf-b3", "bf-b4", "bf-b5", "bf-b6", "bf-b7", "bf-b8"} {
		if _, err := tc.ClientSet.CoreV1().Nodes().Create(tc.Ctx, makeGPUNode(n, "b", 1), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create node %s: %v", n, err)
		}
	}

	pg := schedutil.MakePG("gang", ns, 2, nil, nil)
	pg.Annotations = map[string]string{topologyKeyAnnotation: domainLabelKey}
	longTimeout := int32(120)
	pg.Spec.ScheduleTimeoutSeconds = &longTimeout
	if _, err := tc.SchedClient.SchedulingV1alpha1().PodGroups(ns).Create(tc.Ctx, pg, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create podgroup: %v", err)
	}

	leader := makeGangPod("bf-leader", ns, "gang")
	leader.Labels[roleLabel] = "leader"
	if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, leader, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create leader: %v", err)
	}
	waitForPodRejected(t, tc, ns, "bf-leader", "waiting for all PodGroup member templates", 10*time.Second)

	worker := makeGangPod("bf-worker", ns, "gang")
	worker.Labels[roleLabel] = "worker"
	worker.Spec.Affinity = affinityToLeader("gang")
	if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, worker, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create worker: %v", err)
	}

	const tight = 10 * time.Second
	for _, name := range []string{"bf-leader", "bf-worker"} {
		node := waitForPodBound(t, tc, ns, name, tight)
		if d := domainOfNode(t, tc, node); d != "a" {
			t.Errorf("pod %s bound to %s in domain %q, want the best-fit domain a", name, node, d)
		}
	}
}
