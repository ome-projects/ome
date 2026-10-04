package integration

import (
	"context"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/component-base/metrics/legacyregistry"

	schedutil "sigs.k8s.io/scheduler-plugins/test/util"
)

// holdGate is the scheduling gate that keeps a gang member out of the
// scheduling queue until the test lifts it, so a gang's formation window can be
// held open for as long as the scenario needs.
const holdGate = "testing.example/hold"

// gateWaits reads the plugin's Permit "wait" counter from the scheduler's
// registry: it is the only signal that a member has pinned its domain and is
// holding the gang's reservation at the gate, since a waiting pod is not
// updated in the API.
func gateWaits(t *testing.T) float64 {
	t.Helper()
	families, err := legacyregistry.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() != "ome_scheduler_gang_gate_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "result" && label.GetValue() == "wait" {
					return metric.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

// waitForGateWait polls until at least one more member is waiting at the gate
// than when before was sampled.
func waitForGateWait(t *testing.T, tc *testContext, before float64, timeout time.Duration) {
	t.Helper()
	err := wait.PollUntilContextTimeout(tc.Ctx, 100*time.Millisecond, timeout, true,
		func(context.Context) (bool, error) { return gateWaits(t) > before, nil })
	if err != nil {
		t.Fatalf("no gang member reached the gate within %s: %v", timeout, err)
	}
}

// liftGate removes the hold gate from the pod, admitting it to the queue.
func liftGate(t *testing.T, tc *testContext, ns, name string) {
	t.Helper()
	p, err := tc.ClientSet.CoreV1().Pods(ns).Get(tc.Ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pod %s/%s: %v", ns, name, err)
	}
	p.Spec.SchedulingGates = nil
	if _, err := tc.ClientSet.CoreV1().Pods(ns).Update(tc.Ctx, p, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("lift gate on %s/%s: %v", ns, name, err)
	}
}

// TestGangParkedOnReservedDomainRetriesWhenReservationDrains: a forming gang
// exclusively holds its domain until its last member is assumed. A second gang
// planned in that window finds room in the snapshot but no domain it may use,
// and parks with "no domain has room". The holder's completion is plugin-internal
// state: it produces no pod or node event the parked gang's requeue hints could
// see, so the plugin must wake the parked members itself when the reservation
// drains. The tight bind window is the assertion: without that wake-up the
// second gang sits in the unschedulable pool until the scheduler's periodic
// flush, minutes later.
//
// The holder's formation window is held open by a scheduling gate on its
// worker: the leader pins the domain and waits at Permit, the second gang is
// created and parks, then the gate is lifted and the worker's Reserve drains
// the reservation.
func TestGangParkedOnReservedDomainRetriesWhenReservationDrains(t *testing.T) {
	tc := startScheduler(t, globalKubeConfig, gangPackOptions(t)...)
	defer tc.teardown(t)

	const ns = "gang-reserved-wait"
	createNamespace(t, tc, ns)

	// One domain, two nodes with room for both gangs however the holder's
	// members spread over them; only the reservation keeps the second gang out.
	for _, n := range []string{"rw1", "rw2"} {
		if _, err := tc.ClientSet.CoreV1().Nodes().Create(tc.Ctx, makeGPUNode(n, "a", 4), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create node %s: %v", n, err)
		}
	}
	// Generous permit timeout: convergence must come from the wake-up, never
	// from the holder timing out and unwinding.
	longTimeout := int32(120)
	for _, name := range []string{"holder", "parked"} {
		pg := schedutil.MakePG(name, ns, 2, nil, nil)
		pg.Annotations = map[string]string{topologyKeyAnnotation: domainLabelKey}
		pg.Spec.ScheduleTimeoutSeconds = &longTimeout
		if _, err := tc.SchedClient.SchedulingV1alpha1().PodGroups(ns).Create(tc.Ctx, pg, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create podgroup %s: %v", name, err)
		}
	}

	// The holder's worker exists (so the leader sees a complete member set) but
	// is gated out of the queue; the leader pins the domain, reserves both
	// slots, and waits at the gate.
	worker := makeGangPod("holder-worker", ns, "holder")
	worker.Spec.SchedulingGates = []v1.PodSchedulingGate{{Name: holdGate}}
	if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, worker, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create holder worker: %v", err)
	}
	waitsBefore := gateWaits(t)
	if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, makeGangPod("holder-leader", ns, "holder"), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create holder leader: %v", err)
	}
	waitForGateWait(t, tc, waitsBefore, 10*time.Second)

	// The second gang is planned while the domain is held: both members park.
	for _, name := range []string{"parked-0", "parked-1"} {
		if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, makeGangPod(name, ns, "parked"), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create pod %s: %v", name, err)
		}
	}
	for _, name := range []string{"parked-0", "parked-1"} {
		waitForPodRejected(t, tc, ns, name, "no domain has room", 10*time.Second)
	}

	// The holder completes: its worker's Reserve drains the reservation.
	liftGate(t, tc, ns, "holder-worker")

	const tight = 15 * time.Second
	for _, name := range []string{"holder-leader", "holder-worker", "parked-0", "parked-1"} {
		node := waitForPodBound(t, tc, ns, name, tight)
		if node != "rw1" && node != "rw2" {
			t.Errorf("pod %s bound to %s, want rw1/rw2", name, node)
		}
	}
}
