package integration

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	schedutil "sigs.k8s.io/scheduler-plugins/test/util"
)

// setNodeGPU updates a node's allocatable+capacity for gpuResource, mimicking a
// node gaining or losing accelerator capacity (device-plugin registering, or a
// drain). Used to reproduce the trigger: a burst arriving while some domains
// have no capacity, then capacity restored.
func setNodeGPU(t *testing.T, tc *testContext, name string, gpus int64) {
	t.Helper()
	n, err := tc.ClientSet.CoreV1().Nodes().Get(tc.Ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get node %s: %v", name, err)
	}
	q := *resource.NewQuantity(gpus, resource.DecimalSI)
	n.Status.Capacity[gpuResource] = q
	n.Status.Allocatable[gpuResource] = q
	if _, err := tc.ClientSet.CoreV1().Nodes().UpdateStatus(tc.Ctx, n, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update node %s status: %v", name, err)
	}
}

// TestBurstUnderPartialCapacityThenRestore covers a burst under partial capacity: a
// gang burst arrives while only SOME domains have accelerator capacity (the rest
// at zero — a device-plugin/informer lag on a real cluster). The gangs that fit place;
// the rest wait as no-fit. Then the missing capacity is restored. Every gang must
// converge — the empty domains are now usable and there are exactly enough for the
// waiting gangs. A regression never binds the waiting gangs (assigned to a domain they
// can't use, or blocked by leaked reservations) even though capacity is now free.
//
// Shape: `domains` domains x `gangSize` nodes, one gang per domain; half the domains
// start at zero gpu, so half the gangs are no-fit until the restore.
func TestBurstUnderPartialCapacityThenRestore(t *testing.T) {
	const (
		domains      = 12
		gangSize     = 3 // == nodes per domain
		gangs        = domains
		schedTimeout = 120 * time.Second
	)
	half := domains / 2

	tc := startScheduler(t, globalKubeConfig, gangPackOptions(t)...)
	defer tc.teardown(t)

	const ns = "gang-capchange"
	createNamespace(t, tc, ns)

	domainName := func(d int) string { return fmt.Sprintf("cc-dom-%03d", d) }
	nodeName := func(d, n int) string { return fmt.Sprintf("cc-n-%03d-%02d", d, n) }
	gangName := func(g int) string { return fmt.Sprintf("cc-gang-%03d", g) }
	podName := func(g, m int) string { return fmt.Sprintf("cc-p-%03d-%02d", g, m) }

	// All nodes created with gpu; then zero the second half of domains so only the
	// first half is usable when the burst lands.
	for d := 0; d < domains; d++ {
		for n := 0; n < gangSize; n++ {
			if _, err := tc.ClientSet.CoreV1().Nodes().Create(tc.Ctx, makeGPUNode(nodeName(d, n), domainName(d), 1), metav1.CreateOptions{}); err != nil {
				t.Fatalf("create node: %v", err)
			}
		}
	}
	for d := half; d < domains; d++ {
		for n := 0; n < gangSize; n++ {
			setNodeGPU(t, tc, nodeName(d, n), 0)
		}
	}

	pgTimeout := int32(schedTimeout.Seconds()) + 120

	// Fire all gangs at once (members before their PodGroup — the controller-churn
	// ordering). Only the first `half` domains can take a gang right now.
	var wg sync.WaitGroup
	for g := 0; g < gangs; g++ {
		wg.Add(1)
		go func(gi int) {
			defer wg.Done()
			for m := 0; m < gangSize; m++ {
				_, _ = tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, makeGangPod(podName(gi, m), ns, gangName(gi)), metav1.CreateOptions{})
			}
			pg := schedutil.MakePG(gangName(gi), ns, int32(gangSize), nil, nil)
			pg.Annotations = map[string]string{topologyKeyAnnotation: domainLabelKey}
			pg.Spec.ScheduleTimeoutSeconds = &pgTimeout
			_, _ = tc.SchedClient.SchedulingV1alpha1().PodGroups(ns).Create(tc.Ctx, pg, metav1.CreateOptions{})
		}(g)
	}
	wg.Wait()

	// Let the burst finish against partial capacity: ~half the gangs place.
	time.Sleep(8 * time.Second)
	placed := boundCount(tc, ns, "cc-p-")
	t.Logf("after partial-capacity burst: %d/%d pods bound (expect ~%d = %d gangs)", placed, gangs*gangSize, half*gangSize, half)

	// Restore capacity on the zeroed domains — in PARALLEL, matching the event
	// storm (many UpdateNodeAllocatable at once) that hits the scheduler while
	// it is already churning on the no-fit backlog.
	var rwg sync.WaitGroup
	for d := half; d < domains; d++ {
		for n := 0; n < gangSize; n++ {
			rwg.Add(1)
			go func(name string) { defer rwg.Done(); setNodeGPU(t, tc, name, 1) }(nodeName(d, n))
		}
	}
	rwg.Wait()
	t.Logf("capacity restored on %d domains; every gang must now converge", domains-half)

	// Every pod must bind now.
	deadline := time.Now().Add(schedTimeout)
	var mu sync.Mutex
	var firstFail error
	var pwg sync.WaitGroup
	for g := 0; g < gangs; g++ {
		for m := 0; m < gangSize; m++ {
			pwg.Add(1)
			go func(gi, mi int) {
				defer pwg.Done()
				if _, ok := pollPodBound(tc, ns, podName(gi, mi), time.Until(deadline)); !ok {
					mu.Lock()
					if firstFail == nil {
						firstFail = fmt.Errorf("pod %s never bound after capacity restore", podName(gi, mi))
					}
					mu.Unlock()
				}
			}(g, m)
		}
	}
	pwg.Wait()
	if firstFail != nil {
		bound := boundCount(tc, ns, "cc-p-")
		t.Fatalf("did not converge after capacity restore: %d/%d bound: %v", bound, gangs*gangSize, firstFail)
	}
}

// boundCount returns how many pods whose name has the given prefix are bound.
func boundCount(tc *testContext, ns, prefix string) int {
	pods, err := tc.ClientSet.CoreV1().Pods(ns).List(tc.Ctx, metav1.ListOptions{})
	if err != nil {
		return 0
	}
	c := 0
	for i := range pods.Items {
		p := &pods.Items[i]
		if len(p.Name) >= len(prefix) && p.Name[:len(prefix)] == prefix && p.Spec.NodeName != "" {
			c++
		}
	}
	return c
}
