package integration

import (
	"fmt"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	schedutil "sigs.k8s.io/scheduler-plugins/test/util"
)

// TestFailoverMidBurstConverges probes the one factor a single-scheduler burst
// test cannot: a leader failover WHILE a burst is still forming. Three replicas
// with leader election means the active scheduler can change mid-flight; the new
// leader starts with an empty in-memory assignment store and must rebuild from the
// cluster. Members that were assumed but not yet bound by the old leader are lost
// (assume is in-memory), so their gangs re-plan on the new leader. This asserts the
// whole fleet still converges — no gang short of a node, no domain over-committed — after
// the assignment store is reset under contention.
//
// Reproduced by starting one scheduler, firing the burst, then cancelling it and
// starting a fresh one (empty assignments) while binds are still in flight.
func TestFailoverMidBurstConverges(t *testing.T) {
	const (
		domains        = 16
		nodesPerDomain = 3
		gangSize       = 3
		gangs          = domains
		schedTimeout   = 90 * time.Second
	)

	const ns = "gang-failover"
	// Namespace must exist before either scheduler; create via a throwaway client.
	tc0 := startScheduler(t, globalKubeConfig, gangPackOptions(t)...)
	createNamespace(t, tc0, ns)

	domainName := func(d int) string { return fmt.Sprintf("fo-dom-%03d", d) }
	nodeName := func(d, n int) string { return fmt.Sprintf("fo-n-%03d-%02d", d, n) }
	gangName := func(g int) string { return fmt.Sprintf("fo-gang-%03d", g) }
	podName := func(g, m int) string { return fmt.Sprintf("fo-p-%03d-%02d", g, m) }

	for d := 0; d < domains; d++ {
		for n := 0; n < nodesPerDomain; n++ {
			if _, err := tc0.ClientSet.CoreV1().Nodes().Create(tc0.Ctx, makeGPUNode(nodeName(d, n), domainName(d), 1), metav1.CreateOptions{}); err != nil {
				t.Fatalf("create node: %v", err)
			}
		}
	}

	pgTimeout := int32(schedTimeout.Seconds()) + 60
	var wg sync.WaitGroup
	for g := 0; g < gangs; g++ {
		wg.Add(1)
		go func(gi int) {
			defer wg.Done()
			for m := 0; m < gangSize; m++ {
				_, _ = tc0.ClientSet.CoreV1().Pods(ns).Create(tc0.Ctx, makeGangPod(podName(gi, m), ns, gangName(gi)), metav1.CreateOptions{})
			}
			pg := schedutil.MakePG(gangName(gi), ns, int32(gangSize), nil, nil)
			pg.Annotations = map[string]string{topologyKeyAnnotation: domainLabelKey}
			pg.Spec.ScheduleTimeoutSeconds = &pgTimeout
			_, _ = tc0.SchedClient.SchedulingV1alpha1().PodGroups(ns).Create(tc0.Ctx, pg, metav1.CreateOptions{})
		}(g)
	}
	wg.Wait()

	// Failover mid-burst: let a little progress happen, then kill the leader and
	// start a fresh scheduler (empty assignment store) to finish the job.
	time.Sleep(400 * time.Millisecond)
	tc0.stop(t)
	tc := startScheduler(t, globalKubeConfig, gangPackOptions(t)...)
	defer tc.teardown(t)

	deadline := time.Now().Add(schedTimeout)
	var mu sync.Mutex
	var firstFail error
	var pwg sync.WaitGroup
	results := make([]string, gangs*gangSize)
	for g := 0; g < gangs; g++ {
		for m := 0; m < gangSize; m++ {
			pwg.Add(1)
			go func(gi, mi int) {
				defer pwg.Done()
				node, ok := pollPodBound(tc, ns, podName(gi, mi), time.Until(deadline))
				if !ok {
					mu.Lock()
					if firstFail == nil {
						firstFail = fmt.Errorf("pod %s never bound after failover within %s", podName(gi, mi), schedTimeout)
					}
					mu.Unlock()
					return
				}
				results[gi*gangSize+mi] = node
			}(g, m)
		}
	}
	pwg.Wait()
	if firstFail != nil {
		t.Fatalf("failover convergence: %v", firstFail)
	}

	// No node may hold more than one gang pod (over-commit the failover could cause
	// if the rebuilt assignment store lost a reservation and two gangs raced one domain).
	perNode := map[string]int{}
	for _, n := range results {
		perNode[n]++
	}
	for n, c := range perNode {
		if c > 1 {
			t.Errorf("node %s holds %d gang pods after failover, want <=1 (over-commit)", n, c)
		}
	}
}
