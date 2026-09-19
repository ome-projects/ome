package integration

import (
	"fmt"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	schedutil "sigs.k8s.io/scheduler-plugins/test/util"
)

// TestGangMembersCreatedBeforePodGroupHold covers the PodGroup-cache-lag gap:
// when a gang's pods are created BEFORE (or concurrently with) their PodGroup —
// the common case when a controller applies the whole workload at once — the
// scheduler must not bind a lone member. If PreFilter Skipped an
// unresolvable-PodGroup pod, it would bind immediately with no gate and no
// domain pin, breaking all-or-nothing; instead such a member is held and
// requeues when the PodGroup appears.
//
// Two nodes in one domain, a 2-member gang. The pods are created first; they must
// stay unbound. Once the PodGroup is created, the gang forms and both bind.
func TestGangMembersCreatedBeforePodGroupHold(t *testing.T) {
	tc := startScheduler(t, globalKubeConfig, gangPackOptions(t)...)
	defer tc.teardown(t)

	const ns = "gang-pglag"
	createNamespace(t, tc, ns)

	for _, n := range []string{"pl-a1", "pl-a2"} {
		if _, err := tc.ClientSet.CoreV1().Nodes().Create(tc.Ctx, makeGPUNode(n, "a", 1), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create node %s: %v", n, err)
		}
	}

	// Pods first, PodGroup deliberately absent.
	podNames := []string{"pl-0", "pl-1"}
	for _, name := range podNames {
		if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, makeGangPod(name, ns, "pglag"), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create pod %s: %v", name, err)
		}
	}

	// No member may bind while its PodGroup is unresolvable — otherwise a lone gang
	// member runs with no gate. The scheduler has ample time to try.
	for _, name := range podNames {
		ensureNotBound(t, tc, ns, name, 3*time.Second)
	}

	// The PodGroup appears: the held members requeue and the gang forms in domain a.
	pg := schedutil.MakePG("pglag", ns, 2, nil, nil)
	pg.Annotations = map[string]string{topologyKeyAnnotation: domainLabelKey}
	if _, err := tc.SchedClient.SchedulingV1alpha1().PodGroups(ns).Create(tc.Ctx, pg, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create podgroup: %v", err)
	}

	bound := map[string]bool{}
	for _, name := range podNames {
		node := waitForPodBound(t, tc, ns, name, 30*time.Second)
		if bound[node] {
			t.Errorf("two gang pods bound to the same node %s (whole-node model expects one each)", node)
		}
		bound[node] = true
	}
}

// TestConcurrentGangBurst drives the burst shape that can wedge a scheduler:
// many gangs whose PodGroups and pods are all created at once, from many
// goroutines, with the pods racing ahead of their PodGroups. This stresses two
// paths the serial suite never exercises together — the PodGroup-cache-lag
// hold/requeue, and the domain-contention race where a gang can pin a domain
// that then fills. Every gang must still converge: all members bind, and each
// gang lands in exactly one domain. A regression (a wedged gang with free
// capacity, or an over-committed domain) fails via the never-binds wait or the
// single-domain assertion.
//
// Packed shape (one gang per domain, gang fills its domain) so correctness is
// unambiguous, matching TestScaleManyGangsAcrossManyDomains.
func TestConcurrentGangBurst(t *testing.T) {
	const (
		domains        = 16
		nodesPerDomain = 2
		gangSize       = 2 // == nodesPerDomain: each gang exactly fills a domain
		gangs          = domains
		schedTimeout   = 90 * time.Second
	)

	tc := startScheduler(t, globalKubeConfig, gangPackOptions(t)...)
	defer tc.teardown(t)

	const ns = "gang-burst"
	createNamespace(t, tc, ns)

	domainName := func(d int) string { return fmt.Sprintf("cb-dom-%03d", d) }
	nodeName := func(d, n int) string { return fmt.Sprintf("cb-n-%03d-%02d", d, n) }
	gangName := func(g int) string { return fmt.Sprintf("cb-gang-%03d", g) }
	podName := func(g, m int) string { return fmt.Sprintf("cb-p-%03d-%02d", g, m) }

	// Nodes first (serially) so the whole fleet's capacity exists before the burst.
	for d := 0; d < domains; d++ {
		for n := 0; n < nodesPerDomain; n++ {
			node := makeGPUNode(nodeName(d, n), domainName(d), 1)
			if _, err := tc.ClientSet.CoreV1().Nodes().Create(tc.Ctx, node, metav1.CreateOptions{}); err != nil {
				t.Fatalf("create node %s: %v", node.Name, err)
			}
		}
	}

	pgTimeout := int32(schedTimeout.Seconds()) + 60

	// The burst: one goroutine per gang, each creating its members BEFORE its
	// PodGroup, all firing concurrently. Pods racing ahead of the PodGroup is the
	// cache-lag path; many gangs at once is the contention path.
	var wg sync.WaitGroup
	errCh := make(chan error, gangs)
	for g := 0; g < gangs; g++ {
		wg.Add(1)
		go func(gi int) {
			defer wg.Done()
			for m := 0; m < gangSize; m++ {
				pod := makeGangPod(podName(gi, m), ns, gangName(gi))
				if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, pod, metav1.CreateOptions{}); err != nil {
					errCh <- fmt.Errorf("create pod %s: %w", pod.Name, err)
					return
				}
			}
			pg := schedutil.MakePG(gangName(gi), ns, int32(gangSize), nil, nil)
			pg.Annotations = map[string]string{topologyKeyAnnotation: domainLabelKey}
			pg.Spec.ScheduleTimeoutSeconds = &pgTimeout
			if _, err := tc.SchedClient.SchedulingV1alpha1().PodGroups(ns).Create(tc.Ctx, pg, metav1.CreateOptions{}); err != nil {
				errCh <- fmt.Errorf("create podgroup %s: %w", pg.Name, err)
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("burst setup: %v", err)
	}

	// Every pod must bind — a wedged gang (pinned to a full domain with free
	// capacity elsewhere, never re-planning) would time out here.
	type result struct {
		gang int
		node string
	}
	results := make([]result, gangs*gangSize)
	deadline := time.Now().Add(schedTimeout)
	var mu sync.Mutex
	var firstFail error
	var pwg sync.WaitGroup
	for g := 0; g < gangs; g++ {
		for m := 0; m < gangSize; m++ {
			pwg.Add(1)
			go func(gi, mi int) {
				defer pwg.Done()
				node, ok := pollPodBound(tc, ns, podName(gi, mi), time.Until(deadline))
				if !ok {
					mu.Lock()
					if firstFail == nil {
						firstFail = fmt.Errorf("pod %s never bound within %s (possible wedged gang / over-commit)", podName(gi, mi), schedTimeout)
					}
					mu.Unlock()
					return
				}
				results[gi*gangSize+mi] = result{gang: gi, node: node}
			}(g, m)
		}
	}
	pwg.Wait()
	if firstFail != nil {
		t.Fatalf("convergence: %v", firstFail)
	}

	// Correctness: each gang in exactly one domain; no node holds >1 pod.
	nodeDomain := map[string]string{}
	resolve := func(node string) string {
		if d, ok := nodeDomain[node]; ok {
			return d
		}
		d := domainOfNode(t, tc, node)
		nodeDomain[node] = d
		return d
	}
	gangDomains := make([]map[string]bool, gangs)
	for i := range gangDomains {
		gangDomains[i] = map[string]bool{}
	}
	nodePodCount := map[string]int{}
	for _, r := range results {
		gangDomains[r.gang][resolve(r.node)] = true
		nodePodCount[r.node]++
	}
	for gi := 0; gi < gangs; gi++ {
		if len(gangDomains[gi]) != 1 {
			t.Errorf("gang %s spans %d domains %v, want exactly 1", gangName(gi), len(gangDomains[gi]), keys(gangDomains[gi]))
		}
	}
	for node, count := range nodePodCount {
		if count > 1 {
			t.Errorf("node %s holds %d gang pods, want <=1 (whole-node model)", node, count)
		}
	}
}
