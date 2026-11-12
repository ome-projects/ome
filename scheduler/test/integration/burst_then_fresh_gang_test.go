package integration

import (
	"fmt"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	schedutil "sigs.k8s.io/scheduler-plugins/test/util"
)

// TestBurstThenFreshGangConverges is the never-binds regression: fire a
// burst of gangs at a perfectly-fitting fleet (one gang per domain, minMember 3,
// whole-node), let it finish, then create ONE brand-new gang against a
// physically-empty spare domain. Both the burst and the fresh gang must
// converge.
//
// The symptom this guards is a fresh gang getting PreFilter "no domain
// has room" while whole domains sit empty — the tell of a capacity reservation
// leaked by a burst gang that took a domain but never fully placed there. If a
// leak ever poisoned the shared reservation aggregate globally, the post-burst
// fresh gang would never bind. (Envtest is deterministic and fast, so it does not
// by itself force the leak; this is the convergence backstop, with the leak
// mechanics covered directly by the placement/gc unit tests.)
func TestBurstThenFreshGangConverges(t *testing.T) {
	const (
		domains        = 24
		nodesPerDomain = 3
		gangSize       = 3 // == nodesPerDomain: each gang exactly fills a domain
		gangs          = domains
		schedTimeout   = 90 * time.Second
	)

	tc := startScheduler(t, globalKubeConfig, gangPackOptions(t)...)
	defer tc.teardown(t)

	const ns = "gang-never-binds"
	createNamespace(t, tc, ns)

	domainName := func(d int) string { return fmt.Sprintf("wd-dom-%03d", d) }
	nodeName := func(d, n int) string { return fmt.Sprintf("wd-n-%03d-%02d", d, n) }
	gangName := func(g int) string { return fmt.Sprintf("wd-gang-%03d", g) }
	podName := func(g, m int) string { return fmt.Sprintf("wd-p-%03d-%02d", g, m) }

	// Burst fleet: `domains` domains for the burst, plus one spare EMPTY domain
	// ("wd-dom-fresh") reserved for the post-burst fresh gang.
	for d := 0; d < domains; d++ {
		for n := 0; n < nodesPerDomain; n++ {
			node := makeGPUNode(nodeName(d, n), domainName(d), 1)
			if _, err := tc.ClientSet.CoreV1().Nodes().Create(tc.Ctx, node, metav1.CreateOptions{}); err != nil {
				t.Fatalf("create node %s: %v", node.Name, err)
			}
		}
	}
	for n := 0; n < nodesPerDomain; n++ {
		node := makeGPUNode(fmt.Sprintf("wd-fresh-%02d", n), "wd-dom-fresh", 1)
		if _, err := tc.ClientSet.CoreV1().Nodes().Create(tc.Ctx, node, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create fresh node %s: %v", node.Name, err)
		}
	}

	pgTimeout := int32(schedTimeout.Seconds()) + 60

	// The burst: one goroutine per gang, members created before their PodGroup.
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

	// Every burst pod must bind.
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
						firstFail = fmt.Errorf("burst pod %s never bound within %s", podName(gi, mi), schedTimeout)
					}
					mu.Unlock()
				}
			}(g, m)
		}
	}
	pwg.Wait()
	if firstFail != nil {
		t.Fatalf("burst convergence: %v", firstFail)
	}

	// Now the fresh gang against the empty spare domain. It must place — a leaked
	// reservation from the burst would make PreFilter report "no domain has room".
	for m := 0; m < gangSize; m++ {
		pod := makeGangPod(fmt.Sprintf("wd-fresh-p-%02d", m), ns, "wd-fresh-gang")
		if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, pod, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create fresh pod: %v", err)
		}
	}
	pg := schedutil.MakePG("wd-fresh-gang", ns, int32(gangSize), nil, nil)
	pg.Annotations = map[string]string{topologyKeyAnnotation: domainLabelKey}
	pg.Spec.ScheduleTimeoutSeconds = &pgTimeout
	if _, err := tc.SchedClient.SchedulingV1alpha1().PodGroups(ns).Create(tc.Ctx, pg, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create fresh podgroup: %v", err)
	}
	for m := 0; m < gangSize; m++ {
		name := fmt.Sprintf("wd-fresh-p-%02d", m)
		node := waitForPodBound(t, tc, ns, name, 60*time.Second)
		if d := domainOfNode(t, tc, node); d != "wd-dom-fresh" {
			t.Errorf("fresh pod %s bound to %s (domain %q), want the empty spare domain wd-dom-fresh", name, node, d)
		}
	}
}
