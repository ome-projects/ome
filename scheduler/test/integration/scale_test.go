package integration

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	schedutil "sigs.k8s.io/scheduler-plugins/test/util"
)

// TestScaleManyGangsAcrossManyDomains is the macro scale proof for OMEGangPack:
// a large synthetic accelerator fleet, many gangs competing for domains, driven
// through the REAL in-process kube-scheduler, asserting the plugin's placement
// contract still holds at scale and reporting time-to-schedule-all + throughput.
//
// Cluster shape (each dimension env-overridable — see below):
//
//   - scaleDomains domains, each with scaleNodesPerDomain single-gpu nodes.
//   - scaleGangs gangs, each a PodGroup of scaleGangSize members (one gpu each,
//     so one pod per node — the whole-node gang model).
//
// The shape is deliberately packed so correctness is unambiguous: each domain
// holds exactly one gang (scaleNodesPerDomain == scaleGangSize) and there are
// exactly as many gangs as domains. BestFit therefore has to spread the gangs one
// per domain; the capacity reservation is what prevents two gangs racing into the
// same domain and leaving one short of nodes. If the plugin ever over-committed a domain,
// some gang would be short a node and never fully bind — the wall-clock wait would
// time out and fail, and the per-domain occupancy assertion below would trip.
//
// Correctness asserted at scale (all robust to arrival order):
//   - every gang pod binds (all-or-nothing gate opened for every gang);
//   - each gang lands in exactly ONE domain (gang integrity);
//   - no two gangs share a domain, and no domain holds more pods than it has nodes
//     (no over-commit);
//   - no node holds more than one gang pod (whole-node model).
//
// DIALING UP TOWARD ~20k PODS
// The defaults (below) are a moderate size that finishes well under ~90s on a
// laptop-class machine against envtest. Every dimension is an env var; total pods
// == scaleGangs * scaleGangSize == scaleDomains * scaleNodesPerDomain. To push
// toward a large-fleet target, e.g.:
//
//	OME_SCALE_DOMAINS=1000 OME_SCALE_NODES_PER_DOMAIN=20 \
//	OME_SCALE_GANGS=1000  OME_SCALE_GANG_SIZE=20 \
//	OME_SCALE_TIMEOUT=15m \
//	  go test ./test/integration/... -run TestScaleManyGangsAcrossManyDomains -v -timeout 20m
//
// gives 20000 nodes and 20000 pods across 1000 domains. Note the envtest apiserver
// (not the plugin) becomes the bottleneck at that size — object creation and the
// bind-write storm dominate; keep the -timeout generous and expect several minutes.
// Because the invariant is "domains == gangs, gang fills a domain", keep
// OME_SCALE_DOMAINS == OME_SCALE_GANGS and OME_SCALE_NODES_PER_DOMAIN ==
// OME_SCALE_GANG_SIZE, or the packed-correctness assumption no longer holds.
func TestScaleManyGangsAcrossManyDomains(t *testing.T) {
	var (
		domains        = envInt(t, "OME_SCALE_DOMAINS", 60)
		nodesPerDomain = envInt(t, "OME_SCALE_NODES_PER_DOMAIN", 4)
		gangs          = envInt(t, "OME_SCALE_GANGS", 60)
		gangSize       = envInt(t, "OME_SCALE_GANG_SIZE", 4)
		schedTimeout   = envDuration(t, "OME_SCALE_TIMEOUT", 90*time.Second)
	)

	// The packed shape the correctness assertions assume: one gang per domain,
	// each gang exactly filling a domain. Guard it so a mis-dialed run fails loud
	// rather than silently proving nothing.
	if gangs != domains {
		t.Fatalf("scale invariant: gangs (%d) must equal domains (%d) — one gang per domain", gangs, domains)
	}
	if gangSize != nodesPerDomain {
		t.Fatalf("scale invariant: gangSize (%d) must equal nodesPerDomain (%d) — each gang fills its domain", gangSize, nodesPerDomain)
	}
	if gangs <= 0 || gangSize <= 0 {
		t.Fatalf("scale dimensions must be positive: gangs=%d gangSize=%d", gangs, gangSize)
	}

	totalNodes := domains * nodesPerDomain
	totalPods := gangs * gangSize
	t.Logf("scale shape: %d domains x %d nodes/domain = %d nodes; %d gangs x %d members = %d pods",
		domains, nodesPerDomain, totalNodes, gangs, gangSize, totalPods)

	tc := startScheduler(t, globalKubeConfig, gangPackOptions(t)...)
	defer tc.teardown(t)

	const ns = "gang-scale"
	createNamespace(t, tc, ns)

	// domainName / nodeName / gangName / podName build collision-free identifiers.
	// Node names are unique per test run because the suite shares one envtest
	// apiserver (see TestGangLandsInOneDomain) — the "sc-" prefix keeps this test's
	// nodes out of every other test's snapshot.
	domainName := func(d int) string { return fmt.Sprintf("sc-dom-%05d", d) }
	nodeName := func(d, n int) string { return fmt.Sprintf("sc-n-%05d-%03d", d, n) }
	gangName := func(gi int) string { return fmt.Sprintf("sc-gang-%05d", gi) }
	podName := func(gi, m int) string { return fmt.Sprintf("sc-p-%05d-%03d", gi, m) }

	// --- Build the fleet: nodes first, then PodGroups, then pods. -------------
	// Nodes and PodGroups must exist before the pods so the scheduler's first look
	// at a gang pod already sees its whole domain and its PodGroup facts.
	setupStart := time.Now()

	for d := 0; d < domains; d++ {
		dom := domainName(d)
		for n := 0; n < nodesPerDomain; n++ {
			node := makeGPUNode(nodeName(d, n), dom, 1)
			if _, err := tc.ClientSet.CoreV1().Nodes().Create(tc.Ctx, node, metav1.CreateOptions{}); err != nil {
				t.Fatalf("create node %s: %v", node.Name, err)
			}
		}
	}
	t.Logf("created %d nodes in %s", totalNodes, time.Since(setupStart).Round(time.Millisecond))

	// One PodGroup per gang: minMember = gangSize, domain label declared via the
	// topology-key annotation. Generous gate timeout — completion, not a timeout, is
	// what a healthy run observes; a gang short of a node (over-commit) would still fail
	// via the never-binds wall-clock wait below.
	pgTimeout := int32(schedTimeout.Seconds()) + 60
	for gi := 0; gi < gangs; gi++ {
		pg := schedutil.MakePG(gangName(gi), ns, int32(gangSize), nil, nil)
		pg.Annotations = map[string]string{topologyKeyAnnotation: domainLabelKey}
		pg.Spec.ScheduleTimeoutSeconds = &pgTimeout
		if _, err := tc.SchedClient.SchedulingV1alpha1().PodGroups(ns).Create(tc.Ctx, pg, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create podgroup %s: %v", pg.Name, err)
		}
	}
	t.Logf("created %d podgroups", gangs)

	// --- Fire all pods, then start the clock. ---------------------------------
	// The scheduler is already running, so it begins placing as pods land; we time
	// from the first pod create to the last bind — the end-to-end schedule-all
	// latency the plugin owns (plus envtest write cost, called out in the report).
	scheduleStart := time.Now()
	for gi := 0; gi < gangs; gi++ {
		gn := gangName(gi)
		for m := 0; m < gangSize; m++ {
			pod := makeGangPod(podName(gi, m), ns, gn)
			if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, pod, metav1.CreateOptions{}); err != nil {
				t.Fatalf("create pod %s: %v", pod.Name, err)
			}
		}
	}
	t.Logf("created %d pods in %s; waiting for all to bind (timeout %s)",
		totalPods, time.Since(scheduleStart).Round(time.Millisecond), schedTimeout)

	// --- Wait for every pod to bind (bounded, parallel polling). --------------
	// waitForPodBound is per-pod; at scale we fan it out with a bounded worker pool
	// so the poll loop itself doesn't serialize into the wall-clock measurement.
	// Each worker records the node its pod bound to; a failure to bind within
	// schedTimeout fails the test (the shortfall a domain over-commit would produce).
	type result struct {
		gang int
		node string
	}
	results := make([]result, totalPods)
	deadline := time.Now().Add(schedTimeout)

	const workers = 32
	jobs := make(chan int, totalPods)
	var wg sync.WaitGroup
	var failMu sync.Mutex
	var firstFail error

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				gi := idx / gangSize
				m := idx % gangSize
				name := podName(gi, m)
				node, ok := pollPodBound(tc, ns, name, time.Until(deadline))
				if !ok {
					failMu.Lock()
					if firstFail == nil {
						firstFail = fmt.Errorf("pod %s/%s never bound within %s (possible domain over-commit, a gang short of a node)", ns, name, schedTimeout)
					}
					failMu.Unlock()
					continue
				}
				results[idx] = result{gang: gi, node: node}
			}
		}()
	}
	for idx := 0; idx < totalPods; idx++ {
		jobs <- idx
	}
	close(jobs)
	wg.Wait()

	if firstFail != nil {
		t.Fatalf("schedule-all did not complete: %v", firstFail)
	}

	elapsed := time.Since(scheduleStart)
	throughput := float64(totalPods) / elapsed.Seconds()
	t.Logf("SCALE RESULT: scheduled %d pods (%d gangs across %d domains) in %s => %.1f pods/sec",
		totalPods, gangs, domains, elapsed.Round(time.Millisecond), throughput)

	// --- Correctness at scale. ------------------------------------------------
	// Resolve every bound node's domain once (cache node->domain to avoid one GET
	// per pod), then reduce over the results.
	nodeDomain := make(map[string]string, totalNodes)
	resolveDomain := func(node string) string {
		if d, ok := nodeDomain[node]; ok {
			return d
		}
		d := domainOfNode(t, tc, node)
		nodeDomain[node] = d
		return d
	}

	gangDomains := make([]map[string]bool, gangs) // per-gang set of domains it touched
	for i := range gangDomains {
		gangDomains[i] = map[string]bool{}
	}
	domainPodCount := map[string]int{} // pods packed into each domain
	nodePodCount := map[string]int{}   // pods packed onto each node

	for _, r := range results {
		dom := resolveDomain(r.node)
		gangDomains[r.gang][dom] = true
		domainPodCount[dom]++
		nodePodCount[r.node]++
	}

	// 1) Gang integrity: each gang lands in exactly one domain.
	for gi := 0; gi < gangs; gi++ {
		if len(gangDomains[gi]) != 1 {
			t.Errorf("gang %s spans %d domains %v, want exactly 1 (gang integrity)",
				gangName(gi), len(gangDomains[gi]), keys(gangDomains[gi]))
		}
	}

	// 2) No over-commit: no domain holds more pods than it has nodes, and (given the
	//    packed shape) each occupied domain holds exactly one gang's worth.
	for dom, count := range domainPodCount {
		if count > nodesPerDomain {
			t.Errorf("domain %s over-committed: %d pods on %d nodes", dom, count, nodesPerDomain)
		}
	}

	// 3) Whole-node model: no node holds more than one gang pod.
	for node, count := range nodePodCount {
		if count > 1 {
			t.Errorf("node %s holds %d gang pods, want <=1 (whole-node model)", node, count)
		}
	}

	// 4) One gang per domain (the packed-shape corollary of no-over-commit): with
	//    gangs == domains and each gang filling a domain, every domain must hold
	//    exactly one gang. Distinct occupied domains == gangs proves no two gangs
	//    shared a domain.
	occupied := map[string]bool{}
	for gi := 0; gi < gangs; gi++ {
		for d := range gangDomains[gi] {
			if occupied[d] {
				t.Errorf("domain %s shared by more than one gang (over-commit)", d)
			}
			occupied[d] = true
		}
	}
	if len(occupied) != gangs {
		t.Errorf("gangs occupy %d distinct domains, want %d (one gang per domain)", len(occupied), gangs)
	}
}

// pollPodBound waits up to window for the named pod to have a node assigned,
// returning (node, true) on success or ("", false) on timeout. It is the
// non-fatal sibling of waitForPodBound — a scale worker must record the failure
// and let the test aggregate it, not t.Fatalf from a goroutine.
func pollPodBound(tc *testContext, ns, name string, window time.Duration) (string, bool) {
	if window <= 0 {
		window = time.Second
	}
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		p, err := tc.ClientSet.CoreV1().Pods(ns).Get(tc.Ctx, name, metav1.GetOptions{})
		if err == nil && p.Spec.NodeName != "" {
			return p.Spec.NodeName, true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return "", false
}

// envInt reads a positive integer from an env var, or returns def when unset.
func envInt(t *testing.T, key string, def int) int {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		t.Fatalf("env %s=%q: not an integer: %v", key, v, err)
	}
	return n
}

// envDuration reads a Go duration (e.g. "90s", "15m") from an env var, or returns
// def when unset.
func envDuration(t *testing.T, key string, def time.Duration) time.Duration {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		t.Fatalf("env %s=%q: not a duration: %v", key, v, err)
	}
	return d
}
