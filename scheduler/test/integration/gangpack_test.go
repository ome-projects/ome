package integration

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	"k8s.io/component-base/metrics/legacyregistry"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler"
	schedconfig "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	fwkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"

	schedclientset "sigs.k8s.io/scheduler-plugins/pkg/generated/clientset/versioned"
	schedutil "sigs.k8s.io/scheduler-plugins/test/util"

	"sigs.k8s.io/ome/scheduler/pkg/plugins/gangpack"
)

// enableGangPack turns on OMEGangPack at every extension point it implements,
// keeping the default plugins otherwise intact.
func enableGangPack(p *schedconfig.Plugins) {
	p.PreFilter.Enabled = append(p.PreFilter.Enabled, schedconfig.Plugin{Name: gangpack.Name})
	p.Filter.Enabled = append(p.Filter.Enabled, schedconfig.Plugin{Name: gangpack.Name})
	p.PostFilter.Enabled = append(p.PostFilter.Enabled, schedconfig.Plugin{Name: gangpack.Name})
	p.Permit.Enabled = append(p.Permit.Enabled, schedconfig.Plugin{Name: gangpack.Name})
	p.Reserve.Enabled = append(p.Reserve.Enabled, schedconfig.Plugin{Name: gangpack.Name})
	p.PostBind.Enabled = append(p.PostBind.Enabled, schedconfig.Plugin{Name: gangpack.Name})
}

// gangPackOptions builds the scheduler.Options that register and enable the
// plugin — the exact wiring cmd/ome-scheduler uses, driven from a component
// config so the profile carries our extension-point enrollment.
func gangPackOptions(t *testing.T) []scheduler.Option {
	t.Helper()
	return gangPackOptionsCollectingEvery(t, 60)
}

// gangPackOptionsCollectingEvery is gangPackOptions with the plugin's periodic
// release of gangs that have no pod left running every gcIntervalSeconds.
func gangPackOptionsCollectingEvery(t *testing.T, gcIntervalSeconds int) []scheduler.Option {
	t.Helper()
	cfg, err := schedutil.NewDefaultSchedulerComponentConfig()
	if err != nil {
		t.Fatalf("default scheduler config: %v", err)
	}
	enableGangPack(cfg.Profiles[0].Plugins)
	cfg.Profiles[0].PluginConfig = append(cfg.Profiles[0].PluginConfig, schedconfig.PluginConfig{
		Name: gangpack.Name,
		Args: &runtime.Unknown{Raw: []byte(`{
			"podGroupTopologyKeyAnnotation":"` + topologyKeyAnnotation + `",
			"unsupportedPlacementGroupLabel":"ome.io/placement-group",
			"defaultPermitTimeoutSeconds":30,
			"podGroupSyncTimeoutSeconds":5,
			"gcIntervalSeconds":` + strconv.Itoa(gcIntervalSeconds) + `
		}`)},
	})
	return []scheduler.Option{
		scheduler.WithProfiles(cfg.Profiles...),
		scheduler.WithFrameworkOutOfTreeRegistry(fwkruntime.Registry{gangpack.Name: gangpack.New}),
	}
}

// TestGangLandsInOneDomain is the happy path: an N-pod gang best-fits into a
// single topology domain and every member binds to a node of that domain.
//
// Layout: domain "a" has 2 gpu nodes, domain "b" has 3. A 2-pod gang fits both,
// so best-fit (fewest free that still fits) picks "a". The gate holds both
// members until the whole gang is present, then binds them — one per node,
// because each node has exactly one gpu.
func TestGangLandsInOneDomain(t *testing.T) {
	tc := startScheduler(t, globalKubeConfig, gangPackOptions(t)...)
	defer tc.teardown(t)

	const ns = "gang-onedomain"
	createNamespace(t, tc, ns)

	// Node names are unique per test: the suite shares one envtest apiserver, and a
	// prior test's bound pods linger on its (deleted) nodes — reusing a node name
	// would resurrect them as phantom occupancy.
	nodes := []struct {
		name, domain string
	}{
		{"od-a1", "a"}, {"od-a2", "a"},
		{"od-b1", "b"}, {"od-b2", "b"}, {"od-b3", "b"},
	}
	for _, n := range nodes {
		if _, err := tc.ClientSet.CoreV1().Nodes().Create(tc.Ctx, makeGPUNode(n.name, n.domain, 1), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create node %s: %v", n.name, err)
		}
	}

	// PodGroup declares the gang: 2 members, domain = the domainLabelKey label.
	pg := schedutil.MakePG("gang", ns, 2, nil, nil)
	pg.Annotations = map[string]string{topologyKeyAnnotation: domainLabelKey}
	if _, err := tc.SchedClient.SchedulingV1alpha1().PodGroups(ns).Create(tc.Ctx, pg, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create podgroup: %v", err)
	}

	podNames := []string{"gp-0", "gp-1"}
	for _, name := range podNames {
		if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, makeGangPod(name, ns, "gang"), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create pod %s: %v", name, err)
		}
	}

	domainA := map[string]bool{"od-a1": true, "od-a2": true}
	bound := map[string]bool{}
	for _, name := range podNames {
		node := waitForPodBound(t, tc, ns, name, 30*time.Second)
		if !domainA[node] {
			t.Errorf("pod %s bound to %s, want a node of domain a (od-a1/od-a2)", name, node)
		}
		if bound[node] {
			t.Errorf("two gang pods bound to the same node %s (whole-node model expects one each)", node)
		}
		bound[node] = true
	}
}

// TestGangGateHoldsUntilComplete is the all-or-nothing guarantee: a gang member
// must NOT bind while its gang is incomplete, and both members bind the moment
// the gang is whole. This is what makes it a gang scheduler rather than a
// bin-packer — without the Permit gate, the first pod would bind immediately and
// leave the workload half-scheduled.
//
// Two nodes in one domain, a 2-member PodGroup. Creating just one pod, it best-
// fits, takes a domain, and gates — but stays unbound. Adding the second opens the gate and
// both bind. The PodGroup's gate timeout is set well above the hold window so the
// test never races the timeout unwind.
func TestGangGateHoldsUntilComplete(t *testing.T) {
	tc := startScheduler(t, globalKubeConfig, gangPackOptions(t)...)
	defer tc.teardown(t)

	const ns = "gang-gate"
	createNamespace(t, tc, ns)

	for _, name := range []string{"g1", "g2"} {
		if _, err := tc.ClientSet.CoreV1().Nodes().Create(tc.Ctx, makeGPUNode(name, "a", 1), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create node %s: %v", name, err)
		}
	}

	pg := schedutil.MakePG("gang", ns, 2, nil, nil)
	pg.Annotations = map[string]string{topologyKeyAnnotation: domainLabelKey}
	// Override MakePG's 10s default so the hold window below can't race the gate
	// timeout (which would unwind the lone member and mask the property).
	longTimeout := int32(120)
	pg.Spec.ScheduleTimeoutSeconds = &longTimeout
	if _, err := tc.SchedClient.SchedulingV1alpha1().PodGroups(ns).Create(tc.Ctx, pg, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create podgroup: %v", err)
	}

	// One of two members: the gate must hold it unbound.
	if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, makeGangPod("gp-0", ns, "gang"), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod gp-0: %v", err)
	}
	ensureNotBound(t, tc, ns, "gp-0", 4*time.Second)

	// Complete the gang: the gate opens and both bind.
	if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, makeGangPod("gp-1", ns, "gang"), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod gp-1: %v", err)
	}
	for _, name := range []string{"gp-0", "gp-1"} {
		node := waitForPodBound(t, tc, ns, name, 30*time.Second)
		if node != "g1" && node != "g2" {
			t.Errorf("pod %s bound to %s, want g1/g2", name, node)
		}
	}
}

func TestLabeledGangWithMissingTopologyStaysPending(t *testing.T) {
	tc := startScheduler(t, globalKubeConfig, gangPackOptions(t)...)
	defer tc.teardown(t)

	const ns = "gang-invalid-topology"
	createNamespace(t, tc, ns)
	if _, err := tc.ClientSet.CoreV1().Nodes().Create(tc.Ctx, makeGPUNode("it1", "a", 1), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create node: %v", err)
	}
	pg := schedutil.MakePG("gang", ns, 1, nil, nil) // deliberately no topology annotation
	if _, err := tc.SchedClient.SchedulingV1alpha1().PodGroups(ns).Create(tc.Ctx, pg, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create podgroup: %v", err)
	}
	if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, makeGangPod("invalid-0", ns, "gang"), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod: %v", err)
	}
	ensureNotBound(t, tc, ns, "invalid-0", 3*time.Second)

	current, err := tc.SchedClient.SchedulingV1alpha1().PodGroups(ns).Get(tc.Ctx, "gang", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get podgroup: %v", err)
	}
	current.Annotations = map[string]string{topologyKeyAnnotation: domainLabelKey}
	if _, err := tc.SchedClient.SchedulingV1alpha1().PodGroups(ns).Update(tc.Ctx, current, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("repair podgroup: %v", err)
	}
	if node := waitForPodBound(t, tc, ns, "invalid-0", 30*time.Second); node != "it1" {
		t.Fatalf("repaired gang bound to %s, want it1", node)
	}
}

func TestHeterogeneousGangChoosesDomainForEveryMember(t *testing.T) {
	tc := startScheduler(t, globalKubeConfig, gangPackOptions(t)...)
	defer tc.teardown(t)

	const ns = "gang-heterogeneous"
	createNamespace(t, tc, ns)
	for _, node := range []struct {
		name, domain string
		gpus         int64
	}{{"ha1", "a", 1}, {"ha2", "a", 1}, {"hb1", "b", 2}, {"hb2", "b", 2}} {
		if _, err := tc.ClientSet.CoreV1().Nodes().Create(tc.Ctx, makeGPUNode(node.name, node.domain, node.gpus), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create node %s: %v", node.name, err)
		}
	}
	pg := schedutil.MakePG("gang", ns, 2, nil, nil)
	pg.Annotations = map[string]string{topologyKeyAnnotation: domainLabelKey}
	if _, err := tc.SchedClient.SchedulingV1alpha1().PodGroups(ns).Create(tc.Ctx, pg, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create podgroup: %v", err)
	}
	small := makeGangPod("hetero-small", ns, "gang")
	large := makeGangPod("hetero-large", ns, "gang")
	large.Spec.Containers[0].Resources.Requests[gpuResource] = *resource.NewQuantity(2, resource.DecimalSI)
	large.Spec.Containers[0].Resources.Limits[gpuResource] = *resource.NewQuantity(2, resource.DecimalSI)
	for _, pod := range []*v1.Pod{small, large} {
		if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, pod, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create pod %s: %v", pod.Name, err)
		}
	}
	for _, name := range []string{small.Name, large.Name} {
		if domain := domainOfNode(t, tc, waitForPodBound(t, tc, ns, name, 30*time.Second)); domain != "b" {
			t.Errorf("pod %s landed in domain %s, want b", name, domain)
		}
	}
}

func TestHeterogeneousGangPreservesConstrainedSiblingNode(t *testing.T) {
	tc := startScheduler(t, globalKubeConfig, gangPackOptions(t)...)
	defer tc.teardown(t)

	const ns = "gang-matching"
	createNamespace(t, tc, ns)
	for _, name := range []string{"match-1", "match-2"} {
		node := makeGPUNode(name, "a", 1)
		node.Labels[v1.LabelHostname] = name
		if _, err := tc.ClientSet.CoreV1().Nodes().Create(tc.Ctx, node, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create node %s: %v", name, err)
		}
	}
	pg := schedutil.MakePG("gang", ns, 2, nil, nil)
	pg.Annotations = map[string]string{topologyKeyAnnotation: domainLabelKey}
	if _, err := tc.SchedClient.SchedulingV1alpha1().PodGroups(ns).Create(tc.Ctx, pg, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create podgroup: %v", err)
	}
	flexible := makeGangPod("a-flexible", ns, "gang")
	flexible.Spec.Affinity = &v1.Affinity{NodeAffinity: &v1.NodeAffinity{PreferredDuringSchedulingIgnoredDuringExecution: []v1.PreferredSchedulingTerm{{
		Weight: 100,
		Preference: v1.NodeSelectorTerm{MatchExpressions: []v1.NodeSelectorRequirement{{
			Key: v1.LabelHostname, Operator: v1.NodeSelectorOpIn, Values: []string{"match-1"},
		}}},
	}}}}
	constrained := makeGangPod("b-constrained", ns, "gang")
	constrained.Spec.NodeSelector = map[string]string{v1.LabelHostname: "match-1"}
	for _, pod := range []*v1.Pod{flexible, constrained} {
		if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, pod, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create pod %s: %v", pod.Name, err)
		}
	}
	if node := waitForPodBound(t, tc, ns, constrained.Name, 30*time.Second); node != "match-1" {
		t.Fatalf("constrained pod bound to %s, want match-1", node)
	}
	if node := waitForPodBound(t, tc, ns, flexible.Name, 30*time.Second); node != "match-2" {
		t.Fatalf("flexible pod bound to %s, want match-2 preserving match-1", node)
	}
}

func TestSplitBoundGangFailsClosed(t *testing.T) {
	tc := startScheduler(t, globalKubeConfig, gangPackOptions(t)...)
	defer tc.teardown(t)

	const ns = "gang-split"
	createNamespace(t, tc, ns)
	for _, node := range []*v1.Node{makeGPUNode("split-a", "a", 2), makeGPUNode("split-b", "b", 2)} {
		if _, err := tc.ClientSet.CoreV1().Nodes().Create(tc.Ctx, node, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create node %s: %v", node.Name, err)
		}
	}
	pg := schedutil.MakePG("gang", ns, 3, nil, nil)
	pg.Annotations = map[string]string{topologyKeyAnnotation: domainLabelKey}
	if _, err := tc.SchedClient.SchedulingV1alpha1().PodGroups(ns).Create(tc.Ctx, pg, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create podgroup: %v", err)
	}
	for name, node := range map[string]string{"bound-a": "split-a", "bound-b": "split-b"} {
		pod := makeGangPod(name, ns, "gang")
		pod.Spec.NodeName = node
		if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, pod, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create split member %s: %v", name, err)
		}
	}
	pending := makeGangPod("pending", ns, "gang")
	if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, pending, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pending member: %v", err)
	}
	ensureNotBound(t, tc, ns, pending.Name, 3*time.Second)
}

// TestAssumedWaitersDoNotOpenPermitGate proves that assumed waiting pods are not
// counted once through the snapshot and again through the waiting set. With only
// two arrivals in a four-member gang, neither pod may bind.
func TestAssumedWaitersDoNotOpenPermitGate(t *testing.T) {
	tc := startScheduler(t, globalKubeConfig, gangPackOptions(t)...)
	defer tc.teardown(t)

	const ns = "gang-permit-count"
	createNamespace(t, tc, ns)
	for _, name := range []string{"pc1", "pc2", "pc3", "pc4"} {
		if _, err := tc.ClientSet.CoreV1().Nodes().Create(tc.Ctx, makeGPUNode(name, "a", 1), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create node %s: %v", name, err)
		}
	}
	pg := schedutil.MakePG("gang", ns, 4, nil, nil)
	pg.Annotations = map[string]string{topologyKeyAnnotation: domainLabelKey}
	timeout := int32(60)
	pg.Spec.ScheduleTimeoutSeconds = &timeout
	if _, err := tc.SchedClient.SchedulingV1alpha1().PodGroups(ns).Create(tc.Ctx, pg, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create podgroup: %v", err)
	}
	for _, name := range []string{"pc-0", "pc-1"} {
		if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, makeGangPod(name, ns, "gang"), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create pod %s: %v", name, err)
		}
	}
	ensureNotBound(t, tc, ns, "pc-0", 3*time.Second)
	ensureNotBound(t, tc, ns, "pc-1", 3*time.Second)
}

// TestIncompleteGangTimesOutThenRecovers is the failure-cleanup guarantee: a
// gang that cannot form must stay unbound — never a member partially bound —
// and must still schedule once it becomes completable.
//
// A 2-member PodGroup with a short gate timeout, but only one pod. PreFilter
// holds the lone member because the live member set is short of minMember, so
// it never reaches the gate, and it stays unbound across a window of several
// gate timeouts. Then the gang is made completable by dropping minMember to 1 —
// a single-pod recovery, so there is no two-pod realignment to race — and the
// member binds. The edit is ordered after the member is observed held, so what
// is exercised is the requeue of a held member on a PodGroup change, not a
// race with its first attempt.
func TestIncompleteGangTimesOutThenRecovers(t *testing.T) {
	tc := startScheduler(t, globalKubeConfig, gangPackOptions(t)...)
	defer tc.teardown(t)

	const ns = "gang-timeout"
	createNamespace(t, tc, ns)

	for _, name := range []string{"t1", "t2"} {
		if _, err := tc.ClientSet.CoreV1().Nodes().Create(tc.Ctx, makeGPUNode(name, "a", 1), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create node %s: %v", name, err)
		}
	}

	pg := schedutil.MakePG("gang", ns, 2, nil, nil)
	pg.Annotations = map[string]string{topologyKeyAnnotation: domainLabelKey}
	// Short gate timeout: the hold window below spans several of them.
	shortTimeout := int32(3)
	gate := time.Duration(shortTimeout) * time.Second
	pg.Spec.ScheduleTimeoutSeconds = &shortTimeout
	if _, err := tc.SchedClient.SchedulingV1alpha1().PodGroups(ns).Create(tc.Ctx, pg, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create podgroup: %v", err)
	}

	if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, makeGangPod("gp-0", ns, "gang"), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod gp-0: %v", err)
	}
	// The lone member is held short of minMember; the edit below is ordered
	// after that.
	waitForPodRejected(t, tc, ns, "gp-0", "waiting for all PodGroup member templates", 10*time.Second)
	// Across two gate timeouts it must never bind.
	ensureNotBound(t, tc, ns, "gp-0", 2*gate)

	// Make the gang completable by the lone member: minMember 2 -> 1. The
	// scheduler requeues the member on the PodGroup change, and the plugin wakes
	// it again once its own PodGroup informer holds the edit, so a retry that
	// reads the new minMember happens whichever watch delivers first.
	cur, err := tc.SchedClient.SchedulingV1alpha1().PodGroups(ns).Get(tc.Ctx, "gang", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get podgroup: %v", err)
	}
	cur.Spec.MinMember = 1
	if _, err := tc.SchedClient.SchedulingV1alpha1().PodGroups(ns).Update(tc.Ctx, cur, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("shrink podgroup minMember: %v", err)
	}

	// The retry runs on the queue's cadence: at once when the member's backoff
	// has lapsed, else after at most the queue's maximum backoff; the budget
	// covers several of those, never the periodic unschedulable flush.
	node := waitForPodBound(t, tc, ns, "gp-0", 30*time.Second)
	if node != "t1" && node != "t2" {
		t.Errorf("pod gp-0 bound to %s, want t1/t2", node)
	}
}

// TestTwoGangsDoNotOverCommitDomain is the capacity-claim guarantee: two gangs
// whose best-fit both prefers the same tight domain must not both pile into it
// and over-commit. When one gang takes a domain it reserves its whole-node
// capacity there, so the other best-fits into what's left.
//
// Domain "a" has 3 nodes, domain "b" has 4. Both gangs need 3, and "a" is the
// tightest fit for 3 — so without the reservation both would take "a", leaving 6
// pods to fight over 3 nodes and leaving the losing gang short of nodes. With it, whichever gang
// takes "a" first reserves all of it; the other lands wholly in "b". The invariant
// asserted (robust to arrival order): every pod binds, each gang sits in a single
// domain, and the two gangs occupy different domains.
func TestTwoGangsDoNotOverCommitDomain(t *testing.T) {
	tc := startScheduler(t, globalKubeConfig, gangPackOptions(t)...)
	defer tc.teardown(t)

	const ns = "gang-nooverlap"
	createNamespace(t, tc, ns)

	// Unique node names per test (shared apiserver — see TestGangLandsInOneDomain).
	nodes := []struct{ name, domain string }{
		{"oc-a1", "a"}, {"oc-a2", "a"}, {"oc-a3", "a"},
		{"oc-b1", "b"}, {"oc-b2", "b"}, {"oc-b3", "b"}, {"oc-b4", "b"},
	}
	for _, n := range nodes {
		if _, err := tc.ClientSet.CoreV1().Nodes().Create(tc.Ctx, makeGPUNode(n.name, n.domain, 1), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create node %s: %v", n.name, err)
		}
	}

	// Two independent 3-member gangs. Generous gate timeout so completion, not a
	// timeout, is what we observe.
	longTimeout := int32(60)
	for _, name := range []string{"ga", "gb"} {
		pg := schedutil.MakePG(name, ns, 3, nil, nil)
		pg.Annotations = map[string]string{topologyKeyAnnotation: domainLabelKey}
		pg.Spec.ScheduleTimeoutSeconds = &longTimeout
		if _, err := tc.SchedClient.SchedulingV1alpha1().PodGroups(ns).Create(tc.Ctx, pg, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create podgroup %s: %v", name, err)
		}
	}

	gangPods := map[string][]string{
		"ga": {"ga-0", "ga-1", "ga-2"},
		"gb": {"gb-0", "gb-1", "gb-2"},
	}
	for pgName, pods := range gangPods {
		for _, name := range pods {
			if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, makeGangPod(name, ns, pgName), metav1.CreateOptions{}); err != nil {
				t.Fatalf("create pod %s: %v", name, err)
			}
		}
	}

	// Resolve each gang's domain set from where its members bound.
	gangDomains := map[string]map[string]bool{}
	for pgName, pods := range gangPods {
		domains := map[string]bool{}
		for _, name := range pods {
			node := waitForPodBound(t, tc, ns, name, 60*time.Second)
			domains[domainOfNode(t, tc, node)] = true
		}
		if len(domains) != 1 {
			t.Errorf("gang %s spans %d domains %v, want exactly 1 (gang integrity)", pgName, len(domains), keys(domains))
		}
		gangDomains[pgName] = domains
	}

	// The two gangs must occupy different domains — else they over-committed one.
	if sameDomain(gangDomains["ga"], gangDomains["gb"]) {
		t.Errorf("both gangs landed in the same domain (over-commit): ga=%v gb=%v",
			keys(gangDomains["ga"]), keys(gangDomains["gb"]))
	}
}

// TestGangRecoversAfterSchedulerRestart is the failover guarantee: after a
// scheduler restart the in-memory assignments are gone, but a gang that was already
// partially placed must still complete — its straggler adopts the domain its
// bound siblings sit in (not a fresh best-fit that could split the gang), and the
// rebuilt reservation accounts for those already-bound members at the gate.
//
// We model "a prior instance placed 2 of a 3-member gang" by creating those two
// pods already assigned to nodes BEFORE starting the scheduler — so its initial
// informer sync (like a real restart) sees them. Then the fresh scheduler must
// bind the third member into the same domain's last free node.
func TestGangRecoversAfterSchedulerRestart(t *testing.T) {
	const ns = "gang-restart"

	// Setup client: build the pre-restart world before any scheduler is running.
	setup := clientset.NewForConfigOrDie(globalKubeConfig)
	schedSetup := schedclientset.NewForConfigOrDie(globalKubeConfig)
	ctx := context.Background()

	if _, err := setup.CoreV1().Namespaces().Create(ctx, namespaceObj(ns), metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create namespace: %v", err)
	}
	// Domain "a" with 3 gpu nodes.
	for _, n := range []string{"ra1", "ra2", "ra3"} {
		if _, err := setup.CoreV1().Nodes().Create(ctx, makeGPUNode(n, "a", 1), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create node %s: %v", n, err)
		}
	}
	// A 3-member PodGroup, generous timeout.
	pg := schedutil.MakePG("gang", ns, 3, nil, nil)
	pg.Annotations = map[string]string{topologyKeyAnnotation: domainLabelKey}
	to := int32(120)
	pg.Spec.ScheduleTimeoutSeconds = &to
	if _, err := schedSetup.SchedulingV1alpha1().PodGroups(ns).Create(ctx, pg, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create podgroup: %v", err)
	}
	// Two members already bound (as if by the previous scheduler instance).
	for _, b := range []struct{ pod, node string }{{"rg-0", "ra1"}, {"rg-1", "ra2"}} {
		p := makeGangPod(b.pod, ns, "gang")
		p.Spec.NodeName = b.node
		if _, err := setup.CoreV1().Pods(ns).Create(ctx, p, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create bound pod %s: %v", b.pod, err)
		}
	}

	// Now bring up a fresh scheduler (fresh, empty assignments) — its informer sync sees
	// the two bound members, exactly as a restarted scheduler would.
	tc := startScheduler(t, globalKubeConfig, gangPackOptions(t)...)
	defer tc.teardown(t)

	// The straggler: must adopt domain a and complete the gang on ra3.
	if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, makeGangPod("rg-2", ns, "gang"), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod rg-2: %v", err)
	}
	node := waitForPodBound(t, tc, ns, "rg-2", 30*time.Second)
	if node != "ra3" {
		t.Errorf("straggler rg-2 bound to %s, want ra3 (adopt domain a + rebuilt gate commitment)", node)
	}
}

// TestSurplusPodJoinsCompletedGang: once a gang has met minMember and bound, a
// further member (a pod beyond minMember, or one recreated into the running gang
// by a controller) must still schedule — the gate counts the already-bound
// siblings of the now-committed gang instead of leaving the newcomer waiting forever. Without
// the committed-gang bound-count, such a pod would wait out its timeout forever
// because it sees zero waiting siblings.
func TestSurplusPodJoinsCompletedGang(t *testing.T) {
	tc := startScheduler(t, globalKubeConfig, gangPackOptions(t)...)
	defer tc.teardown(t)

	const ns = "gang-surplus"
	createNamespace(t, tc, ns)

	for _, n := range []string{"su1", "su2", "su3"} {
		if _, err := tc.ClientSet.CoreV1().Nodes().Create(tc.Ctx, makeGPUNode(n, "a", 1), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create node %s: %v", n, err)
		}
	}
	pg := schedutil.MakePG("gang", ns, 2, nil, nil)
	pg.Annotations = map[string]string{topologyKeyAnnotation: domainLabelKey}
	to := int32(120)
	pg.Spec.ScheduleTimeoutSeconds = &to
	if _, err := tc.SchedClient.SchedulingV1alpha1().PodGroups(ns).Create(tc.Ctx, pg, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create podgroup: %v", err)
	}

	// Form and bind the 2-member gang first (deterministic committed state).
	for _, name := range []string{"sp-0", "sp-1"} {
		if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, makeGangPod(name, ns, "gang"), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create pod %s: %v", name, err)
		}
	}
	for _, name := range []string{"sp-0", "sp-1"} {
		waitForPodBound(t, tc, ns, name, 30*time.Second)
	}

	// Now the surplus 3rd member: must join the committed gang, not wait forever.
	if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, makeGangPod("sp-2", ns, "gang"), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create surplus pod sp-2: %v", err)
	}
	// It must bind (not wait forever) into the gang's domain; which of a's free nodes it
	// lands on is the scheduler's choice.
	node := waitForPodBound(t, tc, ns, "sp-2", 30*time.Second)
	if d := domainOfNode(t, tc, node); d != "a" {
		t.Errorf("surplus pod sp-2 bound to %s (domain %q), want a node of domain a", node, d)
	}
}

// TestNoFitGangReschedulesWhenCapacityFrees is the availability guarantee: a gang
// that fits no domain is Unschedulable (resolvable), so when real capacity frees
// it reschedules PROMPTLY via the requeue hints — it does not wait for the slow
// periodic flush.
//
// Note the FORCE delete (grace 0): envtest has no kubelet, so an ordinary delete
// leaves a pod in Terminating forever and its resources never free — the capacity has
// to be removed for real for this to test anything.
func TestNoFitGangReschedulesWhenCapacityFrees(t *testing.T) {
	const ns = "gang-nofit-recover"

	setup := clientset.NewForConfigOrDie(globalKubeConfig)
	schedSetup := schedclientset.NewForConfigOrDie(globalKubeConfig)
	ctx := context.Background()

	if _, err := setup.CoreV1().Namespaces().Create(ctx, namespaceObj(ns), metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create namespace: %v", err)
	}
	// One domain, two gpu nodes, both occupied by non-gang blockers -> a 2-member
	// gang fits nowhere. Created before the scheduler starts so its first snapshot
	// sees the domain full.
	for _, n := range []string{"rc1", "rc2"} {
		if _, err := setup.CoreV1().Nodes().Create(ctx, makeGPUNode(n, "a", 1), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create node %s: %v", n, err)
		}
	}
	for _, b := range []struct{ pod, node string }{{"rcblk-0", "rc1"}, {"rcblk-1", "rc2"}} {
		p := makeGangPod(b.pod, ns, "") // no pod-group label: a plain gpu hog
		p.Spec.NodeName = b.node
		if _, err := setup.CoreV1().Pods(ns).Create(ctx, p, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create blocker %s: %v", b.pod, err)
		}
	}
	pg := schedutil.MakePG("gang", ns, 2, nil, nil)
	pg.Annotations = map[string]string{topologyKeyAnnotation: domainLabelKey}
	to := int32(600)
	pg.Spec.ScheduleTimeoutSeconds = &to
	if _, err := schedSetup.SchedulingV1alpha1().PodGroups(ns).Create(ctx, pg, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create podgroup: %v", err)
	}

	tc := startScheduler(t, globalKubeConfig, gangPackOptions(t)...)
	defer tc.teardown(t)

	for _, name := range []string{"rc-0", "rc-1"} {
		if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, makeGangPod(name, ns, "gang"), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create gang pod %s: %v", name, err)
		}
	}
	ensureNotBound(t, tc, ns, "rc-0", 3*time.Second) // no-fit while the domain is full

	// Force-free the capacity; the no-fit gang must reschedule promptly.
	zero := int64(0)
	for _, b := range []string{"rcblk-0", "rcblk-1"} {
		if err := tc.ClientSet.CoreV1().Pods(ns).Delete(tc.Ctx, b, metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
			t.Fatalf("force-delete blocker %s: %v", b, err)
		}
	}
	for _, name := range []string{"rc-0", "rc-1"} {
		node := waitForPodBound(t, tc, ns, name, 30*time.Second)
		if node != "rc1" && node != "rc2" {
			t.Errorf("gang pod %s bound to %s, want rc1/rc2", name, node)
		}
	}
}

// heldBackGate keeps a pod out of the scheduler's queue until a test removes
// it, as an admission queue does for a pod it has not let in yet.
const heldBackGate = "testing.example/held-back"

// gangOfEight names the pods of a gang of eight: a leader and
// seven workers.
func gangOfEight(prefix string) []string {
	names := []string{prefix + "-leader-0"}
	for worker := 0; worker < 7; worker++ {
		names = append(names, fmt.Sprintf("%s-worker-%d", prefix, worker))
	}
	return names
}

// createGPUNodes creates count one-gpu nodes of the domain, named <prefix>-<n>.
func createGPUNodes(t *testing.T, tc *testContext, prefix, domain string, count int) []string {
	t.Helper()
	names := make([]string, 0, count)
	for n := 1; n <= count; n++ {
		name := fmt.Sprintf("%s-%d", prefix, n)
		if _, err := tc.ClientSet.CoreV1().Nodes().Create(tc.Ctx, makeGPUNode(name, domain, 1), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create node %s: %v", name, err)
		}
		names = append(names, name)
	}
	return names
}

// createPodGroupOf creates a gang's PodGroup: members pods that land in one
// domain, and timeout as the longest a member waits at the gate.
func createPodGroupOf(t *testing.T, tc *testContext, ns, name string, members int32, timeout time.Duration) {
	t.Helper()
	pg := schedutil.MakePG(name, ns, members, nil, nil)
	pg.Annotations = map[string]string{topologyKeyAnnotation: domainLabelKey}
	seconds := int32(timeout.Seconds())
	pg.Spec.ScheduleTimeoutSeconds = &seconds
	if _, err := tc.SchedClient.SchedulingV1alpha1().PodGroups(ns).Create(tc.Ctx, pg, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create podgroup %s: %v", name, err)
	}
}

// createGangPods creates the named members of the gang. The last heldBack of
// them carry heldBackGate and are created first: the plugin plans a gang once
// a member's cycle sees every pod of it, and a held-back pod has no cycle.
func createGangPods(t *testing.T, tc *testContext, ns, pg string, names []string, heldBack int) {
	t.Helper()
	free := len(names) - heldBack
	for _, i := range append(indexes(free, len(names)), indexes(0, free)...) {
		pod := makeGangPod(names[i], ns, pg)
		if i >= free {
			pod.Spec.SchedulingGates = []v1.PodSchedulingGate{{Name: heldBackGate}}
		}
		if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, pod, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create pod %s: %v", names[i], err)
		}
	}
}

// indexes lists from, from+1, ... up to but not including to.
func indexes(from, to int) []int {
	out := make([]int, 0, to-from)
	for i := from; i < to; i++ {
		out = append(out, i)
	}
	return out
}

// letIn removes heldBackGate from the named pods.
func letIn(t *testing.T, tc *testContext, ns string, names []string) {
	t.Helper()
	pods := tc.ClientSet.CoreV1().Pods(ns)
	for _, name := range names {
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			pod, err := pods.Get(tc.Ctx, name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			pod.Spec.SchedulingGates = nil
			_, err = pods.Update(tc.Ctx, pod, metav1.UpdateOptions{})
			return err
		})
		if err != nil {
			t.Fatalf("remove the gate of pod %s: %v", name, err)
		}
	}
}

// boundNodes polls until every named pod is bound and returns each pod's node.
func boundNodes(t *testing.T, tc *testContext, ns string, names []string, timeout time.Duration) map[string]string {
	t.Helper()
	nodes := make(map[string]string, len(names))
	for _, name := range names {
		nodes[name] = waitForPodBound(t, tc, ns, name, timeout)
	}
	return nodes
}

// boundNow returns the named pods that are bound to a node at this moment.
func boundNow(t *testing.T, tc *testContext, ns string, names []string) []string {
	t.Helper()
	var bound []string
	for _, name := range names {
		pod, err := tc.ClientSet.CoreV1().Pods(ns).Get(tc.Ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get pod %s: %v", name, err)
		}
		if pod.Spec.NodeName != "" {
			bound = append(bound, name)
		}
	}
	return bound
}

// membersAtGate returns, for each pod of the namespace that waits at the gate,
// the node the scheduler holds for it.
func membersAtGate(tc *testContext, ns string) map[string]string {
	held := map[string]string{}
	for _, profile := range tc.Scheduler.Profiles {
		profile.IterateOverWaitingPods(func(wp fwk.WaitingPod) {
			if pod := wp.GetPod(); pod.Namespace == ns {
				held[pod.Name] = pod.Spec.NodeName
			}
		})
	}
	return held
}

// waitForMembersAtGate polls until count pods of the namespace wait at the
// gate, and returns the node held for each.
func waitForMembersAtGate(t *testing.T, tc *testContext, ns string, count int, timeout time.Duration) map[string]string {
	t.Helper()
	var held map[string]string
	err := wait.PollUntilContextTimeout(tc.Ctx, 50*time.Millisecond, timeout, true, func(context.Context) (bool, error) {
		held = membersAtGate(tc, ns)
		return len(held) == count, nil
	})
	if err != nil {
		t.Fatalf("%d pod(s) wait at the gate, want %d: %v", len(held), count, err)
	}
	return held
}

// cachedPods is how many pods the scheduler's cache holds: the bound ones and
// the ones it holds a node for at the gate.
func cachedPods(t *testing.T, tc *testContext) int {
	t.Helper()
	count, err := tc.Scheduler.Cache.PodCount()
	if err != nil {
		t.Fatalf("count the scheduler's cached pods: %v", err)
	}
	return count
}

// waitForCachedPods polls until the scheduler's cache holds count pods, so the
// next scheduling cycle plans on that state.
func waitForCachedPods(t *testing.T, tc *testContext, count int, timeout time.Duration) {
	t.Helper()
	err := wait.PollUntilContextTimeout(tc.Ctx, 50*time.Millisecond, timeout, true, func(context.Context) (bool, error) {
		return cachedPods(t, tc) == count, nil
	})
	if err != nil {
		t.Fatalf("the scheduler's cache holds %d pod(s), want %d: %v", cachedPods(t, tc), count, err)
	}
}

// deleteAtOnce deletes the named pods with no grace period: envtest runs no
// kubelet, so a pod deleted with one would never leave.
func deleteAtOnce(t *testing.T, tc *testContext, ns string, names []string) {
	t.Helper()
	zero := int64(0)
	for _, name := range names {
		if err := tc.ClientSet.CoreV1().Pods(ns).Delete(tc.Ctx, name, metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
			t.Fatalf("delete pod %s: %v", name, err)
		}
	}
}

// nodeHeldByNone returns the first of the nodes that the scheduler holds for
// no pod at the gate.
func nodeHeldByNone(t *testing.T, nodes []string, atGate map[string]string) string {
	t.Helper()
	taken := map[string]bool{}
	for _, node := range atGate {
		taken[node] = true
	}
	for _, node := range nodes {
		if !taken[node] {
			return node
		}
	}
	t.Fatalf("every node of %v is held for a pod at the gate", nodes)
	return ""
}

// TestGangRebuiltWhileFiveMembersAreHeldBack: a gang of eight binds whole on
// the eight nodes of its domain. Every pod of it is then deleted and created
// again under the same PodGroup, as a restart of the whole gang does, and five
// of the new pods are held back before the scheduler's queue. The plugin's
// periodic release does not run in between: its period here is an hour.
//
// The plugin plans the rebuilt gang as a new one, so the three members the
// scheduler can place wait at the gate and no pod of the gang is bound. Once
// the five are let in, the gang binds whole.
func TestGangRebuiltWhileFiveMembersAreHeldBack(t *testing.T) {
	tc := startScheduler(t, globalKubeConfig, gangPackOptionsCollectingEvery(t, 3600)...)
	defer tc.teardown(t)

	const ns = "gang-rebuilt"
	createNamespace(t, tc, ns)
	idle := cachedPods(t, tc)
	createGPUNodes(t, tc, "rb", "a", 8)
	createPodGroupOf(t, tc, ns, "gang", 8, 2*time.Minute)
	members := gangOfEight("rb")
	createGangPods(t, tc, ns, "gang", members, 0)
	boundNodes(t, tc, ns, members, time.Minute)

	deleteAtOnce(t, tc, ns, members)
	waitForCachedPods(t, tc, idle, 30*time.Second)
	createGangPods(t, tc, ns, "gang", members, 5)

	waitForMembersAtGate(t, tc, ns, 3, 30*time.Second)
	if bound := boundNow(t, tc, ns, members); len(bound) != 0 {
		t.Fatalf("pods of a gang with five members held back are bound: %v", bound)
	}

	letIn(t, tc, ns, members[3:])
	boundNodes(t, tc, ns, members, time.Minute)
}

// TestGangFormingWhileAnotherSchedulersPodTakesItsRoom: a new gang of eight is
// planned into domain a, whose eight nodes fit it exactly, while domain b has
// nine. Three members are placed and wait at the gate, and five are held back
// before the scheduler's queue. Another scheduler then binds a pod to a free
// node of domain a, and the five are let in.
//
// The plugin refuses the five because domain a cannot take them all, and binds
// nobody. The three keep their nodes until the gang's timeout passes. Then
// the gang is planned again and binds whole in domain b, which had room for
// it all along.
func TestGangFormingWhileAnotherSchedulersPodTakesItsRoom(t *testing.T) {
	tc := startScheduler(t, globalKubeConfig, gangPackOptions(t)...)
	defer tc.teardown(t)

	const (
		ns          = "gang-room-lost"
		gateTimeout = 15 * time.Second
	)
	createNamespace(t, tc, ns)
	idle := cachedPods(t, tc)
	exactFit := createGPUNodes(t, tc, "rl-a", "a", 8)
	createGPUNodes(t, tc, "rl-b", "b", 9)
	createPodGroupOf(t, tc, ns, "gang", 8, gateTimeout)
	members := gangOfEight("rl")
	createGangPods(t, tc, ns, "gang", members, 5)
	atGate := waitForMembersAtGate(t, tc, ns, 3, 30*time.Second)
	began := time.Now()

	other := preBoundPod("rl-other", ns, nodeHeldByNone(t, exactFit, atGate))
	if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, other, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create the other scheduler's pod: %v", err)
	}
	waitForCachedPods(t, tc, idle+len(atGate)+1, 10*time.Second)
	letIn(t, tc, ns, members[3:])
	waitForPodRejected(t, tc, ns, members[3], "cannot fit all remaining gang members", 30*time.Second)
	if bound := boundNow(t, tc, ns, members); len(bound) != 0 {
		t.Fatalf("members bound while their gang's domain cannot take the rest: %v", bound)
	}

	waitForPodBound(t, tc, ns, members[0], 2*time.Minute)
	held := time.Since(began)
	if held < gateTimeout-2*time.Second {
		t.Fatalf("a member bound %s after three reached the gate, before the gang's timeout of %s", held.Round(time.Millisecond), gateTimeout)
	}
	for name, node := range boundNodes(t, tc, ns, members, time.Minute) {
		if domain := domainOfNode(t, tc, node); domain != "b" {
			t.Errorf("pod %s bound to %s in domain %s, want domain b", name, node, domain)
		}
	}
	t.Logf("three members held their nodes for %s before the gang bound whole in domain b (gang timeout %s)", held.Round(time.Millisecond), gateTimeout)
}

// createPod creates the pod in its namespace.
func createPod(t *testing.T, tc *testContext, pod *v1.Pod) {
	t.Helper()
	if _, err := tc.ClientSet.CoreV1().Pods(pod.Namespace).Create(tc.Ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod %s: %v", pod.Name, err)
	}
}

// TestGangThatKeepsThreeMembersHoldsRoomUntilItsTimeout: a gang of eight is
// bound in a domain of nine nodes. Five of its members are deleted and created
// again while three stay. One replacement is placed and waits at the gate, and
// four are held back before the scheduler's queue. While it waits, the plugin
// keeps the free nodes of the domain for the gang: a pod outside the gang is
// refused with "node is reserved for a forming gang". Another scheduler then
// binds two pods to free nodes, which leaves the domain too little room for
// the four, and the plugin refuses them once they are let in.
//
// The room is held until the gang's timeout passes, and not beyond it. Then
// the waiting replacement goes back and the pod outside the gang binds. The
// plugin refuses the replacements again and holds nothing for them, so a
// second pod outside the gang binds as well. No replacement is bound at any
// time.
func TestGangThatKeepsThreeMembersHoldsRoomUntilItsTimeout(t *testing.T) {
	tc := startScheduler(t, globalKubeConfig, gangPackOptionsCollectingEvery(t, 3600)...)
	defer tc.teardown(t)

	const (
		ns          = "gang-keeps-three"
		gateTimeout = 10 * time.Second
	)
	createNamespace(t, tc, ns)
	idle := cachedPods(t, tc)
	nodes := createGPUNodes(t, tc, "kt", "a", 9)
	createPodGroupOf(t, tc, ns, "gang", 8, gateTimeout)
	members := gangOfEight("kt")
	createGangPods(t, tc, ns, "gang", members, 0)
	bound := boundNodes(t, tc, ns, members, time.Minute)

	kept, replaced := members[:3], members[3:]
	deleteAtOnce(t, tc, ns, replaced)
	waitForCachedPods(t, tc, idle+len(kept), 30*time.Second)
	createGangPods(t, tc, ns, "gang", replaced, 4)
	taken := waitForMembersAtGate(t, tc, ns, 1, 30*time.Second)
	began := time.Now()
	for _, name := range kept {
		taken[name] = bound[name]
	}

	createPod(t, tc, makeStandalonePod("kt-single-1", ns))
	waitForPodRejected(t, tc, ns, "kt-single-1", "node is reserved for a forming gang", 30*time.Second)
	for _, name := range []string{"kt-other-1", "kt-other-2"} {
		taken[name] = nodeHeldByNone(t, nodes, taken)
		createPod(t, tc, preBoundPod(name, ns, taken[name]))
	}
	waitForCachedPods(t, tc, idle+len(taken), 10*time.Second)
	letIn(t, tc, ns, replaced[1:])
	waitForPodRejected(t, tc, ns, replaced[1], "cannot fit all remaining gang members", 30*time.Second)

	waitForPodBound(t, tc, ns, "kt-single-1", 2*time.Minute)
	held := time.Since(began)
	if held < gateTimeout-2*time.Second {
		t.Fatalf("the pod outside the gang bound %s after the replacement reached the gate, before the gang's timeout of %s", held.Round(time.Millisecond), gateTimeout)
	}
	waitForPodRejected(t, tc, ns, replaced[1], "cannot fit remaining gang members", 30*time.Second)
	createPod(t, tc, makeStandalonePod("kt-single-2", ns))
	waitForPodBound(t, tc, ns, "kt-single-2", 30*time.Second)
	if now := boundNow(t, tc, ns, replaced); len(now) != 0 {
		t.Fatalf("replacements bound while their gang's domain cannot take them all: %v", now)
	}
	t.Logf("the plugin kept the domain's free nodes for the gang for %s, until the gang's timeout of %s passed", held.Round(time.Millisecond), gateTimeout)
}

// Priorities of the workloads in the gang preemption tests. The envtest API server runs
// without the Priority admission plugin, so pods carry spec.priority directly.
const (
	lowPriority    int32 = 10
	mediumPriority int32 = 50
	highPriority   int32 = 100
)

// shutdownGrace is the grace period of an evicted pod that shuts down slowly. No
// kubelet runs here, so such a pod stays until the test deletes it with grace
// 0, playing the kubelet.
const shutdownGrace int64 = 30

// preemptionGateTimeout is the gangs' gate timeout: long enough that no gang
// in the gang preemption tests times out and re-plans while a scenario runs.
const preemptionGateTimeout = 120 * time.Second

// gangPreemptionOptions builds the scheduler the ome-scheduler chart renders
// for a pool with gang preemption: OMEGangPack at every extension point, the
// built-in DefaultPreemption off, and a preemption block allowing maxInFlight
// preemptions at once. maxInFlight 0 leaves the block out, which is the
// chart's default: gang preemption off.
func gangPreemptionOptions(t *testing.T, maxInFlight int) []scheduler.Option {
	t.Helper()
	cfg, err := schedutil.NewDefaultSchedulerComponentConfig()
	if err != nil {
		t.Fatalf("default scheduler config: %v", err)
	}
	plugins := cfg.Profiles[0].Plugins
	enableGangPack(plugins)
	plugins.PostFilter.Disabled = append(plugins.PostFilter.Disabled, schedconfig.Plugin{Name: names.DefaultPreemption})
	preemption := ""
	if maxInFlight > 0 {
		preemption = `,"preemption":{"maxInFlight":` + strconv.Itoa(maxInFlight) + `}`
	}
	cfg.Profiles[0].PluginConfig = append(cfg.Profiles[0].PluginConfig, schedconfig.PluginConfig{
		Name: gangpack.Name,
		Args: &runtime.Unknown{Raw: []byte(`{
			"podGroupTopologyKeyAnnotation":"` + topologyKeyAnnotation + `",
			"defaultPermitTimeoutSeconds":30,
			"podGroupSyncTimeoutSeconds":5,
			"gcIntervalSeconds":60` + preemption + `
		}`)},
	})
	return []scheduler.Option{
		scheduler.WithProfiles(cfg.Profiles...),
		scheduler.WithFrameworkOutOfTreeRegistry(fwkruntime.Registry{gangpack.Name: gangpack.New}),
	}
}

// startPreemptionScheduler starts the scheduler and checks that OMEGangPack
// runs at PostFilter and the built-in DefaultPreemption does not, so every
// eviction a test sees is the plugin's. DynamicResources stays: its PostFilter
// only releases resource claims, and no pod here has one.
func startPreemptionScheduler(t *testing.T, maxInFlight int) *testContext {
	t.Helper()
	tc := startScheduler(t, globalKubeConfig, gangPreemptionOptions(t, maxInFlight)...)
	for name, profile := range tc.Scheduler.Profiles {
		enabled := map[string]bool{}
		for _, p := range profile.ListPlugins().PostFilter.Enabled {
			enabled[p.Name] = true
		}
		if !enabled[gangpack.Name] || enabled[names.DefaultPreemption] {
			tc.teardown(t)
			t.Fatalf("profile %s runs PostFilter plugins %v, want %s and not %s", name, keys(enabled), gangpack.Name, names.DefaultPreemption)
		}
	}
	return tc
}

// createGang creates a PodGroup of minMember pods in the test's topology
// dimension, then its pods named <name>-0 ... at the given priority and
// grace period. It does not wait for them to bind.
func createGang(t *testing.T, tc *testContext, ns, name string, minMember int, priority int32, grace int64) []string {
	t.Helper()
	createPodGroupOf(t, tc, ns, name, int32(minMember), preemptionGateTimeout)
	var pods []string
	for i := range minMember {
		pod := makeGangPod(name+"-"+strconv.Itoa(i), ns, name)
		pod.Spec.Priority = &priority
		pod.Spec.TerminationGracePeriodSeconds = &grace
		if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, pod, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create pod %s: %v", pod.Name, err)
		}
		pods = append(pods, pod.Name)
	}
	return pods
}

// bindAll waits for every pod to bind and returns the one domain they share.
func bindAll(t *testing.T, tc *testContext, ns string, pods []string) string {
	t.Helper()
	domains := map[string]bool{}
	for _, name := range pods {
		domains[domainOfNode(t, tc, waitForPodBound(t, tc, ns, name, 30*time.Second))] = true
	}
	if len(domains) != 1 {
		t.Fatalf("pods %v landed in domains %v, want one", pods, keys(domains))
	}
	return keys(domains)[0]
}

// podUIDs reads each pod's UID, so a later check can tell a pod from one
// recreated under its name.
func podUIDs(t *testing.T, tc *testContext, ns string, pods []string) map[string]string {
	t.Helper()
	out := make(map[string]string, len(pods))
	for _, name := range pods {
		p, err := tc.ClientSet.CoreV1().Pods(ns).Get(tc.Ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get pod %s: %v", name, err)
		}
		out[name] = string(p.UID)
	}
	return out
}

// expectUntouched checks that every pod is still there under the same UID
// and is not shutting down.
func expectUntouched(t *testing.T, tc *testContext, ns string, uids map[string]string) {
	t.Helper()
	for name, uid := range uids {
		p, err := tc.ClientSet.CoreV1().Pods(ns).Get(tc.Ctx, name, metav1.GetOptions{})
		switch {
		case err != nil:
			t.Errorf("pod %s: %v, want it untouched", name, err)
		case string(p.UID) != uid || p.DeletionTimestamp != nil:
			t.Errorf("pod %s was evicted (uid %s, deletion %v), want it untouched", name, p.UID, p.DeletionTimestamp)
		}
	}
}

// waitForPodsGone polls until none of the pods exists.
func waitForPodsGone(t *testing.T, tc *testContext, ns string, pods []string, timeout time.Duration) {
	t.Helper()
	err := wait.PollUntilContextTimeout(tc.Ctx, 200*time.Millisecond, timeout, true, func(ctx context.Context) (bool, error) {
		for _, name := range pods {
			if _, err := tc.ClientSet.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("pods %v were not all evicted: %v", pods, err)
	}
}

// waitForPodsEvicting polls until every pod is shutting down, then checks that
// the scheduler marked each one as evicted by its preemption.
func waitForPodsEvicting(t *testing.T, tc *testContext, ns string, pods []string, timeout time.Duration) {
	t.Helper()
	err := wait.PollUntilContextTimeout(tc.Ctx, 200*time.Millisecond, timeout, true, func(ctx context.Context) (bool, error) {
		for _, name := range pods {
			p, err := tc.ClientSet.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
			if err != nil || p.DeletionTimestamp == nil {
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("pods %v were not all evicted: %v", pods, err)
	}
	for _, name := range pods {
		p, err := tc.ClientSet.CoreV1().Pods(ns).Get(tc.Ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get pod %s: %v", name, err)
		}
		marked := false
		for _, c := range p.Status.Conditions {
			if c.Type == v1.DisruptionTarget && c.Status == v1.ConditionTrue && c.Reason == v1.PodReasonPreemptionByScheduler {
				marked = true
			}
		}
		if !marked {
			t.Errorf("evicted pod %s carries no DisruptionTarget condition with reason %s", name, v1.PodReasonPreemptionByScheduler)
		}
	}
}

// finishShutdown deletes the pods with grace 0, as a kubelet does once their
// containers have stopped.
func finishShutdown(t *testing.T, tc *testContext, ns string, pods []string) {
	t.Helper()
	zero := int64(0)
	for _, name := range pods {
		err := tc.ClientSet.CoreV1().Pods(ns).Delete(tc.Ctx, name, metav1.DeleteOptions{GracePeriodSeconds: &zero})
		if err != nil && !apierrors.IsNotFound(err) {
			t.Fatalf("finish shutdown of %s: %v", name, err)
		}
	}
}

// preemptionDecisions reads the plugin's gang_preemption_total counter for one
// result from the scheduler's registry, which every scheduler in this process
// shares.
func preemptionDecisions(t *testing.T, result string) float64 {
	t.Helper()
	families, err := legacyregistry.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() != "ome_scheduler_gang_preemption_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "result" && label.GetValue() == result {
					return metric.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

// waitForPreemptionDecision polls until the counter for result is above before.
func waitForPreemptionDecision(t *testing.T, tc *testContext, result string, before float64, timeout time.Duration) {
	t.Helper()
	err := wait.PollUntilContextTimeout(tc.Ctx, 100*time.Millisecond, timeout, true,
		func(context.Context) (bool, error) { return preemptionDecisions(t, result) > before, nil })
	if err != nil {
		t.Fatalf("no %q preemption decision within %s: %v", result, timeout, err)
	}
}

// createStandalonePod creates a one-accelerator pod in no gang at the given
// priority.
func createStandalonePod(t *testing.T, tc *testContext, ns, name string, priority int32) {
	t.Helper()
	pod := makeGangPod(name, ns, "")
	delete(pod.Labels, podGroupLabel)
	pod.Spec.Priority = &priority
	if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod %s: %v", name, err)
	}
}

// TestGangPreemptionFreesOneDomainForAHigherPriorityGang is a worked
// example. Two domains of four one-accelerator nodes: one runs a
// medium-priority gang of 4, the other two low-priority gangs of 2. A
// high-priority gang of 4 fits neither. The scheduler evicts both low-priority
// gangs, every pod of each, and the new gang binds in their domain. The
// medium-priority gang keeps running untouched.
func TestGangPreemptionFreesOneDomainForAHigherPriorityGang(t *testing.T) {
	tc := startPreemptionScheduler(t, 1)
	defer tc.teardown(t)

	const ns = "gang-preempt"
	createNamespace(t, tc, ns)
	createGPUNodes(t, tc, "gp-a", "a", 4)
	createGPUNodes(t, tc, "gp-b", "b", 4)

	medium := createGang(t, tc, ns, "medium", 4, mediumPriority, 0)
	mediumDomain := bindAll(t, tc, ns, medium)
	lowA := createGang(t, tc, ns, "low-a", 2, lowPriority, 0)
	lowB := createGang(t, tc, ns, "low-b", 2, lowPriority, 0)
	lowDomain := bindAll(t, tc, ns, append(append([]string{}, lowA...), lowB...))
	if lowDomain == mediumDomain {
		t.Fatalf("setup: the low gangs share domain %s with the medium gang", lowDomain)
	}
	mediumUIDs := podUIDs(t, tc, ns, medium)

	high := createGang(t, tc, ns, "high", 4, highPriority, 0)
	waitForPodsGone(t, tc, ns, append(append([]string{}, lowA...), lowB...), 30*time.Second)
	if got := bindAll(t, tc, ns, high); got != lowDomain {
		t.Fatalf("high gang landed in domain %s, want the freed domain %s", got, lowDomain)
	}
	expectUntouched(t, tc, ns, mediumUIDs)
}

// TestGangPreemptionHoldsTheDomainWhileEvictedPodsShutDown: the freed domain has
// one empty node and two nodes whose low-priority gang shuts down slowly. A
// low-priority pod created meanwhile would fit the empty node, but the domain
// is held for the high-priority gang, so it waits. When the evicted pods are gone,
// the high-priority gang binds there and the low-priority pod is still
// waiting.
func TestGangPreemptionHoldsTheDomainWhileEvictedPodsShutDown(t *testing.T) {
	tc := startPreemptionScheduler(t, 1)
	defer tc.teardown(t)

	const ns = "gang-preempt-hold"
	createNamespace(t, tc, ns)
	createGPUNodes(t, tc, "gh-a", "a", 3)
	createGPUNodes(t, tc, "gh-b", "b", 3)

	medium := createGang(t, tc, ns, "medium", 3, mediumPriority, 0)
	mediumDomain := bindAll(t, tc, ns, medium)
	low := createGang(t, tc, ns, "low", 2, lowPriority, shutdownGrace)
	if got := bindAll(t, tc, ns, low); got == mediumDomain {
		t.Fatalf("setup: the low gang shares domain %s with the medium gang", got)
	}
	mediumUIDs := podUIDs(t, tc, ns, medium)

	high := createGang(t, tc, ns, "high", 3, highPriority, 0)
	waitForPodsEvicting(t, tc, ns, low, 30*time.Second)

	createStandalonePod(t, tc, ns, "intruder", lowPriority)
	waitForPodRejected(t, tc, ns, "intruder", "reserved for a forming gang", 30*time.Second)

	finishShutdown(t, tc, ns, low)
	bindAll(t, tc, ns, high)
	ensureNotBound(t, tc, ns, "intruder", 2*time.Second)
	expectUntouched(t, tc, ns, mediumUIDs)
}

// TestGangPreemptionLeavesEqualPriorityGangsAlone: preemption runs only across
// priorities. A gang at the same priority as the running gang waits, and
// nothing is evicted.
func TestGangPreemptionLeavesEqualPriorityGangsAlone(t *testing.T) {
	waitingGangEvictsNothing(t, 1, "gang-preempt-equal", mediumPriority, mediumPriority, "no domain can be freed")
}

// TestGangPreemptionIsOffByDefault: without a preemption block the plugin
// never evicts. A high-priority gang waits for capacity, exactly as the chart's
// default scheduler does.
func TestGangPreemptionIsOffByDefault(t *testing.T) {
	waitingGangEvictsNothing(t, 0, "gang-preempt-off", lowPriority, highPriority, "no domain has room")
}

// waitingGangEvictsNothing fills one domain of two nodes with a running gang,
// then creates a second gang that needs the same room. The second gang must be
// rejected for reason and stay unbound, and the running gang must keep running.
func waitingGangEvictsNothing(t *testing.T, maxInFlight int, ns string, runningPriority, waitingPriority int32, reason string) {
	t.Helper()
	tc := startPreemptionScheduler(t, maxInFlight)
	defer tc.teardown(t)

	createNamespace(t, tc, ns)
	createGPUNodes(t, tc, ns+"-a", "a", 2)

	running := createGang(t, tc, ns, "running", 2, runningPriority, 0)
	bindAll(t, tc, ns, running)
	uids := podUIDs(t, tc, ns, running)

	waiting := createGang(t, tc, ns, "waiting", 2, waitingPriority, 0)
	waitForPodRejected(t, tc, ns, waiting[0], reason, 30*time.Second)
	ensureNotBound(t, tc, ns, waiting[0], 3*time.Second)
	expectUntouched(t, tc, ns, uids)
}

// TestGangPreemptionSurvivesASchedulerRestart: the scheduler evicts a slow
// low-priority gang and then restarts, losing the domain it held in memory. The
// new scheduler counts the pods still shutting down as leaving, so it holds
// the same domain again without evicting anyone new: the medium-priority gang
// keeps running, a low-priority pod is kept out of the held domain, and the
// high-priority gang binds there once the evicted pods are gone.
func TestGangPreemptionSurvivesASchedulerRestart(t *testing.T) {
	first := startPreemptionScheduler(t, 1)
	defer first.teardown(t)

	const ns = "gang-preempt-restart"
	createNamespace(t, first, ns)
	createGPUNodes(t, first, "gr-a", "a", 3)
	createGPUNodes(t, first, "gr-b", "b", 3)

	medium := createGang(t, first, ns, "medium", 3, mediumPriority, 0)
	mediumDomain := bindAll(t, first, ns, medium)
	low := createGang(t, first, ns, "low", 2, lowPriority, shutdownGrace)
	if got := bindAll(t, first, ns, low); got == mediumDomain {
		t.Fatalf("setup: the low gang shares domain %s with the medium gang", got)
	}
	mediumUIDs := podUIDs(t, first, ns, medium)

	high := createGang(t, first, ns, "high", 3, highPriority, 0)
	waitForPodsEvicting(t, first, ns, low, 30*time.Second)
	first.stop(t)

	heldBefore := preemptionDecisions(t, "held")
	second := startPreemptionScheduler(t, 1)
	defer second.teardown(t)
	waitForPreemptionDecision(t, second, "held", heldBefore, 30*time.Second)

	createStandalonePod(t, second, ns, "intruder", lowPriority)
	waitForPodRejected(t, second, ns, "intruder", "reserved for a forming gang", 30*time.Second)

	finishShutdown(t, second, ns, low)
	bindAll(t, second, ns, high)
	ensureNotBound(t, second, ns, "intruder", 2*time.Second)
	expectUntouched(t, second, ns, mediumUIDs)
}
