package integration

import (
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/scheduler"
	schedconfig "k8s.io/kubernetes/pkg/scheduler/apis/config"
	fwkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"

	schedutil "sigs.k8s.io/scheduler-plugins/test/util"

	"sigs.k8s.io/ome/scheduler/pkg/plugins/gangpack"
)

// standalonePackOptions enables OMEGangPack including its Score/PreScore points
// and isolates it as the deciding scorer (disable the default spreading/fitting
// scores) so the domain-packing effect is observable — mirroring the production
// chart, which weights OMEGangPack alongside NodeResourcesFit:MostAllocated and
// disables the PodTopologySpread score. A pool-wide topologyKey is configured so
// standalone (non-gang) pods have a domain to pack into.
func standalonePackOptions(t *testing.T) []scheduler.Option {
	t.Helper()
	cfg, err := schedutil.NewDefaultSchedulerComponentConfig()
	if err != nil {
		t.Fatalf("default scheduler config: %v", err)
	}
	p := cfg.Profiles[0].Plugins
	// Full extension-point enrollment, including Score + PreScore.
	enableGangPack(p)
	p.PreScore.Enabled = append(p.PreScore.Enabled, schedconfig.Plugin{Name: gangpack.Name})
	p.Score.Enabled = append(p.Score.Enabled, schedconfig.Plugin{Name: gangpack.Name, Weight: 100})
	// Neutralize the default scorers so the test measures OMEGangPack's packing
	// signal alone (NodeResourcesFit defaults to LeastAllocated = spread; the
	// PodTopologySpread score also spreads). Their Filter/PreFilter roles are
	// unaffected — only the Score extension is disabled here.
	p.Score.Disabled = append(p.Score.Disabled,
		schedconfig.Plugin{Name: "NodeResourcesFit"},
		schedconfig.Plugin{Name: "PodTopologySpread"},
	)
	cfg.Profiles[0].PluginConfig = append(cfg.Profiles[0].PluginConfig, schedconfig.PluginConfig{
		Name: gangpack.Name,
		Args: &runtime.Unknown{Raw: []byte(`{
			"topologyKey":"` + domainLabelKey + `",
			"podGroupTopologyKeyAnnotation":"` + topologyKeyAnnotation + `",
			"unsupportedPlacementGroupLabel":"ome.io/placement-group",
			"defaultPermitTimeoutSeconds":30,
			"podGroupSyncTimeoutSeconds":5,
			"gcIntervalSeconds":60,
			"standaloneDomainPacking":true
		}`)},
	})
	return []scheduler.Option{
		scheduler.WithProfiles(cfg.Profiles...),
		scheduler.WithFrameworkOutOfTreeRegistry(fwkruntime.Registry{gangpack.Name: gangpack.New}),
	}
}

// makeStandalonePod builds a non-gang pod (no pod-group label) requesting one gpu,
// scheduled by the profile's default scheduler (which this suite runs).
func makeStandalonePod(name, ns string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{
				Name:  "app",
				Image: "registry.k8s.io/pause:3.10",
				Resources: v1.ResourceRequirements{
					Requests: v1.ResourceList{gpuResource: *resource.NewQuantity(1, resource.DecimalSI)},
					Limits:   v1.ResourceList{gpuResource: *resource.NewQuantity(1, resource.DecimalSI)},
				},
			}},
		},
	}
}

// preBoundPod is a standalone pod already assigned to a node (Spec.NodeName set),
// simulating an existing replica occupying one node of a domain. It appears as
// occupancy in the scheduler snapshot without going through scheduling.
func preBoundPod(name, ns, node string) *v1.Pod {
	p := makeStandalonePod(name, ns)
	p.Spec.NodeName = node
	return p
}

// TestStandalonePacksIntoPartlyUsedDomain checks that a standalone (single-host,
// non-gang) replica is steered into a domain that is already partly used, keeping
// empty domains whole.
//
// Layout: two 2-node domains, "a" and "b". One node of domain "a" is already
// occupied (a pre-bound replica), so "a" has 1 free node and "b" has 2. A new
// standalone replica must bind to the free node of "a" (packing), not open "b".
func TestStandalonePacksIntoPartlyUsedDomain(t *testing.T) {
	tc := startScheduler(t, globalKubeConfig, standalonePackOptions(t)...)
	defer tc.teardown(t)

	const ns = "standalone-pack"
	createNamespace(t, tc, ns)

	nodes := []struct{ name, domain string }{
		{"sp-a1", "a"}, {"sp-a2", "a"},
		{"sp-b1", "b"}, {"sp-b2", "b"},
	}
	for _, n := range nodes {
		if _, err := tc.ClientSet.CoreV1().Nodes().Create(tc.Ctx, makeGPUNode(n.name, n.domain, 1), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create node %s: %v", n.name, err)
		}
	}

	// Occupy one node of domain "a" so it is the partly-used domain (1 free).
	if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, preBoundPod("occupant", ns, "sp-a1"), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create occupant: %v", err)
	}

	// The new standalone replica must land on the free node of domain "a".
	if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, makeStandalonePod("replica", ns), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create replica: %v", err)
	}
	node := waitForPodBound(t, tc, ns, "replica", 30*time.Second)
	if node != "sp-a2" {
		t.Fatalf("standalone replica bound to %s (domain %s); want sp-a2 (pack into partly-used domain a, keep domain b whole)",
			node, domainOfNode(t, tc, node))
	}
}

// TestStandalonePackingLeavesGangsIntact runs a gang under the same profile that
// enables standalone packing (PreScore + Score enrolled, a pool-wide topologyKey
// set) and checks the gang still best-fits into one domain, one member per node.
// A gang member's domain is assigned in PreFilter, so PreScore skips it and the
// score never fights the assignment — this is the no-harm proof for the production
// profile, where the packing score is enrolled alongside gangs.
//
// Layout mirrors the gang happy path: domain "a" has 2 nodes, "b" has 3. A 2-pod
// gang fits both; best-fit picks the fuller-fitting "a".
func TestStandalonePackingLeavesGangsIntact(t *testing.T) {
	tc := startScheduler(t, globalKubeConfig, standalonePackOptions(t)...)
	defer tc.teardown(t)

	const ns = "gang-under-score"
	createNamespace(t, tc, ns)

	nodes := []struct{ name, domain string }{
		{"gs-a1", "a"}, {"gs-a2", "a"},
		{"gs-b1", "b"}, {"gs-b2", "b"}, {"gs-b3", "b"},
	}
	for _, n := range nodes {
		if _, err := tc.ClientSet.CoreV1().Nodes().Create(tc.Ctx, makeGPUNode(n.name, n.domain, 1), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create node %s: %v", n.name, err)
		}
	}

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

	domainA := map[string]bool{"gs-a1": true, "gs-a2": true}
	bound := map[string]bool{}
	for _, name := range podNames {
		node := waitForPodBound(t, tc, ns, name, 30*time.Second)
		if !domainA[node] {
			t.Errorf("gang pod %s bound to %s, want a node of domain a (gs-a1/gs-a2) — score must not pull an assigned member off its domain", name, node)
		}
		if bound[node] {
			t.Errorf("two gang pods bound to the same node %s (whole-node model expects one each)", name)
		}
		bound[node] = true
	}
}

// TestStandaloneOnUnlabeledNodesSchedulesNeutrally is the label-less-node case
// (e.g. a 2x2x1 pool when the configured key is the 2x2x2 partition label): a
// standalone replica whose candidate nodes carry no domain label still schedules.
// The packing score is inert for label-less nodes (they score 0 / PreScore skips),
// so the pod is not steered away or left unplaced — even though a partly-used labeled
// domain, where packing IS active, exists elsewhere in the cluster.
func TestStandaloneOnUnlabeledNodesSchedulesNeutrally(t *testing.T) {
	tc := startScheduler(t, globalKubeConfig, standalonePackOptions(t)...)
	defer tc.teardown(t)

	const ns = "standalone-nolabel"
	createNamespace(t, tc, ns)

	// poolLabel separates the labeled domain from the label-less pool so the
	// replica can be constrained to the latter, mirroring a per-topology nodeSelector.
	const poolLabel = "topology.ome.io/pool"
	labeled := makeGPUNode("nl-d1", "d", 1)
	labeled.Labels[poolLabel] = "big"
	labeledOcc := makeGPUNode("nl-d2", "d", 1)
	labeledOcc.Labels[poolLabel] = "big"
	// Label-less nodes: empty domain value is treated as "no domain" by the plugin.
	small1 := makeGPUNode("nl-s1", "", 1)
	small1.Labels[poolLabel] = "small"
	small2 := makeGPUNode("nl-s2", "", 1)
	small2.Labels[poolLabel] = "small"
	for _, n := range []*v1.Node{labeled, labeledOcc, small1, small2} {
		if _, err := tc.ClientSet.CoreV1().Nodes().Create(tc.Ctx, n, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create node %s: %v", n.Name, err)
		}
	}

	// Make the labeled domain partly used, so packing is active there — proving the
	// replica is not pulled toward it despite the live packing gradient.
	if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, preBoundPod("occupant", ns, "nl-d2"), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create occupant: %v", err)
	}

	replica := makeStandalonePod("replica", ns)
	replica.Spec.NodeSelector = map[string]string{poolLabel: "small"}
	if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, replica, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create replica: %v", err)
	}
	node := waitForPodBound(t, tc, ns, "replica", 30*time.Second)
	if node != "nl-s1" && node != "nl-s2" {
		t.Fatalf("standalone replica bound to %s; want a label-less node (nl-s1/nl-s2) — packing must be neutral for nodes without the domain label", node)
	}
}
