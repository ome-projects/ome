package gangpack

import (
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	topologyKeyAnnotation = "testing.example/topology-key"
	placementGroupLabel   = "testing.example/placement-group"
)

// fakeReader stands in for the live PodGroup informer: key "ns/name" -> facts.
// inGroup marks a PodGroup that carries the placement-group label.
type fakeReader map[string]struct {
	min     int
	topo    string
	to      time.Duration
	uid     string
	inGroup bool
}

func (f fakeReader) get(namespace, name string) (gangFacts, bool) {
	v, ok := f[namespace+"/"+name]
	if !ok {
		return gangFacts{}, false
	}
	return gangFacts{minMember: v.min, topologyKey: v.topo, timeout: v.to, uid: v.uid, inPlacementGroup: v.inGroup}, true
}

func gangPod(namespace, pg string) *v1.Pod {
	labels := map[string]string{}
	if pg != "" {
		labels[podGroupLabel] = pg
	}
	return &v1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Labels: labels}}
}

func TestPodGroupNameOf_LabelledOrUnlabelledPod_ReturnsNameOnlyWhenLabelled(t *testing.T) {
	ns, name, ok := podGroupNameOf(gangPod("team", "svc-a-prefill"))
	if !ok || ns != "team" || name != "svc-a-prefill" {
		t.Fatalf("podGroupNameOf = %q,%q,%v want team,svc-a-prefill,true", ns, name, ok)
	}
	if _, _, ok := podGroupNameOf(gangPod("team", "")); ok {
		t.Fatal("pod without pod-group label must not be a gang member")
	}
}

// TestSplitGangKey_WellFormedOrMalformedKey_SplitsOnlyWellFormed round-trips a gang key back to its namespace/name, and rejects
// malformed keys — the reservation reconciler resolves PodGroup facts from the key
// alone, so a bad split must not fabricate a lookup.
func TestSplitGangKey_WellFormedOrMalformedKey_SplitsOnlyWellFormed(t *testing.T) {
	ns, name, ok := splitGangKey("team/svc-a")
	if !ok || ns != "team" || name != "svc-a" {
		t.Fatalf("splitGangKey = %q,%q,%v want team,svc-a,true", ns, name, ok)
	}
	for _, bad := range []string{"", "noslash", "/name", "ns/", "/"} {
		if _, _, ok := splitGangKey(bad); ok {
			t.Errorf("splitGangKey(%q) = ok, want not-ok", bad)
		}
	}
}

// TestResolveGang_PodGroupFactsPresentOrNot_ResolvesOnlyCompleteFacts: the plugin learns a gang's size and
// its declared topology key from its PodGroup; the configured topology key
// stands in only when the PodGroup declares none. A pod that isn't a gang
// member, whose PodGroup is missing, or whose PodGroup declares no topology key
// while none is configured, is not resolvable (it won't be domain-assigned).
func TestResolveGang_PodGroupFactsPresentOrNot_ResolvesOnlyCompleteFacts(t *testing.T) {
	reader := fakeReader{
		"team/svc-a-prefill": {min: 18, topo: "nvidia.com/gpu.clique"},
		"team/no-topo":       {min: 4, topo: ""},
		"team/bad-size":      {min: 0, topo: "nvidia.com/gpu.clique"},
	}

	g, ok := resolveGang(gangPod("team", "svc-a-prefill"), reader, "")
	if !ok || g.key != "team/svc-a-prefill" || g.minMember != 18 || g.topologyKey != "nvidia.com/gpu.clique" {
		t.Fatalf("resolveGang = %+v,%v want {team/svc-a-prefill 18 nvidia.com/gpu.clique},true", g, ok)
	}
	if g, ok := resolveGang(gangPod("team", "svc-a-prefill"), reader, "fallback"); !ok || g.topologyKey != "nvidia.com/gpu.clique" {
		t.Fatalf("resolveGang with a declared key = %+v,%v want the declared key, not the fallback", g, ok)
	}
	if g, ok := resolveGang(gangPod("team", "no-topo"), reader, "fallback"); !ok || g.topologyKey != "fallback" || g.minMember != 4 {
		t.Fatalf("resolveGang with the configured key = %+v,%v want {team/no-topo 4 fallback},true", g, ok)
	}

	if _, ok := resolveGang(gangPod("team", ""), reader, "fallback"); ok {
		t.Fatal("non-gang pod must not resolve")
	}
	if _, ok := resolveGang(gangPod("team", "missing"), reader, "fallback"); ok {
		t.Fatal("missing PodGroup must not resolve")
	}
	if _, ok := resolveGang(gangPod("team", "no-topo"), reader, ""); ok {
		t.Fatal("PodGroup with no topology key and none configured must not resolve")
	}
	if _, ok := resolveGang(gangPod("team", "bad-size"), reader, "fallback"); ok {
		t.Fatal("PodGroup with non-positive minMember must not resolve")
	}
}
