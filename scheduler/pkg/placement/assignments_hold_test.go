package placement

import (
	"testing"
	"time"

	"sigs.k8s.io/ome/scheduler/pkg/topology"
)

// A domain held by preemption is closed to every other gang and to its nodes
// under any topology key, exactly as a forming gang's domain is, while the
// evicted pods shut down and the domain shows no room.
func TestHoldClosesTheDomainToOtherGangs(t *testing.T) {
	p := New()
	until := time.Unix(1000, 0)
	h := Gang{Group: "ns/h", Owner: "uid-h", TopologyKey: "rack"}
	id, ok := p.Hold(h, "a", 4, []string{"n1", "n2", "n3", "n4"}, until)
	if !ok || id == 0 {
		t.Fatalf("Hold = %d,%v want a commitment", id, ok)
	}
	if !p.Blocked("rack", "a", nil) || !p.Blocked("other-key", "x", []string{"n3"}) {
		t.Fatal("held domain or one of its nodes reads as free")
	}
	if p.Blocked("rack", "b", []string{"n5"}) {
		t.Fatal("a domain outside the hold reads as blocked")
	}
	if d, _, ok := p.Choose(Gang{Group: "ns/x", TopologyKey: "rack"}, topology.FreeByDomain{"a": 4}, map[string][]string{"a": {"n1", "n2", "n3", "n4"}}, 2); ok {
		t.Fatalf("another gang chose %q inside a held domain", d)
	}
	if !p.BlocksNode("n1", nil) {
		t.Fatal("a standalone pod may take a held node")
	}
	// The held gang follows its commitment through Choose like any assigned gang.
	if d, got, ok := p.Choose(h, topology.FreeByDomain{}, nil, 4); !ok || d != "a" || got != id {
		t.Fatalf("held gang Choose = %q,%d,%v want a,%d,true", d, got, ok, id)
	}
	if gotUntil, gotID, ok := p.HeldUntil("ns/h"); !ok || !gotUntil.Equal(until) || gotID != id {
		t.Fatalf("HeldUntil = %v,%d,%v want %v,%d,true", gotUntil, gotID, ok, until, id)
	}
	if !p.Reserving("ns/h") || p.Holding() != 1 {
		t.Fatalf("Reserving=%v Holding=%d, want true and 1", p.Reserving("ns/h"), p.Holding())
	}
}

func TestHoldRefusesAReservedDomainOrAnAssignedGang(t *testing.T) {
	p := New()
	if _, _, ok := p.Choose(Gang{Group: "ns/f", TopologyKey: "rack"}, topology.FreeByDomain{"a": 4}, map[string][]string{"a": {"n1", "n2"}}, 2); !ok {
		t.Fatal("setup: forming gang found no domain")
	}
	if _, ok := p.Hold(Gang{Group: "ns/h", TopologyKey: "rack"}, "a", 2, []string{"n1", "n2"}, time.Unix(1, 0)); ok {
		t.Fatal("held a domain a forming gang has reserved")
	}
	if _, ok := p.Hold(Gang{Group: "ns/h", TopologyKey: "other"}, "x", 2, []string{"n2"}, time.Unix(1, 0)); ok {
		t.Fatal("held a node a forming gang has reserved under another topology key")
	}
	if _, ok := p.Hold(Gang{Group: "ns/f", TopologyKey: "rack"}, "b", 2, []string{"n9"}, time.Unix(1, 0)); ok {
		t.Fatal("held a domain for a gang that already holds a commitment")
	}
	if _, ok := p.Hold(Gang{Group: "ns/z", TopologyKey: "rack"}, "b", 0, []string{"n9"}, time.Unix(1, 0)); ok {
		t.Fatal("held a domain for an empty gang")
	}
}

// The preemption is in flight until the gang has placed every member; a
// commitment made by Choose is never a hold.
func TestHoldingCountsHoldsUntilTheGangPlacesEveryMember(t *testing.T) {
	p := New()
	if _, ok := p.Hold(Gang{Group: "ns/h", TopologyKey: "rack"}, "a", 2, []string{"n1", "n2"}, time.Unix(1, 0)); !ok {
		t.Fatal("setup: Hold refused")
	}
	if _, _, ok := p.Choose(Gang{Group: "ns/f", TopologyKey: "rack"}, topology.FreeByDomain{"b": 2}, map[string][]string{"b": {"n3", "n4"}}, 2); !ok {
		t.Fatal("setup: Choose refused")
	}
	if _, _, ok := p.HeldUntil("ns/f"); ok {
		t.Fatal("a commitment made by Choose reads as a hold")
	}
	if p.Holding() != 1 {
		t.Fatalf("Holding = %d, want 1", p.Holding())
	}
	place(p, "ns/h")
	place(p, "ns/h")
	if p.Holding() != 0 || p.Reserving("ns/h") {
		t.Fatalf("Holding=%d Reserving=%v after every member placed, want 0 and false", p.Holding(), p.Reserving("ns/h"))
	}
	if !release(p, "ns/h") {
		t.Fatal("the hold could not be released")
	}
	if p.Blocked("rack", "a", []string{"n1"}) {
		t.Fatal("a released hold still blocks its domain")
	}
}
