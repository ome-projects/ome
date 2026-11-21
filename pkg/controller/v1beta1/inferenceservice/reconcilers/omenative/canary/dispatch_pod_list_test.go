package canary

import "testing"

// TestDispatchListsEachMembersPodsFromTheCacheByTheIndex holds the dispatch
// to one pod list per member per pass, through the manager's cache and its
// pod index, with no pod list through the live reader, on a prefill/decode
// unit settled and holding.
func TestDispatchListsEachMembersPodsFromTheCacheByTheIndex(t *testing.T) {
	shape := dispatchFleetShapeNamed(t, "prefill-decode/12:8")
	for _, state := range []dispatchFleetState{dispatchSettled, dispatchHolding} {
		f := buildDispatchFleet(t, shape, state)
		f.pass(t)
		members := int64(len(shape.members))
		if f.live.lists != 0 || f.live.podsListed != 0 {
			t.Errorf("%s: the pass listed pods through the live reader %d times (%d pods); every list goes through the cache", state, f.live.lists, f.live.podsListed)
		}
		if f.cache.podLists != members || f.cache.podListsByIndex != members {
			t.Errorf("%s: the pass made %d pod lists through the cache, %d by the index; one per member (%d) by the index", state, f.cache.podLists, f.cache.podListsByIndex, members)
		}
		if want := int64(20); f.cache.podsListed != want {
			t.Errorf("%s: the cache handed out %d pods, want every pod of the unit (%d)", state, f.cache.podsListed, want)
		}
	}
}

// dispatchFleetShapeNamed is the reference shape of that name.
func dispatchFleetShapeNamed(tb testing.TB, name string) dispatchFleetShape {
	tb.Helper()
	for _, shape := range dispatchFleetShapes() {
		if shape.name == name {
			return shape
		}
	}
	tb.Fatalf("no dispatch fleet shape named %s", name)
	return dispatchFleetShape{}
}
