package preemption

import (
	"reflect"
	"testing"
)

// slotFits is a fit model for tests: each domain has free slots, each unit
// holds Pods slots, and the gang needs need slots.
func slotFits(free map[string]int, need int) FitsFunc {
	return func(domain string, gone []Unit) bool {
		room := free[domain]
		for _, u := range gone {
			room += u.Pods
		}
		return room >= need
	}
}

func keysOf(units []Unit) []string {
	out := make([]string, 0, len(units))
	for _, u := range units {
		out = append(out, u.Key)
	}
	return out
}

// A worked example: domain a holds two low gangs of 2 pods,
// domain b one medium gang of 4. A 4-pod gang frees domain a, because its most
// important evicted gang ranks lower than domain b's.
func TestChoosePrefersTheDomainThatEvictsTheLowestPriority(t *testing.T) {
	domains := []Domain{
		{Name: "b", Units: []Unit{{Key: "gang c", Priority: 50, Pods: 4, Accelerators: 4}}},
		{Name: "a", Units: []Unit{
			{Key: "gang a", Priority: 10, Pods: 2, Accelerators: 2},
			{Key: "gang b", Priority: 10, Pods: 2, Accelerators: 2},
		}},
	}
	choice, ok := Choose(domains, slotFits(map[string]int{}, 4))
	if !ok {
		t.Fatal("no domain chosen")
	}
	if choice.Domain != "a" || !reflect.DeepEqual(keysOf(choice.Evict), []string{"gang a", "gang b"}) {
		t.Fatalf("chose %s %v, want a [gang a gang b]", choice.Domain, keysOf(choice.Evict))
	}
}

// A unit the gang still fits beside stays: only the room the gang needs is
// freed, and the most important unit is kept first.
func TestChooseKeepsEveryUnitTheGangFitsBeside(t *testing.T) {
	domains := []Domain{{Name: "a", Units: []Unit{
		{Key: "pod x", Priority: 1, Pods: 1},
		{Key: "gang y", Priority: 5, Pods: 2},
		{Key: "gang z", Priority: 3, Pods: 2},
	}}}
	// One slot free; the gang needs three. Keeping gang y (the most important)
	// leaves 1+1+2 = 4 slots from x and z, enough; then z cannot also stay.
	choice, ok := Choose(domains, slotFits(map[string]int{"a": 1}, 3))
	if !ok {
		t.Fatal("no domain chosen")
	}
	if got := keysOf(choice.Evict); !reflect.DeepEqual(got, []string{"gang z"}) {
		// x (1 slot) alone cannot make room; z (2 slots) with the free slot can.
		t.Fatalf("evicted %v, want [gang z]", got)
	}
}

func TestChooseSkipsADomainThatCannotFitEvenWhenEmptied(t *testing.T) {
	domains := []Domain{
		{Name: "small", Units: []Unit{{Key: "pod x", Priority: 1, Pods: 1}}},
		{Name: "big", Units: []Unit{{Key: "gang y", Priority: 9, Pods: 4}}},
	}
	choice, ok := Choose(domains, slotFits(map[string]int{}, 4))
	if !ok || choice.Domain != "big" {
		t.Fatalf("chose %+v ok=%v, want big", choice, ok)
	}
	if _, ok := Choose(domains[:1], slotFits(map[string]int{}, 4)); ok {
		t.Fatal("chose a domain that cannot fit the gang")
	}
}

// A domain that fits once its shutting-down pods are gone evicts nothing
// and wins over any domain that must evict.
func TestChoosePrefersADomainThatNeedsNoEviction(t *testing.T) {
	domains := []Domain{
		{Name: "a", Units: []Unit{{Key: "pod x", Priority: 1, Pods: 4}}},
		{Name: "b", Units: []Unit{{Key: "pod y", Priority: 1, Pods: 1}}},
	}
	choice, ok := Choose(domains, slotFits(map[string]int{"b": 4}, 4))
	if !ok || choice.Domain != "b" || len(choice.Evict) != 0 {
		t.Fatalf("chose %s %v, want b evicting nothing", choice.Domain, keysOf(choice.Evict))
	}
}

func TestChooseBreaksTiesOnUnitsThenAcceleratorsThenName(t *testing.T) {
	cases := []struct {
		name    string
		domains []Domain
		want    string
	}{
		{
			name: "fewer units",
			domains: []Domain{
				{Name: "a", Units: []Unit{{Key: "pod 1", Priority: 1, Pods: 1}, {Key: "pod 2", Priority: 1, Pods: 1}}},
				{Name: "b", Units: []Unit{{Key: "gang 3", Priority: 1, Pods: 2}}},
			},
			want: "b",
		},
		{
			name: "fewer accelerators",
			domains: []Domain{
				{Name: "a", Units: []Unit{{Key: "gang 1", Priority: 1, Pods: 2, Accelerators: 16}}},
				{Name: "b", Units: []Unit{{Key: "gang 2", Priority: 1, Pods: 2, Accelerators: 8}}},
			},
			want: "b",
		},
		{
			name: "name",
			domains: []Domain{
				{Name: "b", Units: []Unit{{Key: "gang 1", Priority: 1, Pods: 2}}},
				{Name: "a", Units: []Unit{{Key: "gang 2", Priority: 1, Pods: 2}}},
			},
			want: "a",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			choice, ok := Choose(tc.domains, slotFits(map[string]int{}, 2))
			if !ok || choice.Domain != tc.want {
				t.Fatalf("chose %+v ok=%v, want %s", choice, ok, tc.want)
			}
		})
	}
}

// The answer does not depend on the order the caller lists units in.
func TestChooseIsIndependentOfInputOrder(t *testing.T) {
	units := []Unit{
		{Key: "gang a", Priority: 3, Pods: 2},
		{Key: "gang b", Priority: 3, Pods: 2},
		{Key: "pod c", Priority: 1, Pods: 1},
		{Key: "pod d", Priority: 2, Pods: 1},
	}
	reversed := []Unit{units[3], units[2], units[1], units[0]}
	fits := slotFits(map[string]int{}, 3)
	first, _ := Choose([]Domain{{Name: "a", Units: units}}, fits)
	second, _ := Choose([]Domain{{Name: "a", Units: reversed}}, fits)
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("order changed the answer: %+v vs %+v", first, second)
	}
}
