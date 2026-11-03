package placement

import (
	"fmt"
	"testing"

	"sigs.k8s.io/ome/scheduler/pkg/topology"
)

// BenchmarkChoose measures a fresh (unassigned-group) Choose while N other groups
// are already assigned. Choose subtracts every other group's reservation before
// best-fit, so cost grows with the number of assigned groups — and since this runs
// once per new gang, the whole-fleet cost is quadratic in the number of gangs.
// The sub-benchmarks exist to SEE that scaling, not to hide it.
func BenchmarkChoose(b *testing.B) {
	for _, n := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("assigned=%d", n), func(b *testing.B) {
			p := New()
			// Spread N assigned groups across a realistic domain count.
			raw := topology.FreeByDomain{}
			const domains = 64
			for d := 0; d < domains; d++ {
				raw[fmt.Sprintf("dom-%d", d)] = 32
			}
			for i := 0; i < n; i++ {
				p.byGroup[fmt.Sprintf("bg-%d", i)] = &commitment{domain: Domain{Name: fmt.Sprintf("dom-%d", i%domains)}, remaining: 1}
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// New group each iter (so it takes the O(N) scan + assignment path), then
				// Release to keep the store at N.
				g := "new"
				_, id, _ := p.Choose(Gang{Group: g}, raw, nil, 4)
				p.Release(g, id, nil)
			}
		})
	}
}
