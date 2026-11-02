package gangpack

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/kube-scheduler/framework"

	"sigs.k8s.io/ome/scheduler/pkg/placement"
	"sigs.k8s.io/ome/scheduler/pkg/topology"
)

var updateFleetAllocations = flag.Bool("update", false, "re-record testdata/fleet_allocations.json from this run")

// fleetPermitCase is the gate for one member of a gang of eight while a fleet
// of gangs forms: the name the benchmark and the allocation golden share, and
// the gate's answer for that member.
type fleetPermitCase struct {
	name   string
	permit func(testing.TB)
}

// fleetPermitCases builds fleets of 1, 20 and 160 gangs in which every other
// gang has seven members placed and waiting at the gate. Two members are
// measured: the last one, whose arrival completes its gang and releases the
// seven waiting siblings (admit), and the seventh of another gang, which
// joins the waiting set (wait). Both scan every waiting pod of every gang,
// so the cost per member grows with the fleet and the cost of forming the
// whole fleet grows with its square.
func fleetPermitCases(tb testing.TB) []fleetPermitCase {
	tb.Helper()
	const gangSize = 8
	var cases []fleetPermitCase
	for _, gangs := range []int{1, 20, 160} {
		assignments := placement.New()
		g := &GangPack{assignments: assignments}
		var waiting []framework.WaitingPod
		tokens := make([]uint64, gangs)
		for i := 0; i < gangs; i++ {
			domain := fmt.Sprintf("dom-%d", i)
			nodes := make([]string, 0, gangSize)
			for n := 0; n < gangSize; n++ {
				nodes = append(nodes, fmt.Sprintf("%s-n%d", domain, n))
			}
			_, token, ok := assignments.Choose(placement.Gang{Group: gangKey(i), TopologyKey: testKey},
				topology.FreeByDomain{domain: gangSize}, map[string][]string{domain: nodes}, gangSize)
			if !ok {
				tb.Fatalf("gang %d found no domain", i)
			}
			tokens[i] = token
			for m := 0; m < gangSize-1; m++ {
				assignments.Place(gangKey(i), token)
				pod := fleetGangPod(i, m)
				g.rememberAttempt(pod, token)
				waiting = append(waiting, &benchWaitingPod{pod: pod})
			}
		}
		g.handle = &fakeHandle{waiting: waiting}

		// The last gang's eighth member arrives: Reserve has drained its slot,
		// so the gate opens for the whole gang.
		admitting := gangs - 1
		assignments.Place(gangKey(admitting), tokens[admitting])
		admitState := newCycleState()
		writeAssignment(admitState, fmt.Sprintf("dom-%d", admitting),
			gangInfo{key: gangKey(admitting), minMember: gangSize, topologyKey: testKey, timeout: time.Minute}, tokens[admitting])
		admitPod := fleetGangPod(admitting, gangSize-1)
		cases = append(cases, fleetPermitCase{name: fmt.Sprintf("gangs=%d/admit", gangs), permit: func(tb testing.TB) {
			if status, _ := g.Permit(context.Background(), admitState, admitPod, "node"); !status.IsSuccess() {
				tb.Fatalf("Permit = %v, want Success", status)
			}
		}})

		// Another gang's seventh member arrives while one slot is still open:
		// it joins the waiting set.
		if gangs < 2 {
			continue
		}
		forming := 0
		waitState := newCycleState()
		writeAssignment(waitState, fmt.Sprintf("dom-%d", forming),
			gangInfo{key: gangKey(forming), minMember: gangSize, topologyKey: testKey, timeout: time.Minute}, tokens[forming])
		waitPod := fleetGangPod(forming, gangSize-2)
		cases = append(cases, fleetPermitCase{name: fmt.Sprintf("gangs=%d/wait", gangs), permit: func(tb testing.TB) {
			if status, _ := g.Permit(context.Background(), waitState, waitPod, "node"); !status.IsWait() {
				tb.Fatalf("Permit = %v, want Wait", status)
			}
		}})
	}
	return cases
}

// BenchmarkPermitPerGang is the cost of the gate for one member per fleet.
func BenchmarkPermitPerGang(b *testing.B) {
	for _, c := range fleetPermitCases(b) {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				c.permit(b)
			}
		})
	}
}

func gangKey(i int) string { return fmt.Sprintf("team/pg-%d", i) }

// fleetGangPod is member m of gang i, with the name a real pod would carry
// so the attempt record keys on it.
func fleetGangPod(i, m int) *v1.Pod {
	pod := gangPod("team", fmt.Sprintf("pg-%d", i))
	pod.Name = fmt.Sprintf("pg-%d-%d", i, m)
	return pod
}

// allocationGolden is the shape of testdata/fleet_allocations.json: the
// counts, how far a count may move, and the day and kind of machine the
// counts came from. The manager's packages keep theirs in the same shape.
type allocationGolden struct {
	RecordedOn struct {
		Date    string `json:"date"`
		Machine string `json:"machine"`
	} `json:"recordedOn"`
	// Reason is written by hand with every re-recording: what moved the
	// counts and why that is intended.
	Reason           string           `json:"reason"`
	TolerancePercent float64          `json:"tolerancePercent"`
	Budgets          map[string]int64 `json:"budgets"`
}

// measureAllocations is the smallest of five measured calls of op, each
// after a warm-up call: anything else running in the process can add
// allocations to a call, never remove them.
func measureAllocations(op func()) int64 {
	best := int64(-1)
	for i := 0; i < 5; i++ {
		if n := int64(testing.AllocsPerRun(1, op)); best < 0 || n < best {
			best = n
		}
	}
	return best
}

// TestPermit_FleetForming_AllocationsStayWithinGolden holds the allocations of the
// gate for one member per fleet to the recorded golden, so a change in the
// gate's cost is made on purpose and carries its reason.
func TestPermit_FleetForming_AllocationsStayWithinGolden(t *testing.T) {
	path := filepath.Join("testdata", "fleet_allocations.json")
	var golden allocationGolden
	data, err := os.ReadFile(path)
	if err == nil {
		err = json.Unmarshal(data, &golden)
	}
	if err != nil {
		t.Fatalf("reading the allocation golden: %v", err)
	}
	if golden.TolerancePercent <= 0 {
		t.Fatalf("%s: tolerancePercent must be above zero", path)
	}
	measured := map[string]int64{}
	for _, c := range fleetPermitCases(t) {
		measured[c.name] = measureAllocations(func() { c.permit(t) })
	}
	if *updateFleetAllocations {
		golden.Budgets = measured
		golden.RecordedOn.Date = time.Now().UTC().Format("2006-01-02")
		// The Go release is the first word of runtime.Version(); a build
		// suffix after it is not part of the stamp.
		release, _, _ := strings.Cut(runtime.Version(), " ")
		golden.RecordedOn.Machine = fmt.Sprintf("%s/%s, %d cores, %s", runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), release)
		out, err := json.MarshalIndent(golden, "", "  ")
		if err == nil {
			err = os.WriteFile(path, append(out, '\n'), 0o644)
		}
		if err != nil {
			t.Fatalf("writing the allocation golden: %v", err)
		}
		t.Logf("re-recorded %s; write the reason in its reason field", path)
		return
	}
	for name, count := range measured {
		budget, ok := golden.Budgets[name]
		if !ok {
			t.Errorf("%s has no budget named %s (measured %d allocations per operation); rerun with -update to record it and write the reason in the file", path, name, count)
			continue
		}
		slack := float64(budget) * golden.TolerancePercent / 100
		low, high := int64(math.Ceil(float64(budget)-slack)), int64(math.Floor(float64(budget)+slack))
		if count < low || count > high {
			t.Errorf("%s: %d allocations per operation, outside the budget of %d recorded on %s (%g percent tolerance: %d to %d). If the new cost is intended, rerun with -update to re-record %s and write the reason in its reason field.",
				name, count, budget, golden.RecordedOn.Date, golden.TolerancePercent, low, high, path)
		}
	}
	for name := range golden.Budgets {
		if _, ok := measured[name]; !ok {
			t.Errorf("%s has a budget named %s that nothing measured; remove it or rerun with -update", path, name)
		}
	}
}
