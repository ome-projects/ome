package engine

import (
	"context"
	"testing"

	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/scheduling"
)

// tpuSliceCandidate turns the fixture's candidate into a move onto a new
// OME-provisioned TPU slice: no target nodes and no GPU footprint.
func tpuSliceCandidate(c policy.Candidate) policy.Candidate {
	c.TPUSlice = &policy.TPUSlicePlan{Topology: "2x2x2", FreePartitions: 1}
	c.HintTargetNodes, c.PlacementTargetNodes, c.FootprintGPUs = nil, nil, 0
	return c
}

func TestDispatcherSubmitsTPUSliceMoveWithoutSimulation(t *testing.T) {
	for _, gang := range []bool{false, true} {
		t.Run(map[bool]string{false: "single", true: "gang"}[gang], func(t *testing.T) {
			d, cl, snap, c, cfg, a := dispatchFixture(t, gang)
			c = tpuSliceCandidate(c)
			d.Policies = []policy.Policy{&stubPolicy{out: []policy.Candidate{c}}}
			d.Simulator = simulationFunc(func(context.Context, scheduling.Request) (scheduling.Result, error) {
				t.Fatal("a TPU slice move must not be simulated")
				return scheduling.Result{}, nil
			})
			_, decisions := d.Execute(context.Background(), snap, []policy.Candidate{c}, cfg, a)
			got := decisionFor(t, decisions, "prod/a")
			if got.DispatchStatus != "submitted" || got.RequestUUID == "" || cl.patches != 1 {
				t.Fatalf("TPU slice move was not submitted exactly once: %+v patches=%d", got, cl.patches)
			}
			_, j, err := loadDispatchJournal(context.Background(), cl.Client, "ome")
			if err != nil || len(j.Entries) != 1 || len(j.Entries[0].Targets) != 0 {
				t.Fatalf("bad durable state: %+v %v", j, err)
			}
			req, err := requestForEntry(j.Entries[0])
			if err != nil || req.FromNode != "source" || len(req.HintTargetNodes) != 0 || req.Reason != policy.ReasonNodeMaintenance {
				t.Fatalf("wrong migration API payload: %+v %v", req, err)
			}
			// A restarted leader replays the same source and must not submit again.
			d.Execute(context.Background(), snap, []policy.Candidate{c}, cfg, &Arbiter{Ledger: NewLedger()})
			if cl.patches != 1 {
				t.Fatalf("duplicate TPU slice request after restart: %d", cl.patches)
			}
		})
	}
}

func TestDispatcherWithholdsTPUSliceMoveWhenPolicyNoLongerAgrees(t *testing.T) {
	d, cl, snap, c, cfg, a := dispatchFixture(t, false)
	c = tpuSliceCandidate(c)
	// The fresh policy replay finds no free partition any more.
	withdrawn := c
	withdrawn.Executable, withdrawn.TPUSlice, withdrawn.AdvisoryReason = false, nil, policy.AdvisoryNoTPUSliceCapacity
	d.Policies = []policy.Policy{&stubPolicy{out: []policy.Candidate{withdrawn}}}
	_, decisions := d.Execute(context.Background(), snap, []policy.Candidate{c}, cfg, a)
	got := decisionFor(t, decisions, "prod/a")
	if got.DispatchStatus != "withheld" || got.DispatchReason != "PolicyNoLongerEligible" || cl.patches != 0 {
		t.Fatalf("stale TPU slice move was not withheld: %+v patches=%d", got, cl.patches)
	}
}
