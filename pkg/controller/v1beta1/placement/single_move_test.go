package placement

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/placement/allocation"
	"sigs.k8s.io/ome/pkg/placement/plan"
)

func replacementRaceFixture() (*v1beta1.InferenceService, plan.Proposal, map[string]plannedHomeObservation) {
	source := srcISVCMode(v1beta1.PlacementModeSingle, "")
	source.Spec.Placement.MaxSurge = ptr.To[int32](3)
	home := &v1beta1.PlacementHomePolicy{InputDigest: "intent", ReplicaFloors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 3}}}
	p := plan.Proposal{Mode: v1beta1.PlacementModeSingle, Winner: "member-a", PauseSurge: true, SingleMove: &v1beta1.PlacementSingleMoveStatus{}, InputDigest: "intent", Assignments: map[string]v1beta1.CandidateAllocationStatus{
		"member-a": {ClusterUID: "member-a-uid", OriginalReplicas: 3, CurrentReplicas: 3, CurrentHome: home.DeepCopy()},
		"member-b": {ClusterUID: "member-b-uid", DesiredReplicas: 3, DesiredHome: home.DeepCopy(), RaceCandidate: true},
		"member-c": {ClusterUID: "member-c-uid", DesiredReplicas: 3, DesiredHome: home.DeepCopy(), RaceCandidate: true},
	}}
	o := map[string]plannedHomeObservation{
		"member-a": {Home: allocation.Home{Known: true, Applied: true, Ready: 3, Occupied: 3, Routable: true}, PauseAcknowledged: true, FullHomeReady: true},
		"member-b": {Home: allocation.Home{Known: true, Absent: true, Eligible: true}},
		"member-c": {Home: allocation.Home{Known: true, Absent: true, Eligible: true}},
	}
	return source, p, o
}

func activeReplacement(p *plan.Proposal, o map[string]plannedHomeObservation, name string, admitted bool) {
	a := p.Assignments[name]
	a.CurrentReplicas, a.CurrentHome = a.DesiredReplicas, a.DesiredHome.DeepCopy()
	p.Assignments[name] = a
	phase := v1beta1.CandidatePhaseAdmitting
	if admitted {
		phase = v1beta1.CandidatePhaseAdmitted
	}
	o[name] = plannedHomeObservation{Candidate: v1beta1.CandidatePlacement{Cluster: name, Phase: phase}, Home: allocation.Home{Known: true, Applied: true, Eligible: true, Occupied: a.CurrentReplicas}, PauseAcknowledged: true}
}

func TestBoundedReplacementRace(t *testing.T) {
	for _, tt := range []struct {
		name             string
		edit             func(*v1beta1.InferenceService, *plan.Proposal, map[string]plannedHomeObservation)
		b, c             int32
		selected, reason string
	}{
		{name: "terminal replacement cannot win", b: 3, reason: "SurgeBudgetExhausted", edit: func(_ *v1beta1.InferenceService, p *plan.Proposal, o map[string]plannedHomeObservation) {
			activeReplacement(p, o, "member-b", true)
			h := o["member-b"]
			h.Terminal = true
			o["member-b"] = h
		}},
		{name: "unverified policy cannot win", b: 3, reason: "SurgeBudgetExhausted", edit: func(_ *v1beta1.InferenceService, p *plan.Proposal, o map[string]plannedHomeObservation) {
			activeReplacement(p, o, "member-b", true)
			a := p.Assignments["member-b"]
			a.HomeInputsPending = true
			p.Assignments["member-b"] = a
		}},
		{name: "changed probe policy drains before another grant", reason: "AwaitingMemberCleanup", edit: func(_ *v1beta1.InferenceService, p *plan.Proposal, o map[string]plannedHomeObservation) {
			activeReplacement(p, o, "member-b", false)
			p.Assignments["member-b"].DesiredHome.InputDigest = "changed-intent"
		}},
		{name: "selection does not require growth from unknown peers", b: 3, selected: "member-b", reason: "ReplacementNotReady", edit: func(_ *v1beta1.InferenceService, p *plan.Proposal, o map[string]plannedHomeObservation) {
			activeReplacement(p, o, "member-b", true)
			o["member-c"] = plannedHomeObservation{}
		}},
		{name: "whole home fits shared allowance", b: 3, reason: "SurgeBudgetExhausted"},
		{name: "enough surge races multiple candidates", b: 3, c: 3, reason: "AwaitingMemberConvergence", edit: func(s *v1beta1.InferenceService, _ *plan.Proposal, _ map[string]plannedHomeObservation) {
			s.Spec.Placement.MaxSurge = ptr.To[int32](6)
		}},
		{name: "partial home is forbidden", reason: "SurgeBudgetExhausted", edit: func(s *v1beta1.InferenceService, _ *plan.Proposal, _ map[string]plannedHomeObservation) {
			s.Spec.Placement.MaxSurge = ptr.To[int32](2)
		}},
		{name: "rollout reservation consumes allowance", reason: "SurgeBudgetExhausted", edit: func(_ *v1beta1.InferenceService, _ *plan.Proposal, o map[string]plannedHomeObservation) {
			a := o["member-a"]
			a.RolloutReserved = 1
			o["member-a"] = a
		}},
		{name: "autoscaled occupancy consumes allowance", reason: "SurgeBudgetExhausted", edit: func(_ *v1beta1.InferenceService, _ *plan.Proposal, o map[string]plannedHomeObservation) {
			a := o["member-a"]
			a.Home.Occupied = 4
			o["member-a"] = a
		}},
		{name: "unknown empty ledger entry can still contain resources", reason: "ObservationUnknown", edit: func(_ *v1beta1.InferenceService, _ *plan.Proposal, o map[string]plannedHomeObservation) {
			o["member-c"] = plannedHomeObservation{}
		}},
		{name: "pause acknowledgement precedes growth", reason: "AwaitingSurgePause", edit: func(_ *v1beta1.InferenceService, _ *plan.Proposal, o map[string]plannedHomeObservation) {
			a := o["member-a"]
			a.PauseAcknowledged = false
			o["member-a"] = a
		}},
		{name: "unset allowance blocks movement", reason: "MigrationBlocked", edit: func(s *v1beta1.InferenceService, _ *plan.Proposal, _ map[string]plannedHomeObservation) {
			s.Spec.Placement.MaxSurge = nil
		}},
		{name: "admission selects before full readiness", b: 3, selected: "member-b", reason: "ReplacementNotReady", edit: func(_ *v1beta1.InferenceService, p *plan.Proposal, o map[string]plannedHomeObservation) {
			activeReplacement(p, o, "member-b", true)
		}},
		{name: "simultaneous admissions use lexical order", b: 3, c: 3, selected: "member-b", reason: "ReplacementNotReady", edit: func(s *v1beta1.InferenceService, p *plan.Proposal, o map[string]plannedHomeObservation) {
			s.Spec.Placement.MaxSurge = ptr.To[int32](6)
			activeReplacement(p, o, "member-b", true)
			activeReplacement(p, o, "member-c", true)
		}},
		{name: "cursor advances to the next candidate", c: 3, reason: "SurgeBudgetExhausted", edit: func(_ *v1beta1.InferenceService, p *plan.Proposal, _ map[string]plannedHomeObservation) {
			p.SingleMove.Cursor = "member-b"
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, p, o := replacementRaceFixture()
			if tt.edit != nil {
				tt.edit(s, &p, o)
			}
			before := s.DeepCopy()
			got, err := advanceSingleRace(s, o, &p, time.Unix(100, 0))
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(map[string]int32{"member-a": 3, "member-b": tt.b, "member-c": tt.c}, got.Targets); diff != "" {
				t.Fatal(diff)
			}
			if diff := cmp.Diff(tt.reason, got.Reason); diff != "" {
				t.Fatal(diff)
			}
			if diff := cmp.Diff(tt.selected, p.SingleMove.Selected); diff != "" {
				t.Fatal(diff)
			}
			if diff := cmp.Diff("member-a", p.Winner); diff != "" {
				t.Fatalf("probe selection changed serving winner: %s", diff)
			}
			if diff := cmp.Diff(before, s); diff != "" {
				t.Fatal(diff)
			}
			if tt.selected != "" && p.Assignments[tt.selected].RaceCandidate {
				t.Fatal("selected replacement retains race deletion permission")
			}
		})
	}
}

func TestReplacementTimeoutKeepsOccupancyCharged(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	for _, tt := range []struct {
		name     string
		timeout  bool
		elapsed  time.Duration
		admitted bool
		want     int32
		drain    bool
	}{
		{name: "unset timeout retains pending probe", elapsed: time.Hour, want: 3},
		{name: "pending probe has time to admit", timeout: true, elapsed: 30 * time.Second, want: 3},
		{name: "expired pending probe rotates", timeout: true, elapsed: time.Minute, drain: true},
		{name: "admission takes precedence over expiry", timeout: true, elapsed: time.Minute, admitted: true, want: 3},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, p, o := replacementRaceFixture()
			if tt.timeout {
				s.Spec.Placement.ReplacementTimeout = &metav1.Duration{Duration: time.Minute}
			}
			activeReplacement(&p, o, "member-b", tt.admitted)
			a := p.Assignments["member-b"]
			a.ReplacementStartedAt = ptr.To(metav1.NewMicroTime(start))
			p.Assignments["member-b"] = a
			p.SingleMove.Cursor = "member-b"
			step, err := advanceSingleRace(s, o, &p, start.Add(tt.elapsed))
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(map[string]int32{"member-a": 3, "member-b": tt.want, "member-c": 0}, step.Targets); diff != "" {
				t.Fatal(diff)
			}
			if diff := cmp.Diff(tt.drain, len(step.Drain) > 0); diff != "" {
				t.Fatal(diff)
			}
			if !tt.drain {
				return
			}
			a = p.Assignments["member-b"]
			a.CurrentReplicas, a.DrainRequested = 0, true
			p.Assignments["member-b"] = a
			step, err = advanceSingleRace(s, o, &p, start.Add(2*time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(int32(0), step.Targets["member-c"]); diff != "" {
				t.Fatalf("unreaped probe funded new growth: %s", diff)
			}
			o["member-b"] = plannedHomeObservation{Home: allocation.Home{Known: true, Absent: true, Eligible: true}}
			step, err = advanceSingleRace(s, o, &p, start.Add(2*time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(int32(3), step.Targets["member-c"]); diff != "" {
				t.Fatalf("next candidate did not receive released allowance: %s", diff)
			}
		})
	}
}

func TestReplacementBudgetForDifferentFloors(t *testing.T) {
	for _, tt := range []struct {
		name                  string
		original, b, c, surge int32
		wantB, wantC          int32
	}{
		{name: "larger destination sets nominal floor", original: 2, b: 4, c: 4, surge: 2, wantB: 4},
		{name: "smaller winner cannot inherit a larger probes budget", original: 2, b: 4, c: 1, surge: 2, wantB: 4},
		{name: "small first probe bounds later nominations", original: 2, b: 1, c: 4, surge: 2, wantB: 1},
		{name: "heterogeneous race fits every possible winner", original: 2, b: 4, c: 1, surge: 5, wantB: 4, wantC: 1},
		{name: "original floor remains the larger baseline", original: 5, b: 2, c: 3, surge: 5, wantB: 2, wantC: 3},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, p, o := replacementRaceFixture()
			s.Spec.Placement.MaxSurge = ptr.To(tt.surge)
			for name, floor := range map[string]int32{"member-a": tt.original, "member-b": tt.b, "member-c": tt.c} {
				a := p.Assignments[name]
				home := &v1beta1.PlacementHomePolicy{InputDigest: "intent", ReplicaFloors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: floor}}}
				if name == "member-a" {
					a.OriginalReplicas, a.CurrentReplicas, a.CurrentHome = floor, floor, home
					h := o[name]
					h.Home.Ready, h.Home.Occupied = floor, floor
					o[name] = h
				} else {
					a.DesiredReplicas, a.DesiredHome = floor, home
				}
				p.Assignments[name] = a
			}
			step, err := advanceSingleRace(s, o, &p, time.Unix(100, 0))
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(map[string]int32{"member-a": tt.original, "member-b": tt.wantB, "member-c": tt.wantC}, step.Targets); diff != "" {
				t.Fatal(diff)
			}
			for _, floor := range []int32{tt.wantB, tt.wantC} {
				if floor > 0 && tt.original+tt.wantB+tt.wantC > max(tt.original, floor)+tt.surge {
					t.Fatal("a nominated winner cannot cover the committed budget")
				}
			}
			for name, target := range step.Targets {
				if name != "member-a" && target > 0 {
					activeReplacement(&p, o, name, false)
				}
			}
			next, err := advanceSingleRace(s, o, &p, time.Unix(101, 0))
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(step.Targets, next.Targets); diff != "" {
				t.Fatalf("another reconcile expanded the budget: %s", diff)
			}
		})
	}
}

func TestCancellationRetainsReplacementServingCredit(t *testing.T) {
	for _, tt := range []struct {
		name          string
		originalReady int32
		fullReady     bool
		drain         []string
	}{
		{name: "original lost readiness", originalReady: 0},
		{name: "partial original cannot replace whole home", originalReady: 2},
		{name: "router floor must also recover", originalReady: 3},
		{name: "recovered original permits replacement drain", originalReady: 3, fullReady: true, drain: []string{"member-b"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source, p, observations := replacementRaceFixture()
			activeReplacement(&p, observations, "member-b", true)
			original := p.Assignments["member-a"]
			original.DesiredReplicas, original.DesiredHome = original.CurrentReplicas, original.CurrentHome.DeepCopy()
			p.Assignments["member-a"] = original
			for _, name := range []string{"member-b", "member-c"} {
				a := p.Assignments[name]
				a.DesiredReplicas, a.DesiredHome = 0, nil
				p.Assignments[name] = a
			}
			a := observations["member-a"]
			a.Home.Ready = tt.originalReady
			a.FullHomeReady = tt.fullReady
			observations["member-a"] = a
			b := observations["member-b"]
			b.Home.Ready, b.Home.Routable = 3, true
			observations["member-b"] = b
			source.Status.Placement = &v1beta1.PlacementStatus{Plan: &v1beta1.PlacementPlanStatus{Mode: v1beta1.PlacementModeSingle, Winner: "member-a", PauseSurge: true, AssignedReplicas: 3, SingleMove: &v1beta1.PlacementSingleMoveStatus{Selected: "member-a"}}}
			for _, name := range []string{"member-a", "member-b", "member-c"} {
				a := p.Assignments[name]
				source.Status.Placement.Candidates = append(source.Status.Placement.Candidates, v1beta1.CandidatePlacement{Cluster: name, Allocation: a.DeepCopy()})
			}
			got, err := advanceSplitPlan(source, observations)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.drain, got.Drain); diff != "" {
				t.Fatal(diff)
			}
			if diff := cmp.Diff(map[string]int32{"member-a": 3, "member-b": 3, "member-c": 0}, got.Targets); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestReplacementRaceReleasesOnlyAbsentOriginal(t *testing.T) {
	for _, tt := range []struct {
		name    string
		home    allocation.Home
		release bool
	}{
		{name: "verified absence releases current authority", home: allocation.Home{Known: true, Absent: true}, release: true},
		{name: "disconnected original retains authority"},
		{name: "remaining resources retain authority", home: allocation.Home{Known: true, Occupied: 1}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source, proposal, observations := replacementRaceFixture()
			source.Spec.Placement.MaxSurge = ptr.To[int32](0)
			observations["member-a"] = plannedHomeObservation{Home: tt.home}
			original := proposal.Assignments["member-a"].OriginalReplicas
			step, err := advanceSingleRace(source, observations, &proposal, time.Unix(100, 0))
			if err != nil {
				t.Fatal(err)
			}
			want := int32(3)
			if tt.release {
				want = 0
			}
			if diff := cmp.Diff(map[string]int32{"member-a": want, "member-b": 0, "member-c": 0}, step.Targets); diff != "" {
				t.Fatalf("absence release must precede growth: %s", diff)
			}
			if !tt.release {
				return
			}
			a := proposal.Assignments["member-a"]
			a.CurrentReplicas, a.CurrentHome = 0, nil
			proposal.Assignments["member-a"] = a
			step, err = advanceSingleRace(source, observations, &proposal, time.Unix(101, 0))
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(map[string]int32{"member-a": 0, "member-b": 3, "member-c": 0}, step.Targets); diff != "" {
				t.Fatalf("released baseline must fit one complete probe without surge: %s", diff)
			}
			if diff := cmp.Diff(original, proposal.Assignments["member-a"].OriginalReplicas); diff != "" {
				t.Fatalf("original budget changed: %s", diff)
			}
		})
	}
}
