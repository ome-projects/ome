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

// raceNow is the instant every row runs at; a grant records it on the
// replacement it starts, truncated to the microsecond the status keeps.
var raceNow = time.Unix(1000, 0).UTC()

// raceStarted is the start time a grant writes on a replacement at raceNow.
func raceStarted() *metav1.MicroTime {
	return ptr.To(metav1.NewMicroTime(raceNow.Truncate(time.Microsecond)))
}

// raceWant is the whole result of one pass: the step handed back and the
// plan as the pass leaves it. Every row states all of it.
type raceWant struct {
	step        allocation.Step
	assignments map[string]v1beta1.CandidateAllocationStatus
	selected    string
	cursor      string
}

// raceRow is one rule of the bounded replacement race. edit shapes the
// fixture before the pass; want edits the expected result, which starts
// from the edited plan with no step written.
type raceRow struct {
	name string
	edit func(s *v1beta1.InferenceService, p *plan.Proposal, o map[string]plannedMemberObservation)
	want func(w *raceWant)
}

// raceTargets is the step's targets for the three members of the fixture.
func raceTargets(a, b, c int32) map[string]int32 {
	return map[string]int32{"member-a": a, "member-b": b, "member-c": c}
}

// raceAssign edits one member's expected allocation in place.
func raceAssign(w *raceWant, name string, edit func(a *v1beta1.CandidateAllocationStatus)) {
	a := w.assignments[name]
	edit(&a)
	w.assignments[name] = a
}

// raceGranted is the write a grant leaves on a replacement.
func raceGranted(a *v1beta1.CandidateAllocationStatus) { a.ReplacementStartedAt = raceStarted() }

// raceNoTarget is the write that takes a replacement's target away.
func raceNoTarget(a *v1beta1.CandidateAllocationStatus) { a.DesiredHome, a.DesiredReplicas = nil, 0 }

// raceDrained is a drained replacement: its target gone, its pods still counted.
func raceDrained(w *raceWant, name string) {
	w.step.Targets[name] = 0
	w.step.Drain = append(w.step.Drain, name)
	raceAssign(w, name, raceNoTarget)
}

// raceMinimums gives the chosen cluster and the two replacements the
// minimums the row names, the chosen cluster serving its own.
func raceMinimums(p *plan.Proposal, o map[string]plannedMemberObservation, original, b, c int32) {
	for name, minimum := range map[string]int32{"member-a": original, "member-b": b, "member-c": c} {
		a := p.Assignments[name]
		policy := &v1beta1.PlacementHomePolicy{InputDigest: "intent", ReplicaFloors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: minimum}}}
		if name == "member-a" {
			a.OriginalReplicas, a.CurrentReplicas, a.CurrentHome = minimum, minimum, policy
			h := o[name]
			h.MemberState.Ready, h.MemberState.Occupied = minimum, minimum
			o[name] = h
		} else {
			a.DesiredReplicas, a.DesiredHome = minimum, policy
		}
		p.Assignments[name] = a
	}
}

var raceRows = []raceRow{
	{name: "the first replacement in name order is granted and the cursor moves to it", want: func(w *raceWant) {
		w.step = allocation.Step{Targets: raceTargets(3, 3, 0), Reason: "SurgeBudgetExhausted"}
		raceAssign(w, "member-b", raceGranted)
		w.cursor = "member-b"
	}},
	{name: "the grant order starts after the cursor", edit: func(_ *v1beta1.InferenceService, p *plan.Proposal, _ map[string]plannedMemberObservation) {
		p.SingleMove.Cursor = "member-b"
	}, want: func(w *raceWant) {
		w.step = allocation.Step{Targets: raceTargets(3, 0, 3), Reason: "SurgeBudgetExhausted"}
		raceAssign(w, "member-c", raceGranted)
		w.cursor = "member-c"
	}},
	{name: "an admitted replacement at its target is selected and the rest lose their targets", edit: func(_ *v1beta1.InferenceService, p *plan.Proposal, o map[string]plannedMemberObservation) {
		activeReplacement(p, o, "member-b", true)
	}, want: func(w *raceWant) {
		w.step = allocation.Step{Targets: raceTargets(3, 3, 0), Reason: "ReplacementNotReady"}
		raceAssign(w, "member-b", func(a *v1beta1.CandidateAllocationStatus) { a.RaceCandidate = false })
		raceAssign(w, "member-c", raceNoTarget)
		w.selected = "member-b"
	}},
	{name: "two admitted replacements: the first in name order is selected", edit: func(_ *v1beta1.InferenceService, p *plan.Proposal, o map[string]plannedMemberObservation) {
		activeReplacement(p, o, "member-b", true)
		activeReplacement(p, o, "member-c", true)
	}, want: func(w *raceWant) {
		w.step = allocation.Step{Targets: raceTargets(3, 3, 3), Reason: "ReplacementNotReady"}
		raceAssign(w, "member-b", func(a *v1beta1.CandidateAllocationStatus) { a.RaceCandidate = false })
		raceAssign(w, "member-c", raceNoTarget)
		w.selected = "member-b"
	}},
	{name: "selection comes before the unknown-member check", edit: func(_ *v1beta1.InferenceService, p *plan.Proposal, o map[string]plannedMemberObservation) {
		activeReplacement(p, o, "member-b", true)
		o["member-c"] = plannedMemberObservation{}
	}, want: func(w *raceWant) {
		w.step = allocation.Step{Targets: raceTargets(3, 3, 0), Reason: "ReplacementNotReady"}
		raceAssign(w, "member-b", func(a *v1beta1.CandidateAllocationStatus) { a.RaceCandidate = false })
		raceAssign(w, "member-c", raceNoTarget)
		w.selected = "member-b"
	}},
	{name: "an admitted replacement whose inputs are pending is not selected", edit: func(_ *v1beta1.InferenceService, p *plan.Proposal, o map[string]plannedMemberObservation) {
		activeReplacement(p, o, "member-b", true)
		a := p.Assignments["member-b"]
		a.HomeInputsPending = true
		p.Assignments["member-b"] = a
	}, want: func(w *raceWant) {
		w.step = allocation.Step{Targets: raceTargets(3, 3, 0), Reason: "SurgeBudgetExhausted"}
	}},
	{name: "a terminal replacement is not selected", edit: func(_ *v1beta1.InferenceService, p *plan.Proposal, o map[string]plannedMemberObservation) {
		activeReplacement(p, o, "member-b", true)
		h := o["member-b"]
		h.Terminal = true
		o["member-b"] = h
	}, want: func(w *raceWant) {
		w.step = allocation.Step{Targets: raceTargets(3, 3, 0), Reason: "SurgeBudgetExhausted"}
	}},
	{name: "a replacement still admitting is not selected and holds the budget", edit: func(_ *v1beta1.InferenceService, p *plan.Proposal, o map[string]plannedMemberObservation) {
		activeReplacement(p, o, "member-b", false)
	}, want: func(w *raceWant) {
		w.step = allocation.Step{Targets: raceTargets(3, 3, 0), Reason: "SurgeBudgetExhausted"}
	}},
	{name: "an unknown member stops the pass", edit: func(_ *v1beta1.InferenceService, _ *plan.Proposal, o map[string]plannedMemberObservation) {
		o["member-c"] = plannedMemberObservation{}
	}, want: func(w *raceWant) {
		w.step = allocation.Step{Targets: raceTargets(3, 0, 0), Reason: "ObservationUnknown"}
	}},
	{name: "a chosen cluster that served and is absent is scaled to zero first", edit: func(_ *v1beta1.InferenceService, _ *plan.Proposal, o map[string]plannedMemberObservation) {
		o["member-a"] = plannedMemberObservation{MemberState: allocation.MemberState{Known: true, Absent: true}}
	}, want: func(w *raceWant) {
		w.step = allocation.Step{Targets: raceTargets(0, 0, 0), Reason: "AwaitingMemberConvergence"}
	}},
	{name: "a chosen cluster at zero releases its budget to two replacements", edit: func(_ *v1beta1.InferenceService, p *plan.Proposal, o map[string]plannedMemberObservation) {
		o["member-a"] = plannedMemberObservation{MemberState: allocation.MemberState{Known: true, Absent: true}}
		a := p.Assignments["member-a"]
		a.CurrentReplicas, a.CurrentHome = 0, nil
		p.Assignments["member-a"] = a
	}, want: func(w *raceWant) {
		w.step = allocation.Step{Targets: raceTargets(0, 3, 3), Reason: "AwaitingMemberConvergence"}
		raceAssign(w, "member-b", raceGranted)
		raceAssign(w, "member-c", raceGranted)
		w.cursor = "member-c"
	}},
	{name: "a member that has not acknowledged the pause blocks growth", edit: func(_ *v1beta1.InferenceService, _ *plan.Proposal, o map[string]plannedMemberObservation) {
		h := o["member-a"]
		h.PauseAcknowledged = false
		o["member-a"] = h
	}, want: func(w *raceWant) {
		w.step = allocation.Step{Targets: raceTargets(3, 0, 0), Reason: "AwaitingSurgePause"}
	}},
	{name: "a replacement at zero and absent forgets its placement and may be granted again", edit: func(_ *v1beta1.InferenceService, p *plan.Proposal, _ map[string]plannedMemberObservation) {
		a := p.Assignments["member-b"]
		a.CurrentHome, a.DrainRequested = a.DesiredHome.DeepCopy(), true
		a.ReplacementStartedAt = ptr.To(metav1.NewMicroTime(raceNow.Add(-time.Hour)))
		p.Assignments["member-b"] = a
	}, want: func(w *raceWant) {
		w.step = allocation.Step{Targets: raceTargets(3, 3, 0), Reason: "SurgeBudgetExhausted"}
		raceAssign(w, "member-b", func(a *v1beta1.CandidateAllocationStatus) {
			a.CurrentHome, a.DrainRequested = nil, false
			raceGranted(a)
		})
		w.cursor = "member-b"
	}},
	{name: "a replacement past its timeout is drained", edit: func(s *v1beta1.InferenceService, p *plan.Proposal, o map[string]plannedMemberObservation) {
		s.Spec.Placement.ReplacementTimeout = &metav1.Duration{Duration: time.Minute}
		activeReplacement(p, o, "member-b", false)
		a := p.Assignments["member-b"]
		a.ReplacementStartedAt = ptr.To(metav1.NewMicroTime(raceNow.Add(-2 * time.Minute)))
		p.Assignments["member-b"] = a
	}, want: func(w *raceWant) {
		w.step = allocation.Step{Targets: raceTargets(3, 3, 0), Reason: "AwaitingMemberCleanup"}
		raceDrained(w, "member-b")
	}},
	{name: "a replacement inside its timeout keeps its pods", edit: func(s *v1beta1.InferenceService, p *plan.Proposal, o map[string]plannedMemberObservation) {
		s.Spec.Placement.ReplacementTimeout = &metav1.Duration{Duration: time.Minute}
		activeReplacement(p, o, "member-b", false)
		a := p.Assignments["member-b"]
		a.ReplacementStartedAt = ptr.To(metav1.NewMicroTime(raceNow.Add(-30 * time.Second)))
		p.Assignments["member-b"] = a
	}, want: func(w *raceWant) {
		w.step = allocation.Step{Targets: raceTargets(3, 3, 0), Reason: "SurgeBudgetExhausted"}
	}},
	{name: "a replacement without a target is drained", edit: func(_ *v1beta1.InferenceService, p *plan.Proposal, o map[string]plannedMemberObservation) {
		activeReplacement(p, o, "member-b", false)
		a := p.Assignments["member-b"]
		a.DesiredReplicas = 0
		p.Assignments["member-b"] = a
	}, want: func(w *raceWant) {
		w.step = allocation.Step{Targets: raceTargets(3, 3, 0), Reason: "AwaitingMemberCleanup"}
		raceDrained(w, "member-b")
	}},
	{name: "a replacement whose target changed is drained before another grant", edit: func(_ *v1beta1.InferenceService, p *plan.Proposal, o map[string]plannedMemberObservation) {
		activeReplacement(p, o, "member-b", false)
		p.Assignments["member-b"].DesiredHome.InputDigest = "changed-intent"
	}, want: func(w *raceWant) {
		w.step = allocation.Step{Targets: raceTargets(3, 3, 0), Reason: "AwaitingMemberCleanup"}
		raceDrained(w, "member-b")
	}},
	{name: "a changed target with inputs pending is kept", edit: func(_ *v1beta1.InferenceService, p *plan.Proposal, o map[string]plannedMemberObservation) {
		activeReplacement(p, o, "member-b", false)
		a := p.Assignments["member-b"]
		a.HomeInputsPending = true
		a.DesiredHome.InputDigest = "changed-intent"
		p.Assignments["member-b"] = a
	}, want: func(w *raceWant) {
		w.step = allocation.Step{Targets: raceTargets(3, 3, 0), Reason: "SurgeBudgetExhausted"}
	}},
	{name: "a rollout reservation charges the budget", edit: func(_ *v1beta1.InferenceService, _ *plan.Proposal, o map[string]plannedMemberObservation) {
		h := o["member-a"]
		h.RolloutReserved = 1
		o["member-a"] = h
	}, want: func(w *raceWant) {
		w.step = allocation.Step{Targets: raceTargets(3, 0, 0), Reason: "SurgeBudgetExhausted"}
	}},
	{name: "occupancy above the planned count charges the budget", edit: func(_ *v1beta1.InferenceService, _ *plan.Proposal, o map[string]plannedMemberObservation) {
		h := o["member-a"]
		h.MemberState.Occupied = 4
		o["member-a"] = h
	}, want: func(w *raceWant) {
		w.step = allocation.Step{Targets: raceTargets(3, 0, 0), Reason: "SurgeBudgetExhausted"}
	}},
	{name: "a smaller first replacement bounds the later ones", edit: func(_ *v1beta1.InferenceService, p *plan.Proposal, o map[string]plannedMemberObservation) {
		raceMinimums(p, o, 2, 1, 4)
	}, want: func(w *raceWant) {
		w.step = allocation.Step{Targets: raceTargets(2, 1, 0), Reason: "SurgeBudgetExhausted"}
		raceAssign(w, "member-b", raceGranted)
		w.cursor = "member-b"
	}},
	{name: "a larger replacement sets the budget it fits in", edit: func(_ *v1beta1.InferenceService, p *plan.Proposal, o map[string]plannedMemberObservation) {
		raceMinimums(p, o, 2, 4, 4)
	}, want: func(w *raceWant) {
		w.step = allocation.Step{Targets: raceTargets(2, 4, 0), Reason: "SurgeBudgetExhausted"}
		raceAssign(w, "member-b", raceGranted)
		w.cursor = "member-b"
	}},
	{name: "a replacement that is not eligible gets no grant", edit: func(_ *v1beta1.InferenceService, _ *plan.Proposal, o map[string]plannedMemberObservation) {
		o["member-b"] = plannedMemberObservation{MemberState: allocation.MemberState{Known: true, Absent: true}}
	}, want: func(w *raceWant) {
		w.step = allocation.Step{Targets: raceTargets(3, 0, 3), Reason: "AwaitingMemberConvergence"}
		raceAssign(w, "member-c", raceGranted)
		w.cursor = "member-c"
	}},
	{name: "a replacement whose cluster still holds something gets no grant", edit: func(_ *v1beta1.InferenceService, _ *plan.Proposal, o map[string]plannedMemberObservation) {
		o["member-b"] = plannedMemberObservation{MemberState: allocation.MemberState{Known: true, Eligible: true}, PauseAcknowledged: true}
	}, want: func(w *raceWant) {
		w.step = allocation.Step{Targets: raceTargets(3, 0, 3), Reason: "AwaitingMemberConvergence"}
		raceAssign(w, "member-c", raceGranted)
		w.cursor = "member-c"
	}},
	{name: "a replacement without a resolved target gets no grant", edit: func(_ *v1beta1.InferenceService, p *plan.Proposal, _ map[string]plannedMemberObservation) {
		a := p.Assignments["member-b"]
		a.DesiredHome = nil
		p.Assignments["member-b"] = a
	}, want: func(w *raceWant) {
		w.step = allocation.Step{Targets: raceTargets(3, 0, 3), Reason: "AwaitingMemberConvergence"}
		raceAssign(w, "member-c", raceGranted)
		w.cursor = "member-c"
	}},
	{name: "with no replacement to grant the pass awaits admission", edit: func(_ *v1beta1.InferenceService, _ *plan.Proposal, o map[string]plannedMemberObservation) {
		o["member-b"] = plannedMemberObservation{MemberState: allocation.MemberState{Known: true, Absent: true}}
		o["member-c"] = plannedMemberObservation{MemberState: allocation.MemberState{Known: true, Absent: true}}
	}, want: func(w *raceWant) {
		w.step = allocation.Step{Targets: raceTargets(3, 0, 0), Reason: "AwaitingReplacementAdmission"}
	}},
}

// TestAdvanceSingleRaceRules holds every rule of one pass of the bounded
// replacement race: what the step says, which replacement is selected,
// where the cursor rests and what each member's allocation reads after
// the pass. The source is never written.
func TestAdvanceSingleRaceRules(t *testing.T) {
	for _, row := range raceRows {
		t.Run(row.name, func(t *testing.T) {
			s, p, o := replacementRaceFixture()
			if row.edit != nil {
				row.edit(s, &p, o)
			}
			want := raceWant{assignments: make(map[string]v1beta1.CandidateAllocationStatus, len(p.Assignments)), cursor: p.SingleMove.Cursor}
			for name, a := range p.Assignments {
				want.assignments[name] = *a.DeepCopy()
			}
			row.want(&want)
			before := s.DeepCopy()

			got, err := advanceSingleRace(s, o, &p, raceNow)
			if err != nil {
				t.Fatalf("advanceSingleRace: %v", err)
			}
			if diff := cmp.Diff(want.step, got); diff != "" {
				t.Errorf("step (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(want.assignments, p.Assignments); diff != "" {
				t.Errorf("assignments (-want +got):\n%s", diff)
			}
			if p.SingleMove.Selected != want.selected || p.SingleMove.Cursor != want.cursor {
				t.Errorf("selected %q cursor %q, want %q and %q", p.SingleMove.Selected, p.SingleMove.Cursor, want.selected, want.cursor)
			}
			if p.Winner != "member-a" {
				t.Errorf("chosen cluster %q, want member-a", p.Winner)
			}
			if diff := cmp.Diff(before, s); diff != "" {
				t.Errorf("the source was written (-before +after):\n%s", diff)
			}
		})
	}
}
