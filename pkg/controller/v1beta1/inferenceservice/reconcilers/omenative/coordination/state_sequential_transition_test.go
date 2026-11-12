package coordination

import (
	"regexp"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// sequentialTransition reads a Sequential group one member at a time: a
// Failed member in the order blocks the group, the first member still
// rolling drives the phase once the wait since the previous member finished
// has run, and a group with nothing rolling is complete, between members, or
// waiting for its first change. One row per rule: the order, the wait, the
// members as observed, the finish record the status carries, and the whole
// decision the pass writes.

// sequentialRow is one shape of order, wait, members and carried record.
type sequentialRow struct {
	name string
	// wait is the operator's wait after a member finishes; zero means none.
	wait time.Duration
	// order lists the members in the order they roll.
	order []v1beta1.ComponentType
	// members are the observations; a member of the order may be missing.
	members []ComponentObservation
	// record is the finish the previously written status carries.
	record MemberFinish
	// want is the decision; its Message is empty when wantMessageParts is set.
	want GroupTransition
	// wantMessageParts holds the message text before and after the one word
	// the engine names the wait with, which the rows leave to the engine.
	wantMessageParts []string
}

// sequentialNow is the instant every row reads as now.
var sequentialNow = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

// earlier is the instant the given duration before sequentialNow.
func earlier(d time.Duration) time.Time { return sequentialNow.Add(-d) }

// restingAt is a member at rest on the target revision whose replica
// recorded its roll done at the instant.
func restingAt(c v1beta1.ComponentType, at time.Time) ComponentObservation {
	return edited(settledOn(c, "rev2"), func(m *ComponentObservation) { m.ReadyAt = at })
}

// failedMember is a member whose reconciler flagged a terminal failure.
func failedMember(c v1beta1.ComponentType) ComponentObservation {
	return edited(settledOn(c, "rev2"), func(m *ComponentObservation) {
		m.Failed, m.FailureMessage = true, "ready timeout"
	})
}

var (
	decoderEngine       = []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent}
	decoderEngineRouter = []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent, v1beta1.RouterComponent}
	tenMinutes          = 10 * time.Minute
)

// blocked is the decision for a Failed member, with the member before it in
// the order.
func blocked(failed, previous v1beta1.ComponentType) GroupTransition {
	return GroupTransition{
		Phase: v1beta1.CoordinationPhaseFailed, CompositePhase: v1beta1.CompositePhaseSequentialFailed,
		CurrentComponent: failed, PreviousComponent: previous,
		Message: "Sequential group blocked by Failed Component: " + string(failed),
	}
}

// rolls is the decision while the member drives the group's phase.
func rolls(c v1beta1.ComponentType, phase v1beta1.CoordinationPhase, message string, previous v1beta1.ComponentType) GroupTransition {
	return GroupTransition{
		Phase: phase, CompositePhase: string(c) + "." + string(phase),
		CurrentComponent: c, PreviousComponent: previous, Message: message,
	}
}

// awaits is the decision while the member waits for the one before it.
func awaits(c, previous v1beta1.ComponentType) GroupTransition {
	return GroupTransition{
		Phase: v1beta1.CoordinationPhaseIdle, CompositePhase: v1beta1.CompositePhaseSequentialAwaiting,
		CurrentComponent: c, PreviousComponent: previous,
	}
}

// withFinish adds the finish record the decision carries.
func withFinish(decision GroupTransition, c v1beta1.ComponentType, at time.Time) GroupTransition {
	decision.MemberFinished = MemberFinish{Component: c, At: at}
	return decision
}

var complete = GroupTransition{Phase: v1beta1.CoordinationPhaseIdle, CompositePhase: "Idle", Message: "Sequential group complete"}

var sequentialRows = []sequentialRow{
	{
		name:    "a Failed first member blocks the group",
		order:   decoderEngine,
		members: []ComponentObservation{failedMember(v1beta1.DecoderComponent), settledOn(v1beta1.EngineComponent, "rev2")},
		want:    blocked(v1beta1.DecoderComponent, ""),
	},
	{
		name:    "a Failed later member outranks the first member's roll",
		order:   decoderEngine,
		members: []ComponentObservation{rollingTo(v1beta1.DecoderComponent, 2, 0, 0), failedMember(v1beta1.EngineComponent)},
		want:    blocked(v1beta1.EngineComponent, v1beta1.DecoderComponent),
	},
	{
		name:    "the first Failed member in the order is named",
		order:   decoderEngine,
		members: []ComponentObservation{failedMember(v1beta1.DecoderComponent), failedMember(v1beta1.EngineComponent)},
		want:    blocked(v1beta1.DecoderComponent, ""),
	},
	{
		name:    "a Failed member outside the order does not block",
		order:   []v1beta1.ComponentType{v1beta1.DecoderComponent},
		members: []ComponentObservation{rollingTo(v1beta1.DecoderComponent, 2, 0, 0), failedMember(v1beta1.EngineComponent)},
		want:    rolls(v1beta1.DecoderComponent, v1beta1.CoordinationPhaseSurging, "creating new-revision pods", ""),
	},
	{
		name:    "an unobserved member of the order is skipped on the way to a Failed one",
		order:   decoderEngine,
		members: []ComponentObservation{failedMember(v1beta1.EngineComponent)},
		want:    blocked(v1beta1.EngineComponent, v1beta1.DecoderComponent),
	},
	{
		name:    "a Failed group keeps the recorded finish of a done member",
		wait:    tenMinutes,
		order:   decoderEngine,
		members: []ComponentObservation{settledOn(v1beta1.DecoderComponent, "rev2"), failedMember(v1beta1.EngineComponent)},
		record:  MemberFinish{Component: v1beta1.DecoderComponent, At: earlier(5 * time.Minute)},
		want:    withFinish(blocked(v1beta1.EngineComponent, v1beta1.DecoderComponent), v1beta1.DecoderComponent, earlier(5*time.Minute)),
	},
	{
		name:    "a Failed group drops the record of a member that rolls again",
		wait:    tenMinutes,
		order:   decoderEngine,
		members: []ComponentObservation{rollingTo(v1beta1.DecoderComponent, 2, 0, 0), failedMember(v1beta1.EngineComponent)},
		record:  MemberFinish{Component: v1beta1.DecoderComponent, At: earlier(5 * time.Minute)},
		want:    blocked(v1beta1.EngineComponent, v1beta1.DecoderComponent),
	},
	{
		name:    "the first member's surge is the group's phase",
		order:   decoderEngine,
		members: []ComponentObservation{rollingTo(v1beta1.DecoderComponent, 2, 0, 0), settledOn(v1beta1.EngineComponent, "rev2")},
		want:    rolls(v1beta1.DecoderComponent, v1beta1.CoordinationPhaseSurging, "creating new-revision pods", ""),
	},
	{
		name:    "the first member waiting for Ready pods is the group's phase",
		order:   decoderEngine,
		members: []ComponentObservation{rollingTo(v1beta1.DecoderComponent, 4, 2, 1), settledOn(v1beta1.EngineComponent, "rev2")},
		want:    rolls(v1beta1.DecoderComponent, v1beta1.CoordinationPhaseWaiting, "waiting for new-revision pods to be Ready", ""),
	},
	{
		name:    "the first member shifting traffic is the group's phase",
		order:   decoderEngine,
		members: []ComponentObservation{rollingTo(v1beta1.DecoderComponent, 4, 2, 2), settledOn(v1beta1.EngineComponent, "rev2")},
		want:    rolls(v1beta1.DecoderComponent, v1beta1.CoordinationPhaseShifting, "shifting traffic; old-revision pods still serving", ""),
	},
	{
		name:    "the first member scaling down is the group's phase",
		order:   decoderEngine,
		members: []ComponentObservation{rollingTo(v1beta1.DecoderComponent, 2, 2, 2), settledOn(v1beta1.EngineComponent, "rev2")},
		want:    rolls(v1beta1.DecoderComponent, v1beta1.CoordinationPhaseScalingDown, "scaling down old-revision pods", ""),
	},
	{
		name:    "the next member starts at once when the group has no wait",
		order:   decoderEngine,
		members: []ComponentObservation{restingAt(v1beta1.DecoderComponent, earlier(time.Minute)), rollingTo(v1beta1.EngineComponent, 2, 0, 0)},
		want:    rolls(v1beta1.EngineComponent, v1beta1.CoordinationPhaseSurging, "creating new-revision pods", v1beta1.DecoderComponent),
	},
	{
		name:    "a member at its staged shape lets the next member roll",
		order:   decoderEngine,
		members: []ComponentObservation{stagedMember(v1beta1.DecoderComponent), rollingTo(v1beta1.EngineComponent, 2, 0, 0)},
		want:    rolls(v1beta1.EngineComponent, v1beta1.CoordinationPhaseSurging, "creating new-revision pods", v1beta1.DecoderComponent),
	},
	{
		name:             "the next member waits out the time since the previous member's Ready",
		wait:             tenMinutes,
		order:            decoderEngine,
		members:          []ComponentObservation{restingAt(v1beta1.DecoderComponent, earlier(4*time.Minute)), rollingTo(v1beta1.EngineComponent, 2, 0, 0)},
		want:             withFinish(awaits(v1beta1.EngineComponent, v1beta1.DecoderComponent), v1beta1.DecoderComponent, earlier(4*time.Minute)),
		wantMessageParts: []string{"Sequential ", ": 6m0s remaining before engine starts"},
	},
	{
		name:             "a done member without a Ready instant starts the wait now",
		wait:             tenMinutes,
		order:            decoderEngine,
		members:          []ComponentObservation{settledOn(v1beta1.DecoderComponent, "rev2"), rollingTo(v1beta1.EngineComponent, 2, 0, 0)},
		want:             withFinish(awaits(v1beta1.EngineComponent, v1beta1.DecoderComponent), v1beta1.DecoderComponent, sequentialNow),
		wantMessageParts: []string{"Sequential ", ": 10m0s remaining before engine starts"},
	},
	{
		name:             "the recorded finish measures the wait, not the member's Ready instant",
		wait:             tenMinutes,
		order:            decoderEngine,
		members:          []ComponentObservation{restingAt(v1beta1.DecoderComponent, earlier(time.Minute)), rollingTo(v1beta1.EngineComponent, 2, 0, 0)},
		record:           MemberFinish{Component: v1beta1.DecoderComponent, At: earlier(9 * time.Minute)},
		want:             withFinish(awaits(v1beta1.EngineComponent, v1beta1.DecoderComponent), v1beta1.DecoderComponent, earlier(9*time.Minute)),
		wantMessageParts: []string{"Sequential ", ": 1m0s remaining before engine starts"},
	},
	{
		name:  "a record of an earlier member gives way to the member before the active one",
		wait:  tenMinutes,
		order: decoderEngineRouter,
		members: []ComponentObservation{
			settledOn(v1beta1.DecoderComponent, "rev2"), restingAt(v1beta1.EngineComponent, earlier(3*time.Minute)), rollingTo(v1beta1.RouterComponent, 2, 0, 0),
		},
		record:           MemberFinish{Component: v1beta1.DecoderComponent, At: earlier(9 * time.Minute)},
		want:             withFinish(awaits(v1beta1.RouterComponent, v1beta1.EngineComponent), v1beta1.EngineComponent, earlier(3*time.Minute)),
		wantMessageParts: []string{"Sequential ", ": 7m0s remaining before router starts"},
	},
	{
		name:    "the next member starts when the wait has elapsed",
		wait:    tenMinutes,
		order:   decoderEngine,
		members: []ComponentObservation{settledOn(v1beta1.DecoderComponent, "rev2"), rollingTo(v1beta1.EngineComponent, 2, 0, 0)},
		record:  MemberFinish{Component: v1beta1.DecoderComponent, At: earlier(11 * time.Minute)},
		want: withFinish(
			rolls(v1beta1.EngineComponent, v1beta1.CoordinationPhaseSurging, "creating new-revision pods", v1beta1.DecoderComponent),
			v1beta1.DecoderComponent, earlier(11*time.Minute)),
	},
	{
		name:             "the next member holds while the finished member is not Ready within the wait",
		wait:             tenMinutes,
		order:            decoderEngine,
		members:          []ComponentObservation{darkened(settledOn(v1beta1.DecoderComponent, "rev2")), rollingTo(v1beta1.EngineComponent, 2, 0, 0)},
		record:           MemberFinish{Component: v1beta1.DecoderComponent, At: earlier(4 * time.Minute)},
		want:             heldBy(withFinish(awaits(v1beta1.EngineComponent, v1beta1.DecoderComponent), v1beta1.DecoderComponent, earlier(4*time.Minute)), v1beta1.DecoderComponent),
		wantMessageParts: []string{"Sequential ", ": decoder not Ready; 6m0s remaining before engine starts, once decoder is Ready"},
	},
	{
		name:             "the next member holds while the finished member is not Ready after the wait",
		wait:             tenMinutes,
		order:            decoderEngine,
		members:          []ComponentObservation{darkened(settledOn(v1beta1.DecoderComponent, "rev2")), rollingTo(v1beta1.EngineComponent, 2, 0, 0)},
		record:           MemberFinish{Component: v1beta1.DecoderComponent, At: earlier(11 * time.Minute)},
		want:             heldBy(withFinish(awaits(v1beta1.EngineComponent, v1beta1.DecoderComponent), v1beta1.DecoderComponent, earlier(11*time.Minute)), v1beta1.DecoderComponent),
		wantMessageParts: []string{"Sequential ", ": decoder not Ready; engine starts once decoder is Ready"},
	},
	{
		name:    "the first member starts at once and keeps the record of a done member",
		wait:    tenMinutes,
		order:   decoderEngine,
		members: []ComponentObservation{rollingTo(v1beta1.DecoderComponent, 2, 0, 0), settledOn(v1beta1.EngineComponent, "rev2")},
		record:  MemberFinish{Component: v1beta1.EngineComponent, At: earlier(time.Minute)},
		want: withFinish(rolls(v1beta1.DecoderComponent, v1beta1.CoordinationPhaseSurging, "creating new-revision pods", ""),
			v1beta1.EngineComponent, earlier(time.Minute)),
	},
	{
		name:    "a record of a member that rolls again is dropped",
		wait:    tenMinutes,
		order:   decoderEngine,
		members: []ComponentObservation{rollingTo(v1beta1.DecoderComponent, 2, 0, 0), settledOn(v1beta1.EngineComponent, "rev2")},
		record:  MemberFinish{Component: v1beta1.DecoderComponent, At: earlier(5 * time.Minute)},
		want:    rolls(v1beta1.DecoderComponent, v1beta1.CoordinationPhaseSurging, "creating new-revision pods", ""),
	},
	{
		name:    "a group with every member done is complete",
		order:   decoderEngine,
		members: []ComponentObservation{restingAt(v1beta1.DecoderComponent, earlier(time.Minute)), settledOn(v1beta1.EngineComponent, "rev2")},
		want:    complete,
	},
	{
		name:    "a complete group keeps the recorded finish of a done member",
		wait:    tenMinutes,
		order:   decoderEngine,
		members: []ComponentObservation{settledOn(v1beta1.DecoderComponent, "rev2"), settledOn(v1beta1.EngineComponent, "rev2")},
		record:  MemberFinish{Component: v1beta1.EngineComponent, At: earlier(time.Minute)},
		want:    withFinish(complete, v1beta1.EngineComponent, earlier(time.Minute)),
	},
	{
		name:    "a complete group drops the record of a member that left the group",
		wait:    tenMinutes,
		order:   decoderEngine,
		members: []ComponentObservation{settledOn(v1beta1.DecoderComponent, "rev2"), settledOn(v1beta1.EngineComponent, "rev2")},
		record:  MemberFinish{Component: v1beta1.RouterComponent, At: earlier(time.Minute)},
		want:    complete,
	},
	{
		name:             "a group with no member observed waits for the first change",
		wait:             tenMinutes,
		order:            decoderEngine,
		want:             GroupTransition{Phase: v1beta1.CoordinationPhaseIdle, CompositePhase: "Idle"},
		wantMessageParts: []string{"Sequential group awaiting first Component spec ", ""},
	},
	{
		name:             "a done member before an unobserved member is between members",
		order:            decoderEngine,
		members:          []ComponentObservation{restingAt(v1beta1.DecoderComponent, earlier(2*time.Minute))},
		want:             awaits(v1beta1.EngineComponent, v1beta1.DecoderComponent),
		wantMessageParts: []string{"Sequential group in ", " window between Components"},
	},
	{
		name:             "the wait between members records the done member's Ready instant",
		wait:             tenMinutes,
		order:            decoderEngine,
		members:          []ComponentObservation{restingAt(v1beta1.DecoderComponent, earlier(2*time.Minute))},
		want:             withFinish(awaits(v1beta1.EngineComponent, v1beta1.DecoderComponent), v1beta1.DecoderComponent, earlier(2*time.Minute)),
		wantMessageParts: []string{"Sequential group in ", " window between Components"},
	},
	{
		name:    "a member at its staged shape counts as done for the group",
		order:   decoderEngine,
		members: []ComponentObservation{stagedMember(v1beta1.DecoderComponent), settledOn(v1beta1.EngineComponent, "rev2")},
		want:    complete,
	},
}

// heldBy names the finished member whose readiness holds the decision.
func heldBy(decision GroupTransition, member v1beta1.ComponentType) GroupTransition {
	decision.MemberNotReady = member
	return decision
}

// messageAround reports whether message is the two parts joined by one
// lower-case word.
func messageAround(parts []string, message string) bool {
	pattern := "^" + regexp.QuoteMeta(parts[0]) + "[a-z]+" + regexp.QuoteMeta(parts[1]) + "$"
	return regexp.MustCompile(pattern).MatchString(message)
}

func TestSequentialTransition(t *testing.T) {
	for _, row := range sequentialRows {
		t.Run(row.name, func(t *testing.T) {
			obs := groupOf(v1beta1.CoordinationPolicySequential, row.members...)
			obs.Group.Components, obs.Group.Order, obs.Group.Soak = row.order, row.order, row.wait
			obs.Now, obs.MemberFinished = sequentialNow, row.record

			got := sequentialTransition(obs)

			if row.wantMessageParts != nil {
				if !messageAround(row.wantMessageParts, got.Message) {
					t.Errorf("message %q, want %q and %q around one word", got.Message, row.wantMessageParts[0], row.wantMessageParts[1])
				}
				got.Message = ""
			}
			if diff := cmp.Diff(row.want, got); diff != "" {
				t.Errorf("decision (-want +got):\n%s", diff)
			}
		})
	}
}
