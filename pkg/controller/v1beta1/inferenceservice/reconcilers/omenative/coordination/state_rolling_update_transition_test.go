package coordination

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// rollingUpdateTransition reads a RollingUpdate group as the union of its
// members' rolls: a Failed member fails the group, no roll in flight is Idle,
// a member without a new pod makes the group Surging, one whose new pod is
// not Ready makes it Waiting, old pods beside Ready new ones make it
// ScalingDown, every pod new and Ready is Shifting, and rolling members at
// the shape a partition asks for rest at Staged. The ratio gate's held start
// is named in the phases where a start may still be asked. One row per rule:
// the members as observed and the whole decision the pass writes.

// rollingUpdateRow is one shape of members, either built directly or as the
// 12:8 pair the ratio gate paces.
type rollingUpdateRow struct {
	name string
	// members are the observations the group reads.
	members []ComponentObservation
	// pair is the engine and router pair under maintainRatio at tolerance
	// 20, with the given counts on version 2; edit changes the observations
	// before the pass.
	pair map[v1beta1.ComponentType]batchMemberFixture
	edit func(map[v1beta1.ComponentType]*ComponentObservation)
	want GroupTransition
}

// phaseOf is a decision with the phase and its message and no held start.
func phaseOf(phase v1beta1.CoordinationPhase, message string) GroupTransition {
	return GroupTransition{Phase: phase, CompositePhase: string(phase), Message: message}
}

// withEngineHeld adds the engine's surge start as the one the ratio gate holds.
func withEngineHeld(decision GroupTransition) GroupTransition {
	decision.RatioSkewRejected = true
	decision.RatioHeldStarts = []HeldStart{{Component: v1beta1.EngineComponent, Strategy: workloadtypes.UpdateStrategySurgeThenDrain}}
	return decision
}

var (
	rollingFailed      = phaseOf(v1beta1.CoordinationPhaseFailed, "one or more Components in the RollingUpdate group failed")
	rollingIdle        = phaseOf(v1beta1.CoordinationPhaseIdle, "no rollout in flight")
	rollingSurging     = phaseOf(v1beta1.CoordinationPhaseSurging, "at least one Component is surging")
	rollingWaiting     = phaseOf(v1beta1.CoordinationPhaseWaiting, "waiting for new-revision pods to be Ready")
	rollingScalingDown = phaseOf(v1beta1.CoordinationPhaseScalingDown, "scaling down old-revision pods")
	rollingShifting    = phaseOf(v1beta1.CoordinationPhaseShifting, "writing per-Component traffic weights")
	rollingStaged      = phaseOf(v1beta1.CoordinationPhaseStaged, "converged to partitioned target; holding old-revision pods by design")
)

// flaggedFailed is a member at rest whose reconciler flagged a terminal failure.
func flaggedFailed(c v1beta1.ComponentType) ComponentObservation {
	return edited(settledOn(c, "rev2"), func(m *ComponentObservation) { m.Failed = true })
}

var rollingUpdateRows = []rollingUpdateRow{
	{
		name:    "a Failed member fails the group whatever its peer does",
		members: []ComponentObservation{flaggedFailed(v1beta1.EngineComponent), rollingTo(v1beta1.DecoderComponent, 2, 0, 0)},
		want:    rollingFailed,
	},
	{
		name:    "no member in flight is Idle",
		members: []ComponentObservation{settledOn(v1beta1.EngineComponent, "rev2"), settledOn(v1beta1.DecoderComponent, "rev2")},
		want:    rollingIdle,
	},
	{
		name:    "a member without a new pod makes the group Surging",
		members: []ComponentObservation{rollingTo(v1beta1.EngineComponent, 2, 0, 0), settledOn(v1beta1.DecoderComponent, "rev2")},
		want:    rollingSurging,
	},
	{
		name:    "a surging member outranks a waiting peer",
		members: []ComponentObservation{rollingTo(v1beta1.EngineComponent, 2, 0, 0), rollingTo(v1beta1.DecoderComponent, 3, 1, 0)},
		want:    rollingSurging,
	},
	{
		name:    "a member whose new pod is not Ready makes the group Waiting",
		members: []ComponentObservation{rollingTo(v1beta1.EngineComponent, 3, 1, 0), settledOn(v1beta1.DecoderComponent, "rev2")},
		want:    rollingWaiting,
	},
	{
		name:    "a waiting member outranks a peer scaling down",
		members: []ComponentObservation{rollingTo(v1beta1.EngineComponent, 3, 1, 0), rollingTo(v1beta1.DecoderComponent, 3, 1, 1)},
		want:    rollingWaiting,
	},
	{
		name:    "old pods beside Ready new ones make the group ScalingDown",
		members: []ComponentObservation{rollingTo(v1beta1.EngineComponent, 3, 1, 1), settledOn(v1beta1.DecoderComponent, "rev2")},
		want:    rollingScalingDown,
	},
	{
		name:    "members with every pod new and Ready are Shifting",
		members: []ComponentObservation{rollingTo(v1beta1.EngineComponent, 2, 2, 2), rollingTo(v1beta1.DecoderComponent, 2, 2, 2)},
		want:    rollingShifting,
	},
	{
		name:    "a member at rest does not hold the group from Shifting",
		members: []ComponentObservation{rollingTo(v1beta1.EngineComponent, 2, 2, 2), settledOn(v1beta1.DecoderComponent, "rev2")},
		want:    rollingShifting,
	},
	{
		name: "a member at rest is not read for its pods",
		members: []ComponentObservation{
			rollingTo(v1beta1.EngineComponent, 2, 2, 2),
			edited(settledOn(v1beta1.DecoderComponent, "rev2"), func(m *ComponentObservation) { m.NewRevisionPods = 0 }),
		},
		want: rollingShifting,
	},
	{
		name:    "a rolling member at the shape its partition asks for rests the group at Staged",
		members: []ComponentObservation{stagedMember(v1beta1.EngineComponent), settledOn(v1beta1.DecoderComponent, "rev2")},
		want:    rollingStaged,
	},
	{
		name:    "two members at their staged shapes rest at Staged",
		members: []ComponentObservation{stagedMember(v1beta1.EngineComponent), stagedMember(v1beta1.DecoderComponent)},
		want:    rollingStaged,
	},
	{
		name:    "a staged member beside a peer still scaling down is ScalingDown",
		members: []ComponentObservation{stagedMember(v1beta1.EngineComponent), rollingTo(v1beta1.DecoderComponent, 3, 1, 1)},
		want:    rollingScalingDown,
	},
	{
		name:    "a staged member beside a surging peer is Surging",
		members: []ComponentObservation{stagedMember(v1beta1.EngineComponent), rollingTo(v1beta1.DecoderComponent, 2, 0, 0)},
		want:    rollingSurging,
	},
	{
		name:    "a staged member beside a waiting peer is Waiting",
		members: []ComponentObservation{stagedMember(v1beta1.EngineComponent), rollingTo(v1beta1.DecoderComponent, 3, 1, 0)},
		want:    rollingWaiting,
	},
	{
		name: "a member at its shape without a partition is not Staged",
		members: []ComponentObservation{
			edited(stagedMember(v1beta1.EngineComponent), func(m *ComponentObservation) { m.Partition = 0 }),
			settledOn(v1beta1.DecoderComponent, "rev2"),
		},
		want: rollingScalingDown,
	},
	{
		name: "a member whose partition covers every Instance reads Surging before Staged",
		members: []ComponentObservation{
			edited(rollingTo(v1beta1.EngineComponent, 2, 0, 0), func(m *ComponentObservation) { m.Partition, m.AtDesiredShape = 2, true }),
			settledOn(v1beta1.DecoderComponent, "rev2"),
		},
		want: rollingSurging,
	},
	{
		name: "a partition on a member at rest does not make the group Staged",
		members: []ComponentObservation{
			edited(settledOn(v1beta1.EngineComponent, "rev2"), func(m *ComponentObservation) { m.Partition = 1 }),
			edited(rollingTo(v1beta1.DecoderComponent, 2, 2, 2), func(m *ComponentObservation) { m.AtDesiredShape = true }),
		},
		want: rollingShifting,
	},
	{
		name: "the engine the ratio gate holds is named while the group scales down",
		pair: skewPair(4, 1, 0),
		want: withEngineHeld(rollingScalingDown),
	},
	{
		name: "the held start is named beside a Failed router",
		pair: skewPair(4, 1, 1),
		want: withEngineHeld(rollingFailed),
	},
	{
		name: "a level pair at the start of the roll holds nothing while Surging",
		pair: skewPair(0, 0, 0),
		want: rollingSurging,
	},
	{
		name: "the held start is named while a router's new pod is not Ready",
		pair: skewPair(4, 1, 0),
		edit: func(m map[v1beta1.ComponentType]*ComponentObservation) {
			m[v1beta1.RouterComponent].NewRevisionReadyPods--
		},
		want: withEngineHeld(rollingWaiting),
	},
	{
		name: "nothing left to start holds nothing while Shifting",
		pair: skewPair(4, 1, 0),
		edit: func(m map[v1beta1.ComponentType]*ComponentObservation) {
			for _, co := range m {
				co.NewRevisionPods, co.NewRevisionReadyPods = co.TotalPods, co.TotalPods
			}
		},
		want: rollingShifting,
	},
	{
		name: "a group at rest on a partition holds nothing",
		pair: skewPair(4, 1, 0),
		edit: func(m map[v1beta1.ComponentType]*ComponentObservation) {
			for _, co := range m {
				co.Partition, co.AtDesiredShape = 1, true
			}
		},
		want: rollingStaged,
	},
}

func TestRollingUpdateTransition(t *testing.T) {
	for _, row := range rollingUpdateRows {
		t.Run(row.name, func(t *testing.T) {
			var got GroupTransition
			if row.pair != nil {
				got = skewTransition(t, v1beta1.CoordinationPolicyRollingUpdate, row.pair, row.edit)
			} else {
				got = rollingUpdateTransition(groupOf(v1beta1.CoordinationPolicyRollingUpdate, row.members...))
			}
			if diff := cmp.Diff(row.want, got); diff != "" {
				t.Errorf("decision (-want +got):\n%s", diff)
			}
		})
	}
}
