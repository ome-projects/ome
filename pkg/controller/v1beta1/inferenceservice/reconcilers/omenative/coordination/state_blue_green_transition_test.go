package coordination

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// blueGreenTransition reads a BlueGreen group as one gang: a Failed member
// fails the group, no roll in flight is Idle, a member without a pod on the
// target revision makes the group Surging, a target pod that is not Ready
// makes it Waiting, rolling members at the shape a partition asks for rest
// at Staged, old pods still serving beside Ready target pods are Shifting,
// and none left is ScalingDown. Every member is read for its target pods,
// rolling or not, where a RollingUpdate group reads only the rolling ones.
// The ratio gate's held start is named in the phases where a start may
// still be asked. One row per rule: the members as observed and the whole
// decision the pass writes.

// blueGreenRow is one shape of members, either built directly or as the
// 12:8 pair the ratio gate paces.
type blueGreenRow struct {
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

var (
	blueGreenFailed      = phaseOf(v1beta1.CoordinationPhaseFailed, "one or more Components in the BlueGreen group failed")
	blueGreenIdle        = phaseOf(v1beta1.CoordinationPhaseIdle, "no rollout in flight")
	blueGreenSurging     = phaseOf(v1beta1.CoordinationPhaseSurging, "creating new-revision pods")
	blueGreenWaiting     = phaseOf(v1beta1.CoordinationPhaseWaiting, "waiting for new-revision pods to be Ready")
	blueGreenStaged      = phaseOf(v1beta1.CoordinationPhaseStaged, "converged to partitioned target; holding old-revision pods by design")
	blueGreenShifting    = phaseOf(v1beta1.CoordinationPhaseShifting, "shifting traffic; old-revision pods still serving")
	blueGreenScalingDown = phaseOf(v1beta1.CoordinationPhaseScalingDown, "scaling down old-revision pods")
)

// atShape is the member with its replica reporting the desired shape reached.
func atShape(m ComponentObservation) ComponentObservation {
	return edited(m, func(m *ComponentObservation) { m.AtDesiredShape = true })
}

// withPartition is the member with a spec that keeps partition Instances on
// the old revision.
func withPartition(m ComponentObservation, partition int32) ComponentObservation {
	return edited(m, func(m *ComponentObservation) { m.Partition = partition })
}

// emptyAtRest is a member at rest whose replica manages no Instance, so it
// has no pod on any revision.
func emptyAtRest(c v1beta1.ComponentType) ComponentObservation {
	return edited(settledOn(c, "rev2"), func(m *ComponentObservation) {
		m.DesiredReplicas, m.TotalPods, m.ReadyPods, m.ServingPods = 0, 0, 0, 0
		m.NewRevisionPods, m.NewRevisionReadyPods = 0, 0
	})
}

var blueGreenRows = []blueGreenRow{
	{
		name:    "a Failed member fails the group whatever its peer does",
		members: []ComponentObservation{flaggedFailed(v1beta1.EngineComponent), rollingTo(v1beta1.DecoderComponent, 2, 0, 0)},
		want:    blueGreenFailed,
	},
	{
		name:    "a Failed member at rest reads Failed before Idle",
		members: []ComponentObservation{flaggedFailed(v1beta1.EngineComponent), settledOn(v1beta1.DecoderComponent, "rev2")},
		want:    blueGreenFailed,
	},
	{
		name: "the held start is named beside a Failed router",
		pair: skewPair(4, 1, 1),
		want: withEngineHeld(blueGreenFailed),
	},
	{
		name:    "no member in flight is Idle",
		members: []ComponentObservation{settledOn(v1beta1.EngineComponent, "rev2"), settledOn(v1beta1.DecoderComponent, "rev2")},
		want:    blueGreenIdle,
	},
	{
		name:    "a member at rest without a pod is still Idle",
		members: []ComponentObservation{settledOn(v1beta1.EngineComponent, "rev2"), emptyAtRest(v1beta1.DecoderComponent)},
		want:    blueGreenIdle,
	},
	{
		name:    "a member without a target pod makes the group Surging",
		members: []ComponentObservation{rollingTo(v1beta1.EngineComponent, 2, 0, 0), settledOn(v1beta1.DecoderComponent, "rev2")},
		want:    blueGreenSurging,
	},
	{
		name:    "a surging member outranks a waiting peer",
		members: []ComponentObservation{rollingTo(v1beta1.EngineComponent, 2, 0, 0), rollingTo(v1beta1.DecoderComponent, 3, 1, 0)},
		want:    blueGreenSurging,
	},
	{
		name:    "a member at rest without a pod holds a rolling peer at Surging",
		members: []ComponentObservation{rollingTo(v1beta1.EngineComponent, 2, 2, 2), emptyAtRest(v1beta1.DecoderComponent)},
		want:    blueGreenSurging,
	},
	{
		name:    "a target pod that is not Ready makes the group Waiting",
		members: []ComponentObservation{rollingTo(v1beta1.EngineComponent, 3, 1, 0), settledOn(v1beta1.DecoderComponent, "rev2")},
		want:    blueGreenWaiting,
	},
	{
		name:    "a waiting member outranks a peer with old pods beside Ready target pods",
		members: []ComponentObservation{rollingTo(v1beta1.EngineComponent, 3, 1, 0), rollingTo(v1beta1.DecoderComponent, 3, 1, 1)},
		want:    blueGreenWaiting,
	},
	{
		name: "a level pair at the start of the roll holds nothing while Surging",
		pair: skewPair(0, 0, 0),
		want: blueGreenSurging,
	},
	{
		name: "the held start is named while a router has no target pod yet",
		pair: skewPair(4, 0, 0),
		want: withEngineHeld(blueGreenSurging),
	},
	{
		name: "the held start is named while a router's target pod is not Ready",
		pair: skewPair(4, 1, 0),
		edit: func(m map[v1beta1.ComponentType]*ComponentObservation) {
			m[v1beta1.RouterComponent].NewRevisionReadyPods--
		},
		want: withEngineHeld(blueGreenWaiting),
	},
	{
		name:    "rolling members at the shape their partitions ask for rest at Staged",
		members: []ComponentObservation{stagedMember(v1beta1.EngineComponent), stagedMember(v1beta1.DecoderComponent)},
		want:    blueGreenStaged,
	},
	{
		name: "a member at rest is not read for its shape",
		members: []ComponentObservation{
			stagedMember(v1beta1.EngineComponent),
			edited(settledOn(v1beta1.DecoderComponent, "rev2"), func(m *ComponentObservation) { m.AtDesiredShape = false }),
		},
		want: blueGreenStaged,
	},
	{
		name: "a partition on a member at rest does not make the group Staged",
		members: []ComponentObservation{
			withPartition(settledOn(v1beta1.EngineComponent, "rev2"), 1),
			atShape(rollingTo(v1beta1.DecoderComponent, 2, 2, 2)),
		},
		want: blueGreenScalingDown,
	},
	{
		name: "a rolling member below the shape its partition asks for is not Staged",
		members: []ComponentObservation{
			withPartition(rollingTo(v1beta1.EngineComponent, 2, 1, 1), 1),
			stagedMember(v1beta1.DecoderComponent),
		},
		want: blueGreenShifting,
	},
	{
		name: "members at their shape without a partition scale down",
		members: []ComponentObservation{
			atShape(rollingTo(v1beta1.EngineComponent, 2, 2, 2)),
			atShape(rollingTo(v1beta1.DecoderComponent, 2, 2, 2)),
		},
		want: blueGreenScalingDown,
	},
	{
		name:    "a staged member beside a surging peer is Surging",
		members: []ComponentObservation{stagedMember(v1beta1.EngineComponent), rollingTo(v1beta1.DecoderComponent, 2, 0, 0)},
		want:    blueGreenSurging,
	},
	{
		name:    "a staged member beside a waiting peer is Waiting",
		members: []ComponentObservation{stagedMember(v1beta1.EngineComponent), rollingTo(v1beta1.DecoderComponent, 3, 1, 0)},
		want:    blueGreenWaiting,
	},
	{
		name: "a member whose partition covers every Instance reads Surging before Staged",
		members: []ComponentObservation{
			atShape(withPartition(rollingTo(v1beta1.EngineComponent, 2, 0, 0), 2)),
			settledOn(v1beta1.DecoderComponent, "rev2"),
		},
		want: blueGreenSurging,
	},
	{
		name: "a target pod that is not Ready reads Waiting before Staged, whatever shape the replica last reported",
		members: []ComponentObservation{
			atShape(withPartition(rollingTo(v1beta1.EngineComponent, 3, 1, 0), 1)),
			stagedMember(v1beta1.DecoderComponent),
		},
		want: blueGreenWaiting,
	},
	{
		name: "a group at rest on a partition holds nothing",
		pair: skewPair(4, 1, 0),
		edit: func(m map[v1beta1.ComponentType]*ComponentObservation) {
			for _, co := range m {
				co.Partition, co.AtDesiredShape = 1, true
			}
		},
		want: blueGreenStaged,
	},
	{
		name:    "old pods beside Ready target pods are Shifting",
		members: []ComponentObservation{rollingTo(v1beta1.EngineComponent, 2, 1, 1), rollingTo(v1beta1.DecoderComponent, 2, 1, 1)},
		want:    blueGreenShifting,
	},
	{
		name:    "a member at rest does not hold the group from Shifting",
		members: []ComponentObservation{rollingTo(v1beta1.EngineComponent, 2, 1, 1), settledOn(v1beta1.DecoderComponent, "rev2")},
		want:    blueGreenShifting,
	},
	{
		name: "the engine the ratio gate holds is named while the group shifts",
		pair: skewPair(4, 1, 0),
		want: withEngineHeld(blueGreenShifting),
	},
	{
		name:    "every pod on the target and Ready is ScalingDown",
		members: []ComponentObservation{rollingTo(v1beta1.EngineComponent, 2, 2, 2), rollingTo(v1beta1.DecoderComponent, 2, 2, 2)},
		want:    blueGreenScalingDown,
	},
	{
		name: "nothing left to start holds nothing while scaling down",
		pair: skewPair(4, 1, 0),
		edit: func(m map[v1beta1.ComponentType]*ComponentObservation) {
			for _, co := range m {
				co.NewRevisionPods, co.NewRevisionReadyPods = co.TotalPods, co.TotalPods
			}
		},
		want: blueGreenScalingDown,
	},
}

func TestBlueGreenTransition(t *testing.T) {
	for _, row := range blueGreenRows {
		t.Run(row.name, func(t *testing.T) {
			var got GroupTransition
			if row.pair != nil {
				got = skewTransition(t, v1beta1.CoordinationPolicyBlueGreen, row.pair, row.edit)
			} else {
				got = blueGreenTransition(groupOf(v1beta1.CoordinationPolicyBlueGreen, row.members...))
			}
			if diff := cmp.Diff(row.want, got); diff != "" {
				t.Errorf("decision (-want +got):\n%s", diff)
			}
		})
	}
}
