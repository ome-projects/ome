package coordination

import (
	"k8s.io/apimachinery/pkg/api/equality"
	"knative.dev/pkg/apis"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// LiveGroupEntries is the live object's group entries and their readiness
// condition, taken by the status flush before it writes the pass's status
// over the live one.
type LiveGroupEntries struct {
	groups map[string]v1beta1.RolloutCoordinationGroupStatus
	ready  *apis.Condition
}

// SnapshotLiveGroupEntries takes the group entries and the
// RolloutCoordinationReady condition of the live status.
func SnapshotLiveGroupEntries(live *v1beta1.InferenceServiceStatus) LiveGroupEntries {
	out := LiveGroupEntries{}
	if live == nil {
		return out
	}
	if live.RolloutCoordination != nil {
		out.groups = make(map[string]v1beta1.RolloutCoordinationGroupStatus, len(live.RolloutCoordination.Groups))
		for i := range live.RolloutCoordination.Groups {
			group := live.RolloutCoordination.Groups[i].DeepCopy()
			out.groups[group.Name] = *group
		}
	}
	if cond := live.GetCondition(apis.ConditionType(v1beta1.RolloutCoordinationReady)); cond != nil {
		out.ready = cond.DeepCopy()
	}
	return out
}

// KeepPhaseTimes gives the entries of dst the instants the live object
// already holds for what the pass computed again. A pass compares the phase
// it computes with the copy of the service it read; a copy that trails the
// previous pass's write sees that write's change once more, so the entry it
// carries names the live phase with a transition time of now. For each group
// the live object knows: the same phase keeps the live transition time, a
// finish record for the same member keeps the earlier instant, the one the
// member first read done at, and the readiness condition keeps the live
// transition time while it is the same condition apart from that time. A
// phase the live entry does not hold is a transition and takes the pass's
// time. The entries are changed in place, so a working copy that shares them
// reads the kept instants too.
func (l LiveGroupEntries) KeepPhaseTimes(dst *v1beta1.InferenceServiceStatus) {
	if dst == nil {
		return
	}
	if dst.RolloutCoordination != nil {
		for i := range dst.RolloutCoordination.Groups {
			entry := &dst.RolloutCoordination.Groups[i]
			live, known := l.groups[entry.Name]
			if !known {
				continue
			}
			if live.Phase == entry.Phase && live.LastTransitionTime != nil {
				entry.LastTransitionTime = live.LastTransitionTime.DeepCopy()
			}
			keepEarlierFinish(entry, &live)
		}
	}
	l.keepReadyTime(dst)
}

// keepEarlierFinish keeps the live finish record's instant on entry when both
// records name the same member and the live one is earlier: the record is
// the instant the member first read done, and a pass that takes it again
// reads a later one.
func keepEarlierFinish(entry, live *v1beta1.RolloutCoordinationGroupStatus) {
	if entry.MemberFinished == nil || live.MemberFinished == nil {
		return
	}
	if live.MemberFinished.Component != entry.MemberFinished.Component {
		return
	}
	if live.MemberFinished.At.Time.Before(entry.MemberFinished.At.Time) {
		entry.MemberFinished.At = live.MemberFinished.At
	}
}

// keepReadyTime keeps the live readiness condition's transition time on dst
// while dst carries the same condition apart from that time, the rule the
// condition manager applies against the object it is given.
func (l LiveGroupEntries) keepReadyTime(dst *v1beta1.InferenceServiceStatus) {
	if l.ready == nil {
		return
	}
	for i := range dst.Conditions {
		cond := &dst.Conditions[i]
		if cond.Type != l.ready.Type {
			continue
		}
		same := *l.ready
		same.LastTransitionTime = cond.LastTransitionTime
		if equality.Semantic.DeepEqual(same, *cond) {
			cond.LastTransitionTime = l.ready.LastTransitionTime
		}
		return
	}
}
