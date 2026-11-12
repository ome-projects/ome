package canary

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/rollout"
)

// The stable side of a split. A step keeps a fixed number of Instances on
// the stable revision and sends the rest of the traffic there. The traffic
// follows the Ready endpoints, so while a member runs fewer Ready Instances
// on its stable revision than the step keeps, the canary revision carries a
// larger share of the Ready capacity than the split's weight. The executor
// does not move the split for it: the workload's repair settles it. It
// records the dip on the record while it stands and tells the operator once
// per step whose split the dip distorts. A stable side with no Ready
// Instance is not short but gone: the split itself moves then, and the
// traffic writer speaks for it.

// shortMember is one member whose stable side is short: the Ready Instances
// on its stable revision against the count the step keeps there, and the
// canary's share of the member's Ready capacity.
type shortMember struct {
	component   v1beta1.ComponentType
	stable      string
	readyStable int32
	kept        int32
	share       int32
}

// memberCapacity is one member's Ready capacity on the two revisions of a
// split and the Instance count it is scaled to: the replica's account when
// a replica was observed, else the Ready pods that stand in for it.
type memberCapacity struct {
	readyCanary int32
	readyStable int32
	desired     int32
}

// capacityOf reads a member's capacity: the primary from the pass inputs,
// a secondary from the revisions its replica published.
func capacityOf(in ReconcileInputs, side memberSide) memberCapacity {
	if side.component == in.Component {
		return memberCapacity{readyCanary: readyCanaryCapacity(in), readyStable: readyStableCapacity(in), desired: in.DesiredReplicas}
	}
	m := in.Secondaries[side.component]
	return memberCapacity{
		readyCanary: m.ReadyCanaryCapacity,
		readyStable: canaryCapacity(m.ReadyStableInstances, side.readyPods[m.StableRevisionHash]),
		desired:     m.DesiredReplicas,
	}
}

// stableSideShort lists the members whose stable side is short under the
// split that serves: at least one Ready Instance on the stable revision,
// fewer than the step keeps there, and the canary's share of the member's
// Ready capacity above the split's weight.
func stableSideShort(in ReconcileInputs, step v1beta1.RolloutGroupStep, weight int32) []shortMember {
	var short []shortMember
	for _, side := range unitSides(in, in.CanaryRevisionHash, in.StableRevisionHash) {
		if m, ok := shortUnder(side, capacityOf(in, side), step, weight); ok {
			short = append(short, m)
		}
	}
	return short
}

// shortUnder reads one member against the split a step serves. A member
// whose target is its stable revision, one with no stable revision, and one
// with no pod view are not read, nor is a member with a Ready pod on a
// third revision: a retarget mid-roll leaves the revision it left serving
// beside the two the split names, and the step's kept count says nothing
// about it.
func shortUnder(side memberSide, capacity memberCapacity, step v1beta1.RolloutGroupStep, weight int32) (shortMember, bool) {
	if side.readyPods == nil || side.stable == "" || side.target == "" || side.target == side.stable || servesOtherRevisions(side.readyPods, side.target, side.stable) {
		return shortMember{}, false
	}
	kept := stepPartition(step, capacity.desired)
	if capacity.readyStable <= 0 || capacity.readyStable >= kept {
		return shortMember{}, false
	}
	share := shareOfReady(capacity.readyCanary, capacity.readyStable)
	if share <= weight {
		return shortMember{}, false
	}
	return shortMember{component: side.component, stable: side.stable, readyStable: capacity.readyStable, kept: kept, share: share}, true
}

// servesOtherRevisions reports whether a member has a Ready pod on a revision
// other than the two the split names.
func servesOtherRevisions(pods map[string]int32, canary, stable string) bool {
	for hash, count := range pods {
		if count > 0 && hash != canary && hash != stable {
			return true
		}
	}
	return false
}

// shareOfReady is the canary's percentage of the Ready Instances on the two
// revisions, rounded down; nothing Ready on either is no share.
func shareOfReady(readyCanary, readyStable int32) int32 {
	if readyCanary+readyStable <= 0 {
		return 0
	}
	return readyCanary * 100 / (readyCanary + readyStable)
}

// observeStableSide reads the stable side against the split the pass leaves
// and keeps the record in step with it. Only a partial split a step serves
// can be distorted: Serving and PreHold, with a weight between the two
// ends. Every other state clears the record: a staging retires stable
// Instances by design, so its passes read no dip, and a dip that stands
// when the next step serves is read again under that step's split, with
// that step's numbers. The event is raised on the pass that sets the
// record, not on the passes it stands.
func observeStableSide(in ReconcileInputs, cs *v1beta1.CanaryStatus, plan *v1beta1.GroupCanary) {
	if cs == nil {
		return
	}
	state := rollout.StateOf(cs, in.ISVC.Status.Components[in.Component].RolloutPhase, len(plan.Steps))
	weight := cs.ObservedTrafficWeight
	splitServes := weight > 0 && weight < 100 && int(cs.CurrentStep) < len(plan.Steps) &&
		(state == rollout.CanaryStateServing || state == rollout.CanaryStatePreHold)
	if !splitServes {
		cs.StableSideShortSince = nil
		return
	}
	short := stableSideShort(in, plan.Steps[cs.CurrentStep], weight)
	if len(short) == 0 {
		cs.StableSideShortSince = nil
		return
	}
	if cs.StableSideShortSince != nil {
		return
	}
	cs.StableSideShortSince = &metav1.Time{Time: in.Now}
	emit(in.Recorder, in.ISVC, corev1.EventTypeWarning, EventReasonCanaryStableSideShort,
		"%s canary step %d: stable side short: %s; the traffic follows the Ready endpoints, so the canary revision carries more than its step until the stable side is whole again",
		in.Component, cs.CurrentStep, shortMembersText(short, weight))
}

// shortMembersText spells each short member out in the operator's numbers:
// "8 of 10 engine Instances Ready on stable revision old, canary share 20
// percent above the split's 10 percent".
func shortMembersText(short []shortMember, weight int32) string {
	parts := make([]string, 0, len(short))
	for _, m := range short {
		parts = append(parts, fmt.Sprintf("%d of %d %s Instances Ready on stable revision %s, canary share %d percent above the split's %d percent",
			m.readyStable, m.kept, m.component, m.stable, m.share, weight))
	}
	return strings.Join(parts, "; ")
}

// clearStableSideShort ends the record of a short stable side: a stopped
// unit serves no split the dip can distort.
func clearStableSideShort(cs *v1beta1.CanaryStatus) {
	if cs != nil {
		cs.StableSideShortSince = nil
	}
}
