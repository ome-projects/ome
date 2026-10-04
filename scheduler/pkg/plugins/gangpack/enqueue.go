package gangpack

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/klog/v2"
	"k8s.io/kube-scheduler/framework"
	"sigs.k8s.io/scheduler-plugins/apis/scheduling"
)

// With QueueingHints (GA in recent kube-scheduler), a pod a plugin rejected is
// requeued only on cluster events that plugin has REGISTERED — otherwise it waits
// out the scheduler's slow periodic unschedulable flush. GangPack rejects pods
// two ways, each with its own unblocking events:
//
//   - Permit gate / Unreserve: the gang is incomplete. It becomes admissible when
//     a sibling arrives (Pod Add) or the PodGroup changes (e.g. minMember drops).
//   - PreFilter Unschedulable: no domain currently fits the gang. It becomes
//     admissible when capacity frees (Pod Delete) or is added (Node Add/update).
//
// Registering these is what makes a gang recover promptly instead of on the
// periodic flush. Queueing hints keep unrelated pod/node/PodGroup churn from
// waking every blocked gang at once. The queue only routes an Add for an
// already-assigned pod; an unscheduled sibling's creation reaches parked members
// through the plugin's own activation (see activateGangMembers). PodGroup events
// are routed from the scheduler's own PodGroup watch, which can run ahead of the
// informer PreFilter reads, so the plugin's informer also wakes a gang's members
// when it stores the change (see podGroupChanged).
func (g *GangPack) EventsToRegister(_ context.Context) ([]framework.ClusterEventWithHint, error) {
	// Custom-resource events are named "<resource>.<version>.<group>".
	pgGVK := fmt.Sprintf("podgroups.v1alpha1.%v", scheduling.GroupName)
	return []framework.ClusterEventWithHint{
		// A sibling arriving can complete a gated gang.
		{Event: framework.ClusterEvent{Resource: framework.Pod, ActionType: framework.Add}, QueueingHintFn: g.isSchedulableAfterPodAdd},
		// Mutable membership and tolerations can change heterogeneous feasibility.
		{Event: framework.ClusterEvent{Resource: framework.Pod, ActionType: framework.UpdatePodLabel}, QueueingHintFn: g.isSchedulableAfterPodLabelChange},
		{Event: framework.ClusterEvent{Resource: framework.Pod, ActionType: framework.UpdatePodToleration}, QueueingHintFn: g.isSchedulableAfterPodTolerationChange},
		// A scheduled pod leaving or scaling down can free capacity or another
		// Filter constraint. Node selectors and required affinity are immutable.
		{Event: framework.ClusterEvent{Resource: framework.Pod, ActionType: framework.Delete | framework.UpdatePodScaleDown}, QueueingHintFn: g.isSchedulableAfterPodCapacityChange},
		// A new node, or one that gains allocatable / sheds a taint / turns Ready,
		// may let a gang that fit no domain now fit.
		{Event: framework.ClusterEvent{Resource: framework.Node, ActionType: framework.Add | framework.UpdateNodeAllocatable | framework.UpdateNodeTaint | framework.UpdateNodeCondition | framework.UpdateNodeLabel}, QueueingHintFn: g.isSchedulableAfterNodeChange},
		// Losing a node from a pinned domain may make the pin stale and force a
		// safe re-plan onto another domain.
		{Event: framework.ClusterEvent{Resource: framework.Node, ActionType: framework.Delete}, QueueingHintFn: g.isSchedulableAfterNodeDelete},
		// PodGroup changes (minMember, or the PodGroup first appearing) may make a
		// gang admissible.
		{Event: framework.ClusterEvent{Resource: framework.EventResource(pgGVK), ActionType: framework.Add | framework.Update | framework.Delete}, QueueingHintFn: g.isSchedulableAfterPodGroupChange},
	}, nil
}

func (g *GangPack) isSchedulableAfterPodLabelChange(_ klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (framework.QueueingHint, error) {
	oldPod, oldOK := oldObj.(*v1.Pod)
	newPod, newOK := newObj.(*v1.Pod)
	if !oldOK || !newOK {
		return framework.Queue, fmt.Errorf("pod label event has objects %T/%T", oldObj, newObj)
	}
	if samePodIdentity(pod, newPod) {
		return framework.Queue, nil
	}
	namespace, group, targetOK := podGroupNameOf(pod)
	oldNS, oldGroup, wasSibling := podGroupNameOf(oldPod)
	newNS, newGroup, isSibling := podGroupNameOf(newPod)
	if targetOK && ((wasSibling && namespace == oldNS && group == oldGroup) ||
		(isSibling && namespace == newNS && group == newGroup)) {
		return framework.Queue, nil
	}
	return framework.QueueSkip, nil
}

func (g *GangPack) isSchedulableAfterPodAdd(_ klog.Logger, pod *v1.Pod, _, newObj interface{}) (framework.QueueingHint, error) {
	added, ok := newObj.(*v1.Pod)
	if !ok {
		return framework.Queue, fmt.Errorf("pod add event has object %T", newObj)
	}
	targetNS, targetGroup, targetOK := podGroupNameOf(pod)
	addedNS, addedGroup, addedOK := podGroupNameOf(added)
	if targetOK && addedOK && targetNS == addedNS && targetGroup == addedGroup {
		return framework.Queue, nil
	}
	return framework.QueueSkip, nil
}

func (g *GangPack) isSchedulableAfterPodCapacityChange(_ klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (framework.QueueingHint, error) {
	oldPod, ok := oldObj.(*v1.Pod)
	if !ok {
		return framework.Queue, fmt.Errorf("pod capacity event has old object %T", oldObj)
	}
	var newPod *v1.Pod
	if newObj != nil {
		newPod, ok = newObj.(*v1.Pod)
		if !ok {
			return framework.Queue, fmt.Errorf("pod capacity event has new object %T", newObj)
		}
		if samePodIdentity(pod, newPod) {
			return framework.Queue, nil
		}
	}
	// An unscheduled sibling's request change can alter the heterogeneous matching
	// even though it frees no assigned capacity itself.
	if samePodGroup(pod, oldPod) || samePodGroup(pod, newPod) {
		return framework.Queue, nil
	}
	if oldPod.Spec.NodeName == "" && oldPod.Status.NominatedNodeName == "" {
		return framework.QueueSkip, nil
	}
	// Delete and UpdatePodScaleDown are capacity-improving by definition. Even an
	// unrelated resource can unblock allocatable.pods, ports, or another Filter.
	return framework.Queue, nil
}

func samePodGroup(a, b *v1.Pod) bool {
	nsA, aName, aOK := podGroupNameOf(a)
	nsB, bName, bOK := podGroupNameOf(b)
	return aOK && bOK && nsA == nsB && aName == bName
}

func samePodIdentity(a, b *v1.Pod) bool {
	if a == nil || b == nil {
		return false
	}
	if a.UID != "" && b.UID != "" {
		return a.UID == b.UID
	}
	return a.Namespace == b.Namespace && a.Name != "" && a.Name == b.Name
}

func (g *GangPack) isSchedulableAfterPodTolerationChange(_ klog.Logger, pod *v1.Pod, _, newObj interface{}) (framework.QueueingHint, error) {
	newPod, ok := newObj.(*v1.Pod)
	if !ok {
		return framework.Queue, fmt.Errorf("pod toleration event has new object %T", newObj)
	}
	if samePodIdentity(pod, newPod) || samePodGroup(pod, newPod) {
		return framework.Queue, nil
	}
	return framework.QueueSkip, nil
}

func (g *GangPack) isSchedulableAfterNodeChange(_ klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (framework.QueueingHint, error) {
	newNode, ok := newObj.(*v1.Node)
	if !ok {
		return framework.Queue, fmt.Errorf("node event has new object %T", newObj)
	}
	topologyKey := g.topologyKeyFor(pod)
	if topologyKey == "" {
		return framework.Queue, nil
	}
	if oldObj != nil {
		oldNode, ok := oldObj.(*v1.Node)
		if !ok {
			return framework.Queue, fmt.Errorf("node event has old object %T", oldObj)
		}
		if oldNode.Labels[topologyKey] != "" || newNode.Labels[topologyKey] != "" {
			return framework.Queue, nil
		}
		return framework.QueueSkip, nil
	}
	if newNode.Labels[topologyKey] != "" {
		return framework.Queue, nil
	}
	return framework.QueueSkip, nil
}

func (g *GangPack) isSchedulableAfterNodeDelete(_ klog.Logger, pod *v1.Pod, oldObj, _ interface{}) (framework.QueueingHint, error) {
	deleted, ok := oldObj.(*v1.Node)
	if !ok {
		return framework.Queue, fmt.Errorf("node delete event has old object %T", oldObj)
	}
	topologyKey := g.topologyKeyFor(pod)
	if topologyKey == "" || deleted.Labels[topologyKey] != "" {
		return framework.Queue, nil
	}
	return framework.QueueSkip, nil
}

func (g *GangPack) topologyKeyFor(pod *v1.Pod) string {
	if g.pgReader != nil {
		if gang, ok := resolveGang(pod, g.pgReader); ok {
			return gang.topologyKey
		}
	}
	return g.topologyKey
}

func (g *GangPack) isSchedulableAfterPodGroupChange(_ klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (framework.QueueingHint, error) {
	namespace, name, ok := podGroupNameOf(pod)
	if !ok {
		return framework.QueueSkip, nil
	}
	obj := newObj
	if obj == nil {
		obj = oldObj
	}
	accessor, err := apiMeta.Accessor(obj)
	if err != nil {
		return framework.Queue, err
	}
	if accessor.GetNamespace() == namespace && accessor.GetName() == name {
		return framework.Queue, nil
	}
	return framework.QueueSkip, nil
}
