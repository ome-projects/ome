package gangpack

import (
	"context"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/kube-scheduler/framework"

	"sigs.k8s.io/ome/scheduler/pkg/placement"
)

// TestEventsToRegister_Always_CoversPodNodeAndPodGroupWithHints: the plugin must register the cluster events that unblock
// a rejected gang — Pod (siblings / capacity), Node (capacity), and the PodGroup
// CR (minMember). Without these, QueueingHints leaves a rejected member waiting
// out the scheduler's slow periodic flush.
func TestEventsToRegister_Always_CoversPodNodeAndPodGroupWithHints(t *testing.T) {
	g := &GangPack{}
	events, err := g.EventsToRegister(context.Background())
	if err != nil {
		t.Fatalf("EventsToRegister error: %v", err)
	}

	byResource := map[framework.EventResource]framework.ActionType{}
	for _, e := range events {
		byResource[e.Event.Resource] |= e.Event.ActionType
		if e.QueueingHintFn == nil {
			t.Errorf("event %s/%v has no QueueingHintFn", e.Event.Resource, e.Event.ActionType)
		}
	}

	// Pod: a sibling arriving (Add) completes a gated gang; Delete frees capacity.
	if got := byResource[framework.Pod]; got&framework.Add == 0 || got&framework.Delete == 0 {
		t.Errorf("Pod event = %v, want at least Add|Delete", got)
	}
	// Node: new/updated capacity may let a placement-blocked gang fit.
	if got, ok := byResource[framework.Node]; !ok || got&framework.Add == 0 {
		t.Errorf("Node event = %v (present=%v), want at least Add", got, ok)
	}
	// PodGroup CR: a minMember change may make a gang admissible.
	pgGVK := framework.EventResource("podgroups.v1alpha1.scheduling.x-k8s.io")
	if got, ok := byResource[pgGVK]; !ok || got&framework.Update == 0 {
		t.Errorf("PodGroup event = %v (present=%v), want at least Update", got, ok)
	}
}

func TestPodAddHint_SiblingOrOtherPodAdded_QueuesOnlyForSibling(t *testing.T) {
	g := &GangPack{}
	target := gangGPUPod("team", "gang", "4")
	for name, added := range map[string]struct {
		pod  *v1.Pod
		hint framework.QueueingHint
	}{
		"sibling":         {gangGPUPod("team", "gang", "4"), framework.Queue},
		"other gang":      {gangGPUPod("team", "other", "4"), framework.QueueSkip},
		"other namespace": {gangGPUPod("other", "gang", "4"), framework.QueueSkip},
		"standalone":      {gpuPod("4"), framework.QueueSkip},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := g.isSchedulableAfterPodAdd(klog.Background(), target, nil, added.pod)
			if err != nil || got != added.hint {
				t.Fatalf("hint = %v, %v, want %v", got, err, added.hint)
			}
		})
	}
}

func TestPodCapacityHint_PodDeleted_QueuesOnlyWhenItWasScheduled(t *testing.T) {
	g := &GangPack{}
	target := gangGPUPod("team", "gang", "4")
	matching := gpuPod("4")
	matching.Spec.NodeName = "node-a"
	if got, _ := g.isSchedulableAfterPodCapacityChange(klog.Background(), target, matching, nil); got != framework.Queue {
		t.Fatalf("scheduled matching deletion hint = %v, want Queue", got)
	}
	matching.Spec.NodeName = ""
	if got, _ := g.isSchedulableAfterPodCapacityChange(klog.Background(), target, matching, nil); got != framework.QueueSkip {
		t.Fatalf("unscheduled deletion hint = %v, want QueueSkip", got)
	}
	standardOnly := &v1.Pod{Spec: v1.PodSpec{NodeName: "node-a", Containers: []v1.Container{{}}}}
	if got, _ := g.isSchedulableAfterPodCapacityChange(klog.Background(), target, standardOnly, nil); got != framework.Queue {
		t.Fatalf("unrelated deletion hint = %v, want Queue because it frees a pod slot", got)
	}
	cpuTarget := gangPod("team", "gang")
	cpuTarget.Spec.Containers = []v1.Container{{Resources: v1.ResourceRequirements{Requests: v1.ResourceList{
		v1.ResourceCPU: resource.MustParse("500m"),
	}}}}
	cpuDeleted := cpuTarget.DeepCopy()
	cpuDeleted.Spec.NodeName = "node-a"
	if got, _ := g.isSchedulableAfterPodCapacityChange(klog.Background(), cpuTarget, cpuDeleted, nil); got != framework.Queue {
		t.Fatalf("CPU capacity deletion hint = %v, want Queue", got)
	}
}

func TestPodCapacityHint_TargetRequestsShrink_Queues(t *testing.T) {
	g := &GangPack{}
	oldPod := gangGPUPod("team", "gang", "8")
	oldPod.Name, oldPod.UID = "target", types.UID("target")
	newPod := oldPod.DeepCopy()
	newPod.Spec.Containers[0].Resources.Requests[gpu] = resource.MustParse("4")
	if got, _ := g.isSchedulableAfterPodCapacityChange(klog.Background(), newPod, oldPod, newPod); got != framework.Queue {
		t.Fatalf("target scale-down hint = %v, want Queue", got)
	}
}

func TestPodCapacityHint_SiblingsOtherResourceFreed_Queues(t *testing.T) {
	otherResource := v1.ResourceName("example.com/other-accelerator")
	target := gangGPUPod("team", "gang", "1")
	target.Name = "target"
	sibling := gangPod("team", "gang")
	sibling.Name = "sibling"
	sibling.Spec.Containers = []v1.Container{{Resources: v1.ResourceRequirements{Requests: v1.ResourceList{
		otherResource: resource.MustParse("1"),
	}}}}
	deleted := sibling.DeepCopy()
	deleted.Name, deleted.Spec.NodeName = "occupant", "node-a"
	g := &GangPack{podLister: fakeGangPodLister{pods: []*v1.Pod{target, sibling}}}
	if got, _ := g.isSchedulableAfterPodCapacityChange(klog.Background(), target, deleted, nil); got != framework.Queue {
		t.Fatalf("heterogeneous sibling capacity hint = %v, want Queue", got)
	}
}

func TestPodTolerationHint_TargetOrSiblingChanges_Queues(t *testing.T) {
	target := gangPod("team", "gang")
	target.Name, target.UID = "target", types.UID("target")
	g := &GangPack{}
	if got, _ := g.isSchedulableAfterPodTolerationChange(klog.Background(), target, nil, target.DeepCopy()); got != framework.Queue {
		t.Fatalf("target toleration hint = %v, want Queue", got)
	}
	other := gangPod("team", "gang")
	other.Name, other.UID = "other", types.UID("other")
	if got, _ := g.isSchedulableAfterPodTolerationChange(klog.Background(), target, nil, other); got != framework.Queue {
		t.Fatalf("sibling toleration hint = %v, want Queue", got)
	}
}

func TestPodLabelHint_TargetOrNewSiblingLabelled_QueuesUnrelatedSkips(t *testing.T) {
	g := &GangPack{}
	target := gangGPUPod("team", "gang", "4")
	target.UID = types.UID("target")
	oldTarget := target.DeepCopy()
	delete(oldTarget.Labels, podGroupLabel)
	if got, _ := g.isSchedulableAfterPodLabelChange(klog.Background(), target, oldTarget, target); got != framework.Queue {
		t.Fatalf("target label change hint = %v, want Queue", got)
	}
	oldSibling := gangGPUPod("team", "other", "4")
	newSibling := gangGPUPod("team", "gang", "4")
	if got, _ := g.isSchedulableAfterPodLabelChange(klog.Background(), target, oldSibling, newSibling); got != framework.Queue {
		t.Fatalf("new sibling label hint = %v, want Queue", got)
	}
	if got, _ := g.isSchedulableAfterPodLabelChange(klog.Background(), target, oldSibling, gangGPUPod("team", "unrelated", "4")); got != framework.QueueSkip {
		t.Fatalf("unrelated label hint = %v, want QueueSkip", got)
	}
}

func TestNodeHint_NodeAdded_QueuesOnlyWithDomainLabel(t *testing.T) {
	g := &GangPack{pgReader: fakeReader{"team/gang": {min: 2, topo: testKey}}}
	target := gangGPUPod("team", "gang", "4")
	if got, _ := g.isSchedulableAfterNodeChange(klog.Background(), target, nil, gpuNode("new", "a", "4")); got != framework.Queue {
		t.Fatalf("useful node add hint = %v, want Queue", got)
	}
	if got, _ := g.isSchedulableAfterNodeChange(klog.Background(), target, nil, gpuNode("wrong", "", "4")); got != framework.QueueSkip {
		t.Fatalf("unlabelled node add hint = %v, want QueueSkip", got)
	}
	if got, _ := g.isSchedulableAfterNodeChange(klog.Background(), target, nil, gpuNode("cpu", "a", "")); got != framework.Queue {
		t.Fatalf("topology node add hint = %v, want conservative Queue", got)
	}
}

func TestNodeDeleteHint_NodeInAnyDomainDeleted_Queues(t *testing.T) {
	assignments := placement.New()
	assignments.Set(placement.Gang{Group: "team/gang", TopologyKey: testKey}, "a", 0, nil)
	g := &GangPack{assignments: assignments, pgReader: fakeReader{"team/gang": {min: 2, topo: testKey}}}
	target := gangGPUPod("team", "gang", "4")
	if got, _ := g.isSchedulableAfterNodeDelete(klog.Background(), target, gpuNode("a1", "a", "4"), nil); got != framework.Queue {
		t.Fatalf("assigned-domain delete hint = %v, want Queue", got)
	}
	if got, _ := g.isSchedulableAfterNodeDelete(klog.Background(), target, gpuNode("b1", "b", "4"), nil); got != framework.Queue {
		t.Fatalf("other-domain delete hint = %v, want conservative Queue", got)
	}
}

func TestNodeHint_AssignedDomainLabelRemoved_Queues(t *testing.T) {
	assignments := placement.New()
	assignments.Set(placement.Gang{Group: "team/gang", TopologyKey: testKey}, "a", 1, []string{"a1"})
	g := &GangPack{assignments: assignments, pgReader: fakeReader{"team/gang": {min: 2, topo: testKey}}}
	target := gangGPUPod("team", "gang", "4")
	oldNode := gpuNode("a1", "a", "4")
	newNode := oldNode.DeepCopy()
	delete(newNode.Labels, testKey)
	if got, _ := g.isSchedulableAfterNodeChange(klog.Background(), target, oldNode, newNode); got != framework.Queue {
		t.Fatalf("assigned-domain label removal hint = %v, want Queue", got)
	}
}

func TestPodGroupHint_OwnGroupChangedOrDeleted_QueuesOthersSkip(t *testing.T) {
	g := &GangPack{}
	target := gangPod("team", "gang")
	object := func(namespace, name string) *unstructured.Unstructured {
		u := &unstructured.Unstructured{}
		u.SetNamespace(namespace)
		u.SetName(name)
		return u
	}
	if got, err := g.isSchedulableAfterPodGroupChange(klog.Background(), target, nil, object("team", "gang")); err != nil || got != framework.Queue {
		t.Fatalf("matching PodGroup hint = %v, %v, want Queue", got, err)
	}
	if got, _ := g.isSchedulableAfterPodGroupChange(klog.Background(), target, nil, object("team", "other")); got != framework.QueueSkip {
		t.Fatalf("unrelated PodGroup hint = %v, want QueueSkip", got)
	}
	if got, err := g.isSchedulableAfterPodGroupChange(klog.Background(), target, object("team", "gang"), nil); err != nil || got != framework.Queue {
		t.Fatalf("matching PodGroup delete hint = %v, %v, want Queue", got, err)
	}
}

func TestPodCapacityHint_UnscheduledSiblingShrinks_Queues(t *testing.T) {
	target := gangGPUPod("team", "gang", "4")
	target.Name = "target"
	oldSibling := gangGPUPod("team", "gang", "8")
	oldSibling.Name = "sibling"
	newSibling := gangGPUPod("team", "gang", "4")
	newSibling.Name = "sibling"
	if got, _ := (&GangPack{}).isSchedulableAfterPodCapacityChange(klog.Background(), target, oldSibling, newSibling); got != framework.Queue {
		t.Fatalf("unscheduled sibling scale-down hint = %v, want Queue", got)
	}
}

func TestEventsToRegister_UnrelatedPodStatusUpdate_IsNotRegisteredAndSkips(t *testing.T) {
	events, err := (&GangPack{}).EventsToRegister(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.Event.Resource == framework.Pod && event.Event.ActionType == framework.Update {
			t.Fatal("generic Pod Update registration would wake gangs on unrelated status churn")
		}
	}
	target := gangGPUPod("team", "gang", "4")
	unrelated := gangGPUPod("other", "other", "4")
	unrelated.Spec.NodeName = "assigned"
	updated := unrelated.DeepCopy()
	updated.Status.Phase = v1.PodRunning
	got, err := (&GangPack{}).isSchedulableAfterPodLabelChange(klog.Background(), target, unrelated, updated)
	if err != nil || got != framework.QueueSkip {
		t.Fatalf("unrelated status-only update hint = %v, %v, want QueueSkip", got, err)
	}
}
