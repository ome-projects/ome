package podgroup

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// settledDraw is one random gang: the inputs DesiredPodGroup and
// GroupSettled share, drawn so that the group at the name is the plan's
// group in most draws and off it in one or two fields in the rest.
type settledDraw struct {
	owner     client.Object
	ownerGVK  schema.GroupVersionKind
	ownerName string
	plan      workload.ComponentPlan
	grace     time.Duration
	inst      workload.InstancePlan
	pods      []*corev1.Pod
	existing  *schedulingv1alpha1.PodGroup
	found     bool
}

var settledGVKs = []schema.GroupVersionKind{
	{Group: "ome.io", Version: "v1beta1", Kind: "InferenceService"},
	{Group: "", Version: "v1", Kind: "ConfigMap"},
	{Group: "apps", Version: "v1", Kind: "Deployment"},
}

func pick[T any](r *rand.Rand, items ...T) T { return items[r.IntN(len(items))] }

// drawSettledCase draws one gang. The group at the name starts as the
// group the plan asks for, carrying one of the keys a pass may have left
// on it, and then takes zero, one or two of the changes an operator, a
// scheduler or an older release could have made to it.
func drawSettledCase(r *rand.Rand) settledDraw {
	ownerName := pick(r, "svc", "fleet")
	owner := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name: pick(r, ownerName, ownerName+"-engine"), Namespace: "ns", UID: types.UID(pick(r, "owner-uid", "owner-uid", "owner-uid", "owner-uid", "owner-uid", "owner-uid", "owner-uid", "owner-uid", "owner-uid", "")),
	}}
	gvk := pick(r, settledGVKs...)
	component := pick(r, workload.ComponentEngine, workload.ComponentDecoder, workload.ComponentType(""))
	index := pick(r, int32(0), 1, 7, 99, 100, 256, 1725, 3999, -1)
	workers := pick(r, int32(0), 1, 1, 1, 7, 7, 7, 7)
	inst := workload.InstancePlan{Index: index, Incarnation: 1, Runners: []workload.RunnerPlan{
		{Name: workload.RunnerLeader, Size: 1}, {Name: workload.RunnerWorker, Size: workers}}}
	plan := workload.ComponentPlan{Component: component, TopologyKey: pick(r, "", "rack", "rack", "zone"), Instances: []workload.InstancePlan{inst}}
	if r.IntN(4) == 0 {
		plan.InstanceTopologyKeys = map[int32]string{index: pick(r, "", "host")}
	}
	if r.IntN(2) == 0 {
		plan.GangScheduleTimeout = &workload.GangScheduleTimeoutClamp{Min: time.Minute, Max: 10 * time.Minute}
	}
	grace := pick(r, time.Duration(0), 30*time.Second, 5*time.Minute, time.Hour)
	podsKey := pick(r, "", "rack", "rack", "zone")
	pods := drawGangPods(r, ownerName, component, index, podsKey)
	draw := settledDraw{owner: owner, ownerGVK: gvk, ownerName: ownerName, plan: plan, grace: grace, inst: inst, pods: pods}
	if r.IntN(8) == 0 {
		return draw
	}
	draw.found = true
	group, err := BuildPodGroup(owner, gvk, ownerName, plan, grace, inst)
	if err != nil {
		group = &schedulingv1alpha1.PodGroup{ObjectMeta: metav1.ObjectMeta{Name: "single", Namespace: "ns",
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(owner, gvk)}}}
	}
	setTopologyKeyAnnotation(group, pick(r, podsKey, plan.TopologyKeyForInstance(index), "", "zone"), r.IntN(5) != 0)
	for i := r.IntN(3); i > 0; i-- {
		changeGroup(r, group)
	}
	draw.existing = group
	return draw
}

// drawGangPods draws the pods under the gang's name: a leader, workers on
// key with the generated term, and now and then a worker on another key,
// one whose term names another leader, one with a hand-written term, or
// a pod with none.
func drawGangPods(r *rand.Rand, ownerName string, component workload.ComponentType, index int32, key string) []*corev1.Pod {
	var pods []*corev1.Pod
	for n := r.IntN(5); n > 0; n-- {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
			constants.InferenceServicePodLabelKey: ownerName,
			constants.OMEComponentLabel:           string(component),
			query.LabelInstanceIdx:                fmt.Sprintf("%d", index),
			query.LabelRunner:                     pick(r, workload.RunnerWorker, workload.RunnerWorker, workload.RunnerWorker, workload.RunnerLeader),
		}}}
		if pod.Labels[query.LabelRunner] == workload.RunnerWorker && r.IntN(6) != 0 {
			selected := map[string]string{
				constants.InferenceServicePodLabelKey: ownerName,
				constants.OMEComponentLabel:           string(component),
				query.LabelInstanceIdx:                fmt.Sprintf("%d", index),
				query.LabelRunner:                     workload.RunnerLeader,
			}
			switch r.IntN(12) {
			case 0:
				selected[query.LabelInstanceIdx] = fmt.Sprintf("0%d", index)
			case 1:
				selected[query.LabelInstanceIdx] = fmt.Sprintf("%d", index+1)
			case 2:
				delete(selected, query.LabelInstanceIdx)
			case 3:
				selected[query.LabelRunner] = workload.RunnerWorker
			case 4:
				delete(selected, constants.OMEComponentLabel)
			}
			termKey := key
			if r.IntN(10) == 0 {
				termKey = pick(r, "", "zone", "host")
			}
			term := corev1.PodAffinityTerm{TopologyKey: termKey, LabelSelector: &metav1.LabelSelector{MatchLabels: selected}}
			if r.IntN(10) == 0 {
				term.LabelSelector = nil
			}
			terms := []corev1.PodAffinityTerm{term}
			if r.IntN(8) == 0 {
				terms = append(terms, corev1.PodAffinityTerm{TopologyKey: pick(r, key, "zone"), LabelSelector: &metav1.LabelSelector{MatchLabels: selected}})
			}
			pod.Spec.Affinity = &corev1.Affinity{PodAffinity: &corev1.PodAffinity{RequiredDuringSchedulingIgnoredDuringExecution: terms}}
		}
		pods = append(pods, pod)
	}
	return pods
}

// changeGroup makes one change to a group: a field the pass owns, the
// occupant's identity, or something the pass leaves alone.
func changeGroup(r *rand.Rand, group *schedulingv1alpha1.PodGroup) {
	if group.Labels == nil {
		group.Labels = map[string]string{}
	}
	if len(group.OwnerReferences) == 0 {
		group.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "ConfigMap", Name: "x", UID: "x-uid"}}
	}
	switch r.IntN(22) {
	case 0:
		group.Labels[query.LabelManagedBy] = "someone-else"
	case 1:
		delete(group.Labels, pick(r, query.LabelManagedBy, constants.OMEComponentLabel, query.LabelInstanceIdx))
	case 2:
		group.Labels["example.com/extra"] = "x"
	case 3:
		group.Labels[query.LabelInstanceIdx] = "0" + group.Labels[query.LabelInstanceIdx]
	case 4:
		group.OwnerReferences[0].Name = "renamed"
	case 5:
		group.OwnerReferences[0].Kind = "Other"
	case 6:
		group.OwnerReferences[0].APIVersion = pick(r, "ome.io/v1", "v1", "apps/v1", "ome.io/v1beta1/x")
	case 7:
		group.OwnerReferences[0].UID = pick(r, types.UID("someone-else-uid"), "")
	case 8:
		group.OwnerReferences[0].Controller = pick(r, nil, boolPointer(false))
	case 9:
		group.OwnerReferences[0].BlockOwnerDeletion = pick(r, nil, boolPointer(false))
	case 10:
		group.OwnerReferences = append(group.OwnerReferences, metav1.OwnerReference{APIVersion: "v1", Kind: "ConfigMap", Name: "x", UID: "x-uid"})
	case 11:
		group.OwnerReferences = nil
	case 12:
		group.Spec.MinMember += pick(r, int32(1), -1)
	case 13:
		group.Spec.ScheduleTimeoutSeconds = nil
	case 14:
		group.Spec.ScheduleTimeoutSeconds = int32Pointer(pick(r, int32(30), 60, 299, 300, 600, 3600))
	case 15:
		delete(group.Annotations, query.AnnotationTopologyKey)
	case 16:
		setTopologyKeyAnnotation(group, pick(r, "", "zone", "host"), true)
	case 17:
		if group.Annotations == nil {
			group.Annotations = map[string]string{}
		}
		group.Annotations["example.com/note"] = "kept"
	case 18:
		now := metav1.Now()
		group.DeletionTimestamp = &now
	case 19:
		group.Status.Phase = pick(r, schedulingv1alpha1.PodGroupRunning, schedulingv1alpha1.PodGroupFailed)
	case 20:
		group.Labels = nil
	case 21:
		group.Annotations = nil
	}
}

func boolPointer(v bool) *bool { return &v }

func int32Pointer(v int32) *int32 { return &v }

// buildPathDecision is the decision the build path makes on a draw: the
// built group with no write needed, or not.
func buildPathDecision(d settledDraw) (settled bool, desired *schedulingv1alpha1.PodGroup, err error) {
	desired, err = DesiredPodGroup(d.owner, d.ownerGVK, d.ownerName, d.plan, d.grace, d.inst, d.pods, d.existing, d.found)
	if err != nil {
		return false, nil, err
	}
	return !WriteNeeded(desired, d.existing, d.found), desired, nil
}

// TestGroupSettled_AgreesWithTheBuildPathOnEveryDraw draws 300,000 gangs,
// most with the group at the name the plan's and the rest off it in one
// or two fields, and holds GroupSettled to the build path: it is true
// exactly when DesiredPodGroup builds without error and WriteNeeded finds
// nothing to send, and then the two read the same topology key.
func TestGroupSettled_AgreesWithTheBuildPathOnEveryDraw(t *testing.T) {
	r := rand.New(rand.NewPCG(23, 2026))
	counts := map[string]int{}
	for i := 0; i < 300_000; i++ {
		d := drawSettledCase(r)
		want, desired, err := buildPathDecision(d)
		got := GroupSettled(d.owner, d.ownerGVK, d.ownerName, d.plan, d.grace, d.inst, d.pods, d.existing, d.found)
		if got != want {
			t.Fatalf("draw %d: GroupSettled %t, the build path %t (err %v)\nowner %s/%s gvk %v ownerName %q plan %+v grace %v inst %+v\nexisting %+v found %t\npods %s",
				i, got, want, err, d.owner.GetNamespace(), d.owner.GetName(), d.ownerGVK, d.ownerName, d.plan, d.grace, d.inst, d.existing, d.found, describePods(d.pods))
		}
		switch {
		case got:
			counts["settled"]++
			if TopologyKeyOf(desired) != TopologyKeyOf(d.existing) {
				t.Fatalf("draw %d: settled, but the built group records %q and the existing %q", i, TopologyKeyOf(desired), TopologyKeyOf(d.existing))
			}
		case err != nil:
			counts["refused: "+errorClass(err)]++
		case !d.found:
			counts["free name"]++
		default:
			counts["written"]++
		}
	}
	t.Logf("outcomes: %v", counts)
	for _, outcome := range []string{"settled", "written", "free name", "refused: another owner", "refused: terminating", "refused: topology", "refused: single-pod"} {
		if counts[outcome] < 1000 {
			t.Errorf("outcome %q drawn %d times; want at least 1,000", outcome, counts[outcome])
		}
	}
}

// errorClass names the reason the build path refused a draw.
func errorClass(err error) string {
	msg := err.Error()
	switch {
	case strings.Contains(msg, ErrPodGroupOwnershipConflict.Error()):
		return "another owner"
	case strings.Contains(msg, ErrPodGroupTerminating.Error()):
		return "terminating"
	case strings.Contains(msg, "topology"):
		return "topology"
	case strings.Contains(msg, "single-pod"):
		return "single-pod"
	}
	return msg
}

func describePods(pods []*corev1.Pod) string {
	s := ""
	for _, pod := range pods {
		s += fmt.Sprintf("{runner %s", pod.Labels[query.LabelRunner])
		for _, term := range workerAffinityTerms(pod) {
			s += fmt.Sprintf(" term{key %q", term.TopologyKey)
			if term.LabelSelector != nil {
				s += fmt.Sprintf(" selects %v", term.LabelSelector.MatchLabels)
			}
			s += "}"
		}
		s += "} "
	}
	return s
}

// TestGroupSettled_AllocatesNothing: reading a gang of eight at rest
// against the plan costs no allocation, whatever its index spells.
func TestGroupSettled_AllocatesNothing(t *testing.T) {
	owner := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "ns", UID: "owner-uid"}}
	for _, index := range []int32{0, 7, 100, 1725} {
		inst := workload.InstancePlan{Index: index, Incarnation: 1, Runners: []workload.RunnerPlan{
			{Name: workload.RunnerLeader, Size: 1}, {Name: workload.RunnerWorker, Size: 7}}}
		plan := workload.ComponentPlan{Component: workload.ComponentEngine, TopologyKey: "rack", Instances: []workload.InstancePlan{inst},
			GangScheduleTimeout: &workload.GangScheduleTimeoutClamp{Min: time.Minute, Max: 10 * time.Minute}}
		group, err := BuildPodGroup(owner, settledGVKs[0], "svc", plan, 5*time.Minute, inst)
		if err != nil {
			t.Fatalf("BuildPodGroup: %v", err)
		}
		pods := gangPodsOnKey("svc", workload.ComponentEngine, index, "rack", 7)
		if !GroupSettled(owner, settledGVKs[0], "svc", plan, 5*time.Minute, inst, pods, group, true) {
			t.Fatalf("index %d: a gang at rest reads as unsettled", index)
		}
		allocs := testing.AllocsPerRun(100, func() {
			GroupSettled(owner, settledGVKs[0], "svc", plan, 5*time.Minute, inst, pods, group, true)
		})
		if allocs != 0 {
			t.Errorf("index %d: GroupSettled allocated %.0f times per call; want none", index, allocs)
		}
	}
}

// gangPodsOnKey is a gang at rest: a leader and workers workers, each
// worker carrying the generated term that follows the leader on key.
func gangPodsOnKey(ownerName string, component workload.ComponentType, index int32, key string, workers int) []*corev1.Pod {
	labels := func(runner string) map[string]string {
		return map[string]string{
			constants.InferenceServicePodLabelKey: ownerName,
			constants.OMEComponentLabel:           string(component),
			query.LabelInstanceIdx:                fmt.Sprintf("%d", index),
			query.LabelRunner:                     runner,
		}
	}
	pods := []*corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Labels: labels(workload.RunnerLeader)}}}
	for i := 0; i < workers; i++ {
		pods = append(pods, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Labels: labels(workload.RunnerWorker)},
			Spec: corev1.PodSpec{Affinity: &corev1.Affinity{PodAffinity: &corev1.PodAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
					TopologyKey: key, LabelSelector: &metav1.LabelSelector{MatchLabels: labels(workload.RunnerLeader)},
				}},
			}}},
		})
	}
	return pods
}
