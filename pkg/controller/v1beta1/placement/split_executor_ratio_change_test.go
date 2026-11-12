package placement

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

// TestSplitRatioChange_FallingMemberCannotStartDecoders_MoveFinishes runs the
// autoscaled Split service with engine weights 6/6 and decoder weights 6/2, so
// member-b runs 6 engines with 2 decoders. The change raises the minimums to 15
// engines and 10 decoders, with engine weights 12/3 and decoder weights 8/2.
// Each decoder weight is now 2/3 of its engine weight, so the decoders follow
// the engine shares. member-a's engine target rises from 6 to 12, member-b's
// falls from 6 to 3, and member-b's decoder share stays 2. While the move runs,
// OME asks member-b for 4 decoders, but member-b cannot start more than its 2,
// for example because its accelerator quota is full. member-a's 6 new engines
// stay not ready for the first 6 passes, and member-b must keep its 6 engines
// until member-a serves all 12. The engines member-b has no decoders for count
// for nothing, and the move must still finish.
func TestSplitRatioChange_FallingMemberCannotStartDecoders_MoveFinishes(t *testing.T) {
	const newEnginesReadyAt = 6
	f := autoscaledSplitFixture(t)
	runMinimums := func(minimums map[v1beta1.ComponentType]int32) map[v1beta1.ComponentType]int32 {
		running := maps.Clone(minimums)
		running[v1beta1.RouterComponent] = 2
		return running
	}
	done := func(source *v1beta1.InferenceService) bool {
		return !source.Status.Placement.Plan.PauseSurge && autoscaledAllConverged(source)
	}
	changeAutoscaledSplit(t, f, func(s *v1beta1.InferenceService) {
		s.Spec.Placement.ClusterAffinity = perMemberTerms(map[string]int32{"member-a": 6, "member-b": 6}, map[string]int32{"member-a": 6, "member-b": 2}, nil)
	})
	var got *v1beta1.InferenceService
	for range 24 {
		for _, name := range slices.Sorted(maps.Keys(f.workers)) {
			if member, ok := allMemberOn(t, f, name); ok {
				runAutoscaledSplitMember(t, f, name, runMinimums(memberMinimums(member)), nil)
			}
		}
		routeAutoscaledAll(t, f)
		if got = f.reconcile(t); done(got) {
			break
		}
	}
	if !done(got) {
		t.Fatalf("the service did not settle on decoder weights 6/2: %s", describeWeightedSplit(t, f, got))
	}
	before, _ := allMemberOn(t, f, "member-b")
	decodersB := memberMinimums(before)[v1beta1.DecoderComponent]
	if decodersB != 2 {
		t.Fatalf("member-b runs %d decoders, want 2: %s", decodersB, describeWeightedSplit(t, f, got))
	}
	changeAutoscaledSplit(t, f, func(s *v1beta1.InferenceService) {
		s.Spec.Engine.MinReplicas, s.Spec.Decoder.MinReplicas = ptr.To(15), ptr.To(10)
		s.Spec.Placement.ClusterAffinity = perMemberTerms(map[string]int32{"member-a": 12, "member-b": 3}, map[string]int32{"member-a": 8, "member-b": 2}, nil)
	})
	for pass := range 40 {
		var readyA int32
		for _, name := range slices.Sorted(maps.Keys(f.workers)) {
			member, ok := allMemberOn(t, f, name)
			if !ok {
				continue
			}
			running := runMinimums(memberMinimums(member))
			if name == "member-b" {
				running[v1beta1.DecoderComponent] = min(running[v1beta1.DecoderComponent], decodersB)
			}
			ready := maps.Clone(running)
			if name == "member-a" {
				if pass < newEnginesReadyAt {
					ready[v1beta1.EngineComponent] = min(ready[v1beta1.EngineComponent], 6)
				}
				readyA = ready[v1beta1.EngineComponent]
			}
			runSplitMemberWithAnUpdate(t, f, name, running, ready, 0)
		}
		routeAutoscaledAll(t, f)
		got = f.reconcile(t)
		if engines := candidateOf(t, got, "member-b").Allocation.CurrentReplicas; readyA < 12 && engines < 6 {
			t.Fatalf("pass %d: member-b fell to %d engines while member-a served %d of its 12: %s", pass, engines, readyA, describeWeightedSplit(t, f, got))
		}
		if done(got) {
			break
		}
	}
	if !done(got) {
		t.Fatalf("the move did not finish: %s", describeWeightedSplit(t, f, got))
	}
	for _, candidate := range got.Status.Placement.Candidates {
		want := map[string]int32{"member-a": 12, "member-b": 3}[candidate.Cluster]
		if a := candidate.Allocation; a.CurrentReplicas != want || a.DesiredReplicas != want {
			t.Fatalf("%s did not end at its engine target of %d: %s", candidate.Cluster, want, describeWeightedSplit(t, f, got))
		}
	}
}

// TestSplitRatioChange_EngineMidUpdate_ShrinkingMemberWaitsForReplacements runs
// the autoscaled Split service with 6 engines on each member. One of member-b's
// engines has an update in progress: its old copy serves, but OME does not
// count an engine with an update in progress as ready. member-b keeps that
// engine whatever its engine count, so a lower count removes a ready engine.
// The change raises the engine minimum to 13 with engine weights 10/3, so
// member-a's target rises to 10 and member-b's falls to 3. member-a's 4 new
// engines stay not ready for the first 8 passes. member-b must keep its 6
// engines, and the move must not finish, until member-a serves all 10. Then
// member-b drops to 3 while the update still runs, and the move finishes once
// the update ends.
func TestSplitRatioChange_EngineMidUpdate_ShrinkingMemberWaitsForReplacements(t *testing.T) {
	const newEnginesReadyAt, updateEndsAt = 8, 16
	f := autoscaledSplitFixture(t)
	changeAutoscaledSplit(t, f, func(s *v1beta1.InferenceService) {
		s.Spec.Engine.MinReplicas = ptr.To(13)
		s.Spec.Placement.ClusterAffinity = perMemberTerms(map[string]int32{"member-a": 10, "member-b": 3}, nil, nil)
	})
	engines := func(source *v1beta1.InferenceService, name string) int32 {
		for _, candidate := range source.Status.Placement.Candidates {
			if candidate.Cluster == name {
				return candidate.Allocation.CurrentReplicas
			}
		}
		return 0
	}
	done := func(source *v1beta1.InferenceService) bool {
		return !source.Status.Placement.Plan.PauseSurge && autoscaledAllConverged(source)
	}
	var got *v1beta1.InferenceService
	for pass := range 40 {
		for _, name := range slices.Sorted(maps.Keys(f.workers)) {
			member, ok := allMemberOn(t, f, name)
			if !ok {
				continue
			}
			running := memberMinimums(member)
			running[v1beta1.RouterComponent] = 2
			ready := maps.Clone(running)
			var updating int32
			switch {
			case name == "member-a" && pass < newEnginesReadyAt:
				ready[v1beta1.EngineComponent] = min(ready[v1beta1.EngineComponent], 6)
			case name == "member-b" && pass < updateEndsAt:
				updating = 1
			}
			runSplitMemberWithAnUpdate(t, f, name, running, ready, updating)
		}
		routeAutoscaledAll(t, f)
		got = f.reconcile(t)
		switch {
		case pass < newEnginesReadyAt && (engines(got, "member-b") < 6 || autoscaledAllConverged(got)):
			t.Fatalf("pass %d: member-b gave up engines, or the move finished, before member-a's new engines were ready: %s", pass, describeWeightedSplit(t, f, got))
		case pass == updateEndsAt-1 && engines(got, "member-b") != 3:
			t.Fatalf("member-a serves all 10 engines, but member-b did not reach its target of 3 while its update runs: %s", describeWeightedSplit(t, f, got))
		case pass >= updateEndsAt && done(got):
			if a, b := engines(got, "member-a"), engines(got, "member-b"); a != 10 || b != 3 {
				t.Fatalf("the move finished with member-a at %d and member-b at %d engines, want 10 and 3: %s", a, b, describeWeightedSplit(t, f, got))
			}
			return
		}
	}
	t.Fatalf("the move did not finish after the update ended: %s", describeWeightedSplit(t, f, got))
}

// TestSplitRatioChange_MemberWithRoutersDown_ShrinkingMemberKeepsEngines runs
// the autoscaled Split service with 6 engines on each of three members. The
// change raises the engine minimum from 18 to 19 with engine weights 10/3/6.
// member-a grows from 6 to 10, member-b falls from 6 to 3, and member-c keeps
// 6. One of member-b's engines has an update in progress. Its old copy serves,
// but OME does not count it as ready, and member-b keeps it at any engine
// count. member-c's routers are down for the first 10 passes, so member-c
// serves nothing. member-a's 4 new engines are ready at once, but member-b must
// keep its 6 engines while member-c serves nothing. The move finishes once
// member-c's routers are back and member-b's update ends.
func TestSplitRatioChange_MemberWithRoutersDown_ShrinkingMemberKeepsEngines(t *testing.T) {
	const routersBackAt, updateEndsAt = 10, 14
	f := autoscaledSplitFixture(t)
	registerAutoscaledAllMember(t, f, "member-c")
	changeAutoscaledSplit(t, f, func(s *v1beta1.InferenceService) {
		s.Spec.Engine.MinReplicas, s.Spec.Decoder.MinReplicas = ptr.To(18), ptr.To(12)
		s.Spec.Placement.ClusterAffinity = testAffinity("")
		twoRoutersEach(s, 3)
	})
	if got := serveAutoscaledSplit(t, f, autoscaledAllRunning, autoscaledAllRunning, autoscaledAllConverged); !autoscaledAllConverged(got) {
		t.Fatalf("three members did not settle: %s", describeWeightedSplit(t, f, got))
	}
	changeAutoscaledSplit(t, f, func(s *v1beta1.InferenceService) {
		s.Spec.Engine.MinReplicas = ptr.To(19)
		s.Spec.Placement.ClusterAffinity = perMemberTerms(map[string]int32{"member-a": 10, "member-b": 3, "member-c": 6}, nil, nil)
	})
	done := func(source *v1beta1.InferenceService) bool {
		return !source.Status.Placement.Plan.PauseSurge && autoscaledAllConverged(source)
	}
	var got *v1beta1.InferenceService
	for pass := range 40 {
		for _, name := range slices.Sorted(maps.Keys(f.workers)) {
			member, ok := allMemberOn(t, f, name)
			if !ok {
				continue
			}
			running := memberMinimums(member)
			running[v1beta1.RouterComponent] = 2
			ready := maps.Clone(running)
			var updating int32
			switch {
			case name == "member-b" && pass < updateEndsAt:
				updating = 1
			case name == "member-c" && pass < routersBackAt:
				ready[v1beta1.RouterComponent] = 0
			}
			runSplitMemberWithAnUpdate(t, f, name, running, ready, updating)
		}
		routeAutoscaledAll(t, f)
		got = f.reconcile(t)
		if engines := candidateOf(t, got, "member-b").Allocation.CurrentReplicas; pass < routersBackAt && engines < 6 {
			t.Fatalf("pass %d: member-b fell to %d engines while member-c served nothing: %s", pass, engines, describeWeightedSplit(t, f, got))
		}
		if pass >= updateEndsAt && done(got) {
			for _, candidate := range got.Status.Placement.Candidates {
				want := map[string]int32{"member-a": 10, "member-b": 3, "member-c": 6}[candidate.Cluster]
				if a := candidate.Allocation; a.CurrentReplicas != want || a.DesiredReplicas != want {
					t.Fatalf("%s did not end at its engine target of %d: %s", candidate.Cluster, want, describeWeightedSplit(t, f, got))
				}
			}
			return
		}
	}
	t.Fatalf("the move did not finish after member-c's routers came back and member-b's update ended: %s", describeWeightedSplit(t, f, got))
}

// runSplitMemberWithAnUpdate is runAutoscaledSplitMember for a member whose
// components run running[c] instances, of which the first ready[c] are ready.
// The first updating of its ready engines have an update in progress.
func runSplitMemberWithAnUpdate(t *testing.T, f *backendFixture, name string, running, ready map[v1beta1.ComponentType]int32, updating int32) {
	t.Helper()
	worker := f.workers[name]
	member, _ := allMemberOn(t, f, name)
	policy, err := protocol.FromDerived(member)
	if err != nil || policy == nil {
		t.Fatalf("%s has no member execution policy: %v", name, err)
	}
	minimums := memberMinimums(member)
	for _, component := range declaredComponents(f.source) {
		var inUpdate int32
		if component == v1beta1.EngineComponent {
			inUpdate = updating
		}
		ir := sizeComponentWithAnUpdate(t, worker, member, component, running[component], ready[component], inUpdate)
		ir.Generation++
		ir.Spec.Replicas, ir.Spec.PlacementExecution, ir.Spec.PlacementReplicaLimit = ptr.To(minimums[component]), policy.DeepCopy(), nil
		if policy.PauseSurge {
			ir.Spec.PlacementReplicaLimit = ptr.To(minimums[component])
		}
		if ir.Annotations == nil {
			ir.Annotations = map[string]string{}
		}
		ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] = strconv.FormatInt(member.Generation, 10)
		ir.Status.ObservedGeneration, ir.Status.PlacementObservedGeneration = ir.Generation, ir.Generation
		if err := worker.Update(t.Context(), ir); err != nil {
			t.Fatal(err)
		}
	}
	member.Status.URL = &apis.URL{Scheme: "https", Host: name + ".example.com"}
	member.Status.SetCondition(v1beta1.IngressReady, &apis.Condition{Status: corev1.ConditionTrue})
	if err := worker.Status().Update(t.Context(), member); err != nil {
		t.Fatal(err)
	}
}

// sizeComponentWithAnUpdate is sizeAutoscaledAllComponent with only the first
// ready instances ready, and an update in progress on the first updating of
// them. An instance in an update keeps serving from its old copy while the new
// copy waits to start.
func sizeComponentWithAnUpdate(t *testing.T, worker client.Client, member *v1beta1.InferenceService, component v1beta1.ComponentType, count, ready, updating int32) *v1beta1.InferenceReplica {
	t.Helper()
	key := client.ObjectKey{Namespace: member.Namespace, Name: member.Name + "-" + string(component)}
	ir := &v1beta1.InferenceReplica{}
	err := worker.Get(t.Context(), key, ir)
	fresh := apierrors.IsNotFound(err)
	if err != nil && !fresh {
		t.Fatal(err)
	}
	var inUpdate int32
	for _, row := range ir.Status.InstanceStatuses {
		if row.Operation != nil {
			inUpdate++
		}
	}
	if !fresh && int32(len(ir.Status.InstanceStatuses)) == count && ir.Status.ReadyReplicas == ready && inUpdate == updating {
		return ir
	}
	sized := irWithInstances(component, slices.Repeat([]bool{true}, int(count))...)
	sized.Name, sized.Namespace, sized.Status.ReadyReplicas = key.Name, key.Namespace, ready
	objects := observedWorkerObjects(member.DeepCopy(), sized)[2:]
	for i := range min(updating, ready) {
		row := &sized.Status.InstanceStatuses[i]
		row.Phase = v1beta1.OMENativeInstanceUpdating
		row.Operation = &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate, Step: workloadtypes.UpdateStepSurge, TargetRevision: key.Name + "-b"}
	}
	if !fresh {
		// Each instance's Pod is named after the component and the instance index.
		for _, row := range ir.Status.InstanceStatuses {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: fmt.Sprintf("%s-%d", key.Name, row.Index)}}
			if err := worker.Delete(t.Context(), pod); err != nil {
				t.Fatal(err)
			}
		}
		ir.Status.InstanceStatuses, ir.Status.ReadyReplicas = sized.Status.InstanceStatuses, ready
		objects = objects[:len(objects)-1]
	}
	for _, obj := range objects {
		if err := worker.Create(t.Context(), obj); err != nil {
			t.Fatal(err)
		}
	}
	if fresh {
		if err := worker.Get(t.Context(), key, ir); err != nil {
			t.Fatal(err)
		}
	}
	return ir
}
