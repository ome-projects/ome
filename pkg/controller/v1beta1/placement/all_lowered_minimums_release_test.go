package placement

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/placement/allocation"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

// TestAllCleanupWaitFollowsMovesOnly completes paused All changes while members
// run above their minimums. A change that keeps every member and adds no engine
// replicas ends the pause; a change that moves replicas keeps the cleanup wait.
func TestAllCleanupWaitFollowsMovesOnly(t *testing.T) {
	type edit func(*v1beta1.InferenceService, map[string]plannedMemberObservation)
	policy := func(engine, decoder int32) *v1beta1.PlacementHomePolicy {
		return &v1beta1.PlacementHomePolicy{InputDigest: "intent", ReplicaFloors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.DecoderComponent, Replicas: decoder}, {Component: v1beta1.EngineComponent, Replicas: engine}}}
	}
	chain := func(edits ...edit) edit {
		return func(s *v1beta1.InferenceService, o map[string]plannedMemberObservation) {
			for _, apply := range edits {
				apply(s, o)
			}
		}
	}
	// assigned edits the named cluster's allocation, and observed edits what
	// placement observes on it.
	assigned := func(name string, change func(*v1beta1.CandidateAllocationStatus)) edit {
		return func(s *v1beta1.InferenceService, _ map[string]plannedMemberObservation) {
			for _, candidate := range s.Status.Placement.Candidates {
				if candidate.Cluster == name {
					change(candidate.Allocation)
				}
			}
		}
	}
	observed := func(name string, change func(*plannedMemberObservation)) edit {
		return func(_ *v1beta1.InferenceService, o map[string]plannedMemberObservation) {
			observation := o[name]
			change(&observation)
			o[name] = observation
		}
	}
	// outside adds cluster c, which the plan does not match and which is gone.
	// original is the engine minimum c had when the change began.
	outside := func(original int32) edit {
		return func(s *v1beta1.InferenceService, o map[string]plannedMemberObservation) {
			s.Status.Placement.Candidates = append(s.Status.Placement.Candidates, v1beta1.CandidatePlacement{Cluster: "c", Allocation: &v1beta1.CandidateAllocationStatus{ClusterUID: "c-uid", OriginalReplicas: original}})
			o["c"] = plannedMemberObservation{MemberState: allocation.MemberState{Known: true, Absent: true}}
		}
	}
	atMinimum := func(o *plannedMemberObservation) { o.MemberState.Occupied = 3 }
	// fit runs each member at its minimum, so its replicas alone fit the plan.
	fit := chain(observed("a", atMinimum), observed("b", atMinimum))
	reserve := observed("a", func(o *plannedMemberObservation) { o.RolloutReserved = 1 })
	raiseDecoder := func(a *v1beta1.CandidateAllocationStatus) {
		a.CurrentHome, a.DesiredHome = policy(3, 6), policy(3, 6)
	}
	ends := allocation.Step{Targets: map[string]int32{"a": 3, "b": 3}, Complete: true}
	waiting := allocation.Step{Targets: map[string]int32{"a": 3, "b": 3}, Reason: "AwaitingMemberCleanup"}
	for _, tt := range []struct {
		name string
		edit edit
		want allocation.Step
	}{
		{name: "lowered minimums on members that all stay", want: ends},
		{name: "rollout surge in flight", want: ends, edit: reserve},
		// Only engine minimums are compared with where the change began.
		{name: "raised decoder minimum with a lowered engine minimum", want: ends, edit: chain(assigned("a", raiseDecoder), assigned("b", raiseDecoder))},
		{name: "cluster outside the plan", want: allocation.Step{Targets: map[string]int32{"a": 3, "b": 3, "c": 0}, Complete: true}, edit: outside(0)},
		{name: "engine minimum back where it began", want: allocation.Step{Targets: map[string]int32{"a": 6, "b": 3}, Complete: true}, edit: func(s *v1beta1.InferenceService, o map[string]plannedMemberObservation) {
			s.Status.Placement.Plan.AssignedReplicas = 9
			assigned("a", func(a *v1beta1.CandidateAllocationStatus) {
				a.CurrentHome, a.CurrentReplicas, a.DesiredHome, a.DesiredReplicas = policy(6, 2), 6, policy(6, 2), 6
			})(s, o)
		}},
		{name: "raised engine minimum", want: waiting, edit: assigned("a", func(a *v1beta1.CandidateAllocationStatus) { a.OriginalReplicas = 2 })},
		{name: "added member", want: waiting, edit: assigned("b", func(a *v1beta1.CandidateAllocationStatus) { a.OriginalReplicas = 0 })},
		{name: "released member", want: allocation.Step{Targets: map[string]int32{"a": 3, "b": 3, "c": 0}, Reason: "AwaitingMemberCleanup"}, edit: outside(3)},
		// During a real move a rollout reservation still counts against the plan.
		{name: "released member with a rollout reservation", want: allocation.Step{Targets: map[string]int32{"a": 3, "b": 3, "c": 0}, Reason: "AwaitingMemberCleanup"}, edit: chain(fit, outside(3), reserve)},
		{name: "released member without a rollout reservation", want: allocation.Step{Targets: map[string]int32{"a": 3, "b": 3, "c": 0}, Complete: true}, edit: chain(fit, outside(3))},
		{name: "member asked to drain", want: waiting, edit: assigned("b", func(a *v1beta1.CandidateAllocationStatus) { a.DrainRequested = true })},
		{name: "new minimums not yet in the plan", want: waiting, edit: assigned("a", func(a *v1beta1.CandidateAllocationStatus) { a.CurrentHome = policy(3, 4) })},
		{name: "Split plan", want: waiting, edit: func(s *v1beta1.InferenceService, _ map[string]plannedMemberObservation) {
			s.Spec.Placement.Mode, s.Status.Placement.Plan.Mode = v1beta1.PlacementModeSplit, v1beta1.PlacementModeSplit
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := srcISVCMode(v1beta1.PlacementModeAll, "")
			s.Status.Placement = &v1beta1.PlacementStatus{Plan: &v1beta1.PlacementPlanStatus{Mode: v1beta1.PlacementModeAll, InputDigest: "intent", AssignedReplicas: 6, PauseSurge: true}}
			o := map[string]plannedMemberObservation{}
			// Each member began the change at an engine minimum of 6, took 3, and
			// its autoscaler keeps more replicas than that.
			for _, m := range []struct {
				name     string
				occupied int32
			}{{"a", 6}, {"b", 5}} {
				s.Status.Placement.Candidates = append(s.Status.Placement.Candidates, v1beta1.CandidatePlacement{Cluster: m.name, Allocation: &v1beta1.CandidateAllocationStatus{
					ClusterUID: types.UID(m.name + "-uid"), Matched: true, OriginalReplicas: 6, CurrentHome: policy(3, 2), CurrentReplicas: 3, DesiredHome: policy(3, 2), DesiredReplicas: 3,
				}})
				o[m.name] = plannedMemberObservation{MemberState: allocation.MemberState{Known: true, Eligible: true, Applied: true, Routable: true, Ready: 3, Occupied: m.occupied}, PauseAcknowledged: true}
			}
			if tt.edit != nil {
				tt.edit(s, o)
			}
			got, err := advanceSplitPlan(s, o)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatalf("step (-want +got):\n%s", diff)
			}
		})
	}
}

// TestAllLoweredMinimumsEndAnActivePause raises the engine minimum, which pauses
// growth, then lowers both minimums below where they began while members run
// above them. Nothing moved, so the pause ends once each member applies them.
func TestAllLoweredMinimumsEndAnActivePause(t *testing.T) {
	f := autoscaledAllFixture(t)
	setAutoscaledAll(t, f, 8, 4, "")
	if raised := f.reconcile(t); !raised.Status.Placement.Plan.PauseSurge {
		t.Fatalf("the raised engine minimum did not pause growth: %s", describeAutoscaledAll(t, f, raised))
	}
	setAutoscaledAll(t, f, 3, 2, "")
	got := serveAutoscaledAll(t, f, autoscaledAllRunning, autoscaledAllBusy, autoscaledAllConverged)
	if got.Status.Placement.Plan.PauseSurge || !autoscaledAllConverged(got) {
		t.Fatalf("member growth stayed paused: %s", describeAutoscaledAll(t, f, got))
	}
	for _, candidate := range got.Status.Placement.Candidates {
		a := candidate.Allocation
		if a.OriginalReplicas != 3 || a.CurrentReplicas != 3 || a.DesiredReplicas != 3 || !equality.Semantic.DeepEqual(a.CurrentHome, a.DesiredHome) {
			t.Fatalf("%s did not keep its lowered minimums: %+v", candidate.Cluster, a)
		}
		member, ok := allMemberOn(t, f, candidate.Cluster)
		if !ok {
			t.Fatalf("%s lost its member", candidate.Cluster)
		}
		if policy, err := protocol.FromDerived(member); err != nil || policy == nil || policy.PauseSurge {
			t.Fatalf("%s still defers autoscaler growth: %+v (%v)", candidate.Cluster, policy, err)
		}
	}
}

// TestAllRaisedEngineMinimumKeepsTheCleanupWait raises only the engine minimum
// while autoscalers run more engines than it. The raise grows every member, so
// the pause holds until the engines fall to the new minimum.
func TestAllRaisedEngineMinimumKeepsTheCleanupWait(t *testing.T) {
	f := autoscaledAllFixture(t)
	above := map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 10, v1beta1.DecoderComponent: 4, v1beta1.RouterComponent: 2}
	if got := serveAutoscaledAll(t, f, above, above, autoscaledAllConverged); !autoscaledAllConverged(got) {
		t.Fatalf("the service did not stay converged while its autoscalers added engines: %s", describeAutoscaledAll(t, f, got))
	}
	setAutoscaledAll(t, f, 8, 4, "")
	got := serveAutoscaledAll(t, f, above, above, func(got *v1beta1.InferenceService) bool {
		if !got.Status.Placement.Plan.PauseSurge {
			t.Fatalf("the raised engine minimum released the pause: %s", describeAutoscaledAll(t, f, got))
		}
		return false
	})
	if condition := got.Status.GetCondition(v1beta1.PlacementConverged); condition == nil || condition.Reason != "AwaitingMemberCleanup" {
		t.Fatalf("the raise did not end in the cleanup wait: %s", describeAutoscaledAll(t, f, got))
	}
	for _, candidate := range got.Status.Placement.Candidates {
		if a := candidate.Allocation; a.OriginalReplicas != 6 || a.CurrentReplicas != 8 || a.DesiredReplicas != 8 {
			t.Fatalf("%s did not take the raised engine minimum: %+v", candidate.Cluster, a)
		}
	}
	at := map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 8, v1beta1.DecoderComponent: 4, v1beta1.RouterComponent: 2}
	if got := serveAutoscaledAll(t, f, at, at, autoscaledAllConverged); got.Status.Placement.Plan.PauseSurge || !autoscaledAllConverged(got) {
		t.Fatalf("engines at the raised minimum left growth paused: %s", describeAutoscaledAll(t, f, got))
	}
}

// TestAllUndoneMemberChangeHoldsThePause lowers the minimums while releasing or
// adding a member, then undoes that member change. The pause holds until that
// member is gone or has applied its minimums, then ends with members above them.
func TestAllUndoneMemberChangeHoldsThePause(t *testing.T) {
	t.Run("member released then added back", func(t *testing.T) {
		f := autoscaledAllFixture(t)
		setAutoscaledAll(t, f, 3, 2, "metadata.name=member-a")
		drained, goneFor := false, 0
		// The pause holds while member-b drains, while it is deleted, and for a
		// pass after it is gone.
		gone := serveAutoscaledAll(t, f, autoscaledAllRunning, autoscaledAllBusy, func(got *v1beta1.InferenceService) bool {
			if !got.Status.Placement.Plan.PauseSurge {
				t.Fatalf("the release ended the pause: %s", describeAutoscaledAll(t, f, got))
			}
			a := candidateOf(t, got, "member-b").Allocation
			drained = drained || a.DrainRequested
			if _, present := allMemberOn(t, f, "member-b"); !present && a.CurrentHome == nil && a.CurrentReplicas == 0 {
				goneFor++
			}
			return goneFor == 2
		})
		if !drained || goneFor != 2 {
			t.Fatalf("member-b was not drained and deleted: %s", describeAutoscaledAll(t, f, gone))
		}
		if condition := gone.Status.GetCondition(v1beta1.PlacementConverged); condition == nil || condition.Reason != "AwaitingMemberCleanup" {
			t.Fatalf("the release did not end in the cleanup wait: %s", describeAutoscaledAll(t, f, gone))
		}
		setAutoscaledAll(t, f, 3, 2, "")
		got := serveAutoscaledAll(t, f, autoscaledAllRunning, autoscaledAllBusy, func(got *v1beta1.InferenceService) bool {
			if !got.Status.Placement.Plan.PauseSurge && candidateOf(t, got, "member-b").AppliedPlanID == "" {
				t.Fatalf("the pause ended before member-b was created again and applied its minimums: %s", describeAutoscaledAll(t, f, got))
			}
			return autoscaledAllConverged(got)
		})
		if a := candidateOf(t, got, "member-b").Allocation; got.Status.Placement.Plan.PauseSurge || !autoscaledAllConverged(got) || a.CurrentReplicas != 3 || a.OriginalReplicas != 3 {
			t.Fatalf("member-b came back but growth stayed paused: %s", describeAutoscaledAll(t, f, got))
		}
	})
	t.Run("member added then released", func(t *testing.T) {
		f := autoscaledAllFixture(t)
		registerAutoscaledAllMember(t, f, "member-c")
		setAutoscaledAll(t, f, 3, 2, "")
		joined := serveAutoscaledAll(t, f, autoscaledAllRunning, autoscaledAllBusy, func(got *v1beta1.InferenceService) bool {
			if !got.Status.Placement.Plan.PauseSurge {
				t.Fatalf("the added member ended the pause: %s", describeAutoscaledAll(t, f, got))
			}
			condition := got.Status.GetCondition(v1beta1.PlacementConverged)
			return candidateOf(t, got, "member-c").Allocation.CurrentReplicas == 3 && condition != nil && condition.Reason == "AwaitingMemberCleanup"
		})
		if condition := joined.Status.GetCondition(v1beta1.PlacementConverged); condition == nil || condition.Reason != "AwaitingMemberCleanup" {
			t.Fatalf("member-c did not join under the cleanup wait: %s", describeAutoscaledAll(t, f, joined))
		}
		setAutoscaledAll(t, f, 3, 2, "metadata.name!=member-c")
		got := serveAutoscaledAll(t, f, autoscaledAllRunning, autoscaledAllBusy, func(got *v1beta1.InferenceService) bool {
			if !got.Status.Placement.Plan.PauseSurge {
				if _, present := allMemberOn(t, f, "member-c"); present || candidateOf(t, got, "member-c").Allocation.CurrentHome != nil {
					t.Fatalf("the pause ended before member-c was verified absent: %s", describeAutoscaledAll(t, f, got))
				}
			}
			return autoscaledAllConverged(got)
		})
		if got.Status.Placement.Plan.PauseSurge || !autoscaledAllConverged(got) {
			t.Fatalf("member-c is gone but growth stayed paused: %s", describeAutoscaledAll(t, f, got))
		}
	})
}

// autoscaledAllRunning is what each member of the autoscaled All service runs:
// 6 engines, 4 decoders and 2 routers, its first minimums. Under load each
// member's autoscaler asks for autoscaledAllBusy.
var (
	autoscaledAllRunning = map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 6, v1beta1.DecoderComponent: 4, v1beta1.RouterComponent: 2}
	autoscaledAllBusy    = map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 9, v1beta1.DecoderComponent: 6, v1beta1.RouterComponent: 2}
)

// autoscaledAllFixture converges an All service with an engine, a decoder and a
// router on member-a and member-b at minimums of 6, 4 and 2. Each member runs
// exactly those replicas.
func autoscaledAllFixture(t *testing.T) *backendFixture {
	t.Helper()
	f := singleFixture(t, true)
	f.source.Spec.Placement.Mode = v1beta1.PlacementModeAll
	f.source.Spec.Engine.MinReplicas, f.source.Spec.Engine.MaxReplicas = ptr.To(6), 18
	f.source.Spec.Decoder.MinReplicas, f.source.Spec.Decoder.MaxReplicas = ptr.To(4), 12
	f.source.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(2), MaxReplicas: 2}, Runner: f.source.Spec.Engine.Runner.DeepCopy()}
	if err := f.reconciler.Update(t.Context(), f.source); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t)
	if settled := serveAutoscaledAll(t, f, autoscaledAllRunning, autoscaledAllRunning, autoscaledAllConverged); !autoscaledAllConverged(settled) || settled.Status.Placement.Plan.PauseSurge {
		t.Fatalf("the service did not settle on its first minimums: %s", describeAutoscaledAll(t, f, settled))
	}
	return f
}

// setAutoscaledAll writes the source's engine and decoder minimums and its
// cluster affinity; an empty selector matches every member.
func setAutoscaledAll(t *testing.T, f *backendFixture, engine, decoder int, selector string) {
	t.Helper()
	source := &v1beta1.InferenceService{}
	if err := f.reconciler.Get(t.Context(), client.ObjectKeyFromObject(f.source), source); err != nil {
		t.Fatal(err)
	}
	source.Spec.Engine.MinReplicas, source.Spec.Decoder.MinReplicas = ptr.To(engine), ptr.To(decoder)
	source.Spec.Placement.ClusterAffinity = testAffinity(selector)
	if err := f.reconciler.Update(t.Context(), source); err != nil {
		t.Fatal(err)
	}
}

// serveAutoscaledAll runs up to twelve passes and stops after the first one for
// which stop reports true. Before each pass members run running[c] while asking
// for requested[c], a deleted member's objects go, and routing follows the plan.
func serveAutoscaledAll(t *testing.T, f *backendFixture, running, requested map[v1beta1.ComponentType]int32, stop func(*v1beta1.InferenceService) bool) *v1beta1.InferenceService {
	t.Helper()
	var got *v1beta1.InferenceService
	for range 12 {
		for _, name := range slices.Sorted(maps.Keys(f.workers)) {
			if _, ok := allMemberOn(t, f, name); ok {
				runAutoscaledAllMember(t, f, name, running, requested)
				continue
			}
			for _, kind := range []client.Object{&v1beta1.InferenceReplica{}, &corev1.Pod{}} {
				if err := f.workers[name].DeleteAllOf(t.Context(), kind, client.InNamespace(f.source.Namespace)); err != nil {
					t.Fatal(err)
				}
			}
		}
		routeAutoscaledAll(t, f)
		if got = f.reconcile(t); stop(got) {
			break
		}
	}
	return got
}

// runAutoscaledAllMember acknowledges the member's policy with each component
// running running[c] while asking for requested[c]. A paused component's limit
// follows the projector: max(minimum, request), then max(minimum, min(limit, request)).
func runAutoscaledAllMember(t *testing.T, f *backendFixture, name string, running, requested map[v1beta1.ComponentType]int32) {
	t.Helper()
	worker := f.workers[name]
	member, _ := allMemberOn(t, f, name)
	policy, err := protocol.FromDerived(member)
	if err != nil || policy == nil {
		t.Fatalf("%s has no member execution policy: %v", name, err)
	}
	minimums := map[v1beta1.ComponentType]int32{}
	for _, m := range policy.ReplicaFloors {
		minimums[m.Component] = m.Replicas
	}
	for _, component := range declaredComponents(f.source) {
		ir := sizeAutoscaledAllComponent(t, worker, member, component, running[component])
		wasPaused, limit := ir.Spec.PlacementExecution != nil && ir.Spec.PlacementExecution.PauseSurge, ir.Spec.PlacementReplicaLimit
		ir.Generation++
		ir.Spec.Replicas, ir.Spec.PlacementExecution, ir.Spec.PlacementReplicaLimit = ptr.To(requested[component]), policy.DeepCopy(), nil
		if policy.PauseSurge {
			next := max(minimums[component], requested[component])
			if wasPaused && limit != nil {
				next = max(minimums[component], min(*limit, requested[component]))
			}
			ir.Spec.PlacementReplicaLimit = ptr.To(next)
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

// sizeAutoscaledAllComponent returns the member's component with count ready
// instances, each served by one Pod. It creates the component, or replaces its
// instances and Pods when it runs a different count.
func sizeAutoscaledAllComponent(t *testing.T, worker client.Client, member *v1beta1.InferenceService, component v1beta1.ComponentType, count int32) *v1beta1.InferenceReplica {
	t.Helper()
	key := client.ObjectKey{Namespace: member.Namespace, Name: member.Name + "-" + string(component)}
	ir := &v1beta1.InferenceReplica{}
	err := worker.Get(t.Context(), key, ir)
	fresh := apierrors.IsNotFound(err)
	if err != nil && !fresh {
		t.Fatal(err)
	}
	if !fresh && int32(len(ir.Status.InstanceStatuses)) == count {
		return ir
	}
	sized := irWithInstances(component, slices.Repeat([]bool{true}, int(count))...)
	sized.Name, sized.Namespace, sized.Status.ReadyReplicas = key.Name, key.Namespace, count
	objects := observedWorkerObjects(member.DeepCopy(), sized)[2:]
	if !fresh {
		// Each instance's Pod is named after the component and the instance index.
		for _, row := range ir.Status.InstanceStatuses {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: fmt.Sprintf("%s-%d", key.Name, row.Index)}}
			if err := worker.Delete(t.Context(), pod); err != nil {
				t.Fatal(err)
			}
		}
		ir.Status.InstanceStatuses, ir.Status.ReadyReplicas = sized.Status.InstanceStatuses, count
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

// routeAutoscaledAll publishes routing the way the routing controller does: a
// member in the stored plan keeps a positive weight until placement asks it to
// drain, and then has weight 0. A cluster the plan no longer tracks gets none.
func routeAutoscaledAll(t *testing.T, f *backendFixture) {
	t.Helper()
	current := &v1beta1.InferenceService{}
	if err := f.reconciler.Get(t.Context(), client.ObjectKeyFromObject(f.source), current); err != nil {
		t.Fatal(err)
	}
	weights := map[string]int32{}
	for _, candidate := range current.Status.Placement.Candidates {
		switch a := candidate.Allocation; {
		case a.CurrentReplicas == 0 && a.DesiredReplicas == 0 && a.CurrentHome == nil && a.DesiredHome == nil:
		case a.DrainRequested:
			weights[candidate.Cluster] = 0
		default:
			weights[candidate.Cluster] = 1
		}
	}
	publishAllRouting(t, f, weights)
}

// registerAutoscaledAllMember registers an empty member cluster.
func registerAutoscaledAllMember(t *testing.T, f *backendFixture, name string) {
	t.Helper()
	if err := f.reconciler.Create(t.Context(), readyWC(name, nil)); err != nil {
		t.Fatal(err)
	}
	f.workers[name] = emptyWorker(f.reconciler.Scheme())
	f.connections.m[name] = workloadcluster.NewNeverCachingClient(f.workers[name])
}

// autoscaledAllConverged reports whether the members have applied the accepted
// allocation.
func autoscaledAllConverged(source *v1beta1.InferenceService) bool {
	condition := source.Status.GetCondition(v1beta1.PlacementConverged)
	return condition != nil && condition.Status == corev1.ConditionTrue
}

// describeAutoscaledAll reports the pause and each member's engine minimums with
// the engine replicas placement counts on it.
func describeAutoscaledAll(t *testing.T, f *backendFixture, source *v1beta1.InferenceService) string {
	t.Helper()
	accepted := source.Status.Placement.Plan
	out := fmt.Sprintf("pauseSurge=%t assignedReplicas=%d", accepted.PauseSurge, accepted.AssignedReplicas)
	observed := f.reconciler.observeSplitMembers(t.Context(), source, slices.Sorted(maps.Keys(f.workers)))
	for _, candidate := range source.Status.Placement.Candidates {
		a := candidate.Allocation
		out += fmt.Sprintf("; %s original=%d current=%d desired=%d drain=%t occupied=%d", candidate.Cluster, a.OriginalReplicas, a.CurrentReplicas, a.DesiredReplicas, a.DrainRequested, observed[candidate.Cluster].MemberState.Occupied)
	}
	if condition := source.Status.GetCondition(v1beta1.PlacementConverged); condition != nil {
		out += fmt.Sprintf("; %s=%s %s", v1beta1.PlacementConverged, condition.Status, condition.Reason)
	}
	return out
}
