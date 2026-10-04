package placement

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/placement/allocation"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
	"sigs.k8s.io/ome/pkg/placement/plan"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

type splitTestClusters struct{ fakeClusters }

func (c splitTestClusters) ClientForUID(name string, uid types.UID) (workloadcluster.SelectivelyCachingClient, bool) {
	if uid != types.UID(name+"-uid") {
		return nil, false
	}
	return c.ClientFor(name)
}

func TestReconcileSplitInitialAllocation(t *testing.T) {
	for _, tt := range []struct {
		name           string
		edit           func(*v1beta1.InferenceService, []*v1beta1.WorkloadCluster)
		wantDesired    map[string]int32
		wantCurrent    map[string]int32
		wantUnassigned int32
		wantReason     string
		wantNoPlan     bool
	}{
		{name: "exact remainder across matched members", wantDesired: map[string]int32{"a": 2, "b": 2, "c": 1}, wantCurrent: map[string]int32{"a": 2, "b": 2, "c": 1}, wantReason: "AwaitingMemberConvergence"},
		{name: "not ready member retains assigned share", edit: func(_ *v1beta1.InferenceService, clusters []*v1beta1.WorkloadCluster) {
			clusters[1].Status.Conditions[0].Status = metav1.ConditionFalse
		}, wantDesired: map[string]int32{"a": 2, "b": 2, "c": 1}, wantCurrent: map[string]int32{"a": 2, "b": 0, "c": 1}, wantReason: "AwaitingMemberConvergence"},
		{name: "empty match leaves requested floor unassigned", edit: func(s *v1beta1.InferenceService, _ []*v1beta1.WorkloadCluster) {
			s.Spec.Placement.ClusterAffinity = testAffinity("location=unmatched")
		}, wantDesired: map[string]int32{}, wantCurrent: map[string]int32{}, wantUnassigned: 5, wantReason: "AllocationConverged"},
		{name: "ceiling overflow holds whole allocation", edit: func(s *v1beta1.InferenceService, _ []*v1beta1.WorkloadCluster) {
			s.Spec.Placement.Split.MaxReplicasPerCluster = 1
		}, wantNoPlan: true, wantReason: "AllocationUnresolved"},
		{name: "runtime maximum remains inherited", edit: func(s *v1beta1.InferenceService, _ []*v1beta1.WorkloadCluster) {
			s.Spec.Engine.MaxReplicas = 0
		}, wantDesired: map[string]int32{"a": 2, "b": 2, "c": 1}, wantCurrent: map[string]int32{"a": 2, "b": 2, "c": 1}, wantReason: "AwaitingMemberConvergence"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			scheme := testScheme(t)
			source := srcISVCSplit("", 5)
			registrations := []*v1beta1.WorkloadCluster{readyWC("a", nil), readyWC("b", nil), readyWC("c", nil)}
			if tt.edit != nil {
				tt.edit(source, registrations)
			}
			workers := map[string]client.WithWatch{}
			connections := splitTestClusters{fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}}}
			objects := []client.Object{source}
			for _, registration := range registrations {
				worker := emptyWorker(scheme)
				workers[registration.Name] = worker
				connections.m[registration.Name] = workloadcluster.NewNeverCachingClient(worker)
				objects = append(objects, registration)
			}
			r, root := newPlacer(scheme, connections, objects...)
			if _, err := r.Reconcile(t.Context(), req()); err != nil {
				t.Fatal(err)
			}
			got := &v1beta1.InferenceService{}
			if err := root.Get(t.Context(), client.ObjectKeyFromObject(source), got); err != nil {
				t.Fatal(err)
			}
			if tt.wantNoPlan {
				if got.Status.Placement.Plan != nil {
					t.Fatal("rejected allocation acquired authority")
				}
			} else {
				if got.Status.Placement.Plan == nil {
					t.Fatal("missing persisted allocation")
				}
				desired, current := map[string]int32{}, map[string]int32{}
				for _, candidate := range got.Status.Placement.Candidates {
					desired[candidate.Cluster] = candidate.Allocation.DesiredReplicas
					current[candidate.Cluster] = candidate.Allocation.CurrentReplicas
				}
				if diff := cmp.Diff(tt.wantDesired, desired); diff != "" {
					t.Errorf("desired (-want +got):\n%s", diff)
				}
				if diff := cmp.Diff(tt.wantCurrent, current); diff != "" {
					t.Errorf("current (-want +got):\n%s", diff)
				}
				if diff := cmp.Diff(tt.wantUnassigned, got.Status.Placement.Plan.UnassignedReplicas); diff != "" {
					t.Error(diff)
				}
			}
			condition := got.Status.GetCondition(apis.ConditionType(v1beta1.PlacementConverged))
			if condition == nil {
				t.Fatal("missing execution condition")
			}
			if diff := cmp.Diff(tt.wantReason, condition.Reason); diff != "" {
				t.Error(diff)
			}
			for name, worker := range workers {
				member := &v1beta1.InferenceService{}
				err := worker.Get(t.Context(), client.ObjectKeyFromObject(source), member)
				floor := tt.wantCurrent[name]
				if floor == 0 {
					if err == nil {
						t.Errorf("zero-floor member %q created", name)
					}
					continue
				}
				if err != nil {
					t.Fatal(err)
				}
				if diff := cmp.Diff(ptr.To(int(floor)), member.Spec.Engine.MinReplicas); diff != "" {
					t.Error(diff)
				}
				if diff := cmp.Diff(source.Spec.Engine.MaxReplicas, member.Spec.Engine.MaxReplicas); diff != "" {
					t.Error(diff)
				}
				if member.Spec.Placement != nil {
					t.Fatal("member retained source placement intent")
				}
				policy, err := protocol.FromDerived(member)
				if err != nil {
					t.Fatal(err)
				}
				if policy == nil || policy.PlanID != got.Status.Placement.Plan.ID || !policy.PauseSurge {
					t.Fatalf("member policy = %+v", policy)
				}
			}
		})
	}
}

func TestAdvanceSplitPlanPauseAndCleanup(t *testing.T) {
	for _, tt := range []struct {
		name    string
		edit    func(*v1beta1.InferenceService, map[string]plannedHomeObservation)
		want    allocation.Step
		wantErr bool
	}{
		{name: "new placement waits for every standing pause", edit: func(_ *v1beta1.InferenceService, obs map[string]plannedHomeObservation) {
			o := obs["a"]
			o.PauseAcknowledged = false
			obs["a"] = o
		}, want: allocation.Step{Targets: map[string]int32{"a": 1, "b": 0}, Reason: "AwaitingSurgePause"}},
		{name: "acknowledged pause allows one shared surge", want: allocation.Step{Targets: map[string]int32{"a": 1, "b": 1}, Reason: "AwaitingMemberConvergence"}},
		{name: "existing rollout uses the allowance", edit: func(_ *v1beta1.InferenceService, obs map[string]plannedHomeObservation) {
			o := obs["a"]
			o.RolloutReserved = 1
			obs["a"] = o
		}, want: allocation.Step{Targets: map[string]int32{"a": 1, "b": 0}, Reason: "SurgeBudgetExhausted"}},
		{name: "unset allowance blocks a move", edit: func(s *v1beta1.InferenceService, _ map[string]plannedHomeObservation) {
			s.Spec.Placement.MaxSurge = nil
		}, want: allocation.Step{Targets: map[string]int32{"a": 1, "b": 0}, Reason: "MigrationBlocked"}},
		{name: "unknown route cannot fund a reduction", edit: func(_ *v1beta1.InferenceService, obs map[string]plannedHomeObservation) {
			o := obs["a"]
			o.Home.Known = false
			obs["a"] = o
		}, want: allocation.Step{Targets: map[string]int32{"a": 1, "b": 0}, Reason: "ObservationUnknown"}},
		{name: "released floor still waits for physical cleanup", edit: func(s *v1beta1.InferenceService, obs map[string]plannedHomeObservation) {
			s.Status.Placement.Candidates[0].Allocation.DesiredReplicas = 1
			s.Status.Placement.Candidates[1].Allocation.DesiredReplicas = 0
			o := obs["a"]
			o.Home.Occupied = 2
			obs["a"] = o
		}, want: allocation.Step{Targets: map[string]int32{"a": 1, "b": 0}, Reason: "AwaitingMemberCleanup"}},
		{name: "settled floor releases pause", edit: func(s *v1beta1.InferenceService, _ map[string]plannedHomeObservation) {
			s.Status.Placement.Candidates[0].Allocation.DesiredReplicas = 1
			s.Status.Placement.Candidates[1].Allocation.DesiredReplicas = 0
		}, want: allocation.Step{Targets: map[string]int32{"a": 1, "b": 0}, Complete: true}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := plannedTestSource()
			source.Spec.Placement.MaxSurge = ptr.To[int32](1)
			source.Status.Placement.Plan.PauseSurge = true
			source.Status.Placement.Plan.AssignedReplicas = 1
			source.Status.Placement.Candidates = []v1beta1.CandidatePlacement{
				{Cluster: "a", Allocation: &v1beta1.CandidateAllocationStatus{ClusterUID: "a-uid", OriginalReplicas: 1, CurrentReplicas: 1}},
				{Cluster: "b", Allocation: &v1beta1.CandidateAllocationStatus{ClusterUID: "b-uid", DesiredReplicas: 1}},
			}
			observations := map[string]plannedHomeObservation{
				"a": {Home: allocation.Home{Known: true, Applied: true, Routable: true, Occupied: 1, Ready: 1}, PauseAcknowledged: true},
				"b": {Home: allocation.Home{Known: true, Absent: true, Eligible: true}},
			}
			if tt.edit != nil {
				tt.edit(source, observations)
			}
			got, err := advanceSplitPlan(source, observations)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v", err)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("step (-want +got):\n%s", diff)
			}
		})
	}
}

func TestReconcileMoveUsesRoutingIntent(t *testing.T) {
	for _, tt := range []struct {
		name           string
		mode           v1beta1.PlacementMode
		cancel, absent bool
	}{
		{name: "All", mode: v1beta1.PlacementModeAll},
		{name: "Split", mode: v1beta1.PlacementModeSplit},
		{name: "Single", mode: v1beta1.PlacementModeSingle},
		{name: "Single cancellation", mode: v1beta1.PlacementModeSingle, cancel: true},
		{name: "Single cancellation after original loss", mode: v1beta1.PlacementModeSingle, cancel: true, absent: true},
	} {
		for _, published := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/published=%t", tt.name, published), func(t *testing.T) { testMoveUsesRoutingIntent(t, tt.mode, tt.cancel, tt.absent, published) })
		}
	}
}

func testMoveUsesRoutingIntent(t *testing.T, mode v1beta1.PlacementMode, cancelMove, absentOriginal, published bool) {
	fixture := observationFixture(t)
	source := fixture.source
	source.Namespace = "prod"
	source.Spec.Placement.Split.Replicas = ptr.To[int32](1)
	source.Spec.Placement.MaxSurge = ptr.To[int32](1)
	source.Spec.Placement.ClusterAffinity = testAffinity("target=true")
	source.Status.Placement.Plan.PauseSurge = false
	source.Status.Placement.Candidates[0].Cluster = "a"
	source.Status.Placement.Candidates[0].Allocation.ClusterUID = "a-uid"
	source.Status.Placement.Candidates[0].Allocation.OriginalReplicas = 1
	if mode == v1beta1.PlacementModeAll {
		source.Spec.Placement.Mode, source.Spec.Placement.Split = mode, nil
		source.Spec.Engine.MinReplicas = ptr.To(1)
		source.Status.Placement.Plan.Mode = mode
		home := &v1beta1.PlacementHomePolicy{InputDigest: "initial-intent", ReplicaFloors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 1}}}
		source.Status.Placement.Candidates[0].Allocation.CurrentHome = home.DeepCopy()
		source.Status.Placement.Candidates[0].Allocation.DesiredHome = home.DeepCopy()
	}
	if mode == v1beta1.PlacementModeSingle {
		source.Spec.Placement.Mode, source.Spec.Placement.Split = mode, nil
		source.Spec.Engine.MinReplicas = ptr.To(1)
		source.Status.Placement.Cluster = "a"
		source.Status.Placement.Plan.Mode, source.Status.Placement.Plan.Winner = mode, "a"
		home := &v1beta1.PlacementHomePolicy{InputDigest: "initial-intent", ReplicaFloors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 1}}}
		source.Status.Placement.Candidates[0].Allocation.CurrentHome = home.DeepCopy()
		source.Status.Placement.Candidates[0].Allocation.DesiredHome = home.DeepCopy()
	}
	fixture.member.Namespace = source.Namespace
	fixture.member.Spec.Engine.MinReplicas = ptr.To(1)
	fixture.resources.ir.Namespace = source.Namespace
	fixture.resources.ir.Name = source.Name + "-engine"
	fixture.resources.pods[0].OwnerReferences[0].Name = fixture.resources.ir.Name
	fixture.resources.pods[0].Namespace = source.Namespace
	policy := executionPolicy(source, source.Status.Placement.Candidates[0].Allocation)
	raw, err := protocol.Encode(policy)
	if err != nil {
		t.Fatal(err)
	}
	fixture.member.Annotations[constants.PlacementExecution] = raw
	fixture.resources.ir.Spec.PlacementExecution = policy
	scheme := testScheme(t)
	workers := map[string]client.WithWatch{"a": emptyWorker(scheme), "b": emptyWorker(scheme)}
	for _, object := range []client.Object{fixture.member, fixture.resources.ir, &fixture.resources.pods[0]} {
		if err := workers["a"].Create(t.Context(), object); err != nil {
			t.Fatal(err)
		}
	}
	connections := splitTestClusters{fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}}}
	for name, worker := range workers {
		connections.m[name] = workloadcluster.NewNeverCachingClient(worker)
	}
	r, root := newPlacer(scheme, connections, source, readyWC("a", nil), readyWC("b", map[string]string{"target": "true"}))
	readSource := func() *v1beta1.InferenceService {
		out := &v1beta1.InferenceService{}
		if err := root.Get(t.Context(), client.ObjectKeyFromObject(source), out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	acknowledge := func(name string, materialize bool) {
		worker := workers[name]
		member := &v1beta1.InferenceService{}
		if err := worker.Get(t.Context(), client.ObjectKeyFromObject(source), member); err != nil {
			t.Fatal(err)
		}
		if member.UID == "" {
			member.UID, member.Generation = types.UID("member-"+name), 1
			if err := worker.Update(t.Context(), member); err != nil {
				t.Fatal(err)
			}
		}
		ir := &v1beta1.InferenceReplica{}
		err := worker.Get(t.Context(), client.ObjectKeyFromObject(fixture.resources.ir), ir)
		if err != nil && !materialize {
			t.Fatal(err)
		}
		fresh := err != nil
		if fresh {
			ir = fixture.resources.ir.DeepCopy()
			ir.ResourceVersion = ""
			ir.UID = types.UID("engine-" + name)
			ir.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(member, v1beta1.SchemeGroupVersion.WithKind("InferenceService"))}
		}
		ir.Generation++
		ir.Status.ObservedGeneration = ir.Generation
		ir.Status.PlacementObservedGeneration = ir.Generation
		ir.Spec.PlacementReplicaLimit = ptr.To[int32](1)
		ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] = strconv.FormatInt(member.Generation, 10)
		ir.Spec.PlacementExecution, err = protocol.FromDerived(member)
		if err != nil {
			t.Fatal(err)
		}
		if fresh {
			err = worker.Create(t.Context(), ir)
		} else {
			err = worker.Update(t.Context(), ir)
		}
		if err != nil {
			t.Fatal(err)
		}
		if materialize {
			pod := fixture.resources.pods[0].DeepCopy()
			pod.ResourceVersion = ""
			pod.UID = types.UID("pod-" + name)
			pod.OwnerReferences[0].UID = ir.UID
			if err := worker.Create(t.Context(), pod); err != nil {
				t.Fatal(err)
			}
			member.Status = fixture.member.Status
			if err := worker.Status().Update(t.Context(), member); err != nil {
				t.Fatal(err)
			}
		}
	}
	publish := func(weights map[string]int32) {
		live := readSource()
		tm := &v1beta1.TrafficMap{}
		err := root.Get(t.Context(), client.ObjectKeyFromObject(source), tm)
		fresh := err != nil
		if fresh {
			tm.ObjectMeta = metav1.ObjectMeta{Name: source.Name, Namespace: source.Namespace, UID: "traffic-map", OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(source, v1beta1.SchemeGroupVersion.WithKind("InferenceService"))}}
		}
		tm.Generation++
		tm.Spec.PlacementPlanID = live.Status.Placement.Plan.ID
		tm.Spec.ObservedISVCGeneration = source.Generation
		tm.Spec.Entries = nil
		for _, name := range []string{"a", "b"} {
			weight, exists := weights[name]
			if exists {
				tm.Spec.Entries = append(tm.Spec.Entries, v1beta1.TrafficMapEntry{Cluster: name, Weight: weight, Endpoint: fixture.member.Status.URL})
			}
		}
		tm.Status.Published, tm.Status.SourceUID = published, source.UID
		if published {
			tm.Status.ObservedTrafficMapGeneration = tm.Generation
		}
		if fresh {
			err = root.Create(t.Context(), tm)
		} else {
			err = root.Update(t.Context(), tm)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	type moveStage struct {
		name    string
		before  func()
		current map[string]int32
		drain   bool
		pause   bool
		memberA bool
	}
	stages := []moveStage{
		{name: "persist pause before replacement", current: map[string]int32{"a": 1, "b": 0}, pause: true, memberA: true},
		{name: "acknowledged pause permits replacement", before: func() { acknowledge("a", false); publish(map[string]int32{"a": 1}) }, current: map[string]int32{"a": 1, "b": 1}, pause: true, memberA: true},
		{name: "unready replacement retains original", before: func() { acknowledge("a", false); publish(map[string]int32{"a": 1}) }, current: map[string]int32{"a": 1, "b": 1}, pause: true, memberA: true},
		{name: "ready routable replacement requests drain", before: func() { acknowledge("b", true); publish(map[string]int32{"a": 1, "b": 1}) }, current: map[string]int32{"a": 1, "b": 1}, drain: true, pause: true, memberA: true},
		{name: "routing drain authorizes zero floor", before: func() { acknowledge("a", false); acknowledge("b", false); publish(map[string]int32{"a": 0, "b": 1}) }, current: map[string]int32{"a": 0, "b": 1}, drain: true, pause: true, memberA: true},
		{name: "failed delete retains the original member", before: func() {
			acknowledge("a", false)
			acknowledge("b", false)
			publish(map[string]int32{"a": 0, "b": 1})
			connections.m["a"] = workloadcluster.NewNeverCachingClient(interceptor.NewClient(workers["a"], interceptor.Funcs{Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
				return fmt.Errorf("member delete unavailable")
			}}))
		}, current: map[string]int32{"a": 0, "b": 1}, drain: true, pause: true, memberA: true},
		{name: "current zero routing plan permits deletion", before: func() {
			connections.m["a"] = workloadcluster.NewNeverCachingClient(workers["a"])
			acknowledge("a", false)
			acknowledge("b", false)
			publish(map[string]int32{"a": 0, "b": 1})
		}, current: map[string]int32{"a": 0, "b": 1}, drain: true, pause: true},
		{name: "orphan resources retain pause", current: map[string]int32{"a": 0, "b": 1}, drain: true, pause: true},
		{name: "physical cleanup releases pause", before: func() {
			if err := workers["a"].Delete(t.Context(), fixture.resources.ir); err != nil {
				t.Fatal(err)
			}
			if err := workers["a"].Delete(t.Context(), &fixture.resources.pods[0]); err != nil {
				t.Fatal(err)
			}
		}, current: map[string]int32{"a": 0, "b": 1}},
		{name: "released plan remains stable", before: func() { acknowledge("b", false); publish(map[string]int32{"b": 1}) }, current: map[string]int32{"a": 0, "b": 1}},
	}
	if mode == v1beta1.PlacementModeSingle {
		stages[len(stages)-1].drain = true
		selectReplacement := moveStage{name: "admission selects before serving handoff", before: func() { acknowledge("b", true); publish(map[string]int32{"a": 1, "b": 1}) }, current: map[string]int32{"a": 1, "b": 1}, pause: true, memberA: true}
		stages[3].before = func() { acknowledge("a", false); acknowledge("b", false); publish(map[string]int32{"a": 1, "b": 1}) }
		stages = append(stages[:3], append([]moveStage{selectReplacement}, stages[3:]...)...)
	}
	if cancelMove {
		stages = append(stages[:4], []moveStage{
			{name: "restored affinity drains selected replacement", before: func() {
				live := readSource()
				live.Spec.Placement.ClusterAffinity = testAffinity("target!=true")
				if err := root.Update(t.Context(), live); err != nil {
					t.Fatal(err)
				}
				acknowledge("a", false)
				acknowledge("b", false)
				publish(map[string]int32{"a": 1, "b": 1})
			}, current: map[string]int32{"a": 1, "b": 1}, pause: true, memberA: true},
			{name: "acknowledged cancellation requests replacement drain", before: func() { acknowledge("a", false); acknowledge("b", false); publish(map[string]int32{"a": 1, "b": 1}) }, current: map[string]int32{"a": 1, "b": 1}, pause: true, memberA: true},
			{name: "replacement drain must be in current routing intent", before: func() { acknowledge("a", false); acknowledge("b", false); publish(map[string]int32{"a": 1, "b": 0}) }, current: map[string]int32{"a": 1, "b": 0}, pause: true, memberA: true},
			{name: "replacement deletion retains pause", before: func() { acknowledge("a", false); acknowledge("b", false); publish(map[string]int32{"a": 1, "b": 0}) }, current: map[string]int32{"a": 1, "b": 0}, pause: true, memberA: true},
			{name: "replacement physical cleanup completes cancellation", before: func() {
				for _, obj := range []client.Object{&v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Name: fixture.resources.ir.Name, Namespace: source.Namespace}}, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: fixture.resources.pods[0].Name, Namespace: source.Namespace}}} {
					if err := workers["b"].Delete(t.Context(), obj); err != nil {
						t.Fatal(err)
					}
				}
			}, current: map[string]int32{"a": 1, "b": 0}, memberA: true},
		}...)
		if absentOriginal {
			cancel := stages[4].before
			stages[4].before = func() {
				cancel()
				for _, obj := range []client.Object{fixture.member, fixture.resources.ir, &fixture.resources.pods[0]} {
					if err := workers["a"].Delete(t.Context(), obj); err != nil {
						t.Fatal(err)
					}
				}
			}
			stages[4].name = "absent original receives fresh restoration goal"
			stages[4].current, stages[4].memberA = map[string]int32{"a": 0, "b": 1}, false
			restore := []moveStage{
				{name: "replacement pause acknowledgement permits original recreation", before: func() { acknowledge("b", false); publish(map[string]int32{"b": 1}) }, current: map[string]int32{"a": 1, "b": 1}, pause: true, memberA: true},
				{name: "unready restoration retains serving replacement", before: func() { acknowledge("b", false); publish(map[string]int32{"b": 1}) }, current: map[string]int32{"a": 1, "b": 1}, pause: true, memberA: true},
			}
			stages[5].before = func() { acknowledge("a", true); acknowledge("b", false); publish(map[string]int32{"a": 1, "b": 1}) }
			stages = append(stages[:5], append(restore, stages[5:]...)...)
		}
	}
	for _, stage := range stages {
		if !t.Run(stage.name, func(t *testing.T) {
			if stage.before != nil {
				stage.before()
			}
			result, err := r.Reconcile(t.Context(), req())
			if err != nil {
				t.Fatal(err)
			}
			live := readSource()
			if stage.name == "physical cleanup releases pause" {
				if result.RequeueAfter <= 0 {
					t.Fatal("final policy must schedule an acknowledgement observation")
				}
				if c := live.Status.GetCondition(v1beta1.PlacementConverged); c == nil || c.Status == corev1.ConditionTrue {
					t.Fatalf("unobserved final policy reported convergence: %+v", c)
				}
			}
			if mode == v1beta1.PlacementModeSingle {
				winner := "a"
				if !cancelMove && (stage.drain || stage.current["a"] == 0) {
					winner = "b"
				}
				if diff := cmp.Diff(winner, live.Status.Placement.Plan.Winner); diff != "" {
					t.Errorf("serving winner: %s", diff)
				}
			}
			current := map[string]int32{}
			for _, candidate := range live.Status.Placement.Candidates {
				current[candidate.Cluster] = candidate.Allocation.CurrentReplicas
				if candidate.Cluster == "a" {
					if diff := cmp.Diff(stage.drain, candidate.Allocation.DrainRequested); diff != "" {
						t.Error(diff)
					}
					if absentOriginal {
						if diff := cmp.Diff(int32(1), candidate.Allocation.OriginalReplicas); diff != "" {
							t.Fatalf("restoration changed original budget: %s", diff)
						}
					}
				}
			}
			if diff := cmp.Diff(stage.current, current); diff != "" {
				t.Errorf("floor (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(stage.pause, live.Status.Placement.Plan.PauseSurge); diff != "" {
				t.Error(diff)
			}
			member := &v1beta1.InferenceService{}
			err = workers["a"].Get(t.Context(), client.ObjectKeyFromObject(source), member)
			if diff := cmp.Diff(stage.memberA, err == nil); diff != "" {
				t.Errorf("original member existence (-want +got):\n%s", diff)
			}
		}) {
			return
		}
	}
	if !cancelMove {
		for attempt := 0; attempt < 3; attempt++ {
			acknowledge("b", false)
			publish(map[string]int32{"b": 1})
			result, err := r.Reconcile(t.Context(), req())
			if err != nil {
				t.Fatal(err)
			}
			live := readSource()
			if c := live.Status.GetCondition(v1beta1.PlacementConverged); c != nil && c.Status == corev1.ConditionTrue {
				for _, c := range live.Status.Placement.Candidates {
					if c.Allocation.DesiredReplicas > 0 && c.AppliedPlanID != live.Status.Placement.Plan.ID {
						t.Fatal("final member acknowledgement is stale")
					}
				}
				if c := live.Status.GetCondition(v1beta1.PlacementSatisfied); c == nil || c.Status != corev1.ConditionTrue {
					t.Fatalf("final plan not satisfied: %+v", c)
				}
				return
			}
			if result.RequeueAfter <= 0 {
				t.Fatal("unacknowledged plan stopped reconciling")
			}
		}
		t.Fatal("final member policy did not converge")
	}

}

func TestAdoptSplitMembers(t *testing.T) {
	for _, tt := range []struct {
		name      string
		edit      func(*plannedObservationFixture)
		wantFloor int32
		wantErr   bool
	}{
		{name: "standing pending floor consumes capacity without increasing budget", edit: func(f *plannedObservationFixture) {
			f.member.Spec.Engine.MinReplicas = ptr.To(6)
		}, wantFloor: 6},
		{name: "zero floor with physical resources remains explicit", edit: func(f *plannedObservationFixture) {
			f.member.Spec.Engine.MinReplicas = ptr.To(0)
		}},
		{name: "unrecorded copy outside affinity remains accounted", edit: func(f *plannedObservationFixture) {
			f.source.Status.Placement = nil
			f.source.Spec.Placement.ClusterAffinity = testAffinity("region=unmatched")
			f.member.Spec.Engine.MinReplicas = ptr.To(1)
		}, wantFloor: 1},
		{name: "unresolved member floor holds adoption", edit: func(f *plannedObservationFixture) {
			f.member.Spec.Engine.MinReplicas = nil
		}, wantErr: true},
		{name: "mismatched component floors hold adoption", edit: func(f *plannedObservationFixture) {
			f.member.Spec.Engine.MinReplicas = ptr.To(1)
			f.member.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(2)}}
		}, wantErr: true},
		{name: "missing registration cannot erase a standing home", edit: func(f *plannedObservationFixture) {
			f.source.Status.Placement.Candidates = append(f.source.Status.Placement.Candidates, v1beta1.CandidatePlacement{Cluster: "removed"})
		}, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fixture := observationFixture(t)
			fixture.source.Status.Placement.Plan = nil
			fixture.source.Status.Placement.Candidates[0].Allocation = nil
			delete(fixture.member.Annotations, constants.PlacementExecution)
			fixture.resources.ir.Spec.PlacementExecution = nil
			if tt.edit != nil {
				tt.edit(&fixture)
			}
			scheme := testScheme(t)
			worker := emptyWorker(scheme)
			for _, object := range []client.Object{fixture.member, fixture.resources.ir, &fixture.resources.pods[0]} {
				if err := worker.Create(t.Context(), object); err != nil {
					t.Fatal(err)
				}
			}
			before := &v1beta1.InferenceService{}
			if err := worker.Get(t.Context(), client.ObjectKeyFromObject(fixture.member), before); err != nil {
				t.Fatal(err)
			}
			registration := plannedTestRegistration()
			connections := identifiedTestClusters{fakeClusters: fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{"member-a": workloadcluster.NewNeverCachingClient(worker)}}, uid: registration.UID}
			r, _ := newPlacer(scheme, connections, fixture.source, registration)
			proposal, err := desiredSplitPlan(fixture.source, []v1beta1.WorkloadCluster{*registration}, nil)
			if err != nil {
				t.Fatal(err)
			}
			err = r.adoptSplitMembers(t.Context(), fixture.source, []v1beta1.WorkloadCluster{*registration}, &proposal)
			if diff := cmp.Diff(tt.wantErr, err != nil); diff != "" {
				t.Fatalf("%s: %v", diff, err)
			}
			if !tt.wantErr {
				assignment := proposal.Assignments["member-a"]
				if diff := cmp.Diff(tt.wantFloor, assignment.CurrentReplicas); diff != "" {
					t.Error(diff)
				}
				if diff := cmp.Diff(int32(0), assignment.OriginalReplicas); diff != "" {
					t.Error(diff)
				}
				if diff := cmp.Diff(int32(2), proposal.OriginalUnassignedReplicas); diff != "" {
					t.Error(diff)
				}
				if !proposal.PauseSurge {
					t.Error("adopted member must acknowledge a pause")
				}
			}
			after := &v1beta1.InferenceService{}
			if err := worker.Get(t.Context(), client.ObjectKeyFromObject(fixture.member), after); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(before, after); diff != "" {
				t.Errorf("adoption mutated member (-want +got):\n%s", diff)
			}
		})
	}
}

type recordingSink struct {
	infos  []string
	errors []string
}

func (s *recordingSink) Init(logr.RuntimeInfo)               {}
func (s *recordingSink) Enabled(int) bool                    { return true }
func (s *recordingSink) Info(_ int, msg string, _ ...any)    { s.infos = append(s.infos, msg) }
func (s *recordingSink) Error(_ error, msg string, _ ...any) { s.errors = append(s.errors, msg) }
func (s *recordingSink) WithValues(...any) logr.LogSink      { return s }
func (s *recordingSink) WithName(string) logr.LogSink        { return s }

func TestReconcileSplitApplyErrorsKeepServingHealth(t *testing.T) {
	conflict := apierrors.NewConflict(schema.GroupResource{Group: v1beta1.SchemeGroupVersion.Group, Resource: "inferenceservices"}, "service", fmt.Errorf("concurrent member update"))
	for _, tt := range []struct {
		name       string
		failure    error
		wantReason string
		wantErrors []string
		wantInfos  []string
	}{
		{name: "write failure", failure: fmt.Errorf("member write unavailable"), wantReason: "MemberApplicationFailed", wantErrors: []string{"planned member application failed"}},
		{name: "optimistic lock conflict", failure: conflict, wantReason: "MemberApplicationPending", wantInfos: []string{"planned member application conflicted; retrying"}},
		{name: "wrapped optimistic lock conflict", failure: fmt.Errorf("member apply: %w", conflict), wantReason: "MemberApplicationPending", wantInfos: []string{"planned member application conflicted; retrying"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fixture := observationFixture(t)
			fixture.source.Spec.Placement.Split.Replicas = ptr.To[int32](1)
			fixture.member.Spec.Engine.MinReplicas = ptr.To(1)
			scheme := testScheme(t)
			base := emptyWorker(scheme)
			for _, object := range []client.Object{fixture.member, fixture.resources.ir, &fixture.resources.pods[0]} {
				if err := base.Create(t.Context(), object); err != nil {
					t.Fatal(err)
				}
			}
			updates := 0
			worker := interceptor.NewClient(base, interceptor.Funcs{Update: func(context.Context, client.WithWatch, client.Object, ...client.UpdateOption) error {
				updates++
				return tt.failure
			}})
			connections := identifiedTestClusters{fakeClusters: fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{"member-a": workloadcluster.NewNeverCachingClient(worker)}}, uid: "member-a-uid"}
			r, root := newPlacer(scheme, connections, fixture.source, plannedTestRegistration())
			sink := &recordingSink{}
			r.Log = logr.New(sink)
			result, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(fixture.source)})
			if err != nil {
				t.Fatal(err)
			}
			if updates == 0 {
				t.Fatal("member update was not attempted")
			}
			if diff := cmp.Diff(time.Second, result.RequeueAfter); diff != "" {
				t.Fatalf("retry interval (-want +got):\n%s", diff)
			}
			live := &v1beta1.InferenceService{}
			if err := root.Get(t.Context(), client.ObjectKeyFromObject(fixture.source), live); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(corev1.ConditionTrue, live.Status.GetCondition(apis.ConditionReady).Status); diff != "" {
				t.Error(diff)
			}
			if diff := cmp.Diff(tt.wantReason, live.Status.GetCondition(apis.ConditionType(v1beta1.PlacementConverged)).Reason); diff != "" {
				t.Error(diff)
			}
			if diff := cmp.Diff(tt.wantErrors, sink.errors); diff != "" {
				t.Errorf("error logs (-want +got):\n%s", diff)
			}
			var applicationLogs []string
			for _, message := range sink.infos {
				if strings.HasPrefix(message, "planned member application") {
					applicationLogs = append(applicationLogs, message)
				}
			}
			if diff := cmp.Diff(tt.wantInfos, applicationLogs); diff != "" {
				t.Errorf("retry logs (-want +got):\n%s", diff)
			}
		})
	}
}

func TestReconcileSplitConflictDoesNotHideWriteFailure(t *testing.T) {
	for _, failureOn := range []string{"a", "b"} {
		t.Run("failure on "+failureOn, func(t *testing.T) {
			scheme := testScheme(t)
			source := srcISVCSplit("", 2)
			connections := splitTestClusters{fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}}}
			attempts := 0
			for _, name := range []string{"a", "b"} {
				worker := interceptor.NewClient(emptyWorker(scheme), interceptor.Funcs{Create: func(_ context.Context, _ client.WithWatch, object client.Object, _ ...client.CreateOption) error {
					attempts++
					if name == failureOn {
						return fmt.Errorf("member write unavailable")
					}
					return apierrors.NewConflict(schema.GroupResource{Group: v1beta1.SchemeGroupVersion.Group, Resource: "inferenceservices"}, object.GetName(), fmt.Errorf("concurrent member update"))
				}})
				connections.m[name] = workloadcluster.NewNeverCachingClient(worker)
			}
			r, root := newPlacer(scheme, connections, source, readyWC("a", nil), readyWC("b", nil))
			sink := &recordingSink{}
			r.Log = logr.New(sink)
			result, err := r.Reconcile(t.Context(), req())
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(2, attempts); diff != "" {
				t.Fatalf("member attempts (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(time.Second, result.RequeueAfter); diff != "" {
				t.Fatalf("retry interval (-want +got):\n%s", diff)
			}
			live := &v1beta1.InferenceService{}
			if err := root.Get(t.Context(), client.ObjectKeyFromObject(source), live); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff("MemberApplicationFailed", live.Status.GetCondition(apis.ConditionType(v1beta1.PlacementConverged)).Reason); diff != "" {
				t.Errorf("execution reason (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff([]string{"planned member application failed"}, sink.errors); diff != "" {
				t.Errorf("error logs (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSplitPauseRequired(t *testing.T) {
	for _, tt := range []struct {
		name                   string
		surge                  *int32
		active, adoption, want bool
	}{
		{name: "unconfigured move leaves member execution available"},
		{name: "unconfigured adoption does not freeze members", adoption: true},
		{name: "explicit zero allowance coordinates members", surge: ptr.To[int32](0), want: true},
		{name: "explicit allowance coordinates members", surge: ptr.To[int32](1), want: true},
		{name: "removing allowance cannot release an active move", active: true, want: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := plannedTestSource()
			source.Spec.Placement.MaxSurge = tt.surge
			source.Status.Placement.Plan.PauseSurge = tt.active
			proposal := plan.Proposal{PauseSurge: tt.active || tt.adoption, Assignments: map[string]v1beta1.CandidateAllocationStatus{
				"a": {CurrentReplicas: 1}, "b": {DesiredReplicas: 1},
			}}
			if diff := cmp.Diff(tt.want, splitPauseRequired(source, proposal)); diff != "" {
				t.Error(diff)
			}
		})
	}
}

func TestSplitMemberUpdatesRequireEligibility(t *testing.T) {
	for _, tt := range []struct {
		name              string
		unready, outgoing bool
	}{
		{name: "eligible home receives source spec"},
		{name: "unready home retains serving spec", unready: true},
		{name: "outgoing home retains serving spec", outgoing: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := observationFixture(t)
			f.source.Spec.Placement.Split.Replicas = ptr.To[int32](1)
			f.source.Spec.Engine.MinReplicas = ptr.To(1)
			f.source.Spec.Engine.MaxReplicas = 9
			f.source.Status.Placement.Candidates[0].Allocation.OriginalReplicas = 1
			f.member.Spec.Engine.MinReplicas = ptr.To(1)
			f.member.Spec.Engine.MaxReplicas = 7
			before := f.member.Spec.DeepCopy()
			registration := plannedTestRegistration()
			if tt.unready {
				registration.Status.Conditions[0].Status = metav1.ConditionFalse
			}
			if tt.outgoing {
				f.source.Spec.Placement.ClusterAffinity = testAffinity("location=unmatched")
			}
			scheme := testScheme(t)
			worker := emptyWorker(scheme)
			for _, object := range []client.Object{f.member, f.resources.ir, &f.resources.pods[0]} {
				if err := worker.Create(t.Context(), object); err != nil {
					t.Fatal(err)
				}
			}
			connections := identifiedTestClusters{fakeClusters: fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{"member-a": workloadcluster.NewNeverCachingClient(worker)}}, uid: "member-a-uid"}
			r, root := newPlacer(scheme, connections, f.source, registration)
			if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.source)}); err != nil {
				t.Fatal(err)
			}
			got, source := &v1beta1.InferenceService{}, &v1beta1.InferenceService{}
			if err := worker.Get(t.Context(), client.ObjectKeyFromObject(f.source), got); err != nil {
				t.Fatal(err)
			}
			if err := root.Get(t.Context(), client.ObjectKeyFromObject(f.source), source); err != nil {
				t.Fatal(err)
			}
			if tt.unready || tt.outgoing {
				if diff := cmp.Diff(*before, got.Spec); diff != "" {
					t.Errorf("ineligible member spec changed:\n%s", diff)
				}
			} else if diff := cmp.Diff(9, got.Spec.Engine.MaxReplicas); diff != "" {
				t.Error(diff)
			}
			policy, err := protocol.FromDerived(got)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(executionPolicy(source, source.Status.Placement.Candidates[0].Allocation), policy); diff != "" {
				t.Errorf("coordination policy did not advance:\n%s", diff)
			}
		})
	}
}

func TestSplitObservationWithoutPublication(t *testing.T) {
	for _, tt := range []struct {
		name            string
		edit            func(*plannedObservationFixture)
		known, complete bool
	}{
		{name: "settled positive floor releases its pause", known: true, complete: true},
		{name: "settled floor still requires member acknowledgement", edit: func(f *plannedObservationFixture) { f.resources.ir.Status.PlacementObservedGeneration = 0 }, known: true},
		{name: "a changed target waits for publication", edit: func(f *plannedObservationFixture) {
			f.source.Status.Placement.Candidates[0].Allocation.DesiredReplicas = 2
		}},
		{name: "zero floor with remaining resources requires drain proof", edit: func(f *plannedObservationFixture) {
			f.source.Status.Placement.Candidates[0].Allocation.CurrentReplicas = 0
			f.source.Status.Placement.Candidates[0].Allocation.DesiredReplicas = 0
		}},
		{name: "unverified inventory cannot release a pause", edit: func(f *plannedObservationFixture) { f.resources.pods[0].OwnerReferences = nil }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := observationFixture(t)
			f.source.Status.Placement.Plan.AssignedReplicas = 1
			f.source.Status.Placement.Plan.OriginalUnassignedReplicas = 1
			if tt.edit != nil {
				tt.edit(&f)
			}
			scheme := testScheme(t)
			worker := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.member, f.resources.ir, &f.resources.pods[0]).Build()
			clusters := identifiedTestClusters{fakeClusters: fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{"member-a": workloadcluster.NewNeverCachingClient(worker)}}, uid: "member-a-uid"}
			r, _ := newPlacer(scheme, clusters, f.source, plannedTestRegistration())
			observations := r.observeSplitMembers(t.Context(), f.source, []string{"member-a"})
			if diff := cmp.Diff(tt.known, observations["member-a"].Home.Known); diff != "" {
				t.Error(diff)
			}
			step, err := advanceSplitPlan(f.source, observations)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.complete, step.Complete); diff != "" {
				t.Errorf("completion (-want +got):\n%s; reason: %s", diff, step.Reason)
			}
		})
	}
}
