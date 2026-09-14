package engine

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/scheduling"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
	v1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

type simulationFunc func(context.Context, scheduling.Request) (scheduling.Result, error)

func (f simulationFunc) Evaluate(ctx context.Context, r scheduling.Request) (scheduling.Result, error) {
	return f(ctx, r)
}

func feasiblePrediction(r scheduling.Request) scheduling.Result {
	result := scheduling.Result{SchemaVersion: r.SchemaVersion, RequestID: r.RequestID, SnapshotID: r.SnapshotID,
		SnapshotTime: r.SnapshotTime, Profile: r.Profile, Decision: scheduling.DecisionFeasible, Reason: scheduling.SimulationReasonPlacementFound}
	for _, pod := range r.ReplacementPods {
		result.Placements = append(result.Placements, scheduling.Placement{Pod: scheduling.PodIdentity{Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID}, NodeName: "target"})
	}
	return result
}

type predictionReader struct {
	client.Reader
	lists int
	fail  bool
}

func (r *predictionReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	r.lists++
	if r.fail {
		return errors.New("private API error")
	}
	return r.Reader.List(ctx, list, opts...)
}

// The fixture uses only public API objects and the real observation builder.
func predictionFixture(t *testing.T) (*snapshot.ClusterSnapshot, *predictionReader, policy.Candidate) {
	return predictionScenario(t, false)
}

func predictionScenario(t *testing.T, gang bool) (*snapshot.ClusterSnapshot, *predictionReader, policy.Candidate) {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, appsv1.AddToScheme, v1beta1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	gv := schema.GroupVersion{Group: "scheduling.x-k8s.io", Version: "v1alpha1"}
	scheme.AddKnownTypeWithName(gv.WithKind("PodGroup"), &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(gv.WithKind("PodGroupList"), &unstructured.UnstructuredList{})
	meta := func(name string, uid types.UID) metav1.ObjectMeta {
		return metav1.ObjectMeta{Name: name, Namespace: "prod", UID: uid, Generation: 1}
	}
	isvc := &v1beta1.InferenceService{ObjectMeta: meta("a", "isvc-uid"),
		Spec: v1beta1.InferenceServiceSpec{DeploymentMode: ptr.To(constants.OMENative), Engine: &v1beta1.EngineSpec{}},
		Status: v1beta1.InferenceServiceStatus{Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
			v1beta1.EngineComponent: {Lifecycle: &v1beta1.LifecycleStatus{ObservedGeneration: 1, CurrentRevision: "a-engine-rev-a", UpdateRevision: "a-engine-rev-a"}},
		}}}
	podSpec := corev1.PodSpec{SchedulerName: "default-scheduler", Containers: []corev1.Container{{Name: "model", Image: "example.invalid/model:v1",
		Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")}}}}}
	ir := &v1beta1.InferenceReplica{ObjectMeta: meta("a-engine", "ir-uid"),
		Spec: v1beta1.InferenceReplicaSpec{ParentRef: v1beta1.ParentReference{Name: "a"}, Component: v1beta1.EngineComponent,
			Runners: []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1, Template: corev1.PodTemplateSpec{Spec: podSpec}}}},
		Status: v1beta1.InferenceReplicaStatus{ObservedGeneration: 1, CurrentRevision: "a-engine-rev-a", UpdateRevision: "a-engine-rev-a",
			InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady,
				RunningRevision: "a-engine-rev-a", PodCount: 1, ServingPodCount: 1, AvailablePodCount: 1, Admitted: true}}}}
	ir.OwnerReferences = []metav1.OwnerReference{{APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: "InferenceService", Name: isvc.Name, UID: isvc.UID, Controller: ptr.To(true)}}
	pod := &corev1.Pod{ObjectMeta: meta("source-pod", "pod-uid"), Spec: *podSpec.DeepCopy(),
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
	pod.Spec.NodeName = "source"
	pod.Labels = map[string]string{"ome.io/inferenceservice": "a", "component": "engine", "ome.io/managed-by": "OMENative",
		"ome.io/instance-index": "0", "ome.io/instance-incarnation": "1", "ome.io/runner": "default", "ome.io/pod-ordinal": "0", "ome.io/revision-hash": "a"}
	pod.OwnerReferences = []metav1.OwnerReference{{APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: "InferenceReplica", Name: ir.Name, UID: ir.UID, Controller: ptr.To(true)}}
	objects := []client.Object{isvc, ir, pod, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "prod", UID: "ns-uid"}}}
	if gang {
		ir.Spec.TopologyKey = ptr.To("topology.kubernetes.io/zone")
		ir.Spec.Runners[0].Name = v1beta1.RunnerNameLeader
		ir.Spec.Runners[0].Template.Spec.SchedulerName = "ome-scheduler"
		workerRunner := ir.Spec.Runners[0].DeepCopy()
		workerRunner.Name = v1beta1.RunnerNameWorker
		ir.Spec.Runners = append(ir.Spec.Runners, *workerRunner)
		row := &ir.Status.InstanceStatuses[0]
		row.PodCount, row.ServingPodCount, row.AvailablePodCount = 2, 2, 2
		pod.Spec.SchedulerName = "ome-scheduler"
		pod.Labels["ome.io/runner"] = "leader"
		pod.Labels["scheduling.x-k8s.io/pod-group"] = "a-engine-0"
		worker := pod.DeepCopy()
		worker.Name, worker.UID = "source-worker", "worker-uid"
		worker.Spec.NodeName = "source-worker"
		worker.Labels["ome.io/runner"] = "worker"
		worker.Spec.Affinity = &corev1.Affinity{PodAffinity: &corev1.PodAffinity{RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
			TopologyKey: "topology.kubernetes.io/zone", LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
				"ome.io/inferenceservice": "a", "component": "engine", "ome.io/instance-index": "0", "ome.io/runner": "leader",
			}},
		}}}}
		group := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "scheduling.x-k8s.io/v1alpha1", "kind": "PodGroup",
			"metadata": map[string]any{"name": "a-engine-0", "namespace": "prod", "uid": "group-uid",
				"annotations":     map[string]any{"ome.io/topology-key": "topology.kubernetes.io/zone"},
				"labels":          map[string]any{"ome.io/inferenceservice": "a", "component": "engine", "ome.io/managed-by": "OMENative", "ome.io/instance-index": "0"},
				"ownerReferences": []any{map[string]any{"apiVersion": v1beta1.SchemeGroupVersion.String(), "kind": "InferenceReplica", "name": ir.Name, "uid": string(ir.UID), "controller": true}}},
			"spec": map[string]any{"minMember": int64(2)},
		}}
		objects = append(objects, worker, group)
	}
	names := []string{"source", "target"}
	if gang {
		names = append(names, "source-worker", "target-worker")
	}
	for _, name := range names {
		zone := "source-zone"
		if name == "target" || name == "target-worker" {
			zone = "target-zone"
		}
		objects = append(objects, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name), Labels: map[string]string{"kubernetes.io/hostname": name, "topology.kubernetes.io/zone": zone}},
			Status: corev1.NodeStatus{Capacity: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("8"), corev1.ResourcePods: resource.MustParse("100")},
				Allocatable: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("8"), corev1.ResourcePods: resource.MustParse("100")},
				Conditions:  []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}})
	}
	reader := &predictionReader{Reader: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()}
	snap, err := snapshot.Build(context.Background(), reader, snapshot.Options{Now: func() time.Time { return testNow }})
	if err != nil {
		t.Fatal(err)
	}
	reader.lists = 0
	c := cand("prod/a", "source")
	c.Mode, c.Executable, c.AdvisoryReason = constants.OMENative, false, policy.AdvisoryOMENativeUnavailable
	c.HintTargetNodes, c.PlacementTargetNodes = nil, nil
	comp := snap.Workloads[c.Workload].Components[c.Component]
	if !comp.ObservationValid || len(comp.Instances) == 0 || len(comp.Instances[0].Pods) == 0 || comp.Instances[0].Pods[0].UID == "" {
		t.Fatalf("fixture must preserve valid complete member identity: %+v", comp)
	}
	return snap, reader, c
}

func TestPredictionResultsStayAdvisory(t *testing.T) {
	for _, decision := range []scheduling.Decision{scheduling.DecisionFeasible, scheduling.DecisionInfeasible, scheduling.DecisionUnsupported} {
		t.Run(string(decision), func(t *testing.T) {
			snap, reader, c := predictionFixture(t)
			loop, reporter, _ := newTestLoop(t, snap, &stubPolicy{out: []policy.Candidate{c}})
			if _, err := loop.Store.Update([]byte(schedulingProfilesYAML + "mode: execute\n")); err != nil {
				t.Fatal(err)
			}
			calls := 0
			loop.Predictions = &PredictionStage{Reader: reader, Now: loop.Now, Simulator: simulationFunc(func(_ context.Context, r scheduling.Request) (scheduling.Result, error) {
				calls++
				result := feasiblePrediction(r)
				result.Decision = decision
				if decision != scheduling.DecisionFeasible {
					result.Placements = nil
					result.Reason = scheduling.SimulationReasonUnsupported
					if decision == scheduling.DecisionInfeasible {
						result.Reason = scheduling.SimulationReasonNoFeasiblePlacement
					}
				}
				return result, nil
			})}
			loop.RunOnce(context.Background())
			got := readSchedulingRecommendation(t, reporter)
			if calls != 1 || got.Outcome != OutcomeAdvisory || got.AdvisoryReason != c.AdvisoryReason || got.Scheduling.Status != string(decision) {
				t.Fatalf("prediction not reported safely: calls=%d recommendation=%+v scheduling=%+v", calls, got, got.Scheduling)
			}
			if !reflect.DeepEqual(c, loop.Policies[0].(*stubPolicy).out[0]) {
				t.Fatal("mutated policy candidate")
			}
			if reader.lists != 10 {
				t.Fatalf("expected one capture, got %d lists", reader.lists)
			}
			if len(loop.Arbiter.Ledger.ActiveClaims()) != 0 || loop.Arbiter.Ledger.DispatchesWithinHour(testNow) != 0 {
				t.Fatal("prediction consumed migration capacity or rate budget")
			}
		})
	}
}

func TestPredictionRejectsStaleAndChangedSources(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*snapshot.ClusterSnapshot)
		want   string
	}{
		{"stale observation", func(s *snapshot.ClusterSnapshot) { s.Timestamp = testNow.Add(-time.Minute) }, "ObservationStale"},
		{"isvc uid", func(s *snapshot.ClusterSnapshot) {
			s.Workloads[types.NamespacedName{Namespace: "prod", Name: "a"}].ISVC.UID = "old"
		}, "SourceChanged"},
		{"isvc generation", func(s *snapshot.ClusterSnapshot) {
			s.Workloads[types.NamespacedName{Namespace: "prod", Name: "a"}].ISVC.Generation++
		}, "SourceChanged"},
		{"ir uid", func(s *snapshot.ClusterSnapshot) {
			s.Workloads[types.NamespacedName{Namespace: "prod", Name: "a"}].Components[v1beta1.EngineComponent].IR.UID = "old"
		}, "SourceChanged"},
		{"ir generation", func(s *snapshot.ClusterSnapshot) {
			ir := s.Workloads[types.NamespacedName{Namespace: "prod", Name: "a"}].Components[v1beta1.EngineComponent].IR
			ir.Generation++
			ir.Status.ObservedGeneration++
		}, "SourceChanged"},
		{"revision", func(s *snapshot.ClusterSnapshot) {
			s.Workloads[types.NamespacedName{Namespace: "prod", Name: "a"}].Components[v1beta1.EngineComponent].IR.Status.CurrentRevision = "different"
		}, "SourceChanged"},
		{"missing member", func(s *snapshot.ClusterSnapshot) {
			s.Workloads[types.NamespacedName{Namespace: "prod", Name: "a"}].Components[v1beta1.EngineComponent].Instances[0].Pods = nil
		}, "SourceChanged"},
		{"incarnation", func(s *snapshot.ClusterSnapshot) {
			s.Workloads[types.NamespacedName{Namespace: "prod", Name: "a"}].Components[v1beta1.EngineComponent].Instances[0].Incarnation++
		}, "SourceChanged"},
		{"pod uid", func(s *snapshot.ClusterSnapshot) {
			s.Workloads[types.NamespacedName{Namespace: "prod", Name: "a"}].Components[v1beta1.EngineComponent].Instances[0].Pods[0].UID = "old"
		}, "SourceChanged"},
		{"pod placement", func(s *snapshot.ClusterSnapshot) {
			s.Workloads[types.NamespacedName{Namespace: "prod", Name: "a"}].Components[v1beta1.EngineComponent].Instances[0].Pods[0].Node = "elsewhere"
		}, "SourceChanged"},
		{"pod footprint", func(s *snapshot.ClusterSnapshot) {
			s.Workloads[types.NamespacedName{Namespace: "prod", Name: "a"}].Components[v1beta1.EngineComponent].Instances[0].Pods[0].GPUs++
		}, "SourceChanged"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snap, reader, c := predictionFixture(t)
			tc.mutate(snap)
			loop, reporter, _ := newTestLoop(t, snap, &stubPolicy{out: []policy.Candidate{c}})
			if _, err := loop.Store.Update([]byte(schedulingProfilesYAML)); err != nil {
				t.Fatal(err)
			}
			loop.Predictions = &PredictionStage{Reader: reader, Now: loop.Now, Simulator: simulationFunc(func(context.Context, scheduling.Request) (scheduling.Result, error) {
				t.Fatal("changed source reached worker")
				return scheduling.Result{}, nil
			})}
			loop.RunOnce(context.Background())
			got := readSchedulingRecommendation(t, reporter)
			if got.Scheduling == nil || got.Scheduling.Reason != tc.want {
				t.Fatalf("got %+v want %s", got.Scheduling, tc.want)
			}
		})
	}
}

func TestPredictionFailureAndBudgets(t *testing.T) {
	for _, tc := range []struct{ name, want string }{
		{"capture", "CaptureFailed"}, {"worker", "WorkerFailed"}, {"envelope", "InvalidResponse"},
		{"late", "SnapshotStale"}, {"deadline", "SimulationBudgetExceeded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snap, reader, c := predictionFixture(t)
			loop, reporter, _ := newTestLoop(t, snap, &stubPolicy{out: []policy.Candidate{c}})
			if _, err := loop.Store.Update([]byte(schedulingProfilesYAML)); err != nil {
				t.Fatal(err)
			}
			now := testNow
			if tc.name == "capture" {
				reader.fail = true
			}
			stage := &PredictionStage{Reader: reader, Now: func() time.Time { return now }, Simulator: simulationFunc(func(ctx context.Context, r scheduling.Request) (scheduling.Result, error) {
				result := feasiblePrediction(r)
				switch tc.name {
				case "worker":
					return scheduling.Result{}, errors.New("private worker payload")
				case "envelope":
					result.RequestID = "foreign"
				case "late":
					now = now.Add(time.Minute)
				case "deadline":
					<-ctx.Done()
				}
				return result, nil
			})}
			ctx := context.Background()
			if tc.name == "deadline" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 10*time.Millisecond)
				defer cancel()
			}
			loop.Predictions = stage
			loop.RunOnce(ctx)
			// Inspect stage directly if cancellation prevents reporter writes.
			if tc.name == "deadline" {
				got := stage.Annotate(ctx, snap, loop.Store.Get(), []policy.Candidate{gateSchedulingCandidate(snap, loop.Store.Get(), c)})
				if got[0].Scheduling.Reason != tc.want {
					t.Fatalf("got %+v", got[0].Scheduling)
				}
				return
			}
			got := readSchedulingRecommendation(t, reporter)
			if got.Scheduling == nil || got.Scheduling.Reason != tc.want {
				t.Fatalf("got %+v want %s", got.Scheduling, tc.want)
			}
		})
	}
}

func TestPredictionPassCapAndSeparatePlacements(t *testing.T) {
	snap, reader, c := predictionFixture(t)
	loop, _, _ := newTestLoop(t, snap, &stubPolicy{})
	if _, err := loop.Store.Update([]byte(schedulingProfilesYAML)); err != nil {
		t.Fatal(err)
	}
	candidates := make([]policy.Candidate, 10)
	for i := range candidates {
		candidates[i] = gateSchedulingCandidate(snap, loop.Store.Get(), c)
	}
	calls := 0
	stage := &PredictionStage{Reader: reader, Now: loop.Now, Simulator: simulationFunc(func(_ context.Context, r scheduling.Request) (scheduling.Result, error) {
		calls++
		return feasiblePrediction(r), nil
	})}
	before, _ := json.Marshal(candidates)
	got := stage.Annotate(context.Background(), snap, loop.Store.Get(), candidates)
	if calls != 8 || reader.lists != 10 {
		t.Fatalf("unbounded pass: calls=%d lists=%d", calls, reader.lists)
	}
	for i, c := range got {
		if c.Executable || len(c.HintTargetNodes) != 0 || len(c.PlacementTargetNodes) != 0 {
			t.Fatalf("prediction promoted candidate: %+v", c)
		}
		if i < 8 && (len(c.Scheduling.Placements) != 1 || c.Scheduling.SnapshotID == "" || c.Scheduling.SnapshotTime == nil) {
			t.Fatalf("missing prediction provenance: %+v", c.Scheduling)
		}
		if i >= 8 && c.Scheduling.Reason != "SimulationBudgetExceeded" {
			t.Fatalf("missing cap reason: %+v", c.Scheduling)
		}
	}
	after, _ := json.Marshal(candidates)
	if string(before) != string(after) {
		t.Fatal("stage changed input candidates")
	}
}
