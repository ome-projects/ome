package snapshot

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
)

var (
	buildNow    = time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)
	pendingAt   = metav1.NewTime(buildNow.Add(-20 * time.Minute))
	completedAt = metav1.NewTime(buildNow.Add(-2 * time.Hour))
)

func strPtr(s string) *string { return &s }

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

// testPool is the GFD product label all test nodes carry, and therefore the
// hardware pool they resolve to.
const testPool = "NVIDIA-H100-80GB-HBM3"

func gpuNode(name string, gpus string, mutate func(*corev1.Node)) *corev1.Node {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{
			"nvidia.com/gpu.product": testPool,
		}},
		Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
			"nvidia.com/gpu":   resource.MustParse(gpus),
			corev1.ResourceCPU: resource.MustParse("64"),
		}},
	}
	if mutate != nil {
		mutate(node)
	}
	return node
}

func omePod(namespace, name, node, isvc string, component string, gpus int64, ready bool) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: isvc,
				constants.OMEComponentLabel:           component,
			},
		},
		Spec:   corev1.PodSpec{NodeName: node},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	if gpus > 0 {
		pod.Spec.Containers = []corev1.Container{{
			Name: "main",
			Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{
				"nvidia.com/gpu": *resource.NewQuantity(gpus, resource.DecimalSI),
			}},
		}}
	} else {
		pod.Spec.Containers = []corev1.Container{{Name: "main"}}
	}
	start := metav1.NewTime(buildNow.Add(-24 * time.Hour))
	pod.Status.StartTime = &start
	if ready {
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	}
	return pod
}

func buildTestSnapshot(t *testing.T) *ClusterSnapshot {
	t.Helper()

	requestPayloadBytes, err := json.Marshal(audit.MigrationRequest{
		SchemaVersion: audit.SchemaV1,
		Component:     "engine",
		Instance:      0,
		FromNode:      "node1",
		RequestedAt:   buildNow.Add(-time.Minute).Format(time.RFC3339),
		RequestedBy:   "alfred-controller",
	})
	if err != nil {
		t.Fatal(err)
	}
	requestPayload := string(requestPayloadBytes)
	// ackedPayload shares its UUID ("done-1") with a terminal history entry
	// below — the ack-race window where the executor has recorded the
	// terminal outcome but not yet cleared the annotation.
	ackedPayloadBytes, err := json.Marshal(audit.MigrationRequest{
		SchemaVersion: audit.SchemaV1,
		Component:     "engine",
		Instance:      0,
		FromNode:      "node2",
		RequestedAt:   buildNow.Add(-3 * time.Hour).Format(time.RFC3339),
	})
	if err != nil {
		t.Fatal(err)
	}
	ackedPayload := string(ackedPayloadBytes)

	llamaLabel := constants.GetClusterBaseModelLabel("llama")

	node1 := gpuNode("node1", "8", func(n *corev1.Node) {
		n.Annotations = map[string]string{"cluster-autoscaler.kubernetes.io/scale-down-disabled": "true"}
		n.Labels[llamaLabel] = "Ready"
		n.Labels["topology.kubernetes.io/zone"] = "z1"
	})
	node2 := gpuNode("node2", "8", func(n *corev1.Node) {
		n.Labels[llamaLabel] = "Ready"
		n.Status.Conditions = []corev1.NodeCondition{{Type: "GpuUnhealthy", Status: corev1.ConditionTrue}}
	})
	node3 := gpuNode("node3", "8", func(n *corev1.Node) {
		n.Spec.Unschedulable = true
		n.Spec.Taints = []corev1.Taint{{Key: "ToBeDeletedByClusterAutoscaler", Value: "123", Effect: corev1.TaintEffectNoSchedule}}
		n.Labels["node.kubernetes.io/preemptible"] = "true"
		n.Labels[llamaLabel] = "Updating" // not Ready
	})

	engineA1 := omePod("prod", "svc-a-engine-1", "node1", "svc-a", "engine", 2, true)
	// Requests-only pod exercises the limits→requests fallback.
	engineA2 := omePod("prod", "svc-a-engine-2", "node2", "svc-a", "engine", 0, false)
	engineA2.Spec.Containers = []corev1.Container{{
		Name: "main",
		Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
			"nvidia.com/gpu": resource.MustParse("1"),
		}},
	}}
	routerA := omePod("prod", "svc-a-router-1", "node1", "svc-a", "router", 0, true)

	notebook := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team", Name: "notebook-1"},
		Spec: corev1.PodSpec{NodeName: "node1", Containers: []corev1.Container{{
			Name: "nb",
			Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{
				"nvidia.com/gpu": resource.MustParse("3"),
			}},
		}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}

	deletionTime := metav1.NewTime(buildNow.Add(-time.Minute))
	terminating := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         "team",
			Name:              "batch-1",
			DeletionTimestamp: &deletionTime,
			Finalizers:        []string{"test/finalizer"},
		},
		Spec: corev1.PodSpec{NodeName: "node2", Containers: []corev1.Container{{
			Name: "job",
			Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{
				"nvidia.com/gpu": resource.MustParse("2"),
			}},
		}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}

	pending := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "prod",
			Name:      "svc-a-engine-pending",
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: "svc-a",
				constants.OMEComponentLabel:           "engine",
			},
			CreationTimestamp: metav1.NewTime(buildNow.Add(-30 * time.Minute)),
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "main",
			Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{
				"nvidia.com/gpu": resource.MustParse("8"),
			}},
		}}},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			Conditions: []corev1.PodCondition{{
				Type: corev1.PodScheduled, Status: corev1.ConditionFalse,
				Reason: corev1.PodReasonUnschedulable, LastTransitionTime: pendingAt,
			}},
		},
	}

	isvcA := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "prod",
			Name:      "svc-a",
			Annotations: map[string]string{
				constants.AlfredMovableAnnotationKey:         "false",
				constants.AlfredPriorityAnnotationKey:        "0.2",
				constants.AlfredCooldownMinutesAnnotationKey: "60",
				constants.AlfredTenantGroupAnnotationKey:     "team-alpha",
				migrationAnnotationKey("req-1"):              requestPayload,
				migrationAnnotationKey("done-1"):             ackedPayload,
				migrationAnnotationKey("bad-1"):              "{not-json",
			},
		},
		Spec: v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{},
			Model:  &v1beta1.ModelRef{Name: "llama", Kind: strPtr("ClusterBaseModel")},
		},
		Status: v1beta1.InferenceServiceStatus{
			MigrationHistory: []v1beta1.MigrationHistoryEntry{
				{
					ID: "done-1", Component: v1beta1.EngineComponent, Mode: v1beta1.MigrationModeSurge,
					Phase: v1beta1.MigrationPhaseCompleted, RequestedAt: metav1.NewTime(buildNow.Add(-3 * time.Hour)),
					CompletedAt: &completedAt,
				},
				{
					ID: "hist-1", Component: v1beta1.EngineComponent, Mode: v1beta1.MigrationModeSurge,
					Phase: v1beta1.MigrationPhaseSurgePending, RequestedAt: metav1.NewTime(buildNow.Add(-5 * time.Minute)),
				},
			},
		},
	}

	isvcPVC := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "svc-pvc"},
		Spec: v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{},
			Model:  &v1beta1.ModelRef{Name: "pvc-model", Kind: strPtr("BaseModel")},
		},
	}

	llama := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "llama"},
		Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{
			StorageUri: strPtr("oci://n/ns/b/models/o/llama"),
		}},
	}
	pvcModel := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "pvc-model"},
		Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{
			StorageUri: strPtr("pvc://weights/models/llama"),
		}},
	}
	weightsPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "weights"},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			VolumeName:  "pv-weights",
		},
	}
	weightsPV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-weights"},
		Spec: corev1.PersistentVolumeSpec{
			NodeAffinity: &corev1.VolumeNodeAffinity{Required: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key: "topology.kubernetes.io/zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"z1"},
					}},
				}},
			}},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(
			node1, node2, node3,
			engineA1, engineA2, routerA, notebook, terminating, pending,
			isvcA, isvcPVC, llama, pvcModel, weightsPVC, weightsPV,
		).
		Build()

	snap, err := Build(context.Background(), client.Reader(fakeClient), Options{
		PreemptibleLabels: []string{"node.kubernetes.io/preemptible"},
		OMENativeExecutor: OMENativeExecutorState{
			Available: true, WireVersion: "v2", RenewTime: buildNow.Add(-time.Minute), Reason: "healthy",
		},
		Now: func() time.Time { return buildNow },
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return snap
}

func TestBuildNodeAccounting(t *testing.T) {
	snap := buildTestSnapshot(t)

	node1 := snap.Nodes["node1"]
	if node1 == nil {
		t.Fatal("node1 missing")
	}
	if node1.TotalGPUs != 8 || node1.AllocatedGPUs != 5 || node1.FreeGPUs != 3 {
		t.Fatalf("node1 accounting: total=%d allocated=%d free=%d, want 8/5/3", node1.TotalGPUs, node1.AllocatedGPUs, node1.FreeGPUs)
	}
	if !node1.ScaleDownDisabled || node1.Cordoned || node1.Unhealthy {
		t.Fatalf("node1 flags: %+v", node1)
	}
	if len(node1.OMEPods) != 1 || len(node1.OtherOccupants) != 1 {
		t.Fatalf("node1 occupants: ome=%d other=%d, want 1/1", len(node1.OMEPods), len(node1.OtherOccupants))
	}
	if node1.GPUPool != testPool || node1.GPUResource != "nvidia.com/gpu" {
		t.Fatalf("node1 pool/resource: %q/%q", node1.GPUPool, node1.GPUResource)
	}

	node2 := snap.Nodes["node2"]
	if node2.AllocatedGPUs != 3 || node2.FreeGPUs != 5 || node2.TerminatingGPUs != 2 {
		t.Fatalf("node2 accounting: allocated=%d free=%d terminating=%d, want 3/5/2", node2.AllocatedGPUs, node2.FreeGPUs, node2.TerminatingGPUs)
	}
	if !node2.Unhealthy || len(node2.UnhealthyConditions) != 1 || node2.UnhealthyConditions[0] != "GpuUnhealthy" {
		t.Fatalf("node2 health: %+v", node2)
	}

	node3 := snap.Nodes["node3"]
	if !node3.Cordoned || !node3.ScaleDownMarked || !node3.Preemptible {
		t.Fatalf("node3 flags: %+v", node3)
	}
}

func TestBuildWorkloadsAndMigrationState(t *testing.T) {
	snap := buildTestSnapshot(t)

	svcA := snap.Workloads[types.NamespacedName{Namespace: "prod", Name: "svc-a"}]
	if svcA == nil {
		t.Fatal("svc-a missing")
	}
	if svcA.Movable {
		t.Fatal("svc-a should be movable=false from annotation")
	}
	if svcA.Priority != 0.2 || svcA.TenantGroup != "team-alpha" {
		t.Fatalf("svc-a overrides: priority=%v tenant=%q", svcA.Priority, svcA.TenantGroup)
	}
	if svcA.CooldownOverride == nil || *svcA.CooldownOverride != time.Hour {
		t.Fatalf("svc-a cooldown override: %v", svcA.CooldownOverride)
	}

	engine := svcA.Components[v1beta1.EngineComponent]
	if engine == nil || engine.DeploymentMode != constants.RawDeployment {
		t.Fatalf("engine component: %+v", engine)
	}
	if len(engine.Instances) != 2 {
		t.Fatalf("engine instances: %d, want 2 (one per RawDeployment pod)", len(engine.Instances))
	}
	if !engine.ObservationValid || !engine.Instances[0].ObservationValid || !engine.Instances[1].ObservationValid {
		t.Fatalf("Raw live-Pod observations must be structurally valid: %+v", engine)
	}
	if !snap.OMENativeExecutor.Available || snap.OMENativeExecutor.WireVersion != "v2" ||
		!snap.OMENativeExecutor.RenewTime.Equal(buildNow.Add(-time.Minute)) {
		t.Fatalf("structured executor state = %+v", snap.OMENativeExecutor)
	}
	if engine.Instances[0].Pods[0].Name != "svc-a-engine-1" || engine.Instances[0].TotalGPUs != 2 || engine.Instances[0].ReadyPods != 1 {
		t.Fatalf("instance 0: %+v", engine.Instances[0])
	}
	if engine.Instances[1].Pods[0].GPUs != 1 || engine.Instances[1].ReadyPods != 0 {
		t.Fatalf("instance 1 (requests fallback, not ready): %+v", engine.Instances[1])
	}

	// Router pods exist without a router spec: component still appears.
	router := svcA.Components[v1beta1.RouterComponent]
	if router == nil || len(router.Instances) != 1 || router.Instances[0].TotalGPUs != 0 {
		t.Fatalf("router component from pods only: %+v", router)
	}

	if svcA.LastMigration != nil {
		t.Fatalf("LastMigration = %v, want nil without authoritative IR status", svcA.LastMigration)
	}
	// ISVC migrationHistory is ignored. Both valid annotations remain active
	// until an authoritative IR status row accepts or terminates them.
	if len(svcA.ActiveMigrations) != 2 {
		t.Fatalf("ActiveMigrations: %+v, want done-1 + req-1 annotations", svcA.ActiveMigrations)
	}
	// A corrupt request annotation must stay visible, not make the
	// workload look clean.
	if reason := svcA.MalformedRequests["bad-1"]; reason == "" {
		t.Fatalf("malformed request bad-1 not surfaced: %+v", svcA.MalformedRequests)
	}
	if svcA.MigrationStateValid || svcA.MigrationStateReason != migrationStateReasonRequestInvalid {
		t.Fatalf("malformed request migration validity = %t/%q", svcA.MigrationStateValid, svcA.MigrationStateReason)
	}
	if svcA.ActiveMigrations[0].UUID != "done-1" || svcA.ActiveMigrations[0].FromNode != "node2" {
		t.Fatalf("ActiveMigrations[0]: %+v", svcA.ActiveMigrations[0])
	}
	if svcA.ActiveMigrations[1].UUID != "req-1" || svcA.ActiveMigrations[1].FromNode != "node1" || svcA.ActiveMigrations[1].Instance != 0 {
		t.Fatalf("ActiveMigrations[1]: %+v", svcA.ActiveMigrations[1])
	}
	if svcA.ActiveMigrations[1].RequestedBy != "alfred-controller" {
		t.Fatalf("ActiveMigrations[1].RequestedBy = %q", svcA.ActiveMigrations[1].RequestedBy)
	}
}

func TestBuildPendingPods(t *testing.T) {
	snap := buildTestSnapshot(t)

	if len(snap.PendingPods) != 1 {
		t.Fatalf("pending pods: %+v", snap.PendingPods)
	}
	p := snap.PendingPods[0]
	if p.GPUsNeeded != 8 || p.Virtual {
		t.Fatalf("pending pod: %+v", p)
	}
	if p.ISVC != (types.NamespacedName{Namespace: "prod", Name: "svc-a"}) {
		t.Fatalf("pending ISVC attribution: %+v", p.ISVC)
	}
	if !p.PendingSince.Equal(pendingAt.Time) {
		t.Fatalf("PendingSince = %v, want PodScheduled transition %v", p.PendingSince, pendingAt.Time)
	}
	// Single-pool cluster: demand is attributed to that pool.
	if p.GPUPool != testPool {
		t.Fatalf("pool attribution: %q, want %q", p.GPUPool, testPool)
	}
}

func TestBuildModelAvailability(t *testing.T) {
	snap := buildTestSnapshot(t)

	llama := snap.Models[ModelKey{Kind: "ClusterBaseModel", Name: "llama"}]
	if llama == nil {
		t.Fatal("llama availability missing")
	}
	if llama.Backend != BackendPerNode || llama.ResolveError != "" {
		t.Fatalf("llama: %+v", llama)
	}
	// node3's label value is Updating, so only node1/node2 are Ready.
	if len(llama.NodesReady) != 2 || llama.NodesReady[0] != "node1" || llama.NodesReady[1] != "node2" {
		t.Fatalf("llama NodesReady: %v", llama.NodesReady)
	}

	pvcModel := snap.Models[ModelKey{Kind: "BaseModel", Namespace: "prod", Name: "pvc-model"}]
	if pvcModel == nil {
		t.Fatal("pvc-model availability missing")
	}
	if pvcModel.Backend != BackendPVC || !pvcModel.VolumePinned {
		t.Fatalf("pvc-model should be PVC-backed and RWO-pinned: %+v", pvcModel)
	}
	if len(pvcModel.PVCAccessModes) != 1 || pvcModel.PVCAccessModes[0] != string(corev1.ReadWriteOnce) {
		t.Fatalf("pvc-model access modes: %v", pvcModel.PVCAccessModes)
	}
	// Zone z1 affinity reaches only node1.
	if len(pvcModel.PVCTopologyNodes) != 1 || pvcModel.PVCTopologyNodes[0] != "node1" {
		t.Fatalf("pvc-model topology: %v", pvcModel.PVCTopologyNodes)
	}
}

func TestSnapshotHelpers(t *testing.T) {
	snap := buildTestSnapshot(t)

	pools := snap.GPUPools()
	if len(pools) != 1 || pools[0] != testPool {
		t.Fatalf("GPUPools: %v", pools)
	}
	if got := len(snap.PoolNodes(testPool)); got != 3 {
		t.Fatalf("PoolNodes(%s): %d, want 3", testPool, got)
	}

	derived := snap.WithVirtualPending([]PendingPod{{Name: "virtual-1", GPUsNeeded: 8, Virtual: true}})
	if len(derived.PendingPods) != len(snap.PendingPods)+1 {
		t.Fatal("WithVirtualPending should append")
	}
	if len(snap.PendingPods) != 1 {
		t.Fatal("WithVirtualPending must not mutate the receiver")
	}
	if snap.WithVirtualPending(nil) != snap {
		t.Fatal("WithVirtualPending(nil) should return the receiver")
	}
}
