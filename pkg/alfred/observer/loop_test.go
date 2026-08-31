package observer

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/metrics"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

var loopNow = time.Date(2026, 1, 3, 12, 0, 0, 0, time.UTC)

func newTestLoop(t *testing.T) (*Loop, *metrics.Metrics) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: map[string]string{
			"nvidia.com/gpu.product": "NVIDIA-H100-80GB-HBM3",
		}},
		Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
			"nvidia.com/gpu": resource.MustParse("8"),
		}},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team", Name: "occupant"},
		Spec: corev1.PodSpec{NodeName: "node1", Containers: []corev1.Container{{
			Name: "main",
			Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{
				"nvidia.com/gpu": resource.MustParse("3"),
			}},
		}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	pending := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team", Name: "pending"},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "main",
			Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{
				"nvidia.com/gpu": resource.MustParse("8"),
			}},
		}}},
		Status: corev1.PodStatus{Phase: corev1.PodPending},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(node, pod, pending).Build()

	m := metrics.New(prometheus.NewRegistry())
	loop := &Loop{
		Reader:  fakeClient,
		Store:   config.NewStore(),
		Metrics: m,
		Log:     logr.Discard(),
		Now:     func() time.Time { return loopNow },
	}
	return loop, m
}

func TestLoopRunsOnEveryReplica(t *testing.T) {
	loop, _ := newTestLoop(t)
	if loop.NeedLeaderElection() {
		t.Fatal("observation loop must run on every replica")
	}
}

func TestStartRunsInitialPassAndStopsOnCancel(t *testing.T) {
	loop, _ := newTestLoop(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- loop.Start(ctx) }()

	deadline := time.After(5 * time.Second)
	for loop.Latest() == nil {
		select {
		case <-deadline:
			t.Fatal("Start never completed its initial pass")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start returned error on shutdown: %v", err)
	}
}

func TestRunOncePublishesSnapshotGauges(t *testing.T) {
	loop, m := newTestLoop(t)

	if loop.Latest() != nil {
		t.Fatal("Latest should be nil before the first pass")
	}
	if err := loop.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	snap := loop.Latest()
	if snap == nil {
		t.Fatal("Latest should be set after a pass")
	}

	get := func(vec *prometheus.GaugeVec, labels ...string) float64 {
		return promtestutil.ToFloat64(vec.WithLabelValues(labels...))
	}
	if got := get(m.GPUCapacity, "node1", "total"); got != 8 {
		t.Fatalf("gpu_capacity total = %v, want 8", got)
	}
	if got := get(m.GPUCapacity, "node1", "allocated"); got != 3 {
		t.Fatalf("gpu_capacity allocated = %v, want 3", got)
	}
	if got := get(m.GPUCapacity, "node1", "free"); got != 5 {
		t.Fatalf("gpu_capacity free = %v, want 5", got)
	}
	if got := promtestutil.ToFloat64(m.PendingPodCount); got != 1 {
		t.Fatalf("pending_pod_count = %v, want 1", got)
	}
	if got := get(m.PendingPodGPURequirements, "8"); got != 1 {
		t.Fatalf("pending_pod_gpu_requirements{8} = %v, want 1", got)
	}
	if got := get(m.SurgeHeadroomGPUs, "NVIDIA-H100-80GB-HBM3"); got != 5 {
		t.Fatalf("surge_headroom_gpus{pool} = %v, want 5 (largest free block)", got)
	}
}

func TestRunOnceInvokesScorerHook(t *testing.T) {
	loop, _ := newTestLoop(t)

	var scored *snapshot.ClusterSnapshot
	loop.Scorer = func(s *snapshot.ClusterSnapshot, cfg *config.Config, m *metrics.Metrics) {
		scored = s
		m.ClusterFragmentationScore.Set(0.42)
	}
	if err := loop.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if scored == nil || scored != loop.Latest() {
		t.Fatal("scorer must receive the freshly built snapshot")
	}
	if got := promtestutil.ToFloat64(loop.Metrics.ClusterFragmentationScore); got != 0.42 {
		t.Fatalf("scorer gauge write lost: %v", got)
	}
}

func TestRunOnceRecordsOMENativeExecutorState(t *testing.T) {
	tests := []struct {
		name     string
		supplier func(context.Context) snapshot.OMENativeExecutorState
		want     snapshot.OMENativeExecutorState
		gauge    float64
	}{
		{name: "nil supplier", want: snapshot.OMENativeExecutorState{}, gauge: 1},
		{
			name: "explicit unavailable",
			supplier: func(context.Context) snapshot.OMENativeExecutorState {
				return snapshot.OMENativeExecutorState{Reason: "lease-stale"}
			},
			want:  snapshot.OMENativeExecutorState{Reason: "lease-stale"},
			gauge: 1,
		},
		{
			name: "explicit available",
			supplier: func(context.Context) snapshot.OMENativeExecutorState {
				return snapshot.OMENativeExecutorState{
					Available:   true,
					WireVersion: "v2",
					RenewTime:   loopNow.Add(-time.Minute),
					Reason:      "ready",
				}
			},
			want: snapshot.OMENativeExecutorState{
				Available:   true,
				WireVersion: "v2",
				RenewTime:   loopNow.Add(-time.Minute),
				Reason:      "ready",
			},
			gauge: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			loop, m := newTestLoop(t)
			loop.OMENativeExecutor = tc.supplier
			if err := loop.RunOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			if got := loop.Latest().OMENativeExecutor; got != tc.want {
				t.Fatalf("executor state = %+v, want %+v", got, tc.want)
			}
			if got := promtestutil.ToFloat64(m.OMENativeUnavailable); got != tc.gauge {
				t.Fatalf("omenative_unavailable = %v, want %v", got, tc.gauge)
			}
		})
	}
}
