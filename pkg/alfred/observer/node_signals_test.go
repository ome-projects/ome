package observer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/metrics"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
)

func TestRefreshReloadsHealthAndMaintenance(t *testing.T) {
	loop, m := newTestLoop(t)
	c := loop.Reader.(client.Client)
	node := &corev1.Node{}
	ctx := context.Background()
	if err := c.Get(ctx, types.NamespacedName{Name: "node1"}, node); err != nil {
		t.Fatal(err)
	}
	node.Labels["ops.example/patch"] = "planned"
	if err := c.Update(ctx, node); err != nil {
		t.Fatal(err)
	}
	node.Status.Conditions = []corev1.NodeCondition{{Type: "GpuUnhealthy", Status: corev1.ConditionFalse, LastTransitionTime: metav1.NewTime(loopNow.Add(-10 * time.Minute))}}
	if err := c.Status().Update(ctx, node); err != nil {
		t.Fatal(err)
	}
	setConfig := func(raw string) {
		t.Helper()
		if _, err := loop.Store.Update([]byte("schemaVersion: 1\npolicies:\n  nodeHealth:\n" + raw)); err != nil {
			t.Fatal(err)
		}
	}
	setConfig("    enabled: false\n    nodeSuspicionWindowMinutes: 5\n    maintenance:\n      triggers:\n      - name: patch\n        label: {key: ops.example/patch}\n")
	if err := loop.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	got := loop.Latest().Nodes["node1"]
	if got.Health.State != snapshot.NodeHealthClear || !got.Maintenance.Requested {
		t.Fatalf("health and maintenance=%+v %+v", got.Health, got.Maintenance)
	}
	if got := promtestutil.ToFloat64(m.SurgeHeadroomGPUs.WithLabelValues("NVIDIA-H100-80GB-HBM3")); got != 0 {
		t.Fatalf("maintenance headroom=%v", got)
	}
	setConfig("    enabled: false\n    nodeSuspicionWindowMinutes: 15\n    maintenance: {triggers: []}\n")
	if err := loop.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	got = loop.Latest().Nodes["node1"]
	if got.Health.State != snapshot.NodeHealthSuspect || got.Maintenance.Requested {
		t.Fatalf("reloaded health and maintenance=%+v %+v", got.Health, got.Maintenance)
	}
	setConfig("    enabled: false\n    nodeSuspicionWindowMinutes: 5\n")
	if err := loop.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if got := promtestutil.ToFloat64(m.SurgeHeadroomGPUs.WithLabelValues("NVIDIA-H100-80GB-HBM3")); got != 5 {
		t.Fatalf("clear headroom=%v", got)
	}
}

func TestRefreshSerializesBuildAndPublication(t *testing.T) {
	loop, _ := newTestLoop(t)
	base := loop.Reader.(client.WithWatch)
	enteredBuild := make(chan struct{}, 2)
	firstScoring := make(chan struct{})
	releaseScoring := make(chan struct{})
	var builds, scoring atomic.Int32
	loop.Reader = interceptor.NewClient(base, interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
		if _, ok := list.(*corev1.NodeList); ok {
			builds.Add(1)
			enteredBuild <- struct{}{}
		}
		return c.List(ctx, list, opts...)
	}})
	loop.Scorer = func(*snapshot.ClusterSnapshot, *config.Config, *metrics.Metrics) {
		if scoring.Add(1) == 1 {
			close(firstScoring)
			<-releaseScoring
		}
	}
	defer close(releaseScoring)
	done := make(chan error, 2)
	go func() { done <- loop.Refresh(context.Background()) }()
	<-enteredBuild
	select {
	case <-firstScoring:
	case <-time.After(5 * time.Second):
		t.Fatal("first refresh did not reach publication")
	}
	go func() { done <- loop.RunOnce(context.Background()) }()
	select {
	case <-enteredBuild:
		t.Fatal("second build overlapped first publication")
	case <-time.After(50 * time.Millisecond):
	}
	// Release publication, then verify the waiting refresh progresses.
	releaseScoring <- struct{}{}
	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("refresh did not complete")
		}
	}
	if builds.Load() != 2 || scoring.Load() != 2 {
		t.Fatalf("builds=%d scoring=%d", builds.Load(), scoring.Load())
	}
}
