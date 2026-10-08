package replay

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// TestLaggedClientSeparatesTheTwoReaders pins the stale pass: the cached
// client serves the objects as they stood when the snapshot closed, the
// live reader beside it stays current, and a write through the cached
// client reaches the apiserver.
func TestLaggedClientSeparatesTheTwoReaders(t *testing.T) {
	const ready = `
scenario: ready-pod
arrows: [T-empty-create]
initial:
  spec: {replicas: 1, image: registry.example.com/runtime:v1}
  rows: [{index: 0, phase: Ready, runningRevision: current, readySince: 0s}]
  pods: [{index: 0, ready: true, serving: true, routed: true}]
timeline:
  - tick: 1
    events:
      - ctrl.staleRead: {}
`
	ctx := context.Background()
	d := mustDriver(t, ready)
	if !d.usesStaleReads || d.staleCache == nil {
		t.Fatal("a scenario naming ctrl.staleRead snapshots the cluster from the start")
	}
	name := d.podName(PodRef{Index: 0})
	key := client.ObjectKey{Namespace: d.opts.Namespace, Name: name}
	if err := d.patchPod(ctx, name, func(pod *corev1.Pod) { pod.Status.Phase = corev1.PodFailed }); err != nil {
		t.Fatal(err)
	}
	detail, err := applyStaleRead(ctx, d, TimelineEvent{ID: "ctrl.staleRead"})
	if err != nil || detail != "cache=initial" {
		t.Fatalf("stage: (%q, %v)", detail, err)
	}
	cached, live := d.cachedClient(), d.cli
	stale, fresh := &corev1.Pod{}, &corev1.Pod{}
	if err := cached.Get(ctx, key, stale); err != nil {
		t.Fatal(err)
	}
	if err := live.Get(ctx, key, fresh); err != nil {
		t.Fatal(err)
	}
	if stale.Status.Phase != corev1.PodRunning || fresh.Status.Phase != corev1.PodFailed {
		t.Fatalf("the cache must lag the live reader: cached=%s live=%s", stale.Status.Phase, fresh.Status.Phase)
	}
	pods := &corev1.PodList{}
	if err := cached.List(ctx, pods, client.InNamespace(d.opts.Namespace)); err != nil || len(pods.Items) != 1 || pods.Items[0].Status.Phase != corev1.PodRunning {
		t.Fatalf("a cached list serves the snapshot: err=%v pods=%+v", err, pods.Items)
	}
	fresh.Labels["edited"] = "live"
	if err := cached.Update(ctx, fresh); err != nil {
		t.Fatalf("a write through the cached client reaches the apiserver: %v", err)
	}
	after := &corev1.Pod{}
	if err := live.Get(ctx, key, after); err != nil || after.Labels["edited"] != "live" {
		t.Fatalf("the write must land live: err=%v labels=%v", err, after.Labels)
	}
	if err := refreshStaleCacheAndClear(ctx, d); err != nil {
		t.Fatal(err)
	}
	if _, lagged := d.cachedClient().(*laggedClient); lagged {
		t.Fatal("outside a stale pass the cache is the apiserver itself")
	}
	if detail, err := applyStaleRead(ctx, d, TimelineEvent{ID: "ctrl.staleRead"}); err != nil || detail != "cache=end-of-tick-1" {
		t.Fatalf("the detail names the tick the snapshot closed: (%q, %v)", detail, err)
	}
	if err := cached.Get(ctx, key, stale); err != nil || stale.Status.Phase != corev1.PodRunning {
		t.Fatalf("a staged client keeps its own snapshot: %v %s", err, stale.Status.Phase)
	}
	if _, err := applyStaleRead(ctx, d, TimelineEvent{ID: "ctrl.staleRead", Variant: "deep"}); err == nil || !strings.Contains(err.Error(), "no variant") {
		t.Fatalf("the event takes no variant: %v", err)
	}
}

func refreshStaleCacheAndClear(ctx context.Context, d *driver) error {
	d.stalePass = false
	return d.refreshStaleCache(ctx, 1)
}

// TestScenarioWithoutStaleReadsTakesNoSnapshot keeps the cost where the
// capability is used: a scenario that never stages a stale read never
// copies the cluster.
func TestScenarioWithoutStaleReadsTakesNoSnapshot(t *testing.T) {
	d := mustDriver(t, minimalScenario)
	if d.usesStaleReads || d.staleCache != nil {
		t.Fatal("no stale read staged, no snapshot taken")
	}
	if _, err := applyStaleRead(context.Background(), d, TimelineEvent{ID: "ctrl.staleRead"}); err == nil {
		t.Fatal("a stale read with no snapshot behind it fails the run")
	}
}
