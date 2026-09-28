package acceleratorquota

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/quota/tree"
)

func TestCapacityReportRefreshBoundary(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	stamp := func(offset time.Duration) *metav1.Time { t := metav1.NewTime(now.Add(offset)); return &t }
	for _, tt := range []struct {
		name     string
		observed *metav1.Time
		interval time.Duration
		want     bool
	}{
		{name: "fresh", observed: stamp(-time.Second), interval: time.Minute},
		{name: "due", observed: stamp(-time.Minute), interval: time.Minute, want: true},
		{name: "expired", observed: stamp(-2 * time.Minute), interval: time.Minute, want: true},
		{name: "missing timestamp", interval: time.Minute, want: true},
		{name: "future timestamp", observed: stamp(time.Second), interval: time.Minute, want: true},
		{name: "disabled", observed: stamp(-time.Hour)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			reports := []v1beta1.AcceleratorCapacityStatus{{ObservedAt: tt.observed}}
			require.Equal(t, tt.want, capacityReportDue(reports, now, tt.interval))
		})
	}
	require.False(t, capacityReportDue(nil, now, time.Minute))
}

func TestCapacityHeartbeatResamplesWithoutStatusLoop(t *testing.T) {
	ctx := context.Background()
	scheme := capacityScheme(t)
	var failNodes bool
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1beta1.AcceleratorQuota{}).
		WithObjects(
			resourceFlavor("example-gpu", map[string]string{"accelerator": "example-gpu"}),
			workerNode("node-a", withLabels(map[string]string{"accelerator": "example-gpu"}), withAllocatable(map[string]string{"example.com/gpu": "8"})),
		).WithInterceptorFuncs(interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, nodes := list.(*corev1.NodeList); nodes && failNodes {
				return errors.New("node read unavailable")
			}
			return c.List(ctx, list, opts...)
		},
	}).Build()
	r := &Reconciler{
		Client: c, APIReader: c, Scheme: scheme, Log: logf.Log,
		Options: tree.Options{RootName: rootName}, ResyncInterval: time.Hour,
		Capacity: CapacityOptions{Resources: []string{"example.com/gpu"}, ReportInterval: time.Minute},
	}
	_, err := r.Reconcile(ctx, ctrl.Request{})
	require.NoError(t, err)
	result, err := r.Reconcile(ctx, ctrl.Request{})
	require.NoError(t, err)
	require.Equal(t, time.Minute, result.RequeueAfter, "idle hardware must be sampled on the report cadence")
	var root v1beta1.AcceleratorQuota
	require.NoError(t, c.Get(ctx, client.ObjectKey{Name: rootName}, &root))
	require.Len(t, root.Status.Capacity, 1)
	original := root.Status.Capacity[0].DeepCopy()
	root.Status.Capacity[0].ObservedAt = &metav1.Time{Time: time.Now().Add(-2 * time.Minute)}
	require.NoError(t, c.Status().Update(ctx, &root))
	stale := root.DeepCopy()

	failNodes = true
	require.ErrorContains(t, r.reconcileCapacity(ctx, &root), "node read unavailable")
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(&root), &root))
	require.Equal(t, stale.Status.Capacity, root.Status.Capacity, "a failed sample cannot extend freshness")
	failNodes = false
	start := time.Now().Truncate(time.Second)
	require.NoError(t, r.reconcileCapacity(ctx, &root))
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(&root), &root))
	require.False(t, root.Status.Capacity[0].ObservedAt.Time.Before(start))
	require.Zero(t, original.Allocatable.Cmp(root.Status.Capacity[0].Allocatable))
	require.Zero(t, original.HighWaterMark.Cmp(root.Status.Capacity[0].HighWaterMark))
	fresh := root.DeepCopy()
	require.NoError(t, r.reconcileCapacity(ctx, &root))
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(&root), &root))
	require.Equal(t, fresh.ResourceVersion, root.ResourceVersion, "a heartbeat watch event must not cause another status write")
}

func TestCapacityHeartbeatCadenceIsConfigured(t *testing.T) {
	for _, tt := range []struct {
		name        string
		resync      time.Duration
		report      time.Duration
		capacityOff bool
		want        time.Duration
	}{
		{name: "report only", report: time.Minute, want: time.Minute},
		{name: "earlier resync", resync: time.Second, report: time.Minute, want: time.Second},
		{name: "earlier report", resync: time.Hour, report: time.Minute, want: time.Minute},
		{name: "no report", resync: time.Hour, want: time.Hour},
		{name: "capacity disabled", resync: time.Hour, report: time.Minute, capacityOff: true, want: time.Hour},
		{name: "event driven"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := &Reconciler{ResyncInterval: tt.resync, Capacity: CapacityOptions{ReportInterval: tt.report}}
			if !tt.capacityOff {
				r.Capacity.Resources = []string{"example.com/gpu"}
			}
			require.Equal(t, tt.want, r.nextResync())
		})
	}
	r := &Reconciler{Capacity: CapacityOptions{ReportInterval: -time.Second}}
	require.ErrorContains(t, r.SetupWithManager(nil), "capacity report interval must be nonnegative")
}
