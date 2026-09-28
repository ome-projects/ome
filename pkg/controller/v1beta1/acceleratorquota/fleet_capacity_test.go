package acceleratorquota

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
)

var capacityTime = metav1.NewTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

var compareQuantities = cmp.Comparer(func(a, b resource.Quantity) bool { return a.Cmp(b) == 0 })

type identityFleet struct {
	*fakeFleet
	uids map[string]types.UID
}

func (f *identityFleet) ClientForUID(name string, uid types.UID) (workloadcluster.SelectivelyCachingClient, bool) {
	if uid == "" || f.uids[name] != uid {
		return nil, false
	}
	return f.ClientFor(name)
}

func capacityMemberRoot() *v1beta1.AcceleratorQuota {
	root := cohort(rootName, "")
	root.UID = "report-a"
	root.ResourceVersion = "10"
	root.Status.Capacity = []v1beta1.AcceleratorCapacityStatus{{
		ResourceName: "nvidia.com/gpu", ResourceFlavor: "gpu-a",
		Allocatable: resource.MustParse("8"), HighWaterMark: resource.MustParse("16"),
		ObservedAt: capacityTime.DeepCopy(),
		Attribution: &v1beta1.AcceleratorCapacityAttribution{
			FlavorUID: "flavor-a", FlavorSetHash: "mapping-a", Complete: true,
			NodeLabels: map[string]string{"accelerator": "gpu-a"},
		},
	}}
	return root
}

func capacityRow() v1beta1.AcceleratorClusterCapacityStatus {
	return v1beta1.AcceleratorClusterCapacityStatus{
		Cluster: "member-a", ClusterUID: "registration-a", ReportUID: "report-a",
		ReportResourceVersion: "10", ReportAvailable: true,
		Allocatable: resource.MustParse("8"), HighWaterMark: resource.MustParse("16"),
		ObservedAt: capacityTime.DeepCopy(),
		Attribution: &v1beta1.AcceleratorCapacityAttribution{
			FlavorUID: "flavor-a", FlavorSetHash: "mapping-a", Complete: true,
			NodeLabels: map[string]string{"accelerator": "gpu-a"},
		},
	}
}

func capacityCell(rows ...v1beta1.AcceleratorClusterCapacityStatus) []v1beta1.AcceleratorCapacityStatus {
	return []v1beta1.AcceleratorCapacityStatus{{
		ResourceName: "nvidia.com/gpu", ResourceFlavor: "gpu-a",
		Allocatable: resource.MustParse("8"), HighWaterMark: resource.MustParse("16"),
		ObservedAt: capacityTime.DeepCopy(), PerCluster: rows,
	}}
}

func capacityRegistration(name string, uid types.UID) *v1beta1.WorkloadCluster {
	return &v1beta1.WorkloadCluster{ObjectMeta: metav1.ObjectMeta{Name: name, UID: uid}}
}

func TestCollectFleetCapacity(t *testing.T) {
	fresh := capacityCell(capacityRow())
	unavailable := capacityCell(capacityRow())
	unavailable[0].ObservedAt = nil
	unavailable[0].PerCluster[0].ReportAvailable = false
	zero := capacityCell(capacityRow())
	zero[0].Allocatable = resource.MustParse("0")
	zero[0].PerCluster[0].Allocatable = resource.MustParse("0")
	unknown := capacityCell(capacityRow())
	unknown[0].ObservedAt = nil
	unknown[0].PerCluster[0].ObservedAt = nil
	unknown[0].PerCluster[0].Attribution = nil
	incomplete := capacityCell(capacityRow())
	incomplete[0].PerCluster[0].Attribution.Complete = false

	tests := []struct {
		name         string
		root         func(*v1beta1.AcceleratorQuota)
		registration func(*v1beta1.WorkloadCluster)
		retained     []v1beta1.AcceleratorCapacityStatus
		connectedUID types.UID
		want         []v1beta1.AcceleratorCapacityStatus
	}{
		{name: "identified report", connectedUID: "registration-a", want: fresh},
		{name: "explicit zero", connectedUID: "registration-a", want: zero, root: func(q *v1beta1.AcceleratorQuota) { q.Status.Capacity[0].Allocatable = resource.MustParse("0") }},
		{name: "unreported pool is absent", connectedUID: "registration-a", retained: fresh, root: func(q *v1beta1.AcceleratorQuota) { q.Status.Capacity = nil }},
		{name: "missing provenance remains unknown", connectedUID: "registration-a", want: unknown, root: func(q *v1beta1.AcceleratorQuota) {
			q.Status.Capacity[0].Attribution = nil
			q.Status.Capacity[0].ObservedAt = nil
		}},
		{name: "incomplete attribution is preserved", connectedUID: "registration-a", want: incomplete, root: func(q *v1beta1.AcceleratorQuota) { q.Status.Capacity[0].Attribution.Complete = false }},
		{name: "unreachable retains original time", retained: fresh, want: unavailable},
		{name: "unreachable first collection stays absent"},
		{name: "connection for another UID is refused", connectedUID: "old-registration", retained: fresh, want: unavailable},
		{name: "recreated registration loses old sample", retained: fresh, registration: func(w *v1beta1.WorkloadCluster) { w.UID = "replacement-registration" }},
		{name: "deleting registration loses old sample", retained: fresh, connectedUID: "registration-a", registration: func(w *v1beta1.WorkloadCluster) {
			w.DeletionTimestamp = capacityTime.DeepCopy()
			w.Finalizers = []string{"example.com/cleanup"}
		}},
		{name: "unidentified registration cannot collect", connectedUID: "registration-a", registration: func(w *v1beta1.WorkloadCluster) { w.UID = "" }},
		{name: "unidentified member root retains unavailable sample", connectedUID: "registration-a", retained: fresh, want: unavailable, root: func(q *v1beta1.AcceleratorQuota) { q.UID = "" }},
		{name: "deleting member root retains unavailable sample", connectedUID: "registration-a", retained: fresh, want: unavailable, root: func(q *v1beta1.AcceleratorQuota) {
			q.DeletionTimestamp = capacityTime.DeepCopy()
			q.Finalizers = []string{"example.com/cleanup"}
		}},
		{name: "self registration cannot aggregate itself", connectedUID: "registration-a", root: func(q *v1beta1.AcceleratorQuota) { q.UID = "management-root" }},
		{name: "projected root is refused", connectedUID: "registration-a", root: func(q *v1beta1.AcceleratorQuota) {
			q.Labels = map[string]string{v1beta1.AcceleratorQuotaOriginLabel: "another-plane"}
		}},
		{name: "parented root is refused", connectedUID: "registration-a", root: func(q *v1beta1.AcceleratorQuota) {
			q.Spec.ParentRef = &v1beta1.AcceleratorQuotaParentRef{Name: "parent"}
		}},
		{name: "non cohort root is refused", connectedUID: "registration-a", root: func(q *v1beta1.AcceleratorQuota) { q.Spec.Role = v1beta1.AcceleratorQuotaRoleClusterQueue }},
		{name: "nested aggregate is refused", connectedUID: "registration-a", root: func(q *v1beta1.AcceleratorQuota) {
			q.Status.Capacity[0].PerCluster = []v1beta1.AcceleratorClusterCapacityStatus{capacityRow()}
		}},
		{name: "duplicate pair is refused", connectedUID: "registration-a", root: func(q *v1beta1.AcceleratorQuota) {
			q.Status.Capacity = append(q.Status.Capacity, *q.Status.Capacity[0].DeepCopy())
		}},
		{name: "unnamed resource is refused", connectedUID: "registration-a", root: func(q *v1beta1.AcceleratorQuota) { q.Status.Capacity[0].ResourceName = "" }},
		{name: "unnamed flavor is refused", connectedUID: "registration-a", root: func(q *v1beta1.AcceleratorQuota) { q.Status.Capacity[0].ResourceFlavor = "" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			local := cohort(rootName, "")
			local.UID = "management-root"
			local.Status.Capacity = tt.retained
			registration := capacityRegistration("member-a", "registration-a")
			if tt.registration != nil {
				tt.registration(registration)
			}
			remote := capacityMemberRoot()
			if tt.root != nil {
				tt.root(remote)
			}
			fleet := &identityFleet{fakeFleet: &fakeFleet{members: map[string]workloadcluster.SelectivelyCachingClient{"member-a": member(t, remote)}}, uids: map[string]types.UID{"member-a": tt.connectedUID}}
			r, c := projectingReconciler(t, fleet, local, registration)
			if err := r.reconcileFleetCapacity(context.Background(), local.UID); err != nil {
				t.Fatal(err)
			}
			var got v1beta1.AcceleratorQuota
			if err := c.Get(context.Background(), client.ObjectKey{Name: rootName}, &got); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.want, got.Status.Capacity, compareQuantities); diff != "" {
				t.Fatalf("capacity (-want +got):\n%s", diff)
			}
			version := got.ResourceVersion
			if err := r.reconcileFleetCapacity(context.Background(), local.UID); err != nil {
				t.Fatal(err)
			}
			if err := c.Get(context.Background(), client.ObjectKey{Name: rootName}, &got); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(version, got.ResourceVersion); diff != "" {
				t.Fatalf("unchanged collection wrote status (-want +got):\n%s", diff)
			}
		})
	}
}

func TestCollectFleetCapacityReadFailures(t *testing.T) {
	for _, tt := range []struct {
		name      string
		intercept interceptor.Funcs
	}{
		{name: "report read fails", intercept: interceptor.Funcs{Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return errors.New("read failed")
		}}},
		{name: "metadata read fails", intercept: interceptor.Funcs{Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*metav1.PartialObjectMetadata); ok {
				return errors.New("metadata unavailable")
			}
			return cl.Get(ctx, key, obj, opts...)
		}}},
		{name: "cached report has old UID", intercept: interceptor.Funcs{Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := cl.Get(ctx, key, obj, opts...); err != nil {
				return err
			}
			if _, ok := obj.(*metav1.PartialObjectMetadata); ok {
				obj.SetUID("replacement-root")
			}
			return nil
		}}},
		{name: "cached report has old version", intercept: interceptor.Funcs{Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := cl.Get(ctx, key, obj, opts...); err != nil {
				return err
			}
			if _, ok := obj.(*metav1.PartialObjectMetadata); ok {
				obj.SetResourceVersion("11")
			}
			return nil
		}}},
		{name: "source version is unidentified", intercept: interceptor.Funcs{Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := cl.Get(ctx, key, obj, opts...); err != nil {
				return err
			}
			obj.SetResourceVersion("")
			return nil
		}}},
		{name: "source starts terminating", intercept: interceptor.Funcs{Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := cl.Get(ctx, key, obj, opts...); err != nil {
				return err
			}
			if _, ok := obj.(*metav1.PartialObjectMetadata); ok {
				obj.SetDeletionTimestamp(capacityTime.DeepCopy())
			}
			return nil
		}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			local := cohort(rootName, "")
			local.UID = "management-root"
			local.Status.Capacity = capacityCell(capacityRow())
			remote := workloadcluster.NewNeverCachingClient(interceptor.NewClient(member(t, capacityMemberRoot()), tt.intercept))
			fleet := &identityFleet{fakeFleet: &fakeFleet{members: map[string]workloadcluster.SelectivelyCachingClient{"member-a": remote}}, uids: map[string]types.UID{"member-a": "registration-a"}}
			r, c := projectingReconciler(t, fleet, local, capacityRegistration("member-a", "registration-a"))
			if err := r.reconcileFleetCapacity(context.Background(), local.UID); err != nil {
				t.Fatal(err)
			}
			var got v1beta1.AcceleratorQuota
			if err := c.Get(context.Background(), client.ObjectKey{Name: rootName}, &got); err != nil {
				t.Fatal(err)
			}
			want := capacityCell(capacityRow())
			want[0].ObservedAt = nil
			want[0].PerCluster[0].ReportAvailable = false
			if diff := cmp.Diff(want, got.Status.Capacity, compareQuantities); diff != "" {
				t.Fatalf("retained capacity (-want +got):\n%s", diff)
			}
		})
	}
}

func TestAggregateCapacity(t *testing.T) {
	a := capacityRow()
	b := capacityRow()
	b.Cluster, b.ClusterUID, b.ReportUID = "member-b", "registration-b", "report-b"
	b.Allocatable = resource.MustParse("4")
	b.HighWaterMark = resource.MustParse("4")
	b.ObservedAt = &metav1.Time{Time: capacityTime.Add(-time.Minute)}
	pair := capacityPair{"nvidia.com/gpu", "gpu-a"}
	want := capacityCell(a, b)
	want[0].Allocatable = resource.MustParse("12")
	want[0].HighWaterMark = resource.MustParse("20")
	want[0].ObservedAt = b.ObservedAt.DeepCopy()
	partial := capacityCell(a)
	partial[0].ObservedAt = nil
	multi := capacityCell(a)
	multi = append(multi, *multi[0].DeepCopy(), *multi[0].DeepCopy())
	multi[0].ResourceName = "amd.com/gpu"
	multi[2].ResourceFlavor = "gpu-b"
	for _, tt := range []struct {
		name       string
		reports    map[string]memberCapacity
		registered int
		want       []v1beta1.AcceleratorCapacityStatus
	}{
		{name: "no reports", registered: 2},
		{name: "separate resources and stable flavor order", registered: 1, reports: map[string]memberCapacity{"member-a": {pair: a, {"nvidia.com/gpu", "gpu-b"}: a, {"amd.com/gpu", "gpu-a"}: a}}, want: multi},
		{name: "oldest observation and stable cluster order", registered: 2, reports: map[string]memberCapacity{"member-b": {pair: b}, "member-a": {pair: a}}, want: want},
		{name: "missing member does not claim complete total", registered: 2, reports: map[string]memberCapacity{"member-a": {pair: a}}, want: partial},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := aggregateCapacity(tt.reports, tt.registered)
			if diff := cmp.Diff(tt.want, got, compareQuantities); diff != "" {
				t.Fatalf("aggregate (-want +got):\n%s", diff)
			}
			if len(got) > 0 {
				got[0].PerCluster[0].Attribution.NodeLabels["accelerator"] = "changed"
				if diff := cmp.Diff("gpu-a", a.Attribution.NodeLabels["accelerator"]); diff != "" {
					t.Fatalf("input alias (-want +got):\n%s", diff)
				}
			}
		})
	}
}

func TestFleetCapacityCollectionFencesConcurrentChanges(t *testing.T) {
	for _, tt := range []struct {
		name          string
		mutate        func(context.Context, client.Client, *identityFleet) error
		wantConflict  bool
		wantAvailable bool
		wantRows      bool
	}{
		{name: "registry replaced during read", mutate: func(ctx context.Context, cl client.Client, _ *identityFleet) error {
			w := capacityRegistration("member-a", "registration-a")
			if err := cl.Delete(ctx, w); err != nil {
				return err
			}
			return cl.Create(ctx, capacityRegistration("member-a", "replacement"))
		}},
		{name: "connection disappears during read", wantRows: true, mutate: func(_ context.Context, _ client.Client, f *identityFleet) error {
			delete(f.uids, "member-a")
			return nil
		}},
		{name: "concurrent root writer", wantConflict: true, mutate: func(ctx context.Context, cl client.Client, _ *identityFleet) error {
			var root v1beta1.AcceleratorQuota
			if err := cl.Get(ctx, client.ObjectKey{Name: rootName}, &root); err != nil {
				return err
			}
			root.Status.ObservedGeneration = 42
			return cl.Status().Update(ctx, &root)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			local := cohort(rootName, "")
			local.UID = "management-root"
			fleet := &identityFleet{fakeFleet: &fakeFleet{members: map[string]workloadcluster.SelectivelyCachingClient{}}, uids: map[string]types.UID{"member-a": "registration-a"}}
			r, c := projectingReconciler(t, fleet, local, capacityRegistration("member-a", "registration-a"))
			fleet.members["member-a"] = workloadcluster.NewNeverCachingClient(interceptor.NewClient(member(t, capacityMemberRoot()), interceptor.Funcs{Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if err := cl.Get(ctx, key, obj, opts...); err != nil {
					return err
				}
				if _, ok := obj.(*metav1.PartialObjectMetadata); ok {
					return tt.mutate(ctx, c, fleet)
				}
				return nil
			}}))
			err := r.reconcileFleetCapacity(context.Background(), local.UID)
			if diff := cmp.Diff(tt.wantConflict, apierrors.IsConflict(err)); diff != "" {
				t.Fatalf("conflict (-want +got):\n%s; error: %v", diff, err)
			}
			if err != nil && !tt.wantConflict {
				t.Fatal(err)
			}
			var got v1beta1.AcceleratorQuota
			if err := c.Get(context.Background(), client.ObjectKey{Name: rootName}, &got); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.wantRows, len(got.Status.Capacity) > 0); diff != "" {
				t.Fatalf("rows (-want +got):\n%s", diff)
			}
			if tt.wantRows {
				if diff := cmp.Diff(tt.wantAvailable, got.Status.Capacity[0].PerCluster[0].ReportAvailable); diff != "" {
					t.Fatalf("availability (-want +got):\n%s", diff)
				}
			}
			if tt.wantConflict {
				if diff := cmp.Diff(int64(42), got.Status.ObservedGeneration); diff != "" {
					t.Fatalf("concurrent status (-want +got):\n%s", diff)
				}
			}
		})
	}
}

func TestFleetCapacityReconcileWithUnreachableMember(t *testing.T) {
	local := cohort(rootName, "")
	local.UID = "management-root"
	fleet := &identityFleet{fakeFleet: &fakeFleet{members: map[string]workloadcluster.SelectivelyCachingClient{"member-a": member(t, capacityMemberRoot())}}, uids: map[string]types.UID{"member-a": "registration-a"}}
	r, c := projectingReconciler(t, fleet, local, capacityRegistration("member-a", "registration-a"), capacityRegistration("member-b", "registration-b"))
	if _, err := r.Reconcile(context.Background(), ctrl.Request{}); err != nil {
		t.Fatal(err)
	}
	var got v1beta1.AcceleratorQuota
	if err := c.Get(context.Background(), client.ObjectKey{Name: rootName}, &got); err != nil {
		t.Fatal(err)
	}
	want := capacityCell(capacityRow())
	want[0].ObservedAt = nil
	if diff := cmp.Diff(want, got.Status.Capacity, compareQuantities); diff != "" {
		t.Fatalf("capacity during projection hold (-want +got):\n%s", diff)
	}
}

func TestFleetCapacityLocalReadGuards(t *testing.T) {
	for _, tt := range []struct {
		name      string
		rootUID   types.UID
		root      func(*v1beta1.AcceleratorQuota)
		intercept interceptor.Funcs
		wantError bool
	}{
		{name: "missing expected identity"},
		{name: "recreated root", rootUID: "old-root"},
		{name: "terminating root", rootUID: "management-root", root: func(q *v1beta1.AcceleratorQuota) {
			q.DeletionTimestamp = capacityTime.DeepCopy()
			q.Finalizers = []string{"example.com/cleanup"}
		}},
		{name: "root read fails", rootUID: "management-root", wantError: true, intercept: interceptor.Funcs{Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return errors.New("read failed")
		}}},
		{name: "root vanished", rootUID: "management-root", intercept: interceptor.Funcs{Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return apierrors.NewNotFound(schema.GroupResource{Group: "ome.io", Resource: "acceleratorquotas"}, rootName)
		}}},
		{name: "registry read fails", rootUID: "management-root", wantError: true, intercept: interceptor.Funcs{List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
			return errors.New("registry read failed")
		}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			local := cohort(rootName, "")
			local.UID = "management-root"
			if tt.root != nil {
				tt.root(local)
			}
			fleet := &identityFleet{fakeFleet: &fakeFleet{}, uids: map[string]types.UID{}}
			r, _ := projectingReconciler(t, fleet, local)
			r.APIReader = fake.NewClientBuilder().WithScheme(r.Scheme).WithObjects(local).WithInterceptorFuncs(tt.intercept).Build()
			err := r.reconcileFleetCapacity(context.Background(), tt.rootUID)
			if diff := cmp.Diff(tt.wantError, err != nil); diff != "" {
				t.Fatalf("error presence (-want +got):\n%s; error: %v", diff, err)
			}
		})
	}
}

func TestFleetCapacityRegistryRecheckFailure(t *testing.T) {
	local := cohort(rootName, "")
	local.UID = "management-root"
	local.Status.Capacity = capacityCell(capacityRow())
	fleet := &identityFleet{fakeFleet: &fakeFleet{}, uids: map[string]types.UID{}}
	r, c := projectingReconciler(t, fleet, local)
	var calls int
	r.APIReader = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
		calls++
		if calls == 2 {
			return errors.New("registry unavailable")
		}
		return cl.List(ctx, list, opts...)
	}})
	if err := r.reconcileFleetCapacity(context.Background(), local.UID); err == nil {
		t.Fatal("expected registry read error")
	}
	var got v1beta1.AcceleratorQuota
	if err := c.Get(context.Background(), client.ObjectKey{Name: rootName}, &got); err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(local.Status.Capacity, got.Status.Capacity, compareQuantities); diff != "" {
		t.Fatalf("failed read changed capacity (-want +got):\n%s", diff)
	}
}

func TestCapacityReconcileWithoutFleetParticipation(t *testing.T) {
	for _, mode := range []Mode{ModeWorkload, ModeManagement} {
		t.Run(string(mode), func(t *testing.T) {
			local := capacityMemberRoot()
			r, c := newReconciler(t, local)
			r.Mode = mode
			if _, err := r.Reconcile(context.Background(), ctrl.Request{}); err != nil {
				t.Fatal(err)
			}
			var got v1beta1.AcceleratorQuota
			if err := c.Get(context.Background(), client.ObjectKey{Name: rootName}, &got); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(local.Status.Capacity, got.Status.Capacity, compareQuantities); diff != "" {
				t.Fatalf("local capacity (-want +got):\n%s", diff)
			}
		})
	}
}
