package acceleratorquota

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
)

func TestFleetCapacityReconcileWriteFailures(t *testing.T) {
	conflict := apierrors.NewConflict(quotaGR, rootName, errors.New("resource version changed"))
	capacityFailure := errors.New("capacity write unavailable")
	statusFailure := errors.New("quota status unavailable")
	for _, tt := range []struct {
		name                   string
		capacityErr, statusErr error
		wantResult             ctrl.Result
		wantErr                error
	}{
		{name: "success uses configured resync", wantResult: ctrl.Result{RequeueAfter: time.Minute}},
		{name: "conflict requests a fresh pass", capacityErr: conflict, wantResult: ctrl.Result{Requeue: true}},
		{name: "capacity write failure remains an error", capacityErr: capacityFailure, wantErr: capacityFailure},
		{name: "status failure still permits capacity collection", statusErr: statusFailure, wantErr: statusFailure},
		{name: "conflict does not mask a status failure", capacityErr: conflict, statusErr: statusFailure, wantErr: statusFailure},
	} {
		t.Run(tt.name, func(t *testing.T) {
			root := cohort(rootName, "")
			root.UID = "management-root"
			fleet := &identityFleet{fakeFleet: &fakeFleet{members: map[string]workloadcluster.SelectivelyCachingClient{"member-a": member(t, capacityMemberRoot())}}, uids: map[string]types.UID{"member-a": "registration-a"}}
			r, base := projectingReconciler(t, fleet, root, capacityRegistration("member-a", "registration-a"))
			r.ResyncInterval = time.Minute
			attempts := 0
			r.Client = interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{
				SubResourcePatch: func(ctx context.Context, cl client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
					quota, ok := obj.(*v1beta1.AcceleratorQuota)
					if ok && sub == "status" {
						if len(quota.Status.Capacity) > 0 {
							attempts++
							if tt.capacityErr != nil {
								return tt.capacityErr
							}
						} else if tt.statusErr != nil {
							return tt.statusErr
						}
					}
					return cl.SubResource(sub).Patch(ctx, obj, patch, opts...)
				},
			})
			got, err := r.Reconcile(t.Context(), ctrl.Request{})
			if diff := cmp.Diff(tt.wantResult, got); diff != "" {
				t.Errorf("result (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tt.wantErr, err, cmpopts.EquateErrors()); diff != "" {
				t.Errorf("error (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(1, attempts); diff != "" {
				t.Fatalf("capacity write attempts (-want +got):\n%s", diff)
			}
			var stored v1beta1.AcceleratorQuota
			if err := base.Get(t.Context(), client.ObjectKey{Name: rootName}, &stored); err != nil {
				t.Fatal(err)
			}
			var want []v1beta1.AcceleratorCapacityStatus
			if tt.capacityErr == nil {
				want = capacityCell(capacityRow())
			}
			if diff := cmp.Diff(want, stored.Status.Capacity, compareQuantities); diff != "" {
				t.Fatalf("stored capacity (-want +got):\n%s", diff)
			}
		})
	}
}

func TestFleetCapacityConflictRefreshesObservation(t *testing.T) {
	for _, tt := range []struct {
		name   string
		resync time.Duration
	}{
		{name: "event driven"},
		{name: "configured resync", resync: time.Minute},
	} {
		t.Run(tt.name, func(t *testing.T) {
			root := cohort(rootName, "")
			root.UID = "management-root"
			fleet := &identityFleet{fakeFleet: &fakeFleet{members: map[string]workloadcluster.SelectivelyCachingClient{}}, uids: map[string]types.UID{"member-a": "registration-a"}}
			r, base := projectingReconciler(t, fleet, root, capacityRegistration("member-a", "registration-a"))
			r.ResyncInterval = tt.resync
			remote := member(t, capacityMemberRoot())
			concurrentWrite := true
			fleet.members["member-a"] = workloadcluster.NewNeverCachingClient(interceptor.NewClient(remote, interceptor.Funcs{
				Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if err := cl.Get(ctx, key, obj, opts...); err != nil {
						return err
					}
					if _, ok := obj.(*metav1.PartialObjectMetadata); ok && concurrentWrite {
						concurrentWrite = false
						var updated v1beta1.AcceleratorQuota
						if err := base.Get(ctx, client.ObjectKey{Name: rootName}, &updated); err != nil {
							return err
						}
						updated.Status.ObservedGeneration = 42
						return base.Status().Update(ctx, &updated)
					}
					return nil
				},
			}))
			got, err := r.Reconcile(t.Context(), ctrl.Request{})
			if diff := cmp.Diff(ctrl.Result{Requeue: true}, got); diff != "" {
				t.Errorf("conflict result (-want +got):\n%s", diff)
			}
			if err != nil {
				t.Errorf("conflict must request a fresh pass: %v", err)
			}
			var stored v1beta1.AcceleratorQuota
			if err := base.Get(t.Context(), client.ObjectKey{Name: rootName}, &stored); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(int64(42), stored.Status.ObservedGeneration); diff != "" {
				t.Fatalf("concurrent status (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff([]v1beta1.AcceleratorCapacityStatus(nil), stored.Status.Capacity); diff != "" {
				t.Fatalf("conflicted capacity (-want +got):\n%s", diff)
			}

			var report v1beta1.AcceleratorQuota
			if err := remote.Get(t.Context(), client.ObjectKey{Name: rootName}, &report); err != nil {
				t.Fatal(err)
			}
			report.Status.Capacity[0].Allocatable = resource.MustParse("12")
			if err := remote.Status().Update(t.Context(), &report); err != nil {
				t.Fatal(err)
			}
			if err := remote.Get(t.Context(), client.ObjectKey{Name: rootName}, &report); err != nil {
				t.Fatal(err)
			}
			got, err = r.Reconcile(t.Context(), ctrl.Request{})
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(ctrl.Result{RequeueAfter: tt.resync}, got); diff != "" {
				t.Errorf("recovered result (-want +got):\n%s", diff)
			}
			if err := base.Get(t.Context(), client.ObjectKey{Name: rootName}, &stored); err != nil {
				t.Fatal(err)
			}
			want := capacityCell(capacityRow())
			want[0].Allocatable = resource.MustParse("12")
			want[0].PerCluster[0].Allocatable = resource.MustParse("12")
			want[0].PerCluster[0].ReportResourceVersion = report.ResourceVersion
			if diff := cmp.Diff(want, stored.Status.Capacity, compareQuantities); diff != "" {
				t.Fatalf("fresh capacity (-want +got):\n%s", diff)
			}
		})
	}
}
