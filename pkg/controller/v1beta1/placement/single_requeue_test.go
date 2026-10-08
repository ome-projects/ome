package placement

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
)

func TestSingleAdmissionWaitHonorsStatusBackstop(t *testing.T) {
	for _, tt := range []struct {
		name            string
		safety          time.Duration
		round           time.Duration
		failCreate      bool
		failObservation bool
		staleStatus     bool
		want            time.Duration
	}{
		{name: "all nominees created", safety: 30 * time.Second, want: 30 * time.Second},
		{name: "injected legacy cadence", want: 500 * time.Millisecond},
		{name: "incremental round sooner than backstop", safety: 30 * time.Second, round: 2 * time.Second, want: 2 * time.Second},
		{name: "backstop sooner than incremental round", safety: time.Second, round: 2 * time.Second, want: time.Second},
		{name: "partial creation retries promptly", safety: 30 * time.Second, failCreate: true, want: 500 * time.Millisecond},
		{name: "unreadable member retries promptly", safety: 30 * time.Second, failObservation: true, want: 500 * time.Millisecond},
		{name: "concurrent source update retries promptly", safety: 30 * time.Second, staleStatus: true, want: 500 * time.Millisecond},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := singleFixture(t, false)
			f.reconciler.Requeue = 500 * time.Millisecond
			if tt.safety > 0 {
				f.reconciler.converge = &convergeConfig{safetyRequeue: tt.safety}
			}
			if tt.round > 0 {
				f.reconciler.DispatcherMode = DispatcherModeIncremental
				f.reconciler.DispatcherStepSize = 1
				f.reconciler.DispatcherRoundTimeout = tt.round
			}
			if tt.failCreate || tt.failObservation {
				wrapped := interceptor.NewClient(f.workers["member-b"], interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if _, ok := obj.(*v1beta1.InferenceReplica); ok && tt.failObservation {
						return errors.New("member observation unavailable")
					}
					return c.Get(ctx, key, obj, opts...)
				}, Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if _, ok := obj.(*v1beta1.InferenceService); ok && tt.failCreate {
						return errors.New("member write unavailable")
					}
					return c.Create(ctx, obj, opts...)
				}})
				f.connections.m["member-b"] = workloadcluster.NewNeverCachingClient(wrapped)
			}
			if tt.staleStatus {
				f.reconciler.Client = interceptor.NewClient(f.reconciler.Client.(client.WithWatch), interceptor.Funcs{SubResourceUpdate: func(ctx context.Context, c client.Client, subresource string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
					service, ok := obj.(*v1beta1.InferenceService)
					if ok && subresource == "status" && service.Status.ObservedGeneration == service.Generation {
						live := &v1beta1.InferenceService{}
						if err := c.Get(ctx, client.ObjectKeyFromObject(service), live); err != nil {
							return err
						}
						live.Generation++
						if err := c.Update(ctx, live); err != nil {
							return err
						}
						return apierrors.NewConflict(schema.GroupResource{Group: v1beta1.SchemeGroupVersion.Group, Resource: "inferenceservices"}, service.Name, errors.New("source changed"))
					}
					return c.SubResource(subresource).Update(ctx, obj, opts...)
				}})
			}
			got, err := f.reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.source)})
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(ctrl.Result{RequeueAfter: tt.want, Priority: ptr.To(retryPriority)}, got); diff != "" {
				t.Errorf("next reconcile (-want +got):\n%s", diff)
			}
			source := &v1beta1.InferenceService{}
			if err := f.reconciler.Get(t.Context(), client.ObjectKeyFromObject(f.source), source); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff("", source.Status.Placement.Plan.Winner); diff != "" {
				t.Errorf("winner before admission: %s", diff)
			}
		})
	}
}

func TestSingleAdmissionStatusStableAcrossReconciles(t *testing.T) {
	for _, tt := range []struct {
		name string
		pd   bool
	}{
		{name: "engine"},
		{name: "engine and decoder", pd: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := singleFixture(t, tt.pd)
			request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.source)}
			if _, err := f.reconciler.Reconcile(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			before := &v1beta1.InferenceService{}
			if err := f.reconciler.Get(t.Context(), request.NamespacedName, before); err != nil {
				t.Fatal(err)
			}
			for i := range before.Status.Conditions {
				before.Status.Conditions[i].LastTransitionTime = apis.VolatileTime{Inner: metav1.NewTime(time.Unix(10, 0))}
			}
			if err := f.reconciler.Status().Update(t.Context(), before); err != nil {
				t.Fatal(err)
			}
			if _, err := f.reconciler.Reconcile(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			after := &v1beta1.InferenceService{}
			if err := f.reconciler.Get(t.Context(), request.NamespacedName, after); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(before.Status, after.Status); diff != "" {
				t.Errorf("stable admission wait status (-want +got):\n%s", diff)
			}
		})
	}
}
