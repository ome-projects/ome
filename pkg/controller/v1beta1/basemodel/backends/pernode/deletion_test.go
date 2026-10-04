package pernode

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-logr/zapr"
	"github.com/google/go-cmp/cmp"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestModelFinalizerRemoval(t *testing.T) {
	for _, kind := range []struct {
		name      string
		model     client.Object
		finalizer string
	}{
		{"namespaced", &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a"}}, constants.BaseModelFinalizer},
		{"cluster scoped", &v1beta1.ClusterBaseModel{}, constants.ClusterBaseModelFinalizer},
	} {
		t.Run(kind.name, func(t *testing.T) {
			for _, tc := range []struct {
				name         string
				conflict     bool
				agentChanges bool
				forbidden    bool
			}{
				{name: "completed deletion"},
				{name: "concurrent metadata update", conflict: true},
				{name: "recheck agent acknowledgement after conflict", conflict: true, agentChanges: true},
				{name: "forbidden update", forbidden: true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					logCore, logs := observer.New(zap.ErrorLevel)
					ctx := ctrl.LoggerInto(t.Context(), zapr.NewLogger(zap.New(logCore)))
					scheme := runtime.NewScheme()
					if err := corev1.AddToScheme(scheme); err != nil {
						t.Fatal(err)
					}
					if err := v1beta1.AddToScheme(scheme); err != nil {
						t.Fatal(err)
					}
					model := kind.model.DeepCopyObject().(client.Object)
					model.SetName("example-model")
					model.SetFinalizers([]string{kind.finalizer, "example.com/keep"})
					now := metav1.Now()
					model.SetDeletionTimestamp(&now)
					live := fake.NewClientBuilder().WithScheme(scheme).WithObjects(model).Build()
					key := client.ObjectKeyFromObject(model)
					if err := live.Get(ctx, key, model); err != nil {
						t.Fatal(err)
					}

					cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
						Name: "node-a", Namespace: constants.OMENamespace,
						Labels: map[string]string{constants.ModelStatusConfigMapLabel: "true"},
					}}
					modelKey := constants.GetModelConfigMapKey(model.GetNamespace(), model.GetName(), model.GetNamespace() == "")
					if tc.agentChanges {
						cm.Data = map[string]string{modelKey: `{"status":"Deleted"}`}
						for _, obj := range []client.Object{cm, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: cm.Name}}} {
							if err := live.Create(ctx, obj); err != nil {
								t.Fatal(err)
							}
						}
					}

					updates := 0
					c := interceptor.NewClient(live, interceptor.Funcs{
						Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
							updates++
							if tc.forbidden {
								return apierrors.NewForbidden(schema.GroupResource{Group: v1beta1.SchemeGroupVersion.Group, Resource: "models"}, obj.GetName(), fmt.Errorf("denied"))
							}
							if tc.conflict && updates == 1 {
								latest := kind.model.DeepCopyObject().(client.Object)
								if err := c.Get(ctx, key, latest); err != nil {
									return err
								}
								latest.SetAnnotations(map[string]string{"example.com/owner": "updated"})
								latest.SetFinalizers(append(latest.GetFinalizers(), "example.com/other"))
								if err := c.Update(ctx, latest); err != nil {
									return err
								}
								if tc.agentChanges {
									cm.Data[modelKey] = `{"status":"Ready"}`
									if err := c.Update(ctx, cm); err != nil {
										return err
									}
								}
							}
							return c.Update(ctx, obj, opts...)
						},
					})

					result, err := HandleModelDeletion(ctx, c, live, model, kind.finalizer)
					var reason metav1.StatusReason
					if err != nil {
						reason = apierrors.ReasonForError(err)
					}
					type outcome struct {
						Result    ctrl.Result
						Reason    metav1.StatusReason
						ErrorLogs int
						Updates   int
					}
					want := outcome{Updates: 1}
					if tc.conflict {
						want.Result.Requeue = true
					}
					if tc.forbidden {
						want.Reason, want.ErrorLogs = metav1.StatusReasonForbidden, 1
					}
					if diff := cmp.Diff(want, outcome{result, reason, logs.Len(), updates}); diff != "" {
						t.Fatalf("removal outcome (-want +got):\n%s", diff)
					}

					if err := live.Get(ctx, key, model); err != nil {
						t.Fatal(err)
					}
					wantFinalizers := []string{"example.com/keep"}
					var wantAnnotations map[string]string
					if tc.conflict || tc.forbidden {
						wantFinalizers = append([]string{kind.finalizer}, wantFinalizers...)
					}
					if tc.conflict {
						wantFinalizers = append(wantFinalizers, "example.com/other")
						wantAnnotations = map[string]string{"example.com/owner": "updated"}
					}
					if diff := cmp.Diff(wantFinalizers, model.GetFinalizers()); diff != "" {
						t.Fatalf("finalizers after first attempt (-want +got):\n%s", diff)
					}
					if !tc.conflict {
						return
					}
					result, err = HandleModelDeletion(ctx, c, live, model, kind.finalizer)
					if err != nil {
						t.Fatal(err)
					}
					if tc.agentChanges {
						if diff := cmp.Diff(ctrl.Result{RequeueAfter: 30 * time.Second}, result); diff != "" {
							t.Fatalf("unacknowledged model must wait (-want +got):\n%s", diff)
						}
						if diff := cmp.Diff(1, updates); diff != "" {
							t.Fatalf("unacknowledged model writes (-want +got):\n%s", diff)
						}
						cm.Data[modelKey] = `{"status":"Deleted"}`
						if err := live.Update(ctx, cm); err != nil {
							t.Fatal(err)
						}
						result, err = HandleModelDeletion(ctx, c, live, model, kind.finalizer)
						if err != nil {
							t.Fatal(err)
						}
					}
					if diff := cmp.Diff(ctrl.Result{}, result); diff != "" {
						t.Fatalf("successful removal (-want +got):\n%s", diff)
					}
					if err := live.Get(ctx, key, model); err != nil {
						t.Fatal(err)
					}
					if diff := cmp.Diff([]string{"example.com/keep", "example.com/other"}, model.GetFinalizers()); diff != "" {
						t.Errorf("other finalizers must survive (-want +got):\n%s", diff)
					}
					if diff := cmp.Diff(wantAnnotations, model.GetAnnotations()); diff != "" {
						t.Errorf("concurrent annotations must survive (-want +got):\n%s", diff)
					}
					if diff := cmp.Diff(0, logs.Len()); diff != "" {
						t.Errorf("successful retry error logs (-want +got):\n%s", diff)
					}
				})
			}
		})
	}
}
