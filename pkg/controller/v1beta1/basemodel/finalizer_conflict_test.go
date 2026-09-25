package basemodel

import (
	"context"
	"errors"
	"testing"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel/shared"
)

type finalizerTestBackend struct{ reconciled bool }

func (b *finalizerTestBackend) Name() string                          { return "test" }
func (b *finalizerTestBackend) Matches(_ *v1beta1.BaseModelSpec) bool { return true }
func (b *finalizerTestBackend) Reconcile(_ context.Context, _ shared.BackendArgs) (ctrl.Result, error) {
	b.reconciled = true
	return ctrl.Result{}, nil
}
func (b *finalizerTestBackend) HandleDeletion(_ context.Context, _ shared.BackendArgs) (ctrl.Result, error) {
	return ctrl.Result{}, nil
}

func TestReconcileModelFinalizerWriteOutcome(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}

	conflict := apierrors.NewConflict(
		schema.GroupResource{Group: "ome.io", Resource: "clusterbasemodels"},
		"model", errors.New("the object has been modified"))
	forbidden := apierrors.NewForbidden(
		schema.GroupResource{Group: "ome.io", Resource: "clusterbasemodels"},
		"model", errors.New("forbidden"))

	tests := []struct {
		name        string
		updateErr   error
		wantResult  ctrl.Result
		wantErr     error
		wantBackend bool
	}{
		{
			name:        "successful write reaches the backend",
			wantBackend: true,
		},
		{
			name:       "conflict requeues without an error",
			updateErr:  conflict,
			wantResult: ctrl.Result{Requeue: true},
		},
		{
			name:      "non-conflict error is returned",
			updateErr: forbidden,
			wantErr:   forbidden,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			obj := &v1beta1.ClusterBaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model"}}
			builder := ctrlclientfake.NewClientBuilder().WithScheme(scheme).WithObjects(obj)
			if test.updateErr != nil {
				builder = builder.WithInterceptorFuncs(interceptor.Funcs{
					Update: func(context.Context, client.WithWatch, client.Object, ...client.UpdateOption) error {
						return test.updateErr
					},
				})
			}

			backend := &finalizerTestBackend{}
			result, err := reconcileModel(
				context.Background(), builder.Build(), scheme, logr.Discard(),
				[]shared.Backend{backend}, obj.DeepCopy(), "test.ome.io/finalizer", true, "ClusterBaseModel",
			)

			if result != test.wantResult {
				t.Errorf("result = %+v, want %+v", result, test.wantResult)
			}
			if test.wantErr == nil && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if test.wantErr != nil && !errors.Is(err, test.wantErr) {
				t.Errorf("error = %v, want %v", err, test.wantErr)
			}
			if backend.reconciled != test.wantBackend {
				t.Errorf("backend reconciled = %v, want %v", backend.reconciled, test.wantBackend)
			}
		})
	}
}
