package inferencereplica

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestCapabilityReadinessSentinelSignalsOnceWithoutAPI(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	base := fake.NewClientBuilder().WithScheme(scheme).Build()
	var apiCalls atomic.Int32
	tracked := interceptor.NewClient(base, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			apiCalls.Add(1)
			return c.Get(ctx, key, obj, opts...)
		},
	})
	ready := make(chan struct{})
	wrapped := newCapabilityReadinessReconciler(&Reconciler{Client: tracked, Log: logr.Discard()}, ready)
	sentinel := reconcile.Request{NamespacedName: types.NamespacedName{Name: capabilityReadinessSentinelName}}

	for range 2 {
		result, err := wrapped.Reconcile(context.Background(), sentinel)
		require.NoError(t, err)
		assert.Equal(t, reconcile.Result{}, result)
	}

	select {
	case <-ready:
	default:
		t.Fatal("capability readiness was not signaled")
	}
	assert.Equal(t, int32(0), apiCalls.Load())
}

func TestCapabilityReadinessSentinelRequiresExactReservedKey(t *testing.T) {
	wantResult := reconcile.Result{RequeueAfter: time.Second}
	wantErr := errors.New("delegated")
	tests := []struct {
		name string
		req  reconcile.Request
	}{
		{
			name: "reserved name in a namespace",
			req:  reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ome", Name: capabilityReadinessSentinelName}},
		},
		{
			name: "empty namespace with another name",
			req:  reconcile.Request{NamespacedName: types.NamespacedName{Name: "real-inference-replica"}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ready := make(chan struct{})
			var calls atomic.Int32
			delegate := reconcile.Func(func(_ context.Context, got reconcile.Request) (reconcile.Result, error) {
				calls.Add(1)
				assert.Equal(t, test.req, got)
				return wantResult, wantErr
			})
			wrapped := newCapabilityReadinessReconciler(delegate, ready)

			gotResult, gotErr := wrapped.Reconcile(context.Background(), test.req)

			assert.Equal(t, wantResult, gotResult)
			assert.ErrorIs(t, gotErr, wantErr)
			assert.Equal(t, int32(1), calls.Load())
			select {
			case <-ready:
				t.Fatal("non-sentinel request signaled capability readiness")
			default:
			}
		})
	}
}
