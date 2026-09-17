package inferencereplica

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
)

// countingReader records every Get so a test can assert which reader a
// conflict-retry closure re-read through.
type countingReader struct {
	client.Reader
	gets int
}

func (r *countingReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	r.gets++
	return r.Reader.Get(ctx, key, obj, opts...)
}

// Every conflict-retry closure must re-read its base through the
// authoritative reader. Re-reading the informer cache cannot converge: a 409
// means the apiserver already holds a ResourceVersion the cache has not
// observed, so each attempt resubmits the same stale base until the backoff
// is spent.
func TestRetryClosuresReReadThroughLiveReader(t *testing.T) {
	scheme := testScheme(t)
	ctx := context.Background()

	newFixture := func(t *testing.T) (*Reconciler, *countingReader, *v1beta1.InferenceReplica) {
		t.Helper()
		ir := baselineIR("llama-engine", "default", 1)
		cached := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(ir).WithStatusSubresource(&v1beta1.InferenceReplica{}).Build()
		live := &countingReader{Reader: fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(ir).WithStatusSubresource(&v1beta1.InferenceReplica{}).Build()}
		return &Reconciler{
			Client:       cached,
			APIReader:    live,
			Log:          ctrl.Log.WithName("test"),
			Expectations: workload.NewExpectations(),
		}, live, ir
	}

	t.Run("MutateInstance", func(t *testing.T) {
		r, live, ir := newFixture(t)
		err := buildMutateInstance(r.statusWriter(), r.APIReader, ir)(ctx, 0, func(s *workload.InstanceStatus) bool {
			s.Phase = workload.InstancePhaseReady
			return true
		})
		if err != nil {
			t.Fatalf("mutate instance: %v", err)
		}
		if live.gets == 0 {
			t.Error("re-read must go through the live reader, not the cache")
		}
	})

	t.Run("PromoteCurrentRevision", func(t *testing.T) {
		r, live, ir := newFixture(t)
		if err := buildPromoteCurrentRevision(r.statusWriter(), r.APIReader, ir)(ctx, "llama-engine-abc123"); err != nil {
			t.Fatalf("promote: %v", err)
		}
		if live.gets == 0 {
			t.Error("re-read must go through the live reader, not the cache")
		}
	})

	t.Run("WriteAggregateCondition", func(t *testing.T) {
		r, live, ir := newFixture(t)
		err := buildWriteAggregateCondition(r.statusWriter(), r.APIReader, ir)(ctx, metav1.Condition{
			Type:    InferenceReplicaConditionReady,
			Status:  metav1.ConditionTrue,
			Reason:  ReasonAllInstancesReady,
			Message: "ready",
		})
		if err != nil {
			t.Fatalf("write aggregate condition: %v", err)
		}
		if live.gets == 0 {
			t.Error("re-read must go through the live reader, not the cache")
		}
	})

	t.Run("MutateRetryBlock", func(t *testing.T) {
		r, live, ir := newFixture(t)
		err := buildMutateRetryBlock(r.statusWriter(), r.APIReader, ir)(ctx, "llama-engine-abc123",
			func(*workload.RetryBlock) workload.RetryBlockDisposition {
				return workload.RetryBlockRemove
			})
		if err != nil {
			t.Fatalf("mutate retry block: %v", err)
		}
		if live.gets == 0 {
			t.Error("re-read must go through the live reader, not the cache")
		}
	})

	t.Run("RemoveInstance", func(t *testing.T) {
		r, live, ir := newFixture(t)
		if _, err := buildRemoveInstance(r.statusWriter(), r.APIReader, ir, r.Expectations)(ctx, 0); err != nil {
			t.Fatalf("remove instance: %v", err)
		}
		if live.gets == 0 {
			t.Error("re-read must go through the live reader, not the cache")
		}
	})
}
