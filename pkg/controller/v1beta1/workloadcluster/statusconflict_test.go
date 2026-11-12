package workloadcluster

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// The status write reads its object through the cache, so a pass that runs
// before the cache catches up writes at a stale resourceVersion and loses the
// race. That is not a failure -- the next pass recomputes the same status from
// fresh state -- but reporting it as one has a cost beyond the misleading log
// line: the integration framework fails a whole suite in teardown on any
// error-level optimistic-lock entry, on the stated grounds that it marks a
// reconciler with no conflict-aware path. The suite then reddens in AfterSuite
// with every spec passed, which reads as an unexplained flake.
func TestFinishStatusWriteOutcome(t *testing.T) {
	gr := schema.GroupResource{Group: "ome.io", Resource: "workloadclusters"}

	tests := []struct {
		name       string
		updateErr  error
		wantResult ctrl.Result
		wantErr    bool
	}{
		{
			name:       "a clean write returns the steady requeue",
			updateErr:  nil,
			wantResult: ctrl.Result{RequeueAfter: DefaultHealthInterval},
			wantErr:    false,
		},
		{
			name:       "a lost optimistic-lock race requeues without an error",
			updateErr:  apierrors.NewConflict(gr, "m1", errors.New("the object has been modified")),
			wantResult: ctrl.Result{Requeue: true},
			wantErr:    false,
		},
		{
			// The object went away mid-pass; there is nothing left to report on.
			name:       "a deleted object ends the pass quietly",
			updateErr:  apierrors.NewNotFound(gr, "m1"),
			wantResult: ctrl.Result{},
			wantErr:    false,
		},
		{
			name:       "any other write failure is still reported",
			updateErr:  apierrors.NewForbidden(gr, "m1", errors.New("nope")),
			wantResult: ctrl.Result{},
			wantErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := fakeclient.Scheme(t)
			wc := wcWithSecret("m1")

			opts := []fakeclient.Option{fakeclient.WithScheme(s), fakeclient.WithObjects(wc), fakeclient.WithStatusSubresource(&v1beta1.WorkloadCluster{})}
			if tc.updateErr != nil {
				opts = append(opts, fakeclient.FailingVerb(fakeclient.StatusUpdate, fakeclient.EveryCall, tc.updateErr))
			}
			c := fakeclient.New(t, opts...)
			r := &Reconciler{Client: c, Log: log.Log}

			got, err := r.finish(context.Background(), wc, DefaultHealthInterval)

			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.wantResult, got)
		})
	}
}
