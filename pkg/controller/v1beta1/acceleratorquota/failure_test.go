package acceleratorquota

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/quota/tree"
)

// quotaGR is the group-resource the apiserver would name in a conflict or
// not-found for these objects.
var quotaGR = schema.GroupResource{Group: "ome.io", Resource: "acceleratorquotas"}

// The reconcile loop's error paths, which the happy-path tables cannot reach.
// A pass either aborts or absorbs the failure, and getting that backwards is
// silent either way: a wrongly-absorbed error leaves every node's verdict stale
// with nothing logged, while a wrongly-surfaced one turns an ordinary mid-pass
// rewrite into endless backoff.
func TestReconcileFailurePaths(t *testing.T) {
	quotas := []client.Object{
		cohort(rootName, "", budget("128")),
		cohort("org", rootName, budget("100")),
		leaf("team-a", "org", budget("60")),
	}
	patchFails := func(err error) interceptor.Funcs {
		return interceptor.Funcs{
			SubResourcePatch: func(context.Context, client.Client, string, client.Object,
				client.Patch, ...client.SubResourcePatchOption) error {
				return err
			},
		}
	}

	tests := []struct {
		name    string
		funcs   interceptor.Funcs
		options *tree.Options // nil keeps the working configuration
		wantErr string        // empty means the pass must succeed
	}{
		{
			// Without the list there is no tree, and writing a verdict derived
			// from a partial one would freeze live tenants.
			name: "a failed LIST aborts the pass",
			funcs: interceptor.Funcs{
				List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
					return apierrors.NewServiceUnavailable("apiserver down")
				},
			},
			wantErr: "listing AcceleratorQuotas",
		},
		{
			// An operator misconfiguration rather than a property of the nodes,
			// so it must requeue and say so instead of concluding there is
			// nothing to reconcile.
			name:    "an unconfigured root aborts the pass",
			options: &tree.Options{},
			wantErr: "assembling the quota tree",
		},
		{
			// Rewritten between the list and the patch. The next pass sees the
			// newer state, so failing here would only add backoff.
			name:  "a conflicted status write is absorbed",
			funcs: patchFails(apierrors.NewConflict(quotaGR, "org", errors.New("stale"))),
		},
		{
			name:  "a status write for a deleted node is absorbed",
			funcs: patchFails(apierrors.NewNotFound(quotaGR, "org")),
		},
		{
			name:    "any other status-write failure fails the pass",
			funcs:   patchFails(apierrors.NewInternalError(errors.New("etcd unavailable"))),
			wantErr: "writing status for node",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := testScheme(t)
			c := fake.NewClientBuilder().
				WithScheme(s).
				WithObjects(quotas...).
				WithStatusSubresource(&v1beta1.AcceleratorQuota{}).
				WithInterceptorFuncs(tc.funcs).
				Build()
			r := &Reconciler{
				Client: c, Scheme: s, Log: logf.Log.WithName("test"), APIReader: c,
				Options: tree.Options{RootName: rootName, MaxDepth: 5},
			}
			if tc.options != nil {
				r.Options = *tc.options
			}

			_, err := r.Reconcile(context.Background(), ctrl.Request{})
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("Reconcile() = %v, want the failure absorbed", err)
			case tc.wantErr != "" && err == nil:
				t.Fatalf("Reconcile() = nil, want an error naming %q", tc.wantErr)
			case tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr):
				t.Errorf("Reconcile() = %q, want it to name %q", err, tc.wantErr)
			}
		})
	}
}

// One node's failed write must not skip the rest. An early return would leave
// whichever nodes sort after the failure carrying a stale verdict until
// something else touched them, and that order is not something an operator can
// see or influence.
func TestReconcileWritesEveryNodeDespiteOneFailure(t *testing.T) {
	s := testScheme(t)
	var attempted []string
	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(
			cohort(rootName, "", budget("128")),
			cohort("org", rootName, budget("100")),
			leaf("team-a", "org", budget("60")),
		).
		WithStatusSubresource(&v1beta1.AcceleratorQuota{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, cl client.Client, sub string,
				obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				attempted = append(attempted, obj.GetName())
				// Fail the node that sorts first, so an early return shows up
				// as the other two never being tried.
				if obj.GetName() == "org" {
					return apierrors.NewInternalError(errors.New("etcd unavailable"))
				}
				return cl.Status().Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()
	r := &Reconciler{
		Client: c, Scheme: s, Log: logf.Log.WithName("test"), APIReader: c,
		Options: tree.Options{RootName: rootName, MaxDepth: 5},
	}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{}); err == nil {
		t.Fatal("Reconcile() = nil, want the collected failure to surface")
	}

	sort.Strings(attempted)
	if diff := cmp.Diff([]string{"org", rootName, "team-a"}, attempted); diff != "" {
		t.Errorf("nodes attempted (-want +got):\n%s", diff)
	}
	// And the two that did not fail were actually written, not just attempted.
	if got := observe(t, c)["team-a"]; got.Parent != "org" {
		t.Errorf("team-a status was not written: %+v", got)
	}
}

// The startup root check writes through this component's own fail-closed
// validating webhook, whose caBundle the same process injects seconds later. A
// refused create is therefore the normal first outcome on a fresh install, and a
// Runnable that returned it would stop the manager before the rotator ran —
// making every restart lose the same race.
func TestBootstrapRootSurvivesARefusedCreate(t *testing.T) {
	// Fast enough to keep the test honest about retrying without paying the
	// production cadence.
	const retry = time.Millisecond

	webhookRefusal := apierrors.NewInternalError(errors.New(
		`failed calling webhook "acceleratorquota.ome-quota-manager.validator": ` +
			`could not get REST client: unable to load root certificates`))

	tests := []struct {
		name string
		// refusals is how many creates are refused before the admission path
		// comes up. -1 never lets it up.
		refusals int
		wantRoot bool
	}{
		{name: "the admission path is already up", refusals: 0, wantRoot: true},
		{name: "the caBundle lands on a later attempt", refusals: 3, wantRoot: true},
		{name: "an admission path that never comes up is not fatal", refusals: -1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var attempts int
			s := testScheme(t)
			c := fake.NewClientBuilder().
				WithScheme(s).
				WithStatusSubresource(&v1beta1.AcceleratorQuota{}).
				WithInterceptorFuncs(interceptor.Funcs{
					Create: func(ctx context.Context, cl client.WithWatch, obj client.Object,
						opts ...client.CreateOption) error {
						attempts++
						if tc.refusals < 0 || attempts <= tc.refusals {
							return webhookRefusal
						}
						return cl.Create(ctx, obj, opts...)
					},
				}).
				Build()
			r := &Reconciler{
				Client: c, Scheme: s, Log: logf.Log.WithName("test"), APIReader: c,
				Options: tree.Options{RootName: rootName, MaxDepth: 5},
			}

			ctx := context.Background()
			if !tc.wantRoot {
				// Nothing else ends the poll, so this stands in for the manager
				// shutting down while the check is still trying.
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 100*retry)
				defer cancel()
			}

			// The whole point: whatever the apiserver said, the manager lives.
			if err := r.bootstrapRootEvery(ctx, retry); err != nil {
				t.Fatalf("bootstrapRootEvery() = %v, want the manager left running", err)
			}

			err := c.Get(context.Background(), client.ObjectKey{Name: rootName}, &v1beta1.AcceleratorQuota{})
			switch {
			case tc.wantRoot && err != nil:
				t.Fatalf("the reserved root was not created after %d attempts: %v", attempts, err)
			case !tc.wantRoot && err == nil:
				t.Fatal("a root was created while every create was refused")
			}
			if tc.wantRoot && attempts != tc.refusals+1 {
				t.Errorf("attempts = %d, want %d (one per refusal, then the one that lands)",
					attempts, tc.refusals+1)
			}
		})
	}
}
