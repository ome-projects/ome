package irprojector

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/placement/protocol"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// ensureWorld is one projection's surroundings: the service, the store the
// apiserver holds (live), the counters on it and the parameters the pass
// under test runs with. Every write of the projector lands in live.
type ensureWorld struct {
	t         *testing.T
	isvc      *v1beta1.InferenceService
	live      client.WithWatch
	creates   *fakeclient.Calls
	patches   *fakeclient.Calls
	liveGets  *fakeclient.Calls
	patchData []string
	params    Params
}

// newEnsureWorld builds a world whose live store carries the options given,
// with the counters and the patch recorder in front of it.
func newEnsureWorld(t *testing.T, opts ...fakeclient.Option) *ensureWorld {
	t.Helper()
	w := &ensureWorld{t: t, isvc: baselineISVC("example", "prod")}
	w.creates = fakeclient.CountingVerb(fakeclient.Create)
	w.patches = fakeclient.CountingVerb(fakeclient.Patch)
	w.liveGets = fakeclient.CountingVerb(fakeclient.Get).OnKind(&v1beta1.InferenceReplica{})
	record := fakeclient.WithInterceptors(interceptor.Funcs{Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
		if data, err := patch.Data(obj); err == nil {
			w.patchData = append(w.patchData, string(data))
		}
		return c.Patch(ctx, obj, patch, opts...)
	}})
	all := append([]fakeclient.Option{w.creates, w.patches, w.liveGets, record}, opts...)
	w.live = fakeclient.New(t, all...)
	w.params = minimalParams(t, w.isvc, w.live)
	return w
}

// key is the projected replica's key.
func (w *ensureWorld) key() types.NamespacedName {
	return types.NamespacedName{Namespace: w.isvc.Namespace, Name: InferenceReplicaName(w.isvc.Name, v1beta1.EngineComponent)}
}

// project runs one projection onto the live store and requires it to pass,
// so the store holds the replica as the projector writes it.
func (w *ensureWorld) project() *v1beta1.InferenceReplica {
	w.t.Helper()
	ir, err := EnsureInferenceReplica(context.Background(), minimalParams(w.t, w.isvc, w.live))
	if err != nil {
		w.t.Fatalf("first projection: %v", err)
	}
	return ir
}

// stored reads the replica the live store holds.
func (w *ensureWorld) stored() *v1beta1.InferenceReplica {
	w.t.Helper()
	ir := &v1beta1.InferenceReplica{}
	if err := w.live.Get(context.Background(), w.key(), ir); err != nil {
		w.t.Fatalf("get the stored replica: %v", err)
	}
	return ir
}

// placement makes the service a placement-managed member of the source
// named by the policy, which the projector reads from the annotation.
func (w *ensureWorld) placement(policy *v1beta1.PlacementExecutionPolicy) {
	w.t.Helper()
	raw, err := protocol.Encode(policy)
	if err != nil {
		w.t.Fatal(err)
	}
	w.isvc.Labels = map[string]string{constants.PlacementOrigin: string(policy.SourceUID)}
	w.isvc.Annotations = map[string]string{constants.PlacementExecution: raw}
}

// cacheShowing runs the pass with a cache that shows exactly the objects
// given and the live store as the live reader.
func (w *ensureWorld) cacheShowing(objects ...client.Object) {
	w.t.Helper()
	w.params.Client = &cachedView{Client: w.live, shows: fakeclient.New(w.t, fakeclient.WithObjects(objects...))}
	w.params.Reader = w.live
}

// memberPolicy is a valid placement policy of the source given.
func memberPolicy(source string) *v1beta1.PlacementExecutionPolicy {
	return &v1beta1.PlacementExecutionPolicy{PlanID: "plan-a", Revision: 1, SourceUID: types.UID(source), ClusterUID: "cluster-a"}
}

// replicaResource names the replica resource in the apiserver errors the
// rows inject.
var replicaResource = schema.GroupResource{Group: v1beta1.SchemeGroupVersion.Group, Resource: "inferencereplicas"}

// ensureWant is what one pass must do: the error it reports (a fragment,
// empty for none), the writes it makes and whether it hands a replica back.
type ensureWant struct {
	err       string
	creates   int
	patches   int
	committed bool
	// noLiveRead holds that the pass never read the live replica.
	noLiveRead bool
	// check reads the world after the pass for the row's own invariants.
	check func(t *testing.T, w *ensureWorld, committed *v1beta1.InferenceReplica)
}

// ensureRow is one rule of EnsureInferenceReplica.
type ensureRow struct {
	name  string
	world func(t *testing.T) *ensureWorld
	want  ensureWant
}

var ensureRows = []ensureRow{
	{name: "a nil client is refused before anything is read", world: func(t *testing.T) *ensureWorld {
		w := newEnsureWorld(t)
		w.params.Client = nil
		return w
	}, want: ensureWant{err: "EnsureInferenceReplica: nil client"}},
	{name: "an unreadable placement policy on the service is refused", world: func(t *testing.T) *ensureWorld {
		w := newEnsureWorld(t)
		w.isvc.Labels = map[string]string{constants.PlacementOrigin: "source-a"}
		w.isvc.Annotations = map[string]string{constants.PlacementExecution: "not a policy"}
		return w
	}, want: ensureWant{err: "invalid placement execution policy"}},
	{name: "a placement minimum that differs from the component's is refused", world: func(t *testing.T) *ensureWorld {
		w := newEnsureWorld(t)
		policy := memberPolicy("source-a")
		policy.ReplicaFloors = []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 5}}
		w.placement(policy)
		return w
	}, want: ensureWant{err: "differs from its accepted replica count"}},
	{name: "a paused placement without a component minimum is refused", world: func(t *testing.T) *ensureWorld {
		w := newEnsureWorld(t)
		policy := memberPolicy("source-a")
		policy.PauseSurge = true
		w.placement(policy)
		w.isvc.Spec.Engine.MinReplicas = nil
		return w
	}, want: ensureWant{err: "placement pause requires a resolved positive component"}},
	{name: "a replica at rest on the cached copy is handed back with no live read and no write", world: func(t *testing.T) *ensureWorld {
		w := newEnsureWorld(t)
		w.project()
		w.cacheShowing(w.stored())
		return w
	}, want: ensureWant{committed: true, noLiveRead: true, check: func(t *testing.T, w *ensureWorld, committed *v1beta1.InferenceReplica) {
		if diff := cmp.Diff(w.stored().Spec, committed.Spec); diff != "" {
			t.Errorf("committed spec (-stored +committed):\n%s", diff)
		}
	}}},
	{name: "no replica: one is created as the service's controlled child", world: func(t *testing.T) *ensureWorld {
		return newEnsureWorld(t)
	}, want: ensureWant{creates: 1, committed: true, check: func(t *testing.T, w *ensureWorld, committed *v1beta1.InferenceReplica) {
		stored := w.stored()
		if ref := metav1.GetControllerOf(stored); ref == nil || ref.UID != w.isvc.UID || ref.Kind != "InferenceService" {
			t.Errorf("controller ref = %+v, want the service %s", ref, w.isvc.UID)
		}
		if stored.Spec.Component != v1beta1.EngineComponent || committed.Name != stored.Name {
			t.Errorf("stored %s component %q, committed %s", stored.Name, stored.Spec.Component, committed.Name)
		}
		if stored.Annotations[constants.InferenceReplicaControllerWriteAnnotationKey] != constants.InferenceReplicaControllerWriteAnnotationVal {
			t.Errorf("annotations = %v, want the controller-write record", stored.Annotations)
		}
	}}},
	{name: "a create that finds the replica already there re-reads it and writes nothing more", world: func(t *testing.T) *ensureWorld {
		w := newEnsureWorld(t)
		w.project()
		w.cacheShowing()
		w.params.Reader = fakeclient.Wrap(t, w.live, fakeclient.FailingVerb(fakeclient.Get, 1, apierrors.NewNotFound(replicaResource, w.key().Name)))
		return w
	}, want: ensureWant{creates: 1, committed: true, check: func(t *testing.T, w *ensureWorld, committed *v1beta1.InferenceReplica) {
		if committed.UID != w.stored().UID {
			t.Errorf("committed %s, want the stored replica %s", committed.UID, w.stored().UID)
		}
	}}},
	{name: "a create the apiserver refuses is reported and nothing is committed", world: func(t *testing.T) *ensureWorld {
		return newEnsureWorld(t, fakeclient.FailingVerb(fakeclient.Create, fakeclient.EveryCall, errors.New("quota exceeded")))
	}, want: ensureWant{err: "create IR prod/example-engine: quota exceeded", creates: 1}},
	{name: "a live read that fails is reported, not read as a missing replica", world: func(t *testing.T) *ensureWorld {
		return newEnsureWorld(t, fakeclient.FailingVerb(fakeclient.Get, fakeclient.EveryCall, errors.New("apiserver down")).OnKind(&v1beta1.InferenceReplica{}))
	}, want: ensureWant{err: "get IR prod/example-engine: apiserver down"}},
	{name: "a live replica another owner holds under the projected name is refused and left as it is", world: func(t *testing.T) *ensureWorld {
		w := newEnsureWorld(t, fakeclient.WithObjects(&v1beta1.InferenceReplica{
			ObjectMeta: metav1.ObjectMeta{Name: "example-engine", Namespace: "prod", UID: "replica-uid"},
			Spec:       v1beta1.InferenceReplicaSpec{Component: v1beta1.EngineComponent},
		}))
		return w
	}, want: ensureWant{err: "not projected by InferenceService", check: func(t *testing.T, w *ensureWorld, _ *v1beta1.InferenceReplica) {
		if stored := w.stored(); stored.Spec.ParentRef != nil || len(stored.OwnerReferences) != 0 {
			t.Errorf("the other owner's replica was written: %+v", stored.ObjectMeta)
		}
	}}},
	{name: "a live replica whose placement identity differs is refused", world: func(t *testing.T) *ensureWorld {
		w := newEnsureWorld(t)
		w.placement(memberPolicy("source-a"))
		w.project()
		w.placement(memberPolicy("source-b"))
		return w
	}, want: ensureWant{err: "project IR prod/example-engine: placement execution identity changed"}},
	{name: "a live replica the projection leaves unchanged is handed back without a write", world: func(t *testing.T) *ensureWorld {
		w := newEnsureWorld(t)
		w.project()
		stale := w.stored()
		stale.Spec.Replicas = ptr.To(int32(9))
		w.cacheShowing(stale)
		return w
	}, want: ensureWant{committed: true, check: func(t *testing.T, w *ensureWorld, committed *v1beta1.InferenceReplica) {
		if diff := cmp.Diff(w.stored().Spec, committed.Spec); diff != "" {
			t.Errorf("committed spec (-stored +committed):\n%s", diff)
		}
	}}},
	{name: "a changed projection is patched onto the live replica", world: func(t *testing.T) *ensureWorld {
		w := newEnsureWorld(t)
		w.project()
		w.isvc.Spec.Engine.MinReplicas = ptr.To(3)
		return w
	}, want: ensureWant{patches: 1, committed: true, check: func(t *testing.T, w *ensureWorld, committed *v1beta1.InferenceReplica) {
		stored := w.stored()
		if stored.Spec.Replicas == nil || *stored.Spec.Replicas != 3 {
			t.Errorf("stored replicas = %v, want 3", stored.Spec.Replicas)
		}
		if diff := cmp.Diff(stored.Spec, committed.Spec); diff != "" {
			t.Errorf("committed spec (-stored +committed):\n%s", diff)
		}
		if len(w.patchData) != 1 || strings.Contains(w.patchData[0], "resourceVersion") {
			t.Errorf("patch = %v, want one merge patch without a version lock", w.patchData)
		}
	}}},
	{name: "a placement-managed projection patches under the live replica's version", world: func(t *testing.T) *ensureWorld {
		w := newEnsureWorld(t)
		w.placement(memberPolicy("source-a"))
		w.project()
		w.isvc.Spec.Engine.MinReplicas = ptr.To(3)
		return w
	}, want: ensureWant{patches: 1, committed: true, check: func(t *testing.T, w *ensureWorld, _ *v1beta1.InferenceReplica) {
		if len(w.patchData) != 1 || !strings.Contains(w.patchData[0], "resourceVersion") {
			t.Errorf("patch = %v, want one merge patch locked on the version", w.patchData)
		}
	}}},
	{name: "a patch the apiserver refuses is reported", world: func(t *testing.T) *ensureWorld {
		w := newEnsureWorld(t, fakeclient.FailingVerb(fakeclient.Patch, fakeclient.EveryCall, errors.New("denied")))
		w.project()
		w.isvc.Spec.Engine.MinReplicas = ptr.To(3)
		return w
	}, want: ensureWant{err: "patch IR prod/example-engine: denied", patches: 1}},
	{name: "a patch conflict is retried on the replica read again", world: func(t *testing.T) *ensureWorld {
		w := newEnsureWorld(t, fakeclient.FailingVerb(fakeclient.Patch, 1, apierrors.NewConflict(replicaResource, "example-engine", errors.New("the object has been modified"))))
		w.project()
		w.isvc.Spec.Engine.MinReplicas = ptr.To(3)
		return w
	}, want: ensureWant{patches: 2, committed: true, check: func(t *testing.T, w *ensureWorld, _ *v1beta1.InferenceReplica) {
		if stored := w.stored(); stored.Spec.Replicas == nil || *stored.Spec.Replicas != 3 {
			t.Errorf("stored replicas = %v, want 3 after the retry", stored.Spec.Replicas)
		}
	}}},
}

// TestEnsureInferenceReplicaRules holds every rule of one projection pass:
// which parameters and policies are refused before a read, when the cached
// copy is the answer, when the replica is created, re-read, left alone or
// patched, and which apiserver answers are reported or retried.
func TestEnsureInferenceReplicaRules(t *testing.T) {
	for _, row := range ensureRows {
		t.Run(row.name, func(t *testing.T) {
			w := row.world(t)
			creates, patches, liveGets := w.creates.Count(), w.patches.Count(), w.liveGets.Count()
			w.patchData = nil

			committed, err := EnsureInferenceReplica(context.Background(), w.params)

			switch {
			case row.want.err == "" && err != nil:
				t.Fatalf("error = %v, want none", err)
			case row.want.err != "" && (err == nil || !strings.Contains(err.Error(), row.want.err)):
				t.Fatalf("error = %v, want one containing %q", err, row.want.err)
			}
			if (committed != nil) != row.want.committed {
				t.Errorf("committed = %v, want committed %t", committed, row.want.committed)
			}
			if got := w.creates.Count() - creates; got != row.want.creates {
				t.Errorf("creates = %d, want %d", got, row.want.creates)
			}
			if got := w.patches.Count() - patches; got != row.want.patches {
				t.Errorf("patches = %d, want %d", got, row.want.patches)
			}
			if got := w.liveGets.Count() - liveGets; row.want.noLiveRead && got != 0 {
				t.Errorf("live reads = %d, want none", got)
			}
			if row.want.check != nil {
				row.want.check(t, w, committed)
			}
		})
	}
}
