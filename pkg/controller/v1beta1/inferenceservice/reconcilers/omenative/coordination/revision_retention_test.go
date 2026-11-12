package coordination

import (
	"context"
	"errors"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// retentionReplica is an engine replica whose status names version 2
// (hash2) as its update revision and version 1 (hash1) as its current one.
func retentionReplica(isvc *v1beta1.InferenceService) *v1beta1.InferenceReplica {
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: isvc.Name + "-engine", Namespace: isvc.Namespace, UID: types.UID(isvc.Name + "-engine-uid")},
		Spec:       v1beta1.InferenceReplicaSpec{Component: v1beta1.EngineComponent},
		Status: v1beta1.InferenceReplicaStatus{
			CurrentRevision: isvc.Name + "-engine-hash1",
			UpdateRevision:  isvc.Name + "-engine-hash2",
		},
	}
}

// retentionRevision is the ControllerRevision of one engine revision,
// controlled by the replica when controlled is set and by nothing otherwise.
func retentionRevision(isvc *v1beta1.InferenceService, ir *v1beta1.InferenceReplica, hash string, controlled bool) *appsv1.ControllerRevision {
	cr := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: isvc.Name + "-engine-" + hash, Namespace: isvc.Namespace}}
	if controlled {
		cr.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(ir, v1beta1.SchemeGroupVersion.WithKind("InferenceReplica"))}
	}
	return cr
}

func pointAt(ir *v1beta1.InferenceReplica, name string) *v1beta1.InferenceReplica {
	ir.Spec.Pacing = &v1beta1.InferenceReplicaPacing{RollbackToRevision: &name}
	return ir
}

// The revision a replica rolls toward: its rollback target while that
// ControllerRevision is retained for it, else its update revision, else its
// current one; nothing for no replica.
func TestReplicaRollTargetHash(t *testing.T) {
	isvc := testOMENativeISVC()
	for _, tc := range []struct {
		name string
		ir   *v1beta1.InferenceReplica
		objs []runtime.Object
		want string
	}{
		{name: "no replica", ir: nil, want: ""},
		{name: "update revision", ir: retentionReplica(isvc), want: "hash2"},
		{name: "current revision when no update revision is reported", ir: func() *v1beta1.InferenceReplica {
			ir := retentionReplica(isvc)
			ir.Status.UpdateRevision = ""
			return ir
		}(), want: "hash1"},
		{name: "no revision reported", ir: &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Name: isvc.Name + "-engine", Namespace: isvc.Namespace}}, want: ""},
		{name: "retained rollback target", ir: pointAt(retentionReplica(isvc), isvc.Name+"-engine-hash1"),
			objs: []runtime.Object{retentionRevision(isvc, retentionReplica(isvc), "hash1", true)}, want: "hash1"},
		{name: "rollback target outside the replica's history", ir: pointAt(retentionReplica(isvc), isvc.Name+"-engine-hash1"),
			objs: []runtime.Object{retentionRevision(isvc, retentionReplica(isvc), "hash1", false)}, want: "hash2"},
		{name: "rollback target gone", ir: pointAt(retentionReplica(isvc), isvc.Name+"-engine-hash1"), want: "hash2"},
		{name: "empty rollback target", ir: pointAt(retentionReplica(isvc), ""), want: "hash2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ReplicaRollTargetHash(context.Background(), testClient(tc.objs...), tc.ir)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("roll target: got %q want %q", got, tc.want)
			}
		})
	}
}

// failingReader answers every Get with the same error.
type failingReader struct {
	client.Reader
	err error
}

func (f failingReader) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	return f.err
}

// A read of the rollback target that fails for any reason but absence
// propagates: the sweep must not guess an empty target.
func TestReplicaRollTargetHash_ReadErrorPropagates(t *testing.T) {
	isvc := testOMENativeISVC()
	ir := pointAt(retentionReplica(isvc), isvc.Name+"-engine-hash1")
	boom := errors.New("apiserver unavailable")
	_, err := ReplicaRollTargetHash(context.Background(), failingReader{err: boom}, ir)
	if !errors.Is(err, boom) {
		t.Fatalf("read error must propagate, got %v", err)
	}
}

// The sweep keeps the revision the replica rolls toward with no pod behind
// it, and under a retained rollback target that revision is the rollback
// one: the spec target's Services, with no pod, go. The rule is the same
// whether the sweep reads the replica or a caller hands it the replica it
// read.
func TestSweepPerRevisionServices_KeepsTheRollTargetOnly(t *testing.T) {
	isvc := testOMENativeISVC()
	ir := retentionReplica(isvc)
	services := func(hash string) []runtime.Object {
		return []runtime.Object{
			perRevisionServiceFixture(isvc, v1beta1.EngineComponent, hash, PerRevisionServiceName(isvc.Name, v1beta1.EngineComponent, hash)),
			perRevisionServiceFixture(isvc, v1beta1.EngineComponent, hash, PerRevisionHeadlessServiceName(isvc.Name, v1beta1.EngineComponent, hash)),
		}
	}
	for _, tc := range []struct {
		name          string
		ir            *v1beta1.InferenceReplica
		objs          []runtime.Object
		kept, swept   []string
		livePodHashes map[string]int32
	}{
		{name: "update revision kept with no pod, stale revision swept",
			ir: ir, livePodHashes: map[string]int32{"hash1": 1},
			kept: []string{"hash1", "hash2"}, swept: []string{"hash0"}},
		{name: "retained rollback target kept, spec target with no pod swept",
			ir:            pointAt(retentionReplica(isvc), isvc.Name+"-engine-hash1"),
			objs:          []runtime.Object{retentionRevision(isvc, ir, "hash1", true)},
			livePodHashes: map[string]int32{"hash0": 1},
			kept:          []string{"hash0", "hash1"}, swept: []string{"hash2"}},
		{name: "no replica keeps only the revisions with pods",
			ir: nil, livePodHashes: map[string]int32{"hash1": 1},
			kept: []string{"hash1"}, swept: []string{"hash0", "hash2"}},
	} {
		for _, handed := range []bool{false, true} {
			name := tc.name + ", the replica read by the sweep"
			if handed {
				name = tc.name + ", the replica handed by the caller"
			}
			t.Run(name, func(t *testing.T) {
				objs := append([]runtime.Object{}, tc.objs...)
				if tc.ir != nil {
					objs = append(objs, tc.ir)
				}
				for _, hash := range []string{"hash0", "hash1", "hash2"} {
					objs = append(objs, services(hash)...)
				}
				c := testClient(objs...)
				var err error
				if handed {
					err = SweepPerRevisionServicesOfReplica(context.Background(), c, c, isvc, v1beta1.EngineComponent, tc.ir.DeepCopy(), tc.livePodHashes)
				} else {
					err = SweepPerRevisionServices(context.Background(), c, c, isvc, v1beta1.EngineComponent, tc.livePodHashes)
				}
				if err != nil {
					t.Fatalf("sweep: %v", err)
				}
				expectEngineServices(t, c, isvc, tc.kept, true)
				expectEngineServices(t, c, isvc, tc.swept, false)
			})
		}
	}
}

// expectEngineServices checks that both Services of each engine revision
// exist (present) or are gone (not present).
func expectEngineServices(t *testing.T, c client.Client, isvc *v1beta1.InferenceService, hashes []string, present bool) {
	t.Helper()
	for _, hash := range hashes {
		for _, name := range []string{PerRevisionServiceName(isvc.Name, v1beta1.EngineComponent, hash), PerRevisionHeadlessServiceName(isvc.Name, v1beta1.EngineComponent, hash)} {
			err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: name}, &corev1.Service{})
			switch {
			case present && err != nil:
				t.Errorf("%s must be kept: %v", name, err)
			case !present && !apierrors.IsNotFound(err):
				t.Errorf("%s must be swept, Get returned %v", name, err)
			}
		}
	}
}

// A sweep whose replica read fails deletes nothing and reports the error.
func TestSweepPerRevisionServices_ReadErrorDeletesNothing(t *testing.T) {
	isvc := testOMENativeISVC()
	ir := pointAt(retentionReplica(isvc), isvc.Name+"-engine-hash1")
	name := PerRevisionServiceName(isvc.Name, v1beta1.EngineComponent, "hash2")
	c := testClient(ir, perRevisionServiceFixture(isvc, v1beta1.EngineComponent, "hash2", name))
	boom := errors.New("apiserver unavailable")
	err := SweepPerRevisionServices(context.Background(), c, failingReader{err: boom}, isvc, v1beta1.EngineComponent, map[string]int32{"hash1": 1})
	if !errors.Is(err, boom) {
		t.Fatalf("read error must propagate, got %v", err)
	}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: name}, &corev1.Service{}); err != nil {
		t.Fatalf("nothing may be deleted on a failed read: %v", err)
	}
}

// A sweep handed the replica still reads the ControllerRevision its
// rollback target names; when that read fails it deletes nothing and
// reports the error.
func TestSweepPerRevisionServicesOfReplica_RevisionReadErrorDeletesNothing(t *testing.T) {
	isvc := testOMENativeISVC()
	ir := pointAt(retentionReplica(isvc), isvc.Name+"-engine-hash1")
	name := PerRevisionServiceName(isvc.Name, v1beta1.EngineComponent, "hash2")
	c := testClient(perRevisionServiceFixture(isvc, v1beta1.EngineComponent, "hash2", name))
	boom := errors.New("apiserver unavailable")
	err := SweepPerRevisionServicesOfReplica(context.Background(), c, failingReader{err: boom}, isvc, v1beta1.EngineComponent, ir, map[string]int32{"hash1": 1})
	if !errors.Is(err, boom) {
		t.Fatalf("read error must propagate, got %v", err)
	}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: name}, &corev1.Service{}); err != nil {
		t.Fatalf("nothing may be deleted on a failed read: %v", err)
	}
}

// A nil client is a no-op, as every sweep caller without a writer expects.
func TestSweepPerRevisionServices_NilClient(t *testing.T) {
	if err := SweepPerRevisionServices(context.Background(), nil, nil, testOMENativeISVC(), v1beta1.EngineComponent, nil); err != nil {
		t.Fatalf("nil client: %v", err)
	}
	if err := SweepPerRevisionServicesOfReplica(context.Background(), nil, nil, testOMENativeISVC(), v1beta1.EngineComponent, nil, nil); err != nil {
		t.Fatalf("nil client: %v", err)
	}
}
