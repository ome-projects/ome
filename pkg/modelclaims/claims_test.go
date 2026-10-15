package modelclaims

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

var key = types.NamespacedName{Namespace: "customer", Name: "model"}

const uid types.UID = "model-uid"
const residency = "ome.io/artifact-residency"

func newClient(t *testing.T) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{
		Namespace: key.Namespace, Name: key.Name, UID: uid, ResourceVersion: "7",
		Annotations: map[string]string{"keep": "annotation"}, Labels: map[string]string{"keep": "label"},
	}}
	model.Status.State = v1beta1.LifeCycleStateReady
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(model).Build()
}

func readModel(t *testing.T, c client.Reader) *v1beta1.BaseModel {
	t.Helper()
	model := &v1beta1.BaseModel{}
	require.NoError(t, c.Get(context.Background(), key, model))
	return model
}

// The hook changes the stored object before the real fake-client Patch. A stale
// resourceVersion must conflict. Acquire returns it; Release and Evict reread.
type hookedWriter struct {
	client.Writer
	beforePatch func()
	patches     int
}

func (w *hookedWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	w.patches++
	if w.beforePatch != nil {
		w.beforePatch()
	}
	return w.Writer.Patch(ctx, obj, patch, opts...)
}

func TestClaimWinsEvictionRace(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	writer := &hookedWriter{Writer: c}
	writer.beforePatch = func() {
		writer.beforePatch = nil
		require.NoError(t, NewCoordinator(c, c).Acquire(ctx, readModel(t, c), "endpoint-a"))
	}
	err := NewCoordinator(c, writer).Evict(ctx, key, uid)
	require.ErrorIs(t, err, ErrInUse)
	require.Equal(t, 1, writer.patches)
	model := readModel(t, c)
	require.Empty(t, model.Annotations[residency])
	require.JSONEq(t, `{"endpoint-a":true}`, model.Annotations[constants.ModelArtifactUseClaimsAnnotation])
}

func TestEvictionWinsClaimRace(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	writer := &hookedWriter{Writer: c}
	writer.beforePatch = func() {
		writer.beforePatch = nil
		require.NoError(t, NewCoordinator(c, c).Evict(ctx, key, uid))
	}
	err := NewCoordinator(c, writer).Acquire(ctx, readModel(t, c), "endpoint-a")
	require.True(t, apierrors.IsConflict(err), "%v", err)
	require.Equal(t, 1, writer.patches)
	model := readModel(t, c)
	require.Equal(t, "Evicted", model.Annotations[residency])
	require.Empty(t, model.Annotations[constants.ModelArtifactUseClaimsAnnotation])
}

func TestConcurrentClaimsPreserveBothEndpoints(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	var arrived sync.WaitGroup
	arrived.Add(2)
	var patches atomic.Int32
	writer := &barrierWriter{Writer: c, before: func() {
		if patches.Add(1) <= 2 {
			arrived.Done()
			arrived.Wait()
		}
	}}
	coordinator := NewCoordinator(c, writer)
	type result struct {
		claimID string
		err     error
	}
	results := make(chan result, 2)
	snapshot := readModel(t, c)
	for _, id := range []string{"endpoint-a", "endpoint-b"} {
		go func(id string) { results <- result{id, coordinator.Acquire(ctx, snapshot, id)} }(id)
	}
	first, second := <-results, <-results
	if first.err == nil {
		first, second = second, first
	}
	require.NoError(t, second.err)
	require.True(t, apierrors.IsConflict(first.err), "%v", first.err)
	require.Equal(t, int32(2), patches.Load(), "Acquire must not retry internally")

	// The losing caller rereads and confirms readiness before merging its claim.
	current := readModel(t, c)
	require.Equal(t, v1beta1.LifeCycleStateReady, current.Status.State)
	require.Empty(t, current.Annotations[residency])
	require.NoError(t, coordinator.Acquire(ctx, current, first.claimID))

	require.JSONEq(t, `{"endpoint-a":true,"endpoint-b":true}`, readModel(t, c).Annotations[constants.ModelArtifactUseClaimsAnnotation])
}

type barrierWriter struct {
	client.Writer
	before func()
}

func (w *barrierWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	w.before()
	return w.Writer.Patch(ctx, obj, patch, opts...)
}

func TestReleasePreservesConcurrentClaim(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	coordinator := NewCoordinator(c, c)
	require.NoError(t, coordinator.Acquire(ctx, readModel(t, c), "endpoint-a"))
	writer := &hookedWriter{Writer: c}
	writer.beforePatch = func() {
		writer.beforePatch = nil
		require.NoError(t, coordinator.Acquire(ctx, readModel(t, c), "endpoint-b"))
	}
	require.NoError(t, NewCoordinator(c, writer).Release(ctx, key, uid, "endpoint-a"))
	require.Equal(t, 2, writer.patches)
	require.JSONEq(t, `{"endpoint-b":true}`, readModel(t, c).Annotations[constants.ModelArtifactUseClaimsAnnotation])
}

func TestIdempotencyAndReleaseAfterRestart(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	coordinator := NewCoordinator(c, c)
	for i := 0; i < 2; i++ {
		require.NoError(t, coordinator.Acquire(ctx, readModel(t, c), "endpoint-a"))
	}
	model := readModel(t, c)
	require.JSONEq(t, `{"endpoint-a":true}`, model.Annotations[constants.ModelArtifactUseClaimsAnnotation])
	require.Equal(t, "annotation", model.Annotations["keep"])
	require.Equal(t, "label", model.Labels["keep"])
	require.Equal(t, v1beta1.LifeCycleStateReady, model.Status.State)

	// The workflow resumes with the same ID. No in-memory ownership or TTL is used.
	restarted := NewCoordinator(c, c)
	for i := 0; i < 2; i++ {
		require.NoError(t, restarted.Release(ctx, key, uid, "endpoint-a"))
	}
	held, err := HasClaims(readModel(t, c).Annotations)
	require.NoError(t, err)
	require.False(t, held)
	require.NoError(t, restarted.Evict(ctx, key, uid))
}

func TestReadinessChangeReturnsConflictToCaller(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	writer := &hookedWriter{Writer: c}
	writer.beforePatch = func() {
		writer.beforePatch = nil
		model := readModel(t, c)
		model.Status.State = v1beta1.LifeCycleStateFailed
		require.NoError(t, c.Update(ctx, model))
	}
	err := NewCoordinator(c, writer).Acquire(ctx, readModel(t, c), "endpoint-a")
	require.True(t, apierrors.IsConflict(err), "%v", err)
	require.Equal(t, 1, writer.patches)
	require.Empty(t, readModel(t, c).Annotations[constants.ModelArtifactUseClaimsAnnotation])
}

func TestInvalidClaimsAreNeverOverwritten(t *testing.T) {
	for _, raw := range []string{"", "null", "[]", "broken", `{"endpoint-a":false}`, `{"endpoint-a":null}`, `{"":true}`} {
		t.Run(raw, func(t *testing.T) {
			ctx := context.Background()
			c := newClient(t)
			model := readModel(t, c)
			model.Annotations[constants.ModelArtifactUseClaimsAnnotation] = raw
			require.NoError(t, c.Update(ctx, model))
			writer := &hookedWriter{Writer: c}
			coordinator := NewCoordinator(c, writer)
			_, err := HasClaims(model.Annotations)
			require.Error(t, err)
			require.Error(t, coordinator.Acquire(ctx, readModel(t, c), "endpoint-a"))
			require.Error(t, coordinator.Evict(ctx, key, uid))
			require.Error(t, coordinator.Release(ctx, key, uid, "endpoint-a"))
			require.Zero(t, writer.patches)
			require.Equal(t, raw, readModel(t, c).Annotations[constants.ModelArtifactUseClaimsAnnotation])
		})
	}
}

func TestEmptyAndOccupiedClaims(t *testing.T) {
	for _, tc := range []struct {
		annotations map[string]string
		want        bool
	}{
		{nil, false},
		{map[string]string{constants.ModelArtifactUseClaimsAnnotation: `{}`}, false},
		{map[string]string{constants.ModelArtifactUseClaimsAnnotation: `{"endpoint-a":true}`}, true},
	} {
		got, err := HasClaims(tc.annotations)
		require.NoError(t, err)
		require.Equal(t, tc.want, got)
	}
}

func TestUIDReplacementStopsRetry(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	writer := &hookedWriter{Writer: c}
	writer.beforePatch = func() {
		writer.beforePatch = nil
		old := readModel(t, c)
		require.NoError(t, c.Delete(ctx, old))
		replacement := old.DeepCopy()
		replacement.UID = "replacement"
		replacement.ResourceVersion = ""
		require.NoError(t, c.Create(ctx, replacement))
	}
	err := NewCoordinator(c, writer).Evict(ctx, key, uid)
	require.ErrorContains(t, err, "UID")
	require.Empty(t, readModel(t, c).Annotations[residency])
}

func TestConflictsAreBounded(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	writer := &hookedWriter{Writer: c}
	writer.beforePatch = func() {
		model := readModel(t, c)
		model.Annotations["concurrent-writer"] = fmt.Sprint(writer.patches)
		require.NoError(t, c.Update(ctx, model))
	}
	err := NewCoordinator(c, writer).Evict(ctx, key, uid)
	require.True(t, apierrors.IsConflict(err), "%v", err)
	require.LessOrEqual(t, writer.patches, 5)
	require.Empty(t, readModel(t, c).Annotations[residency])
}

func TestAcquireUsesCallerSnapshotWithoutReadingOrMutatingIt(t *testing.T) {
	c := newClient(t)
	model := readModel(t, c)
	before := model.DeepCopy()
	// A nil reader proves Acquire only uses the supplied, validated snapshot.
	require.NoError(t, NewCoordinator(nil, c).Acquire(context.Background(), model, "endpoint-a"))
	require.Equal(t, before, model)
	require.JSONEq(t, `{"endpoint-a":true}`, readModel(t, c).Annotations[constants.ModelArtifactUseClaimsAnnotation])
}

func TestRequiredInputsDoNotWrite(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	writer := &hookedWriter{Writer: c}
	coordinator := NewCoordinator(c, writer)
	require.Error(t, coordinator.Acquire(ctx, nil, "endpoint-a"))
	require.Error(t, coordinator.Acquire(ctx, readModel(t, c), ""))
	for _, missing := range []string{"namespace", "name", "uid", "resourceVersion"} {
		model := readModel(t, c)
		switch missing {
		case "namespace":
			model.Namespace = ""
		case "name":
			model.Name = ""
		case "uid":
			model.UID = ""
		case "resourceVersion":
			model.ResourceVersion = ""
		}
		require.Error(t, coordinator.Acquire(ctx, model, "endpoint-a"), missing)
	}
	require.Error(t, coordinator.Release(ctx, key, uid, ""))
	require.Error(t, coordinator.Evict(ctx, key, ""))
	require.Error(t, coordinator.Evict(ctx, types.NamespacedName{Name: key.Name}, uid))
	require.Zero(t, writer.patches)
}

// Run against a local API server when envtest assets are configured. This checks
// actual merge-patch preconditions in addition to the fast fake-client suite.
func TestConditionalWritesWithAPIServer(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("set KUBEBUILDER_ASSETS to run local API server validation")
	}
	env := &envtest.Environment{CRDDirectoryPaths: []string{"../../config/crd/full/ome.io_basemodels.yaml"}, ErrorIfCRDPathMissing: true}
	config, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Stop()) })
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	c, err := client.New(config, client.Options{Scheme: scheme})
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: key.Namespace}}))

	for _, claimFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("claim_first=%v", claimFirst), func(t *testing.T) {
			uri := "hf://org/model"
			model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, GenerateName: "model-"},
				Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: &uri}}}
			require.NoError(t, c.Create(ctx, model))
			model.Status.State = v1beta1.LifeCycleStateReady
			require.NoError(t, c.Status().Update(ctx, model))
			modelKey := client.ObjectKeyFromObject(model)
			writer := &hookedWriter{Writer: c}
			writer.beforePatch = func() {
				writer.beforePatch = nil
				other := NewCoordinator(c, c)
				if claimFirst {
					require.NoError(t, other.Acquire(ctx, model, "endpoint-a"))
				} else {
					require.NoError(t, other.Evict(ctx, modelKey, model.UID))
				}
			}
			coordinator := NewCoordinator(c, writer)
			if claimFirst {
				require.ErrorIs(t, coordinator.Evict(ctx, modelKey, model.UID), ErrInUse)
			} else {
				err := coordinator.Acquire(ctx, model, "endpoint-a")
				require.True(t, apierrors.IsConflict(err), "%v", err)
			}
			current := &v1beta1.BaseModel{}
			require.NoError(t, c.Get(ctx, modelKey, current))
			held, err := HasClaims(current.Annotations)
			require.NoError(t, err)
			require.Equal(t, claimFirst, held)
			require.Equal(t, !claimFirst, current.Annotations[residency] == "Evicted")
		})
	}
}

func TestNewReservationStopsEvictionRetry(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	writer := &hookedWriter{Writer: c}
	writer.beforePatch = func() {
		writer.beforePatch = nil
		model := readModel(t, c)
		model.Labels[constants.ReserveModelArtifact] = "true"
		require.NoError(t, c.Update(ctx, model))
	}
	err := NewCoordinator(c, writer).Evict(ctx, key, uid)
	require.ErrorContains(t, err, "reserved")
	require.Empty(t, readModel(t, c).Annotations[residency])
}

func TestVendorModelCannotBeEvicted(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	model := readModel(t, c)
	uri := "vendor://model"
	model.Spec.Storage = &v1beta1.StorageSpec{StorageUri: &uri}
	require.NoError(t, c.Update(ctx, model))
	require.ErrorContains(t, NewCoordinator(c, c).Evict(ctx, key, uid), "vendor")
	require.Empty(t, readModel(t, c).Annotations[residency])
}
