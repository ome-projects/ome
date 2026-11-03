package routing

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/trafficdrain"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

const validDrain = `{"drain":{"cluster":"a","reason":"mitigation"}}`

// parseError is the error a pass returns for raw when its status write succeeds.
func parseError(t *testing.T, raw string) string {
	t.Helper()
	_, err := trafficdrain.Parse(raw)
	require.Error(t, err)
	return "parse " + constants.TrafficDrainAnnotation + ": " + err.Error()
}

func reconcileOnce(r *Reconciler) error {
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: testName, Namespace: testNS},
	})
	return err
}

// heldSplitReconciler builds a two-cluster Split source holding cluster a, runs
// one valid pass, and counts the status applies made after it.
func heldSplitReconciler(t *testing.T) (*Reconciler, client.WithWatch, *fakeclient.Calls) {
	t.Helper()
	isvc := splitISVC([]string{"a", "b"}, []int32{7, 3}, []int32{7, 3})
	isvc.Annotations = map[string]string{constants.TrafficDrainAnnotation: validDrain}
	applies := fakeclient.CountingVerb(fakeclient.StatusApply)
	c := newRoutingClient(t, []client.Object{isvc}, applies)
	r := &Reconciler{Client: c, APIReader: c, Log: log.Log, Config: controllerTestConfig()}
	reconcile(t, r)
	return r, c, applies
}

func TestReconcile_InvalidTrafficDrain_ReportsOverridesInvalid(t *testing.T) {
	r, c, _ := heldSplitReconciler(t)
	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	generation := tm.Generation
	routable := routableCond(tm).DeepCopy()
	capacity := capacityFallbackCond(tm).DeepCopy()

	for _, tc := range []struct {
		name string
		raw  string
	}{
		{name: "missing reason", raw: `{"drain":{"cluster":"a"}}`},
		{name: "unknown field", raw: `{"drain":{"cluster":"a","reason":"mitigation","weight":"0"}}`},
		{name: "trailing space", raw: `{"drain":{"cluster":"a ","reason":"mitigation"}}`},
		{name: "list instead of object", raw: `[{"cluster":"a","reason":"mitigation"}]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			updateDrainAnnotation(t, c, tc.raw)
			err := reconcileOnce(r)
			require.EqualError(t, err, parseError(t, tc.raw))

			tm, ok := getTrafficMap(t, c)
			require.True(t, ok)
			assert.Equal(t, generation, tm.Generation, "the last valid table is kept")
			assert.Equal(t, []string{"drain"}, tm.Spec.Entries[0].DrainRefs)
			override := overrideActiveCond(tm)
			require.NotNil(t, override)
			assert.Equal(t, metav1.ConditionUnknown, override.Status)
			assert.Equal(t, v1beta1.TrafficMapReasonOverridesInvalid, override.Reason)
			assert.Contains(t, override.Message, err.Error())
			assert.Equal(t, generation, override.ObservedGeneration)
			gotRoutable := routableCond(tm)
			require.NotNil(t, gotRoutable)
			assert.Equal(t, routable.Status, gotRoutable.Status, "Routable still describes the kept table")
			assert.Equal(t, routable.Reason, gotRoutable.Reason)
			gotCapacity := capacityFallbackCond(tm)
			require.NotNil(t, gotCapacity)
			assert.Equal(t, capacity.Reason, gotCapacity.Reason)
		})
	}
}

func TestReconcile_TrafficDrainFixedAfterInvalid_ReportsOverridesApplied(t *testing.T) {
	r, c, _ := heldSplitReconciler(t)

	updateDrainAnnotation(t, c, `{"drain":{"cluster":"b"}}`)
	require.Error(t, reconcileOnce(r))
	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	require.NotNil(t, overrideActiveCond(tm))
	require.Equal(t, v1beta1.TrafficMapReasonOverridesInvalid, overrideActiveCond(tm).Reason)

	updateDrainAnnotation(t, c, `{"drain":{"cluster":"b","reason":"mitigation"}}`)
	reconcile(t, r)

	tm, ok = getTrafficMap(t, c)
	require.True(t, ok)
	assert.Equal(t, int32(7), tm.Spec.Entries[0].Weight)
	assert.Equal(t, int32(0), tm.Spec.Entries[1].Weight)
	assert.Equal(t, []string{"drain"}, tm.Spec.Entries[1].DrainRefs)
	override := overrideActiveCond(tm)
	require.NotNil(t, override)
	assert.Equal(t, metav1.ConditionTrue, override.Status)
	assert.Equal(t, v1beta1.TrafficMapReasonOverridesApplied, override.Reason)
}

func TestReconcile_InvalidTrafficDrainUnchanged_WritesStatusOnce(t *testing.T) {
	r, c, applies := heldSplitReconciler(t)
	before := applies.Count()

	first := `{"drain":{"cluster":"a"}}`
	updateDrainAnnotation(t, c, first)
	for range 3 {
		require.EqualError(t, reconcileOnce(r), parseError(t, first))
	}
	assert.Equal(t, before+1, applies.Count(), "repeated passes over the same value write once")

	second := `{"drain":{"cluster":"a","reason":"mitigation","weight":"0"}}`
	updateDrainAnnotation(t, c, second)
	require.EqualError(t, reconcileOnce(r), parseError(t, second))
	assert.Equal(t, before+2, applies.Count(), "a different error is written again")
	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	assert.Contains(t, overrideActiveCond(tm).Message, parseError(t, second))
}

func TestReconcile_OversizedInvalidTrafficDrain_BoundsMessage(t *testing.T) {
	r, c, _ := heldSplitReconciler(t)

	raw := `{"` + strings.Repeat("é", 20000) + `":{"cluster":"a"}}`
	updateDrainAnnotation(t, c, raw)
	require.Error(t, reconcileOnce(r))

	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	override := overrideActiveCond(tm)
	require.NotNil(t, override)
	assert.Equal(t, v1beta1.TrafficMapReasonOverridesInvalid, override.Reason)
	assert.LessOrEqual(t, len(override.Message), maxConditionMessageBytes)
	assert.True(t, strings.HasPrefix(override.Message, "parse "+constants.TrafficDrainAnnotation))
	assert.Contains(t, override.Message, "...; the last valid route table is kept")
	assert.NotContains(t, override.Message, "�", "the cut keeps whole characters")
}

func TestReconcile_InvalidTrafficDrainBeforeFirstMap_CreatesNoTrafficMap(t *testing.T) {
	raw := `{"drain":{"cluster":"a"}}`
	isvc := splitISVC([]string{"a", "b"}, []int32{7, 3}, []int32{7, 3})
	isvc.Annotations = map[string]string{constants.TrafficDrainAnnotation: raw}
	applies := fakeclient.CountingVerb(fakeclient.StatusApply)
	c := newRoutingClient(t, []client.Object{isvc}, applies)
	r := &Reconciler{Client: c, APIReader: c, Log: log.Log, Config: controllerTestConfig()}

	require.EqualError(t, reconcileOnce(r), parseError(t, raw))
	_, exists := getTrafficMap(t, c)
	assert.False(t, exists)
	assert.Zero(t, applies.Count())
}

func TestReconcile_InvalidTrafficDrainOnUnownedMap_LeavesMapUntouched(t *testing.T) {
	raw := `{"drain":{"cluster":"a"}}`
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	isvc.Annotations = map[string]string{constants.TrafficDrainAnnotation: raw}
	applies := fakeclient.CountingVerb(fakeclient.StatusApply)
	c := newRoutingClient(t, []client.Object{isvc, unownedTrafficMap()}, applies)
	r := &Reconciler{Client: c, APIReader: c, Log: log.Log, Config: controllerTestConfig()}

	require.EqualError(t, reconcileOnce(r), parseError(t, raw))
	tm, exists := getTrafficMap(t, c)
	require.True(t, exists)
	assert.Empty(t, tm.Status.Conditions)
	assert.Empty(t, tm.Status.SourceUID)
	assert.Zero(t, applies.Count())
}

func TestReconcile_InvalidTrafficDrainWithMismatchedSourceUID_ReturnsSourceUIDError(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	isvc.Annotations = map[string]string{constants.TrafficDrainAnnotation: `{"drain":{"cluster":"a"}}`}
	tm := &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testName,
			Namespace: testNS,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(isvc, v1beta1.SchemeGroupVersion.WithKind("InferenceService")),
			},
		},
		Spec:   v1beta1.TrafficMapSpec{Service: "existing-service"},
		Status: v1beta1.TrafficMapStatus{SourceUID: "other-source-uid"},
	}
	applies := fakeclient.CountingVerb(fakeclient.StatusApply)
	c := newRoutingClient(t, []client.Object{isvc, tm}, applies)
	r := &Reconciler{Client: c, APIReader: c, Log: log.Log, Config: controllerTestConfig()}

	err := reconcileOnce(r)
	require.Error(t, err)
	assert.ErrorContains(t, err, "source UID")
	current, exists := getTrafficMap(t, c)
	require.True(t, exists)
	assert.Equal(t, types.UID("other-source-uid"), current.Status.SourceUID)
	assert.Empty(t, current.Status.Conditions)
	assert.Zero(t, applies.Count())
}

func TestReconcile_InvalidTrafficDrainStatusConflict_RetriesWrite(t *testing.T) {
	r, c, _ := heldSplitReconciler(t)
	raw := `{"drain":{"cluster":"a"}}`
	updateDrainAnnotation(t, c, raw)
	r.Client = fakeclient.Wrap(t, c, fakeclient.FailingVerb(fakeclient.StatusApply, 1, conflictOnDelete(unownedTrafficMap())))

	require.EqualError(t, reconcileOnce(r), parseError(t, raw), "a conflict is retried, not reported")
	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	assert.Equal(t, v1beta1.TrafficMapReasonOverridesInvalid, overrideActiveCond(tm).Reason)
}

func TestReconcile_InvalidTrafficDrainStatusRefused_ReturnsBothErrors(t *testing.T) {
	r, c, _ := heldSplitReconciler(t)
	raw := `{"drain":{"cluster":"a"}}`
	updateDrainAnnotation(t, c, raw)
	refused := apierrors.NewForbidden(v1beta1.Resource("trafficmaps"), testName, errors.New("status writes refused"))
	r.Client = fakeclient.Wrap(t, c, fakeclient.FailingVerb(fakeclient.StatusApply, fakeclient.EveryCall, refused))

	err := reconcileOnce(r)
	require.Error(t, err)
	assert.ErrorIs(t, err, refused)
	assert.ErrorContains(t, err, parseError(t, raw))
}
