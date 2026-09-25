package routing

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/apis"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/trafficdrain"
)

func overrideActiveCond(tm *v1beta1.TrafficMap) *metav1.Condition {
	return apimeta.FindStatusCondition(tm.Status.Conditions, v1beta1.TrafficMapOverrideActive)
}

func TestRoutingTableChange_ManualDrainAnnotation(t *testing.T) {
	first := `{"drain":{"cluster":"a","reason":"maintenance"}}`
	second := `{"drain":{"cluster":"b","reason":"maintenance"}}`
	for _, tc := range []struct {
		name   string
		before map[string]string
		after  map[string]string
		want   bool
	}{
		{name: "add", after: map[string]string{constants.TrafficDrainAnnotation: first}, want: true},
		{name: "change", before: map[string]string{constants.TrafficDrainAnnotation: first}, after: map[string]string{constants.TrafficDrainAnnotation: second}, want: true},
		{name: "remove", before: map[string]string{constants.TrafficDrainAnnotation: first}, want: true},
		{name: "add empty value", after: map[string]string{constants.TrafficDrainAnnotation: ""}, want: true},
		{name: "unchanged", before: map[string]string{constants.TrafficDrainAnnotation: first}, after: map[string]string{constants.TrafficDrainAnnotation: first}},
		{name: "unrelated", after: map[string]string{"unrelated": "value"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := splitISVC([]string{"a", "b"}, []int32{5, 5}, []int32{5, 5})
			after := before.DeepCopy()
			before.Annotations = tc.before
			after.Annotations = tc.after
			assert.Equal(t, before.Generation, after.Generation)
			assert.Equal(t, tc.want, routingTableChange.Update(event.UpdateEvent{ObjectOld: before, ObjectNew: after}))
		})
	}
}

func TestReconcile_TrafficDrainAnnotationComposesAndRemovalRestoresComputedWeight(t *testing.T) {
	isvc := splitISVC([]string{"a", "b"}, []int32{7, 3}, []int32{7, 3})
	isvc.Annotations = map[string]string{
		constants.TrafficDrainAnnotation: `{
			"drain-b":{"cluster":"a","reason":"second operator hold"},
			"drain-a":{"cluster":"a","reason":"first operator hold"}
		}`,
	}
	r, c := newReconciler(t, controllerTestConfig(), isvc)

	reconcile(t, r)
	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	require.Len(t, tm.Spec.Entries, 2)
	assert.Equal(t, int32(0), tm.Spec.Entries[0].Weight)
	assert.True(t, tm.Spec.Entries[0].Healthy, "manual drain must not rewrite automatic health")
	assert.Equal(t, []string{"drain-a", "drain-b"}, tm.Spec.Entries[0].DrainRefs)
	assert.Equal(t, int32(3), tm.Spec.Entries[1].Weight)
	override := overrideActiveCond(tm)
	require.NotNil(t, override)
	assert.Equal(t, metav1.ConditionTrue, override.Status)
	assert.Equal(t, v1beta1.TrafficMapReasonOverridesApplied, override.Reason)
	assert.Contains(t, override.Message, "2 of 2")

	updateDrainAnnotation(t, c, `{"drain-b":{"cluster":"a","reason":"second operator hold"}}`)
	reconcile(t, r)
	tm, ok = getTrafficMap(t, c)
	require.True(t, ok)
	assert.Equal(t, int32(0), tm.Spec.Entries[0].Weight,
		"one remaining override must keep the arm drained")
	assert.Equal(t, []string{"drain-b"}, tm.Spec.Entries[0].DrainRefs)

	cur := &v1beta1.InferenceService{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: testName, Namespace: testNS}, cur))
	cur.Status.Placement.Candidates[0].AdmittedReplicas = 8
	cur.Status.Placement.Candidates[0].ReadyReplicas = 8
	cur.Status.Placement.Candidates[1].AdmittedReplicas = 2
	cur.Status.Placement.Candidates[1].ReadyReplicas = 2
	require.NoError(t, c.Status().Update(context.Background(), cur))
	reconcile(t, r)
	tm, ok = getTrafficMap(t, c)
	require.True(t, ok)
	assert.Equal(t, int32(0), tm.Spec.Entries[0].Weight,
		"capacity changes cannot raise a manually drained arm")
	assert.Equal(t, int32(1), tm.Spec.Entries[1].Weight)

	updateDrainAnnotation(t, c, "")
	reconcile(t, r)
	tm, ok = getTrafficMap(t, c)
	require.True(t, ok)
	assert.Equal(t, int32(4), tm.Spec.Entries[0].Weight,
		"removing the final override recomputes the current automatic weight")
	assert.Empty(t, tm.Spec.Entries[0].DrainRefs)
	assert.Equal(t, int32(1), tm.Spec.Entries[1].Weight)
	override = overrideActiveCond(tm)
	require.NotNil(t, override)
	assert.Equal(t, metav1.ConditionFalse, override.Status)
	assert.Equal(t, v1beta1.TrafficMapReasonNoOverrides, override.Reason)
}

func updateDrainAnnotation(t *testing.T, c client.Client, raw string) {
	t.Helper()
	cur := &v1beta1.InferenceService{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: testName, Namespace: testNS}, cur))
	if raw == "" {
		delete(cur.Annotations, constants.TrafficDrainAnnotation)
	} else {
		if cur.Annotations == nil {
			cur.Annotations = make(map[string]string)
		}
		cur.Annotations[constants.TrafficDrainAnnotation] = raw
	}
	require.NoError(t, c.Update(context.Background(), cur))
}

func TestReconcile_TrafficDrainAnnotationCanExplicitlyZeroEveryArm(t *testing.T) {
	isvc := splitISVC([]string{"a", "b"}, []int32{7, 3}, []int32{7, 3})
	isvc.Annotations = map[string]string{
		constants.TrafficDrainAnnotation: `{
			"drain-a":{"cluster":"a","reason":"regional mitigation"},
			"drain-b":{"cluster":"b","reason":"regional mitigation"}
		}`,
	}
	r, c := newReconciler(t, controllerTestConfig(), isvc)

	reconcile(t, r)
	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	require.Len(t, tm.Spec.Entries, 2)
	assert.Equal(t, int32(0), tm.Spec.Entries[0].Weight)
	assert.Equal(t, []string{"drain-a"}, tm.Spec.Entries[0].DrainRefs)
	assert.Equal(t, int32(0), tm.Spec.Entries[1].Weight)
	assert.Equal(t, []string{"drain-b"}, tm.Spec.Entries[1].DrainRefs)
	cond := routableCond(tm)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, v1beta1.TrafficMapReasonTrafficDrain, cond.Reason)
}

func TestReconcile_PendingTrafficDrainAppliesWhenArmBecomesRoutable(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{7}, []int32{7})
	isvc.Annotations = map[string]string{
		constants.TrafficDrainAnnotation: `{"turnup-hold":{"cluster":"b","reason":"hold new cluster until verified"}}`,
	}
	r, c := newReconciler(t, controllerTestConfig(), isvc)

	reconcile(t, r)
	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	require.Len(t, tm.Spec.Entries, 1)
	assert.Equal(t, int32(1), tm.Spec.Entries[0].Weight)
	pending := overrideActiveCond(tm)
	require.NotNil(t, pending)
	assert.Equal(t, metav1.ConditionFalse, pending.Status)
	assert.Equal(t, v1beta1.TrafficMapReasonOverridesPending, pending.Reason)

	cur := &v1beta1.InferenceService{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: testName, Namespace: testNS}, cur))
	cur.Status.Placement.Candidates = append(cur.Status.Placement.Candidates, v1beta1.CandidatePlacement{
		Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted, Endpoint: apis.HTTPS("b.example"),
		AdmittedReplicas: 3, ReadyReplicas: 3,
	})
	require.NoError(t, c.Status().Update(context.Background(), cur))

	reconcile(t, r)
	tm, ok = getTrafficMap(t, c)
	require.True(t, ok)
	require.Len(t, tm.Spec.Entries, 2)
	assert.Equal(t, "b", tm.Spec.Entries[1].Cluster)
	assert.Equal(t, int32(0), tm.Spec.Entries[1].Weight,
		"a pre-armed cluster must enter its first TrafficMap at zero")
	assert.Equal(t, []string{"turnup-hold"}, tm.Spec.Entries[1].DrainRefs)
	active := overrideActiveCond(tm)
	require.NotNil(t, active)
	assert.Equal(t, metav1.ConditionTrue, active.Status)
	assert.Equal(t, v1beta1.TrafficMapReasonOverridesApplied, active.Reason)
}

func TestReconcile_InvalidTrafficDrainPreservesLastValidTrafficMap(t *testing.T) {
	isvc := splitISVC([]string{"a", "b"}, []int32{7, 3}, []int32{7, 3})
	isvc.Annotations = map[string]string{
		constants.TrafficDrainAnnotation: `{"drain":{"cluster":"a","reason":"mitigation"}}`,
	}
	r, c := newReconciler(t, controllerTestConfig(), isvc)
	reconcile(t, r)

	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	assert.Equal(t, int32(0), tm.Spec.Entries[0].Weight)
	oldGeneration := tm.Generation

	updateDrainAnnotation(t, c, `{"drain":`)
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: testName, Namespace: testNS},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), constants.TrafficDrainAnnotation)

	tm, ok = getTrafficMap(t, c)
	require.True(t, ok)
	assert.Equal(t, oldGeneration, tm.Generation)
	assert.Equal(t, int32(0), tm.Spec.Entries[0].Weight,
		"malformed intent must never be treated as an absent drain")
	assert.Equal(t, []string{"drain"}, tm.Spec.Entries[0].DrainRefs)
}

func TestBuildSpec_ManualDrainsComposeWithProbePolicy(t *testing.T) {
	for _, tt := range []struct {
		name            string
		failing         [2]bool
		allFailedPolicy AllFailedPolicy
		manualHold      bool
		weights         [2]int32
		status          metav1.ConditionStatus
		reason          string
	}{
		{
			name:    "all-failed PreserveTraffic cannot clear a manual hold",
			failing: [2]bool{true, true}, allFailedPolicy: AllFailedPolicyPreserveTraffic, manualHold: true,
			weights: [2]int32{7, 0}, status: metav1.ConditionTrue, reason: v1beta1.TrafficMapReasonAllHomesProbeFailed,
		},
		{
			name:    "all probes failing remains the cause while a manual hold is active",
			failing: [2]bool{true, true}, manualHold: true,
			weights: [2]int32{0, 0}, status: metav1.ConditionFalse,
			reason: v1beta1.TrafficMapReasonAllHomesProbeFailed,
		},
		{
			name:       "probe recovery cannot clear a manual hold",
			manualHold: true, weights: [2]int32{7, 0}, status: metav1.ConditionTrue,
			reason: v1beta1.TrafficMapReasonRoutable,
		},
		{
			name:    "removing a manual hold cannot restore probe-failed homes",
			failing: [2]bool{true, true}, weights: [2]int32{0, 0}, status: metav1.ConditionFalse,
			reason: v1beta1.TrafficMapReasonAllHomesProbeFailed,
		},
		{
			name:    "recovered homes without manual holds use current capacity",
			weights: [2]int32{7, 3}, status: metav1.ConditionTrue,
			reason: v1beta1.TrafficMapReasonRoutable,
		},
		{
			name:    "a manual hold drains the only probe-healthy home",
			failing: [2]bool{true, false}, manualHold: true,
			weights: [2]int32{0, 0}, status: metav1.ConditionFalse,
			reason: v1beta1.TrafficMapReasonTrafficDrain,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			isvc := splitISVC([]string{"a", "b"}, []int32{7, 3}, []int32{7, 3})
			var failing []string
			for i, cluster := range []string{"a", "b"} {
				if tt.failing[i] {
					failing = append(failing, cluster)
				}
			}
			allFailedPolicy := tt.allFailedPolicy
			if allFailedPolicy == "" {
				allFailedPolicy = AllFailedPolicyDrain
			}
			p, policy := failingProbeState(t, isvc, allFailedPolicy, failing...)
			var overrides []trafficdrain.Override
			if tt.manualHold {
				overrides = []trafficdrain.Override{{ID: "hold-b", Cluster: "b", Reason: "maintenance"}}
			}

			spec, status, reason := buildSpec(isvc, policy, ResolvedCapacityPolicy{}, p, nil, overrides...)
			require.Len(t, spec.Entries, 2)
			assert.Equal(t, tt.status, status)
			assert.Equal(t, tt.reason, reason)
			for i := range spec.Entries {
				assert.Equal(t, tt.weights[i], spec.Entries[i].Weight)
				assert.Equal(t, !tt.failing[i], spec.Entries[i].Healthy)
			}
			assert.Empty(t, spec.Entries[0].DrainRefs)
			if tt.manualHold {
				assert.Equal(t, []string{"hold-b"}, spec.Entries[1].DrainRefs)
			} else {
				assert.Empty(t, spec.Entries[1].DrainRefs)
			}
		})
	}
}
