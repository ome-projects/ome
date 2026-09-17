package waitruntime

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	knapis "knative.dev/pkg/apis"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/waitengine"
	"sigs.k8s.io/ome/pkg/constants"
)

const requestID = "123e4567-e89b-42d3-a456-426614174000"
const requestToken = "cli-runtime-sync-" + requestID

func service() *ome.InferenceService {
	autoSync := false
	return &ome.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "service", Namespace: "work", UID: "uid-1", ResourceVersion: "17", Generation: 3,
			Annotations: map[string]string{constants.RuntimeSyncAnnotationKey: requestToken},
		},
		Spec:   ome.InferenceServiceSpec{Runtime: &ome.ServingRuntimeRef{Name: "runtime", AutoSync: &autoSync}},
		Status: ome.InferenceServiceStatus{PinnedRevisionName: "pin-abc", LastRuntimeSyncToken: requestToken},
	}
}

func TestExactRequestAcknowledgmentAndReportedDriftClear(t *testing.T) {
	for _, generation := range []int64{0, 3, 9} {
		v := service()
		v.Status.ObservedGeneration = generation
		decision, observation, err := Evaluate(v, requestID)
		require.NoError(t, err)
		require.True(t, decision.Matched)
		require.Equal(t, ReasonObserved, decision.Reason)
		require.Equal(t, TokenAcknowledged, observation.TokenState)
		require.Equal(t, DriftClear, observation.DriftState)
		require.Equal(t, PinManaged, observation.PinState)
		require.Equal(t, "Unverifiable", observation.GenerationFreshness)
	}
}

func TestTokenStatesAreExactAndPrivate(t *testing.T) {
	for _, tc := range []struct {
		name, annotation, status, want, validity string
	}{
		{"pending", requestToken, "PRIVATE_OLD_TOKEN", "Pending", "Valid"},
		{"status only", "", requestToken, "StatusOnly", "Unavailable"},
		{"superseded", "PRIVATE_NEW_TOKEN", requestToken, "Superseded", "Valid"},
		{"absent", "", "", "Absent", "Unavailable"},
		{"different request", "cli-runtime-sync-123e4567-e89b-42d3-a456-426614174001", "cli-runtime-sync-123e4567-e89b-42d3-a456-426614174001", "Superseded", "Valid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := service()
			v.Annotations[constants.RuntimeSyncAnnotationKey] = tc.annotation
			v.Status.LastRuntimeSyncToken = tc.status
			decision, observation, err := Evaluate(v, requestID)
			require.NoError(t, err)
			require.False(t, decision.Matched)
			require.Equal(t, ReasonNotAcknowledged, decision.Reason)
			require.Equal(t, tc.want, string(observation.TokenState))
			require.Equal(t, tc.validity, observation.Validity)
			require.NotContains(t, fmt.Sprintf("%+v", observation), "PRIVATE")
		})
	}
}

func TestAnyReportedDriftBlocksObservation(t *testing.T) {
	condition := func(status corev1.ConditionStatus) knapis.Condition {
		return knapis.Condition{Type: knapis.ConditionType(constants.RuntimeDriftedConditionType), Status: status, Reason: "PRIVATE_REASON", Message: "PRIVATE_MESSAGE"}
	}
	invalidMessage := condition(corev1.ConditionTrue)
	invalidMessage.Message = string([]byte{0xff})
	oversizedReason := condition(corev1.ConditionTrue)
	oversizedReason.Reason = strings.Repeat("x", 1025)
	for _, tc := range []struct {
		name, drift, validity string
		conditions            []knapis.Condition
	}{
		{"true", "ReportedTrue", "Valid", []knapis.Condition{condition(corev1.ConditionTrue)}},
		{"false is not clear", "ReportedFalse", "Valid", []knapis.Condition{condition(corev1.ConditionFalse)}},
		{"unknown", "ReportedUnknown", "Valid", []knapis.Condition{condition(corev1.ConditionUnknown)}},
		{"invalid status", "Invalid", "Invalid", []knapis.Condition{condition("PRIVATE_STATUS")}},
		{"invalid message", "Invalid", "Invalid", []knapis.Condition{invalidMessage}},
		{"oversized reason", "Invalid", "Invalid", []knapis.Condition{oversizedReason}},
		{"duplicate", "Invalid", "Invalid", []knapis.Condition{condition(corev1.ConditionTrue), condition(corev1.ConditionTrue)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := service()
			v.Status.Conditions = tc.conditions
			decision, observation, err := Evaluate(v, requestID)
			require.NoError(t, err)
			require.False(t, decision.Matched)
			require.Equal(t, tc.drift, string(observation.DriftState))
			require.Equal(t, tc.validity, observation.Validity)
			require.Equal(t, len(tc.conditions), observation.InspectedConditions)
			require.NotContains(t, fmt.Sprintf("%+v", observation), "PRIVATE")
		})
	}
}

func TestUnrelatedConditionPayloadDoesNotBlockExactSync(t *testing.T) {
	v := service()
	v.Status.Conditions = []knapis.Condition{{Type: "Ready", Status: corev1.ConditionFalse, Message: strings.Repeat("PRIVATE", 1000)}}
	decision, observation, err := Evaluate(v, requestID)
	require.NoError(t, err)
	require.True(t, decision.Matched)
	require.Equal(t, 1, observation.InspectedConditions)
	require.NotContains(t, fmt.Sprintf("%+v", observation), "PRIVATE")
}

func TestRejectsNonCanonicalOrNonV4RequestID(t *testing.T) {
	for _, id := range []string{
		"", "PRIVATE", strings.ToUpper(requestID), "{" + requestID + "}",
		"123e4567-e89b-12d3-a456-426614174000", // Version 1.
		"123e4567-e89b-42d3-7456-426614174000", // Non-RFC4122 variant.
		strings.Repeat("x", 1000),
	} {
		decision, observation, err := Evaluate(service(), id)
		require.False(t, decision.Matched)
		var waitErr *waitengine.Error
		require.ErrorAs(t, err, &waitErr)
		require.Equal(t, waitengine.ReasonInvalidOptions, waitErr.Reason)
		require.Empty(t, observation.RequestID)
		require.NotContains(t, fmt.Sprintf("%+v", observation), "PRIVATE")
	}
}

func TestRejectsUnboundOrUnsafeIdentity(t *testing.T) {
	for _, change := range []func(*ome.InferenceService){
		func(v *ome.InferenceService) { v.Name = "../PRIVATE" },
		func(v *ome.InferenceService) { v.Namespace = "bad/ns" },
		func(v *ome.InferenceService) { v.UID = "" },
		func(v *ome.InferenceService) { v.ResourceVersion = "" },
		func(v *ome.InferenceService) { v.Kind = "Secret" },
		func(v *ome.InferenceService) { v.APIVersion = "other/v1" },
		func(v *ome.InferenceService) { v.Generation = 0 },
	} {
		v := service()
		change(v)
		decision, _, err := Evaluate(v, requestID)
		require.False(t, decision.Matched)
		var waitErr *waitengine.Error
		require.ErrorAs(t, err, &waitErr)
		require.Equal(t, waitengine.ReasonInvalidIdentity, waitErr.Reason)
	}
	decision, _, err := Evaluate(nil, requestID)
	require.False(t, decision.Matched)
	var waitErr *waitengine.Error
	require.ErrorAs(t, err, &waitErr)
	require.Equal(t, waitengine.ReasonInvalidIdentity, waitErr.Reason)
}

func TestManagedPinMustRemainApplicable(t *testing.T) {
	for _, tc := range []struct {
		name, want, validity string
		change               func(*ome.InferenceService)
	}{
		{"no runtime", "NotApplicable", "Unavailable", func(v *ome.InferenceService) { v.Spec.Runtime = nil }},
		{"default auto sync", "NotApplicable", "Unavailable", func(v *ome.InferenceService) { v.Spec.Runtime.AutoSync = nil }},
		{"auto sync true", "NotApplicable", "Unavailable", func(v *ome.InferenceService) { yes := true; v.Spec.Runtime.AutoSync = &yes }},
		{"explicit revision", "NotApplicable", "Unavailable", func(v *ome.InferenceService) { rev := "pin-other"; v.Spec.Runtime.Revision = &rev }},
		{"no reported pin", "Unavailable", "Unavailable", func(v *ome.InferenceService) { v.Status.PinnedRevisionName = "" }},
		{"invalid pin", "Invalid", "Invalid", func(v *ome.InferenceService) { v.Status.PinnedRevisionName = "../PRIVATE" }},
		{"invalid runtime name", "Invalid", "Invalid", func(v *ome.InferenceService) { v.Spec.Runtime.Name = "../PRIVATE" }},
		{"invalid runtime kind", "Invalid", "Invalid", func(v *ome.InferenceService) { kind := "Secret"; v.Spec.Runtime.Kind = &kind }},
		{"invalid runtime API group", "Invalid", "Invalid", func(v *ome.InferenceService) { group := "other.io"; v.Spec.Runtime.APIGroup = &group }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := service()
			tc.change(v)
			decision, observation, err := Evaluate(v, requestID)
			require.NoError(t, err)
			require.False(t, decision.Matched)
			require.Equal(t, tc.want, string(observation.PinState))
			require.Equal(t, tc.validity, observation.Validity)
		})
	}
	v := service()
	kind, group, empty := "ServingRuntime", "ome.io", ""
	v.Spec.Runtime.Kind, v.Spec.Runtime.APIGroup, v.Spec.Runtime.Revision = &kind, &group, &empty
	decision, _, err := Evaluate(v, requestID)
	require.NoError(t, err)
	require.True(t, decision.Matched)
}

func TestConditionInspectionIsBoundedAndCatchesLateDuplicate(t *testing.T) {
	unrelated := knapis.Condition{Type: "Ready", Status: corev1.ConditionFalse}
	v := service()
	v.Status.Conditions = make([]knapis.Condition, 64)
	for i := range v.Status.Conditions {
		v.Status.Conditions[i] = unrelated
	}
	decision, observation, err := Evaluate(v, requestID)
	require.NoError(t, err)
	require.True(t, decision.Matched)
	require.Equal(t, 64, observation.InspectedConditions)

	v.Status.Conditions[0] = knapis.Condition{Type: knapis.ConditionType(constants.RuntimeDriftedConditionType), Status: corev1.ConditionTrue}
	v.Status.Conditions[63] = knapis.Condition{Type: knapis.ConditionType(constants.RuntimeDriftedConditionType), Status: corev1.ConditionFalse}
	decision, observation, err = Evaluate(v, requestID)
	require.NoError(t, err)
	require.False(t, decision.Matched)
	require.Equal(t, "Invalid", string(observation.DriftState))
	require.Equal(t, 64, observation.InspectedConditions)

	v.Status.Conditions = append(v.Status.Conditions, unrelated)
	decision, _, err = Evaluate(v, requestID)
	require.False(t, decision.Matched)
	var waitErr *waitengine.Error
	require.ErrorAs(t, err, &waitErr)
	require.Equal(t, waitengine.ReasonInspectionLimit, waitErr.Reason)
}

func TestMalformedConditionTypeOrOversizedTokenNeverMatches(t *testing.T) {
	for _, malformedType := range []string{"", strings.Repeat("x", 254), string([]byte{0xff}), "RuntimeDrifted\n"} {
		v := service()
		v.Status.Conditions = []knapis.Condition{{Type: knapis.ConditionType(malformedType), Status: corev1.ConditionTrue}}
		decision, observation, err := Evaluate(v, requestID)
		require.NoError(t, err)
		require.False(t, decision.Matched)
		require.Equal(t, "Invalid", observation.Validity)
	}
	for _, change := range []func(*ome.InferenceService){
		func(v *ome.InferenceService) {
			v.Annotations[constants.RuntimeSyncAnnotationKey] = strings.Repeat("PRIVATE", 37)
		},
		func(v *ome.InferenceService) { v.Status.LastRuntimeSyncToken = strings.Repeat("PRIVATE", 37) },
	} {
		v := service()
		change(v)
		decision, observation, err := Evaluate(v, requestID)
		require.NoError(t, err)
		require.False(t, decision.Matched)
		require.Equal(t, "Invalid", string(observation.TokenState))
		require.Equal(t, "Invalid", observation.Validity)
		require.NotContains(t, fmt.Sprintf("%+v", observation), "PRIVATE")
	}
}

func TestPlacementOwnedSnapshotsCannotSatisfyDirectSyncWait(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*ome.InferenceService)
	}{
		{"spec placement", func(v *ome.InferenceService) { v.Spec.Placement = &ome.PlacementSpec{} }},
		{"status placement", func(v *ome.InferenceService) { v.Status.Placement = &ome.PlacementStatus{} }},
		{"finalizer", func(v *ome.InferenceService) { v.Finalizers = []string{"ome.io/placement"} }},
		{"origin annotation", func(v *ome.InferenceService) { v.Annotations[constants.PlacementOrigin] = "PRIVATE" }},
		{"origin UID label", func(v *ome.InferenceService) { v.Labels = map[string]string{constants.PlacementOriginUID: "PRIVATE"} }},
		{"control plane annotation", func(v *ome.InferenceService) { v.Annotations[constants.PlacementControlPlane] = "PRIVATE" }},
		{"cluster selector label", func(v *ome.InferenceService) { v.Labels = map[string]string{"ome.io/cluster-selector": "PRIVATE"} }},
		{"accelerator requirements annotation", func(v *ome.InferenceService) { v.Annotations["ome.io/accelerator-requirements"] = "PRIVATE" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := service()
			tc.change(v)
			decision, observation, err := Evaluate(v, requestID)
			require.NoError(t, err)
			require.False(t, decision.Matched)
			require.Equal(t, ReasonUnsupportedPlacement, decision.Reason)
			require.Equal(t, "UnsupportedPlacement", string(observation.PlacementState))
			require.Equal(t, PinNotApplicable, observation.PinState)
			require.Equal(t, "Unavailable", string(observation.TokenState))
			require.Equal(t, "Unavailable", string(observation.DriftState))
			require.Zero(t, observation.InspectedConditions)
			require.NotContains(t, fmt.Sprintf("%+v", observation), "PRIVATE")
		})
	}
}

func TestDeletingSnapshotCannotMatchDespiteExactReportedSync(t *testing.T) {
	v := service()
	deleting := metav1.Now()
	v.DeletionTimestamp = &deleting
	decision, observation, err := Evaluate(v, requestID)
	require.NoError(t, err)
	require.False(t, decision.Matched)
	require.Equal(t, TokenAcknowledged, observation.TokenState)
	require.Equal(t, DriftClear, observation.DriftState)
	require.Equal(t, PinManaged, observation.PinState)
	require.Equal(t, "Unavailable", observation.Validity)
}
