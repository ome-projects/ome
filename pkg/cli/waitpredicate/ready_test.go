package waitpredicate

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/waitengine"
)

var now = time.Unix(1000000, 0)

func service(conditions ...apis.Condition) *ome.InferenceService {
	return &ome.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "work", UID: "uid", ResourceVersion: "opaque", Generation: 9}, Status: ome.InferenceServiceStatus{Status: duckv1.Status{Conditions: conditions}}}
}
func ready(status corev1.ConditionStatus) apis.Condition {
	return apis.Condition{Type: apis.ConditionReady, Status: status}
}
func TestOptionalKnativeFieldsDoNotBlockReady(t *testing.T) {
	for _, generation := range []int64{0, 9, 77} {
		v := service(ready(corev1.ConditionTrue))
		v.Status.ObservedGeneration = generation
		d, o, err := EvaluateReady(v, corev1.ConditionTrue, now)
		require.NoError(t, err)
		require.True(t, d.Matched)
		require.Equal(t, "Unverifiable", o.GenerationFreshness)
		require.Equal(t, "True", o.Status)
		require.Equal(t, "Complete", o.Inspection.State)
	}
}
func TestExplicitStatusesAndMissingUnknown(t *testing.T) {
	for _, status := range []corev1.ConditionStatus{corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionUnknown} {
		d, o, err := EvaluateReady(service(ready(status)), status, now)
		require.NoError(t, err)
		require.True(t, d.Matched)
		require.Equal(t, string(status), o.Status)
	}
	d, o, err := EvaluateReady(service(), corev1.ConditionUnknown, now)
	require.NoError(t, err)
	require.False(t, d.Matched)
	require.Equal(t, "NotRecorded", o.Status)
	require.Equal(t, waitengine.ReasonNotRecorded, d.Reason)
	d, _, err = EvaluateReady(service(ready(corev1.ConditionFalse)), corev1.ConditionTrue, now)
	require.NoError(t, err)
	require.False(t, d.Matched)
}
func TestReadyInvalidityIsUnmetAndWholeInspectionVisible(t *testing.T) {
	future := ready(corev1.ConditionTrue)
	future.LastTransitionTime = apis.VolatileTime{Inner: metav1.NewTime(now.Add(time.Second))}
	for _, tc := range []struct {
		name       string
		conditions []apis.Condition
		warning    Warning
	}{
		{"future", []apis.Condition{future}, WarningFutureTimestamp},
		{"contradiction", []apis.Condition{ready(corev1.ConditionTrue), ready(corev1.ConditionFalse)}, WarningConflictingReady},
		{"status", []apis.Condition{ready("Bearer PRIVATE")}, WarningInvalidRecord},
		{"severity", []apis.Condition{{Type: apis.ConditionReady, Status: corev1.ConditionTrue, Severity: "Error"}}, WarningInvalidRecord},
		{"reason", []apis.Condition{{Type: apis.ConditionReady, Status: corev1.ConditionTrue, Reason: strings.Repeat("x", 1025)}}, WarningOversizedRecord},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, o, err := EvaluateReady(service(tc.conditions...), corev1.ConditionTrue, now)
			require.NoError(t, err)
			require.False(t, d.Matched)
			require.Equal(t, "Invalid", o.Validity)
			require.Contains(t, o.Inspection.Warnings, tc.warning)
			require.Equal(t, len(tc.conditions), o.Inspection.Inspected)
		})
	}
}
func TestLateDuplicateAndUnrelatedScalarIndependence(t *testing.T) {
	conditions := []apis.Condition{ready(corev1.ConditionTrue)}
	for i := 0; i < 62; i++ {
		conditions = append(conditions, apis.Condition{Type: "Other", Status: corev1.ConditionFalse})
	}
	conditions = append(conditions, ready(corev1.ConditionFalse))
	d, o, err := EvaluateReady(service(conditions...), corev1.ConditionTrue, now)
	require.NoError(t, err)
	require.False(t, d.Matched)
	require.Equal(t, 64, o.Inspection.Inspected)
	conditions = []apis.Condition{ready(corev1.ConditionTrue), {Type: apis.ConditionType(strings.Repeat("x", 254)), Status: corev1.ConditionFalse, Reason: strings.Repeat("r", 1025), Message: strings.Repeat("PRIVATE", 1000)}}
	d, o, err = EvaluateReady(service(conditions...), corev1.ConditionTrue, now)
	require.NoError(t, err)
	require.True(t, d.Matched)
	require.Equal(t, "Partial", o.Inspection.State)
	require.Contains(t, o.Inspection.Warnings, WarningOversizedRecord)
}
func TestFatalCountAndMalformedMetadata(t *testing.T) {
	conditions := make([]apis.Condition, 65)
	conditions[0] = ready(corev1.ConditionTrue)
	d, _, err := EvaluateReady(service(conditions...), corev1.ConditionTrue, now)
	require.False(t, d.Matched)
	require.Equal(t, waitengine.ReasonInspectionLimit, err.(*waitengine.Error).Reason)
	v := service(ready(corev1.ConditionTrue))
	v.Name = "../PRIVATE"
	_, _, err = EvaluateReady(v, corev1.ConditionTrue, now)
	require.Equal(t, waitengine.ReasonInvalidIdentity, err.(*waitengine.Error).Reason)
	_, _, err = EvaluateReady(nil, corev1.ConditionTrue, now)
	require.Error(t, err)
}
func TestValidSeverityTimestampAndDuplicate(t *testing.T) {
	for _, severity := range []apis.ConditionSeverity{"", apis.ConditionSeverityInfo, apis.ConditionSeverityWarning} {
		c := ready(corev1.ConditionTrue)
		c.Severity = severity
		c.LastTransitionTime = apis.VolatileTime{Inner: metav1.NewTime(now)}
		d, o, err := EvaluateReady(service(c, c), corev1.ConditionTrue, now)
		require.NoError(t, err)
		require.True(t, d.Matched)
		require.Equal(t, "Valid", o.Validity)
		require.Contains(t, o.Inspection.Warnings, WarningDuplicateReady)
	}
}
