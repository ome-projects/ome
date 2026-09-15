package quotastatusprojection

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	api "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	v "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

const healthyWire = `{"apiVersion":"ome.io/v1beta1","kind":"AcceleratorQuota","metadata":{"name":"team","generation":7,"labels":{"token":"SECRET"},"annotations":{"token":"SECRET"},"uid":"SECRET","resourceVersion":"SECRET"},"spec":{"role":"ClusterQueue","parentRef":{"name":"root"}},"status":{"observedGeneration":7,"parent":"root","sourceGeneration":22,"budgets":[{"resourceName":"nvidia.com/gpu","resourceFlavor":"a100","nominal":"8","admitted":"3","reserved":"5","borrowed":"0","perCluster":[{"cluster":"west","nominal":"8","admitted":"3","reserved":"5","borrowed":"0"}]}],"clusters":[{"cluster":"west","appliedGeneration":7,"materializedGeneration":6,"appliedTime":"2026-09-15T11:00:00Z","message":"SECRET"}],"materialization":{"frozen":true,"frozenAt":"2026-09-15T11:30:00Z","reason":"CapacityExceeded","lastAppliedGeneration":6,"lastAppliedTime":"2026-09-15T11:00:00Z"},"conditions":[{"type":"Ready","status":"False","observedGeneration":7,"reason":"Frozen","message":"SECRET","lastTransitionTime":"2026-09-15T11:30:00Z"}]}}`

var fixed = v.ClockFunc(func() time.Time { return time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC) })

func wireQuota(t *testing.T) api.AcceleratorQuota {
	t.Helper()
	var q api.AcceleratorQuota
	require.NoError(t, json.Unmarshal([]byte(healthyWire), &q))
	return q
}
func one(t *testing.T, q api.AcceleratorQuota) v.QuotaStatusObject {
	t.Helper()
	r, err := Project(Input{Quotas: []api.AcceleratorQuota{q}, Target: q.Name, ObservedPages: 1, ObservedItems: 1, Scope: "Named"}, fixed)
	require.NoError(t, err)
	require.Len(t, r.Content.Objects, 1)
	data, err := json.Marshal(r)
	require.NoError(t, err)
	assert.NotContains(t, string(data), "SECRET")
	return r.Content.Objects[0]
}

func TestCurrentWireReportsIndependentGenerationAndUsage(t *testing.T) {
	o := one(t, wireQuota(t))
	assert.Equal(t, "Current", o.Freshness)
	assert.Equal(t, "root", o.ReportedParent)
	assert.Equal(t, int64(22), o.SourceGeneration)
	assert.Equal(t, "Reported/Unverifiable", o.SourceGenerationState)
	require.Len(t, o.Budgets, 1)
	assert.Equal(t, v.QuotaStatusUsage{Nominal: "8", Admitted: "3", Reserved: "5", Borrowed: "Unknown/Defaulted"}, o.Budgets[0].Usage)
	require.Len(t, o.Projections, 1)
	assert.Equal(t, "Current", o.Projections[0].ProjectionFreshness)
	assert.Equal(t, "Stale", o.Projections[0].MaterializationFreshness)
	require.NotNil(t, o.Materialization)
	assert.Equal(t, "Frozen", o.Materialization.FreezeState)
	assert.Equal(t, "Stale", o.Materialization.Freshness)
	assert.Equal(t, "CapacityExceeded", o.Materialization.Reason)
	require.Len(t, o.Conditions, 1)
	assert.Equal(t, "False", o.Conditions[0].Status)
	assert.Equal(t, "Frozen", o.Conditions[0].Reason)
}

func TestFreshnessAndAbsentStatusAreNotAffirmative(t *testing.T) {
	for _, tt := range []struct {
		generation, observed int64
		want                 string
	}{{7, 0, "Unknown"}, {0, 0, "Unknown"}, {7, 6, "Stale"}, {7, 8, "Inconsistent"}, {7, -1, "Inconsistent"}, {7, 7, "Current"}} {
		q := api.AcceleratorQuota{ObjectMeta: metav1.ObjectMeta{Name: "root", Generation: tt.generation}, Status: api.AcceleratorQuotaStatus{ObservedGeneration: tt.observed}}
		o := one(t, q)
		assert.Equal(t, tt.want, o.Freshness)
		assert.Equal(t, "Unavailable", o.BudgetsGroup.State)
		assert.Nil(t, o.Materialization)
		assert.Equal(t, "Unknown", o.SourceGenerationState)
		assert.Equal(t, "Unavailable", o.ParentState)
	}
}

func TestLateContradictionInvalidatesWholeGroupNotSafePrefix(t *testing.T) {
	q := wireQuota(t)
	original := q.Status.Budgets[0]
	q.Status.Budgets = nil
	for i := 0; i < 20; i++ {
		b := *original.DeepCopy()
		b.ResourceFlavor = fmt.Sprintf("flavor-%02d", i)
		q.Status.Budgets = append(q.Status.Budgets, b)
	}
	// A late duplicate beyond the 16-row display cap cannot disappear.
	q.Status.Budgets = append(q.Status.Budgets, *q.Status.Budgets[19].DeepCopy())
	o := one(t, q)
	assert.Equal(t, "Invalid", o.BudgetsGroup.State)
	assert.Equal(t, "MalformedPayload", o.BudgetsGroup.Reason)
	assert.Empty(t, o.Budgets)
	assert.Equal(t, "Available", o.ProjectionsGroup.State)
}

func TestBoundedCanonicalGroupAndOversize(t *testing.T) {
	q := wireQuota(t)
	b := q.Status.Budgets[0]
	q.Status.Budgets = nil
	for i := 19; i >= 0; i-- {
		copy := *b.DeepCopy()
		copy.ResourceFlavor = fmt.Sprintf("flavor-%02d", i)
		q.Status.Budgets = append(q.Status.Budgets, copy)
	}
	o := one(t, q)
	assert.True(t, o.BudgetsGroup.Truncated)
	assert.Equal(t, 20, o.BudgetsGroup.Observed)
	assert.Equal(t, 16, o.BudgetsGroup.Displayed)
	require.Len(t, o.Budgets, 16)
	assert.Equal(t, "flavor-00", o.Budgets[0].ResourceFlavor)
	assert.Equal(t, "flavor-15", o.Budgets[15].ResourceFlavor)
	for len(q.Status.Budgets) < 65 {
		q.Status.Budgets = append(q.Status.Budgets, b)
	}
	o = one(t, q)
	assert.Equal(t, "Oversize", o.BudgetsGroup.Reason)
	assert.Empty(t, o.Budgets)
}

func TestConditionsAndProjectionRejectLateMalformedData(t *testing.T) {
	q := wireQuota(t)
	q.Status.Conditions = append(q.Status.Conditions, validCondition("Ready", metav1.ConditionTrue, 7))
	o := one(t, q)
	assert.Equal(t, "Invalid", o.ConditionsGroup.State)
	assert.Empty(t, o.Conditions)
	q = wireQuota(t)
	q.Status.Clusters[0].MaterializedGeneration = 8
	o = one(t, q)
	assert.Equal(t, "Invalid", o.ProjectionsGroup.State)
	assert.Empty(t, o.Projections)
	q = wireQuota(t)
	q.Status.Conditions[0].Reason = "SECRET"
	o = one(t, q)
	assert.Equal(t, "Other", o.Conditions[0].Reason)
	q = wireQuota(t)
	q.Status.Materialization.Frozen = false
	o = one(t, q)
	assert.Equal(t, "Invalid", o.MaterializationGroup.State)
	assert.Nil(t, o.Materialization)
}

func TestRejectsInvalidIdentityAndSnapshotBeforeOutput(t *testing.T) {
	for _, in := range []Input{{Quotas: []api.AcceleratorQuota{{ObjectMeta: metav1.ObjectMeta{Name: "BAD"}}}, ObservedPages: 1, ObservedItems: 1, Scope: "Named"}, {Quotas: []api.AcceleratorQuota{wireQuota(t)}, ObservedPages: 1, ObservedItems: 0, Scope: "Cluster"}, {ObservedPages: 1, Scope: "Cluster", Truncated: true}} {
		_, err := Project(in, fixed)
		require.Error(t, err)
	}
	q := wireQuota(t)
	q.Name = "team-sk-" + strings.Repeat("x", 24)
	_, err := Project(Input{Quotas: []api.AcceleratorQuota{q}, ObservedPages: 1, ObservedItems: 1, Scope: "Named"}, fixed)
	require.Error(t, err)
}

func TestContradictoryCurrentConditionsAreUnavailable(t *testing.T) {
	q := wireQuota(t)
	q.Status.Conditions = []metav1.Condition{validCondition("Ready", metav1.ConditionTrue, 7), validCondition("Degraded", metav1.ConditionTrue, 7)}
	o := one(t, q)
	assert.Equal(t, "Invalid", o.ConditionsGroup.State)
	assert.Empty(t, o.Conditions)
}

func TestNamedEmptySnapshotCannotPretendSuccessfulGet(t *testing.T) {
	_, err := Project(Input{Target: "team", Scope: "Named", ObservedPages: 1}, fixed)
	require.Error(t, err)
}

func TestRequiredConditionShapeMissingTimeOrReasonFailsClosed(t *testing.T) {
	for _, change := range []func(*api.AcceleratorQuota){
		func(q *api.AcceleratorQuota) { q.Status.Conditions[0].LastTransitionTime = metav1.Time{} },
		func(q *api.AcceleratorQuota) { q.Status.Conditions[0].Reason = "" },
		func(q *api.AcceleratorQuota) { q.Status.Conditions[0].Reason = "Bearer SECRET" },
		func(q *api.AcceleratorQuota) { q.Status.Conditions[0].Message = strings.Repeat("x", 32769) },
		func(q *api.AcceleratorQuota) { q.Status.Conditions[0].Type = "Bad Type" },
	} {
		q := wireQuota(t)
		q.Status.Conditions[0].Status = metav1.ConditionTrue
		change(&q)
		o := one(t, q)
		assert.Equal(t, "Invalid", o.ConditionsGroup.State)
		assert.Equal(t, "MalformedPayload", o.ConditionsGroup.Reason)
		assert.Empty(t, o.Conditions)
		assert.Equal(t, "Available", o.BudgetsGroup.State)
		assert.Equal(t, "Available", o.ProjectionsGroup.State)
		assert.Equal(t, "Available", o.MaterializationGroup.State)
	}
	q := wireQuota(t)
	q.Status.Conditions[0].Status = metav1.ConditionTrue
	o := one(t, q)
	require.Len(t, o.Conditions, 1)
	assert.Equal(t, "Current", o.Conditions[0].Freshness)
	assert.Equal(t, "Available", o.ConditionsGroup.State)
}

func validCondition(typ string, status metav1.ConditionStatus, observed int64) metav1.Condition {
	return metav1.Condition{Type: typ, Status: status, ObservedGeneration: observed, Reason: "Admitted", LastTransitionTime: metav1.Time{Time: time.Date(2026, 9, 15, 11, 0, 0, 0, time.UTC)}}
}
