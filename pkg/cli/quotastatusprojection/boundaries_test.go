package quotastatusprojection

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	api "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

const rootCapacityWire = `{"metadata":{"name":"root","generation":7},"status":{"observedGeneration":7,"capacity":[{"resourceName":"nvidia.com/gpu","resourceFlavor":"a100","allocatable":"6","highWaterMark":"8","observedAt":"2026-09-15T11:00:00Z","perCluster":[{"cluster":"west","allocatable":"2","highWaterMark":"3","observedAt":"2026-09-15T10:59:00Z"},{"cluster":"east","allocatable":"4","highWaterMark":"5"}]}]}}`

func capacityQuota(t *testing.T) api.AcceleratorQuota {
	t.Helper()
	var q api.AcceleratorQuota
	require.NoError(t, json.Unmarshal([]byte(rootCapacityWire), &q))
	return q
}

func TestRootCapacityIsReportedAndHighWaterHistorical(t *testing.T) {
	o := one(t, capacityQuota(t))
	assert.Equal(t, "Available", o.CapacityGroup.State)
	require.Len(t, o.Capacity, 1)
	c := o.Capacity[0]
	assert.Equal(t, "6", c.Allocatable)
	assert.Equal(t, "8", c.HighWaterMark)
	assert.Equal(t, "2026-09-15T11:00:00Z", c.ObservedAt)
	assert.Equal(t, "Reported/Unverifiable", c.SampleState)
	require.Len(t, c.PerCluster, 2)
	assert.Equal(t, "east", c.PerCluster[0].Cluster)
	assert.Equal(t, "Unknown", c.PerCluster[0].SampleState)
	assert.Equal(t, "3", c.PerCluster[1].HighWaterMark)
	q := capacityQuota(t)
	q.Name = "team"
	o = one(t, q)
	assert.Equal(t, "UnexpectedNonRootCapacity", o.CapacityGroup.Reason)
	assert.Empty(t, o.Capacity)
}

func TestCapacityInvalidBoundsDoNotEraseOtherEvidence(t *testing.T) {
	for _, change := range []func(*api.AcceleratorQuota){
		func(q *api.AcceleratorQuota) { q.Status.Capacity[0].Allocatable = resource.MustParse("-1") },
		func(q *api.AcceleratorQuota) { q.Status.Capacity[0].HighWaterMark = resource.MustParse("2") },
		func(q *api.AcceleratorQuota) {
			q.Status.Capacity[0].ObservedAt = &metav1.Time{Time: time.Date(2026, 9, 16, 0, 0, 0, 0, time.UTC)}
		},
		func(q *api.AcceleratorQuota) {
			q.Status.Capacity = append(q.Status.Capacity, *q.Status.Capacity[0].DeepCopy())
		},
		func(q *api.AcceleratorQuota) { q.Status.Capacity[0].ResourceName = "bad resource" },
	} {
		q := capacityQuota(t)
		change(&q)
		o := one(t, q)
		assert.Equal(t, "Invalid", o.CapacityGroup.State)
		assert.Empty(t, o.Capacity)
		assert.Equal(t, "Current", o.Freshness)
	}
	q := capacityQuota(t)
	c := q.Status.Capacity[0]
	q.Status.Capacity = nil
	for i := 0; i < 65; i++ {
		q.Status.Capacity = append(q.Status.Capacity, c)
	}
	o := one(t, q)
	assert.Equal(t, "Oversize", o.CapacityGroup.Reason)
}

func TestNestedGroupsValidateLateEntriesBeforeDisplayCap(t *testing.T) {
	q := capacityQuota(t)
	q.Status.Capacity[0].PerCluster = nil
	for i := 19; i >= 0; i-- {
		q.Status.Capacity[0].PerCluster = append(q.Status.Capacity[0].PerCluster, api.AcceleratorClusterCapacityStatus{Cluster: fmt.Sprintf("cluster-%02d", i), Allocatable: resource.MustParse("2"), HighWaterMark: resource.MustParse("3")})
	}
	o := one(t, q)
	require.Len(t, o.Capacity, 1)
	assert.True(t, o.Capacity[0].PerClusterGroup.Truncated)
	require.Len(t, o.Capacity[0].PerCluster, 16)
	assert.Equal(t, "cluster-00", o.Capacity[0].PerCluster[0].Cluster)
	q.Status.Capacity[0].PerCluster[19].HighWaterMark = resource.MustParse("1")
	o = one(t, q)
	assert.Equal(t, "Available", o.CapacityGroup.State)
	assert.Equal(t, "Invalid", o.Capacity[0].PerClusterGroup.State)
	assert.Empty(t, o.Capacity[0].PerCluster)
	q = wireQuota(t)
	q.Status.Budgets[0].PerCluster = append(q.Status.Budgets[0].PerCluster, q.Status.Budgets[0].PerCluster[0])
	o = one(t, q)
	assert.Equal(t, "Invalid", o.Budgets[0].PerClusterGroup.State)
	assert.Empty(t, o.Budgets[0].PerCluster)
	q = wireQuota(t)
	for len(q.Status.Budgets[0].PerCluster) < 65 {
		q.Status.Budgets[0].PerCluster = append(q.Status.Budgets[0].PerCluster, q.Status.Budgets[0].PerCluster[0])
	}
	o = one(t, q)
	assert.Equal(t, "Oversize", o.Budgets[0].PerClusterGroup.Reason)
	q = capacityQuota(t)
	for len(q.Status.Capacity[0].PerCluster) < 65 {
		q.Status.Capacity[0].PerCluster = append(q.Status.Capacity[0].PerCluster, q.Status.Capacity[0].PerCluster[0])
	}
	o = one(t, q)
	assert.Equal(t, "Oversize", o.Capacity[0].PerClusterGroup.Reason)
}

func TestUsageContradictionsAndZeroDefaults(t *testing.T) {
	for _, change := range []func(*api.AcceleratorQuota){func(q *api.AcceleratorQuota) { q.Status.Budgets[0].Reserved = resource.MustParse("2") }, func(q *api.AcceleratorQuota) { q.Status.Budgets[0].Borrowed = resource.MustParse("4") }, func(q *api.AcceleratorQuota) { q.Status.Budgets[0].Nominal = resource.MustParse("-1") }, func(q *api.AcceleratorQuota) { q.Status.Budgets[0].Nominal = resource.MustParse("1e999") }} {
		q := wireQuota(t)
		change(&q)
		o := one(t, q)
		assert.Equal(t, "Invalid", o.BudgetsGroup.State)
		assert.Empty(t, o.Budgets)
	}
	q := wireQuota(t)
	q.Status.Budgets[0].Reserved = resource.MustParse("0")
	o := one(t, q)
	assert.Equal(t, "Available", o.BudgetsGroup.State)
	assert.Equal(t, "Unknown/Defaulted", o.Budgets[0].Usage.Reserved)
	q.Status.Materialization = &api.AcceleratorQuotaMaterialization{}
	o = one(t, q)
	assert.Equal(t, "Unknown/Defaulted", o.Materialization.FreezeState)
	assert.Equal(t, "Unknown", o.Materialization.Freshness)
}

func TestProjectionConditionsAndTimeCaps(t *testing.T) {
	q := wireQuota(t)
	for len(q.Status.Clusters) < 65 {
		q.Status.Clusters = append(q.Status.Clusters, q.Status.Clusters[0])
	}
	o := one(t, q)
	assert.Equal(t, "Oversize", o.ProjectionsGroup.Reason)
	q = wireQuota(t)
	q.Status.Conditions = nil
	for i := 0; i < 65; i++ {
		q.Status.Conditions = append(q.Status.Conditions, validCondition(fmt.Sprintf("Other-%02d", i), metav1.ConditionUnknown, 0))
	}
	o = one(t, q)
	assert.Equal(t, "Oversize", o.ConditionsGroup.Reason)
	q = wireQuota(t)
	q.Status.Conditions = []metav1.Condition{validCondition("Other", metav1.ConditionUnknown, 0)}
	o = one(t, q)
	assert.Equal(t, "NoSupportedConditions", o.ConditionsGroup.Reason)
	q = wireQuota(t)
	q.Status.Conditions[0].Type = strings.Repeat("x", 65)
	o = one(t, q)
	assert.Equal(t, "Invalid", o.ConditionsGroup.State)
	q = wireQuota(t)
	q.Status.Materialization.FrozenAt = &metav1.Time{Time: time.Date(2026, 9, 16, 0, 0, 0, 0, time.UTC)}
	o = one(t, q)
	assert.Nil(t, o.Materialization)
	q = wireQuota(t)
	q.Status.Conditions = append(q.Status.Conditions, validCondition("Other", metav1.ConditionUnknown, 0))
	o = one(t, q)
	assert.Equal(t, "UnsupportedTypesOmitted", o.ConditionsGroup.Reason)
}

func TestCompleteObjectBoundAndCallerImmutabilityUnderRace(t *testing.T) {
	quotas := make([]api.AcceleratorQuota, 65)
	for i := range quotas {
		quotas[i] = wireQuota(t)
		quotas[i].Name = fmt.Sprintf("quota-%02d", 64-i)
	}
	in := Input{Quotas: quotas, Scope: "Cluster", ObservedPages: 1, ObservedItems: 65}
	r, err := Project(in, fixed)
	require.NoError(t, err)
	assert.True(t, r.Content.ObjectsGroup.Truncated)
	require.Len(t, r.Content.Objects, 64)
	assert.Equal(t, "quota-00", r.Content.Objects[0].Name)
	assert.Equal(t, "quota-63", r.Content.Objects[63].Name)
	quotas[64].Namespace = "bad"
	_, err = Project(in, fixed)
	require.Error(t, err)
	q := wireQuota(t)
	q.Status.Parent = "bad parent"
	q.Status.SourceGeneration = -1
	o := one(t, q)
	assert.Equal(t, "Invalid", o.ParentState)
	assert.Empty(t, o.ReportedParent)
	assert.Equal(t, "Invalid", o.SourceGenerationState)
	q = capacityQuota(t)
	before, _ := json.Marshal(q)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := Project(Input{Quotas: []api.AcceleratorQuota{q}, Target: q.Name, Scope: "Named", ObservedPages: 1, ObservedItems: 1}, fixed)
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
	after, _ := json.Marshal(q)
	assert.Equal(t, string(before), string(after))
	_, err = Project(Input{Scope: "Cluster", ObservedPages: 1}, nil)
	require.NoError(t, err)
}
