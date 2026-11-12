package kueue

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"
	kueueac "sigs.k8s.io/kueue/client-go/applyconfiguration/kueue/v1beta2"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func renderedPreemption(t *testing.T, policy *v1beta1.AcceleratorQuotaPreemption) *kueueac.ClusterQueuePreemptionApplyConfiguration {
	t.Helper()
	quotas := simpleTree()
	quotas[1].Spec.Preemption = policy
	objects := Render(planFor(t, quotas, "root", "team-a"), flavorSet("a100"), testOptions())
	require.Len(t, objects.ClusterQueues, 1)
	return objects.ClusterQueues[0].Spec.Preemption
}

func TestRenderPreemptionOmitsUnspecifiedPolicies(t *testing.T) {
	for name, policy := range map[string]*v1beta1.AcceleratorQuotaPreemption{
		"absent":                 nil,
		"empty":                  {},
		"empty borrowing policy": {BorrowWithinCohort: &v1beta1.AcceleratorQuotaBorrowWithinCohort{}},
	} {
		t.Run(name, func(t *testing.T) {
			require.Nil(t, renderedPreemption(t, policy), "an unspecified policy must not acquire field ownership")
		})
	}
}

func TestRenderPreemptionPolicies(t *testing.T) {
	tests := []struct {
		name   string
		policy v1beta1.AcceleratorQuotaPreemption
		want   string
	}{
		{
			name:   "reclaim without setting other policies",
			policy: v1beta1.AcceleratorQuotaPreemption{ReclaimWithinCohort: v1beta1.AcceleratorQuotaPreemptionAny},
			want:   `{"reclaimWithinCohort":"Any"}`,
		},
		{
			name:   "within queue ordering",
			policy: v1beta1.AcceleratorQuotaPreemption{WithinClusterQueue: v1beta1.AcceleratorQuotaPreemptionLowerOrNewerEqualPriority},
			want:   `{"withinClusterQueue":"LowerOrNewerEqualPriority"}`,
		},
		{
			name: "explicitly disable all policies",
			policy: v1beta1.AcceleratorQuotaPreemption{
				ReclaimWithinCohort: v1beta1.AcceleratorQuotaPreemptionNever,
				WithinClusterQueue:  v1beta1.AcceleratorQuotaPreemptionNever,
				BorrowWithinCohort: &v1beta1.AcceleratorQuotaBorrowWithinCohort{
					Policy: v1beta1.AcceleratorQuotaPreemptionNever,
				},
			},
			want: `{"reclaimWithinCohort":"Never","withinClusterQueue":"Never","borrowWithinCohort":{"policy":"Never"}}`,
		},
		{
			name: "all policies with a preemption priority threshold",
			policy: v1beta1.AcceleratorQuotaPreemption{
				ReclaimWithinCohort: v1beta1.AcceleratorQuotaPreemptionLowerPriority,
				WithinClusterQueue:  v1beta1.AcceleratorQuotaPreemptionLowerPriority,
				BorrowWithinCohort: &v1beta1.AcceleratorQuotaBorrowWithinCohort{
					Policy:               v1beta1.AcceleratorQuotaPreemptionLowerPriority,
					MaxPriorityThreshold: ptr.To[int32](100),
				},
			},
			want: `{"reclaimWithinCohort":"LowerPriority","withinClusterQueue":"LowerPriority","borrowWithinCohort":{"policy":"LowerPriority","maxPriorityThreshold":100}}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := json.Marshal(renderedPreemption(t, &tc.policy))
			require.NoError(t, err)
			require.JSONEq(t, tc.want, string(got))
		})
	}
}

func TestRenderPreemptionCopiesZeroThresholdWithoutAddingPolicy(t *testing.T) {
	policy := &v1beta1.AcceleratorQuotaPreemption{
		BorrowWithinCohort: &v1beta1.AcceleratorQuotaBorrowWithinCohort{MaxPriorityThreshold: ptr.To[int32](0)},
	}
	got := renderedPreemption(t, policy)
	encoded, err := json.Marshal(got)
	require.NoError(t, err)
	require.JSONEq(t, `{"borrowWithinCohort":{"maxPriorityThreshold":0}}`, string(encoded))

	*got.BorrowWithinCohort.MaxPriorityThreshold = 10
	require.Zero(t, *policy.BorrowWithinCohort.MaxPriorityThreshold, "rendering must not alias the source threshold")
}
