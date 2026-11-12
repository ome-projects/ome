package projection

import (
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/quota/tree"
)

func projectPreemptionLeaf(t *testing.T, source *tree.Tree, member string) *v1beta1.AcceleratorQuota {
	t.Helper()
	got, err := For(source, member,
		[]Allowance{{Node: "team-a", ResourceName: tpu, ResourceFlavor: flavor, Nominal: qty("50")}},
		Options{Origin: origin})
	require.NoError(t, err)
	require.Nil(t, find(got, "org").Spec.Preemption, "grouping nodes must not acquire a leaf policy")
	leaf := find(got, "team-a")
	require.NotNil(t, leaf)
	return leaf
}

func TestForPreservesOptionalPreemptionPolicy(t *testing.T) {
	for name, policy := range map[string]*v1beta1.AcceleratorQuotaPreemption{
		"absent":       nil,
		"empty":        {},
		"partial":      {ReclaimWithinCohort: v1beta1.AcceleratorQuotaPreemptionAny},
		"nested empty": {BorrowWithinCohort: &v1beta1.AcceleratorQuotaBorrowWithinCohort{}},
	} {
		t.Run(name, func(t *testing.T) {
			source := fleet(t)
			leaf, _ := source.Node("team-a")
			leaf.Quota.Spec.Preemption = policy
			got := projectPreemptionLeaf(t, source, cluster)
			require.Equal(t, policy, got.Spec.Preemption)
		})
	}
}

func TestForCopiesPreemptionIndependentlyToEachMember(t *testing.T) {
	source := fleet(t)
	leaf, _ := source.Node("team-a")
	leaf.Quota.Spec.Preemption = &v1beta1.AcceleratorQuotaPreemption{
		ReclaimWithinCohort: v1beta1.AcceleratorQuotaPreemptionAny,
		WithinClusterQueue:  v1beta1.AcceleratorQuotaPreemptionLowerOrNewerEqualPriority,
		BorrowWithinCohort: &v1beta1.AcceleratorQuotaBorrowWithinCohort{
			Policy:               v1beta1.AcceleratorQuotaPreemptionLowerPriority,
			MaxPriorityThreshold: ptr.To[int32](0),
		},
	}
	first := projectPreemptionLeaf(t, source, "member-a").Spec.Preemption
	second := projectPreemptionLeaf(t, source, "member-b").Spec.Preemption
	require.Equal(t, leaf.Quota.Spec.Preemption, first)
	require.Equal(t, leaf.Quota.Spec.Preemption, second)

	first.ReclaimWithinCohort = v1beta1.AcceleratorQuotaPreemptionNever
	first.WithinClusterQueue = v1beta1.AcceleratorQuotaPreemptionNever
	first.BorrowWithinCohort.Policy = v1beta1.AcceleratorQuotaPreemptionNever
	*first.BorrowWithinCohort.MaxPriorityThreshold = 100
	require.Equal(t, v1beta1.AcceleratorQuotaPreemptionAny, second.ReclaimWithinCohort)
	require.Equal(t, v1beta1.AcceleratorQuotaPreemptionLowerOrNewerEqualPriority, second.WithinClusterQueue)
	require.Equal(t, v1beta1.AcceleratorQuotaPreemptionLowerPriority, second.BorrowWithinCohort.Policy)
	require.Zero(t, *second.BorrowWithinCohort.MaxPriorityThreshold)
	require.Equal(t, second, leaf.Quota.Spec.Preemption, "editing one member must not change the hub or another member")
}
