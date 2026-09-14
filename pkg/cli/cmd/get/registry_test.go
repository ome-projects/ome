package get

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestResolveCanonicalAndAlias(t *testing.T) {
	for _, name := range []string{"inferenceservices", "inferenceservice", "isvc", "ISVC"} {
		e, err := resolve(name)
		require.NoError(t, err, name)
		assert.Equal(t, "inferenceservices", e.Canonical, name)
	}
}

func TestResolveAcceleratorQuotaCanonicalAndAlias(t *testing.T) {
	for _, name := range []string{"acceleratorquotas", "acceleratorquota", "aq", "AQ"} {
		e, err := resolve(name)
		require.NoError(t, err, name)
		assert.Equal(t, "acceleratorquotas", e.Canonical, name)
	}
}

func TestResolveRolloutPolicyCanonicalAndAliases(t *testing.T) {
	for _, name := range []string{"rolloutpolicies", "rolloutpolicy", "rp", "RP"} {
		e, err := resolve(name)
		require.NoError(t, err, name)
		assert.Equal(t, "rolloutpolicies", e.Canonical, name)
		assert.True(t, e.Namespaced, name)
	}
}

func TestResolveUnknownListsChoices(t *testing.T) {
	_, err := resolve("banana")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown resource \"banana\"")
	assert.Contains(t, err.Error(), "inferenceservices") // error enumerates valid names
}

func TestRegistryHasAllResources(t *testing.T) {
	want := []string{
		"inferenceservices", "basemodels", "clusterbasemodels",
		"servingruntimes", "clusterservingruntimes", "acceleratorclasses",
		"acceleratorquotas", "benchmarkjobs", "finetunedweights", "inferencereplicas",
		"workloadclusters", "rolloutpolicies", "models", "runtimes",
	}
	var got []string
	for _, e := range registry {
		got = append(got, e.Canonical)
	}
	assert.ElementsMatch(t, want, got)
}

func TestAcceleratorQuotaTableRowsMatchVisibleColumns(t *testing.T) {
	quota := &v1beta1.AcceleratorQuota{
		Status: v1beta1.AcceleratorQuotaStatus{Budgets: []v1beta1.AcceleratorBudgetStatus{
			{ResourceName: "nvidia.com/gpu", ResourceFlavor: "h100"},
			{ResourceName: "nvidia.com/gpu", ResourceFlavor: "h200"},
		}},
	}
	for _, wide := range []bool{false, true} {
		visible := 0
		for _, column := range acceleratorQuotasEntry.Columns {
			if !column.Wide || wide {
				visible++
			}
		}
		for _, object := range []runtime.Object{
			quota,
			&v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "wrong-kind"}},
		} {
			rows := acceleratorQuotasEntry.TableRows(object, wide)
			require.NotEmpty(t, rows)
			for _, row := range rows {
				assert.Len(t, row, visible)
			}
		}
	}
}
