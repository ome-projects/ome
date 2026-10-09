package artifactinventory

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestBuildUnknownSizesRetainMapping(t *testing.T) {
	for _, scenario := range []struct {
		name string
		code string
	}{
		{"missingJob", "jobMissing"},
		{"missingSize", "sizeUnavailable"},
		{"zeroSize", "sizeUnavailable"},
		{"runningJob", "jobNotCompleted"},
		{"destinationMismatch", "destinationMismatch"},
		{"regionMismatch", "regionMismatch"},
		{"revisionMismatch", "revisionMismatch"},
		{"filteredFiles", "filteredFiles"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			m := testModel("customer", "m", "/models/m")
			job := testJob("m", 100)
			switch scenario.name {
			case "missingSize":
				unstructured.RemoveNestedField(job.Object, "status", "observed")
			case "zeroSize":
				require.NoError(t, unstructured.SetNestedField(job.Object, int64(0), "status", "observed", "sourceArtifactSizeBytes"))
			case "runningJob":
				require.NoError(t, unstructured.SetNestedField(job.Object, "Running", "status", "status"))
			case "destinationMismatch":
				require.NoError(t, unstructured.SetNestedField(job.Object, "oci://different", "spec", "destination", "storageUri"))
			case "regionMismatch":
				require.NoError(t, unstructured.SetNestedField(job.Object, "different", "spec", "destination", "parameters", "region"))
			case "revisionMismatch":
				require.NoError(t, unstructured.SetNestedField(job.Object, "different", "spec", "destination", "parameters", "revision"))
			case "filteredFiles":
				m.Spec.ModelFormat.Name = "TensorRTLLM"
			}
			objects := []client.Object{testCM(t, m), m}
			if scenario.name != "missingJob" {
				objects = append(objects, job)
			}
			r := newTestReader(t, objects...)
			result, err := NewBuilder(r, "").Build(context.Background(), "node-a")
			require.NoError(t, err)
			require.Len(t, result.Artifacts, 1)
			a := result.Artifacts[0]
			require.Len(t, a.Models, 1)
			require.Nil(t, a.EstimatedArtifactBytes)
			require.False(t, a.EvictionEligible)
			require.Equal(t, scenario.code, a.Issues[0].Code)
			require.Equal(t, modelKey(m), a.Issues[0].ModelKey)
			encoded, err := json.Marshal(a)
			require.NoError(t, err)
			require.Contains(t, string(encoded), `"estimatedArtifactBytes":null`)
		})
	}
}

func TestBuildSameJobDifferentPaths(t *testing.T) {
	a := testModel("customer-a", "m", "/models/a")
	b := testModel("customer-b", "m", "/models/b")
	// There is deliberately no default/m BaseModel: only the Job is needed.
	r := newTestReader(t, testCM(t, a, b), a, b, testJob("m", 100))
	result, err := NewBuilder(r, "").Build(context.Background(), "node-a")
	require.NoError(t, err)
	require.Len(t, result.Artifacts, 2)
	for _, a := range result.Artifacts {
		require.True(t, a.EvictionEligible)
		require.EqualValues(t, 100, *a.EstimatedArtifactBytes)
	}
	require.Equal(t, 1, r.reads["ReplicationJob:default/m"])
}

func TestBuildSharedJobValidatedForEveryModel(t *testing.T) {
	a := testModel("default", "m", "/models/m")
	b := testModel("customer", "m", "/models/m")
	(*b.Spec.Storage.Parameters)["revision"] = "different"
	r := newTestReader(t, testCM(t, a, b), a, b, testJob("m", 100))
	result, err := NewBuilder(r, "").Build(context.Background(), "node-a")
	require.NoError(t, err)
	require.Len(t, result.Artifacts, 1)
	aResult := result.Artifacts[0]
	require.Len(t, aResult.Models, 2)
	require.False(t, aResult.EvictionEligible)
	require.Nil(t, aResult.EstimatedArtifactBytes)
	require.Equal(t, "revisionMismatch", aResult.Issues[0].Code)
	require.Equal(t, 1, r.reads["ReplicationJob:default/m"])
}

func TestBuildDifferentJobsDoNotAddSizes(t *testing.T) {
	a := testModel("default", "a", "/models/a")
	b := testModel("default", "b", "/models/b")
	cm := testCM(t, a, b)
	shareModels(t, cm, a, b)
	r := newTestReader(t, cm, a, b, testJob("a", 100), testJob("b", 100))
	result, err := NewBuilder(r, "").Build(context.Background(), "node-a")
	require.NoError(t, err)
	require.Len(t, result.Artifacts, 1)
	require.Len(t, result.Artifacts[0].Models, 2)
	require.Nil(t, result.Artifacts[0].EstimatedArtifactBytes)
	require.False(t, result.Artifacts[0].EvictionEligible)
	require.Equal(t, "multipleJobs", result.Artifacts[0].Issues[0].Code)
}

func TestBuildCachesMissingJob(t *testing.T) {
	a := testModel("default", "m", "/models/a")
	b := testModel("customer", "m", "/models/b")
	r := newTestReader(t, testCM(t, a, b), a, b)
	result, err := NewBuilder(r, "").Build(context.Background(), "node-a")
	require.NoError(t, err)
	require.Len(t, result.Artifacts, 2)
	require.Equal(t, 1, r.reads["ReplicationJob:default/m"])
}

func TestReadJobPreservesIntegerSize(t *testing.T) {
	const size = int64(1<<53 + 1)
	r := newTestReader(t, testJob("m", size))
	job, err := NewBuilder(r, "").readJob(context.Background(), "m")
	require.NoError(t, err)
	require.Equal(t, size, *job.Status.Observed.SourceArtifactSizeBytes)
}
