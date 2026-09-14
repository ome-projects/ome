package instanceprojection_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/instancecollection"
	"sigs.k8s.io/ome/pkg/cli/instanceprojection"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestProjectUsesCurrentInstanceStatusesAndQualifiesStaleAndSparseEvidence(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 14, 20, 0, 0, 0, time.UTC)
	isvc := instanceISVC()
	engine := instanceReplica(isvc, "chat-engine", omev1beta1.EngineComponent, 2, 2)
	engine.Status.Replicas = 2
	engine.Status.ReadyReplicas = 1
	engine.Status.ServingReplicas = 1
	engine.Status.AvailableReplicas = 1
	engine.Status.UpdatedReplicas = 1
	engine.Status.UpdatedReadyReplicas = 1
	engine.Status.CurrentRevision = "chat-engine-old"
	engine.Status.UpdateRevision = "chat-engine-new"
	engine.Status.InstanceStatuses = []omev1beta1.OMENativeInstanceStatus{
		{
			Index: 2, Incarnation: 4, Phase: omev1beta1.OMENativeInstanceUpdating,
			RunningRevision: "chat-engine-old", TargetRevision: "chat-engine-new",
			PodCount: 4, ReadyPodCount: 2, ServingPodCount: 1, AvailablePodCount: 1,
			Admitted: true, Operation: &omev1beta1.InstanceOperation{ID: "op"},
			LastFailure: &omev1beta1.InstanceTermination{PodName: "old-pod"},
		},
		{
			Index: 0, Incarnation: 1, Phase: omev1beta1.OMENativeInstanceReady,
			RunningRevision: "chat-engine-old", PodCount: 1, ReadyPodCount: 0,
			ServingPodCount: 1, AvailablePodCount: 1, Admitted: true,
		},
	}
	decoder := instanceReplica(isvc, "chat-decoder", omev1beta1.DecoderComponent, 4, 3)
	decoder.Status.Replicas = 1
	decoder.Status.InstanceStatuses = []omev1beta1.OMENativeInstanceStatus{{
		Index: 0, Phase: omev1beta1.OMENativeInstancePending,
	}}

	got, err := instanceprojection.Project(instanceprojection.Input{
		InferenceService: isvc,
		Collection: instancecollection.Result{
			Items: []omev1beta1.InferenceReplica{decoder, engine}, Pages: 2,
		},
		MaxInstances: 100,
	}, reportv1alpha1.ClockFunc(func() time.Time { return now }))
	require.NoError(t, err)

	assert.Equal(t, reportv1alpha1.InstanceListSummary{
		State: reportv1alpha1.InstanceListStatePartial, Components: 2, Instances: 3,
	}, got.Content.Summary)
	require.Len(t, got.Content.Components, 2)
	assert.Equal(t, reportv1alpha1.RuntimeComponentEngine, got.Content.Components[0].Type)
	assert.Equal(t, reportv1alpha1.InstanceIndexSetSparse, got.Content.Components[0].IndexSet)
	assert.Equal(t, reportv1alpha1.InstanceEvidenceReported, got.Content.Components[0].State)
	assert.Equal(t, int32(1), got.Content.Components[0].UpdatedReplicas)
	assert.Equal(t, int32(1), got.Content.Components[0].UpdatedReadyReplicas)
	assert.Equal(t, reportv1alpha1.InstanceEvidenceStale, got.Content.Components[1].State)

	require.Len(t, got.Content.Instances, 3)
	assert.Equal(t, []int32{0, 2, 0}, []int32{
		got.Content.Instances[0].Index, got.Content.Instances[1].Index, got.Content.Instances[2].Index,
	})
	assert.Equal(t, reportv1alpha1.InstancePodCounts{
		Total: 1, Serving: 1, Available: 1,
	}, got.Content.Instances[0].Pods)
	assert.Equal(t, reportv1alpha1.InstancePodCounts{
		Total: 4, Serving: 1, Available: 1,
	}, got.Content.Instances[1].Pods, "the non-persisted ReadyPodCount compatibility field must be ignored")
	assert.True(t, got.Content.Instances[1].Admitted)
	assert.True(t, got.Content.Instances[1].OperationPresent)
	assert.True(t, got.Content.Instances[1].LastFailurePresent)
	assert.Equal(t, reportv1alpha1.InstanceEvidenceStale, got.Content.Instances[2].Evidence)

	assert.Contains(t, got.Content.Issues, reportv1alpha1.InstanceListIssue{
		Code: reportv1alpha1.InstanceIssueSparseIndices, Component: reportv1alpha1.RuntimeComponentEngine,
		InferenceReplica: "chat-engine",
	})
	assert.Contains(t, got.Content.Issues, reportv1alpha1.InstanceListIssue{
		Code: reportv1alpha1.InstanceIssueStaleGeneration, Component: reportv1alpha1.RuntimeComponentDecoder,
		InferenceReplica: "chat-decoder",
	})
	assert.Equal(t, []reportv1alpha1.InstanceListWarning{{Code: reportv1alpha1.WarningStaleEvidence}}, got.Warnings)
	require.Len(t, got.Sources, 3)
	assert.Equal(t, now, got.CollectedAt)
}

func TestProjectRejectsDuplicateComponentIdentity(t *testing.T) {
	t.Parallel()

	isvc := instanceISVC()
	first := instanceReplica(isvc, "chat-engine-a", omev1beta1.EngineComponent, 1, 1)
	first.Status.InstanceStatuses = []omev1beta1.OMENativeInstanceStatus{{
		Index: 0, Phase: omev1beta1.OMENativeInstanceReady,
	}}
	second := instanceReplica(isvc, "chat-engine-b", omev1beta1.EngineComponent, 1, 1)
	second.Status.InstanceStatuses = []omev1beta1.OMENativeInstanceStatus{{
		Index: 1, Phase: omev1beta1.OMENativeInstanceReady,
	}}

	got, err := instanceprojection.Project(instanceprojection.Input{
		InferenceService: isvc,
		Collection: instancecollection.Result{
			Items: []omev1beta1.InferenceReplica{second, first}, Pages: 1,
		},
		MaxInstances: 100,
	}, reportv1alpha1.ClockFunc(func() time.Time { return time.Unix(1, 0) }))
	require.NoError(t, err)

	assert.Equal(t, reportv1alpha1.InstanceListStatePartial, got.Content.Summary.State)
	assert.Equal(t, 1, got.Content.Summary.Components)
	assert.Zero(t, got.Content.Summary.Instances)
	assert.Empty(t, got.Content.Instances)
	require.Len(t, got.Content.Components, 2)
	for _, component := range got.Content.Components {
		assert.Equal(t, reportv1alpha1.InstanceEvidenceMalformed, component.State)
		assert.Equal(t, reportv1alpha1.InstanceIndexSetMalformed, component.IndexSet)
		assert.Contains(t, got.Content.Issues, reportv1alpha1.InstanceListIssue{
			Code:             reportv1alpha1.InstanceIssueDuplicateComponent,
			Component:        reportv1alpha1.RuntimeComponentEngine,
			InferenceReplica: component.InferenceReplica,
		})
	}
	assert.Equal(t, []reportv1alpha1.InstanceListWarning{{Code: reportv1alpha1.WarningPartialData}}, got.Warnings)
}

func TestProjectRejectsDuplicateInstanceIndex(t *testing.T) {
	t.Parallel()

	isvc := instanceISVC()
	ir := instanceReplica(isvc, "chat-engine", omev1beta1.EngineComponent, 1, 1)
	ir.Status.InstanceStatuses = []omev1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: omev1beta1.OMENativeInstanceReady},
		{Index: 0, Phase: omev1beta1.OMENativeInstanceUpdating},
	}

	got, err := instanceprojection.Project(instanceprojection.Input{
		InferenceService: isvc,
		Collection:       instancecollection.Result{Items: []omev1beta1.InferenceReplica{ir}, Pages: 1},
		MaxInstances:     100,
	}, reportv1alpha1.ClockFunc(func() time.Time { return time.Unix(1, 0) }))
	require.NoError(t, err)

	assert.Empty(t, got.Content.Instances)
	require.Len(t, got.Content.Components, 1)
	assert.Equal(t, reportv1alpha1.InstanceEvidenceMalformed, got.Content.Components[0].State)
	assert.Equal(t, reportv1alpha1.InstanceIndexSetMalformed, got.Content.Components[0].IndexSet)
	index := int32(0)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.InstanceListIssue{
		Code: reportv1alpha1.InstanceIssueDuplicateIndex, Component: reportv1alpha1.RuntimeComponentEngine,
		InferenceReplica: "chat-engine", Index: &index,
	})
}

func TestProjectChoosesDuplicateIndexDeterministically(t *testing.T) {
	t.Parallel()

	isvc := instanceISVC()
	project := func(indices []int32) reportv1alpha1.InstanceListReport {
		ir := instanceReplica(isvc, "chat-engine", omev1beta1.EngineComponent, 1, 1)
		ir.Status.Replicas = int32(len(indices))
		for _, index := range indices {
			ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses, omev1beta1.OMENativeInstanceStatus{
				Index: index, Phase: omev1beta1.OMENativeInstanceReady,
			})
		}
		got, err := instanceprojection.Project(instanceprojection.Input{
			InferenceService: isvc,
			Collection:       instancecollection.Result{Items: []omev1beta1.InferenceReplica{ir}, Pages: 1},
			MaxInstances:     100,
		}, reportv1alpha1.ClockFunc(func() time.Time { return time.Unix(1, 0) }))
		require.NoError(t, err)
		return got
	}

	left := project([]int32{5, 5, 1, 1})
	right := project([]int32{1, 1, 5, 5})

	assert.Equal(t, left, right)
	index := int32(1)
	assert.Contains(t, left.Content.Issues, reportv1alpha1.InstanceListIssue{
		Code: reportv1alpha1.InstanceIssueDuplicateIndex, Component: reportv1alpha1.RuntimeComponentEngine,
		InferenceReplica: "chat-engine", Index: &index,
	})
}

func TestProjectRejectsMalformedInstanceStatuses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*omev1beta1.OMENativeInstanceStatus)
		issue  reportv1alpha1.InstanceIssueCode
	}{
		{
			name: "negative index",
			mutate: func(row *omev1beta1.OMENativeInstanceStatus) {
				row.Index = -1
			},
			issue: reportv1alpha1.InstanceIssueIndexInvalid,
		},
		{
			name: "negative incarnation",
			mutate: func(row *omev1beta1.OMENativeInstanceStatus) {
				row.Incarnation = -1
			},
			issue: reportv1alpha1.InstanceIssueIncarnationInvalid,
		},
		{
			name: "unknown phase",
			mutate: func(row *omev1beta1.OMENativeInstanceStatus) {
				row.Phase = omev1beta1.OMENativeInstancePhase("Future")
			},
			issue: reportv1alpha1.InstanceIssuePhaseInvalid,
		},
		{
			name: "invalid revision",
			mutate: func(row *omev1beta1.OMENativeInstanceStatus) {
				row.RunningRevision = "BAD REVISION"
			},
			issue: reportv1alpha1.InstanceIssueInstanceRevisionInvalid,
		},
		{
			name: "negative pod count",
			mutate: func(row *omev1beta1.OMENativeInstanceStatus) {
				row.PodCount = -1
			},
			issue: reportv1alpha1.InstanceIssuePodCountsInvalid,
		},
		{
			name: "serving exceeds total",
			mutate: func(row *omev1beta1.OMENativeInstanceStatus) {
				row.PodCount = 1
				row.ServingPodCount = 2
			},
			issue: reportv1alpha1.InstanceIssuePodCountsInvalid,
		},
		{
			name: "available exceeds serving",
			mutate: func(row *omev1beta1.OMENativeInstanceStatus) {
				row.PodCount = 2
				row.AvailablePodCount = 2
				row.ServingPodCount = 1
			},
			issue: reportv1alpha1.InstanceIssuePodCountsInvalid,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			isvc := instanceISVC()
			ir := instanceReplica(isvc, "chat-engine", omev1beta1.EngineComponent, 1, 1)
			row := omev1beta1.OMENativeInstanceStatus{
				Index: 2, Incarnation: 1, Phase: omev1beta1.OMENativeInstanceReady,
				RunningRevision: "chat-engine-a", PodCount: 1, ReadyPodCount: 1,
				ServingPodCount: 1, AvailablePodCount: 1,
			}
			test.mutate(&row)
			ir.Status.InstanceStatuses = []omev1beta1.OMENativeInstanceStatus{row}

			got, err := instanceprojection.Project(instanceprojection.Input{
				InferenceService: isvc,
				Collection:       instancecollection.Result{Items: []omev1beta1.InferenceReplica{ir}, Pages: 1},
				MaxInstances:     100,
			}, reportv1alpha1.ClockFunc(func() time.Time { return time.Unix(1, 0) }))
			require.NoError(t, err)

			assert.Empty(t, got.Content.Instances)
			require.Len(t, got.Content.Components, 1)
			assert.Equal(t, reportv1alpha1.InstanceEvidenceMalformed, got.Content.Components[0].State)
			assert.Equal(t, reportv1alpha1.InstanceIndexSetMalformed, got.Content.Components[0].IndexSet)
			assert.Contains(t, got.Content.Issues, reportv1alpha1.InstanceListIssue{
				Code: test.issue, Component: reportv1alpha1.RuntimeComponentEngine,
				InferenceReplica: "chat-engine", Index: &row.Index,
			})
		})
	}
}

func TestProjectDoesNotTreatUnobservedStatusAsInstanceEvidence(t *testing.T) {
	t.Parallel()

	isvc := instanceISVC()
	ir := instanceReplica(isvc, "chat-engine", omev1beta1.EngineComponent, 1, 0)
	ir.Status.InstanceStatuses = []omev1beta1.OMENativeInstanceStatus{{
		Index: 0, Phase: omev1beta1.OMENativeInstanceReady,
	}}

	got, err := instanceprojection.Project(instanceprojection.Input{
		InferenceService: isvc,
		Collection:       instancecollection.Result{Items: []omev1beta1.InferenceReplica{ir}, Pages: 1},
		MaxInstances:     100,
	}, reportv1alpha1.ClockFunc(func() time.Time { return time.Unix(1, 0) }))
	require.NoError(t, err)

	assert.Empty(t, got.Content.Instances)
	require.Len(t, got.Content.Components, 1)
	assert.Equal(t, reportv1alpha1.InstanceEvidenceUnavailable, got.Content.Components[0].State)
	assert.Equal(t, reportv1alpha1.InstanceIndexSetNotReported, got.Content.Components[0].IndexSet)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.InstanceListIssue{
		Code: reportv1alpha1.InstanceIssueStatusUnobserved, Component: reportv1alpha1.RuntimeComponentEngine,
		InferenceReplica: "chat-engine",
	})
}

func TestProjectTreatsFreshZeroReplicaStatusAsReportedEmpty(t *testing.T) {
	t.Parallel()

	isvc := instanceISVC()
	ir := instanceReplica(isvc, "chat-engine", omev1beta1.EngineComponent, 1, 1)
	ir.Status.Replicas = 0

	got, err := instanceprojection.Project(instanceprojection.Input{
		InferenceService: isvc,
		Collection:       instancecollection.Result{Items: []omev1beta1.InferenceReplica{ir}, Pages: 1},
		MaxInstances:     100,
	}, reportv1alpha1.ClockFunc(func() time.Time { return time.Unix(1, 0) }))
	require.NoError(t, err)

	assert.Equal(t, reportv1alpha1.InstanceListStateReported, got.Content.Summary.State)
	assert.Zero(t, got.Content.Summary.Instances)
	require.Len(t, got.Content.Components, 1)
	assert.Equal(t, reportv1alpha1.InstanceEvidenceReported, got.Content.Components[0].State)
	assert.Equal(t, reportv1alpha1.InstanceIndexSetEmpty, got.Content.Components[0].IndexSet)
	assert.Empty(t, got.Content.Issues)
	require.Len(t, got.Table().Rows, 1)
	assert.Equal(t, "EMPTY", got.Table().Rows[0][6])
}

func TestProjectRejectsMalformedInferenceReplicaSummary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*omev1beta1.InferenceReplica)
		issue  reportv1alpha1.InstanceIssueCode
	}{
		{
			name: "future observed generation",
			mutate: func(ir *omev1beta1.InferenceReplica) {
				ir.Status.ObservedGeneration = ir.Generation + 1
			},
			issue: reportv1alpha1.InstanceIssueObservedGenerationInvalid,
		},
		{
			name: "negative observed generation",
			mutate: func(ir *omev1beta1.InferenceReplica) {
				ir.Status.ObservedGeneration = -1
			},
			issue: reportv1alpha1.InstanceIssueObservedGenerationInvalid,
		},
		{
			name: "aggregate ready exceeds replicas",
			mutate: func(ir *omev1beta1.InferenceReplica) {
				ir.Status.Replicas = 1
				ir.Status.ReadyReplicas = 2
			},
			issue: reportv1alpha1.InstanceIssueAggregateInvalid,
		},
		{
			name: "aggregate replicas disagree with instance rows",
			mutate: func(ir *omev1beta1.InferenceReplica) {
				ir.Status.Replicas = 2
			},
			issue: reportv1alpha1.InstanceIssueAggregateInvalid,
		},
		{
			name: "aggregate serving exceeds ready",
			mutate: func(ir *omev1beta1.InferenceReplica) {
				ir.Status.ReadyReplicas = 0
				ir.Status.ServingReplicas = 1
			},
			issue: reportv1alpha1.InstanceIssueAggregateInvalid,
		},
		{
			name: "aggregate available exceeds serving",
			mutate: func(ir *omev1beta1.InferenceReplica) {
				ir.Status.ServingReplicas = 0
				ir.Status.AvailableReplicas = 1
			},
			issue: reportv1alpha1.InstanceIssueAggregateInvalid,
		},
		{
			name: "aggregate updated ready exceeds ready",
			mutate: func(ir *omev1beta1.InferenceReplica) {
				ir.Status.ReadyReplicas = 0
				ir.Status.UpdatedReplicas = 1
				ir.Status.UpdatedReadyReplicas = 1
			},
			issue: reportv1alpha1.InstanceIssueAggregateInvalid,
		},
		{
			name: "invalid current revision",
			mutate: func(ir *omev1beta1.InferenceReplica) {
				ir.Status.CurrentRevision = "BAD REVISION"
			},
			issue: reportv1alpha1.InstanceIssueRevisionInvalid,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			isvc := instanceISVC()
			ir := instanceReplica(isvc, "chat-engine", omev1beta1.EngineComponent, 1, 1)
			ir.Status.InstanceStatuses = []omev1beta1.OMENativeInstanceStatus{{
				Index: 0, Phase: omev1beta1.OMENativeInstanceReady,
			}}
			test.mutate(&ir)

			got, err := instanceprojection.Project(instanceprojection.Input{
				InferenceService: isvc,
				Collection:       instancecollection.Result{Items: []omev1beta1.InferenceReplica{ir}, Pages: 1},
				MaxInstances:     100,
			}, reportv1alpha1.ClockFunc(func() time.Time { return time.Unix(1, 0) }))
			require.NoError(t, err)

			assert.Empty(t, got.Content.Instances)
			require.Len(t, got.Content.Components, 1)
			assert.Equal(t, reportv1alpha1.InstanceEvidenceMalformed, got.Content.Components[0].State)
			assert.Contains(t, got.Content.Issues, reportv1alpha1.InstanceListIssue{
				Code: test.issue, Component: reportv1alpha1.RuntimeComponentEngine,
				InferenceReplica: "chat-engine",
			})
		})
	}
}

func TestProjectReportsMissingStatusesEvenWhenGenerationIsStale(t *testing.T) {
	t.Parallel()

	isvc := instanceISVC()
	ir := instanceReplica(isvc, "chat-engine", omev1beta1.EngineComponent, 3, 2)
	ir.Status.Replicas = 2
	ir.Status.InstanceStatuses = nil

	got, err := instanceprojection.Project(instanceprojection.Input{
		InferenceService: isvc,
		Collection:       instancecollection.Result{Items: []omev1beta1.InferenceReplica{ir}, Pages: 1},
		MaxInstances:     100,
	}, reportv1alpha1.ClockFunc(func() time.Time { return time.Unix(1, 0) }))
	require.NoError(t, err)

	require.Len(t, got.Content.Components, 1)
	assert.Equal(t, reportv1alpha1.InstanceEvidenceStale, got.Content.Components[0].State)
	assert.Equal(t, reportv1alpha1.InstanceIndexSetNotReported, got.Content.Components[0].IndexSet)
	assert.Empty(t, got.Content.Instances)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.InstanceListIssue{
		Code: reportv1alpha1.InstanceIssueStaleGeneration, Component: reportv1alpha1.RuntimeComponentEngine,
		InferenceReplica: "chat-engine",
	})
	assert.Contains(t, got.Content.Issues, reportv1alpha1.InstanceListIssue{
		Code: reportv1alpha1.InstanceIssueStatusesNotReported, Component: reportv1alpha1.RuntimeComponentEngine,
		InferenceReplica: "chat-engine",
	})
	assert.Contains(t, got.Warnings, reportv1alpha1.InstanceListWarning{Code: reportv1alpha1.WarningStaleEvidence})
	assert.Contains(t, got.Warnings, reportv1alpha1.InstanceListWarning{Code: reportv1alpha1.WarningSourceUnavailable})
	assert.Equal(t, "NOT-REPORTED", got.Table().Rows[0][6])
}

func TestProjectQualifiesParentProjectionGenerationEvidence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		stamp       *string
		wantState   reportv1alpha1.InstanceEvidenceState
		wantIssue   reportv1alpha1.InstanceIssueCode
		wantRows    int
		wantWarning reportv1alpha1.WarningCode
	}{
		{
			name: "missing", wantState: reportv1alpha1.InstanceEvidenceUnavailable,
			wantIssue:   reportv1alpha1.InstanceIssueParentGenerationMissing,
			wantWarning: reportv1alpha1.WarningSourceUnavailable,
		},
		{
			name: "malformed", stamp: stringPointer("SECRET\u202e\nnot-a-generation"),
			wantState:   reportv1alpha1.InstanceEvidenceMalformed,
			wantIssue:   reportv1alpha1.InstanceIssueParentGenerationInvalid,
			wantWarning: reportv1alpha1.WarningPartialData,
		},
		{
			name: "unbounded malformed", stamp: stringPointer("SECRET" + strings.Repeat("9", 10_000)),
			wantState:   reportv1alpha1.InstanceEvidenceMalformed,
			wantIssue:   reportv1alpha1.InstanceIssueParentGenerationInvalid,
			wantWarning: reportv1alpha1.WarningPartialData,
		},
		{
			name: "behind", stamp: stringPointer("6"),
			wantState: reportv1alpha1.InstanceEvidenceStale,
			wantIssue: reportv1alpha1.InstanceIssueParentGenerationStale, wantRows: 1,
			wantWarning: reportv1alpha1.WarningStaleEvidence,
		},
		{
			name: "ahead", stamp: stringPointer("8"),
			wantState:   reportv1alpha1.InstanceEvidenceMalformed,
			wantIssue:   reportv1alpha1.InstanceIssueParentGenerationAhead,
			wantWarning: reportv1alpha1.WarningPartialData,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			isvc := instanceISVC()
			ir := instanceReplica(isvc, "chat-engine", omev1beta1.EngineComponent, 1, 1)
			ir.Status.InstanceStatuses = []omev1beta1.OMENativeInstanceStatus{{
				Index: 0, Phase: omev1beta1.OMENativeInstanceReady,
			}}
			if test.stamp == nil {
				delete(ir.Annotations, constants.InferenceReplicaParentGenerationAnnotationKey)
			} else {
				ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] = *test.stamp
			}

			got, err := instanceprojection.Project(instanceprojection.Input{
				InferenceService: isvc,
				Collection: instancecollection.Result{
					Items: []omev1beta1.InferenceReplica{ir}, Pages: 1,
				},
				MaxInstances: 100,
			}, reportv1alpha1.ClockFunc(func() time.Time { return time.Unix(1, 0) }))
			require.NoError(t, err)

			require.Len(t, got.Content.Components, 1)
			assert.Equal(t, test.wantState, got.Content.Components[0].State)
			assert.Len(t, got.Content.Instances, test.wantRows)
			assert.Contains(t, got.Content.Issues, reportv1alpha1.InstanceListIssue{
				Code: test.wantIssue, Component: reportv1alpha1.RuntimeComponentEngine,
				InferenceReplica: "chat-engine",
			})
			assert.Contains(t, got.Warnings, reportv1alpha1.InstanceListWarning{Code: test.wantWarning})
			encoded, marshalErr := json.Marshal(got)
			require.NoError(t, marshalErr)
			assert.NotContains(t, string(encoded), "SECRET")
		})
	}
}

func TestProjectDoesNotDowngradeMalformedParentGenerationToStale(t *testing.T) {
	t.Parallel()

	isvc := instanceISVC()
	ir := instanceReplica(isvc, "chat-engine", omev1beta1.EngineComponent, 2, 1)
	ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] = "8"
	ir.Status.InstanceStatuses = []omev1beta1.OMENativeInstanceStatus{{
		Index: 0, Phase: omev1beta1.OMENativeInstanceReady,
	}}

	got, err := instanceprojection.Project(instanceprojection.Input{
		InferenceService: isvc,
		Collection: instancecollection.Result{
			Items: []omev1beta1.InferenceReplica{ir}, Pages: 1,
		},
		MaxInstances: 100,
	}, reportv1alpha1.ClockFunc(func() time.Time { return time.Unix(1, 0) }))
	require.NoError(t, err)

	require.Len(t, got.Content.Components, 1)
	assert.Equal(t, reportv1alpha1.InstanceEvidenceMalformed, got.Content.Components[0].State)
	assert.Empty(t, got.Content.Instances)
}

func TestProjectRejectsOversizedNestedStatusBeforeScanningRows(t *testing.T) {
	t.Parallel()

	isvc := instanceISVC()
	ir := instanceReplica(isvc, "chat-engine", omev1beta1.EngineComponent, 10_000, 10_000)
	ir.Status.Replicas = 10_000
	ir.Status.InstanceStatuses = make([]omev1beta1.OMENativeInstanceStatus, 10_000)
	for index := range ir.Status.InstanceStatuses {
		ir.Status.InstanceStatuses[index] = omev1beta1.OMENativeInstanceStatus{
			Index: int32(index), Phase: omev1beta1.OMENativeInstancePhase("SECRET-INVALID-PHASE"),
		}
	}

	got, err := instanceprojection.Project(instanceprojection.Input{
		InferenceService: isvc,
		Collection: instancecollection.Result{
			Items: []omev1beta1.InferenceReplica{ir}, Pages: 1,
		},
		MaxInstances: 1,
	}, reportv1alpha1.ClockFunc(func() time.Time { return time.Unix(1, 0) }))
	require.NoError(t, err)

	assert.Empty(t, got.Content.Instances)
	assert.True(t, got.Content.Summary.Truncated)
	require.Len(t, got.Content.Components, 1)
	assert.Equal(t, reportv1alpha1.InstanceEvidenceUnavailable, got.Content.Components[0].State)
	assert.Equal(t, reportv1alpha1.InstanceIndexSetNotReported, got.Content.Components[0].IndexSet)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.InstanceListIssue{
		Code:      reportv1alpha1.InstanceIssueStatusRowsTruncated,
		Component: reportv1alpha1.RuntimeComponentEngine, InferenceReplica: "chat-engine",
	})
	for _, issue := range got.Content.Issues {
		assert.NotEqual(t, reportv1alpha1.InstanceIssuePhaseInvalid, issue.Code,
			"rows beyond the hard work cap must not be scanned")
	}
}

func TestProjectQualifiesCollectorStatusRowTruncation(t *testing.T) {
	t.Parallel()

	isvc := instanceISVC()
	ir := instanceReplica(isvc, "chat-engine", omev1beta1.EngineComponent, 1, 1)
	ir.Status.Replicas = 2

	got, err := instanceprojection.Project(instanceprojection.Input{
		InferenceService: isvc,
		Collection: instancecollection.Result{
			Items: []omev1beta1.InferenceReplica{ir}, Pages: 1,
			StatusRowsTruncated: []instancecollection.StatusRowsTruncation{{
				Name: ir.Name, Component: ir.Spec.Component,
			}},
		},
		MaxInstances: 1,
	}, reportv1alpha1.ClockFunc(func() time.Time { return time.Unix(1, 0) }))
	require.NoError(t, err)

	assert.Empty(t, got.Content.Instances)
	assert.True(t, got.Content.Summary.Truncated)
	require.Len(t, got.Content.Components, 1)
	assert.Equal(t, reportv1alpha1.InstanceEvidenceUnavailable, got.Content.Components[0].State)
	assert.Equal(t, reportv1alpha1.InstanceIndexSetNotReported, got.Content.Components[0].IndexSet)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.InstanceListIssue{
		Code:      reportv1alpha1.InstanceIssueStatusRowsTruncated,
		Component: reportv1alpha1.RuntimeComponentEngine, InferenceReplica: "chat-engine",
	})
	assert.NotContains(t, got.Content.Issues, reportv1alpha1.InstanceListIssue{
		Code:      reportv1alpha1.InstanceIssueStatusesNotReported,
		Component: reportv1alpha1.RuntimeComponentEngine, InferenceReplica: "chat-engine",
	})
	assert.Contains(t, got.Warnings, reportv1alpha1.InstanceListWarning{Code: reportv1alpha1.WarningTruncated})
	assert.Contains(t, got.Warnings, reportv1alpha1.InstanceListWarning{Code: reportv1alpha1.WarningSourceUnavailable})
	assert.Equal(t, "ROWS-LIMIT", got.Table().Rows[0][6])
}

func TestProjectBoundsMalformedRowEvidenceDeterministicallyAcrossInputOrder(t *testing.T) {
	t.Parallel()

	isvc := instanceISVC()
	project := func(rows []omev1beta1.OMENativeInstanceStatus) reportv1alpha1.InstanceListReport {
		ir := instanceReplica(isvc, "chat-engine", omev1beta1.EngineComponent, 2, 2)
		ir.Status.Replicas = int32(len(rows))
		ir.Status.InstanceStatuses = rows
		got, err := instanceprojection.Project(instanceprojection.Input{
			InferenceService: isvc,
			Collection:       instancecollection.Result{Items: []omev1beta1.InferenceReplica{ir}, Pages: 1},
			MaxInstances:     2,
		}, reportv1alpha1.ClockFunc(func() time.Time { return time.Unix(1, 0) }))
		require.NoError(t, err)
		return got
	}
	phaseInvalid := omev1beta1.OMENativeInstanceStatus{
		Index: -1, Incarnation: 0, Phase: omev1beta1.OMENativeInstancePhase("Future"),
	}
	incarnationInvalid := omev1beta1.OMENativeInstanceStatus{
		Index: 1, Incarnation: -1, Phase: omev1beta1.OMENativeInstanceReady,
	}

	left := project([]omev1beta1.OMENativeInstanceStatus{phaseInvalid, incarnationInvalid})
	right := project([]omev1beta1.OMENativeInstanceStatus{incarnationInvalid, phaseInvalid})

	assert.Equal(t, left, right)
	wantIndex := int32(1)
	assert.Contains(t, left.Content.Issues, reportv1alpha1.InstanceListIssue{
		Code: reportv1alpha1.InstanceIssueIncarnationInvalid, Component: reportv1alpha1.RuntimeComponentEngine,
		InferenceReplica: "chat-engine", Index: &wantIndex,
	})
}

func TestProjectScrubsInvalidOpaqueSourceAndRevisionValues(t *testing.T) {
	t.Parallel()

	isvc := instanceISVC()
	isvc.ResourceVersion = "SECRET_ISVC_RV\u202e\n" + strings.Repeat("x", 10_000)
	ir := instanceReplica(isvc, "chat-engine", omev1beta1.EngineComponent, 1, 1)
	ir.ResourceVersion = "SECRET_IR_RV\x1b[31m" + strings.Repeat("x", 10_000)
	ir.Status.CurrentRevision = "SECRET_CURRENT\u202e" + strings.Repeat("x", 254)
	ir.Status.UpdateRevision = "safe-update"
	ir.Status.InstanceStatuses = []omev1beta1.OMENativeInstanceStatus{{
		Index: 0, Phase: omev1beta1.OMENativeInstanceReady,
	}}

	got, err := instanceprojection.Project(instanceprojection.Input{
		InferenceService: isvc,
		Collection:       instancecollection.Result{Items: []omev1beta1.InferenceReplica{ir}, Pages: 1},
		MaxInstances:     100,
	}, reportv1alpha1.ClockFunc(func() time.Time { return time.Unix(1, 0) }))
	require.NoError(t, err)
	require.Len(t, got.Content.Components, 1)
	assert.Empty(t, got.Content.Components[0].CurrentRevision)
	assert.Equal(t, "safe-update", got.Content.Components[0].UpdateRevision)
	for _, source := range got.Sources {
		assert.Empty(t, source.ResourceVersion)
	}

	encoded, err := json.Marshal(got)
	require.NoError(t, err)
	for _, hostile := range []string{"SECRET_ISVC_RV", "SECRET_IR_RV", "SECRET_CURRENT"} {
		assert.NotContains(t, string(encoded), hostile)
	}
}

func TestProjectSanitizesRejectedIdentityAndRejectsUnsafeAcceptedIdentity(t *testing.T) {
	t.Parallel()

	isvc := instanceISVC()
	got, err := instanceprojection.Project(instanceprojection.Input{
		InferenceService: isvc,
		Collection: instancecollection.Result{Rejected: []instancecollection.Rejection{{
			Name: "SECRET_REJECTION\u202e\n", Reason: instancecollection.RejectionMetadata,
		}}},
		MaxInstances: 100,
	}, reportv1alpha1.ClockFunc(func() time.Time { return time.Unix(1, 0) }))
	require.NoError(t, err)
	require.Len(t, got.Content.Issues, 1)
	assert.Equal(t, "INVALID", got.Content.Issues[0].InferenceReplica)

	ir := instanceReplica(isvc, "chat-engine", omev1beta1.EngineComponent, 1, 1)
	ir.UID = "SECRET_UID\u202e\n"
	_, err = instanceprojection.Project(instanceprojection.Input{
		InferenceService: isvc,
		Collection:       instancecollection.Result{Items: []omev1beta1.InferenceReplica{ir}},
		MaxInstances:     100,
	}, reportv1alpha1.ClockFunc(func() time.Time { return time.Unix(1, 0) }))
	require.ErrorIs(t, err, instanceprojection.ErrCollectionEvidenceInvalid)
}

func TestProjectRejectsUnsafePrimaryIdentity(t *testing.T) {
	t.Parallel()

	isvc := instanceISVC()
	isvc.UID = "SECRET_UID\u202e\n"
	_, err := instanceprojection.Project(instanceprojection.Input{
		InferenceService: isvc, MaxInstances: 100,
	}, reportv1alpha1.ClockFunc(func() time.Time { return time.Unix(1, 0) }))
	require.ErrorIs(t, err, instanceprojection.ErrInferenceServiceIdentityInvalid)

	isvc = instanceISVC()
	isvc.Generation = 0
	_, err = instanceprojection.Project(instanceprojection.Input{
		InferenceService: isvc, MaxInstances: 100,
	}, reportv1alpha1.ClockFunc(func() time.Time { return time.Unix(1, 0) }))
	require.ErrorIs(t, err, instanceprojection.ErrInferenceServiceIdentityInvalid)
}

func TestProjectOutputLimitIsDeterministicAcrossCollectionOrder(t *testing.T) {
	t.Parallel()

	isvc := instanceISVC()
	engine := instanceReplica(isvc, "chat-engine", omev1beta1.EngineComponent, 1, 1)
	engine.Status.InstanceStatuses = []omev1beta1.OMENativeInstanceStatus{{
		Index: 0, Phase: omev1beta1.OMENativeInstanceReady,
	}}
	decoder := instanceReplica(isvc, "chat-decoder", omev1beta1.DecoderComponent, 1, 1)
	decoder.Status.InstanceStatuses = []omev1beta1.OMENativeInstanceStatus{{
		Index: 0, Phase: omev1beta1.OMENativeInstancePending,
	}}
	project := func(items []omev1beta1.InferenceReplica) reportv1alpha1.InstanceListReport {
		got, err := instanceprojection.Project(instanceprojection.Input{
			InferenceService: isvc,
			Collection:       instancecollection.Result{Items: items, Pages: 1},
			MaxInstances:     1,
		}, reportv1alpha1.ClockFunc(func() time.Time { return time.Unix(1, 0) }))
		require.NoError(t, err)
		return got
	}

	left := project([]omev1beta1.InferenceReplica{decoder, engine})
	right := project([]omev1beta1.InferenceReplica{engine, decoder})
	assert.Equal(t, left, right)
	require.Len(t, left.Content.Instances, 1)
	assert.Equal(t, reportv1alpha1.RuntimeComponentEngine, left.Content.Instances[0].Component)
	assert.True(t, left.Content.Summary.Truncated)
	assert.Contains(t, left.Content.Issues, reportv1alpha1.InstanceListIssue{Code: reportv1alpha1.InstanceIssueOutputTruncated})
}

func TestProjectQualifiesUnavailablePartialCollection(t *testing.T) {
	t.Parallel()

	isvc := instanceISVC()
	engine := instanceReplica(isvc, "chat-engine", omev1beta1.EngineComponent, 1, 1)
	engine.Status.Replicas = 1
	engine.Status.InstanceStatuses = []omev1beta1.OMENativeInstanceStatus{{
		Index: 0, Phase: omev1beta1.OMENativeInstanceReady,
	}}

	got, err := instanceprojection.Project(instanceprojection.Input{
		InferenceService:      isvc,
		Collection:            instancecollection.Result{Items: []omev1beta1.InferenceReplica{engine}, Pages: 2},
		CollectionUnavailable: reportv1alpha1.UnavailableForbidden,
		MaxInstances:          100,
	}, reportv1alpha1.ClockFunc(func() time.Time { return time.Unix(1, 0) }))
	require.NoError(t, err)

	assert.Equal(t, reportv1alpha1.InstanceListStatePartial, got.Content.Summary.State)
	require.Len(t, got.Content.Instances, 1, "bounded progress from earlier pages must be preserved")
	assert.Contains(t, got.Content.Issues, reportv1alpha1.InstanceListIssue{
		Code:              reportv1alpha1.InstanceIssueCollectionUnavailable,
		UnavailableReason: reportv1alpha1.UnavailableForbidden,
	})
	assert.Contains(t, got.Warnings, reportv1alpha1.InstanceListWarning{Code: reportv1alpha1.WarningSourceUnavailable})
	assert.Contains(t, got.Sources, reportv1alpha1.SourceReference{
		Kind: "InferenceReplicaList", Namespace: "prod", Name: "chat",
		Evidence: reportv1alpha1.EvidenceUnavailable, UnavailableReason: reportv1alpha1.UnavailableForbidden,
		CollectedAt: time.Unix(1, 0).UTC(),
	})
}

func TestProjectQualifiesUnavailableEmptyCollection(t *testing.T) {
	t.Parallel()

	isvc := instanceISVC()
	got, err := instanceprojection.Project(instanceprojection.Input{
		InferenceService: isvc, CollectionUnavailable: reportv1alpha1.UnavailableUnreadable,
		MaxInstances: 100,
	}, reportv1alpha1.ClockFunc(func() time.Time { return time.Unix(1, 0) }))
	require.NoError(t, err)

	assert.Equal(t, reportv1alpha1.InstanceListStateUnavailable, got.Content.Summary.State)
	assert.Empty(t, got.Content.Components)
	assert.Empty(t, got.Content.Instances)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.InstanceListIssue{
		Code:              reportv1alpha1.InstanceIssueCollectionUnavailable,
		UnavailableReason: reportv1alpha1.UnavailableUnreadable,
	})
}

func TestProjectQualifiesRejectedAndTruncatedCollection(t *testing.T) {
	t.Parallel()

	isvc := instanceISVC()
	got, err := instanceprojection.Project(instanceprojection.Input{
		InferenceService: isvc,
		Collection: instancecollection.Result{
			Rejected: []instancecollection.Rejection{
				{Name: "wrong-owner", Reason: instancecollection.RejectionOwnerReference},
				{Name: "wrong-component", Reason: instancecollection.RejectionComponent},
			},
			Pages: 3, Truncated: true,
		},
		MaxInstances: 100,
	}, reportv1alpha1.ClockFunc(func() time.Time { return time.Unix(1, 0) }))
	require.NoError(t, err)

	assert.Equal(t, reportv1alpha1.InstanceListStatePartial, got.Content.Summary.State)
	assert.True(t, got.Content.Summary.Truncated)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.InstanceListIssue{
		Code: reportv1alpha1.InstanceIssueCollectionTruncated,
	})
	assert.Contains(t, got.Content.Issues, reportv1alpha1.InstanceListIssue{
		Code: reportv1alpha1.InstanceIssueIdentityRejected, InferenceReplica: "wrong-owner",
		RejectionReason: reportv1alpha1.InstanceIdentityRejectionOwnerReference,
	})
	assert.Contains(t, got.Content.Issues, reportv1alpha1.InstanceListIssue{
		Code: reportv1alpha1.InstanceIssueIdentityRejected, InferenceReplica: "wrong-component",
		RejectionReason: reportv1alpha1.InstanceIdentityRejectionComponent,
	})
	assert.Equal(t, []reportv1alpha1.InstanceListWarning{
		{Code: reportv1alpha1.WarningPartialData}, {Code: reportv1alpha1.WarningTruncated},
	}, got.Warnings)
}

func TestProjectBoundsMalformedRowEvidence(t *testing.T) {
	t.Parallel()

	isvc := instanceISVC()
	ir := instanceReplica(isvc, "chat-engine", omev1beta1.EngineComponent, 1, 1)
	ir.Status.Replicas = 20
	for index := int32(0); index < 20; index++ {
		ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses, omev1beta1.OMENativeInstanceStatus{
			Index: index, Phase: omev1beta1.OMENativeInstancePhase("Future"),
		})
	}

	got, err := instanceprojection.Project(instanceprojection.Input{
		InferenceService: isvc,
		Collection:       instancecollection.Result{Items: []omev1beta1.InferenceReplica{ir}, Pages: 1},
		MaxInstances:     3,
	}, reportv1alpha1.ClockFunc(func() time.Time { return time.Unix(1, 0) }))
	require.NoError(t, err)

	assert.True(t, got.Content.Summary.Truncated)
	assert.LessOrEqual(t, len(got.Content.Issues), 4, "row issues plus one truncation marker")
	assert.Contains(t, got.Content.Issues, reportv1alpha1.InstanceListIssue{
		Code: reportv1alpha1.InstanceIssueOutputTruncated,
	})
	assert.Empty(t, got.Content.Instances)
}

func TestProjectRejectsUnknownCollectionEvidence(t *testing.T) {
	t.Parallel()

	isvc := instanceISVC()
	tests := []instanceprojection.Input{
		{
			InferenceService: isvc, MaxInstances: 1,
			CollectionUnavailable: reportv1alpha1.UnavailableReason("FutureReason"),
		},
		{
			InferenceService: isvc, MaxInstances: 1,
			Collection: instancecollection.Result{Rejected: []instancecollection.Rejection{{
				Name: "chat-engine", Reason: instancecollection.RejectionReason("FutureReason"),
			}}},
		},
		{
			InferenceService: isvc, MaxInstances: 1,
			Collection: instancecollection.Result{StatusRowsTruncated: []instancecollection.StatusRowsTruncation{{
				Name: "missing-engine", Component: omev1beta1.EngineComponent,
			}}},
		},
	}
	for _, input := range tests {
		_, err := instanceprojection.Project(input, reportv1alpha1.ClockFunc(func() time.Time { return time.Unix(1, 0) }))
		require.ErrorIs(t, err, instanceprojection.ErrCollectionEvidenceInvalid)
	}
}

func TestProjectAcceptsExactOwnerEvidenceForLongParentName(t *testing.T) {
	t.Parallel()

	isvc := instanceISVC()
	isvc.Name = strings.Repeat("a", 64)
	ir := instanceReplica(isvc, "engine", omev1beta1.EngineComponent, 2, 2)
	delete(ir.Labels, constants.InferenceServiceLabel)
	ir.Status.InstanceStatuses = []omev1beta1.OMENativeInstanceStatus{{
		Index: 0, Incarnation: 1, Phase: omev1beta1.OMENativeInstanceReady,
	}}

	got, err := instanceprojection.Project(instanceprojection.Input{
		InferenceService: isvc,
		Collection:       instancecollection.Result{Items: []omev1beta1.InferenceReplica{ir}},
		MaxInstances:     10,
	}, reportv1alpha1.ClockFunc(func() time.Time { return time.Unix(1, 0) }))

	require.NoError(t, err)
	require.Len(t, got.Content.Components, 1)
	assert.Equal(t, "engine", got.Content.Components[0].InferenceReplica)
	require.Len(t, got.Content.Instances, 1)
}

func instanceISVC() *omev1beta1.InferenceService {
	return &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "chat", Namespace: "prod", UID: types.UID("isvc-uid"), Generation: 7,
		ResourceVersion: "100",
	}}
}

func instanceReplica(
	isvc *omev1beta1.InferenceService,
	name string,
	component omev1beta1.ComponentType,
	generation, observed int64,
) omev1beta1.InferenceReplica {
	controller := true
	return omev1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: isvc.Namespace, UID: types.UID(name + "-uid"),
			Generation: generation, ResourceVersion: "20",
			Annotations: map[string]string{
				constants.InferenceReplicaParentGenerationAnnotationKey: fmt.Sprintf("%d", isvc.Generation),
			},
			Labels: map[string]string{
				constants.InferenceServiceLabel: isvc.Name,
				constants.OMEComponentLabel:     string(component),
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: omev1beta1.SchemeGroupVersion.String(), Kind: "InferenceService",
				Name: isvc.Name, UID: isvc.UID, Controller: &controller,
			}},
		},
		Spec: omev1beta1.InferenceReplicaSpec{
			ParentRef: omev1beta1.ParentReference{Name: isvc.Name}, Component: component,
		},
		Status: omev1beta1.InferenceReplicaStatus{ObservedGeneration: observed, Replicas: 1},
	}
}

func stringPointer(value string) *string { return &value }
