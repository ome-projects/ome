package waitir_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/waitir"
)

func readyReport(count int32) v1alpha1.InstanceListReport {
	return v1alpha1.InstanceListReport{
		Metadata: v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		Sources: []v1alpha1.SourceReference{
			{Kind: "InferenceService", Namespace: "prod", Name: "chat", UID: "parent-uid", Evidence: v1alpha1.EvidenceReported},
			{Kind: "InferenceReplica", Namespace: "prod", Name: "chat-engine", UID: "engine-uid", Evidence: v1alpha1.EvidenceReported},
		},
		Content: v1alpha1.InstanceListContent{
			Summary: v1alpha1.InstanceListSummary{State: v1alpha1.InstanceListStateReported, Components: 1},
			Components: []v1alpha1.InstanceListComponent{{
				Type: v1alpha1.RuntimeComponentEngine, State: v1alpha1.InstanceEvidenceReported,
				InferenceReplica: "chat-engine", Generation: 2, ObservedGeneration: 2,
				Replicas: count, ReadyReplicas: count,
			}},
		},
	}
}

func TestReadyEvaluatorMatchesOnlyExactCount(t *testing.T) {
	for _, tc := range []struct {
		name, wantReason string
		observed         int32
		matched          bool
	}{
		{name: "equal", observed: 2, matched: true, wantReason: "ReplicaReadyMatched"},
		{name: "below", observed: 1, wantReason: "ReplicaReadyNotMatched"},
		{name: "above", observed: 3, wantReason: "ReplicaReadyNotMatched"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			decision, observation := new(waitir.Evaluator).Evaluate(readyReport(tc.observed), v1alpha1.RuntimeComponentEngine, 2)
			require.Equal(t, tc.matched, decision.Matched)
			require.Equal(t, tc.wantReason, string(decision.Reason))
			require.Equal(t, "Valid", observation.Validity)
			require.NotNil(t, observation.ReadyReplicas)
			require.Equal(t, tc.observed, *observation.ReadyReplicas)
		})
	}
}

func TestReadyEvaluatorDoesNotMistakeUnobservedZeroForReadiness(t *testing.T) {
	unobserved := readyReport(0)
	unobserved.Content.Components[0].ObservedGeneration = 0
	decision, observation := new(waitir.Evaluator).Evaluate(unobserved, v1alpha1.RuntimeComponentEngine, 0)
	require.False(t, decision.Matched)
	require.Nil(t, observation.ReadyReplicas)
	require.NotEqual(t, "Valid", observation.Validity)

	current := readyReport(0)
	decision, observation = new(waitir.Evaluator).Evaluate(current, v1alpha1.RuntimeComponentEngine, 0)
	require.True(t, decision.Matched)
	require.Equal(t, "Valid", observation.Validity)
	require.NotNil(t, observation.ReadyReplicas)
	require.Zero(t, *observation.ReadyReplicas)
}

func TestReadyEvaluatorRejectsIncompleteOrStaleEvidence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*v1alpha1.InstanceListReport)
	}{
		{name: "partial", change: func(r *v1alpha1.InstanceListReport) { r.Content.Summary.State = v1alpha1.InstanceListStatePartial }},
		{name: "truncated", change: func(r *v1alpha1.InstanceListReport) { r.Content.Summary.Truncated = true }},
		{name: "rejected identity", change: func(r *v1alpha1.InstanceListReport) {
			r.Content.Issues = []v1alpha1.InstanceListIssue{{Code: v1alpha1.InstanceIssueIdentityRejected}}
		}},
		{name: "stale component", change: func(r *v1alpha1.InstanceListReport) { r.Content.Components[0].State = v1alpha1.InstanceEvidenceStale }},
		{name: "stale generation", change: func(r *v1alpha1.InstanceListReport) { r.Content.Components[0].ObservedGeneration = 1 }},
		{name: "duplicate source", change: func(r *v1alpha1.InstanceListReport) { r.Sources = append(r.Sources, r.Sources[1]) }},
		{name: "unavailable source", change: func(r *v1alpha1.InstanceListReport) { r.Sources[1].Evidence = v1alpha1.EvidenceUnavailable }},
		{name: "unavailable sibling source", change: func(r *v1alpha1.InstanceListReport) {
			r.Content.Components = append(r.Content.Components, v1alpha1.InstanceListComponent{
				Type: v1alpha1.RuntimeComponentDecoder, State: v1alpha1.InstanceEvidenceReported,
				InferenceReplica: "chat-decoder", Generation: 2, ObservedGeneration: 2,
			})
			r.Content.Summary.Components = 2
			r.Sources = append(r.Sources, v1alpha1.SourceReference{
				Kind: "InferenceReplica", Namespace: "prod", Name: "chat-decoder", UID: "decoder-uid",
				Evidence: v1alpha1.EvidenceUnavailable,
			})
		}},
		{name: "sibling UID collision", change: func(r *v1alpha1.InstanceListReport) {
			r.Content.Components = append(r.Content.Components, v1alpha1.InstanceListComponent{
				Type: v1alpha1.RuntimeComponentDecoder, State: v1alpha1.InstanceEvidenceReported,
				InferenceReplica: "chat-decoder", Generation: 2, ObservedGeneration: 2,
			})
			r.Content.Summary.Components = 2
			r.Sources = append(r.Sources, v1alpha1.SourceReference{
				Kind: "InferenceReplica", Namespace: "prod", Name: "chat-decoder", UID: "engine-uid",
				Evidence: v1alpha1.EvidenceReported,
			})
		}},
		{name: "duplicate sibling component", change: func(r *v1alpha1.InstanceListReport) {
			r.Content.Components = append(r.Content.Components,
				v1alpha1.InstanceListComponent{Type: v1alpha1.RuntimeComponentDecoder, State: v1alpha1.InstanceEvidenceReported, InferenceReplica: "chat-decoder-a", Generation: 2, ObservedGeneration: 2},
				v1alpha1.InstanceListComponent{Type: v1alpha1.RuntimeComponentDecoder, State: v1alpha1.InstanceEvidenceReported, InferenceReplica: "chat-decoder-b", Generation: 2, ObservedGeneration: 2})
			r.Content.Summary.Components = 3
			r.Sources = append(r.Sources,
				v1alpha1.SourceReference{Kind: "InferenceReplica", Namespace: "prod", Name: "chat-decoder-a", UID: "decoder-a", Evidence: v1alpha1.EvidenceReported},
				v1alpha1.SourceReference{Kind: "InferenceReplica", Namespace: "prod", Name: "chat-decoder-b", UID: "decoder-b", Evidence: v1alpha1.EvidenceReported})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := readyReport(2)
			tc.change(&r)
			decision, observation := new(waitir.Evaluator).Evaluate(r, v1alpha1.RuntimeComponentEngine, 2)
			require.False(t, decision.Matched)
			require.Nil(t, observation.ReadyReplicas)
			require.NotEqual(t, "Valid", observation.Validity)
		})
	}
}

func TestReadyEvaluatorPinsFirstSelectedIRIdentityAcrossPolls(t *testing.T) {
	evaluator := new(waitir.Evaluator)
	first := readyReport(1)
	decision, _ := evaluator.Evaluate(first, v1alpha1.RuntimeComponentEngine, 2)
	require.False(t, decision.Matched)

	replaced := readyReport(2)
	replaced.Sources[1].UID = "replacement-uid"
	decision, observation := evaluator.Evaluate(replaced, v1alpha1.RuntimeComponentEngine, 2)
	require.False(t, decision.Matched)
	require.Nil(t, observation.ReadyReplicas)
	require.Equal(t, "Invalid", observation.Validity)
}

func TestReadyEvaluatorRequiresSelectedComponentAndNonnegativeTarget(t *testing.T) {
	for _, tc := range []struct {
		component v1alpha1.RuntimeComponentType
		replicas  int32
	}{
		{component: "", replicas: 2},
		{component: "future", replicas: 2},
		{component: v1alpha1.RuntimeComponentEngine, replicas: -1},
		{component: v1alpha1.RuntimeComponentDecoder, replicas: 2},
	} {
		decision, observation := new(waitir.Evaluator).Evaluate(readyReport(2), tc.component, tc.replicas)
		require.False(t, decision.Matched)
		require.Nil(t, observation.ReadyReplicas)
	}
}
