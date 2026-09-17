package v1alpha1

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStatusPlacementRejectsReplicaTotalsOutsideSplit(t *testing.T) {
	count := int64(9)
	got := (StatusContent{Placement: StatusPlacement{
		State: "Reported", Mode: "All", ModeEvidence: "Declared", Phase: "Placed",
		Candidates:    PlacementPreview{State: "Validated", Total: 1, Kept: 1},
		EndpointState: "Present", Evidence: EvidenceReported, Freshness: "Unverifiable",
		AdmittedReplicas: StatusPlacementCount{Value: &count, State: "Reported"},
		ReadyReplicas:    StatusPlacementCount{Value: &count, State: "Reported"},
	}}).Canonical().Placement
	require.Equal(t, PlacementValue("NotApplicable"), got.AdmittedReplicas.State)
	require.Nil(t, got.AdmittedReplicas.Value)
	require.Equal(t, PlacementValue("NotApplicable"), got.ReadyReplicas.State)
	require.Nil(t, got.ReadyReplicas.Value)
}

func TestStatusPlacementSplitTableShowsOnlyReportedAggregate(t *testing.T) {
	admitted, ready := int64(5), int64(3)
	c := StatusContent{Placement: StatusPlacement{
		State: "Reported", Mode: "Split", ModeEvidence: "Declared", Phase: "Placed",
		Candidates:    PlacementPreview{State: "Validated", Total: 2, Kept: 2},
		EndpointState: "Present", Evidence: EvidenceReported, Freshness: "Unverifiable",
		AdmittedReplicas: StatusPlacementCount{Value: &admitted, State: "Reported"},
		ReadyReplicas:    StatusPlacementCount{Value: &ready, State: "Reported"},
	}}
	var out bytes.Buffer
	require.NoError(t, c.Table().Write(&out))
	require.Contains(t, out.String(), "Placement replicas")
	require.Contains(t, out.String(), "admitted=5 ready=3 (reported)")
}

func TestStatusPlacementMalformedEvidenceCannotRemainReported(t *testing.T) {
	count := int64(2)
	base := StatusPlacement{
		State: "Reported", Mode: "Split", ModeEvidence: "Declared", Phase: "Placed",
		Candidates:    PlacementPreview{State: "Validated", Total: 1, Kept: 1},
		EndpointState: "Present", Evidence: EvidenceReported, Freshness: "Unverifiable",
		AdmittedReplicas: StatusPlacementCount{Value: &count, State: "Reported"},
		ReadyReplicas:    StatusPlacementCount{Value: &count, State: "Reported"},
	}
	for _, tc := range []struct {
		name string
		edit func(*StatusPlacement)
	}{
		{"unknown mode", func(v *StatusPlacement) { v.Mode = "Bogus" }},
		{"impossible preview", func(v *StatusPlacement) { v.Candidates.Kept = 2 }},
		{"invalid endpoint", func(v *StatusPlacement) { v.EndpointState = "Bogus" }},
		{"missing reported count", func(v *StatusPlacement) { v.ReadyReplicas.Value = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value := base
			tc.edit(&value)
			got := (StatusContent{Placement: value}).Canonical().Placement
			require.Equal(t, PlacementValue("Partial"), got.State)
		})
	}
}

func TestStatusPlacementCanonicalDropsAggregateWhenCandidateSetIncomplete(t *testing.T) {
	count := int64(4)
	value := StatusPlacement{
		State: "Reported", Mode: "Split", ModeEvidence: "Declared", Phase: "Placed",
		Candidates:    PlacementPreview{State: "Validated", Total: 65, Kept: 64, Truncated: true},
		EndpointState: "Present", Evidence: EvidenceReported, Freshness: "Unverifiable",
		AdmittedReplicas: StatusPlacementCount{Value: &count, State: "Reported"},
		ReadyReplicas:    StatusPlacementCount{Value: &count, State: "Reported"},
	}
	got := (StatusContent{Placement: value}).Canonical().Placement
	require.Equal(t, PlacementValue("Partial"), got.State)
	require.Equal(t, PlacementValue("Unavailable"), got.AdmittedReplicas.State)
	require.Nil(t, got.AdmittedReplicas.Value)
	require.Equal(t, PlacementValue("Unavailable"), got.ReadyReplicas.State)
	require.Nil(t, got.ReadyReplicas.Value)
}

func TestStatusPlacementUnavailableEvidenceCannotRetainReportedFacts(t *testing.T) {
	count := int64(4)
	value := StatusPlacement{
		State: "Reported", Mode: "Split", ModeEvidence: "Declared", Phase: "Placed",
		ReportedCluster: "west", Candidates: PlacementPreview{State: "Validated", Total: 1, Kept: 1},
		EndpointState: "Present", Evidence: EvidenceUnavailable, Freshness: "Unverifiable",
		AdmittedReplicas: StatusPlacementCount{Value: &count, State: "Reported"},
		ReadyReplicas:    StatusPlacementCount{Value: &count, State: "Reported"},
	}
	got := (StatusContent{Placement: value}).Canonical().Placement
	require.Equal(t, PlacementValue("Unavailable"), got.State)
	require.Empty(t, got.ReportedCluster)
	require.Equal(t, PlacementValue("NotRecorded"), got.Phase)
	require.Equal(t, PlacementValue("NotRecorded"), got.EndpointState)
	require.Nil(t, got.AdmittedReplicas.Value)
	require.Nil(t, got.ReadyReplicas.Value)
}

func TestStatusPlacementCannotCallZeroAggregateReported(t *testing.T) {
	zero := int64(0)
	value := StatusPlacement{
		State: "Reported", Mode: "Split", ModeEvidence: "Declared", Phase: "Pending",
		Candidates:    PlacementPreview{State: "Validated", Total: 1, Kept: 1},
		EndpointState: "NotRecorded", Evidence: EvidenceReported, Freshness: "Unverifiable",
		AdmittedReplicas: StatusPlacementCount{Value: &zero, State: "Reported"},
		ReadyReplicas:    StatusPlacementCount{Value: &zero, State: "Reported"},
	}
	got := (StatusContent{Placement: value}).Canonical().Placement
	require.Equal(t, PlacementValue("Unavailable"), got.AdmittedReplicas.State)
	require.Equal(t, PlacementValue("Unavailable"), got.ReadyReplicas.State)
	require.Nil(t, got.AdmittedReplicas.Value)
	require.Nil(t, got.ReadyReplicas.Value)
}

func TestStatusPlacementPlacedSplitUnknownAggregateIsPartial(t *testing.T) {
	count := int64(2)
	value := StatusPlacement{
		State: "Reported", Mode: "Split", ModeEvidence: "Declared", Phase: "Placed",
		Candidates:    PlacementPreview{State: "Validated", Total: 1, Kept: 1},
		EndpointState: "Present", Evidence: EvidenceReported, Freshness: "Unverifiable",
		AdmittedReplicas: StatusPlacementCount{Value: &count, State: "Reported"},
		ReadyReplicas:    StatusPlacementCount{State: "Unknown"},
	}
	got := (StatusContent{Placement: value}).Canonical().Placement
	require.Equal(t, PlacementValue("Partial"), got.State)
	require.Equal(t, PlacementValue("Unknown"), got.ReadyReplicas.State)
	require.Nil(t, got.ReadyReplicas.Value)
}
