package migrationhistoryprojection

import (
	"encoding/json"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/migrationhistorycollection"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
)

var (
	projectionNow        = time.Date(2026, time.September, 14, 20, 0, 0, 0, time.UTC)
	testProjectionLimits = Limits{
		MaxRecords: 20, MaxScannedRecordsPerSource: 80,
		MaxNodeHints: 4, MaxScannedNodeHints: 16,
		MaxEvents: 8, MaxScannedEvents: 32, MaxAuditBytes: 1 << 20,
	}
)

func TestProjectJoinsThreeSourcesWithoutPromotingHistoryToWorkState(t *testing.T) {
	t.Parallel()

	parent := projectionISVC()
	parent.Status.MigrationHistory = []omev1beta1.MigrationHistoryEntry{{
		ID: "req-complete", Component: omev1beta1.EngineComponent, Instance: 1,
		ReplacementInstance: int32Ptr(3), Mode: omev1beta1.MigrationModeSurge,
		Phase:       omev1beta1.MigrationPhaseCompleted,
		RequestedAt: metav1.NewTime(projectionNow.Add(-10 * time.Minute)),
		StartedAt:   timePtr(projectionNow.Add(-9 * time.Minute)),
		CompletedAt: timePtr(projectionNow.Add(-2 * time.Minute)),
		Reason:      "SECRET_PARENT_REASON", RequestedBy: "SECRET_REQUESTER",
		Events:        []omev1beta1.MigrationEvent{{At: metav1.NewTime(projectionNow.Add(-8 * time.Minute)), Message: "SECRET_EVENT"}},
		OutcomeReason: "ReplacementReady",
	}}
	ir := projectionIR(parent, "chat-engine", omev1beta1.EngineComponent)
	ir.Status.Migrations = []omev1beta1.MigrationStatus{
		{
			RequestUUID: "req-active", Trigger: omev1beta1.MigrationTriggerManual,
			SourceInstance: 2, SurgeInstance: int32Ptr(4), Phase: omev1beta1.MigrationPhaseSurgePending,
			FromNode: "node-a", HintTargetNodes: []string{"node-c", "node-b"},
			StartedAt:   metav1.NewTime(projectionNow.Add(-time.Minute)),
			AllocatedAt: timePtr(projectionNow.Add(-30 * time.Second)),
			Deadline:    metav1.NewTime(projectionNow.Add(9 * time.Minute)),
		},
		{
			RequestUUID: "req-complete", Trigger: omev1beta1.MigrationTriggerManual,
			SourceInstance: 1, SurgeInstance: int32Ptr(3), Phase: omev1beta1.MigrationPhaseCompleted,
			StartedAt:   metav1.NewTime(projectionNow.Add(-9 * time.Minute)),
			AllocatedAt: timePtr(projectionNow.Add(-8 * time.Minute)),
			Deadline:    metav1.NewTime(projectionNow.Add(time.Minute)),
			CompletedAt: timePtr(projectionNow.Add(-2 * time.Minute)),
		},
	}
	audit := `{"entries":[` +
		`{"requestUUID":"req-complete","component":"engine","sourceInstance":1,"surgeInstance":3,"phase":"Completed","reason":"SECRET_AUDIT_REASON","fromNode":"node-a","hintTargetNodes":["node-b"],"startedAt":"2026-09-14T19:51:00Z","completedAt":"2026-09-14T19:58:00Z","outcome":"ReplacementReady"},` +
		`{"requestUUID":"req-audit-only","component":"router","sourceInstance":0,"phase":"Failed","reason":"SECRET_ONLY_REASON","startedAt":"2026-09-14T19:40:00Z","completedAt":"2026-09-14T19:41:00Z","outcome":"NodeUnavailable"}` +
		`]}`

	got, err := Project(migrationhistorycollection.Result{
		InferenceService:  parent,
		InferenceReplicas: []omev1beta1.InferenceReplica{ir},
		Replicas: migrationhistorycollection.ReplicaObservation{
			Availability: migrationhistorycollection.AvailabilityAvailable, ObservedPages: 1, ObservedItems: 1,
		},
		Audit: migrationhistorycollection.AuditObservation{
			Namespace: "prod", Name: "chat-ome-migration-audit",
			Availability: migrationhistorycollection.AvailabilityAvailable, HistoryJSON: audit,
		},
	}, "", testProjectionLimits, fixedClock{projectionNow})

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.MigrationHistoryReportReported, got.Content.Summary.State)
	assert.Equal(t, 3, got.Content.Summary.Requests)
	assert.Equal(t, 5, got.Content.Summary.Records)
	assert.Equal(t, 2, got.Content.Summary.Authoritative)
	assert.Equal(t, 1, got.Content.Summary.ParentSummary)
	assert.Equal(t, 2, got.Content.Summary.Audit)
	assert.Equal(t, 1, got.Content.Summary.ActiveAuthoritative)
	assert.Equal(t, 1, got.Content.Summary.TerminalAuthoritative)
	assert.False(t, got.Content.Summary.Truncated)
	require.Len(t, got.Sources, 3)

	var active, parentRecord, auditRecord *reportv1alpha1.MigrationHistoryRecord
	for i := range got.Content.Records {
		record := &got.Content.Records[i]
		switch {
		case record.RequestID == "req-active":
			active = record
		case record.RequestID == "req-complete" && record.Evidence == reportv1alpha1.MigrationHistoryEvidenceParent:
			parentRecord = record
		case record.RequestID == "req-complete" && record.Evidence == reportv1alpha1.MigrationHistoryEvidenceAudit:
			auditRecord = record
		}
	}
	require.NotNil(t, active)
	assert.Equal(t, reportv1alpha1.MigrationHistoryStateActive, active.State)
	assert.Equal(t, reportv1alpha1.MigrationHistoryEvidenceAuthoritative, active.Evidence)
	assert.Equal(t, []string{"node-b", "node-c"}, active.TargetNodeHints)
	require.NotNil(t, parentRecord)
	assert.Equal(t, 1, parentRecord.EventCount)
	assert.Equal(t, reportv1alpha1.MigrationHistoryEvidenceParent, parentRecord.Evidence)
	require.NotNil(t, auditRecord)
	assert.Equal(t, reportv1alpha1.MigrationHistoryFreshnessUnverifiable, auditRecord.Freshness)
	assert.Equal(t, reportv1alpha1.MigrationHistoryEvidenceAudit, auditRecord.Evidence)

	encoded, err := json.Marshal(got)
	require.NoError(t, err)
	for _, secret := range []string{
		"SECRET_PARENT_REASON", "SECRET_REQUESTER", "SECRET_EVENT", "SECRET_AUDIT_REASON", "SECRET_ONLY_REASON",
		"uid-chat", "uid-chat-engine", "history.json",
	} {
		assert.NotContains(t, string(encoded), secret)
	}
}

func TestProjectMarksOptionalAuditAvailabilityAndMalformedPayloads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		availability migrationhistorycollection.Availability
		raw          string
		wantReason   reportv1alpha1.MigrationHistoryUnavailableReason
		wantIssue    reportv1alpha1.MigrationHistoryIssueCode
		wantState    reportv1alpha1.MigrationHistoryReportState
	}{
		{name: "absent", availability: migrationhistorycollection.AvailabilityAbsent, wantReason: reportv1alpha1.MigrationHistoryUnavailableNotFound, wantState: reportv1alpha1.MigrationHistoryReportEmpty},
		{name: "forbidden", availability: migrationhistorycollection.AvailabilityForbidden, wantReason: reportv1alpha1.MigrationHistoryUnavailableForbidden, wantIssue: reportv1alpha1.MigrationHistoryIssueAuditUnavailable, wantState: reportv1alpha1.MigrationHistoryReportPartial},
		{name: "unreadable", availability: migrationhistorycollection.AvailabilityUnreadable, wantReason: reportv1alpha1.MigrationHistoryUnavailableUnreadable, wantIssue: reportv1alpha1.MigrationHistoryIssueAuditUnavailable, wantState: reportv1alpha1.MigrationHistoryReportPartial},
		{name: "invalid owner", availability: migrationhistorycollection.AvailabilityInvalid, wantReason: reportv1alpha1.MigrationHistoryUnavailableInvalidIdentity, wantIssue: reportv1alpha1.MigrationHistoryIssueAuditIdentityInvalid, wantState: reportv1alpha1.MigrationHistoryReportPartial},
		{name: "malformed JSON", availability: migrationhistorycollection.AvailabilityAvailable, raw: `{"entries":[`, wantReason: reportv1alpha1.MigrationHistoryUnavailableMalformed, wantIssue: reportv1alpha1.MigrationHistoryIssueAuditMalformed, wantState: reportv1alpha1.MigrationHistoryReportPartial},
		{name: "oversized JSON", availability: migrationhistorycollection.AvailabilityAvailable, raw: strings.Repeat("x", 65), wantReason: reportv1alpha1.MigrationHistoryUnavailablePayloadTooLarge, wantIssue: reportv1alpha1.MigrationHistoryIssueAuditPayloadTooLarge, wantState: reportv1alpha1.MigrationHistoryReportPartial},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			limits := testProjectionLimits
			limits.MaxAuditBytes = 64
			got, err := Project(migrationhistorycollection.Result{
				InferenceService: projectionISVC(),
				Replicas:         migrationhistorycollection.ReplicaObservation{Availability: migrationhistorycollection.AvailabilityAvailable},
				Audit: migrationhistorycollection.AuditObservation{
					Namespace: "prod", Name: "chat-ome-migration-audit", Availability: test.availability, HistoryJSON: test.raw,
				},
			}, "", limits, fixedClock{projectionNow})

			require.NoError(t, err)
			assert.Equal(t, test.wantState, got.Content.Summary.State)
			auditSource := sourceByEvidence(t, got, reportv1alpha1.MigrationHistoryEvidenceAudit)
			assert.Equal(t, reportv1alpha1.MigrationHistoryAvailabilityUnavailable, auditSource.Availability)
			assert.Equal(t, test.wantReason, auditSource.UnavailableReason)
			if test.wantIssue == "" {
				assert.Empty(t, got.Content.Issues)
			} else {
				assert.Contains(t, got.Content.Issues, reportv1alpha1.MigrationHistoryIssue{Code: test.wantIssue})
			}
			encoded, encodeErr := json.Marshal(got)
			require.NoError(t, encodeErr)
			if test.raw != "" {
				assert.NotContains(t, string(encoded), test.raw)
			}
		})
	}
}

func TestProjectPreservesUnavailableAndTruncatedAuthoritativeEvidence(t *testing.T) {
	t.Parallel()

	for _, availability := range []migrationhistorycollection.Availability{
		migrationhistorycollection.AvailabilityForbidden,
		migrationhistorycollection.AvailabilityUnreadable,
	} {
		got, err := Project(migrationhistorycollection.Result{
			InferenceService: projectionISVC(),
			Replicas:         migrationhistorycollection.ReplicaObservation{Availability: availability},
			Audit:            migrationhistorycollection.AuditObservation{Namespace: "prod", Name: "chat-ome-migration-audit", Availability: migrationhistorycollection.AvailabilityAbsent},
		}, "", testProjectionLimits, fixedClock{projectionNow})
		require.NoError(t, err)
		assert.Equal(t, reportv1alpha1.MigrationHistoryReportPartial, got.Content.Summary.State)
		assert.Contains(t, got.Content.Issues, reportv1alpha1.MigrationHistoryIssue{Code: reportv1alpha1.MigrationHistoryIssueAuthoritativeUnavailable})
	}

	parent := projectionISVC()
	ir := projectionIR(parent, "chat-engine", omev1beta1.EngineComponent)
	got, err := Project(migrationhistorycollection.Result{
		InferenceService: parent, InferenceReplicas: []omev1beta1.InferenceReplica{ir},
		Replicas: migrationhistorycollection.ReplicaObservation{
			Availability: migrationhistorycollection.AvailabilityAvailable, ObservedPages: 2, ObservedItems: 9, Truncated: true,
		},
		Audit: migrationhistorycollection.AuditObservation{Namespace: "prod", Name: "chat-ome-migration-audit", Availability: migrationhistorycollection.AvailabilityAbsent},
	}, "", testProjectionLimits, fixedClock{projectionNow})
	require.NoError(t, err)
	assert.True(t, got.Content.Summary.Truncated)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.MigrationHistoryIssue{Code: reportv1alpha1.MigrationHistoryIssueAuthoritativeTruncated})
	auth := sourceByEvidence(t, got, reportv1alpha1.MigrationHistoryEvidenceAuthoritative)
	assert.True(t, auth.Truncated)
	assert.True(t, auth.Bounded)
	assert.Equal(t, 2, auth.ObservedPages)
	assert.Equal(t, 9, auth.ObservedItems)
}

func TestProjectMarksInternallyTruncatedAuthoritativeRecords(t *testing.T) {
	t.Parallel()

	parent := projectionISVC()
	ir := projectionIR(parent, "chat-engine", omev1beta1.EngineComponent)
	for i := 0; i < testProjectionLimits.MaxScannedRecordsPerSource+1; i++ {
		ir.Status.Migrations = append(ir.Status.Migrations, omev1beta1.MigrationStatus{
			RequestUUID: "request-" + strings.Repeat("x", i%4) + string(rune('a'+i%26)),
			Trigger:     omev1beta1.MigrationTriggerManual, SourceInstance: int32(i),
			Phase:     omev1beta1.MigrationPhaseAccepted,
			StartedAt: metav1.NewTime(projectionNow.Add(-time.Minute)),
			Deadline:  metav1.NewTime(projectionNow.Add(time.Minute)),
		})
	}

	got, err := Project(migrationhistorycollection.Result{
		InferenceService: parent, InferenceReplicas: []omev1beta1.InferenceReplica{ir},
		Replicas: migrationhistorycollection.ReplicaObservation{Availability: migrationhistorycollection.AvailabilityAvailable},
		Audit: migrationhistorycollection.AuditObservation{
			Namespace: "prod", Name: "chat-ome-migration-audit", Availability: migrationhistorycollection.AvailabilityAbsent,
		},
	}, "", testProjectionLimits, fixedClock{projectionNow})

	require.NoError(t, err)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.MigrationHistoryIssue{
		Code: reportv1alpha1.MigrationHistoryIssueAuthoritativeTruncated,
	})
	assert.True(t, got.Content.Summary.Truncated)
	assert.True(t, sourceByEvidence(t, got, reportv1alpha1.MigrationHistoryEvidenceAuthoritative).Truncated)
}

func TestProjectTracksEachEvidenceSourcesOwnFreshness(t *testing.T) {
	t.Parallel()

	parent := projectionISVC()
	parent.Status.ObservedGeneration = parent.Generation - 1
	ir := projectionIR(parent, "chat-engine", omev1beta1.EngineComponent)

	got, err := Project(migrationhistorycollection.Result{
		InferenceService: parent, InferenceReplicas: []omev1beta1.InferenceReplica{ir},
		Replicas: migrationhistorycollection.ReplicaObservation{Availability: migrationhistorycollection.AvailabilityAvailable},
		Audit: migrationhistorycollection.AuditObservation{
			Namespace: "prod", Name: "chat-ome-migration-audit", Availability: migrationhistorycollection.AvailabilityAbsent,
		},
	}, "", testProjectionLimits, fixedClock{projectionNow})

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.MigrationHistoryFreshnessStale,
		sourceByEvidence(t, got, reportv1alpha1.MigrationHistoryEvidenceParent).Freshness)
	assert.Equal(t, reportv1alpha1.MigrationHistoryFreshnessCurrent,
		sourceByEvidence(t, got, reportv1alpha1.MigrationHistoryEvidenceAuthoritative).Freshness,
		"parent summary staleness must not be attributed to current IR status")

	ir.Status.ObservedGeneration = ir.Generation - 1
	got, err = Project(migrationhistorycollection.Result{
		InferenceService: parent, InferenceReplicas: []omev1beta1.InferenceReplica{ir},
		Replicas: migrationhistorycollection.ReplicaObservation{Availability: migrationhistorycollection.AvailabilityAvailable},
		Audit: migrationhistorycollection.AuditObservation{
			Namespace: "prod", Name: "chat-ome-migration-audit", Availability: migrationhistorycollection.AvailabilityAbsent,
		},
	}, "", testProjectionLimits, fixedClock{projectionNow})
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.MigrationHistoryFreshnessStale,
		sourceByEvidence(t, got, reportv1alpha1.MigrationHistoryEvidenceAuthoritative).Freshness)
}

func TestProjectDetectsDuplicatesIdentityAndChronologyConflictsDeterministically(t *testing.T) {
	t.Parallel()

	parent := projectionISVC()
	parent.Status.MigrationHistory = []omev1beta1.MigrationHistoryEntry{
		{ID: "same-request", Component: omev1beta1.EngineComponent, Instance: 1, Mode: omev1beta1.MigrationModeSurge, Phase: omev1beta1.MigrationPhaseCompleted, RequestedAt: metav1.NewTime(projectionNow.Add(-time.Minute)), CompletedAt: timePtr(projectionNow.Add(-2 * time.Minute))},
		{ID: "same-request", Component: omev1beta1.RouterComponent, Instance: 2, Mode: omev1beta1.MigrationModeSurge, Phase: omev1beta1.MigrationPhaseCompleted, RequestedAt: metav1.NewTime(projectionNow.Add(-time.Minute)), CompletedAt: timePtr(projectionNow)},
	}
	ir := projectionIR(parent, "chat-engine", omev1beta1.EngineComponent)
	ir.Status.Migrations = []omev1beta1.MigrationStatus{{
		RequestUUID: "same-request", Trigger: omev1beta1.MigrationTriggerManual,
		SourceInstance: 1, Phase: omev1beta1.MigrationPhaseCompleted,
		StartedAt: metav1.NewTime(projectionNow.Add(-3 * time.Minute)), Deadline: metav1.NewTime(projectionNow.Add(time.Minute)),
		CompletedAt: timePtr(projectionNow.Add(-2 * time.Minute)),
	}}
	rawEntries := []string{
		`{"requestUUID":"same-request","component":"engine","sourceInstance":1,"phase":"Completed","startedAt":"2026-09-14T19:57:00Z","completedAt":"2026-09-14T19:58:00Z","outcome":"Ready"}`,
		`{"requestUUID":"same-request","component":"engine","sourceInstance":1,"phase":"Failed","startedAt":"2026-09-14T19:57:00Z","completedAt":"2026-09-14T19:59:00Z","outcome":"Failed"}`,
	}

	var baseline reportv1alpha1.MigrationHistoryReport
	for seed := int64(0); seed < 20; seed++ {
		shuffledParent := parent.DeepCopy()
		rand.New(rand.NewSource(seed)).Shuffle(len(shuffledParent.Status.MigrationHistory), func(i, j int) {
			shuffledParent.Status.MigrationHistory[i], shuffledParent.Status.MigrationHistory[j] = shuffledParent.Status.MigrationHistory[j], shuffledParent.Status.MigrationHistory[i]
		})
		entries := append([]string{}, rawEntries...)
		rand.New(rand.NewSource(seed+100)).Shuffle(len(entries), func(i, j int) { entries[i], entries[j] = entries[j], entries[i] })
		snapshot := migrationhistorycollection.Result{
			InferenceService: shuffledParent, InferenceReplicas: []omev1beta1.InferenceReplica{ir},
			Replicas: migrationhistorycollection.ReplicaObservation{Availability: migrationhistorycollection.AvailabilityAvailable},
			Audit: migrationhistorycollection.AuditObservation{
				Namespace: "prod", Name: "chat-ome-migration-audit", Availability: migrationhistorycollection.AvailabilityAvailable,
				HistoryJSON: `{"entries":[` + strings.Join(entries, ",") + `]}`,
			},
		}
		got, err := Project(snapshot, "", testProjectionLimits, fixedClock{projectionNow})
		require.NoError(t, err)
		if seed == 0 {
			baseline = got
		} else {
			assert.Equal(t, baseline, got)
		}
	}

	assert.Contains(t, baseline.Content.Issues, reportv1alpha1.MigrationHistoryIssue{Code: reportv1alpha1.MigrationHistoryIssueDuplicateWithinSource, RequestID: "same-request", Evidence: reportv1alpha1.MigrationHistoryEvidenceParent})
	assert.Contains(t, baseline.Content.Issues, reportv1alpha1.MigrationHistoryIssue{Code: reportv1alpha1.MigrationHistoryIssueDuplicateWithinSource, RequestID: "same-request", Evidence: reportv1alpha1.MigrationHistoryEvidenceAudit})
	assert.Contains(t, baseline.Content.Issues, reportv1alpha1.MigrationHistoryIssue{Code: reportv1alpha1.MigrationHistoryIssueCrossSourceIdentityConflict, RequestID: "same-request"})
	assert.Contains(t, baseline.Content.Issues, reportv1alpha1.MigrationHistoryIssue{Code: reportv1alpha1.MigrationHistoryIssueChronologyConflict, RequestID: "same-request"})
	assert.Contains(t, baseline.Content.Issues, reportv1alpha1.MigrationHistoryIssue{Code: reportv1alpha1.MigrationHistoryIssueTerminalOutcomeConflict, RequestID: "same-request"})
}

func TestProjectDoesNotMislabelWithinSourceIdentityDifferencesAsCrossSource(t *testing.T) {
	t.Parallel()

	parent := projectionISVC()
	parent.Status.MigrationHistory = []omev1beta1.MigrationHistoryEntry{
		{ID: "duplicate", Component: omev1beta1.EngineComponent, Instance: 1, Mode: omev1beta1.MigrationModeSurge, Phase: omev1beta1.MigrationPhaseCompleted, RequestedAt: metav1.NewTime(projectionNow.Add(-2 * time.Minute)), CompletedAt: timePtr(projectionNow.Add(-time.Minute))},
		{ID: "duplicate", Component: omev1beta1.RouterComponent, Instance: 2, Mode: omev1beta1.MigrationModeSurge, Phase: omev1beta1.MigrationPhaseCompleted, RequestedAt: metav1.NewTime(projectionNow.Add(-2 * time.Minute)), CompletedAt: timePtr(projectionNow.Add(-time.Minute))},
	}

	got, err := Project(migrationhistorycollection.Result{
		InferenceService: parent,
		Replicas:         migrationhistorycollection.ReplicaObservation{Availability: migrationhistorycollection.AvailabilityAvailable},
		Audit: migrationhistorycollection.AuditObservation{
			Namespace: "prod", Name: "chat-ome-migration-audit", Availability: migrationhistorycollection.AvailabilityAbsent,
		},
	}, "", testProjectionLimits, fixedClock{projectionNow})

	require.NoError(t, err)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.MigrationHistoryIssue{
		Code:     reportv1alpha1.MigrationHistoryIssueDuplicateWithinSource,
		Evidence: reportv1alpha1.MigrationHistoryEvidenceParent, RequestID: "duplicate",
	})
	assert.NotContains(t, got.Content.Issues, reportv1alpha1.MigrationHistoryIssue{
		Code: reportv1alpha1.MigrationHistoryIssueCrossSourceIdentityConflict, RequestID: "duplicate",
	})
}

func TestProjectComparesExplicitReplacementIdentityAcrossSources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		parentReplacement *int32
		auditJSON         string
		wantConflict      bool
	}{
		{
			name: "different explicit replacements", parentReplacement: int32Ptr(3), wantConflict: true,
			auditJSON: `{"entries":[{"requestUUID":"same-request","component":"engine","sourceInstance":1,"surgeInstance":4,"phase":"Completed","startedAt":"2026-09-14T19:51:00Z","completedAt":"2026-09-14T19:59:00Z","outcome":"Ready"}]}`,
		},
		{
			name: "matching explicit replacements", parentReplacement: int32Ptr(3),
			auditJSON: `{"entries":[{"requestUUID":"same-request","component":"engine","sourceInstance":1,"surgeInstance":3,"phase":"Completed","startedAt":"2026-09-14T19:51:00Z","completedAt":"2026-09-14T19:59:00Z","outcome":"Ready"}]}`,
		},
		{
			name:      "parent missing and audit present",
			auditJSON: `{"entries":[{"requestUUID":"same-request","component":"engine","sourceInstance":1,"surgeInstance":3,"phase":"Completed","startedAt":"2026-09-14T19:51:00Z","completedAt":"2026-09-14T19:59:00Z","outcome":"Ready"}]}`,
		},
		{
			name: "parent present and audit missing", parentReplacement: int32Ptr(3),
			auditJSON: `{"entries":[{"requestUUID":"same-request","component":"engine","sourceInstance":1,"phase":"Completed","startedAt":"2026-09-14T19:51:00Z","completedAt":"2026-09-14T19:59:00Z","outcome":"Ready"}]}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			parent := projectionISVC()
			parent.Status.MigrationHistory = []omev1beta1.MigrationHistoryEntry{{
				ID: "same-request", Component: omev1beta1.EngineComponent, Instance: 1,
				ReplacementInstance: test.parentReplacement, Mode: omev1beta1.MigrationModeSurge,
				Phase:       omev1beta1.MigrationPhaseCompleted,
				RequestedAt: metav1.NewTime(projectionNow.Add(-10 * time.Minute)),
				StartedAt:   timePtr(projectionNow.Add(-9 * time.Minute)),
				CompletedAt: timePtr(projectionNow.Add(-time.Minute)),
			}}

			got, err := Project(migrationhistorycollection.Result{
				InferenceService: parent,
				Replicas:         migrationhistorycollection.ReplicaObservation{Availability: migrationhistorycollection.AvailabilityAvailable},
				Audit: migrationhistorycollection.AuditObservation{
					Namespace: "prod", Name: "chat-ome-migration-audit",
					Availability: migrationhistorycollection.AvailabilityAvailable, HistoryJSON: test.auditJSON,
				},
			}, "", testProjectionLimits, fixedClock{projectionNow})

			require.NoError(t, err)
			issue := reportv1alpha1.MigrationHistoryIssue{
				Code: reportv1alpha1.MigrationHistoryIssueCrossSourceIdentityConflict, RequestID: "same-request",
			}
			if test.wantConflict {
				assert.Contains(t, got.Content.Issues, issue)
			} else {
				assert.NotContains(t, got.Content.Issues, issue)
			}
		})
	}
}

func TestProjectFiltersComponentsBeforeCountingAndBoundsEverySource(t *testing.T) {
	t.Parallel()

	parent := projectionISVC()
	for i := int32(0); i < 3; i++ {
		parent.Status.MigrationHistory = append(parent.Status.MigrationHistory,
			omev1beta1.MigrationHistoryEntry{ID: "engine-" + string(rune('a'+i)), Component: omev1beta1.EngineComponent, Instance: i, Mode: omev1beta1.MigrationModeSurge, Phase: omev1beta1.MigrationPhasePending, RequestedAt: metav1.NewTime(projectionNow.Add(-time.Duration(i) * time.Minute))},
			omev1beta1.MigrationHistoryEntry{ID: "router-" + string(rune('a'+i)), Component: omev1beta1.RouterComponent, Instance: i, Mode: omev1beta1.MigrationModeSurge, Phase: omev1beta1.MigrationPhasePending, RequestedAt: metav1.NewTime(projectionNow.Add(-time.Duration(i) * time.Minute))},
		)
	}
	limits := testProjectionLimits
	limits.MaxRecords = 2
	limits.MaxScannedRecordsPerSource = 3
	raw := `{"entries":[` + strings.Join([]string{
		`{"requestUUID":"audit-a","component":"engine","sourceInstance":0,"phase":"Started","startedAt":"2026-09-14T19:59:00Z"}`,
		`{"requestUUID":"audit-b","component":"engine","sourceInstance":1,"phase":"Started","startedAt":"2026-09-14T19:58:00Z"}`,
		`{"requestUUID":"audit-c","component":"engine","sourceInstance":2,"phase":"Started","startedAt":"2026-09-14T19:57:00Z"}`,
		`{"requestUUID":"audit-d","component":"engine","sourceInstance":3,"phase":"Started","startedAt":"2026-09-14T19:56:00Z"}`,
	}, ",") + `]}`

	got, err := Project(migrationhistorycollection.Result{
		InferenceService: parent,
		Replicas:         migrationhistorycollection.ReplicaObservation{Availability: migrationhistorycollection.AvailabilityAvailable},
		Audit:            migrationhistorycollection.AuditObservation{Namespace: "prod", Name: "chat-ome-migration-audit", Availability: migrationhistorycollection.AvailabilityAvailable, HistoryJSON: raw},
	}, "engine", limits, fixedClock{projectionNow})

	require.NoError(t, err)
	require.Len(t, got.Content.Records, 2)
	for _, record := range got.Content.Records {
		assert.Equal(t, reportv1alpha1.RuntimeComponentEngine, record.Component)
	}
	assert.True(t, got.Content.Summary.Truncated)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.MigrationHistoryIssue{Code: reportv1alpha1.MigrationHistoryIssueParentTruncated})
	assert.Contains(t, got.Content.Issues, reportv1alpha1.MigrationHistoryIssue{Code: reportv1alpha1.MigrationHistoryIssueAuditTruncated})
	assert.Contains(t, got.Content.Issues, reportv1alpha1.MigrationHistoryIssue{Code: reportv1alpha1.MigrationHistoryIssueOutputTruncated})
}

func TestProjectRejectsInvalidInputs(t *testing.T) {
	t.Parallel()

	_, err := Project(migrationhistorycollection.Result{}, "", testProjectionLimits, fixedClock{projectionNow})
	require.ErrorIs(t, err, ErrInferenceServiceRequired)

	parent := projectionISVC()
	parent.UID = ""
	_, err = Project(migrationhistorycollection.Result{InferenceService: parent}, "", testProjectionLimits, fixedClock{projectionNow})
	require.ErrorIs(t, err, ErrInferenceServiceIdentityInvalid)

	_, err = Project(migrationhistorycollection.Result{InferenceService: projectionISVC()}, "predictor", testProjectionLimits, fixedClock{projectionNow})
	require.ErrorIs(t, err, ErrInvalidComponent)

	for _, mutate := range []func(*Limits){
		func(l *Limits) { l.MaxRecords = 0 },
		func(l *Limits) { l.MaxScannedRecordsPerSource = 0 },
		func(l *Limits) { l.MaxRecords = l.MaxScannedRecordsPerSource + 1 },
		func(l *Limits) { l.MaxNodeHints = 0 },
		func(l *Limits) { l.MaxScannedNodeHints = l.MaxNodeHints - 1 },
		func(l *Limits) { l.MaxEvents = 0 },
		func(l *Limits) { l.MaxScannedEvents = l.MaxEvents - 1 },
		func(l *Limits) { l.MaxAuditBytes = 0 },
	} {
		limits := testProjectionLimits
		mutate(&limits)
		_, err = Project(migrationhistorycollection.Result{InferenceService: projectionISVC()}, "", limits, fixedClock{projectionNow})
		require.ErrorIs(t, err, ErrInvalidLimits)
	}
}

func TestProjectRedactsMalformedRecordsAndMarksStaleParent(t *testing.T) {
	t.Parallel()

	parent := projectionISVC()
	parent.Status.ObservedGeneration = 4
	parent.Status.MigrationHistory = []omev1beta1.MigrationHistoryEntry{{
		ID: "SECRET_INVALID_ID\n", Component: "SECRET_COMPONENT", Instance: -1,
		ReplacementInstance: int32Ptr(-2), Mode: "SECRET_MODE", Phase: "SECRET_PHASE",
		RequestedAt: metav1.Time{}, StartedAt: timePtr(projectionNow.Add(time.Hour)),
		CompletedAt: timePtr(projectionNow.Add(-time.Hour)), OutcomeReason: "safe summary",
	}}
	raw := `{"entries":[{"requestUUID":"SECRET_AUDIT_ID\n","component":"SECRET_AUDIT_COMPONENT","sourceInstance":-1,"surgeInstance":-2,"phase":"SECRET_AUDIT_PHASE","fromNode":"SECRET_NODE/value","hintTargetNodes":["node-a","SECRET_HINT/value"],"startedAt":"bad","completedAt":"also-bad","outcome":"safe audit summary","secret":"SECRET_RAW_FIELD"}]}`

	got, err := Project(migrationhistorycollection.Result{
		InferenceService: parent,
		Replicas:         migrationhistorycollection.ReplicaObservation{Availability: migrationhistorycollection.AvailabilityAvailable},
		Audit:            migrationhistorycollection.AuditObservation{Namespace: "prod", Name: "chat-ome-migration-audit", Availability: migrationhistorycollection.AvailabilityAvailable, HistoryJSON: raw},
	}, "", testProjectionLimits, fixedClock{projectionNow})

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.MigrationHistoryReportPartial, got.Content.Summary.State)
	parentSource := sourceByEvidence(t, got, reportv1alpha1.MigrationHistoryEvidenceParent)
	assert.Equal(t, reportv1alpha1.MigrationHistoryFreshnessStale, parentSource.Freshness)
	encoded, err := json.Marshal(got)
	require.NoError(t, err)
	for _, secret := range []string{
		"SECRET_INVALID_ID", "SECRET_COMPONENT", "SECRET_MODE", "SECRET_PHASE",
		"SECRET_AUDIT_ID", "SECRET_AUDIT_COMPONENT", "SECRET_AUDIT_PHASE", "SECRET_NODE", "SECRET_HINT", "SECRET_RAW_FIELD",
	} {
		assert.NotContains(t, string(encoded), secret)
	}
}

func TestProjectRequiresSourceSpecificStartTimestamps(t *testing.T) {
	t.Parallel()

	parent := projectionISVC()
	parent.Status.MigrationHistory = []omev1beta1.MigrationHistoryEntry{{
		ID: "parent-missing-time", Component: omev1beta1.EngineComponent,
		Mode: omev1beta1.MigrationModeSurge, Phase: omev1beta1.MigrationPhasePending,
	}}
	audit := `{"entries":[{"requestUUID":"audit-missing-time","component":"engine","sourceInstance":0,"phase":"Started"}]}`

	got, err := Project(migrationhistorycollection.Result{
		InferenceService: parent,
		Replicas:         migrationhistorycollection.ReplicaObservation{Availability: migrationhistorycollection.AvailabilityAvailable},
		Audit: migrationhistorycollection.AuditObservation{
			Namespace: "prod", Name: "chat-ome-migration-audit",
			Availability: migrationhistorycollection.AvailabilityAvailable, HistoryJSON: audit,
		},
	}, "", testProjectionLimits, fixedClock{projectionNow})

	require.NoError(t, err)
	require.Len(t, got.Content.Records, 2)
	for _, record := range got.Content.Records {
		assert.Equal(t, reportv1alpha1.MigrationHistoryStateInvalid, record.State)
		assert.Contains(t, record.Issues, reportv1alpha1.MigrationHistoryIssueTimestampInvalid)
	}
}

func TestProjectRejectsStartedBeforeRequestedAndAllowsRejectedWithoutStart(t *testing.T) {
	t.Parallel()

	parent := projectionISVC()
	parent.Status.MigrationHistory = []omev1beta1.MigrationHistoryEntry{
		{
			ID: "started-before-requested", Component: omev1beta1.EngineComponent, Instance: 1,
			ReplacementInstance: int32Ptr(3), Mode: omev1beta1.MigrationModeSurge,
			Phase:       omev1beta1.MigrationPhaseCompleted,
			RequestedAt: metav1.NewTime(projectionNow.Add(-time.Minute)),
			StartedAt:   timePtr(projectionNow.Add(-2 * time.Minute)),
			CompletedAt: timePtr(projectionNow),
		},
		{
			ID: "rejected-without-start", Component: omev1beta1.EngineComponent, Instance: 2,
			Mode: omev1beta1.MigrationModeSurge, Phase: omev1beta1.MigrationPhaseFailed,
			RequestedAt: metav1.NewTime(projectionNow.Add(-3 * time.Minute)),
			CompletedAt: timePtr(projectionNow.Add(-2 * time.Minute)),
		},
	}

	got, err := Project(migrationhistorycollection.Result{
		InferenceService: parent,
		Replicas:         migrationhistorycollection.ReplicaObservation{Availability: migrationhistorycollection.AvailabilityAvailable},
		Audit: migrationhistorycollection.AuditObservation{
			Namespace: "prod", Name: "chat-ome-migration-audit", Availability: migrationhistorycollection.AvailabilityAbsent,
		},
	}, "", testProjectionLimits, fixedClock{projectionNow})

	require.NoError(t, err)
	records := make(map[string]reportv1alpha1.MigrationHistoryRecord, len(got.Content.Records))
	for _, record := range got.Content.Records {
		records[record.RequestID] = record
	}
	malformed := records["started-before-requested"]
	assert.Equal(t, reportv1alpha1.MigrationHistoryStateInvalid, malformed.State)
	assert.Contains(t, malformed.Issues, reportv1alpha1.MigrationHistoryIssueTimestampInvalid)
	rejected := records["rejected-without-start"]
	assert.Nil(t, rejected.StartedAt)
	assert.Equal(t, reportv1alpha1.MigrationHistoryStateTerminal, rejected.State)
	assert.NotContains(t, rejected.Issues, reportv1alpha1.MigrationHistoryIssueTimestampInvalid)
}

func TestProjectRejectsAuditEntriesMissingRequiredSourceInstance(t *testing.T) {
	t.Parallel()

	got, err := Project(migrationhistorycollection.Result{
		InferenceService: projectionISVC(),
		Replicas:         migrationhistorycollection.ReplicaObservation{Availability: migrationhistorycollection.AvailabilityAvailable},
		Audit: migrationhistorycollection.AuditObservation{
			Namespace: "prod", Name: "chat-ome-migration-audit",
			Availability: migrationhistorycollection.AvailabilityAvailable,
			HistoryJSON:  `{"entries":[{"requestUUID":"missing-instance","component":"engine","phase":"Started","startedAt":"2026-09-14T19:55:00Z"}]}`,
		},
	}, "", testProjectionLimits, fixedClock{projectionNow})

	require.NoError(t, err)
	require.Len(t, got.Content.Records, 1)
	assert.EqualValues(t, -1, got.Content.Records[0].SourceInstance)
	assert.Equal(t, reportv1alpha1.MigrationHistoryStateInvalid, got.Content.Records[0].State)
	assert.Contains(t, got.Content.Records[0].Issues, reportv1alpha1.MigrationHistoryIssueInstanceInvalid)
}

func TestProjectExcludesForceDeleteUIDsAndClassifiesAutoRecovery(t *testing.T) {
	t.Parallel()

	raw := `{"entries":[` +
		`{"requestUUID":"SECRET_POD_UID","component":"engine","sourceInstance":0,"phase":"Completed","reason":"ForceDelete","startedAt":"2026-09-14T19:55:00Z","completedAt":"2026-09-14T19:55:00Z","outcome":"force-delete-unreachable"},` +
		`{"requestUUID":"SECRET_FINALIZER_UID","component":"engine","sourceInstance":0,"phase":"Completed","reason":"ForceDelete","startedAt":"2026-09-14T19:55:00Z","completedAt":"2026-09-14T19:55:00Z","outcome":"finalizer-report"},` +
		`{"requestUUID":"manual-reason-collision","component":"engine","sourceInstance":2,"phase":"Completed","reason":"ForceDelete","startedAt":"2026-09-14T19:54:00Z","completedAt":"2026-09-14T19:54:00Z","outcome":"migrated"},` +
		`{"requestUUID":"auto-recover-request","component":"engine","sourceInstance":1,"phase":"Completed","reason":"AutoRecover","startedAt":"2026-09-14T19:56:00Z","completedAt":"2026-09-14T19:56:00Z","outcome":"relocate-recreate"}` +
		`]}`

	got, err := Project(migrationhistorycollection.Result{
		InferenceService: projectionISVC(),
		Replicas:         migrationhistorycollection.ReplicaObservation{Availability: migrationhistorycollection.AvailabilityAvailable},
		Audit: migrationhistorycollection.AuditObservation{
			Namespace: "prod", Name: "chat-ome-migration-audit",
			Availability: migrationhistorycollection.AvailabilityAvailable, HistoryJSON: raw,
		},
	}, "", testProjectionLimits, fixedClock{projectionNow})

	require.NoError(t, err)
	require.Len(t, got.Content.Records, 2)
	record := got.Content.Records[0]
	assert.Equal(t, "auto-recover-request", record.RequestID)
	assert.Equal(t, reportv1alpha1.MigrationTriggerAuto, record.Trigger)
	assert.Equal(t, reportv1alpha1.MigrationOutcomeRelocated, record.Outcome)
	encoded, encodeErr := json.Marshal(got)
	require.NoError(t, encodeErr)
	assert.NotContains(t, string(encoded), "SECRET_POD_UID")
	assert.NotContains(t, string(encoded), "SECRET_FINALIZER_UID")
	assert.NotContains(t, string(encoded), "ForceDelete")
}

func TestProjectTreatsConfirmedAndAuditRelocationAsCompatibleOutcomes(t *testing.T) {
	t.Parallel()

	parent := projectionISVC()
	ir := projectionIR(parent, "chat-engine", omev1beta1.EngineComponent)
	succeeded := true
	ir.Status.Migrations = []omev1beta1.MigrationStatus{{
		RequestUUID: "auto-recover-request", Trigger: omev1beta1.MigrationTriggerAuto,
		SourceInstance: 1, Attempt: 1, Phase: omev1beta1.MigrationPhaseRelocated,
		StartedAt:   metav1.NewTime(projectionNow.Add(-4 * time.Minute)),
		Deadline:    metav1.NewTime(projectionNow.Add(time.Minute)),
		CompletedAt: timePtr(projectionNow.Add(-3 * time.Minute)), Succeeded: &succeeded,
	}}
	audit := `{"entries":[{"requestUUID":"auto-recover-request","component":"engine","sourceInstance":1,"phase":"Completed","reason":"AutoRecover","startedAt":"2026-09-14T19:56:00Z","completedAt":"2026-09-14T19:57:00Z","outcome":"relocate-recreate"}]}`

	got, err := Project(migrationhistorycollection.Result{
		InferenceService: parent, InferenceReplicas: []omev1beta1.InferenceReplica{ir},
		Replicas: migrationhistorycollection.ReplicaObservation{Availability: migrationhistorycollection.AvailabilityAvailable},
		Audit: migrationhistorycollection.AuditObservation{
			Namespace: "prod", Name: "chat-ome-migration-audit",
			Availability: migrationhistorycollection.AvailabilityAvailable, HistoryJSON: audit,
		},
	}, "", testProjectionLimits, fixedClock{projectionNow})

	require.NoError(t, err)
	assert.NotContains(t, got.Content.Issues, reportv1alpha1.MigrationHistoryIssue{
		Code:      reportv1alpha1.MigrationHistoryIssueTerminalOutcomeConflict,
		RequestID: "auto-recover-request",
	})
}

func sourceByEvidence(t *testing.T, report reportv1alpha1.MigrationHistoryReport, evidence reportv1alpha1.MigrationHistoryEvidence) reportv1alpha1.MigrationHistorySource {
	t.Helper()
	for _, source := range report.Sources {
		if source.Evidence == evidence {
			return source
		}
	}
	t.Fatalf("source %q not found", evidence)
	return reportv1alpha1.MigrationHistorySource{}
}

func projectionISVC() *omev1beta1.InferenceService {
	result := &omev1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "prod", UID: types.UID("uid-chat"), Generation: 5},
	}
	result.Status.ObservedGeneration = 5
	return result
}

func projectionIR(parent *omev1beta1.InferenceService, name string, component omev1beta1.ComponentType) omev1beta1.InferenceReplica {
	controller := true
	return omev1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: parent.Namespace, UID: types.UID("uid-" + name), Generation: 2,
			Labels: map[string]string{constants.InferenceServicePodLabelKey: parent.Name},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: omev1beta1.SchemeGroupVersion.String(), Kind: "InferenceService",
				Name: parent.Name, UID: parent.UID, Controller: &controller,
			}},
		},
		Spec:   omev1beta1.InferenceReplicaSpec{ParentRef: omev1beta1.ParentReference{Name: parent.Name}, Component: component},
		Status: omev1beta1.InferenceReplicaStatus{ObservedGeneration: 2},
	}
}

func int32Ptr(value int32) *int32 { return &value }

func timePtr(value time.Time) *metav1.Time {
	result := metav1.NewTime(value)
	return &result
}

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }
