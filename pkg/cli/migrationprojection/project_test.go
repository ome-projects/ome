package migrationprojection

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
	"sigs.k8s.io/ome/pkg/cli/migrationcollection"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
)

var projectionNow = time.Date(2026, time.September, 14, 19, 0, 0, 0, time.UTC)

func TestProjectUsesOnlyCurrentIRMigrationRecordsAndPreservesOnlyBoundedMessageText(t *testing.T) {
	t.Parallel()

	parent := projectionISVC()
	engine := projectionIR(parent, "chat-engine", omev1beta1.EngineComponent)
	engine.Status.Migrations = []omev1beta1.MigrationStatus{{
		RequestUUID: "11111111-1111-1111-1111-111111111111", Trigger: omev1beta1.MigrationTriggerManual,
		SourceInstance: 2, FromNode: "node-a", HintTargetNodes: []string{"node-c", "node-b"},
		Phase: omev1beta1.MigrationPhaseAccepted, Reason: "token=do-not-copy", Message: "waiting for replacement capacity",
		StartedAt: metaTime(projectionNow.Add(-10 * time.Minute)), Deadline: metaTime(projectionNow.Add(20 * time.Minute)),
	}}
	router := projectionIR(parent, "chat-router", omev1beta1.RouterComponent)
	succeeded := true
	router.Status.Migrations = []omev1beta1.MigrationStatus{{
		RequestUUID: "22222222-2222-2222-2222-222222222222", Trigger: omev1beta1.MigrationTriggerAuto,
		SourceInstance: 4, FromNode: "node-z", Phase: omev1beta1.MigrationPhaseRelocated,
		Attempt: 2, Reason: "AutoRecover", Message: "relocation confirmed by ready instance", StartedAt: metaTime(projectionNow.Add(-time.Hour)),
		Deadline: metaTime(projectionNow.Add(-time.Hour)), CompletedAt: metaTimePointer(projectionNow.Add(-50 * time.Minute)),
		Succeeded: &succeeded,
	}}
	snapshot := migrationcollection.Result{
		InferenceService:  parent,
		InferenceReplicas: []omev1beta1.InferenceReplica{router, engine},
	}

	got, err := Project(snapshot, "", Limits{MaxRecords: 20, MaxScannedRecords: 80, MaxNodeHints: 4, MaxScannedNodeHints: 32}, fixedClock{projectionNow})

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.MigrationSummary{
		State: reportv1alpha1.MigrationReportStateReported, Records: 2, Active: 1, Terminal: 1,
	}, got.Content.Summary)
	assert.Equal(t, reportv1alpha1.MigrationCapacityEvidence{
		Evidence: reportv1alpha1.EvidenceComputed, ActiveAllocated: 0,
		ConcurrencyLimit: reportv1alpha1.EvidenceUnavailable, RateLimit: reportv1alpha1.EvidenceUnavailable,
	}, got.Content.Capacity)
	require.Len(t, got.Content.Migrations, 2)
	manual := got.Content.Migrations[0]
	assert.Equal(t, reportv1alpha1.MigrationClassificationActive, manual.Classification)
	assert.Equal(t, reportv1alpha1.MigrationReasonOperatorSupplied, manual.ReasonEvidence)
	assert.Equal(t, reportv1alpha1.MigrationMessagePresent, manual.MessageEvidence)
	assert.Equal(t, reportv1alpha1.EvidenceUnavailable, manual.RequestedAtEvidence)
	assert.Nil(t, manual.RequestedAt)
	require.NotNil(t, manual.StartedAt)
	assert.Nil(t, manual.AllocatedAt)
	assert.Equal(t, []string{"node-b", "node-c"}, manual.TargetNodeHints)
	auto := got.Content.Migrations[1]
	assert.Equal(t, reportv1alpha1.MigrationClassificationTerminal, auto.Classification)
	assert.Equal(t, reportv1alpha1.MigrationOutcomeRelocationConfirmed, auto.Outcome)
	assert.Equal(t, reportv1alpha1.MigrationReasonAutoRecover, auto.ReasonEvidence)

	encoded, err := json.Marshal(got)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), "token=do-not-copy")
	var wire struct {
		Content struct {
			Migrations []struct {
				Message string `json:"message"`
			} `json:"migrations"`
		} `json:"content"`
	}
	require.NoError(t, json.Unmarshal(encoded, &wire))
	require.Len(t, wire.Content.Migrations, 2)
	assert.Equal(t, "waiting for replacement capacity", wire.Content.Migrations[0].Message)
	assert.Equal(t, "relocation confirmed by ready instance", wire.Content.Migrations[1].Message)
	assert.NotContains(t, string(encoded), "resourceVersion")
	assert.NotContains(t, string(encoded), "ownerReferences")
	assert.NotContains(t, string(encoded), "annotations")
}

func TestProjectSanitizesAndCapsMigrationMessage(t *testing.T) {
	t.Parallel()

	parent := projectionISVC()
	engine := projectionIR(parent, "chat-engine", omev1beta1.EngineComponent)
	message := "waiting\n\x1b[31m\u202esecret " + strings.Repeat("界", 300)
	record := validManualRecord("bounded-message", omev1beta1.MigrationPhaseAccepted)
	record.Message = message
	engine.Status.Migrations = []omev1beta1.MigrationStatus{record}

	got, err := Project(
		migrationcollection.Result{InferenceService: parent, InferenceReplicas: []omev1beta1.InferenceReplica{engine}},
		"", Limits{MaxRecords: 20, MaxScannedRecords: 80, MaxNodeHints: 4, MaxScannedNodeHints: 32}, fixedClock{projectionNow},
	)

	require.NoError(t, err)
	encoded, err := json.Marshal(got)
	require.NoError(t, err)
	var wire struct {
		Content struct {
			Migrations []struct {
				Message string `json:"message"`
			} `json:"migrations"`
		} `json:"content"`
	}
	require.NoError(t, json.Unmarshal(encoded, &wire))
	require.Len(t, wire.Content.Migrations, 1)
	projected := wire.Content.Migrations[0].Message
	assert.NotContains(t, projected, "\n")
	assert.NotContains(t, projected, "\x1b")
	assert.NotContains(t, projected, "\u202e")
	assert.Contains(t, projected, `\n`)
	assert.Contains(t, projected, `\u001b`)
	assert.Contains(t, projected, `\u202e`)
	assert.LessOrEqual(t, len([]rune(projected)), 256)
	assert.True(t, strings.HasSuffix(projected, "..."), projected)
}

func TestProjectClassifiesOnlyCurrentExecutionPhases(t *testing.T) {
	t.Parallel()

	parent := projectionISVC()
	phases := []struct {
		phase omev1beta1.MigrationPhase
		want  reportv1alpha1.MigrationClassification
	}{
		{omev1beta1.MigrationPhaseAccepted, reportv1alpha1.MigrationClassificationActive},
		{omev1beta1.MigrationPhaseSurgePending, reportv1alpha1.MigrationClassificationActive},
		{omev1beta1.MigrationPhaseSurgeReady, reportv1alpha1.MigrationClassificationActive},
		{omev1beta1.MigrationPhaseDraining, reportv1alpha1.MigrationClassificationActive},
		{omev1beta1.MigrationPhaseCompleted, reportv1alpha1.MigrationClassificationTerminal},
		{omev1beta1.MigrationPhaseFailed, reportv1alpha1.MigrationClassificationTerminal},
	}
	ir := projectionIR(parent, "chat-engine", omev1beta1.EngineComponent)
	for index, test := range phases {
		record := validManualRecord(string(rune('a'+index))+"-record", test.phase)
		ir.Status.Migrations = append(ir.Status.Migrations, record)
	}
	for index, phase := range []omev1beta1.MigrationPhase{
		omev1beta1.MigrationPhasePending, omev1beta1.MigrationPhaseInProgress, "FuturePhase",
	} {
		record := validManualRecord(string(rune('x'+index))+"-legacy", phase)
		ir.Status.Migrations = append(ir.Status.Migrations, record)
	}

	got, err := Project(migrationcollection.Result{InferenceService: parent, InferenceReplicas: []omev1beta1.InferenceReplica{ir}}, "", Limits{MaxRecords: 20, MaxScannedRecords: 80, MaxNodeHints: 4, MaxScannedNodeHints: 32}, fixedClock{projectionNow})

	require.NoError(t, err)
	byID := recordsByID(got.Content.Migrations)
	for index, test := range phases {
		assert.Equal(t, test.want, byID[string(rune('a'+index))+"-record"].Classification)
	}
	for index := range 3 {
		record := byID[string(rune('x'+index))+"-legacy"]
		assert.Equal(t, reportv1alpha1.MigrationPhaseUnknown, record.Phase)
		assert.Equal(t, reportv1alpha1.MigrationClassificationInvalid, record.Classification)
		assert.Contains(t, record.Issues, reportv1alpha1.MigrationIssuePhaseInvalid)
	}
}

func TestProjectValidatesReplicaIdentityBeforeUsingStatus(t *testing.T) {
	t.Parallel()

	parent := projectionISVC()
	tests := []struct {
		name   string
		mutate func(*omev1beta1.InferenceReplica)
		code   reportv1alpha1.MigrationIssueCode
	}{
		{name: "namespace", mutate: func(ir *omev1beta1.InferenceReplica) { ir.Namespace = "other" }, code: reportv1alpha1.MigrationIssueSourceIdentityInvalid},
		{name: "name", mutate: func(ir *omev1beta1.InferenceReplica) { ir.Name = "bad\nname" }, code: reportv1alpha1.MigrationIssueSourceIdentityInvalid},
		{name: "uid", mutate: func(ir *omev1beta1.InferenceReplica) { ir.UID = "bad\nuid" }, code: reportv1alpha1.MigrationIssueSourceIdentityInvalid},
		{name: "label", mutate: func(ir *omev1beta1.InferenceReplica) { ir.Labels[constants.InferenceServicePodLabelKey] = "other" }, code: reportv1alpha1.MigrationIssueSourceLabelMismatch},
		{name: "parentRef", mutate: func(ir *omev1beta1.InferenceReplica) { ir.Spec.ParentRef.Name = "other" }, code: reportv1alpha1.MigrationIssueSourceParentMismatch},
		{name: "owner uid", mutate: func(ir *omev1beta1.InferenceReplica) { ir.OwnerReferences[0].UID = "other" }, code: reportv1alpha1.MigrationIssueSourceOwnerMismatch},
		{name: "owner gvk", mutate: func(ir *omev1beta1.InferenceReplica) { ir.OwnerReferences[0].APIVersion = "ome.io/v9" }, code: reportv1alpha1.MigrationIssueSourceOwnerMismatch},
		{name: "owner kind", mutate: func(ir *omev1beta1.InferenceReplica) { ir.OwnerReferences[0].Kind = "Secret" }, code: reportv1alpha1.MigrationIssueSourceOwnerMismatch},
		{name: "owner controller", mutate: func(ir *omev1beta1.InferenceReplica) { value := false; ir.OwnerReferences[0].Controller = &value }, code: reportv1alpha1.MigrationIssueSourceOwnerMismatch},
		{name: "component", mutate: func(ir *omev1beta1.InferenceReplica) { ir.Spec.Component = "predictor" }, code: reportv1alpha1.MigrationIssueSourceComponentInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ir := projectionIR(parent, "chat-engine", omev1beta1.EngineComponent)
			ir.Status.Migrations = []omev1beta1.MigrationStatus{validManualRecord("safe-record", omev1beta1.MigrationPhaseAccepted)}
			test.mutate(&ir)

			got, err := Project(migrationcollection.Result{InferenceService: parent, InferenceReplicas: []omev1beta1.InferenceReplica{ir}}, "", Limits{MaxRecords: 20, MaxScannedRecords: 80, MaxNodeHints: 4, MaxScannedNodeHints: 32}, fixedClock{projectionNow})

			require.NoError(t, err)
			assert.Empty(t, got.Content.Migrations)
			assert.Equal(t, reportv1alpha1.MigrationReportStatePartial, got.Content.Summary.State)
			assertIssueCode(t, got.Content.Issues, test.code)
			require.Len(t, got.Sources, 2)
			assert.Equal(t, reportv1alpha1.StatusFreshnessInvalid, got.Sources[1].Freshness)
			assert.Zero(t, got.Sources[1].ObservedGeneration, "unbound source status must not be projected")
			encoded, marshalErr := json.Marshal(got)
			require.NoError(t, marshalErr)
			assert.NotContains(t, string(encoded), "bad\\nname")
			assert.NotContains(t, string(encoded), "bad\\nuid")
		})
	}
}

func TestProjectUsesExactOwnerEvidenceWhenParentNameCannotBeALabelValue(t *testing.T) {
	t.Parallel()

	parent := projectionISVC()
	parent.Name = strings.Repeat("a", 64)
	ir := projectionIR(parent, "engine", omev1beta1.EngineComponent)
	ir.Labels = nil
	ir.Status.Migrations = []omev1beta1.MigrationStatus{
		validManualRecord("safe-record", omev1beta1.MigrationPhaseAccepted),
	}

	got, err := Project(
		migrationcollection.Result{InferenceService: parent, InferenceReplicas: []omev1beta1.InferenceReplica{ir}},
		"", Limits{MaxRecords: 20, MaxScannedRecords: 80, MaxNodeHints: 4, MaxScannedNodeHints: 32}, fixedClock{projectionNow},
	)

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.MigrationReportStateReported, got.Content.Summary.State)
	require.Len(t, got.Content.Migrations, 1)
	assert.Equal(t, "safe-record", got.Content.Migrations[0].RequestID)
	assert.Empty(t, got.Content.Issues)
}

func TestProjectLongNameStillRejectsAnInexactOwner(t *testing.T) {
	t.Parallel()

	parent := projectionISVC()
	parent.Name = strings.Repeat("a", 64)
	ir := projectionIR(parent, "engine", omev1beta1.EngineComponent)
	ir.Labels = nil
	ir.OwnerReferences[0].UID = "unrelated"
	ir.Status.Migrations = []omev1beta1.MigrationStatus{
		validManualRecord("must-not-project", omev1beta1.MigrationPhaseAccepted),
	}

	got, err := Project(
		migrationcollection.Result{InferenceService: parent, InferenceReplicas: []omev1beta1.InferenceReplica{ir}},
		"", Limits{MaxRecords: 20, MaxScannedRecords: 80, MaxNodeHints: 4, MaxScannedNodeHints: 32}, fixedClock{projectionNow},
	)

	require.NoError(t, err)
	assert.Empty(t, got.Content.Migrations)
	assertIssueCode(t, got.Content.Issues, reportv1alpha1.MigrationIssueSourceOwnerMismatch)
}

func TestProjectReportsStalenessAndDuplicateSources(t *testing.T) {
	t.Parallel()

	parent := projectionISVC()
	first := projectionIR(parent, "chat-engine-a", omev1beta1.EngineComponent)
	first.Status.ObservedGeneration = first.Generation - 1
	first.Status.Migrations = []omev1beta1.MigrationStatus{validManualRecord("first-record", omev1beta1.MigrationPhaseAccepted)}
	second := projectionIR(parent, "chat-engine-b", omev1beta1.EngineComponent)
	second.Status.Migrations = []omev1beta1.MigrationStatus{validManualRecord("second-record", omev1beta1.MigrationPhaseAccepted)}

	got, err := Project(migrationcollection.Result{InferenceService: parent, InferenceReplicas: []omev1beta1.InferenceReplica{second, first}}, "", Limits{MaxRecords: 20, MaxScannedRecords: 80, MaxNodeHints: 4, MaxScannedNodeHints: 32}, fixedClock{projectionNow})

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.MigrationReportStatePartial, got.Content.Summary.State)
	assertIssueCode(t, got.Content.Issues, reportv1alpha1.MigrationIssueSourceStale)
	assertIssueCode(t, got.Content.Issues, reportv1alpha1.MigrationIssueSourceDuplicate)
	for _, source := range got.Sources[1:] {
		assert.Equal(t, reportv1alpha1.StatusFreshnessInvalid, source.Freshness)
	}
	for _, record := range got.Content.Migrations {
		assert.Equal(t, reportv1alpha1.MigrationClassificationInvalid, record.Classification)
		assert.Contains(t, record.Issues, reportv1alpha1.MigrationIssueSourceDuplicate)
	}
}

func TestProjectRejectsMalformedRecordCombinations(t *testing.T) {
	t.Parallel()

	parent := projectionISVC()
	tests := []struct {
		name   string
		mutate func(*omev1beta1.MigrationStatus)
		code   reportv1alpha1.MigrationIssueCode
	}{
		{name: "request", mutate: func(record *omev1beta1.MigrationStatus) { record.RequestUUID = "secret\nvalue" }, code: reportv1alpha1.MigrationIssueRequestInvalid},
		{name: "request oversized", mutate: func(record *omev1beta1.MigrationStatus) { record.RequestUUID = strings.Repeat("x", 64) }, code: reportv1alpha1.MigrationIssueRequestInvalid},
		{name: "trigger", mutate: func(record *omev1beta1.MigrationStatus) { record.Trigger = "Future" }, code: reportv1alpha1.MigrationIssueTriggerInvalid},
		{name: "auto active", mutate: func(record *omev1beta1.MigrationStatus) { record.Trigger = omev1beta1.MigrationTriggerAuto }, code: reportv1alpha1.MigrationIssueTriggerPhaseConflict},
		{name: "manual relocated", mutate: func(record *omev1beta1.MigrationStatus) {
			record.Phase = omev1beta1.MigrationPhaseRelocated
			record.CompletedAt = metaTimePointer(projectionNow)
		}, code: reportv1alpha1.MigrationIssueTriggerPhaseConflict},
		{name: "source negative", mutate: func(record *omev1beta1.MigrationStatus) { record.SourceInstance = -1 }, code: reportv1alpha1.MigrationIssueSourceIndexInvalid},
		{name: "surge equals source", mutate: func(record *omev1beta1.MigrationStatus) {
			value := record.SourceInstance
			record.SurgeInstance = &value
			record.AllocatedAt = metaTimePointer(projectionNow.Add(-time.Minute))
		}, code: reportv1alpha1.MigrationIssueSurgeIndexInvalid},
		{name: "accepted allocated", mutate: func(record *omev1beta1.MigrationStatus) {
			value := int32(4)
			record.SurgeInstance = &value
			record.AllocatedAt = metaTimePointer(projectionNow.Add(-time.Minute))
		}, code: reportv1alpha1.MigrationIssueAllocationInvalid},
		{name: "surge missing allocation", mutate: func(record *omev1beta1.MigrationStatus) { record.Phase = omev1beta1.MigrationPhaseSurgePending }, code: reportv1alpha1.MigrationIssueAllocationInvalid},
		{name: "terminal missing completion", mutate: func(record *omev1beta1.MigrationStatus) { record.Phase = omev1beta1.MigrationPhaseFailed }, code: reportv1alpha1.MigrationIssueTerminalShapeInvalid},
		{name: "active has completion", mutate: func(record *omev1beta1.MigrationStatus) { record.CompletedAt = metaTimePointer(projectionNow) }, code: reportv1alpha1.MigrationIssueTerminalShapeInvalid},
		{name: "auto attempt", mutate: func(record *omev1beta1.MigrationStatus) {
			record.Trigger = omev1beta1.MigrationTriggerAuto
			record.Phase = omev1beta1.MigrationPhaseRelocated
			record.CompletedAt = metaTimePointer(projectionNow)
		}, code: reportv1alpha1.MigrationIssueAttemptInvalid},
		{name: "timestamp", mutate: func(record *omev1beta1.MigrationStatus) { record.StartedAt = metav1.Time{} }, code: reportv1alpha1.MigrationIssueTimestampInvalid},
		{name: "succeeded manual", mutate: func(record *omev1beta1.MigrationStatus) { value := true; record.Succeeded = &value }, code: reportv1alpha1.MigrationIssueSucceededInvalid},
		{name: "node", mutate: func(record *omev1beta1.MigrationStatus) { record.FromNode = "node\nsecret" }, code: reportv1alpha1.MigrationIssueNodeHintInvalid},
		{name: "node oversized", mutate: func(record *omev1beta1.MigrationStatus) { record.FromNode = strings.Repeat("x", 254) }, code: reportv1alpha1.MigrationIssueNodeHintInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ir := projectionIR(parent, "chat-engine", omev1beta1.EngineComponent)
			record := validManualRecord("safe-record", omev1beta1.MigrationPhaseAccepted)
			test.mutate(&record)
			ir.Status.Migrations = []omev1beta1.MigrationStatus{record}

			got, err := Project(migrationcollection.Result{InferenceService: parent, InferenceReplicas: []omev1beta1.InferenceReplica{ir}}, "", Limits{MaxRecords: 20, MaxScannedRecords: 80, MaxNodeHints: 4, MaxScannedNodeHints: 32}, fixedClock{projectionNow})

			require.NoError(t, err)
			require.Len(t, got.Content.Migrations, 1)
			projected := got.Content.Migrations[0]
			assert.Contains(t, projected.Issues, test.code)
			if test.code != reportv1alpha1.MigrationIssueNodeHintInvalid {
				assert.Equal(t, reportv1alpha1.MigrationClassificationInvalid, projected.Classification)
			}
			encoded, marshalErr := json.Marshal(got)
			require.NoError(t, marshalErr)
			assert.NotContains(t, string(encoded), "secret")
		})
	}
}

func TestProjectIsDeterministicAcrossSourceAndRecordOrder(t *testing.T) {
	t.Parallel()

	parent := projectionISVC()
	base := []omev1beta1.InferenceReplica{
		projectionIR(parent, "chat-engine", omev1beta1.EngineComponent),
		projectionIR(parent, "chat-router", omev1beta1.RouterComponent),
	}
	base[0].Status.Migrations = []omev1beta1.MigrationStatus{
		validManualRecord("duplicate-id", omev1beta1.MigrationPhaseAccepted),
		validManualRecord("a-record", omev1beta1.MigrationPhaseAccepted),
	}
	base[1].Status.Migrations = []omev1beta1.MigrationStatus{
		validManualRecord("duplicate-id", omev1beta1.MigrationPhaseFailed),
		validManualRecord("z-record", omev1beta1.MigrationPhaseAccepted),
	}
	base[1].Status.Migrations[0].CompletedAt = metaTimePointer(projectionNow)
	limits := Limits{MaxRecords: 3, MaxScannedRecords: 20, MaxNodeHints: 2, MaxScannedNodeHints: 16}
	var want string
	for seed := int64(0); seed < 100; seed++ {
		items := make([]omev1beta1.InferenceReplica, len(base))
		for index := range base {
			items[index] = *base[index].DeepCopy()
		}
		rand.New(rand.NewSource(seed)).Shuffle(len(items), func(i, j int) { items[i], items[j] = items[j], items[i] })
		for index := range items {
			rand.New(rand.NewSource(seed+int64(index)+100)).Shuffle(len(items[index].Status.Migrations), func(i, j int) {
				items[index].Status.Migrations[i], items[index].Status.Migrations[j] = items[index].Status.Migrations[j], items[index].Status.Migrations[i]
			})
		}
		got, err := Project(migrationcollection.Result{InferenceService: parent, InferenceReplicas: items}, "", limits, fixedClock{projectionNow})
		require.NoError(t, err)
		encoded, marshalErr := json.Marshal(got.Canonical())
		require.NoError(t, marshalErr)
		if seed == 0 {
			want = string(encoded)
		} else {
			assert.Equal(t, want, string(encoded), "seed %d", seed)
		}
	}
	assert.Contains(t, want, string(reportv1alpha1.MigrationIssueRequestDuplicate))
	assert.Contains(t, want, string(reportv1alpha1.MigrationIssueRecordsTruncated))
}

func TestProjectAppliesComponentAndNodeHintBoundsDeterministically(t *testing.T) {
	t.Parallel()

	parent := projectionISVC()
	engine := projectionIR(parent, "chat-engine", omev1beta1.EngineComponent)
	record := validManualRecord("engine-record", omev1beta1.MigrationPhaseAccepted)
	record.HintTargetNodes = []string{"node-z", "node-a", "bad\nnode", "node-b", "node-a"}
	engine.Status.Migrations = []omev1beta1.MigrationStatus{record}
	router := projectionIR(parent, "chat-router", omev1beta1.RouterComponent)
	router.Status.Migrations = []omev1beta1.MigrationStatus{validManualRecord("router-record", omev1beta1.MigrationPhaseAccepted)}

	got, err := Project(migrationcollection.Result{InferenceService: parent, InferenceReplicas: []omev1beta1.InferenceReplica{router, engine}}, string(omev1beta1.EngineComponent), Limits{MaxRecords: 20, MaxScannedRecords: 80, MaxNodeHints: 2, MaxScannedNodeHints: 16}, fixedClock{projectionNow})

	require.NoError(t, err)
	require.Len(t, got.Content.Migrations, 1)
	assert.Equal(t, "engine-record", got.Content.Migrations[0].RequestID)
	assert.Equal(t, []string{"node-a", "node-b"}, got.Content.Migrations[0].TargetNodeHints)
	assert.Contains(t, got.Content.Migrations[0].Issues, reportv1alpha1.MigrationIssueNodeHintInvalid)
	assert.Contains(t, got.Content.Migrations[0].Issues, reportv1alpha1.MigrationIssueNodeHintsTruncated)
}

func TestProjectComponentFilterScopesAllReportEvidence(t *testing.T) {
	t.Parallel()

	parent := projectionISVC()
	engine := projectionIR(parent, "chat-engine", omev1beta1.EngineComponent)
	engine.Status.Migrations = []omev1beta1.MigrationStatus{validManualRecord("engine-record", omev1beta1.MigrationPhaseAccepted)}
	router := projectionIR(parent, "chat-router", omev1beta1.RouterComponent)
	router.Status.ObservedGeneration = router.Generation - 1
	router.Status.Migrations = []omev1beta1.MigrationStatus{validManualRecord("router-record", omev1beta1.MigrationPhaseAccepted)}
	decoder := projectionIR(parent, "chat-decoder", omev1beta1.DecoderComponent)
	decoder.OwnerReferences[0].UID = "unrelated"
	decoder.Status.Migrations = []omev1beta1.MigrationStatus{validManualRecord("decoder-record", omev1beta1.MigrationPhaseAccepted)}

	got, err := Project(
		migrationcollection.Result{InferenceService: parent, InferenceReplicas: []omev1beta1.InferenceReplica{router, decoder, engine}},
		string(omev1beta1.EngineComponent), Limits{MaxRecords: 20, MaxScannedRecords: 80, MaxNodeHints: 2, MaxScannedNodeHints: 16}, fixedClock{projectionNow},
	)

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.MigrationReportStateReported, got.Content.Summary.State)
	assert.Equal(t, reportv1alpha1.EvidenceComputed, got.Content.Capacity.Evidence)
	assert.Empty(t, got.Content.Issues)
	require.Len(t, got.Sources, 2)
	assert.Equal(t, reportv1alpha1.MigrationSourceInferenceService, got.Sources[0].Kind)
	assert.Equal(t, "chat-engine", got.Sources[1].Name)
	require.Len(t, got.Content.Migrations, 1)
	assert.Equal(t, "engine-record", got.Content.Migrations[0].RequestID)
	encoded, marshalErr := json.Marshal(got)
	require.NoError(t, marshalErr)
	assert.NotContains(t, string(encoded), "chat-router")
	assert.NotContains(t, string(encoded), "chat-decoder")
}

func TestProjectBoundsRecordAndNodeHintScanning(t *testing.T) {
	t.Parallel()

	parent := projectionISVC()
	engine := projectionIR(parent, "chat-engine", omev1beta1.EngineComponent)
	for index := range 4 {
		engine.Status.Migrations = append(engine.Status.Migrations,
			validManualRecord(string(rune('a'+index))+"-engine", omev1beta1.MigrationPhaseAccepted))
	}
	limits := Limits{MaxRecords: 3, MaxScannedRecords: 3, MaxNodeHints: 2, MaxScannedNodeHints: 4}

	first, err := Project(
		migrationcollection.Result{InferenceService: parent, InferenceReplicas: []omev1beta1.InferenceReplica{engine}},
		"", limits, fixedClock{projectionNow},
	)
	require.NoError(t, err)
	assert.Empty(t, first.Content.Migrations, "an oversized untrusted status slice must not be partially scanned")
	assertIssueCode(t, first.Content.Issues, reportv1alpha1.MigrationIssueRecordsTruncated)
	assert.Contains(t, first.Warnings, reportv1alpha1.MigrationWarning{Code: reportv1alpha1.MigrationWarningTruncated})

	for left, right := 0, len(engine.Status.Migrations)-1; left < right; left, right = left+1, right-1 {
		engine.Status.Migrations[left], engine.Status.Migrations[right] = engine.Status.Migrations[right], engine.Status.Migrations[left]
	}
	second, err := Project(
		migrationcollection.Result{InferenceService: parent, InferenceReplicas: []omev1beta1.InferenceReplica{engine}},
		"", limits, fixedClock{projectionNow},
	)
	require.NoError(t, err)
	assert.Equal(t, first, second)

	hints := projectionIR(parent, "chat-router", omev1beta1.RouterComponent)
	record := validManualRecord("hint-record", omev1beta1.MigrationPhaseAccepted)
	record.HintTargetNodes = []string{"node-z", "node-y", "node-x", "node-w", "hostile\nnode"}
	hints.Status.Migrations = []omev1beta1.MigrationStatus{record}
	hintReport, err := Project(
		migrationcollection.Result{InferenceService: parent, InferenceReplicas: []omev1beta1.InferenceReplica{hints}},
		"", limits, fixedClock{projectionNow},
	)
	require.NoError(t, err)
	require.Len(t, hintReport.Content.Migrations, 1)
	assert.Empty(t, hintReport.Content.Migrations[0].TargetNodeHints, "oversized hint input must not be partially scanned")
	assert.Contains(t, hintReport.Content.Migrations[0].Issues, reportv1alpha1.MigrationIssueNodeHintsTruncated)
	assert.NotContains(t, hintReport.Content.Migrations[0].Issues, reportv1alpha1.MigrationIssueNodeHintInvalid)
}

func TestProjectDistinguishesCurrentStaleUnobservedAndInvalidGenerations(t *testing.T) {
	t.Parallel()

	parent := projectionISVC()
	tests := []struct {
		name       string
		observed   int64
		freshness  reportv1alpha1.StatusFreshness
		issue      reportv1alpha1.MigrationIssueCode
		wantRecord bool
	}{
		{name: "current", observed: 3, freshness: reportv1alpha1.StatusFreshnessCurrent, wantRecord: true},
		{name: "stale", observed: 2, freshness: reportv1alpha1.StatusFreshnessStale, issue: reportv1alpha1.MigrationIssueSourceStale, wantRecord: true},
		{name: "unobserved", observed: 0, freshness: reportv1alpha1.StatusFreshnessUnobserved, issue: reportv1alpha1.MigrationIssueSourceUnobserved},
		{name: "negative", observed: -1, freshness: reportv1alpha1.StatusFreshnessInvalid, issue: reportv1alpha1.MigrationIssueSourceGenerationInvalid},
		{name: "future", observed: 4, freshness: reportv1alpha1.StatusFreshnessInvalid, issue: reportv1alpha1.MigrationIssueSourceGenerationInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ir := projectionIR(parent, "chat-engine", omev1beta1.EngineComponent)
			ir.Status.ObservedGeneration = test.observed
			ir.Status.Migrations = []omev1beta1.MigrationStatus{validManualRecord("record", omev1beta1.MigrationPhaseAccepted)}

			got, err := Project(
				migrationcollection.Result{InferenceService: parent, InferenceReplicas: []omev1beta1.InferenceReplica{ir}},
				"", Limits{MaxRecords: 20, MaxScannedRecords: 80, MaxNodeHints: 2, MaxScannedNodeHints: 16}, fixedClock{projectionNow},
			)

			require.NoError(t, err)
			require.Len(t, got.Sources, 2)
			assert.Equal(t, test.freshness, got.Sources[1].Freshness)
			if test.wantRecord {
				require.Len(t, got.Content.Migrations, 1)
				assert.Equal(t, test.freshness, got.Content.Migrations[0].Freshness)
			} else {
				assert.Empty(t, got.Content.Migrations)
			}
			if test.issue == "" {
				assert.Empty(t, got.Content.Issues)
			} else {
				assertIssueCode(t, got.Content.Issues, test.issue)
			}
		})
	}
}

func TestProjectTreatsCollectionTruncationExplicitly(t *testing.T) {
	t.Parallel()

	parent := projectionISVC()
	got, err := Project(migrationcollection.Result{
		InferenceService: parent,
		Completeness:     migrationcollection.Completeness{ObservedPages: 2, ObservedItems: 4, Truncated: true},
	}, "", Limits{MaxRecords: 20, MaxScannedRecords: 80, MaxNodeHints: 2, MaxScannedNodeHints: 16}, fixedClock{projectionNow})

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.MigrationReportStatePartial, got.Content.Summary.State)
	assertIssueCode(t, got.Content.Issues, reportv1alpha1.MigrationIssueSourcesTruncated)
	assert.Equal(t, []reportv1alpha1.MigrationWarning{
		{Code: reportv1alpha1.MigrationWarningPartialData},
		{Code: reportv1alpha1.MigrationWarningTruncated},
	}, got.Warnings)
	assert.Equal(t, reportv1alpha1.EvidenceUnavailable, got.Content.Capacity.Evidence)
}

func TestProjectReadsClockOnceForOneCoherentSnapshot(t *testing.T) {
	t.Parallel()

	parent := projectionISVC()
	ir := projectionIR(parent, "chat-engine", omev1beta1.EngineComponent)
	clock := &advancingClock{next: projectionNow}

	got, err := Project(
		migrationcollection.Result{InferenceService: parent, InferenceReplicas: []omev1beta1.InferenceReplica{ir}},
		"", Limits{MaxRecords: 20, MaxScannedRecords: 80, MaxNodeHints: 2, MaxScannedNodeHints: 16}, clock,
	)

	require.NoError(t, err)
	assert.Equal(t, 1, clock.calls)
	for _, source := range got.Sources {
		assert.Equal(t, got.CollectedAt, source.CollectedAt)
	}
}

func TestProjectRejectsMissingParentInvalidFilterAndLimits(t *testing.T) {
	t.Parallel()

	_, err := Project(migrationcollection.Result{}, "", Limits{MaxRecords: 1, MaxScannedRecords: 4, MaxNodeHints: 1, MaxScannedNodeHints: 4}, fixedClock{projectionNow})
	require.ErrorIs(t, err, ErrInferenceServiceRequired)
	_, err = Project(migrationcollection.Result{InferenceService: projectionISVC()}, "predictor", Limits{MaxRecords: 1, MaxScannedRecords: 4, MaxNodeHints: 1, MaxScannedNodeHints: 4}, fixedClock{projectionNow})
	require.ErrorIs(t, err, ErrInvalidComponent)
	_, err = Project(migrationcollection.Result{InferenceService: projectionISVC()}, "", Limits{}, fixedClock{projectionNow})
	require.Error(t, err)
}

func TestProjectRejectsUnsafeParentIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*omev1beta1.InferenceService)
	}{
		{name: "namespace", mutate: func(parent *omev1beta1.InferenceService) { parent.Namespace = "Bad_Namespace" }},
		{name: "name", mutate: func(parent *omev1beta1.InferenceService) { parent.Name = "bad\nname" }},
		{name: "uid", mutate: func(parent *omev1beta1.InferenceService) { parent.UID = "bad\nuid" }},
		{name: "generation", mutate: func(parent *omev1beta1.InferenceService) { parent.Generation = 0 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			parent := projectionISVC()
			test.mutate(parent)

			got, err := Project(migrationcollection.Result{InferenceService: parent}, "", Limits{MaxRecords: 1, MaxScannedRecords: 4, MaxNodeHints: 1, MaxScannedNodeHints: 4}, fixedClock{projectionNow})

			require.ErrorIs(t, err, ErrInferenceServiceIdentityInvalid)
			assert.Equal(t, reportv1alpha1.MigrationStatusReport{}, got)
		})
	}
}

func projectionISVC() *omev1beta1.InferenceService {
	return &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "chat", Namespace: "prod", UID: types.UID("uid-chat"), Generation: 8,
	}}
}

func projectionIR(parent *omev1beta1.InferenceService, name string, component omev1beta1.ComponentType) omev1beta1.InferenceReplica {
	controller := true
	return omev1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: parent.Namespace, UID: types.UID("uid-" + name), Generation: 3,
			Labels: map[string]string{constants.InferenceServicePodLabelKey: parent.Name},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: omev1beta1.SchemeGroupVersion.String(), Kind: "InferenceService",
				Name: parent.Name, UID: parent.UID, Controller: &controller,
			}},
		},
		Spec:   omev1beta1.InferenceReplicaSpec{ParentRef: omev1beta1.ParentReference{Name: parent.Name}, Component: component},
		Status: omev1beta1.InferenceReplicaStatus{ObservedGeneration: 3},
	}
}

func validManualRecord(id string, phase omev1beta1.MigrationPhase) omev1beta1.MigrationStatus {
	record := omev1beta1.MigrationStatus{
		RequestUUID: id, Trigger: omev1beta1.MigrationTriggerManual, SourceInstance: 1,
		Phase: phase, StartedAt: metaTime(projectionNow.Add(-10 * time.Minute)), Deadline: metaTime(projectionNow.Add(20 * time.Minute)),
	}
	if phase == omev1beta1.MigrationPhaseSurgePending || phase == omev1beta1.MigrationPhaseSurgeReady || phase == omev1beta1.MigrationPhaseDraining || phase == omev1beta1.MigrationPhaseCompleted {
		surge := int32(2)
		record.SurgeInstance = &surge
		record.AllocatedAt = metaTimePointer(projectionNow.Add(-5 * time.Minute))
	}
	if phase == omev1beta1.MigrationPhaseCompleted || phase == omev1beta1.MigrationPhaseFailed {
		record.CompletedAt = metaTimePointer(projectionNow)
	}
	return record
}

func recordsByID(records []reportv1alpha1.MigrationRecord) map[string]reportv1alpha1.MigrationRecord {
	result := make(map[string]reportv1alpha1.MigrationRecord, len(records))
	for _, record := range records {
		result[record.RequestID] = record
	}
	return result
}

func assertIssueCode(t *testing.T, issues []reportv1alpha1.MigrationIssue, code reportv1alpha1.MigrationIssueCode) {
	t.Helper()
	for _, issue := range issues {
		if issue.Code == code {
			return
		}
	}
	t.Fatalf("issue %q not found in %+v", code, issues)
}

func metaTime(value time.Time) metav1.Time { return metav1.NewTime(value) }

func metaTimePointer(value time.Time) *metav1.Time {
	result := metav1.NewTime(value)
	return &result
}

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

type advancingClock struct {
	next  time.Time
	calls int
}

func (c *advancingClock) Now() time.Time {
	result := c.next
	c.next = c.next.Add(time.Second)
	c.calls++
	return result
}
