package snapshot

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
)

func migrationTestWorkload() *Workload {
	return &Workload{Components: map[v1beta1.ComponentType]*Component{
		v1beta1.EngineComponent: {
			Type: v1beta1.EngineComponent,
			Instances: []*Instance{
				{Index: 0, ObservationValid: true},
				{Index: 2, ObservationValid: true},
			},
		},
	}}
}

func migrationStatus(uuid string, trigger v1beta1.MigrationTrigger, source int32, phase v1beta1.MigrationPhase, started time.Time) v1beta1.MigrationStatus {
	return v1beta1.MigrationStatus{
		RequestUUID:    uuid,
		Trigger:        trigger,
		SourceInstance: source,
		FromNode:       "gpu-a",
		Phase:          phase,
		StartedAt:      metav1.NewTime(started),
	}
}

func migrationStatusIR(migrations ...v1beta1.MigrationStatus) *v1beta1.InferenceReplica {
	return &v1beta1.InferenceReplica{Status: v1beta1.InferenceReplicaStatus{Migrations: migrations}}
}

func withMigrationIR(workload *Workload, component v1beta1.ComponentType, migrations ...v1beta1.MigrationStatus) {
	workload.Components[component].IR = migrationStatusIR(migrations...)
}

func TestParsePendingMigrationAcceptsCanonicalSnakeCase(t *testing.T) {
	now := time.Date(2026, 8, 31, 9, 30, 0, 0, time.UTC)
	raw, err := json.Marshal(audit.MigrationRequest{
		SchemaVersion:   audit.SchemaV1,
		Component:       string(v1beta1.EngineComponent),
		Instance:        2,
		FromNode:        "gpu-a",
		HintTargetNodes: []string{"gpu-b", "gpu-c"},
		Reason:          "fragmentation",
		RequestedAt:     now.Format(time.RFC3339),
		RequestedBy:     "alfred-controller",
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	got, err := parsePendingMigration("request-1", string(raw), migrationTestWorkload())
	if err != nil {
		t.Fatalf("parsePendingMigration: %v", err)
	}
	if got.UUID != "request-1" || got.Component != v1beta1.EngineComponent || got.FromNode != "gpu-a" {
		t.Fatalf("identity mismatch: %+v", got)
	}
	if got.RequestedBy != "alfred-controller" {
		t.Fatalf("RequestedBy = %q, want alfred-controller", got.RequestedBy)
	}
	if got.Instance != 2 {
		t.Fatalf("Instance = %d, want 2", got.Instance)
	}
	if !got.RequestedAt.Equal(now) {
		t.Fatalf("RequestedAt = %v, want %v", got.RequestedAt, now)
	}
}

func TestParsePendingMigrationRejectsCamelCase(t *testing.T) {
	raw := `{"schemaVersion":"v1","component":"engine","instance":2,"fromNode":"gpu-a","requestedAt":"2026-08-31T09:30:00Z","requestedBy":"alfred-controller"}`

	_, err := parsePendingMigration("request-1", raw, migrationTestWorkload())
	if err == nil {
		t.Fatal("parsePendingMigration accepted camel-case request")
	}
	if !strings.Contains(err.Error(), "from_node") {
		t.Fatalf("error = %q, want missing from_node", err)
	}
}

func TestParsePendingMigrationValidatesCurrentInstance(t *testing.T) {
	valid := audit.MigrationRequest{
		SchemaVersion: audit.SchemaV1,
		Component:     string(v1beta1.EngineComponent),
		Instance:      2,
		FromNode:      "gpu-a",
		RequestedAt:   "2026-08-31T09:30:00Z",
	}
	tests := []struct {
		name        string
		uuid        string
		mutate      func(*audit.MigrationRequest)
		wantErrPart string
	}{
		{name: "empty UUID", uuid: "", mutate: func(*audit.MigrationRequest) {}, wantErrPart: "UUID"},
		{name: "missing source", uuid: "request-1", mutate: func(r *audit.MigrationRequest) { r.FromNode = "" }, wantErrPart: "from_node"},
		{name: "negative instance", uuid: "request-1", mutate: func(r *audit.MigrationRequest) { r.Instance = -1 }, wantErrPart: "instance"},
		{name: "unknown component", uuid: "request-1", mutate: func(r *audit.MigrationRequest) { r.Component = "sidecar" }, wantErrPart: "component"},
		{name: "missing current instance", uuid: "request-1", mutate: func(r *audit.MigrationRequest) { r.Instance = 1 }, wantErrPart: "current instance"},
		{name: "invalid requested_at", uuid: "request-1", mutate: func(r *audit.MigrationRequest) { r.RequestedAt = "yesterday" }, wantErrPart: "requested_at"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := valid
			tt.mutate(&req)
			raw, err := json.Marshal(req)
			if err != nil {
				t.Fatalf("marshal request: %v", err)
			}
			_, err = parsePendingMigration(tt.uuid, string(raw), migrationTestWorkload())
			if err == nil || !strings.Contains(err.Error(), tt.wantErrPart) {
				t.Fatalf("error = %v, want substring %q", err, tt.wantErrPart)
			}
		})
	}
}

func TestParsePendingMigrationPreservesMissingRequestedAt(t *testing.T) {
	raw, err := json.Marshal(audit.MigrationRequest{
		SchemaVersion: audit.SchemaV1,
		Component:     string(v1beta1.EngineComponent),
		Instance:      2,
		FromNode:      "gpu-a",
		RequestedBy:   "foreign-controller",
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	got, err := parsePendingMigration("legacy-1", string(raw), migrationTestWorkload())
	if err != nil {
		t.Fatalf("parsePendingMigration: %v", err)
	}
	if !got.RequestedAt.IsZero() {
		t.Fatalf("RequestedAt = %v, want zero", got.RequestedAt)
	}
}

func TestApplyMigrationStatePreservesRequesterFromPendingAnnotation(t *testing.T) {
	raw, err := json.Marshal(audit.MigrationRequest{
		SchemaVersion: audit.SchemaV1,
		Component:     string(v1beta1.EngineComponent),
		Instance:      0,
		FromNode:      "gpu-a",
		RequestedAt:   "2026-08-31T09:30:00Z",
		RequestedBy:   "alfred-controller",
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	workload := migrationTestWorkload()
	started := time.Date(2026, 8, 31, 9, 30, 0, 0, time.UTC)
	withMigrationIR(workload, v1beta1.EngineComponent, migrationStatus(
		"request-1", v1beta1.MigrationTriggerManual, 0, v1beta1.MigrationPhaseSurgePending, started,
	))
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
			migrationAnnotationKey("request-1"): string(raw),
		}},
	}

	applyMigrationState(workload, isvc)

	if len(workload.ActiveMigrations) != 1 {
		t.Fatalf("ActiveMigrations = %+v, want one migration", workload.ActiveMigrations)
	}
	if got := workload.ActiveMigrations[0].RequestedBy; got != "alfred-controller" {
		t.Fatalf("RequestedBy = %q, want alfred-controller", got)
	}
	if got := workload.ActiveMigrations[0]; got.Instance != 0 || got.Mode != "" || !got.RequestedAt.Equal(started) {
		t.Fatalf("IR overlay = %+v, want source instance 0, empty mode, and status start time", got)
	}
}

func TestApplyMigrationStateOverlaysAuthoritativeIRStatus(t *testing.T) {
	now := time.Date(2026, 8, 31, 10, 0, 0, 0, time.UTC)
	annotation, err := json.Marshal(audit.MigrationRequest{
		SchemaVersion: audit.SchemaV1,
		Component:     string(v1beta1.EngineComponent),
		Instance:      0,
		FromNode:      "gpu-a",
		RequestedAt:   now.Add(-time.Minute).Format(time.RFC3339),
		RequestedBy:   "alfred-controller",
	})
	if err != nil {
		t.Fatalf("marshal annotation: %v", err)
	}

	t.Run("annotation only", func(t *testing.T) {
		workload := migrationTestWorkload()
		applyMigrationState(workload, &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
			migrationAnnotationKey("annotation-only"): string(annotation),
		}}})
		if len(workload.ActiveMigrations) != 1 || workload.ActiveMigrations[0].UUID != "annotation-only" || workload.ActiveMigrations[0].Instance != 0 {
			t.Fatalf("annotation migration = %+v", workload.ActiveMigrations)
		}
		if !workload.MigrationStateValid {
			t.Fatalf("annotation-only state invalid: %q", workload.MigrationStateReason)
		}
	})

	t.Run("accepted status overrides annotation", func(t *testing.T) {
		workload := migrationTestWorkload()
		started := now.Add(-2 * time.Minute)
		withMigrationIR(workload, v1beta1.EngineComponent, migrationStatus(
			"accepted", v1beta1.MigrationTriggerManual, 0, v1beta1.MigrationPhaseAccepted, started,
		))
		applyMigrationState(workload, &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
			migrationAnnotationKey("accepted"): string(annotation),
		}}})
		if len(workload.ActiveMigrations) != 1 {
			t.Fatalf("active migrations = %+v", workload.ActiveMigrations)
		}
		got := workload.ActiveMigrations[0]
		if got.Phase != v1beta1.MigrationPhaseAccepted || got.Mode != "" || got.Instance != 0 || !got.RequestedAt.Equal(started) || got.RequestedBy != "alfred-controller" {
			t.Fatalf("accepted status migration = %+v", got)
		}
		if !workload.MigrationStateValid {
			t.Fatalf("accepted status marked invalid: %q", workload.MigrationStateReason)
		}
	})

	t.Run("requester requires matching component and source", func(t *testing.T) {
		workload := migrationTestWorkload()
		withMigrationIR(workload, v1beta1.EngineComponent, migrationStatus(
			"accepted", v1beta1.MigrationTriggerManual, 2, v1beta1.MigrationPhaseAccepted, now,
		))
		applyMigrationState(workload, &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
			migrationAnnotationKey("accepted"): string(annotation),
		}}})
		if got := workload.ActiveMigrations[0].RequestedBy; got != "" {
			t.Fatalf("RequestedBy = %q, want empty for mismatched source", got)
		}
	})

	t.Run("invalid source observation remains busy with active evidence", func(t *testing.T) {
		workload := migrationTestWorkload()
		workload.Components[v1beta1.EngineComponent].Instances[0].ObservationValid = false
		withMigrationIR(workload, v1beta1.EngineComponent, migrationStatus(
			"observed", v1beta1.MigrationTriggerManual, 0, v1beta1.MigrationPhaseAccepted, now,
		))
		applyMigrationState(workload, &v1beta1.InferenceService{})
		if workload.MigrationStateValid || len(workload.ActiveMigrations) != 1 || workload.ActiveMigrations[0].UUID != "observed" {
			t.Fatalf("invalid observed source state = valid:%t active:%+v", workload.MigrationStateValid, workload.ActiveMigrations)
		}
	})

	t.Run("terminal status clears malformed annotation and uses completion", func(t *testing.T) {
		workload := migrationTestWorkload()
		started := now.Add(-3 * time.Minute)
		completed := metav1.NewTime(now.Add(-time.Minute))
		status := migrationStatus("done", v1beta1.MigrationTriggerManual, 0, v1beta1.MigrationPhaseCompleted, started)
		status.CompletedAt = &completed
		withMigrationIR(workload, v1beta1.EngineComponent, status)
		applyMigrationState(workload, &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
			migrationAnnotationKey("done"): "{not-json",
		}}})
		if len(workload.ActiveMigrations) != 0 || len(workload.MalformedRequests) != 0 {
			t.Fatalf("terminal overlay left busy evidence: active=%+v malformed=%+v", workload.ActiveMigrations, workload.MalformedRequests)
		}
		if workload.LastMigration == nil || !workload.LastMigration.Equal(completed.Time) {
			t.Fatalf("LastMigration = %v, want %v", workload.LastMigration, completed.Time)
		}
	})

	t.Run("terminal status falls back to start", func(t *testing.T) {
		workload := migrationTestWorkload()
		started := now.Add(-4 * time.Minute)
		withMigrationIR(workload, v1beta1.EngineComponent, migrationStatus(
			"done", v1beta1.MigrationTriggerManual, 0, v1beta1.MigrationPhaseFailed, started,
		))
		applyMigrationState(workload, &v1beta1.InferenceService{})
		if workload.LastMigration == nil || !workload.LastMigration.Equal(started) {
			t.Fatalf("LastMigration = %v, want %v", workload.LastMigration, started)
		}
	})

	t.Run("sparse source index", func(t *testing.T) {
		workload := migrationTestWorkload()
		withMigrationIR(workload, v1beta1.EngineComponent, migrationStatus(
			"sparse", v1beta1.MigrationTriggerManual, 2, v1beta1.MigrationPhaseDraining, now,
		))
		applyMigrationState(workload, &v1beta1.InferenceService{})
		if len(workload.ActiveMigrations) != 1 || workload.ActiveMigrations[0].Instance != 2 {
			t.Fatalf("sparse migration = %+v", workload.ActiveMigrations)
		}
	})

	t.Run("multiple components have UUID ordering", func(t *testing.T) {
		workload := migrationTestWorkload()
		workload.Components[v1beta1.DecoderComponent] = &Component{Type: v1beta1.DecoderComponent, Instances: []*Instance{{Index: 7, ObservationValid: true}}}
		withMigrationIR(workload, v1beta1.EngineComponent, migrationStatus(
			"zeta", v1beta1.MigrationTriggerManual, 0, v1beta1.MigrationPhaseSurgeReady, now,
		))
		withMigrationIR(workload, v1beta1.DecoderComponent, migrationStatus(
			"alpha", v1beta1.MigrationTriggerManual, 7, v1beta1.MigrationPhaseDraining, now,
		))
		applyMigrationState(workload, &v1beta1.InferenceService{})
		if len(workload.ActiveMigrations) != 2 || workload.ActiveMigrations[0].UUID != "alpha" || workload.ActiveMigrations[1].UUID != "zeta" {
			t.Fatalf("active ordering = %+v", workload.ActiveMigrations)
		}
	})
}

func TestApplyMigrationStateRejectsInvalidIRStatus(t *testing.T) {
	now := time.Date(2026, 8, 31, 10, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		statuses []v1beta1.MigrationStatus
	}{
		{name: "empty UUID", statuses: []v1beta1.MigrationStatus{migrationStatus("", v1beta1.MigrationTriggerManual, 0, v1beta1.MigrationPhaseAccepted, now)}},
		{name: "legacy phase", statuses: []v1beta1.MigrationStatus{migrationStatus("legacy", v1beta1.MigrationTriggerManual, 0, v1beta1.MigrationPhasePending, now)}},
		{name: "unknown phase", statuses: []v1beta1.MigrationStatus{migrationStatus("unknown", v1beta1.MigrationTriggerManual, 0, v1beta1.MigrationPhase("Future"), now)}},
		{name: "manual relocated", statuses: []v1beta1.MigrationStatus{migrationStatus("bad-trigger", v1beta1.MigrationTriggerManual, 0, v1beta1.MigrationPhaseRelocated, now)}},
		{name: "auto nonterminal", statuses: []v1beta1.MigrationStatus{migrationStatus("bad-auto", v1beta1.MigrationTriggerAuto, 0, v1beta1.MigrationPhaseAccepted, now)}},
		{name: "negative source", statuses: []v1beta1.MigrationStatus{migrationStatus("negative", v1beta1.MigrationTriggerManual, -1, v1beta1.MigrationPhaseAccepted, now)}},
		{name: "zero start", statuses: []v1beta1.MigrationStatus{migrationStatus("zero", v1beta1.MigrationTriggerManual, 0, v1beta1.MigrationPhaseAccepted, time.Time{})}},
		{name: "nonterminal completed", statuses: func() []v1beta1.MigrationStatus {
			status := migrationStatus("completed", v1beta1.MigrationTriggerManual, 0, v1beta1.MigrationPhaseAccepted, now)
			at := metav1.NewTime(now)
			status.CompletedAt = &at
			return []v1beta1.MigrationStatus{status}
		}()},
		{name: "missing source", statuses: []v1beta1.MigrationStatus{migrationStatus("missing", v1beta1.MigrationTriggerManual, 9, v1beta1.MigrationPhaseAccepted, now)}},
		{name: "terminal before start", statuses: func() []v1beta1.MigrationStatus {
			status := migrationStatus("before", v1beta1.MigrationTriggerManual, 0, v1beta1.MigrationPhaseCompleted, now)
			at := metav1.NewTime(now.Add(-time.Second))
			status.CompletedAt = &at
			return []v1beta1.MigrationStatus{status}
		}()},
		{name: "terminal zero completion", statuses: func() []v1beta1.MigrationStatus {
			status := migrationStatus("zero-completion", v1beta1.MigrationTriggerManual, 0, v1beta1.MigrationPhaseCompleted, now)
			at := metav1.Time{}
			status.CompletedAt = &at
			return []v1beta1.MigrationStatus{status}
		}()},
		{name: "duplicate UUID in component", statuses: []v1beta1.MigrationStatus{
			migrationStatus("duplicate", v1beta1.MigrationTriggerManual, 0, v1beta1.MigrationPhaseAccepted, now),
			migrationStatus("duplicate", v1beta1.MigrationTriggerManual, 0, v1beta1.MigrationPhaseDraining, now),
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			workload := migrationTestWorkload()
			withMigrationIR(workload, v1beta1.EngineComponent, tt.statuses...)
			applyMigrationState(workload, &v1beta1.InferenceService{})
			if workload.MigrationStateValid || workload.MigrationStateReason == "" || len(workload.MigrationStateReason) > 128 {
				t.Fatalf("invalid status state = valid:%t reason:%q", workload.MigrationStateValid, workload.MigrationStateReason)
			}
		})
	}

	t.Run("empty UUID clears matching malformed annotation before rejection", func(t *testing.T) {
		workload := migrationTestWorkload()
		withMigrationIR(workload, v1beta1.EngineComponent, migrationStatus(
			"", v1beta1.MigrationTriggerManual, 0, v1beta1.MigrationPhaseAccepted, now,
		))
		applyMigrationState(workload, &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
			migrationAnnotationKey(""): "{not-json",
		}}})
		if len(workload.MalformedRequests) != 0 || len(workload.ActiveMigrations) != 0 {
			t.Fatalf("empty UUID IR retained annotation evidence: malformed=%+v active=%+v", workload.MalformedRequests, workload.ActiveMigrations)
		}
		if workload.MigrationStateValid || workload.MigrationStateReason != migrationStateReasonStatusInvalid {
			t.Fatalf("empty UUID status invalidity = valid:%t reason:%q", workload.MigrationStateValid, workload.MigrationStateReason)
		}
	})

	t.Run("duplicate UUID across components", func(t *testing.T) {
		workload := migrationTestWorkload()
		workload.Components[v1beta1.DecoderComponent] = &Component{Type: v1beta1.DecoderComponent, Instances: []*Instance{{Index: 0, ObservationValid: true}}}
		withMigrationIR(workload, v1beta1.EngineComponent, migrationStatus("duplicate", v1beta1.MigrationTriggerManual, 0, v1beta1.MigrationPhaseAccepted, now))
		withMigrationIR(workload, v1beta1.DecoderComponent, migrationStatus("duplicate", v1beta1.MigrationTriggerManual, 0, v1beta1.MigrationPhaseDraining, now))
		applyMigrationState(workload, &v1beta1.InferenceService{})
		if workload.MigrationStateValid || len(workload.ActiveMigrations) != 0 {
			t.Fatalf("cross-component duplicate state = valid:%t active:%+v", workload.MigrationStateValid, workload.ActiveMigrations)
		}
	})
}

func TestApplyMigrationStateIgnoresLegacyMigrationHistory(t *testing.T) {
	workload := migrationTestWorkload()
	started := time.Date(2026, 8, 31, 10, 0, 0, 0, time.UTC)
	isvc := &v1beta1.InferenceService{Status: v1beta1.InferenceServiceStatus{MigrationHistory: []v1beta1.MigrationHistoryEntry{{
		ID:          "legacy",
		Component:   v1beta1.EngineComponent,
		Phase:       v1beta1.MigrationPhaseSurgePending,
		RequestedAt: metav1.NewTime(started),
	}, {
		ID:          "legacy-done",
		Component:   v1beta1.EngineComponent,
		Phase:       v1beta1.MigrationPhaseCompleted,
		RequestedAt: metav1.NewTime(started),
	}}}}
	applyMigrationState(workload, isvc)
	if len(workload.ActiveMigrations) != 0 || workload.LastMigration != nil {
		t.Fatalf("legacy history reconstructed migration state: active=%+v last=%v", workload.ActiveMigrations, workload.LastMigration)
	}
}

func TestMarshalAlfredMigrationRequestRoundTripsThroughAudit(t *testing.T) {
	now := time.Date(2026, 8, 31, 9, 30, 0, 0, time.UTC)
	req := audit.MigrationRequest{
		SchemaVersion:   audit.SchemaV1,
		Component:       string(v1beta1.EngineComponent),
		Instance:        2,
		FromNode:        "gpu-a",
		HintTargetNodes: []string{"gpu-b", "gpu-c"},
		Reason:          "fragmentation",
		RequestedAt:     now.UTC().Format(time.RFC3339),
		RequestedBy:     "alfred-controller",
	}
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(raw, &keys); err != nil {
		t.Fatalf("inspect request keys: %v", err)
	}
	for _, key := range []string{"from_node", "hint_target_nodes", "requested_at", "requested_by"} {
		if _, ok := keys[key]; !ok {
			t.Errorf("canonical key %q missing from %s", key, raw)
		}
	}
	for _, key := range []string{"fromNode", "hintTargetNodes", "requestedAt", "requestedBy"} {
		if _, ok := keys[key]; ok {
			t.Errorf("camel-case key %q present in %s", key, raw)
		}
	}
	parsed, err := audit.ParseMigrationRequest(string(raw))
	if err != nil {
		t.Fatalf("audit.ParseMigrationRequest: %v", err)
	}
	if parsed.FromNode != req.FromNode || parsed.RequestedAt != req.RequestedAt || parsed.RequestedBy != req.RequestedBy {
		t.Fatalf("round-trip mismatch: got %+v want %+v", parsed, req)
	}
	if got := migrationAnnotationKey("request-1"); got != audit.MigrationRequestAnnotationPrefix+"request-1" {
		t.Fatalf("migrationAnnotationKey = %q", got)
	}
}
