package snapshot

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	codec "sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

func TestCompactedIRPreservesMigrationEvidence(t *testing.T) {
	// Catches row decoding that drops the independently stored migration status.
	fixture := newOMENativeFixture()
	started := time.Date(2026, 9, 17, 9, 0, 0, 0, time.UTC)
	fixture.ir.Status.Migrations = []v1beta1.MigrationStatus{migrationStatus("active-compact", v1beta1.MigrationTriggerManual, 3, v1beta1.MigrationPhaseAccepted, started)}
	columns, err := codec.EncodeColumns(fixture.ir.Status.InstanceStatuses, 10_000)
	if err != nil {
		t.Fatal(err)
	}
	marker := v1beta1.InstanceStatusEncodingColumnarV2
	fixture.ir.Status.InstanceStatuses = nil
	fixture.ir.Status.InstanceStatusEncoding = &marker
	fixture.ir.Status.InstanceStatusColumns = columns
	workload := buildOMENativeFixtureWorkload(t, fixture)
	if !workload.MigrationStateValid || len(workload.ActiveMigrations) != 1 || workload.ActiveMigrations[0].UUID != "active-compact" || workload.ActiveMigrations[0].Instance != 3 {
		t.Fatalf("compact migration evidence = valid:%t active:%+v", workload.MigrationStateValid, workload.ActiveMigrations)
	}
}

func TestMalformedCompactUnionCannotCreateMigrationSource(t *testing.T) {
	// Catches an undecodable status accidentally authorizing an annotated request.
	fixture := newOMENativeFixture()
	fixture.pods = nil
	fixture.isvc.Annotations = map[string]string{migrationAnnotationKey("pending"): `{"schemaVersion":"v1","component":"engine","instance":3,"from_node":"node-a","requested_at":"2026-09-17T09:00:00Z"}`}
	marker := v1beta1.InstanceStatusEncodingColumnarV2
	fixture.ir.Status.InstanceStatusEncoding = &marker // mixed with the existing dense row
	fixture.ir.Status.InstanceStatusColumns = &v1beta1.InstanceStatusColumns{Members: "3"}
	workload := buildOMENativeFixtureWorkload(t, fixture)
	if workload.Components[v1beta1.EngineComponent].ObservationValid || len(workload.ActiveMigrations) != 0 || workload.MigrationStateValid {
		t.Fatalf("malformed union created usable migration state: %+v", workload)
	}
}

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
	raw := `{"schemaVersion":"v1","component":"engine","instance":2,"from_node":"gpu-a","hint_target_nodes":["gpu-b","gpu-c"],"reason":"fragmentation","requested_at":"2026-08-31T09:30:00Z","requested_by":"alfred-controller"}`

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
	tests := []struct {
		name        string
		uuid        string
		mutate      func(map[string]any)
		wantErrPart string
	}{
		{name: "empty UUID", uuid: "", mutate: func(map[string]any) {}, wantErrPart: "UUID"},
		{name: "missing source", uuid: "request-1", mutate: func(r map[string]any) { r["from_node"] = "" }, wantErrPart: "from_node"},
		{name: "negative instance", uuid: "request-1", mutate: func(r map[string]any) { r["instance"] = -1 }, wantErrPart: "instance"},
		{name: "unknown component", uuid: "request-1", mutate: func(r map[string]any) { r["component"] = "sidecar" }, wantErrPart: "component"},
		{name: "missing current instance", uuid: "request-1", mutate: func(r map[string]any) { r["instance"] = 1 }, wantErrPart: "current instance"},
		{name: "invalid requested_at", uuid: "request-1", mutate: func(r map[string]any) { r["requested_at"] = "yesterday" }, wantErrPart: "requested_at"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := map[string]any{"schemaVersion": "v1", "component": "engine", "instance": 2, "from_node": "gpu-a", "requested_at": "2026-08-31T09:30:00Z"}
			tt.mutate(req)
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
	raw := `{"schemaVersion":"v1","component":"engine","instance":2,"from_node":"gpu-a","requested_by":"foreign-controller"}`

	got, err := parsePendingMigration("legacy-1", string(raw), migrationTestWorkload())
	if err != nil {
		t.Fatalf("parsePendingMigration: %v", err)
	}
	if !got.RequestedAt.IsZero() {
		t.Fatalf("RequestedAt = %v, want zero", got.RequestedAt)
	}
}

func TestApplyMigrationStatePreservesRequesterFromPendingAnnotation(t *testing.T) {
	raw := `{"schemaVersion":"v1","component":"engine","instance":0,"from_node":"gpu-a","requested_at":"2026-08-31T09:30:00Z","requested_by":"alfred-controller"}`

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

func TestApplyMigrationStateStoresPayloadFreeMalformedRequestReason(t *testing.T) {
	schemaPayload := strings.Repeat("private-schema-", 32)
	componentPayload := strings.Repeat("private-component-", 32)
	requestedAtPayload := strings.Repeat("private-requested-at-", 32)
	raw, err := json.Marshal(map[string]interface{}{
		"schemaVersion": schemaPayload,
		"component":     componentPayload,
		"instance":      0,
		"from_node":     "gpu-a",
		"requested_at":  requestedAtPayload,
	})
	if err != nil {
		t.Fatalf("marshal malformed request: %v", err)
	}

	workload := migrationTestWorkload()
	applyMigrationState(workload, &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Annotations: map[string]string{migrationAnnotationKey("malformed"): string(raw)},
	}})

	got := workload.MalformedRequests["malformed"]
	if got != migrationStateReasonRequestInvalid {
		t.Fatalf("malformed reason = %q, want fixed %q", got, migrationStateReasonRequestInvalid)
	}
	if len(got) > 128 {
		t.Fatalf("malformed reason is not bounded: %d bytes", len(got))
	}
	for _, payload := range []string{schemaPayload, componentPayload, requestedAtPayload} {
		if strings.Contains(got, payload) {
			t.Fatalf("malformed reason exposes request payload: %q", got)
		}
	}
}

func TestApplyMigrationStateOverlaysAuthoritativeIRStatus(t *testing.T) {
	now := time.Date(2026, 8, 31, 10, 0, 0, 0, time.UTC)
	annotation := `{"schemaVersion":"v1","component":"engine","instance":0,"from_node":"gpu-a","requested_at":"2026-08-31T09:59:00Z","requested_by":"alfred-controller"}`

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

func TestApplyMigrationStateFailsClosedWhenOMENativeSiblingHasNoAcceptedIR(t *testing.T) {
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name       string
		wantReason string
		mutate     func(*omeNativeFixture, *v1beta1.InferenceReplica)
	}{
		{
			name:       "missing sibling IR",
			wantReason: observationReasonIRMissing,
			mutate: func(f *omeNativeFixture, _ *v1beta1.InferenceReplica) {
				f.extra = nil
			},
		},
		{
			name:       "duplicate sibling IR",
			wantReason: observationReasonIRDuplicate,
			mutate: func(f *omeNativeFixture, ir *v1beta1.InferenceReplica) {
				duplicate := ir.DeepCopy()
				duplicate.Name += "-duplicate"
				duplicate.UID += "-duplicate"
				f.extra = append(f.extra, duplicate)
			},
		},
		{
			name:       "bad-owner sibling IR",
			wantReason: observationReasonIROwner,
			mutate: func(_ *omeNativeFixture, ir *v1beta1.InferenceReplica) {
				ir.OwnerReferences[0].UID = "wrong-isvc-uid"
			},
		},
		{
			name:       "stale sibling IR",
			wantReason: observationReasonIRStale,
			mutate: func(_ *omeNativeFixture, ir *v1beta1.InferenceReplica) {
				ir.Status.ObservedGeneration--
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newOMENativeFixture()
			decoderIR, _ := addOMENativeDecoder(fixture)
			active := migrationStatus("active", v1beta1.MigrationTriggerManual, 3,
				v1beta1.MigrationPhaseAccepted, now.Add(-2*time.Minute))
			terminal := migrationStatus("terminal", v1beta1.MigrationTriggerManual, 3,
				v1beta1.MigrationPhaseCompleted, now.Add(-4*time.Minute))
			completed := metav1.NewTime(now.Add(-3 * time.Minute))
			terminal.CompletedAt = &completed
			fixture.ir.Status.Migrations = []v1beta1.MigrationStatus{active, terminal}
			test.mutate(fixture, decoderIR)

			workload := buildOMENativeFixtureWorkload(t, fixture)
			sibling := workload.Components[v1beta1.DecoderComponent]
			if sibling == nil || sibling.DeploymentMode != constants.OMENative || sibling.IR != nil ||
				sibling.ObservationReason != test.wantReason {
				t.Fatalf("unaccepted OMENative sibling = %+v, want reason %q", sibling, test.wantReason)
			}
			if workload.MigrationStateValid || workload.MigrationStateReason != migrationStateReasonStatusInvalid {
				t.Fatalf("migration state = valid:%t reason:%q, want invalid status evidence",
					workload.MigrationStateValid, workload.MigrationStateReason)
			}
			if len(workload.ActiveMigrations) != 1 || workload.ActiveMigrations[0].UUID != "active" {
				t.Fatalf("accepted sibling active evidence = %+v, want active", workload.ActiveMigrations)
			}
			if workload.LastMigration == nil || !workload.LastMigration.Equal(completed.Time) {
				t.Fatalf("accepted sibling terminal evidence = %v, want %v", workload.LastMigration, completed.Time)
			}
		})
	}
}

func TestApplyMigrationStateAllowsNonOMENativeComponentWithoutIR(t *testing.T) {
	workload := migrationTestWorkload()
	workload.Components[v1beta1.EngineComponent].DeploymentMode = constants.RawDeployment

	applyMigrationState(workload, &v1beta1.InferenceService{})

	if !workload.MigrationStateValid || workload.MigrationStateReason != "" {
		t.Fatalf("Raw component without IR invalidated migration state: valid:%t reason:%q",
			workload.MigrationStateValid, workload.MigrationStateReason)
	}
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

func TestPendingMigrationWireCompatibility(t *testing.T) {
	const valid = `{"schemaVersion":"v1","component":"engine","instance":2,"from_node":"gpu-a","hint_target_nodes":["gpu-b"],"reason":"fragmentation","requested_at":"2026-08-31T09:30:00Z","requested_by":"alfred-controller"}`
	wantTime := time.Date(2026, 8, 31, 9, 30, 0, 0, time.UTC)
	for _, tt := range []struct {
		name         string
		raw          string
		wantInstance int32
		wantTime     time.Time
		wantErr      bool
	}{
		{name: "canonical", raw: valid, wantInstance: 2, wantTime: wantTime},
		{name: "additive nested field", raw: strings.TrimSuffix(valid, "}") + `,"future":{"flags":[true,1,"value"]}}`, wantInstance: 2, wantTime: wantTime},
		{name: "fractional offset timestamp", raw: strings.Replace(valid, "2026-08-31T09:30:00Z", "2026-08-31T02:30:00.123456789-07:00", 1), wantInstance: 2, wantTime: wantTime.Add(123456789 * time.Nanosecond)},
		{name: "missing instance defaults to zero", raw: strings.Replace(valid, `"instance":2,`, "", 1), wantTime: wantTime},
		{name: "null instance defaults to zero", raw: strings.Replace(valid, `"instance":2`, `"instance":null`, 1), wantTime: wantTime},
		{name: "duplicate scalar uses last value", raw: strings.Replace(valid, `"instance":2`, `"instance":1,"instance":2`, 1), wantInstance: 2, wantTime: wantTime},
		{name: "unsupported schema", raw: strings.Replace(valid, `"v1"`, `"v2"`, 1), wantErr: true},
		{name: "missing schema", raw: strings.Replace(valid, `"schemaVersion":"v1",`, "", 1), wantErr: true},
		{name: "null schema", raw: strings.Replace(valid, `"schemaVersion":"v1"`, `"schemaVersion":null`, 1), wantErr: true},
		{name: "trailing object", raw: valid + `{}`, wantErr: true},
		{name: "trailing whitespace", raw: valid + "\n\t ", wantInstance: 2, wantTime: wantTime},
		{name: "null request", raw: `null`, wantErr: true},
		{name: "array request", raw: `[]`, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parsePendingMigration("request-1", tt.raw, migrationTestWorkload())
			if tt.wantErr {
				if err == nil {
					t.Fatal("accepted invalid migration request")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.UUID != "request-1" || got.Component != v1beta1.EngineComponent || got.Instance != tt.wantInstance || got.FromNode != "gpu-a" || got.RequestedBy != "alfred-controller" || !got.RequestedAt.Equal(tt.wantTime) {
				t.Fatalf("migration = %+v, want instance %d and requested_at %v with unchanged identity", got, tt.wantInstance, tt.wantTime)
			}
		})
	}
	if got := migrationAnnotationKey("request-1"); got != "ome.io/migration-request-v1-request-1" {
		t.Fatalf("migrationAnnotationKey = %q", got)
	}
}

func TestPendingMigrationRejectsMalformedKnownFields(t *testing.T) {
	for _, tt := range []struct {
		name  string
		field string
		value any
	}{
		{name: "schema type", field: "schemaVersion", value: 1},
		{name: "component type", field: "component", value: []string{"engine"}},
		{name: "instance string", field: "instance", value: "2"},
		{name: "instance fraction", field: "instance", value: 2.5},
		{name: "instance overflow", field: "instance", value: int64(2147483648)},
		{name: "source type", field: "from_node", value: 1},
		{name: "hints scalar", field: "hint_target_nodes", value: "gpu-b"},
		{name: "hints element type", field: "hint_target_nodes", value: []any{"gpu-b", 1}},
		{name: "reason type", field: "reason", value: false},
		{name: "timestamp type", field: "requested_at", value: 1},
		{name: "requester type", field: "requested_by", value: map[string]any{"name": "alfred"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			request := map[string]any{"schemaVersion": "v1", "component": "engine", "instance": 2, "from_node": "gpu-a"}
			request[tt.field] = tt.value
			raw, err := json.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := parsePendingMigration("request-1", string(raw), migrationTestWorkload()); err == nil {
				t.Fatalf("accepted invalid %s: %s", tt.field, raw)
			}
		})
	}
}
