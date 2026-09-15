package alfredrecommendations

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"

	reportv1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

var fixedNow = time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)

func projectFixture(raw string) reportv1.AlfredRecommendationsReport {
	return Project(Snapshot{Namespace: "ome", ConfigName: "alfred-config", ConfigKey: "config.yaml",
		Config: ConfigEvidence{State: "Available", Enabled: true, RecordName: "alfred-recommendations", Mode: "recommend-only", Interval: 5 * time.Minute}, RecordState: "Available", Record: raw}, reportv1.ClockFunc(func() time.Time { return fixedNow }))
}

const advisoryRow = `{"workload":"prod/chat","component":"engine","instance":0,"policy":"defragmentation","reason":"Fragmentation","outcome":"advisory","advisoryReason":"RawDeploymentMigrationUnsupported","score":0.5}`

func cycle(rows string) string {
	return `{"timestamp":"2026-09-15T11:59:00Z","mode":"recommend-only","recommendations":` + rows + `}`
}

func TestParseConfig(t *testing.T) {
	for _, tc := range []struct {
		name, raw, state, record, mode string
		enabled                        bool
		interval                       time.Duration
	}{
		{"defaults", "schemaVersion: 1", "Available", "alfred-recommendations", "recommend-only", true, 5 * time.Minute},
		{"explicit", "schemaVersion: 1\nmode: execute\nrecommendationsConfigMapEnabled: false\nrecommendationsConfigMapName: custom\ndecisionLoopInterval: 30s\n", "Available", "custom", "execute", false, 30 * time.Second},
		{"empty", "", "Malformed", "", "", false, 0},
		{"schema", "schemaVersion: 2", "UnsupportedSchema", "", "", false, 0},
		{"negative schema", "schemaVersion: -1", "UnsupportedSchema", "", "", false, 0},
		{"bad yaml", "schemaVersion: [secret", "Malformed", "", "", false, 0},
		{"wrong bool", "schemaVersion: 1\nrecommendationsConfigMapEnabled: secret", "Malformed", "", "", false, 0},
		{"wrong mode", "schemaVersion: 1\nmode: SECRET", "Malformed", "", "", false, 0},
		{"bad name", "schemaVersion: 1\nrecommendationsConfigMapName: ../secret", "Malformed", "", "", false, 0},
		{"duplicate", "schemaVersion: 1\nmode: execute\nmode: recommend-only", "Malformed", "", "", false, 0},
		{"unknown credential", "schemaVersion: 1\npassword: hunter2", "Malformed", "", "", false, 0},
		{"bad interval", "schemaVersion: 1\ndecisionLoopInterval: -1s", "Malformed", "", "", false, 0},
		{"oversized", strings.Repeat(" ", MaxConfigBytes+1), "Oversized", "", "", false, 0},
		{"exact limit", "schemaVersion: 1\n#" + strings.Repeat("x", MaxConfigBytes-18), "Available", "alfred-recommendations", "recommend-only", true, 5 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := ParseConfig(tc.raw)
			if got.State != tc.state || got.RecordName != tc.record || got.Enabled != tc.enabled || got.Mode != tc.mode || got.Interval != tc.interval {
				t.Fatalf("got %+v; want %s %s %s %t %s", got, tc.state, tc.record, tc.mode, tc.enabled, tc.interval)
			}
		})
	}
}

func TestRecordShapeAndFreshness(t *testing.T) {
	for _, tc := range []struct{ name, raw, state, record, freshness string }{
		{"empty", cycle("[]"), "Empty", "Available", "Recent"},
		{"reporter null", cycle("null"), "Empty", "Available", "Recent"},
		{"nonempty", cycle("[" + advisoryRow + "]"), "Reported", "Available", "Recent"},
		{"missing recommendations", `{"timestamp":"2026-09-15T11:59:00Z","mode":"recommend-only"}`, "Unavailable", "Malformed", "Unavailable"},
		{"missing timestamp", `{"mode":"recommend-only","recommendations":[]}`, "Unavailable", "Malformed", "Unavailable"},
		{"null", "null", "Unavailable", "Malformed", "Unavailable"},
		{"bad json", "{secret", "Unavailable", "Malformed", "Unavailable"},
		{"extra document", cycle("[]") + " {}", "Unavailable", "Malformed", "Unavailable"},
		{"timestamp zero", strings.Replace(cycle("[]"), "2026-09-15T11:59:00Z", "0001-01-01T00:00:00Z", 1), "Unavailable", "Malformed", "Unavailable"},
		{"timestamp bad", strings.Replace(cycle("[]"), "2026-09-15T11:59:00Z", "SECRET", 1), "Unavailable", "Malformed", "Unavailable"},
		{"mode bad", strings.Replace(cycle("[]"), "recommend-only", "SECRET", 1), "Unavailable", "Malformed", "Unavailable"},
		{"old", strings.Replace(cycle("[]"), "11:59:00Z", "11:49:59Z", 1), "Empty", "Available", "Stale"},
		{"exact age", strings.Replace(cycle("[]"), "11:59:00Z", "11:50:00Z", 1), "Empty", "Available", "Recent"},
		{"future", strings.Replace(cycle("[]"), "11:59:00Z", "12:00:01Z", 1), "Empty", "Available", "Future"},
		{"duplicate", `{"timestamp":"2026-09-15T11:59:00Z","mode":"recommend-only","mode":"execute","recommendations":[]}`, "Unavailable", "Malformed", "Unavailable"},
		{"deep", strings.TrimSuffix(cycle("[]"), "}") + `,"unknown":` + strings.Repeat("[", 34) + "0" + strings.Repeat("]", 34) + "}", "Unavailable", "Malformed", "Unavailable"},
		{"oversized", strings.Repeat("x", MaxRecordBytes+1), "Unavailable", "Oversized", "Unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := projectFixture(tc.raw)
			if got.Content.State != tc.state || got.Content.RecordState != tc.record || got.Content.Freshness != tc.freshness {
				t.Fatalf("content = %+v", got.Content)
			}
			if got.Content.FreshnessWindowSeconds != 600 {
				t.Fatalf("default freshness = %v", got.Content.FreshnessWindowSeconds)
			}
		})
	}
}

func TestReporterOutcomeSemantics(t *testing.T) {
	for _, tc := range []struct{ outcome, dispatch, advisory, reject, reason, class string }{
		{"advisory", "", "RawDeploymentMigrationUnsupported", "", "", "Advisory"},
		{"withheld", "", "", "", "", "Withheld"},
		{"withheld", "withheld", "", "", "ExecutionDisabled", "Withheld"},
		{"rejected", "", "", "Cooldown", "", "Rejected"},
		{"admitted", "", "", "", "", "Unverifiable"},
		{"submitted", "submitted", "", "", "", "ReportedDispatch"},
		// Outstanding dispatch observations can retain an advisory origin.
		{"acknowledged", "acknowledged", "RawDeploymentMigrationUnsupported", "", "", "ReportedDispatch"},
		{"completed", "completed", "", "", "", "ReportedDispatch"},
		{"failed", "failed", "", "", "", "ReportedDispatch"},
		{"stalled", "stalled", "", "", "", "ReportedDispatch"},
	} {
		t.Run(tc.outcome+tc.dispatch, func(t *testing.T) {
			row := fmt.Sprintf(`{"workload":"prod/chat","component":"engine","instance":0,"policy":"nodehealth","reason":"NodeUnhealthy","outcome":%q,"dispatchStatus":%q,"advisoryReason":%q,"rejectReason":%q,"dispatchReason":%q}`, tc.outcome, tc.dispatch, tc.advisory, tc.reject, tc.reason)
			got := projectFixture(cycle("[" + row + "]"))
			if len(got.Content.Recommendations) != 1 {
				t.Fatalf("content = %+v", got.Content)
			}
			r := got.Content.Recommendations[0]
			if r.Classification != tc.class || r.Executability != "Unverifiable" || r.Outcome != tc.outcome || r.AdvisoryReason != tc.advisory || r.DispatchStatus != tc.dispatch || r.RejectReason != tc.reject || r.DispatchReason != tc.reason {
				t.Fatalf("row = %+v", r)
			}
		})
	}
}

func TestPersistedDispatchReasonCodes(t *testing.T) {
	for _, code := range []string{"OwnerUnavailable", "ReplicaUnavailable", "ReplicaIdentityChanged", "MigrationStatusInvalid", "TerminalStatusObserved", "UUIDStatusObserved", "ConsumerDeadlineExceeded", "AcknowledgedStatusMissing", "RequestPayloadChanged", "RequestAnnotationObserved", "AcknowledgementTimeout", "OwnerChanged", "SubmissionPrepared", "RequestSubmitted", "SubmissionUncertain", "SubmissionJournalUncertain"} {
		row := fmt.Sprintf(`{"workload":"prod/chat","component":"engine","instance":0,"policy":"nodehealth","reason":"NodeUnhealthy","outcome":"stalled","dispatchStatus":"stalled","dispatchReason":%q}`, code)
		got := projectFixture(cycle("[" + row + "]"))
		if got.Content.Rows != 1 || got.Content.Recommendations[0].DispatchReason != code {
			t.Errorf("persisted code %s dropped: %+v", code, got.Content)
		}
	}
}

func TestInvalidRowsAndPrivateFields(t *testing.T) {
	for _, tc := range []struct{ name, old, new string }{
		{"workload", "prod/chat", "prod/chat/SECRET"},
		{"namespace", "prod/chat", "UPPER/chat"},
		{"component", "engine", "SECRET"},
		{"index", `"instance":0`, `"instance":-1`},
		{"index missing", `"instance":0,`, ``},
		{"policy", "defragmentation", "SECRET"},
		{"reason", `"reason":"Fragmentation"`, `"reason":"SECRET"`},
		{"outcome", `"outcome":"advisory"`, `"outcome":"SECRET"`},
		{"advisory", "RawDeploymentMigrationUnsupported", "SECRET"},
		{"dispatch", `"score":0.5`, `"dispatchStatus":"SECRET"`},
		{"dispatch mismatch", `"score":0.5`, `"dispatchStatus":"submitted"`},
		{"unproven dispatch", `"outcome":"advisory"`, `"outcome":"submitted"`},
		{"reject", `"score":0.5`, `"rejectReason":"SECRET"`},
		{"reject mismatch", `"score":0.5`, `"rejectReason":"Cooldown"`},
		{"dispatch reason", `"score":0.5`, `"dispatchReason":"SECRET"`},
		{"dispatch reason mismatch", `"score":0.5`, `"dispatchReason":"Cooldown"`},
		{"node", `"score":0.5`, `"fromNode":"node\nSECRET"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := projectFixture(cycle("[" + strings.Replace(advisoryRow, tc.old, tc.new, 1) + "]"))
			if got.Content.State != "Partial" || got.Content.Invalid != 1 || got.Content.Rows != 0 {
				t.Fatalf("content = %+v", got.Content)
			}
			b, _ := json.Marshal(got)
			if strings.Contains(string(b), "SECRET") {
				t.Fatalf("private value escaped: %s", b)
			}
		})
	}
	private := strings.TrimSuffix(advisoryRow, "}") + `,"requestUUID":"SECRET","target":"SECRET","hintTargets":["SECRET"],"scheduling":{"password":"SECRET"},"status":"Bearer SECRET\n\u001b[31m"}`
	got := projectFixture(cycle("[" + private + "]"))
	b, _ := json.Marshal(got)
	if got.Content.Rows != 1 || strings.Contains(string(b), "SECRET") || strings.Contains(string(b), "requestUUID") || strings.Contains(string(b), "scheduling\"") {
		t.Fatalf("projection leaked or lost row: %s", b)
	}
}

func TestBoundsDuplicateValidationAndOrdering(t *testing.T) {
	rows := make([]string, 201)
	for i := range rows {
		rows[i] = strings.Replace(advisoryRow, "prod/chat", fmt.Sprintf("prod/chat-%03d", i), 1)
	}
	got := projectFixture(cycle("[" + strings.Join(rows, ",") + "]"))
	if got.Content.Rows != 200 || got.Content.Omitted != 1 || !got.Content.Truncated || got.Content.Recommendations[199].Workload != "prod/chat-199" {
		t.Fatalf("output cap = %+v", got.Content)
	}
	// A conflicting duplicate beyond the output cap invalidates the first
	// candidate before clipping; it may not hide behind the display cap.
	rows = append(rows, strings.Replace(rows[0], `"outcome":"advisory"`, `"outcome":"withheld"`, 1))
	got = projectFixture(cycle("[" + strings.Join(rows, ",") + "]"))
	if got.Content.Rows != 200 || got.Content.Invalid != 2 || got.Content.Duplicates != 2 || got.Content.Recommendations[0].Workload != "prod/chat-001" {
		t.Fatalf("duplicate before cap = %+v", got.Content)
	}
	want, _ := json.Marshal(got)
	for seed := int64(0); seed < 8; seed++ {
		rand.New(rand.NewSource(seed)).Shuffle(len(rows), func(i, j int) { rows[i], rows[j] = rows[j], rows[i] })
		shuffled, _ := json.Marshal(projectFixture(cycle("[" + strings.Join(rows, ",") + "]")))
		if string(want) != string(shuffled) {
			t.Fatal("input order changed bounded report")
		}
	}
	for _, count := range []int{200, 800, 801} {
		rows = make([]string, count)
		for i := range rows {
			rows[i] = fmt.Sprintf(`{"workload":"p/a-%03d","component":"engine","instance":0,"policy":"nodehealth","reason":"NodeUnhealthy","outcome":"withheld"}`, i)
		}
		got = projectFixture(cycle("[" + strings.Join(rows, ",") + "]"))
		if count == 801 {
			if got.Content.RecordState != "ScanLimitExceeded" || got.Content.Rows != 0 || got.Content.Scanned != 0 || got.Content.Omitted != 801 || !got.Content.Truncated {
				t.Fatalf("scan cap = %+v", got.Content)
			}
		} else if got.Content.Scanned != count || got.Content.Rows != 200 || got.Content.Invalid != 0 {
			t.Fatalf("scan = %+v", got.Content)
		}
	}
	raw := cycle("[]")
	raw += strings.Repeat(" ", MaxRecordBytes-len(raw))
	if projectFixture(raw).Content.State != "Empty" {
		t.Fatal("exact payload bound rejected")
	}
}

func TestUnavailablePartialAndConfigAuthority(t *testing.T) {
	for _, state := range []string{"NotFound", "Forbidden", "Unreadable", "IdentityMismatch", "KeyAbsent", "Malformed", "UnsupportedSchema", "Oversized"} {
		t.Run(state, func(t *testing.T) {
			snapshot := Snapshot{Namespace: "ome", ConfigName: "alfred-config", ConfigKey: "config.yaml", Config: ConfigEvidence{State: state}, RecordState: "NotRead"}
			got := Project(snapshot, nil)
			if got.Content.State != "Unavailable" || len(got.Sources) != 1 || got.Sources[0].Evidence != "Unavailable" {
				t.Fatalf("config evidence = %+v", got)
			}
			snapshot.Config = ConfigEvidence{State: "Available", Enabled: true, RecordName: "alfred-recommendations"}
			snapshot.RecordState = state
			got = Project(snapshot, nil)
			if got.Content.ConfigState != "Available" || got.Content.RecordState != state || len(got.Sources) != 2 || got.Sources[0].Evidence != "Observed" {
				t.Fatalf("partial evidence = %+v", got)
			}
		})
	}
	snapshot := Snapshot{Namespace: "ome", ConfigName: "alfred-config", ConfigKey: "config.yaml", Config: ConfigEvidence{State: "Available", Enabled: false}, RecordState: "Disabled"}
	if got := Project(snapshot, nil); got.Content.State != "Disabled" || len(got.Sources) != 1 {
		t.Fatalf("disabled = %+v", got)
	}
	snapshot.Config = ConfigEvidence{State: "Available", Enabled: true, Mode: "execute", Interval: 30 * time.Second, RecordName: "alfred-recommendations"}
	snapshot.RecordState = "Available"
	snapshot.Record = cycle("[]")
	before := snapshot
	got := Project(snapshot, reportv1.ClockFunc(func() time.Time { return fixedNow }))
	if !reflect.DeepEqual(before, snapshot) || got.Content.ConfigMode != "execute" || got.Content.RecordMode != "recommend-only" || got.Content.FreshnessWindowSeconds != 60 || !reflect.DeepEqual(got.Content.Issues, []string{"ConfigRecordModeMismatch"}) {
		t.Fatalf("authority = %+v", got)
	}
}

// time.Sub saturates near 292 years. A valid long configured interval must
// not make a much older persisted record appear recent after saturation.
func TestFreshnessBeyondDurationRange(t *testing.T) {
	snapshot := Snapshot{Namespace: "ome", ConfigName: "alfred-config", ConfigKey: "config.yaml",
		Config:      ParseConfig("schemaVersion: 1\ndecisionLoopInterval: 2562047h47m16.854775807s"),
		RecordState: "Available", Record: strings.Replace(cycle("[]"), "2026-09-15T11:59:00Z", "1000-01-01T00:00:00Z", 1)}
	got := Project(snapshot, reportv1.ClockFunc(func() time.Time { return fixedNow }))
	if got.Content.ConfigState != "Available" || got.Content.Freshness != "Stale" {
		t.Fatalf("ancient cycle = %+v", got.Content)
	}
}
