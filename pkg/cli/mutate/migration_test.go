package mutate

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/exitcode"
	"sigs.k8s.io/ome/pkg/cli/printers"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
)

const migrationTestID = "12345678-1234-4123-8123-123456789abc"
const migrationTestPayload = `{"schemaVersion":"v1","component":"engine","instance":3,"from_node":"node-a","requested_at":"2026-09-15T21:00:00Z","requested_by":"kubectl-ome"}`

func TestMigrationCanonicalLocalValues(t *testing.T) {
	for _, raw := range []string{"0", "3", "2147483647"} {
		_, err := ParseMigrationIndex(raw)
		require.NoError(t, err)
	}
	for _, raw := range []string{"", "-1", "+0", "00", "01", " 0", "1 ", "2147483648", "1e0"} {
		_, err := ParseMigrationIndex(raw)
		require.Error(t, err)
		require.NotContains(t, err.Error(), raw+"secret")
	}
	o := MigrationOptions{Component: v1beta1.EngineComponent, Instance: 3, RequestedBy: "kubectl-ome"}
	require.NoError(t, ValidateMigrationOptions(o))
	for _, edit := range []func(*MigrationOptions){func(o *MigrationOptions) { o.Component = "secret" }, func(o *MigrationOptions) { o.Instance = -1 }, func(o *MigrationOptions) { o.FromNode = "node secret" }, func(o *MigrationOptions) { o.HintNodes = []string{"node-a", "node-a"} }, func(o *MigrationOptions) { o.HintNodes = make([]string, 9) }, func(o *MigrationOptions) { o.Reason = "Bearer secret" }, func(o *MigrationOptions) { o.Reason = "secret\n" }, func(o *MigrationOptions) { o.Reason = "secret\u202e" }, func(o *MigrationOptions) { o.Reason = "AutoRecover" }, func(o *MigrationOptions) { o.Reason = "ForceDelete" }, func(o *MigrationOptions) { o.Reason = strings.Repeat("s", 257) }, func(o *MigrationOptions) { o.RequestedBy = "user:secret@host" }, func(o *MigrationOptions) { o.RequestID = "00000000-0000-0000-0000-000000000000" }, func(o *MigrationOptions) { o.RequestID = strings.ToUpper(migrationTestID) }} {
		bad := o
		edit(&bad)
		err := ValidateMigrationOptions(bad)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "secret")
	}
}

func TestMigrationRetainedParserStrict(t *testing.T) {
	r, err := parseMigrationRequest(migrationTestPayload)
	require.NoError(t, err)
	require.Equal(t, int32(3), r.Instance)
	for _, raw := range []string{`{}`, strings.Replace(migrationTestPayload, `"v1"`, `1`, 1), strings.Replace(migrationTestPayload, `"instance":3`, `"instance":null`, 1), strings.Replace(migrationTestPayload, `"instance":3`, `"instance":3,"instance":4`, 1), strings.Replace(migrationTestPayload, `"instance":3`, `"instance":3,"unknown":0`, 1), strings.Replace(migrationTestPayload, `"requested_by":"kubectl-ome"`, `"requested_by":null`, 1), migrationTestPayload + ` {}`, strings.Replace(migrationTestPayload, "2026-09-15T21:00:00Z", "bad", 1), strings.Repeat(" ", 8193)} {
		_, err := parseMigrationRequest(raw)
		require.Error(t, err, "%s", raw[:min(len(raw), 100)])
	}
	require.False(t, strictJSON([]byte(`{"a":{"b":0,"b":1}}`)))
	require.False(t, strictJSON([]byte(strings.Repeat("[", 34)+"0"+strings.Repeat("]", 34))))
}

func TestMigrationReasonPrintableNotLineSeparators(t *testing.T) {
	for _, reason := range []string{"private\u2028secret", "private\u2029secret"} {
		err := ValidateMigrationOptions(MigrationOptions{Component: v1beta1.EngineComponent, Reason: reason})
		require.Error(t, err)
		require.NotContains(t, err.Error(), "private")
	}
}

func TestMigrationPlanExactCASAndDefensiveCopy(t *testing.T) {
	v, state := nativeTarget(t)
	o := MigrationOptions{Component: v1beta1.EngineComponent, Instance: 3, HintNodes: []string{"node-b", "node-c"}, Reason: "maintenance", RequestedBy: "kubectl-ome"}
	e := MigrationEvidence{complete: true, uid: string(v.UID), rv: v.ResourceVersion, options: migrationOptionKey(o), source: &v1beta1.OMENativeInstanceStatus{Index: 3}, fromNode: "node-a", known: map[string]bool{}}
	p, err := PrepareMigration(v, state, e, o, func() (string, error) { return migrationTestID, nil }, testClock)
	require.NoError(t, err)
	var ops []struct {
		Op, Path string
		Value    json.RawMessage
	}
	require.NoError(t, json.Unmarshal(p.Patch(), &ops))
	require.Len(t, ops, 4)
	require.Equal(t, "/metadata/uid", ops[0].Path)
	require.Equal(t, "/metadata/resourceVersion", ops[1].Path)
	require.Equal(t, "/metadata/annotations", ops[2].Path)
	require.Equal(t, "/metadata/annotations/ome.io~1migration-request-v1-"+migrationTestID, ops[3].Path)
	var raw string
	require.NoError(t, json.Unmarshal(ops[3].Value, &raw))
	server, err := audit.ParseMigrationRequest(raw)
	require.NoError(t, err)
	require.Equal(t, audit.SchemaV1, server.SchemaVersion)
	require.Equal(t, int32(3), server.Instance)
	require.Equal(t, "node-a", server.FromNode)
	require.Equal(t, []string{"node-b", "node-c"}, server.HintTargetNodes)
	require.Equal(t, testNow.Format("2006-01-02T15:04:05Z07:00"), server.RequestedAt)
	copy := p.Patch()
	copy[0] = '!'
	require.Equal(t, byte('['), p.Patch()[0])
	require.Nil(t, v.Annotations)
	_, err = PrepareMigration(v, state, e, o, func() (string, error) { return "", errors.New("entropy secret") }, testClock)
	require.Error(t, err)
	require.Equal(t, 1, exitcode.FromError(err))
	require.NotContains(t, err.Error(), "secret")
	e.known[migrationTestID] = true
	_, err = PrepareMigration(v, state, e, o, func() (string, error) { return migrationTestID, nil }, testClock)
	require.Equal(t, 3, exitcode.FromError(err))
	e.complete = false
	_, err = PrepareMigration(v, state, e, o, nil, testClock)
	require.Equal(t, 3, exitcode.FromError(err))
}

func TestMigrationLookupNeverMintsOrReplays(t *testing.T) {
	v, state := nativeTarget(t)
	r, err := parseMigrationRequest(migrationTestPayload)
	require.NoError(t, err)
	o := MigrationOptions{Component: v1beta1.EngineComponent, Instance: 3, RequestedBy: "kubectl-ome", RequestID: migrationTestID}
	e := MigrationEvidence{complete: true, uid: string(v.UID), rv: v.ResourceVersion, options: migrationOptionKey(o), pending: map[string]migrationRequest{migrationTestID: r}, conflicting: map[string]bool{}}
	p, err := PrepareMigration(v, state, e, o, func() (string, error) { t.Fatal("lookup minted UUID"); return "", nil }, testClock)
	require.NoError(t, err)
	require.True(t, p.Existing())
	require.Empty(t, p.Patch())
	require.Equal(t, r, p.request)
	for _, edit := range []func(*MigrationOptions){func(o *MigrationOptions) { o.Reason = "changed" }, func(o *MigrationOptions) { o.RequestedBy = "other" }, func(o *MigrationOptions) { o.FromNode = "node-b" }, func(o *MigrationOptions) { o.HintNodes = []string{"node-b"} }, func(o *MigrationOptions) { o.Instance = 4 }} {
		bad := o
		edit(&bad)
		e.options = migrationOptionKey(bad)
		_, err := PrepareMigration(v, state, e, bad, nil, testClock)
		require.Equal(t, 3, exitcode.FromError(err))
	}
	e.options = migrationOptionKey(o)
	e.conflicting[migrationTestID] = true
	_, err = PrepareMigration(v, state, e, o, nil, testClock)
	require.Equal(t, 3, exitcode.FromError(err))
	delete(e.pending, migrationTestID)
	_, err = PrepareMigration(v, state, e, o, nil, testClock)
	require.Equal(t, 3, exitcode.FromError(err))
}

func TestMigrationOpaqueEvidenceAndPlan(t *testing.T) {
	e := MigrationEvidence{ir: &v1beta1.InferenceReplica{}, fingerprint: "private-secret"}
	p := MigrationPlan{evidence: e, request: migrationRequest{Reason: "private-secret"}}
	for _, value := range []any{e, p} {
		_, err := json.Marshal(value)
		require.Error(t, err)
		require.NotContains(t, fmt.Sprintf("%v %#v", value, value), "private-secret")
	}
}

func TestMigrationPreviewFullSafeValuesAndBounds(t *testing.T) {
	p := MigrationPlan{target: reportv1alpha1.ActionTarget{Kind: "InferenceService", Name: "chat", Namespace: "prod", UID: "uid-chat", ResourceVersion: "42"}, id: migrationTestID, request: migrationRequest{SchemaVersion: "v1", Component: "engine", Instance: 3, FromNode: "node-a", Reason: strings.Repeat("漢", 70)}}
	p.evidence.warnings = []string{"Node hints are soft preferences; capacity, scheduling and controller convergence are not guaranteed."}
	var out bytes.Buffer
	require.NoError(t, p.WritePreview(&out, "moirai", "ome", reportv1alpha1.DryRunClient))
	require.Equal(t, 70, strings.Count(out.String(), "漢"))
	for _, line := range strings.Split(out.String(), "\n") {
		require.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
	}
	require.Contains(t, out.String(), migrationTestID)
	require.NotContains(t, out.String(), constants.MigrationRequestAnnotationPrefix)
}

func TestMigrationPreviewPreservesExactReasonWhitespace(t *testing.T) {
	p := MigrationPlan{target: reportv1alpha1.ActionTarget{UID: "uid"}, request: migrationRequest{Reason: "  maintenance  "}}
	var out bytes.Buffer
	require.NoError(t, p.WritePreview(&out, "moirai", "ome", reportv1alpha1.DryRunClient))
	require.Contains(t, out.String(), `"  maintenance  "`)
}
