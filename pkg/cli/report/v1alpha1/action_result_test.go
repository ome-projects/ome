package v1alpha1_test

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/cli/report"
	"sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func TestNewActionResultUsesVersionedTypedDefaultsAndInjectedTime(t *testing.T) {
	now := time.Date(2026, time.August, 31, 18, 30, 0, 0, time.UTC)
	target := v1alpha1.ActionTarget{
		Kind:            "InferenceService",
		Namespace:       "prod",
		Name:            "chat",
		UID:             "uid-1",
		ResourceVersion: "42",
	}

	got := v1alpha1.NewActionResult("pause", target, v1alpha1.DryRunServer, fixedClock{now: now})

	assert.Equal(t, v1alpha1.APIVersion, got.APIVersion)
	assert.Equal(t, v1alpha1.ActionResultKind, got.Kind)
	assert.Equal(t, now, got.CollectedAt)
	assert.Equal(t, "pause", got.Action)
	assert.Equal(t, target, got.Target)
	assert.Equal(t, v1alpha1.DryRunServer, got.DryRun)
	assert.False(t, got.Accepted)
	assert.False(t, got.Applied)
}

func TestActionResultCanonicalNeverMarksDryRunApplied(t *testing.T) {
	for _, mode := range []v1alpha1.DryRunMode{v1alpha1.DryRunClient, v1alpha1.DryRunServer} {
		t.Run(string(mode), func(t *testing.T) {
			result := actionResult()
			result.DryRun = mode
			result.Accepted = true
			result.Applied = true

			got := result.Canonical()

			assert.Equal(t, mode == v1alpha1.DryRunServer, got.Accepted)
			assert.False(t, got.Applied)
		})
	}
}

func TestActionResultCanonicalKeepsRealRequestApplication(t *testing.T) {
	result := actionResult()
	result.DryRun = v1alpha1.DryRunNone
	result.Accepted = true
	result.Applied = true

	got := result.Canonical()

	assert.True(t, got.Accepted)
	assert.True(t, got.Applied)
}

func TestActionResultCanonicalEnforcesVersionAndKind(t *testing.T) {
	result := actionResult()
	result.APIVersion = "incorrect.example/v9"
	result.Kind = "IncorrectKind"

	got := result.Canonical()

	assert.Equal(t, v1alpha1.APIVersion, got.APIVersion)
	assert.Equal(t, v1alpha1.ActionResultKind, got.Kind)
	assert.Equal(t, "incorrect.example/v9", result.APIVersion)
	assert.Equal(t, "IncorrectKind", result.Kind)
}

func TestActionResultTableUsesTypedResult(t *testing.T) {
	result := actionResult()
	result.DryRun = v1alpha1.DryRunServer
	result.Accepted = true
	result.Applied = false
	result.RequestID = "request-7"
	result.RevisionHash = "sha256:abc"
	result.Message = "request validated"
	result.FollowUp = "kubectl ome status chat -n prod"
	var output bytes.Buffer

	require.NoError(t, report.Write(&output, report.FormatTable, result))
	assert.Equal(t, "FIELD           VALUE\n"+
		"action          pause\n"+
		"target          InferenceService/prod/chat\n"+
		"dry-run         server\n"+
		"accepted        Yes\n"+
		"applied         No\n"+
		"request-id      request-7\n"+
		"revision-hash   sha256:abc\n"+
		"message         request validated\n"+
		"follow-up       kubectl ome status chat -n prod\n"+
		"hint            Use -o json or -o yaml for full values.\n", output.String())
}

func TestActionResultTableIncludesOperationalFields(t *testing.T) {
	result := actionResult()
	result.RequestID = "request-7"
	result.RevisionHash = "sha256:abc"
	result.Message = "request accepted"
	result.FollowUp = "kubectl ome status chat -n prod"

	got := result.Table()

	assert.Equal(t, []string{"FIELD", "VALUE"}, got.Headers)
	assert.Equal(t, [][]string{
		{"action", "pause"}, {"target", "InferenceService/prod/chat"},
		{"dry-run", "none"}, {"accepted", "No"}, {"applied", "No"},
		{"request-id", "request-7"}, {"revision-hash", "sha256:abc"},
		{"message", "request accepted"}, {"follow-up", "kubectl ome status chat -n prod"},
		{"hint", "Use -o json or -o yaml for full values."},
	}, got.Rows)
}

func TestActionResultTableUsesDashesForEmptyOptionalFields(t *testing.T) {
	result := actionResult()

	got := result.Table()

	for _, row := range got.Rows[5:9] {
		assert.Equal(t, "-", row[1])
	}
}

func TestActionResultMachineOutputContract(t *testing.T) {
	result := actionResult()
	result.RequestID = "request-7"
	result.RevisionHash = "sha256:abc"
	result.Accepted = true
	result.Applied = true
	result.Message = "request accepted"
	result.FollowUp = "kubectl ome status chat -n prod"
	tests := []struct {
		name   string
		format report.Format
		want   string
	}{
		{
			name:   "json",
			format: report.FormatJSON,
			want: "{\n" +
				`  "apiVersion": "cli.ome.io/v1alpha1",` + "\n" +
				`  "kind": "ActionResult",` + "\n" +
				`  "collectedAt": "2026-08-31T18:30:00Z",` + "\n" +
				`  "action": "pause",` + "\n" +
				`  "target": {` + "\n" +
				`    "kind": "InferenceService",` + "\n" +
				`    "namespace": "prod",` + "\n" +
				`    "name": "chat",` + "\n" +
				`    "uid": "uid-1",` + "\n" +
				`    "resourceVersion": "42"` + "\n" +
				"  },\n" +
				`  "dryRun": "none",` + "\n" +
				`  "requestID": "request-7",` + "\n" +
				`  "revisionHash": "sha256:abc",` + "\n" +
				`  "accepted": true,` + "\n" +
				`  "applied": true,` + "\n" +
				`  "message": "request accepted",` + "\n" +
				`  "followUp": "kubectl ome status chat -n prod"` + "\n" +
				"}\n",
		},
		{
			name:   "yaml",
			format: report.FormatYAML,
			want: "accepted: true\n" +
				"action: pause\n" +
				"apiVersion: cli.ome.io/v1alpha1\n" +
				"applied: true\n" +
				"collectedAt: \"2026-08-31T18:30:00Z\"\n" +
				"dryRun: none\n" +
				"followUp: kubectl ome status chat -n prod\n" +
				"kind: ActionResult\n" +
				"message: request accepted\n" +
				"requestID: request-7\n" +
				"revisionHash: sha256:abc\n" +
				"target:\n" +
				"  kind: InferenceService\n" +
				"  name: chat\n" +
				"  namespace: prod\n" +
				"  resourceVersion: \"42\"\n" +
				"  uid: uid-1\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var output bytes.Buffer
			require.NoError(t, report.Write(&output, tt.format, result))
			assert.Equal(t, tt.want, output.String())
		})
	}
}

func TestActionResultJSONOmitsEmptyOptionalFields(t *testing.T) {
	var output bytes.Buffer

	require.NoError(t, report.Write(&output, report.FormatJSON, actionResult()))
	assert.Equal(t, "{\n"+
		`  "apiVersion": "cli.ome.io/v1alpha1",`+"\n"+
		`  "kind": "ActionResult",`+"\n"+
		`  "collectedAt": "2026-08-31T18:30:00Z",`+"\n"+
		`  "action": "pause",`+"\n"+
		`  "target": {`+"\n"+
		`    "kind": "InferenceService",`+"\n"+
		`    "namespace": "prod",`+"\n"+
		`    "name": "chat",`+"\n"+
		`    "uid": "uid-1",`+"\n"+
		`    "resourceVersion": "42"`+"\n"+
		"  },\n"+
		`  "dryRun": "none",`+"\n"+
		`  "accepted": false,`+"\n"+
		`  "applied": false`+"\n"+
		"}\n", output.String())
}

func TestActionResultSchemaHasNoUnstructuredOrSecretBearingFields(t *testing.T) {
	assertTypedSchema(t, reflect.TypeOf(v1alpha1.ActionResult{}), map[reflect.Type]bool{})
}

func actionResult() v1alpha1.ActionResult {
	return v1alpha1.ActionResult{
		Action: "pause",
		Target: v1alpha1.ActionTarget{
			Kind:            "InferenceService",
			Namespace:       "prod",
			Name:            "chat",
			UID:             "uid-1",
			ResourceVersion: "42",
		},
		CollectedAt: time.Date(2026, time.August, 31, 18, 30, 0, 0, time.UTC),
		DryRun:      v1alpha1.DryRunNone,
	}
}
