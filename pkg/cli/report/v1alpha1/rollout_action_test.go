package v1alpha1_test

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
	"sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func TestRolloutActionDetailsMachineAndCompactContracts(t *testing.T) {
	result := actionResult()
	result.Action = "rollout repin"
	result.Rollout = &v1alpha1.RolloutActionDetails{
		RunID: "chat-0123456789ab", PinnedPlanDigest: "rp1:aaaaaaaaaaaa",
		RequestedPlanDigest: "rp1:bbbbbbbbbbbb", GroupCount: 2,
	}
	result.Message = "API accepted repin annotation; controller consumption and plan replacement were not observed."
	result.FollowUp = "kubectl ome rollout explain chat -n prod --context=moirai"

	var compact bytes.Buffer
	require.NoError(t, report.Write(&compact, report.FormatTable, result))
	for _, want := range []string{"run-id", "pinned-plan-digest", "requested-plan-digest", "group-count", "2"} {
		require.Contains(t, compact.String(), want)
	}
	for _, line := range strings.Split(compact.String(), "\n") {
		require.LessOrEqual(t, printers.CellDisplayWidth(line), 80, line)
	}

	var machine bytes.Buffer
	require.NoError(t, report.Write(&machine, report.FormatJSON, result))
	require.JSONEq(t, `{
      "apiVersion":"cli.ome.io/v1alpha1","kind":"ActionResult",
      "collectedAt":"2026-08-31T18:30:00Z","action":"rollout repin",
      "target":{"kind":"InferenceService","namespace":"prod","name":"chat","uid":"uid-1","resourceVersion":"42"},
      "dryRun":"none","accepted":false,"applied":false,
		"message":"API accepted repin annotation; controller consumption and plan replacement were not observed.",
		"followUp":"kubectl ome rollout explain chat -n prod --context=moirai",
		"rollout":{"runID":"chat-0123456789ab","pinnedPlanDigest":"rp1:aaaaaaaaaaaa","requestedPlanDigest":"rp1:bbbbbbbbbbbb","groupCount":2}
    }`, machine.String())
}

func TestRolloutActionDetailsCanonicalCopyAndWideIdentity(t *testing.T) {
	result := actionResult()
	result.Rollout = &v1alpha1.RolloutActionDetails{RunID: "chat-0123456789ab"}
	canonical := result.Canonical()
	require.NotSame(t, result.Rollout, canonical.Rollout)
	canonical.Rollout.RunID = "changed"
	require.Equal(t, "chat-0123456789ab", result.Rollout.RunID)

	wide := result.WideTable()
	require.Contains(t, wide.Rows, []string{"uid", "uid-1"})
	require.Contains(t, wide.Rows, []string{"resource-version", "42"})
}

func TestActionResultSchemaIncludesOnlyTypedRolloutDetails(t *testing.T) {
	typeOf := reflect.TypeOf(v1alpha1.ActionResult{})
	field, ok := typeOf.FieldByName("Rollout")
	require.True(t, ok)
	require.Equal(t, reflect.TypeOf((*v1alpha1.RolloutActionDetails)(nil)), field.Type)
	assertTypedSchema(t, typeOf, map[reflect.Type]bool{})
}

func TestRolloutActionCompactAndWideTablesStayWithin80Columns(t *testing.T) {
	for _, wide := range []bool{false, true} {
		result := actionResult()
		result.Target.Name = strings.Repeat("n", 253)
		result.Target.UID = strings.Repeat("u", 256)
		result.Target.ResourceVersion = strings.Repeat("r", 256)
		result.Rollout = &v1alpha1.RolloutActionDetails{
			RunID:               strings.Repeat("n", 253) + "-0123456789ab",
			PinnedPlanDigest:    "rp1:aaaaaaaaaaaa",
			RequestedPlanDigest: "rp1:bbbbbbbbbbbb",
			GroupCount:          3,
		}
		result.Message = strings.Repeat("accepted-", 30)
		result.FollowUp = "kubectl ome rollout explain " + result.Target.Name + " -n prod"
		table := result.Table()
		if wide {
			table = result.WideTable()
		}
		var out bytes.Buffer
		require.NoError(t, table.Write(&out))
		for _, line := range strings.Split(out.String(), "\n") {
			require.LessOrEqual(t, printers.CellDisplayWidth(line), 80, line)
		}
	}
}
