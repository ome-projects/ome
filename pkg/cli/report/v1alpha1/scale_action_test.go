package v1alpha1

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
	"sigs.k8s.io/yaml"
)

func TestActionResultProvidesBoundedWideScaleView(t *testing.T) {
	_, ok := any(ActionResult{}).(interface{ WideTable() report.Table })
	require.True(t, ok, "scale action requires a typed wide view")
}

func TestScaleWideSourceIdentityRespectsResourceScope(t *testing.T) {
	result := NewActionResult("scale", ActionTarget{Kind: "InferenceReplica", Namespace: "prod", Name: "chat-engine"}, DryRunClient, SystemClock{})
	result.Scale = &ScaleActionDetails{
		Parent: ScaleSourceIdentity{Kind: "InferenceService", Namespace: "prod", Name: "chat"},
		Sources: []ScaleSourceIdentity{
			{Kind: "ClusterBaseModel", Name: "model"},
			{Kind: "ClusterServingRuntime", Name: "runtime"},
			{Kind: "ServingRuntime", Namespace: "prod", Name: "runtime"},
		},
	}
	var identities []string
	for _, row := range result.WideTable().Rows {
		if row[0] == "parent" || row[0] == "source identity" {
			identities = append(identities, row[1])
		}
	}
	require.Equal(t, []string{
		"InferenceService/prod/chat",
		"ClusterBaseModel/model",
		"ClusterServingRuntime/runtime",
		"ServingRuntime/prod/runtime",
	}, identities)
	result.Scale.Parent = ScaleSourceIdentity{Kind: "ClusterServingRuntime", Name: "runtime"}
	for _, row := range result.WideTable().Rows {
		if row[0] == "parent" {
			require.Equal(t, "ClusterServingRuntime/runtime", row[1])
		}
	}
}

func TestScaleActionCanonicalFourViewsAndUnicodeWidth(t *testing.T) {
	result := NewActionResult("scale", ActionTarget{Kind: "InferenceReplica", Namespace: "prod", Name: "chat-engine", UID: "uid-ir", ResourceVersion: "81"}, DryRunNone, ClockFunc(func() time.Time { return time.Unix(1, 0) }))
	result.Accepted = true
	result.Applied = true
	result.Message = strings.Repeat("界é🙂", 40)
	result.Scale = &ScaleActionDetails{Component: "engine", Subresource: "/scale", Field: "spec.replicas", PriorReplicas: 1, RequestedReplicas: 3, MinReplicas: 1, MaxReplicas: 10, Class: "None", ManagedBy: "none", SpecSource: "isvc", Override: true, Transient: true, ParentFreshness: "Unverifiable", Sources: []ScaleSourceIdentity{{Kind: "ServingRuntime", Name: "z", UID: "uid-z"}, {Kind: "ServingRuntime", Name: "a", UID: "uid-a"}}, Warnings: []string{"TransientReplicaRequest", "AlphaAction", "AlphaAction"}, Issues: []string{"ReportedDesiredCountDiscrepancy"}}
	canonical := result.Canonical()
	require.Equal(t, "a", canonical.Scale.Sources[0].Name)
	require.Len(t, canonical.Scale.Warnings, 2)
	canonical.Scale.Sources[0].UID = "changed"
	canonical.Scale.Warnings[0] = "changed"
	require.Equal(t, "uid-a", result.Scale.Sources[1].UID)
	require.NotEqual(t, "changed", result.Scale.Warnings[0])
	for _, wide := range []bool{false, true} {
		var out bytes.Buffer
		table := result.Table()
		if wide {
			table = result.WideTable()
		}
		require.NoError(t, table.Write(&out))
		for _, line := range strings.Split(out.String(), "\n") {
			require.LessOrEqual(t, printers.CellDisplayWidth(line), 80, line)
		}
	}
	for _, format := range []report.Format{report.FormatJSON, report.FormatYAML} {
		var out bytes.Buffer
		require.NoError(t, report.Write(&out, format, result))
		var decoded ActionResult
		if format == report.FormatJSON {
			require.NoError(t, json.Unmarshal(out.Bytes(), &decoded))
		} else {
			require.NoError(t, yaml.Unmarshal(out.Bytes(), &decoded))
		}
		require.Equal(t, result.Message, decoded.Message)
		require.Equal(t, int32(3), decoded.Scale.RequestedReplicas)
		require.True(t, decoded.Accepted)
		require.True(t, decoded.Applied)
	}
	require.Equal(t, (ActionResult{}).Table(), (ActionResult{}).WideTable())
}
