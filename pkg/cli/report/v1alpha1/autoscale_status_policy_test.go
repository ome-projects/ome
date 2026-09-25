package v1alpha1_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/cli/report"
	"sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func TestAutoscalePolicyEnumsAreClosedAndStable(t *testing.T) {
	states := []string{
		string(v1alpha1.AutoscalePolicyCurrent),
		string(v1alpha1.AutoscalePolicyHeld),
		string(v1alpha1.AutoscalePolicyShadowed),
		string(v1alpha1.AutoscalePolicyUnresolved),
		string(v1alpha1.AutoscalePolicyUnsupported),
	}
	assert.Equal(t, []string{"Current", "Held", "Shadowed", "Unresolved", "Unsupported"}, states)

	resolutions := []string{
		string(v1alpha1.AutoscalePolicyRenderedFromPolicy),
		string(v1alpha1.AutoscalePolicyInlinePrecedence),
		string(v1alpha1.AutoscalePolicyNotFound),
		string(v1alpha1.AutoscalePolicyInvalid),
		string(v1alpha1.AutoscalePolicyAuthNotFound),
		string(v1alpha1.AutoscalePolicyClassUnavailable),
		string(v1alpha1.AutoscalePolicyUnsupportedMode),
	}
	assert.Equal(t, []string{
		"RenderedFromPolicy", "InlinePrecedence", "PolicyNotFound", "PolicyInvalid",
		"AuthNotFound", "ClassUnavailable", "UnsupportedDeploymentMode",
	}, resolutions)
}

func TestAutoscalePolicyCanonicalizesAndDeepCopies(t *testing.T) {
	policy := &v1alpha1.AutoscalePolicyStatus{
		State:          v1alpha1.AutoscalePolicyCurrent,
		Resolution:     v1alpha1.AutoscalePolicyRenderedFromPolicy,
		Name:           "request-activity-v1",
		Generation:     17,
		PortableDigest: "pv1:0123456789ab",
		RenderDigest:   "rv1:abcdef012345",
	}
	componentA := v1alpha1.AutoscaleComponentStatus{
		Type: v1alpha1.RuntimeComponentEngine, Policy: policy,
	}
	componentB := componentA
	componentB.Policy = &v1alpha1.AutoscalePolicyStatus{
		State: v1alpha1.AutoscalePolicyHeld, Resolution: v1alpha1.AutoscalePolicyNotFound,
		Name: "request-activity-v1", Generation: 16,
		PortableDigest: "pv1:1123456789ab", RenderDigest: "rv1:bbcdef012345",
	}

	left := v1alpha1.AutoscaleStatusContent{
		Components: []v1alpha1.AutoscaleComponentStatus{componentA, componentB},
	}.Canonical()
	right := v1alpha1.AutoscaleStatusContent{
		Components: []v1alpha1.AutoscaleComponentStatus{componentB, componentA},
	}.Canonical()
	require.Equal(t, left, right)
	require.Equal(t, v1alpha1.AutoscalePolicyCurrent, left.Components[0].Policy.State)
	require.NotSame(t, policy, left.Components[0].Policy)

	left.Components[0].Policy.Name = "mutated"
	assert.Equal(t, "request-activity-v1", policy.Name)
}

func TestAutoscalePolicyTablesSeparateCompactIdentityFromWideEvidence(t *testing.T) {
	reportValue := autoscaleStatusFixture()
	reportValue.Content.Components[0].SpecSource = v1alpha1.AutoscaleSpecSourcePolicy
	reportValue.Content.Components[0].Policy = &v1alpha1.AutoscalePolicyStatus{
		State:          v1alpha1.AutoscalePolicyHeld,
		Resolution:     v1alpha1.AutoscalePolicyClassUnavailable,
		Name:           "request-activity-policy-with-a-long-name",
		Generation:     17,
		PortableDigest: "pv1:0123456789ab",
		RenderDigest:   "rv1:abcdef012345",
	}

	compact := reportValue.Table()
	rows := make(map[string][]string)
	for _, row := range compact.Rows {
		rows[row[0]] = row
	}
	assert.Equal(t, "Held", rows["POLICY-STATE"][2])
	assert.Equal(t, "reque...-name", rows["POLICY"][2])
	for _, forbidden := range []string{"POLICY-RESOLUTION", "POLICY-GENERATION", "POLICY-PORTABLE", "POLICY-RENDER"} {
		assert.NotContains(t, rows, forbidden)
	}
	var compactOutput bytes.Buffer
	require.NoError(t, report.Write(&compactOutput, report.FormatTable, reportValue))
	for _, line := range strings.Split(strings.TrimSuffix(compactOutput.String(), "\n"), "\n") {
		assert.LessOrEqual(t, len([]rune(line)), 80, "line %q", line)
	}

	wide := reportValue.WideTable()
	assert.Equal(t, []string{
		"STATE", "COMPONENT", "COMPONENT-STATE", "CLASS", "MANAGED-BY", "SPEC-SOURCE",
		"POLICY-STATE", "POLICY", "POLICY-RESOLUTION", "POLICY-GENERATION",
		"POLICY-PORTABLE", "POLICY-RENDER", "TARGET", "TARGET-EVIDENCE", "CURRENT",
		"DESIRED", "REPLICA-EVIDENCE", "LAST-SCALE", "CONDITION-EVIDENCE", "CONDITIONS", "ISSUES",
	}, wide.Headers)
	require.Len(t, wide.Rows, 1)
	assert.Equal(t, []string{
		"Held", "request-activity-policy-with-a-long-name", "ClassUnavailable", "17",
		"pv1:0123456789ab", "rv1:abcdef012345",
	}, wide.Rows[0][6:12])
}

func TestAutoscalePolicyMachineOutputIsTypedAndCanonical(t *testing.T) {
	reportValue := autoscaleStatusFixture()
	reportValue.Content.Components[0].SpecSource = v1alpha1.AutoscaleSpecSourceISVC
	reportValue.Content.Components[0].Policy = &v1alpha1.AutoscalePolicyStatus{
		State:          v1alpha1.AutoscalePolicyShadowed,
		Resolution:     v1alpha1.AutoscalePolicyInlinePrecedence,
		Name:           "request-activity-v1",
		PortableDigest: "pv1:0123456789ab",
		RenderDigest:   "rv1:abcdef012345",
	}

	var jsonOutput, yamlOutput bytes.Buffer
	require.NoError(t, report.Write(&jsonOutput, report.FormatJSON, reportValue))
	require.NoError(t, report.Write(&yamlOutput, report.FormatYAML, reportValue))
	assert.Contains(t, jsonOutput.String(), `"policy": {`)
	assert.Contains(t, jsonOutput.String(), `"state": "Shadowed"`)
	assert.Contains(t, jsonOutput.String(), `"resolution": "InlinePrecedence"`)
	assert.Contains(t, jsonOutput.String(), `"portableDigest": "pv1:0123456789ab"`)
	assert.Contains(t, jsonOutput.String(), `"renderDigest": "rv1:abcdef012345"`)
	assert.Contains(t, yamlOutput.String(), "policy:\n")
	assert.Contains(t, yamlOutput.String(), "resolution: InlinePrecedence\n")
	assert.Contains(t, yamlOutput.String(), "state: Shadowed\n")

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(jsonOutput.Bytes(), &decoded))
	content := decoded["content"].(map[string]any)
	components := content["components"].([]any)
	policy := components[0].(map[string]any)["policy"].(map[string]any)
	assert.NotContains(t, policy, "generation")
	assert.Equal(t, map[string]any{
		"state": "Shadowed", "resolution": "InlinePrecedence", "name": "request-activity-v1",
		"portableDigest": "pv1:0123456789ab", "renderDigest": "rv1:abcdef012345",
	}, policy)
}
