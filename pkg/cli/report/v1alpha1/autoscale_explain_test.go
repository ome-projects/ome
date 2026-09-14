package v1alpha1

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"

	"sigs.k8s.io/ome/pkg/cli/report"
)

func TestAutoscaleExplainReportExactTableAndWidth(t *testing.T) {
	reportValue := sampleAutoscaleExplainReport()
	var out bytes.Buffer
	require.NoError(t, report.Write(&out, report.FormatTable, reportValue))

	want := "FIELD                ENGINE                    DECODER\n" +
		"STATE                Partial                   Partial\n" +
		"MODE                 Raw                       Native\n" +
		"POLICY               Independent               Independent\n" +
		"DESIRED              HPA/default               KEDA/isvc\n" +
		"RANGE                1..4                      1..8\n" +
		"ZERO                 -                         yes\n" +
		"EXPECTED-TARGET      Deployment/svc-engine     InferenceReplica/svc-decoder\n" +
		"REPORTED             HPA/default/ome           HPA/runtime/ome\n" +
		"REPORTED-TARGET      Deployment/svc-engine     InferenceReplica/svc-decoder\n" +
		"CUR/DES              2/2                       1/5\n" +
		"LAST-SCALE           -                         -\n" +
		"CONDITION-EVIDENCE   reported                  reported\n" +
		"CONDITIONS           -                         -\n" +
		"CHECK                match                     mismatch\n" +
		"WHY                  inheritance-unavailable   class-mismatch,+1\n"
	assert.Equal(t, want, out.String())
	for _, line := range strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n") {
		assert.LessOrEqual(t, len(line), 120, "table line must fit the fixed terminal-safe contract: %q", line)
	}
}

func TestAutoscaleExplainTableKeepsAuthoritativeEmptyStateVisible(t *testing.T) {
	reportValue := AutoscaleExplainReport{Content: AutoscaleExplainContent{
		Summary: AutoscaleExplainSummary{
			State: AutoscaleExplainPartial, StatusFreshness: StatusFreshnessUnobserved,
		},
		ScalingPolicy: AutoscaleScalingPolicy{
			State: AutoscaleScalingPolicyAvailable, Mode: AutoscaleScalingIndependent,
			Source: AutoscaleScalingPolicySourceDefault,
		},
		Components: []AutoscaleExplainComponent{},
		Issues:     []AutoscaleExplainIssue{{Code: AutoscaleExplainIssueStatusUnobserved}},
	}}
	var out bytes.Buffer

	require.NoError(t, report.Write(&out, report.FormatTable, reportValue))

	assert.Equal(t,
		"FIELD    SERVICE\n"+
			"STATE    Partial\n"+
			"POLICY   Independent\n"+
			"WHY      status-unobserved\n",
		out.String(),
	)
}

func TestAutoscaleExplainTableShowsReportedTargetAndReplicaGap(t *testing.T) {
	reportValue := sampleAutoscaleExplainReport()
	component := &reportValue.Content.Components[1]
	current, desired := int32(1), int32(5)
	component.Reported.Target = &AutoscaleTargetIdentity{
		APIVersion: "ome.io/v1beta1", Kind: AutoscaleTargetInferenceReplica,
		Namespace: "workloads", Name: "chat-decoder",
	}
	component.Reported.Replicas = AutoscaleReplicaStatus{
		State: AutoscaleReplicasReported, CurrentReplicas: &current, DesiredReplicas: &desired,
	}
	var out bytes.Buffer

	require.NoError(t, report.Write(&out, report.FormatTable, reportValue))

	assert.Contains(t, out.String(), "InferenceReplica/chat-decoder")
	assert.Contains(t, out.String(), "1/5")
}

func TestAutoscaleExplainTableShowsHealthConditionsAndLastScaleTime(t *testing.T) {
	reportValue := sampleAutoscaleExplainReport()
	lastScale := time.Date(2026, time.September, 7, 18, 5, 0, 0, time.UTC)
	reportValue.Content.Components[0].Reported.Replicas.LastScaleTime = &lastScale
	reportValue.Content.Components[0].Reported.Conditions = AutoscaleConditionsStatus{
		State: AutoscaleConditionsReported,
		Items: []AutoscaleCondition{{
			Type: AutoscaleConditionAbleToScale, Status: AutoscaleConditionFalse,
			LastTransitionTime: time.Date(2026, time.September, 7, 18, 4, 0, 0, time.UTC),
		}},
	}

	table := reportValue.Table()
	rows := map[string][]string{}
	for _, row := range table.Rows {
		rows[row[0]] = row
	}

	assert.Equal(t, []string{"LAST-SCALE", "2026-09-07T18:05:00Z", "-"}, rows["LAST-SCALE"])
	assert.Equal(t, []string{"CONDITION-EVIDENCE", "reported", "reported"}, rows["CONDITION-EVIDENCE"])
	assert.Equal(t, []string{"CONDITIONS", "AbleToScale=False", "-"}, rows["CONDITIONS"])
}

func TestAutoscaleExplainTableDoesNotMisattributeReportedOnlyComponents(t *testing.T) {
	reportValue := sampleAutoscaleExplainReport()
	reportValue.Content.Components = reportValue.Content.Components[:1]
	reportValue.Content.Issues = []AutoscaleExplainIssue{
		{Code: AutoscaleExplainIssueReportedComponentUnexpected, Component: RuntimeComponentDecoder},
		{Code: AutoscaleExplainIssueReportedComponentUnexpected, Component: RuntimeComponentRouter},
	}

	table := reportValue.Table()
	assert.Equal(t, []string{"FIELD", "ENGINE", "SERVICE"}, table.Headers)
	rows := map[string][]string{}
	for _, row := range table.Rows {
		rows[row[0]] = row
	}
	require.Len(t, rows["WHY"], 3)
	assert.Equal(t, "inheritance-unavailable", rows["WHY"][1])
	assert.Equal(t, "decoder:unexpected-component,router:unexpected-component", rows["WHY"][2])
}

func TestAutoscaleExplainReportBoundsEveryPhysicalTerminalLine(t *testing.T) {
	reportValue := sampleAutoscaleExplainReport()
	reportValue.Content.ScalingPolicy.Mode = AutoscaleScalingProportional
	component := &reportValue.Content.Components[1]
	max := int32(2147483647)
	component.Desired.Bounds.MaxReplicas = &max
	component.Desired.SpecSource = AutoscaleSpecSourceRuntime
	component.Reported.Class = AutoscaleClassExternal
	component.Reported.ManagedBy = AutoscaleManagedByExternal
	component.Reconciliation.Issues = []AutoscaleExplainIssueCode{
		AutoscaleExplainIssueInheritanceUnavailable,
		AutoscaleExplainIssueReportedClassMismatch,
		AutoscaleExplainIssueReportedOwnershipMismatch,
		AutoscaleExplainIssueReportedSpecSourceMismatch,
	}
	w := &autoscaleExplainTerminalBuffer{width: 120}

	require.NoError(t, report.Write(w, report.FormatTable, reportValue))

	for _, line := range strings.Split(strings.TrimSuffix(w.String(), "\n"), "\n") {
		assert.LessOrEqual(t, len(line), 120, "terminal line exceeds advertised width: %q", line)
	}
}

func TestAutoscaleExplainTablePrioritizesActionableInvalidIssue(t *testing.T) {
	reportValue := sampleAutoscaleExplainReport()
	reportValue.Content.Summary.State = AutoscaleExplainInvalid
	reportValue.Content.Components = reportValue.Content.Components[:1]
	reportValue.Content.Issues = []AutoscaleExplainIssue{}
	component := &reportValue.Content.Components[0]
	component.Desired.State = AutoscaleDesiredInvalid
	component.Reconciliation.State = AutoscaleReconciliationInvalid
	component.Reconciliation.Issues = []AutoscaleExplainIssueCode{
		AutoscaleExplainIssueInheritanceUnavailable,
		AutoscaleExplainIssueKEDATriggersRequired,
		AutoscaleExplainIssueStatusStale,
	}
	var out bytes.Buffer

	require.NoError(t, report.Write(&out, report.FormatTable, reportValue))

	assert.Contains(t, out.String(), "keda-triggers,+2")
	assert.NotContains(t, out.String(), "inheritance-unavailable,+2")
}

func TestAutoscaleExplainKEDAConfigurationIssueHasExactSafeFixtures(t *testing.T) {
	issue := AutoscaleExplainIssue{
		Code: AutoscaleExplainIssueCode("KEDAConfigurationInvalid"), Component: RuntimeComponentEngine,
	}
	encoded, err := json.Marshal(issue)
	require.NoError(t, err)
	assert.Equal(t, `{"code":"KEDAConfigurationInvalid","component":"engine"}`, string(encoded))

	reportValue := sampleAutoscaleExplainReport()
	reportValue.Content.Components = reportValue.Content.Components[:1]
	reportValue.Content.Components[0].Reconciliation.Issues = []AutoscaleExplainIssueCode{issue.Code}
	var out bytes.Buffer
	require.NoError(t, report.Write(&out, report.FormatTable, reportValue))
	assert.Contains(t, out.String(), "WHY                  keda-config-invalid")
}

func TestAutoscaleExplainReportCanonicalIsDeterministicAndNonMutating(t *testing.T) {
	reportValue := sampleAutoscaleExplainReport()
	reportValue.Content.Components[0], reportValue.Content.Components[1] = reportValue.Content.Components[1], reportValue.Content.Components[0]
	reportValue.Content.Issues = []AutoscaleExplainIssue{
		{Code: AutoscaleExplainIssueReportedTargetMismatch, Component: RuntimeComponentDecoder},
		{Code: AutoscaleExplainIssueInheritanceUnavailable},
		{Code: AutoscaleExplainIssueReportedClassMismatch, Component: RuntimeComponentDecoder},
		{Code: AutoscaleExplainIssueReportedClassMismatch, Component: RuntimeComponentDecoder},
	}
	reportValue.Warnings = []AutoscaleExplainWarning{
		{Code: AutoscaleExplainWarningPartialData},
		{Code: AutoscaleExplainWarningPartialData},
	}
	originalFirst := reportValue.Content.Components[0].Type

	canonical := reportValue.Canonical()
	require.Len(t, canonical.Content.Components, 2)
	assert.Equal(t, RuntimeComponentEngine, canonical.Content.Components[0].Type)
	assert.Equal(t, RuntimeComponentDecoder, canonical.Content.Components[1].Type)
	assert.Equal(t, originalFirst, reportValue.Content.Components[0].Type)
	assert.Len(t, canonical.Content.Issues, 3)
	assert.Len(t, canonical.Warnings, 1)
	assert.NotSame(t, reportValue.Content.Components[0].Desired.Target, canonical.Content.Components[1].Desired.Target)
	assert.NotSame(t, reportValue.Content.Components[1].Desired.MetricCount, canonical.Content.Components[0].Desired.MetricCount)
	assert.NotSame(t, reportValue.Content.Components[1].Desired.TriggerCount, canonical.Content.Components[0].Desired.TriggerCount)
	assert.Equal(t, APIVersion, canonical.APIVersion)
	assert.Equal(t, AutoscaleExplainReportKind, canonical.Kind)
}

func TestAutoscaleExplainCanonicalDeterministicallyOrdersDuplicateComponentTypes(t *testing.T) {
	reportValue := sampleAutoscaleExplainReport()
	first := reportValue.Content.Components[0]
	second := reportValue.Content.Components[1]
	second.Type = first.Type

	forward := AutoscaleExplainContent{Components: []AutoscaleExplainComponent{first, second}}.Canonical()
	reverse := AutoscaleExplainContent{Components: []AutoscaleExplainComponent{second, first}}.Canonical()

	assert.Equal(t, forward.Components, reverse.Components)
}

func TestAutoscaleExplainReportedTargetPointerContract(t *testing.T) {
	for _, tt := range []struct {
		state       AutoscaleReportedState
		targetState AutoscaleTargetState
	}{
		{state: AutoscaleReportedNotReported, targetState: AutoscaleTargetNotReported},
		{state: AutoscaleReportedUnavailable, targetState: AutoscaleTargetUnavailable},
		{state: AutoscaleReportedInvalid, targetState: AutoscaleTargetInvalid},
	} {
		t.Run(string(tt.state), func(t *testing.T) {
			component := AutoscaleExplainComponent{
				Type: RuntimeComponentEngine,
				Reported: AutoscaleReportedConfiguration{
					State: tt.state, Class: AutoscaleClassUnknown, ManagedBy: AutoscaleManagedByUnknown,
					SpecSource: AutoscaleSpecSourceUnknown, TargetState: tt.targetState,
					Target: &AutoscaleTargetIdentity{
						APIVersion: "secret/v1", Kind: AutoscaleTargetDeployment,
						Namespace: "secret-namespace", Name: "secret-target",
					},
					Replicas:   AutoscaleReplicaStatus{State: AutoscaleReplicasUnavailable},
					Conditions: AutoscaleConditionsStatus{State: AutoscaleConditionsUnavailable, Items: []AutoscaleCondition{}},
				},
			}
			canonical := (AutoscaleExplainContent{Components: []AutoscaleExplainComponent{component}}).Canonical()
			require.Len(t, canonical.Components, 1)
			assert.Nil(t, canonical.Components[0].Reported.Target)
			encoded, err := json.Marshal(canonical.Components[0])
			require.NoError(t, err)
			assert.NotContains(t, string(encoded), `"target":`)
			assert.Contains(t, string(encoded), `"targetState":"`+string(tt.targetState)+`"`)
			assert.NotContains(t, string(encoded), "secret-target")
		})
	}

	reported := AutoscaleReportedConfiguration{
		State: AutoscaleReportedAvailable, Class: AutoscaleClassHPA, ManagedBy: AutoscaleManagedByOME,
		SpecSource: AutoscaleSpecSourceDefault, TargetState: AutoscaleTargetReported,
		Target:     &AutoscaleTargetIdentity{APIVersion: "apps/v1", Kind: AutoscaleTargetDeployment, Namespace: "workloads", Name: "svc-engine"},
		Replicas:   AutoscaleReplicaStatus{State: AutoscaleReplicasReported},
		Conditions: AutoscaleConditionsStatus{State: AutoscaleConditionsReported, Items: []AutoscaleCondition{}},
	}
	encoded, err := json.Marshal(reported)
	require.NoError(t, err)
	assert.Contains(t, string(encoded), `"target":{"apiVersion":"apps/v1","kind":"Deployment","namespace":"workloads","name":"svc-engine"}`)
}

func TestAutoscaleExplainDesiredTargetPointerContract(t *testing.T) {
	for _, state := range []AutoscaleDesiredState{
		AutoscaleDesiredUnavailable,
		AutoscaleDesiredUnsupported,
		AutoscaleDesiredInvalid,
	} {
		t.Run(string(state), func(t *testing.T) {
			component := AutoscaleExplainComponent{
				Type: RuntimeComponentEngine,
				Desired: AutoscaleDesiredConfiguration{
					State: state, Class: AutoscaleClassUnknown, ManagedBy: AutoscaleManagedByUnknown,
					SpecSource: AutoscaleSpecSourceUnknown,
					Target: &AutoscaleTargetIdentity{
						APIVersion: "secret/v1", Kind: AutoscaleTargetDeployment,
						Namespace: "secret-namespace", Name: "secret-target",
					},
				},
			}
			canonical := (AutoscaleExplainContent{Components: []AutoscaleExplainComponent{component}}).Canonical()
			require.Len(t, canonical.Components, 1)
			assert.Nil(t, canonical.Components[0].Desired.Target)
			encoded, err := json.Marshal(canonical.Components[0])
			require.NoError(t, err)
			assert.NotContains(t, string(encoded), `"target":`)
			assert.NotContains(t, string(encoded), "secret-target")
		})
	}

	target := &AutoscaleTargetIdentity{
		APIVersion: "apps/v1", Kind: AutoscaleTargetDeployment, Namespace: "workloads", Name: "svc-engine",
	}
	canonical := (AutoscaleExplainContent{Components: []AutoscaleExplainComponent{{
		Type: RuntimeComponentEngine,
		Desired: AutoscaleDesiredConfiguration{
			State: AutoscaleDesiredAvailable, Class: AutoscaleClassHPA, ManagedBy: AutoscaleManagedByOME,
			SpecSource: AutoscaleSpecSourceDefault, Target: target,
		},
	}}}).Canonical()
	require.NotNil(t, canonical.Components[0].Desired.Target)
	assert.Equal(t, "svc-engine", canonical.Components[0].Desired.Target.Name)
	assert.NotSame(t, target, canonical.Components[0].Desired.Target)
}

func TestAutoscaleExplainCanonicalClearsContradictoryPointerEvidence(t *testing.T) {
	min, max, current, desired := int32(1), int32(4), int32(7), int32(8)
	lastScale := time.Date(2026, time.September, 7, 18, 0, 0, 0, time.UTC)
	target := &AutoscaleTargetIdentity{
		APIVersion: "apps/v1", Kind: AutoscaleTargetDeployment,
		Namespace: "workloads", Name: "svc-engine",
	}
	component := AutoscaleExplainComponent{
		Desired: AutoscaleDesiredConfiguration{
			State: AutoscaleDesiredUnavailable,
			Bounds: AutoscaleExplainBounds{
				State: AutoscaleBoundsUnavailable, MinReplicas: &min, MaxReplicas: &max,
			},
		},
		Reported: AutoscaleReportedConfiguration{
			State: AutoscaleReportedNotReported, TargetState: AutoscaleTargetReported, Target: target,
			Replicas: AutoscaleReplicaStatus{
				State: AutoscaleReplicasUnavailable, CurrentReplicas: &current,
				DesiredReplicas: &desired, LastScaleTime: &lastScale,
			},
			Conditions: AutoscaleConditionsStatus{
				State: AutoscaleConditionsInvalid,
				Items: []AutoscaleCondition{{Type: AutoscaleConditionReady, Status: AutoscaleConditionTrue}},
			},
		},
	}

	canonical := (AutoscaleExplainContent{Components: []AutoscaleExplainComponent{component}}).Canonical().Components[0]

	assert.Nil(t, canonical.Desired.Bounds.MinReplicas)
	assert.Nil(t, canonical.Desired.Bounds.MaxReplicas)
	require.NotNil(t, canonical.Reported.Target, "target evidence is independent of the missing autoscaler block")
	assert.Equal(t, "svc-engine", canonical.Reported.Target.Name)
	assert.Nil(t, canonical.Reported.Replicas.CurrentReplicas)
	assert.Nil(t, canonical.Reported.Replicas.DesiredReplicas)
	assert.Nil(t, canonical.Reported.Replicas.LastScaleTime)
	assert.Empty(t, canonical.Reported.Conditions.Items)
}

func TestAutoscaleExplainCanonicalRetainsValidConditionSubsetWhenUnavailable(t *testing.T) {
	condition := AutoscaleCondition{Type: AutoscaleConditionReady, Status: AutoscaleConditionTrue}
	component := AutoscaleExplainComponent{Reported: AutoscaleReportedConfiguration{
		Conditions: AutoscaleConditionsStatus{State: AutoscaleConditionsUnavailable, Items: []AutoscaleCondition{condition}},
	}}

	canonical := (AutoscaleExplainContent{Components: []AutoscaleExplainComponent{component}}).Canonical().Components[0]

	assert.Equal(t, []AutoscaleCondition{condition}, canonical.Reported.Conditions.Items)
}

func TestAutoscaleExplainReportedStateExactFixtures(t *testing.T) {
	tests := []struct {
		name           string
		reported       AutoscaleReportedConfiguration
		summary        AutoscaleExplainState
		reconciliation AutoscaleReconciliationState
		issue          AutoscaleExplainIssueCode
		wantJSON       string
		wantYAML       string
		wantTable      string
	}{
		{
			name: "NotReported",
			reported: AutoscaleReportedConfiguration{
				State: AutoscaleReportedNotReported, Class: AutoscaleClassUnknown,
				ManagedBy: AutoscaleManagedByUnknown, SpecSource: AutoscaleSpecSourceUnknown,
				TargetState: AutoscaleTargetNotReported,
				Replicas:    AutoscaleReplicaStatus{State: AutoscaleReplicasNotReported},
				Conditions:  AutoscaleConditionsStatus{State: AutoscaleConditionsNotReported, Items: []AutoscaleCondition{}},
			},
			summary: AutoscaleExplainPartial, reconciliation: AutoscaleReconciliationNotReported,
			issue:    AutoscaleExplainIssueStatusNotReported,
			wantJSON: `{"state":"NotReported","class":"Unknown","managedBy":"Unknown","specSource":"Unknown","targetState":"NotReported","replicas":{"state":"NotReported"},"conditions":{"state":"NotReported","items":[]}}`,
			wantYAML: "class: Unknown\nconditions:\n  items: []\n  state: NotReported\nmanagedBy: Unknown\nreplicas:\n  state: NotReported\nspecSource: Unknown\nstate: NotReported\ntargetState: NotReported\n",
			wantTable: "FIELD                ENGINE\n" +
				"STATE                Partial\n" +
				"MODE                 Raw\n" +
				"POLICY               Independent\n" +
				"DESIRED              HPA/default\n" +
				"RANGE                1..4\n" +
				"ZERO                 -\n" +
				"EXPECTED-TARGET      Deployment/svc-engine\n" +
				"REPORTED             -\n" +
				"REPORTED-TARGET      -\n" +
				"CUR/DES              -\n" +
				"LAST-SCALE           -\n" +
				"CONDITION-EVIDENCE   missing\n" +
				"CONDITIONS           -\n" +
				"CHECK                missing\n" +
				"WHY                  status-missing\n",
		},
		{
			name: "Unavailable",
			reported: AutoscaleReportedConfiguration{
				State: AutoscaleReportedUnavailable, Class: AutoscaleClassUnknown,
				ManagedBy: AutoscaleManagedByUnknown, SpecSource: AutoscaleSpecSourceUnknown,
				TargetState: AutoscaleTargetUnavailable,
				Replicas:    AutoscaleReplicaStatus{State: AutoscaleReplicasUnavailable},
				Conditions:  AutoscaleConditionsStatus{State: AutoscaleConditionsUnavailable, Items: []AutoscaleCondition{}},
			},
			summary: AutoscaleExplainPartial, reconciliation: AutoscaleReconciliationUnavailable,
			issue:    AutoscaleExplainIssueStatusStale,
			wantJSON: `{"state":"Unavailable","class":"Unknown","managedBy":"Unknown","specSource":"Unknown","targetState":"Unavailable","replicas":{"state":"Unavailable"},"conditions":{"state":"Unavailable","items":[]}}`,
			wantYAML: "class: Unknown\nconditions:\n  items: []\n  state: Unavailable\nmanagedBy: Unknown\nreplicas:\n  state: Unavailable\nspecSource: Unknown\nstate: Unavailable\ntargetState: Unavailable\n",
			wantTable: "FIELD                ENGINE\n" +
				"STATE                Partial\n" +
				"MODE                 Raw\n" +
				"POLICY               Independent\n" +
				"DESIRED              HPA/default\n" +
				"RANGE                1..4\n" +
				"ZERO                 -\n" +
				"EXPECTED-TARGET      Deployment/svc-engine\n" +
				"REPORTED             ?\n" +
				"REPORTED-TARGET      ?\n" +
				"CUR/DES              ?\n" +
				"LAST-SCALE           ?\n" +
				"CONDITION-EVIDENCE   unknown\n" +
				"CONDITIONS           ?\n" +
				"CHECK                unknown\n" +
				"WHY                  status-stale\n",
		},
		{
			name: "Invalid",
			reported: AutoscaleReportedConfiguration{
				State: AutoscaleReportedInvalid, Class: AutoscaleClassUnknown,
				ManagedBy: AutoscaleManagedByUnknown, SpecSource: AutoscaleSpecSourceUnknown,
				TargetState: AutoscaleTargetInvalid,
				Replicas:    AutoscaleReplicaStatus{State: AutoscaleReplicasInvalid},
				Conditions:  AutoscaleConditionsStatus{State: AutoscaleConditionsInvalid, Items: []AutoscaleCondition{}},
			},
			summary: AutoscaleExplainInvalid, reconciliation: AutoscaleReconciliationInvalid,
			issue:    AutoscaleExplainIssueStatusInvalid,
			wantJSON: `{"state":"Invalid","class":"Unknown","managedBy":"Unknown","specSource":"Unknown","targetState":"Invalid","replicas":{"state":"Invalid"},"conditions":{"state":"Invalid","items":[]}}`,
			wantYAML: "class: Unknown\nconditions:\n  items: []\n  state: Invalid\nmanagedBy: Unknown\nreplicas:\n  state: Invalid\nspecSource: Unknown\nstate: Invalid\ntargetState: Invalid\n",
			wantTable: "FIELD                ENGINE\n" +
				"STATE                Invalid\n" +
				"MODE                 Raw\n" +
				"POLICY               Independent\n" +
				"DESIRED              HPA/default\n" +
				"RANGE                1..4\n" +
				"ZERO                 -\n" +
				"EXPECTED-TARGET      Deployment/svc-engine\n" +
				"REPORTED             invalid\n" +
				"REPORTED-TARGET      invalid\n" +
				"CUR/DES              invalid\n" +
				"LAST-SCALE           invalid\n" +
				"CONDITION-EVIDENCE   invalid\n" +
				"CONDITIONS           invalid\n" +
				"CHECK                invalid\n" +
				"WHY                  status-invalid\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encodedJSON, err := json.Marshal(tt.reported)
			require.NoError(t, err)
			assert.Equal(t, tt.wantJSON, string(encodedJSON))
			encodedYAML, err := yaml.Marshal(tt.reported)
			require.NoError(t, err)
			assert.Equal(t, tt.wantYAML, string(encodedYAML))

			reportValue := sampleAutoscaleExplainReport()
			reportValue.Content.Summary.State = tt.summary
			reportValue.Content.Components = reportValue.Content.Components[:1]
			reportValue.Content.Components[0].Reported = tt.reported
			reportValue.Content.Components[0].Reconciliation = AutoscaleReconciliation{
				State: tt.reconciliation, Issues: []AutoscaleExplainIssueCode{tt.issue},
			}
			reportValue.Content.Issues = []AutoscaleExplainIssue{{Code: tt.issue, Component: RuntimeComponentEngine}}
			var table bytes.Buffer
			require.NoError(t, report.Write(&table, report.FormatTable, reportValue))
			assert.Equal(t, tt.wantTable, table.String())
		})
	}
}

func sampleAutoscaleExplainReport() AutoscaleExplainReport {
	collected := time.Date(2026, time.September, 7, 12, 0, 0, 0, time.FixedZone("offset", -7*60*60))
	minOne, maxFour, maxEight := int32(1), int32(4), int32(8)
	currentTwo, desiredTwo, currentOne, desiredFive := int32(2), int32(2), int32(1), int32(5)
	countZero, countOne := 0, 1
	content := AutoscaleExplainContent{
		Summary: AutoscaleExplainSummary{State: AutoscaleExplainPartial, StatusFreshness: StatusFreshnessCurrent},
		ActiveConfiguration: AutoscaleActiveConfiguration{
			State:  AutoscaleActiveConfigurationAvailable,
			Origin: ConfigurationOriginLiveRuntime, Consistency: RevisionConsistencyConsistent,
			Runtime:     &RuntimeObjectReference{APIVersion: "ome.io/v1beta1", Kind: RuntimeKindServingRuntime, Namespace: "workloads", Name: "runtime"},
			Inheritance: &RuntimeInheritance{State: InheritanceStateUnavailable, Sources: []RuntimeObjectReference{}, UnavailableReason: UnavailableUnreadable},
		},
		ScalingPolicy: AutoscaleScalingPolicy{State: AutoscaleScalingPolicyAvailable, Mode: "Independent", Source: AutoscaleScalingPolicySourceDefault},
		Components: []AutoscaleExplainComponent{
			{
				Type: RuntimeComponentEngine, DeploymentMode: DeploymentModeRawDeployment,
				Desired: AutoscaleDesiredConfiguration{
					State: AutoscaleDesiredAvailable, Class: AutoscaleClassHPA, ManagedBy: AutoscaleManagedByOME,
					SpecSource:  AutoscaleSpecSourceDefault,
					Target:      &AutoscaleTargetIdentity{APIVersion: "apps/v1", Kind: AutoscaleTargetDeployment, Namespace: "workloads", Name: "svc-engine"},
					Bounds:      AutoscaleExplainBounds{State: AutoscaleBoundsAvailable, MinReplicas: &minOne, MaxReplicas: &maxFour},
					ScaleToZero: AutoscaleScaleToZeroNotRequested, MetricCount: &countOne, TriggerCount: &countZero,
				},
				Reported: AutoscaleReportedConfiguration{
					State: AutoscaleReportedAvailable, Class: AutoscaleClassHPA, ManagedBy: AutoscaleManagedByOME,
					SpecSource: AutoscaleSpecSourceDefault, TargetState: AutoscaleTargetReported,
					Target: &AutoscaleTargetIdentity{APIVersion: "apps/v1", Kind: AutoscaleTargetDeployment, Namespace: "workloads", Name: "svc-engine"},
					Replicas: AutoscaleReplicaStatus{
						State: AutoscaleReplicasReported, CurrentReplicas: &currentTwo, DesiredReplicas: &desiredTwo,
					},
					Conditions: AutoscaleConditionsStatus{State: AutoscaleConditionsReported, Items: []AutoscaleCondition{}},
				},
				Reconciliation: AutoscaleReconciliation{State: AutoscaleReconciliationConsistent, Issues: []AutoscaleExplainIssueCode{AutoscaleExplainIssueInheritanceUnavailable}},
			},
			{
				Type: RuntimeComponentDecoder, DeploymentMode: DeploymentModeOMENative,
				Desired: AutoscaleDesiredConfiguration{
					State: AutoscaleDesiredAvailable, Class: AutoscaleClassKEDA, ManagedBy: AutoscaleManagedByOME,
					SpecSource:  AutoscaleSpecSourceISVC,
					Target:      &AutoscaleTargetIdentity{APIVersion: "ome.io/v1beta1", Kind: AutoscaleTargetInferenceReplica, Namespace: "workloads", Name: "svc-decoder"},
					Bounds:      AutoscaleExplainBounds{State: AutoscaleBoundsAvailable, MinReplicas: &minOne, MaxReplicas: &maxEight},
					ScaleToZero: AutoscaleScaleToZeroEligible, MetricCount: &countZero, TriggerCount: &countOne,
				},
				Reported: AutoscaleReportedConfiguration{
					State: AutoscaleReportedAvailable, Class: AutoscaleClassHPA, ManagedBy: AutoscaleManagedByOME,
					SpecSource: AutoscaleSpecSourceRuntime, TargetState: AutoscaleTargetReported,
					Target: &AutoscaleTargetIdentity{APIVersion: "ome.io/v1beta1", Kind: AutoscaleTargetInferenceReplica, Namespace: "workloads", Name: "svc-decoder"},
					Replicas: AutoscaleReplicaStatus{
						State: AutoscaleReplicasReported, CurrentReplicas: &currentOne, DesiredReplicas: &desiredFive,
					},
					Conditions: AutoscaleConditionsStatus{State: AutoscaleConditionsReported, Items: []AutoscaleCondition{}},
				},
				Reconciliation: AutoscaleReconciliation{State: AutoscaleReconciliationReportedMismatch, Issues: []AutoscaleExplainIssueCode{AutoscaleExplainIssueReportedClassMismatch, AutoscaleExplainIssueReportedSpecSourceMismatch}},
			},
		},
		Issues: []AutoscaleExplainIssue{{Code: AutoscaleExplainIssueInheritanceUnavailable}, {Code: AutoscaleExplainIssueReportedClassMismatch, Component: RuntimeComponentDecoder}, {Code: AutoscaleExplainIssueReportedSpecSourceMismatch, Component: RuntimeComponentDecoder}},
	}
	reportValue := NewAutoscaleExplainReport(Metadata{Namespace: "workloads", Name: "svc"}, content, ClockFunc(func() time.Time { return collected }))
	reportValue.Sources = []RuntimeSourceReference{{
		Kind: "InferenceService", Namespace: "workloads", Name: "svc", UID: "isvc-uid", Generation: 4,
		Evidence: EvidenceObserved, CollectedAt: reportValue.CollectedAt,
	}}
	reportValue.Warnings = []AutoscaleExplainWarning{{Code: AutoscaleExplainWarningPartialData}}
	return reportValue
}

type autoscaleExplainTerminalBuffer struct {
	bytes.Buffer
	width int
}

func (w *autoscaleExplainTerminalBuffer) TerminalWidth() (int, bool) {
	return w.width, true
}
