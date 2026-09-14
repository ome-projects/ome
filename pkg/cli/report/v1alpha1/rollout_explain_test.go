package v1alpha1_test

import (
	"bytes"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/cli/report"
	v1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func TestRolloutExplainReportCanonicalizesWithoutMutatingAndBuildsNarrowTable(t *testing.T) {
	group := 0
	content := v1alpha1.RolloutExplainContent{
		Summary: v1alpha1.RolloutExplainSummary{
			EffectivePlan: v1alpha1.RolloutPlanSelection{
				Mode: v1alpha1.RolloutPlanModePinned, Evidence: v1alpha1.EvidenceReported,
			},
			PlanReady: v1alpha1.RolloutPlanCondition{
				State: v1alpha1.RolloutConditionTrue, Reason: v1alpha1.RolloutPlanReasonPinned,
				Evidence: v1alpha1.EvidenceReported,
			},
			PlanDrift: v1alpha1.RolloutPlanCondition{
				State: v1alpha1.RolloutConditionTrue, Reason: v1alpha1.RolloutPlanReasonSpecNewer,
				Evidence: v1alpha1.EvidenceReported,
			},
		},
		DeclaredGroups: []v1alpha1.RolloutPlanGroup{{
			Index: 0, Evidence: v1alpha1.EvidenceDeclared, Source: v1alpha1.RolloutPlanSourceInline,
			Strategy:   v1alpha1.RolloutStrategyCanary,
			Components: []v1alpha1.RuntimeComponentType{v1alpha1.RuntimeComponentEngine},
			Steps: []v1alpha1.RolloutPlanStep{{
				Index: 0, Capacity: "25%", Traffic: 10, Gate: v1alpha1.RolloutGateManual,
			}},
		}},
		LiveGroups: []v1alpha1.RolloutPlanGroup{{
			Index: 0, Evidence: v1alpha1.EvidenceDeclared, Source: v1alpha1.RolloutPlanSourceInline,
			Strategy:   v1alpha1.RolloutStrategyCanary,
			Components: []v1alpha1.RuntimeComponentType{v1alpha1.RuntimeComponentEngine},
			Steps: []v1alpha1.RolloutPlanStep{{
				Index: 0, Capacity: "25%", Traffic: 10, Gate: v1alpha1.RolloutGateManual,
			}},
		}},
		EffectiveGroups: []v1alpha1.RolloutPlanGroup{{
			Index: 0, Evidence: v1alpha1.EvidenceReported, Source: v1alpha1.RolloutPlanSourcePolicy,
			Strategy:   v1alpha1.RolloutStrategyCanary,
			Components: []v1alpha1.RuntimeComponentType{v1alpha1.RuntimeComponentEngine},
			Policy: &v1alpha1.RolloutPolicyReference{
				Kind: "RolloutPolicy", Name: "guarded", Progression: "canary",
				Generation: 4, Digest: "rp1:aaaaaaaaaaaa",
				Evidence: v1alpha1.EvidenceReported,
			},
			Steps: []v1alpha1.RolloutPlanStep{{
				Index: 0, Capacity: "50%", Traffic: 20, Gate: v1alpha1.RolloutGateAnalysis,
			}},
		}},
		Observed: v1alpha1.RolloutStatusContent{
			Summary: v1alpha1.RolloutSummary{
				State: v1alpha1.RolloutStateUnknown, ReportedState: v1alpha1.RolloutStateInProgress,
				Evidence: v1alpha1.EvidenceReported, Epoch: v1alpha1.RolloutEpochUnverifiable,
				CoordinationReady: v1alpha1.RolloutConditionNotApplicable,
			},
			Groups: []v1alpha1.RolloutGroupStatus{{
				Index: 0, Strategy: v1alpha1.RolloutStrategyCanary,
				Phase:              v1alpha1.RolloutPhaseCanarying,
				Components:         []v1alpha1.RuntimeComponentType{v1alpha1.RuntimeComponentEngine},
				StableRevisionHash: "aaaaaaaa", TargetRevisionHash: "bbbbbbbb",
				Step: &v1alpha1.RolloutStepStatus{
					Index: 0, Total: 1, Capacity: "50%", TargetTraffic: 20,
					ObservedTraffic: 20, Gate: v1alpha1.RolloutGateAnalysis,
				},
			}},
			Components: []v1alpha1.RolloutComponentStatus{{
				Type: v1alpha1.RuntimeComponentEngine, Strategy: v1alpha1.RolloutStrategyCanary,
				Group: &group, Phase: v1alpha1.RolloutPhaseCanarying,
				Traffic: []v1alpha1.RolloutTrafficTarget{{
					RevisionHash: "bbbbbbbb", Percent: 20, Role: v1alpha1.RolloutRevisionTarget,
				}},
			}},
			Issues: []v1alpha1.RolloutIssue{{Code: v1alpha1.RolloutIssueEpochUnverifiable}},
		},
		Holds: []v1alpha1.RolloutExplainHold{{
			Kind: v1alpha1.RolloutHoldAnalysisGate, Evidence: v1alpha1.EvidenceReported,
			Group: &group,
		}},
		Issues: []v1alpha1.RolloutExplainIssue{},
	}
	before, err := json.Marshal(content)
	require.NoError(t, err)
	reportValue := v1alpha1.NewRolloutExplainReport(
		v1alpha1.Metadata{Namespace: "prod", Name: "chat"}, content,
		fixedClock{now: time.Date(2026, time.August, 31, 18, 30, 0, 0, time.UTC)},
	)

	after, err := json.Marshal(content)
	require.NoError(t, err)
	assert.Equal(t, string(before), string(after))
	assert.Equal(t, v1alpha1.RolloutExplainReportKind, reportValue.Kind)
	assert.Equal(t, report.Table{
		Headers: []string{
			"VIEW", "GROUP", "EVIDENCE", "PLAN-MODE", "SOURCE", "STRATEGY", "COMPONENTS",
			"CONFIG", "STEP", "GATE", "PHASE", "SEQUENCE", "PLAN-READY", "DRIFT", "HOLD",
			"REVISIONS", "TRAFFIC", "ISSUES",
		},
		Rows: [][]string{
			{"Declared", "0", "Declared", "-", "Inline", "Canary", "engine", "-", "1/1 25%/10%", "Manual", "-", "-", "-", "-", "-", "-", "-", "-"},
			{"Live", "0", "Declared", "Live", "Inline", "Canary", "engine", "-", "1/1 25%/10%", "Manual", "-", "-", "-", "-", "-", "-", "-", "-"},
			{"Effective", "0", "Reported", "Pinned", "Policy", "Canary", "engine", "policy=RolloutPolicy/guarded@4,digest=rp1:aaaaaaaaaaaa", "1/1 50%/20%", "Analysis", "Canarying", "-", "True/Pinned", "True/SpecNewerThanRun", "AnalysisGate", "stable=aaaaaaaa,target=bbbbbbbb", "engine:bbbbbbbb=20%", "EpochUnverifiable"},
		},
	}, reportValue.Table())

	var output bytes.Buffer
	require.NoError(t, report.Write(&output, report.FormatJSON, reportValue))
	assert.Contains(t, output.String(), `"kind": "RolloutExplainReport"`)
	assert.Contains(t, output.String(), `"declaredGroups"`)
	assert.Contains(t, output.String(), `"liveGroups"`)
	assert.Contains(t, output.String(), `"effectiveGroups"`)
	assert.Contains(t, output.String(), `"observed"`)
}

func TestRolloutExplainReportCanonicalClosesFreeFormPlanFields(t *testing.T) {
	content := v1alpha1.RolloutExplainContent{
		Summary: v1alpha1.RolloutExplainSummary{
			EffectivePlan: v1alpha1.RolloutPlanSelection{Mode: "SECRET_MODE", Evidence: "SECRET_EVIDENCE"},
			PlanReady:     v1alpha1.RolloutPlanCondition{State: "SECRET_STATE", Reason: "SECRET_REASON"},
		},
		EffectiveGroups: []v1alpha1.RolloutPlanGroup{{
			Index: 0, Evidence: v1alpha1.EvidenceReported, Source: v1alpha1.RolloutPlanSourceInline,
			Strategy: v1alpha1.RolloutStrategyCanary,
			Soak:     &v1alpha1.RolloutSetting{Value: "SECRET_SOAK", Source: v1alpha1.RolloutSettingConfigured},
			RollingUpdate: &v1alpha1.RolloutRollingUpdateSettings{
				MaxSurge: v1alpha1.RolloutSetting{
					Value: "1", Source: v1alpha1.RolloutSettingConfigured, Effect: "SECRET_EFFECT",
				},
			},
			Steps: []v1alpha1.RolloutPlanStep{{
				Index: 0, Capacity: "SECRET_CAPACITY", Traffic: 101, Gate: v1alpha1.RolloutGateAnalysis,
				Pause: "SECRET_PAUSE", Analysis: &v1alpha1.RolloutPlanAnalysis{
					MetricCount: 99, Interval: "SECRET_INTERVAL", InitialDelay: "SECRET_DELAY",
					FailureLimit: -1, OnInconclusive: "SECRET_ON_INCONCLUSIVE",
				},
			}},
		}},
	}
	reportValue := v1alpha1.NewRolloutExplainReport(
		v1alpha1.Metadata{Namespace: "prod", Name: "chat"}, content, fixedClock{},
	)
	encoded, err := json.Marshal(reportValue)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), "SECRET")
	group := reportValue.Content.EffectiveGroups[0]
	assert.Nil(t, group.Soak)
	assert.Equal(t, v1alpha1.RolloutSettingEffectUnknown, group.RollingUpdate.MaxSurge.Effect)
	assert.Equal(t, "", group.Steps[0].Capacity)
	assert.Equal(t, int32(0), group.Steps[0].Traffic)
	require.NotNil(t, group.Steps[0].Analysis)
	assert.Equal(t, "Unknown", group.Steps[0].Analysis.OnInconclusive)
}

func TestRolloutExplainCanonicalDoesNotCoerceUnknownOperationalClaims(t *testing.T) {
	content := v1alpha1.RolloutExplainContent{
		Holds:  []v1alpha1.RolloutExplainHold{{Kind: "SECRET_HOLD"}},
		Issues: []v1alpha1.RolloutExplainIssue{{Code: "SECRET_ISSUE"}},
	}

	canonical := content.Canonical()
	require.Len(t, canonical.Holds, 1)
	assert.Equal(t, v1alpha1.RolloutHoldUnknown, canonical.Holds[0].Kind)
	assert.NotEqual(t, v1alpha1.RolloutHoldPlanParked, canonical.Holds[0].Kind)
	require.Len(t, canonical.Issues, 1)
	assert.Equal(t, v1alpha1.RolloutExplainIssueUnknown, canonical.Issues[0].Code)
	assert.NotEqual(t, v1alpha1.RolloutExplainIssueEffectivePlanMalformed, canonical.Issues[0].Code)
}

func TestRolloutExplainTableMapsCollapsedSequentialObservationToEachDeclaredGroup(t *testing.T) {
	zero := 0
	content := v1alpha1.RolloutExplainContent{
		Summary: v1alpha1.RolloutExplainSummary{
			EffectivePlan: v1alpha1.RolloutPlanSelection{Mode: v1alpha1.RolloutPlanModeLive, Evidence: v1alpha1.EvidenceDeclared},
		},
		EffectiveGroups: []v1alpha1.RolloutPlanGroup{
			{Index: 0, Evidence: v1alpha1.EvidenceDeclared, Source: v1alpha1.RolloutPlanSourceDefaulted, Strategy: v1alpha1.RolloutStrategyBlueGreen, Components: []v1alpha1.RuntimeComponentType{v1alpha1.RuntimeComponentDecoder}},
			{Index: 1, Evidence: v1alpha1.EvidenceDeclared, Source: v1alpha1.RolloutPlanSourceDefaulted, Strategy: v1alpha1.RolloutStrategyBlueGreen, Components: []v1alpha1.RuntimeComponentType{v1alpha1.RuntimeComponentEngine}},
		},
		Observed: v1alpha1.RolloutStatusContent{
			Groups: []v1alpha1.RolloutGroupStatus{{
				Index: 0, Strategy: v1alpha1.RolloutStrategySequential, Phase: v1alpha1.RolloutPhaseWaiting,
				Components:       []v1alpha1.RuntimeComponentType{v1alpha1.RuntimeComponentDecoder, v1alpha1.RuntimeComponentEngine},
				CurrentComponent: v1alpha1.RuntimeComponentEngine, PreviousComponent: v1alpha1.RuntimeComponentDecoder,
			}},
			Components: []v1alpha1.RolloutComponentStatus{
				{Type: v1alpha1.RuntimeComponentDecoder, Group: &zero, Traffic: []v1alpha1.RolloutTrafficTarget{{RevisionHash: "aaaaaaaa", Percent: 100}}},
				{Type: v1alpha1.RuntimeComponentEngine, Group: &zero, Traffic: []v1alpha1.RolloutTrafficTarget{{RevisionHash: "bbbbbbbb", Percent: 100}}},
			},
		},
	}
	table := content.Table()
	require.Len(t, table.Rows, 2)
	sequenceColumn := slices.Index(table.Headers, "SEQUENCE")
	require.NotEqual(t, -1, sequenceColumn)
	assert.Equal(t, "Waiting", table.Rows[1][slices.Index(table.Headers, "PHASE")])
	assert.Equal(t, "current=engine,previous=decoder", table.Rows[1][sequenceColumn])
	assert.Equal(t, "engine:bbbbbbbb=100%", table.Rows[1][slices.Index(table.Headers, "TRAFFIC")])
}

func TestRolloutExplainTableKeepsUngroupedObservedComponentsVisible(t *testing.T) {
	content := v1alpha1.RolloutExplainContent{
		Summary: v1alpha1.RolloutExplainSummary{
			EffectivePlan: v1alpha1.RolloutPlanSelection{Mode: v1alpha1.RolloutPlanModeLive, Evidence: v1alpha1.EvidenceDeclared},
		},
		Observed: v1alpha1.RolloutStatusContent{
			Summary: v1alpha1.RolloutSummary{Evidence: v1alpha1.EvidenceReported},
			Components: []v1alpha1.RolloutComponentStatus{{
				Type: v1alpha1.RuntimeComponentEngine, Strategy: v1alpha1.RolloutStrategyIndependent,
				Phase: v1alpha1.RolloutPhaseStable, RolledOutRevisionHash: "aaaaaaaa",
				Traffic: []v1alpha1.RolloutTrafficTarget{{RevisionHash: "aaaaaaaa", Percent: 100}},
			}},
		},
	}
	table := content.Table()
	require.Len(t, table.Rows, 2)
	assert.Equal(t, "Effective", table.Rows[0][0])
	assert.Equal(t, "Live", table.Rows[0][slices.Index(table.Headers, "PLAN-MODE")])
	assert.Equal(t, "Observed", table.Rows[1][0])
	assert.Equal(t, "Independent", table.Rows[1][slices.Index(table.Headers, "STRATEGY")])
	assert.Equal(t, "engine", table.Rows[1][slices.Index(table.Headers, "COMPONENTS")])
	assert.Equal(t, "Stable", table.Rows[1][slices.Index(table.Headers, "PHASE")])
	assert.Equal(t, "current=aaaaaaaa", table.Rows[1][slices.Index(table.Headers, "REVISIONS")])
	assert.Equal(t, "engine:aaaaaaaa=100%", table.Rows[1][slices.Index(table.Headers, "TRAFFIC")])
}

func TestRolloutExplainTableAlwaysShowsEmptyPinnedEffectivePlan(t *testing.T) {
	content := v1alpha1.RolloutExplainContent{
		Summary: v1alpha1.RolloutExplainSummary{
			EffectivePlan: v1alpha1.RolloutPlanSelection{
				Mode: v1alpha1.RolloutPlanModePinned, Evidence: v1alpha1.EvidenceReported,
			},
			PlanReady: v1alpha1.RolloutPlanCondition{
				State: v1alpha1.RolloutConditionTrue, Reason: v1alpha1.RolloutPlanReasonPinned,
				Evidence: v1alpha1.EvidenceReported,
			},
		},
		DeclaredGroups: []v1alpha1.RolloutPlanGroup{{
			Index: 0, Evidence: v1alpha1.EvidenceDeclared, Source: v1alpha1.RolloutPlanSourceInline,
			Strategy: v1alpha1.RolloutStrategyCanary,
		}},
	}

	table := content.Table()
	modeColumn := slices.Index(table.Headers, "PLAN-MODE")
	require.NotEqual(t, -1, modeColumn)
	require.Len(t, table.Rows, 3)
	assert.Equal(t, "Declared", table.Rows[0][0])
	assert.Equal(t, "-", table.Rows[0][modeColumn])
	assert.Equal(t, "Live", table.Rows[1][0])
	assert.Equal(t, "Live", table.Rows[1][modeColumn])
	assert.Equal(t, "groups=0", table.Rows[1][slices.Index(table.Headers, "CONFIG")])
	assert.Equal(t, "-", table.Rows[1][slices.Index(table.Headers, "PLAN-READY")])
	assert.Equal(t, "-", table.Rows[1][slices.Index(table.Headers, "DRIFT")])
	assert.Equal(t, "Effective", table.Rows[2][0])
	assert.Equal(t, "Pinned", table.Rows[2][modeColumn])
	assert.Equal(t, "groups=0", table.Rows[2][slices.Index(table.Headers, "CONFIG")])
	assert.Equal(t, "True/Pinned", table.Rows[2][slices.Index(table.Headers, "PLAN-READY")])
}

func TestRolloutExplainTableDoesNotDuplicateLiveViewWithoutActiveRun(t *testing.T) {
	content := v1alpha1.RolloutExplainContent{
		Summary: v1alpha1.RolloutExplainSummary{EffectivePlan: v1alpha1.RolloutPlanSelection{
			Mode: v1alpha1.RolloutPlanModeLive, Evidence: v1alpha1.EvidenceDeclared,
		}},
		DeclaredGroups: []v1alpha1.RolloutPlanGroup{{
			Index: 0, Evidence: v1alpha1.EvidenceDeclared, Source: v1alpha1.RolloutPlanSourceInline,
			Strategy: v1alpha1.RolloutStrategyBlueGreen,
		}},
		LiveGroups: []v1alpha1.RolloutPlanGroup{},
		EffectiveGroups: []v1alpha1.RolloutPlanGroup{{
			Index: 0, Evidence: v1alpha1.EvidenceDeclared, Source: v1alpha1.RolloutPlanSourceInline,
			Strategy: v1alpha1.RolloutStrategyBlueGreen,
		}},
	}

	table := content.Table()
	assert.Equal(t, [][]string{
		{"Declared", "0", "Declared", "-", "Inline", "BlueGreen", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-"},
		{"Effective", "0", "Declared", "Live", "Inline", "BlueGreen", "-", "-", "-", "-", "-", "-", "Invalid", "Invalid", "-", "-", "-", "-"},
	}, table.Rows)
}

func TestRolloutExplainTableLabelsIgnoredSettings(t *testing.T) {
	content := v1alpha1.RolloutExplainContent{
		Summary: v1alpha1.RolloutExplainSummary{
			EffectivePlan: v1alpha1.RolloutPlanSelection{
				Mode: v1alpha1.RolloutPlanModeLive, Evidence: v1alpha1.EvidenceDeclared,
			},
		},
		EffectiveGroups: []v1alpha1.RolloutPlanGroup{{
			Index: 0, Evidence: v1alpha1.EvidenceDeclared, Source: v1alpha1.RolloutPlanSourceInline,
			Strategy: v1alpha1.RolloutStrategyBlueGreen,
			Soak: &v1alpha1.RolloutSetting{
				Value: "1m0s", Source: v1alpha1.RolloutSettingConfigured,
				Effect: v1alpha1.RolloutSettingEffectIgnoredFinalGroup,
			},
		}},
	}

	table := content.Table()
	require.Len(t, table.Rows, 1)
	assert.Equal(t, "soak=1m0s(Configured;IgnoredFinalGroup)", table.Rows[0][slices.Index(table.Headers, "CONFIG")])
}
