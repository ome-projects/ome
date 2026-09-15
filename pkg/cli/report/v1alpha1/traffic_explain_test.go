package v1alpha1_test

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/cli/report"
	"sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func TestTrafficExplainCanonicalizesWithoutMutatingCaller(t *testing.T) {
	value := trafficExplainReportFixture()
	value.Content.Intent.Extensions = []v1alpha1.TrafficDeclaredExtension{
		{Kind: v1alpha1.TrafficExtensionRetry, Count: 2},
		{Kind: v1alpha1.TrafficExtensionCircuitBreaker, Count: 1},
		{Kind: v1alpha1.TrafficExtensionRetry, Count: 2},
	}
	value.Content.Comparisons = []v1alpha1.TrafficExplainComparison{
		{Field: v1alpha1.TrafficComparisonPolicy, State: v1alpha1.TrafficComparisonMatch},
		{Field: v1alpha1.TrafficComparisonAlgorithm, State: v1alpha1.TrafficComparisonMatch},
		{Field: v1alpha1.TrafficComparisonPolicy, State: v1alpha1.TrafficComparisonMatch},
	}
	value.Content.Issues = []v1alpha1.TrafficExplainIssue{
		{Code: v1alpha1.TrafficExplainIssueUnsupportedDeclaredFields},
		{Code: v1alpha1.TrafficExplainIssueUnsupportedDeclaredFields},
	}
	value.Warnings = []v1alpha1.TrafficWarning{
		{Code: v1alpha1.WarningPartialData},
		{Code: v1alpha1.WarningPartialData},
	}
	originalRoute := value.Content.Reported.Routes[0].Name
	originalExtension := value.Content.Intent.Extensions[0].Kind
	originalComparison := value.Content.Comparisons[0].Field

	canonical := value.Canonical()

	assert.Equal(t, v1alpha1.APIVersion, canonical.APIVersion)
	assert.Equal(t, v1alpha1.TrafficExplainReportKind, canonical.Kind)
	assert.Equal(t, time.UTC, canonical.CollectedAt.Location())
	assert.Equal(t, canonical.CollectedAt, canonical.Sources[0].CollectedAt)
	assert.Equal(t, []v1alpha1.TrafficExtensionKind{
		v1alpha1.TrafficExtensionCircuitBreaker,
		v1alpha1.TrafficExtensionRetry,
	}, []v1alpha1.TrafficExtensionKind{
		canonical.Content.Intent.Extensions[0].Kind,
		canonical.Content.Intent.Extensions[1].Kind,
	})
	assert.Equal(t, []v1alpha1.TrafficComparisonField{
		v1alpha1.TrafficComparisonAlgorithm,
		v1alpha1.TrafficComparisonPolicy,
	}, []v1alpha1.TrafficComparisonField{
		canonical.Content.Comparisons[0].Field,
		canonical.Content.Comparisons[1].Field,
	})
	assert.Len(t, canonical.Content.Issues, 1)
	assert.Len(t, canonical.Warnings, 1)

	canonical.Content.Reported.Routes[0].Name = "changed"
	canonical.Content.Intent.Extensions[0].Kind = v1alpha1.TrafficExtensionTimeout
	canonical.Content.Comparisons[0].Field = v1alpha1.TrafficComparisonCanaryWeight
	canonical.Sources[0].Name = "changed"
	assert.Equal(t, originalRoute, value.Content.Reported.Routes[0].Name)
	assert.Equal(t, originalExtension, value.Content.Intent.Extensions[0].Kind)
	assert.Equal(t, originalComparison, value.Content.Comparisons[0].Field)
	assert.Equal(t, "chat", value.Sources[0].Name)
}

func TestTrafficExplainCanonicalUnknownEnumsHaveTotalOrder(t *testing.T) {
	for _, order := range [][]string{{"FutureB", "FutureA", "FutureC"}, {"FutureC", "FutureB", "FutureA"}, {"FutureA", "FutureC", "FutureB"}} {
		value := v1alpha1.TrafficExplainContent{}
		for _, enum := range order {
			value.Intent.Extensions = append(value.Intent.Extensions, v1alpha1.TrafficDeclaredExtension{Kind: v1alpha1.TrafficExtensionKind(enum), Count: 1})
			value.Comparisons = append(value.Comparisons, v1alpha1.TrafficExplainComparison{Field: v1alpha1.TrafficComparisonField(enum), State: v1alpha1.TrafficComparisonMatch})
		}
		got := value.Canonical()
		assert.Equal(t, []v1alpha1.TrafficDeclaredExtension{{Kind: "FutureA", Count: 1}, {Kind: "FutureB", Count: 1}, {Kind: "FutureC", Count: 1}}, got.Intent.Extensions)
		assert.Equal(t, []v1alpha1.TrafficExplainComparison{{Field: "FutureA", State: v1alpha1.TrafficComparisonMatch}, {Field: "FutureB", State: v1alpha1.TrafficComparisonMatch}, {Field: "FutureC", State: v1alpha1.TrafficComparisonMatch}}, got.Comparisons)
		assert.Equal(t, order[0], string(value.Intent.Extensions[0].Kind))
		assert.Equal(t, order[0], string(value.Comparisons[0].Field))
	}
}

func TestTrafficExplainCompactAndWideTables(t *testing.T) {
	value := trafficExplainReportFixture()

	assert.Equal(t, report.Table{
		Headers: []string{"LAYER", "STATE", "VALUE", "SOURCE"},
		Rows: [][]string{
			{"SUMMARY", "Consistent", "-", "Computed/Unverifiable"},
			{"INTENT", "Declared", "RoundRobin", "Declared/Current"},
			{"SUPPORT", "Honored", "-", "Computed/Current"},
			{"TRANSLATE", "Computed", "envoy-gateway", "Computed/Current"},
			{"REALIZE", "Reported", "r=2 e=1 w=2", "Reported/Unverifiable"},
			{"CHECK", "Match", "algorithm", "Computed/Current"},
			{"CHECK", "Match", "policy", "Computed/Current"},
			{"CHECK", "Match", "canary-weight", "Computed/Unverifiable"},
		},
	}, value.Table())

	var compact bytes.Buffer
	require.NoError(t, value.Table().Write(&compact))
	for _, line := range strings.Split(strings.TrimSuffix(compact.String(), "\n"), "\n") {
		assert.LessOrEqual(t, len([]rune(line)), 80, "compact line: %q", line)
	}

	value.Warnings = []v1alpha1.TrafficWarning{{Code: v1alpha1.WarningPartialData}}
	wide := value.WideTable()
	assert.Equal(t, []string{"LAYER", "FIELD", "STATE", "VALUE", "SOURCE"}, wide.Headers)
	joined := flattenTrafficTable(wide)
	for _, wanted := range []string{
		"REPORT|api-version|-|cli.ome.io/v1alpha1|-",
		"SUBJECT|namespace|-|prod|-",
		"SOURCE|InferenceService|Reported|prod/chat gen=7 at=2026-09-14T17:00:00Z|Reported/Current",
		"INTENT|consistent-hash|Declared|Header inputs=2|Declared/Current",
		"INTENT|endpoint-override|Declared|Header inputs=1|Declared/Current",
		"EXTENSION|CircuitBreaker|Declared|1|Declared/Current",
		"REPORTED|policy|Reported|gateway.envoyproxy.io/v1alpha1/BackendTrafficPolicy/prod/chat|Reported/Current",
		"REPORTED|route|Reported|chat|Reported/Current",
		"OBSERVED|target|Reported|engine/stable/chat-engine-rev-a1b2c3d4=80%|Reported/Unverifiable",
		"REPORTED|condition|Reported|BackendPolicyReady=True/AcceptedByGateway gen=7 at=2026-09-14T16:59:00Z|Reported/Current",
		"WARNING|-|-|PartialData|Computed/Unverifiable",
	} {
		assert.Contains(t, joined, wanted)
	}
}

func TestTrafficExplainWideRetainsUnallocatedCanaryHashes(t *testing.T) {
	value := trafficExplainReportFixture()
	value.Content.Reported.Allocations = nil
	wide := flattenTrafficTable(value.WideTable())
	assert.Contains(t, wide, "OBSERVED|stable-revision|Reported|a1b2c3d4|Reported/Unverifiable")
	assert.Contains(t, wide, "OBSERVED|canary-revision|Reported|e5f6a7b8|Reported/Unverifiable")
}

func TestTrafficExplainMachineOutputIsTypedAndSecretFree(t *testing.T) {
	value := trafficExplainReportFixture()
	for _, format := range []report.Format{report.FormatJSON, report.FormatYAML} {
		var first bytes.Buffer
		var second bytes.Buffer
		require.NoError(t, report.Write(&first, format, value))
		require.NoError(t, report.Write(&second, format, value))
		assert.Equal(t, first.String(), second.String())
		output := first.String()
		for _, wanted := range []string{
			"TrafficExplainReport", "RoundRobin", "CircuitBreaker",
			"BackendPolicyReady", "chat-engine-rev-a1b2c3d4",
		} {
			assert.Contains(t, output, wanted)
		}
		for _, forbidden := range []string{
			"uid-chat", "resourceVersion", "annotations", "message",
			"authorization", "bearer", "cookie-name", "x-session-token",
		} {
			assert.NotContains(t, strings.ToLower(output), strings.ToLower(forbidden))
		}
	}

	for _, typ := range []reflect.Type{
		reflect.TypeOf(v1alpha1.TrafficExplainReport{}),
		reflect.TypeOf(v1alpha1.TrafficExplainContent{}),
		reflect.TypeOf(v1alpha1.TrafficDeclaredIntent{}),
		reflect.TypeOf(v1alpha1.TrafficDeclaredFeature{}),
		reflect.TypeOf(v1alpha1.TrafficDeclaredExtension{}),
		reflect.TypeOf(v1alpha1.TrafficExplainComparison{}),
		reflect.TypeOf(v1alpha1.TrafficExplainIssue{}),
	} {
		assertTrafficSchema(t, typ, map[reflect.Type]bool{})
	}
}

func TestTrafficExplainCompactTableIsBoundedAtProjectionLimits(t *testing.T) {
	value := trafficExplainReportFixture()
	value.Content.Summary.State = v1alpha1.TrafficExplainUnavailable
	value.Content.Summary.Support = v1alpha1.TrafficSupportUnavailable
	value.Content.Summary.Realization = v1alpha1.TrafficRealizationPartial
	value.Content.Summary.Source = v1alpha1.TrafficValueSource{
		Evidence:  v1alpha1.EvidenceUnavailable,
		Freshness: v1alpha1.TrafficFreshnessUnverifiable,
	}
	value.Content.Reported.Summary.Translator = v1alpha1.TrafficTranslatorUnavailable
	value.Content.Reported.Summary.Source.PolicyReady = v1alpha1.TrafficValueSource{
		Evidence:  v1alpha1.EvidenceUnavailable,
		Freshness: v1alpha1.TrafficFreshnessUnavailable,
	}
	value.Content.Reported.Routes = nil
	value.Content.Reported.Endpoints = nil
	value.Content.Reported.Allocations = nil
	value.Content.Reported.Canary = nil
	for i := 0; i < 4; i++ {
		value.Content.Reported.Routes = append(value.Content.Reported.Routes,
			v1alpha1.TrafficRoute{Name: fmt.Sprintf("route-%d", i), Source: v1alpha1.TrafficValueSource{
				Evidence:  v1alpha1.EvidenceReported,
				Freshness: v1alpha1.TrafficFreshnessUnverifiable,
			}})
	}
	for i := 0; i < 16; i++ {
		value.Content.Reported.Endpoints = append(value.Content.Reported.Endpoints,
			v1alpha1.TrafficEndpoint{URL: fmt.Sprintf("https://e-%d.example/", i), Source: v1alpha1.TrafficValueSource{
				Evidence:  v1alpha1.EvidenceReported,
				Freshness: v1alpha1.TrafficFreshnessUnverifiable,
			}})
	}
	for i := 0; i < 24; i++ {
		value.Content.Reported.Allocations = append(value.Content.Reported.Allocations,
			v1alpha1.TrafficAllocation{
				Component:    v1alpha1.RuntimeComponentEngine,
				Role:         v1alpha1.TrafficRoleOther,
				RevisionName: fmt.Sprintf("chat-engine-rev-%08d", i),
				RevisionHash: fmt.Sprintf("%08d", i), Percent: int32(i),
				Source: v1alpha1.TrafficValueSource{
					Evidence:  v1alpha1.EvidenceReported,
					Freshness: v1alpha1.TrafficFreshnessUnverifiable,
				},
			})
	}
	value.Content.Issues = []v1alpha1.TrafficExplainIssue{
		{Code: v1alpha1.TrafficExplainIssueDeclaredAnnotationsInvalid},
		{Code: v1alpha1.TrafficExplainIssueUnsupportedDeclaredFields},
		{Code: v1alpha1.TrafficExplainIssueRealizationUnavailable},
	}

	var output bytes.Buffer
	require.NoError(t, value.Table().Write(&output))
	for _, line := range strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n") {
		assert.LessOrEqual(t, len([]rune(line)), 80, "compact line: %q", line)
	}
}

func trafficExplainReportFixture() v1alpha1.TrafficExplainReport {
	current := v1alpha1.TrafficValueSource{
		Evidence:  v1alpha1.EvidenceReported,
		Freshness: v1alpha1.TrafficFreshnessCurrent,
	}
	unverifiable := v1alpha1.TrafficValueSource{
		Evidence:  v1alpha1.EvidenceReported,
		Freshness: v1alpha1.TrafficFreshnessUnverifiable,
	}
	declared := v1alpha1.TrafficValueSource{
		Evidence:  v1alpha1.EvidenceDeclared,
		Freshness: v1alpha1.TrafficFreshnessCurrent,
	}
	computedCurrent := v1alpha1.TrafficValueSource{
		Evidence:  v1alpha1.EvidenceComputed,
		Freshness: v1alpha1.TrafficFreshnessCurrent,
	}
	computedUnverifiable := v1alpha1.TrafficValueSource{
		Evidence:  v1alpha1.EvidenceComputed,
		Freshness: v1alpha1.TrafficFreshnessUnverifiable,
	}
	collected := time.Date(2026, 9, 14, 17, 0, 0, 0, time.UTC)
	value := v1alpha1.NewTrafficExplainReport(
		v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		v1alpha1.TrafficExplainContent{
			Summary: v1alpha1.TrafficExplainSummary{
				State:       v1alpha1.TrafficExplainConsistent,
				Intent:      v1alpha1.TrafficIntentDeclared,
				Support:     v1alpha1.TrafficSupportHonored,
				Realization: v1alpha1.TrafficRealizationReported,
				Source:      computedUnverifiable,
			},
			Intent: v1alpha1.TrafficDeclaredIntent{
				State:     v1alpha1.TrafficIntentDeclared,
				Algorithm: v1alpha1.TrafficAlgorithmRoundRobin,
				ConsistentHash: v1alpha1.TrafficDeclaredFeature{
					State:      v1alpha1.TrafficFeatureDeclared,
					Variant:    v1alpha1.TrafficFeatureVariantHeader,
					InputCount: 2,
				},
				EndpointOverride: v1alpha1.TrafficDeclaredFeature{
					State:      v1alpha1.TrafficFeatureDeclared,
					Variant:    v1alpha1.TrafficFeatureVariantHeader,
					InputCount: 1,
				},
				Extensions: []v1alpha1.TrafficDeclaredExtension{
					{Kind: v1alpha1.TrafficExtensionCircuitBreaker, Count: 1},
				},
				Source: declared,
			},
			Reported: v1alpha1.TrafficStatusContent{
				Summary: v1alpha1.TrafficSummary{
					State:      v1alpha1.TrafficStateReported,
					Translator: v1alpha1.TrafficTranslatorEnvoyGateway,
					Algorithm:  v1alpha1.TrafficAlgorithmRoundRobin,
					PolicyReady: v1alpha1.TrafficConditionValue{
						Status: v1alpha1.TrafficConditionTrue,
						Reason: v1alpha1.TrafficReasonAcceptedByGateway,
					},
					Unsupported: v1alpha1.TrafficUnsupportedNone,
					Source: v1alpha1.TrafficSummarySources{
						State: computedCurrent, Translator: computedCurrent,
						Algorithm: current, PolicyReady: current,
						Unsupported: current,
					},
				},
				Policy: &v1alpha1.TrafficPolicy{
					APIVersion: "gateway.envoyproxy.io/v1alpha1",
					Kind:       v1alpha1.TrafficPolicyBackendTrafficPolicy,
					Namespace:  "prod", Name: "chat", Source: current,
				},
				Routes: []v1alpha1.TrafficRoute{
					{Name: "chat-router", Source: current},
					{Name: "chat", Source: current},
				},
				Endpoints: []v1alpha1.TrafficEndpoint{
					{URL: "https://chat.prod.example/", Source: unverifiable},
				},
				Canary: &v1alpha1.TrafficCanary{
					Component:   v1alpha1.RuntimeComponentEngine,
					CurrentStep: 0, TotalSteps: 2, ObservedTraffic: 20,
					StableRevisionHash: "a1b2c3d4",
					CanaryRevisionHash: "e5f6a7b8", Source: unverifiable,
				},
				Allocations: []v1alpha1.TrafficAllocation{
					{Component: v1alpha1.RuntimeComponentEngine,
						Role:         v1alpha1.TrafficRoleCanary,
						RevisionName: "chat-engine-rev-e5f6a7b8",
						RevisionHash: "e5f6a7b8", Percent: 20,
						Source: unverifiable},
					{Component: v1alpha1.RuntimeComponentEngine,
						Role:         v1alpha1.TrafficRoleStable,
						RevisionName: "chat-engine-rev-a1b2c3d4",
						RevisionHash: "a1b2c3d4", Percent: 80,
						Source: unverifiable},
				},
				Conditions: []v1alpha1.TrafficCondition{{
					Type:               v1alpha1.TrafficConditionBackendPolicyReady,
					Status:             v1alpha1.TrafficConditionTrue,
					Reason:             v1alpha1.TrafficReasonAcceptedByGateway,
					ObservedGeneration: 7,
					LastTransitionTime: collected.Add(-time.Minute),
					Source:             current,
				}},
				Issues: []v1alpha1.TrafficIssue{},
			},
			Comparisons: []v1alpha1.TrafficExplainComparison{
				{Field: v1alpha1.TrafficComparisonCanaryWeight,
					State:  v1alpha1.TrafficComparisonMatch,
					Source: computedUnverifiable},
				{Field: v1alpha1.TrafficComparisonPolicy,
					State:  v1alpha1.TrafficComparisonMatch,
					Source: computedCurrent},
				{Field: v1alpha1.TrafficComparisonAlgorithm,
					State:  v1alpha1.TrafficComparisonMatch,
					Source: computedCurrent},
			},
			Issues: []v1alpha1.TrafficExplainIssue{},
		},
		fixedClock{now: collected},
	)
	value.Sources = []v1alpha1.TrafficExplainSourceReference{{
		Kind:      v1alpha1.TrafficSourceInferenceService,
		Namespace: "prod", Name: "chat", Generation: 7,
		Evidence: v1alpha1.EvidenceReported,
	}}
	return value
}
