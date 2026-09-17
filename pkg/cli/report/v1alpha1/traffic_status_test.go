package v1alpha1_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/cli/report"
	"sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func TestTrafficStatusCanonicalizesWithoutMutatingCaller(t *testing.T) {
	collected := time.Date(2026, time.September, 14, 10, 0, 0, 0, time.FixedZone("west", -7*60*60))
	transition := collected.Add(-time.Minute)
	current := v1alpha1.TrafficValueSource{Evidence: v1alpha1.EvidenceReported, Freshness: v1alpha1.TrafficFreshnessCurrent}
	unverifiable := v1alpha1.TrafficValueSource{Evidence: v1alpha1.EvidenceReported, Freshness: v1alpha1.TrafficFreshnessUnverifiable}
	value := v1alpha1.NewTrafficStatusReport(
		v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		v1alpha1.TrafficStatusContent{
			Summary: v1alpha1.TrafficSummary{
				State: v1alpha1.TrafficStatePending, Translator: v1alpha1.TrafficTranslatorEnvoyGateway,
				Algorithm:   v1alpha1.TrafficAlgorithmRoundRobin,
				PolicyReady: v1alpha1.TrafficConditionValue{Status: v1alpha1.TrafficConditionUnknown, Reason: v1alpha1.TrafficReasonPending},
				Unsupported: v1alpha1.TrafficUnsupportedNone,
				Source: v1alpha1.TrafficSummarySources{
					State:      v1alpha1.TrafficValueSource{Evidence: v1alpha1.EvidenceComputed, Freshness: v1alpha1.TrafficFreshnessUnverifiable},
					Translator: current, Algorithm: current, PolicyReady: current, Unsupported: current,
				},
			},
			Policy: &v1alpha1.TrafficPolicy{
				APIVersion: "gateway.envoyproxy.io/v1alpha1", Kind: v1alpha1.TrafficPolicyBackendTrafficPolicy,
				Namespace: "prod", Name: "chat", Source: current,
			},
			Routes: []v1alpha1.TrafficRoute{
				{Name: "chat-router", Source: current}, {Name: "chat", Source: current},
			},
			Endpoints: []v1alpha1.TrafficEndpoint{
				{URL: "https://chat.prod.example/", Source: unverifiable},
				{URL: "http://chat-engine.prod.svc.cluster.local", Source: unverifiable},
			},
			Canary: &v1alpha1.TrafficCanary{
				Component: v1alpha1.RuntimeComponentEngine, CurrentStep: 0, TotalSteps: 2,
				ObservedTraffic: 20, StableRevisionHash: "a1b2c3d4", CanaryRevisionHash: "e5f6a7b8", Source: unverifiable,
			},
			Allocations: []v1alpha1.TrafficAllocation{
				{Component: v1alpha1.RuntimeComponentEngine, Role: v1alpha1.TrafficRoleCanary, RevisionName: "chat-engine-rev-e5f6a7b8", RevisionHash: "e5f6a7b8", Percent: 20, Source: unverifiable},
				{Component: v1alpha1.RuntimeComponentEngine, Role: v1alpha1.TrafficRoleStable, RevisionName: "chat-engine-rev-a1b2c3d4", RevisionHash: "a1b2c3d4", Percent: 80, Source: unverifiable},
			},
			Conditions: []v1alpha1.TrafficCondition{
				{Type: v1alpha1.TrafficConditionBackendPolicyReady, Status: v1alpha1.TrafficConditionUnknown, Reason: v1alpha1.TrafficReasonPending, ObservedGeneration: 7, LastTransitionTime: transition, Source: current},
			},
			Issues: []v1alpha1.TrafficIssue{{Code: v1alpha1.TrafficIssueRoutesTruncated}, {Code: v1alpha1.TrafficIssueRoutesTruncated}},
		},
		fixedClock{now: collected},
	)
	value.Sources = []v1alpha1.TrafficSourceReference{{
		Kind: v1alpha1.TrafficSourceInferenceService, Namespace: "prod", Name: "chat",
		UID: "uid-chat", Generation: 7, Evidence: v1alpha1.EvidenceReported,
	}}
	value.Warnings = []v1alpha1.TrafficWarning{{Code: v1alpha1.WarningTruncated}, {Code: v1alpha1.WarningTruncated}}
	originalRouteName := value.Content.Routes[0].Name
	originalAllocationName := value.Content.Allocations[0].RevisionName
	originalConditionReason := value.Content.Conditions[0].Reason
	originalSourceName := value.Sources[0].Name
	originalPolicyName := value.Content.Policy.Name
	originalCanaryHash := value.Content.Canary.CanaryRevisionHash

	canonical := value.Canonical()

	assert.Equal(t, v1alpha1.APIVersion, canonical.APIVersion)
	assert.Equal(t, v1alpha1.TrafficStatusReportKind, canonical.Kind)
	assert.Equal(t, "2026-09-14T17:00:00Z", canonical.CollectedAt.Format(time.RFC3339))
	assert.Equal(t, canonical.CollectedAt, canonical.Sources[0].CollectedAt)
	assert.Equal(t, []string{"chat", "chat-router"}, []string{canonical.Content.Routes[0].Name, canonical.Content.Routes[1].Name})
	assert.Equal(t, []v1alpha1.TrafficAllocationRole{v1alpha1.TrafficRoleStable, v1alpha1.TrafficRoleCanary}, []v1alpha1.TrafficAllocationRole{
		canonical.Content.Allocations[0].Role, canonical.Content.Allocations[1].Role,
	})
	assert.Len(t, canonical.Content.Issues, 1)
	assert.Len(t, canonical.Warnings, 1)
	assert.Equal(t, time.UTC, canonical.Content.Conditions[0].LastTransitionTime.Location())

	canonical.Content.Routes[0].Name = "changed"
	canonical.Content.Allocations[0].RevisionName = "changed"
	canonical.Content.Conditions[0].Reason = v1alpha1.TrafficReasonGatewayRejected
	canonical.Sources[0].Name = "changed"
	canonical.Content.Policy.Name = "changed"
	canonical.Content.Canary.CanaryRevisionHash = "ffffffff"
	assert.Equal(t, originalRouteName, value.Content.Routes[0].Name)
	assert.Equal(t, originalAllocationName, value.Content.Allocations[0].RevisionName)
	assert.Equal(t, originalConditionReason, value.Content.Conditions[0].Reason)
	assert.Equal(t, originalSourceName, value.Sources[0].Name)
	assert.Equal(t, originalPolicyName, value.Content.Policy.Name)
	assert.Equal(t, originalCanaryHash, value.Content.Canary.CanaryRevisionHash)
}

func TestTrafficStatusCompactAndWideTables(t *testing.T) {
	value := trafficStatusReportFixture()

	assert.Equal(t, report.Table{
		Headers: []string{"FIELD", "COMP", "VALUE", "SOURCE"},
		Rows: [][]string{
			{"STATE", "-", "Pending", "Computed/Unverifiable"},
			{"TRANSLATOR", "-", "envoy-gateway", "Computed/Current"},
			{"ALGORITHM", "-", "RoundRobin", "Reported/Current"},
			{"POLICY-READY", "-", "Unknown/Pending", "Reported/Current"},
			{"UNSUPPORTED", "-", "None", "Reported/Current"},
			{"ROUTES", "-", "2", "Reported/Current"},
			{"ENDPOINTS", "-", "2", "Reported/Unverifiable"},
			{"CANARY", "engine", "1/2 @ 20%", "Reported/Unverifiable"},
			{"WEIGHT", "engine", "stable:a1b2c3d4=80%", "Reported/Unverifiable"},
			{"WEIGHT", "engine", "canary:e5f6a7b8=20%", "Reported/Unverifiable"},
		},
	}, value.Table())

	wide := value.WideTable()
	assert.Equal(t, []string{"FIELD", "COMP", "VALUE", "SOURCE"}, wide.Headers)
	joined := flattenTrafficTable(wide)
	for _, wanted := range []string{
		"POLICY|-|gateway.envoyproxy.io/v1alpha1/BackendTrafficPolicy/prod/chat|Reported/Current",
		"ROUTE|-|chat|Reported/Current", "ROUTE|-|chat-router|Reported/Current",
		"ENDPOINT|-|http://chat-engine.prod.svc.cluster.local|Reported/Unverifiable",
		"TARGET|engine|stable:chat-engine-rev-a1b2c3d4=80%|Reported/Unverifiable",
		"CONDITION|-|BackendPolicyReady=Unknown/Pending gen=7 at=2026-09-14T16:59:00Z|Reported/Current",
	} {
		assert.Contains(t, joined, wanted)
	}
}

func TestTrafficStatusConcurrentCanariesAreTypedAndIndividuallyRendered(t *testing.T) {
	var content v1alpha1.TrafficStatusContent
	require.NoError(t, json.Unmarshal([]byte(`{"canaries":[
		{"component":"router","currentStep":1,"totalSteps":3,"observedTraffic":50,"stableRevisionHash":"11112222","canaryRevisionHash":"33334444","source":{"evidence":"Reported","freshness":"Unverifiable"}},
		{"component":"engine","currentStep":0,"totalSteps":2,"observedTraffic":20,"stableRevisionHash":"a1b2c3d4","canaryRevisionHash":"e5f6a7b8","source":{"evidence":"Reported","freshness":"Unverifiable"}}
	]}`), &content))

	canonical := content.Canonical()
	compact := flattenTrafficTable(canonical.Table())
	assert.Contains(t, compact, "CANARY|engine|1/2 @ 20%|Reported/Unverifiable")
	assert.Contains(t, compact, "CANARY|router|2/3 @ 50%|Reported/Unverifiable")
	assert.Less(t, strings.Index(compact, "CANARY|engine"), strings.Index(compact, "CANARY|router"))
	wide := flattenTrafficTable(canonical.WideTable())
	assert.Contains(t, wide, "CANARY|engine|1/2 @ 20%|Reported/Unverifiable")
	assert.Contains(t, wide, "CANARY|router|2/3 @ 50%|Reported/Unverifiable")
	encoded, err := json.Marshal(canonical)
	require.NoError(t, err)
	assert.Contains(t, string(encoded), `"canaries":[`)
	assert.NotContains(t, string(encoded), `"canary":`)

	canonical.Canaries[0].CanaryRevisionHash = "changed"
	assert.Equal(t, "e5f6a7b8", content.Canaries[1].CanaryRevisionHash)

	single, err := json.Marshal(trafficStatusReportFixture().Content)
	require.NoError(t, err)
	assert.Contains(t, string(single), `"canary":`)
	assert.NotContains(t, string(single), `"canaries":`)
}

func TestTrafficStatusCanonicalPrefersSingularWhenBothFormsAreSet(t *testing.T) {
	content := trafficStatusReportFixture().Content
	content.Canaries = []v1alpha1.TrafficCanary{{
		Component: v1alpha1.RuntimeComponentRouter, CurrentStep: 1, TotalSteps: 3,
		ObservedTraffic: 50, CanaryRevisionHash: "33334444",
	}}

	canonical := content.Canonical()

	require.NotNil(t, canonical.Canary)
	assert.Empty(t, canonical.Canaries)
	encoded, err := json.Marshal(canonical)
	require.NoError(t, err)
	assert.Contains(t, string(encoded), `"canary":`)
	assert.NotContains(t, string(encoded), `"canaries":`)
}

func TestTrafficStatusMachineOutputIsStableAndTyped(t *testing.T) {
	value := trafficStatusReportFixture()
	for _, format := range []report.Format{report.FormatJSON, report.FormatYAML} {
		var first bytes.Buffer
		var second bytes.Buffer
		require.NoError(t, report.Write(&first, format, value))
		require.NoError(t, report.Write(&second, format, value))
		assert.Equal(t, first.String(), second.String())
		output := first.String()
		for _, wanted := range []string{"TrafficStatusReport", "chat-engine-rev-a1b2c3d4", "BackendPolicyReady"} {
			assert.Contains(t, output, wanted)
		}
		for _, forbidden := range []string{"message", "annotations", "tags", "credentials", "resourceVersion", "continueToken"} {
			assert.NotContains(t, strings.ToLower(output), strings.ToLower(forbidden))
		}
	}

	for _, typ := range []reflect.Type{
		reflect.TypeOf(v1alpha1.TrafficStatusReport{}), reflect.TypeOf(v1alpha1.TrafficStatusContent{}),
		reflect.TypeOf(v1alpha1.TrafficPolicy{}), reflect.TypeOf(v1alpha1.TrafficRoute{}),
		reflect.TypeOf(v1alpha1.TrafficEndpoint{}), reflect.TypeOf(v1alpha1.TrafficCanary{}),
		reflect.TypeOf(v1alpha1.TrafficAllocation{}), reflect.TypeOf(v1alpha1.TrafficCondition{}),
		reflect.TypeOf(v1alpha1.TrafficIssue{}),
	} {
		assertTrafficSchema(t, typ, map[reflect.Type]bool{})
	}
}

func trafficStatusReportFixture() v1alpha1.TrafficStatusReport {
	current := v1alpha1.TrafficValueSource{Evidence: v1alpha1.EvidenceReported, Freshness: v1alpha1.TrafficFreshnessCurrent}
	unverifiable := v1alpha1.TrafficValueSource{Evidence: v1alpha1.EvidenceReported, Freshness: v1alpha1.TrafficFreshnessUnverifiable}
	value := v1alpha1.NewTrafficStatusReport(
		v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		v1alpha1.TrafficStatusContent{
			Summary: v1alpha1.TrafficSummary{
				State: v1alpha1.TrafficStatePending, Translator: v1alpha1.TrafficTranslatorEnvoyGateway,
				Algorithm:   v1alpha1.TrafficAlgorithmRoundRobin,
				PolicyReady: v1alpha1.TrafficConditionValue{Status: v1alpha1.TrafficConditionUnknown, Reason: v1alpha1.TrafficReasonPending},
				Unsupported: v1alpha1.TrafficUnsupportedNone,
				Source: v1alpha1.TrafficSummarySources{
					State:      v1alpha1.TrafficValueSource{Evidence: v1alpha1.EvidenceComputed, Freshness: v1alpha1.TrafficFreshnessUnverifiable},
					Translator: v1alpha1.TrafficValueSource{Evidence: v1alpha1.EvidenceComputed, Freshness: v1alpha1.TrafficFreshnessCurrent},
					Algorithm:  current, PolicyReady: current, Unsupported: current,
				},
			},
			Policy:    &v1alpha1.TrafficPolicy{APIVersion: "gateway.envoyproxy.io/v1alpha1", Kind: v1alpha1.TrafficPolicyBackendTrafficPolicy, Namespace: "prod", Name: "chat", Source: current},
			Routes:    []v1alpha1.TrafficRoute{{Name: "chat-router", Source: current}, {Name: "chat", Source: current}},
			Endpoints: []v1alpha1.TrafficEndpoint{{URL: "https://chat.prod.example/", Source: unverifiable}, {URL: "http://chat-engine.prod.svc.cluster.local", Source: unverifiable}},
			Canary:    &v1alpha1.TrafficCanary{Component: v1alpha1.RuntimeComponentEngine, CurrentStep: 0, TotalSteps: 2, ObservedTraffic: 20, StableRevisionHash: "a1b2c3d4", CanaryRevisionHash: "e5f6a7b8", Source: unverifiable},
			Allocations: []v1alpha1.TrafficAllocation{
				{Component: v1alpha1.RuntimeComponentEngine, Role: v1alpha1.TrafficRoleCanary, RevisionName: "chat-engine-rev-e5f6a7b8", RevisionHash: "e5f6a7b8", Percent: 20, Source: unverifiable},
				{Component: v1alpha1.RuntimeComponentEngine, Role: v1alpha1.TrafficRoleStable, RevisionName: "chat-engine-rev-a1b2c3d4", RevisionHash: "a1b2c3d4", Percent: 80, Source: unverifiable},
			},
			Conditions: []v1alpha1.TrafficCondition{{Type: v1alpha1.TrafficConditionBackendPolicyReady, Status: v1alpha1.TrafficConditionUnknown, Reason: v1alpha1.TrafficReasonPending, ObservedGeneration: 7, LastTransitionTime: time.Date(2026, 9, 14, 16, 59, 0, 0, time.UTC), Source: current}},
			Issues:     []v1alpha1.TrafficIssue{},
		},
		fixedClock{now: time.Date(2026, 9, 14, 17, 0, 0, 0, time.UTC)},
	)
	value.Sources = []v1alpha1.TrafficSourceReference{{Kind: v1alpha1.TrafficSourceInferenceService, Namespace: "prod", Name: "chat", UID: "uid-chat", Generation: 7, Evidence: v1alpha1.EvidenceReported}}
	return value
}

func flattenTrafficTable(table report.Table) string {
	values := make([]string, len(table.Rows))
	for i := range table.Rows {
		values[i] = strings.Join(table.Rows[i], "|")
	}
	return strings.Join(values, "\n")
}

func assertTrafficSchema(t *testing.T, typ reflect.Type, seen map[reflect.Type]bool) {
	t.Helper()
	for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array {
		typ = typ.Elem()
	}
	if typ.PkgPath() == "time" || seen[typ] {
		return
	}
	seen[typ] = true
	require.NotEqual(t, reflect.Interface, typ.Kind())
	require.NotEqual(t, reflect.Map, typ.Kind())
	if typ.Kind() != reflect.Struct {
		return
	}
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		name := strings.ToLower(field.Name + " " + strings.Split(field.Tag.Get("json"), ",")[0])
		for _, forbidden := range []string{"message", "annotation", "tag", "credential", "resourceversion", "managedfield", "ownerreference"} {
			assert.NotContains(t, name, forbidden, "unsafe field %s.%s", typ, field.Name)
		}
		assertTrafficSchema(t, field.Type, seen)
	}
}
