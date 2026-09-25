package placementprojection

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/report"
	v "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func TestCurrentTrafficMapReasonsStayResourceSpecific(t *testing.T) {
	t.Parallel()

	tests := []struct {
		conditionType string
		reason        string
		want          v.PlacementValue
	}{
		{"Routable", "Routable", "Routable"},
		{"Routable", "NotPlaced", "NotPlaced"},
		{"Routable", "NoAddressableHome", "NoAddressableHome"},
		{"Routable", "AllHomesUnready", "AllHomesUnready"},
		{"Routable", "NoRoutableCapacity", "NoRoutableCapacity"},
		{"Routable", "AllHomesProbeFailed", "AllHomesProbeFailed"},
		{"Routable", "TrafficDrain", "TrafficDrain"},
		{"Published", "Published", "Published"},
		{"Published", "Withdrawn", "Withdrawn"},
		{"Published", "InvalidOwner", "InvalidOwner"},
		{"Published", "InvalidOptions", "InvalidOptions"},
		{"Published", "InvalidPlan", "InvalidPlan"},
		{"Published", "LegacyLifecycleActive", "LegacyLifecycleActive"},
		{"Published", "ClaimRejected", "ClaimRejected"},
		{"Published", "DrainFailed", "DrainFailed"},
		{"Published", "ApplyFailed", "ApplyFailed"},
		{"Published", "UnpublishFailed", "UnpublishFailed"},
		{"Published", "PublisherChanged", "PublisherChanged"},
		{"Published", "Unpublished", "Unpublished"},
		{"OverrideActive", "OverridesApplied", "OverridesApplied"},
		{"OverrideActive", "OverridesPending", "OverridesPending"},
		{"OverrideActive", "NoOverrides", "NoOverrides"},
		{"CapacityFallback", "CapacityPollingDisabled", "CapacityPollingDisabled"},
		{"CapacityFallback", "NoCapacityTargets", "NoCapacityTargets"},
		{"CapacityFallback", "EndpointCapacityAvailable", "EndpointCapacityAvailable"},
		{"CapacityFallback", "EndpointCapacityUnavailable", "EndpointCapacityUnavailable"},
		{"Routable", "ProbeSucceeded", "OtherReportedReason"},
		{"Published", "ConnectionFailed", "OtherReportedReason"},
		{"OverrideActive", "TrafficDrain", "OtherReportedReason"},
		{"CapacityFallback", "OverridesApplied", "OtherReportedReason"},
		{"CapacityFallback", "FuturePrivateReason", "OtherReportedReason"},
	}

	for _, test := range tests {
		t.Run(test.conditionType+"/"+test.reason, func(t *testing.T) {
			t.Parallel()

			snapshot := fixture(t)
			snapshot.TrafficMap.Status.Conditions = []metav1.Condition{
				routingCondition(test.conditionType, test.reason, 4),
			}
			reportValue, err := ProjectEndpoint(snapshot, fixtureClock)
			require.NoError(t, err)
			projected := placementConditionByType(t, reportValue.Content.Conditions, test.conditionType)
			require.Equal(t, test.want, projected.Reason)
			require.Equal(t, v.PlacementValue("Current"), projected.Source.Freshness)
			if test.want == "OtherReportedReason" {
				encoded, marshalErr := json.Marshal(reportValue)
				require.NoError(t, marshalErr)
				require.NotContains(t, string(encoded), test.reason)
			}
		})
	}
}

func TestCurrentPublisherReasonsRemainVisibleAndPrivateInEveryFormat(t *testing.T) {
	t.Parallel()

	reasons := []struct {
		value     string
		status    metav1.ConditionStatus
		tableCell string
	}{
		{"Published", metav1.ConditionTrue, "True: Published (Current)"},
		{"Withdrawn", metav1.ConditionFalse, "False: Withdrawn (Current)"},
		{"InvalidOwner", metav1.ConditionFalse, "False: InvalidOwner (Current)"},
		{"InvalidOptions", metav1.ConditionFalse, "False: InvalidOptions (Current)"},
		{"InvalidPlan", metav1.ConditionFalse, "False: InvalidPlan (Current)"},
		{"LegacyLifecycleActive", metav1.ConditionFalse, "False: LegacyLifecycleActive (Current)"},
		{"ClaimRejected", metav1.ConditionFalse, "False: ClaimRejected (Current)"},
		{"DrainFailed", metav1.ConditionFalse, "False: DrainFailed (Current)"},
		{"ApplyFailed", metav1.ConditionFalse, "False: ApplyFailed (Current)"},
		{"UnpublishFailed", metav1.ConditionFalse, "False: UnpublishFailed (Current)"},
		{"PublisherChanged", metav1.ConditionFalse, "False: PublisherChanged (Current)"},
		{"Unpublished", metav1.ConditionFalse, "False: Unpublished (Current)"},
	}
	for _, reason := range reasons {
		reason := reason
		t.Run(reason.value, func(t *testing.T) {
			t.Parallel()

			snapshot := fixture(t)
			condition := routingCondition("Published", reason.value, 4)
			condition.Status = reason.status
			snapshot.TrafficMap.Status.Conditions = []metav1.Condition{condition}
			reportValue, err := ProjectEndpoint(snapshot, fixtureClock)
			require.NoError(t, err)
			require.Equal(t, v.PlacementValue(reason.value), reportValue.Content.Routing.Published.Reason)

			outputs := placementEndpointOutputs(t, reportValue)
			for name, output := range outputs {
				require.Contains(t, output, reason.value, name)
			}
			for _, name := range []string{"table", "wide"} {
				require.Contains(t, outputs[name], reason.tableCell, name)
			}
		})
	}

	t.Run("unknown reason remains code-only", func(t *testing.T) {
		t.Parallel()

		const privateReason = "PrivatePublisherFailureToken"
		snapshot := fixture(t)
		condition := routingCondition("Published", privateReason, 4)
		condition.Status = metav1.ConditionFalse
		snapshot.TrafficMap.Status.Conditions = []metav1.Condition{condition}
		reportValue, err := ProjectEndpoint(snapshot, fixtureClock)
		require.NoError(t, err)
		require.Equal(t, v.PlacementValue("OtherReportedReason"), reportValue.Content.Routing.Published.Reason)

		outputs := placementEndpointOutputs(t, reportValue)
		for name, output := range outputs {
			require.Contains(t, output, "OtherReportedReason", name)
			require.NotContains(t, output, privateReason, name)
		}
		for _, name := range []string{"table", "wide"} {
			require.Contains(t, outputs[name], "False: OtherReportedReason (Current)", name)
		}
	})
}

func TestPublisherFailuresAfterPreviouslyRealizedGenerationStayPrecise(t *testing.T) {
	t.Parallel()

	failureReasons := []string{
		"InvalidOwner",
		"InvalidOptions",
		"InvalidPlan",
		"LegacyLifecycleActive",
		"ClaimRejected",
		"DrainFailed",
		"ApplyFailed",
		"UnpublishFailed",
		"PublisherChanged",
	}
	for _, reason := range failureReasons {
		reason := reason
		t.Run(reason, func(t *testing.T) {
			t.Parallel()

			snapshot := fixture(t)
			snapshot.TrafficMap.Status.Published = false
			snapshot.TrafficMap.Status.ObservedTrafficMapGeneration = 3
			condition := routingCondition("Published", reason, 4)
			condition.Status = metav1.ConditionFalse
			snapshot.TrafficMap.Status.Conditions = []metav1.Condition{condition}

			reportValue, err := ProjectEndpoint(snapshot, fixtureClock)
			require.NoError(t, err)
			require.Equal(t, v.PlacementValue("ReportedFalse"), reportValue.Content.Routing.Acknowledgement)
			require.Equal(t, v.PlacementValue(reason), reportValue.Content.Routing.Published.Reason)
			require.Equal(t, v.PlacementValue("Current"), reportValue.Content.Routing.Published.Source.Freshness)
			require.Equal(t, v.PlacementValue("Current"), reportValue.Content.Routing.AcknowledgementSource.Freshness)
			require.Empty(t, reportValue.Content.Routing.AcknowledgementSource.Reason)

			for name, output := range placementEndpointOutputs(t, reportValue) {
				require.Contains(t, output, reason, name)
				require.NotContains(t, output, "ConflictingAcknowledgement", name)
			}
		})
	}
}

func TestPublisherFailureAcknowledgementUsesConditionFreshness(t *testing.T) {
	t.Parallel()

	snapshot := fixture(t)
	snapshot.TrafficMap.Status.ObservedTrafficMapGeneration = 2
	condition := routingCondition("Published", "ApplyFailed", 3)
	condition.Status = metav1.ConditionFalse
	snapshot.TrafficMap.Status.Conditions = []metav1.Condition{condition}

	reportValue, err := ProjectEndpoint(snapshot, fixtureClock)
	require.NoError(t, err)
	require.Equal(t, v.PlacementValue("ReportedFalse"), reportValue.Content.Routing.Acknowledgement)
	require.Equal(t, v.PlacementValue("Stale"), reportValue.Content.Routing.Published.Source.Freshness)
	require.Equal(t, v.PlacementValue("Stale"), reportValue.Content.Routing.AcknowledgementSource.Freshness)
	require.Equal(t, v.PlacementValue("StaleGeneration"), reportValue.Content.Routing.AcknowledgementSource.Reason)
}

func TestPublisherGenerationExceptionRejectsSuccessAndShapeContradictions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		scalar     bool
		observed   int64
		status     metav1.ConditionStatus
		reason     string
		generation int64
	}{
		{"published generation diverges", true, 3, metav1.ConditionTrue, "Published", 4},
		{"withdrawn generation diverges", false, 3, metav1.ConditionFalse, "Withdrawn", 4},
		{"failure cannot trail realized generation", false, 4, metav1.ConditionFalse, "ApplyFailed", 3},
		{"published scalar cannot accompany failure", true, 3, metav1.ConditionFalse, "ApplyFailed", 4},
		{"published cannot report false", false, 4, metav1.ConditionFalse, "Published", 4},
		{"withdrawn cannot report true", false, 4, metav1.ConditionTrue, "Withdrawn", 4},
		{"failure cannot report true", false, 4, metav1.ConditionTrue, "ApplyFailed", 4},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			snapshot := fixture(t)
			snapshot.TrafficMap.Status.Published = test.scalar
			snapshot.TrafficMap.Status.ObservedTrafficMapGeneration = test.observed
			condition := routingCondition("Published", test.reason, test.generation)
			condition.Status = test.status
			snapshot.TrafficMap.Status.Conditions = []metav1.Condition{condition}

			reportValue, err := ProjectEndpoint(snapshot, fixtureClock)
			require.NoError(t, err)
			require.Equal(t, v.PlacementValue("Invalid"), reportValue.Content.Routing.Acknowledgement)
			require.Equal(t, v.PlacementValue("Invalid"), reportValue.Content.Routing.AcknowledgementSource.Freshness)
			require.Equal(t, v.PlacementValue("ConflictingAcknowledgement"), reportValue.Content.Routing.AcknowledgementSource.Reason)
		})
	}
}

func TestUnknownPublisherReasonsFailClosedAtMatchingGeneration(t *testing.T) {
	t.Parallel()

	const privateReason = "PrivateFuturePublisherReason"
	for _, status := range []metav1.ConditionStatus{
		metav1.ConditionTrue,
		metav1.ConditionFalse,
		metav1.ConditionUnknown,
	} {
		status := status
		t.Run(string(status), func(t *testing.T) {
			t.Parallel()

			snapshot := fixture(t)
			snapshot.TrafficMap.Status.Published = false
			snapshot.TrafficMap.Status.ObservedTrafficMapGeneration = 4
			condition := routingCondition("Published", privateReason, 4)
			condition.Status = status
			snapshot.TrafficMap.Status.Conditions = []metav1.Condition{condition}

			reportValue, err := ProjectEndpoint(snapshot, fixtureClock)
			require.NoError(t, err)
			require.Equal(t, v.PlacementValue("OtherReportedReason"), reportValue.Content.Routing.Published.Reason)
			require.Equal(t, v.PlacementValue(status), reportValue.Content.Routing.Published.Status)
			require.Equal(t, v.PlacementValue("Invalid"), reportValue.Content.Routing.Acknowledgement)
			require.Equal(t, v.PlacementValue("Invalid"), reportValue.Content.Routing.AcknowledgementSource.Freshness)
			require.Equal(t, v.PlacementValue("ConflictingAcknowledgement"), reportValue.Content.Routing.AcknowledgementSource.Reason)

			outputs := placementEndpointOutputs(t, reportValue)
			for name, output := range outputs {
				require.Contains(t, output, "OtherReportedReason", name)
				require.Contains(t, output, "Invalid", name)
				require.NotContains(t, output, privateReason, name)
			}
			for _, name := range []string{"json", "yaml"} {
				require.Contains(t, outputs[name], "ConflictingAcknowledgement", name)
			}
		})
	}
}

func TestRoutingConditionsProjectDirectFieldsAndMissingState(t *testing.T) {
	t.Parallel()

	snapshot := fixture(t)
	snapshot.TrafficMap.Status.Conditions = []metav1.Condition{
		routingCondition("OverrideActive", "OverridesApplied", 4),
		routingCondition("CapacityFallback", "EndpointCapacityUnavailable", 4),
	}
	reportValue, err := ProjectEndpoint(snapshot, fixtureClock)
	require.NoError(t, err)
	require.Equal(t, v.PlacementValue("OverridesApplied"), reportValue.Content.Routing.OverrideActive.Reason)
	require.Equal(t, v.PlacementValue("EndpointCapacityUnavailable"), reportValue.Content.Routing.CapacityFallback.Reason)
	require.Equal(t, v.PlacementValue("OverridesApplied"), placementConditionByType(t, reportValue.Content.Conditions, "OverrideActive").Reason)
	require.Equal(t, v.PlacementValue("EndpointCapacityUnavailable"), placementConditionByType(t, reportValue.Content.Conditions, "CapacityFallback").Reason)

	missing := fixture(t)
	missing.TrafficMap.Status.Conditions = nil
	missingReport, err := ProjectEndpoint(missing, fixtureClock)
	require.NoError(t, err)
	for _, condition := range []v.PlacementCondition{
		missingReport.Content.Routing.Routable,
		missingReport.Content.Routing.Published,
		missingReport.Content.Routing.OverrideActive,
		missingReport.Content.Routing.CapacityFallback,
	} {
		require.Equal(t, v.PlacementValue("Unknown"), condition.Status)
		require.Equal(t, v.PlacementValue("NotRecorded"), condition.Reason)
		require.NotEqual(t, v.PlacementValue("False"), condition.Status)
	}

	missing.TrafficMap = nil
	noMapReport, err := ProjectEndpoint(missing, fixtureClock)
	require.NoError(t, err)
	require.Equal(t, v.PlacementValue("Unknown"), noMapReport.Content.Routing.OverrideActive.Status)
	require.Equal(t, v.PlacementValue("NotRecorded"), noMapReport.Content.Routing.OverrideActive.Reason)
	require.Equal(t, v.PlacementValue("Unknown"), noMapReport.Content.Routing.CapacityFallback.Status)
	require.Equal(t, v.PlacementValue("NotRecorded"), noMapReport.Content.Routing.CapacityFallback.Reason)
}

func TestRoutingConditionsKeepIndependentFreshness(t *testing.T) {
	t.Parallel()

	snapshot := fixture(t)
	snapshot.TrafficMap.Status.Conditions = []metav1.Condition{
		routingCondition("OverrideActive", "OverridesPending", 3),
		routingCondition("CapacityFallback", "EndpointCapacityAvailable", 4),
	}
	reportValue, err := ProjectEndpoint(snapshot, fixtureClock)
	require.NoError(t, err)
	require.Equal(t, v.PlacementValue("Current"), reportValue.Content.Routing.Source.Freshness)
	require.Equal(t, v.PlacementValue("Stale"), reportValue.Content.Routing.OverrideActive.Source.Freshness)
	require.Equal(t, v.PlacementValue("Current"), reportValue.Content.Routing.CapacityFallback.Source.Freshness)
	require.Equal(t, v.PlacementValue("OverridesPending"), reportValue.Content.Routing.OverrideActive.Reason)
	require.Equal(t, v.PlacementValue("EndpointCapacityAvailable"), reportValue.Content.Routing.CapacityFallback.Reason)
}

func TestRoutingConditionsBudgetInvalidatesOnlyRequestedRules(t *testing.T) {
	t.Parallel()

	snapshot := fixture(t)
	snapshot.TrafficMap.Status.Conditions = make([]metav1.Condition, 65)
	for i := range snapshot.TrafficMap.Status.Conditions {
		snapshot.TrafficMap.Status.Conditions[i] = routingCondition(fmt.Sprintf("Evidence%02d", i), "Reported", 4)
	}
	reportValue, err := ProjectEndpoint(snapshot, fixtureClock)
	require.NoError(t, err)
	require.Equal(t, v.PlacementPreview{State: "BudgetExceeded", Total: 65}, reportValue.Content.ConditionPreview)
	require.Len(t, reportValue.Content.Conditions, 4)
	for _, conditionType := range []v.PlacementValue{"Routable", "Published", "OverrideActive", "CapacityFallback"} {
		condition := placementConditionByType(t, reportValue.Content.Conditions, string(conditionType))
		require.Equal(t, v.PlacementValue("Unknown"), condition.Status)
		require.Equal(t, v.PlacementValue("BudgetExceeded"), condition.Reason)
	}
	for _, condition := range reportValue.Content.Conditions {
		require.NotEqual(t, v.PlacementValue("Ready"), condition.Type)
	}
	require.Equal(t, []v.PlacementIssue{{Group: "TrafficMapConditions", Code: "BudgetExceeded", Count: 1}}, reportValue.Content.Issues)
	require.Len(t, reportValue.Content.Entries, 2)
}

func TestRoutingConditionsExactDuplicatesDeduplicate(t *testing.T) {
	t.Parallel()

	override := routingCondition("OverrideActive", "NoOverrides", 4)
	capacity := routingCondition("CapacityFallback", "NoCapacityTargets", 4)
	snapshot := fixture(t)
	snapshot.TrafficMap.Status.Conditions = []metav1.Condition{override, capacity, override, capacity}
	reportValue, err := ProjectEndpoint(snapshot, fixtureClock)
	require.NoError(t, err)
	require.Equal(t, v.PlacementPreview{State: "Validated", Total: 4, Kept: 2}, reportValue.Content.ConditionPreview)
	require.Len(t, reportValue.Content.Conditions, 2)
	require.Equal(t, v.PlacementValue("NoOverrides"), reportValue.Content.Routing.OverrideActive.Reason)
	require.Equal(t, v.PlacementValue("NoCapacityTargets"), reportValue.Content.Routing.CapacityFallback.Reason)
	require.Empty(t, reportValue.Content.Issues)
}

func TestRoutingConditionsConflictsAreOrderIndependent(t *testing.T) {
	t.Parallel()

	first := routingCondition("OverrideActive", "OverridesApplied", 4)
	first.Message = "private-first-message"
	second := routingCondition("OverrideActive", "OverridesPending", 4)
	second.Status = metav1.ConditionFalse
	second.Message = "private-second-message"
	capacity := routingCondition("CapacityFallback", "EndpointCapacityAvailable", 4)

	var canonical []byte
	for order, conditions := range [][]metav1.Condition{
		{first, capacity, second},
		{second, capacity, first},
	} {
		snapshot := fixture(t)
		snapshot.TrafficMap.Status.Conditions = conditions
		reportValue, err := ProjectEndpoint(snapshot, fixtureClock)
		require.NoError(t, err)
		require.Equal(t, v.PlacementPreview{State: "MalformedPayload", Total: 3, Kept: 2}, reportValue.Content.ConditionPreview)
		require.Equal(t, v.PlacementValue("Unknown"), reportValue.Content.Routing.OverrideActive.Status)
		require.Equal(t, v.PlacementValue("ConflictingDuplicates"), reportValue.Content.Routing.OverrideActive.Reason)
		require.Equal(t, v.PlacementValue("EndpointCapacityAvailable"), reportValue.Content.Routing.CapacityFallback.Reason)
		require.Equal(t, []v.PlacementIssue{{Group: "TrafficMapConditions", Code: "MalformedPayload", Count: 1}}, reportValue.Content.Issues)
		require.Len(t, reportValue.Content.Entries, 2)

		encoded, marshalErr := json.Marshal(reportValue)
		require.NoError(t, marshalErr)
		require.NotContains(t, string(encoded), "private-first-message")
		require.NotContains(t, string(encoded), "private-second-message")
		if order == 0 {
			canonical = encoded
			continue
		}
		require.True(t, bytes.Equal(canonical, encoded), "canonical JSON changed with duplicate order:\n%s\n%s", canonical, encoded)
	}
}

func TestRoutingConditionsConflictDominatesMalformedInEveryOrder(t *testing.T) {
	t.Parallel()

	permutations := [][3]int{
		{0, 1, 2},
		{0, 2, 1},
		{1, 0, 2},
		{1, 2, 0},
		{2, 0, 1},
		{2, 1, 0},
	}
	tests := []struct {
		name      string
		malformed func(*metav1.Condition)
	}{
		{
			name: "invalid condition shape",
			malformed: func(condition *metav1.Condition) {
				condition.Status = metav1.ConditionFalse
				condition.Reason = ""
			},
		},
		{
			name: "oversized condition field",
			malformed: func(condition *metav1.Condition) {
				condition.Status = metav1.ConditionFalse
				condition.Reason = "OverridesPending"
				condition.Message = strings.Repeat("private-oversized-message", 200)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			valid := routingCondition("OverrideActive", "OverridesApplied", 4)
			malformed := routingCondition("OverrideActive", "OverridesPending", 4)
			test.malformed(&malformed)
			items := []metav1.Condition{valid, malformed, malformed}
			capacity := routingCondition("CapacityFallback", "EndpointCapacityAvailable", 4)

			var canonical []byte
			for index, permutation := range permutations {
				snapshot := fixture(t)
				snapshot.TrafficMap.Status.Conditions = []metav1.Condition{
					items[permutation[0]],
					items[permutation[1]],
					items[permutation[2]],
					capacity,
				}
				reportValue, err := ProjectEndpoint(snapshot, fixtureClock)
				require.NoError(t, err)
				require.Equal(t, v.PlacementPreview{State: "MalformedPayload", Total: 4, Kept: 2}, reportValue.Content.ConditionPreview)
				require.Equal(t, v.PlacementValue("Unknown"), reportValue.Content.Routing.OverrideActive.Status)
				require.Equal(t, v.PlacementValue("ConflictingDuplicates"), reportValue.Content.Routing.OverrideActive.Reason)
				require.Equal(t, v.PlacementValue("EndpointCapacityAvailable"), reportValue.Content.Routing.CapacityFallback.Reason)
				require.Equal(t, []v.PlacementIssue{{Group: "TrafficMapConditions", Code: "MalformedPayload", Count: 1}}, reportValue.Content.Issues)
				require.Len(t, reportValue.Content.Entries, 2)

				encoded, marshalErr := json.Marshal(reportValue)
				require.NoError(t, marshalErr)
				require.NotContains(t, string(encoded), "private-oversized-message")
				if index == 0 {
					canonical = encoded
					continue
				}
				require.True(t, bytes.Equal(canonical, encoded), "permutation %v changed canonical JSON:\n%s\n%s", permutation, canonical, encoded)
			}
		})
	}
}

func TestRoutingConditionsWorkloadClusterProjectsOnlyReady(t *testing.T) {
	t.Parallel()

	snapshot := fixture(t)
	snapshot.WorkloadClusters[0].Status.Conditions = append(
		snapshot.WorkloadClusters[0].Status.Conditions,
		routingCondition("Routable", "TrafficDrain", 3),
		routingCondition("OverrideActive", "OverridesApplied", 3),
	)
	reportValue, err := ProjectExplain(snapshot, fixtureClock)
	require.NoError(t, err)
	require.Equal(t, v.PlacementPreview{State: "Validated", Total: 3, Kept: 1}, reportValue.Content.Clusters[0].ConditionPreview)
	require.Equal(t, v.PlacementValue("Ready"), reportValue.Content.Clusters[0].ReportedReady.Type)
	require.Equal(t, v.PlacementValue("ProbeFailedRetrying"), reportValue.Content.Clusters[0].ReportedReady.Reason)

	snapshot = fixture(t)
	snapshot.WorkloadClusters[0].Status.Conditions[0].Reason = "TrafficDrain"
	reportValue, err = ProjectExplain(snapshot, fixtureClock)
	require.NoError(t, err)
	require.Equal(t, v.PlacementValue("OtherReportedReason"), reportValue.Content.Clusters[0].ReportedReady.Reason)
}

func TestRouteEvidenceProjectsClosedValuesInEveryFormat(t *testing.T) {
	t.Parallel()

	digest := "sha256:" + strings.Repeat("a", 64)
	snapshot := fixture(t)
	entry := &snapshot.TrafficMap.Spec.Entries[0]
	entry.DrainRefs = []string{"maintenance-a", "drain-a"}
	entry.Probe = &ome.TrafficMapProbe{
		PolicyDigest: digest,
		Result:       ome.ProbeResultFailing,
		Gated:        true,
		Message:      "https://private-user:private-token@private.invalid",
	}
	entry.Capacity.FallbackReason = "capacity endpoint unreachable: GET https://private-user:private-token@private.invalid"

	reportValue, err := ProjectEndpoint(snapshot, fixtureClock)
	require.NoError(t, err)
	route := placementRouteByCluster(t, reportValue.Content.Entries, "demo-a")
	require.Equal(t, []string{"drain-a", "maintenance-a"}, route.DrainRefs)
	require.Equal(t, v.PlacementPreview{State: "Validated", Total: 2, Kept: 2}, route.DrainPreview)
	require.Equal(t, digest, route.Probe.PolicyDigest)
	require.Equal(t, v.PlacementValue("Reported"), route.Probe.PolicyDigestState)
	require.True(t, route.Probe.Gated)
	require.Equal(t, v.PlacementValue("Reported"), route.Probe.GatedState)
	require.Equal(t, v.PlacementValue("Unreachable"), route.Capacity.FallbackReason)
	require.Empty(t, reportValue.Content.Issues)

	outputs := placementEndpointOutputs(t, reportValue)
	for name, output := range outputs {
		require.Contains(t, output, "Unreachable", name)
		require.NotContains(t, output, "private-user", name)
		require.NotContains(t, output, "private-token", name)
		require.NotContains(t, output, "private.invalid", name)
		require.NotContains(t, output, "private-condition-message", name)
	}
	require.Contains(t, outputs["table"], "drains=2; gate=True; fallback=Unreachable")
	require.Contains(t, outputs["wide"], "drain-a, maintenance-a")
	require.Contains(t, outputs["wide"], "True (Reported)")
	require.Contains(t, outputs["json"], digest)
	require.Contains(t, outputs["yaml"], digest)
}

func TestTrafficMapWeightBoundsFailClosed(t *testing.T) {
	t.Parallel()

	t.Run("maximum remains valid", func(t *testing.T) {
		t.Parallel()
		snapshot := fixture(t)
		snapshot.TrafficMap.Spec.Entries[0].Weight = 1_000_000

		reportValue, err := ProjectEndpoint(snapshot, fixtureClock)
		require.NoError(t, err)
		require.Equal(t, v.PlacementPreview{State: "Validated", Total: 2, Kept: 2}, reportValue.Content.Routing.EntryPreview)
		route := placementRouteByCluster(t, reportValue.Content.Entries, "demo-a")
		require.Equal(t, int32(1_000_000), route.Weight)
	})

	t.Run("above maximum is rejected", func(t *testing.T) {
		t.Parallel()
		snapshot := fixture(t)
		snapshot.TrafficMap.Spec.Entries[0].Weight = 1_000_001

		reportValue, err := ProjectEndpoint(snapshot, fixtureClock)
		require.NoError(t, err)
		require.Equal(t, v.PlacementPreview{State: "MalformedPayload", Total: 2}, reportValue.Content.Routing.EntryPreview)
		require.NotNil(t, reportValue.Content.Entries)
		require.Empty(t, reportValue.Content.Entries)
		for name, output := range placementEndpointOutputs(t, reportValue) {
			require.Contains(t, output, "MalformedPayload", name)
		}
	})
}

func TestDrainRefsBoundedPrivateAndDeterministic(t *testing.T) {
	t.Parallel()

	t.Run("retains sorted prefix after complete validation", func(t *testing.T) {
		t.Parallel()
		refs := make([]string, 17)
		for i := range refs {
			refs[i] = fmt.Sprintf("drain-%02d", 16-i)
		}
		want := append([]string{}, refs...)
		sort.Strings(want)
		want = want[:16]
		projected, preview := projectDrainRefs(refs)
		require.Equal(t, want, projected)
		require.Equal(t, v.PlacementPreview{State: "Validated", Total: 17, Kept: 16, Truncated: true}, preview)

		snapshot := fixture(t)
		snapshot.TrafficMap.Spec.Entries[0].DrainRefs = refs
		reportValue, err := ProjectEndpoint(snapshot, fixtureClock)
		require.NoError(t, err)
		route := placementRouteByCluster(t, reportValue.Content.Entries, "demo-a")
		require.Equal(t, want, route.DrainRefs)
		require.Equal(t, v.PlacementPreview{State: "Validated", Total: 17, Kept: 16, Truncated: true}, route.DrainPreview)
		require.Empty(t, reportValue.Content.Issues)
	})

	t.Run("rejects source budget without erasing route", func(t *testing.T) {
		t.Parallel()
		refs := make([]string, 65)
		for i := range refs {
			refs[i] = fmt.Sprintf("drain-%02d", i)
		}
		projected, preview := projectDrainRefs(refs)
		require.NotNil(t, projected)
		require.Empty(t, projected)
		require.Equal(t, v.PlacementPreview{State: "BudgetExceeded", Total: 65, Truncated: true}, preview)
		snapshot := fixture(t)
		snapshot.TrafficMap.Spec.Entries[0].DrainRefs = refs
		reportValue, err := ProjectEndpoint(snapshot, fixtureClock)
		require.NoError(t, err)
		route := placementRouteByCluster(t, reportValue.Content.Entries, "demo-a")
		require.NotNil(t, route.DrainRefs)
		require.Empty(t, route.DrainRefs)
		require.Equal(t, v.PlacementPreview{State: "BudgetExceeded", Total: 65, Truncated: true}, route.DrainPreview)
		require.Equal(t, v.PlacementValue("Present"), route.Address.State)
		require.Equal(t, int32(1), route.Weight)
		require.Equal(t, []v.PlacementIssue{{Group: "DrainRefs", Code: "BudgetExceeded", Count: 1}}, reportValue.Content.Issues)
	})

	invalidCases := []struct {
		name       string
		refs       []string
		leakMarker string
	}{
		{"duplicate", []string{"drain-a", "drain-a"}, "drain-a"},
		{"invalid utf8", []string{string([]byte("private-utf8-\xff"))}, "private-utf8"},
		{"control", []string{"private-control\nvalue"}, "private-control"},
		{"leading whitespace", []string{" private-leading"}, "private-leading"},
		{"trailing whitespace", []string{"private-trailing "}, "private-trailing"},
		{"over byte budget", []string{"private-over-" + strings.Repeat("a", 129)}, "private-over"},
		{"credential shaped", []string{"sk-" + strings.Repeat("a", 24)}, "sk-"},
		{"URL userinfo", []string{"https://private-user:private-token@private.invalid/drain"}, "private-user"},
		{"URL username userinfo", []string{"https://private-user@private.invalid/drain"}, "private-user"},
		{"escaped URL username userinfo", []string{"https://private%2Duser@private.invalid/drain"}, "private%2Duser"},
		{"escaped-colon URL userinfo", []string{"https://private-user%3Aprivate-token@private.invalid/drain"}, "private-user"},
		{"schemeless URL userinfo", []string{"private-user:private-token@private.invalid/drain"}, "private-user"},
		{"schemeless escaped-colon URL userinfo", []string{"private-user%3Aprivate-token@private.invalid/drain"}, "private-user"},
		{"schemeless escaped lowercase dot host", []string{"alice:hunter2@example%2ecom"}, "hunter2"},
		{"schemeless escaped uppercase dot host", []string{"alice:hunter2@example%2Ecom"}, "hunter2"},
		{"schemeless escaped host colon", []string{"alice:hunter2@example%3A8443"}, "hunter2"},
		{"schemeless escaped IPv6 host", []string{"alice:hunter2@%5B2001%3Adb8%3A%3A1%5D"}, "hunter2"},
		{"schemeless escaped userinfo and host", []string{"alice%3Ahunter2@example%2Ecom"}, "hunter2"},
		{"schemeless escaped separator and host", []string{"alice%3Ahunter2%40example%2Ecom"}, "hunter2"},
		{"schemeless doubly escaped authority", []string{"alice%253Ahunter2%2540example%252Ecom"}, "hunter2"},
		{"schemeless malformed-escape URL userinfo", []string{"private-user%zzprivate-token@private.invalid/drain"}, "private-user"},
		{"schemeless URL userinfo with path", []string{"private-user:private-token@drain-a/path"}, "private-user"},
		{"schemeless URL userinfo with query", []string{"private-user:private-token@drain-a?scope"}, "private-user"},
		{"schemeless URL userinfo with fragment", []string{"private-user:private-token@drain-a#fragment"}, "private-user"},
		{"schemeless URL userinfo with later delimiter", []string{"private-user:private-token@private.invalid://drain"}, "private-user"},
		{"scheme-relative parse-error URL userinfo", []string{"//private-user:private-token@private.invalid/%zz"}, "private-user"},
		{"parse-error URL userinfo", []string{"https://private-user:private-token@private.invalid/%zz"}, "private-user"},
		{"single escaped authority with malformed path", []string{"https://alice%3Ahunter2%40example%2Ecom/%zz"}, "hunter2"},
		{"double escaped authority with malformed path", []string{"https://alice%253Ahunter2%2540example%252Ecom/%zz"}, "hunter2"},
		{"triple escaped mixed-case authority with malformed path", []string{"HtTpS://ALICE%25253aHUNTER2%252540EXAMPLE%25252eCOM/path%zZ"}, "HUNTER2"},
		{"single escaped authority with malformed query", []string{"https://alice%3Ahunter2%40example%2Ecom?next=%zz"}, "hunter2"},
		{"double escaped authority with malformed query", []string{"https://alice%253Ahunter2%2540example%252Ecom?next=%zz"}, "hunter2"},
		{"triple escaped mixed-case authority with malformed fragment", []string{"HtTpS://ALICE%25253aHUNTER2%252540EXAMPLE%25252eCOM#next-%zZ"}, "HUNTER2"},
		{"malformed path escape", []string{"https://example.invalid/%zz"}, "%zz"},
		{"malformed query escape", []string{"https://example.invalid?next=%zz"}, "%zz"},
		{"malformed fragment escape", []string{"https://example.invalid#next-%zz"}, "%zz"},
	}
	for _, test := range invalidCases {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			snapshot := fixture(t)
			snapshot.TrafficMap.Spec.Entries[0].DrainRefs = test.refs
			reportValue, err := ProjectEndpoint(snapshot, fixtureClock)
			require.NoError(t, err)
			route := placementRouteByCluster(t, reportValue.Content.Entries, "demo-a")
			require.NotNil(t, route.DrainRefs)
			require.Empty(t, route.DrainRefs)
			require.Equal(t, v.PlacementPreview{State: "MalformedPayload", Total: len(test.refs)}, route.DrainPreview)
			require.Equal(t, []v.PlacementIssue{{Group: "DrainRefs", Code: "MalformedPayload", Count: 1}}, reportValue.Content.Issues)
			for name, output := range placementEndpointOutputs(t, reportValue) {
				require.NotContains(t, output, test.leakMarker, name)
			}
		})
	}

	validCases := []struct {
		name string
		ref  string
	}{
		{"ordinary at identity", "operator@drain-a"},
		{"double escaped ordinary at identity", "operator%2540drain-a"},
		{"open ticket identity", "ticket:operator@drain-a"},
		{"URL port and at path", "https://example.invalid:8443/operator@drain"},
		{"URL port and double escaped at path", "https://example.invalid:8443/operator%2540drain"},
		{"IPv6 URL port and at path", "https://[2001:db8::1]:8443/operator@drain"},
	}
	for _, test := range validCases {
		t.Run(test.name+" remains valid", func(t *testing.T) {
			t.Parallel()
			snapshot := fixture(t)
			snapshot.TrafficMap.Spec.Entries[0].DrainRefs = []string{test.ref}
			reportValue, err := ProjectEndpoint(snapshot, fixtureClock)
			require.NoError(t, err)
			route := placementRouteByCluster(t, reportValue.Content.Entries, "demo-a")
			require.Equal(t, []string{test.ref}, route.DrainRefs)
			require.Equal(t, v.PlacementPreview{State: "Validated", Total: 1, Kept: 1}, route.DrainPreview)
			require.Empty(t, reportValue.Content.Issues)
		})
	}
}

func TestEncodedURLDrainRefCredentialsRemainPrivate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		ref     string
		private []string
	}{
		{
			name: "double encoded schemed authority",
			ref:  "https://alice%253Ahunter2%2540example%252Ecom",
			private: []string{
				"https://alice%253Ahunter2%2540example%252Ecom",
				"alice%3Ahunter2%40example%2Ecom",
				"alice:hunter2@example.com",
				"hunter2",
			},
		},
		{
			name: "mixed case double encoded authority",
			ref:  "HtTpS://ALICE%253aHUNTER2%2540EXAMPLE%252eCOM",
			private: []string{
				"HtTpS://ALICE%253aHUNTER2%2540EXAMPLE%252eCOM",
				"ALICE%3aHUNTER2%40EXAMPLE%2eCOM",
				"ALICE:HUNTER2@EXAMPLE.COM",
				"HUNTER2",
			},
		},
		{
			name: "double encoded host port",
			ref:  "https://alice%253Ahunter2%2540example%253A8443",
			private: []string{
				"https://alice%253Ahunter2%2540example%253A8443",
				"alice%3Ahunter2%40example%3A8443",
				"alice:hunter2@example:8443",
				"hunter2",
			},
		},
		{
			name: "double encoded IPv6 authority",
			ref:  "https://alice%253Ahunter2%2540%255B2001%253Adb8%253A%253A1%255D",
			private: []string{
				"https://alice%253Ahunter2%2540%255B2001%253Adb8%253A%253A1%255D",
				"alice%3Ahunter2%40%5B2001%3Adb8%3A%3A1%5D",
				"alice:hunter2@[2001:db8::1]",
				"hunter2",
			},
		},
		{
			name: "triple encoded schemed authority",
			ref:  "https://alice%25253Ahunter2%252540example%25252Ecom",
			private: []string{
				"https://alice%25253Ahunter2%252540example%25252Ecom",
				"alice%253Ahunter2%2540example%252Ecom",
				"alice%3Ahunter2%40example%2Ecom",
				"alice:hunter2@example.com",
				"hunter2",
			},
		},
		{
			name: "double encoded scheme relative authority",
			ref:  "//alice%253Ahunter2%2540example%252Ecom",
			private: []string{
				"//alice%253Ahunter2%2540example%252Ecom",
				"alice%3Ahunter2%40example%2Ecom",
				"alice:hunter2@example.com",
				"hunter2",
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			snapshot := fixture(t)
			snapshot.TrafficMap.Spec.Entries[0].DrainRefs = []string{test.ref}
			reportValue, err := ProjectEndpoint(snapshot, fixtureClock)
			require.NoError(t, err)
			route := placementRouteByCluster(t, reportValue.Content.Entries, "demo-a")
			require.NotNil(t, route.DrainRefs)
			require.Empty(t, route.DrainRefs)
			require.Equal(t, v.PlacementPreview{State: "MalformedPayload", Total: 1}, route.DrainPreview)
			require.Equal(t, []v.PlacementIssue{{Group: "DrainRefs", Code: "MalformedPayload", Count: 1}}, reportValue.Content.Issues)

			for name, output := range placementEndpointOutputs(t, reportValue) {
				for _, private := range test.private {
					require.NotContains(t, output, private, "%s leaked %q", name, private)
				}
			}
		})
	}
}

func TestDrainRefsSetOrderDoesNotConflictDuplicateEntries(t *testing.T) {
	t.Parallel()

	snapshot := fixture(t)
	entry := *snapshot.TrafficMap.Spec.Entries[0].DeepCopy()
	entry.DrainRefs = []string{"maintenance-a", "drain-a"}
	duplicate := *entry.DeepCopy()
	duplicate.DrainRefs = []string{"drain-a", "maintenance-a"}
	snapshot.TrafficMap.Spec.Entries = []ome.TrafficMapEntry{entry, duplicate}

	reportValue, err := ProjectEndpoint(snapshot, fixtureClock)
	require.NoError(t, err)
	require.Equal(t, v.PlacementPreview{State: "Validated", Total: 1, Kept: 1}, reportValue.Content.Routing.EntryPreview)
	require.Len(t, reportValue.Content.Entries, 1)
	require.Equal(t, []string{"drain-a", "maintenance-a"}, reportValue.Content.Entries[0].DrainRefs)
	require.Empty(t, reportValue.Content.Issues)
}

func TestDrainRefsDifferingBeyondRetainedWindowConflict(t *testing.T) {
	t.Parallel()

	common := make([]string, 16)
	for i := range common {
		common[i] = fmt.Sprintf("drain-%02d", i)
	}
	snapshot := fixture(t)
	first := *snapshot.TrafficMap.Spec.Entries[0].DeepCopy()
	first.DrainRefs = append(append([]string{}, common...), "private-tail-a")
	second := *first.DeepCopy()
	second.DrainRefs = append(append([]string{}, common...), "private-tail-b")

	var canonical []byte
	for index, entries := range [][]ome.TrafficMapEntry{{first, second}, {second, first}} {
		snapshot := fixture(t)
		snapshot.TrafficMap.Spec.Entries = entries
		reportValue, err := ProjectEndpoint(snapshot, fixtureClock)
		require.NoError(t, err)
		require.Equal(t, v.PlacementPreview{State: "Validated", Total: 1, Kept: 1}, reportValue.Content.Routing.EntryPreview)
		require.Len(t, reportValue.Content.Entries, 1)
		route := reportValue.Content.Entries[0]
		require.Equal(t, v.PlacementValue("Present"), route.Address.State)
		require.Equal(t, int32(1), route.Weight)
		require.NotNil(t, route.DrainRefs)
		require.Empty(t, route.DrainRefs)
		require.Equal(t, v.PlacementPreview{State: "ConflictingDuplicates", Total: 17, Truncated: true}, route.DrainPreview)
		require.Equal(t, []v.PlacementIssue{{Group: "DrainRefs", Code: "ConflictingDuplicates", Count: 1}}, reportValue.Content.Issues)
		require.Equal(t, v.PlacementValue("Passing"), route.Probe.Result)
		require.NotNil(t, route.Capacity)

		for name, output := range placementEndpointOutputs(t, reportValue) {
			require.NotContains(t, output, "private-tail-a", name)
			require.NotContains(t, output, "private-tail-b", name)
		}
		encoded, marshalErr := json.Marshal(reportValue)
		require.NoError(t, marshalErr)
		if index == 0 {
			canonical = encoded
			continue
		}
		require.True(t, bytes.Equal(canonical, encoded), "tail-conflict order changed canonical JSON:\n%s\n%s", canonical, encoded)
	}
}

func TestNilAndEmptyDrainRefsAreSameSet(t *testing.T) {
	t.Parallel()

	snapshot := fixture(t)
	entry := *snapshot.TrafficMap.Spec.Entries[0].DeepCopy()
	entry.DrainRefs = nil
	duplicate := *entry.DeepCopy()
	duplicate.DrainRefs = []string{}
	snapshot.TrafficMap.Spec.Entries = []ome.TrafficMapEntry{entry, duplicate}

	reportValue, err := ProjectEndpoint(snapshot, fixtureClock)
	require.NoError(t, err)
	require.Equal(t, v.PlacementPreview{State: "Validated", Total: 1, Kept: 1}, reportValue.Content.Routing.EntryPreview)
	require.Len(t, reportValue.Content.Entries, 1)
	require.NotNil(t, reportValue.Content.Entries[0].DrainRefs)
	require.Empty(t, reportValue.Content.Entries[0].DrainRefs)
	require.Equal(t, v.PlacementPreview{State: "Validated"}, reportValue.Content.Entries[0].DrainPreview)
	require.Empty(t, reportValue.Content.Issues)
}

func TestDuplicateEntryNestedEvidenceConflictsAreIsolatedAndOrderIndependent(t *testing.T) {
	t.Parallel()

	permutations := [][3]int{
		{0, 1, 2},
		{0, 2, 1},
		{1, 0, 2},
		{1, 2, 0},
		{2, 0, 1},
		{2, 1, 0},
	}
	snapshot := fixture(t)
	first := *snapshot.TrafficMap.Spec.Entries[0].DeepCopy()
	first.DrainRefs = []string{"private-drain-a"}
	first.Probe.PolicyDigest = "sha256:" + strings.Repeat("a", 64)
	first.Probe.Gated = false
	first.Capacity.FallbackReason = "no capacity report yet"
	second := *first.DeepCopy()
	second.DrainRefs = []string{"private-drain-b"}
	second.Probe.PolicyDigest = "sha256:" + strings.Repeat("b", 64)
	second.Probe.Gated = true
	second.Capacity.FallbackReason = "capacity endpoint unreachable: https://private-user:private-token@private.invalid"
	third := *first.DeepCopy()
	items := []ome.TrafficMapEntry{first, second, third}

	var canonical []byte
	for index, permutation := range permutations {
		snapshot := fixture(t)
		snapshot.TrafficMap.Spec.Entries = []ome.TrafficMapEntry{
			items[permutation[0]],
			items[permutation[1]],
			items[permutation[2]],
		}
		reportValue, err := ProjectEndpoint(snapshot, fixtureClock)
		require.NoError(t, err)
		require.Equal(t, v.PlacementPreview{State: "Validated", Total: 1, Kept: 1}, reportValue.Content.Routing.EntryPreview)
		require.Len(t, reportValue.Content.Entries, 1)
		route := reportValue.Content.Entries[0]
		require.Equal(t, "demo-a", route.Cluster)
		require.Equal(t, v.PlacementValue("Present"), route.Address.State)
		require.Equal(t, int32(1), route.Weight)
		require.False(t, route.Healthy)
		require.NotNil(t, route.DrainRefs)
		require.Empty(t, route.DrainRefs)
		require.Equal(t, v.PlacementPreview{State: "ConflictingDuplicates", Total: 1}, route.DrainPreview)
		require.Equal(t, v.PlacementValue("Invalid"), route.Probe.Result)
		require.Empty(t, route.Probe.PolicyDigest)
		require.Equal(t, v.PlacementValue("Unavailable"), route.Probe.PolicyDigestState)
		require.False(t, route.Probe.Gated)
		require.Equal(t, v.PlacementValue("Unavailable"), route.Probe.GatedState)
		require.Equal(t, v.PlacementValue("ConflictingDuplicates"), route.Probe.Source.Reason)
		require.NotNil(t, route.Capacity)
		require.Equal(t, v.PlacementValue("ConflictingDuplicates"), route.Capacity.FallbackReason)
		require.Equal(t, []v.PlacementIssue{
			{Group: "CapacityFallbackReasons", Code: "ConflictingDuplicates", Count: 1},
			{Group: "DrainRefs", Code: "ConflictingDuplicates", Count: 1},
			{Group: "RecordedProbes", Code: "ConflictingDuplicates", Count: 1},
		}, reportValue.Content.Issues)

		for name, output := range placementEndpointOutputs(t, reportValue) {
			for _, private := range []string{"private-drain-a", "private-drain-b", "private-user", "private-token", "private.invalid", first.Probe.PolicyDigest, second.Probe.PolicyDigest} {
				require.NotContains(t, output, private, "%s leaked %q", name, private)
			}
		}
		encoded, marshalErr := json.Marshal(reportValue)
		require.NoError(t, marshalErr)
		if index == 0 {
			canonical = encoded
			continue
		}
		require.True(t, bytes.Equal(canonical, encoded), "nested conflict permutation %v changed canonical JSON:\n%s\n%s", permutation, canonical, encoded)
	}
}

func TestDuplicateEntryOverCapEvidenceCanonicalizesWithoutRawComparison(t *testing.T) {
	t.Parallel()

	snapshot := fixture(t)
	first := *snapshot.TrafficMap.Spec.Entries[0].DeepCopy()
	first.DrainRefs = make([]string, 65)
	for i := range first.DrainRefs {
		first.DrainRefs[i] = fmt.Sprintf("private-first-drain-%02d", i)
	}
	first.Probe.PolicyDigest = "private-first-policy-" + strings.Repeat("a", 80)
	first.Probe.Gated = false
	first.Capacity.FallbackReason = "private-first-fallback-" + strings.Repeat("a", 4097)
	second := *first.DeepCopy()
	second.DrainRefs = make([]string, 66)
	for i := range second.DrainRefs {
		second.DrainRefs[i] = fmt.Sprintf("private-second-drain-%02d", i)
	}
	second.Probe.PolicyDigest = "private-second-policy-" + strings.Repeat("b", 81)
	second.Probe.Gated = true
	second.Capacity.FallbackReason = "private-second-fallback-" + strings.Repeat("b", 4098)

	var canonical []byte
	for index, entries := range [][]ome.TrafficMapEntry{{first, second}, {second, first}} {
		snapshot := fixture(t)
		snapshot.TrafficMap.Spec.Entries = entries
		reportValue, err := ProjectEndpoint(snapshot, fixtureClock)
		require.NoError(t, err)
		require.Equal(t, v.PlacementPreview{State: "Validated", Total: 1, Kept: 1}, reportValue.Content.Routing.EntryPreview)
		require.Len(t, reportValue.Content.Entries, 1)
		route := reportValue.Content.Entries[0]
		require.Equal(t, "demo-a", route.Cluster)
		require.Equal(t, v.PlacementValue("Present"), route.Address.State)
		require.Equal(t, int32(1), route.Weight)
		require.NotNil(t, route.DrainRefs)
		require.Empty(t, route.DrainRefs)
		require.Equal(t, v.PlacementPreview{State: "BudgetExceeded", Total: 66, Truncated: true}, route.DrainPreview)
		require.Equal(t, v.PlacementValue("Invalid"), route.Probe.Result)
		require.Equal(t, v.PlacementValue("MalformedPayload"), route.Probe.Source.Reason)
		require.NotNil(t, route.Capacity)
		require.Equal(t, v.PlacementValue("MalformedPayload"), route.Capacity.FallbackReason)
		require.Equal(t, []v.PlacementIssue{
			{Group: "CapacityFallbackReasons", Code: "MalformedPayload", Count: 1},
			{Group: "DrainRefs", Code: "BudgetExceeded", Count: 1},
			{Group: "RecordedProbes", Code: "MalformedPayload", Count: 1},
		}, reportValue.Content.Issues)

		for name, output := range placementEndpointOutputs(t, reportValue) {
			for _, private := range []string{"private-first-drain", "private-second-drain", "private-first-policy", "private-second-policy", "private-first-fallback", "private-second-fallback"} {
				require.NotContains(t, output, private, "%s leaked %q", name, private)
			}
		}
		encoded, marshalErr := json.Marshal(reportValue)
		require.NoError(t, marshalErr)
		if index == 0 {
			canonical = encoded
			continue
		}
		require.True(t, bytes.Equal(canonical, encoded), "over-cap duplicate order changed canonical JSON:\n%s\n%s", canonical, encoded)
	}
}

func TestProbePolicyAndGateStatesAreExplicitAndFailClosed(t *testing.T) {
	t.Parallel()

	t.Run("absent probe", func(t *testing.T) {
		t.Parallel()
		snapshot := fixture(t)
		snapshot.TrafficMap.Spec.Entries[0].Probe = nil
		reportValue, err := ProjectEndpoint(snapshot, fixtureClock)
		require.NoError(t, err)
		probe := placementRouteByCluster(t, reportValue.Content.Entries, "demo-a").Probe
		require.Empty(t, probe.PolicyDigest)
		require.Equal(t, v.PlacementValue("NotRecorded"), probe.PolicyDigestState)
		require.False(t, probe.Gated)
		require.Equal(t, v.PlacementValue("NotRecorded"), probe.GatedState)
	})

	t.Run("present probe with empty digest and open gate", func(t *testing.T) {
		t.Parallel()
		snapshot := fixture(t)
		snapshot.TrafficMap.Spec.Entries[0].Probe.PolicyDigest = ""
		snapshot.TrafficMap.Spec.Entries[0].Probe.Gated = false
		reportValue, err := ProjectEndpoint(snapshot, fixtureClock)
		require.NoError(t, err)
		probe := placementRouteByCluster(t, reportValue.Content.Entries, "demo-a").Probe
		require.Empty(t, probe.PolicyDigest)
		require.Equal(t, v.PlacementValue("NotRecorded"), probe.PolicyDigestState)
		require.False(t, probe.Gated)
		require.Equal(t, v.PlacementValue("Reported"), probe.GatedState)
	})

	for _, gated := range []bool{false, true} {
		t.Run(fmt.Sprintf("valid digest gated=%t", gated), func(t *testing.T) {
			t.Parallel()
			digest := "sha256:" + strings.Repeat("b", 64)
			projectedDigest, digestState, ok := projectProbePolicyDigest(digest)
			require.True(t, ok)
			require.Equal(t, digest, projectedDigest)
			require.Equal(t, v.PlacementValue("Reported"), digestState)
			snapshot := fixture(t)
			snapshot.TrafficMap.Spec.Entries[0].Probe.PolicyDigest = digest
			snapshot.TrafficMap.Spec.Entries[0].Probe.Gated = gated
			reportValue, err := ProjectEndpoint(snapshot, fixtureClock)
			require.NoError(t, err)
			probe := placementRouteByCluster(t, reportValue.Content.Entries, "demo-a").Probe
			require.Equal(t, digest, probe.PolicyDigest)
			require.Equal(t, v.PlacementValue("Reported"), probe.PolicyDigestState)
			require.Equal(t, gated, probe.Gated)
			require.Equal(t, v.PlacementValue("Reported"), probe.GatedState)
		})
	}

	malformedDigests := []struct {
		name   string
		digest string
		marker string
	}{
		{"uppercase", "sha256:" + strings.Repeat("A", 64), "AAAA"},
		{"short", "sha256:private-short", "private-short"},
		{"long", "sha256:private-long-" + strings.Repeat("a", 64), "private-long"},
		{"non hex", "sha256:private-nonhex-" + strings.Repeat("g", 64), "private-nonhex"},
	}
	for _, test := range malformedDigests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			projectedDigest, digestState, ok := projectProbePolicyDigest(test.digest)
			require.False(t, ok)
			require.Empty(t, projectedDigest)
			require.Equal(t, v.PlacementValue("Unavailable"), digestState)
			snapshot := fixture(t)
			entry := &snapshot.TrafficMap.Spec.Entries[0]
			entry.DrainRefs = []string{"drain-a"}
			entry.Probe.PolicyDigest = test.digest
			entry.Probe.Gated = true
			entry.Capacity.FallbackReason = "no capacity report yet"
			reportValue, err := ProjectEndpoint(snapshot, fixtureClock)
			require.NoError(t, err)
			route := placementRouteByCluster(t, reportValue.Content.Entries, "demo-a")
			require.Empty(t, route.Probe.PolicyDigest)
			require.Equal(t, v.PlacementValue("Unavailable"), route.Probe.PolicyDigestState)
			require.False(t, route.Probe.Gated)
			require.Equal(t, v.PlacementValue("Unavailable"), route.Probe.GatedState)
			require.Equal(t, []string{"drain-a"}, route.DrainRefs)
			require.Equal(t, v.PlacementValue("NoReport"), route.Capacity.FallbackReason)
			require.Equal(t, v.PlacementValue("Present"), route.Address.State)
			require.Equal(t, []v.PlacementIssue{{Group: "RecordedProbes", Code: "MalformedPayload", Count: 1}}, reportValue.Content.Issues)
			for name, output := range placementEndpointOutputs(t, reportValue) {
				require.NotContains(t, output, test.marker, name)
			}
		})
	}

	t.Run("future probe cannot claim policy or gate", func(t *testing.T) {
		t.Parallel()
		snapshot := fixture(t)
		entry := &snapshot.TrafficMap.Spec.Entries[0]
		entry.Probe.PolicyDigest = "sha256:" + strings.Repeat("c", 64)
		entry.Probe.Gated = true
		entry.Probe.LastProbeTime = &metav1.Time{Time: fixtureClock.Now().Add(time.Hour)}
		reportValue, err := ProjectEndpoint(snapshot, fixtureClock)
		require.NoError(t, err)
		probe := placementRouteByCluster(t, reportValue.Content.Entries, "demo-a").Probe
		require.Equal(t, v.PlacementValue("Invalid"), probe.Result)
		require.Empty(t, probe.PolicyDigest)
		require.Equal(t, v.PlacementValue("Unavailable"), probe.PolicyDigestState)
		require.False(t, probe.Gated)
		require.Equal(t, v.PlacementValue("Unavailable"), probe.GatedState)
		require.Equal(t, []v.PlacementIssue{{Group: "RecordedProbes", Code: "FutureObservationTime", Count: 1}}, reportValue.Content.Issues)
	})
}

func TestCapacityFallbackClassifierIsClosedAndPrivate(t *testing.T) {
	t.Parallel()

	invalidUTF8 := string([]byte("private-invalid-utf8-\xff"))
	overBudget := "private-over-budget-" + strings.Repeat("x", 4097)
	tests := []struct {
		name       string
		raw        string
		want       v.PlacementValue
		ok         bool
		leakMarker string
	}{
		{"empty", "", "NotRecorded", true, ""},
		{"no report", "no capacity report yet", "NoReport", true, "no capacity report yet"},
		{"invalid target", "capacity target is invalid: private-target", "InvalidTarget", true, "private-target"},
		{"clock unavailable", "capacity clock is not configured", "ClockUnavailable", true, "capacity clock is not configured"},
		{"executor unavailable", "capacity observer executor is not configured", "ObserverUnavailable", true, "capacity observer executor is not configured"},
		{"submission unavailable", "capacity observation could not be submitted: private-queue", "ObserverUnavailable", true, "private-queue"},
		{"unreachable", "capacity endpoint unreachable: GET https://private-user:private-token@private.invalid", "Unreachable", true, "private-user"},
		{"http error", "capacity endpoint returned HTTP 503 private-http", "HTTPError", true, "private-http"},
		{"read error", "reading capacity response: private-read", "ResponseReadError", true, "private-read"},
		{"request incomplete", "capacity request did not complete: private-timeout", "RequestIncomplete", true, "private-timeout"},
		{"negative report", "capacity report is negative (-1 private-negative)", "InvalidReport", true, "private-negative"},
		{"future report", "capacity report timestamp is in the future", "InvalidReport", true, "capacity report timestamp is in the future"},
		{"stale report", "capacity report is stale (private-stale)", "StaleReport", true, "private-stale"},
		{"awaiting quorum", "capacity report awaiting quorum (private-quorum)", "AwaitingQuorum", true, "private-quorum"},
		{"awaiting quorum after stale samples", "capacity report awaiting quorum after stale samples expired (1/2 private-samples)", "AwaitingQuorum", true, "private-samples"},
		{"unknown safe", "private future capacity reason", "OtherReportedReason", true, "private future capacity reason"},
		{"over budget", overBudget, "MalformedPayload", false, "private-over-budget"},
		{"invalid utf8", invalidUTF8, "MalformedPayload", false, "private-invalid-utf8"},
		{"control", "private-control\nvalue", "MalformedPayload", false, "private-control"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, ok := projectCapacityFallbackReason(test.raw)
			require.Equal(t, test.want, got)
			require.Equal(t, test.ok, ok)

			snapshot := fixture(t)
			snapshot.TrafficMap.Spec.Entries[0].Capacity.FallbackReason = test.raw
			reportValue, err := ProjectEndpoint(snapshot, fixtureClock)
			require.NoError(t, err)
			route := placementRouteByCluster(t, reportValue.Content.Entries, "demo-a")
			require.NotNil(t, route.Capacity)
			require.Equal(t, test.want, route.Capacity.FallbackReason)
			require.Equal(t, v.PlacementValue("Present"), route.Address.State)
			if test.ok {
				require.Empty(t, reportValue.Content.Issues)
			} else {
				require.Equal(t, []v.PlacementIssue{{Group: "CapacityFallbackReasons", Code: "MalformedPayload", Count: 1}}, reportValue.Content.Issues)
			}
			for name, output := range placementEndpointOutputs(t, reportValue) {
				if test.leakMarker != "" {
					require.NotContains(t, output, test.leakMarker, name)
				}
			}
		})
	}
}

func placementEndpointOutputs(t *testing.T, reportValue v.PlacementEndpointReport) map[string]string {
	t.Helper()
	outputs := map[string]string{}
	for _, view := range []struct {
		name  string
		write func(*bytes.Buffer) error
	}{
		{"table", func(out *bytes.Buffer) error { return reportValue.Content.Table().Write(out) }},
		{"wide", func(out *bytes.Buffer) error { return reportValue.Content.WideTable().Write(out) }},
		{"json", func(out *bytes.Buffer) error { return report.Write(out, report.FormatJSON, reportValue) }},
		{"yaml", func(out *bytes.Buffer) error { return report.Write(out, report.FormatYAML, reportValue) }},
	} {
		var out bytes.Buffer
		require.NoError(t, view.write(&out))
		outputs[view.name] = out.String()
	}
	return outputs
}

func placementRouteByCluster(t *testing.T, routes []v.PlacementRoute, cluster string) v.PlacementRoute {
	t.Helper()
	for _, route := range routes {
		if route.Cluster == cluster {
			return route
		}
	}
	t.Fatalf("route %q missing from %+v", cluster, routes)
	return v.PlacementRoute{}
}

func routingCondition(conditionType, reason string, observedGeneration int64) metav1.Condition {
	return metav1.Condition{
		Type:               conditionType,
		Status:             metav1.ConditionTrue,
		Reason:             reason,
		ObservedGeneration: observedGeneration,
		LastTransitionTime: metav1.NewTime(fixtureClock.Now().Add(-time.Hour)),
		Message:            "private-condition-message",
	}
}

func placementConditionByType(t *testing.T, conditions []v.PlacementCondition, conditionType string) v.PlacementCondition {
	t.Helper()
	for _, condition := range conditions {
		if condition.Type == v.PlacementValue(conditionType) {
			return condition
		}
	}
	t.Fatalf("condition %q missing from %+v", conditionType, conditions)
	return v.PlacementCondition{}
}
