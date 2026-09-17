package trafficprojection_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/trafficprojection"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/yaml"
)

var explainProjectionNow = time.Date(2026, 9, 14, 17, 0, 0, 0, time.UTC)

func TestProjectExplainHealthyIntentSupportAndRealization(t *testing.T) {
	isvc := currentTrafficISVC(t)
	algorithm := omev1beta1.LoadBalancingTypeRoundRobin
	isvc.Spec.Traffic = &omev1beta1.TrafficSpec{
		Algorithm: &algorithm,
		EndpointOverride: &omev1beta1.EndpointOverrideSpec{
			Type:    omev1beta1.EndpointOverrideTypeHeader,
			Headers: []omev1beta1.HashHeader{{Name: "X-SESSION-TOKEN"}},
		},
	}
	isvc.Annotations = map[string]string{
		constants.CircuitBreakerMaxConnectionsAnnotation: "1024",
	}
	isvc.Status.Traffic.Conditions = []metav1.Condition{
		trafficCondition(omev1beta1.TrafficConditionBackendPolicyReady,
			metav1.ConditionTrue, omev1beta1.TrafficReasonAcceptedByGateway, 7),
	}

	got, err := trafficprojection.ProjectExplain(isvc, projectionClock)
	require.NoError(t, err)

	assert.Equal(t, reportv1alpha1.TrafficExplainConsistent,
		got.Content.Summary.State)
	assert.Equal(t, reportv1alpha1.TrafficSupportHonored,
		got.Content.Summary.Support)
	assert.Equal(t, reportv1alpha1.TrafficRealizationReported,
		got.Content.Summary.Realization)
	assert.Equal(t, reportv1alpha1.TrafficAlgorithmRoundRobin,
		got.Content.Intent.Algorithm)
	assert.Equal(t, reportv1alpha1.TrafficFeatureVariantHeader,
		got.Content.Intent.EndpointOverride.Variant)
	assert.Equal(t, 1, got.Content.Intent.EndpointOverride.InputCount)
	assert.Equal(t, []reportv1alpha1.TrafficDeclaredExtension{{
		Kind: reportv1alpha1.TrafficExtensionCircuitBreaker, Count: 1,
	}}, got.Content.Intent.Extensions)
	assert.Equal(t, reportv1alpha1.TrafficComparisonMatch,
		explainComparison(t, got, reportv1alpha1.TrafficComparisonAlgorithm).State)
	assert.Equal(t, reportv1alpha1.TrafficComparisonMatch,
		explainComparison(t, got, reportv1alpha1.TrafficComparisonPolicy).State)
	assert.Equal(t, reportv1alpha1.TrafficComparisonMatch,
		explainComparison(t, got, reportv1alpha1.TrafficComparisonCanaryWeight).State)
	require.Len(t, got.Sources, 1)
	assert.Equal(t, int64(7), got.Sources[0].Generation)

	for _, format := range []report.Format{report.FormatJSON, report.FormatYAML} {
		var out bytes.Buffer
		require.NoError(t, report.Write(&out, format, got))
		for _, forbidden := range []string{
			"uid-chat", "X-SESSION-TOKEN", "1024", "annotations",
			"resourceVersion", "message",
		} {
			assert.NotContains(t, strings.ToLower(out.String()),
				strings.ToLower(forbidden))
		}
	}
}

func TestProjectExplainConcurrentCanaryComparisonIsUnverifiable(t *testing.T) {
	isvc := concurrentCanaryTrafficISVC(t)
	setReadyCondition(isvc, metav1.ConditionTrue, omev1beta1.TrafficReasonAcceptedByGateway, 7)

	got, err := trafficprojection.ProjectExplain(isvc, projectionClock)

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.TrafficExplainPartial, got.Content.Summary.State)
	assert.Equal(t, reportv1alpha1.TrafficComparisonUnverifiable,
		explainComparison(t, got, reportv1alpha1.TrafficComparisonCanaryWeight).State)
	assert.NotContains(t, got.Content.Reported.Issues,
		reportv1alpha1.TrafficIssue{Code: reportv1alpha1.TrafficIssueCanaryInvalid})
}

func TestProjectExplainClassifiesAlgorithmMismatch(t *testing.T) {
	isvc := currentTrafficISVC(t)
	algorithm := omev1beta1.LoadBalancingTypeLeastRequest
	isvc.Spec.Traffic = &omev1beta1.TrafficSpec{Algorithm: &algorithm}
	isvc.Status.Traffic.Conditions = []metav1.Condition{
		trafficCondition(omev1beta1.TrafficConditionBackendPolicyReady,
			metav1.ConditionTrue, omev1beta1.TrafficReasonAcceptedByGateway, 7),
	}

	got, err := trafficprojection.ProjectExplain(isvc, projectionClock)
	require.NoError(t, err)

	assert.Equal(t, reportv1alpha1.TrafficExplainMismatch,
		got.Content.Summary.State)
	assert.Equal(t, reportv1alpha1.TrafficComparisonMismatch,
		explainComparison(t, got, reportv1alpha1.TrafficComparisonAlgorithm).State)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.TrafficExplainIssue{
		Code: reportv1alpha1.TrafficExplainIssueAlgorithmMismatch,
	})
}

func TestProjectExplainProjectsEveryDeclaredTypedVariant(t *testing.T) {
	tests := []struct {
		name      string
		spec      *omev1beta1.TrafficSpec
		annotate  map[string]string
		algorithm reportv1alpha1.TrafficAlgorithm
		statusAlg string
		hash      reportv1alpha1.TrafficFeatureVariant
		hashCount int
		override  reportv1alpha1.TrafficFeatureVariant
		ovrCount  int
	}{
		{
			name:      "annotation-only default",
			annotate:  map[string]string{constants.TimeoutIdleAnnotation: "30s"},
			algorithm: reportv1alpha1.TrafficAlgorithmDefault,
			statusAlg: "Default",
		},
		{
			name: "least request", spec: trafficAlgorithmSpec(omev1beta1.LoadBalancingTypeLeastRequest),
			algorithm: reportv1alpha1.TrafficAlgorithmLeastRequest, statusAlg: "LeastRequest",
		},
		{
			name: "random", spec: trafficAlgorithmSpec(omev1beta1.LoadBalancingTypeRandom),
			algorithm: reportv1alpha1.TrafficAlgorithmRandom, statusAlg: "Random",
		},
		{
			name: "hash header",
			spec: consistentHashSpec(&omev1beta1.ConsistentHashSpec{
				Type:    omev1beta1.HashTypeHeader,
				Headers: []omev1beta1.HashHeader{{Name: "SECRET-A"}, {Name: "SECRET-B"}},
			}),
			algorithm: reportv1alpha1.TrafficAlgorithmConsistentHash,
			statusAlg: "ConsistentHash",
			hash:      reportv1alpha1.TrafficFeatureVariantHeader, hashCount: 2,
		},
		{
			name: "hash cookie",
			spec: consistentHashSpec(&omev1beta1.ConsistentHashSpec{
				Type:   omev1beta1.HashTypeCookie,
				Cookie: &omev1beta1.HashCookie{Name: "SECRET-COOKIE"},
			}),
			algorithm: reportv1alpha1.TrafficAlgorithmConsistentHash,
			statusAlg: "ConsistentHash",
			hash:      reportv1alpha1.TrafficFeatureVariantCookie, hashCount: 1,
		},
		{
			name: "hash source IP",
			spec: consistentHashSpec(&omev1beta1.ConsistentHashSpec{
				Type: omev1beta1.HashTypeSourceIP,
			}),
			algorithm: reportv1alpha1.TrafficAlgorithmConsistentHash,
			statusAlg: "ConsistentHash",
			hash:      reportv1alpha1.TrafficFeatureVariantSourceIP,
		},
		{
			name: "metadata endpoint override",
			spec: &omev1beta1.TrafficSpec{EndpointOverride: &omev1beta1.EndpointOverrideSpec{
				Type: omev1beta1.EndpointOverrideTypeMetadata,
			}},
			algorithm: reportv1alpha1.TrafficAlgorithmDefault,
			statusAlg: "Default",
			override:  reportv1alpha1.TrafficFeatureVariantMetadata,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			isvc := currentTrafficISVC(t)
			isvc.Spec.Traffic = test.spec
			isvc.Annotations = test.annotate
			isvc.Status.Traffic.Algorithm = test.statusAlg
			isvc.Status.Traffic.Conditions = []metav1.Condition{
				trafficCondition(omev1beta1.TrafficConditionBackendPolicyReady,
					metav1.ConditionTrue,
					omev1beta1.TrafficReasonAcceptedByGateway, 7),
			}

			got, err := trafficprojection.ProjectExplain(isvc, projectionClock)
			require.NoError(t, err)
			assert.Equal(t, reportv1alpha1.TrafficExplainConsistent,
				got.Content.Summary.State)
			assert.Equal(t, test.algorithm, got.Content.Intent.Algorithm)
			if test.hash != "" {
				assert.Equal(t, test.hash, got.Content.Intent.ConsistentHash.Variant)
				assert.Equal(t, test.hashCount, got.Content.Intent.ConsistentHash.InputCount)
			}
			if test.override != "" {
				assert.Equal(t, test.override, got.Content.Intent.EndpointOverride.Variant)
				assert.Equal(t, test.ovrCount, got.Content.Intent.EndpointOverride.InputCount)
			}
			var output bytes.Buffer
			require.NoError(t, report.Write(&output, report.FormatJSON, got))
			assert.NotContains(t, output.String(), "SECRET")
		})
	}
}

func TestProjectExplainPendingRejectedAndPartialStates(t *testing.T) {
	t.Run("pending", func(t *testing.T) {
		isvc := currentTrafficISVC(t)
		algorithm := omev1beta1.LoadBalancingTypeRoundRobin
		isvc.Spec.Traffic = &omev1beta1.TrafficSpec{Algorithm: &algorithm}

		got, err := trafficprojection.ProjectExplain(isvc, projectionClock)
		require.NoError(t, err)
		assert.Equal(t, reportv1alpha1.TrafficExplainPending,
			got.Content.Summary.State)
		assert.Equal(t, reportv1alpha1.TrafficSupportPending,
			got.Content.Summary.Support)
	})

	t.Run("gateway rejected", func(t *testing.T) {
		isvc := currentTrafficISVC(t)
		algorithm := omev1beta1.LoadBalancingTypeRoundRobin
		isvc.Spec.Traffic = &omev1beta1.TrafficSpec{Algorithm: &algorithm}
		isvc.Status.Traffic.Conditions = []metav1.Condition{
			trafficCondition(omev1beta1.TrafficConditionBackendPolicyReady,
				metav1.ConditionFalse,
				omev1beta1.TrafficReasonGatewayRejected, 7),
		}

		got, err := trafficprojection.ProjectExplain(isvc, projectionClock)
		require.NoError(t, err)
		assert.Equal(t, reportv1alpha1.TrafficExplainMismatch,
			got.Content.Summary.State)
		assert.Equal(t, reportv1alpha1.TrafficSupportRejected,
			got.Content.Summary.Support)
		assert.Contains(t, got.Content.Issues, reportv1alpha1.TrafficExplainIssue{
			Code: reportv1alpha1.TrafficExplainIssuePolicyRejected,
		})
	})

	t.Run("truncated realization", func(t *testing.T) {
		isvc := currentTrafficISVC(t)
		algorithm := omev1beta1.LoadBalancingTypeRoundRobin
		isvc.Spec.Traffic = &omev1beta1.TrafficSpec{Algorithm: &algorithm}
		isvc.Status.Traffic.Conditions = []metav1.Condition{
			trafficCondition(omev1beta1.TrafficConditionBackendPolicyReady,
				metav1.ConditionTrue,
				omev1beta1.TrafficReasonAcceptedByGateway, 7),
		}
		isvc.Status.Traffic.TargetedHTTPRoutes = []string{
			"route-a", "route-b", "route-c", "route-d", "route-e",
		}

		got, err := trafficprojection.ProjectExplain(isvc, projectionClock)
		require.NoError(t, err)
		assert.Equal(t, reportv1alpha1.TrafficExplainPartial,
			got.Content.Summary.State)
		assert.Equal(t, reportv1alpha1.TrafficRealizationPartial,
			got.Content.Summary.Realization)
		assert.Contains(t, got.Warnings, reportv1alpha1.TrafficWarning{
			Code: reportv1alpha1.WarningTruncated,
		})
	})
}

func TestProjectExplainCountsExtensionFamiliesWithoutCopyingPayloads(t *testing.T) {
	isvc := currentTrafficISVC(t)
	isvc.Spec.Traffic = nil
	isvc.Annotations = map[string]string{
		constants.CircuitBreakerMaxConnectionsAnnotation:        "10",
		constants.CircuitBreakerMaxPendingRequestsAnnotation:    "20",
		constants.RetryAttemptsAnnotation:                       "2",
		constants.RetryOnAnnotation:                             "5xx",
		constants.TimeoutTCPConnectAnnotation:                   "3s",
		constants.PassthroughEnvoyGatewayPrefix + "SECRET_PATH": "SECRET_VALUE",
	}
	isvc.Status.Traffic.Algorithm = "Default"
	isvc.Status.Traffic.Conditions = []metav1.Condition{
		trafficCondition(omev1beta1.TrafficConditionBackendPolicyReady,
			metav1.ConditionTrue, omev1beta1.TrafficReasonAcceptedByGateway, 7),
	}

	got, err := trafficprojection.ProjectExplain(isvc, projectionClock)
	require.NoError(t, err)
	assert.Equal(t, []reportv1alpha1.TrafficDeclaredExtension{
		{Kind: reportv1alpha1.TrafficExtensionCircuitBreaker, Count: 2},
		{Kind: reportv1alpha1.TrafficExtensionRetry, Count: 2},
		{Kind: reportv1alpha1.TrafficExtensionTimeout, Count: 1},
		{Kind: reportv1alpha1.TrafficExtensionEnvoyPassthrough, Count: 1},
	}, got.Content.Intent.Extensions)
	var output bytes.Buffer
	require.NoError(t, report.Write(&output, report.FormatYAML, got))
	assert.NotContains(t, output.String(), "SECRET_PATH")
	assert.NotContains(t, output.String(), "SECRET_VALUE")
}

func TestProjectExplainUnsupportedFieldsDoNotParseMessage(t *testing.T) {
	isvc := currentTrafficISVC(t)
	algorithm := omev1beta1.LoadBalancingTypeRoundRobin
	isvc.Spec.Traffic = &omev1beta1.TrafficSpec{Algorithm: &algorithm}
	isvc.Status.Traffic.Conditions = []metav1.Condition{
		trafficCondition(omev1beta1.TrafficConditionBackendPolicyReady,
			metav1.ConditionTrue, omev1beta1.TrafficReasonAcceptedByGateway, 7),
		{
			Type:               omev1beta1.TrafficConditionBackendPolicyUnsupportedFields,
			Status:             metav1.ConditionTrue,
			Reason:             omev1beta1.TrafficReasonUnsupportedField,
			Message:            "dropped authorization=Bearer SECRET_TOKEN",
			ObservedGeneration: 7,
			LastTransitionTime: metav1.NewTime(explainProjectionNow.Add(-time.Minute)),
		},
	}

	got, err := trafficprojection.ProjectExplain(isvc, projectionClock)
	require.NoError(t, err)

	assert.Equal(t, reportv1alpha1.TrafficExplainUnsupported,
		got.Content.Summary.State)
	assert.Equal(t, reportv1alpha1.TrafficSupportPartial,
		got.Content.Summary.Support)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.TrafficExplainIssue{
		Code: reportv1alpha1.TrafficExplainIssueUnsupportedDeclaredFields,
	})
	var out bytes.Buffer
	require.NoError(t, report.Write(&out, report.FormatJSON, got))
	assert.NotContains(t, out.String(), "SECRET_TOKEN")
	assert.NotContains(t, strings.ToLower(out.String()), "authorization")
}

func TestProjectExplainNoTranslatorIsExplicitAndUnrealized(t *testing.T) {
	isvc := currentTrafficISVC(t)
	algorithm := omev1beta1.LoadBalancingTypeRoundRobin
	isvc.Spec.Traffic = &omev1beta1.TrafficSpec{Algorithm: &algorithm}
	isvc.Status.Traffic.BackendPolicyResource = nil
	isvc.Status.Traffic.TargetedHTTPRoutes = nil
	isvc.Status.Traffic.Conditions = []metav1.Condition{{
		Type:               omev1beta1.TrafficConditionBackendPolicyReady,
		Status:             metav1.ConditionFalse,
		Reason:             omev1beta1.TrafficReasonNoTranslatorAvailable,
		Message:            "credential=https://user:SECRET@example.test",
		ObservedGeneration: 7,
		LastTransitionTime: metav1.NewTime(explainProjectionNow.Add(-time.Minute)),
	}}
	isvc.Status.Addresses = nil
	isvc.Status.Components = nil
	isvc.Status.Canary = nil

	got, err := trafficprojection.ProjectExplain(isvc, projectionClock)
	require.NoError(t, err)

	assert.Equal(t, reportv1alpha1.TrafficExplainUnsupported,
		got.Content.Summary.State)
	assert.Equal(t, reportv1alpha1.TrafficSupportRejected,
		got.Content.Summary.Support)
	assert.Equal(t, reportv1alpha1.TrafficRealizationUnavailable,
		got.Content.Summary.Realization)
	assert.Equal(t, reportv1alpha1.TrafficTranslatorUnavailable,
		got.Content.Reported.Summary.Translator)
	assert.Equal(t, reportv1alpha1.TrafficComparisonMismatch,
		explainComparison(t, got, reportv1alpha1.TrafficComparisonPolicy).State)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.TrafficExplainIssue{
		Code: reportv1alpha1.TrafficExplainIssueNoTranslatorAvailable,
	})
	var out bytes.Buffer
	require.NoError(t, report.Write(&out, report.FormatYAML, got))
	assert.NotContains(t, out.String(), "SECRET")
}

func TestProjectExplainInvalidDeclaredDataFailsClosed(t *testing.T) {
	isvc := currentTrafficISVC(t)
	unknown := omev1beta1.LoadBalancingType("SECRET_ALGORITHM")
	isvc.Spec.Traffic = &omev1beta1.TrafficSpec{
		Algorithm: &unknown,
		ConsistentHash: &omev1beta1.ConsistentHashSpec{
			Type:    omev1beta1.HashType("SECRET_HASH_TYPE"),
			Headers: []omev1beta1.HashHeader{{Name: "SECRET_HEADER"}},
		},
	}
	isvc.Annotations = map[string]string{
		constants.RetryAttemptsAnnotation:       "SECRET_NOT_AN_INT",
		constants.PassthroughEnvoyGatewayPrefix: "SECRET_PASSTHROUGH",
	}

	got, err := trafficprojection.ProjectExplain(isvc, projectionClock)
	require.NoError(t, err)

	assert.Equal(t, reportv1alpha1.TrafficExplainInvalid,
		got.Content.Summary.State)
	assert.Equal(t, reportv1alpha1.TrafficIntentInvalid,
		got.Content.Intent.State)
	assert.Equal(t, reportv1alpha1.TrafficAlgorithmUnknown,
		got.Content.Intent.Algorithm)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.TrafficExplainIssue{
		Code: reportv1alpha1.TrafficExplainIssueDeclaredSpecInvalid,
	})
	assert.Contains(t, got.Content.Issues, reportv1alpha1.TrafficExplainIssue{
		Code: reportv1alpha1.TrafficExplainIssueDeclaredAnnotationsInvalid,
	})
	var out bytes.Buffer
	require.NoError(t, report.Write(&out, report.FormatJSON, got))
	for _, forbidden := range []string{
		"SECRET_ALGORITHM", "SECRET_HASH_TYPE", "SECRET_HEADER",
		"SECRET_NOT_AN_INT", "SECRET_PASSTHROUGH",
	} {
		assert.NotContains(t, out.String(), forbidden)
	}
}

func TestProjectExplainRejectsUnknownAlgorithmWithoutRelyingOnCRDValidation(t *testing.T) {
	isvc := currentTrafficISVC(t)
	unknown := omev1beta1.LoadBalancingType("UnknownFromStoredObject")
	isvc.Spec.Traffic = &omev1beta1.TrafficSpec{Algorithm: &unknown}

	got, err := trafficprojection.ProjectExplain(isvc, projectionClock)
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.TrafficIntentInvalid,
		got.Content.Intent.State)
	assert.Equal(t, reportv1alpha1.TrafficExplainInvalid,
		got.Content.Summary.State)
	assert.Equal(t, reportv1alpha1.TrafficAlgorithmUnknown,
		got.Content.Intent.Algorithm)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.TrafficExplainIssue{
		Code: reportv1alpha1.TrafficExplainIssueDeclaredSpecInvalid,
	})
}

func TestProjectExplainStaleAndMissingEvidenceRemainUnverifiable(t *testing.T) {
	t.Run("stale", func(t *testing.T) {
		isvc := currentTrafficISVC(t)
		algorithm := omev1beta1.LoadBalancingTypeRoundRobin
		isvc.Spec.Traffic = &omev1beta1.TrafficSpec{Algorithm: &algorithm}
		isvc.Status.Traffic.Conditions = []metav1.Condition{
			trafficCondition(omev1beta1.TrafficConditionBackendPolicyReady,
				metav1.ConditionTrue, omev1beta1.TrafficReasonAcceptedByGateway, 6),
		}

		got, err := trafficprojection.ProjectExplain(isvc, projectionClock)
		require.NoError(t, err)
		assert.Equal(t, reportv1alpha1.TrafficExplainPartial,
			got.Content.Summary.State)
		assert.Equal(t, reportv1alpha1.TrafficComparisonUnverifiable,
			explainComparison(t, got,
				reportv1alpha1.TrafficComparisonAlgorithm).State)
		assert.Contains(t, got.Content.Issues, reportv1alpha1.TrafficExplainIssue{
			Code: reportv1alpha1.TrafficExplainIssueReportedEvidenceStale,
		})
		assert.Contains(t, got.Warnings, reportv1alpha1.TrafficWarning{
			Code: reportv1alpha1.WarningStaleEvidence,
		})
	})

	t.Run("missing", func(t *testing.T) {
		isvc := currentTrafficISVC(t)
		algorithm := omev1beta1.LoadBalancingTypeRoundRobin
		isvc.Spec.Traffic = &omev1beta1.TrafficSpec{Algorithm: &algorithm}
		isvc.Status.Traffic = nil
		isvc.Status.Addresses = nil
		isvc.Status.Components = nil
		isvc.Status.Canary = nil

		got, err := trafficprojection.ProjectExplain(isvc, projectionClock)
		require.NoError(t, err)
		assert.Equal(t, reportv1alpha1.TrafficExplainUnavailable,
			got.Content.Summary.State)
		assert.Equal(t, reportv1alpha1.TrafficSupportUnavailable,
			got.Content.Summary.Support)
		assert.Equal(t, reportv1alpha1.TrafficComparisonUnverifiable,
			explainComparison(t, got,
				reportv1alpha1.TrafficComparisonPolicy).State)
		assert.Contains(t, got.Content.Issues, reportv1alpha1.TrafficExplainIssue{
			Code: reportv1alpha1.TrafficExplainIssueIntentNotReported,
		})
	})
}

func TestProjectExplainNoIntentIsNotAnError(t *testing.T) {
	isvc := currentTrafficISVC(t)
	isvc.Spec.Rollout = nil
	isvc.Spec.Traffic = nil
	isvc.Annotations = nil
	isvc.Status.Traffic = nil
	isvc.Status.Addresses = nil
	isvc.Status.Components = nil
	isvc.Status.Canary = nil

	got, err := trafficprojection.ProjectExplain(isvc, projectionClock)
	require.NoError(t, err)

	assert.Equal(t, reportv1alpha1.TrafficExplainNoIntent,
		got.Content.Summary.State)
	assert.Equal(t, reportv1alpha1.TrafficIntentAbsent,
		got.Content.Intent.State)
	assert.Equal(t, reportv1alpha1.TrafficSupportNotApplicable,
		got.Content.Summary.Support)
	assert.Empty(t, got.Content.Issues)
	assert.Equal(t, reportv1alpha1.TrafficComparisonNotApplicable,
		explainComparison(t, got, reportv1alpha1.TrafficComparisonAlgorithm).State)
}

func TestProjectExplainSamplesClockOnceAndDoesNotMutateInput(t *testing.T) {
	isvc := currentTrafficISVC(t)
	algorithm := omev1beta1.LoadBalancingTypeRoundRobin
	isvc.Spec.Traffic = &omev1beta1.TrafficSpec{Algorithm: &algorithm}
	original := isvc.DeepCopy()
	calls := 0
	clock := reportv1alpha1.ClockFunc(func() time.Time {
		calls++
		return explainProjectionNow.Add(time.Duration(calls) * time.Second)
	})

	got, err := trafficprojection.ProjectExplain(isvc, clock)
	require.NoError(t, err)

	assert.Equal(t, 1, calls)
	assert.Equal(t, explainProjectionNow.Add(time.Second), got.CollectedAt)
	assert.Equal(t, original, isvc)
}

func TestProjectExplainPolicyRequiresCurrentReadiness(t *testing.T) {
	for _, generation := range []int64{6, 0, 8, -1} {
		t.Run(fmt.Sprint(generation), func(t *testing.T) {
			isvc := currentTrafficISVC(t)
			isvc.Spec.Traffic = trafficAlgorithmSpec(omev1beta1.LoadBalancingTypeRoundRobin)
			isvc.Status.Traffic.Conditions = []metav1.Condition{trafficCondition(
				omev1beta1.TrafficConditionBackendPolicyReady, metav1.ConditionTrue,
				omev1beta1.TrafficReasonAcceptedByGateway, generation)}
			if generation == -1 {
				isvc.Status.Traffic.Conditions = nil
			}
			got, err := trafficprojection.ProjectExplain(isvc, projectionClock)
			require.NoError(t, err)
			require.NotNil(t, got.Content.Reported.Policy)
			assert.Equal(t, reportv1alpha1.TrafficComparisonUnverifiable,
				explainComparison(t, got, reportv1alpha1.TrafficComparisonPolicy).State)
		})
	}
}

func TestProjectExplainStaleUnsupportedIsHistorical(t *testing.T) {
	isvc := currentTrafficISVC(t)
	isvc.Spec.Rollout = nil
	isvc.Spec.Traffic = trafficAlgorithmSpec(omev1beta1.LoadBalancingTypeRoundRobin)
	isvc.Status.Addresses = nil
	isvc.Status.Components = nil
	isvc.Status.Canary = nil
	isvc.Status.Traffic.Conditions = []metav1.Condition{
		trafficCondition(omev1beta1.TrafficConditionBackendPolicyReady, metav1.ConditionTrue, omev1beta1.TrafficReasonAcceptedByGateway, 7),
		trafficCondition(omev1beta1.TrafficConditionBackendPolicyUnsupportedFields, metav1.ConditionTrue, omev1beta1.TrafficReasonUnsupportedField, 6),
	}
	got, err := trafficprojection.ProjectExplain(isvc, projectionClock)
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.TrafficExplainPartial, got.Content.Summary.State)
	assert.Equal(t, reportv1alpha1.TrafficFreshnessStale, got.Content.Summary.Source.Freshness)
	assert.Equal(t, reportv1alpha1.TrafficUnsupportedPresent, got.Content.Reported.Summary.Unsupported)
	assert.NotContains(t, got.Content.Issues, reportv1alpha1.TrafficExplainIssue{Code: reportv1alpha1.TrafficExplainIssueUnsupportedDeclaredFields})
}

func TestProjectExplainInvalidEndpointDoesNotInvalidatePolicy(t *testing.T) {
	isvc := currentTrafficISVC(t)
	isvc.Spec.Traffic = trafficAlgorithmSpec(omev1beta1.LoadBalancingTypeRoundRobin)
	isvc.Status.Traffic.Conditions = []metav1.Condition{trafficCondition(
		omev1beta1.TrafficConditionBackendPolicyReady, metav1.ConditionTrue,
		omev1beta1.TrafficReasonAcceptedByGateway, 7)}
	isvc.Status.Addresses[0].URL.User = url.UserPassword("secret-user", "secret-password")
	got, err := trafficprojection.ProjectExplain(isvc, projectionClock)
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.TrafficExplainInvalid, got.Content.Summary.State)
	assert.Equal(t, reportv1alpha1.TrafficRealizationInvalid, got.Content.Summary.Realization)
	assert.Equal(t, reportv1alpha1.TrafficSupportHonored, got.Content.Summary.Support)
	for _, field := range []reportv1alpha1.TrafficComparisonField{reportv1alpha1.TrafficComparisonAlgorithm, reportv1alpha1.TrafficComparisonPolicy} {
		assert.Equal(t, reportv1alpha1.TrafficComparisonMatch, explainComparison(t, got, field).State)
	}
}

func TestProjectExplainCanaryOnlyIsTrafficIntent(t *testing.T) {
	isvc := currentTrafficISVC(t)
	isvc.Spec.Traffic = nil
	isvc.Annotations = nil
	got, err := trafficprojection.ProjectExplain(isvc, projectionClock)
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.TrafficIntentDeclared, got.Content.Intent.State)
	assert.Equal(t, reportv1alpha1.TrafficExplainConsistent, got.Content.Summary.State)
	assert.Equal(t, reportv1alpha1.TrafficSupportNotApplicable, got.Content.Summary.Support)
	assert.Equal(t, reportv1alpha1.TrafficComparisonNotApplicable, explainComparison(t, got, reportv1alpha1.TrafficComparisonPolicy).State)
	assert.Equal(t, reportv1alpha1.TrafficComparisonMatch, explainComparison(t, got, reportv1alpha1.TrafficComparisonCanaryWeight).State)
	assert.Equal(t, reportv1alpha1.TrafficFreshnessUnverifiable, got.Content.Summary.Source.Freshness)
}

func TestProjectExplainConflictingConditionsRemainInvalid(t *testing.T) {
	isvc := currentTrafficISVC(t)
	isvc.Spec.Traffic = trafficAlgorithmSpec(omev1beta1.LoadBalancingTypeRoundRobin)
	ready := trafficCondition(omev1beta1.TrafficConditionBackendPolicyReady,
		metav1.ConditionTrue, omev1beta1.TrafficReasonAcceptedByGateway, 7)
	isvc.Status.Traffic.Conditions = []metav1.Condition{ready, ready}
	got, err := trafficprojection.ProjectExplain(isvc, projectionClock)
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.TrafficSupportInvalid, got.Content.Summary.Support)
	assert.Equal(t, reportv1alpha1.TrafficComparisonInvalid, explainComparison(t, got, reportv1alpha1.TrafficComparisonPolicy).State)
}

func TestProjectExplainLayerEvidenceBoundaries(t *testing.T) {
	for _, scenario := range []struct {
		name, support, supportSource, realization, realizationSource, algorithm, comparison string
	}{
		{"stale-unsupported", "Partial", "Computed/Stale", "Reported", "Reported/Current", "Reported", "Match"},
		{"hostile-secret", "Honored", "Computed/Current", "Invalid", "Computed/Unverifiable", "Reported", "Match"},
		{"malformed", "Invalid", "Computed/Unverifiable", "Reported", "Reported/Current", "Reported", "Match"},
		{"canary-only", "NotApplicable", "Computed/Current", "Reported", "Reported/Unverifiable", "Unavailable", "NotApplicable"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("testdata", "explain", scenario.name+".input.json"))
			require.NoError(t, err)
			var isvc omev1beta1.InferenceService
			require.NoError(t, json.Unmarshal(data, &isvc))
			if scenario.name == "canary-only" {
				isvc.Status.Traffic = nil
			}
			got, err := trafficprojection.ProjectExplain(&isvc, projectionClock)
			require.NoError(t, err)
			assert.Equal(t, scenario.realization, string(got.Content.Summary.Realization))
			assert.Equal(t, scenario.comparison, string(explainComparison(t, got, reportv1alpha1.TrafficComparisonAlgorithm).State))
			compact := got.Table().Rows
			assert.Equal(t, []string{"SUPPORT", scenario.support, "-", scenario.supportSource}, compact[2])
			assert.Equal(t, scenario.realizationSource, compact[4][3])
			var machine bytes.Buffer
			require.NoError(t, report.Write(&machine, report.FormatJSON, got))
			var document map[string]any
			require.NoError(t, json.Unmarshal(machine.Bytes(), &document))
			summary := document["content"].(map[string]any)["summary"].(map[string]any)
			for field, want := range map[string]string{"supportSource": scenario.supportSource, "realizationSource": scenario.realizationSource} {
				parts := strings.Split(want, "/")
				assert.Equal(t, map[string]any{"evidence": parts[0], "freshness": parts[1]}, summary[field])
			}
			for _, row := range got.WideTable().Rows {
				if row[0] == "SUMMARY" && row[1] == "support" {
					assert.Equal(t, scenario.supportSource, row[4])
				}
				if row[0] == "SUMMARY" && row[1] == "realization" {
					assert.Equal(t, scenario.realizationSource, row[4])
				}
				if row[0] == "REPORTED" && row[1] == "algorithm" {
					assert.Equal(t, scenario.algorithm, row[2])
				}
			}
			if scenario.name == "hostile-secret" || scenario.name == "malformed" {
				assert.Equal(t, reportv1alpha1.TrafficFreshnessUnverifiable, got.Content.Summary.Source.Freshness)
			}
			if scenario.name == "malformed" {
				assert.Equal(t, reportv1alpha1.TrafficFreshnessUnverifiable,
					explainComparison(t, got, reportv1alpha1.TrafficComparisonPolicy).Source.Freshness)
			}
		})
	}
}

func TestProjectExplainCompletedCanaryUsesTerminalDisplay(t *testing.T) {
	isvc := currentTrafficISVC(t)
	isvc.Status.Canary.CurrentStep = 2
	isvc.Status.Canary.ObservedTrafficWeight = 100
	isvc.Status.Canary.StableRevisionHash = ""
	component := isvc.Status.Components[omev1beta1.EngineComponent]
	component.RolloutPhase = omev1beta1.RolloutPhaseStable
	component.Traffic = component.Traffic[:1]
	component.Traffic[0].Percent = 100
	isvc.Status.Components[omev1beta1.EngineComponent] = component
	got, err := trafficprojection.ProjectExplain(isvc, projectionClock)
	require.NoError(t, err)
	require.NotNil(t, got.Content.Reported.Canary)
	assert.Contains(t, got.WideTable().Rows, []string{"OBSERVED", "canary", "Reported", "engine step=2/2 traffic=100%", "Reported/Unverifiable"})
	assert.Contains(t, got.WideTable().Rows, []string{"OBSERVED", "stable-revision", "Reported", "-", "Reported/Unverifiable"})
	require.Len(t, got.Content.Reported.Allocations, 1)
	assert.Equal(t, int32(100), got.Content.Reported.Allocations[0].Percent)
}

func TestProjectExplainCanaryAllocationValidityIsPrimaryScoped(t *testing.T) {
	for _, scenario := range []struct {
		name             string
		primary, invalid omev1beta1.ComponentType
		comparison       string
		conflict         bool
	}{
		{"engine-unrelated-decoder", omev1beta1.EngineComponent, omev1beta1.DecoderComponent, "Match", false},
		{"engine-same-primary", omev1beta1.EngineComponent, omev1beta1.EngineComponent, "Invalid", false},
		{"decoder-unrelated-engine", omev1beta1.DecoderComponent, omev1beta1.EngineComponent, "Match", false},
		{"decoder-same-primary", omev1beta1.DecoderComponent, omev1beta1.DecoderComponent, "Invalid", false},
		{"engine-unrelated-decoder-conflict", omev1beta1.EngineComponent, omev1beta1.DecoderComponent, "Match", true},
		{"engine-same-primary-conflict", omev1beta1.EngineComponent, omev1beta1.EngineComponent, "Invalid", true},
		{"decoder-unrelated-engine-conflict", omev1beta1.DecoderComponent, omev1beta1.EngineComponent, "Match", true},
		{"decoder-same-primary-conflict", omev1beta1.DecoderComponent, omev1beta1.DecoderComponent, "Invalid", true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("testdata", "explain", "canary-only.input.json"))
			require.NoError(t, err)
			var isvc omev1beta1.InferenceService
			require.NoError(t, json.Unmarshal(data, &isvc))
			isvc.Spec.Rollout.Groups[0].Components = []omev1beta1.ComponentType{scenario.primary}
			component := isvc.Status.Components[omev1beta1.EngineComponent]
			component.Traffic[0].RevisionName = "chat-" + string(scenario.primary) + "-rev-e5f6a7b8"
			isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{scenario.primary: component}
			baseline, err := trafficprojection.ProjectExplain(&isvc, projectionClock)
			require.NoError(t, err)
			require.NotNil(t, baseline.Content.Reported.Canary)
			assert.Equal(t, reportv1alpha1.RuntimeComponentType(scenario.primary), baseline.Content.Reported.Canary.Component)
			issueCode := reportv1alpha1.TrafficIssueAllocationInvalid
			traffic := []omev1beta1.ComponentTrafficTarget{{RevisionName: "invalid", Percent: 100}}
			if scenario.conflict {
				issueCode = reportv1alpha1.TrafficIssueAllocationConflict
				name := "chat-" + string(scenario.invalid) + "-rev-e5f6a7b8"
				traffic = []omev1beta1.ComponentTrafficTarget{{RevisionName: name, Percent: 50}, {RevisionName: name, Percent: 50}}
			}
			isvc.Status.Components[scenario.invalid] = omev1beta1.ComponentStatusSpec{
				Traffic: traffic,
			}
			got, err := trafficprojection.ProjectExplain(&isvc, projectionClock)
			require.NoError(t, err)
			assert.Equal(t, reportv1alpha1.TrafficRealizationInvalid, got.Content.Summary.Realization)
			assert.Contains(t, got.Content.Reported.Issues, reportv1alpha1.TrafficIssue{
				Code: issueCode, Component: reportv1alpha1.RuntimeComponentType(scenario.invalid),
			})
			if scenario.primary != scenario.invalid {
				assert.Equal(t, baseline.Content.Reported.Canary, got.Content.Reported.Canary)
			}
			assert.Equal(t, scenario.comparison, string(explainComparison(t, got, reportv1alpha1.TrafficComparisonCanaryWeight).State))
			assert.Equal(t, []string{"CHECK", scenario.comparison, "canary-weight", "Computed/Unverifiable"}, got.Table().Rows[7])
			assert.Contains(t, got.WideTable().Rows, []string{"CHECK", "canary-weight", scenario.comparison, "-", "Computed/Unverifiable"})
			for _, format := range []report.Format{report.FormatTable, report.Format("wide"), report.FormatJSON, report.FormatYAML} {
				var out bytes.Buffer
				if format == report.Format("wide") {
					require.NoError(t, got.WideTable().Write(&out))
				} else {
					require.NoError(t, report.Write(&out, format, got))
				}
				if format == report.FormatTable || format == report.Format("wide") {
					assert.Regexp(t, "CHECK +(?:"+scenario.comparison+" +canary-weight|canary-weight +"+scenario.comparison+") +(?:- +)?Computed/Unverifiable", out.String())
					continue
				}
				machine := out.Bytes()
				if format == report.FormatYAML {
					machine, err = yaml.YAMLToJSON(machine)
					require.NoError(t, err)
				}
				var decoded reportv1alpha1.TrafficExplainReport
				require.NoError(t, json.Unmarshal(machine, &decoded))
				assert.Equal(t, reportv1alpha1.TrafficRealizationInvalid, decoded.Content.Summary.Realization)
				assert.Equal(t, scenario.comparison, string(explainComparison(t, decoded, reportv1alpha1.TrafficComparisonCanaryWeight).State))
				assert.Equal(t, got.Content.Reported.Issues, decoded.Content.Reported.Issues)
			}
		})
	}
}

func explainComparison(
	t *testing.T,
	reportValue reportv1alpha1.TrafficExplainReport,
	field reportv1alpha1.TrafficComparisonField,
) reportv1alpha1.TrafficExplainComparison {
	t.Helper()
	for _, comparison := range reportValue.Content.Comparisons {
		if comparison.Field == field {
			return comparison
		}
	}
	t.Fatalf("comparison %q not found", field)
	return reportv1alpha1.TrafficExplainComparison{}
}

func trafficAlgorithmSpec(algorithm omev1beta1.LoadBalancingType) *omev1beta1.TrafficSpec {
	return &omev1beta1.TrafficSpec{Algorithm: &algorithm}
}

func consistentHashSpec(hash *omev1beta1.ConsistentHashSpec) *omev1beta1.TrafficSpec {
	algorithm := omev1beta1.LoadBalancingTypeConsistentHash
	return &omev1beta1.TrafficSpec{Algorithm: &algorithm, ConsistentHash: hash}
}
