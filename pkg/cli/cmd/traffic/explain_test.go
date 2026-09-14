package traffic

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	ktesting "k8s.io/client-go/testing"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/trafficprojection"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
)

func TestExplainGetsExactlyOneInferenceServiceAndWritesCompactOutput(t *testing.T) {
	isvc := &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "chat", Namespace: "prod", UID: types.UID("uid-chat"), Generation: 7,
	}}
	client := omefake.NewSimpleClientset(isvc)
	var projected *omev1beta1.InferenceService
	deps := explainDependencies{
		clock: fixedClock{now: time.Date(2026, 9, 14, 17, 0, 0, 0, time.UTC)},
		project: func(got *omev1beta1.InferenceService, _ reportv1alpha1.Clock) (reportv1alpha1.TrafficExplainReport, error) {
			projected = got
			return commandTrafficExplainReport(), nil
		},
	}

	out, err := executeTrafficExplain(t,
		factory.Static{OME: client, NS: "prod"}, deps, "chat")

	require.NoError(t, err)
	require.NotNil(t, projected)
	for _, wanted := range []string{
		"LAYER", "SUMMARY", "INTENT", "SUPPORT", "REALIZE", "CHECK",
	} {
		assert.Contains(t, out, wanted)
	}
	for _, line := range strings.Split(strings.TrimSuffix(out, "\n"), "\n") {
		assert.LessOrEqual(t, len([]rune(line)), 80,
			"line exceeded 80 columns: %q", line)
	}
	require.Len(t, client.Actions(), 1)
	action, ok := client.Actions()[0].(ktesting.GetAction)
	require.True(t, ok)
	assert.Equal(t, "get", action.GetVerb())
	assert.Equal(t, "inferenceservices", action.GetResource().Resource)
	assert.Equal(t, "prod", action.GetNamespace())
	assert.Equal(t, "chat", action.GetName())
}

func TestExplainWideAndMachineOutputExposeOnlyTypedEvidence(t *testing.T) {
	isvc := &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "chat", Namespace: "prod", UID: types.UID("uid-chat"),
	}}
	deps := explainDependencies{
		project: func(*omev1beta1.InferenceService, reportv1alpha1.Clock) (reportv1alpha1.TrafficExplainReport, error) {
			return commandTrafficExplainReport(), nil
		},
	}
	for _, format := range []string{"wide", "json", "yaml"} {
		out, err := executeTrafficExplain(t,
			factory.Static{OME: omefake.NewSimpleClientset(isvc), NS: "prod"},
			deps, "chat", "-o", format)
		require.NoError(t, err)
		for _, wanted := range []string{
			"RoundRobin", "envoy-gateway", "chat-engine-rev-a1b2c3d4",
		} {
			assert.Contains(t, out, wanted)
		}
		for _, forbidden := range []string{
			"SECRET", "uid-chat", "resourceVersion", "annotations", "message",
		} {
			assert.NotContains(t, strings.ToLower(out), strings.ToLower(forbidden))
		}
	}
}

func TestExplainValidatesArgumentsAndOutputBeforeFactoryAccess(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "missing name", want: "accepts 1 arg(s), received 0"},
		{name: "extra name", args: []string{"chat", "other"}, want: "accepts 1 arg(s), received 2"},
		{name: "unsupported output", args: []string{"chat", "-o", "name"}, want: `unsupported output format "name"`},
		{name: "invalid name", args: []string{"Bad_Name"}, want: `invalid InferenceService name "Bad_Name"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := executeTrafficExplain(t, panicFactory{}, explainDependencies{}, test.args...)
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.want)
		})
	}
}

func TestExplainRejectsUnboundResponses(t *testing.T) {
	tests := []struct {
		name   string
		object *omev1beta1.InferenceService
		want   error
	}{
		{name: "nil", want: trafficprojection.ErrInferenceServiceRequired},
		{name: "name", object: &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "prod", UID: "uid"}}, want: ErrReturnedInferenceServiceNameMismatch},
		{name: "namespace", object: &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "other", UID: "uid"}}, want: ErrReturnedInferenceServiceNamespaceMismatch},
		{name: "uid", object: &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "prod"}}, want: ErrReturnedInferenceServiceUIDMissing},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := omefake.NewSimpleClientset()
			client.PrependReactor("get", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) {
				if test.object == nil {
					var typedNil *omev1beta1.InferenceService
					return true, typedNil, nil
				}
				return true, test.object.DeepCopy(), nil
			})
			_, err := executeTrafficExplain(t,
				factory.Static{OME: client, NS: "prod"},
				explainDependencies{project: panicExplainProjector}, "chat")
			require.ErrorIs(t, err, test.want)
			require.Len(t, client.Actions(), 1)
		})
	}
}

func TestExplainSanitizesAPIErrorsWithoutBreakingUnwrap(t *testing.T) {
	secret := "Bearer SECRET_API_TOKEN"
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "not found", err: apierrors.NewNotFound(schema.GroupResource{Group: "ome.io", Resource: "inferenceservices"}, "chat"), want: "not found"},
		{name: "forbidden", err: apierrors.NewForbidden(schema.GroupResource{Group: "ome.io", Resource: "inferenceservices"}, "chat", errors.New(secret)), want: "forbidden"},
		{name: "cancelled", err: context.Canceled, want: "cancelled"},
		{name: "arbitrary", err: errors.New(secret), want: "API request failed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := omefake.NewSimpleClientset()
			client.PrependReactor("get", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) {
				return true, nil, test.err
			})
			out, err := executeTrafficExplain(t,
				factory.Static{OME: client, NS: "prod"}, explainDependencies{}, "chat")
			require.Error(t, err)
			assert.ErrorIs(t, err, test.err)
			assert.Contains(t, err.Error(), `get InferenceService "prod/chat"`)
			assert.Contains(t, err.Error(), test.want)
			assert.NotContains(t, err.Error(), secret)
			assert.NotContains(t, fmt.Sprintf("%#v", err), secret)
			assert.Equal(t, err.Error(), fmt.Sprintf("%#v", err))
			assert.Empty(t, out)
		})
	}
}

func TestExplainReturnsNamespaceClientProjectionAndWriterErrors(t *testing.T) {
	t.Run("namespace", func(t *testing.T) {
		_, err := executeTrafficExplain(t,
			namespaceResultFactory{err: errors.New("namespace backend failed")},
			explainDependencies{}, "chat")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "resolve namespace")
	})
	t.Run("client", func(t *testing.T) {
		_, err := executeTrafficExplain(t,
			factory.Static{NS: "prod"}, explainDependencies{}, "chat")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "create OME client")
	})
	t.Run("projector missing", func(t *testing.T) {
		isvc := &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
			Name: "chat", Namespace: "prod", UID: "uid",
		}}
		_, err := executeTrafficExplain(t,
			factory.Static{OME: omefake.NewSimpleClientset(isvc), NS: "prod"},
			explainDependencies{}, "chat")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "projector is not configured")
	})
	t.Run("projection", func(t *testing.T) {
		want := errors.New("projection failed")
		isvc := &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
			Name: "chat", Namespace: "prod", UID: "uid",
		}}
		_, err := executeTrafficExplain(t,
			factory.Static{OME: omefake.NewSimpleClientset(isvc), NS: "prod"},
			explainDependencies{project: func(*omev1beta1.InferenceService, reportv1alpha1.Clock) (reportv1alpha1.TrafficExplainReport, error) {
				return reportv1alpha1.TrafficExplainReport{}, want
			}}, "chat")
		require.ErrorIs(t, err, want)
		assert.Contains(t, err.Error(), "project traffic explanation")
	})
	t.Run("writer", func(t *testing.T) {
		isvc := &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
			Name: "chat", Namespace: "prod", UID: "uid",
		}}
		cmd := newExplainCmd(
			factory.Static{OME: omefake.NewSimpleClientset(isvc), NS: "prod"},
			genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: errorWriter{err: errors.New("writer failed")}, ErrOut: &bytes.Buffer{}},
			explainDependencies{project: func(*omev1beta1.InferenceService, reportv1alpha1.Clock) (reportv1alpha1.TrafficExplainReport, error) {
				return commandTrafficExplainReport(), nil
			}},
		)
		cmd.SetArgs([]string{"chat", "-o", "wide"})
		err := cmd.Execute()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "write traffic explanation")
	})
}

func TestExplainHelpStatesEvidenceBoundaryAndIsRegistered(t *testing.T) {
	var output bytes.Buffer
	cmd := NewCmd(factory.Static{NS: "prod"}, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &output, ErrOut: &output,
	})
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs([]string{"explain", "--help"})
	require.NoError(t, cmd.Execute())
	for _, wanted := range []string{
		"declared traffic intent", "controller-reported support",
		"does not query emitted policies", "does not prove data-plane realization",
	} {
		assert.Contains(t, output.String(), wanted)
	}
}

func executeTrafficExplain(
	t *testing.T,
	f factory.Factory,
	deps explainDependencies,
	args ...string,
) (string, error) {
	t.Helper()
	var output bytes.Buffer
	cmd := newExplainCmd(f, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &output, ErrOut: &output,
	}, deps)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return output.String(), err
}

func commandTrafficExplainReport() reportv1alpha1.TrafficExplainReport {
	status := commandTrafficReport().Content
	status.Summary.State = reportv1alpha1.TrafficStateReported
	status.Summary.PolicyReady = reportv1alpha1.TrafficConditionValue{
		Status: reportv1alpha1.TrafficConditionTrue,
		Reason: reportv1alpha1.TrafficReasonAcceptedByGateway,
	}
	status.Conditions[0].Status = reportv1alpha1.TrafficConditionTrue
	status.Conditions[0].Reason = reportv1alpha1.TrafficReasonAcceptedByGateway
	declared := reportv1alpha1.TrafficValueSource{
		Evidence:  reportv1alpha1.EvidenceDeclared,
		Freshness: reportv1alpha1.TrafficFreshnessCurrent,
	}
	computed := reportv1alpha1.TrafficValueSource{
		Evidence:  reportv1alpha1.EvidenceComputed,
		Freshness: reportv1alpha1.TrafficFreshnessCurrent,
	}
	value := reportv1alpha1.NewTrafficExplainReport(
		reportv1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		reportv1alpha1.TrafficExplainContent{
			Summary: reportv1alpha1.TrafficExplainSummary{
				State:       reportv1alpha1.TrafficExplainConsistent,
				Intent:      reportv1alpha1.TrafficIntentDeclared,
				Support:     reportv1alpha1.TrafficSupportHonored,
				Realization: reportv1alpha1.TrafficRealizationReported,
				Source: reportv1alpha1.TrafficValueSource{
					Evidence:  reportv1alpha1.EvidenceComputed,
					Freshness: reportv1alpha1.TrafficFreshnessUnverifiable,
				},
			},
			Intent: reportv1alpha1.TrafficDeclaredIntent{
				State:     reportv1alpha1.TrafficIntentDeclared,
				Algorithm: reportv1alpha1.TrafficAlgorithmRoundRobin,
				ConsistentHash: reportv1alpha1.TrafficDeclaredFeature{
					State:   reportv1alpha1.TrafficFeatureAbsent,
					Variant: reportv1alpha1.TrafficFeatureVariantUnavailable,
				},
				EndpointOverride: reportv1alpha1.TrafficDeclaredFeature{
					State:   reportv1alpha1.TrafficFeatureAbsent,
					Variant: reportv1alpha1.TrafficFeatureVariantUnavailable,
				},
				Extensions: []reportv1alpha1.TrafficDeclaredExtension{},
				Source:     declared,
			},
			Reported: status,
			Comparisons: []reportv1alpha1.TrafficExplainComparison{
				{Field: reportv1alpha1.TrafficComparisonAlgorithm,
					State: reportv1alpha1.TrafficComparisonMatch, Source: computed},
				{Field: reportv1alpha1.TrafficComparisonPolicy,
					State: reportv1alpha1.TrafficComparisonMatch, Source: computed},
			},
			Issues: []reportv1alpha1.TrafficExplainIssue{},
		},
		fixedClock{now: time.Date(2026, 9, 14, 17, 0, 0, 0, time.UTC)},
	)
	value.Sources = []reportv1alpha1.TrafficExplainSourceReference{{
		Kind:      reportv1alpha1.TrafficSourceInferenceService,
		Namespace: "prod", Name: "chat", Generation: 7,
		Evidence: reportv1alpha1.EvidenceReported,
	}}
	return value.Canonical()
}

func panicExplainProjector(
	*omev1beta1.InferenceService,
	reportv1alpha1.Clock,
) (reportv1alpha1.TrafficExplainReport, error) {
	panic("explain projector must not run")
}
