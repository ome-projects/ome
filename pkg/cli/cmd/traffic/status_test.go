package traffic

import (
	"bytes"
	"context"
	"errors"
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

func TestStatusGetsExactlyOneInferenceServiceAndWritesCompactOutput(t *testing.T) {
	isvc := &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "prod", UID: types.UID("uid-chat"), Generation: 7}}
	client := omefake.NewSimpleClientset(isvc)
	var projected *omev1beta1.InferenceService
	deps := statusDependencies{
		clock: fixedClock{now: time.Date(2026, 9, 14, 17, 0, 0, 0, time.UTC)},
		project: func(got *omev1beta1.InferenceService, _ reportv1alpha1.Clock) (reportv1alpha1.TrafficStatusReport, error) {
			projected = got
			return commandTrafficReport(), nil
		},
	}

	out, err := executeTrafficStatus(t, factory.Static{OME: client, NS: "prod"}, deps, "chat")

	require.NoError(t, err)
	require.NotNil(t, projected)
	assert.Contains(t, out, "FIELD")
	assert.Contains(t, out, "POLICY-READY")
	assert.Contains(t, out, "Computed/Unverifiable")
	require.Len(t, client.Actions(), 1)
	action, ok := client.Actions()[0].(ktesting.GetAction)
	require.True(t, ok)
	assert.Equal(t, "get", action.GetVerb())
	assert.Equal(t, "inferenceservices", action.GetResource().Resource)
	assert.Equal(t, "prod", action.GetNamespace())
	assert.Equal(t, "chat", action.GetName())
}

func TestStatusWideIsCommandLocalAndExpandsVettedDetails(t *testing.T) {
	isvc := &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "prod", UID: types.UID("uid-chat")}}
	out, err := executeTrafficStatus(t, factory.Static{OME: omefake.NewSimpleClientset(isvc), NS: "prod"}, statusDependencies{
		project: func(*omev1beta1.InferenceService, reportv1alpha1.Clock) (reportv1alpha1.TrafficStatusReport, error) {
			return commandTrafficReport(), nil
		},
	}, "chat", "-o", "wide")

	require.NoError(t, err)
	for _, wanted := range []string{"POLICY", "gateway.envoyproxy.io/v1alpha1/BackendTrafficPolicy/prod/chat", "ROUTE", "chat", "ENDPOINT", "https://chat.prod.example/", "CONDITION", "gen=7"} {
		assert.Contains(t, out, wanted)
	}
	assert.NotContains(t, out, "SECRET")
}

func TestStatusCompactOutputStaysWithinEightyColumns(t *testing.T) {
	isvc := &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "prod", UID: types.UID("uid-chat")}}
	out, err := executeTrafficStatus(t, factory.Static{OME: omefake.NewSimpleClientset(isvc), NS: "prod"}, statusDependencies{
		project: func(*omev1beta1.InferenceService, reportv1alpha1.Clock) (reportv1alpha1.TrafficStatusReport, error) {
			return commandTrafficReport(), nil
		},
	}, "chat")
	require.NoError(t, err)
	for _, line := range strings.Split(strings.TrimSuffix(out, "\n"), "\n") {
		assert.LessOrEqual(t, len([]rune(line)), 80, "line exceeded 80 columns: %q", line)
	}
}

func TestStatusValidatesArgumentsAndOutputBeforeFactoryAccess(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "missing name", want: "accepts 1 arg(s), received 0"},
		{name: "extra name", args: []string{"chat", "other"}, want: "accepts 1 arg(s), received 2"},
		{name: "unsupported output", args: []string{"chat", "-o", "name"}, want: `unsupported output format "name" (supported: table, wide, json, yaml)`},
		{name: "invalid name", args: []string{"Bad_Name"}, want: `invalid InferenceService name "Bad_Name"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := executeTrafficStatus(t, panicFactory{}, statusDependencies{}, tt.args...)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestStatusRejectsUnboundResponses(t *testing.T) {
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
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := omefake.NewSimpleClientset()
			client.PrependReactor("get", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) {
				if tt.object == nil {
					var typedNil *omev1beta1.InferenceService
					return true, typedNil, nil
				}
				return true, tt.object.DeepCopy(), nil
			})
			_, err := executeTrafficStatus(t, factory.Static{OME: client, NS: "prod"}, statusDependencies{project: panicProjector}, "chat")
			require.ErrorIs(t, err, tt.want)
			require.Len(t, client.Actions(), 1)
		})
	}
}

func TestStatusReturnsAcquisitionProjectionAndWriterErrors(t *testing.T) {
	t.Run("namespace", func(t *testing.T) {
		_, err := executeTrafficStatus(t, namespaceResultFactory{err: errors.New("namespace backend failed")}, statusDependencies{}, "chat")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "resolve namespace")
	})
	for _, test := range []struct {
		name      string
		namespace string
		want      string
	}{
		{name: "empty namespace", want: "resolved namespace must not be empty"},
		{name: "invalid namespace", namespace: "Bad_NS", want: "invalid resolved namespace"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := executeTrafficStatus(t, namespaceResultFactory{namespace: test.namespace}, statusDependencies{}, "chat")
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.want)
		})
	}
	t.Run("client", func(t *testing.T) {
		_, err := executeTrafficStatus(t, factory.Static{NS: "prod"}, statusDependencies{}, "chat")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "create OME client")
	})
	for _, test := range []struct {
		name string
		err  error
		is   func(error) bool
	}{
		{name: "not found", err: apierrors.NewNotFound(schema.GroupResource{Group: "ome.io", Resource: "inferenceservices"}, "chat"), is: apierrors.IsNotFound},
		{name: "forbidden", err: apierrors.NewForbidden(schema.GroupResource{Group: "ome.io", Resource: "inferenceservices"}, "chat", errors.New("denied")), is: apierrors.IsForbidden},
		{name: "cancelled", err: context.Canceled, is: func(err error) bool { return errors.Is(err, context.Canceled) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := omefake.NewSimpleClientset()
			client.PrependReactor("get", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, test.err })
			out, err := executeTrafficStatus(t, factory.Static{OME: client, NS: "prod"}, statusDependencies{}, "chat")
			require.Error(t, err)
			assert.True(t, test.is(err))
			assert.Contains(t, err.Error(), `get InferenceService "prod/chat"`)
			assert.Empty(t, out)
		})
	}
	t.Run("projection", func(t *testing.T) {
		want := errors.New("projection failed")
		isvc := &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "prod", UID: "uid"}}
		_, err := executeTrafficStatus(t, factory.Static{OME: omefake.NewSimpleClientset(isvc), NS: "prod"}, statusDependencies{project: func(*omev1beta1.InferenceService, reportv1alpha1.Clock) (reportv1alpha1.TrafficStatusReport, error) {
			return reportv1alpha1.TrafficStatusReport{}, want
		}}, "chat")
		require.ErrorIs(t, err, want)
		assert.Contains(t, err.Error(), "project traffic status")
	})
	t.Run("writer", func(t *testing.T) {
		isvc := &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "prod", UID: "uid"}}
		cmd := newStatusCmd(factory.Static{OME: omefake.NewSimpleClientset(isvc), NS: "prod"}, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: errorWriter{err: errors.New("writer failed")}, ErrOut: &bytes.Buffer{}}, statusDependencies{project: func(*omev1beta1.InferenceService, reportv1alpha1.Clock) (reportv1alpha1.TrafficStatusReport, error) {
			return commandTrafficReport(), nil
		}})
		cmd.SetArgs([]string{"chat", "-o", "wide"})
		err := cmd.Execute()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "write traffic status")
	})
}

func TestStatusHelpStatesEvidenceBoundary(t *testing.T) {
	var output bytes.Buffer
	cmd := NewCmd(factory.Static{NS: "prod"}, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &output, ErrOut: &output})
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs([]string{"status", "--help"})
	require.NoError(t, cmd.Execute())
	for _, wanted := range []string{"controller-reported", "one InferenceService", "does not query backend policies", "does not prove data-plane"} {
		assert.Contains(t, output.String(), wanted)
	}
}

func executeTrafficStatus(t *testing.T, f factory.Factory, deps statusDependencies, args ...string) (string, error) {
	t.Helper()
	var output bytes.Buffer
	cmd := newStatusCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &output, ErrOut: &output}, deps)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return output.String(), err
}

func commandTrafficReport() reportv1alpha1.TrafficStatusReport {
	current := reportv1alpha1.TrafficValueSource{Evidence: reportv1alpha1.EvidenceReported, Freshness: reportv1alpha1.TrafficFreshnessCurrent}
	unverifiable := reportv1alpha1.TrafficValueSource{Evidence: reportv1alpha1.EvidenceReported, Freshness: reportv1alpha1.TrafficFreshnessUnverifiable}
	value := reportv1alpha1.NewTrafficStatusReport(reportv1alpha1.Metadata{Namespace: "prod", Name: "chat"}, reportv1alpha1.TrafficStatusContent{
		Summary: reportv1alpha1.TrafficSummary{
			State: reportv1alpha1.TrafficStatePending, Translator: reportv1alpha1.TrafficTranslatorEnvoyGateway,
			Algorithm:   reportv1alpha1.TrafficAlgorithmRoundRobin,
			PolicyReady: reportv1alpha1.TrafficConditionValue{Status: reportv1alpha1.TrafficConditionUnknown, Reason: reportv1alpha1.TrafficReasonPending},
			Unsupported: reportv1alpha1.TrafficUnsupportedNone,
			Source: reportv1alpha1.TrafficSummarySources{
				State:      reportv1alpha1.TrafficValueSource{Evidence: reportv1alpha1.EvidenceComputed, Freshness: reportv1alpha1.TrafficFreshnessUnverifiable},
				Translator: reportv1alpha1.TrafficValueSource{Evidence: reportv1alpha1.EvidenceComputed, Freshness: reportv1alpha1.TrafficFreshnessCurrent},
				Algorithm:  current, PolicyReady: current, Unsupported: current,
			},
		},
		Policy:      &reportv1alpha1.TrafficPolicy{APIVersion: "gateway.envoyproxy.io/v1alpha1", Kind: reportv1alpha1.TrafficPolicyBackendTrafficPolicy, Namespace: "prod", Name: "chat", Source: current},
		Routes:      []reportv1alpha1.TrafficRoute{{Name: "chat", Source: current}},
		Endpoints:   []reportv1alpha1.TrafficEndpoint{{URL: "https://chat.prod.example/", Source: unverifiable}},
		Allocations: []reportv1alpha1.TrafficAllocation{{Component: reportv1alpha1.RuntimeComponentEngine, Role: reportv1alpha1.TrafficRoleStable, RevisionName: "chat-engine-rev-a1b2c3d4", RevisionHash: "a1b2c3d4", Percent: 100, Source: unverifiable}},
		Conditions:  []reportv1alpha1.TrafficCondition{{Type: reportv1alpha1.TrafficConditionBackendPolicyReady, Status: reportv1alpha1.TrafficConditionUnknown, Reason: reportv1alpha1.TrafficReasonPending, ObservedGeneration: 7, LastTransitionTime: time.Date(2026, 9, 14, 16, 59, 0, 0, time.UTC), Source: current}},
		Issues:      []reportv1alpha1.TrafficIssue{},
	}, fixedClock{now: time.Date(2026, 9, 14, 17, 0, 0, 0, time.UTC)})
	value.Sources = []reportv1alpha1.TrafficSourceReference{{Kind: reportv1alpha1.TrafficSourceInferenceService, Namespace: "prod", Name: "chat", UID: "uid-chat", Generation: 7, Evidence: reportv1alpha1.EvidenceReported}}
	return value.Canonical()
}

func panicProjector(*omev1beta1.InferenceService, reportv1alpha1.Clock) (reportv1alpha1.TrafficStatusReport, error) {
	panic("projection must not run")
}

type fixedClock struct{ now time.Time }

func (clock fixedClock) Now() time.Time { return clock.now }

type panicFactory struct{ factory.Factory }

type namespaceResultFactory struct {
	factory.Factory
	namespace string
	err       error
}

func (f namespaceResultFactory) Namespace() (string, bool, error) { return f.namespace, false, f.err }

type errorWriter struct{ err error }

func (writer errorWriter) Write([]byte) (int, error) { return 0, writer.err }
