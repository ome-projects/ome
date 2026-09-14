package autoscale

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	"sigs.k8s.io/ome/pkg/cli/autoscaleprojection"
	"sigs.k8s.io/ome/pkg/cli/factory"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
)

func TestStatusGetsOneInferenceServiceAndWritesExactTable(t *testing.T) {
	isvc := &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "chat", Namespace: "prod", UID: types.UID("uid-chat"), Generation: 7,
	}}
	client := omefake.NewSimpleClientset(isvc)
	var projected *omev1beta1.InferenceService
	deps := statusDependencies{
		clock: fixedClock{now: time.Date(2026, time.August, 31, 18, 30, 0, 0, time.UTC)},
		project: func(got *omev1beta1.InferenceService, _ reportv1alpha1.Clock) (reportv1alpha1.AutoscaleStatusReport, error) {
			projected = got
			return commandReportFixture(), nil
		},
	}

	out, err := executeStatus(t, factory.Static{OME: client, NS: "prod"}, deps, "chat")

	require.NoError(t, err)
	require.NotNil(t, projected)
	assert.Equal(t, isvc.ObjectMeta, projected.ObjectMeta)
	assert.Equal(t,
		"FIELD              SERVICE    ENGINE         DECODER   ROUTER\n"+
			"STATE              Reported   Reported       -         -\n"+
			"CLASS              -          HPA            -         -\n"+
			"MANAGED-BY         -          ome            -         -\n"+
			"SPEC-SOURCE        -          default        -         -\n"+
			"TARGET-KIND        -          IR             -         -\n"+
			"TARGET-NAME        -          chat-engine    -         -\n"+
			"TARGET-EVIDENCE    -          Reported       -         -\n"+
			"CURRENT            -          2              -         -\n"+
			"DESIRED            -          3              -         -\n"+
			"REPLICA-EVIDENCE   -          Reported       -         -\n"+
			"LAST-SCALE         -          Aug31 18:20Z   -         -\n"+
			"COND-EVIDENCE      -          Reported       -         -\n"+
			"ABLE-TO-SCALE      -          True           -         -\n"+
			"SCALING-ACTIVE     -          True           -         -\n",
		out,
	)
	require.Len(t, client.Actions(), 1)
	action := client.Actions()[0]
	assert.Equal(t, "get", action.GetVerb())
	assert.Equal(t, "inferenceservices", action.GetResource().Resource)
	getAction, ok := action.(ktesting.GetAction)
	require.True(t, ok)
	assert.Equal(t, "prod", getAction.GetNamespace())
	assert.Equal(t, "chat", getAction.GetName())
}

func TestStatusValidatesArgumentsBeforeFactoryAccess(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "missing name", want: "accepts 1 arg(s), received 0"},
		{name: "extra name", args: []string{"chat", "other"}, want: "accepts 1 arg(s), received 2"},
		{name: "unsupported output", args: []string{"chat", "--output", "toml"}, want: `unsupported output format "toml"`},
		{name: "invalid name", args: []string{"Bad_Name"}, want: `invalid InferenceService name "Bad_Name"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := executeStatus(t, panicFactory{}, statusDependencies{}, tt.args...)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestStatusWidePreservesCompleteLegacyTable(t *testing.T) {
	isvc := &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "chat", Namespace: "prod", UID: types.UID("uid-chat"),
	}}
	deps := statusDependencies{project: func(
		*omev1beta1.InferenceService,
		reportv1alpha1.Clock,
	) (reportv1alpha1.AutoscaleStatusReport, error) {
		return commandReportFixture(), nil
	}}

	out, err := executeStatus(
		t, factory.Static{OME: omefake.NewSimpleClientset(isvc), NS: "prod"},
		deps, "chat", "--output", "wide",
	)

	require.NoError(t, err)
	assert.Equal(t,
		"STATE      COMPONENT   COMPONENT-STATE   CLASS   MANAGED-BY   SPEC-SOURCE   TARGET                              TARGET-EVIDENCE   CURRENT   DESIRED   REPLICA-EVIDENCE   LAST-SCALE             CONDITION-EVIDENCE   CONDITIONS                            ISSUES\n"+
			"Reported   engine      Reported          HPA     ome          default       InferenceReplica/prod/chat-engine   Reported          2         3         Reported           2026-08-31T18:20:00Z   Reported             AbleToScale=True,ScalingActive=True   -\n",
		out,
	)
}

func TestStatusRejectsUnboundInferenceServiceResponses(t *testing.T) {
	tests := []struct {
		name   string
		object *omev1beta1.InferenceService
		want   string
		kind   error
	}{
		{
			name: "nil response",
			want: autoscaleprojection.ErrInferenceServiceRequired.Error(),
			kind: autoscaleprojection.ErrInferenceServiceRequired,
		},
		{
			name: "different name",
			object: &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
				Name: "other", Namespace: "prod", UID: types.UID("uid-other"),
			}},
			want: ErrReturnedInferenceServiceNameMismatch.Error(),
			kind: ErrReturnedInferenceServiceNameMismatch,
		},
		{
			name: "different namespace",
			object: &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
				Name: "chat", Namespace: "other", UID: types.UID("uid-chat"),
			}},
			want: ErrReturnedInferenceServiceNamespaceMismatch.Error(),
			kind: ErrReturnedInferenceServiceNamespaceMismatch,
		},
		{
			name: "missing UID",
			object: &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
				Name: "chat", Namespace: "prod",
			}},
			want: ErrReturnedInferenceServiceUIDMissing.Error(),
			kind: ErrReturnedInferenceServiceUIDMissing,
		},
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
			deps := statusDependencies{project: func(
				*omev1beta1.InferenceService,
				reportv1alpha1.Clock,
			) (reportv1alpha1.AutoscaleStatusReport, error) {
				panic("projection must not run for an unbound response")
			}}

			out, err := executeStatus(t, factory.Static{OME: client, NS: "prod"}, deps, "chat")

			require.Error(t, err)
			assert.ErrorIs(t, err, tt.kind)
			assert.Contains(t, err.Error(), tt.want)
			assert.Empty(t, out)
			require.Len(t, client.Actions(), 1)
		})
	}
}

func TestStatusStopsAtInvalidNamespaceResolution(t *testing.T) {
	wantErr := errors.New("namespace backend failed")
	tests := []struct {
		name      string
		namespace string
		err       error
		want      string
	}{
		{name: "resolver error", err: wantErr, want: wantErr.Error()},
		{name: "empty namespace", want: "resolved namespace must not be empty"},
		{name: "invalid namespace", namespace: "Bad_Namespace", want: "invalid resolved namespace"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := namespaceResultFactory{namespace: tt.namespace, err: tt.err}
			_, err := executeStatus(t, f, statusDependencies{}, "chat")
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
			if tt.err != nil {
				assert.Contains(t, err.Error(), "resolve namespace")
			}
		})
	}
}

func TestStatusReturnsAcquisitionErrorsWithoutRendering(t *testing.T) {
	t.Run("client construction", func(t *testing.T) {
		out, err := executeStatus(t, factory.Static{NS: "prod"}, statusDependencies{}, "chat")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "create OME client")
		assert.Contains(t, err.Error(), "static factory: no OME client configured")
		assert.Empty(t, out)
	})

	tests := []struct {
		name       string
		reactorErr error
		assertErr  func(*testing.T, error)
	}{
		{
			name: "not found",
			reactorErr: apierrors.NewNotFound(
				schema.GroupResource{Group: "ome.io", Resource: "inferenceservices"}, "chat",
			),
			assertErr: func(t *testing.T, err error) { assert.True(t, apierrors.IsNotFound(err)) },
		},
		{
			name: "forbidden",
			reactorErr: apierrors.NewForbidden(
				schema.GroupResource{Group: "ome.io", Resource: "inferenceservices"}, "chat", errors.New("denied"),
			),
			assertErr: func(t *testing.T, err error) { assert.True(t, apierrors.IsForbidden(err)) },
		},
		{
			name:       "canceled",
			reactorErr: context.Canceled,
			assertErr:  func(t *testing.T, err error) { assert.ErrorIs(t, err, context.Canceled) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := omefake.NewSimpleClientset()
			client.PrependReactor("get", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) {
				return true, nil, tt.reactorErr
			})

			out, err := executeStatus(t, factory.Static{OME: client, NS: "prod"}, statusDependencies{}, "chat")

			require.Error(t, err)
			tt.assertErr(t, err)
			assert.Contains(t, err.Error(), `get InferenceService "prod/chat"`)
			assert.Empty(t, out)
			require.Len(t, client.Actions(), 1)
		})
	}
}

func TestStatusReturnsProjectionAndWriterErrors(t *testing.T) {
	isvc := &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "chat", Namespace: "prod", UID: types.UID("uid-chat"),
	}}

	t.Run("projection", func(t *testing.T) {
		wantErr := errors.New("projection failed")
		deps := statusDependencies{
			project: func(*omev1beta1.InferenceService, reportv1alpha1.Clock) (reportv1alpha1.AutoscaleStatusReport, error) {
				return reportv1alpha1.AutoscaleStatusReport{}, wantErr
			},
		}

		out, err := executeStatus(t, factory.Static{OME: omefake.NewSimpleClientset(isvc), NS: "prod"}, deps, "chat")

		require.ErrorIs(t, err, wantErr)
		assert.Contains(t, err.Error(), `project autoscale status for InferenceService "prod/chat"`)
		assert.Empty(t, out)
	})

	t.Run("writer", func(t *testing.T) {
		wantErr := errors.New("writer failed")
		cmd := newStatusCmd(
			factory.Static{OME: omefake.NewSimpleClientset(isvc), NS: "prod"},
			genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: errorWriter{err: wantErr}, ErrOut: &bytes.Buffer{}},
			statusDependencies{project: func(
				*omev1beta1.InferenceService,
				reportv1alpha1.Clock,
			) (reportv1alpha1.AutoscaleStatusReport, error) {
				return commandReportFixture(), nil
			}},
		)
		cmd.SetArgs([]string{"chat"})

		err := cmd.Execute()

		require.ErrorIs(t, err, wantErr)
		assert.Contains(t, err.Error(), "write autoscale status")
	})

	t.Run("wide writer", func(t *testing.T) {
		wantErr := errors.New("wide writer failed")
		cmd := newStatusCmd(
			factory.Static{OME: omefake.NewSimpleClientset(isvc), NS: "prod"},
			genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: errorWriter{err: wantErr}, ErrOut: &bytes.Buffer{}},
			statusDependencies{project: func(
				*omev1beta1.InferenceService,
				reportv1alpha1.Clock,
			) (reportv1alpha1.AutoscaleStatusReport, error) {
				return commandReportFixture(), nil
			}},
		)
		cmd.SetArgs([]string{"chat", "--output", "wide"})

		err := cmd.Execute()

		require.ErrorIs(t, err, wantErr)
		assert.Contains(t, err.Error(), "write autoscale status")
	})
}

func TestStatusDegradedReportExitsSuccessfully(t *testing.T) {
	isvc := &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "chat", Namespace: "prod", UID: types.UID("uid-chat"),
	}}
	reportValue := commandReportFixture()
	reportValue.Content.Summary.State = reportv1alpha1.AutoscaleStatePartial
	reportValue.Content.Components[0].State = reportv1alpha1.AutoscaleComponentPartial
	reportValue.Warnings = []reportv1alpha1.AutoscaleWarning{{Code: reportv1alpha1.AutoscaleWarningPartialData}}
	deps := statusDependencies{project: func(
		*omev1beta1.InferenceService,
		reportv1alpha1.Clock,
	) (reportv1alpha1.AutoscaleStatusReport, error) {
		return reportValue, nil
	}}

	out, err := executeStatus(t, factory.Static{OME: omefake.NewSimpleClientset(isvc), NS: "prod"}, deps, "chat")

	require.NoError(t, err)
	assert.Contains(t, out, "Partial")
}

func TestStatusWritesExactJSONAndYAML(t *testing.T) {
	tests := []struct {
		name   string
		format string
		want   string
	}{
		{name: "json", format: "json", want: `{
  "apiVersion": "cli.ome.io/v1alpha1",
  "kind": "AutoscaleStatusReport",
  "metadata": {
    "namespace": "prod",
    "name": "chat"
  },
  "collectedAt": "2026-08-31T18:30:00Z",
  "sources": [
    {
      "kind": "InferenceService",
      "namespace": "prod",
      "name": "chat",
      "uid": "uid-chat",
      "generation": 7,
      "evidence": "Reported",
      "collectedAt": "2026-08-31T18:30:00Z"
    }
  ],
  "content": {
    "summary": {
      "state": "Reported"
    },
    "components": [
      {
        "type": "engine",
        "state": "Reported",
        "class": "HPA",
        "managedBy": "ome",
        "specSource": "default",
        "target": {
          "state": "Reported",
          "apiVersion": "ome.io/v1beta1",
          "kind": "InferenceReplica",
          "namespace": "prod",
          "name": "chat-engine"
        },
        "replicas": {
          "state": "Reported",
          "currentReplicas": 2,
          "desiredReplicas": 3,
          "lastScaleTime": "2026-08-31T18:20:00Z"
        },
        "conditions": {
          "state": "Reported",
          "items": [
            {
              "type": "AbleToScale",
              "status": "True",
              "lastTransitionTime": "2026-08-31T18:15:00Z"
            },
            {
              "type": "ScalingActive",
              "status": "True",
              "lastTransitionTime": "2026-08-31T18:15:00Z"
            }
          ]
        }
      }
    ],
    "issues": []
  },
  "warnings": []
}
`},
		{name: "yaml", format: "yaml", want: `apiVersion: cli.ome.io/v1alpha1
collectedAt: "2026-08-31T18:30:00Z"
content:
  components:
  - class: HPA
    conditions:
      items:
      - lastTransitionTime: "2026-08-31T18:15:00Z"
        status: "True"
        type: AbleToScale
      - lastTransitionTime: "2026-08-31T18:15:00Z"
        status: "True"
        type: ScalingActive
      state: Reported
    managedBy: ome
    replicas:
      currentReplicas: 2
      desiredReplicas: 3
      lastScaleTime: "2026-08-31T18:20:00Z"
      state: Reported
    specSource: default
    state: Reported
    target:
      apiVersion: ome.io/v1beta1
      kind: InferenceReplica
      name: chat-engine
      namespace: prod
      state: Reported
    type: engine
  issues: []
  summary:
    state: Reported
kind: AutoscaleStatusReport
metadata:
  name: chat
  namespace: prod
sources:
- collectedAt: "2026-08-31T18:30:00Z"
  evidence: Reported
  generation: 7
  kind: InferenceService
  name: chat
  namespace: prod
  uid: uid-chat
warnings: []
`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
				Name: "chat", Namespace: "prod", UID: types.UID("uid-chat"),
			}}
			deps := statusDependencies{project: func(
				*omev1beta1.InferenceService,
				reportv1alpha1.Clock,
			) (reportv1alpha1.AutoscaleStatusReport, error) {
				return commandReportFixture(), nil
			}}

			out, err := executeStatus(
				t, factory.Static{OME: omefake.NewSimpleClientset(isvc), NS: "prod"},
				deps, "chat", "--output", tt.format,
			)

			require.NoError(t, err)
			assert.Equal(t, tt.want, out)
		})
	}
}

func TestStatusProductionWiringProjectsTheSingleFetchedParent(t *testing.T) {
	transition := metav1.NewTime(time.Date(2026, time.August, 31, 18, 15, 0, 0, time.UTC))
	isvc := &omev1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "chat", Namespace: "prod", UID: types.UID("uid-chat"), Generation: 7,
		},
		Status: omev1beta1.InferenceServiceStatus{Components: map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
			omev1beta1.EngineComponent: {
				Autoscaler: &omev1beta1.ComponentAutoscalerStatus{
					Class: omev1beta1.AutoscalerHPA, ManagedBy: omev1beta1.AutoscalerManagedByOME,
					SpecSource: "default", CurrentReplicas: 2, DesiredReplicas: 3,
					Conditions: []metav1.Condition{{
						Type: "AbleToScale", Status: metav1.ConditionTrue, Reason: "Observed", LastTransitionTime: transition,
					}},
				},
				ScaleTargetRef: &omev1beta1.ScaleTargetRef{
					APIVersion: "ome.io/v1beta1", Kind: "InferenceReplica", Name: "chat-engine",
				},
			},
		}},
	}
	client := omefake.NewSimpleClientset(isvc)
	var output bytes.Buffer
	cmd := NewCmd(factory.Static{OME: client, NS: "prod"}, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &output, ErrOut: &output,
	})
	cmd.SetArgs([]string{"status", "chat", "--output", "json"})

	require.NoError(t, cmd.Execute())
	var got reportv1alpha1.AutoscaleStatusReport
	require.NoError(t, json.Unmarshal(output.Bytes(), &got))
	assert.Equal(t, reportv1alpha1.Metadata{Namespace: "prod", Name: "chat"}, got.Metadata)
	assert.Equal(t, reportv1alpha1.AutoscaleStateReported, got.Content.Summary.State)
	require.Len(t, got.Sources, 1)
	assert.Equal(t, "uid-chat", got.Sources[0].UID)
	assert.Equal(t, int64(7), got.Sources[0].Generation)
	assert.Equal(t, reportv1alpha1.EvidenceReported, got.Sources[0].Evidence)
	require.Len(t, client.Actions(), 1)
	action, ok := client.Actions()[0].(ktesting.GetAction)
	require.True(t, ok)
	assert.Equal(t, "prod", action.GetNamespace())
	assert.Equal(t, "chat", action.GetName())
}

func executeStatus(t *testing.T, f factory.Factory, deps statusDependencies, args ...string) (string, error) {
	t.Helper()
	var output bytes.Buffer
	cmd := newStatusCmd(f, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &output, ErrOut: &output,
	}, deps)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return output.String(), err
}

func commandReportFixture() reportv1alpha1.AutoscaleStatusReport {
	current, desired := int32(2), int32(3)
	lastScale := time.Date(2026, time.August, 31, 18, 20, 0, 0, time.UTC)
	transition := time.Date(2026, time.August, 31, 18, 15, 0, 0, time.UTC)
	result := reportv1alpha1.NewAutoscaleStatusReport(
		reportv1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		reportv1alpha1.AutoscaleStatusContent{
			Summary: reportv1alpha1.AutoscaleSummary{State: reportv1alpha1.AutoscaleStateReported},
			Components: []reportv1alpha1.AutoscaleComponentStatus{{
				Type: reportv1alpha1.RuntimeComponentEngine, State: reportv1alpha1.AutoscaleComponentReported,
				Class: reportv1alpha1.AutoscaleClassHPA, ManagedBy: reportv1alpha1.AutoscaleManagedByOME,
				SpecSource: reportv1alpha1.AutoscaleSpecSourceDefault,
				Target: reportv1alpha1.AutoscaleTarget{
					State: reportv1alpha1.AutoscaleTargetReported, APIVersion: "ome.io/v1beta1",
					Kind: reportv1alpha1.AutoscaleTargetInferenceReplica, Namespace: "prod", Name: "chat-engine",
				},
				Replicas: reportv1alpha1.AutoscaleReplicaStatus{
					State: reportv1alpha1.AutoscaleReplicasReported, CurrentReplicas: &current,
					DesiredReplicas: &desired, LastScaleTime: &lastScale,
				},
				Conditions: reportv1alpha1.AutoscaleConditionsStatus{
					State: reportv1alpha1.AutoscaleConditionsReported,
					Items: []reportv1alpha1.AutoscaleCondition{{
						Type:   reportv1alpha1.AutoscaleConditionAbleToScale,
						Status: reportv1alpha1.AutoscaleConditionTrue, LastTransitionTime: transition,
					}, {
						Type:   reportv1alpha1.AutoscaleConditionScalingActive,
						Status: reportv1alpha1.AutoscaleConditionTrue, LastTransitionTime: transition,
					}},
				},
			}},
		},
		fixedClock{now: time.Date(2026, time.August, 31, 18, 30, 0, 0, time.UTC)},
	)
	result.Sources = []reportv1alpha1.AutoscaleSourceReference{{
		Kind: reportv1alpha1.AutoscaleSourceInferenceService, Namespace: "prod", Name: "chat",
		UID: "uid-chat", Generation: 7, Evidence: reportv1alpha1.EvidenceReported,
		CollectedAt: time.Date(2026, time.August, 31, 18, 30, 0, 0, time.UTC),
	}}
	return result.Canonical()
}

type fixedClock struct{ now time.Time }

func (clock fixedClock) Now() time.Time { return clock.now }

// panicFactory makes any factory access fail the test through a panic. It is
// intentionally backed by a nil embedded interface so preflight tests prove
// validation happens before namespace resolution or client construction.
type panicFactory struct{ factory.Factory }

type namespaceResultFactory struct {
	factory.Factory
	namespace string
	err       error
}

func (f namespaceResultFactory) Namespace() (string, bool, error) {
	return f.namespace, false, f.err
}

type errorWriter struct{ err error }

func (writer errorWriter) Write([]byte) (int, error) { return 0, writer.err }
