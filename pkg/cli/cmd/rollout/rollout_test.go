package rollout

import (
	"bytes"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	k8stesting "k8s.io/client-go/testing"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"sigs.k8s.io/ome/pkg/client/clientset/versioned"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/rolloutprojection"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestStatusRejectsOutputBeforeNamespaceOrClient(t *testing.T) {
	f := &trackingFactory{}
	_, err := execute(t, f, fixedClock(), "status", "chat", "-o", "csv")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "supported: table, wide, json, yaml")
	assert.Zero(t, f.namespaceCalls)
	assert.Zero(t, f.omeCalls)
}

func TestStatusRequiresExactlyOneInferenceServiceBeforeReads(t *testing.T) {
	f := &trackingFactory{}
	_, err := execute(t, f, fixedClock(), "status")

	require.Error(t, err)
	assert.Zero(t, f.namespaceCalls)
	assert.Zero(t, f.omeCalls)
}

func TestStatusRejectsInvalidInferenceServiceNameBeforeReads(t *testing.T) {
	for _, name := range []string{"", "   ", "bad/name", "UpperCase"} {
		t.Run(name, func(t *testing.T) {
			f := &trackingFactory{}
			output, err := execute(t, f, fixedClock(), "status", name)

			require.ErrorIs(t, err, ErrInvalidInferenceServiceName)
			assert.Empty(t, output)
			assert.Zero(t, f.namespaceCalls)
			assert.Zero(t, f.omeCalls)
			if name != "" {
				assert.NotContains(t, err.Error(), name)
			}
		})
	}
}

func TestStatusPerformsExactlyOneInferenceServiceGet(t *testing.T) {
	isvc := minimalInferenceService()
	client := omefake.NewSimpleClientset(isvc)
	f := factory.Static{OME: client, NS: "prod"}

	output, err := execute(t, f, fixedClock(), "status", "chat")
	require.NoError(t, err)
	assert.Equal(t,
		"FIELD          SERVICE         ENGINE   DECODER   ROUTER\n"+
			"STATE          NotConfigured   -        -         -\n"+
			"REPORTED       NotConfigured   -        -         -\n"+
			"EVIDENCE       Declared        -        -         -\n"+
			"EPOCH          NotApplicable   -        -         -\n"+
			"COORDINATION   NotApplicable   -        -         -\n",
		output,
	)
	require.Len(t, client.Actions(), 1)
	action := client.Actions()[0]
	assert.Equal(t, "get", action.GetVerb())
	assert.Equal(t, "inferenceservices", action.GetResource().Resource)
	assert.Equal(t, "prod", action.GetNamespace())
}

func TestRolloutCommandsRejectUnboundInferenceServiceResponses(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*omev1beta1.InferenceService)
		want   error
	}{
		{name: "returned name mismatch", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Name = "other"
		}, want: ErrReturnedInferenceServiceNameMismatch},
		{name: "returned namespace mismatch", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Namespace = "other"
		}, want: ErrReturnedInferenceServiceNamespaceMismatch},
		{name: "returned uid absent", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.UID = ""
		}, want: rolloutprojection.ErrSubjectUIDRequired},
	}
	for _, command := range []string{"status", "explain", "history"} {
		for _, tt := range tests {
			t.Run(command+" "+tt.name, func(t *testing.T) {
				returned := minimalInferenceService()
				tt.mutate(returned)
				client := omefake.NewSimpleClientset()
				client.PrependReactor("get", "inferenceservices", func(k8stesting.Action) (bool, runtime.Object, error) {
					return true, returned.DeepCopy(), nil
				})

				output, err := execute(
					t,
					factory.Static{OME: client, NS: "prod"},
					fixedClock(),
					command, "chat",
				)

				require.ErrorIs(t, err, tt.want)
				assert.Empty(t, output)
				require.Len(t, client.Actions(), 1)
				assert.Equal(t, "get", client.Actions()[0].GetVerb())
			})
		}
	}
}

func TestStatusWritesExactJSON(t *testing.T) {
	client := omefake.NewSimpleClientset(minimalInferenceService())
	output, err := execute(t, factory.Static{OME: client, NS: "prod"}, fixedClock(), "status", "chat", "-o", "json")

	require.NoError(t, err)
	assert.Equal(t, `{
  "apiVersion": "cli.ome.io/v1alpha1",
  "kind": "RolloutStatusReport",
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
      "uid": "isvc-uid",
      "generation": 7,
      "evidence": "Observed",
      "collectedAt": "2026-08-31T18:30:00Z"
    }
  ],
  "content": {
    "summary": {
      "state": "NotConfigured",
      "reportedState": "NotConfigured",
      "evidence": "Declared",
      "epoch": "NotApplicable",
      "coordinationReady": "NotApplicable"
    },
    "groups": [],
    "components": [],
    "issues": []
  },
  "warnings": []
}
`, output)
}

func TestStatusWritesExactYAML(t *testing.T) {
	client := omefake.NewSimpleClientset(minimalInferenceService())
	output, err := execute(t, factory.Static{OME: client, NS: "prod"}, fixedClock(), "status", "chat", "-o", "yaml")

	require.NoError(t, err)
	assert.Equal(t, `apiVersion: cli.ome.io/v1alpha1
collectedAt: "2026-08-31T18:30:00Z"
content:
  components: []
  groups: []
  issues: []
  summary:
    coordinationReady: NotApplicable
    epoch: NotApplicable
    evidence: Declared
    reportedState: NotConfigured
    state: NotConfigured
kind: RolloutStatusReport
metadata:
  name: chat
  namespace: prod
sources:
- collectedAt: "2026-08-31T18:30:00Z"
  evidence: Observed
  generation: 7
  kind: InferenceService
  name: chat
  namespace: prod
  uid: isvc-uid
warnings: []
`, output)
}

func TestStatusWritesExactIndependentFormats(t *testing.T) {
	tests := []struct {
		name   string
		format string
		want   string
	}{
		{
			name: "table",
			want: "FIELD          SERVICE             ENGINE        DECODER   ROUTER\n" +
				"STATE          Unknown             -             -         -\n" +
				"REPORTED       Succeeded           -             -         -\n" +
				"EVIDENCE       Reported            -             -         -\n" +
				"EPOCH          Unverifiable        -             -         -\n" +
				"COORDINATION   NotApplicable       -             -         -\n" +
				"STRATEGY       -                   Independent   -         -\n" +
				"PHASE          -                   Stable        -         -\n" +
				"ISSUES         EpochUnverifiable   -             -         -\n",
		},
		{
			name: "wide", format: "wide",
			want: "STATE     REPORTED-STATE   EVIDENCE   EPOCH          GROUP   STRATEGY      GROUP-PHASE   CURRENT-COMPONENT   PREVIOUS-COMPONENT   COMPONENT   COMPONENT-PHASE   STEP   GATE   CAPACITY   TARGET-TRAFFIC   OBSERVED-TRAFFIC   ROLLED-OUT   READY   PREVIOUS   ISSUES\n" +
				"Unknown   Succeeded        Reported   Unverifiable   -       Independent   -             -                   -                    engine      Stable            -      -      -          -                -                  -            -       -          EpochUnverifiable\n",
		},
		{
			name: "json", format: "json",
			want: `{
  "apiVersion": "cli.ome.io/v1alpha1",
  "kind": "RolloutStatusReport",
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
      "uid": "isvc-uid",
      "generation": 7,
      "evidence": "Observed",
      "collectedAt": "2026-08-31T18:30:00Z"
    }
  ],
  "content": {
    "summary": {
      "state": "Unknown",
      "reportedState": "Succeeded",
      "evidence": "Reported",
      "epoch": "Unverifiable",
      "coordinationReady": "NotApplicable"
    },
    "groups": [],
    "components": [
      {
        "type": "engine",
        "strategy": "Independent",
        "phase": "Stable",
        "traffic": []
      }
    ],
    "issues": [
      {
        "code": "EpochUnverifiable"
      }
    ]
  },
  "warnings": [
    {
      "code": "PartialData"
    }
  ]
}
`,
		},
		{
			name: "yaml", format: "yaml",
			want: `apiVersion: cli.ome.io/v1alpha1
collectedAt: "2026-08-31T18:30:00Z"
content:
  components:
  - phase: Stable
    strategy: Independent
    traffic: []
    type: engine
  groups: []
  issues:
  - code: EpochUnverifiable
  summary:
    coordinationReady: NotApplicable
    epoch: Unverifiable
    evidence: Reported
    reportedState: Succeeded
    state: Unknown
kind: RolloutStatusReport
metadata:
  name: chat
  namespace: prod
sources:
- collectedAt: "2026-08-31T18:30:00Z"
  evidence: Observed
  generation: 7
  kind: InferenceService
  name: chat
  namespace: prod
  uid: isvc-uid
warnings:
- code: PartialData
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := omefake.NewSimpleClientset(independentInferenceService())
			args := []string{"status", "chat"}
			if tt.format != "" {
				args = append(args, "-o", tt.format)
			}
			output, err := execute(t, factory.Static{OME: client, NS: "prod"}, fixedClock(), args...)
			require.NoError(t, err)
			assert.Equal(t, tt.want, output)
		})
	}
}

func TestStatusReturnsFriendlyGetError(t *testing.T) {
	output, err := execute(t, factory.Static{OME: omefake.NewSimpleClientset(), NS: "prod"}, fixedClock(), "status", "missing")

	require.Error(t, err)
	assert.Contains(t, strings.ToLower(err.Error()), "not found")
	assert.Empty(t, output)
}

func TestStatusCompactTableBoundsEveryPhysicalLineAtSupportedTerminalWidths(t *testing.T) {
	for _, width := range []int{80, 120} {
		client := omefake.NewSimpleClientset(independentInferenceService())
		output := &narrowTerminalBuffer{width: width}
		streams := genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: output, ErrOut: &bytes.Buffer{}}
		cmd := newCmdWithClock(factory.Static{OME: client, NS: "prod"}, streams, fixedClock())
		cmd.SilenceErrors = true
		cmd.SilenceUsage = true
		cmd.SetArgs([]string{"status", "chat"})

		require.NoError(t, cmd.Execute())
		assert.Contains(t, output.String(), "FIELD")
		assert.Contains(t, output.String(), "SERVICE")
		assert.Contains(t, output.String(), "ENGINE")
		for lineNumber, line := range strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n") {
			assert.LessOrEqual(t, len(line), width, "width %d line %d: %q", width, lineNumber+1, line)
		}
	}
}

func TestStatusWidePropagatesWriterError(t *testing.T) {
	client := omefake.NewSimpleClientset(independentInferenceService())
	want := errors.New("wide write failed")
	streams := genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: failingWriter{err: want}, ErrOut: &bytes.Buffer{},
	}
	cmd := newCmdWithClock(factory.Static{OME: client, NS: "prod"}, streams, fixedClock())
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetArgs([]string{"status", "chat", "-o", "wide"})

	require.ErrorIs(t, cmd.Execute(), want)
	require.Len(t, client.Actions(), 1)
}

func TestExplainRejectsArgumentsAndOutputBeforeReads(t *testing.T) {
	for _, args := range [][]string{
		{"explain"},
		{"explain", "chat", "extra"},
		{"explain", "chat", "-o", "csv"},
		{"explain", "bad/name"},
	} {
		f := &trackingFactory{}
		output, err := execute(t, f, fixedClock(), args...)
		require.Error(t, err)
		assert.Empty(t, output)
		assert.Zero(t, f.namespaceCalls)
		assert.Zero(t, f.omeCalls)
	}
}

func TestExplainPerformsExactlyOneNamespacedInferenceServiceGet(t *testing.T) {
	client := omefake.NewSimpleClientset(minimalInferenceService())
	output, err := execute(
		t, factory.Static{OME: client, NS: "prod"}, fixedClock(),
		"explain", "chat", "-o", "json",
	)

	require.NoError(t, err)
	assert.Contains(t, output, `"kind": "RolloutExplainReport"`)
	assert.Contains(t, output, `"mode": "Live"`)
	assert.Contains(t, output, `"declaredGroups": []`)
	assert.Contains(t, output, `"effectiveGroups": []`)
	require.Len(t, client.Actions(), 1)
	action := client.Actions()[0]
	assert.Equal(t, "get", action.GetVerb())
	assert.Equal(t, "inferenceservices", action.GetResource().Resource)
	assert.Equal(t, "prod", action.GetNamespace())
}

func TestExplainReturnsFriendlyGetError(t *testing.T) {
	output, err := execute(
		t, factory.Static{OME: omefake.NewSimpleClientset(), NS: "prod"}, fixedClock(),
		"explain", "missing",
	)

	require.Error(t, err)
	assert.Contains(t, strings.ToLower(err.Error()), "not found")
	assert.Empty(t, output)
}

func TestExplainRejectsEmptyNamespaceBeforeClientConstruction(t *testing.T) {
	f := &namespaceOnlyFactory{}
	output, err := execute(t, f, fixedClock(), "explain", "chat")

	require.ErrorIs(t, err, ErrInvalidNamespace)
	assert.Empty(t, output)
	assert.Equal(t, 1, f.namespaceCalls)
	assert.Zero(t, f.omeCalls)
}

func TestExplainPropagatesShortWrite(t *testing.T) {
	client := omefake.NewSimpleClientset(minimalInferenceService())
	want := errors.New("short write")
	streams := genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: failingWriter{err: want}, ErrOut: &bytes.Buffer{}}
	cmd := newCmdWithClock(factory.Static{OME: client, NS: "prod"}, streams, fixedClock())
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetArgs([]string{"explain", "chat"})

	require.ErrorIs(t, cmd.Execute(), want)
	require.Len(t, client.Actions(), 1)
}

func TestExplainWidePropagatesWriterError(t *testing.T) {
	client := omefake.NewSimpleClientset(explainInferenceService())
	want := errors.New("wide write failed")
	streams := genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: failingWriter{err: want}, ErrOut: &bytes.Buffer{},
	}
	cmd := newCmdWithClock(factory.Static{OME: client, NS: "prod"}, streams, fixedClock())
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetArgs([]string{"explain", "chat", "-o", "wide"})

	require.ErrorIs(t, cmd.Execute(), want)
	require.Len(t, client.Actions(), 1)
}

func TestExplainTableBoundsEveryPhysicalLineAtSupportedTerminalWidths(t *testing.T) {
	for _, width := range []int{80, 120} {
		client := omefake.NewSimpleClientset(explainInferenceService())
		output := &narrowTerminalBuffer{width: width}
		streams := genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: output, ErrOut: &bytes.Buffer{}}
		cmd := newCmdWithClock(factory.Static{OME: client, NS: "prod"}, streams, fixedClock())
		cmd.SilenceErrors = true
		cmd.SilenceUsage = true
		cmd.SetArgs([]string{"explain", "chat"})

		require.NoError(t, cmd.Execute())
		assert.Contains(t, output.String(), "VIEW")
		assert.Contains(t, output.String(), "Declared")
		assert.Contains(t, output.String(), "Live")
		assert.Contains(t, output.String(), "Effective")
		assert.Contains(t, output.String(), "mode=Pinned")
		assert.Contains(t, output.String(), "True/Pinned")
		assert.NotContains(t, output.String(), "VIEW:")
		for lineNumber, line := range strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n") {
			assert.LessOrEqual(t, len(line), width, "width %d line %d: %q", width, lineNumber+1, line)
		}
	}
}

func TestExplainExactOperatorExampleTable(t *testing.T) {
	client := omefake.NewSimpleClientset(explainInferenceService())
	output, err := execute(t, factory.Static{OME: client, NS: "prod"}, fixedClock(), "explain", "chat")

	require.NoError(t, err)
	assert.Equal(t, "VIEW        ITEM        DETAIL\n"+
		"Declared    PLAN        groups=1\n"+
		"            GROUP 0     Canary; components=engine; source=Declared/Inline\n"+
		"            STEP 1/2    capacity=25%; traffic=10%; gate=Manual\n"+
		"            STEP 2/2    capacity=100%; traffic=100%; gate=Immediate\n"+
		"Live        PLAN        mode=Live; groups=1\n"+
		"            GROUP 0     Canary; components=engine; source=Declared/Inline\n"+
		"            STEP 1/2    capacity=25%; traffic=10%; gate=Manual\n"+
		"            STEP 2/2    capacity=100%; traffic=100%; gate=Immediate\n"+
		"Effective   PLAN        mode=Pinned; evidence=Reported; groups=1\n"+
		"            READY       True/Pinned; evidence=Reported\n"+
		"            DRIFT       True/SpecNewerThanRun; evidence=Reported\n"+
		"            GROUP 0     Canary; components=engine; source=Reported/Policy\n"+
		"            CONFIG      policy=RolloutPolicy/guarded@4\n"+
		"                        digest=rp1:aaaaaaaaaaaa\n"+
		"            PHASE       Canarying\n"+
		"            REVISIONS   stable=aaaaaaaa,target=bbbbbbbb\n"+
		"            TRAFFIC     engine:aaaaaaaa=80%,engine:bbbbbbbb=20%\n"+
		"            STEP 1/2    capacity=50%; traffic=20%; gate=Manual\n"+
		"            STEP 2/2    capacity=100%; traffic=100%; gate=Immediate\n"+
		"            ISSUES      EpochUnverifiable\n",
		output)
}

func TestExplainExactOperatorExampleWide(t *testing.T) {
	client := omefake.NewSimpleClientset(explainInferenceService())
	output, err := execute(
		t, factory.Static{OME: client, NS: "prod"}, fixedClock(),
		"explain", "chat", "-o", "wide",
	)

	require.NoError(t, err)
	assert.Equal(t, "VIEW        GROUP   EVIDENCE   PLAN-MODE   SOURCE   STRATEGY   COMPONENTS   CONFIG                                                   STEP            GATE        PHASE       SEQUENCE   PLAN-READY    DRIFT                   HOLD   REVISIONS                         TRAFFIC                                   ISSUES\n"+
		"Declared    0       Declared   -           Inline   Canary     engine       -                                                        1/2 25%/10%     Manual      -           -          -             -                       -      -                                 -                                         -\n"+
		"Declared    0       Declared   -           Inline   Canary     engine       -                                                        2/2 100%/100%   Immediate   -           -          -             -                       -      -                                 -                                         -\n"+
		"Live        0       Declared   Live        Inline   Canary     engine       -                                                        1/2 25%/10%     Manual      -           -          -             -                       -      -                                 -                                         -\n"+
		"Live        0       Declared   Live        Inline   Canary     engine       -                                                        2/2 100%/100%   Immediate   -           -          -             -                       -      -                                 -                                         -\n"+
		"Effective   0       Reported   Pinned      Policy   Canary     engine       policy=RolloutPolicy/guarded@4,digest=rp1:aaaaaaaaaaaa   1/2 50%/20%     Manual      Canarying   -          True/Pinned   True/SpecNewerThanRun   -      stable=aaaaaaaa,target=bbbbbbbb   engine:aaaaaaaa=80%,engine:bbbbbbbb=20%   EpochUnverifiable\n"+
		"Effective   0       Reported   Pinned      Policy   Canary     engine       policy=RolloutPolicy/guarded@4,digest=rp1:aaaaaaaaaaaa   2/2 100%/100%   Immediate   Canarying   -          True/Pinned   True/SpecNewerThanRun   -      stable=aaaaaaaa,target=bbbbbbbb   engine:aaaaaaaa=80%,engine:bbbbbbbb=20%   EpochUnverifiable\n",
		output)
}

func TestExplainExactOperatorExampleJSON(t *testing.T) {
	client := omefake.NewSimpleClientset(explainInferenceService())
	output, err := execute(t, factory.Static{OME: client, NS: "prod"}, fixedClock(), "explain", "chat", "-o", "json")
	require.NoError(t, err)
	want, err := os.ReadFile("testdata/explain.json")
	require.NoError(t, err)
	assert.Equal(t, string(want), output)
}

func TestExplainExactOperatorExampleYAML(t *testing.T) {
	client := omefake.NewSimpleClientset(explainInferenceService())
	output, err := execute(t, factory.Static{OME: client, NS: "prod"}, fixedClock(), "explain", "chat", "-o", "yaml")
	require.NoError(t, err)
	want, err := os.ReadFile("testdata/explain.yaml")
	require.NoError(t, err)
	assert.Equal(t, string(want), output)
}

func TestRolloutCommandLocalHelpIsExact(t *testing.T) {
	var output bytes.Buffer
	cmd := NewCmd(factory.Static{NS: "prod"}, genericiooptions.IOStreams{Out: &output, ErrOut: &output})
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs([]string{"status", "--help"})

	require.NoError(t, cmd.Execute())
	assert.Equal(t, `Show rollout progress for an InferenceService

Usage:
  rollout status INFERENCESERVICE [flags]

Flags:
  -h, --help            help for status
  -o, --output string   Output format: table, wide, json, or yaml (default "table")
`, output.String())
}

func TestExplainCommandLocalHelpIsExact(t *testing.T) {
	var output bytes.Buffer
	cmd := NewCmd(factory.Static{NS: "prod"}, genericiooptions.IOStreams{Out: &output, ErrOut: &output})
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs([]string{"explain", "--help"})

	require.NoError(t, cmd.Execute())
	assert.Equal(t, `Explain rollout intent, effective plan, and observed progress

Usage:
  rollout explain INFERENCESERVICE [flags]

Flags:
  -h, --help            help for explain
  -o, --output string   Output format: table, wide, json, or yaml (default "table")
`, output.String())
}

func execute(t *testing.T, f factory.Factory, clock reportv1alpha1.Clock, args ...string) (string, error) {
	t.Helper()
	var output bytes.Buffer
	streams := genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &output, ErrOut: &output}
	cmd := newCmdWithClock(f, streams, clock)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetArgs(args)
	err := cmd.Execute()
	return output.String(), err
}

func minimalInferenceService() *omev1beta1.InferenceService {
	return &omev1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "prod", Name: "chat", UID: "isvc-uid", Generation: 7,
			ResourceVersion: "SECRET_RESOURCE_VERSION",
			Annotations:     map[string]string{"ome.io/rollout-promote": "SECRET_MAILBOX"},
		},
		Status: omev1beta1.InferenceServiceStatus{
			Status: duckv1.Status{
				ObservedGeneration: 7,
				Annotations:        map[string]string{"secret": "SECRET_STATUS_ANNOTATION"},
			},
			LastRuntimeSyncToken: "SECRET_SYNC_TOKEN",
		},
	}
}

func independentInferenceService() *omev1beta1.InferenceService {
	isvc := minimalInferenceService()
	mode := constants.OMENative
	isvc.Spec.DeploymentMode = &mode
	isvc.Spec.Engine = &omev1beta1.EngineSpec{}
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.EngineComponent: {
			Lifecycle: &omev1beta1.LifecycleStatus{
				CurrentRevision: "chat-engine-aaaaaaaa",
				UpdateRevision:  "chat-engine-aaaaaaaa",
			},
		},
	}
	return isvc
}

func explainInferenceService() *omev1beta1.InferenceService {
	isvc := minimalInferenceService()
	mode := constants.OMENative
	isvc.Spec.DeploymentMode = &mode
	isvc.Spec.Engine = &omev1beta1.EngineSpec{}
	isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{{
		Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
		Canary: &omev1beta1.GroupCanary{Steps: []omev1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("25%"), Traffic: 10, Pause: &omev1beta1.RolloutPause{}},
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		}},
	}}}
	pinnedGroup := omev1beta1.RolloutGroup{
		Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
		Canary: &omev1beta1.GroupCanary{Steps: []omev1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("50%"), Traffic: 20, Pause: &omev1beta1.RolloutPause{}},
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		}},
	}
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{ActiveRun: &omev1beta1.RolloutRun{
		RunID: "redacted-by-contract",
		Plan: omev1beta1.RolloutRunPlan{Groups: []omev1beta1.RolloutRunGroup{{
			Source: omev1beta1.RolloutPlanSourcePolicy,
			PolicyRef: &omev1beta1.RolloutPolicyRef{
				Kind: "RolloutPolicy", Name: "guarded", Progression: omev1beta1.RolloutProgressionCanary,
			},
			PolicyGeneration: 4, PortableDigest: "rp1:aaaaaaaaaaaa", Group: pinnedGroup,
		}}},
	}}
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.EngineComponent: {
			RolloutPhase:            omev1beta1.RolloutPhaseCanarying,
			LatestRolledoutRevision: "chat-engine-rev-aaaaaaaa",
			LatestReadyRevision:     "chat-engine-rev-bbbbbbbb",
			Traffic: []omev1beta1.ComponentTrafficTarget{
				{RevisionName: "chat-engine-rev-aaaaaaaa", Percent: 80},
				{RevisionName: "chat-engine-rev-bbbbbbbb", Percent: 20},
			},
		},
	}
	isvc.Status.Canary = &omev1beta1.CanaryStatus{
		StableRevisionHash: "aaaaaaaa", CanaryRevisionHash: "bbbbbbbb",
		CurrentStep: 0, ObservedTrafficWeight: 20,
	}
	isvc.Status.SetCondition(apis.ConditionType(omev1beta1.RolloutPlanReadyCondition), &apis.Condition{
		Type: apis.ConditionType(omev1beta1.RolloutPlanReadyCondition), Status: corev1.ConditionTrue,
		Reason: omev1beta1.RolloutPlanReasonPinned,
	})
	isvc.Status.SetCondition(apis.ConditionType(omev1beta1.RolloutPlanDriftCondition), &apis.Condition{
		Type: apis.ConditionType(omev1beta1.RolloutPlanDriftCondition), Status: corev1.ConditionTrue,
		Reason: omev1beta1.RolloutPlanDriftReasonSpecNewerThanRun,
	})
	return isvc
}

func fixedClock() reportv1alpha1.Clock {
	return reportv1alpha1.ClockFunc(func() time.Time {
		return time.Date(2026, time.August, 31, 18, 30, 0, 0, time.UTC)
	})
}

type trackingFactory struct {
	factory.Static
	namespaceCalls int
	omeCalls       int
}

type namespaceOnlyFactory struct {
	factory.Static
	namespaceCalls int
	omeCalls       int
}

func (f *namespaceOnlyFactory) Namespace() (string, bool, error) {
	f.namespaceCalls++
	return "", false, nil
}

func (f *namespaceOnlyFactory) OMEClient() (versioned.Interface, error) {
	f.omeCalls++
	return nil, errors.New("OME client must not be called")
}

type failingWriter struct{ err error }

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }

type narrowTerminalBuffer struct {
	bytes.Buffer
	width int
}

func (w *narrowTerminalBuffer) TerminalWidth() (int, bool) { return w.width, true }

func (f *trackingFactory) Namespace() (string, bool, error) {
	f.namespaceCalls++
	return "", false, errors.New("namespace must not be called")
}

func (f *trackingFactory) OMEClient() (versioned.Interface, error) {
	f.omeCalls++
	return nil, errors.New("OME client must not be called")
}
