package rollout

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	k8stesting "k8s.io/client-go/testing"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/exitcode"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/rolloutprojection"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestValidateRejectsArgumentsOutputAndIdentityBeforeReads(t *testing.T) {
	for _, args := range [][]string{
		{"validate"},
		{"validate", "chat", "extra"},
		{"validate", "chat", "-o", "csv"},
		{"validate", "bad/name"},
		{"validate", "UpperCase"},
	} {
		f := &trackingFactory{}
		output, err := execute(t, f, fixedClock(), args...)
		require.Error(t, err)
		assert.Empty(t, output)
		assert.Zero(t, f.namespaceCalls)
		assert.Zero(t, f.omeCalls)
	}
}

func TestValidatePerformsExactlyOneNamespacedInferenceServiceGetAndNoOtherRead(t *testing.T) {
	isvc := validationCommandISVC()
	isvc.OwnerReferences = []metav1.OwnerReference{{APIVersion: "SECRET_API", Kind: "SECRET_KIND", Name: "SECRET_OWNER", UID: "SECRET_UID"}}
	client := omefake.NewSimpleClientset(isvc)

	output, err := execute(
		t, factory.Static{OME: client, NS: "prod"}, fixedClock(),
		"validate", "chat", "-o", "json",
	)

	require.NoError(t, err)
	assert.Contains(t, output, `"kind": "RolloutValidationReport"`)
	assert.Contains(t, output, `"state": "Valid"`)
	assert.NotContains(t, output, "SECRET_")
	require.Len(t, client.Actions(), 1)
	action := client.Actions()[0]
	assert.Equal(t, "get", action.GetVerb())
	assert.Equal(t, "inferenceservices", action.GetResource().Resource)
	assert.Equal(t, "prod", action.GetNamespace())
	get, ok := action.(k8stesting.GetAction)
	require.True(t, ok)
	assert.Equal(t, "chat", get.GetName())
}

func TestValidateWritesReportBeforeReturningAssertionExitTwo(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*omev1beta1.InferenceService)
		want   error
		state  string
	}{
		{
			name: "invalid",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Spec.ScalingPolicy = &omev1beta1.ScalingPolicy{Mode: omev1beta1.ScalingProportional}
			},
			want:  ErrRolloutValidationInvalid,
			state: "Invalid",
		},
		{
			name: "unverifiable",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{{
					Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
					BlueGreen:  &omev1beta1.GroupBlueGreen{},
				}}}
			},
			want:  ErrRolloutValidationUnverifiable,
			state: "Unverifiable",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := validationCommandISVC()
			tt.mutate(isvc)
			client := omefake.NewSimpleClientset(isvc)
			output, err := execute(t, factory.Static{OME: client, NS: "prod"}, fixedClock(), "validate", "chat")

			require.ErrorIs(t, err, tt.want)
			assert.Equal(t, exitcode.AssertionUnmet, exitcode.FromError(err))
			assert.Contains(t, output, tt.state)
			assert.Contains(t, output, "CHECK")
			require.Len(t, client.Actions(), 1)
		})
	}
}

func TestValidateValidReportReturnsExitZero(t *testing.T) {
	client := omefake.NewSimpleClientset(validationCommandISVC())
	output, err := execute(t, factory.Static{OME: client, NS: "prod"}, fixedClock(), "validate", "chat")

	require.NoError(t, err)
	assert.Equal(t, exitcode.Success, exitcode.FromError(err))
	assert.Equal(t, "CHECK             COMP     RESULT          SOURCE\n"+
		"OVERALL           -        Valid           Computed/Current\n"+
		"ROLLOUT-REFS      -        Valid           Computed/Current\n"+
		"ROLLOUT-PLAN      -        Valid           Computed/Current\n"+
		"ROLLOUT-ORDER     -        Valid           Computed/Current\n"+
		"ROLLOUT-RESOLVE   -        NotApplicable   Computed/NotApplicable\n"+
		"TRAFFIC-SPEC      -        Valid           Computed/Current\n"+
		"TRAFFIC-READY     -        NotApplicable   Computed/NotApplicable\n"+
		"SCALING-POLICY    -        Valid           Computed/Current\n"+
		"AUTOSCALER-SPEC   -        Valid           Computed/Current\n"+
		"AUTOSCALER-SPEC   engine   Valid           Computed/Current\n", output)
}

func TestValidateRejectsUnboundResponsesAfterOnlyTheGet(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*omev1beta1.InferenceService)
		want   error
	}{
		{name: "name", mutate: func(isvc *omev1beta1.InferenceService) { isvc.Name = "other" }, want: ErrReturnedInferenceServiceNameMismatch},
		{name: "namespace", mutate: func(isvc *omev1beta1.InferenceService) { isvc.Namespace = "other" }, want: ErrReturnedInferenceServiceNamespaceMismatch},
		{name: "uid", mutate: func(isvc *omev1beta1.InferenceService) { isvc.UID = "" }, want: rolloutprojection.ErrSubjectUIDRequired},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			returned := validationCommandISVC()
			tt.mutate(returned)
			client := omefake.NewSimpleClientset()
			client.PrependReactor("get", "inferenceservices", func(k8stesting.Action) (bool, runtime.Object, error) {
				return true, returned.DeepCopy(), nil
			})
			output, err := execute(t, factory.Static{OME: client, NS: "prod"}, fixedClock(), "validate", "chat")
			require.ErrorIs(t, err, tt.want)
			assert.Empty(t, output)
			require.Len(t, client.Actions(), 1)
		})
	}
}

func TestValidateRejectsInvalidNamespaceBeforeClientConstruction(t *testing.T) {
	f := &namespaceOnlyFactory{}
	output, err := execute(t, f, fixedClock(), "validate", "chat")

	require.ErrorIs(t, err, ErrInvalidNamespace)
	assert.Empty(t, output)
	assert.Equal(t, 1, f.namespaceCalls)
	assert.Zero(t, f.omeCalls)
}

func TestValidateReturnsFriendlyAPIAndCancellationErrors(t *testing.T) {
	t.Run("not found", func(t *testing.T) {
		output, err := execute(t, factory.Static{OME: omefake.NewSimpleClientset(), NS: "prod"}, fixedClock(), "validate", "missing")
		require.Error(t, err)
		assert.Contains(t, strings.ToLower(err.Error()), "not found")
		assert.Equal(t, exitcode.GeneralError, exitcode.FromError(err))
		assert.Empty(t, output)
	})
	t.Run("cancelled", func(t *testing.T) {
		client := omefake.NewSimpleClientset()
		client.PrependReactor("get", "inferenceservices", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, context.Canceled
		})
		streams := genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &bytes.Buffer{}, ErrOut: &bytes.Buffer{}}
		options := validateOptions{streams: streams, output: "table", clock: fixedClock()}
		err := options.run(context.Background(), factory.Static{OME: client, NS: "prod"}, "chat", "table", false)
		require.ErrorIs(t, err, context.Canceled)
		assert.Equal(t, exitcode.GeneralError, exitcode.FromError(err))
	})
	t.Run("forbidden", func(t *testing.T) {
		client := omefake.NewSimpleClientset()
		client.PrependReactor("get", "inferenceservices", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(schema.GroupResource{Group: "ome.io", Resource: "inferenceservices"}, "chat", errors.New("SECRET_SERVER"))
		})
		output, err := execute(t, factory.Static{OME: client, NS: "prod"}, fixedClock(), "validate", "chat")
		require.Error(t, err)
		assert.Contains(t, strings.ToLower(err.Error()), "forbidden")
		assert.Empty(t, output)
	})
}

func TestValidatePropagatesWriterErrorsBeforeAssertionResult(t *testing.T) {
	for _, format := range []string{"table", "wide", "json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			isvc := validationCommandISVC()
			isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{{
				Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
			}}}
			client := omefake.NewSimpleClientset(isvc)
			want := errors.New("write failed")
			streams := genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: failingWriter{err: want}, ErrOut: &bytes.Buffer{}}
			cmd := newCmdWithClock(factory.Static{OME: client, NS: "prod"}, streams, fixedClock())
			cmd.SilenceErrors = true
			cmd.SilenceUsage = true
			cmd.SetArgs([]string{"validate", "chat", "-o", format})

			err := cmd.Execute()
			require.ErrorIs(t, err, want)
			assert.Equal(t, exitcode.GeneralError, exitcode.FromError(err))
			require.Len(t, client.Actions(), 1)
		})
	}
}

func TestValidateDefaultAndWideOutputStayWithinDocumentedWidths(t *testing.T) {
	for _, test := range []struct {
		format string
		width  int
	}{{format: "table", width: 80}, {format: "wide", width: 120}} {
		t.Run(test.format, func(t *testing.T) {
			client := omefake.NewSimpleClientset(validationCommandISVC())
			output, err := execute(t, factory.Static{OME: client, NS: "prod"}, fixedClock(), "validate", "chat", "-o", test.format)
			require.NoError(t, err)
			for lineNumber, line := range strings.Split(strings.TrimSuffix(output, "\n"), "\n") {
				assert.LessOrEqual(t, len([]rune(line)), test.width, "line %d: %q", lineNumber+1, line)
			}
		})
	}
}

func TestValidateMachineFormatsUseTypedSchema(t *testing.T) {
	for _, format := range []string{"json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			client := omefake.NewSimpleClientset(validationCommandISVC())
			output, err := execute(t, factory.Static{OME: client, NS: "prod"}, fixedClock(), "validate", "chat", "-o", format)
			require.NoError(t, err)
			assert.Contains(t, output, "RolloutValidationReport")
			assert.Contains(t, output, "AutoscalerSpec")
			assert.Contains(t, output, "checks")
			assert.Contains(t, output, "issues")
		})
	}
}

func TestValidateCommandLocalHelpDocumentsExitSemantics(t *testing.T) {
	var output bytes.Buffer
	cmd := NewCmd(factory.Static{NS: "prod"}, genericiooptions.IOStreams{Out: &output, ErrOut: &output})
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs([]string{"validate", "--help"})

	require.NoError(t, cmd.Execute())
	assert.Equal(t, `Validate rollout, traffic, and autoscaling configuration.

The report is written before its assertion is evaluated. Valid returns exit
code 0. Invalid and Unverifiable return exit code 2. API, projection, and
output failures return exit code 1.

Usage:
  rollout validate INFERENCESERVICE [flags]

Flags:
  -h, --help            help for validate
  -o, --output string   Output format: table, wide, json, or yaml (default "table")
`, output.String())
}

func validationCommandISVC() *omev1beta1.InferenceService {
	isvc := minimalInferenceService()
	isvc.Annotations = map[string]string{}
	mode := constants.OMENative
	isvc.Spec.DeploymentMode = &mode
	isvc.Spec.Engine = &omev1beta1.EngineSpec{}
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{}
	return isvc
}
