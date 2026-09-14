package rollout

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	k8stesting "k8s.io/client-go/testing"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
)

func TestHistoryRejectsArgumentsAndOutputBeforeReads(t *testing.T) {
	for _, args := range [][]string{
		{"history"},
		{"history", "chat", "extra"},
		{"history", "chat", "-o", "csv"},
		{"history", "bad/name"},
	} {
		f := &trackingFactory{}
		output, err := execute(t, f, fixedClock(), args...)
		require.Error(t, err)
		assert.Empty(t, output)
		assert.Zero(t, f.namespaceCalls)
		assert.Zero(t, f.omeCalls)
	}
}

func TestHistoryRejectsInvalidResolvedNamespaceBeforeClient(t *testing.T) {
	f := &namespaceOnlyFactory{}
	output, err := execute(t, f, fixedClock(), "history", "chat")

	require.ErrorIs(t, err, ErrInvalidNamespace)
	assert.Empty(t, output)
	assert.Equal(t, 1, f.namespaceCalls)
	assert.Zero(t, f.omeCalls)
}

func TestHistoryPerformsOneBoundNamespacedGet(t *testing.T) {
	isvc := minimalInferenceService()
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{}
	client := omefake.NewSimpleClientset(isvc)

	output, err := execute(
		t, factory.Static{OME: client, NS: "prod"}, fixedClock(),
		"history", "chat",
	)

	require.NoError(t, err)
	assert.Equal(t,
		"TYPE     STATE        COMP   IDENT      TIME   DETAIL    ISS\n"+
			"WINDOW   Empty        -      -          -      bounded   0\n"+
			"CURR     NotConf...   -      Declared   -      N/A       0\n",
		output,
	)
	require.Len(t, client.Actions(), 1)
	action := client.Actions()[0]
	assert.Equal(t, "get", action.GetVerb())
	assert.Equal(t, "inferenceservices", action.GetResource().Resource)
	assert.Equal(t, "prod", action.GetNamespace())
}

func TestHistoryPrintsRetainedEvidenceEndToEnd(t *testing.T) {
	client := omefake.NewSimpleClientset(historyInferenceService(t))
	output, err := execute(
		t, factory.Static{OME: client, NS: "prod"}, fixedClock(),
		"history", "chat",
	)

	require.NoError(t, err)
	assert.Equal(t, "TYPE     STATE       COMP     IDENT          TIME          DETAIL       ISS\n"+
		"WINDOW   Partial     -        -              -             bounded      1\n"+
		"CURR     Unknown     -        Reported       -             Unverified   1\n"+
		"ACTIVE   Active      -        0123456789ab   08-31T18:00   G:1 T:1      1\n"+
		"TARGET   Reported    engine   cccccccc       -             run-target   1\n"+
		"LAST     Completed   -        -              08-31T17:00   G:1          1\n"+
		"PROV-A   Inline      g0       cca45ec1fb0f   08-31T18:30   -            1\n"+
		"PROV-L   Inline      g0       cca45ec1fb0f   08-31T17:00   -            1\n"+
		"PROV-C   Inline      g0       cca45ec1fb0f   -             -            1\n"+
		"REV      Unknown     engine   aaaaaaaa       -             Current      1\n"+
		"REV      Unknown     engine   bbbbbbbb       -             Ready        1\n"+
		"REV      Unknown     engine   dddddddd       -             Previous     1\n", output)
	require.Len(t, client.Actions(), 1)
}

func TestHistoryWritesTypedJSONAndYAMLWithoutRawObjectFields(t *testing.T) {
	for _, format := range []string{"json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			isvc := minimalInferenceService()
			isvc.UID = "SECRET_HOSTILE_UID"
			client := omefake.NewSimpleClientset(isvc)
			output, err := execute(
				t, factory.Static{OME: client, NS: "prod"}, fixedClock(),
				"history", "chat", "-o", format,
			)

			require.NoError(t, err)
			assert.Contains(t, output, "cli.ome.io/v1alpha1")
			assert.Contains(t, output, "RolloutHistoryReport")
			assert.Contains(t, output, "RetentionBounded")
			for _, secret := range []string{
				"SECRET_RESOURCE_VERSION", "SECRET_MAILBOX",
				"SECRET_STATUS_ANNOTATION", "SECRET_SYNC_TOKEN",
				"SECRET_HOSTILE_UID",
			} {
				assert.NotContains(t, output, secret)
			}
			require.Len(t, client.Actions(), 1)
		})
	}
}

func TestHistoryReturnsFriendlyGetError(t *testing.T) {
	output, err := execute(
		t, factory.Static{OME: omefake.NewSimpleClientset(), NS: "prod"}, fixedClock(),
		"history", "missing",
	)

	require.Error(t, err)
	assert.Contains(t, strings.ToLower(err.Error()), "not found")
	assert.Empty(t, output)
}

func TestHistoryRejectsEmptyResponse(t *testing.T) {
	client := omefake.NewSimpleClientset()
	client.PrependReactor("get", "inferenceservices", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, nil
	})

	output, err := execute(
		t, factory.Static{OME: client, NS: "prod"}, fixedClock(),
		"history", "chat",
	)

	require.ErrorIs(t, err, ErrReturnedInferenceServiceNameMismatch)
	assert.Empty(t, output)
	require.Len(t, client.Actions(), 1)
}

func TestHistoryPropagatesProjectorErrorAfterOneGet(t *testing.T) {
	want := errors.New("project history failed")
	client := omefake.NewSimpleClientset(minimalInferenceService())
	var output bytes.Buffer
	streams := genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &output, ErrOut: &output}
	cmd := newHistoryCmdWithProjector(
		factory.Static{OME: client, NS: "prod"}, streams, fixedClock(),
		func(*omev1beta1.InferenceService, reportv1alpha1.Clock) (reportv1alpha1.RolloutHistoryReport, error) {
			return reportv1alpha1.RolloutHistoryReport{}, want
		},
	)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetArgs([]string{"chat"})

	require.ErrorIs(t, cmd.Execute(), want)
	assert.Empty(t, output.String())
	require.Len(t, client.Actions(), 1)
}

func TestHistoryPropagatesWriterErrors(t *testing.T) {
	for _, format := range []string{"table", "wide", "json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			want := errors.New("history write failed")
			client := omefake.NewSimpleClientset(minimalInferenceService())
			streams := genericiooptions.IOStreams{
				In: &bytes.Buffer{}, Out: failingWriter{err: want}, ErrOut: &bytes.Buffer{},
			}
			cmd := newHistoryCmd(factory.Static{OME: client, NS: "prod"}, streams, fixedClock())
			cmd.SilenceErrors = true
			cmd.SilenceUsage = true
			cmd.SetArgs([]string{"chat", "-o", format})

			require.ErrorIs(t, cmd.Execute(), want)
			require.Len(t, client.Actions(), 1)
		})
	}
}

func TestHistoryHelpStatesRetentionBoundary(t *testing.T) {
	var output bytes.Buffer
	cmd := newHistoryCmd(factory.Static{}, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &output, ErrOut: &output,
	}, fixedClock())
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs([]string{"--help"})

	require.NoError(t, cmd.Execute())
	assert.Contains(t, output.String(), "not a durable audit trail")
	assert.Contains(t, output.String(), "active run and")
	assert.Contains(t, output.String(), "single last-run slot")
	assert.Contains(t, output.String(), "history INFERENCESERVICE")
}

func TestHistoryWritesExactMachineGoldens(t *testing.T) {
	healthy := minimalInferenceService()
	healthy.Status.Rollout = &omev1beta1.RolloutStatus{}
	fixtures := map[string]*omev1beta1.InferenceService{
		"healthy": healthy, "partial": historyInferenceService(t),
		"unavailable": minimalInferenceService(),
	}
	for state, isvc := range fixtures {
		isvc.UID = "SECRET_HOSTILE_UID"
		for _, format := range []string{"json", "yaml"} {
			t.Run(state+"/"+format, func(t *testing.T) {
				want, err := os.ReadFile(filepath.Join("testdata", "history_"+state+"."+format))
				require.NoError(t, err)
				output, err := execute(
					t, factory.Static{OME: omefake.NewSimpleClientset(isvc.DeepCopy()), NS: "prod"},
					fixedClock(), "history", "chat", "-o", format,
				)
				require.NoError(t, err)
				assert.Equal(t, string(want), output)
				assert.NotContains(t, output, "SECRET_HOSTILE_UID")
				assert.NotContains(t, output, "uid:")
				assert.NotContains(t, output, `"uid"`)
			})
		}
	}
}

func historyInferenceService(t *testing.T) *omev1beta1.InferenceService {
	t.Helper()
	isvc := minimalInferenceService()
	mode := constants.OMENative
	isvc.Spec.DeploymentMode = &mode
	isvc.Spec.Engine = &omev1beta1.EngineSpec{}
	group := omev1beta1.RolloutGroup{
		Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
		BlueGreen:  &omev1beta1.GroupBlueGreen{},
	}
	isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{group}}
	digest, err := rolloutpolicy.ProgressionDigest(&group)
	require.NoError(t, err)
	timestamp := func(hour, minute int) metav1.Time {
		return metav1.NewTime(time.Date(2026, time.August, 31, hour, minute, 0, 0, time.UTC))
	}
	opened, pinned := timestamp(18, 0), timestamp(18, 30)
	lastOpened, lastClosed := timestamp(16, 0), timestamp(17, 0)
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.EngineComponent: {
			LatestRolledoutRevision:   "chat-engine-rev-aaaaaaaa",
			LatestReadyRevision:       "chat-engine-rev-bbbbbbbb",
			PreviousRolledoutRevision: "chat-engine-rev-dddddddd",
		},
	}
	isvc.Status.RolloutCoordination = &omev1beta1.RolloutCoordinationStatus{
		Groups: []omev1beta1.RolloutCoordinationGroupStatus{{
			Name: "0", Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
			Policy: omev1beta1.CoordinationPolicyBlueGreen, Phase: omev1beta1.CoordinationPhaseWaiting,
		}},
	}
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{
		ActiveRun: &omev1beta1.RolloutRun{
			RunID: "chat-0123456789ab", OpenedAt: opened, PinnedAt: pinned,
			TargetRevisions: []omev1beta1.RolloutRunTarget{{
				Component: omev1beta1.EngineComponent, Revision: "cccccccc",
			}},
			Plan: omev1beta1.RolloutRunPlan{Groups: []omev1beta1.RolloutRunGroup{{
				Source: omev1beta1.RolloutPlanSourceInline, PortableDigest: digest, Group: group,
			}}},
		},
		LastRun: &omev1beta1.RolloutRunRecord{
			Outcome: omev1beta1.RolloutRunCompleted, OpenedAt: &lastOpened, ClosedAt: &lastClosed,
			Groups: []omev1beta1.RolloutRunProvenance{{
				Source: omev1beta1.RolloutPlanSourceInline, PortableDigest: digest,
			}},
		},
		Groups: []omev1beta1.RolloutGroupResolution{{
			Index: 0, Source: omev1beta1.RolloutPlanSourceInline, ObservedDigest: digest,
		}},
	}
	return isvc
}
