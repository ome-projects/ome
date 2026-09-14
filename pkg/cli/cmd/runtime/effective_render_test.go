package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	knapis "knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/runtimerevision"
	"sigs.k8s.io/ome/pkg/runtimeselector"
)

const (
	secretImage       = "private.registry.invalid/runtime:credential-canary"
	secretEnvironment = "secret-environment-canary"
	secretArgument    = "--token=secret-argument-canary"
	secretLabel       = "secret-label-canary"
	secretAnnotation  = "secret-annotation-canary"
	secretRuntimeRV   = "secret-runtime-resource-version"
	secretRevisionRV  = "secret-revision-resource-version"
	secretISVCRV      = "secret-isvc-resource-version"
	secretSyncToken   = "secret-sync-token-canary"
	secretMessage     = "secret-status-message-canary"
	secretKeyRefName  = "secret-key-ref-name-canary"
	secretKeyRefKey   = "secret-key-ref-key-canary"
	secretVolumeName  = "secret-volume-ref-name-canary"
	secretNodeValue   = "secret-node-selector-value-canary"
)

func healthyPinnedFactory(t *testing.T) (*acquisitionFactory, string) {
	return healthyPinnedFactoryVariant(t, false)
}

func orderedStringMap(reversed bool, entries ...[2]string) map[string]string {
	result := make(map[string]string, len(entries))
	if reversed {
		for index := len(entries) - 1; index >= 0; index-- {
			result[entries[index][0]] = entries[index][1]
		}
		return result
	}
	for _, entry := range entries {
		result[entry[0]] = entry[1]
	}
	return result
}

type permutingRuntimeResponseClient struct {
	ctrlclient.Client
	reversed bool
	gets     int
}

func (c *permutingRuntimeResponseClient) Get(
	ctx context.Context,
	key ctrlclient.ObjectKey,
	object ctrlclient.Object,
	options ...ctrlclient.GetOption,
) error {
	if err := c.Client.Get(ctx, key, object, options...); err != nil {
		return err
	}
	runtimeObject, ok := object.(*v1beta1.ClusterServingRuntime)
	if !ok {
		return nil
	}
	// Alternate equivalent map insertion order across resolver responses. The
	// reversed fixture starts with the opposite response order.
	reverseThisResponse := (c.gets%2 == 0) == c.reversed
	c.gets++
	runtimeObject.Labels = orderedStringMap(reverseThisResponse,
		[2]string{"private-label", secretLabel}, [2]string{"second-label", "second-value"},
	)
	runtimeObject.Annotations = orderedStringMap(reverseThisResponse,
		[2]string{"private-annotation", secretAnnotation}, [2]string{"second-annotation", "second-value"},
	)
	runtimeObject.Spec.NodeSelector = orderedStringMap(reverseThisResponse,
		[2]string{"accelerator", "gpu"}, [2]string{"private-node", secretNodeValue},
	)
	return nil
}

func healthyPinnedFactoryVariant(t *testing.T, reversed bool) (*acquisitionFactory, string) {
	t.Helper()
	autoSync := false
	kind := runtimeselector.KindClusterServingRuntime
	runtimeSpec := v1beta1.ServingRuntimeSpec{
		ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{NodeSelector: orderedStringMap(reversed,
			[2]string{"accelerator", "gpu"}, [2]string{"private-node", secretNodeValue},
		)},
		EngineConfig: &v1beta1.EngineSpec{
			PodSpec: v1beta1.PodSpec{Volumes: []corev1.Volume{{
				Name: "runtime-secret", VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{SecretName: secretVolumeName},
				},
			}}},
			Runner: &v1beta1.RunnerSpec{Container: corev1.Container{
				Name: "runner", Image: secretImage,
				Env: []corev1.EnvVar{
					{Name: "TOKEN", Value: secretEnvironment},
					{Name: "SECRET_KEY", ValueFrom: &corev1.EnvVarSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: secretKeyRefName},
							Key:                  secretKeyRefKey,
						},
					}},
				},
				Args: []string{secretArgument},
			}},
		}}
	_, shortHash, err := runtimerevision.Hash(&runtimeSpec)
	require.NoError(t, err)
	revisionName := runtimerevision.Name(
		runtimerevision.KindClusterServingRuntime, "", "cluster-runtime", shortHash,
	)
	runtimeObject := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name: "cluster-runtime", UID: types.UID("runtime-uid"), Generation: 2,
			ResourceVersion: secretRuntimeRV,
			Labels: orderedStringMap(reversed,
				[2]string{"private-label", secretLabel}, [2]string{"second-label", "second-value"},
			),
			Annotations: orderedStringMap(reversed,
				[2]string{"private-annotation", secretAnnotation}, [2]string{"second-annotation", "second-value"},
			),
		},
		Spec: runtimeSpec,
	}
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "service", Namespace: "team-a", UID: types.UID("isvc-uid"),
			ResourceVersion: secretISVCRV, Generation: 4,
			Labels: orderedStringMap(reversed,
				[2]string{"private-label", secretLabel}, [2]string{"second-label", "second-value"},
			),
			Annotations: orderedStringMap(reversed,
				[2]string{"private-annotation", secretAnnotation},
				[2]string{constants.RuntimeSyncAnnotationKey, secretSyncToken},
			),
		},
		Spec: v1beta1.InferenceServiceSpec{
			Runtime: &v1beta1.ServingRuntimeRef{
				Name: "cluster-runtime", Kind: &kind, AutoSync: &autoSync, Revision: &revisionName,
			},
			Engine: &v1beta1.EngineSpec{},
		},
	}
	isvc.Status.ObservedGeneration = 4
	isvc.Status.PinnedRevisionName = revisionName
	isvc.Status.LastRuntimeSyncToken = secretSyncToken
	isvc.Status.Conditions = duckv1.Conditions{{
		Type:   knapis.ConditionType(constants.RuntimeDriftedConditionType),
		Status: corev1.ConditionTrue, Reason: "RevisionMismatch", Message: secretMessage,
	}}
	raw, err := json.Marshal(&runtimeSpec)
	require.NoError(t, err)
	revision := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name: revisionName, Namespace: "control-plane", UID: types.UID("revision-uid"),
			ResourceVersion:   secretRevisionRV,
			CreationTimestamp: metav1.NewTime(time.Date(2026, 8, 30, 1, 2, 3, 0, time.UTC)),
			Labels: orderedStringMap(reversed,
				[2]string{constants.RuntimeRevisionOfLabelKey, "cluster-runtime"},
				[2]string{constants.RuntimeRevisionOfKindLabelKey, string(runtimerevision.KindClusterServingRuntime)},
				[2]string{constants.RuntimeRevisionOfNamespaceLabelKey, ""},
				[2]string{constants.RuntimeRevisionHashLabelKey, shortHash},
				[2]string{"private-label", secretLabel},
			),
			Annotations: orderedStringMap(reversed,
				[2]string{constants.RuntimeRevisionCreatedByKey, constants.RuntimeRevisionCreatedByOMEValue},
				[2]string{"private-annotation", secretAnnotation},
			),
		},
		Data: runtime.RawExtension{Raw: raw}, Revision: 1,
	}
	runtimeClient := &permutingRuntimeResponseClient{
		Client:   ctrlfake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(runtimeObject).Build(),
		reversed: reversed,
	}
	return &acquisitionFactory{
		ome: omefake.NewSimpleClientset(isvc), kube: k8sfake.NewSimpleClientset(revision),
		runtime:   runtimeClient,
		namespace: "team-a",
	}, shortHash
}

func renderHealthyPinned(t *testing.T, format string) string {
	return renderHealthyPinnedVariant(t, format, false)
}

func renderHealthyPinnedVariant(t *testing.T, format string, reversed bool) string {
	t.Helper()
	f, _ := healthyPinnedFactoryVariant(t, reversed)
	var out, errOut bytes.Buffer
	cmd := newEffectiveCmdWithDependencies(f, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &out, ErrOut: &errOut,
	}, fixedEffectiveDependencies())
	cmd.SetArgs([]string{"service", "--ome-namespace", "control-plane", "--output", format})
	require.NoError(t, cmd.Execute())
	require.Empty(t, errOut.String())
	return out.String()
}

func renderStalePinned(t *testing.T, format string) string {
	t.Helper()
	f, _ := healthyPinnedFactory(t)
	inferenceService, err := f.ome.OmeV1beta1().InferenceServices("team-a").Get(
		context.Background(), "service", metav1.GetOptions{},
	)
	require.NoError(t, err)
	inferenceService.Status.ObservedGeneration = 3
	_, err = f.ome.OmeV1beta1().InferenceServices("team-a").Update(
		context.Background(), inferenceService, metav1.UpdateOptions{},
	)
	require.NoError(t, err)

	var out, errOut bytes.Buffer
	cmd := newEffectiveCmdWithDependencies(f, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &out, ErrOut: &errOut,
	}, fixedEffectiveDependencies())
	cmd.SetArgs([]string{"service", "--ome-namespace", "control-plane", "--output", format})
	require.NoError(t, cmd.Execute())
	require.Empty(t, errOut.String())
	return out.String()
}

func renderNotConfigured(t *testing.T, format string) string {
	t.Helper()
	f := completeFactory(t)
	var out, errOut bytes.Buffer
	cmd := newEffectiveCmdWithDependencies(f, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &out, ErrOut: &errOut,
	}, fixedEffectiveDependencies())
	cmd.SetArgs([]string{"service", "--output", format})
	require.NoError(t, cmd.Execute())
	require.Empty(t, errOut.String())
	return out.String()
}

type erroringRuntimeClient struct {
	ctrlclient.Client
	err error
}

type terminalRenderBuffer struct {
	bytes.Buffer
	width int
}

func (w *terminalRenderBuffer) TerminalWidth() (int, bool) {
	return w.width, true
}

func (c erroringRuntimeClient) Get(
	context.Context,
	ctrlclient.ObjectKey,
	ctrlclient.Object,
	...ctrlclient.GetOption,
) error {
	return c.err
}

// TestEffectiveHealthyPinnedExactFormats catches command-level divergence
// between table, JSON, and YAML or loss of the fixed collection timestamp.
func TestEffectiveHealthyPinnedExactFormats(t *testing.T) {
	tests := []struct {
		format string
		want   string
	}{
		{format: "table", want: `SCOPE     FIELD           VALUE
Live      STATE           Available
Live      RUNTIME         CSR/cluster-runtime
Live      HASH            e0e2b0d6
Live      ENGINE          RawDeployment (Default)
Active    STATE           Available
Active    RUNTIME         CSR/cluster-runtime
Active    REVISION        cr-cluster-runtime-e0e2b0d6
Active    HASH            e0e2b0d6
Active    ENGINE          RawDeployment (Default)
Service   PIN             ExplicitPin/Resolved
Service   SYNC            Acknowledged
Service   STATUS          Current
Service   DRIFT           ReportedTrue/RevisionMismatch
Service   LIVE-RELATION   Equal
`},
		{format: "wide", want: `VIEW     STATE       REASON   RUNTIME                                 REVISION                      HASH       COMPONENT   MODE            MODE-SOURCE   PIN           PIN-STATE   SYNC           STATUS    DRIFT                           LIVE-RELATION   ISSUES
Live     Available   -        ClusterServingRuntime/cluster-runtime   -                             e0e2b0d6   engine      RawDeployment   Default       ExplicitPin   Resolved    Acknowledged   Current   ReportedTrue/RevisionMismatch   Equal           -
Active   Available   -        ClusterServingRuntime/cluster-runtime   cr-cluster-runtime-e0e2b0d6   e0e2b0d6   engine      RawDeployment   Default       ExplicitPin   Resolved    Acknowledged   Current   ReportedTrue/RevisionMismatch   Equal           -
`},
		{format: "json", want: `{
  "apiVersion": "cli.ome.io/v1alpha1",
  "kind": "RuntimeEffectiveReport",
  "metadata": {
    "namespace": "team-a",
    "name": "service"
  },
  "collectedAt": "2026-08-31T19:34:56Z",
  "sources": [
    {
      "kind": "ClusterServingRuntime",
      "name": "cluster-runtime",
      "uid": "runtime-uid",
      "generation": 2,
      "evidence": "Observed",
      "collectedAt": "2026-08-31T19:34:56Z"
    },
    {
      "kind": "ControllerRevision",
      "namespace": "control-plane",
      "name": "cr-cluster-runtime-e0e2b0d6",
      "uid": "revision-uid",
      "evidence": "Observed",
      "collectedAt": "2026-08-31T19:34:56Z"
    },
    {
      "kind": "InferenceService",
      "namespace": "team-a",
      "name": "service",
      "uid": "isvc-uid",
      "generation": 4,
      "evidence": "Observed",
      "collectedAt": "2026-08-31T19:34:56Z"
    }
  ],
  "content": {
    "selection": {
      "source": "Explicit",
      "runtime": {
        "apiVersion": "ome.io/v1beta1",
        "kind": "ClusterServingRuntime",
        "name": "cluster-runtime",
        "uid": "runtime-uid",
        "generation": 2
      }
    },
    "inheritance": {
      "state": "Observed",
      "sources": [
        {
          "apiVersion": "ome.io/v1beta1",
          "kind": "ClusterServingRuntime",
          "name": "cluster-runtime",
          "uid": "runtime-uid",
          "generation": 2
        }
      ]
    },
    "pin": {
      "mode": "ExplicitPin",
      "state": "Resolved",
      "requestedRevision": "cr-cluster-runtime-e0e2b0d6",
      "reportedRevision": "cr-cluster-runtime-e0e2b0d6",
      "status": {
        "generation": 4,
        "observedGeneration": 4,
        "freshness": "Current"
      },
      "reportedDrift": {
        "state": "ReportedTrue",
        "cause": "RevisionMismatch"
      },
      "syncState": "Acknowledged"
    },
    "live": {
      "state": "Available",
      "origin": "LiveRuntime",
      "source": {
        "apiVersion": "ome.io/v1beta1",
        "kind": "ClusterServingRuntime",
        "name": "cluster-runtime",
        "uid": "runtime-uid",
        "generation": 2
      },
      "hash": "e0e2b0d6",
      "components": [
        {
          "type": "engine",
          "deploymentMode": "RawDeployment",
          "deploymentModeSource": "Default"
        }
      ]
    },
    "active": {
      "state": "Available",
      "origin": "ControllerRevision",
      "source": {
        "apiVersion": "ome.io/v1beta1",
        "kind": "ClusterServingRuntime",
        "name": "cluster-runtime",
        "uid": "runtime-uid",
        "generation": 2
      },
      "revision": {
        "namespace": "control-plane",
        "name": "cr-cluster-runtime-e0e2b0d6",
        "uid": "revision-uid",
        "createdAt": "2026-08-30T01:02:03Z"
      },
      "hash": "e0e2b0d6",
      "components": [
        {
          "type": "engine",
          "deploymentMode": "RawDeployment",
          "deploymentModeSource": "Default"
        }
      ]
    },
    "liveToActive": "Equal",
    "issues": []
  },
  "warnings": []
}
`},
		{format: "yaml", want: `apiVersion: cli.ome.io/v1alpha1
collectedAt: "2026-08-31T19:34:56Z"
content:
  active:
    components:
    - deploymentMode: RawDeployment
      deploymentModeSource: Default
      type: engine
    hash: e0e2b0d6
    origin: ControllerRevision
    revision:
      createdAt: "2026-08-30T01:02:03Z"
      name: cr-cluster-runtime-e0e2b0d6
      namespace: control-plane
      uid: revision-uid
    source:
      apiVersion: ome.io/v1beta1
      generation: 2
      kind: ClusterServingRuntime
      name: cluster-runtime
      uid: runtime-uid
    state: Available
  inheritance:
    sources:
    - apiVersion: ome.io/v1beta1
      generation: 2
      kind: ClusterServingRuntime
      name: cluster-runtime
      uid: runtime-uid
    state: Observed
  issues: []
  live:
    components:
    - deploymentMode: RawDeployment
      deploymentModeSource: Default
      type: engine
    hash: e0e2b0d6
    origin: LiveRuntime
    source:
      apiVersion: ome.io/v1beta1
      generation: 2
      kind: ClusterServingRuntime
      name: cluster-runtime
      uid: runtime-uid
    state: Available
  liveToActive: Equal
  pin:
    mode: ExplicitPin
    reportedDrift:
      cause: RevisionMismatch
      state: ReportedTrue
    reportedRevision: cr-cluster-runtime-e0e2b0d6
    requestedRevision: cr-cluster-runtime-e0e2b0d6
    state: Resolved
    status:
      freshness: Current
      generation: 4
      observedGeneration: 4
    syncState: Acknowledged
  selection:
    runtime:
      apiVersion: ome.io/v1beta1
      generation: 2
      kind: ClusterServingRuntime
      name: cluster-runtime
      uid: runtime-uid
    source: Explicit
kind: RuntimeEffectiveReport
metadata:
  name: service
  namespace: team-a
sources:
- collectedAt: "2026-08-31T19:34:56Z"
  evidence: Observed
  generation: 2
  kind: ClusterServingRuntime
  name: cluster-runtime
  uid: runtime-uid
- collectedAt: "2026-08-31T19:34:56Z"
  evidence: Observed
  kind: ControllerRevision
  name: cr-cluster-runtime-e0e2b0d6
  namespace: control-plane
  uid: revision-uid
- collectedAt: "2026-08-31T19:34:56Z"
  evidence: Observed
  generation: 4
  kind: InferenceService
  name: service
  namespace: team-a
  uid: isvc-uid
warnings: []
`},
	}
	for _, test := range tests {
		t.Run(test.format, func(t *testing.T) {
			assert.Equal(t, test.want, renderHealthyPinned(t, test.format))
		})
	}
}

func TestEffectiveTableAdaptsToTerminalWidth(t *testing.T) {
	f, _ := healthyPinnedFactory(t)
	out := &terminalRenderBuffer{width: 80}
	var errOut bytes.Buffer
	cmd := newEffectiveCmdWithDependencies(f, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: out, ErrOut: &errOut,
	}, fixedEffectiveDependencies())
	cmd.SetArgs([]string{"service", "--ome-namespace", "control-plane"})

	require.NoError(t, cmd.Execute())
	require.Empty(t, errOut.String())
	for _, line := range strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n") {
		assert.LessOrEqual(t, len([]rune(line)), 80, "line %q", line)
	}
	assert.Contains(t, out.String(), "SCOPE")
	assert.NotContains(t, out.String(), "SCOPE:")
	assert.Contains(t, out.String(), "CSR/cluster-runtime")
	assert.Contains(t, out.String(), "ReportedTrue/RevisionMismatch")
}

// TestEffectiveStalePinnedExactFormats catches observedGeneration lag being
// presented as current in any full-command output format.
func TestEffectiveStalePinnedExactFormats(t *testing.T) {
	tests := []struct {
		format string
		want   string
	}{
		{format: "table", want: `SCOPE     FIELD           VALUE
Live      STATE           Available
Live      RUNTIME         CSR/cluster-runtime
Live      HASH            e0e2b0d6
Live      ENGINE          RawDeployment (Default)
Active    STATE           Available
Active    RUNTIME         CSR/cluster-runtime
Active    REVISION        cr-cluster-runtime-e0e2b0d6
Active    HASH            e0e2b0d6
Active    ENGINE          RawDeployment (Default)
Service   PIN             ExplicitPin/Resolved
Service   SYNC            Acknowledged
Service   STATUS          Stale
Service   DRIFT           ReportedTrue/RevisionMismatch
Service   LIVE-RELATION   Equal
Service   ISSUE           StatusStale
`},
		{format: "json", want: `{
  "apiVersion": "cli.ome.io/v1alpha1",
  "kind": "RuntimeEffectiveReport",
  "metadata": {
    "namespace": "team-a",
    "name": "service"
  },
  "collectedAt": "2026-08-31T19:34:56Z",
  "sources": [
    {
      "kind": "ClusterServingRuntime",
      "name": "cluster-runtime",
      "uid": "runtime-uid",
      "generation": 2,
      "evidence": "Observed",
      "collectedAt": "2026-08-31T19:34:56Z"
    },
    {
      "kind": "ControllerRevision",
      "namespace": "control-plane",
      "name": "cr-cluster-runtime-e0e2b0d6",
      "uid": "revision-uid",
      "evidence": "Observed",
      "collectedAt": "2026-08-31T19:34:56Z"
    },
    {
      "kind": "InferenceService",
      "namespace": "team-a",
      "name": "service",
      "uid": "isvc-uid",
      "generation": 4,
      "evidence": "Observed",
      "collectedAt": "2026-08-31T19:34:56Z"
    }
  ],
  "content": {
    "selection": {
      "source": "Explicit",
      "runtime": {
        "apiVersion": "ome.io/v1beta1",
        "kind": "ClusterServingRuntime",
        "name": "cluster-runtime",
        "uid": "runtime-uid",
        "generation": 2
      }
    },
    "inheritance": {
      "state": "Observed",
      "sources": [
        {
          "apiVersion": "ome.io/v1beta1",
          "kind": "ClusterServingRuntime",
          "name": "cluster-runtime",
          "uid": "runtime-uid",
          "generation": 2
        }
      ]
    },
    "pin": {
      "mode": "ExplicitPin",
      "state": "Resolved",
      "requestedRevision": "cr-cluster-runtime-e0e2b0d6",
      "reportedRevision": "cr-cluster-runtime-e0e2b0d6",
      "status": {
        "generation": 4,
        "observedGeneration": 3,
        "freshness": "Stale"
      },
      "reportedDrift": {
        "state": "ReportedTrue",
        "cause": "RevisionMismatch"
      },
      "syncState": "Acknowledged"
    },
    "live": {
      "state": "Available",
      "origin": "LiveRuntime",
      "source": {
        "apiVersion": "ome.io/v1beta1",
        "kind": "ClusterServingRuntime",
        "name": "cluster-runtime",
        "uid": "runtime-uid",
        "generation": 2
      },
      "hash": "e0e2b0d6",
      "components": [
        {
          "type": "engine",
          "deploymentMode": "RawDeployment",
          "deploymentModeSource": "Default"
        }
      ]
    },
    "active": {
      "state": "Available",
      "origin": "ControllerRevision",
      "source": {
        "apiVersion": "ome.io/v1beta1",
        "kind": "ClusterServingRuntime",
        "name": "cluster-runtime",
        "uid": "runtime-uid",
        "generation": 2
      },
      "revision": {
        "namespace": "control-plane",
        "name": "cr-cluster-runtime-e0e2b0d6",
        "uid": "revision-uid",
        "createdAt": "2026-08-30T01:02:03Z"
      },
      "hash": "e0e2b0d6",
      "components": [
        {
          "type": "engine",
          "deploymentMode": "RawDeployment",
          "deploymentModeSource": "Default"
        }
      ]
    },
    "liveToActive": "Equal",
    "issues": [
      {
        "code": "StatusStale"
      }
    ]
  },
  "warnings": [
    {
      "code": "PartialData"
    },
    {
      "code": "StaleEvidence"
    }
  ]
}
`},
		{format: "yaml", want: `apiVersion: cli.ome.io/v1alpha1
collectedAt: "2026-08-31T19:34:56Z"
content:
  active:
    components:
    - deploymentMode: RawDeployment
      deploymentModeSource: Default
      type: engine
    hash: e0e2b0d6
    origin: ControllerRevision
    revision:
      createdAt: "2026-08-30T01:02:03Z"
      name: cr-cluster-runtime-e0e2b0d6
      namespace: control-plane
      uid: revision-uid
    source:
      apiVersion: ome.io/v1beta1
      generation: 2
      kind: ClusterServingRuntime
      name: cluster-runtime
      uid: runtime-uid
    state: Available
  inheritance:
    sources:
    - apiVersion: ome.io/v1beta1
      generation: 2
      kind: ClusterServingRuntime
      name: cluster-runtime
      uid: runtime-uid
    state: Observed
  issues:
  - code: StatusStale
  live:
    components:
    - deploymentMode: RawDeployment
      deploymentModeSource: Default
      type: engine
    hash: e0e2b0d6
    origin: LiveRuntime
    source:
      apiVersion: ome.io/v1beta1
      generation: 2
      kind: ClusterServingRuntime
      name: cluster-runtime
      uid: runtime-uid
    state: Available
  liveToActive: Equal
  pin:
    mode: ExplicitPin
    reportedDrift:
      cause: RevisionMismatch
      state: ReportedTrue
    reportedRevision: cr-cluster-runtime-e0e2b0d6
    requestedRevision: cr-cluster-runtime-e0e2b0d6
    state: Resolved
    status:
      freshness: Stale
      generation: 4
      observedGeneration: 3
    syncState: Acknowledged
  selection:
    runtime:
      apiVersion: ome.io/v1beta1
      generation: 2
      kind: ClusterServingRuntime
      name: cluster-runtime
      uid: runtime-uid
    source: Explicit
kind: RuntimeEffectiveReport
metadata:
  name: service
  namespace: team-a
sources:
- collectedAt: "2026-08-31T19:34:56Z"
  evidence: Observed
  generation: 2
  kind: ClusterServingRuntime
  name: cluster-runtime
  uid: runtime-uid
- collectedAt: "2026-08-31T19:34:56Z"
  evidence: Observed
  kind: ControllerRevision
  name: cr-cluster-runtime-e0e2b0d6
  namespace: control-plane
  uid: revision-uid
- collectedAt: "2026-08-31T19:34:56Z"
  evidence: Observed
  generation: 4
  kind: InferenceService
  name: service
  namespace: team-a
  uid: isvc-uid
warnings:
- code: PartialData
- code: StaleEvidence
`},
	}
	for _, test := range tests {
		t.Run(test.format, func(t *testing.T) {
			assert.Equal(t, test.want, renderStalePinned(t, test.format))
		})
	}
}

// TestEffectiveNotConfiguredExactFormats catches degraded evidence being
// escalated to an error or rendered differently across the three formats.
func TestEffectiveNotConfiguredExactFormats(t *testing.T) {
	tests := []struct {
		format string
		want   string
	}{
		{format: "table", want: `SCOPE     FIELD           VALUE
Live      STATE           Unavailable
Live      REASON          NotConfigured
Active    STATE           Unavailable
Active    REASON          NotConfigured
Service   PIN             AutoSync/Unavailable
Service   SYNC            Absent
Service   STATUS          Unobserved
Service   DRIFT           NotReported
Service   LIVE-RELATION   Unknown
Service   ISSUE           InheritanceUnavailable
Service   ISSUE           StatusUnobserved
`},
		{format: "json", want: `{
  "apiVersion": "cli.ome.io/v1alpha1",
  "kind": "RuntimeEffectiveReport",
  "metadata": {
    "namespace": "team-a",
    "name": "service"
  },
  "collectedAt": "2026-08-31T19:34:56Z",
  "sources": [
    {
      "kind": "InferenceService",
      "namespace": "team-a",
      "name": "service",
      "uid": "isvc-uid",
      "evidence": "Observed",
      "collectedAt": "2026-08-31T19:34:56Z"
    }
  ],
  "content": {
    "selection": {
      "source": "Selected"
    },
    "inheritance": {
      "state": "Unavailable",
      "sources": [],
      "unavailableReason": "NotConfigured"
    },
    "pin": {
      "mode": "AutoSync",
      "state": "Unavailable",
      "status": {
        "generation": 0,
        "observedGeneration": 0,
        "freshness": "Unobserved"
      },
      "reportedDrift": {
        "state": "NotReported"
      },
      "syncState": "Absent"
    },
    "live": {
      "state": "Unavailable",
      "components": [],
      "unavailableReason": "NotConfigured"
    },
    "active": {
      "state": "Unavailable",
      "components": [],
      "unavailableReason": "NotConfigured"
    },
    "liveToActive": "Unknown",
    "issues": [
      {
        "code": "InheritanceUnavailable"
      },
      {
        "code": "StatusUnobserved"
      }
    ]
  },
  "warnings": [
    {
      "code": "PartialData"
    }
  ]
}
`},
		{format: "yaml", want: `apiVersion: cli.ome.io/v1alpha1
collectedAt: "2026-08-31T19:34:56Z"
content:
  active:
    components: []
    state: Unavailable
    unavailableReason: NotConfigured
  inheritance:
    sources: []
    state: Unavailable
    unavailableReason: NotConfigured
  issues:
  - code: InheritanceUnavailable
  - code: StatusUnobserved
  live:
    components: []
    state: Unavailable
    unavailableReason: NotConfigured
  liveToActive: Unknown
  pin:
    mode: AutoSync
    reportedDrift:
      state: NotReported
    state: Unavailable
    status:
      freshness: Unobserved
      generation: 0
      observedGeneration: 0
    syncState: Absent
  selection:
    source: Selected
kind: RuntimeEffectiveReport
metadata:
  name: service
  namespace: team-a
sources:
- collectedAt: "2026-08-31T19:34:56Z"
  evidence: Observed
  kind: InferenceService
  name: service
  namespace: team-a
  uid: isvc-uid
warnings:
- code: PartialData
`},
	}
	for _, test := range tests {
		t.Run(test.format, func(t *testing.T) {
			assert.Equal(t, test.want, renderNotConfigured(t, test.format))
		})
	}
}

// TestEffectiveHealthyPinnedOutputIsDeterministic catches output depending on
// map insertion or the order of semantically equivalent resolver responses.
func TestEffectiveHealthyPinnedOutputIsDeterministic(t *testing.T) {
	for _, format := range []string{"table", "json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			renderCounts := [2]int{}
			var baseline string
			for repetition := 0; repetition < 3; repetition++ {
				forward := renderHealthyPinnedVariant(t, format, false)
				renderCounts[0]++
				reversed := renderHealthyPinnedVariant(t, format, true)
				renderCounts[1]++
				if repetition == 0 {
					baseline = forward
				}
				assert.Equal(t, baseline, forward,
					"forward semantic variant repetition %d must be byte-identical", repetition)
				assert.Equal(t, baseline, reversed,
					"reversed map insertion and resolver response ordering repetition %d must be byte-identical", repetition)
			}
			assert.Equal(t, [2]int{3, 3}, renderCounts,
				"each semantic variant must be rendered exactly three times")
		})
	}
}

// TestEffectiveHealthyPinnedRedactsPrivateFields catches secret-bearing runtime
// or revision content crossing the allowlisted report boundary in any format.
func TestEffectiveHealthyPinnedRedactsPrivateFields(t *testing.T) {
	canaries := []string{
		secretImage, secretEnvironment, secretArgument, secretLabel, secretAnnotation,
		secretRuntimeRV, secretRevisionRV, secretISVCRV, secretSyncToken, secretMessage,
		secretKeyRefName, secretKeyRefKey, secretVolumeName, secretNodeValue,
	}
	for _, format := range []string{"table", "json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			output := renderHealthyPinned(t, format)
			for _, canary := range canaries {
				assert.NotContains(t, output, canary)
			}
		})
	}
}

// TestEffectiveRedactsHostileConditionEnums catches arbitrary condition status
// and reason strings escaping instead of their bounded classifications.
func TestEffectiveRedactsHostileConditionEnums(t *testing.T) {
	const hostileReason = "secret-hostile-reason-enum"
	const hostileStatus = "secret-hostile-status-enum"
	for _, format := range []string{"table", "json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			f, _ := healthyPinnedFactory(t)
			client := f.ome.(*omefake.Clientset)
			resource := v1beta1.SchemeGroupVersion.WithResource("inferenceservices")
			object, err := client.Tracker().Get(resource, "team-a", "service")
			require.NoError(t, err)
			isvc := object.(*v1beta1.InferenceService).DeepCopy()
			isvc.Status.Conditions[0].Reason = hostileReason
			isvc.Status.Conditions[0].Status = corev1.ConditionStatus(hostileStatus)
			require.NoError(t, client.Tracker().Update(resource, isvc, "team-a"))
			var out, errOut bytes.Buffer
			cmd := newEffectiveCmdWithDependencies(f, genericiooptions.IOStreams{
				In: &bytes.Buffer{}, Out: &out, ErrOut: &errOut,
			}, fixedEffectiveDependencies())
			cmd.SetArgs([]string{"service", "--ome-namespace", "control-plane", "--output", format})
			require.NoError(t, cmd.Execute())
			assert.Contains(t, out.String(), "ReportedDriftConflict")
			assert.NotContains(t, out.String(), hostileReason)
			assert.NotContains(t, out.String(), hostileStatus)
			assert.Empty(t, errOut.String())
		})
	}
}

// TestEffectiveOptionalFailuresRenderBoundedDiagnostics catches optional API
// causes leaking to stdout/stderr or incorrectly turning degraded evidence into
// a command failure.
func TestEffectiveOptionalFailuresRenderBoundedDiagnostics(t *testing.T) {
	tests := []struct {
		name   string
		issue  string
		canary string
		mutate func(*acquisitionFactory)
	}{
		{
			name: "exact revision", issue: "RevisionUnavailable", canary: "optional-revision-error-secret",
			mutate: func(f *acquisitionFactory) {
				client := k8sfake.NewSimpleClientset()
				client.PrependReactor("get", "controllerrevisions", func(k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, errors.New("optional-revision-error-secret")
				})
				f.kube = client
			},
		},
		{
			name: "live runtime", issue: "LiveRuntimeUnavailable", canary: "optional-live-error-secret",
			mutate: func(f *acquisitionFactory) {
				f.runtime = erroringRuntimeClient{Client: f.runtime, err: errors.New("optional-live-error-secret")}
			},
		},
	}
	for _, test := range tests {
		for _, format := range []string{"table", "json", "yaml"} {
			t.Run(test.name+"/"+format, func(t *testing.T) {
				f, _ := healthyPinnedFactory(t)
				test.mutate(f)
				var out, errOut bytes.Buffer
				cmd := newEffectiveCmdWithDependencies(f, genericiooptions.IOStreams{
					In: &bytes.Buffer{}, Out: &out, ErrOut: &errOut,
				}, fixedEffectiveDependencies())
				cmd.SetArgs([]string{"service", "--ome-namespace", "control-plane", "--output", format})
				require.NoError(t, cmd.Execute())
				assert.Contains(t, out.String(), test.issue)
				assert.NotContains(t, out.String(), test.canary)
				assert.NotContains(t, errOut.String(), test.canary)
				assert.Empty(t, errOut.String())
			})
		}
	}
}
