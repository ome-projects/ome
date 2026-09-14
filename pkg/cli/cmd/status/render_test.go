package status

import (
	"bytes"
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

var update = flag.Bool("update", false, "rewrite golden files")

func assertGolden(t *testing.T, name string, got []byte) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if *update {
		require.NoError(t, os.WriteFile(path, got, 0o644))
	}
	want, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, string(want), string(got))
}

func TestRenderNotReady(t *testing.T) {
	r := &report{
		ISVC: &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "llama-70b", Namespace: "team-a"},
			Spec: v1beta1.InferenceServiceSpec{
				Model:   &v1beta1.ModelRef{Name: "llama-3-3-70b"},
				Runtime: &v1beta1.ServingRuntimeRef{Name: "srt-llama-70b"},
			},
			Status: v1beta1.InferenceServiceStatus{
				Status: duckv1.Status{Conditions: duckv1.Conditions{
					{Type: "EngineReady", Status: corev1.ConditionTrue},
					{Type: "DecoderReady", Status: corev1.ConditionFalse, Reason: "RevisionFailed", Message: "0/1 replicas ready"},
					{Type: apis.ConditionReady, Status: corev1.ConditionFalse, Reason: "DecoderNotReady"},
				}},
				Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
					v1beta1.EngineComponent:  {LatestReadyRevision: "llama-70b-engine-00002"},
					v1beta1.DecoderComponent: {LatestReadyRevision: "llama-70b-decoder-00002"},
				},
			},
		},
		Pods: map[v1beta1.ComponentType][]corev1.Pod{
			v1beta1.EngineComponent: {{
				ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-5d4-x2p"},
				Spec: corev1.PodSpec{
					NodeName:   "gpu-node-1",
					Containers: []corev1.Container{{Name: "engine"}, {Name: "engine-metrics"}},
				},
				Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{
					{Ready: true}, {Ready: true},
				}},
			}},
			v1beta1.DecoderComponent: {{
				ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-decoder-7c9-k4m"},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "decoder"}, {Name: "decoder-metrics"}}},
				Status:     corev1.PodStatus{Phase: corev1.PodPending, ContainerStatuses: []corev1.ContainerStatus{{}, {}}},
			}},
		},
		Events: []corev1.Event{{
			InvolvedObject: corev1.ObjectReference{Kind: "Pod", Name: "llama-70b-decoder-7c9-k4m"},
			Reason:         "FailedScheduling",
			Message:        "0/12 nodes: insufficient nvidia.com/gpu",
		}},
	}
	var buf bytes.Buffer
	require.NoError(t, render(r, &buf))
	assertGolden(t, "status_notready.golden", buf.Bytes())
}

// TestRenderShowsUnlabeledPods pins the forward-note from Task 3.1: gather()
// buckets pods missing the component label under ComponentType(""), and the
// renderer must surface them (as an explicit "(unlabeled)" section) rather
// than silently dropping them because they fall outside componentOrder.
func TestRenderShowsUnlabeledPods(t *testing.T) {
	r := &report{
		ISVC: &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "orphan-pods", Namespace: "team-b"},
			Status: v1beta1.InferenceServiceStatus{
				Status: duckv1.Status{Conditions: duckv1.Conditions{
					{Type: apis.ConditionReady, Status: corev1.ConditionTrue},
				}},
			},
		},
		Pods: map[v1beta1.ComponentType][]corev1.Pod{
			v1beta1.EngineComponent: {{
				ObjectMeta: metav1.ObjectMeta{Name: "orphan-pods-engine-1"},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "engine"}}},
				Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{
					{Ready: true},
				}},
			}},
			v1beta1.ComponentType(""): {{
				ObjectMeta: metav1.ObjectMeta{Name: "orphan-pods-stray-7f8"},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "main"}}},
				Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{
					{Ready: true},
				}},
			}},
		},
	}
	var buf bytes.Buffer
	require.NoError(t, render(r, &buf))
	assertGolden(t, "status_unlabeled.golden", buf.Bytes())
	// Belt-and-suspenders: fail even if a golden update ever masks a
	// regression here (blind `-update` reruns would still hide a dropped
	// pod otherwise).
	assert.Contains(t, buf.String(), "(unlabeled)")
	assert.Contains(t, buf.String(), "orphan-pods-stray-7f8")
}

// TestRenderShowsOptionalStatusSections pins the v1-deferred presence
// indicators for Traffic/Canary/Placement/RolloutCoordination: full
// rendering is follow-up work, but their presence must never be silently
// omitted from the report. TestRenderNotReady (all four nil) already pins
// the absent case via its golden file.
func TestRenderShowsOptionalStatusSections(t *testing.T) {
	r := &report{
		ISVC: &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "llama-70b", Namespace: "team-a"},
			Status: v1beta1.InferenceServiceStatus{
				Traffic:             &v1beta1.TrafficStatus{},
				Canary:              &v1beta1.CanaryStatus{},
				Placement:           &v1beta1.PlacementStatus{},
				RolloutCoordination: &v1beta1.RolloutCoordinationStatus{},
			},
		},
	}
	var buf bytes.Buffer
	require.NoError(t, render(r, &buf))
	out := buf.String()
	assert.Contains(t, out, "  Traffic: present (inspect with kubectl get inferenceservice -o yaml)\n")
	assert.Contains(t, out, "  Canary: present (inspect with kubectl get inferenceservice -o yaml)\n")
	assert.Contains(t, out, "  Placement: present (inspect with kubectl get inferenceservice -o yaml)\n")
	assert.Contains(t, out, "  RolloutCoordination: present (inspect with kubectl get inferenceservice -o yaml)\n")
}

func TestRenderPropagatesObservationWarningWriterError(t *testing.T) {
	want := errors.New("warning write failed")
	w := &failOnWrite{call: 5, err: want}
	r := &report{
		ISVC: &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "team-a"},
		},
		Pods:     map[v1beta1.ComponentType][]corev1.Pod{},
		Warnings: []string{"bounded warning"},
	}

	require.ErrorIs(t, render(r, w), want)
}

type failOnWrite struct {
	writes int
	call   int
	err    error
}

func (w *failOnWrite) Write(p []byte) (int, error) {
	w.writes++
	if w.writes == w.call {
		return 0, w.err
	}
	return io.Discard.Write(p)
}
