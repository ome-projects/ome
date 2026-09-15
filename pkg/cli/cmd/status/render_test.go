package status

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

var update = flag.Bool("update", false, "rewrite golden files")

func TestRenderReadyEvidenceAgreesAcrossViews(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		status corev1.ConditionStatus
		want   string
	}{
		{name: "absent", want: "Unknown"},
		{name: "unknown", status: corev1.ConditionUnknown, want: "Unknown"},
		{name: "false", status: corev1.ConditionFalse, want: "False"},
		{name: "true", status: corev1.ConditionTrue, want: "True"},
		{name: "invalid", status: "HIDDEN-INVALID-READY", want: "Unknown"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r := &report{ISVC: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "team-a"},
			}}
			if test.status != "" {
				r.ISVC.Status.Conditions = duckv1.Conditions{{
					Type: apis.ConditionReady, Status: test.status,
				}}
			}
			var compact, wide bytes.Buffer
			require.NoError(t, renderCompact(r, &compact))
			require.NoError(t, render(r, &wide))
			assert.Contains(t, compact.String(), "READY       "+test.want+"\n")
			assert.Contains(t, wide.String(), "Ready:      "+test.want+"\n")
			assert.NotContains(t, compact.String(), "HIDDEN-")
			// Wide conditions retain their existing independent detail view;
			// the summary must never promote an invalid value to False or True.
		})
	}
}

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

func TestRenderCompactSummarizesOperationalEvidence(t *testing.T) {
	r := compactReportFixture()

	var buf bytes.Buffer
	require.NoError(t, renderCompact(r, &buf))

	assert.Equal(t,
		"FIELD          VALUE\n"+
			"NAME           llama-70b\n"+
			"NAMESPACE      team-a\n"+
			"READY          False\n"+
			"READY-REASON   DecoderNotReady\n"+
			"MODEL          llama-3-3-70b\n"+
			"MODEL-STATE    UpToDate\n"+
			"RUNTIME        srt-llama-70b\n"+
			"\n"+
			"COMPONENT   STATUS   READY-PODS   RESTARTS   PHASES\n"+
			"engine      True     1/1          2          R1\n"+
			"decoder     False    0/1          1          P1\n"+
			"Phase key: R=Running P=Pending F=Failed S=Succeeded U=Unknown T=Terminating\n"+
			"\n"+
			"FEATURE   STATUS\n"+
			"TRAFFIC   present\n"+
			"ROLLOUT   run,canary,coordination\n"+
			"\n"+
			"Observation Warnings:\n"+
			"WARNING\n"+
			"warning-one\n"+
			"warning-two\n"+
			"warning-three\n"+
			"warning-four\n"+
			"warning-five\n"+
			"... 1 more warning; use -o wide\n"+
			"\n"+
			"Recent Warning Events:\n"+
			"OBJECT       REASON             MESSAGE\n"+
			"Pod/pod-01   FailedScheduling   message-01\n"+
			"Pod/pod-02   FailedScheduling   message-02\n"+
			"Pod/pod-03   FailedScheduling   message-03\n"+
			"Pod/pod-04   FailedScheduling   message-04\n"+
			"Pod/pod-05   FailedScheduling   message-05\n"+
			"... 1 more event; use -o wide\n",
		buf.String(),
	)
}

func TestRenderCompactRetainsUnlabeledPodHealth(t *testing.T) {
	r := &report{
		ISVC: &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "orphan-pods", Namespace: "team-b"},
		},
		Pods: map[v1beta1.ComponentType][]corev1.Pod{
			v1beta1.ComponentType(""): {{
				ObjectMeta: metav1.ObjectMeta{Name: "orphan-pods-stray-7f8"},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "main"}}},
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
					Conditions: []corev1.PodCondition{{
						Type: corev1.PodReady, Status: corev1.ConditionTrue,
					}},
				},
			}},
		},
	}

	var buf bytes.Buffer
	require.NoError(t, renderCompact(r, &buf))

	assert.Contains(t, buf.String(), "(unlabeled)   -        1/1")
}

func TestRenderCompactBoundsTerminalLinesAndDynamicCells(t *testing.T) {
	r := compactReportFixture()
	r.ISVC.Name = strings.Repeat("service-", 30)
	r.ISVC.Spec.Model.Name = strings.Repeat("model-", 30)
	r.ISVC.Spec.Runtime.Name = strings.Repeat("runtime-", 30)
	r.Warnings[0] = strings.Repeat("warning-", 30) + "HIDDEN-WARNING-TAIL"
	r.Events[0].InvolvedObject.Name = strings.Repeat("pod-", 30) + "HIDDEN-OBJECT-TAIL"
	r.Events[0].Reason = strings.Repeat("reason-", 30) + "HIDDEN-REASON-TAIL"
	r.Events[0].Message = strings.Repeat("message-", 30) + "HIDDEN-MESSAGE-TAIL"
	out := &statusTerminalBuffer{width: 80}

	require.NoError(t, renderCompact(r, out))

	for lineNumber, line := range strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n") {
		assert.LessOrEqual(t, len([]rune(line)), 80, "line %d: %q", lineNumber+1, line)
	}
	for _, hidden := range []string{
		"HIDDEN-WARNING-TAIL", "HIDDEN-OBJECT-TAIL", "HIDDEN-REASON-TAIL", "HIDDEN-MESSAGE-TAIL",
	} {
		assert.NotContains(t, out.String(), hidden)
	}
}

func TestRenderCompactBoundsSanitizedDisplayWidthWithoutTerminalMetadata(t *testing.T) {
	r := compactReportFixture()
	r.Warnings = []string{strings.Repeat("\n", maxCompactWarningRunes) + "HIDDEN-WARNING-TAIL"}
	r.Events = []corev1.Event{{
		InvolvedObject: corev1.ObjectReference{
			Kind: "Pod", Name: strings.Repeat("界", maxCompactEventObjectRunes) + "HIDDEN-OBJECT-TAIL",
		},
		Reason:  strings.Repeat("\x1b", maxCompactEventReasonRunes) + "HIDDEN-REASON-TAIL",
		Message: strings.Repeat("界", maxCompactEventMessageRunes) + "HIDDEN-MESSAGE-TAIL",
	}}

	var buf bytes.Buffer
	require.NoError(t, renderCompact(r, &buf))

	for lineNumber, line := range strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n") {
		assert.LessOrEqual(t, fixtureDisplayWidth(line), 80, "line %d: %q", lineNumber+1, line)
	}
	for _, hidden := range []string{
		"HIDDEN-WARNING-TAIL", "HIDDEN-OBJECT-TAIL", "HIDDEN-REASON-TAIL", "HIDDEN-MESSAGE-TAIL",
	} {
		assert.NotContains(t, buf.String(), hidden)
	}
	assert.NotContains(t, buf.String(), "\x1b")
}

func TestRenderCompactBoundsMixedWidthEventRowsWithoutTerminalMetadata(t *testing.T) {
	r := compactReportFixture()
	r.Warnings = nil
	r.Events = []corev1.Event{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "ascii"},
			InvolvedObject: corev1.ObjectReference{
				Kind: "Pod", Name: strings.Repeat("a", 20),
			},
			Reason:  strings.Repeat("a", maxCompactEventReasonRunes),
			Message: strings.Repeat("a", maxCompactEventMessageRunes),
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "wide"},
			InvolvedObject: corev1.ObjectReference{
				Kind: "Pod", Name: strings.Repeat("b", 20),
			},
			Reason:  strings.Repeat("界", maxCompactEventReasonRunes/2),
			Message: strings.Repeat("b", maxCompactEventMessageRunes),
		},
	}

	var buf bytes.Buffer
	require.NoError(t, renderCompact(r, &buf))

	for lineNumber, line := range strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n") {
		assert.LessOrEqual(t, fixtureDisplayWidth(line), 80, "line %d: %q", lineNumber+1, line)
	}
}

func fixtureDisplayWidth(value string) int {
	width := 0
	for _, char := range value {
		if char == '界' {
			width += 2
			continue
		}
		width++
	}
	return width
}

func TestRenderCompactSelectsMostRecentWarningEvents(t *testing.T) {
	r := &report{
		ISVC: &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "team-a"},
		},
	}
	for i := 1; i <= 6; i++ {
		r.Events = append(r.Events, corev1.Event{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("event-%02d", i)},
			InvolvedObject: corev1.ObjectReference{
				Kind: "Pod", Name: fmt.Sprintf("pod-%02d", i),
			},
			Reason:        "ProbeFailed",
			Message:       fmt.Sprintf("message-%02d", i),
			LastTimestamp: metav1.NewTime(time.Unix(int64(i), 0)),
		})
	}

	var buf bytes.Buffer
	require.NoError(t, renderCompact(r, &buf))

	assert.NotContains(t, buf.String(), "pod-01")
	for i := 2; i <= 6; i++ {
		assert.Contains(t, buf.String(), fmt.Sprintf("pod-%02d", i))
	}
	assert.Less(t, strings.Index(buf.String(), "pod-06"), strings.Index(buf.String(), "pod-05"))
}

func TestRenderCompactCapsNoncanonicalComponentsWithAggregateEvidence(t *testing.T) {
	r := &report{
		ISVC: &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "team-a"},
		},
		Pods: make(map[v1beta1.ComponentType][]corev1.Pod),
	}
	for _, component := range []string{
		"custom-a", "custom-b", "custom-c", "custom-d", "custom-e", "custom-f", "custom-g",
	} {
		r.Pods[v1beta1.ComponentType(component)] = []corev1.Pod{{
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{{
					Type: corev1.PodReady, Status: corev1.ConditionTrue,
				}},
			},
		}}
	}

	var buf bytes.Buffer
	require.NoError(t, renderCompact(r, &buf))

	for _, component := range []string{"custom-a", "custom-b", "custom-c", "custom-d", "custom-e"} {
		assert.Contains(t, buf.String(), component)
	}
	assert.NotContains(t, buf.String(), "custom-f")
	assert.NotContains(t, buf.String(), "custom-g")
	assert.Contains(t, buf.String(), "(2 more)")
	assert.Contains(t, buf.String(), "... 2 more components; use -o wide")
}

func TestRenderCompactWorstCaseAggregatesFitWithoutTerminalMetadata(t *testing.T) {
	const component = v1beta1.ComponentType("abcdefghijklmnopqrstuv")
	r := &report{
		ISVC: &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "team-a"},
		},
		Pods: map[v1beta1.ComponentType][]corev1.Pod{component: {}},
	}
	now := metav1.NewTime(time.Unix(1, 0))
	for i := 0; i < maxStatusPods; i++ {
		pod := corev1.Pod{Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{RestartCount: 1<<31 - 1}},
		}}
		switch i % 6 {
		case 0:
			pod.Status.Phase = corev1.PodRunning
		case 1:
			pod.Status.Phase = corev1.PodPending
		case 2:
			pod.Status.Phase = corev1.PodFailed
		case 3:
			pod.Status.Phase = corev1.PodSucceeded
		case 4:
			pod.Status.Phase = corev1.PodPhase("FuturePhase")
		case 5:
			pod.Status.Phase = corev1.PodRunning
			pod.DeletionTimestamp = &now
		}
		r.Pods[component] = append(r.Pods[component], pod)
	}

	var buf bytes.Buffer
	require.NoError(t, renderCompact(r, &buf))

	assert.Contains(t, buf.String(), "R167/P167/F167/S167/U166/T166")
	assert.Contains(t, buf.String(), ">9999999")
	for lineNumber, line := range strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n") {
		assert.LessOrEqual(t, len([]rune(line)), 80, "line %d: %q", lineNumber+1, line)
	}
}

func TestRenderCompactCanonicalizesInvalidConditionStatuses(t *testing.T) {
	r := compactReportFixture()
	r.ISVC.Status.Conditions[0].Status = corev1.ConditionStatus("HIDDEN-ENGINE-STATUS")
	r.ISVC.Status.Conditions[2].Status = corev1.ConditionStatus("HIDDEN-READY-STATUS")

	var buf bytes.Buffer
	require.NoError(t, renderCompact(r, &buf))

	assert.Contains(t, buf.String(), "READY          Unknown")
	assert.Contains(t, buf.String(), "engine      Unknown")
	assert.NotContains(t, buf.String(), "HIDDEN-")
}

func compactReportFixture() *report {
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
					{Type: "DecoderReady", Status: corev1.ConditionFalse, Reason: "RevisionFailed"},
					{Type: apis.ConditionReady, Status: corev1.ConditionFalse, Reason: "DecoderNotReady"},
				}},
				Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
					v1beta1.EngineComponent:  {LatestReadyRevision: "llama-70b-engine-00002"},
					v1beta1.DecoderComponent: {LatestReadyRevision: "llama-70b-decoder-00002"},
				},
				ModelStatus:         v1beta1.ModelStatus{TransitionStatus: v1beta1.UpToDate},
				Traffic:             &v1beta1.TrafficStatus{},
				Canary:              &v1beta1.CanaryStatus{},
				Rollout:             &v1beta1.RolloutStatus{},
				RolloutCoordination: &v1beta1.RolloutCoordinationStatus{},
			},
		},
		Pods: map[v1beta1.ComponentType][]corev1.Pod{
			v1beta1.EngineComponent: {{
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "engine"}}},
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
					Conditions: []corev1.PodCondition{{
						Type: corev1.PodReady, Status: corev1.ConditionTrue,
					}},
					ContainerStatuses: []corev1.ContainerStatus{{RestartCount: 2}},
				},
			}},
			v1beta1.DecoderComponent: {{
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "decoder"}}},
				Status: corev1.PodStatus{
					Phase: corev1.PodPending,
					Conditions: []corev1.PodCondition{{
						Type: corev1.PodReady, Status: corev1.ConditionFalse,
					}},
					ContainerStatuses: []corev1.ContainerStatus{{RestartCount: 1}},
				},
			}},
		},
		Warnings: []string{
			"warning-one", "warning-two", "warning-three", "warning-four", "warning-five", "warning-six",
		},
	}
	for i := 1; i <= 6; i++ {
		r.Events = append(r.Events, corev1.Event{
			InvolvedObject: corev1.ObjectReference{Kind: "Pod", Name: fmt.Sprintf("pod-%02d", i)},
			Reason:         "FailedScheduling",
			Message:        fmt.Sprintf("message-%02d", i),
		})
	}
	return r
}

type statusTerminalBuffer struct {
	bytes.Buffer
	width int
}

func (b *statusTerminalBuffer) TerminalWidth() (int, bool) { return b.width, true }

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
