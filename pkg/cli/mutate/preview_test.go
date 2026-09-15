package mutate

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/ome/pkg/cli/printers"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestPreviewShowsExactSnapshotAndDiscardWithoutWrappingOrPrivateData(t *testing.T) {
	v, state := nativeTarget(t)
	v.Annotations = map[string]string{constants.PausedRolloutAnnotation: "freeze", constants.RolloutPromoteAnnotation: "cccccccc", "private": "SECRET_NOT_FOR_PREVIEW"}
	plan, err := PrepareRollout(v, state, ReplicaEvidence{}, "resume", true, true, testClock)
	require.NoError(t, err)
	var out bytes.Buffer
	require.NoError(t, plan.WritePreview(&out, "moirai", "ome", reportv1alpha1.DryRunClient))
	text := out.String()
	for _, value := range []string{"moirai", "prod", "uid-chat", "42", "freeze", "ome.io/rollout-promote", "cccccccc", "RestartPolicy", "continue aging", "scale-down", "deletion"} {
		require.Contains(t, text, value)
	}
	require.NotContains(t, text, "SECRET_NOT_FOR_PREVIEW")
	for _, line := range strings.Split(text, "\n") {
		require.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
	}
	require.Error(t, plan.WritePreview(errorWriter{}, "moirai", "ome", reportv1alpha1.DryRunClient))
	require.Error(t, plan.WritePreview(&out, "Bearer SECRET_TOKEN", "ome", reportv1alpha1.DryRunClient))
}

func TestPausePreviewWrapsExactLongIdentityAndAbortsEveryWriteFailure(t *testing.T) {
	v, state := nativeTarget(t)
	plan, err := PrepareRollout(v, state, activeEvidence(v), "pause", false, true, testClock)
	require.NoError(t, err)
	plan.target.Name = strings.Repeat("a", 253)
	plan.target.UID = strings.Repeat("b", 256)
	var out bytes.Buffer
	require.NoError(t, plan.WritePreview(&out, "moirai", "ome", reportv1alpha1.DryRunClient))
	require.Contains(t, out.String(), strings.Repeat("a", 56))
	require.Contains(t, out.String(), "(continued)")
	require.NotContains(t, out.String(), "...")
	for _, line := range strings.Split(out.String(), "\n") {
		require.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
	}
	failed := 0
	for limit := 0; limit < 100; limit++ {
		err = plan.WritePreview(&failAfterWriter{remaining: limit}, "moirai", "ome", reportv1alpha1.DryRunClient)
		if err == nil {
			break
		}
		failed++
		require.NotContains(t, err.Error(), "SECRET_WRITER_DETAIL")
	}
	require.Positive(t, failed)
	require.Error(t, (RolloutPlan{}).WritePreview(&out, "moirai", "ome", reportv1alpha1.DryRunClient))
}
