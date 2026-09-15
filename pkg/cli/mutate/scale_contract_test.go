package mutate

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestScalePlanResponseAndOpaqueContract(t *testing.T) {
	p, state, r, s, source := scaleFixture(t)
	evidence, err := InspectScaleEvidence(p, r, s, source, testClock)
	require.NoError(t, err)
	plan, err := PrepareScale(p, state, evidence, v1beta1.EngineComponent, 3, true, true, testClock)
	require.NoError(t, err)
	_, err = plan.MarshalYAML()
	require.Error(t, err)
	_, err = evidence.MarshalYAML()
	require.Error(t, err)
	require.Equal(t, r.Name, plan.Target().Name)
	require.Zero(t, (ScaleEvidence{}).parentStamp())
	response := &autoscalingv1.Scale{TypeMeta: metav1.TypeMeta{APIVersion: "autoscaling/v1", Kind: "Scale"}, ObjectMeta: metav1.ObjectMeta{Name: r.Name, Namespace: r.Namespace, UID: r.UID, ResourceVersion: "82"}, Spec: autoscalingv1.ScaleSpec{Replicas: 3}}
	require.NoError(t, plan.ValidateResponse(response))
	for _, edit := range []func(*autoscalingv1.Scale){
		func(s *autoscalingv1.Scale) { s.APIVersion = "v1" }, func(s *autoscalingv1.Scale) { s.Kind = "Status" },
		func(s *autoscalingv1.Scale) { s.Name = "other" }, func(s *autoscalingv1.Scale) { s.Namespace = "other" },
		func(s *autoscalingv1.Scale) { s.UID = "other" }, func(s *autoscalingv1.Scale) { s.ResourceVersion = "" },
		func(s *autoscalingv1.Scale) { s.Spec.Replicas = 4 },
	} {
		copy := response.DeepCopy()
		edit(copy)
		require.ErrorIs(t, plan.ValidateResponse(copy), ErrScaleEvidence)
	}
	require.Error(t, plan.ValidateResponse(nil))
	require.Error(t, (ScalePlan{}).ValidateResponse(response))
	_, err = PrepareScale(nil, state, evidence, v1beta1.EngineComponent, 3, true, true, testClock)
	require.Error(t, err)
	_, err = PrepareScale(p, state, ScaleEvidence{}, v1beta1.EngineComponent, 3, true, true, testClock)
	require.Error(t, err)
	_, err = PrepareScale(p, nil, evidence, v1beta1.EngineComponent, 3, true, true, testClock)
	require.Error(t, err)
	_, err = PrepareScale(p, state, evidence, v1beta1.DecoderComponent, 3, true, true, testClock)
	require.Error(t, err)
	for _, key := range []string{constants.RolloutPromoteAnnotation, constants.RolloutRollbackAnnotation} {
		copy := p.DeepCopy()
		copy.Annotations = map[string]string{key: "bbbbbbbb"}
		// Metadata mailbox is intentionally independent of source Spec/Status.
		_, err = PrepareScale(copy, state, evidence, v1beta1.EngineComponent, 3, true, true, testClock)
		require.ErrorIs(t, err, ErrPending)
	}
	require.False(t, containsScaleComponent([]string{"engine"}, "decoder"))
	require.ErrorIs(t, requireScaleStable(nil, evidence, v1beta1.EngineComponent, testClock), ErrStale)
	for _, phase := range []v1beta1.RolloutPhase{v1beta1.RolloutPhasePending, v1beta1.RolloutPhasePaused, v1beta1.RolloutPhaseCanarying} {
		copy := p.DeepCopy()
		status := copy.Status.Components[v1beta1.EngineComponent]
		status.RolloutPhase = phase
		copy.Status.Components[v1beta1.EngineComponent] = status
		require.Error(t, requireScaleStable(copy, evidence, v1beta1.EngineComponent, testClock))
	}
}

func TestScalePreviewExactLongProofsAndAllWriteFailureStages(t *testing.T) {
	evidence, state, sibling := completedScaleEvidenceFixture(t)
	collected, err := CollectScalePinnedTargets(t.Context(), omefake.NewSimpleClientset(sibling).OmeV1beta1(), evidence, testClock)
	require.NoError(t, err)
	plan, err := PrepareScale(evidence.parent, state, collected, v1beta1.EngineComponent, 3, true, true, testClock)
	require.NoError(t, err)
	plan.target.Name, plan.target.UID = strings.Repeat("a", 253), strings.Repeat("b", 256)
	var out bytes.Buffer
	require.NoError(t, plan.WritePreview(&out, "synthetic", "ome", reportv1alpha1.DryRunClient))
	require.Contains(t, out.String(), "pinned sibling proof")
	require.Contains(t, strings.Join(strings.Fields(out.String()), " "), "best-effort revalidation")
	for _, line := range strings.Split(out.String(), "\n") {
		require.LessOrEqual(t, len(line), 80, line)
	}
	failed := 0
	for limit := 0; limit < 1000; limit++ {
		err := plan.WritePreview(&failAfterWriter{remaining: limit}, "synthetic", "ome", reportv1alpha1.DryRunClient)
		if err == nil {
			break
		}
		failed++
		require.NotContains(t, err.Error(), "SECRET_WRITER_DETAIL")
	}
	require.Greater(t, failed, 5, "exercise every distinct writer call before success")
	require.Error(t, (ScalePlan{}).WritePreview(&out, "synthetic", "ome", reportv1alpha1.DryRunClient))
	require.Error(t, plan.WritePreview(&out, "private unsafe context", "ome", reportv1alpha1.DryRunClient))
	for _, class := range []string{"HPA", "KEDA", "External", "None", "Unknown"} {
		require.NotEmpty(t, scaleOwnerWarning(class))
	}
}

func TestScalePartitionUsesProjectedLifecycleRules(t *testing.T) {
	for _, partition := range []int32{-1, 0, 2, 3, 4} {
		r := &v1beta1.InferenceReplica{Spec: v1beta1.InferenceReplicaSpec{Lifecycle: &v1beta1.LifecycleSpec{UpdateStrategy: &v1beta1.UpdateStrategy{Type: v1beta1.UpdateStrategySurgeThenDrain, RollingUpdate: &v1beta1.RollingUpdate{Partition: ptr.To(partition)}}}}}
		require.Equal(t, partition >= 0 && partition <= 3, scalePartitionAllows(r, 3))
	}
	r := &v1beta1.InferenceReplica{Spec: v1beta1.InferenceReplicaSpec{Lifecycle: &v1beta1.LifecycleSpec{UpdateStrategy: &v1beta1.UpdateStrategy{Type: "Unknown"}}}}
	require.False(t, scalePartitionAllows(r, 3))
}
