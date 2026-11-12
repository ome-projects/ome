package coordination

import (
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// A blueGreen group of 12 prefill with a partition of 2 and 8 decode rolls
// to version 2. The prefill side rests at its partition, every decoder runs
// version 2, and the decoder's spec carries a partition of 8 set after its
// roll, so that no later push moves a decoder. The decoder has no roll in
// flight, so it neither holds the group from Staged nor counts as holding
// Instances: the group reads Staged, the way a rollingUpdate group reads
// the same picture, and the run's close reads the rest.
func TestComputeTransition_BlueGreenStagedLeavesAConvergedMemberWithAPartitionOut(t *testing.T) {
	for _, policy := range []v1beta1.CoordinationPolicy{v1beta1.CoordinationPolicyBlueGreen, v1beta1.CoordinationPolicyRollingUpdate} {
		g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: policy}
		obs := idleObs(g)
		engine := obs.Components[v1beta1.EngineComponent]
		engine.RolloutInFlight = true
		engine.AtDesiredShape = true
		engine.Partition = 2
		engine.DesiredReplicas = 12
		engine.TotalPods = 12
		engine.ReadyPods, engine.ServingPods = 12, 12
		engine.NewRevisionPods, engine.NewRevisionReadyPods = 10, 10
		engine.CurrentRevisionHash, engine.TargetRevisionHash = "rev1", "rev2"
		obs.Components[v1beta1.EngineComponent] = engine
		decoder := obs.Components[v1beta1.DecoderComponent]
		decoder.RolloutInFlight = false
		decoder.AtDesiredShape = false // every Instance runs the target; the shape reading needs a held one
		decoder.Partition = 8
		decoder.DesiredReplicas = 8
		decoder.TotalPods = 8
		decoder.ReadyPods, decoder.ServingPods = 8, 8
		decoder.NewRevisionPods, decoder.NewRevisionReadyPods = 8, 8
		decoder.CurrentRevisionHash, decoder.TargetRevisionHash = "rev2", "rev2"
		obs.Components[v1beta1.DecoderComponent] = decoder
		if tr := ComputeTransition(obs); tr.Phase != v1beta1.CoordinationPhaseStaged {
			t.Errorf("%s: a converged member with a partition leaves the group's Staged reading to the member resting at its partition; got %q (%s)", policy, tr.Phase, tr.Message)
		}
	}
}
