package inferencereplica

import (
	"context"
	"testing"

	"github.com/onsi/gomega"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestWireCoordinationGates_PlanGateHoldsWhileTheSpecTrailsTheService shows
// what a replica's pass answers when it runs between an operator's change
// of the service and the service controller's write of the changed spec.
// The service is at generation 2 and the replica still holds the spec
// written for generation 1, so its roll target may be one the operator has
// already replaced: the plan gate holds every start. Once the projector's
// write carries generation 2 the same gate admits.
func TestWireCoordinationGates_PlanGateHoldsWhileTheSpecTrailsTheService(t *testing.T) {
	g := gomega.NewWithT(t)
	engineIR := mkIR(v1beta1.EngineComponent, 1)
	setParentGenStamp(engineIR, 1)
	r, _ := newReconciler(t, engineIR)
	parent := mkSequentialParent()

	input := r.buildReconcileInput(context.Background(), engineIR, engineIR.Spec.Runners, parent, nil, nil, lifecycleSettings{}, 0, coordination.GroupDefaults{})
	r.wireCoordinationGates(context.Background(), &input, engineIR, parent, coordination.GroupDefaults{}, specTargetRevision(engineIR))
	g.Expect(input.PlanGate).NotTo(gomega.BeNil(), "the plan seam must be wired beside the update gate")

	allowed, gate, reason := input.PlanGate()
	g.Expect(allowed).To(gomega.BeFalse(), "the service changed and the changed spec has not arrived: no start is admitted")
	g.Expect(gate).To(gomega.Equal(workloadtypes.RolloutHoldGate(v1beta1.RolloutHoldGatePlan)))
	g.Expect(reason).To(gomega.And(gomega.ContainSubstring("generation 2"), gomega.ContainSubstring("generation 1")))

	setParentGenStamp(engineIR, 2)
	allowed, _, _ = input.PlanGate()
	g.Expect(allowed).To(gomega.BeTrue(), "a spec written for the service's generation is the plan again")
}
