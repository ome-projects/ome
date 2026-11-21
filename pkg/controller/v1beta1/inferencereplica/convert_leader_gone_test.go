package inferencereplica

import (
	"context"
	"testing"
	"time"

	"github.com/onsi/gomega"
	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestBuildReconcileInput_LeaderGoneGraceFromTheReplicasLifecycle: the grace
// a replica sets in its own spec.lifecycle.leaderGoneGracePeriod reaches the
// workload engine. A replica that sets none hands the engine none: the field
// has no fallback in the operator's configuration.
func TestBuildReconcileInput_LeaderGoneGraceFromTheReplicasLifecycle(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	c, _ := newWireNormalizingStatusClient(t, ir)
	r := &Reconciler{
		Client:               c,
		APIReader:            c,
		Clock:                clocktesting.NewFakeClock(time.Date(2026, time.August, 14, 12, 0, 0, 0, time.UTC)),
		Expectations:         workloadtypes.NewExpectations(),
		InstanceStatusTarget: irstatus.EncodingDenseV1,
	}

	input := r.buildReconcileInput(context.Background(), ir, ir.Spec.Runners, nil, nil, nil, lifecycleSettings{}, 0, coordination.GroupDefaults{})
	_, set := input.DesiredSpec.Lifecycle.LeaderGoneGrace()
	g.Expect(set).To(gomega.BeFalse(), "a replica that sets no grace hands the engine none")

	setLeaderGoneGrace(ir, 30*time.Second)
	input = r.buildReconcileInput(context.Background(), ir, ir.Spec.Runners, nil, nil, nil, lifecycleSettings{}, 0, coordination.GroupDefaults{})
	grace, set := input.DesiredSpec.Lifecycle.LeaderGoneGrace()
	g.Expect(set).To(gomega.BeTrue())
	g.Expect(grace).To(gomega.Equal(30 * time.Second))
}
