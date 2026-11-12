package canary

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/analysis"
	"sigs.k8s.io/ome/pkg/rollout"
)

// scriptedSampler answers the metric gate for one step with one outcome,
// dated when it was scripted, the way the production sampler serves the
// latest result of a query: a read for another step, or one whose last
// consumed sample is at least as new, has no fresh sample.
type scriptedSampler struct {
	step    int32
	outcome analysis.Outcome
	at      time.Time
}

func (s *scriptedSampler) Get(req SampleRequest, since time.Time) (analysis.Result, time.Time, bool) {
	if s == nil || req.Key.Step != s.step || !s.at.After(since) {
		return analysis.Result{}, time.Time{}, false
	}
	res := analysis.Result{Outcome: s.outcome}
	if req.Analysis != nil {
		for _, m := range req.Analysis.Metrics {
			res.Metrics = append(res.Metrics, analysis.MetricResult{Name: m.Name, Threshold: m.Threshold, Operator: m.Operator, Passed: s.outcome == analysis.Pass})
		}
	}
	return res, s.at, true
}

// newGatedPassSim is a two-Instance engine unit on a canary whose first step
// is gated on a metric with a failure limit of one and whose last step
// takes the whole fleet: one Ready pod per revision, the run's recorded
// target engnew over the stable engold, the first step's partition projected.
func newGatedPassSim(t *testing.T, name string) *passSim {
	t.Helper()
	ns := "default"
	n2 := 2
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n2}}
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("50%"), Traffic: 50, Analysis: &v1beta1.RolloutAnalysis{
				Interval:     metav1.Duration{Duration: 20 * time.Second},
				FailureLimit: 1,
				Metrics:      []v1beta1.AnalysisMetric{{Name: "err", Query: "q", Operator: v1beta1.ComparisonLTE, Threshold: "0.05"}},
			}},
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		}},
	}}}
	pinActiveRun(isvc)
	isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
		{Component: v1beta1.EngineComponent, Revision: simEngineCanary, StableRevision: simEngineStable},
	}
	one := int32(1)
	replica := ir(ns, name, v1beta1.EngineComponent, simEngineCanary)
	replica.Generation = 1
	replica.Status.ObservedGeneration = 1
	replica.Status.CurrentRevision = name + "-engine-" + simEngineStable
	replica.Spec.Pacing = &v1beta1.InferenceReplicaPacing{Partition: &one}
	objs := []client.Object{isvc, replica,
		canaryPod(ns, name, "engine", simEngineStable, name+"-engine-0"),
		canaryPod(ns, name, "engine", simEngineCanary, name+"-engine-1"),
		canaryControllerRevision(ns, name, "engine", simEngineStable, 1),
		canaryControllerRevision(ns, name, "engine", simEngineCanary, 2),
	}
	return newPassSimWith(t, ns, name, []v1beta1.ComponentType{v1beta1.EngineComponent}, objs)
}

// sample scripts the gate's next answer for a step, dated at the sim's clock
// moved forward by d.
func (s *passSim) sample(step int32, outcome analysis.Outcome, d time.Duration) {
	s.now = s.now.Add(d)
	s.sampler = &scriptedSampler{step: step, outcome: outcome, at: s.now}
}

// engineRecord is the persisted canary record of the engine unit.
func (s *passSim) engineRecord(at string) *v1beta1.CanaryStatus {
	s.t.Helper()
	cs := rollout.CanaryStatusFor(&s.persisted().Status, v1beta1.EngineComponent)
	if cs == nil {
		s.t.Fatalf("%s: the engine unit carries no canary record", at)
	}
	return cs
}

// expectEngineRevert checks the persisted unit rolling back with its
// rejection recorded and the engine replica pointed at its stable revision.
func (s *passSim) expectEngineRevert(at string) {
	s.t.Helper()
	cs := s.engineRecord(at)
	phase := s.persisted().Status.Components[v1beta1.EngineComponent].RolloutPhase
	if cs.RolledBackRevisionHash != simEngineCanary || phase != v1beta1.RolloutPhaseRollingBack {
		s.t.Fatalf("%s: the unit must hold RollingBack with its rejection recorded, got rolledBack=%q phase=%q", at, cs.RolledBackRevisionHash, phase)
	}
	if !s.pinnedAtStable(v1beta1.EngineComponent, simEngineStable) {
		s.t.Fatalf("%s: the engine replica must stay pointed at its stable revision, got %v", at, s.replica(v1beta1.EngineComponent).Spec.Pacing)
	}
}

// expectNoEngineRevert checks the persisted unit free of a rejection and
// the engine replica pointed at nothing.
func (s *passSim) expectNoEngineRevert(at string, step int32) {
	s.t.Helper()
	cs := s.engineRecord(at)
	phase := s.persisted().Status.Components[v1beta1.EngineComponent].RolloutPhase
	if cs.RolledBackRevisionHash != "" || phase == v1beta1.RolloutPhaseRollingBack || cs.CurrentStep != step {
		s.t.Fatalf("%s: the unit must stand at step %d with no rejection, got step=%d rolledBack=%q phase=%q", at, step, cs.CurrentStep, cs.RolledBackRevisionHash, phase)
	}
	if p := s.replica(v1beta1.EngineComponent).Spec.Pacing; p != nil && p.RollbackToRevision != nil {
		s.t.Fatalf("%s: the engine replica must be pointed at no revert target, got %q", at, *p.RollbackToRevision)
	}
}

// The metric gate rejects version 2 and the replica is pointed at version
// 1. The pass after reads a copy the informer has not caught up on, from
// before the rejection, and the newest sample passes: it decides from the
// older record, the flush keeps the live rejection, and the replica's
// revert target is left as it stands, so the replica keeps rolling back.
func TestAStalePassAfterAGateRejectionLeavesTheRevertTarget(t *testing.T) {
	s := newGatedPassSim(t, "gate")
	s.pass("serve", false)
	s.expectNoEngineRevert("after the split serves", 0)

	s.sample(0, analysis.Fail, time.Minute)
	s.pass("reject", false)
	s.expectEngineRevert("after the gate's rejection")
	s.catchUp(v1beta1.EngineComponent)

	s.sample(0, analysis.Pass, 10*time.Second)
	s.pass("stale", true)
	s.expectEngineRevert("after the pass that read the copy from before the rejection")

	s.pass("fresh", false)
	s.expectEngineRevert("after the pass that read the live rejection")
}

// The metric gate passes version 2 at its first step and the record moves
// to the last step. The pass after reads a copy the informer has not caught
// up on, from before the step moved, and the newest sample fails the step
// the record left: it decides a rejection from the older record, the flush
// keeps the live record, and the replica is pointed at nothing.
func TestAStalePassWritesNoRevertTargetForAStepTheRecordLeft(t *testing.T) {
	s := newGatedPassSim(t, "gate")
	s.pass("serve", false)
	s.expectNoEngineRevert("after the split serves", 0)

	s.sample(0, analysis.Pass, time.Minute)
	s.pass("advance", false)
	s.expectNoEngineRevert("after the gate passed the first step", 1)
	s.catchUp(v1beta1.EngineComponent)

	s.sample(0, analysis.Fail, 10*time.Second)
	s.pass("stale", true)
	s.expectNoEngineRevert("after the pass that read the copy from before the step moved", 1)

	s.pass("fresh", false)
	s.expectNoEngineRevert("after the pass that read the live record", 1)
}
