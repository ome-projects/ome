package canary

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
)

func TestResolveStepNewCount(t *testing.T) {
	cases := []struct {
		name    string
		step    v1beta1.RolloutGroupStep
		desired int32
		wantNew int32
	}{
		{"10pct", v1beta1.RolloutGroupStep{Capacity: intstr.FromString("10%")}, 10, 1},
		{"100pct", v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%")}, 4, 4},
		{"absolute-new", v1beta1.RolloutGroupStep{Capacity: intstr.FromInt(2)}, 10, 2},
		{"25pct-ceil", v1beta1.RolloutGroupStep{Capacity: intstr.FromString("25%")}, 4, 1},
		{"10pct-of-4-ceils-to-1", v1beta1.RolloutGroupStep{Capacity: intstr.FromString("10%")}, 4, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveStepNewCount(tc.step, tc.desired); got != tc.wantNew {
				t.Fatalf("resolveStepNewCount(%+v, %d) = %d, want %d", tc.step, tc.desired, got, tc.wantNew)
			}
		})
	}
}

// partitionForNewCount maps a desired new-revision count to the StatefulSet-style
// RollingUpdate.Partition the workload reconcile honors (instances with index <
// Partition are held on the old revision).
func TestPartitionForNewCount(t *testing.T) {
	if got := partitionForNewCount(10, 1); got != 9 {
		t.Fatalf("partition for 1 new of 10 = %d, want 9", got)
	}
	if got := partitionForNewCount(4, 4); got != 0 {
		t.Fatalf("partition for full roll = %d, want 0", got)
	}
	if got := partitionForNewCount(4, 7); got != 0 {
		t.Fatalf("partition clamps at 0 when newCount>desired, got %d", got)
	}
}

func TestCanaryWeights(t *testing.T) {
	w := canaryWeights("new", "old", "proto-b", "proto-a", 10)
	if len(w) != 2 {
		t.Fatalf("want 2 weights, got %d", len(w))
	}
	// canary first (LatestRevision), then stable.
	if w[0].RevisionHash != "new" || w[0].Percent != 10 || !w[0].LatestRevision {
		t.Fatalf("canary weight wrong: %+v", w[0])
	}
	if w[1].RevisionHash != "old" || w[1].Percent != 90 {
		t.Fatalf("stable weight wrong: %+v", w[1])
	}
	if w[0].PairingProtocol != "proto-b" || w[1].PairingProtocol != "proto-a" {
		t.Fatalf("pairing protocols not carried per revision: %+v", w)
	}

	w = canaryWeights("same", "same", "proto", "proto", 10)
	if len(w) != 1 || w[0].RevisionHash != "same" || w[0].Percent != 100 {
		t.Fatalf("unchanged primary must remain a single 100%% route, got %+v", w)
	}
}

func TestApplyTraffic(t *testing.T) {
	isvc := &v1beta1.InferenceService{}
	isvc.Name = "svc"
	isvc.Status.Canary = &v1beta1.CanaryStatus{}

	// 10% to canary → both per-revision Services present.
	applyTraffic(isvc, v1beta1.EngineComponent, "new", "old", "proto-b", "proto-a", 10)
	tr := isvc.Status.Components[v1beta1.EngineComponent].Traffic
	if len(tr) != 2 {
		t.Fatalf("want 2 targets at 10%%, got %d (%+v)", len(tr), tr)
	}
	wantNew := coordination.PerRevisionServiceName("svc", v1beta1.EngineComponent, "new")
	wantOld := coordination.PerRevisionServiceName("svc", v1beta1.EngineComponent, "old")
	got := map[string]int32{}
	for _, x := range tr {
		got[x.RevisionName] = x.Percent
	}
	if got[wantNew] != 10 || got[wantOld] != 90 {
		t.Fatalf("weights wrong: %+v", got)
	}
	if isvc.Status.Canary.ObservedTrafficWeight != 10 {
		t.Fatalf("observed weight not recorded: %d", isvc.Status.Canary.ObservedTrafficWeight)
	}

	// 100% → only the canary Service (stable 0 is filtered out).
	applyTraffic(isvc, v1beta1.EngineComponent, "new", "old", "proto-b", "proto-a", 100)
	tr = isvc.Status.Components[v1beta1.EngineComponent].Traffic
	if len(tr) != 1 || tr[0].RevisionName != wantNew || tr[0].Percent != 100 {
		t.Fatalf("at 100%% want only canary 100, got %+v", tr)
	}

	// 0% → only the stable Service.
	applyTraffic(isvc, v1beta1.EngineComponent, "new", "old", "proto-b", "proto-a", 0)
	tr = isvc.Status.Components[v1beta1.EngineComponent].Traffic
	if len(tr) != 1 || tr[0].RevisionName != wantOld || tr[0].Percent != 100 {
		t.Fatalf("at 0%% want only stable 100, got %+v", tr)
	}
}

func TestEffectivePartition(t *testing.T) {
	mk := func(steps ...v1beta1.RolloutGroupStep) *v1beta1.InferenceService {
		return &v1beta1.InferenceService{Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Canary:     &v1beta1.GroupCanary{Steps: steps},
			}}}}}
	}

	// No canary → not active.
	if _, ok := EffectivePartition(&v1beta1.InferenceService{}, v1beta1.EngineComponent, 4); ok {
		t.Fatal("no canary must be inactive")
	}

	// Step 0 (no status yet) of a 50% step at N=4 → 2 new, partition = 4-2 = 2.
	isvc := mk(
		v1beta1.RolloutGroupStep{Capacity: intstr.FromString("50%"), Traffic: 50},
		v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100},
	)
	p, ok := EffectivePartition(isvc, v1beta1.EngineComponent, 4)
	if !ok || p == nil || *p != 2 {
		t.Fatalf("step0: want partition 2, got %v ok=%v", p, ok)
	}

	// Advance to the final step → 100% new → partition 0.
	isvc.Status.Canary = &v1beta1.CanaryStatus{CurrentStep: 1}
	p, ok = EffectivePartition(isvc, v1beta1.EngineComponent, 4)
	if !ok || p == nil || *p != 0 {
		t.Fatalf("final: want partition 0, got %v ok=%v", p, ok)
	}

	// Done sentinel (CurrentStep == len(steps)) → partition 0 (all on the canary
	// revision, old drains). A finished canary must NOT re-default to step 0's
	// partition, which would hold instances on the old revision after completion.
	isvc.Status.Canary = &v1beta1.CanaryStatus{CurrentStep: 2}
	p, ok = EffectivePartition(isvc, v1beta1.EngineComponent, 4)
	if !ok || p == nil || *p != 0 {
		t.Fatalf("done sentinel: want partition 0, got %v ok=%v", p, ok)
	}

	// Rolled back (RolledBackRevisionHash set) → partition 0 regardless of step,
	// so the IR's RollbackToRevision target drives the revert without the partition
	// fighting it. Step 0 would otherwise be partition 2.
	isvc.Status.Canary = &v1beta1.CanaryStatus{CurrentStep: 0, RolledBackRevisionHash: "rejected"}
	p, ok = EffectivePartition(isvc, v1beta1.EngineComponent, 4)
	if !ok || p == nil || *p != 0 {
		t.Fatalf("rolled back: want partition 0, got %v ok=%v", p, ok)
	}

	// The bare ome.io/rollout-rollback annotation does NOT change the partition —
	// EffectivePartition keys off status (RolledBackRevisionHash), set by the
	// executor, not the annotation directly.
	isvc.Status.Canary = &v1beta1.CanaryStatus{CurrentStep: 0}
	isvc.Annotations = map[string]string{constants.RolloutRollbackAnnotation: "true"}
	p, ok = EffectivePartition(isvc, v1beta1.EngineComponent, 4)
	if !ok || p == nil || *p != 2 {
		t.Fatalf("annotation alone: want step-0 partition 2, got %v ok=%v", p, ok)
	}
}

func TestStampStepPartition(t *testing.T) {
	n := 4
	isvc := &v1beta1.InferenceService{Spec: v1beta1.InferenceServiceSpec{
		Rollout: &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
			Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
				{Capacity: intstr.FromString("50%"), Traffic: 50},
				{Capacity: intstr.FromString("100%"), Traffic: 100},
			}},
		}}}}}
	ext := &v1beta1.ComponentExtensionSpec{MinReplicas: &n, MaxReplicas: 4}
	StampStepPartition(isvc, v1beta1.EngineComponent, ext)
	ru := ext.Lifecycle.UpdateStrategy.RollingUpdate
	if ru == nil || ru.Partition == nil || *ru.Partition != 2 {
		t.Fatalf("step0 50%% of 4 → partition 2, got %+v", ru)
	}

	// No canary → no-op (lifecycle chain not created).
	ext2 := &v1beta1.ComponentExtensionSpec{MinReplicas: &n}
	StampStepPartition(&v1beta1.InferenceService{}, v1beta1.EngineComponent, ext2)
	if ext2.Lifecycle != nil {
		t.Fatal("no canary must not stamp lifecycle")
	}

	// Negative CurrentStep (status is an unvalidated subresource, an external
	// write can go below 0) must clamp to step 0, not panic indexing plan.Steps.
	isvc.Status.Canary = &v1beta1.CanaryStatus{CurrentStep: -1}
	ext3 := &v1beta1.ComponentExtensionSpec{MinReplicas: &n, MaxReplicas: 4}
	StampStepPartition(isvc, v1beta1.EngineComponent, ext3)
	ru3 := ext3.Lifecycle.UpdateStrategy.RollingUpdate
	if ru3 == nil || ru3.Partition == nil || *ru3.Partition != 2 {
		t.Fatalf("negative step must clamp to step-0 partition 2, got %+v", ru3)
	}
}

func TestRecordDispatch_StepCounterAndEvent(t *testing.T) {
	isvc := &v1beta1.InferenceService{}
	isvc.Namespace = "ns"
	isvc.Name = "metrics-svc"
	isvc.Status.Canary = &v1beta1.CanaryStatus{CurrentStep: 1, ObservedTrafficWeight: 50}
	rec := record.NewFakeRecorder(8)

	before := testutil.ToFloat64(canaryStepTotal.WithLabelValues(isvc.Namespace, isvc.Name, "engine"))
	recordDispatch(rec, isvc, v1beta1.EngineComponent, &Result{Active: true, Stepped: true})
	if got := testutil.ToFloat64(canaryStepTotal.WithLabelValues(isvc.Namespace, isvc.Name, "engine")) - before; got != 1 {
		t.Fatalf("step counter should increment by 1 on Stepped, got %v", got)
	}
	if got := testutil.ToFloat64(canaryCurrentStep.WithLabelValues(isvc.Namespace, isvc.Name, "engine")); got != 1 {
		t.Fatalf("current-step gauge should be 1, got %v", got)
	}
	select {
	case ev := <-rec.Events:
		if !strings.Contains(ev, EventReasonCanaryStepAdvanced) {
			t.Fatalf("expected a %s event, got %q", EventReasonCanaryStepAdvanced, ev)
		}
	default:
		t.Fatal("expected a step-advanced event")
	}

	// A non-stepped pass must NOT increment the step counter.
	after := testutil.ToFloat64(canaryStepTotal.WithLabelValues(isvc.Namespace, isvc.Name, "engine"))
	recordDispatch(rec, isvc, v1beta1.EngineComponent, &Result{Active: true})
	if got := testutil.ToFloat64(canaryStepTotal.WithLabelValues(isvc.Namespace, isvc.Name, "engine")); got != after {
		t.Fatalf("non-stepped pass must not increment the step counter, got %v want %v", got, after)
	}
}

func TestRecordDispatch_Completion(t *testing.T) {
	isvc := &v1beta1.InferenceService{}
	isvc.Namespace = "ns"
	isvc.Name = "complete-svc"
	// Done sentinel: completion keeps Status.Canary (CurrentStep past last step).
	isvc.Status.Canary = &v1beta1.CanaryStatus{CurrentStep: 2, ObservedTrafficWeight: 100}
	rec := record.NewFakeRecorder(8)

	before := testutil.ToFloat64(canaryCompleteTotal.WithLabelValues(isvc.Namespace, isvc.Name))
	recordDispatch(rec, isvc, v1beta1.EngineComponent, &Result{Active: true, Complete: true})
	if got := testutil.ToFloat64(canaryCompleteTotal.WithLabelValues(isvc.Namespace, isvc.Name)) - before; got != 1 {
		t.Fatalf("complete counter should increment by 1, got %v", got)
	}
	if got := testutil.ToFloat64(canaryCurrentStep.WithLabelValues(isvc.Namespace, isvc.Name, "engine")); got != 0 {
		t.Fatalf("current-step gauge should reset to 0 on completion, got %v", got)
	}
	if got := testutil.ToFloat64(canaryTrafficWeight.WithLabelValues(isvc.Namespace, isvc.Name, "engine")); got != 0 {
		t.Fatalf("traffic-weight gauge should reset to 0 on completion, got %v", got)
	}
	select {
	case ev := <-rec.Events:
		if !strings.Contains(ev, EventReasonCanaryCompleted) {
			t.Fatalf("expected a %s event, got %q", EventReasonCanaryCompleted, ev)
		}
	default:
		t.Fatal("expected a completed event")
	}
}

// A retargeted-then-rolled-back canary can leave stragglers on an
// INTERMEDIATE revision (A -> partial B -> retarget C -> rollback): the
// revert-complete phase must wait for every non-stable revision to drain,
// because the run closes on RolledBack and the plan gate then holds any
// remaining reverts with no run left to open.
func TestReconcileRollback_HoldsForIntermediateStragglers(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llm-a", Namespace: "ns"},
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{},
		},
	}
	cs := &v1beta1.CanaryStatus{
		StableRevisionHash:     "aaaaaaaa",
		CanaryRevisionHash:     "cccccccc",
		RolledBackRevisionHash: "cccccccc",
	}
	in := ReconcileInputs{
		ISVC:      isvc,
		Component: v1beta1.EngineComponent,
		// Rejected revision C fully drained, but a straggler from the
		// partially-rolled intermediate B still serves.
		PerRevisionPods: map[string]int32{"aaaaaaaa": 2, "bbbbbbbb": 1, "cccccccc": 0},
	}
	res := reconcileRollback(in, cs)
	if !res.RolledBack || isvc.Status.Components[v1beta1.EngineComponent].RolloutPhase != v1beta1.RolloutPhaseRollingBack {
		t.Fatalf("intermediate straggler must hold RollingBack, got phase %q",
			isvc.Status.Components[v1beta1.EngineComponent].RolloutPhase)
	}

	// Straggler reverted: only the stable revision serves — now RolledBack.
	in.PerRevisionPods = map[string]int32{"aaaaaaaa": 3, "bbbbbbbb": 0, "cccccccc": 0}
	res = reconcileRollback(in, cs)
	if !res.RolledBack || isvc.Status.Components[v1beta1.EngineComponent].RolloutPhase != v1beta1.RolloutPhaseRolledBack {
		t.Fatalf("stable-only pods must complete the rollback, got phase %q",
			isvc.Status.Components[v1beta1.EngineComponent].RolloutPhase)
	}
}
