package coordination

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// The reference prefill/decode shapes under the chart's settings: the
// ratio rule on at the reference tolerance (20), the decoder
// named as the side that moves first inside every batch, both sides at a
// 10 percent budget of their own count, under RecreatePod and under
// SurgeThenDrain. 12 prefill beside 8 decode plan 2, 2, 2, 2, 1, 2, 1, 0
// prefill per batch beside one decode each; 16 prefill beside 8 decode
// keep their own steps, 2 and 1, since the steps are the ratio already.
// The tests below read the gate's answer and the phase the coordination
// pass computes from the same replicas, picture by picture.

// referenceRules are the chart's prefill/decode settings.
func referenceRules() GroupDefaults { return keepRatioDecoderFirst(20) }

// sixteenEight is the 16 prefill beside 8 decode shape, both sides at a 10
// percent budget of their own, under the given strategy.
func sixteenEight(strategy v1beta1.UpdateStrategyType) (engine, decoder batchMemberFixture) {
	engine, decoder = twelveEight(strategy)
	engine.desired = 16
	return engine, decoder
}

// referenceShape names one reference shape by its builder.
type referenceShape struct {
	name  string
	build func(v1beta1.UpdateStrategyType) (engine, decoder batchMemberFixture)
}

var referenceShapes = []referenceShape{{"12:8", twelveEight}, {"16:8", sixteenEight}}

// referenceStrategy is one update strategy with the reference budget (10
// percent) and the mechanism a start runs on.
type referenceStrategy struct {
	name      string
	strategy  v1beta1.UpdateStrategyType
	group     batchGroupFixture
	mechanism workloadtypes.UpdateStrategyType
}

var referenceStrategies = []referenceStrategy{
	{"RecreatePod", v1beta1.UpdateStrategyRecreatePod, batchGroupFixture{maxUnavailable: "10%"}, workloadtypes.UpdateStrategyRecreatePod},
	{"SurgeThenDrain", v1beta1.UpdateStrategySurgeThenDrain, batchGroupFixture{maxUnavailable: "0", maxSurge: "10%"}, workloadtypes.UpdateStrategySurgeThenDrain},
}

// referencePicture is one moment of a roll on a reference shape: both
// members as their replicas publish them, plus, per side, how many of its
// promoted Instances have lost their only pod since they served.
type referencePicture struct {
	engine, decoder batchMemberFixture
	podGone         map[v1beta1.ComponentType]int32
}

func (p referencePicture) members() map[v1beta1.ComponentType]batchMemberFixture {
	return map[v1beta1.ComponentType]batchMemberFixture{v1beta1.EngineComponent: p.engine, v1beta1.DecoderComponent: p.decoder}
}

// replicas builds both replicas as the controller publishes them, the lost
// pods taken away.
func (p referencePicture) replicas() map[v1beta1.ComponentType]*v1beta1.InferenceReplica {
	out := map[v1beta1.ComponentType]*v1beta1.InferenceReplica{}
	for comp, m := range p.members() {
		ir := batchReplica(comp, m)
		for i := int32(0); i < p.podGone[comp]; i++ {
			promotedPodGone(ir)
		}
		out[comp] = ir
	}
	return out
}

// client is a reader over the picture's replicas.
func (p referencePicture) client(t *testing.T) client.Client {
	t.Helper()
	var objs []client.Object
	for _, ir := range p.replicas() {
		objs = append(objs, ir)
	}
	return fake.NewClientBuilder().WithScheme(pairingScheme(t)).WithObjects(objs...).Build()
}

// phase is what the coordination pass computes for the picture under the
// named policy: every pod of a member live, the old ones beside the
// promoted ones, less the pods that are gone.
func (p referencePicture) phase(policy v1beta1.CoordinationPolicy) v1beta1.CoordinationPhase {
	components := []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}
	obs := GroupObservation{
		Group:      ResolvedGroup{Name: "0", Policy: policy, Components: components},
		Components: map[v1beta1.ComponentType]ComponentObservation{},
	}
	replicas := p.replicas()
	for comp, m := range p.members() {
		old := m.desired - m.promoted - m.creating + m.retiring
		pods := map[string]int32{batchRevision(comp, "old"): old, batchRevision(comp, "target"): m.promoted - p.podGone[comp]}
		obs.Components[comp] = buildComponentObservation(&replicas[comp].Status, comp, pods, 0)
	}
	return ComputeTransition(obs).Phase
}

// promotedPodGone takes the only pod of one promoted Instance away, as the
// replica publishes the loss: the row keeps the target revision and reads
// Pending, and the counters of Ready, serving and updated-and-Ready
// Instances each drop by one while the updated count stands.
func promotedPodGone(ir *v1beta1.InferenceReplica) {
	for i := range ir.Status.InstanceStatuses {
		row := &ir.Status.InstanceStatuses[i]
		if row.Phase == v1beta1.OMENativeInstanceReady && row.RunningRevision == ir.Status.UpdateRevision && row.Operation == nil && row.PodCount > 0 {
			row.Phase, row.PodCount, row.ServingPodCount = v1beta1.OMENativeInstancePending, 0, 0
			ir.Status.ReadyReplicas--
			ir.Status.ServingReplicas--
			ir.Status.UpdatedReadyReplicas--
			return
		}
	}
	panic("no promoted Instance with a pod to lose")
}

// started marks n Instances of a member as started toward the target on
// the strategy's mechanism: a recreate in flight, or a surge whose source
// still serves.
func started(m batchMemberFixture, n int32) batchMemberFixture {
	if m.strategy == v1beta1.UpdateStrategySurgeThenDrain {
		m.surging += n
	} else {
		m.inFlight += n
	}
	return m
}

// ask asks the update gate about one more start of comp on the picture
// under the reference rules and returns its answer.
func (p referencePicture) ask(t *testing.T, group batchGroupFixture, comp v1beta1.ComponentType, mechanism workloadtypes.UpdateStrategyType, defaults GroupDefaults) (bool, v1beta1.RolloutHoldGate, string) {
	t.Helper()
	isvc := batchService(group, p.members())
	return batchGateWith(t, isvc, p.client(t), comp, mechanism, 0, defaults)
}

// expectHold holds the gate's answer to the case: no hold, or a hold under
// the named gate with the exact reason.
func expectHold(t *testing.T, allowed bool, gate v1beta1.RolloutHoldGate, reason string, wantGate v1beta1.RolloutHoldGate, wantReason string) {
	t.Helper()
	if wantReason == "" {
		if !allowed {
			t.Fatalf("held under %q with %q, want the start admitted", gate, reason)
		}
		return
	}
	if allowed {
		t.Fatalf("admitted, want a hold under %q with %q", wantGate, wantReason)
	}
	if gate != wantGate || reason != wantReason {
		t.Fatalf("held under %q with %q, want %q with %q", gate, reason, wantGate, wantReason)
	}
}

// TestEvaluateUpdateGate_FirstSideHoldsWhateverPhaseTheGroupReads asks the
// gate for the other side's start under the chart's settings at each phase
// a prefill/decode group reads mid-roll, on 12:8 and 16:8 under both
// strategies: at the push, when no member has a new pod yet (Surging);
// between two batches with the old pods still serving (ScalingDown under
// rollingUpdate, Shifting under blueGreen on the surge path); while a
// promoted prefill Instance of the batch has lost its only pod (Waiting);
// and while a prefill attempt of the batch ended Failed (Failed). In every
// phase the engine's batch waits for the decoder's new Instances of that
// batch under the Ratio gate with the first-side words, and the decoder's
// own start is admitted; an attempt of the named side's batch that ended
// Failed and waits on the schedule settles that batch, so the engine's
// batch opens on it while the decoder's own next start waits for the
// engine's batch as any start does. An attempt that ended with nothing
// counted on the record, as the placement grace ends one, is started
// again at once: the side's next start is that attempt and asks as its
// own batch, and the other side keeps waiting on it with the Instance
// named.
func TestEvaluateUpdateGate_FirstSideHoldsWhateverPhaseTheGroupReads(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	ru, bg := v1beta1.CoordinationPolicyRollingUpdate, v1beta1.CoordinationPolicyBlueGreen
	type picture struct {
		name      string
		build     func(engine, decoder batchMemberFixture) referencePicture
		policy    v1beta1.CoordinationPolicy
		wantPhase v1beta1.CoordinationPhase
		// wantPhaseSurge is the phase on the surge path when it differs: a
		// surge that ended while its old pod serves publishes Ready with the
		// target kept, and the group reads no Failed row for it.
		wantPhaseSurge v1beta1.CoordinationPhase
		wantReason     string // the engine's hold, or admitted when empty
		// decoderWait is the hold on the decoder's own start, admitted when
		// empty; the first-side rule never holds the named side, so a hold
		// here is the previous-batch rule's or the budget's.
		decoderWait string
	}
	pictures := []picture{
		{
			name:   "at the push no member has a new pod",
			build:  func(e, d batchMemberFixture) referencePicture { return referencePicture{engine: e, decoder: d} },
			policy: ru, wantPhase: v1beta1.CoordinationPhaseSurging,
			wantReason: "engine batch 1 waits for decoder batch 1 to serve first (0 of 1 serving)",
		},
		{
			name: "between two batches the old pods still serve",
			build: func(e, d batchMemberFixture) referencePicture {
				e.promoted, d.promoted = 2, 1
				return referencePicture{engine: e, decoder: d}
			},
			policy: ru, wantPhase: v1beta1.CoordinationPhaseScalingDown,
			wantReason: "engine batch 2 waits for decoder batch 2 to serve first (1 of 2 serving)",
		},
		{
			name: "between two batches under blueGreen",
			build: func(e, d batchMemberFixture) referencePicture {
				e.promoted, d.promoted = 2, 1
				return referencePicture{engine: e, decoder: d}
			},
			policy: bg, wantPhase: v1beta1.CoordinationPhaseShifting,
			wantReason: "engine batch 2 waits for decoder batch 2 to serve first (1 of 2 serving)",
		},
		{
			name: "a promoted prefill Instance of the batch has lost its pod",
			build: func(e, d batchMemberFixture) referencePicture {
				e.promoted, d.promoted = 2, 1
				return referencePicture{engine: e, decoder: d, podGone: map[v1beta1.ComponentType]int32{eng: 1}}
			},
			policy: ru, wantPhase: v1beta1.CoordinationPhaseWaiting,
			wantReason: "engine batch 2 waits for decoder batch 2 to serve first (1 of 2 serving)",
		},
		{
			name: "a prefill attempt of the batch ended Failed and waits on the schedule",
			build: func(e, d batchMemberFixture) referencePicture {
				e.promoted, e.failed, e.backoff, d.promoted = 1, 1, 1, 1
				return referencePicture{engine: e, decoder: d}
			},
			policy: ru, wantPhase: v1beta1.CoordinationPhaseFailed,
			wantReason: "engine batch 2 waits for decoder batch 2 to serve first (1 of 2 serving)",
		},
		{
			name: "the decoder's first attempt ended Failed and waits on the schedule: the ended attempt settles the batch, so the engine's first batch opens",
			build: func(e, d batchMemberFixture) referencePicture {
				d.failed, d.backoff = 1, 1
				return referencePicture{engine: e, decoder: d}
			},
			policy: ru, wantPhase: v1beta1.CoordinationPhaseFailed,
			decoderWait: "decoder batch 2 waits for engine batch 1 to serve (0 of 2 serving)",
		},
		{
			name: "a prefill attempt of the batch ended with nothing counted: the prefill's next start is that attempt again and the decoder waits on it",
			build: func(e, d batchMemberFixture) referencePicture {
				e.promoted, e.graceEnded, d.promoted = 1, 1, 1
				return referencePicture{engine: e, decoder: d}
			},
			policy: ru, wantPhase: v1beta1.CoordinationPhaseFailed, wantPhaseSurge: v1beta1.CoordinationPhaseScalingDown,
			decoderWait: "decoder batch 2 waits for engine batch 1 to serve (1 of 2 serving; engine Instance 1 Unschedulable, starts again)",
		},
		{
			name: "the decoder's first attempt ended with nothing counted: the decoder starts it again and the engine's first batch waits on it",
			build: func(e, d batchMemberFixture) referencePicture {
				d.graceEnded = 1
				return referencePicture{engine: e, decoder: d}
			},
			policy: ru, wantPhase: v1beta1.CoordinationPhaseFailed, wantPhaseSurge: v1beta1.CoordinationPhaseSurging,
			wantReason: "engine batch 1 waits for decoder batch 1 to serve first (0 of 1 serving; decoder Instance 0 Unschedulable, starts again)",
		},
	}
	for _, shape := range referenceShapes {
		for _, strategy := range referenceStrategies {
			for _, pic := range pictures {
				if pic.policy == bg && strategy.strategy != v1beta1.UpdateStrategySurgeThenDrain {
					continue // a blueGreen group names no unavailability budget, so only the surge path rolls under it
				}
				t.Run(shape.name+"/"+strategy.name+"/"+pic.name, func(t *testing.T) {
					group := strategy.group
					if pic.policy == bg {
						group = batchGroupFixture{blueGreen: true}
					}
					engine, decoder := shape.build(strategy.strategy)
					p := pic.build(engine, decoder)
					wantPhase := pic.wantPhase
					if pic.wantPhaseSurge != "" && strategy.strategy == v1beta1.UpdateStrategySurgeThenDrain {
						wantPhase = pic.wantPhaseSurge
					}
					if got := p.phase(pic.policy); got != wantPhase {
						t.Fatalf("the group reads %s, want %s", got, wantPhase)
					}
					allowed, gate, reason := p.ask(t, group, eng, strategy.mechanism, referenceRules())
					expectHold(t, allowed, gate, reason, v1beta1.RolloutHoldGateRatio, pic.wantReason)
					allowed, gate, reason = p.ask(t, group, dec, strategy.mechanism, referenceRules())
					if pic.decoderWait != "" {
						expectHold(t, allowed, gate, reason, v1beta1.RolloutHoldGateRatio, pic.decoderWait)
						return
					}
					if gate == v1beta1.RolloutHoldGateRatio {
						t.Fatalf("the named side is never held by the first-side rule; held under %q with %q", gate, reason)
					}
					if !allowed && gate != v1beta1.RolloutHoldGateBudget {
						t.Fatalf("the decoder's start is admitted, or held by its budget alone; held under %q with %q", gate, reason)
					}
				})
			}
		}
	}
}

// TestEvaluateUpdateGate_BatchStepReadsALostPodInsideABatch is a pod lost
// inside a batch of the reference roll under the chart's settings, on
// 12:8 and 16:8 under both strategies. The batch step counts a promoted
// Instance as served while its only pod is being rebuilt, so the other
// side's next batch opens on it: a lost pod of the decoder's batch 2 does
// not hold the engine's batch 2 once the decoder's rows are promoted. What
// holds the side that lost the pod is its own group budget, and only on the
// drain-first path: the decode side's unavailability budget of one is spent
// by the lost pod, so its next drain waits for the rebuild, while the
// prefill side's budget of two leaves one drain beside the lost pod, and
// the surge budget never counts a lost pod. A decoder Instance whose new
// pod was lost before it served is an attempt still open, and the engine
// waits on it as on any start.
func TestEvaluateUpdateGate_BatchStepReadsALostPodInsideABatch(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	ru := v1beta1.CoordinationPolicyRollingUpdate
	budgetHold := "unavailable budget 1 exhausted (current 1, in-flight 0, would become 2); counted as unavailable: Instance 0 being rebuilt"
	type picture struct {
		name       string
		build      func(engine, decoder batchMemberFixture) referencePicture
		comp       v1beta1.ComponentType
		wantPhase  v1beta1.CoordinationPhase
		wantGate   v1beta1.RolloutHoldGate
		wantReason map[workloadtypes.UpdateStrategyType]string // empty: admitted
	}
	pictures := []picture{
		{
			name: "the decoder's batch 2 served and its newest Instance lost its pod: the engine's batch 2 opens",
			build: func(e, d batchMemberFixture) referencePicture {
				e.promoted, d.promoted = 2, 2
				return referencePicture{engine: e, decoder: d, podGone: map[v1beta1.ComponentType]int32{dec: 1}}
			},
			comp: eng, wantPhase: v1beta1.CoordinationPhaseWaiting,
		},
		{
			name: "the decoder's batch 2 is still starting, its first new Instance has lost its pod: the engine waits for batch 2 to serve first",
			build: func(e, d batchMemberFixture) referencePicture {
				e.promoted, d.promoted = 2, 1
				return referencePicture{engine: e, decoder: started(d, 1), podGone: map[v1beta1.ComponentType]int32{dec: 1}}
			},
			comp: eng, wantPhase: v1beta1.CoordinationPhaseWaiting, wantGate: v1beta1.RolloutHoldGateRatio,
			wantReason: map[workloadtypes.UpdateStrategyType]string{
				workloadtypes.UpdateStrategyRecreatePod:    "engine batch 2 waits for decoder batch 2 to serve first (1 of 2 serving)",
				workloadtypes.UpdateStrategySurgeThenDrain: "engine batch 2 waits for decoder batch 2 to serve first (1 of 2 serving)",
			},
		},
		{
			name: "the decoder's own next drain while its lost pod is rebuilt is held by its budget of one, and its next surge is not",
			build: func(e, d batchMemberFixture) referencePicture {
				e.promoted, d.promoted = 4, 2
				return referencePicture{engine: e, decoder: d, podGone: map[v1beta1.ComponentType]int32{dec: 1}}
			},
			comp: dec, wantPhase: v1beta1.CoordinationPhaseWaiting, wantGate: v1beta1.RolloutHoldGateBudget,
			wantReason: map[workloadtypes.UpdateStrategyType]string{workloadtypes.UpdateStrategyRecreatePod: budgetHold},
		},
		{
			name: "the prefill side's budget of two leaves one drain beside its lost pod",
			build: func(e, d batchMemberFixture) referencePicture {
				e.promoted, d.promoted = 4, 3
				return referencePicture{engine: e, decoder: d, podGone: map[v1beta1.ComponentType]int32{eng: 1}}
			},
			comp: eng, wantPhase: v1beta1.CoordinationPhaseWaiting,
		},
		{
			name: "a decoder Instance whose new pod was lost before it served is an attempt still open: the engine waits on it",
			build: func(e, d batchMemberFixture) referencePicture {
				e.promoted = 2
				return referencePicture{engine: e, decoder: started(d, 1)}
			},
			comp: eng, wantPhase: v1beta1.CoordinationPhaseSurging, wantGate: v1beta1.RolloutHoldGateRatio,
			wantReason: map[workloadtypes.UpdateStrategyType]string{
				workloadtypes.UpdateStrategyRecreatePod:    "engine batch 2 waits for decoder batch 1 to serve (0 of 1 serving)",
				workloadtypes.UpdateStrategySurgeThenDrain: "engine batch 2 waits for decoder batch 1 to serve (0 of 1 serving)",
			},
		},
	}
	for _, shape := range referenceShapes {
		for _, strategy := range referenceStrategies {
			for _, pic := range pictures {
				t.Run(shape.name+"/"+strategy.name+"/"+pic.name, func(t *testing.T) {
					engine, decoder := shape.build(strategy.strategy)
					p := pic.build(engine, decoder)
					if got := p.phase(ru); got != pic.wantPhase {
						t.Fatalf("the group reads %s, want %s", got, pic.wantPhase)
					}
					allowed, gate, reason := p.ask(t, strategy.group, pic.comp, strategy.mechanism, referenceRules())
					expectHold(t, allowed, gate, reason, pic.wantGate, pic.wantReason[strategy.mechanism])
				})
			}
		}
	}
}

// TestEvaluateUpdateGate_AMemberBudgetEditedMidBatchResizesTheStep edits a
// member's own rollingUpdate budget while the second batch of the
// reference roll is open, under the chart's settings on 12:8 with the
// group's budget at a quarter so the member's own 10 percent is the step.
// The step is read from the spec at the next consult and the two sides'
// batches are planned again from it: the prefill side's budget raised to a
// quarter plans 3, 2, 1, 2, 1, 2, 1 prefill beside one decode per batch, so
// the two attempts open on the prefill side now belong to its first batch
// of three and the side waits on itself; lowered to one Instance both sides
// plan one and one, so the prefill side's four started read as four batches
// and it waits for the decoder's fourth; the decode side's budget raised to
// a quarter plans 2, 2, 2, 2, 2 prefill beside 2, 2, 1, 1, 2 decode, so the
// engine waits for a decoder batch of two. The group's own budget lowered
// to one Instance mid-run is read from the spec at the next consult too,
// while the run keeps the plan it recorded: both steps read one, so the
// sides wait as they do under the prefill side's own budget of one. The
// phase the pass computes from the same replicas does not move on any of
// the edits.
func TestEvaluateUpdateGate_AMemberBudgetEditedMidBatchResizesTheStep(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	ru := v1beta1.CoordinationPolicyRollingUpdate
	quarter := map[v1beta1.UpdateStrategyType]batchGroupFixture{
		v1beta1.UpdateStrategyRecreatePod:    {maxUnavailable: "25%"},
		v1beta1.UpdateStrategySurgeThenDrain: {maxUnavailable: "0", maxSurge: "25%"},
	}
	// setBudget writes a member's own budget for the strategy it rolls on.
	setBudget := func(m *batchMemberFixture, budget string) {
		if m.strategy == v1beta1.UpdateStrategySurgeThenDrain {
			m.surge = budget
		} else {
			m.step = budget
		}
	}
	type edit struct {
		name       string
		apply      func(p *referencePicture)
		engineWait string
		decodeWait string
	}
	edits := []edit{
		{
			name:       "before any edit",
			apply:      func(*referencePicture) {},
			engineWait: "engine batch 3 waits for decoder batch 2 to serve (1 of 2 serving)",
			decodeWait: "decoder batch 3 waits for engine batch 2 to serve (2 of 4 serving)",
		},
		{
			name:       "the prefill side's budget raised to a quarter",
			apply:      func(p *referencePicture) { setBudget(&p.engine, "25%") },
			engineWait: "engine batch 2 waits for engine batch 1 to serve (2 of 3 serving)",
			decodeWait: "decoder batch 3 waits for engine batch 2 to serve (2 of 5 serving)",
		},
		{
			name:       "the prefill side's budget lowered to one Instance",
			apply:      func(p *referencePicture) { setBudget(&p.engine, "1") },
			engineWait: "engine batch 5 waits for decoder batch 4 to serve (1 of 4 serving)",
			decodeWait: "decoder batch 3 waits for decoder batch 2 to serve (1 of 2 serving)",
		},
		{
			name:       "the decode side's budget raised to a quarter",
			apply:      func(p *referencePicture) { setBudget(&p.decoder, "25%") },
			engineWait: "engine batch 3 waits for decoder batch 2 to serve (1 of 4 serving)",
			decodeWait: "decoder batch 2 waits for decoder batch 1 to serve (1 of 2 serving)",
		},
	}
	for _, strategy := range referenceStrategies {
		for _, e := range edits {
			t.Run(strategy.name+"/"+e.name, func(t *testing.T) {
				engine, decoder := twelveEight(strategy.strategy)
				engine.promoted, decoder.promoted = 2, 1
				p := referencePicture{engine: started(engine, 2), decoder: started(decoder, 1)}
				before := p.phase(ru)
				e.apply(&p)
				if got := p.phase(ru); got != before || got != v1beta1.CoordinationPhaseScalingDown {
					t.Fatalf("the group read %s before the edit and %s after, want ScalingDown both times", before, got)
				}
				allowed, gate, reason := p.ask(t, quarter[strategy.strategy], eng, strategy.mechanism, referenceRules())
				expectHold(t, allowed, gate, reason, v1beta1.RolloutHoldGateRatio, e.engineWait)
				allowed, gate, reason = p.ask(t, quarter[strategy.strategy], dec, strategy.mechanism, referenceRules())
				expectHold(t, allowed, gate, reason, v1beta1.RolloutHoldGateRatio, e.decodeWait)
			})
		}
		t.Run(strategy.name+"/the group's budget lowered to one mid-run resizes both steps from the next start", func(t *testing.T) {
			engine, decoder := twelveEight(strategy.strategy)
			engine.promoted, decoder.promoted = 2, 1
			p := referencePicture{engine: started(engine, 2), decoder: started(decoder, 1)}
			lowered := quarter[strategy.strategy]
			lowered.edit = func(spec *v1beta1.RolloutSpec) {
				one := intstr.FromInt32(1)
				spec.Groups[0].RollingUpdate.MaxUnavailable = &one
				spec.Groups[0].RollingUpdate.MaxSurge = &one
			}
			isvc := batchService(lowered, p.members())
			if recorded := isvc.Status.Rollout.ActiveRun.Plan.Groups[0].Group.RollingUpdate; recorded.MaxSurge != nil && recorded.MaxSurge.String() == "1" {
				t.Fatal("the run must keep the budget it recorded")
			}
			allowed, gate, reason := batchGateWith(t, isvc, p.client(t), eng, strategy.mechanism, 0, referenceRules())
			expectHold(t, allowed, gate, reason, v1beta1.RolloutHoldGateRatio, "engine batch 5 waits for decoder batch 4 to serve (1 of 4 serving)")
			allowed, gate, reason = batchGateWith(t, isvc, p.client(t), dec, strategy.mechanism, 0, referenceRules())
			expectHold(t, allowed, gate, reason, v1beta1.RolloutHoldGateRatio, "decoder batch 3 waits for decoder batch 2 to serve (1 of 2 serving)")
		})
	}
}

// TestComputeTransition_AMemberBudgetIsNoFactOfThePhase computes the group
// phase from the same members under three values of a member's own
// rollingUpdate budget, in every phase a group can read: the budget sizes
// the step the batch step counts in and nothing the phase reads, so the
// phase is the same under each.
func TestComputeTransition_AMemberBudgetIsNoFactOfThePhase(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	ru, bg, seq := v1beta1.CoordinationPolicyRollingUpdate, v1beta1.CoordinationPolicyBlueGreen, v1beta1.CoordinationPolicySequential
	recreate := v1beta1.UpdateStrategyRecreatePod
	// observe reads one member's replica as the pass does, with its old pods
	// beside the promoted ones less the pods gone, under a partition.
	observe := func(comp v1beta1.ComponentType, m batchMemberFixture, podGone int32, partition int32, settled bool) ComponentObservation {
		ir := batchReplica(comp, m)
		for i := int32(0); i < podGone; i++ {
			promotedPodGone(ir)
		}
		if settled {
			ir.Status.CurrentRevision = ir.Status.UpdateRevision
		}
		old := m.desired - m.promoted
		pods := map[string]int32{batchRevision(comp, "old"): old, batchRevision(comp, "target"): m.promoted - podGone}
		return buildComponentObservation(&ir.Status, comp, pods, partition)
	}
	type picture struct {
		name      string
		policy    v1beta1.CoordinationPolicy
		build     func(budget string) GroupObservation
		want      v1beta1.CoordinationPhase
		composite string
	}
	member := func(budget string, promoted, failed, partition int32) batchMemberFixture {
		return batchMemberFixture{desired: 8, step: budget, strategy: recreate, promoted: promoted, failed: failed, partition: partition}
	}
	pair := func(policy v1beta1.CoordinationPolicy, engine, decoder ComponentObservation) GroupObservation {
		obs := GroupObservation{
			Group:      ResolvedGroup{Name: "0", Policy: policy, Components: []v1beta1.ComponentType{eng, dec}},
			Components: map[v1beta1.ComponentType]ComponentObservation{eng: engine, dec: decoder},
		}
		if policy == seq {
			obs.Group.Order = []v1beta1.ComponentType{dec, eng}
		}
		return obs
	}
	pictures := []picture{
		{name: "Idle", policy: ru, want: v1beta1.CoordinationPhaseIdle, build: func(b string) GroupObservation {
			return pair(ru, observe(eng, member(b, 8, 0, 0), 0, 0, true), observe(dec, member(b, 8, 0, 0), 0, 0, true))
		}},
		{name: "SeqAwaiting", policy: seq, want: v1beta1.CoordinationPhaseIdle, composite: v1beta1.CompositePhaseSequentialAwaiting, build: func(b string) GroupObservation {
			obs := pair(seq, observe(eng, member(b, 0, 0, 0), 0, 0, true), observe(dec, member(b, 8, 0, 0), 0, 0, true))
			delete(obs.Components, eng) // the next member's replica is not projected yet
			return obs
		}},
		{name: "Surging", policy: ru, want: v1beta1.CoordinationPhaseSurging, build: func(b string) GroupObservation {
			return pair(ru, observe(eng, member(b, 0, 0, 0), 0, 0, false), observe(dec, member(b, 0, 0, 0), 0, 0, false))
		}},
		{name: "Waiting", policy: ru, want: v1beta1.CoordinationPhaseWaiting, build: func(b string) GroupObservation {
			return pair(ru, observe(eng, member(b, 2, 0, 0), 1, 0, false), observe(dec, member(b, 1, 0, 0), 0, 0, false))
		}},
		{name: "Shifting", policy: bg, want: v1beta1.CoordinationPhaseShifting, build: func(b string) GroupObservation {
			return pair(bg, observe(eng, member(b, 2, 0, 0), 0, 0, false), observe(dec, member(b, 1, 0, 0), 0, 0, false))
		}},
		{name: "ScalingDown", policy: ru, want: v1beta1.CoordinationPhaseScalingDown, build: func(b string) GroupObservation {
			return pair(ru, observe(eng, member(b, 2, 0, 0), 0, 0, false), observe(dec, member(b, 1, 0, 0), 0, 0, false))
		}},
		{name: "Staged", policy: ru, want: v1beta1.CoordinationPhaseStaged, build: func(b string) GroupObservation {
			return pair(ru, observe(eng, member(b, 6, 0, 2), 0, 2, false), observe(dec, member(b, 8, 0, 0), 0, 0, true))
		}},
		{name: "Failed", policy: ru, want: v1beta1.CoordinationPhaseFailed, build: func(b string) GroupObservation {
			return pair(ru, observe(eng, member(b, 1, 1, 0), 0, 0, false), observe(dec, member(b, 1, 0, 0), 0, 0, false))
		}},
		{name: "Paused", policy: ru, want: v1beta1.CoordinationPhasePaused, build: func(b string) GroupObservation {
			obs := pair(ru, observe(eng, member(b, 2, 0, 0), 0, 0, false), observe(dec, member(b, 1, 0, 0), 0, 0, false))
			obs.PausedGlobal = true
			return obs
		}},
	}
	for _, pic := range pictures {
		t.Run(pic.name, func(t *testing.T) {
			for _, budget := range []string{"10%", "25%", "1"} {
				got := ComputeTransition(pic.build(budget))
				if got.Phase != pic.want {
					t.Fatalf("budget %s: the group reads %s, want %s", budget, got.Phase, pic.want)
				}
				if pic.composite != "" && got.CompositePhase != pic.composite {
					t.Fatalf("budget %s: the composite reads %s, want %s", budget, got.CompositePhase, pic.composite)
				}
			}
		})
	}
}
