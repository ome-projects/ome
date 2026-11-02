package coordination

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// pushedRow is one Instance of a replica at a second push: the version it
// runs and, when its move toward the push is open, the version it moves to.
type pushedRow struct {
	running string
	moving  string
}

// pushedReplica is one side of the 6:2 service as its InferenceReplica
// stands when the other side's pass reads it: the spec's generation and the
// service generation the projector wrote it from, the generation and the
// roll target its status was written for, and its Instances.
type pushedReplica struct {
	comp      v1beta1.ComponentType
	step      int32
	spec      int64 // metadata.generation
	projected int64 // the service generation recorded by the projector
	observed  int64 // status.observedGeneration
	target    string
	rows      []pushedRow
	// templateHash is the one the projector wrote beside the spec and
	// observedHash the one the status repeats; both empty on a replica the
	// readers read by its generation.
	templateHash, observedHash string
}

// pushRevision names a side's revision for a version the way the replica
// records it.
func pushRevision(comp v1beta1.ComponentType, version string) string {
	return batchServiceName + "-" + string(comp) + "-" + version
}

func (p pushedReplica) build() *v1beta1.InferenceReplica {
	annotations := parentGenAnnotation(p.projected)
	if p.templateHash != "" {
		annotations[constants.InferenceReplicaTemplateHashAnnotationKey] = p.templateHash
	}
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   batchNamespace,
			Name:        batchServiceName + "-" + string(p.comp),
			Generation:  p.spec,
			Annotations: annotations,
		},
		Spec: v1beta1.InferenceReplicaSpec{
			Component: p.comp,
			Replicas:  ptr.To(int32(len(p.rows))),
			Lifecycle: &v1beta1.LifecycleSpec{UpdateStrategy: &v1beta1.UpdateStrategy{
				Type:          v1beta1.UpdateStrategyRecreatePod,
				RollingUpdate: &v1beta1.RollingUpdate{MaxUnavailable: ptr.To(intstr.FromInt32(p.step))},
			}},
		},
		Status: v1beta1.InferenceReplicaStatus{
			ObservedGeneration:   p.observed,
			ObservedTemplateHash: p.observedHash,
			CurrentRevision:      pushRevision(p.comp, "v1"),
			UpdateRevision:       pushRevision(p.comp, p.target),
			Replicas:             int32(len(p.rows)),
		},
	}
	for i, r := range p.rows {
		row := v1beta1.OMENativeInstanceStatus{Index: int32(i), Phase: v1beta1.OMENativeInstanceReady, RunningRevision: pushRevision(p.comp, r.running), PodCount: 1, ServingPodCount: 1}
		if r.moving != "" {
			row.Phase, row.PodCount, row.ServingPodCount = v1beta1.OMENativeInstanceUpdating, 0, 0
			row.TargetRevision = pushRevision(p.comp, r.moving)
			row.Operation = &v1beta1.InstanceOperation{ID: "attempt", Type: v1beta1.InstanceOperationUpdate, Step: workloadtypes.UpdateStepDrain, TargetRevision: row.TargetRevision}
		} else {
			ir.Status.ServingReplicas++
			ir.Status.ReadyReplicas++
		}
		if r.running == p.target {
			ir.Status.UpdatedReplicas++
		}
		ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses, row)
	}
	return ir
}

// TestEvaluateUpdateGate_BatchStepWaitsForAPeerWhoseStatusPredatesThePush
// is the second push of a 6 prefill and 2 decode service rolling under
// RecreatePod, 3 and 1 at a time, with the decoder named to move first and
// the ratio rule at the group's tolerance of 20. Version 2 was pushed and
// the first new decoder serves; nothing of the prefill side has moved.
// Version 3 is pushed on both sides. The prefill side's pass reads the
// decoder's replica one status write behind the push: its status still
// names version 2 as the roll target, with Instance 0 promoted on it. The
// prefill side must wait for the decoder's first batch of the new push, as
// it does one write later once the status records version 3, and not read
// the promoted version-2 decoder as that batch served. A peer whose spec
// was projected from an older service generation than the acting side's is
// read the same way. The named side is never held by the other side's
// status, and with no side named the first batch waits on nobody. Whether
// the status predates the push is read off the template hash the projector
// writes beside the spec: a decoder whose generation moved twice for writes
// outside its templates (an annotation value the revision leaves out, a
// count) is current, and one whose templates changed while its status still
// repeats the previous hash predates the push even with its generation
// observed.
func TestEvaluateUpdateGate_BatchStepWaitsForAPeerWhoseStatusPredatesThePush(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	const firstBatchWait = "engine batch 1 waits for decoder batch 1 to serve first (0 of 1 serving)"
	ready := func(versions ...string) []pushedRow {
		rows := make([]pushedRow, 0, len(versions))
		for _, v := range versions {
			rows = append(rows, pushedRow{running: v})
		}
		return rows
	}
	engine := func(observed int64, target string) pushedReplica {
		return pushedReplica{comp: eng, step: 3, spec: 3, projected: 7, observed: observed, target: target, rows: ready("v1", "v1", "v1", "v1", "v1", "v1")}
	}
	decoder := func(spec, projected, observed int64, target string, rows []pushedRow) pushedReplica {
		return pushedReplica{comp: dec, step: 1, spec: spec, projected: projected, observed: observed, target: target, rows: rows}
	}
	decoderFirst := GroupDefaults{PrefillDecode: PrefillDecodeRules{KeepRatio: true, FirstSide: dec}}
	cases := []struct {
		name       string
		engine     pushedReplica
		decoder    pushedReplica
		ask        v1beta1.ComponentType
		defaults   GroupDefaults
		wantHold   bool
		wantReason string
	}{
		{
			name:    "the decoder's status still records the previous push",
			engine:  engine(2, "v2"),
			decoder: decoder(3, 7, 2, "v2", ready("v2", "v1")),
			ask:     eng, defaults: decoderFirst, wantHold: true, wantReason: firstBatchWait,
		},
		{
			name:    "the decoder's spec was projected from an older service generation",
			engine:  engine(2, "v2"),
			decoder: decoder(2, 6, 2, "v2", ready("v2", "v1")),
			ask:     eng, defaults: decoderFirst, wantHold: true, wantReason: firstBatchWait,
		},
		{
			name:    "the decoder's status records the push with nothing moved yet",
			engine:  engine(3, "v3"),
			decoder: decoder(3, 7, 3, "v3", ready("v2", "v1")),
			ask:     eng, defaults: decoderFirst, wantHold: true, wantReason: firstBatchWait,
		},
		{
			name:    "the decoder's first start toward the push is open",
			engine:  engine(3, "v3"),
			decoder: decoder(3, 7, 3, "v3", []pushedRow{{running: "v2", moving: "v3"}, {running: "v1"}}),
			ask:     eng, defaults: decoderFirst, wantHold: true, wantReason: firstBatchWait,
		},
		{
			name:    "the first new decoder of the push serves, so the prefill side's first batch opens",
			engine:  engine(3, "v3"),
			decoder: decoder(3, 7, 3, "v3", ready("v3", "v1")),
			ask:     eng, defaults: decoderFirst,
		},
		{
			name:    "the named side's first start is admitted while the other side's status predates the push",
			engine:  engine(2, "v2"),
			decoder: decoder(3, 7, 3, "v3", ready("v2", "v1")),
			ask:     dec, defaults: decoderFirst,
		},
		{
			name:    "with no side named the first batch waits on nobody, status behind the push or not",
			engine:  engine(2, "v2"),
			decoder: decoder(3, 7, 2, "v2", ready("v2", "v1")),
			ask:     eng, defaults: GroupDefaults{PrefillDecode: PrefillDecodeRules{KeepRatio: true}},
		},
		{
			name:    "the decoder's generation moved for writes outside its templates, so its status is current and the first new decoder opens the prefill side's batch",
			engine:  engine(3, "v3"),
			decoder: pushedReplica{comp: dec, step: 1, spec: 5, projected: 7, observed: 3, target: "v3", rows: ready("v3", "v1"), templateHash: "t3", observedHash: "t3"},
			ask:     eng, defaults: decoderFirst,
		},
		{
			name:    "the decoder's templates changed and its status still repeats the previous template hash, so it predates the push with its generation observed",
			engine:  engine(3, "v3"),
			decoder: pushedReplica{comp: dec, step: 1, spec: 3, projected: 7, observed: 3, target: "v2", rows: ready("v2", "v1"), templateHash: "t3", observedHash: "t2"},
			ask:     eng, defaults: decoderFirst, wantHold: true, wantReason: firstBatchWait,
		},
		{
			name:    "a peer rewritten mid-roll reads as not yet observed for the one pass until its status follows",
			engine:  pushedReplica{comp: eng, step: 3, spec: 3, projected: 7, observed: 3, target: "v3", rows: ready("v3", "v3", "v3", "v1", "v1", "v1")},
			decoder: decoder(4, 7, 3, "v3", ready("v3", "v1")),
			ask:     eng, defaults: decoderFirst, wantHold: true, wantReason: "engine batch 2 waits for decoder batch 1 to serve (0 of 1 serving)",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			members := map[v1beta1.ComponentType]batchMemberFixture{eng: {desired: 6}, dec: {desired: 2}}
			isvc := batchService(batchGroupFixture{maxUnavailable: "50%", tolerance: ptr.To(int32(20))}, members)
			isvc.Generation = 7
			objs := []client.Object{tc.engine.build(), tc.decoder.build()}
			reads := fakeclient.New(t, fakeclient.WithObjects(objs...))
			target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Namespace: batchNamespace, Name: pushRevision(tc.ask, "v3")}}
			allowed, gate, reason := EvaluateUpdateGate(context.Background(), reads, isvc, tc.ask, pushRevision(tc.ask, "v1"), target, nil, tc.defaults, workloadtypes.UpdateStrategyRecreatePod, 0, 0)
			if tc.wantHold == allowed {
				t.Fatalf("allowed=%v, want hold=%v (gate=%q reason=%q)", allowed, tc.wantHold, gate, reason)
			}
			if !tc.wantHold {
				return
			}
			if gate != v1beta1.RolloutHoldGateRatio {
				t.Errorf("gate=%q, want %q (reason=%q)", gate, v1beta1.RolloutHoldGateRatio, reason)
			}
			if reason != tc.wantReason {
				t.Errorf("reason=%q, want %q", reason, tc.wantReason)
			}
		})
	}
}
