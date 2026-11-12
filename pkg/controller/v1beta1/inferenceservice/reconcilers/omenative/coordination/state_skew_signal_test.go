package coordination

import (
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// The group's skew signal, the RatioSkewRejected event and its counter, on
// an engine of gangs rolled under SurgeThenDrain beside a decoder, in one
// rollingUpdate group that keeps their ratio within 20 percent. A gang's
// replacement is a row of its own beside its source until the old gang
// leaves, so the replica publishes 5 rows for 4 gangs and, once the
// replacement serves, counts 5 serving. The signal reads the fleet in
// Instances, as the replica-side gate does: the step it proposes for a
// member is a share of the Instance count the spec asks for, and a
// replacement gang serving beside its source counts once with that source.
// A gang surge alone therefore raises no signal; a real skew still does.

// skewGroup is the group of the engine and the decoder under maintainRatio
// at tolerance 20 that names no budget, so the proposed step is the default
// quarter.
func skewGroup() ResolvedGroup {
	tol := int32(20)
	return ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Policy:     v1beta1.CoordinationPolicyRollingUpdate,
		Pacing:     v1beta1.CoordinationPacing{Type: v1beta1.CoordinationPacingRatioBalanced, RatioTolerancePercent: &tol},
	}
}

// observeMember reads a member the way the coordination pass reads it: the
// observation built from the replica's status, with the spec's count beside
// it. The pod counts per revision only feed TotalPods, which the signal does
// not read, so one pod per row on the current revision stands in for them.
func observeMember(ir *v1beta1.InferenceReplica) ComponentObservation {
	pods := map[string]int32{ir.Status.CurrentRevision: ir.Status.Replicas}
	co := buildComponentObservation(&ir.Status, ir.Spec.Component, pods, 0)
	co.SpecReplicas = specReplicasOf(ir)
	return co
}

// skewSignal runs the group machine over the two members with the anchor at
// 4:4 and reports the phase and whether the pass would raise the signal.
func skewSignal(engine, decoder *v1beta1.InferenceReplica) GroupTransition {
	return ComputeTransition(GroupObservation{
		Group: skewGroup(),
		Components: map[v1beta1.ComponentType]ComponentObservation{
			v1beta1.EngineComponent:  observeMember(engine),
			v1beta1.DecoderComponent: observeMember(decoder),
		},
		OriginalReplicas: map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 4, v1beta1.DecoderComponent: 4},
	})
}

// replacementGangsServing marks every open pair's replacement gang as
// serving beside its source, the moment before the old gang leaves, and
// counts it the way the replica does.
func replacementGangsServing(ir *v1beta1.InferenceReplica) *v1beta1.InferenceReplica {
	for i := range ir.Status.InstanceStatuses {
		row := &ir.Status.InstanceStatuses[i]
		if row.Operation == nil || row.Operation.Step != workloadtypes.UpdateStepGangSurgeTarget {
			continue
		}
		row.ReadyPodCount, row.ServingPodCount, row.AvailablePodCount = gangPods, gangPods, gangPods
		ir.Status.ServingReplicas++
		ir.Status.ReadyReplicas++
	}
	return ir
}

// sourceGangsLeaving takes every open pair's source gang out of rotation, the
// window after the replacement took over and before it is promoted in the
// source's place.
func sourceGangsLeaving(ir *v1beta1.InferenceReplica) *v1beta1.InferenceReplica {
	for i := range ir.Status.InstanceStatuses {
		row := &ir.Status.InstanceStatuses[i]
		if row.Operation == nil || row.Operation.Step != workloadtypes.UpdateStepSurge {
			continue
		}
		row.ServingPodCount, row.AvailablePodCount = 0, 0
		ir.Status.ServingReplicas--
	}
	return ir
}

// singlePodDecoder is a decoder of four single-pod Instances with promoted
// of them already on the new version.
func singlePodDecoder(promoted int32) *v1beta1.InferenceReplica {
	return batchReplica(v1beta1.DecoderComponent, batchMemberFixture{desired: 4, strategy: v1beta1.UpdateStrategySurgeThenDrain, promoted: promoted})
}

// TestComputeTransition_AReplacementGangServingBesideItsSourceRaisesNoSkew
// is the operator's story: 4 engine gangs of one leader and one worker
// beside 4 decoders, in a group that keeps their ratio within 20 percent.
// Version 2 is pushed under SurgeThenDrain. The first gang's replacement is
// built as a row of its own and comes up serving beside its source, so the
// replica publishes 5 rows and counts 5 serving for 4 gangs, and the decoder
// has not moved yet. The fleet still serves 4 gangs beside 4 decoders, so the
// group records no skew: the replacement and its source are one Instance,
// and the step the signal proposes is a quarter of the 4 gangs the spec asks
// for, not of the 5 rows. Once the old gang leaves rotation and before the
// replacement is promoted in its place, the engine serves 4 again and the
// answer is the same.
func TestComputeTransition_AReplacementGangServingBesideItsSourceRaisesNoSkew(t *testing.T) {
	cases := []struct {
		name   string
		engine *v1beta1.InferenceReplica
	}{
		{name: "replacement-gang-serving-beside-its-source-5-rows", engine: replacementGangsServing(gangRollReplica(gangRollFixture{desired: 4, open: 1}))},
		{name: "replacement-gang-not-yet-serving-5-rows", engine: gangRollReplica(gangRollFixture{desired: 4, open: 1})},
		{name: "source-gang-left-replacement-not-yet-promoted-5-rows", engine: sourceGangsLeaving(replacementGangsServing(gangRollReplica(gangRollFixture{desired: 4, open: 1})))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := skewSignal(tc.engine, singlePodDecoder(0))
			if tr.Phase != v1beta1.CoordinationPhaseSurging {
				t.Fatalf("phase %s, want Surging: the first pair is open and nothing is promoted yet", tr.Phase)
			}
			if tr.RatioSkewRejected {
				t.Errorf("the pass would raise RatioSkewRejected on 4 gangs serving beside 4 decoders; a gang surge alone is no skew (engine rows %d, serving %d)",
					tc.engine.Status.Replicas, tc.engine.Status.ServingReplicas)
			}
		})
	}
}

// TestComputeTransition_ARealSkewWithoutAGangSurgeStillRaisesTheSignal keeps
// the signal where it belongs, on the same 4 gangs beside 4 decoders with no
// gang surge open. With 2 gangs already on version 2 and no decoder moved,
// the engine runs ahead of the decoder by half its count, past the tolerance
// of a fifth, and the signal says so. With one gang's pods down and no surge
// open, 3 gangs serve beside 4 decoders, and the decoder's next step would
// widen the gap to 3:5, so the signal says so as well.
func TestComputeTransition_ARealSkewWithoutAGangSurgeStillRaisesTheSignal(t *testing.T) {
	gangDown := gangRollReplica(gangRollFixture{desired: 4})
	gangDown.Status.InstanceStatuses[0].ServingPodCount, gangDown.Status.InstanceStatuses[0].AvailablePodCount = 0, 0
	gangDown.Status.ServingReplicas--
	cases := []struct {
		name   string
		engine *v1beta1.InferenceReplica
	}{
		{name: "two-gangs-on-the-new-version-ahead-of-the-decoder", engine: gangRollReplica(gangRollFixture{desired: 4, promoted: 2})},
		{name: "one-gang-down-the-decoder-step-would-widen-the-gap", engine: gangDown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := skewSignal(tc.engine, singlePodDecoder(0))
			if tr.Phase != v1beta1.CoordinationPhaseSurging {
				t.Fatalf("phase %s, want Surging: the decoder has no Instance on the new version", tr.Phase)
			}
			if !tr.RatioSkewRejected {
				t.Errorf("the pass would raise no RatioSkewRejected; the skew is real (engine rows %d, serving %d, on the new version %d)",
					tc.engine.Status.Replicas, tc.engine.Status.ServingReplicas, tc.engine.Status.UpdatedReplicas)
			}
		})
	}
}
