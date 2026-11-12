package rolloutrun

import (
	"context"
	"testing"

	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// stagedPairFixture is a prefill/decode rollingUpdate group whose engine
// rests at a partition of one (Instance 0 on the old revision, Instance 1
// on the new one), whose group status reads Staged, and whose run is open
// toward the new revision on both members. The decoder is the caller's.
func stagedPairFixture(decoder *v1beta1.InferenceReplica) (*v1beta1.InferenceService, *v1beta1.InferenceReplica) {
	isvc := isvcFixture(v1beta1.RolloutGroup{
		Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		RollingUpdate: &v1beta1.GroupRollingUpdate{},
	})
	isvc.Status.RolloutCoordination = &v1beta1.RolloutCoordinationStatus{Groups: []v1beta1.RolloutCoordinationGroupStatus{
		{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Phase: v1beta1.CoordinationPhaseStaged},
	}}
	decoderTarget := decoder.Status.UpdateRevision
	isvc.Status.Rollout = &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{
		RunID: "llm-a-run",
		TargetRevisions: []v1beta1.RolloutRunTarget{
			{Component: v1beta1.EngineComponent, Revision: "bbbbbbbb", StableRevision: "aaaaaaaa"},
			{Component: v1beta1.DecoderComponent, Revision: revisionHashOf(decoderTarget), StableRevision: "aaaaaaaa"},
		},
		Plan: v1beta1.RolloutRunPlan{Groups: []v1beta1.RolloutRunGroup{{Group: isvc.Spec.Rollout.Groups[0]}}},
	}}
	engine := irFixture(oldRev, newRev)
	engine.Spec.Lifecycle = &v1beta1.LifecycleSpec{UpdateStrategy: &v1beta1.UpdateStrategy{RollingUpdate: &v1beta1.RollingUpdate{Partition: ptr.To(int32(1))}}}
	engine.Status.Replicas = 2
	engine.Status.UpdatedReplicas = 1
	engine.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: oldRev},
		{Index: 1, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: newRev},
	}
	return isvc, engine
}

// revisionHashOf is the trailing segment of a revision name, the hash the
// run records.
func revisionHashOf(name string) string {
	for i := len(name) - 1; i >= 0; i-- {
		if name[i] == '-' {
			return name[i+1:]
		}
	}
	return name
}

// A prefill/decode group rests at the prefill side's partition and the
// operator scales the decoder up. While the new decoder is being created
// (no running revision until it is Ready) the run stays open; it closes
// Completed once the Instance serves. A run that closed Completed on the
// Staged group before the decoder converged would reopen on the decoder's
// trailing Instance every pass, two status writes and two events each,
// until the Instance served.
func TestAStagedGroupExcusesOnlyTheMemberRestingAtItsPartition(t *testing.T) {
	decoderRev := "llm-a-decoder-bbbbbbbb"
	decoder := irFixture(decoderRev, decoderRev)
	decoder.Name = "llm-a-decoder"
	decoder.Status.Replicas = 3
	decoder.Status.UpdatedReplicas = 2
	decoder.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: decoderRev},
		{Index: 1, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: decoderRev},
		{Index: 2, Phase: v1beta1.OMENativeInstanceCreating, TargetRevision: decoderRev,
			Operation: &v1beta1.InstanceOperation{ID: "create", Type: v1beta1.InstanceOperationCreate, TargetRevision: decoderRev}},
	}
	isvc, engine := stagedPairFixture(decoder)
	in := testInputs(t, isvc, engine, decoder)
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if !v1beta1.RolloutRunActive(isvc) {
		t.Fatalf("the decoder's new Instance is still being created, so the run stays open; it closed: %+v", isvc.Status.Rollout.LastRun)
	}
	// The Instance serves: the decoder converges and the run closes.
	decoder.Status.UpdatedReplicas = 3
	decoder.Status.InstanceStatuses[2] = v1beta1.OMENativeInstanceStatus{Index: 2, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: decoderRev}
	in = testInputs(t, isvc, engine, decoder)
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if v1beta1.RolloutRunActive(isvc) || isvc.Status.Rollout.LastRun == nil || isvc.Status.Rollout.LastRun.Outcome != v1beta1.RolloutRunCompleted {
		t.Fatalf("once the Instance serves the run closes Completed; got %+v", isvc.Status.Rollout)
	}
}

// The same group, and the decoder's every Instance runs the new revision
// while a retry record of a failed attempt keeps its current revision at
// the old one (the status writer promotes the current revision only once
// no record names the update revision). The run stays open until the
// record is removed: the Staged group excuses only the member resting at
// its partition, so no pass closes on the group and opens again on the
// decoder's revisions.
func TestAStagedGroupDoesNotExcuseAMemberWhoseRecordKeepsItsCurrentRevisionBehind(t *testing.T) {
	oldDecoderRev, newDecoderRev := "llm-a-decoder-aaaaaaaa", "llm-a-decoder-bbbbbbbb"
	decoder := irFixture(oldDecoderRev, newDecoderRev)
	decoder.Name = "llm-a-decoder"
	decoder.Status.Replicas = 2
	decoder.Status.UpdatedReplicas = 2
	decoder.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: newDecoderRev},
		{Index: 1, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: newDecoderRev},
	}
	decoder.Status.RetryBlocks = []v1beta1.RetryBlock{{TargetRevision: newDecoderRev, State: v1beta1.RetryBlockBackoff, AttemptsStarted: 1}}
	isvc, engine := stagedPairFixture(decoder)
	in := testInputs(t, isvc, engine, decoder)
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if !v1beta1.RolloutRunActive(isvc) {
		t.Fatalf("the decoder's current revision is behind its target, so the run stays open; it closed: %+v", isvc.Status.Rollout.LastRun)
	}
	// The record is removed and the current revision promoted: the run closes.
	decoder.Status.RetryBlocks = nil
	decoder.Status.CurrentRevision = newDecoderRev
	in = testInputs(t, isvc, engine, decoder)
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if v1beta1.RolloutRunActive(isvc) || isvc.Status.Rollout.LastRun == nil || isvc.Status.Rollout.LastRun.Outcome != v1beta1.RolloutRunCompleted {
		t.Fatalf("once the decoder's current revision reaches the target the run closes Completed; got %+v", isvc.Status.Rollout)
	}
}
