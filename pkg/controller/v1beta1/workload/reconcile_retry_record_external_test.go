package workload_test

import (
	"slices"
	"testing"
	"time"

	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/ome/pkg/constants"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// siblingCrashStory drives the recovery harness through a crash loop two
// Instances of one revision share: rows by index, the set number each is
// Ready on, and the revision's retry record as the pass left it.
type siblingCrashStory struct {
	t   *testing.T
	h   *recoveryHarness
	rev string
}

// newSiblingCrashStory settles eight single-pod Instances on version 1
// under RecreateInstanceOnPodRestart with a retry schedule of three
// attempts, the first backoff 30 s.
func newSiblingCrashStory(t *testing.T) *siblingCrashStory {
	t.Helper()
	const replicas = int32(8)
	h := newRecoveryHarnessFor(t, false, constants.MainContainerName)
	h.recorder = record.NewFakeRecorder(1024)
	h.replicas = replicas
	rebuild := workloadtypes.RestartPolicyRecreateInstance
	h.lifecycle.RestartPolicy = &rebuild
	h.stepCap = 5 * time.Second
	h.kubeletLag = time.Second
	h.retryPolicy = &workloadtypes.RetryPolicy{MaxAttempts: 3, InitialDelay: 30 * time.Second, MaxDelay: 2 * time.Minute, Multiplier: 2}
	h.useRollingBudget(workloadtypes.UpdateStrategyRecreatePod)
	s := &siblingCrashStory{t: t, h: h, rev: h.revV1.Name}
	s.mustReach("the initial create never settled on v1", 200, func() bool { return h.settledOn(h.revV1, replicas) })
	return s
}

// mustReach drives up to passes passes until the reading holds, or dumps
// the state and stops the test with what.
func (s *siblingCrashStory) mustReach(what string, passes int, reading func() bool) {
	s.t.Helper()
	if !s.h.run(passes, reading) {
		s.h.dumpState(what)
		s.t.Fatalf("%s; record %+v", what, s.record())
	}
}

func (s *siblingCrashStory) row(idx int32) workloadtypes.InstanceStatus {
	s.t.Helper()
	for _, row := range s.h.irStatuses() {
		if row.Index == idx {
			return row
		}
	}
	s.t.Fatalf("Instance %d has no row", idx)
	return workloadtypes.InstanceStatus{}
}

// readyOn reports whether idx is Ready with no operation on its set
// number n; every rebuild raises the number by one.
func (s *siblingCrashStory) readyOn(idx int32, n int64) bool {
	row := s.row(idx)
	return row.Phase == workloadtypes.InstancePhaseReady && row.Operation == nil && row.Incarnation == n
}

// remembers reports whether idx's row records a death of the set it is
// Ready on.
func (s *siblingCrashStory) remembers(idx int32) bool {
	row := s.row(idx)
	return row.LastFailure != nil && row.ReadySince != nil && row.LastFailure.Time.After(row.ReadySince.Time)
}

// runnerDies has the kubelet report idx's runner ended on a signal, once.
func (s *siblingCrashStory) runnerDies(idx int32) {
	s.h.killRunnerOnce(s.h.podOf(idx, ""))
}

// record is the revision's retry record, empty before the first count.
func (s *siblingCrashStory) record() workloadtypes.RetryBlock {
	if b := s.h.findBlock(s.rev); b != nil {
		return *b
	}
	return workloadtypes.RetryBlock{}
}

// requireRecord checks the record's count and state after a step.
func (s *siblingCrashStory) requireRecord(attempts int32, state workloadtypes.RetryBlockState, what string) {
	s.t.Helper()
	if b := s.record(); b.AttemptsStarted != attempts || b.State != state {
		s.t.Fatalf("%s; record %+v, want %d attempt(s) in %s", what, b, attempts, state)
	}
}

// lastHold is the reason of the latest hold the roll reported, "" when
// it reported none.
func (s *siblingCrashStory) lastHold() string {
	if len(s.h.holds) == 0 {
		return ""
	}
	return s.h.holds[len(s.h.holds)-1].Reason
}

// Eight single-pod Instances serve version 1 under
// RecreateInstanceOnPodRestart with a retry schedule of three attempts.
// Instance 0's runner dies after Ready and the policy rebuilds the set
// once for free; the rebuilt set dies inside the crash window, so the
// revision's retry record counts attempt 1 and waits for its backoff.
// Instance 1's runner dies next and the policy rebuilds that set for free
// too; it serves again before the record comes due. The backoff comes
// due and the record's attempt rebuilds Instance 0. While that set is
// still coming up, Instance 1's rebuilt set dies.
//
// Instance 1's set was built before the record came due, so the record
// never admitted it. Its death is remembered on the row and folded into
// the attempt under way: the count stays at 1, the hold names Instance 1
// among the attempt's rebuilds, and the attempt rebuilds Instance 1 once
// Instance 0's set is up, with the count unchanged. The count moves to 2
// only when a set the attempt built dies, Instance 0's. The operator's
// three attempts are therefore three sets the record built and watched
// die; a death the policy answered on its own spends none of them.
func TestCrashAfterReady_PolicyRebuildOfASiblingDiesUnderTheOpenAttempt_CountsNoAttempt(t *testing.T) {
	s := newSiblingCrashStory(t)

	// Instance 0: the first death after Ready is the policy's free rebuild.
	s.runnerDies(0)
	s.mustReach("the policy did not rebuild Instance 0 after its first death", 40, func() bool { return s.readyOn(0, 2) })
	// The rebuilt set dies inside the crash window: attempt 1 counts.
	s.runnerDies(0)
	s.mustReach("the death of Instance 0's rebuilt set was not counted", 40, func() bool { return s.record().AttemptsStarted == 1 })
	s.requireRecord(1, workloadtypes.RetryBlockBackoff, "after the first count the record must wait in backoff")
	// Instance 1: its first death, the policy's free rebuild, serving
	// again before the record comes due.
	s.runnerDies(1)
	s.mustReach("the policy did not rebuild Instance 1 after its first death", 40, func() bool { return s.readyOn(1, 2) })
	s.requireRecord(1, workloadtypes.RetryBlockBackoff, "the record came due before Instance 1's rebuilt set served, so the story did not run")
	// The backoff comes due: the record's attempt rebuilds Instance 0.
	s.mustReach("the record's attempt never opened on Instance 0", 60, func() bool {
		return s.record().State == workloadtypes.RetryBlockRetryInProgress && s.row(0).Phase == workloadtypes.InstancePhaseRestarting
	})
	// Instance 1's rebuilt set dies while Instance 0's new set is coming up.
	s.runnerDies(1)
	s.mustReach("the death of Instance 1's rebuilt set was never read", 10, func() bool { return s.remembers(1) })
	if phase := s.row(0).Phase; phase != workloadtypes.InstancePhaseRestarting {
		t.Fatalf("Instance 0's attempt set was already up (%s) when Instance 1's death was read, so the story did not run", phase)
	}
	s.requireRecord(1, workloadtypes.RetryBlockRetryInProgress, "the death of a set the record never admitted spent an attempt")
	if row := s.row(1); row.Phase != workloadtypes.InstancePhaseReady || row.Operation != nil {
		t.Fatalf("Instance 1 must wait for its turn in the attempt under way; got phase %s operation %+v", row.Phase, row.Operation)
	}
	if hold := s.lastHold(); !slices.Contains(namedRebuilds(hold), 1) {
		t.Fatalf("the hold must name Instance 1 among the attempt's rebuilds; got %q", hold)
	}
	// Once Instance 0's set is up, the attempt rebuilds Instance 1 with
	// the count unchanged.
	s.mustReach("Instance 0's attempt set never came up", 40, func() bool { return s.readyOn(0, 3) })
	s.mustReach("the attempt did not rebuild Instance 1 once Instance 0's set was up", 10, func() bool { return s.row(1).Incarnation == 3 })
	s.requireRecord(1, workloadtypes.RetryBlockRetryInProgress, "Instance 1's rebuild inside the attempt moved the count")
	// Instance 0's attempt set dies: that is the attempt's failure, and
	// the record counts attempt 2.
	s.runnerDies(0)
	s.mustReach("the death of the set the attempt built was not counted", 10, func() bool { return s.record().AttemptsStarted == 2 })
	s.requireRecord(2, workloadtypes.RetryBlockBackoff, "after the second count the record must wait in backoff")
	if !s.remembers(0) {
		t.Fatal("after the second count Instance 0 must remember the death of the set the attempt built")
	}
}
