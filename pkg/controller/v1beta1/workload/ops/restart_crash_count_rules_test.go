package ops

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// The crash count reads the crash of a Ready row's promoted pod set
// against the running revision's retry record before a repair opens. The
// record is written before the row: a crash the record does not count yet
// is counted on it, then remembered on the row dated at this pass. A crash
// no repair answers is only remembered and holds the row. Once the record
// holds the revision, a set that is down is written Failed with one
// InstanceFailed warning per pod set, and a set that serves again is named
// on the row. A record in backoff holds the row until it is due; a due
// record, or one whose attempt has nothing else in flight, admits the
// rebuild and names the revision. One row per rule: the row, its pods and
// records, and the writes in their order.

const (
	crashCountRevision = "example-engine-rev1"
	crashCountImage    = "registry.example/engine:v1"
	crashCountPod      = "example-engine-0-0"
)

var (
	crashCountNow        = time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	crashCountReadySince = metav1.NewTime(crashCountNow.Add(-time.Hour))
	crashCountCrashedAt  = crashCountReadySince.Add(20 * time.Minute)

	errRecordWriteRefused = errors.New("record write refused")
	errRowWriteRefused    = errors.New("row write refused")

	// crashCountRetry is a schedule of three attempts a minute apart;
	// crashCountLastAttempt holds the revision at the first failure.
	crashCountRetry       = &workload.RetryPolicy{MaxAttempts: 3, InitialDelay: time.Minute, MaxDelay: 10 * time.Minute, Multiplier: 2}
	crashCountLastAttempt = &workload.RetryPolicy{MaxAttempts: 1, InitialDelay: time.Minute, MaxDelay: 10 * time.Minute, Multiplier: 2}
)

// crashCountAt is a time d from now.
func crashCountAt(d time.Duration) *metav1.Time {
	t := metav1.NewTime(crashCountNow.Add(d))
	return &t
}

// crashCountPromoted gives a pod the identity of the set's one member:
// created before the row entered Ready, running the desired image.
func crashCountPromoted(pod *corev1.Pod) *corev1.Pod {
	pod.CreationTimestamp = metav1.NewTime(crashCountReadySince.Add(-5 * time.Minute))
	pod.Spec.Containers = []corev1.Container{{Name: constants.MainContainerName, Image: crashCountImage}}
	return pod
}

// crashCountCrashingPod is the set's pod down in CrashLoopBackOff since a
// crash after the row entered Ready.
func crashCountCrashingPod() *corev1.Pod {
	return crashCountPromoted(crashLoopingRunnerPod(crashCountPod, crashCountCrashedAt))
}

// crashCountServingAgainPod is the set's pod back in rotation after such a
// crash: the run that died began after Ready, the run since is Ready and
// serving.
func crashCountServingAgainPod() *corev1.Pod {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: crashCountPod, Namespace: "prod"}}
	pod.Status.Phase = corev1.PodRunning
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: constants.MainContainerName, Ready: true, RestartCount: 2,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(crashCountCrashedAt.Add(time.Minute))}},
		LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			ExitCode: 1, Reason: "Error",
			StartedAt:  metav1.NewTime(crashCountReadySince.Add(5 * time.Minute)),
			FinishedAt: metav1.NewTime(crashCountCrashedAt),
		}},
	}}
	since := metav1.NewTime(crashCountCrashedAt.Add(2 * time.Minute))
	pod.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue, LastTransitionTime: since},
		{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: since},
		{Type: query.ServingConditionType, Status: corev1.ConditionTrue, LastTransitionTime: crashCountReadySince},
	}
	return crashCountPromoted(pod)
}

// crashCountSteadyPod is the set's pod serving on with no restart since Ready.
func crashCountSteadyPod() *corev1.Pod {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: crashCountPod, Namespace: "prod"}}
	pod.Status.Phase = corev1.PodRunning
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: constants.MainContainerName, Ready: true,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(crashCountReadySince.Add(-time.Minute))}},
	}}
	pod.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue, LastTransitionTime: crashCountReadySince},
		{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: crashCountReadySince},
		{Type: query.ServingConditionType, Status: corev1.ConditionTrue, LastTransitionTime: crashCountReadySince},
	}
	return crashCountPromoted(pod)
}

// crashCountRow is Instance 0 at rest on the revision; rememberedAt dates
// the crash the row already carries, nil for a row that carries none.
func crashCountRow(rememberedAt *metav1.Time) workload.InstanceStatus {
	row := workload.InstanceStatus{Index: 0, Incarnation: 1, Phase: workload.InstancePhaseReady, RunningRevision: crashCountRevision, ReadySince: &crashCountReadySince}
	if rememberedAt != nil {
		row.LastFailure = &workload.InstanceTermination{PodName: crashCountPod, ContainerName: constants.MainContainerName, Reason: "Error", Time: *rememberedAt}
	}
	return row
}

// crashCountRecord is the revision's retry record.
func crashCountRecord(state workload.RetryBlockState, attempts int32, first, last, next *metav1.Time) workload.RetryBlock {
	return workload.RetryBlock{TargetRevision: crashCountRevision, State: state, AttemptsStarted: attempts, FirstFailureAt: first, LastFailureAt: last, NextRetryAt: next, Reason: "Error"}
}

// crashCountWorld is what the count reads and what it writes through: the
// row writer, the record writer and the warning sink, each kept in a journal.
type crashCountWorld struct {
	policy  workload.RestartPolicy
	row     workload.InstanceStatus
	peers   []workload.InstanceStatus
	pods    []*corev1.Pod
	records []workload.RetryBlock
	retry   *workload.RetryPolicy
	grace   time.Duration
	// warned marks the row as having delivered its InstanceFailed warning.
	warned bool
	// noWarningSink leaves the input without a warning sink.
	noWarningSink bool
	// refuseRecordWrite fails every record write; refuseRowWrite fails the
	// n-th row write, counted from 1.
	refuseRecordWrite bool
	refuseRowWrite    int
}

type crashCountAnswer struct {
	held bool
	rev  string
	// err is the error's text in pieces; "_" stands for one word.
	err     []string
	journal []string
}

type crashCountRule struct {
	name  string
	world crashCountWorld
	want  crashCountAnswer
}

func crashCountRowLine(idx int32, s *workload.InstanceStatus) string {
	failure := "none"
	if s.LastFailure != nil {
		failure = fmt.Sprintf("%s on %s at %s", s.LastFailure.Reason, s.LastFailure.PodName, s.LastFailure.Time.UTC().Format("15:04:05"))
	}
	return fmt.Sprintf("row %d %s operation=%v failure=%s noted=%v warned=%v", idx, s.Phase, s.Operation != nil, failure,
		status.FailureNoted(s, CrashLoopServingNote), status.Announced(*s, workload.EventReasonInstanceFailed))
}

func crashCountRecordLine(b workload.RetryBlock) string {
	next := "none"
	if b.NextRetryAt != nil {
		next = b.NextRetryAt.UTC().Format("15:04:05")
	}
	return fmt.Sprintf("record %s %s attempts=%d reason=%s next=%s", b.TargetRevision, b.State, b.AttemptsStarted, b.Reason, next)
}

// markWarned records the InstanceFailed warning as delivered on the row.
func markWarned(t *testing.T, row *workload.InstanceStatus) {
	t.Helper()
	in := workload.ReconcileInput{MutateInstance: func(_ context.Context, _ int32, mutate func(*workload.InstanceStatus) bool) error {
		mutate(row)
		return nil
	}}
	recorded, err := status.Announce(context.Background(), in, row.Index, workload.EventReasonInstanceFailed)
	if err != nil || !recorded {
		t.Fatalf("marking the warning as delivered: recorded=%v err=%v", recorded, err)
	}
}

// wordsAround reports whether text is the pieces in order, one space apart,
// with exactly one word wherever a piece is "_".
func wordsAround(text string, pieces []string) bool {
	parts := make([]string, len(pieces))
	for i, piece := range pieces {
		if piece == "_" {
			parts[i] = `\S+`
		} else {
			parts[i] = regexp.QuoteMeta(piece)
		}
	}
	return regexp.MustCompile("^" + strings.Join(parts, " ") + "$").MatchString(text)
}

func (w crashCountWorld) count(t *testing.T) (bool, string, error, []string) {
	t.Helper()
	rows := append([]workload.InstanceStatus{w.row}, w.peers...)
	if w.warned {
		markWarned(t, &rows[0])
	}
	live := slices.Clone(rows)
	records := slices.Clone(w.records)
	journal := []string{}
	rowWrites := 0
	input := workload.ReconcileInput{
		Clock: clocktesting.NewFakeClock(crashCountNow),
		ObservedState: workload.WorkloadObservedState{
			InstanceStatuses: rows,
			RetryBlocks:      w.records,
			UpdateRevision:   crashCountRevision,
			CurrentRevision:  crashCountRevision,
		},
		DesiredSpec: workload.WorkloadDesiredSpec{PodSpec: &corev1.PodSpec{
			Containers: []corev1.Container{{Name: constants.MainContainerName, Image: crashCountImage}},
		}},
		StuckPodGrace:     w.grace,
		UpdateRetryPolicy: w.retry,
		PassWake:          &workload.PassWake{},
	}
	input.MutateInstance = func(_ context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
		rowWrites++
		if rowWrites == w.refuseRowWrite {
			return errRowWriteRefused
		}
		s := &live[idx]
		if mutate(s) {
			journal = append(journal, crashCountRowLine(idx, s))
		}
		return nil
	}
	input.MutateRetryBlock = func(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		if w.refuseRecordWrite {
			return errRecordWriteRefused
		}
		i := slices.IndexFunc(records, func(b workload.RetryBlock) bool { return b.TargetRevision == rev })
		block := workload.RetryBlock{TargetRevision: rev}
		if i >= 0 {
			block = records[i]
		}
		switch mutate(&block) {
		case workload.RetryBlockPersist:
			if i >= 0 {
				records[i] = block
			} else {
				records = append(records, block)
			}
			journal = append(journal, crashCountRecordLine(block))
		case workload.RetryBlockRemove:
			journal = append(journal, "record "+rev+" removed")
		case workload.RetryBlockUnchanged:
		}
		return nil
	}
	if !w.noWarningSink {
		input.WarnInstanceFailed = func(idx int32, pod, reason string) {
			journal = append(journal, fmt.Sprintf("warning instance=%d pod=%s: %s", idx, pod, reason))
		}
	}
	inst := workload.InstancePlan{Index: 0, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "runner-0", Size: 1}}}
	plan := workload.ComponentPlan{Component: workload.ComponentEngine, RestartPolicy: w.policy, Instances: []workload.InstancePlan{inst}}
	held, rev, err := countCrashOnLadder(context.Background(), input, plan, inst, &input.ObservedState.InstanceStatuses[0], w.pods)
	return held, rev, err, journal
}

var (
	crashCountHolds  = crashCountAnswer{held: true, journal: []string{}}
	crashCountAdmits = crashCountAnswer{rev: crashCountRevision, journal: []string{}}

	// the journal lines the rows share
	firstCountLine     = "record example-engine-rev1 Backoff attempts=1 reason=CrashLoopBackOff next=05:07:07"
	lastCountLine      = "record example-engine-rev1 Held attempts=1 reason=CrashLoopBackOff next=none"
	rememberedNowLine  = "row 0 Ready operation=false failure=Error on example-engine-0-0 at 05:06:07 noted=false warned=false"
	failedLine         = "row 0 Failed operation=false failure=Error on example-engine-0-0 at 04:26:07 noted=false warned=false"
	failedWarnedLine   = "row 0 Failed operation=false failure=Error on example-engine-0-0 at 04:26:07 noted=false warned=true"
	namedLine          = "row 0 Ready operation=false failure=Error on example-engine-0-0 at 04:26:07 noted=true warned=false"
	heldWarning        = "warning instance=0 pod=example-engine-0-0: CrashLoopBackOff: crash after promotion; revision example-engine-rev1 held after 3 failed attempts"
	lastAttemptWarning = "warning instance=0 pod=example-engine-0-0: CrashLoopBackOff: crash after promotion; revision example-engine-rev1 held after 1 failed attempts"
)

// heldRecordWorld is a revision the record holds after three attempts, the
// row remembering its set's crash, and the set's pod as given.
func heldRecordWorld(pod *corev1.Pod) crashCountWorld {
	return crashCountWorld{
		policy:  workload.RestartPolicyRecreateInstance,
		row:     crashCountRow(crashCountAt(-40 * time.Minute)),
		pods:    []*corev1.Pod{pod},
		records: []workload.RetryBlock{crashCountRecord(workload.RetryBlockHeld, 3, crashCountAt(-50*time.Minute), crashCountAt(-40*time.Minute), nil)},
		retry:   crashCountRetry,
	}
}

// countedRecordWorld is a record in the state given whose wave already
// counts the crash the row remembers.
func countedRecordWorld(state workload.RetryBlockState, next *metav1.Time) crashCountWorld {
	return crashCountWorld{
		policy:  workload.RestartPolicyRecreateInstance,
		row:     crashCountRow(crashCountAt(-time.Minute)),
		pods:    []*corev1.Pod{crashCountCrashingPod()},
		records: []workload.RetryBlock{crashCountRecord(state, 1, crashCountAt(-2*time.Minute), crashCountAt(-time.Minute), next)},
		retry:   crashCountRetry,
	}
}

// firstCrashWorld is a first crash under the schedule given, no record yet.
func firstCrashWorld(retry *workload.RetryPolicy) crashCountWorld {
	return crashCountWorld{
		policy: workload.RestartPolicyRecreateInstance,
		row:    crashCountRow(nil),
		pods:   []*corev1.Pod{crashCountCrashingPod()},
		retry:  retry,
	}
}

func with(w crashCountWorld, edit func(*crashCountWorld)) crashCountWorld {
	edit(&w)
	return w
}

var crashCountRules = []crashCountRule{
	{
		name:  "a set that serves on without a restart records nothing",
		world: with(firstCrashWorld(crashCountRetry), func(w *crashCountWorld) { w.pods = []*corev1.Pod{crashCountSteadyPod()} }),
		want:  crashCountAnswer{journal: []string{}},
	},
	{
		name:  "a first crash is counted on the record, then remembered on the row, and waits out the backoff",
		world: firstCrashWorld(crashCountRetry),
		want:  crashCountAnswer{held: true, journal: []string{firstCountLine, rememberedNowLine}},
	},
	{
		name:  "a crash remembered before the record counted it is dated again at the pass that counts it",
		world: with(firstCrashWorld(crashCountRetry), func(w *crashCountWorld) { w.row = crashCountRow(crashCountAt(-40 * time.Minute)) }),
		want:  crashCountAnswer{held: true, journal: []string{firstCountLine, rememberedNowLine}},
	},
	{
		name:  "a first crash that spends the last attempt holds the revision and writes the row Failed with one warning",
		world: firstCrashWorld(crashCountLastAttempt),
		want:  crashCountAnswer{held: true, journal: []string{lastCountLine, rememberedNowLine, failedLine, failedWarnedLine, lastAttemptWarning}},
	},
	{
		name:  "a crash on a held revision writes the row Failed once its set is down and warns once",
		world: heldRecordWorld(crashCountCrashingPod()),
		want:  crashCountAnswer{held: true, journal: []string{failedLine, failedWarnedLine, heldWarning}},
	},
	{
		name:  "the warning is owed once per pod set",
		world: with(heldRecordWorld(crashCountCrashingPod()), func(w *crashCountWorld) { w.warned = true }),
		want:  crashCountAnswer{held: true, journal: []string{"row 0 Failed operation=false failure=Error on example-engine-0-0 at 04:26:07 noted=false warned=true"}},
	},
	{
		name:  "without a warning sink the hold still writes the row Failed and records the warning as delivered",
		world: with(heldRecordWorld(crashCountCrashingPod()), func(w *crashCountWorld) { w.noWarningSink = true }),
		want:  crashCountAnswer{held: true, journal: []string{failedLine, failedWarnedLine}},
	},
	{
		name:  "a held revision whose set serves again is named on the row, not written Failed",
		world: heldRecordWorld(crashCountServingAgainPod()),
		want:  crashCountAnswer{held: true, journal: []string{namedLine}},
	},
	{
		name: "a crash no repair answers is remembered and holds the row",
		world: with(firstCrashWorld(crashCountRetry), func(w *crashCountWorld) {
			w.policy = workload.RestartPolicyNone
		}),
		want: crashCountAnswer{held: true, journal: []string{rememberedNowLine}},
	},
	{
		name: "a crash no repair answers leaves a due record as it is",
		world: with(firstCrashWorld(crashCountRetry), func(w *crashCountWorld) {
			w.policy = workload.RestartPolicyNone
			w.records = []workload.RetryBlock{crashCountRecord(workload.RetryBlockBackoff, 1, crashCountAt(-2*time.Minute), crashCountAt(-time.Minute), crashCountAt(-time.Second))}
		}),
		want: crashCountAnswer{held: true, journal: []string{rememberedNowLine}},
	},
	{
		name:  "a record in backoff not due holds the row until it is",
		world: countedRecordWorld(workload.RetryBlockBackoff, crashCountAt(30*time.Second)),
		want:  crashCountHolds,
	},
	{
		name:  "a record due admits the rebuild and names the revision",
		world: countedRecordWorld(workload.RetryBlockBackoff, crashCountAt(-time.Second)),
		want:  crashCountAdmits,
	},
	{
		name: "an attempt in flight at the revision holds the rebuild",
		world: with(countedRecordWorld(workload.RetryBlockRetryInProgress, nil), func(w *crashCountWorld) {
			w.peers = []workload.InstanceStatus{{Index: 1, Incarnation: 2, Phase: workload.InstancePhaseRestarting, RunningRevision: crashCountRevision}}
		}),
		want: crashCountHolds,
	},
	{
		name:  "an attempt the record admits goes on when nothing else is in flight",
		world: countedRecordWorld(workload.RetryBlockRetryInProgress, nil),
		want:  crashCountAdmits,
	},
	{
		name:  "a record write that fails stops the count",
		world: with(firstCrashWorld(crashCountRetry), func(w *crashCountWorld) { w.refuseRecordWrite = true }),
		want:  crashCountAnswer{err: []string{"count crash on retry", "_", "(instance=0, rev=example-engine-rev1): record write refused"}, journal: []string{}},
	},
	{
		name:  "a row write that fails while remembering stops the count",
		world: with(firstCrashWorld(crashCountRetry), func(w *crashCountWorld) { w.refuseRowWrite = 1 }),
		want:  crashCountAnswer{err: []string{"remember crash on the row (instance=0): row write refused"}, journal: []string{firstCountLine}},
	},
	{
		name:  "a row write that fails while naming the loop stops the count",
		world: with(heldRecordWorld(crashCountServingAgainPod()), func(w *crashCountWorld) { w.refuseRowWrite = 1 }),
		want:  crashCountAnswer{err: []string{"name the crash loop on the row (instance=0): row write refused"}, journal: []string{}},
	},
	{
		name:  "a row write that fails while writing Failed stops the count",
		world: with(heldRecordWorld(crashCountCrashingPod()), func(w *crashCountWorld) { w.refuseRowWrite = 1 }),
		want:  crashCountAnswer{err: []string{"_", "Failed under held retry", "_", "(instance=0): row write refused"}, journal: []string{}},
	},
	{
		name:  "a row write that fails while recording the warning stops the count after the Failed write",
		world: with(heldRecordWorld(crashCountCrashingPod()), func(w *crashCountWorld) { w.refuseRowWrite = 2 }),
		want:  crashCountAnswer{err: []string{"_", "the", "_", "under held retry", "_", "(instance=0): row write refused"}, journal: []string{failedLine}},
	},
}

func TestCrashCountRules(t *testing.T) {
	for _, rule := range crashCountRules {
		t.Run(rule.name, func(t *testing.T) {
			held, rev, err, journal := rule.world.count(t)
			errText := ""
			if err != nil {
				errText = err.Error()
			}
			switch {
			case len(rule.want.err) == 0 && err != nil:
				t.Errorf("error = %q, want none", errText)
			case len(rule.want.err) > 0 && !wordsAround(errText, rule.want.err):
				t.Errorf("error = %q, want the pieces %q", errText, rule.want.err)
			}
			if held != rule.want.held || rev != rule.want.rev {
				t.Errorf("answer = (held %v, revision %q), want (held %v, revision %q)", held, rev, rule.want.held, rule.want.rev)
			}
			if diff := cmp.Diff(rule.want.journal, journal); diff != "" {
				t.Errorf("writes (-want +got):\n%s", diff)
			}
		})
	}
}
