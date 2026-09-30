package holds

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

const provisioningMessage = "capacity is still being provisioned"

// slot names one pod the provisioner is asked about.
type slot struct {
	runner  string
	ordinal int32
}

// fakeProvisioner withholds the slots in pending, explaining with
// message (provisioningMessage when empty), and records every slot the
// hold pass asks about. The pass must only ever ask.
type fakeProvisioner struct {
	pending map[slot]bool
	message string
	err     error
	asked   []slot
	placed  int
}

func (f *fakeProvisioner) Place(context.Context, types.ReconcileInput, types.ComponentPlan, types.InstancePlan, types.RunnerPlan, int32) (map[string]string, bool, error) {
	f.placed++
	return nil, true, nil
}

func (f *fakeProvisioner) Pending(_ context.Context, _ types.ReconcileInput, _ types.ComponentPlan, _ types.InstancePlan, runner types.RunnerPlan, ordinal int32) (string, bool, error) {
	s := slot{runner.Name, ordinal}
	f.asked = append(f.asked, s)
	if f.err != nil {
		return "", false, f.err
	}
	if f.message != "" {
		return f.message, f.pending[s], nil
	}
	return provisioningMessage, f.pending[s], nil
}

// withholding is a provisioner that withholds exactly the given slots.
func withholding(slots ...slot) *fakeProvisioner {
	f := &fakeProvisioner{pending: make(map[slot]bool, len(slots))}
	for _, s := range slots {
		f.pending[s] = true
	}
	return f
}

// provisionFixture is one hold pass over a store with a provisioner
// wired: the planned Instance the rows carry, the pods observed, and the
// clock the pass reads.
type provisionFixture struct {
	instance    func(idx int32) *types.InstancePlan
	provisioner types.Provisioner
	pods        map[int32][]*corev1.Pod
	clock       *clocktesting.FakeClock
}

func (f provisionFixture) run(store *rowStore) (Result, error) {
	in := store.input()
	if f.clock != nil {
		in.Clock = f.clock
	}
	instance := f.instance
	if instance == nil {
		instance = singlePodPlan
	}
	rows := rowsFor(in, 1)
	for i := range rows {
		rows[i].Instance = instance(rows[i].Status.Index)
	}
	return Run(context.Background(), PassInput{
		Deps:  types.Deps{Provisioner: f.provisioner},
		Input: in,
		Plan:  types.ComponentPlan{Component: types.ComponentEngine},
		Rows:  rows,
		Pods: func(context.Context) (map[int32][]*corev1.Pod, error) {
			return f.pods, nil
		},
	})
}

func (f provisionFixture) apply(t *testing.T, store *rowStore) Result {
	t.Helper()
	res, err := f.run(store)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	return res
}

// presentPod is a created pod of the given name, in whatever state.
func presentPod(name string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"}}
}

// TestProvisioning_RecordsAndReleasesTheHold: a pod the provisioner
// withholds does not exist, so neither admission nor the scheduler can
// report its wait. The row reports it here — naming the withheld pod and
// the provisioner's explanation — and releases it once the pod exists or
// the capacity is ready. The pass only asks: it never places.
func TestProvisioning_RecordsAndReleasesTheHold(t *testing.T) {
	const target = "svc-a-engine-0-default-0"
	defaultSlot := slot{types.RunnerDefault, 0}

	t.Run("records the wait on a withheld pod", func(t *testing.T) {
		store, _ := newStore(creatingRow(""))
		p := withholding(defaultSlot)
		res := provisionFixture{provisioner: p}.apply(t, store)
		if got := store.waiting(0); got != types.WaitingReasonCapacityProvisioning {
			t.Errorf("waiting = %q, want %q", got, types.WaitingReasonCapacityProvisioning)
		}
		if !res.Holding(0, types.WaitingReasonCapacityProvisioning) {
			t.Error("the result must report the hold the escalation pass reads")
		}
		lf := store.lastFailure(0)
		if lf == nil || lf.Reason != types.WaitingReasonCapacityProvisioning || lf.PodName != target ||
			!strings.Contains(lf.Message, provisioningMessage) {
			t.Errorf("lastFailure = %+v, want the withheld pod and the provisioner's explanation", lf)
		}
		if p.placed != 0 {
			t.Errorf("placed = %d, want 0: the hold pass must not change the cluster", p.placed)
		}
	})

	t.Run("re-observing the wait writes nothing", func(t *testing.T) {
		clock := clocktesting.NewFakeClock(time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC))
		store, _ := newStore(creatingRow(""))
		fixture := provisionFixture{provisioner: withholding(defaultSlot), clock: clock}
		fixture.apply(t, store)
		first := *store.lastFailure(0)
		clock.Step(time.Minute)
		fixture.apply(t, store)
		if store.writes != 1 {
			t.Errorf("writes = %d, want 1: an unchanged wait costs one write", store.writes)
		}
		if got := store.lastFailure(0); !got.Time.Equal(&first.Time) {
			t.Errorf("lastFailure.Time = %v, want the hold start %v", got.Time, first.Time)
		}
	})

	t.Run("a new explanation is recorded when it is first seen", func(t *testing.T) {
		clock := clocktesting.NewFakeClock(time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC))
		store, _ := newStore(creatingRow(""))
		p := withholding(defaultSlot)
		fixture := provisionFixture{provisioner: p, clock: clock}
		fixture.apply(t, store)
		clock.Step(time.Minute)
		p.message = "capacity failed to form"
		fixture.apply(t, store)
		lf := store.lastFailure(0)
		if lf == nil || !strings.Contains(lf.Message, p.message) {
			t.Fatalf("lastFailure = %+v, want the new explanation", lf)
		}
		if want := metav1.NewTime(clock.Now()); !lf.Time.Equal(&want) {
			t.Errorf("lastFailure.Time = %v, want %v, when the new explanation was first seen", lf.Time, want)
		}
		clock.Step(time.Minute)
		fixture.apply(t, store)
		if store.writes != 2 {
			t.Errorf("writes = %d, want 2: one per explanation", store.writes)
		}
	})

	t.Run("released once the capacity is ready", func(t *testing.T) {
		store, _ := newStore(creatingRow(types.WaitingReasonCapacityProvisioning))
		provisionFixture{provisioner: withholding()}.apply(t, store)
		if got := store.waiting(0); got != "" {
			t.Errorf("waiting = %q, want the token released", got)
		}
	})

	t.Run("a pod that exists is not asked about", func(t *testing.T) {
		store, _ := newStore(creatingRow(types.WaitingReasonCapacityProvisioning))
		p := withholding(defaultSlot)
		provisionFixture{provisioner: p, pods: map[int32][]*corev1.Pod{0: {presentPod(target)}}}.apply(t, store)
		if got := store.waiting(0); got != "" {
			t.Errorf("waiting = %q, want released: whatever holds a created pod is another authority's", got)
		}
		if len(p.asked) != 0 {
			t.Errorf("asked = %v, want nothing: the pod was not withheld", p.asked)
		}
	})

	t.Run("a teardown row is left to DeleteBatch", func(t *testing.T) {
		row := creatingRow("")
		row.Phase = types.InstancePhaseDeleting
		row.Operation = operation("delete-0", types.InstanceOperationDelete, "Drain", "")
		store, _ := newStore(row)
		provisionFixture{provisioner: withholding(defaultSlot)}.apply(t, store)
		if got := store.waiting(0); got != "" {
			t.Errorf("waiting = %q, want none on a row DeleteBatch owns", got)
		}
	})

	t.Run("no provisioner holds nothing and releases what it left", func(t *testing.T) {
		store, _ := newStore(creatingRow(types.WaitingReasonCapacityProvisioning))
		provisionFixture{}.apply(t, store)
		if got := store.waiting(0); got != "" {
			t.Errorf("waiting = %q, want the token released", got)
		}
	})

	t.Run("a row the plan no longer covers is not asked about", func(t *testing.T) {
		store, _ := newStore(creatingRow(types.WaitingReasonCapacityProvisioning))
		p := withholding(defaultSlot)
		provisionFixture{provisioner: p, instance: func(int32) *types.InstancePlan { return nil }}.apply(t, store)
		if got := store.waiting(0); got != "" {
			t.Errorf("waiting = %q, want the token released", got)
		}
		if len(p.asked) != 0 {
			t.Errorf("asked = %v, want nothing: the plan asks for no pod", p.asked)
		}
	})

	t.Run("the provisioner's error fails the pass", func(t *testing.T) {
		store, _ := newStore(creatingRow(""))
		boom := errors.New("slice read failed")
		_, err := provisionFixture{provisioner: &fakeProvisioner{err: boom}}.run(store)
		if !errors.Is(err, boom) {
			t.Fatalf("Run err = %v, want the provisioner's error", err)
		}
		if store.writes != 0 {
			t.Errorf("writes = %d, want 0", store.writes)
		}
	})
}

// TestProvisioning_ASurgeReplacementIsHeldOnlyWhileTheRowIsNotServing:
// a single-pod surge builds its replacement at the ordinal opposite the
// active one, and that is the pod the provisioner withholds. A source
// already in rotation retires the hold, as it retires the scheduler's; a
// source that is not serving leaves the replacement's wait as the row's.
func TestProvisioning_ASurgeReplacementIsHeldOnlyWhileTheRowIsNotServing(t *testing.T) {
	const source, replacement = "svc-a-engine-0-default-0", "svc-a-engine-0-default-1"
	surgeSlot := slot{types.RunnerDefault, 1}

	t.Run("a serving source retires the hold", func(t *testing.T) {
		store, _ := newStore(surgingRow(""))
		p := withholding(surgeSlot)
		provisionFixture{provisioner: p, pods: map[int32][]*corev1.Pod{0: {sourcePod(source, true, false)}}}.apply(t, store)
		if got := store.waiting(0); got != "" {
			t.Errorf("waiting = %q, want none: the row is in rotation", got)
		}
		if len(p.asked) != 1 || p.asked[0] != surgeSlot {
			t.Errorf("asked = %v, want only the replacement %v", p.asked, surgeSlot)
		}
	})

	t.Run("a source out of rotation leaves the replacement's wait", func(t *testing.T) {
		store, _ := newStore(surgingRow(""))
		provisionFixture{
			provisioner: withholding(surgeSlot),
			pods:        map[int32][]*corev1.Pod{0: {sourcePod(source, false, false)}},
		}.apply(t, store)
		if got := store.waiting(0); got != types.WaitingReasonCapacityProvisioning {
			t.Errorf("waiting = %q, want %q", got, types.WaitingReasonCapacityProvisioning)
		}
		if lf := store.lastFailure(0); lf == nil || lf.PodName != replacement {
			t.Errorf("lastFailure = %+v, want the replacement named", lf)
		}
	})
}

// TestProvisioning_AsksAboutEveryMissingMember: a multi-pod Instance
// occupies every ordinal of each Runner, so each member the row lacks is
// asked about, and the first one withheld is the one the row names.
func TestProvisioning_AsksAboutEveryMissingMember(t *testing.T) {
	gang := func(idx int32) *types.InstancePlan {
		return &types.InstancePlan{Index: idx, Runners: []types.RunnerPlan{
			{Name: types.RunnerLeader, Size: 1},
			{Name: types.RunnerWorker, Size: 2},
		}}
	}
	store, _ := newStore(creatingRow(""))
	p := withholding(slot{types.RunnerWorker, 1})
	provisionFixture{
		instance:    gang,
		provisioner: p,
		pods: map[int32][]*corev1.Pod{0: {
			presentPod("svc-a-engine-0-leader-0"),
			presentPod("svc-a-engine-0-worker-0"),
		}},
	}.apply(t, store)
	if want := []slot{{types.RunnerWorker, 1}}; len(p.asked) != 1 || p.asked[0] != want[0] {
		t.Errorf("asked = %v, want only the missing member %v", p.asked, want)
	}
	if lf := store.lastFailure(0); lf == nil || lf.PodName != "svc-a-engine-0-worker-1" {
		t.Errorf("lastFailure = %+v, want the withheld member named", lf)
	}
}
