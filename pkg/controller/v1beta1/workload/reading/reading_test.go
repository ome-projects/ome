package reading_test

import (
	"flag"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/reading"
	fixture "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/testfixture"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
)

// now is the pass's clock in every test.
var now = fixture.Epoch

const podName = "engine-0-default-0"

// base is the Context most cases read under: the pass's clock, a one-minute
// unready grace, no availability window, the pod in rotation, no pairing
// transition.
func base() reading.Context {
	return reading.Context{
		Now:          now,
		UnreadyGrace: time.Minute,
		Rotation:     reading.Rotation{podName: {}},
	}
}

type levelCase struct {
	name string
	pod  *corev1.Pod
	ctx  reading.Context
	want bool
}

func runLevel(t *testing.T, level string, get func(reading.PodReading) bool, cases []levelCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := get(reading.ReadPod(tc.pod, tc.ctx)); got != tc.want {
				t.Fatalf("%s = %v, want %v", level, got, tc.want)
			}
		})
	}
}

func TestPodReading_Live(t *testing.T) {
	runLevel(t, "Live", func(p reading.PodReading) bool { return p.Live }, []levelCase{
		{name: "serving pod", pod: fixture.Pod(podName), ctx: base(), want: true},
		{name: "pod still coming up", pod: fixture.Pod(podName, fixture.NeverReady), ctx: base(), want: true},
		{name: "pending pod", pod: fixture.Pod(podName, fixture.Pending), ctx: base(), want: true},
		{name: "being deleted", pod: fixture.Pod(podName, fixture.Terminating), ctx: base(), want: false},
		{name: "failed", pod: fixture.Pod(podName, fixture.Failed), ctx: base(), want: false},
		{name: "succeeded", pod: fixture.Pod(podName, fixture.Succeeded), ctx: base(), want: false},
	})
}

func TestPodReading_RuntimeReady(t *testing.T) {
	runLevel(t, "RuntimeReady", func(p reading.PodReading) bool { return p.RuntimeReady }, []levelCase{
		{name: "containers ready", pod: fixture.Pod(podName), ctx: base(), want: true},
		{name: "containers ready, gate not written yet", pod: fixture.Pod(podName, fixture.GateAbsent, fixture.GateNotFolded), ctx: base(), want: true},
		{name: "containers unready", pod: fixture.Pod(podName, fixture.UnreadySince(now.Add(-time.Second))), ctx: base(), want: false},
		{name: "being deleted with containers ready", pod: fixture.Pod(podName, fixture.Terminating), ctx: base(), want: false},
		{name: "failed with stale conditions", pod: fixture.Pod(podName, fixture.Failed), ctx: base(), want: false},
	})
}

func TestPodReading_Serving(t *testing.T) {
	runLevel(t, "Serving", func(p reading.PodReading) bool { return p.Serving }, []levelCase{
		{name: "PodReady and the gate", pod: fixture.Pod(podName), ctx: base(), want: true},
		{name: "gate condition absent, PodReady true", pod: fixture.Pod(podName, fixture.GateAbsent), ctx: base(), want: false},
		{name: "gate written, PodReady not folded yet", pod: fixture.Pod(podName, fixture.GateNotFolded), ctx: base(), want: false},
		{name: "gate held by a drain", pod: fixture.Pod(podName, fixture.GateHeldBy(podreadiness.WriterUpdateInPlace, "0-1")), ctx: base(), want: false},
		{name: "node lost under a serving pod", pod: fixture.Pod(podName, fixture.NodeLost), ctx: base(), want: false},
		{name: "being deleted", pod: fixture.Pod(podName, fixture.Terminating), ctx: base(), want: false},
		{name: "failed with stale conditions", pod: fixture.Pod(podName, fixture.Failed), ctx: base(), want: false},
	})
}

func TestPodReading_Routed(t *testing.T) {
	noRotation := base()
	noRotation.Rotation = nil
	runLevel(t, "Routed", func(p reading.PodReading) bool { return p.Routed }, []levelCase{
		{name: "routable pod in rotation", pod: fixture.Pod(podName), ctx: base(), want: true},
		{name: "Ready but not in rotation", pod: fixture.Pod(podName), ctx: noRotation, want: false},
		{name: "worker in the set", pod: fixture.Pod(podName, fixture.AsWorker), ctx: base(), want: false},
		{name: "no revision hash", pod: fixture.Pod(podName, fixture.NoRevision), ctx: base(), want: false},
		{name: "being deleted but still listed", pod: fixture.Pod(podName, fixture.Terminating), ctx: base(), want: false},
		{name: "in rotation while the gate is held", pod: fixture.Pod(podName, fixture.GateHeldBy(podreadiness.WriterUpdateInPlace, "0-1")), ctx: base(), want: true},
	})
}

func TestPodReading_Available(t *testing.T) {
	window := func(d time.Duration) reading.Context {
		ctx := base()
		ctx.AvailabilityWindow = d
		return ctx
	}
	runLevel(t, "Available", func(p reading.PodReading) bool { return p.Available }, []levelCase{
		{name: "no window: same as Serving", pod: fixture.Pod(podName), ctx: window(0), want: true},
		{name: "no window, not serving", pod: fixture.Pod(podName, fixture.NodeLost), ctx: window(0), want: false},
		{name: "Ready for an hour, ten-minute window", pod: fixture.Pod(podName), ctx: window(10 * time.Minute), want: true},
		{name: "Ready for a minute, ten-minute window", pod: fixture.Pod(podName, fixture.ReadySince(now.Add(-time.Minute))), ctx: window(10 * time.Minute), want: false},
		{name: "Ready exactly a window ago", pod: fixture.Pod(podName, fixture.ReadySince(now.Add(-10*time.Minute))), ctx: window(10 * time.Minute), want: true},
		{name: "Ready with no transition time", pod: fixture.Pod(podName, fixture.ReadyWithoutTransitionTime), ctx: window(10 * time.Minute), want: false},
		{name: "serving gate absent", pod: fixture.Pod(podName, fixture.GateAbsent), ctx: window(10 * time.Minute), want: false},
	})
}

func TestPodReading_AvailableMatchesPodreadinessRule(t *testing.T) {
	for _, window := range []time.Duration{0, 30 * time.Minute, 2 * time.Hour} {
		for _, pod := range fixture.Pods(
			fixture.Pod(podName),
			fixture.Pod(podName, fixture.ReadyWithoutTransitionTime),
			fixture.Pod(podName, fixture.NodeLost),
			fixture.Pod(podName, fixture.ReadySince(now.Add(-time.Hour))),
		) {
			ctx := base()
			ctx.AvailabilityWindow = window
			got := reading.ReadPod(pod, ctx).Available
			available, _ := podreadiness.IsPodAvailable(pod, int32(window/time.Second), now)
			if want := podreadiness.ReadyAndServing(pod) && available; got != want {
				t.Fatalf("window %s: Available = %v, podreadiness rule says %v", window, got, want)
			}
		}
	}
}

func TestPodReading_Pairable(t *testing.T) {
	pairing := func(inFlight bool, peer map[string]int32) reading.Context {
		ctx := base()
		ctx.Pairing = reading.Pairing{TransitionInFlight: inFlight, PeerServingByProtocol: peer}
		return ctx
	}
	runLevel(t, "Pairable", func(p reading.PodReading) bool { return p.Pairable }, []levelCase{
		{name: "no transition in flight", pod: fixture.Pod(podName, fixture.WithPairingProtocol("v1")), ctx: pairing(false, nil), want: true},
		{name: "transition, pod has no protocol", pod: fixture.Pod(podName), ctx: pairing(true, map[string]int32{"v2": 1}), want: true},
		{name: "transition, peer serves the same cohort", pod: fixture.Pod(podName, fixture.WithPairingProtocol("v1")), ctx: pairing(true, map[string]int32{"v1": 1}), want: true},
		{name: "transition, peer serves the empty cohort", pod: fixture.Pod(podName, fixture.WithPairingProtocol("v1")), ctx: pairing(true, map[string]int32{"": 2}), want: true},
		{name: "transition, peer serves another cohort only", pod: fixture.Pod(podName, fixture.WithPairingProtocol("v1")), ctx: pairing(true, map[string]int32{"v2": 1}), want: false},
		{name: "transition, peer cohort count is zero", pod: fixture.Pod(podName, fixture.WithPairingProtocol("v1")), ctx: pairing(true, map[string]int32{"v1": 0}), want: false},
		{name: "transition, no peer serving", pod: fixture.Pod(podName, fixture.WithPairingProtocol("v1")), ctx: pairing(true, nil), want: false},
		{name: "not serving", pod: fixture.Pod(podName, fixture.WithPairingProtocol("v1"), fixture.NodeLost), ctx: pairing(false, nil), want: false},
	})
}

func TestPodReading_KeepsTheShape(t *testing.T) {
	p := reading.ReadPod(fixture.Pod(podName, fixture.AsLeader, fixture.WithPairingProtocol("v1"), fixture.OnRevision(fixture.RevisionHashB)), base())
	if p.Name != podName || p.Protocol != "v1" || p.RevisionHash != fixture.RevisionHashB || p.Runner != types.RunnerLeader || p.Worker {
		t.Fatalf("labels not kept: %+v", p)
	}
	if p.Terminal || p.Terminating || p.SurgeDraining || p.HeldIn != "" {
		t.Fatalf("healthy pod reads a lifecycle flag: %+v", p)
	}
	if w := reading.ReadPod(fixture.Pod(podName, fixture.AsWorker), base()); !w.Worker {
		t.Fatalf("worker not read as one: %+v", w)
	}
	if f := reading.ReadPod(fixture.Pod(podName, fixture.Failed), base()); !f.Terminal || f.Terminating {
		t.Fatalf("failed pod: %+v", f)
	}
	if d := reading.ReadPod(fixture.Pod(podName, fixture.Terminating), base()); !d.Terminating || d.Terminal {
		t.Fatalf("deleting pod: %+v", d)
	}
	if s := reading.ReadPod(fixture.Pod(podName, fixture.GateHeldBy(podreadiness.WriterUpdateSurgeDrain, "0-1")), base()); !s.SurgeDraining {
		t.Fatalf("surge-drained source: %+v", s)
	}
	if o := reading.ReadPod(fixture.Pod(podName, fixture.GateHeldBy(podreadiness.WriterUpdateInPlace, "0-1")), base()); o.SurgeDraining {
		t.Fatalf("in-place drain read as the surge drain: %+v", o)
	}
	if c := reading.ReadPod(fixture.Pod(podName, fixture.ContainerWaiting("CrashLoopBackOff")), base()); c.HeldIn != "CrashLoopBackOff" {
		t.Fatalf("held pod: %+v", c)
	}
}

func TestPodReading_OutOfService(t *testing.T) {
	noGrace := base()
	noGrace.UnreadyGrace = 0
	for _, tc := range []struct {
		name                       string
		pod                        *corev1.Pod
		ctx                        reading.Context
		out                        bool
		unready, containersUnready bool
	}{
		{name: "serving", pod: fixture.Pod(podName), ctx: base()},
		{name: "held in CrashLoopBackOff", pod: fixture.Pod(podName, fixture.ContainerWaiting("CrashLoopBackOff")), ctx: base(), out: true, unready: true, containersUnready: true},
		{name: "held, no grace configured", pod: fixture.Pod(podName, fixture.ContainerWaiting("ImagePullBackOff")), ctx: noGrace, out: true},
		{name: "served, then unready past the grace", pod: fixture.Pod(podName, fixture.UnreadySince(now.Add(-2*time.Minute))), ctx: base(), out: true, unready: true, containersUnready: true},
		{name: "served, then unready inside the grace", pod: fixture.Pod(podName, fixture.UnreadySince(now.Add(-30*time.Second))), ctx: base()},
		{name: "unready past the grace, no grace configured", pod: fixture.Pod(podName, fixture.UnreadySince(now.Add(-2*time.Minute))), ctx: noGrace},
		{name: "never promoted, unready past the grace", pod: fixture.Pod(podName, fixture.NeverReady), ctx: base(), containersUnready: true},
		{name: "drained, then unready past the grace", pod: fixture.Pod(podName, fixture.GateHeldBy(podreadiness.WriterUpdateInPlace, "0-1"), fixture.UnreadySince(now.Add(-2*time.Minute))), ctx: base(), containersUnready: true},
		{name: "node lost", pod: fixture.Pod(podName, fixture.NodeLost), ctx: base()},
		{name: "held but being deleted", pod: fixture.Pod(podName, fixture.ContainerWaiting("CrashLoopBackOff"), fixture.Terminating), ctx: base()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := reading.ReadPod(tc.pod, tc.ctx)
			if p.OutOfService() != tc.out || p.UnreadyPastGrace != tc.unready || p.ContainersUnreadyPastGrace != tc.containersUnready {
				t.Fatalf("OutOfService=%v UnreadyPastGrace=%v ContainersUnreadyPastGrace=%v, want %v %v %v",
					p.OutOfService(), p.UnreadyPastGrace, p.ContainersUnreadyPastGrace, tc.out, tc.unready, tc.containersUnready)
			}
		})
	}
}

// TestPodReading_CreateStillInFlight_NotOutOfService checks the reading
// against the engine's terminal waiting set: a CreateContainerError for a
// create the runtime has not finished leaves a pod that never served in
// service, a refused create takes it out.
func TestPodReading_CreateStillInFlight_NotOutOfService(t *testing.T) {
	waiting := func(message string) *corev1.Pod {
		pod := fixture.Pod(podName, fixture.NeverReady, fixture.ContainerWaiting("CreateContainerError"))
		pod.Status.ContainerStatuses[0].State.Waiting.Message = message
		return pod
	}
	if reading.ReadPod(waiting("context deadline exceeded"), base()).OutOfService() {
		t.Errorf("a create still in flight reads out of service, want in service")
	}
	if !reading.ReadPod(waiting("failed to create containerd container: error unpacking image: no space left on device"), base()).OutOfService() {
		t.Errorf("a refused create reads in service, want out of service")
	}
}

func TestRead_Counts(t *testing.T) {
	ctx := base()
	ctx.AvailabilityWindow = 10 * time.Minute
	ctx.Rotation = reading.Rotation{"a": {}, "b": {}, "e": {}}
	ctx.Pairing = reading.Pairing{TransitionInFlight: true, PeerServingByProtocol: map[string]int32{"v1": 1}}
	r := reading.Read(fixture.Pods(
		fixture.Pod("a", fixture.WithPairingProtocol("v1")),                                            // every level
		fixture.Pod("b", fixture.WithPairingProtocol("v2"), fixture.ReadySince(now.Add(-time.Minute))), // serving, routed, not available, not pairable
		fixture.Pod("c", fixture.GateNotFolded),                                                        // live, runtime ready only, not in rotation
		fixture.Pod("d", fixture.NeverReady),                                                           // live only
		fixture.Pod("e", fixture.Terminating),                                                          // nothing
		nil,
		fixture.Pod("f", fixture.Failed), // nothing
	), 2, ctx)
	want := reading.Counts{Pods: 6, Live: 4, RuntimeReady: 3, Serving: 2, Routed: 2, Available: 1, Pairable: 1}
	if r.Counts != want {
		t.Fatalf("Counts = %+v, want %+v", r.Counts, want)
	}
	if len(r.Pods) != 6 || r.Desired != 2 {
		t.Fatalf("Read kept %d readings for desired %d", len(r.Pods), r.Desired)
	}
}

type verdictCase struct {
	name    string
	pods    []*corev1.Pod
	desired int32
	want    bool
}

func runVerdict(t *testing.T, verdict string, ctx reading.Context, get func(reading.InstanceReading) bool, cases []verdictCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := get(reading.Read(tc.pods, tc.desired, ctx)); got != tc.want {
				t.Fatalf("%s = %v, want %v", verdict, got, tc.want)
			}
		})
	}
}

func TestInstanceReading_FullyServing(t *testing.T) {
	surgeSource := fixture.Pod("engine-0-default-1", fixture.GateHeldBy(podreadiness.WriterUpdateSurgeDrain, "0-1"))
	runVerdict(t, "FullyServing", base(), reading.InstanceReading.FullyServing, []verdictCase{
		{name: "two serving pods, two desired", pods: fixture.Pods(fixture.Pod("a"), fixture.Pod("b")), desired: 2, want: true},
		{name: "two serving pods, three desired", pods: fixture.Pods(fixture.Pod("a"), fixture.Pod("b")), desired: 3},
		{name: "one pod not serving", pods: fixture.Pods(fixture.Pod("a"), fixture.Pod("b", fixture.NodeLost)), desired: 2},
		{name: "gate written, PodReady not folded yet", pods: fixture.Pods(fixture.Pod("a", fixture.GateNotFolded)), desired: 1},
		{name: "terminal pod beside a serving one", pods: fixture.Pods(fixture.Pod("a"), fixture.Pod("b", fixture.Failed)), desired: 1},
		{name: "deleting pod beside a serving one", pods: fixture.Pods(fixture.Pod("a"), fixture.Pod("b", fixture.Terminating)), desired: 1, want: true},
		{name: "surge source draining beside its serving replacement", pods: fixture.Pods(fixture.Pod(podName), surgeSource), desired: 1, want: true},
		{name: "in-place drained pod beside a serving one", pods: fixture.Pods(fixture.Pod("a"), fixture.Pod("b", fixture.GateHeldBy(podreadiness.WriterUpdateInPlace, "0-1"))), desired: 1},
		{name: "serving worker beside a serving leader", pods: fixture.Pods(fixture.Pod("a", fixture.AsLeader), fixture.Pod("b", fixture.AsWorker)), desired: 2, want: true},
		{name: "desired zero", pods: fixture.Pods(fixture.Pod("a")), desired: 0},
		{name: "no pods", pods: nil, desired: 1},
	})
}

func TestInstanceReading_ServesNothing(t *testing.T) {
	held := fixture.Pod("a", fixture.ContainerWaiting("CrashLoopBackOff"))
	runVerdict(t, "ServesNothing", base(), reading.InstanceReading.ServesNothing, []verdictCase{
		{name: "held pod", pods: fixture.Pods(held), desired: 1, want: true},
		{name: "terminal pod alone", pods: fixture.Pods(fixture.Pod("a", fixture.Failed)), desired: 1, want: true},
		{name: "held beside terminal", pods: fixture.Pods(held, fixture.Pod("b", fixture.Failed)), desired: 2, want: true},
		{name: "served, then unready past the grace", pods: fixture.Pods(fixture.Pod("a", fixture.UnreadySince(now.Add(-2*time.Minute)))), desired: 1, want: true},
		{name: "served, then unready inside the grace", pods: fixture.Pods(fixture.Pod("a", fixture.UnreadySince(now.Add(-30*time.Second)))), desired: 1},
		{name: "serving pod", pods: fixture.Pods(fixture.Pod("a")), desired: 1},
		{name: "held beside serving", pods: fixture.Pods(held, fixture.Pod("b")), desired: 2},
		{name: "still coming up", pods: fixture.Pods(fixture.Pod("a", fixture.NeverReady)), desired: 1},
		{name: "node lost under a serving pod", pods: fixture.Pods(fixture.Pod("a", fixture.NodeLost)), desired: 1},
		{name: "held leader beside a serving worker", pods: fixture.Pods(fixture.Pod("a", fixture.AsLeader, fixture.ContainerWaiting("CrashLoopBackOff")), fixture.Pod("b", fixture.AsWorker)), desired: 2, want: true},
		{name: "serving leader beside a held worker", pods: fixture.Pods(fixture.Pod("a", fixture.AsLeader), fixture.Pod("b", fixture.AsWorker, fixture.ContainerWaiting("CrashLoopBackOff"))), desired: 2},
		{name: "only a serving worker is left", pods: fixture.Pods(fixture.Pod("b", fixture.AsWorker)), desired: 2, want: true},
		{name: "only a deleting pod", pods: fixture.Pods(fixture.Pod("a", fixture.ContainerWaiting("CrashLoopBackOff"), fixture.Terminating)), desired: 1},
		{name: "no pods", pods: nil, desired: 1},
	})
}

func TestInstanceReading_OutOfRotation(t *testing.T) {
	runVerdict(t, "OutOfRotation", base(), reading.InstanceReading.OutOfRotation, []verdictCase{
		{name: "still coming up", pods: fixture.Pods(fixture.Pod("a", fixture.NeverReady)), desired: 1, want: true},
		{name: "served, then unready inside the grace", pods: fixture.Pods(fixture.Pod("a", fixture.UnreadySince(now.Add(-time.Second)))), desired: 1, want: true},
		{name: "terminal pod alone", pods: fixture.Pods(fixture.Pod("a", fixture.Failed)), desired: 1, want: true},
		{name: "only a serving worker is left", pods: fixture.Pods(fixture.Pod("b", fixture.AsWorker)), desired: 2, want: true},
		{name: "serving pod", pods: fixture.Pods(fixture.Pod("a")), desired: 1},
		{name: "one serving beside one coming up", pods: fixture.Pods(fixture.Pod("a"), fixture.Pod("b", fixture.NeverReady)), desired: 2},
		{name: "only a deleting pod", pods: fixture.Pods(fixture.Pod("a", fixture.Terminating)), desired: 1},
		{name: "no pods", pods: nil, desired: 1},
	})
}

func TestInstanceReading_ContainersUnreadyPastGrace(t *testing.T) {
	unreadyFor := fixture.UnreadySince(now.Add(-2 * time.Minute))
	runVerdict(t, "ContainersUnreadyPastGrace", base(), reading.InstanceReading.ContainersUnreadyPastGrace, []verdictCase{
		{name: "served, then unready past the grace", pods: fixture.Pods(fixture.Pod("a", unreadyFor)), desired: 1, want: true},
		{name: "drained, then unready past the grace", pods: fixture.Pods(fixture.Pod("a", fixture.GateHeldBy(podreadiness.WriterUpdateInPlace, "0-1"), unreadyFor)), desired: 1, want: true},
		{name: "never promoted, unready past the grace", pods: fixture.Pods(fixture.Pod("a", fixture.NeverReady)), desired: 1, want: true},
		{name: "unready inside the grace", pods: fixture.Pods(fixture.Pod("a", fixture.UnreadySince(now.Add(-time.Second)))), desired: 1},
		{name: "serving pod", pods: fixture.Pods(fixture.Pod("a")), desired: 1},
		{name: "one unready past the grace beside one serving", pods: fixture.Pods(fixture.Pod("a", unreadyFor), fixture.Pod("b")), desired: 2},
		{name: "leader unready past the grace beside a serving worker", pods: fixture.Pods(fixture.Pod("a", fixture.AsLeader, unreadyFor), fixture.Pod("b", fixture.AsWorker)), desired: 2, want: true},
		{name: "serving leader beside a worker unready past the grace", pods: fixture.Pods(fixture.Pod("a", fixture.AsLeader), fixture.Pod("b", fixture.AsWorker, unreadyFor)), desired: 2},
		{name: "terminal pod alone", pods: fixture.Pods(fixture.Pod("a", fixture.Failed)), desired: 1},
		{name: "only a worker, unready past the grace", pods: fixture.Pods(fixture.Pod("b", fixture.AsWorker, unreadyFor)), desired: 2},
		{name: "no pods", pods: nil, desired: 1},
	})
}

func TestInstanceReading_UnavailableOnHeldMember(t *testing.T) {
	held := fixture.Pod("b", fixture.AsWorker, fixture.ContainerWaiting("CrashLoopBackOff"))
	runVerdict(t, "UnavailableOnHeldMember", base(), reading.InstanceReading.UnavailableOnHeldMember, []verdictCase{
		{name: "one serving, one held, two desired", pods: fixture.Pods(fixture.Pod("a", fixture.AsLeader), held), desired: 2, want: true},
		{name: "one serving, one unready past the grace, two desired", pods: fixture.Pods(fixture.Pod("a", fixture.AsLeader), fixture.Pod("b", fixture.AsWorker, fixture.UnreadySince(now.Add(-2*time.Minute)))), desired: 2, want: true},
		{name: "one serving, one held, one desired", pods: fixture.Pods(fixture.Pod("a", fixture.AsLeader), held), desired: 1},
		{name: "one serving, one coming up, two desired", pods: fixture.Pods(fixture.Pod("a", fixture.AsLeader), fixture.Pod("b", fixture.AsWorker, fixture.NeverReady)), desired: 2},
		{name: "one serving, one unready inside the grace", pods: fixture.Pods(fixture.Pod("a", fixture.AsLeader), fixture.Pod("b", fixture.AsWorker, fixture.UnreadySince(now.Add(-time.Second)))), desired: 2},
		{name: "one serving, one terminal, two desired", pods: fixture.Pods(fixture.Pod("a", fixture.AsLeader), fixture.Pod("b", fixture.AsWorker, fixture.Failed)), desired: 2},
		{name: "one serving, one held but deleting", pods: fixture.Pods(fixture.Pod("a", fixture.AsLeader), fixture.Pod("b", fixture.AsWorker, fixture.ContainerWaiting("CrashLoopBackOff"), fixture.Terminating)), desired: 2},
		{name: "both serving", pods: fixture.Pods(fixture.Pod("a", fixture.AsLeader), fixture.Pod("b", fixture.AsWorker)), desired: 2},
	})
}

func TestInstanceReading_PairableServing(t *testing.T) {
	ctx := base()
	ctx.Pairing = reading.Pairing{TransitionInFlight: true, PeerServingByProtocol: map[string]int32{"v1": 1}}
	r := reading.Read(fixture.Pods(
		fixture.Pod("a", fixture.WithPairingProtocol("v1")),
		fixture.Pod("b", fixture.WithPairingProtocol("v2")),
		fixture.Pod("c"),
		fixture.Pod("d", fixture.WithPairingProtocol("v1"), fixture.NodeLost),
	), 4, ctx)
	if got := r.PairableServing(); got != 2 {
		t.Fatalf("PairableServing = %d, want 2", got)
	}
	ctx.Pairing.TransitionInFlight = false
	if got := reading.Read(fixture.Pods(fixture.Pod("a", fixture.WithPairingProtocol("v1")), fixture.Pod("b", fixture.WithPairingProtocol("v2"))), 2, ctx).PairableServing(); got != 2 {
		t.Fatalf("PairableServing with no transition = %d, want 2", got)
	}
}

// A pod with no conditions at all, as the apiserver hands out a pod the
// kubelet has not reported on yet.
func TestPodReading_NoConditions(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: podName}, Status: corev1.PodStatus{Phase: corev1.PodPending}}
	p := reading.ReadPod(pod, base())
	if !p.Live || p.RuntimeReady || p.Serving || p.Routed || p.Available || p.Pairable || p.OutOfService() {
		t.Fatalf("bare pending pod: %+v", p)
	}
}

var updateFleetAllocations = flag.Bool("update", false, "re-record testdata/fleet_allocations.json from this run")

// fleetReadCase is one pass's reading of every Instance of a fleet: the
// name the benchmark and the allocation golden share, and the reading.
type fleetReadCase struct {
	name string
	run  func(testing.TB)
}

// fleetReadCases reads every Instance of a fleet once, the way one pass of
// the replica controller classifies its pods, at the benchmark's largest
// shapes: 4,000 single-pod Instances, and 160 gangs of one leader and
// seven workers. Every pod serves and is in rotation, the common steady
// state.
func fleetReadCases() []fleetReadCase {
	shapes := []struct {
		name            string
		instances, pods int
	}{
		{name: "4000x1", instances: 4000, pods: 1},
		{name: "160x8", instances: 160, pods: 8},
	}
	var cases []fleetReadCase
	for _, s := range shapes {
		ctx := reading.Context{
			Now:                now,
			AvailabilityWindow: 30 * time.Second,
			UnreadyGrace:       5 * time.Minute,
			Rotation:           reading.Rotation{},
		}
		pods := make([][]*corev1.Pod, s.instances)
		for i := range pods {
			for ordinal := 0; ordinal < s.pods; ordinal++ {
				name := fmt.Sprintf("svc-engine-%d-%d", i, ordinal)
				var pod *corev1.Pod
				switch {
				case s.pods == 1:
					pod = fixture.ServingPod(name)
				case ordinal == 0:
					pod = fixture.Pod(name, fixture.AsLeader)
				default:
					pod = fixture.Pod(name, fixture.AsWorker)
				}
				pods[i] = append(pods[i], pod)
				ctx.Rotation[name] = struct{}{}
			}
		}
		perInstance := s.pods
		cases = append(cases, fleetReadCase{name: s.name, run: func(tb testing.TB) {
			for i := range pods {
				r := reading.Read(pods[i], int32(perInstance), ctx)
				if int(r.Counts.Serving) != perInstance {
					tb.Fatalf("instance %d: %d of %d pods serving", i, r.Counts.Serving, perInstance)
				}
			}
		}})
	}
	return cases
}

// BenchmarkReadFleet is the cost of one pass's reading per shape.
func BenchmarkReadFleet(b *testing.B) {
	for _, c := range fleetReadCases() {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				c.run(b)
			}
		})
	}
}

// TestFleetReadAllocationsStayWithinBudget holds the allocations of one
// pass's reading per shape to the recorded golden, so a change in the
// reading's cost is made on purpose and carries its reason.
func TestFleetReadAllocationsStayWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "fleet_allocations.json"), *updateFleetAllocations)
	defer golden.Close()
	for _, c := range fleetReadCases() {
		golden.Check(c.name, func() { c.run(t) })
	}
}
