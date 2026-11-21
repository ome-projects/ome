package coordination

import (
	"context"
	"fmt"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// These tests put the drain gate between two signals of one pod. A roll
// takes an old pod out of rotation by writing its serving gate False. The
// kubelet turns the pod's Ready condition False only on its next report.
// Between the two, the pod reads Ready while it no longer serves. The fleet
// here keeps both conditions on every pod, and its kubelet reports only when
// a test says so.

// lastDrains is one fleet at the moment the last old pods of one side leave
// rotation during a pairing-protocol change from v1 to v2. Every Instance
// of both sides declares none allowed out of rotation.
type lastDrains struct {
	name string
	// engines and decoders are the Instances of each side; workers is the
	// worker count beside each routed pod, 0 for single-pod Instances.
	engines, decoders, workers int
	// leaving is the side whose last old pods are asked about, drains how
	// many of them, each beside a replacement that serves.
	leaving v1beta1.ComponentType
	drains  int
	// partnerless is how many Instances of the other side still run v1
	// alone: their replacements have not started serving.
	partnerless int
}

func (f lastDrains) count(side v1beta1.ComponentType) int {
	if side == v1beta1.EngineComponent {
		return f.engines
	}
	return f.decoders
}

// lastDrainShapes are the counts the pictures are staged on:
// 24, 12 and 30 prefill pods beside 8, 8 and 10 decode pods, and 6 prefill
// gangs beside 2 decode gangs, each gang one leader and seven workers.
func lastDrainShapes() []lastDrains {
	return []lastDrains{
		{name: "24 prefill beside 8 decode, the last 2 decode pods", engines: 24, decoders: 8, leaving: v1beta1.DecoderComponent, drains: 2, partnerless: 6},
		{name: "12 prefill beside 8 decode, the last 2 decode pods", engines: 12, decoders: 8, leaving: v1beta1.DecoderComponent, drains: 2, partnerless: 3},
		{name: "12 prefill beside 8 decode, the last 3 prefill pods", engines: 12, decoders: 8, leaving: v1beta1.EngineComponent, drains: 3, partnerless: 2},
		{name: "30 prefill beside 10 decode, the last 6 prefill pods", engines: 30, decoders: 10, leaving: v1beta1.EngineComponent, drains: 6, partnerless: 1},
		{name: "6 prefill gangs beside 2 decode gangs of 8 pods, the last 2 decode gangs", engines: 6, decoders: 2, workers: 7, leaving: v1beta1.DecoderComponent, drains: 2, partnerless: 5},
	}
}

// rotationPod is a pod as the apiserver holds it once the kubelet has
// reported on it: the labels of drainPod, the serving gate among its
// readiness gates and, when it serves, ContainersReady, the serving gate and
// Ready all True. A pod that does not serve yet carries no serving gate and
// is not Ready.
func rotationPod(comp v1beta1.ComponentType, idx int, runner string, member int, proto string, serves bool) *corev1.Pod {
	name := fmt.Sprintf("llama-%s-%d-%s-%d", comp, idx, runner, member)
	pod := drainPod("llama", comp, name, runner, proto, serves)
	pod.Spec.ReadinessGates = []corev1.PodReadinessGate{{ConditionType: podreadiness.ConditionType}}
	pod.Status.Phase = corev1.PodRunning
	if serves {
		pod.Status.Conditions = append(pod.Status.Conditions,
			corev1.PodCondition{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			corev1.PodCondition{Type: podreadiness.ConditionType, Status: corev1.ConditionTrue})
	}
	return pod
}

// instancePods are the pods of one Instance: a single pod at the given slot
// of its Instance, or a gang's leader and workers, numbered from zero.
func (f lastDrains) instancePods(comp v1beta1.ComponentType, idx, slot int, proto string, serves bool) []client.Object {
	if f.workers == 0 {
		return []client.Object{rotationPod(comp, idx, workloadtypes.RunnerDefault, slot, proto, serves)}
	}
	out := []client.Object{rotationPod(comp, idx, workloadtypes.RunnerLeader, 0, proto, serves)}
	for k := 0; k < f.workers; k++ {
		out = append(out, rotationPod(comp, idx, workloadtypes.RunnerWorker, k, proto, serves))
	}
	return out
}

// side lays out one side: the Instances already on v2, then `old` Instances
// whose v1 pods serve beside a v2 replacement that serves or still starts.
// A single pod's replacement shares its Instance, at the other slot; a
// gang's replacement is a gang of its own at the first free index. It
// returns the pods and, per old Instance, the names of its v1 pods.
func (f lastDrains) side(comp v1beta1.ComponentType, old int, replacementServes bool) (objs []client.Object, oldPods [][]string) {
	count := f.count(comp)
	for idx := 0; idx < count-old; idx++ {
		objs = append(objs, f.instancePods(comp, idx, 1, "v2", true)...)
	}
	for k := 0; k < old; k++ {
		idx := count - old + k
		source := f.instancePods(comp, idx, 0, "v1", true)
		names := make([]string, 0, len(source))
		for _, pod := range source {
			names = append(names, pod.GetName())
		}
		objs = append(objs, source...)
		oldPods = append(oldPods, names)
		replacementIdx := idx
		if f.workers > 0 {
			replacementIdx = count + k
		}
		objs = append(objs, f.instancePods(comp, replacementIdx, 1, "v2", replacementServes)...)
	}
	return objs, oldPods
}

// drainFleet is one lastDrains picture on a fake apiserver.
type drainFleet struct {
	t     *testing.T
	shape lastDrains
	isvc  *v1beta1.InferenceService
	api   client.WithWatch
	// sources are the v1 pods of each old Instance of the leaving side, in
	// the order a pass asks about them.
	sources [][]string
}

func newDrainFleet(t *testing.T, shape lastDrains) *drainFleet {
	t.Helper()
	isvc := pairingISVC("v2")
	objs := []client.Object{
		sizedReplica(isvc, v1beta1.EngineComponent, v1beta1.UpdateStrategySurgeThenDrain, shape.engines, 0),
		sizedReplica(isvc, v1beta1.DecoderComponent, v1beta1.UpdateStrategySurgeThenDrain, shape.decoders, 0),
	}
	leaving, sources := shape.side(shape.leaving, shape.drains, true)
	others, _ := shape.side(pairingPeer(shape.leaving), shape.partnerless, false)
	objs = append(append(objs, leaving...), others...)
	return &drainFleet{
		t: t, shape: shape, isvc: isvc, sources: sources,
		api: fakeclient.New(t, fakeclient.WithObjects(objs...), fakeclient.WithStatusSubresource(&corev1.Pod{})),
	}
}

func (w *drainFleet) pod(name string) *corev1.Pod {
	w.t.Helper()
	pod := &corev1.Pod{}
	if err := w.api.Get(context.Background(), client.ObjectKey{Namespace: w.isvc.Namespace, Name: name}, pod); err != nil {
		w.t.Fatalf("read pod %s: %v", name, err)
	}
	return pod
}

func (w *drainFleet) pods() []corev1.Pod {
	w.t.Helper()
	list := &corev1.PodList{}
	if err := w.api.List(context.Background(), list, client.InNamespace(w.isvc.Namespace)); err != nil {
		w.t.Fatalf("list pods: %v", err)
	}
	return list.Items
}

// askDrain asks the drain gate whether the v1 pods of old Instance i may
// leave rotation.
func (w *drainFleet) askDrain(i int) (bool, string) {
	side := w.shape.leaving
	allowed, _, reason := EvaluatePairingDrain(context.Background(), w.api, w.isvc, side, forwardTarget(w.isvc, side), GroupDefaults{}, w.sources[i])
	return allowed, reason
}

// takeOut writes the serving gate of the named pods False under writer, the
// one write every drain of the roll starts with. Ready is the kubelet's to
// move.
func (w *drainFleet) takeOut(names []string, writer, key string) {
	w.t.Helper()
	for _, name := range names {
		if err := podreadiness.MarkPodNotServing(context.Background(), w.api, w.api, w.pod(name), writer, key); err != nil {
			w.t.Fatalf("take %s out of rotation: %v", name, err)
		}
	}
}

// kubeletReports folds every pod's serving gate into its Ready condition, as
// the kubelet does on its next report of the pod.
func (w *drainFleet) kubeletReports() {
	w.t.Helper()
	for _, item := range w.pods() {
		pod := item
		ready := corev1.ConditionFalse
		if podreadiness.IsContainersReady(&pod) && podreadiness.IsServing(&pod) {
			ready = corev1.ConditionTrue
		}
		for i := range pod.Status.Conditions {
			if cond := &pod.Status.Conditions[i]; cond.Type == corev1.PodReady && cond.Status != ready {
				cond.Status = ready
				if err := w.api.Status().Update(context.Background(), &pod); err != nil {
					w.t.Fatalf("kubelet report of %s: %v", pod.Name, err)
				}
			}
		}
	}
}

// pairable counts the Instances of one side with a routed pod in rotation
// (Ready, the serving gate True, not being deleted) whose protocol a routed
// pod in rotation on the other side speaks.
func (w *drainFleet) pairable(side v1beta1.ComponentType) int {
	speaks := map[v1beta1.ComponentType]map[string]bool{v1beta1.EngineComponent: {}, v1beta1.DecoderComponent: {}}
	instances := map[string]map[string]bool{}
	for _, item := range w.pods() {
		pod := item
		if pod.DeletionTimestamp != nil || !query.RoutedRunner(&pod) || !podreadiness.ReadyAndServing(&pod) {
			continue
		}
		comp := v1beta1.ComponentType(pod.Labels[constants.OMEComponentLabel])
		proto := pod.Labels[query.LabelPairingProtocol]
		speaks[comp][proto] = true
		if comp != side {
			continue
		}
		idx := pod.Labels[query.LabelInstanceIdx]
		if instances[idx] == nil {
			instances[idx] = map[string]bool{}
		}
		instances[idx][proto] = true
	}
	n := 0
	for _, protos := range instances {
		for proto := range protos {
			if speaks[pairingPeer(side)][proto] {
				n++
				break
			}
		}
	}
	return n
}

// requireReadyButOut fails unless every named pod that carries traffic
// reads Ready while its serving gate is False: the state between the roll's
// write and the kubelet's report.
func (w *drainFleet) requireReadyButOut(names []string) {
	w.t.Helper()
	for _, name := range names {
		pod := w.pod(name)
		if !query.RoutedRunner(pod) {
			continue
		}
		if !podreadiness.IsPodReady(pod) || !podreadiness.HeldNotServing(pod) {
			w.t.Fatalf("pod %s: Ready=%v, serving gate held=%v; want Ready with the serving gate False", name, podreadiness.IsPodReady(pod), podreadiness.HeldNotServing(pod))
		}
	}
}

// drainInTurn asks about each old Instance in the order one pass does and
// takes the pods of every drain the gate allows out of rotation. With
// report set the kubelet reports after each drain. Without it, every pod
// taken out earlier must still read Ready with its serving gate False when
// the next is asked about. It returns the first drain the gate kept, or the
// number of drains when it kept none, with the gate's reason.
func (w *drainFleet) drainInTurn(report bool) (kept int, reason string) {
	w.t.Helper()
	for i, source := range w.sources {
		if !report {
			for _, earlier := range w.sources[:i] {
				w.requireReadyButOut(earlier)
			}
		}
		allowed, why := w.askDrain(i)
		if !allowed {
			return i, why
		}
		w.takeOut(source, podreadiness.WriterUpdateSurgeDrain, fmt.Sprintf("drain-%d", i))
		if report {
			w.kubeletReports()
		}
	}
	return len(w.sources), ""
}

// requireLastKept fails unless the gate allowed every drain but the last
// and kept that one for the other side's Instances still on v1, naming how
// many Instances would pair without it.
func (w *drainFleet) requireLastKept(kept int, reason string) {
	w.t.Helper()
	peer := pairingPeer(w.shape.leaving)
	want := fmt.Sprintf("would leave the %s with %d pairable serving Instance(s)", peer, w.shape.count(peer)-w.shape.partnerless)
	if kept != len(w.sources)-1 || !strings.Contains(reason, want) {
		w.t.Fatalf("the gate kept drain %d of %d with reason %q; want the last one kept with %q", kept+1, len(w.sources), reason, want)
	}
}

// requirePairable fails unless that many Instances of the side pair.
func (w *drainFleet) requirePairable(side v1beta1.ComponentType, want int, when string) {
	w.t.Helper()
	if got := w.pairable(side); got != want {
		w.t.Fatalf("%s %d of %d %s Instances pair, want %d", when, got, w.shape.count(side), side, want)
	}
}

// The story on 24 prefill beside 8 decode: 18 prefill and 6 decode Instances
// run v2, the last 6 prefill replacements still start, and the last 2 decode
// replacements serve. One pass asks about both old decode pods in turn and
// takes the first out before it asks about the second. The kubelet has not
// reported yet, so the first pod still reads Ready with its serving gate
// False. The gate does not count a pod taken out of rotation, so it keeps
// the second old decode pod: it is the last partner of the 6 old prefill
// pods, and 18 of 24 prefill Instances would pair without it, under a
// pairable minimum of 24. All 24 keep a decode pod they can pair with. The
// other counts play the same way.
func TestEvaluatePairingDrain_DrainsAskedBeforeTheKubeletReportsTheFirst(t *testing.T) {
	for _, shape := range lastDrainShapes() {
		t.Run(shape.name, func(t *testing.T) {
			fleet := newDrainFleet(t, shape)
			peer := pairingPeer(shape.leaving)
			fleet.requirePairable(peer, shape.count(peer), "before any drain")
			fleet.requireLastKept(fleet.drainInTurn(false))
			fleet.kubeletReports()
			fleet.requirePairable(peer, shape.count(peer), "after the kubelet reports")
		})
	}
}

// The same asks with the kubelet reporting after each drain: every earlier
// old pod reads not Ready by the time the next is asked about. The gate
// gives the same answers: it keeps the last old pod and names how many
// Instances would pair without it.
func TestEvaluatePairingDrain_DrainsAskedAfterTheKubeletReportsEach(t *testing.T) {
	for _, shape := range lastDrainShapes() {
		t.Run(shape.name, func(t *testing.T) {
			fleet := newDrainFleet(t, shape)
			peer := pairingPeer(shape.leaving)
			fleet.requireLastKept(fleet.drainInTurn(true))
			fleet.requirePairable(peer, shape.count(peer), "with the last old pod kept")
		})
	}
}

// Every drain of the roll starts with the same write under its own writer:
// an in-place update, a recreate, a restart, a scale-down, a move and a
// surge. On 6 prefill beside 2 decode the first decode pod is taken out by
// each writer in turn, and the second old decode pod is asked about before
// the kubelet reports. Whatever took the first pod out, the gate does not
// count it and keeps the second, so all 6 prefill Instances keep a partner.
func TestEvaluatePairingDrain_TheFirstPodTakenOutByEachDrainWriter(t *testing.T) {
	shape := lastDrains{engines: 6, decoders: 2, leaving: v1beta1.DecoderComponent, drains: 2, partnerless: 3}
	for _, writer := range []string{
		podreadiness.WriterUpdateInPlace, podreadiness.WriterUpdateRecreateDrain, podreadiness.WriterRestartDrain,
		podreadiness.WriterDeleteDrain, podreadiness.WriterMigrateSourceDrain, podreadiness.WriterUpdateSurgeDrain,
	} {
		t.Run(writer, func(t *testing.T) {
			fleet := newDrainFleet(t, shape)
			fleet.takeOut(fleet.sources[0], writer, "first")
			fleet.requireReadyButOut(fleet.sources[0])
			allowed, reason := fleet.askDrain(1)
			if allowed {
				t.Fatal("the gate let the second old decode pod go")
			}
			fleet.requireLastKept(1, reason)
			fleet.kubeletReports()
			fleet.requirePairable(v1beta1.EngineComponent, 6, "after the kubelet reports")
		})
	}
}

// A new decode pod with no prefill pod of its protocol in rotation has no
// partner. On 6 prefill beside 2 decode the one v2 prefill pod is taken out
// of rotation, and the first old decode pod is asked about before the
// kubelet reports. The gate does not count the v2 prefill pod, so it reads
// the decode replacement as unable to pair yet and keeps the old decode
// pod. Both decode Instances pair after the report.
func TestEvaluatePairingDrain_TheReplacementsOnlyPartnerTakenOutBeforeTheAsk(t *testing.T) {
	isvc := pairingISVC("v2")
	shape := lastDrains{engines: 6, decoders: 2, leaving: v1beta1.DecoderComponent}
	objs := []client.Object{
		sizedReplica(isvc, v1beta1.EngineComponent, v1beta1.UpdateStrategyRecreatePod, 6, 1),
		sizedReplica(isvc, v1beta1.DecoderComponent, v1beta1.UpdateStrategySurgeThenDrain, 2, 0),
	}
	for idx := 0; idx < 6; idx++ {
		proto := "v1"
		if idx == 0 {
			proto = "v2"
		}
		objs = append(objs, shape.instancePods(v1beta1.EngineComponent, idx, 0, proto, true)...)
	}
	for idx := 0; idx < 2; idx++ {
		objs = append(objs, shape.instancePods(v1beta1.DecoderComponent, idx, 0, "v1", true)...)
	}
	objs = append(objs, shape.instancePods(v1beta1.DecoderComponent, 0, 1, "v2", true)...)
	fleet := &drainFleet{
		t: t, shape: shape, isvc: isvc,
		sources: [][]string{{"llama-decoder-0-default-0"}},
		api:     fakeclient.New(t, fakeclient.WithObjects(objs...), fakeclient.WithStatusSubresource(&corev1.Pod{})),
	}
	newPrefill := []string{"llama-engine-0-default-0"}

	fleet.takeOut(newPrefill, podreadiness.WriterRestartDrain, "restart")
	fleet.requireReadyButOut(newPrefill)
	allowed, reason := fleet.askDrain(0)
	if want := `the replacement decoder cannot pair because no serving engine runs cohort "v2" yet`; allowed || !strings.Contains(reason, want) {
		t.Fatalf("the old decode pod: allowed=%v reason=%q; want it kept with %q", allowed, reason, want)
	}
	fleet.kubeletReports()
	fleet.requirePairable(v1beta1.DecoderComponent, 2, "after the kubelet reports")
}
