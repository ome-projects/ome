package inferenceservice

import (
	"fmt"
	"os"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// This file rolls a pairing-protocol change through the service-pass world:
// the real service controller and replica controller on one fake
// cluster, with the model's kubelet and endpoints controller between their
// passes. The push changes the protocol alone, so an old prefill pod pairs
// only with an old decode pod and a new one only with a new one. Both sides
// replace each pod beside its old one and declare none allowed out of
// rotation, so every Instance must keep a pod with a partner on the other
// side for the whole roll. The reading counts, at the end of every tick,
// the Instances of each side that do.
//
// OME_PROTOCOL_CHANGE_HUNT=full adds, for every count, the roll with the
// other side slow and with neither side slow.

const (
	protocolOld = "pair-v1"
	protocolNew = "pair-v2"
	// protocolRollTicks bounds one roll: 200 ticks of five seconds.
	protocolRollTicks = 200
	// protocolSlowTicks is how much later the slow side's pods start: six
	// ticks, thirty seconds.
	protocolSlowTicks = 6
	protocolHuntEnv   = "OME_PROTOCOL_CHANGE_HUNT"
)

// protocolShape is one fleet and the way it rolls.
type protocolShape struct {
	name string
	// prefill and decode are the Instances of each side, pods the pods of
	// one Instance: 1, or a gang of a leader and its workers.
	prefill, decode, pods int32
	// prefillSurge and decodeSurge are each side's own maxSurge.
	prefillSurge, decodeSurge string
	// group is the one rollout group over both sides.
	group v1beta1.RolloutGroup
	// slow is the side whose pods start late; empty when neither does.
	slow v1beta1.ComponentType
	// The picture the roll shows as the last old pods of one side leave
	// rotation: together of them leave in one tick, and from that tick
	// short Instances of the other side have no partner. A shape the wider
	// run adds names no side.
	leaving         v1beta1.ComponentType
	together, short int32
}

func (s protocolShape) count(side v1beta1.ComponentType) int32 {
	if side == v1beta1.EngineComponent {
		return s.prefill
	}
	return s.decode
}

func (s protocolShape) String() string {
	slow := "neither side slow"
	switch s.slow {
	case v1beta1.EngineComponent:
		slow = "prefill slow"
	case v1beta1.DecoderComponent:
		slow = "decode slow"
	}
	return s.name + ", " + slow
}

// pairGroup is a group over prefill and decode with the ratio rule at the
// given tolerance: blueGreen, which names no budget, or rollingUpdate with
// the given maxSurge.
func pairGroup(tolerance int32, maxSurge string) v1beta1.RolloutGroup {
	g := v1beta1.RolloutGroup{
		Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		MaintainRatio: &v1beta1.MaintainRatio{Tolerance: ptr.To(tolerance)},
	}
	if maxSurge == "" {
		g.BlueGreen = &v1beta1.GroupBlueGreen{}
		return g
	}
	surge := intstr.FromString(maxSurge)
	g.RollingUpdate = &v1beta1.GroupRollingUpdate{MaxSurge: &surge}
	return g
}

// protocolShapes are the reference counts: 24, 12 and 30 prefill pods
// beside 8, 8 and 10 decode pods in one blueGreen group, and 6 prefill
// gangs beside 2 decode gangs of a leader and seven workers, whose
// rollingUpdate group lets both decode gangs move in one batch. Each rolls
// with the side slow whose replacements the other side's last old pods wait
// for. The wider run adds the other side slow and neither side slow.
func protocolShapes(wide bool) []protocolShape {
	counts := []protocolShape{
		{name: "24 prefill beside 8 decode", prefill: 24, decode: 8, pods: 1, prefillSurge: "34%", decodeSurge: "34%", group: pairGroup(0, "")},
		{name: "12 prefill beside 8 decode", prefill: 12, decode: 8, pods: 1, prefillSurge: "100%", decodeSurge: "100%", group: pairGroup(20, "")},
		{name: "30 prefill beside 10 decode", prefill: 30, decode: 10, pods: 1, prefillSurge: "25%", decodeSurge: "25%", group: pairGroup(0, "")},
		{name: "6 prefill gangs beside 2 decode gangs of 8 pods", prefill: 6, decode: 2, pods: 8, prefillSurge: "34%", decodeSurge: "100%", group: pairGroup(20, "100%")},
	}
	pictures := []protocolShape{
		{slow: v1beta1.EngineComponent, leaving: v1beta1.DecoderComponent, together: 1, short: 0},
		{slow: v1beta1.EngineComponent, leaving: v1beta1.DecoderComponent, together: 1, short: 0},
		{slow: v1beta1.DecoderComponent, leaving: v1beta1.EngineComponent, together: 1, short: 0},
		{slow: v1beta1.EngineComponent, leaving: v1beta1.DecoderComponent, together: 1, short: 0},
	}
	var out []protocolShape
	for i, shape := range counts {
		pictured := shape
		pictured.slow, pictured.leaving, pictured.together, pictured.short = pictures[i].slow, pictures[i].leaving, pictures[i].together, pictures[i].short
		out = append(out, pictured)
		if !wide {
			continue
		}
		other := shape
		other.slow = pairingPeerOf(pictured.slow)
		out = append(out, other, shape)
	}
	return out
}

// protocolReading is the end of one tick: per side, the routed pods in
// rotation by protocol and the Instances with a pod in rotation that has a
// partner on the other side.
type protocolReading struct {
	tick     int
	serving  map[v1beta1.ComponentType]map[string]int32
	pairable map[v1beta1.ComponentType]int32
}

func (r protocolReading) String() string {
	return fmt.Sprintf("t=%d prefill %d pairable (old %d, new %d in rotation) decode %d pairable (old %d, new %d in rotation)", r.tick,
		r.pairable[v1beta1.EngineComponent], r.serving[v1beta1.EngineComponent][protocolOld], r.serving[v1beta1.EngineComponent][protocolNew],
		r.pairable[v1beta1.DecoderComponent], r.serving[v1beta1.DecoderComponent][protocolOld], r.serving[v1beta1.DecoderComponent][protocolNew])
}

// protocolWorld is the service-pass world with a pairing protocol on the
// service, one side slow to start, and the pairable reading.
type protocolWorld struct {
	*refusedWorld
	shape protocolShape
	// pushed is set once the new protocol is written: the pods of the roll
	// start late on the slow side, the first fleet does not.
	pushed   bool
	readings []protocolReading
}

func newProtocolWorld(t *testing.T, shape protocolShape) *protocolWorld {
	t.Helper()
	p := &protocolWorld{shape: shape}
	surge := map[v1beta1.ComponentType]intstr.IntOrString{
		v1beta1.EngineComponent:  intstr.FromString(shape.prefillSurge),
		v1beta1.DecoderComponent: intstr.FromString(shape.decodeSurge),
	}
	none := intstr.FromInt32(0)
	ext := &worldExtension{
		lifecycle: func(side v1beta1.ComponentType, lc *v1beta1.LifecycleSpec) {
			own := surge[side]
			lc.UpdateStrategy.RollingUpdate.MaxSurge = &own
			lc.UpdateStrategy.RollingUpdate.MaxUnavailable = &none
		},
		advance: p.startsLate,
		observe: func(*refusedWorld) { p.read() },
	}
	cfg := refusedConfig{
		shape: refusedShape{prefill: shape.prefill, decode: shape.decode, pods: shape.pods},
		settings: refusedSettings{
			prefill: v1beta1.UpdateStrategySurgeThenDrain, decode: v1beta1.UpdateStrategySurgeThenDrain,
			gangTiming: shape.pods > 1,
		},
	}
	p.refusedWorld = newRefusedWorldWithExtension(t, cfg, ext)
	p.writeProtocol(protocolOld)
	return p
}

// startsLate is the kubelet rule of the slow side: a placed pod of the roll
// stays Pending for the extra ticks before the world's own rule starts it.
func (p *protocolWorld) startsLate(w *refusedWorld, pod *corev1.Pod) bool {
	if !p.pushed || p.shape.slow == "" || pod.Labels[constants.OMEComponentLabel] != string(p.shape.slow) || pod.Spec.NodeName == "" || pod.Status.Phase != corev1.PodPending {
		return false
	}
	return w.tickNo-w.scheduledAt[pod.Name] < protocolSlowTicks
}

// writeProtocol writes the rollout block: the one group and the pairing
// protocol. The world renders the rest of the spec and never this block, so
// this is the only writer of the spec once the world exists.
func (p *protocolWorld) writeProtocol(protocol string) {
	p.t.Helper()
	p.refuse.own(func() {
		isvc := &v1beta1.InferenceService{}
		if err := p.cli.Get(p.ctx, p.serviceKey(), isvc); err != nil {
			p.t.Fatalf("read service: %v", err)
		}
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{PairingProtocol: ptr.To(protocol), Groups: []v1beta1.RolloutGroup{p.shape.group}}
		if err := p.cli.Update(p.ctx, isvc); err != nil {
			p.t.Fatalf("write the rollout block: %v", err)
		}
	})
	p.note("operator: pairing protocol %s", protocol)
}

var pairSides = []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}

// read counts the pods in rotation and the Instances that pair. A pod is in
// rotation when it is Ready with its serving gate True and is not being
// deleted; only a routed pod carries traffic, so a gang counts by its
// leader.
func (p *protocolWorld) read() {
	list := &corev1.PodList{}
	if err := p.cli.List(p.ctx, list, client.InNamespace(refusedNamespace)); err != nil {
		p.t.Fatalf("list pods: %v", err)
	}
	r := protocolReading{tick: p.tickNo, serving: map[v1beta1.ComponentType]map[string]int32{}, pairable: map[v1beta1.ComponentType]int32{}}
	instances := map[v1beta1.ComponentType]map[string]map[string]bool{}
	for _, side := range pairSides {
		r.serving[side] = map[string]int32{}
		instances[side] = map[string]map[string]bool{}
	}
	for i := range list.Items {
		pod := &list.Items[i]
		side := v1beta1.ComponentType(pod.Labels[constants.OMEComponentLabel])
		if r.serving[side] == nil || pod.DeletionTimestamp != nil || !query.RoutedRunner(pod) || !podreadiness.ReadyAndServing(pod) {
			continue
		}
		proto := pod.Labels[query.LabelPairingProtocol]
		r.serving[side][proto]++
		idx := pod.Labels[query.LabelInstanceIdx]
		if instances[side][idx] == nil {
			instances[side][idx] = map[string]bool{}
		}
		instances[side][idx][proto] = true
	}
	for _, side := range pairSides {
		for _, protos := range instances[side] {
			for proto := range protos {
				if r.serving[pairingPeerOf(side)][proto] > 0 {
					r.pairable[side]++
					break
				}
			}
		}
	}
	p.readings = append(p.readings, r)
}

// rollProtocol builds the fleet on the old protocol, pushes the new one and
// plays the roll out. It returns the readings of the roll alone.
func (p *protocolWorld) rollProtocol() []protocolReading {
	p.t.Helper()
	p.reachIdle()
	built := p.readings[len(p.readings)-1]
	for _, side := range pairSides {
		if built.pairable[side] != p.shape.count(side) {
			p.t.Fatalf("before the push %d of %d %s Instances pair: %s", built.pairable[side], p.shape.count(side), side, built)
		}
	}
	p.readings = nil
	p.path = "protocol change"
	p.pushed = true
	p.writeProtocol(protocolNew)
	if !p.playOut(protocolRollTicks) {
		p.t.Fatalf("the protocol change did not finish in %d ticks: %s\n%s", protocolRollTicks, p.holdText(), readingLines(p.readings))
	}
	last := p.readings[len(p.readings)-1]
	for _, side := range pairSides {
		if last.serving[side][protocolOld] != 0 || last.serving[side][protocolNew] != p.shape.count(side) {
			p.t.Fatalf("the roll ended with the %s off the new protocol: %s", side, last)
		}
	}
	return p.readings
}

func readingLines(readings []protocolReading) string {
	lines := make([]string, 0, len(readings))
	for _, r := range readings {
		lines = append(lines, r.String())
	}
	return strings.Join(lines, "\n")
}

// fewestPairable is the fewest Instances of one side that paired on any
// tick of the readings, the ticks that many paired, and the first of them.
func fewestPairable(readings []protocolReading, side v1beta1.ComponentType) (fewest int32, ticks int, first protocolReading) {
	fewest = -1
	for _, r := range readings {
		switch n := r.pairable[side]; {
		case fewest < 0 || n < fewest:
			fewest, ticks, first = n, 1, r
		case n == fewest:
			ticks++
		}
	}
	return fewest, ticks, first
}

// lastOldLeft is the first reading with no old pod of the side in rotation,
// and the reading before it.
func lastOldLeft(readings []protocolReading, side v1beta1.ComponentType) (before, at protocolReading, found bool) {
	for i, r := range readings {
		if i > 0 && r.serving[side][protocolOld] == 0 {
			return readings[i-1], r, true
		}
	}
	return protocolReading{}, protocolReading{}, false
}

// The story on 24 prefill beside 8 decode, prefill slow: three batches of 6
// prefill and 2 decode have moved, and the last batch is open. The two
// decode replacements serve while the six prefill replacements still start.
// One pass of the decode replica asks the drain gate about both old decode
// pods in turn. It takes the first out of rotation, and the gate keeps the
// second: the first no longer counts, though it reads Ready until the
// kubelet reports, so the second is the last partner of the six old prefill
// pods. It leaves alone, once the six replacements serve. The other counts
// show the same picture, with decode slow and on gangs.
//
// On every tick of every roll, the wider run included, every Instance of
// both sides has a pod in rotation with a partner on the other side.
func TestProtocolChange_TheLastOldPodsOfASideLeaveRotation(t *testing.T) {
	wide := os.Getenv(protocolHuntEnv) == "full"
	shapes := protocolShapes(wide)
	short, ticks := 0, 0
	for _, shape := range shapes {
		t.Run(shape.String(), func(t *testing.T) {
			world := newProtocolWorld(t, shape)
			readings := world.rollProtocol()
			ticks += len(readings)
			for _, side := range pairSides {
				fewest, held, first := fewestPairable(readings, side)
				if fewest >= shape.count(side) {
					continue
				}
				short++
				t.Errorf("%d of %d %s Instances pair for %d tick(s) from %s\n%s", fewest, shape.count(side), side, held, first, readingLines(readings))
			}
			if shape.leaving == "" {
				return
			}
			other := pairingPeerOf(shape.leaving)
			before, at, found := lastOldLeft(readings, shape.leaving)
			if !found {
				t.Fatalf("the old %s pods never left rotation\n%s", shape.leaving, readingLines(readings))
			}
			if got := before.serving[shape.leaving][protocolOld]; got != shape.together {
				t.Fatalf("%d old %s pods were in rotation one tick before none was (%s), want %d\n%s", got, shape.leaving, at, shape.together, readingLines(readings))
			}
			if fewest, _, _ := fewestPairable(readings, other); fewest != shape.count(other)-shape.short {
				t.Fatalf("the fewest %s Instances that pair is %d of %d, want %d\n%s", other, fewest, shape.count(other), shape.count(other)-shape.short, readingLines(readings))
			}
			if fewest, _, _ := fewestPairable(readings, shape.leaving); fewest < shape.count(shape.leaving) {
				t.Fatalf("the fewest %s Instances that pair is %d of %d\n%s", shape.leaving, fewest, shape.count(shape.leaving), readingLines(readings))
			}
			alone := 0
			for _, r := range readings {
				if r.serving[shape.leaving][protocolOld] == 1 {
					alone++
				}
			}
			t.Logf("the last old %s pod stayed in rotation for %d tick(s) as the only old one and left at tick %d", shape.leaving, alone, at.tick)
		})
	}
	t.Logf("%d shapes, %d ticks read, %d sides with fewer Instances pairing than their count", len(shapes), ticks, short)
}
