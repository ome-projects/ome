package coordination

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/drain"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// The traffic table is Status.Components.<c>.Traffic: the share of a
// Component's traffic each revision carries, the list a router reads.
// Reconcile writes it on every pass from the pods that serve (Running,
// PodReady, the serving gate on) and that the endpoints controller lists as
// Ready endpoints of their revision's routed Service
// (servingPodsListedByRevision, updateTrafficStatus,
// ComputeWeightsFromPods, NormalizeWeights, TrafficDiffersMeaningfully,
// TrafficWithinDeadband). A revision whose serving pod is not listed yet
// carries nothing and the TrafficEndpointsReady condition names the wait;
// a Component with nothing listed keeps the table it had.
//
// This file drives that pass tick by tick through an in-place push of the
// prefill side of the reference prefill/decode shapes, while the last old
// pod stays Ready, exits once, or exits again and again, with the patch
// landing before and after the pod's readiness flips, under both forms of
// the in-place strategy, with the endpoints controller trailing the pods,
// and with the operator's deadband off and on. The decode side stands
// still beside it.
//
// The world is the cluster around the pass. The replica's in-place roll of
// one Instance moves its pod through the phases the roll takes, one write
// per pass (ops/update_inplace.go: the gate off, the drain, the image
// record, the one patch that moves images and labels together, the
// confirmation and the gate on, the promote); a gang is rebuilt instead.
// The kubelet restarts a patched container and folds the gate into
// PodReady a tick later. The endpoints controller rewrites each routed
// Service's EndpointSlice from the pods, a tick behind when configured. The
// fault takes the last old pod's runner while it runs the old image. The
// roll's drain reads the same slices through the real gate
// (drain.IsPodDrained). The roll takes its Instances in index order, as
// many at once as its budget, and the old pod's own outages do not hold
// it: the budget is not what this model reads.
//
// Two readings per tick, written from what the operator and the router
// see. Between the world's move and the pass: a table that still names a
// revision whose pods all stopped serving while another revision serves is
// the pass behind the pods, and this tick's pass must correct it. After
// the pass: every revision named with a positive weight has a pod that
// serves traffic, or the status names the endpoint that pod waits for, or
// nothing of the Component serves at all; no revision carries a share
// before one of its pods is listed while another revision's pods are; the
// weights sum to 100; no revision with a listed serving pod is left out; each weight is within
// the deadband and rounding of the listed pod count; the entry of the
// revision the replica rolls to carries the latest flag; the table moves
// only when the listed pods moved; the untouched side keeps its table; and
// the push ends with the new revision alone at 100 and no wait recorded.
//
// OME_TRAFFIC_TABLE_HUNT=full widens the shapes and the knobs for a long
// local run; OME_TRAFFIC_TABLE_HUNT_ONLY=<text>[,<text>] keeps the
// configurations whose name contains one of the texts.

// tableShape is one reference shape the model pushes.
type tableShape struct {
	name string
	// prefill and decode are the Instances per side.
	prefill, decode int
	// gang is the pods per Instance: one, or a leader and gang-1 workers.
	gang int
	// inFlight is how many prefill Instances the roll takes at once.
	inFlight int
	// group puts both sides under one rollingUpdate group.
	group bool
}

// oldPodMode is what the last old pod's runner does during the push.
type oldPodMode int

const (
	// oldPodStays keeps serving until the roll reaches it.
	oldPodStays oldPodMode = iota
	// oldPodExitsOnce exits once and the kubelet restarts it in place.
	oldPodExitsOnce
	// oldPodLoops exits again and again, up for a while, down for a while.
	oldPodLoops
)

func (m oldPodMode) String() string {
	switch m {
	case oldPodExitsOnce:
		return "exits-once"
	case oldPodLoops:
		return "loops"
	}
	return "stays"
}

// tableConfig is one run of the model.
type tableConfig struct {
	shape tableShape
	// drainFirst is the strategy's markNotReadyDuringLifecycle: the pod
	// leaves rotation before the patch and returns after it.
	drainFirst bool
	oldPod     oldPodMode
	// up and down are the loop's ticks serving and ticks exited; exits
	// once uses down alone.
	up, down int
	// offset is the tick after the push at which the old pod first exits.
	offset int
	// restart is the ticks a patched container takes to come back.
	restart int
	// lag is the ticks the endpoints controller trails the pods.
	lag int
	// deadband is the operator's traffic weight deadband, in percent.
	deadband int32
}

func (c tableConfig) name() string {
	strategy := "drain-first"
	if !c.drainFirst {
		strategy = "no-drain"
	}
	fault := c.oldPod.String()
	switch c.oldPod {
	case oldPodExitsOnce:
		fault = fmt.Sprintf("exits-once(down=%d,at=%d)", c.down, c.offset)
	case oldPodLoops:
		fault = fmt.Sprintf("loops(up=%d,down=%d,at=%d)", c.up, c.down, c.offset)
	}
	return fmt.Sprintf("%s/%s/%s/restart=%d/lag=%d/deadband=%d", c.shape.name, strategy, fault, c.restart, c.lag, c.deadband)
}

// tableShapes are the shapes the model pushes: the two-by-two pair of the
// smallest reference shape, the same pair under a group, the 12:8 pair
// under its rollingUpdate group with the ratio rule, and the gang shapes.
func tableShapes(full bool) []tableShape {
	shapes := []tableShape{
		{name: "2-prefill-2-decode", prefill: 2, decode: 2, gang: 1, inFlight: 1},
		{name: "2-prefill-2-decode-grouped", prefill: 2, decode: 2, gang: 1, inFlight: 1, group: true},
		{name: "12-prefill-8-decode-grouped", prefill: 12, decode: 8, gang: 1, inFlight: 2, group: true},
		{name: "3-prefill-gangs-of-2-3-decode", prefill: 3, decode: 3, gang: 2, inFlight: 1},
	}
	if full {
		shapes = append(shapes,
			tableShape{name: "3-prefill-gangs-of-8-3-decode", prefill: 3, decode: 3, gang: 8, inFlight: 1},
			tableShape{name: "12-prefill-8-decode", prefill: 12, decode: 8, gang: 1, inFlight: 2},
			tableShape{name: "1-prefill-1-decode", prefill: 1, decode: 1, gang: 1, inFlight: 1},
		)
	}
	return shapes
}

// tableConfigs lists the configurations of a run: every shape under both
// strategies, with the old pod steady, exiting once at a few moments, and
// exiting again and again at every phase of its loop, with the deadband
// off and on. The full run adds the endpoints controller keeping pace,
// invisible and longer restarts, and longer outages.
func tableConfigs(full bool) []tableConfig {
	restarts, lags, exitLengths := []int{1}, []int{1}, []int{1}
	if full {
		restarts, lags, exitLengths = []int{0, 1, 2}, []int{0, 1}, []int{1, 2}
	}
	var out []tableConfig
	add := func(base tableConfig) {
		for _, restart := range restarts {
			for _, lag := range lags {
				for _, deadband := range []int32{0, 10} {
					cfg := base
					cfg.restart, cfg.lag, cfg.deadband = restart, lag, deadband
					out = append(out, cfg)
				}
			}
		}
	}
	for _, shape := range tableShapes(full) {
		for _, drainFirst := range []bool{true, false} {
			base := tableConfig{shape: shape, drainFirst: drainFirst}
			add(base)
			for _, down := range exitLengths {
				for _, at := range []int{1, 3, 5} {
					cfg := base
					cfg.oldPod, cfg.down, cfg.offset = oldPodExitsOnce, down, at
					add(cfg)
				}
			}
			for _, loop := range [][2]int{{2, 1}, {1, 1}} {
				for at := 0; at < loop[0]+loop[1]; at++ {
					cfg := base
					cfg.oldPod, cfg.up, cfg.down, cfg.offset = oldPodLoops, loop[0], loop[1], at
					add(cfg)
				}
			}
		}
	}
	return out
}

// The verb's phases of one Instance, one write a pass.
type tablePhase int

const (
	tableAtRest tablePhase = iota
	// tableGateOff: the pods are out of rotation; the drain is awaited.
	tableGateOff
	// tableRecorded: the image record is written; the patch comes next.
	tableRecorded
	// tablePatched: images and labels moved in one write; the kubelet is
	// awaited.
	tablePatched
	// tableGateOn: the gate is back on; PodReady is awaited.
	tableGateOn
	// tableDeleting: the gang's pods are terminating.
	tableDeleting
	// tableGone: the gang's pods are gone; the new set comes next.
	tableGone
	// tableCreated: the new gang is Pending.
	tableCreated
	// tableStarting: the new gang runs and its containers come up.
	tableStarting
	// tableDone: the row is Ready on the new revision.
	tableDone
)

// tablePod is one pod of the world and what the kubelet reports for it.
type tablePod struct {
	name      string
	component v1beta1.ComponentType
	index     int
	runner    v1beta1.RunnerName
	revision  string
	image     string
	running   bool
	// ready is ContainersReady; podReady is PodReady, folded by the
	// kubelet from ready and the gate a tick later.
	ready, podReady bool
	gateOn          bool
	// restartLeft counts the ticks until a restarted container is back.
	restartLeft int
	deleting    bool
	gone        bool
	inClient    bool
	deleted     bool
}

// tableInstance is one Instance: its pods, the verb's phase, and its row.
type tableInstance struct {
	component v1beta1.ComponentType
	index     int
	pods      []*tablePod
	phase     tablePhase
	running   string
	target    string
	// generation numbers a gang's pod sets so a rebuilt set's names are new.
	generation int
}

func (i *tableInstance) routed() *tablePod {
	for _, p := range i.pods {
		if query.RoutedRunner(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{query.LabelRunner: string(p.runner)}}}) {
			return p
		}
	}
	return nil
}

func (i *tableInstance) inFlight() bool { return i.phase != tableAtRest && i.phase != tableDone }

// podPicture is what the endpoints controller reads of one routed pod.
type podPicture struct {
	component v1beta1.ComponentType
	revision  string
	name      string
	running   bool
	podReady  bool
	deleting  bool
}

// tableFinding is the first picture of one class in a run.
type tableFinding struct {
	tick    int
	message string
}

// tableReport is what one run found.
type tableReport struct {
	ticks, passes int
	counts        map[string]int
	first         map[string]tableFinding
	// moves counts the table's moves per Component and revision.
	moves map[string]int
}

// tableWorld is the cluster around the pass for one configuration.
type tableWorld struct {
	t     testing.TB
	cfg   tableConfig
	ctx   context.Context
	c     client.Client
	isvc  *v1beta1.InferenceService
	rec   *record.FakeRecorder
	start time.Time
	tick  int

	engine, decode []*tableInstance
	nextIndex      int
	fault          *tablePod
	faultSet       bool
	pictures       []map[string]podPicture
	lastPicture    map[string]podPicture
	slices         map[string]bool

	// lastTable and lastListed are what the previous pass wrote and read,
	// per Component and revision hash.
	lastTable  map[v1beta1.ComponentType]map[string]int32
	lastListed map[v1beta1.ComponentType]map[string]int32
	lastLatest map[v1beta1.ComponentType]string
	// weightSeen remembers a revision that carried weight and then none,
	// so a return is read as one.
	weightSeen map[string]int
	// unlistedFor counts the ticks a serving pod has waited for its endpoint.
	unlistedFor map[string]int
	quiet       int
	report      tableReport
}

const (
	tableOld = "v1"
	tableNew = "v2"
	tableMax = 240
)

// tableKnown are the classes the model accepts, each with what it
// means to the operator. A class outside this list fails the run.
var tableKnown = map[string]string{
	"stale-until-the-next-pass": "between two passes the table still names the revision whose last pod stopped serving while the relabelled pod serves the new one; a pass-driven writer sees the change at its next pass, and that pass corrects the table",
	"names-a-revision-nothing-serves-while-the-other-waits-for-its-endpoint": "the only pod that serves is not a Ready endpoint of its routed Service yet, so its revision carries nothing, the status names the wait, and the table stands on the revision it had",
	"names-a-revision-nothing-serves-while-nothing-serves":                   "no pod of the Component serves, so no revision could carry the traffic and the table stands as it was",
	"share-returns-to-a-pod-that-came-back":                                  "a revision whose pod stopped serving lost its share and takes it back when the pod is listed again; the table follows the pods the endpoints controller lists",
	"record-names-the-new-revision-while-an-old-pod-still-runs":              "the rolled-out record names the new revision as soon as it alone has listed pods, while an old pod between two exits still runs the old revision",
	"record-moves-back-to-the-old-revision":                                  "the rolled-out record returns to the old revision when the new revision's pods stop serving and an old pod serves alone",
}

// tableFailing are the classes that are defects wherever they appear.
var tableFailing = map[string]bool{
	"sum-not-100":                                             true,
	"pass-reads-the-pods-unlike-the-world":                    true,
	"names-a-revision-before-its-pod-is-listed":               true,
	"names-a-revision-nothing-serves-while-another-is-listed": true,
	"names-a-revision-nothing-serves-with-no-word-why":        true,
	"a-listed-revision-is-left-out":                           true,
	"share-off-the-listed-pod-count":                          true,
	"latest-flag-off-the-target":                              true,
	"untouched-side-moved":                                    true,
	"moved-with-no-listed-change":                             true,
	"stale-past-the-next-pass":                                true,
	"waits-for-its-endpoint-past-the-lag":                     true,
	"roll-did-not-finish":                                     true,
	"did-not-end-on-the-new-revision-alone":                   true,
}

func tableFails(class string) bool {
	_, known := tableKnown[class]
	return tableFailing[class] || !known
}

func tableReading(class string) string {
	switch {
	case tableFailing[class]:
		return "FAILS"
	case tableKnown[class] != "":
		return tableKnown[class]
	}
	return "UNKNOWN CLASS, fails until classified"
}

// newTableWorld builds the service at rest on the old revision: every
// Instance Ready, every routed pod a Ready endpoint of the old revision's
// routed Service, one pass taken so both tables name the old revision at
// 100.
func newTableWorld(t testing.TB, cfg tableConfig) *tableWorld {
	t.Helper()
	isvc := testOMENativeISVC()
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
		},
	}
	if cfg.shape.group {
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
			Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			RollingUpdate: &v1beta1.GroupRollingUpdate{MaxUnavailable: ptr.To(intstr.FromString("10%"))},
			MaintainRatio: &v1beta1.MaintainRatio{Tolerance: ptr.To(int32(20))},
		}}}
	}
	w := &tableWorld{
		t: t, cfg: cfg, ctx: context.Background(), isvc: isvc,
		rec:    record.NewFakeRecorder(4096),
		start:  time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		slices: map[string]bool{}, weightSeen: map[string]int{}, unlistedFor: map[string]int{},
		lastTable: map[v1beta1.ComponentType]map[string]int32{}, lastListed: map[v1beta1.ComponentType]map[string]int32{},
		lastLatest: map[v1beta1.ComponentType]string{},
		report:     tableReport{counts: map[string]int{}, first: map[string]tableFinding{}, moves: map[string]int{}},
	}
	w.c = testClient()
	for idx := 0; idx < cfg.shape.prefill; idx++ {
		w.engine = append(w.engine, w.newInstance(v1beta1.EngineComponent, idx, tableOld, 0))
	}
	for idx := 0; idx < cfg.shape.decode; idx++ {
		w.decode = append(w.decode, w.newInstance(v1beta1.DecoderComponent, idx, tableOld, 0))
	}
	for _, c := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
		ir := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Name: isvc.Name + "-" + string(c), Namespace: isvc.Namespace}}
		if err := w.c.Create(w.ctx, ir); err != nil {
			t.Fatalf("create replica %s: %v", c, err)
		}
	}
	w.fault = w.engine[len(w.engine)-1].routed()
	w.faultSet = cfg.oldPod != oldPodStays
	// At rest: every pod serves and is listed; the first pass writes both
	// tables on the old revision.
	for _, inst := range append(append([]*tableInstance{}, w.engine...), w.decode...) {
		for _, p := range inst.pods {
			p.running, p.ready, p.podReady, p.gateOn = true, true, true, true
		}
	}
	w.writeWorld()
	w.pictures = append(w.pictures, w.picture())
	w.writeEndpoints(w.pictures[0])
	w.pass()
	for _, c := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
		got := w.table(c)
		if len(got) != 1 || got[tableOld] != 100 {
			t.Fatalf("at rest the %s table must name the old revision alone at 100, got %v", c, got)
		}
		w.lastTable[c], w.lastListed[c], w.lastLatest[c] = got, map[string]int32{tableOld: int32(len(w.instances(c)) * w.servingMembers())}, w.latestRecord(c)
	}
	return w
}

func (w *tableWorld) servingMembers() int { return w.cfg.shape.gang }

func (w *tableWorld) instances(c v1beta1.ComponentType) []*tableInstance {
	if c == v1beta1.EngineComponent {
		return w.engine
	}
	return w.decode
}

// newInstance builds the pods of one Instance on a revision, not yet in
// the client.
func (w *tableWorld) newInstance(c v1beta1.ComponentType, index int, revision string, generation int) *tableInstance {
	inst := &tableInstance{component: c, index: index, running: revision, target: revision, generation: generation}
	runners := []v1beta1.RunnerName{v1beta1.RunnerNameDefault}
	if w.cfg.shape.gang > 1 {
		runners = []v1beta1.RunnerName{v1beta1.RunnerNameLeader}
		for k := 1; k < w.cfg.shape.gang; k++ {
			runners = append(runners, v1beta1.RunnerNameWorker)
		}
	}
	for k, runner := range runners {
		inst.pods = append(inst.pods, &tablePod{
			name:      fmt.Sprintf("%s-%s-%d-%d-g%d", w.isvc.Name, c, index, k, generation),
			component: c, index: index, runner: runner, revision: revision, image: revision,
		})
	}
	return inst
}

// run pushes the prefill side and reads every tick until the push ends
// and the tables have been quiet for two ticks.
func (w *tableWorld) run() tableReport {
	w.push()
	for w.tick < tableMax && !w.done() {
		w.step()
	}
	if !w.done() {
		w.note("roll-did-not-finish", "after %d ticks the push has not ended: %s", w.tick, w.rows())
	}
	got := w.table(v1beta1.EngineComponent)
	cond := w.isvc.Status.GetCondition(apis.ConditionType(v1beta1.TrafficEndpointsReady))
	if len(got) != 1 || got[tableNew] != 100 || (cond != nil && cond.Status == corev1.ConditionFalse) || w.latestRecord(v1beta1.EngineComponent) != tableNew {
		w.note("did-not-end-on-the-new-revision-alone", "the push ended with the table %v, the record %q and the wait %v", got, w.latestRecord(v1beta1.EngineComponent), cond)
	}
	w.report.ticks = w.tick
	return w.report
}

// push moves the prefill replica's update revision to the new revision.
func (w *tableWorld) push() {
	for _, inst := range w.engine {
		inst.target = tableNew
	}
}

// done: every prefill Instance is promoted, no revision waits for its
// endpoints, and the table has stood for two ticks.
func (w *tableWorld) done() bool {
	for _, inst := range w.engine {
		if inst.phase != tableDone {
			return false
		}
	}
	return w.quiet >= 2
}

// waiting reports whether the status names a revision waiting for its
// endpoints.
func (w *tableWorld) waiting() bool {
	cond := w.isvc.Status.GetCondition(apis.ConditionType(v1beta1.TrafficEndpointsReady))
	return cond != nil && cond.Status == corev1.ConditionFalse
}

// step is one tick: the kubelet, the roll, the fault, the writes, the
// endpoints controller, the reading before the pass, the pass, and the
// readings after it.
func (w *tableWorld) step() {
	w.tick++
	w.kubelet()
	w.roll()
	w.runFault()
	w.writeWorld()
	w.pictures = append(w.pictures, w.picture())
	w.writeEndpoints(w.pictures[max(0, len(w.pictures)-1-w.cfg.lag)])
	w.readBeforePass()
	w.pass()
	w.readAfterPass()
}

// kubelet brings restarted containers back and folds the gate and the
// containers into PodReady.
func (w *tableWorld) kubelet() {
	for _, inst := range append(append([]*tableInstance{}, w.engine...), w.decode...) {
		for _, p := range inst.pods {
			if p.restartLeft > 0 {
				p.restartLeft--
				if p.restartLeft == 0 {
					p.ready = true
				}
			}
			p.podReady = p.running && p.ready && p.gateOn && !p.gone
		}
	}
}

// roll advances every prefill Instance in flight by one write and admits
// the next Instances up to the budget.
func (w *tableWorld) roll() {
	for _, inst := range w.engine {
		if inst.inFlight() {
			w.advance(inst)
		}
	}
	for w.inFlightCount() < w.cfg.shape.inFlight && w.nextIndex < len(w.engine) {
		inst := w.engine[w.nextIndex]
		w.nextIndex++
		w.open(inst)
	}
}

func (w *tableWorld) inFlightCount() int {
	n := 0
	for _, inst := range w.engine {
		if inst.inFlight() {
			n++
		}
	}
	return n
}

// open is the roll's first write on an Instance: the gate off under the
// drain-first strategy or on a gang, the image record otherwise.
func (w *tableWorld) open(inst *tableInstance) {
	if w.cfg.shape.gang > 1 || w.cfg.drainFirst {
		for _, p := range inst.pods {
			p.gateOn = false
		}
		inst.phase = tableGateOff
		return
	}
	inst.phase = tableRecorded
}

// advance is the roll's next write on an Instance, decided from the phase
// and the pods as the roll reads them.
func (w *tableWorld) advance(inst *tableInstance) {
	if w.cfg.shape.gang > 1 {
		w.advanceGang(inst)
		return
	}
	pod := inst.pods[0]
	switch inst.phase {
	case tableGateOff:
		if w.drained(pod) {
			inst.phase = tableRecorded
		}
	case tableRecorded:
		pod.revision, pod.image = tableNew, tableNew
		if w.cfg.restart > 0 {
			pod.ready, pod.restartLeft = false, w.cfg.restart
		}
		inst.phase = tablePatched
	case tablePatched:
		if !pod.ready || pod.image != tableNew {
			return
		}
		if !pod.gateOn {
			pod.gateOn = true
			inst.phase = tableGateOn
			return
		}
		if pod.podReady {
			w.promote(inst)
		}
	case tableGateOn:
		if pod.podReady {
			w.promote(inst)
		}
	}
}

// advanceGang rebuilds a gang: the gate off, the drain, the delete, the
// new set, the gate on once every member's containers are up, the
// promote once every member is PodReady.
func (w *tableWorld) advanceGang(inst *tableInstance) {
	switch inst.phase {
	case tableGateOff:
		if w.drained(inst.routed()) {
			for _, p := range inst.pods {
				p.deleting = true
			}
			inst.phase = tableDeleting
		}
	case tableDeleting:
		for _, p := range inst.pods {
			p.gone = true
		}
		inst.phase = tableGone
	case tableGone:
		fresh := w.newInstance(inst.component, inst.index, tableNew, inst.generation+1)
		inst.generation = fresh.generation
		w.retire(inst.pods)
		inst.pods = fresh.pods
		inst.phase = tableCreated
	case tableCreated:
		for _, p := range inst.pods {
			p.running, p.restartLeft = true, 1
		}
		inst.phase = tableStarting
	case tableStarting:
		for _, p := range inst.pods {
			if !p.ready {
				return
			}
		}
		for _, p := range inst.pods {
			p.gateOn = true
		}
		inst.phase = tableGateOn
	case tableGateOn:
		for _, p := range inst.pods {
			if !p.podReady {
				return
			}
		}
		w.promote(inst)
	}
}

// retire removes a gone pod set from the client once and for all.
func (w *tableWorld) retire(pods []*tablePod) {
	for _, p := range pods {
		w.writePod(p)
	}
}

func (w *tableWorld) promote(inst *tableInstance) {
	inst.running, inst.phase = tableNew, tableDone
}

// drained asks the real drain gate whether the pod has left its old
// revision's routed Service.
func (w *tableWorld) drained(p *tablePod) bool {
	service := PerRevisionServiceName(prefixFor(w.isvc, p.component), p.component, p.revision)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: p.name, Namespace: w.isvc.Namespace}}
	drained, err := drain.IsPodDrained(w.ctx, w.c, w.isvc.Namespace, service, pod)
	if err != nil {
		w.t.Fatalf("drain gate for %s: %v", p.name, err)
	}
	return drained
}

// runFault takes the last old pod's runner at the configured moments, while
// the pod still runs the old image.
func (w *tableWorld) runFault() {
	if !w.faultSet || w.fault.image != tableOld || w.fault.gone {
		return
	}
	since := w.tick - 1 - w.cfg.offset
	if since < 0 {
		return
	}
	switch w.cfg.oldPod {
	case oldPodExitsOnce:
		if since == 0 {
			w.takeRunner(w.fault, w.cfg.down)
		}
	case oldPodLoops:
		if since%(w.cfg.up+w.cfg.down) == 0 {
			w.takeRunner(w.fault, w.cfg.down)
		}
	}
}

// takeRunner ends the pod's runner; the kubelet restarts it in place and it
// is back after down ticks.
func (w *tableWorld) takeRunner(p *tablePod, down int) {
	p.ready, p.restartLeft = false, down
}

// writeWorld writes every pod and both replicas as the cluster would hold
// them.
func (w *tableWorld) writeWorld() {
	for _, inst := range append(append([]*tableInstance{}, w.engine...), w.decode...) {
		for _, p := range inst.pods {
			w.writePod(p)
		}
	}
	for _, c := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
		w.writeReplica(c)
	}
}

func (w *tableWorld) podObject(p *tablePod) *corev1.Pod {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: p.name, Namespace: w.isvc.Namespace,
		Labels: map[string]string{
			constants.InferenceServicePodLabelKey: w.isvc.Name,
			constants.OMEComponentLabel:           string(p.component),
			query.LabelManagedBy:                  query.ManagedByOMENative,
			query.LabelRevisionHash:               p.revision,
			query.LabelInstanceIdx:                strconv.Itoa(p.index),
			query.LabelRunner:                     string(p.runner),
			query.LabelPodOrdinal:                 "0",
		},
	}}
	pod.Spec.Containers = []corev1.Container{{Name: "runner", Image: "runner:" + p.image}}
	pod.Spec.ReadinessGates = []corev1.PodReadinessGate{{ConditionType: podreadiness.ConditionType}}
	phase := corev1.PodPending
	if p.running {
		phase = corev1.PodRunning
	}
	pod.Status = corev1.PodStatus{Phase: phase, Conditions: []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: conditionOf(p.ready)},
		{Type: corev1.PodReady, Status: conditionOf(p.podReady)},
		{Type: podreadiness.ConditionType, Status: conditionOf(p.gateOn)},
	}}
	return pod
}

func conditionOf(b bool) corev1.ConditionStatus {
	if b {
		return corev1.ConditionTrue
	}
	return corev1.ConditionFalse
}

// writePod brings the client's copy of the pod level with the world: its
// labels and spec by an update, its status by a status update, a delete
// with a finalizer while it terminates, and the finalizer dropped once it
// is gone.
func (w *tableWorld) writePod(p *tablePod) {
	if p.deleted {
		return
	}
	desired := w.podObject(p)
	key := client.ObjectKeyFromObject(desired)
	stored := &corev1.Pod{}
	if !p.inClient {
		if err := w.c.Create(w.ctx, desired); err != nil {
			w.t.Fatalf("create pod %s: %v", p.name, err)
		}
		p.inClient = true
	}
	if err := w.c.Get(w.ctx, key, stored); err != nil {
		w.t.Fatalf("get pod %s: %v", p.name, err)
	}
	stored.Labels, stored.Spec.Containers = desired.Labels, desired.Spec.Containers
	if p.gone {
		w.removePod(p, stored)
		return
	}
	if p.deleting && stored.DeletionTimestamp == nil {
		stored.Finalizers = []string{"model.ome.io/termination-grace"}
	}
	if err := w.c.Update(w.ctx, stored); err != nil {
		w.t.Fatalf("update pod %s: %v", p.name, err)
	}
	if err := w.c.Get(w.ctx, key, stored); err != nil {
		w.t.Fatalf("get pod %s: %v", p.name, err)
	}
	stored.Status = desired.Status
	if err := w.c.Status().Update(w.ctx, stored); err != nil {
		w.t.Fatalf("update pod status %s: %v", p.name, err)
	}
	if p.deleting && stored.DeletionTimestamp == nil {
		if err := w.c.Delete(w.ctx, stored); err != nil {
			w.t.Fatalf("delete pod %s: %v", p.name, err)
		}
	}
}

// removePod takes a gone pod out of the client: the finalizer dropped
// from a terminating pod, a delete for one that never terminated.
func (w *tableWorld) removePod(p *tablePod, stored *corev1.Pod) {
	stored.Finalizers = nil
	if err := w.c.Update(w.ctx, stored); err != nil && !apierrors.IsNotFound(err) {
		w.t.Fatalf("let pod %s go: %v", p.name, err)
	}
	if stored.DeletionTimestamp == nil {
		if err := w.c.Delete(w.ctx, stored); err != nil && !apierrors.IsNotFound(err) {
			w.t.Fatalf("delete pod %s: %v", p.name, err)
		}
	}
	p.deleted = true
}

// writeReplica publishes the replica as its controller would: the rows,
// the counters, the revision pair and the Ready condition.
func (w *tableWorld) writeReplica(c v1beta1.ComponentType) {
	ir := &v1beta1.InferenceReplica{}
	key := client.ObjectKey{Namespace: w.isvc.Namespace, Name: w.isvc.Name + "-" + string(c)}
	if err := w.c.Get(w.ctx, key, ir); err != nil {
		w.t.Fatalf("get replica %s: %v", c, err)
	}
	insts := w.instances(c)
	ir.Spec.Replicas = ptr.To(int32(len(insts)))
	status := v1beta1.InferenceReplicaStatus{Replicas: int32(len(insts))}
	target := tableOld
	allNew := true
	for _, inst := range insts {
		row := v1beta1.OMENativeInstanceStatus{Index: int32(inst.index), Phase: v1beta1.OMENativeInstanceReady,
			RunningRevision: w.revisionName(c, inst.running), PodCount: int32(len(inst.pods))}
		if inst.target != inst.running {
			target = inst.target
			row.Phase, row.TargetRevision = v1beta1.OMENativeInstanceUpdating, w.revisionName(c, inst.target)
			row.Operation = &v1beta1.InstanceOperation{ID: fmt.Sprintf("update-%d", inst.index), Type: v1beta1.InstanceOperationUpdate}
		}
		if inst.running != tableNew {
			allNew = false
		}
		serving := 0
		for _, p := range inst.pods {
			if p.podReady {
				row.ReadyPodCount++
				serving++
			}
		}
		row.ServingPodCount = int32(serving)
		if row.ReadyPodCount == row.PodCount && row.Phase == v1beta1.OMENativeInstanceReady {
			status.ReadyReplicas++
			status.ServingReplicas++
			status.AvailableReplicas++
		}
		if inst.running == tableNew {
			status.UpdatedReplicas++
			if row.ReadyPodCount == row.PodCount {
				status.UpdatedReadyReplicas++
			}
		}
		status.InstanceStatuses = append(status.InstanceStatuses, row)
	}
	if allNew {
		target = tableNew
	}
	status.CurrentRevision = w.revisionName(c, tableOld)
	if allNew {
		status.CurrentRevision = w.revisionName(c, tableNew)
	}
	status.UpdateRevision = w.revisionName(c, target)
	status.Conditions = []metav1.Condition{{Type: conditionReady, Status: metav1.ConditionTrue, Reason: "Serving",
		LastTransitionTime: metav1.NewTime(w.start)}}
	ir.Status = status
	if err := w.c.Update(w.ctx, ir); err != nil {
		w.t.Fatalf("update replica %s: %v", c, err)
	}
}

func (w *tableWorld) revisionName(c v1beta1.ComponentType, hash string) string {
	return w.isvc.Name + "-" + string(c) + "-" + hash
}

// picture is what the endpoints controller reads of every routed pod now.
func (w *tableWorld) picture() map[string]podPicture {
	out := map[string]podPicture{}
	for _, inst := range append(append([]*tableInstance{}, w.engine...), w.decode...) {
		p := inst.routed()
		if p == nil || p.gone {
			continue
		}
		out[p.name] = podPicture{component: p.component, revision: p.revision, name: p.name, running: p.running, podReady: p.podReady, deleting: p.deleting}
	}
	return out
}

// writeEndpoints rewrites each routed Service's EndpointSlice from a
// picture: every running routed pod of the revision an endpoint, Ready as
// its PodReady, Terminating while it is deleted.
func (w *tableWorld) writeEndpoints(picture map[string]podPicture) {
	w.lastPicture = picture
	byService := map[string][]podPicture{}
	names := make([]string, 0, len(picture))
	for name := range picture {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		p := picture[name]
		service := PerRevisionServiceName(prefixFor(w.isvc, p.component), p.component, p.revision)
		if p.running {
			byService[service] = append(byService[service], p)
		} else if _, ok := byService[service]; !ok {
			byService[service] = nil
		}
	}
	for service := range w.slices {
		if _, ok := byService[service]; !ok {
			byService[service] = nil
		}
	}
	for service, pods := range byService {
		w.writeSlice(service, pods)
	}
}

func (w *tableWorld) writeSlice(service string, pods []podPicture) {
	slice := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{
		Name: service + "-m", Namespace: w.isvc.Namespace,
		Labels: map[string]string{discoveryv1.LabelServiceName: service},
	}, AddressType: discoveryv1.AddressTypeIPv4}
	for i, p := range pods {
		slice.Endpoints = append(slice.Endpoints, discoveryv1.Endpoint{
			Addresses:  []string{fmt.Sprintf("10.%d.%d.%d", len(service)%200, i/250, i%250+1)},
			Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(p.podReady), Serving: ptr.To(p.podReady), Terminating: ptr.To(p.deleting)},
			TargetRef:  &corev1.ObjectReference{Kind: "Pod", Namespace: w.isvc.Namespace, Name: p.name},
		})
	}
	key := client.ObjectKeyFromObject(slice)
	stored := &discoveryv1.EndpointSlice{}
	err := w.c.Get(w.ctx, key, stored)
	switch {
	case apierrors.IsNotFound(err) && len(pods) == 0:
		return
	case apierrors.IsNotFound(err):
		if cerr := w.c.Create(w.ctx, slice); cerr != nil {
			w.t.Fatalf("create slice %s: %v", key.Name, cerr)
		}
		w.slices[service] = true
	case err != nil:
		w.t.Fatalf("get slice %s: %v", key.Name, err)
	case len(pods) == 0:
		if derr := w.c.Delete(w.ctx, stored); derr != nil {
			w.t.Fatalf("delete slice %s: %v", key.Name, derr)
		}
		delete(w.slices, service)
	default:
		stored.Endpoints = slice.Endpoints
		if uerr := w.c.Update(w.ctx, stored); uerr != nil {
			w.t.Fatalf("update slice %s: %v", key.Name, uerr)
		}
	}
}

// pass is one pass of the real traffic writer.
func (w *tableWorld) pass() {
	if _, err := Reconcile(w.ctx, ReconcileInputs{
		ISVC: w.isvc, Client: w.c, Reader: w.c, Recorder: w.rec, Now: w.start.Add(time.Duration(w.tick) * time.Second),
		ComponentDeploymentModes:     testOMENativeModes(v1beta1.EngineComponent, v1beta1.DecoderComponent),
		ComponentRunnerPorts:         testComponentRunnerPorts(),
		TrafficWeightDeadbandPercent: w.cfg.deadband,
	}); err != nil {
		w.t.Fatalf("tick %d: pass: %v", w.tick, err)
	}
	w.report.passes++
	for {
		select {
		case <-w.rec.Events:
		default:
			return
		}
	}
}

// table reads the Component's table by revision hash.
func (w *tableWorld) table(c v1beta1.ComponentType) map[string]int32 {
	out := map[string]int32{}
	for _, target := range w.isvc.Status.Components[c].Traffic {
		out[query.RevisionFromName(target.RevisionName).Hash()] = target.Percent
	}
	return out
}

func (w *tableWorld) latestRecord(c v1beta1.ComponentType) string {
	return query.RevisionFromName(w.isvc.Status.Components[c].LatestRolledoutRevision).Hash()
}

// servedAndListed counts, per revision, the Component's pods the world
// knows serve traffic (Running, containers up, the gate on, a routed
// runner, not terminating) and, of those, the ones the endpoints
// controller's last write lists as Ready endpoints of the routed Service
// of the revision they carry now.
func (w *tableWorld) servedAndListed(c v1beta1.ComponentType) (served, listed map[string]int32) {
	served, listed, _ = w.servedListedUnlisted(c)
	return served, listed
}

// servedListedUnlisted is servedAndListed with the names of the serving
// pods their routed Service does not list yet.
func (w *tableWorld) servedListedUnlisted(c v1beta1.ComponentType) (served, listed map[string]int32, unlisted []string) {
	served, listed = map[string]int32{}, map[string]int32{}
	for _, inst := range w.instances(c) {
		p := inst.routed()
		if p == nil || p.gone || p.deleting || !p.running || !p.podReady || !p.gateOn {
			continue
		}
		served[p.revision]++
		if pic, ok := w.lastPicture[p.name]; ok && pic.revision == p.revision && pic.running && pic.podReady && !pic.deleting {
			listed[p.revision]++
		} else {
			unlisted = append(unlisted, p.name)
		}
	}
	return served, listed, unlisted
}

// passServedAndListed reads the same two counts through the pass's own
// readers (podReadyAndServing, query.RoutedRunner, drain.IsPodInRotation)
// over the client's pods, so the world's reading and the pass's can be
// held to each other.
func (w *tableWorld) passServedAndListed(c v1beta1.ComponentType) (served, listed map[string]int32) {
	served, listed = map[string]int32{}, map[string]int32{}
	pods := &corev1.PodList{}
	if err := w.c.List(w.ctx, pods, client.InNamespace(w.isvc.Namespace), client.MatchingLabels{constants.OMEComponentLabel: string(c)}); err != nil {
		w.t.Fatalf("list pods of %s: %v", c, err)
	}
	rotation := drain.NewBatcher(w.c, w.isvc.Namespace)
	for i := range pods.Items {
		pod := &pods.Items[i]
		hash := pod.Labels[query.LabelRevisionHash]
		if !query.RoutedRunner(pod) || !podReadyAndServing(pod) {
			continue
		}
		served[hash]++
		in, err := rotation.IsPodInRotation(w.ctx, PerRevisionServiceName(prefixFor(w.isvc, c), c, hash), pod)
		if err != nil {
			w.t.Fatalf("rotation of %s: %v", pod.Name, err)
		}
		if in {
			listed[hash]++
		}
	}
	return served, listed
}

// readPassReaders holds the pass's readers to the world's counts: a pass
// that reads a pod as serving or listed when the world knows it is not,
// or the other way round, writes its table from another cluster than the
// one the world holds.
func (w *tableWorld) readPassReaders(served, listed map[string]int32) {
	passServed, passListed := w.passServedAndListed(v1beta1.EngineComponent)
	if !sameTable(served, passServed) || !sameTable(listed, passListed) {
		w.note("pass-reads-the-pods-unlike-the-world", "the pass reads served %v listed %v where the world holds served %v listed %v: %s", passServed, passListed, served, listed, w.rows())
	}
}

// readEndpointWaits counts, once a tick, how long each serving pod has
// waited for its endpoint; a wait past the endpoints controller's lag is
// the world or the reader misreading.
func (w *tableWorld) readEndpointWaits(unlisted []string) {
	waiting := map[string]bool{}
	for _, name := range unlisted {
		waiting[name] = true
		w.unlistedFor[name]++
		if w.unlistedFor[name] > w.cfg.lag+1 {
			w.note("waits-for-its-endpoint-past-the-lag", "%s serves and has waited %d ticks for its endpoint", name, w.unlistedFor[name])
		}
	}
	for name := range w.unlistedFor {
		if !waiting[name] {
			delete(w.unlistedFor, name)
		}
	}
}

// readBeforePass reads the table the previous pass left against the pods
// as they stand now: a weighted revision with no serving pod beside a
// revision that serves is the pass behind the pods.
func (w *tableWorld) readBeforePass() {
	c := v1beta1.EngineComponent
	served, _ := w.servedAndListed(c)
	table := w.table(c)
	for hash, percent := range table {
		if percent <= 0 || served[hash] > 0 {
			continue
		}
		for other, n := range served {
			if other != hash && n > 0 {
				w.note("stale-until-the-next-pass", "before the pass the table names %s at %d%% with no serving pod while %s has %d serving: %s", hash, percent, other, n, w.rows())
			}
		}
	}
}

// readAfterPass takes every reading of the table the pass wrote.
func (w *tableWorld) readAfterPass() {
	w.readDecoder()
	c := v1beta1.EngineComponent
	served, listed, unlisted := w.servedListedUnlisted(c)
	w.readEndpointWaits(unlisted)
	w.readPassReaders(served, listed)
	table := w.table(c)
	var sum int32
	for _, percent := range table {
		sum += percent
	}
	if len(table) > 0 && sum != 100 {
		w.note("sum-not-100", "the weights sum to %d: %v", sum, table)
	}
	w.readBacking(table, served, listed)
	w.readShares(table, listed)
	w.readLatestFlag(table, listed)
	w.readMoves(table, listed)
	w.readRecord()
	if sameTable(table, w.lastTable[c]) && !w.waiting() {
		w.quiet++
	} else {
		w.quiet = 0
	}
	w.lastTable[c], w.lastListed[c] = table, listed
}

// readDecoder holds the untouched side's table where it was.
func (w *tableWorld) readDecoder() {
	got := w.table(v1beta1.DecoderComponent)
	if len(got) != 1 || got[tableOld] != 100 {
		w.note("untouched-side-moved", "the decode side, which nobody touched, reads %v", got)
	}
}

// readBacking: every weighted revision has a serving pod, or the status
// says what the serving pod waits for, or nothing serves.
func (w *tableWorld) readBacking(table, served, listed map[string]int32) {
	cond := w.isvc.Status.GetCondition(apis.ConditionType(v1beta1.TrafficEndpointsReady))
	for hash, percent := range table {
		if percent <= 0 {
			continue
		}
		if served[hash] > 0 && listed[hash] == 0 && anyListed(listed) {
			w.note("names-a-revision-before-its-pod-is-listed", "the table names %s at %d%% while none of its %d serving pods is listed and another revision is: %s", hash, percent, served[hash], w.rows())
			continue
		}
		if served[hash] > 0 {
			continue
		}
		var otherListed, otherServed string
		for other, n := range listed {
			if other != hash && n > 0 {
				otherListed = other
			}
		}
		for other, n := range served {
			if other != hash && n > 0 {
				otherServed = other
			}
		}
		switch {
		case otherListed != "":
			w.note("names-a-revision-nothing-serves-while-another-is-listed", "the table names %s at %d%% with no serving pod while %s has %d listed serving pods: %s", hash, percent, otherListed, listed[otherListed], w.rows())
		case otherServed != "":
			if cond == nil || cond.Status != corev1.ConditionFalse || !strings.Contains(cond.Message, string(v1beta1.EngineComponent)+" revision "+otherServed) {
				w.note("names-a-revision-nothing-serves-with-no-word-why", "the table names %s at %d%% with no serving pod while %s serves unlisted and the status says %v: %s", hash, percent, otherServed, cond, w.rows())
				continue
			}
			w.note("names-a-revision-nothing-serves-while-the-other-waits-for-its-endpoint", "the table names %s at %d%% with no serving pod while %s's pod waits for its endpoint (%s): %s", hash, percent, otherServed, cond.Message, w.rows())
		default:
			w.note("names-a-revision-nothing-serves-while-nothing-serves", "the table names %s at %d%% while nothing of the Component serves: %s", hash, percent, w.rows())
		}
	}
}

// readShares: every listed revision has a share, within the deadband and
// rounding of its listed pod count.
func (w *tableWorld) readShares(table, listed map[string]int32) {
	var total int32
	for _, n := range listed {
		total += n
	}
	if total == 0 {
		return
	}
	for hash, n := range listed {
		if n <= 0 {
			continue
		}
		percent, ok := table[hash]
		if !ok || percent <= 0 {
			w.note("a-listed-revision-is-left-out", "%s has %d listed serving pods and no share: %v: %s", hash, n, table, w.rows())
			continue
		}
		want := n * 100 / total
		slack := w.cfg.deadband + int32(len(listed))
		if percent-want > slack || want-percent > slack {
			w.note("share-off-the-listed-pod-count", "%s carries %d%% for %d of %d listed pods (want about %d%%): %s", hash, percent, n, total, want, w.rows())
		}
	}
}

// readLatestFlag: the entry of the revision the replica rolls to carries
// the latest flag and no other entry does. A table the pass left standing
// because nothing is listed keeps its flags with its weights.
func (w *tableWorld) readLatestFlag(table, listed map[string]int32) {
	if len(listed) == 0 {
		return
	}
	target := tableOld
	for _, inst := range w.engine {
		if inst.target == tableNew {
			target = tableNew
		}
	}
	for _, entry := range w.isvc.Status.Components[v1beta1.EngineComponent].Traffic {
		hash := query.RevisionFromName(entry.RevisionName).Hash()
		if entry.LatestRevision != (hash == target) {
			w.note("latest-flag-off-the-target", "the entry of %s carries latest=%v while the replica rolls to %s: %v", hash, entry.LatestRevision, target, table)
		}
	}
}

// readMoves: the table moves only when the listed pods moved; a revision
// that lost its share and takes it back is counted.
func (w *tableWorld) readMoves(table, listed map[string]int32) {
	c := v1beta1.EngineComponent
	if sameTable(table, w.lastTable[c]) {
		return
	}
	if sameTable(listed, w.lastListed[c]) {
		w.note("moved-with-no-listed-change", "the table moved from %v to %v while the listed pods stayed %v: %s", w.lastTable[c], table, listed, w.rows())
	}
	for hash, percent := range table {
		w.report.moves[string(c)+"/"+hash]++
		if percent > 0 && w.lastTable[c][hash] <= 0 && w.weightSeen[hash] > 0 {
			w.note("share-returns-to-a-pod-that-came-back", "%s carries %d%% again after it carried none (listed %v): %s", hash, percent, listed, w.rows())
		}
	}
	for hash, percent := range w.lastTable[c] {
		if percent > 0 && table[hash] <= 0 {
			w.weightSeen[hash]++
		}
	}
}

// readRecord reads the rolled-out record against the pods.
func (w *tableWorld) readRecord() {
	c := v1beta1.EngineComponent
	record := w.latestRecord(c)
	if record == w.lastLatest[c] {
		return
	}
	switch {
	case record == tableNew && w.oldPodRuns():
		w.note("record-names-the-new-revision-while-an-old-pod-still-runs", "the record names %s (previous %s) while an old pod still runs: %s", record, query.RevisionFromName(w.isvc.Status.Components[c].PreviousRolledoutRevision).Hash(), w.rows())
	case record == tableOld && w.lastLatest[c] == tableNew:
		w.note("record-moves-back-to-the-old-revision", "the record moved back to %s: %s", record, w.rows())
	}
	w.lastLatest[c] = record
}

func (w *tableWorld) oldPodRuns() bool {
	for _, inst := range w.engine {
		for _, p := range inst.pods {
			if !p.gone && p.revision == tableOld {
				return true
			}
		}
	}
	return false
}

func anyListed(listed map[string]int32) bool {
	for _, n := range listed {
		if n > 0 {
			return true
		}
	}
	return false
}

func sameTable(a, b map[string]int32) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// rows spells the prefill pods and the table in one line.
func (w *tableWorld) rows() string {
	var parts []string
	for _, inst := range w.engine {
		for _, p := range inst.pods {
			flags := ""
			if p.gateOn {
				flags += "G"
			}
			if p.podReady {
				flags += "R"
			}
			if p.deleting {
				flags += "T"
			}
			if p.gone {
				flags += "X"
			}
			if !p.running {
				flags += "P"
			}
			parts = append(parts, fmt.Sprintf("%d%s:%s[%s]", p.index, runnerLetter(p.runner), p.revision, flags))
		}
	}
	return fmt.Sprintf("tick %d pods %s table %v", w.tick, strings.Join(parts, " "), w.table(v1beta1.EngineComponent))
}

func runnerLetter(r v1beta1.RunnerName) string {
	switch r {
	case v1beta1.RunnerNameLeader:
		return "L"
	case v1beta1.RunnerNameWorker:
		return "w"
	}
	return ""
}

func (w *tableWorld) note(class, format string, args ...any) {
	w.report.counts[class]++
	if _, seen := w.report.first[class]; !seen {
		w.report.first[class] = tableFinding{tick: w.tick, message: fmt.Sprintf(format, args...)}
	}
}

// tableSummary gathers what every configuration found.
type tableSummary struct {
	mu                     sync.Mutex
	configs, ticks, passes int
	totals, seen           map[string]int
	examples               map[string][]string
}

func (s *tableSummary) add(name string, rep tableReport) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.configs++
	s.ticks += rep.ticks
	s.passes += rep.passes
	for class, n := range rep.counts {
		s.totals[class] += n
		s.seen[class]++
		if len(s.examples[class]) < 4 {
			f := rep.first[class]
			s.examples[class] = append(s.examples[class], fmt.Sprintf("[%s] tick %d: %s", name, f.tick, f.message))
		}
	}
}

func (s *tableSummary) log(t *testing.T) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t.Logf("%d configurations, %d ticks, %d passes", s.configs, s.ticks, s.passes)
	classes := make([]string, 0, len(s.totals))
	for class := range s.totals {
		classes = append(classes, class)
	}
	sort.Strings(classes)
	for _, class := range classes {
		t.Logf("%s: %d ticks across %d configurations: %s", class, s.totals[class], s.seen[class], tableReading(class))
		for _, ex := range s.examples[class] {
			t.Logf("  %s", ex)
		}
	}
}

func tableNamed(name string, only []string) bool {
	for _, text := range only {
		if text == "" || strings.Contains(name, text) {
			return true
		}
	}
	return false
}

// TestTrafficTable_EveryTickOfAnInPlacePushNamesOnlyRevisionsThatServe
// drives the traffic writer through the in-place pushes above and fails on
// any tick where the table names a revision nothing serves while another
// revision is listed, leaves a listed revision out, sums to anything but
// 100, strays from the listed pod counts past the deadband, flags the
// wrong revision as latest, moves without the pods, touches the untouched
// side, or stays behind the pods past the next pass; every other picture
// is a known reading, named with what the operator sees.
func TestTrafficTable_EveryTickOfAnInPlacePushNamesOnlyRevisionsThatServe(t *testing.T) {
	full := os.Getenv("OME_TRAFFIC_TABLE_HUNT") == "full"
	only := strings.Split(os.Getenv("OME_TRAFFIC_TABLE_HUNT_ONLY"), ",")
	workers := 4
	if full {
		workers = 16
	}
	admission := make(chan struct{}, workers)
	summary := &tableSummary{totals: map[string]int{}, seen: map[string]int{}, examples: map[string][]string{}}
	t.Cleanup(func() { summary.log(t) })
	for _, cfg := range tableConfigs(full) {
		name := cfg.name()
		if !tableNamed(name, only) {
			continue
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			admission <- struct{}{}
			defer func() { <-admission }()
			w := newTableWorld(t, cfg)
			rep := w.run()
			summary.add(name, rep)
			classes := make([]string, 0, len(rep.counts))
			for class := range rep.counts {
				classes = append(classes, class)
			}
			sort.Strings(classes)
			var parts []string
			for _, class := range classes {
				parts = append(parts, fmt.Sprintf("%s=%d", class, rep.counts[class]))
				if tableFails(class) {
					f := rep.first[class]
					t.Errorf("%s: %d ticks; first at tick %d: %s", class, rep.counts[class], f.tick, f.message)
				}
			}
			t.Logf("%d ticks, %d passes: %s", rep.ticks, rep.passes, strings.Join(parts, " "))
		})
	}
}

// twoByTwo is the smallest reference pair under the drain-first in-place
// strategy with the old pod steady, the endpoints controller a tick behind
// and a restart that shows for one tick.
func twoByTwo() tableConfig {
	return tableConfig{shape: tableShapes(false)[0], drainFirst: true, restart: 1, lag: 1}
}

// twelveByEight is the 12:8 pair under its group, the prefill side pushed
// in place two Instances at a time, with the same kubelet and endpoints
// timings.
func twelveByEight() tableConfig {
	return tableConfig{shape: tableShapes(false)[2], drainFirst: true, restart: 1, lag: 1}
}

// stepUntil steps the world until the condition holds or the bound is
// reached.
func (w *tableWorld) stepUntil(bound int, until func() bool) bool {
	for i := 0; i < bound; i++ {
		if until() {
			return true
		}
		w.step()
	}
	return until()
}

// failOnDefects fails the test on any class the model reads as a defect.
func (w *tableWorld) failOnDefects(t *testing.T) {
	t.Helper()
	for class := range w.report.counts {
		if tableFails(class) {
			t.Fatalf("%s: %s", class, w.report.first[class].message)
		}
	}
}

// TestTrafficTable_ARelabelledPodCarriesNoShareUntilItsRoutedServiceListsIt:
// 12 prefill and 8 decode, the prefill side pushed in place two Instances
// at a time. The pods of Instances 0 and 1 come back on version 2 and
// serve by the gate one tick before the endpoints controller lists them
// in version 2's routed Service. On that tick version 2 carries nothing,
// version 1 keeps 100, and the status names the wait with the two pods.
// One tick later the pods are listed and version 2 takes its share of the
// listed pods, with the wait cleared.
func TestTrafficTable_ARelabelledPodCarriesNoShareUntilItsRoutedServiceListsIt(t *testing.T) {
	w := newTableWorld(t, twelveByEight())
	w.push()
	first, second := w.engine[0].pods[0], w.engine[1].pods[0]
	if !w.stepUntil(40, func() bool { return first.revision == tableNew && first.podReady && second.podReady }) {
		t.Fatalf("Instances 0 and 1 never came back on version 2: %s", w.rows())
	}
	served, listed := w.servedAndListed(v1beta1.EngineComponent)
	if served[tableNew] != 2 || listed[tableNew] != 0 {
		t.Fatalf("the picture must be two version 2 pods serving and unlisted, got served %v listed %v", served, listed)
	}
	if got := w.table(v1beta1.EngineComponent); got[tableNew] != 0 || got[tableOld] != 100 {
		t.Fatalf("an unlisted pod carries a share: %v", got)
	}
	cond := w.isvc.Status.GetCondition(apis.ConditionType(v1beta1.TrafficEndpointsReady))
	if cond == nil || cond.Status != corev1.ConditionFalse || !strings.Contains(cond.Message, "engine revision v2: waiting for endpoints: 2 pods Ready, 0 listed") {
		t.Fatalf("the status does not name the wait: %v", cond)
	}
	w.step()
	_, listed = w.servedAndListed(v1beta1.EngineComponent)
	total := listed[tableOld] + listed[tableNew]
	got := w.table(v1beta1.EngineComponent)
	if listed[tableNew] != 2 || got[tableNew] != 2*100/total || got[tableOld] != 100-2*100/total {
		t.Fatalf("once listed the two pods take their share of the %d listed (%v), got %v", total, listed, got)
	}
	if cond := w.isvc.Status.GetCondition(apis.ConditionType(v1beta1.TrafficEndpointsReady)); cond == nil || cond.Status != corev1.ConditionTrue {
		t.Fatalf("the wait did not clear: %v", cond)
	}
	w.failOnDefects(t)
}

// TestTrafficTable_AnOldPodBetweenTwoExitsCarriesNoShare: 2 prefill and 2
// decode, the prefill side pushed in place one Instance at a time. The
// relabelled pod of Instance 0 serves version 2 by the gate; the roll
// takes Instance 1's old pod out of rotation in the same pass, and that
// pod's runner exits as well. Before the pass that reads the relabelled
// pod listed, the table still names version 1 at 100 with no version 1
// pod serving: the pass is behind the pods, and the picture lasts from
// the gate going on to the endpoints controller's list, two ticks here.
// The pass that reads the pod listed moves everything to version 2, and
// nothing names version 1 again.
func TestTrafficTable_AnOldPodBetweenTwoExitsCarriesNoShare(t *testing.T) {
	w := newTableWorld(t, twoByTwo())
	w.push()
	moved, old := w.engine[0].pods[0], w.engine[1].pods[0]
	if !w.stepUntil(40, func() bool { return moved.revision == tableNew && moved.podReady }) {
		t.Fatalf("Instance 0 never came back on version 2: %s", w.rows())
	}
	if got := w.table(v1beta1.EngineComponent); len(got) != 1 || got[tableOld] != 100 {
		t.Fatalf("with the relabelled pod unlisted the table stands on version 1, got %v", got)
	}
	if n := w.report.counts["stale-until-the-next-pass"]; n != 1 {
		t.Fatalf("the pass must read the relabelled pod serving while the table names version 1 once so far, counted %d: %s", n, w.rows())
	}
	// The old pod's runner exits with the next tick's kubelet, the tick the
	// endpoints controller lists the relabelled pod.
	w.takeRunner(old, 2)
	w.step()
	if old.podReady || old.gateOn {
		t.Fatalf("the old pod must be out of rotation and between two exits: %s", w.rows())
	}
	if n := w.report.counts["stale-until-the-next-pass"]; n != 2 {
		t.Fatalf("before the pass the table must still name version 1 with no serving pod, counted %d: %s", n, w.rows())
	}
	if got := w.table(v1beta1.EngineComponent); len(got) != 1 || got[tableNew] != 100 {
		t.Fatalf("with the relabelled pod listed and the old pod between two exits, version 2 must carry everything, got %v", got)
	}
	w.failOnDefects(t)
	for i := 0; i < 3; i++ {
		w.step()
		if got := w.table(v1beta1.EngineComponent); len(got) != 1 || got[tableNew] != 100 {
			t.Fatalf("tick %d: the table must stay on version 2, got %v: %s", w.tick, got, w.rows())
		}
	}
	w.failOnDefects(t)
}

// TestTrafficTable_TheShareFollowsAnOldPodThatComesAndGoes: 12 prefill
// and 8 decode under their group, the prefill side pushed in place two at
// a time while the last old pod exits every sixth tick and is back two
// ticks later. Every time the endpoints controller lists the pod again
// the old revision's weight is its share of the listed pods, return after
// return: the table follows the endpoints controller's list up and down,
// and nothing holds a share off a pod that came back between two exits.
func TestTrafficTable_TheShareFollowsAnOldPodThatComesAndGoes(t *testing.T) {
	cfg := twelveByEight()
	cfg.oldPod, cfg.up, cfg.down, cfg.offset = oldPodLoops, 4, 2, 0
	w := newTableWorld(t, cfg)
	w.push()
	old := w.engine[len(w.engine)-1].pods[0]
	returns, wasListed := 0, true
	for w.tick < tableMax && !w.done() && old.revision == tableOld {
		w.step()
		_, listed, unlisted := w.servedListedUnlisted(v1beta1.EngineComponent)
		listedNow := old.podReady
		for _, name := range unlisted {
			if name == old.name {
				listedNow = false
			}
		}
		if listedNow && !wasListed {
			returns++
			total := listed[tableOld] + listed[tableNew]
			if got := w.table(v1beta1.EngineComponent)[tableOld]; got != listed[tableOld]*100/total && got != listed[tableOld]*100/total+1 {
				t.Fatalf("tick %d: the old pod is listed again and the old revision carries %d%% for %d of %d listed pods: %s", w.tick, got, listed[tableOld], total, w.rows())
			}
		}
		wasListed = listedNow
	}
	w.failOnDefects(t)
	if returns < 3 {
		t.Fatalf("the old pod must be listed again at least three times before the roll reaches it, counted %d over %d ticks", returns, w.tick)
	}
	t.Logf("the old pod came back %d times over %d ticks; moves %v; classes %v", returns, w.tick, w.report.moves, w.report.counts)
}

// TestTrafficTable_WeightsSumToOneHundredAndEveryListedRevisionHasItsShare:
// 12 prefill pushed in place two at a time while the last old pod exits
// now and then, so the listed pods number 11 as often as 12. At every tick
// the weights sum to 100 and every revision with a listed serving pod has
// a share near its pod count: 7 old and 4 new listed pods read 64 and 36,
// never 63 and 36.
func TestTrafficTable_WeightsSumToOneHundredAndEveryListedRevisionHasItsShare(t *testing.T) {
	cfg := twelveByEight()
	cfg.oldPod, cfg.up, cfg.down, cfg.offset = oldPodLoops, 4, 2, 0
	w := newTableWorld(t, cfg)
	w.push()
	splits, rounded := 0, 0
	for w.tick < tableMax && !w.done() {
		w.step()
		table := w.table(v1beta1.EngineComponent)
		var sum int32
		for _, p := range table {
			sum += p
		}
		if sum != 100 {
			t.Fatalf("tick %d: the weights sum to %d: %v", w.tick, sum, table)
		}
		_, listed := w.servedAndListed(v1beta1.EngineComponent)
		if len(table) == 2 {
			splits++
			total := listed[tableOld] + listed[tableNew]
			if total > 0 && (listed[tableOld]*100)%total != 0 {
				rounded++
			}
		}
	}
	w.failOnDefects(t)
	if splits == 0 || rounded == 0 {
		t.Fatalf("the push must pass through split tables whose shares need rounding (split ticks %d, rounded %d)", splits, rounded)
	}
	t.Logf("%d split ticks, %d of them rounded, over %d ticks", splits, rounded, w.tick)
}

// TestTrafficTable_TheDeadbandKeepsTheTableStillWhileAnOldPodComesAndGoes:
// 12 prefill pushed in place two at a time under a deadband of 10 percent
// while the last old pod exits and returns. Each step of the roll moves
// the shares by more than the band and is written. The old pod's exits
// and returns move the old revision's share by two or three points, and
// the table stands through them. The last step takes the old revision out
// of the table and is written at once, whatever the band.
func TestTrafficTable_TheDeadbandKeepsTheTableStillWhileAnOldPodComesAndGoes(t *testing.T) {
	cfg := twelveByEight()
	cfg.oldPod, cfg.up, cfg.down, cfg.offset, cfg.deadband = oldPodLoops, 4, 2, 0, 10
	w := newTableWorld(t, cfg)
	w.push()
	old := w.engine[len(w.engine)-1].pods[0]
	_, lastListed := w.servedAndListed(v1beta1.EngineComponent)
	lastTable := w.table(v1beta1.EngineComponent)
	flaps, moved := 0, 0
	for w.tick < tableMax && !w.done() {
		w.step()
		_, listed := w.servedAndListed(v1beta1.EngineComponent)
		table := w.table(v1beta1.EngineComponent)
		oldOnly := listed[tableNew] == lastListed[tableNew] && listed[tableOld] != lastListed[tableOld] &&
			listed[tableOld]-lastListed[tableOld] <= 1 && lastListed[tableOld]-listed[tableOld] <= 1 && old.revision == tableOld
		if oldOnly {
			flaps++
			if !sameTable(table, lastTable) {
				moved++
				t.Logf("tick %d: the old pod alone moved the listed pods %v -> %v and the table %v -> %v", w.tick, lastListed, listed, lastTable, table)
			}
		}
		lastListed, lastTable = listed, table
	}
	w.failOnDefects(t)
	if flaps < 3 {
		t.Fatalf("the old pod must come and go at least three times with nothing else moving, counted %d", flaps)
	}
	if moved != 0 {
		t.Fatalf("under a deadband of 10 the table must stand through the old pod's exits and returns; it moved on %d of %d such ticks", moved, flaps)
	}
	if got := w.table(v1beta1.EngineComponent); len(got) != 1 || got[tableNew] != 100 {
		t.Fatalf("the push must end with the new revision alone at 100, got %v", got)
	}
	t.Logf("%d ticks; the old pod alone moved the listed pods on %d ticks and the table on none; moves %v", w.tick, flaps, w.report.moves)
}

// TestTrafficTable_TheRolledOutRecordAdvancesWhileAnOldPodStillRuns records
// what the rolled-out record does. 2 prefill pushed in place one at
// a time: the relabelled pod of Instance 0 is listed on version 2 while
// Instance 1's old pod is out of rotation for its own patch, still running
// version 1. The record names version 2 as the rolled-out revision, with
// version 1 as the previous one, before Instance 1 has moved: the record
// reads one revision with listed pods as the roll's end and does not count
// the pod the roll holds out of rotation.
func TestTrafficTable_TheRolledOutRecordAdvancesWhileAnOldPodStillRuns(t *testing.T) {
	w := newTableWorld(t, twoByTwo())
	w.push()
	old := w.engine[1].pods[0]
	if !w.stepUntil(40, func() bool { return w.latestRecord(v1beta1.EngineComponent) == tableNew }) {
		t.Fatalf("the record never moved to version 2: %s", w.rows())
	}
	cs := w.isvc.Status.Components[v1beta1.EngineComponent]
	if old.revision != tableOld || old.gone {
		t.Fatalf("the record moves while the old pod still runs version 1; here the old pod reads %s: %s", old.revision, w.rows())
	}
	if query.RevisionFromName(cs.PreviousRolledoutRevision).Hash() != tableOld {
		t.Fatalf("the previous revision must read version 1, got %q", cs.PreviousRolledoutRevision)
	}
	if n := w.report.counts["record-names-the-new-revision-while-an-old-pod-still-runs"]; n != 1 {
		t.Fatalf("the early record must be read once, counted %d", n)
	}
	w.run()
	w.failOnDefects(t)
	if w.latestRecord(v1beta1.EngineComponent) != tableNew {
		t.Fatalf("the push must end with the record on version 2, got %q", w.isvc.Status.Components[v1beta1.EngineComponent].LatestRolledoutRevision)
	}
}
