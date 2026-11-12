package coordination

import (
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils"
)

// A prefill/decode service answers a request of one version with a prefill
// and a decode of that version. While a roll runs, each version should
// stay near the fleet's prefill-to-decode ratio, no version should keep
// one side with no partner of its own for longer than a partner takes to
// start, every batch should follow the plan, and the serving capacity of
// each side should never drop below what its budget allows.
//
// This file walks whole rolls of the reference prefill/decode shapes
// through the update gate, one round of starts at a time, under each update
// path a prefill/decode group runs (RecreatePod, SurgeThenDrain, in-place
// and the blueGreen group forms) with and without the chart's prefill/decode
// settings, each roll plain and each with one event at one moment: a pod
// lost on the leading or the trailing side on either version, a node lost
// under one pod of each side, a scale-up of one or both sides, a pause,
// and a second push. It reads every walk by the per-version pictures, the
// stranded side, the batch order and the least serving capacity, and fails
// only where the engine contradicts its own rule: a round where nothing can
// move with Instances left to roll, a consulted start admitted out of the
// plan's order, a side serving fewer Instances than its budget leaves.
// Everything else is reported on the walk's log line.

// pdStage is what one Instance of a side is doing.
type pdStage int

const (
	pdServing  pdStage = iota // its pod serves its version
	pdStarting                // an update attempt toward the target is open
	pdCreating                // a scale-up is building it on the target
	pdDown                    // its only pod is gone; a repair rebuilds it on its version
)

// pdRow is one Instance of a side in the walk: the version it serves or
// moves toward, the version its old pod still serves while a surge is open,
// its stage and how many rounds the stage has left.
type pdRow struct {
	version int
	from    int
	stage   pdStage
	left    int
	surge   bool // the open attempt keeps the old pod serving until the new one serves
	lost    bool // a fault took its pod and it has not served since
}

// pdSide is one side of the pair as the walk rolls it.
type pdSide struct {
	comp      v1beta1.ComponentType
	strategy  v1beta1.UpdateStrategyType
	mechanism workloadtypes.UpdateStrategyType
	budget    string // the side's own maxUnavailable, or its maxSurge under SurgeThenDrain
	target    int    // the version the roll moves toward
	rows      []pdRow
}

func (s *pdSide) desired() int32 { return int32(len(s.rows)) }

func (s *pdSide) surges() bool { return s.strategy == v1beta1.UpdateStrategySurgeThenDrain }

// inRotation counts the Instances whose pod serves version v: a serving
// row of v, or a surging row whose old pod still serves v.
func (s *pdSide) inRotation(v int) int32 {
	var n int32
	for _, r := range s.rows {
		if (r.stage == pdServing && r.version == v) || (r.stage == pdStarting && r.surge && r.from == v) {
			n++
		}
	}
	return n
}

// projected counts the Instances with a pod of version v present: serving,
// starting toward it, being created on it, or down with its pod rebuilt.
func (s *pdSide) projected(v int) int32 {
	var n int32
	for _, r := range s.rows {
		if r.version == v {
			n++
		}
	}
	return n
}

// serving counts the Instances with a pod in rotation on any version.
func (s *pdSide) serving() int32 {
	var n int32
	for _, r := range s.rows {
		if r.stage == pdServing || (r.stage == pdStarting && r.surge) {
			n++
		}
	}
	return n
}

// started counts the Instances on the target or moving toward it, the
// batch step's reading of a side's progress.
func (s *pdSide) started(target int) int32 {
	var n int32
	for _, r := range s.rows {
		if r.version == target && r.stage != pdCreating {
			n++
		}
	}
	return n
}

// settled counts the Instances the batch step reads as served on the
// target: promoted rows, their pod up or being rebuilt.
func (s *pdSide) settled(target int) int32 {
	var n int32
	for _, r := range s.rows {
		if r.version == target && (r.stage == pdServing || r.stage == pdDown) {
			n++
		}
	}
	return n
}

// creating counts the Instances a scale-up is still building.
func (s *pdSide) creating() int32 {
	var n int32
	for _, r := range s.rows {
		if r.stage == pdCreating {
			n++
		}
	}
	return n
}

// taken counts the Instances a fault took that are not back in rotation.
func (s *pdSide) taken() int32 {
	var n int32
	for _, r := range s.rows {
		if r.lost {
			n++
		}
	}
	return n
}

// ownBudget is the side's own budget of its kind against its count.
func (s *pdSide) ownBudget() int32 {
	return utils.ScaledCountFromIntOrString(ptr.To(intstr.Parse(s.budget)), s.desired(), true)
}

// attemptsOpen counts what the per-Component budget charges before one
// more start: open surges against a surge budget; open drains, in-place
// patches and the repairs of new-version pods against an unavailability
// budget. An old Instance whose pod is gone carries no attempt and is not
// charged. A side that surges names no unavailability budget, so a rebuild
// of its lost Instance is never charged.
func (s *pdSide) attemptsOpen() int32 {
	var n int32
	for _, r := range s.rows {
		switch {
		case s.surges() && r.stage == pdStarting && r.surge:
			n++
		case !s.surges() && r.stage == pdStarting:
			n++
		case !s.surges() && r.stage == pdDown && r.version == s.target:
			n++
		}
	}
	return n
}

// fixture renders the side as its replica publishes it toward target, and
// says how many promoted and how many old Instances have lost their pod.
func (s *pdSide) fixture(target int) (m batchMemberFixture, newGone, oldGone int32) {
	m = batchMemberFixture{desired: s.desired(), strategy: s.strategy}
	if s.surges() {
		m.surge = s.budget
	} else {
		m.step = s.budget
	}
	for _, r := range s.rows {
		switch {
		case r.stage == pdCreating:
			m.creating++
		case r.version != target && r.stage == pdDown:
			oldGone++
		case r.version != target:
		case r.stage == pdServing:
			m.promoted++
		case r.stage == pdDown:
			m.promoted++
			newGone++
		case r.surge:
			m.surging++
		case s.mechanism == workloadtypes.UpdateStrategyInPlaceIfPossible:
			m.inPlace++
		default:
			m.inFlight++
		}
	}
	return m, newGone, oldGone
}

// oldPodGone takes the only pod of one untouched Instance away, as the
// replica publishes the loss: the row keeps its old revision with no pod
// and the counters of Ready and serving Instances drop by one.
func oldPodGone(ir *v1beta1.InferenceReplica) {
	for i := range ir.Status.InstanceStatuses {
		row := &ir.Status.InstanceStatuses[i]
		if row.Phase == v1beta1.OMENativeInstanceReady && row.RunningRevision == ir.Status.CurrentRevision && row.Operation == nil && row.TargetRevision == "" && row.PodCount > 0 {
			row.Phase, row.PodCount, row.ServingPodCount = v1beta1.OMENativeInstancePending, 0, 0
			ir.Status.ReadyReplicas--
			ir.Status.ServingReplicas--
			return
		}
	}
	panic("no untouched Instance with a pod to lose")
}

// pdShape is one reference shape: its prefill and decode counts.
type pdShape struct{ prefill, decode int32 }

func (s pdShape) String() string { return fmt.Sprintf("%d:%d", s.prefill, s.decode) }

// pdShapes are the reference shapes of a prefill/decode rollout group.
var pdShapes = []pdShape{{8, 8}, {12, 8}, {24, 8}, {30, 10}, {6, 2}, {8, 2}}

// pdPath is one update path with the reference group settings for it and
// each side's own budget.
type pdPath struct {
	name      string
	strategy  v1beta1.UpdateStrategyType
	mechanism workloadtypes.UpdateStrategyType
	group     batchGroupFixture
	budget    string
}

// pdPaths are the update paths of the reference prefill/decode groups: both
// sides at 10 percent of their own count under a rollingUpdate group at
// the same budget and the reference tolerance (20), and the two
// blueGreen forms (RecreatePod at 20 percent, and SurgeThenDrain at 100
// percent, which the group's default quarter caps).
var pdPaths = []pdPath{
	{"RecreatePod", v1beta1.UpdateStrategyRecreatePod, workloadtypes.UpdateStrategyRecreatePod, batchGroupFixture{maxUnavailable: "10%", tolerance: ptr.To(int32(20))}, "10%"},
	{"SurgeThenDrain", v1beta1.UpdateStrategySurgeThenDrain, workloadtypes.UpdateStrategySurgeThenDrain, batchGroupFixture{maxUnavailable: "0", maxSurge: "10%", tolerance: ptr.To(int32(20))}, "10%"},
	{"InPlace", v1beta1.UpdateStrategyInPlaceIfPossible, workloadtypes.UpdateStrategyInPlaceIfPossible, batchGroupFixture{maxUnavailable: "10%", tolerance: ptr.To(int32(20))}, "10%"},
	{"BlueGreenRecreate20", v1beta1.UpdateStrategyRecreatePod, workloadtypes.UpdateStrategyRecreatePod, batchGroupFixture{blueGreen: true, tolerance: ptr.To(int32(20))}, "20%"},
	{"BlueGreenSurge100", v1beta1.UpdateStrategySurgeThenDrain, workloadtypes.UpdateStrategySurgeThenDrain, batchGroupFixture{blueGreen: true, tolerance: ptr.To(int32(20))}, "100%"},
}

// pdSetting is the operator's prefill/decode setting the walk runs under.
type pdSetting struct {
	name     string
	defaults GroupDefaults
}

var pdSettings = []pdSetting{
	{"chart", keepRatioDecoderFirst(20)},
	{"none", GroupDefaults{}},
}

// pdFault is one event the walk fires at one moment.
type pdFault string

const (
	pdNoFault        pdFault = "none"
	pdLossNewLeading pdFault = "new pod lost on the leading side"
	pdLossNewTrail   pdFault = "new pod lost on the trailing side"
	pdLossOldLeading pdFault = "old pod lost on the leading side"
	pdLossOldTrail   pdFault = "old pod lost on the trailing side"
	pdNodeLoss       pdFault = "node lost under one pod of each side"
	pdScaleBoth      pdFault = "both sides scaled up a quarter"
	pdScalePrefill   pdFault = "prefill scaled up a quarter"
	pdPause          pdFault = "paused three rounds"
	pdSecondPush     pdFault = "version 3 pushed"
)

var pdFaults = []pdFault{pdNoFault, pdLossNewLeading, pdLossNewTrail, pdLossOldLeading, pdLossOldTrail, pdNodeLoss, pdScaleBoth, pdScalePrefill, pdPause, pdSecondPush}

// pdPicture is one version's Instances on each side at one moment.
type pdPicture struct{ prefill, decode int32 }

func (p pdPicture) String() string { return fmt.Sprintf("%d:%d", p.prefill, p.decode) }

func (p pdPicture) empty() bool { return p.prefill == 0 && p.decode == 0 }

func (p pdPicture) oneSided() bool { return (p.prefill > 0) != (p.decode > 0) }

// pastTheLine is how far a picture stands past the fleet's line, in
// Instances of the side that is over.
func (p pdPicture) pastTheLine(fleet pdShape) float64 {
	overPrefill := float64(p.prefill) - float64(p.decode)*float64(fleet.prefill)/float64(fleet.decode)
	overDecode := float64(p.decode) - float64(p.prefill)*float64(fleet.decode)/float64(fleet.prefill)
	return math.Max(overPrefill, overDecode)
}

// pdMoment is the fleet at one time of the walk: after a round's starts
// opened (open) or after they served (served), with each version's picture
// in rotation, the newest version's projection, the fleet's counts and each
// side's serving count.
type pdMoment struct {
	round     int
	served    bool
	paused    bool
	fleet     pdShape
	rotation  map[int]pdPicture
	projected pdPicture
	target    int
	serving   pdPicture
	taken     pdPicture
	creating  pdPicture
	// atBoundary is a served moment with no attempt open on either side
	// whose started counts sit on the same planned batch of both sides.
	atBoundary bool
	note       string
}

func (m pdMoment) time() float64 {
	if m.served {
		return float64(m.round)
	}
	return float64(m.round - 1)
}

func (m pdMoment) String() string {
	when := "open"
	if m.served {
		when = "served"
	}
	versions := make([]int, 0, len(m.rotation))
	for v := range m.rotation {
		versions = append(versions, v)
	}
	sort.Ints(versions)
	parts := make([]string, 0, len(versions))
	for _, v := range versions {
		if p := m.rotation[v]; !p.empty() || v == m.target {
			parts = append(parts, fmt.Sprintf("v%d=%s", v, p))
		}
	}
	s := fmt.Sprintf("r%d %s new=%s %s", m.round, when, m.projected, strings.Join(parts, " "))
	if m.note != "" {
		s += " [" + m.note + "]"
	}
	return s
}

// pdStart is one start the walk opened: which side, by consult or exempt
// from the gate, and the batch step's reading of both sides just before it.
type pdStart struct {
	round    int
	comp     v1beta1.ComponentType
	index    int
	exempt   bool
	breaches []string
}

// pdWalk is one whole roll.
type pdWalk struct {
	moments []pdMoment
	starts  []pdStart
	holds   []string
	halt    string
	rounds  int
}

func (w pdWalk) String() string {
	out := make([]string, 0, len(w.moments))
	for _, m := range w.moments {
		out = append(out, m.String())
	}
	return strings.Join(out, "; ")
}

// servedPictures is the newest version's picture after every round, the
// shape of the walk the plan produces.
func (w pdWalk) servedPictures() string {
	out := make([]string, 0, len(w.moments)/2)
	for _, m := range w.moments {
		if m.served {
			out = append(out, m.projected.String())
		}
	}
	return strings.Join(out, " ")
}

// startsByRound lists the Instances each round started, side by side.
func (w pdWalk) startsByRound() string {
	byRound := map[int]map[v1beta1.ComponentType][]string{}
	for _, s := range w.starts {
		if byRound[s.round] == nil {
			byRound[s.round] = map[v1beta1.ComponentType][]string{}
		}
		byRound[s.round][s.comp] = append(byRound[s.round][s.comp], fmt.Sprint(s.index))
	}
	var out []string
	for r := 1; r <= w.rounds; r++ {
		parts := []string{}
		for _, c := range []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent} {
			if idx := byRound[r][c]; len(idx) > 0 {
				parts = append(parts, fmt.Sprintf("%s[%s]", c, strings.Join(idx, ",")))
			}
		}
		out = append(out, fmt.Sprintf("r%d %s", r, strings.Join(parts, " ")))
	}
	return strings.Join(out, "; ")
}

// pdCell is one cell of the grid: a shape on a path under a setting, with
// one fault at one served moment.
type pdCell struct {
	shape   pdShape
	path    pdPath
	setting pdSetting
	fault   pdFault
	moment  int // the served moment the fault fires after; 0 means none
}

func (c pdCell) String() string {
	s := fmt.Sprintf("%s/%s/%s", c.shape, c.path.name, c.setting.name)
	if c.fault != pdNoFault {
		s += fmt.Sprintf("/%s after round %d", c.fault, c.moment)
	}
	return s
}

// pdDriver walks one cell.
type pdDriver struct {
	t       *testing.T
	cell    pdCell
	engine  pdSide
	decoder pdSide
	target  int
	paused  int
	walk    pdWalk
}

func newPDDriver(t *testing.T, cell pdCell) *pdDriver {
	side := func(comp v1beta1.ComponentType, n int32) pdSide {
		s := pdSide{comp: comp, strategy: cell.path.strategy, mechanism: cell.path.mechanism, budget: cell.path.budget, target: 2}
		for i := int32(0); i < n; i++ {
			s.rows = append(s.rows, pdRow{version: 1, stage: pdServing})
		}
		return s
	}
	return &pdDriver{t: t, cell: cell, engine: side(v1beta1.EngineComponent, cell.shape.prefill), decoder: side(v1beta1.DecoderComponent, cell.shape.decode), target: 2}
}

func (d *pdDriver) sides() map[v1beta1.ComponentType]*pdSide {
	return map[v1beta1.ComponentType]*pdSide{v1beta1.EngineComponent: &d.engine, v1beta1.DecoderComponent: &d.decoder}
}

func (d *pdDriver) fleet() pdShape {
	return pdShape{prefill: d.engine.desired(), decode: d.decoder.desired()}
}

func (d *pdDriver) done() bool {
	for _, s := range d.sides() {
		for _, r := range s.rows {
			if r.version != d.target || r.stage != pdServing {
				return false
			}
		}
	}
	return true
}

// order is the order the two sides are asked in each round: the named side
// first, else the decoder.
func (d *pdDriver) order() []v1beta1.ComponentType {
	if d.cell.setting.defaults.PrefillDecode.FirstSide == v1beta1.EngineComponent {
		return []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}
	}
	return []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent}
}

// ask asks the update gate about one more start of comp on the fleet
// as the replicas publish it now.
func (d *pdDriver) ask(comp v1beta1.ComponentType) (bool, v1beta1.RolloutHoldGate, string) {
	t := d.t
	fixtures := map[v1beta1.ComponentType]batchMemberFixture{}
	gone := map[v1beta1.ComponentType][2]int32{}
	for c, s := range d.sides() {
		m, newGone, oldGone := s.fixture(d.target)
		fixtures[c] = m
		gone[c] = [2]int32{newGone, oldGone}
	}
	isvc := batchService(d.cell.path.group, fixtures)
	var objs []*v1beta1.InferenceReplica
	for c, m := range fixtures {
		ir := batchReplica(c, m)
		for i := int32(0); i < gone[c][0]; i++ {
			promotedPodGone(ir)
		}
		for i := int32(0); i < gone[c][1]; i++ {
			oldPodGone(ir)
		}
		objs = append(objs, ir)
	}
	var clientObjs []client.Object
	for _, ir := range objs {
		clientObjs = append(clientObjs, ir)
	}
	c := fake.NewClientBuilder().WithScheme(pairingScheme(t)).WithObjects(clientObjs...).Build()
	return batchGateWith(t, isvc, c, comp, d.sides()[comp].mechanism, 0, d.cell.setting.defaults)
}

// nextCandidate is the Instance the side's next start takes: the lowest
// index still off the target with nothing open on it.
func (s *pdSide) nextCandidate(target int) int {
	for i, r := range s.rows {
		if r.version != target && r.stage == pdServing {
			return i
		}
	}
	return -1
}

// open opens a start on row i toward the target: a surge keeps the old pod
// serving, every other mechanism takes it out of rotation.
func (d *pdDriver) open(s *pdSide, i int, rounds int) {
	r := &s.rows[i]
	r.from = r.version
	r.version = d.target
	r.stage = pdStarting
	r.left = rounds
	r.surge = s.surges()
}

// steps resolves both sides' steps as the batch step reads them, and the
// planned marks when the setting plans them.
func (d *pdDriver) marks() (prefill, decode []int32, prefillStep, decodeStep int32) {
	step := func(s *pdSide) int32 {
		pacing := groupPacing(&batchService(d.cell.path.group, nil).Spec.Rollout.Groups[0], 2, d.cell.setting.defaults)
		if s.surges() {
			own := utils.ScaledCountFromIntOrString(ptr.To(intstr.Parse(s.budget)), s.desired(), true)
			return min32(MaxSurgeBudget(pacing, s.desired()), own)
		}
		own := utils.ScaledCountFromIntOrString(ptr.To(intstr.Parse(s.budget)), s.desired(), true)
		return min32(MaxUnavailableBudget(pacing, s.desired()), own)
	}
	prefillStep, decodeStep = step(&d.engine), step(&d.decoder)
	if d.cell.setting.defaults.PrefillDecode.KeepRatio {
		tol := d.cell.path.group.tolerance
		if tol == nil {
			tol = d.cell.setting.defaults.RatioTolerancePercent
		}
		if tol != nil {
			prefill, decode = pairedBatchPlan(d.engine.desired(), d.decoder.desired(), prefillStep, decodeStep, *tol)
		}
	}
	return prefill, decode, prefillStep, decodeStep
}

// mark is the cumulative count batch b should have started on a side.
func pdMark(marks []int32, step, desired, b int32) int32 {
	if b <= 0 {
		return 0
	}
	if marks != nil {
		if int(b) > len(marks) {
			return desired
		}
		return min32(marks[b-1], desired)
	}
	return min32(b*step, desired)
}

// batchOf is the batch a side's next start belongs to.
func pdBatchOf(started int32, marks []int32, step, desired int32) int32 {
	for b := int32(1); ; b++ {
		mark := pdMark(marks, step, desired, b)
		if mark > started {
			return b
		}
		if mark >= desired {
			return b + 1
		}
	}
}

// orderBreaches reads one start of comp against the plan the way reading O
// does: every side must have served the mark of the batch before, and the
// other side's start of a batch waits for the named side's batch.
func (d *pdDriver) orderBreaches(comp v1beta1.ComponentType) []string {
	pm, dm, ps, ds := d.marks()
	type side struct {
		s     *pdSide
		marks []int32
		step  int32
	}
	sides := map[v1beta1.ComponentType]side{
		v1beta1.EngineComponent:  {&d.engine, pm, ps},
		v1beta1.DecoderComponent: {&d.decoder, dm, ds},
	}
	me := sides[comp]
	b := pdBatchOf(me.s.started(d.target), me.marks, me.step, me.s.desired())
	var out []string
	for c, x := range sides {
		if mark := pdMark(x.marks, x.step, x.s.desired(), b-1); x.s.settled(d.target) < mark {
			out = append(out, fmt.Sprintf("%s batch %d opened with %s batch %d at %d of %d served", comp, b, c, b-1, x.s.settled(d.target), mark))
		}
	}
	if first := d.cell.setting.defaults.PrefillDecode.FirstSide; first != "" && comp != first {
		x := sides[first]
		if mark := pdMark(x.marks, x.step, x.s.desired(), b); x.s.settled(d.target) < mark {
			out = append(out, fmt.Sprintf("%s batch %d opened before %s batch %d served (%d of %d)", comp, b, first, b, x.s.settled(d.target), mark))
		}
	}
	sort.Strings(out)
	return out
}

// moment records the fleet now.
func (d *pdDriver) moment(round int, served bool, note string) {
	m := pdMoment{round: round, served: served, paused: d.paused > 0, fleet: d.fleet(), rotation: map[int]pdPicture{}, target: d.target, note: note}
	for v := 1; v <= d.target; v++ {
		m.rotation[v] = pdPicture{prefill: d.engine.inRotation(v), decode: d.decoder.inRotation(v)}
	}
	m.projected = pdPicture{prefill: d.engine.projected(d.target), decode: d.decoder.projected(d.target)}
	m.serving = pdPicture{prefill: d.engine.serving(), decode: d.decoder.serving()}
	m.taken = pdPicture{prefill: d.engine.taken(), decode: d.decoder.taken()}
	m.creating = pdPicture{prefill: d.engine.creating(), decode: d.decoder.creating()}
	if served {
		m.atBoundary = d.atBoundary()
	}
	d.walk.moments = append(d.walk.moments, m)
}

// atBoundary says whether both sides have started exactly the counts the
// plan names for one and the same batch, with nothing open.
func (d *pdDriver) atBoundary() bool {
	pm, dm, _, _ := d.marks()
	if pm == nil {
		return false
	}
	for _, s := range d.sides() {
		for _, r := range s.rows {
			if r.stage != pdServing {
				return false
			}
		}
	}
	p, dd := d.engine.started(d.target), d.decoder.started(d.target)
	for b := range pm {
		if pm[b] == p && dm[b] == dd {
			return true
		}
	}
	return false
}

// leading is the side whose started share is larger, the named side on a
// tie, else the decoder.
func (d *pdDriver) leading() (lead, trail *pdSide) {
	e := float64(d.engine.started(d.target)) / float64(d.engine.desired())
	dd := float64(d.decoder.started(d.target)) / float64(d.decoder.desired())
	switch {
	case e > dd:
		return &d.engine, &d.decoder
	case dd > e:
		return &d.decoder, &d.engine
	case d.cell.setting.defaults.PrefillDecode.FirstSide == v1beta1.EngineComponent:
		return &d.engine, &d.decoder
	}
	return &d.decoder, &d.engine
}

// lose takes the pod of one Instance of s away: the newest promoted one
// when new is set, else the oldest Instance still on an old version. It
// reports whether there was one to lose.
func (d *pdDriver) lose(s *pdSide, new bool, rounds int) bool {
	if new {
		for i := len(s.rows) - 1; i >= 0; i-- {
			if r := &s.rows[i]; r.stage == pdServing && r.version == d.target {
				r.stage, r.left, r.lost = pdDown, rounds, true
				return true
			}
		}
		return false
	}
	for i := range s.rows {
		if r := &s.rows[i]; r.stage == pdServing && r.version != d.target {
			r.stage, r.left, r.lost = pdDown, rounds, true
			return true
		}
	}
	return false
}

// scale grows a side by n Instances created on the target.
func (d *pdDriver) scale(s *pdSide, n int32) {
	for i := int32(0); i < n; i++ {
		s.rows = append(s.rows, pdRow{version: d.target, stage: pdCreating, left: 1})
	}
}

// fire fires the cell's fault and says what it did, or that it had nothing
// to act on at this moment.
func (d *pdDriver) fire() string {
	lead, trail := d.leading()
	quarter := func(n int32) int32 { return (n + 3) / 4 }
	switch d.cell.fault {
	case pdLossNewLeading:
		if d.lose(lead, true, 1) {
			return fmt.Sprintf("a version %d %s pod lost", d.target, lead.comp)
		}
	case pdLossNewTrail:
		if d.lose(trail, true, 1) {
			return fmt.Sprintf("a version %d %s pod lost", d.target, trail.comp)
		}
	case pdLossOldLeading:
		if d.lose(lead, false, 1) {
			return fmt.Sprintf("an old %s pod lost", lead.comp)
		}
	case pdLossOldTrail:
		if d.lose(trail, false, 1) {
			return fmt.Sprintf("an old %s pod lost", trail.comp)
		}
	case pdNodeLoss:
		var did []string
		for _, s := range []*pdSide{&d.engine, &d.decoder} {
			if d.lose(s, false, 2) {
				did = append(did, fmt.Sprintf("an old %s pod", s.comp))
			} else if d.lose(s, true, 2) {
				did = append(did, fmt.Sprintf("a version %d %s pod", d.target, s.comp))
			}
		}
		if len(did) > 0 {
			return "node lost under " + strings.Join(did, " and ")
		}
	case pdScaleBoth:
		p := quarter(d.engine.desired())
		newPrefill := d.engine.desired() + p
		newDecode := int32(math.Ceil(float64(newPrefill) * float64(d.cell.shape.decode) / float64(d.cell.shape.prefill)))
		d.scale(&d.engine, p)
		d.scale(&d.decoder, newDecode-d.decoder.desired())
		return fmt.Sprintf("scaled to %d:%d", newPrefill, newDecode)
	case pdScalePrefill:
		p := quarter(d.engine.desired())
		d.scale(&d.engine, p)
		return fmt.Sprintf("prefill scaled to %d", d.engine.desired())
	case pdPause:
		d.paused = 3
		return "paused"
	case pdSecondPush:
		d.target++
		for _, s := range d.sides() {
			s.target = d.target
			for i := range s.rows {
				if r := &s.rows[i]; (r.stage == pdStarting || r.stage == pdCreating) && r.version == d.target-1 {
					r.version = d.target
				}
			}
		}
		return fmt.Sprintf("version %d pushed", d.target)
	}
	return ""
}

// run walks the cell to its end or to a round where nothing moves.
func (d *pdDriver) run() pdWalk {
	t := d.t
	fired := d.cell.fault == pdNoFault
	for round := 1; round <= 96; round++ {
		if d.done() {
			d.walk.rounds = round - 1
			return d.walk
		}
		note := ""
		if !fired && d.cell.moment == round-1 {
			fired = true
			note = d.fire()
			if note == "" {
				note = "nothing to act on"
			}
		}
		moved := false
		holds := map[string]bool{}
		if d.paused == 0 {
			for _, comp := range d.order() {
				s := d.sides()[comp]
				// Instances out of rotation first: a lost old pod is rebuilt
				// on the target with no consult, under the side's own budget.
				for i := range s.rows {
					if r := &s.rows[i]; r.stage == pdDown && r.version != d.target && r.left > 0 {
						if !s.surges() && s.attemptsOpen() >= s.ownBudget() {
							// The side's own budget is spent: the Create pass refills
							// the Instance on its own version instead, and the roll
							// takes it later like any other.
							holds[fmt.Sprintf("%s: the rebuild of a lost old Instance waits on the side's own budget; refilled on its own version", comp)] = true
							r.left = -r.left
							continue
						}
						d.walk.starts = append(d.walk.starts, pdStart{round: round, comp: comp, index: i, exempt: true, breaches: d.orderBreaches(comp)})
						rounds := r.left
						d.open(s, i, rounds)
						r.surge = false
						moved = true
					}
				}
				for {
					i := s.nextCandidate(d.target)
					if i < 0 {
						break
					}
					if s.attemptsOpen() >= s.ownBudget() {
						holds[fmt.Sprintf("%s: per-Component budget %d exhausted", comp, s.ownBudget())] = true
						break
					}
					allowed, gate, reason := d.ask(comp)
					if !allowed {
						holds[fmt.Sprintf("%s: %s: %s", comp, gate, reason)] = true
						break
					}
					d.walk.starts = append(d.walk.starts, pdStart{round: round, comp: comp, index: i, breaches: d.orderBreaches(comp)})
					d.open(s, i, 1)
					moved = true
				}
			}
		}
		for h := range holds {
			d.walk.holds = append(d.walk.holds, fmt.Sprintf("r%d %s", round, h))
		}
		d.moment(round, false, note)
		// The round ends: open attempts serve, created Instances come up,
		// rebuilt pods return, a pause runs down.
		advanced := false
		for _, s := range d.sides() {
			for i := range s.rows {
				r := &s.rows[i]
				switch r.stage {
				case pdStarting, pdCreating, pdDown:
					if r.left < 0 {
						r.left++ // a refill on its own version
					} else {
						r.left--
					}
					advanced = true
					if r.left == 0 {
						r.stage, r.from, r.surge, r.left, r.lost = pdServing, 0, false, 0, false
					}
				}
			}
		}
		if d.paused > 0 {
			d.paused--
			advanced = true
		}
		d.moment(round, true, "")
		if !moved && !advanced {
			reasons := make([]string, 0, len(holds))
			for h := range holds {
				reasons = append(reasons, h)
			}
			sort.Strings(reasons)
			d.walk.halt = fmt.Sprintf("round %d moved nothing: %s", round, strings.Join(reasons, "; "))
			d.walk.rounds = round
			return d.walk
		}
	}
	t.Fatalf("%s: the roll did not finish in 96 rounds: %s", d.cell, d.walk)
	return d.walk
}

// pdWindowRounds is how long a version may stand one-sided, or an old
// version past the line, before the reading fails it: the missing side's
// start window, 45 s against rounds of about 20 s.
const pdWindowRounds = 2.25

// pdFinding is one picture of a walk that breaks a reading.
type pdFinding struct {
	kind string // P-new, P-old, T, O, capacity, halt
	text string
}

func (f pdFinding) String() string { return f.kind + ": " + f.text }

// readWalk reads a walk for the findings below. tolerance is the
// decode share the keep-ratio reading allows a version past the line; the
// one-Instance line is read beside it.
func readWalk(cell pdCell, w pdWalk) []pdFinding {
	var out []pdFinding
	add := func(kind, format string, args ...any) {
		out = append(out, pdFinding{kind: kind, text: fmt.Sprintf(format, args...)})
	}
	if w.halt != "" {
		add("halt", "%s", w.halt)
	}
	allowance := func(fleet pdShape) float64 {
		if !cell.setting.defaults.PrefillDecode.KeepRatio {
			return 1
		}
		return math.Max(1, math.Ceil(float64(*cell.path.group.tolerance)*float64(fleet.decode)/100))
	}
	// The newest version's projection, at any moment that lasts.
	worstNew := 0.0
	var worstNewMoment pdMoment
	for i, m := range w.moments {
		var lasts float64
		if i+1 < len(w.moments) {
			lasts = w.moments[i+1].time() - m.time()
		}
		if lasts <= 0 {
			continue
		}
		if over := m.projected.pastTheLine(m.fleet); over > worstNew+1e-9 {
			worstNew, worstNewMoment = over, m
		}
	}
	if worstNew > 1.0001 {
		kind := "P-new-over-one"
		if worstNew > allowance(worstNewMoment.fleet)+1e-9 {
			kind = "P-new"
		}
		add(kind, "version %d reads %s against a fleet of %s after round %d: %.2f Instance(s) past the line (allowance %.0f)",
			worstNewMoment.target, worstNewMoment.projected, worstNewMoment.fleet, worstNewMoment.round, worstNew, allowance(worstNewMoment.fleet))
	}
	// The plan's own promise, between batches: the gap between the two
	// sides' started shares stays within the tolerance where any batch fits
	// it; a side too small for its tolerance (two decoders at 20 percent)
	// is walked by the batch closest to the line instead, read cell by
	// cell.
	if cell.setting.defaults.PrefillDecode.KeepRatio {
		tol := float64(*cell.path.group.tolerance) / 100
		for i, m := range w.moments {
			if !m.served || !m.atBoundary || i+1 >= len(w.moments) {
				continue
			}
			skew := math.Abs(float64(m.projected.prefill)/float64(m.fleet.prefill) - float64(m.projected.decode)/float64(m.fleet.decode))
			if skew > tol+1e-9 {
				add("P-share", "round %d: version %d at %s on a fleet of %s between batches: the shares are %.0f points apart against a tolerance of %.0f",
					m.round, m.target, m.projected, m.fleet, skew*100, tol*100)
			}
		}
	}
	// Older versions past the line, and any version one-sided, for longer
	// than the window.
	type standing struct {
		rounds float64
		first  pdMoment
		pic    pdPicture
	}
	oldOver := map[int]*standing{}
	oneSided := map[int]*standing{}
	end := func(kind string, v int, st map[int]*standing) {
		s := st[v]
		if s == nil {
			return
		}
		if s.rounds > pdWindowRounds {
			switch kind {
			case "P-old":
				add(kind, "version %d holds %s in rotation against a fleet of %s from round %d for %.2f rounds: %.2f Instance(s) past the line",
					v, s.pic, s.first.fleet, s.first.round, s.rounds, s.pic.pastTheLine(s.first.fleet))
			default:
				have, missing := "prefill", "decode"
				if s.pic.decode > 0 {
					have, missing = "decode", "prefill"
				}
				add(kind, "version %d stands %.2f rounds from round %d with %s in rotation and no %s of its own (%s at first)",
					v, s.rounds, s.first.round, have, missing, s.pic)
			}
		}
		delete(st, v)
	}
	for i, m := range w.moments {
		var lasts float64
		if i+1 < len(w.moments) {
			lasts = w.moments[i+1].time() - m.time()
		}
		for v, p := range m.rotation {
			switch {
			case v != m.target && !p.empty() && p.pastTheLine(m.fleet) > 1.0001:
				if oldOver[v] == nil {
					oldOver[v] = &standing{first: m, pic: p}
				}
				oldOver[v].rounds += lasts
			case lasts > 0:
				end("P-old", v, oldOver)
			}
			switch {
			case p.oneSided():
				if oneSided[v] == nil {
					oneSided[v] = &standing{first: m, pic: p}
				}
				oneSided[v].rounds += lasts
			case lasts > 0:
				end("T", v, oneSided)
			}
		}
	}
	for v := range oldOver {
		end("P-old", v, oldOver)
	}
	for v := range oneSided {
		end("T", v, oneSided)
	}
	// The batch order of every start, where the batch step governs; where
	// an in-place pair rolls under the lockstep alone, the lockstep's own
	// bound: one side's started share may lead the other's by the tolerance
	// and one step of its own, no more.
	if pdBatchStepGoverns(cell) {
		for _, s := range w.starts {
			for _, b := range s.breaches {
				kind := "O"
				if s.exempt {
					kind = "O-exempt"
				}
				add(kind, "round %d: %s", s.round, b)
			}
		}
	} else {
		worst, worstAllowed := 0.0, 0.0
		var worstMoment pdMoment
		for _, m := range w.moments {
			if m.served {
				continue
			}
			e := float64(m.projected.prefill) / float64(m.fleet.prefill) * 100
			dd := float64(m.projected.decode) / float64(m.fleet.decode) * 100
			lead, step := e-dd, float64(pdSideBudget(cell, m.fleet.prefill))/float64(m.fleet.prefill)*100
			if dd > e {
				lead, step = dd-e, float64(pdSideBudget(cell, m.fleet.decode))/float64(m.fleet.decode)*100
			}
			allowed := float64(*cell.path.group.tolerance) + step
			if lead-allowed > worst-worstAllowed+1e-9 {
				worst, worstAllowed, worstMoment = lead, allowed, m
			}
		}
		if worst > worstAllowed+1e-9 {
			add("N", "round %d: version %d at %s on a fleet of %s: one side leads by %.1f points against the tolerance plus one step (%.1f)",
				worstMoment.round, worstMoment.target, worstMoment.projected, worstMoment.fleet, worst, worstAllowed)
		}
	}
	// The least serving capacity of each side: its count less what its
	// budget allows out and less what a fault took; a surging side keeps
	// every pod until its replacement serves.
	for i, m := range w.moments {
		var lasts float64
		if i+1 < len(w.moments) {
			lasts = w.moments[i+1].time() - m.time()
		}
		if lasts <= 0 && i+1 < len(w.moments) {
			continue
		}
		check := func(name string, serving, desired, taken, budget int32) {
			least := desired - taken - budget
			if serving < least {
				add("capacity", "round %d: %s serves %d of %d, below the %d its budget leaves (%d allowed out, %d taken)", m.round, name, serving, desired, least, budget, taken)
			}
		}
		// A scale-up's Instances count once they serve; until then the
		// least capacity is read against the Instances that exist.
		pb, db := pdSideBudget(cell, m.fleet.prefill), pdSideBudget(cell, m.fleet.decode)
		check("prefill", m.serving.prefill, m.fleet.prefill-m.creating.prefill, m.taken.prefill, pb)
		check("decode", m.serving.decode, m.fleet.decode-m.creating.decode, m.taken.decode, db)
	}
	return out
}

// pdBatchStepGoverns says whether the batch step paces the cell's pair: it
// does unless a side patches single pods in place with keepRatio off.
func pdBatchStepGoverns(cell pdCell) bool {
	return cell.setting.defaults.PrefillDecode.KeepRatio || cell.path.strategy != v1beta1.UpdateStrategyInPlaceIfPossible
}

// pdSideBudget is the number of Instances a side's budget lets out of
// rotation at once: its own maxUnavailable capped by the group's on the
// drain-first and in-place paths; none on the surge path.
func pdSideBudget(cell pdCell, desired int32) int32 {
	if cell.path.strategy == v1beta1.UpdateStrategySurgeThenDrain {
		return 0
	}
	pacing := groupPacing(&batchService(cell.path.group, nil).Spec.Rollout.Groups[0], 2, cell.setting.defaults)
	own := utils.ScaledCountFromIntOrString(ptr.To(intstr.Parse(cell.path.budget)), desired, true)
	return min32(MaxUnavailableBudget(pacing, desired), own)
}

// pdHard says whether a finding is one the engine must never produce: a
// round where nothing moves with Instances left, a consulted start out of
// the plan's order, a side under the least capacity its budget leaves.
func pdHard(f pdFinding) bool {
	switch f.kind {
	case "halt", "O", "capacity":
		return true
	}
	return false
}

// pdMoments are the served moments a fault fires after: the moments of the
// plain walk of the same shape, path and setting; the default run takes
// the second and the middle one, the full run every one.
func pdMoments(t *testing.T, cell pdCell, full bool) []int {
	plain := cell
	plain.fault, plain.moment = pdNoFault, 0
	w := newPDDriver(t, plain).run()
	if w.rounds <= 1 {
		return nil
	}
	if full {
		out := make([]int, 0, w.rounds-1)
		for r := 1; r < w.rounds; r++ {
			out = append(out, r)
		}
		return out
	}
	mid := w.rounds / 2
	if mid == 2 || mid < 1 {
		return []int{2}
	}
	return []int{2, mid}
}

// pdPlainWalks are the walks the plan produces for each shape on each
// path, with and without the chart's settings: the newest version's
// picture after every round. A moved walk is a changed plan.
var pdPlainWalks = map[string]string{
	"8:8/RecreatePod/chart":           "0:1 1:1 1:2 2:2 2:3 3:3 3:4 4:4 4:5 5:5 5:6 6:6 6:7 7:7 7:8 8:8",
	"8:8/RecreatePod/none":            "1:1 2:2 3:3 4:4 5:5 6:6 7:7 8:8",
	"8:8/SurgeThenDrain/chart":        "0:1 1:1 1:2 2:2 2:3 3:3 3:4 4:4 4:5 5:5 5:6 6:6 6:7 7:7 7:8 8:8",
	"8:8/SurgeThenDrain/none":         "1:1 2:2 3:3 4:4 5:5 6:6 7:7 8:8",
	"8:8/InPlace/chart":               "0:1 1:1 1:2 2:2 2:3 3:3 3:4 4:4 4:5 5:5 5:6 6:6 6:7 7:7 7:8 8:8",
	"8:8/InPlace/none":                "1:1 2:2 3:3 4:4 5:5 6:6 7:7 8:8",
	"8:8/BlueGreenRecreate20/chart":   "0:2 2:2 2:4 4:4 4:6 6:6 6:8 8:8",
	"8:8/BlueGreenRecreate20/none":    "2:2 4:4 6:6 8:8",
	"8:8/BlueGreenSurge100/chart":     "0:2 2:2 2:4 4:4 4:6 6:6 6:8 8:8",
	"8:8/BlueGreenSurge100/none":      "2:2 4:4 6:6 8:8",
	"12:8/RecreatePod/chart":          "0:1 2:1 2:2 4:2 4:3 6:3 6:4 8:4 8:5 9:5 9:6 11:6 11:7 12:7 12:8",
	"12:8/RecreatePod/none":           "2:1 4:2 6:3 8:4 10:5 12:6 12:7 12:8",
	"12:8/SurgeThenDrain/chart":       "0:1 2:1 2:2 4:2 4:3 6:3 6:4 8:4 8:5 9:5 9:6 11:6 11:7 12:7 12:8",
	"12:8/SurgeThenDrain/none":        "2:1 4:2 6:3 8:4 10:5 12:6 12:7 12:8",
	"12:8/InPlace/chart":              "0:1 2:1 2:2 4:2 4:3 6:3 6:4 8:4 8:5 9:5 9:6 11:6 11:7 12:7 12:8",
	"12:8/InPlace/none":               "2:1 4:2 6:3 7:4 9:5 10:6 12:7 12:8",
	"12:8/BlueGreenRecreate20/chart":  "0:2 3:2 3:4 6:4 6:6 9:6 9:8 12:8",
	"12:8/BlueGreenRecreate20/none":   "3:2 6:4 9:6 12:8",
	"12:8/BlueGreenSurge100/chart":    "0:2 3:2 3:4 6:4 6:6 9:6 9:8 12:8",
	"12:8/BlueGreenSurge100/none":     "3:2 6:4 9:6 12:8",
	"24:8/RecreatePod/chart":          "0:1 3:1 3:2 6:2 6:3 9:3 9:4 12:4 12:5 15:5 15:6 18:6 18:7 21:7 21:8 24:8",
	"24:8/RecreatePod/none":           "3:1 6:2 9:3 12:4 15:5 18:6 21:7 24:8",
	"24:8/SurgeThenDrain/chart":       "0:1 3:1 3:2 6:2 6:3 9:3 9:4 12:4 12:5 15:5 15:6 18:6 18:7 21:7 21:8 24:8",
	"24:8/SurgeThenDrain/none":        "3:1 6:2 9:3 12:4 15:5 18:6 21:7 24:8",
	"24:8/InPlace/chart":              "0:1 3:1 3:2 6:2 6:3 9:3 9:4 12:4 12:5 15:5 15:6 18:6 18:7 21:7 21:8 24:8",
	"24:8/InPlace/none":               "3:1 6:2 9:3 12:4 15:5 18:6 21:7 24:8",
	"24:8/BlueGreenRecreate20/chart":  "0:2 5:2 5:4 10:4 10:6 15:6 15:8 20:8 24:8",
	"24:8/BlueGreenRecreate20/none":   "5:2 10:4 15:6 20:8 24:8",
	"24:8/BlueGreenSurge100/chart":    "0:2 6:2 6:4 12:4 12:6 18:6 18:8 24:8",
	"24:8/BlueGreenSurge100/none":     "6:2 12:4 18:6 24:8",
	"30:10/RecreatePod/chart":         "0:1 3:1 3:2 6:2 6:3 9:3 9:4 12:4 12:5 15:5 15:6 18:6 18:7 21:7 21:8 24:8 24:9 27:9 27:10 30:10",
	"30:10/RecreatePod/none":          "3:1 6:2 9:3 12:4 15:5 18:6 21:7 24:8 27:9 30:10",
	"30:10/SurgeThenDrain/chart":      "0:1 3:1 3:2 6:2 6:3 9:3 9:4 12:4 12:5 15:5 15:6 18:6 18:7 21:7 21:8 24:8 24:9 27:9 27:10 30:10",
	"30:10/SurgeThenDrain/none":       "3:1 6:2 9:3 12:4 15:5 18:6 21:7 24:8 27:9 30:10",
	"30:10/InPlace/chart":             "0:1 3:1 3:2 6:2 6:3 9:3 9:4 12:4 12:5 15:5 15:6 18:6 18:7 21:7 21:8 24:8 24:9 27:9 27:10 30:10",
	"30:10/InPlace/none":              "3:1 6:2 9:3 12:4 15:5 18:6 21:7 24:8 27:9 30:10",
	"30:10/BlueGreenRecreate20/chart": "0:2 6:2 6:4 12:4 12:6 18:6 18:8 24:8 24:10 30:10",
	"30:10/BlueGreenRecreate20/none":  "6:2 12:4 18:6 24:8 30:10",
	"30:10/BlueGreenSurge100/chart":   "0:3 8:3 8:6 16:6 16:9 24:9 24:10 30:10",
	"30:10/BlueGreenSurge100/none":    "8:3 16:6 24:9 30:10",
	"6:2/RecreatePod/chart":           "1:0 1:1 2:1 3:1 4:1 4:2 5:2 6:2",
	"6:2/RecreatePod/none":            "1:1 2:2 3:2 4:2 5:2 6:2",
	"6:2/SurgeThenDrain/chart":        "1:0 1:1 2:1 3:1 4:1 4:2 5:2 6:2",
	"6:2/SurgeThenDrain/none":         "1:1 2:2 3:2 4:2 5:2 6:2",
	"6:2/InPlace/chart":               "1:0 1:1 2:1 3:1 4:1 4:2 5:2 6:2",
	"6:2/InPlace/none":                "1:1 2:1 3:1 4:2 5:2 6:2",
	"6:2/BlueGreenRecreate20/chart":   "0:1 2:1 4:1 4:2 6:2",
	"6:2/BlueGreenRecreate20/none":    "2:1 4:2 6:2",
	"6:2/BlueGreenSurge100/chart":     "0:1 2:1 4:1 4:2 6:2",
	"6:2/BlueGreenSurge100/none":      "2:1 4:2 6:2",
	"8:2/RecreatePod/chart":           "1:0 1:1 2:1 3:1 4:1 5:1 5:2 6:2 7:2 8:2",
	"8:2/RecreatePod/none":            "1:1 2:2 3:2 4:2 5:2 6:2 7:2 8:2",
	"8:2/SurgeThenDrain/chart":        "1:0 1:1 2:1 3:1 4:1 5:1 5:2 6:2 7:2 8:2",
	"8:2/SurgeThenDrain/none":         "1:1 2:2 3:2 4:2 5:2 6:2 7:2 8:2",
	"8:2/InPlace/chart":               "1:0 1:1 2:1 3:1 4:1 5:1 5:2 6:2 7:2 8:2",
	"8:2/InPlace/none":                "1:1 2:1 3:1 4:1 5:2 6:2 7:2 8:2",
	"8:2/BlueGreenRecreate20/chart":   "1:0 1:1 3:1 5:1 5:2 7:2 8:2",
	"8:2/BlueGreenRecreate20/none":    "2:1 4:2 6:2 8:2",
	"8:2/BlueGreenSurge100/chart":     "1:0 1:1 3:1 5:1 5:2 7:2 8:2",
	"8:2/BlueGreenSurge100/none":      "2:1 4:2 6:2 8:2",
}

// TestEvaluateUpdateGate_ReferenceShapesWalkTheirPlans walks every shape on
// every path with and without the chart's settings and holds the newest
// version's picture after each round to the walk the plan produces:
// under the chart's settings the decoder's half of a batch first, then the
// prefill's; without them each side on its own step, so the side with the
// smaller count finishes first and the other walks on alone. A plan change
// moves the recorded walks.
func TestEvaluateUpdateGate_ReferenceShapesWalkTheirPlans(t *testing.T) {
	for _, shape := range pdShapes {
		for _, path := range pdPaths {
			for _, setting := range pdSettings {
				cell := pdCell{shape: shape, path: path, setting: setting, fault: pdNoFault}
				t.Run(cell.String(), func(t *testing.T) {
					t.Parallel()
					w := newPDDriver(t, cell).run()
					t.Logf("%s: %s", cell, w.startsByRound())
					if w.halt != "" {
						t.Fatal(w.halt)
					}
					want, ok := pdPlainWalks[cell.String()]
					if !ok {
						t.Fatalf("no walk recorded for %s; the current walk is %q", cell, w.servedPictures())
					}
					if got := w.servedPictures(); got != want {
						t.Errorf("the walk moved: got %s, want %s", got, want)
					}
				})
			}
		}
	}
}

// TestEvaluateUpdateGate_ReferenceShapesWalkEveryPathUnderOneFault walks
// the reference prefill/decode shapes through the update gate under
// each update path, with and without the chart's settings, plain and with
// one fault at one moment, and fails where the engine contradicts its own
// rule. OME_PD_WALK_HUNT=full fires every fault after every round.
func TestEvaluateUpdateGate_ReferenceShapesWalkEveryPathUnderOneFault(t *testing.T) {
	full := os.Getenv("OME_PD_WALK_HUNT") == "full"
	for _, shape := range pdShapes {
		for _, path := range pdPaths {
			for _, setting := range pdSettings {
				base := pdCell{shape: shape, path: path, setting: setting, fault: pdNoFault}
				t.Run(base.String(), func(t *testing.T) {
					t.Parallel()
					var cells []pdCell
					moments := pdMoments(t, base, full)
					for _, fault := range pdFaults {
						if fault == pdNoFault {
							cells = append(cells, base)
							continue
						}
						for _, m := range moments {
							c := base
							c.fault, c.moment = fault, m
							cells = append(cells, c)
						}
					}
					for _, cell := range cells {
						w := newPDDriver(t, cell).run()
						findings := readWalk(cell, w)
						t.Logf("%s: %d rounds: %s", cell, w.rounds, w)
						for _, h := range w.holds {
							t.Logf("%s: hold %s", cell, h)
						}
						for _, f := range findings {
							if pdHard(f) {
								t.Errorf("%s: %s", cell, f)
							} else {
								t.Logf("%s: %s", cell, f)
							}
						}
					}
				})
			}
		}
	}
}
