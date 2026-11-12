package coordination

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// A prefill/decode service answers a request of one version with a prefill
// and a decode of that version, so while a roll runs each version should
// stay near the fleet's prefill-to-decode ratio, and no version should keep
// one side in rotation with no partner of its own for longer than the
// partner takes to start. This file drives whole rolls of the reference shapes
// and the pairing families' shapes through both
// production gates, the start-time gate (EvaluateUpdateGate) and the
// drain-time gate (EvaluatePairingDrain), under the chart's settings (the
// ratio rule on, the decoder first, a default tolerance of 5), with and
// without a pairing-protocol change, and reads both versions' pictures
// after every round the way an outside observer reads them: version 2 with
// its started Instances counted, version 1 as it stands in rotation.
//
// A picture is read against the allowance an observer grants the side that
// is ahead: the group's tolerance of that side's own count in whole Instances,
// rounded up and at least one (the allowance the ratio rule
// grants). Version 2 must stay within it at every moment;
// version 1 may stand past it, and either version may stand one-sided, for
// one start window and no longer. During a pairing-protocol change a side
// whose start takes its pod out of rotation at once (RecreatePod, or in
// place) has its last Instance rebuilt only once the other side has moved
// down to its pairable minimum, desired less maxUnavailable, because that
// Instance's old pod is the last partner of every old Instance across from
// it; the other side may lead by that minimum for that one start time
// (the lead the pairing rule allows).

// versionPicture is one version's Instances on each side at one moment of
// a roll: for version 2 the Instances serving it or started toward it, for
// version 1 the Instances still in rotation on it.
type versionPicture struct{ prefill, decode int32 }

func (p versionPicture) String() string { return fmt.Sprintf("%d:%d", p.prefill, p.decode) }

// oneSided reports whether a version holds Instances on one side and none
// on the other.
func (p versionPicture) oneSided() bool { return (p.prefill > 0) != (p.decode > 0) }

// pastTheLine is how far a version's picture stands past the fleet's
// prefill-to-decode line, in Instances of the side that is over, and which
// side that is: prefill over what its decode count allows, or decode over
// what its prefill count allows, whichever is larger. 6:8 on an 8:8 fleet
// is 2 decode Instances over; 2:1 on 12:8 is 0.67 prefill over.
func pastTheLine(p versionPicture, prefill, decode int32) (float64, v1beta1.ComponentType) {
	overPrefill := float64(p.prefill) - float64(p.decode)*float64(prefill)/float64(decode)
	overDecode := float64(p.decode) - float64(p.prefill)*float64(decode)/float64(prefill)
	if overDecode > overPrefill {
		return overDecode, v1beta1.DecoderComponent
	}
	return overPrefill, v1beta1.EngineComponent
}

// sideAllowance is how many Instances the side that is ahead may stand
// past the line: the tolerance of its own count, rounded up, at least one.
func sideAllowance(tolerancePercent, count int32) float64 {
	return math.Max(1, math.Ceil(float64(tolerancePercent)*float64(count)/100))
}

// fleetLine is the shape a walk is read against: both sides' counts and
// the tolerance the allowance is taken from.
type fleetLine struct {
	prefill, decode int32
	tolerance       int32
}

// excess is how far a picture stands past the allowance of the side that
// is over, in that side's Instances; zero or less within it.
func (l fleetLine) excess(p versionPicture) float64 {
	over, side := pastTheLine(p, l.prefill, l.decode)
	count := l.prefill
	if side == v1beta1.DecoderComponent {
		count = l.decode
	}
	return over - sideAllowance(l.tolerance, count)
}

// rollMoment is both versions' pictures at one of the two moments of a
// round: after the round's starts opened and after they served.
type rollMoment struct {
	round  int
	served bool
	new    versionPicture
	old    versionPicture
}

func (m rollMoment) String() string {
	when := "open"
	if m.served {
		when = "served"
	}
	return fmt.Sprintf("r%d %s v2=%s v1=%s", m.round, when, m.new, m.old)
}

func momentsOf(ms []rollMoment) string {
	out := make([]string, 0, len(ms))
	for _, m := range ms {
		out = append(out, m.String())
	}
	return strings.Join(out, "; ")
}

// rollTime is the moment's time in rounds: a round's starts open when the
// previous round served, so "open" of round r and "served" of round r-1
// share one time.
func (m rollMoment) rollTime() float64 {
	if m.served {
		return float64(m.round)
	}
	return float64(m.round - 1)
}

// partnerWindowRounds is how long a version may stand with one side and no
// partner of its own, and how long version 1 may stand past its allowance:
// the missing side's start window as an observer allows it, one start of
// 15 s plus 30 s of slack, 45 s against rounds of about 20 s.
const partnerWindowRounds = 2.25

// versionFindings reads a walk the way an outside observer reads a roll.
// Version 2 stands within its allowance at every moment, except the lead
// a rebuilt side's last move needs (lastMoveLead). Version 1 may stand
// past it, and either version may stand one-sided, for the partner window
// and no longer; a moment that lasts no time (a round's served picture,
// replaced at once by the next round's starts) does not end a run, as the
// lanes' five-second ticks rarely land on it. Each finding is reported
// once, in the order the walk produces it.
func versionFindings(moments []rollMoment, line fleetLine, lastMoveLead func(versionPicture) bool) []string {
	var findings []string
	seen := map[string]bool{}
	add := func(s string) {
		if !seen[s] {
			seen[s] = true
			findings = append(findings, s)
		}
	}
	type run struct {
		rounds float64
		first  rollMoment
	}
	versions := []string{"version 2", "version 1"}
	pictureOf := func(m rollMoment, version string) versionPicture {
		if version == "version 1" {
			return m.old
		}
		return m.new
	}
	oldOver := run{}
	oneSided := map[string]run{}
	endOldOver := func() {
		if oldOver.rounds > partnerWindowRounds {
			over, _ := pastTheLine(oldOver.first.old, line.prefill, line.decode)
			add(fmt.Sprintf("version 1 holds %s in rotation against a fleet of %d:%d from round %d for %.1f rounds: %.1f Instance(s) past the line",
				oldOver.first.old, line.prefill, line.decode, oldOver.first.round, oldOver.rounds, over))
		}
		oldOver = run{}
	}
	endOneSided := func(version string) {
		r := oneSided[version]
		if r.rounds > partnerWindowRounds {
			p := pictureOf(r.first, version)
			have, missing := "prefill", "decode"
			if p.decode > 0 {
				have, missing = "decode", "prefill"
			}
			add(fmt.Sprintf("%s stands %.1f rounds from round %d with %s in rotation and no %s of its own (%s at first)",
				version, r.rounds, r.first.round, have, missing, p))
		}
		oneSided[version] = run{}
	}
	for i, m := range moments {
		var lasts float64
		if i+1 < len(moments) {
			lasts = moments[i+1].rollTime() - m.rollTime()
		}
		if excess := line.excess(m.new); excess > 0.0001 && lasts > 0 && !lastMoveLead(m.new) {
			over, _ := pastTheLine(m.new, line.prefill, line.decode)
			add(fmt.Sprintf("version 2 holds %s against a fleet of %d:%d after round %d: %.1f Instance(s) past the line, %.1f past its allowance",
				m.new, line.prefill, line.decode, m.round, over, excess))
		}
		switch excess := line.excess(m.old); {
		case excess > 0.0001 && m.old.prefill+m.old.decode > 0:
			if oldOver.rounds == 0 {
				oldOver.first = m
			}
			oldOver.rounds += lasts
		case lasts > 0:
			endOldOver()
		}
		for _, version := range versions {
			switch p := pictureOf(m, version); {
			case p.oneSided():
				r := oneSided[version]
				if r.rounds == 0 {
					r.first = m
				}
				r.rounds += lasts
				oneSided[version] = r
			case lasts > 0:
				endOneSided(version)
			}
		}
	}
	endOldOver()
	for _, version := range versions {
		endOneSided(version)
	}
	return findings
}

// pictureStage is what the roll has done to one Instance of a side.
type pictureStage int

const (
	stageOld                pictureStage = iota // serves the old revision; nothing started
	stageSurging                                // a replacement of the new revision is coming up beside the serving old pod
	stageReplacementServing                     // both pods serve; the old pod leaves only when the drain gate allows
	stageRebuilding                             // the old pod is gone and the new one is coming up
	stageNew                                    // runs the new revision; its pod serves
)

// pictureSide is one side of the pair as the walk rolls it: its strategy,
// the mechanism its starts run on, its own budgets and its Instances.
type pictureSide struct {
	comp           v1beta1.ComponentType
	strategy       v1beta1.UpdateStrategyType
	mechanism      workloadtypes.UpdateStrategyType
	maxSurge       string // the side's own rollingUpdate.maxSurge; "" leaves it unset
	maxUnavailable string // the side's own rollingUpdate.maxUnavailable; "" leaves it unset
	rows           []pictureStage
}

// side builds one side with n Instances all serving the old revision.
func side(comp v1beta1.ComponentType, n int, strategy v1beta1.UpdateStrategyType, mechanism workloadtypes.UpdateStrategyType, maxSurge, maxUnavailable string) *pictureSide {
	return &pictureSide{comp: comp, strategy: strategy, mechanism: mechanism, maxSurge: maxSurge, maxUnavailable: maxUnavailable, rows: make([]pictureStage, n)}
}

func (s *pictureSide) surges() bool { return s.mechanism == workloadtypes.UpdateStrategySurgeThenDrain }

// started is the Instances serving version 2 or started toward it.
func (s *pictureSide) started() int32 {
	var n int32
	for _, r := range s.rows {
		if r != stageOld {
			n++
		}
	}
	return n
}

// inRotationOnOld is the Instances whose old pod still serves version 1:
// a drain-first start takes it out of rotation at once, a surge keeps it
// until the replacement serves and the drain gate lets it go.
func (s *pictureSide) inRotationOnOld() int32 {
	var n int32
	for _, r := range s.rows {
		if r == stageOld || r == stageSurging || r == stageReplacementServing {
			n++
		}
	}
	return n
}

func (s *pictureSide) done() bool {
	for _, r := range s.rows {
		if r != stageNew {
			return false
		}
	}
	return true
}

// pictureWalk is one roll: the group the two sides roll in, the pairing
// protocol of the old and the new revision (equal or empty for a plain
// roll), the operator's settings and the two sides.
type pictureWalk struct {
	group    batchGroupFixture
	oldProto string
	newProto string
	defaults GroupDefaults
	engine   *pictureSide
	decoder  *pictureSide
	isvc     *v1beta1.InferenceService
}

func (w *pictureWalk) revision(comp v1beta1.ComponentType, which string) string {
	return w.isvc.Name + "-" + string(comp) + "-" + which
}

func (w *pictureWalk) pod(comp v1beta1.ComponentType, index, ordinal int) string {
	return fmt.Sprintf("%s-%s-%d-default-%d", w.isvc.Name, comp, index, ordinal)
}

// service is the InferenceService the gates read, with its run recorded: a
// blueGreen group over the engine and the decoder with the new protocol
// declared (admission lets a pairing protocol change roll under a blueGreen
// group only), or a rollingUpdate group with the fixture's budgets, under
// maintainRatio when the fixture names a tolerance.
func (w *pictureWalk) service(t *testing.T) *v1beta1.InferenceService {
	t.Helper()
	if w.group.blueGreen {
		return pairingISVC(w.newProto)
	}
	if w.newProto != "" {
		t.Fatalf("a pairing protocol change rolls under a blueGreen group only")
	}
	return batchService(w.group, map[v1beta1.ComponentType]batchMemberFixture{
		v1beta1.EngineComponent:  {desired: int32(len(w.engine.rows))},
		v1beta1.DecoderComponent: {desired: int32(len(w.decoder.rows))},
	})
}

// replica renders a side as its replica controller publishes it: the spec
// with the side's strategy and budgets, the rows by stage, and the
// counters derived from them.
func (w *pictureWalk) replica(s *pictureSide) *v1beta1.InferenceReplica {
	old, target := w.revision(s.comp, "v1"), w.revision(s.comp, "target")
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Namespace: w.isvc.Namespace, Name: w.isvc.Name + "-" + string(s.comp)},
		Spec: v1beta1.InferenceReplicaSpec{
			Component: s.comp,
			Replicas:  ptr.To(int32(len(s.rows))),
			Lifecycle: &v1beta1.LifecycleSpec{UpdateStrategy: &v1beta1.UpdateStrategy{Type: s.strategy, RollingUpdate: &v1beta1.RollingUpdate{}}},
		},
		Status: v1beta1.InferenceReplicaStatus{CurrentRevision: old, UpdateRevision: target, Replicas: int32(len(s.rows))},
	}
	if s.maxSurge != "" {
		ir.Spec.Lifecycle.UpdateStrategy.RollingUpdate.MaxSurge = ptr.To(intstr.Parse(s.maxSurge))
	}
	if s.maxUnavailable != "" {
		ir.Spec.Lifecycle.UpdateStrategy.RollingUpdate.MaxUnavailable = ptr.To(intstr.Parse(s.maxUnavailable))
	}
	attempt := func(step string) *v1beta1.InstanceOperation {
		return &v1beta1.InstanceOperation{ID: "attempt", Type: v1beta1.InstanceOperationUpdate, Step: step, TargetRevision: target}
	}
	rebuildStep := workloadtypes.UpdateStepDrain
	if s.mechanism == workloadtypes.UpdateStrategyInPlaceIfPossible || s.mechanism == workloadtypes.UpdateStrategyInPlaceOnly {
		rebuildStep = workloadtypes.UpdateStepInPlace
	}
	for i, r := range s.rows {
		row := v1beta1.OMENativeInstanceStatus{Index: int32(i)}
		switch r {
		case stageOld:
			row.Phase, row.RunningRevision = v1beta1.OMENativeInstanceReady, old
			row.PodCount, row.ReadyPodCount, row.ServingPodCount = 1, 1, 1
		case stageSurging:
			row.Phase, row.RunningRevision, row.TargetRevision = v1beta1.OMENativeInstanceUpdating, old, target
			row.PodCount, row.ReadyPodCount, row.ServingPodCount = 2, 1, 1
			row.Operation = attempt(workloadtypes.UpdateStepSurge)
		case stageReplacementServing:
			row.Phase, row.RunningRevision, row.TargetRevision = v1beta1.OMENativeInstanceUpdating, old, target
			row.PodCount, row.ReadyPodCount, row.ServingPodCount = 2, 2, 2
			row.Operation = attempt(workloadtypes.UpdateStepSurge)
		case stageRebuilding:
			row.Phase, row.RunningRevision, row.TargetRevision = v1beta1.OMENativeInstanceUpdating, old, target
			row.PodCount = 1
			row.Operation = attempt(rebuildStep)
		case stageNew:
			row.Phase, row.RunningRevision = v1beta1.OMENativeInstanceReady, target
			row.PodCount, row.ReadyPodCount, row.ServingPodCount = 1, 1, 1
			ir.Status.UpdatedReplicas++
			ir.Status.UpdatedReadyReplicas++
		}
		if row.ServingPodCount > 0 {
			ir.Status.ServingReplicas++
			ir.Status.ReadyReplicas++
		}
		ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses, row)
	}
	return ir
}

// pods renders a side's routable pods with their cohort labels: the old
// pod while it is in rotation, the new pod from its create on, Ready once
// it serves.
func (w *pictureWalk) pods(s *pictureSide) []client.Object {
	var out []client.Object
	oldPod := func(i int) client.Object {
		return drainPod(w.isvc.Name, s.comp, w.pod(s.comp, i, 0), "default", w.oldProto, true)
	}
	newPod := func(i int, ready bool) client.Object {
		return drainPod(w.isvc.Name, s.comp, w.pod(s.comp, i, 1), "default", w.newProto, ready)
	}
	for i, r := range s.rows {
		switch r {
		case stageOld:
			out = append(out, oldPod(i))
		case stageSurging:
			out = append(out, oldPod(i), newPod(i, false))
		case stageReplacementServing:
			out = append(out, oldPod(i), newPod(i, true))
		case stageRebuilding:
			out = append(out, newPod(i, false))
		case stageNew:
			out = append(out, newPod(i, true))
		}
	}
	return out
}

// client is the apiserver as the gates read it: both replicas, every pod,
// and the revisions of both cohorts.
func (w *pictureWalk) client(t *testing.T) client.Client {
	t.Helper()
	objs := []client.Object{w.replica(w.engine), w.replica(w.decoder)}
	for _, s := range []*pictureSide{w.engine, w.decoder} {
		objs = append(objs, pairingCR(w.isvc.Namespace, w.isvc.Name, s.comp, "v1", w.oldProto), w.target(s.comp))
		objs = append(objs, w.pods(s)...)
	}
	return fakeclient.New(t, fakeclient.WithObjects(objs...))
}

func (w *pictureWalk) target(comp v1beta1.ComponentType) *appsv1.ControllerRevision {
	return forwardTarget(w.isvc, comp)
}

// askStart asks the production start-time gate about the next start of a
// side, from the old revision, with the starts already taken this round on
// their rows.
func (w *pictureWalk) askStart(t *testing.T, s *pictureSide) (bool, string) {
	t.Helper()
	ok, gate, reason := EvaluateUpdateGate(context.Background(), w.client(t), w.isvc, s.comp, w.revision(s.comp, "v1"), w.target(s.comp), nil, w.defaults, s.mechanism, 0, 0)
	return ok, string(gate) + ": " + reason
}

// askDrain asks the production drain-time gate whether the old pod of a
// surging Instance whose replacement serves may leave rotation.
func (w *pictureWalk) askDrain(t *testing.T, s *pictureSide, index int) (bool, string) {
	t.Helper()
	ok, gate, reason := EvaluatePairingDrain(context.Background(), w.client(t), w.isvc, s.comp, w.target(s.comp), w.defaults, []string{w.pod(s.comp, index, 0)})
	return ok, string(gate) + ": " + reason
}

func (w *pictureWalk) moment(round int, served bool) rollMoment {
	return rollMoment{
		round:  round,
		served: served,
		new:    versionPicture{prefill: w.engine.started(), decode: w.decoder.started()},
		old:    versionPicture{prefill: w.engine.inRotationOnOld(), decode: w.decoder.inRotationOnOld()},
	}
}

// run walks the roll through both production gates one round at a time
// and records both versions' pictures after the round's starts open and
// after they serve. Each side, decoder first, opens starts until the gate
// holds it; every admitted start lands on its row and its pods before the
// next question; then every pod started this round serves, and every old
// pod whose replacement serves leaves rotation as soon as the drain gate
// allows it, asked again after each one that left. One round is one start
// time, 18 to 20 s on a real cluster. It returns the moments and the
// gates' answers that held something.
func (w *pictureWalk) run(t *testing.T) ([]rollMoment, []string) {
	t.Helper()
	w.isvc = w.service(t)
	sides := []*pictureSide{w.decoder, w.engine}
	var moments []rollMoment
	var holds []string
	for round := 1; round <= 64; round++ {
		if w.engine.done() && w.decoder.done() {
			return moments, holds
		}
		moved := false
		for _, s := range sides {
			for {
				next := -1
				for i, r := range s.rows {
					if r == stageOld {
						next = i
						break
					}
				}
				if next < 0 {
					break
				}
				ok, why := w.askStart(t, s)
				if !ok {
					holds = append(holds, fmt.Sprintf("r%d %s start held: %s", round, s.comp, why))
					break
				}
				if s.surges() {
					s.rows[next] = stageSurging
				} else {
					s.rows[next] = stageRebuilding
				}
				moved = true
			}
		}
		moments = append(moments, w.moment(round, false))
		for _, s := range sides {
			for i, r := range s.rows {
				switch r {
				case stageSurging:
					s.rows[i] = stageReplacementServing
				case stageRebuilding:
					s.rows[i] = stageNew
				}
			}
		}
		for left := true; left; {
			left = false
			for _, s := range sides {
				for i, r := range s.rows {
					if r != stageReplacementServing {
						continue
					}
					ok, why := w.askDrain(t, s, i)
					if !ok {
						holds = append(holds, fmt.Sprintf("r%d %s %d old pod kept: %s", round, s.comp, i, why))
						continue
					}
					s.rows[i] = stageNew
					left, moved = true, true
				}
			}
		}
		moments = append(moments, w.moment(round, true))
		if !moved {
			t.Fatalf("round %d moved nothing: %s\n%s", round, momentsOf(moments), strings.Join(holds, "\n"))
		}
	}
	t.Fatalf("the roll did not finish in 64 rounds: %s", momentsOf(moments))
	return nil, nil
}

// lastMoveLead reports a picture of version 2 that is a rebuilt side's last
// move during a pairing-protocol change: a side whose start takes its pod
// out of rotation at once has one Instance left to start, and the other
// side, the one ahead, stands at or below its pairable minimum, its count
// less the smaller of its own maxUnavailable and the group's budget. The
// start-time gate rebuilds that last Instance only then, so the lead is
// the gate's own. Never on a plain roll.
func (w *pictureWalk) lastMoveLead(p versionPicture) bool {
	if w.newProto == "" || w.oldProto == w.newProto {
		return false
	}
	pacing := pacingWithDefaults(&v1beta1.CoordinationPacing{}, w.defaults)
	for _, pair := range [][2]*pictureSide{{w.decoder, w.engine}, {w.engine, w.decoder}} {
		rebuilt, other := pair[0], pair[1]
		if rebuilt.surges() {
			continue
		}
		started := map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: p.prefill, v1beta1.DecoderComponent: p.decode}
		if started[rebuilt.comp] != int32(len(rebuilt.rows))-1 {
			continue
		}
		_, side := pastTheLine(p, int32(len(w.engine.rows)), int32(len(w.decoder.rows)))
		if side == other.comp && started[other.comp] > 0 && started[other.comp] <= other.pairableMinimum(pacing) {
			return true
		}
	}
	return false
}

// pairableMinimum is the Instances of the side that must keep a serving
// pod with a partner through a pairing-protocol change: its count less the
// smaller of its own maxUnavailable and the group's budget.
func (s *pictureSide) pairableMinimum(pacing v1beta1.CoordinationPacing) int32 {
	desired := int32(len(s.rows))
	out := MaxUnavailableBudget(pacing, desired)
	if s.maxUnavailable != "" {
		if own := utils.ScaledCountFromIntOrString(ptr.To(intstr.Parse(s.maxUnavailable)), desired, true); own < out {
			out = own
		}
	}
	return desired - out
}

// newVersionWalk is the newest version's picture after every round, the
// form the reference walks are recorded in.
func newVersionWalk(moments []rollMoment) string {
	out := make([]string, 0, len(moments)/2)
	for _, m := range moments {
		if m.served {
			out = append(out, m.new.String())
		}
	}
	return strings.Join(out, " ")
}

// versionPictureCase is one shape: its line, its group, its two sides,
// and whether the roll changes the pairing protocol. plain is the newest
// version's walk the same shape takes with no protocol change, the current
// batches, which a protocol change must not move.
type versionPictureCase struct {
	name     string
	line     fleetLine
	group    batchGroupFixture
	engine   *pictureSide
	decoder  *pictureSide
	protocol bool
	plain    string
	// lead is the picture a rebuilt side's last move leaves for exactly one
	// start time, the lead the other side's pairable minimum allows.
	lead string
}

// TestEvaluateUpdateGate_EachVersionKeepsTheFleetRatioUnderTheChartSettings
// drives the prefill/decode shapes of the pairing families and the
// reference 12:8 under the chart's settings through a pairing-protocol
// change, and asks of every round what an outside observer asks: each
// version within its allowance of the fleet ratio, no version one-sided
// for longer than its partner's start. Every picture is the walk the gates
// admit, not a side moving out of turn.
//
// Where both sides surge, the surging side's last old pod stays in rotation
// for the other side's old Instances until they have moved, so version 1 is
// never left one-sided and version 2 never runs past the line. Where the
// side with two Instances is rebuilt, its last old Instance is the last
// partner of the other side's old Instances, and the start-time gate holds
// its rebuild until the other side has moved down to its own pairable
// minimum: 6 prefill surging beside 2 decode rebuilt one at a time, each
// allowing one out, reads version 2 at 5:1 for one start time, and 2
// prefill rebuilt beside 6 decode surging reads 1:5 the same way; that lead
// is the decided reading, and the walk is held to show it exactly once. The
// same shapes with no protocol change keep the current batches. The 8:8 shapes
// at the quarter budget and the 2:6 shapes where both sides surge, whose
// decoder's step or half of a batch lands first, are the first side's question
// and are not driven here; the plain rolls that end one-sided are a
// question of their own.
func TestEvaluateUpdateGate_EachVersionKeepsTheFleetRatioUnderTheChartSettings(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	recreate, surge := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain
	drains, surges := workloadtypes.UpdateStrategyRecreatePod, workloadtypes.UpdateStrategySurgeThenDrain
	blueGreen := batchGroupFixture{blueGreen: true}
	cases := []versionPictureCase{
		{
			name: "6 prefill and 2 decode, both surging, one allowed out, change the pairing protocol", line: fleetLine{6, 2, 5},
			group:  blueGreen,
			engine: side(eng, 6, surge, surges, "3", "1"), decoder: side(dec, 2, surge, surges, "1", "1"),
			protocol: true, plain: "0:1 2:1 3:1 3:2 5:2 6:2",
		},
		{
			name: "6 prefill and 2 decode, both surging, none allowed out, change the pairing protocol", line: fleetLine{6, 2, 5},
			group:  blueGreen,
			engine: side(eng, 6, surge, surges, "3", "0"), decoder: side(dec, 2, surge, surges, "1", "0"),
			protocol: true, plain: "0:1 2:1 3:1 3:2 5:2 6:2",
		},
		{
			name: "6 prefill rebuilt one at a time beside 2 decode surging one at a time change the pairing protocol", line: fleetLine{6, 2, 5},
			group:  blueGreen,
			engine: side(eng, 6, recreate, drains, "", "1"), decoder: side(dec, 2, surge, surges, "1", "1"),
			protocol: true, plain: "1:0 1:1 2:1 3:1 4:1 4:2 5:2 6:2",
		},
		{
			name: "6 prefill surging beside 2 decode rebuilt one at a time change the pairing protocol", line: fleetLine{6, 2, 5},
			group:  blueGreen,
			engine: side(eng, 6, surge, surges, "3", "1"), decoder: side(dec, 2, recreate, drains, "", "1"),
			protocol: true, plain: "0:1 2:1 3:1 3:2 5:2 6:2",
			lead: "5:1", // the last decode is rebuilt once 5 new prefill serve (6 less 1 allowed out), for one start time
		},
		{
			name: "2 prefill rebuilt one at a time beside 6 decode surging one at a time change the pairing protocol", line: fleetLine{2, 6, 5},
			group:  blueGreen,
			engine: side(eng, 2, recreate, drains, "", "1"), decoder: side(dec, 6, surge, surges, "1", "1"),
			protocol: true, plain: "0:1 0:2 1:2 1:3 1:4 1:5 2:5 2:6",
			lead: "1:5", // the last prefill is rebuilt once 5 new decode serve (6 less 1 allowed out), for one start time
		},
		{
			name: "8 prefill and 2 decode, both surging one at a time, none allowed out, change the pairing protocol", line: fleetLine{8, 2, 5},
			group:  blueGreen,
			engine: side(eng, 8, surge, surges, "1", "0"), decoder: side(dec, 2, surge, surges, "1", "0"),
			protocol: true, plain: "1:0 1:1 2:1 3:1 4:1 5:1 5:2 6:2 7:2 8:2",
		},
		{
			name: "12 prefill and 8 decode, both surging at 10 percent, change the pairing protocol", line: fleetLine{12, 8, 5},
			group:  blueGreen,
			engine: side(eng, 12, surge, surges, "10%", "0"), decoder: side(dec, 8, surge, surges, "10%", "0"),
			protocol: true, plain: "0:1 2:1 2:2 3:2 3:3 5:3 5:4 6:4 6:5 8:5 8:6 9:6 9:7 11:7 11:8 12:8",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			walk := func(protocol bool) ([]rollMoment, []string) {
				w := pictureWalk{group: tc.group, defaults: keepRatioDecoderFirst(5), engine: sideCopy(tc.engine), decoder: sideCopy(tc.decoder)}
				if protocol {
					w.oldProto, w.newProto = "pair-v1", "pair-v2"
				}
				return w.run(t)
			}
			moments, holds := walk(tc.protocol)
			t.Logf("under the chart's settings: %s", momentsOf(moments))
			for _, h := range holds {
				t.Logf("  %s", h)
			}
			if tc.protocol {
				plain, _ := walk(false)
				t.Logf("the same shape with no protocol change: %s", momentsOf(plain))
				if got := newVersionWalk(plain); got != tc.plain {
					t.Errorf("the plain roll's batches moved: got %s, want %s", got, tc.plain)
				}
			}
			w := pictureWalk{group: tc.group, defaults: keepRatioDecoderFirst(5), engine: tc.engine, decoder: tc.decoder, oldProto: "pair-v1", newProto: "pair-v2"}
			for _, finding := range versionFindings(moments, tc.line, w.lastMoveLead) {
				t.Error(finding)
			}
			if tc.lead != "" {
				var times int
				for _, m := range moments {
					if m.served && m.new.String() == tc.lead {
						times++
					}
				}
				if times != 1 {
					t.Errorf("version 2 reads %s after %d round(s), want exactly one: %s", tc.lead, times, newVersionWalk(moments))
				}
			}
		})
	}
}

// sideCopy is a fresh copy of a side with every Instance serving the old
// revision, so each walk starts from the same place.
func sideCopy(s *pictureSide) *pictureSide {
	c := *s
	c.rows = make([]pictureStage, len(s.rows))
	return &c
}
