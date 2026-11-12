package coordination

import (
	"fmt"
	"maps"
	"math/rand"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/utils"
)

// This file drives the ratio rule of a prefill/decode group (maintainRatio
// and its tolerance: the lockstep over the two sides' new-version progress
// and the serving band, ratio.go) through the full gate stack while
// the operator resizes the group, in the world of
// pd_pacing_gates_never_wait_on_each_other_test.go. The configurations
// are the reference shapes (12:8, 15:15, 16:8, 20:10, 24:8, 30:10 and
// 12:4) at 10 percent under every update strategy pair: the pairs the
// ratio rule reaches on main, where one or both sides patch single pods in
// place and the batch step is off; the pairs the batch step governs, where
// the rule's reference is still written and its tolerance read; and a
// router beside the pair, which the band paces alone. Each side is scaled
// up or down by one Instance or by half, both sides together, and both to
// a new ratio, at every moment of the roll (idle, settling, rotating,
// draining, the last Instance, a side held on its spent retries), with and
// without a pairing-protocol change, under the group's own tolerance (5
// and a wide 20), the operator's default, no
// tolerance at all and the chart's keepRatio setting beside the rule, and
// with the reference a resize left behind while the group was idle.
//
// The reference the gates read is written by the production service pass
// (buildRatioState through BuildGroupStatus and MergeGroupStatus) as a move
// of its own, so every gate question between a resize and that pass reads
// the reference one pass behind the counts, as the controller does.
//
// Beside the world's readings (no hold without a way out, no two gates
// waiting on each other, the budgets, the batch step's shares where it
// governs) the model reads at every state (ratioReadings): the two sides'
// new-version shares, where the leading side may stand past the line the
// other side's share draws by at most max(1, ceil(tolerance x its count /
// 100)) of its own Instances, the allowance the ratio rule grants; the live
// serving ratio against the ratio the specs ask for, within the tolerance,
// a picture outside it read by its cause; and the reference against the
// counts once the service pass ran. A reading with no known cause fails.
//
// OME_RATIO_GATE_HUNT=full widens the configurations and the bounds;
// OME_RATIO_GATE_HUNT_ONLY=<text> keeps the configurations whose name
// contains the text.

// ratioTolerance is the tolerance the ratio rule resolves for the group:
// its maintainRatio tolerance, else the operator's default when the group
// sets maintainRatio without one; nil when no tolerance resolves, which
// disables the rule's bound.
func (w *huntWorld) ratioTolerance() *int32 {
	switch {
	case w.cfg.tolerance != nil:
		return w.cfg.tolerance
	case w.cfg.ratioRule:
		return w.cfg.defaults.RatioTolerancePercent
	}
	return nil
}

// shareCounts are one side's Instances as the ratio readings count them:
// the count the spec asks for, the Instances a partition holds, the ones
// promoted on the new version, the attempts open toward it, and whether an
// attempt failed, stopped retrying or the retries are spent.
type shareCounts struct {
	comp                  v1beta1.ComponentType
	count, held           int32
	promoted, open        int32
	creating, inPlaceOpen int32
	drainsOpen, uncounted int32
	failed                bool
	// retrying reports a retry record standing on the side: a rebuild of a
	// failed Instance may be in flight, written like a drain and asked of no
	// gate.
	retrying bool
}

func (s shareCounts) started() int32 { return s.promoted + s.open }
func (s shareCounts) movable() int32 { return s.count - s.held }

// sideShares counts side i's rows for the readings.
func (w *huntWorld) sideShares(st *huntState, i int) shareCounts {
	s := st.sides[i]
	held := s.held()
	out := shareCounts{comp: w.cfg.members[i].comp, count: int32(len(s.rows)), held: int32(len(held)), failed: s.schedule == scheduleHeld, retrying: s.schedule != scheduleNone}
	for idx, r := range s.rows {
		if held[idx] {
			continue
		}
		switch {
		case r == huntPromoted || r == huntPromotedUncounted || r == huntPromotedShort:
			out.promoted++
			if r == huntPromotedUncounted {
				out.uncounted++
			}
		case r.open():
			out.open++
			switch r {
			case huntOpenInPlace:
				out.inPlaceOpen++
			case huntOpenDrain:
				out.drainsOpen++
			}
		case r == huntCreating:
			out.creating++
		case r.ended():
			out.failed = true
		}
	}
	return out
}

// leadPast is how far side a's share stands past the line side b's share
// draws, in a's Instances, as a fraction num/den (den is b's movable
// count): started_a - started_b x movable_a / movable_b.
func leadPast(startedA, startedB int32, a, b shareCounts) (num, den int64) {
	return int64(startedA)*int64(b.movable()) - int64(startedB)*int64(a.movable()), int64(b.movable())
}

// ratioReadings reads the ratio rule's promises at one state of a group
// with a ratio rule; batch is the batch step's reading of the state, which
// says whether the step governs the pair in place of the rule.
func (w *huntWorld) ratioReadings(st *huntState, batch batchStep) []huntFinding {
	var out []huntFinding
	add := func(class, format string, args ...any) {
		out = append(out, huntFinding{class: class, message: fmt.Sprintf(format, args...), state: st})
	}
	if st.referenceOff {
		add("reference", "the service pass wrote the reference %s while the specs ask for %s", referenceRatioText(st.reference), referenceRatioText(st.desiredCounts(w.cfg.members)))
	}
	tolerance := w.ratioTolerance()
	eng, dec := w.pairIndex()
	if tolerance == nil || eng < 0 || dec < 0 || st.sides[eng].behind || st.sides[dec].behind {
		return out
	}
	e, d := w.sideShares(st, eng), w.sideShares(st, dec)
	if e.movable() <= 0 || d.movable() <= 0 {
		return out
	}
	for _, pair := range [][2]shareCounts{{e, d}, {d, e}} {
		lead, other := pair[0], pair[1]
		// The allowance the ratio rule grants, as batch_step_version_pictures_test.go reads it.
		allowance := int32(sideAllowance(*tolerance, lead.movable()))
		num, den := leadPast(lead.started(), other.started(), lead, other)
		if num <= int64(allowance)*den {
			continue
		}
		// One step of the leading side, in its Instances: its own budget on
		// the count it has now.
		step := int32(1)
		for _, m := range w.cfg.members {
			if m.comp == lead.comp && m.budget != "" {
				step = utils.ScaledCountFromIntOrString(ptr.To(intstr.Parse(m.budget)), lead.count, true)
			}
		}
		class := w.leadClass(st, batch, lead, other, num <= int64(allowance+step)*den)
		add(class, "the %s has started %d of %d (%d promoted, %d open) and the %s %d of %d: the %s stands %d.%d of its Instances past the line, more than its allowance of %d at tolerance %d",
			lead.comp, lead.started(), lead.movable(), lead.promoted, lead.open, other.comp, other.started(), other.movable(), lead.comp, num/den, (num%den)*10/den, allowance, *tolerance)
	}
	out = append(out, w.servingRatioReadings(st, batch, e, d, *tolerance)...)
	return out
}

// leadClass names the cause of a lead past the allowance, in this order:
// a batch step that pairs batch numbers under maintainRatio alone; a scale
// that widened or cut a side under the shares; a partition that moved the
// movable count; a lagging side whose attempts failed or whose retries are
// spent; a protocol change, where the pairing waiver lets a side run
// ahead; the named first side moving first inside a batch, by one step;
// else ratio-excess, which fails. withinAStep reports a lead at most one
// step of the leading side past the allowance.
func (w *huntWorld) leadClass(st *huntState, batch batchStep, lead, other shareCounts, withinAStep bool) string {
	keepRatio, firstSide := huntRulesOf(w.cfg.defaults)
	var added, removed, parts int32
	for _, s := range st.sides {
		added += s.added
		removed += s.removed
		parts += int32(w.cfg.parts) - int32(s.parts)
	}
	switch {
	case batch.governs && !keepRatio:
		return "version-mix-by-batch-number"
	case added > 0:
		return "version-mix-from-a-scale-up"
	case removed > 0:
		return "version-mix-from-a-scale-down"
	case parts > 0 || st.partitioned():
		return "version-mix-after-a-partition-change"
	case other.failed:
		return "version-mix-beside-a-held-side"
	case w.cfg.protocolChange:
		return "ratio-excess-in-protocol-change"
	case batch.governs && firstSide == lead.comp && withinAStep:
		return "version-mix-under-the-first-side"
	}
	return "ratio-excess"
}

// withinTolerance reports the two sides' serving counts within the
// tolerance of the ratio their specs ask for, read the way the operator
// wrote it and in exact arithmetic: with the side of the larger desired
// count on top (the engine on a tie), serving_top / serving_other sits
// inside (desired_top / desired_other) x (1 +- tolerance / 100). A side
// serving nothing is outside.
func withinTolerance(desired, serving map[v1beta1.ComponentType]int32, tolerance int32) bool {
	top, other := v1beta1.EngineComponent, v1beta1.DecoderComponent
	if desired[other] > desired[top] {
		top, other = other, top
	}
	if serving[top] <= 0 || serving[other] <= 0 {
		return false
	}
	ratio := int64(serving[top]) * int64(desired[other]) * 100
	base := int64(desired[top]) * int64(serving[other])
	return ratio >= base*int64(100-tolerance) && ratio <= base*int64(100+tolerance)
}

// servingRatioReadings reads the live serving ratio of the two sides
// against the ratio their specs ask for, within the tolerance
// (withinTolerance). A picture outside the tolerance is read by its cause, in
// this order: the batch step governing the pair, which never reads the
// serving ratio; a scale-up whose Instances have not come up; the reference
// one pass behind a resize; a promoted Instance whose pods the status has
// not counted yet; a failed Instance or spent retries; a failed Instance
// being rebuilt, which no gate is asked about; an in-place patch,
// whose pod is out of rotation while the band is waived for it; one drain
// of a side the band admitted as the one step no single step keeps inside
// the tolerance; two or more drains of a side admitted while its in-place
// peer's patches had the peer's serving count down; else
// serving-ratio-outside-tolerance, which fails.
func (w *huntWorld) servingRatioReadings(st *huntState, batch batchStep, e, d shareCounts, tolerance int32) []huntFinding {
	eng, dec := w.pairIndex()
	serving := map[v1beta1.ComponentType]int32{
		e.comp: w.replica(eng, st.sides[eng]).Status.ServingReplicas,
		d.comp: w.replica(dec, st.sides[dec]).Status.ServingReplicas,
	}
	desired := map[v1beta1.ComponentType]int32{e.comp: e.count, d.comp: d.count}
	if withinTolerance(desired, serving, tolerance) {
		return nil
	}
	oneStepBack := func(s shareCounts) bool {
		if s.drainsOpen != 1 {
			return false
		}
		back := map[v1beta1.ComponentType]int32{e.comp: serving[e.comp], d.comp: serving[d.comp]}
		back[s.comp]++
		return withinTolerance(desired, back, tolerance)
	}
	drainsBack := func(s shareCounts) bool {
		back := map[v1beta1.ComponentType]int32{e.comp: serving[e.comp], d.comp: serving[d.comp]}
		back[s.comp] += s.drainsOpen
		return withinTolerance(desired, back, tolerance)
	}
	peerPatchesInPlace := func(s shareCounts) bool {
		for _, m := range w.cfg.members {
			if m.comp == pairingPeer(s.comp) {
				return m.inPlace() && !m.isGang()
			}
		}
		return false
	}
	class := "serving-ratio-outside-tolerance"
	switch {
	case batch.governs:
		class = "serving-ratio-under-the-batch-step"
	case e.creating > 0 || d.creating > 0:
		class = "serving-ratio-while-a-scale-up-comes-up"
	case st.reference != nil && !maps.Equal(st.reference, st.desiredCounts(w.cfg.members)):
		class = "serving-ratio-before-the-service-pass"
	case e.uncounted > 0 || d.uncounted > 0:
		class = "serving-ratio-before-the-status-counts-a-promoted-pod"
	case e.failed || d.failed:
		class = "serving-ratio-beside-a-failed-instance"
	case (e.retrying && e.drainsOpen > 0) || (d.retrying && d.drainsOpen > 0):
		class = "serving-ratio-while-a-failed-instance-is-rebuilt"
	case e.inPlaceOpen > 0 || d.inPlaceOpen > 0:
		class = "serving-ratio-under-an-in-place-patch"
	case oneStepBack(e) || oneStepBack(d):
		class = "serving-ratio-one-step-outside-the-tolerance"
	case (e.drainsOpen > 1 && peerPatchesInPlace(e) && drainsBack(e)) || (d.drainsOpen > 1 && peerPatchesInPlace(d) && drainsBack(d)):
		class = "serving-ratio-drains-admitted-on-a-peer-patch-dip"
	}
	return []huntFinding{{class: class, state: st, message: fmt.Sprintf("%d %s and %d %s serve against %d:%d asked for: outside the tolerance of %d",
		serving[e.comp], e.comp, serving[d.comp], d.comp, e.count, d.count, tolerance)}}
}

// ratioModelSetting is one operator setting of the rule: the group's own
// tolerance, or maintainRatio left to the operator's default, and the
// prefill/decode rules beside it.
type ratioModelSetting struct {
	name      string
	tolerance *int32
	ratioRule bool
	defaults  GroupDefaults
}

// ratioModelSettings are the settings explored: a wide tolerance of
// 20; the chart's default; maintainRatio left to the operator's
// default of 5; the wide tolerance beside the chart's keepRatio
// setting; and maintainRatio with no tolerance anywhere, which disables
// the bound.
func ratioModelSettings() []ratioModelSetting {
	twenty, five := ptr.To(int32(20)), ptr.To(int32(5))
	return []ratioModelSetting{
		{name: "maintainRatio=20", tolerance: twenty},
		{name: "maintainRatio=5", tolerance: five},
		{name: "maintainRatio=default", ratioRule: true, defaults: GroupDefaults{RatioTolerancePercent: five}},
		{name: "maintainRatio=20/chart", tolerance: twenty, defaults: huntRules(true, five, v1beta1.DecoderComponent)},
		{name: "maintainRatio=none", ratioRule: true},
	}
}

// ratioModelConfig is one configuration of the pair at a reference shape,
// both sides at 10 percent, with one scale per side (by one or by half, up
// or down, on one side, both or the ratio) tried at every moment, one
// failure and the readings on.
func ratioModelConfig(family string, shape [2]int32, es, ds v1beta1.UpdateStrategyType, setting ratioModelSetting, protocol bool, router v1beta1.UpdateStrategyType) huntConfig {
	cfg := huntConfig{
		members: []huntMember{
			{comp: v1beta1.EngineComponent, desired: shape[0], strategy: es, budget: "10%"},
			{comp: v1beta1.DecoderComponent, desired: shape[1], strategy: ds, budget: "10%"},
		},
		defaults: setting.defaults, tolerance: setting.tolerance, ratioRule: setting.ratioRule, protocolChange: protocol, maxAttempts: 3,
		fails: 1, scales: 1, scaleDeltas: []int32{1, -1, 2, -2},
		probes: true, shareBound: true, ratioReadings: true,
	}
	if router != "" {
		cfg.members = append(cfg.members, huntMember{comp: v1beta1.RouterComponent, desired: 2, strategy: router, budget: "10%"})
	}
	cfg.name = family + ": " + cfg.String() + " " + setting.name
	return cfg
}

// withReferenceBehind starts the roll with the reference a resize left
// behind while the group was idle.
func withReferenceBehind(cfg huntConfig, engine, decoder int32) huntConfig {
	family, _, _ := strings.Cut(cfg.name, ": ")
	_, setting, _ := strings.Cut(cfg.name, cfg.String()+" ")
	cfg.referenceBehind = map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: engine, v1beta1.DecoderComponent: decoder}
	cfg.name = family + ": " + cfg.String() + " " + setting
	return cfg
}

// ratioModelConfigs lists the configurations: the pairs the ratio rule
// reaches (one or both sides in place) at every reference shape under the
// wide tolerance, a few under the chart's default, the operator's
// default and no tolerance; the pairs the batch step governs; a protocol
// change beside the rule; the reference a resize left behind; a router
// beside the pair; and in full mode every pair, setting and shape, with
// and without a protocol change, two scales per side, and a seeded sample.
func ratioModelConfigs(full bool) []huntConfig {
	recreate, surge, inPlace := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain, v1beta1.UpdateStrategyInPlaceIfPossible
	settings := ratioModelSettings()
	wide, chartDefault, operatorDefault := settings[0], settings[1], settings[2]
	rulePairs := [][2]v1beta1.UpdateStrategyType{{inPlace, inPlace}, {inPlace, recreate}, {surge, inPlace}, {recreate, inPlace}, {inPlace, surge}}
	stepPairs := [][2]v1beta1.UpdateStrategyType{{recreate, recreate}, {surge, surge}, {recreate, surge}, {surge, recreate}}
	var out []huntConfig
	if full {
		for _, shape := range batchModelShapes {
			for _, pair := range append(append([][2]v1beta1.UpdateStrategyType{}, rulePairs...), stepPairs...) {
				for _, setting := range settings {
					for _, protocol := range []bool{false, true} {
						cfg := ratioModelConfig("rule", shape, pair[0], pair[1], setting, protocol, "")
						cfg.scales = 2
						out = append(out, cfg)
					}
				}
			}
		}
	} else {
		for _, shape := range batchModelShapes {
			for _, pair := range rulePairs[:3] {
				out = append(out, ratioModelConfig("rule", shape, pair[0], pair[1], wide, false, ""))
			}
		}
		for _, shape := range [][2]int32{{12, 8}, {30, 10}, {12, 4}} {
			for _, pair := range rulePairs[3:] {
				out = append(out, ratioModelConfig("rule", shape, pair[0], pair[1], chartDefault, false, ""))
			}
		}
		for _, pair := range rulePairs[:3] {
			out = append(out, ratioModelConfig("protocol", [2]int32{12, 8}, pair[0], pair[1], wide, true, ""))
		}
		out = append(out, ratioModelConfig("protocol", [2]int32{30, 10}, surge, inPlace, wide, true, ""),
			ratioModelConfig("protocol", [2]int32{12, 4}, inPlace, surge, chartDefault, true, ""))
		for _, shape := range [][2]int32{{12, 8}, {30, 10}, {15, 15}} {
			for _, pair := range stepPairs[:2] {
				out = append(out, ratioModelConfig("step", shape, pair[0], pair[1], wide, false, ""))
			}
		}
		out = append(out,
			ratioModelConfig("chart", [2]int32{12, 8}, inPlace, recreate, settings[3], false, ""),
			ratioModelConfig("chart", [2]int32{12, 8}, recreate, recreate, settings[3], false, ""),
			ratioModelConfig("default", [2]int32{12, 8}, inPlace, recreate, operatorDefault, false, ""),
			ratioModelConfig("default", [2]int32{15, 15}, inPlace, inPlace, operatorDefault, false, ""),
			ratioModelConfig("none", [2]int32{12, 8}, inPlace, recreate, settings[4], false, ""),
		)
	}
	// The reference a resize left behind while the group was idle: a
	// resize from 15:15 to 30:10, a decoder doubled, a prefill side cut.
	behind := []struct {
		shape           [2]int32
		engine, decoder int32
		strategies      [2]v1beta1.UpdateStrategyType
	}{
		{[2]int32{30, 10}, 15, 15, [2]v1beta1.UpdateStrategyType{surge, inPlace}},
		{[2]int32{12, 8}, 12, 4, [2]v1beta1.UpdateStrategyType{inPlace, recreate}},
		{[2]int32{12, 8}, 16, 8, [2]v1beta1.UpdateStrategyType{recreate, recreate}},
	}
	for _, b := range behind {
		out = append(out, withReferenceBehind(ratioModelConfig("reference", b.shape, b.strategies[0], b.strategies[1], wide, false, ""), b.engine, b.decoder))
		if full {
			for _, pair := range append(append([][2]v1beta1.UpdateStrategyType{}, rulePairs...), stepPairs...) {
				for _, setting := range settings[:3] {
					out = append(out, withReferenceBehind(ratioModelConfig("reference", b.shape, pair[0], pair[1], setting, false, ""), b.engine, b.decoder))
				}
			}
		}
	}
	// A router beside the pair: left out of the batches when it steps
	// unlike the pairing members, so the band paces it alone.
	out = append(out,
		ratioModelConfig("router", [2]int32{12, 8}, recreate, recreate, wide, false, surge),
		ratioModelConfig("router", [2]int32{12, 8}, inPlace, inPlace, wide, false, recreate),
	)
	if !full {
		return out
	}
	for _, shape := range batchModelShapes {
		for _, pair := range [][2]v1beta1.UpdateStrategyType{{recreate, recreate}, {inPlace, inPlace}, {surge, surge}} {
			for _, router := range []v1beta1.UpdateStrategyType{recreate, surge} {
				out = append(out, ratioModelConfig("router", shape, pair[0], pair[1], wide, false, router))
			}
		}
	}
	rng := rand.New(rand.NewSource(20261103))
	allPairs := append(append([][2]v1beta1.UpdateStrategyType{}, rulePairs...), stepPairs...)
	for n := 0; n < 120; n++ {
		shape := batchModelShapes[rng.Intn(len(batchModelShapes))]
		pair := allPairs[rng.Intn(len(allPairs))]
		cfg := ratioModelConfig("sample", shape, pair[0], pair[1], settings[rng.Intn(len(settings))], rng.Intn(3) == 0, "")
		cfg.scales, cfg.fails = int8(1+rng.Intn(2)), int8(rng.Intn(3))
		if rng.Intn(4) == 0 {
			cfg.maxAttempts = 1
		}
		if rng.Intn(5) == 0 {
			cfg = gangs(cfg, []int32{2, 4}[rng.Intn(2)], []int32{2, 4}[rng.Intn(2)])
			cfg.losses, cfg.refills = 1, 1
		}
		out = append(out, cfg)
	}
	return out
}

// ratioModelFailing are the classes that name a bug: a hold with no way
// out or two gates waiting on each other, the ratio rule's among them; a
// budget exceeded; a lead past the allowance or a serving ratio outside the
// tolerance with no known cause; a reference the service pass wrote off the
// counts; the batch step's own failing classes; a read the gate could not
// make; a run cut short by the depth bound.
var ratioModelFailing = map[string]bool{
	"no-way-out": true, "ratio-rule": true, "ratio-rule-waits-on-the-pairing-gate": true, "budget": true, "ratio-excess": true, "serving-ratio-outside-tolerance": true, "reference": true,
	"classifier": true, "batch-promise": true, "ratio-promise": true, "runs-ahead-of-a-peer-coming-up": true,
	"starts-before-the-named-side": true, "stranded-replacement": true, "waits-on-held-instances": true, "read": true, "depth": true,
}

// ratioModelKnown are the classes that record a known reading of the current
// engine: each names what the operator sees and the test or decision that
// holds it. A finding of any other class fails as a reading to classify.
var ratioModelKnown = map[string]string{
	"version-mix-under-the-first-side":                      "the named first side moves first inside each batch, so it leads by its own step until the other side's part of the batch starts (the firstSide setting, by design)",
	"serving-ratio-one-step-outside-the-tolerance":          "the band admits one drain of a side no single step keeps inside the tolerance and says so on the service (RatioStepOutsideTolerance; TestRatioGate_ADecoderCutToOneTakesItsOneDrainOutsideTheTolerance)",
	"serving-ratio-under-an-in-place-patch":                 "an in-place patch takes its pod out of rotation while the band is waived for it (RatioGateBypassed), so the serving ratio drifts by the side's unavailability budget",
	"serving-ratio-drains-admitted-on-a-peer-patch-dip":     "the band reads the peer's live serving count, which the peer's own in-place patches lower, so it admits drains it would refuse at the peer's full count and the ratio stands outside the tolerance by up to the side's budget once the patches return",
	"serving-ratio-under-the-batch-step":                    "where the batch step governs the band is not consulted; the step keeps the ratio at batch boundaries, and a scale-down beside open attempts leaves the serving ratio outside the tolerance until they return (budget-after-scale)",
	"serving-ratio-before-the-status-counts-a-promoted-pod": "a rebuilt or patched Instance is promoted in one status write and its pods counted as serving in the next; the band reads the count in between",
	"serving-ratio-while-a-scale-up-comes-up":               "a scale-up's Instances serve nothing until they come up; the reference already asks for them",
	"serving-ratio-before-the-service-pass":                 "between a resize and the service pass the reference is the shape before the resize (TestRatioGate_AResizeReachesTheBandOneServicePassLater)",
	"serving-ratio-beside-a-failed-instance":                "a failed Instance serves nothing; the repair or the retry schedule owns it",
	"serving-ratio-while-a-failed-instance-is-rebuilt":      "the rebuild of a failed Instance is asked of no gate; its capacity was lost at the failure and comes back when it serves",
	"version-mix-by-batch-number":                           "under maintainRatio alone the batch step pairs batch numbers and plans no shares",
	"version-mix-from-a-scale-up":                           "a scale-up builds its Instances on the new version with no gate asked, so the scaled side's share drops and then jumps",
	"version-mix-from-a-scale-down":                         "a scale-down retires the highest indices whatever version they run, so the shares jump apart and close as the roll goes on",
	"version-mix-after-a-partition-change":                  "a partition change moves the movable count under the shares; the roll closes the gap from there",
	"version-mix-beside-a-held-side":                        "a side whose attempts failed or whose retries are spent is run past by the other side",
	"ratio-excess-in-protocol-change":                       "during a protocol change the pairing gate holds one side still and lets the other run ahead, by design",
	"progress-from-a-scale-up":                              "the batch step counts a scaled-up side's created Instances as starts",
	"batch-promise-after-scale":                             "a scale re-plans the batches from the new counts",
	"batch-promise-in-protocol-change":                      "the pairing waiver lets a side run ahead by design",
	"ratio-promise-after-scale":                             "a scale re-plans the planned batches from the new counts",
	"ratio-promise-in-protocol-change":                      "the pairing waiver lets a side run ahead of the planned batches by design",
	"budget-after-scale":                                    "attempts open above a budget a scale-down lowered finish before anything more opens",
	"waits-on-held-peer":                                    "where a spent retry schedule leaves the other side is an open question",
	"held":                                                  "every side with Instances left is held on its spent retries, or with the named side's Held record",
	"stranded-replacement-after-scale":                      "a side scaled up builds its new Instances on the new cohort with no old pod to keep",
	"starts-before-the-named-side-in-protocol-change":       "the pairing waiver lets the other side go first by design",
}

// ratioModelRunner reads the ratio gate model's classes.
func ratioModelRunner() huntModelRunner {
	return huntModelRunner{fullEnv: "OME_RATIO_GATE_HUNT", onlyEnv: "OME_RATIO_GATE_HUNT_ONLY", failing: ratioModelFailing, known: ratioModelKnown}
}

// TestRatioGate_ReferenceShapesResizedUnderTheRatioRule drives the
// full gate stack over the reference prefill/decode shapes under
// maintainRatio while the operator scales either side, both or the ratio
// at every moment of the roll, with and without a protocol change, under
// every update strategy pair: no hold without a way out, no two gates
// waiting on each other, no budget exceeded, the reference following the
// spec, and every lead past the allowance or serving ratio outside the
// tolerance with a known cause.
func TestRatioGate_ReferenceShapesResizedUnderTheRatioRule(t *testing.T) {
	runner := ratioModelRunner()
	runner.explore(t, ratioModelConfigs(runner.full()))
}

// findingClasses are the classes the explorer reads at a state.
func findingClasses(w *huntWorld, st *huntState) map[string]string {
	out := map[string]string{}
	for _, f := range w.check(st) {
		out[f.class] = f.message
	}
	return out
}

// TestRatioGate_TheLockstepHoldsTheLeadingSideAtItsAllowance records what
// the lockstep does at the start of a roll. 30 prefill patched in place
// beside 10 decode surging, both at 10 percent, maintainRatio at the
// chart's default tolerance of 5. Row 47's allowance for the prefill side
// is max(1, ceil(5 x 30 / 100)) = 2 of its Instances. The lockstep projects
// each patch it is asked about on the moves the side already has under
// way, so the prefill side's first pass opens 2 patches of its step of 3
// and holds the third with the lockstep's reason: 2 of 30 started against
// a decoder at 0 of 10, the allowance and no more. The explorer reads no
// lead past the allowance, and the roll finishes.
func TestRatioGate_TheLockstepHoldsTheLeadingSideAtItsAllowance(t *testing.T) {
	inPlace, surge := v1beta1.UpdateStrategyInPlaceIfPossible, v1beta1.UpdateStrategySurgeThenDrain
	cfg := ratioModelConfig("lock", [2]int32{30, 10}, inPlace, surge, ratioModelSettings()[1], false, "")
	cfg.fails, cfg.scales = 0, 0
	w := newHuntWorld(t, cfg)
	st := w.initial()
	w.settle(st, w.order(0))
	if got := st.sides[0].count(huntOpenInPlace); got != 2 {
		t.Fatalf("the prefill side's first pass must open 2 patches, its allowance at tolerance 5, opened %d: %s", got, st)
	}
	if h := st.holds[0]; h == nil || h.gate != v1beta1.RolloutHoldGateRatio || !strings.Contains(h.reason, "outpace") {
		t.Fatalf("the prefill side's third patch must be held by the lockstep: hold = %s, state %s", h, st)
	}
	if got := st.sides[1].count(huntOpenSurge, huntPromoted); got != 0 {
		t.Fatalf("the decoder must have nothing started when the prefill side opens its patches, has %d: %s", got, st)
	}
	for class, msg := range findingClasses(w, st) {
		if class == "ratio-excess" || strings.HasPrefix(class, "lead-") {
			t.Fatalf("the explorer must read no lead past the allowance, read %s: %s", class, msg)
		}
	}
	if end := w.playOut(st); !end.complete() {
		t.Fatalf("the roll must finish: %s", end)
	}
}

// TestRatioGate_DrainsAdmittedWhileThePeerPatchesInPlaceLeaveTheRatioOutsideTheTolerance
// records a reading of the serving band beside an in-place peer. 30
// prefill under RecreatePod beside 10 decode patched in place, both at 10
// percent, maintainRatio at 5 (the band 2.85 to 3.15 around 3.0). The
// prefill side's first drain projects 29:10 and is admitted; the second
// projects 28:10, outside the band, and waits. The decoder's first patch
// takes its pod out of rotation (the band is waived for it, by design), so
// the decoder serves 9 and the band now reads the prefill side's second
// drain at 28:9, inside the band, and admits it; the third is held by the
// lockstep at the prefill side's allowance of two. When the patch returns,
// 28 prefill serve against 10 decode, 2.8 against 3.0, 6.7 percent off
// under a tolerance of five; only then does the band refuse the prefill
// side's third drain and name the skew, until the drains return. The band
// reads each start against the peer's live count, which the peer's own
// band-waived patch lowers. The explorer reports the picture as
// serving-ratio-drains-admitted-on-a-peer-patch-dip; this test fails the
// day the band reads the peer at the count its patch returns to.
func TestRatioGate_DrainsAdmittedWhileThePeerPatchesInPlaceLeaveTheRatioOutsideTheTolerance(t *testing.T) {
	recreate, inPlace := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategyInPlaceIfPossible
	cfg := ratioModelConfig("lock", [2]int32{30, 10}, recreate, inPlace, ratioModelSettings()[1], false, "")
	cfg.fails, cfg.scales = 0, 0
	w := newHuntWorld(t, cfg)
	st := w.initial()
	w.settle(st, w.order(0))
	if drains, patches := st.sides[0].count(huntOpenDrain), st.sides[1].count(huntOpenInPlace); drains != 2 || patches != 1 {
		t.Fatalf("after the first passes 2 prefill drains must stand open beside the decoder's patch, got %d drains and %d patches: %s", drains, patches, st)
	}
	if h := st.holds[0]; h == nil || h.gate != v1beta1.RolloutHoldGateRatio || !strings.Contains(h.reason, "outpace") {
		t.Fatalf("the prefill side's third drain must be held by the lockstep at its allowance of two: hold = %s, state %s", h, st)
	}
	desired := map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 30, v1beta1.DecoderComponent: 10}
	full := map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 28, v1beta1.DecoderComponent: 10}
	if withinTolerance(desired, full, 5) {
		t.Fatalf("28 prefill against 10 decode must sit outside the tolerance of 5 around 3.0; the second drain is admitted only against the dipped decoder")
	}
	st = w.apply(st, huntMove{kind: "serve", side: 1, idx: 0})
	if got := st.sides[0].count(huntOpenDrain); got != 2 {
		t.Fatalf("the two prefill drains must still stand when the patch returns, got %d: %s", got, st)
	}
	serving := map[v1beta1.ComponentType]int32{
		v1beta1.EngineComponent:  w.replica(0, st.sides[0]).Status.ServingReplicas,
		v1beta1.DecoderComponent: w.replica(1, st.sides[1]).Status.ServingReplicas,
	}
	if serving[v1beta1.EngineComponent] != 28 || serving[v1beta1.DecoderComponent] != 10 || withinTolerance(desired, serving, 5) {
		t.Fatalf("28 prefill against 10 decode must serve, outside the tolerance, got %d:%d: %s", serving[v1beta1.EngineComponent], serving[v1beta1.DecoderComponent], st)
	}
	if h := st.holds[0]; h == nil || h.gate != v1beta1.RolloutHoldGateRatio || !strings.Contains(h.reason, "skew") {
		t.Fatalf("with the patch back the band must refuse the prefill side's third drain and name the skew: hold = %s, state %s", h, st)
	}
	if _, ok := findingClasses(w, st)["serving-ratio-drains-admitted-on-a-peer-patch-dip"]; !ok {
		t.Fatalf("the explorer must read the picture as serving-ratio-drains-admitted-on-a-peer-patch-dip: %s", st)
	}
	if end := w.playOut(st); !end.complete() {
		t.Fatalf("the roll must finish: %s", end)
	}
}

// TestRatioGate_AResizeReachesTheBandOneServicePassLater records how a
// resize reaches the ratio rule. 12 prefill patched in place beside 8
// decode under RecreatePod, both at 10 percent, maintainRatio at 20. The
// operator scales the decoder to 4 before the first pass. The replica's
// pass runs first: the decoder's drain is read against the recorded
// reference of 12:8 while 12 prefill serve beside 4 decode, so it holds
// on "drain would skew cross-Component serving ratio past tolerance". The
// service pass then rewrites the reference from the counts the specs ask
// for, 12:4, and the next pass admits the drain: 12:3 is 4.0 against 3.0,
// outside the tolerance but inside twice it, the one pod the band lets
// out. This test fails the day the reference stops following the spec or
// the lag grows past one service pass.
func TestRatioGate_AResizeReachesTheBandOneServicePassLater(t *testing.T) {
	inPlace, recreate := v1beta1.UpdateStrategyInPlaceIfPossible, v1beta1.UpdateStrategyRecreatePod
	cfg := ratioModelConfig("lock", [2]int32{12, 8}, inPlace, recreate, ratioModelSettings()[0], false, "")
	cfg.fails, cfg.scales = 0, 1
	w := newHuntWorld(t, cfg)
	st := w.apply(w.initial(), huntMove{kind: "scale", side: 1, delta: -4})
	if ref := referenceRatioText(st.reference); ref != "12:8" {
		t.Fatalf("the recorded reference must still read 12:8 before the service pass, reads %s", ref)
	}
	want := "drain would skew cross-Component serving ratio past tolerance"
	if h := st.holds[1]; h == nil || h.gate != v1beta1.RolloutHoldGateRatio || h.reason != want {
		t.Fatalf("before the service pass the decoder's drain must hold on the band at the old reference, holds on %s: %s", h, st)
	}
	if !w.servicePassPending(st) {
		t.Fatalf("a service pass must be pending after the resize: %s", st)
	}
	st = w.apply(st, huntMove{kind: "service-pass"})
	if ref := referenceRatioText(st.reference); ref != "12:4" || st.referenceOff {
		t.Fatalf("the service pass must write the reference from the specs, 12:4, wrote %s", ref)
	}
	if got := st.sides[1].count(huntOpenDrain); got != 1 {
		t.Fatalf("after the service pass the decoder's drain must be admitted, %d open: %s", got, st)
	}
	if end := w.playOut(st); !end.complete() {
		t.Fatalf("the roll must finish: %s", end)
	}
}

// TestRatioGate_ADecoderCutToOneTakesItsOneDrainOutsideTheTolerance
// records the band's admission of the one step no step keeps inside the
// tolerance. 12 prefill patched in place beside 8 decode under
// RecreatePod, both at 10 percent, maintainRatio at 20. An autoscaler cuts
// the decoder to one Instance (admission refuses the shape on the service,
// the replica's scale subresource does not) and the service pass records
// 12:1. The decoder's one drain projects 12:0, which no tolerance admits;
// the band admits it as the side's one drain-first step and the service
// says so with RatioStepOutsideTolerance. This test fails the day a side
// of one holds for good again or the event stops naming the step.
func TestRatioGate_ADecoderCutToOneTakesItsOneDrainOutsideTheTolerance(t *testing.T) {
	inPlace, recreate := v1beta1.UpdateStrategyInPlaceIfPossible, v1beta1.UpdateStrategyRecreatePod
	cfg := ratioModelConfig("lock", [2]int32{12, 8}, inPlace, recreate, ratioModelSettings()[0], false, "")
	cfg.fails, cfg.scales = 0, 1
	w := newHuntWorld(t, cfg)
	st := w.apply(w.initial(), huntMove{kind: "scale", side: 1, delta: -7})
	st = w.apply(st, huntMove{kind: "service-pass"})
	if ref := referenceRatioText(st.reference); ref != "12:1" {
		t.Fatalf("the service pass must record 12:1, recorded %s", ref)
	}
	if got := st.sides[1].count(huntOpenDrain); got != 1 {
		t.Fatalf("the decoder's one drain must be admitted, %d open: %s", got, st)
	}
	found := false
	for _, e := range st.events {
		if strings.Contains(e, EventReasonRatioStepOutsideTolerance) && strings.Contains(e, "decoder 0 of 1 serving after the drain; no single step fits tolerance 20 at 12:1; admitting one") {
			found = true
		}
	}
	if !found {
		t.Fatalf("the service must say the step runs outside the tolerance, events %q: %s", st.events, st)
	}
	if end := w.playOut(st); !end.complete() {
		t.Fatalf("the roll must finish: %s", end)
	}
}

// TestRatioGate_TheLockstepLetsASideFinishWhosePeerThePairingGatesHoldStill
// drives the picture where the drain gate keeps the decoder's last old pod
// for the prefill side and the lockstep reads the prefill as too far ahead
// of that decoder. 12 prefill patched in place
// beside 4 decode surging, both at a budget of one, maintainRatio at the
// chart's default tolerance of 5, a pairing-protocol change. The prefill
// side reaches 10 of 12 promoted and the decoder 3 of 4 with its last
// replacement serving beside its old pod. The drain gate keeps that old
// pod: the two old prefill Instances need a partner of the old cohort, and
// letting it go would leave the prefill side with 10 pairable Instances,
// below the 11 the drain gate keeps (12 less a maxUnavailable of 1). The
// lockstep reads promoted Instances, and 10 of 12 leads 3 of 4 by 8.3
// points, more than 5; but the decoder can move only once the prefill has,
// so the lockstep does not pace the prefill by it, as the batch step does
// not wait on a peer the pairing gates hold still: the two patches run
// beside the kept pod, the drain gate releases it once every old prefill
// has moved, and the decoder promotes its last Instance. The stop needs the
// smaller side's step, one Instance in its count, to be wider than the
// tolerance plus the larger side's maxUnavailable share, so at 10 percent
// the reference shapes never meet it; a resize that makes a side coarser
// (12:4 cut to 12:2 at tolerance 20, 12:8 grown to 18:4 at 5) does. The
// explorer fails the picture as ratio-rule-waits-on-the-pairing-gate.
func TestRatioGate_TheLockstepLetsASideFinishWhosePeerThePairingGatesHoldStill(t *testing.T) {
	inPlace, surge := v1beta1.UpdateStrategyInPlaceIfPossible, v1beta1.UpdateStrategySurgeThenDrain
	cfg := ratioModelConfig("lock", [2]int32{12, 4}, inPlace, surge, ratioModelSettings()[1], true, "")
	cfg.fails, cfg.scales = 0, 0
	cfg.members[0].budget, cfg.members[1].budget = "1", "1"
	w := newHuntWorld(t, cfg)
	st := w.initial()
	w.settle(st, w.order(0))
	end := w.playOut(st)
	if !end.complete() {
		t.Fatalf("the roll must finish: a peer the pairing gates hold still does not pace the lockstep: %s", end)
	}
	if end.sides[0].count(huntPromoted) != 12 || end.sides[1].count(huntPromoted) != 4 {
		t.Fatalf("every Instance of both sides must promote, stands %s | %s", end.sides[0], end.sides[1])
	}
	if end.holds[0] != nil || end.holds[1] != nil {
		t.Fatalf("nothing holds once the roll is done: %s | %s", end.holds[0], end.holds[1])
	}
}
