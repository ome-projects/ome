package coordination

import (
	"fmt"
	"maps"
	"math/rand"
	"testing"

	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// This file drives the ratio rule of a prefill/decode group (the lockstep
// and the serving band of ratio.go, and the batch step where it governs
// the pair) through the full gate stack while a proportional scaling
// policy sizes one side from the other, in the world of
// pd_pacing_gates_never_wait_on_each_other_test.go with the readings of
// ratio_gate_model_test.go. The policy is the coordinator's: the operator
// scales the anchor alone, and the follower's count is written one service
// pass later from the anchor's replica (scaling.Target over the ratio, the
// follower's minimum of one), a resize the operator did not make. The
// configurations are the reference shapes at 10 percent under the pairs
// the rule reaches (one or both sides patched in place) and the pairs the
// batch step governs, with the decoder following the engine and the
// engine following the decoder, under the wide tolerance, the
// chart's default and the chart's keepRatio setting beside the rule.
// Beside them run configurations with the group's maxUnavailable and
// maxSurge at one Instance: a side of twelve or more Instances has an own
// budget of two or three at 10 percent, so its second drain is the group's
// unavailability gate's to deny and its second surge the group's surge
// gate's, and the budget reading is the production gates' rather than the
// world's own arithmetic alone.
//
// At every moment of the roll (idle, settling, rotating, draining, the
// last Instance, a side held on its spent retries) the operator edits the
// policy: adds one where none stands, at the ratio the counts already
// keep, at one that cuts the follower by half and at one that raises it by
// half; moves a standing policy's ratio down or up by half of the
// follower; moves the anchor onto the other side at the ratio that keeps
// its count; removes the policy, which leaves the follower at the count it
// carries. Beside the edits the anchor is scaled by one Instance or by
// half, each way, and one attempt fails.
//
// The readings are the world's (no hold without a way out, no two gates
// waiting on each other, no side past its own budget and no group past
// its budget, the batch step's shares where it governs) and the ratio
// rule's (ratioReadings: the two sides' shares against the allowance the
// ratio rule grants, the live serving ratio against the tolerance, the
// reference against the counts once the service pass ran). The
// coordinator's write of the follower is read as the scale it is; a
// reading with no known cause fails.
//
// Left out: no partition, no canary unit and so no canary-step moment, no
// pause, no rollback and no second push; gangs and a router beside the
// pair appear only in the full mode's sample.
//
// OME_RATIO_POLICY_HUNT=full widens the configurations and the bounds;
// OME_RATIO_POLICY_HUNT_ONLY=<text> keeps the configurations whose name
// contains the text.

// huntPolicy is the proportional scaling policy in force at a state: the
// pairing side whose count the coordinator writes (the follower) from the
// other side's (the anchor), and the ratio in thousandths. The zero value
// is no policy.
type huntPolicy struct {
	follower v1beta1.ComponentType
	milli    int64
}

func (p huntPolicy) String() string {
	if p.follower == "" {
		return "none"
	}
	return fmt.Sprintf("%s follows %s at %d.%03d", p.follower, pairingPeer(p.follower), p.milli/1000, p.milli%1000)
}

// initialPolicy is the policy the roll starts under: the configured
// follower at the ratio that gives it its configured count, none without
// a follower.
func (c huntConfig) initialPolicy() huntPolicy {
	if c.follower == "" {
		return huntPolicy{}
	}
	return huntPolicy{follower: c.follower, milli: c.followerMilli()}
}

// policySides is the index of the following side and of its anchor under
// the policy in force at a state, -1 for both when none stands.
func (w *huntWorld) policySides(st *huntState) (follower, anchor int) {
	follower, anchor = -1, -1
	if st.policy.follower == "" {
		return follower, anchor
	}
	for i, m := range w.cfg.members {
		switch m.comp {
		case st.policy.follower:
			follower = i
		case pairingPeer(st.policy.follower):
			anchor = i
		}
	}
	if follower < 0 || anchor < 0 {
		return -1, -1
	}
	return follower, anchor
}

// milliFor is the ratio in thousandths, the largest that gives the
// follower exactly target Instances from the anchor's count: scaling.Target
// rounds the product up, so target x 1000 / anchor rounded down lands on
// target for every anchor count under a thousand.
func milliFor(target, anchor int32) int64 {
	return int64(target) * 1000 / int64(anchor)
}

// followerTargets are the counts a policy edit may move the follower to
// from its count now: half of it taken away and half of it added, each
// at least one Instance and other than the count itself.
func followerTargets(n int32) []int32 {
	half := max(n/2, 1)
	var out []int32
	for _, t := range []int32{n - half, n + half} {
		if t >= 1 && t != n {
			out = append(out, t)
		}
	}
	return out
}

// policyMoves are the operator's edits of the scaling policy at a state,
// while the configuration has edits left: with no policy standing, one
// added with the decoder following the engine at the ratio the counts
// keep, at one that cuts the decoder by half and at one that raises it by
// half, and one with the engine following the decoder at the ratio the
// counts keep; with a policy standing, its ratio moved so the follower
// loses or gains half of itself, the anchor moved onto the other side at
// the ratio that keeps its count, and the policy removed.
func (w *huntWorld) policyMoves(st *huntState) []huntMove {
	if st.policyEdits <= 0 {
		return nil
	}
	eng, dec := w.pairIndex()
	if eng < 0 || dec < 0 {
		return nil
	}
	var out []huntMove
	add := func(p huntPolicy) { out = append(out, huntMove{kind: "policy", policy: p}) }
	count := func(i int) int32 { return int32(len(st.sides[i].rows)) }
	follower, anchor := w.policySides(st)
	if follower < 0 {
		add(huntPolicy{follower: v1beta1.DecoderComponent, milli: milliFor(count(dec), count(eng))})
		for _, t := range followerTargets(count(dec)) {
			add(huntPolicy{follower: v1beta1.DecoderComponent, milli: milliFor(t, count(eng))})
		}
		add(huntPolicy{follower: v1beta1.EngineComponent, milli: milliFor(count(eng), count(dec))})
		return out
	}
	for _, t := range followerTargets(count(follower)) {
		add(huntPolicy{follower: st.policy.follower, milli: milliFor(t, count(anchor))})
	}
	add(huntPolicy{follower: w.cfg.members[anchor].comp, milli: milliFor(count(anchor), count(follower))})
	add(huntPolicy{})
	return out
}

// strategyPair is the update strategies of the engine and the decoder.
type strategyPair = [2]v1beta1.UpdateStrategyType

// policyModelFollowers are the sides a policy may size when the roll
// starts: the decoder, the engine, and none for a policy added mid-roll.
var policyModelFollowers = []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent, ""}

// policyModelName names a configuration by its family, its members and
// settings, its policy edits and the ratio rule's setting.
func policyModelName(family string, cfg huntConfig, setting ratioModelSetting) string {
	return fmt.Sprintf("%s: %s policyEdits=%d %s", family, cfg.String(), cfg.policyEdits, setting.name)
}

// policyModelConfig is one configuration of the pair at a reference
// shape, both sides at 10 percent, under a setting of the ratio rule, with
// the follower named (none for a policy added mid-roll), one policy edit
// and one scale of the anchor tried at every moment, one failure, and the
// ratio readings on.
func policyModelConfig(family string, shape [2]int32, es, ds v1beta1.UpdateStrategyType, setting ratioModelSetting, follower v1beta1.ComponentType) huntConfig {
	cfg := ratioModelConfig(family, shape, es, ds, setting, false, "")
	cfg.follower, cfg.policyEdits = follower, 1
	cfg.name = policyModelName(family, cfg, setting)
	return cfg
}

// policyModelGroupConfig is policyModelConfig with the group's
// maxUnavailable and maxSurge at one Instance. No side's own 10 percent
// at the reference shapes goes below one, and a side of twelve or more
// Instances stands above it, so that side's second drain or surge is the
// group's budget gate's alone to deny.
func policyModelGroupConfig(shape [2]int32, pair strategyPair, setting ratioModelSetting, follower v1beta1.ComponentType) huntConfig {
	cfg := policyModelConfig("group", shape, pair[0], pair[1], setting, follower)
	cfg.groupUnavail, cfg.groupSurge = "1", "1"
	cfg.name = policyModelName("group", cfg, setting)
	return cfg
}

// withTwoEditsAndScales gives a configuration the full mode's bounds, two
// policy edits and two scales of the anchor, and names it again.
func withTwoEditsAndScales(family string, cfg huntConfig, setting ratioModelSetting) huntConfig {
	cfg.scales, cfg.policyEdits = 2, 2
	cfg.name = policyModelName(family, cfg, setting)
	return cfg
}

// policyModelSettings are the settings explored: the wide tolerance;
// the wide tolerance beside the chart's keepRatio setting; the
// chart's default tolerance; maintainRatio left to the operator's default;
// and in full mode the chart's keepRatio setting alone, where the batch
// step plans the shares and the rule's own readings are off.
func policyModelSettings(full bool) []ratioModelSetting {
	settings := ratioModelSettings()
	out := []ratioModelSetting{settings[0], settings[3], settings[1], settings[2]}
	if full {
		out = append(out, ratioModelSetting{name: "chart", defaults: huntRules(true, ptr.To(int32(5)), v1beta1.DecoderComponent)})
	}
	return out
}

// policyModelPairs are the strategy pairs explored: the pairs the ratio
// rule reaches (one or both sides patched in place), then the pairs the
// batch step governs.
func policyModelPairs() []strategyPair {
	recreate, surge, inPlace := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain, v1beta1.UpdateStrategyInPlaceIfPossible
	return []strategyPair{
		{inPlace, inPlace}, {inPlace, recreate}, {surge, inPlace}, {recreate, inPlace}, {inPlace, surge},
		{recreate, recreate}, {surge, surge}, {recreate, surge}, {surge, recreate},
	}
}

// policyModelConfigs lists the configurations: in bounded mode the
// decoder following the engine at three reference shapes under four
// strategy pairs, with the wide tolerance alone and beside the
// chart's setting, the engine following the decoder, the square shape, a
// policy added mid-roll, and ten configurations with the group's budgets
// at one Instance; in full mode every reference shape, strategy pair,
// setting and follower with two edits and two scales, the same shapes and
// pairs with the group's budgets at one Instance, and a seeded sample with
// gangs, a protocol change and a router beside the pair.
func policyModelConfigs(full bool) []huntConfig {
	settings := policyModelSettings(full)
	if full {
		out := append(policyModelFullConfigs(settings), policyModelFullGroupConfigs(settings)...)
		return append(out, policyModelSample(settings)...)
	}
	return append(policyModelBoundedConfigs(settings), policyModelBoundedGroupConfigs(settings)...)
}

// policyModelBoundedConfigs are the configurations CI explores with each
// side's own budget alone.
func policyModelBoundedConfigs(settings []ratioModelSetting) []huntConfig {
	recreate, surge, inPlace := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain, v1beta1.UpdateStrategyInPlaceIfPossible
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	wide, chart := settings[0], settings[1]
	pairs := []strategyPair{{inPlace, inPlace}, {inPlace, recreate}, {recreate, recreate}, {surge, surge}}
	var out []huntConfig
	for _, shape := range [][2]int32{{12, 8}, {30, 10}, {12, 4}} {
		for _, pair := range pairs {
			for _, setting := range []ratioModelSetting{wide, chart} {
				out = append(out, policyModelConfig("policy", shape, pair[0], pair[1], setting, dec))
			}
		}
	}
	for _, pair := range pairs {
		out = append(out, policyModelConfig("policy", [2]int32{12, 8}, pair[0], pair[1], wide, eng))
	}
	for _, pair := range pairs[:1] {
		out = append(out, policyModelConfig("policy", [2]int32{15, 15}, pair[0], pair[1], wide, dec),
			policyModelConfig("policy", [2]int32{15, 15}, pair[0], pair[1], wide, eng))
	}
	out = append(out,
		policyModelConfig("policy", [2]int32{15, 15}, surge, surge, wide, dec),
		policyModelConfig("policy", [2]int32{15, 15}, surge, surge, wide, eng))
	for _, pair := range []strategyPair{{inPlace, recreate}, {recreate, recreate}} {
		for _, setting := range []ratioModelSetting{wide, chart} {
			out = append(out, policyModelConfig("added", [2]int32{12, 8}, pair[0], pair[1], setting, ""))
		}
	}
	return out
}

// policyModelBoundedGroupConfigs are the configurations CI explores with
// the group's budgets at one Instance: the pairs the rule reaches at the
// shapes whose patched or surging side has an own budget of two or three,
// the pairs the batch step governs, the chart's keepRatio setting beside
// the rule, and a policy added mid-roll.
func policyModelBoundedGroupConfigs(settings []ratioModelSetting) []huntConfig {
	recreate, surge, inPlace := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain, v1beta1.UpdateStrategyInPlaceIfPossible
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	wide, chart := settings[0], settings[1]
	return []huntConfig{
		policyModelGroupConfig([2]int32{30, 10}, strategyPair{inPlace, inPlace}, wide, dec),
		policyModelGroupConfig([2]int32{12, 4}, strategyPair{inPlace, inPlace}, wide, dec),
		policyModelGroupConfig([2]int32{12, 8}, strategyPair{inPlace, recreate}, wide, dec),
		policyModelGroupConfig([2]int32{30, 10}, strategyPair{surge, inPlace}, wide, dec),
		policyModelGroupConfig([2]int32{12, 4}, strategyPair{surge, inPlace}, wide, dec),
		policyModelGroupConfig([2]int32{15, 15}, strategyPair{inPlace, surge}, wide, eng),
		policyModelGroupConfig([2]int32{30, 10}, strategyPair{recreate, recreate}, wide, dec),
		policyModelGroupConfig([2]int32{12, 8}, strategyPair{surge, surge}, wide, dec),
		policyModelGroupConfig([2]int32{12, 8}, strategyPair{inPlace, recreate}, chart, dec),
		policyModelGroupConfig([2]int32{12, 8}, strategyPair{inPlace, recreate}, wide, ""),
	}
}

// policyModelFullConfigs are every reference shape, strategy pair,
// setting and follower, with two policy edits and two scales of the
// anchor.
func policyModelFullConfigs(settings []ratioModelSetting) []huntConfig {
	var out []huntConfig
	for _, shape := range batchModelShapes {
		for _, pair := range policyModelPairs() {
			out = append(out, policyModelFullConfigsOf(shape, pair, settings)...)
		}
	}
	return out
}

// policyModelFullConfigsOf is one shape and strategy pair under every
// setting and follower, with the full mode's bounds.
func policyModelFullConfigsOf(shape [2]int32, pair strategyPair, settings []ratioModelSetting) []huntConfig {
	var out []huntConfig
	for _, setting := range settings {
		for _, follower := range policyModelFollowers {
			out = append(out, withTwoEditsAndScales("policy", policyModelConfig("policy", shape, pair[0], pair[1], setting, follower), setting))
		}
	}
	return out
}

// policyModelFullGroupConfigs are every reference shape and strategy
// pair with the group's budgets at one Instance: under the wide
// tolerance with every follower, and under the chart's keepRatio setting
// beside the rule with the decoder following; the full mode's bounds.
func policyModelFullGroupConfigs(settings []ratioModelSetting) []huntConfig {
	wide, chart := settings[0], settings[1]
	var out []huntConfig
	for _, shape := range batchModelShapes {
		for _, pair := range policyModelPairs() {
			for _, follower := range policyModelFollowers {
				out = append(out, withTwoEditsAndScales("group", policyModelGroupConfig(shape, pair, wide, follower), wide))
			}
			out = append(out, withTwoEditsAndScales("group", policyModelGroupConfig(shape, pair, chart, v1beta1.DecoderComponent), chart))
		}
	}
	return out
}

// policyModelSample is a seeded sample over the whole space: a protocol
// change, a router beside the pair, gangs of two and four, one or two
// edits and scales, up to two failures and a retry schedule of one.
func policyModelSample(settings []ratioModelSetting) []huntConfig {
	recreate, surge := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain
	pairs := policyModelPairs()
	routers := []v1beta1.UpdateStrategyType{"", recreate, surge}
	rng := rand.New(rand.NewSource(20261110))
	var out []huntConfig
	for n := 0; n < 120; n++ {
		shape := batchModelShapes[rng.Intn(len(batchModelShapes))]
		pair := pairs[rng.Intn(len(pairs))]
		setting := settings[rng.Intn(len(settings))]
		cfg := ratioModelConfig("sample", shape, pair[0], pair[1], setting, rng.Intn(3) == 0, routers[rng.Intn(len(routers))])
		cfg.follower, cfg.policyEdits = policyModelFollowers[rng.Intn(len(policyModelFollowers))], int8(1+rng.Intn(2))
		cfg.scales, cfg.fails = int8(1+rng.Intn(2)), int8(rng.Intn(3))
		if rng.Intn(4) == 0 {
			cfg.maxAttempts = 1
		}
		if rng.Intn(4) == 0 {
			cfg = gangs(cfg, []int32{2, 4}[rng.Intn(2)], []int32{2, 4}[rng.Intn(2)])
			cfg.losses, cfg.refills = 1, 1
		}
		cfg.name = policyModelName("sample", cfg, setting)
		out = append(out, cfg)
	}
	return out
}

// policyModelKnown are the classes that record a known reading of today's
// engine beside the ratio gate model's: the coordinator's follower write
// is a scale the operator did not make, one service pass after the
// anchor's or after the policy edit, and is read as one (the batch-step
// events model's reading); a gang short of a worker that the roll rebuilt
// with no gate asked, among the sampled gangs, is the batch-step model's
// decided reading. A class taken from a sibling model must be a reading
// there, or a renamed key would become a known class with an empty
// reading that never fails.
func policyModelKnown(t *testing.T) map[string]string {
	t.Helper()
	out := maps.Clone(ratioModelKnown)
	take := func(from map[string]string, model string, classes ...string) {
		for _, class := range classes {
			text, ok := from[class]
			if !ok || text == "" {
				t.Fatalf("class %q is not a known reading of %s", class, model)
			}
			out[class] = text
		}
	}
	take(eventModelKnown(), "the batch-step events model", "version-mix-from-a-follower-scale")
	take(batchModelKnown, "the batch-step model", "batch-promise-after-a-rebuild", "ratio-promise-after-a-rebuild", "version-mix-after-a-rebuild")
	return out
}

// policyModelRunner reads the policy model's classes: the ratio gate
// model's failing classes and known readings, with the follower's write
// beside them.
func policyModelRunner(t *testing.T) huntModelRunner {
	return huntModelRunner{fullEnv: "OME_RATIO_POLICY_HUNT", onlyEnv: "OME_RATIO_POLICY_HUNT_ONLY", failing: ratioModelFailing, known: policyModelKnown(t)}
}

// TestRatioGate_ReferenceShapesUnderAScalingPolicyEditedMidRoll drives
// the full gate stack over the reference prefill/decode shapes
// under maintainRatio while a proportional scaling policy sizes one side
// from the other and the operator adds, edits or removes the policy and
// scales the anchor at every moment of the roll: no hold without a way
// out, no two gates waiting on each other, no side past its own budget
// and no group past its budget, the reference following the spec, and
// every lead past the allowance or serving ratio outside the tolerance
// with a known cause.
func TestRatioGate_ReferenceShapesUnderAScalingPolicyEditedMidRoll(t *testing.T) {
	runner := policyModelRunner(t)
	runner.explore(t, policyModelConfigs(runner.full()))
}
