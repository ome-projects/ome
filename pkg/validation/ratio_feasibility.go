package validation

import (
	"fmt"
	"strings"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// RatioDrainDefaults carries the operator-configured fills the drain
// feasibility rule reads, so admission judges a group as the rollout will
// run it. Both come from the operator ConfigMap, read once at startup.
type RatioDrainDefaults struct {
	// TolerancePercent fills a group that sets maintainRatio but omits its
	// tolerance (the coordination block). Nil means no default is
	// configured: such a group rolls with no drift bound and is not judged.
	TolerancePercent *int32
	// MinReplicas fills a Component that sets no minReplicas (the
	// deploy.replicas block), the fill the reconciler applies before it
	// projects the Instance count. Nil means unconfigured, and an unset
	// count then resolves to the API default of one.
	MinReplicas *int
}

// ValidateRatioDrainFeasibility rejects a maintainRatio group whose drain-first
// member could never take its first rollout step. The ratio check at runtime
// reads a drain as one Instance leaving rotation and admits it only while the
// live serving ratio of every member pair stays within the tolerance of the
// ratio the rollout started at, or, for a single drain from a fully serving
// group, within twice the tolerance; it never admits a drain that empties a
// member's side. A member whose single drain fails even the relaxed rule is
// held before its first step and the rollout parks for good, so admission
// refuses the spec instead.
//
// A group whose engine and decoder both step is paced by the batch step
// instead of the ratio check: a drain-first member counts its drains in
// batches of its maxUnavailable, a SurgeThenDrain member its surges in
// batches of its maxSurge, and none opens its next batch until every one's
// previous batch serves, which keeps the ratio at every batch boundary. A
// router does not decide that. The ratio check is never consulted for a
// drain of such a group, so nothing here refuses it (batchStepGoverns). The
// refusal stays for a group whose engine or decoder patches single pods in
// place or leaves its strategy unset beside a drain-first member, and for a
// group that lacks one of the two.
//
// The operator's prefillDecodeRoll.keepRatio setting widens the batch step to
// every path of a group that lists both the engine and the decoder, counts a
// pairing member that patches single pods in place, and never parks a roll;
// admission does not read that setting, so the refusal below stays as the
// reading of the engine without it.
//
// A member rolls drain-first when its update strategy is RecreatePod, or an
// in-place strategy on a Component whose Instances are multi-node gangs (an
// in-place update rebuilds the whole gang, and the ratio check reads the
// rebuild as a drain). SurgeThenDrain members and single-pod in-place members
// add or keep capacity and are not checked. An unset strategy is not classified
// here: it resolves through the runtime and operator config, which admission
// does not read.
//
// Replicas are read the way the rollout sizes a member: minReplicas when set
// and positive, else maxReplicas when set, else the count the reconciler
// projects for an unset field: the configured minReplicas default, else the
// API default of one. A Component the group names but the spec does not
// declare is left out of the pairwise check.
func ValidateRatioDrainFeasibility(spec *v1beta1.InferenceServiceSpec, defaults RatioDrainDefaults) error {
	groups := spec.GetRolloutGroups()
	for i := range groups {
		g := &groups[i]
		if g.MaintainRatio == nil || rolloutGroupKind(g) == v1beta1.RolloutProgressionCanary {
			continue
		}
		tolerance := g.MaintainRatio.Tolerance
		if tolerance == nil {
			tolerance = defaults.TolerancePercent
		}
		if tolerance == nil {
			continue
		}
		members := ratioGroupMembers(spec, g.Components, defaults.MinReplicas)
		if len(members) < 2 || batchStepGoverns(members) {
			continue
		}
		for _, m := range members {
			if m.drainFirst == "" {
				continue
			}
			if err := ratioDrainFeasible(i, m, members, *tolerance); err != nil {
				return err
			}
		}
	}
	return nil
}

// ratioMember is one group member as the feasibility rule sees it: its
// replica count, the unit a drain removes, why it rolls drain-first (empty
// when it does not), and whether it declares SurgeThenDrain.
type ratioMember struct {
	component  v1beta1.ComponentType
	replicas   int32
	unit       string
	drainFirst string
	surges     bool
}

// steps reports whether the batch step counts the member's starts: it rolls
// drain-first or surges. A member with no declared strategy, or one that
// patches single pods in place, does not step at admission.
func (m ratioMember) steps() bool { return m.drainFirst != "" || m.surges }

// batchStepGoverns reports whether the rollout paces the group by the batch
// step rather than the ratio check: the group lists both the engine and the
// decoder, and each of them steps; a router does not decide it. It is the
// admission reading of the engine's rule, taken from the declared strategies
// and shapes rather than the replicas; a group that lists both is never the
// Sequential chain, which the engine also leaves out.
func batchStepGoverns(members []ratioMember) bool {
	var engine, decoder bool
	for _, m := range members {
		switch m.component {
		case v1beta1.EngineComponent:
			engine = m.steps()
		case v1beta1.DecoderComponent:
			decoder = m.steps()
		}
	}
	return engine && decoder
}

// batchStepWayOut is the way out a refusal offers through the batch step,
// for a group that lists both the engine and the decoder: giving each
// pairing member that does not step a strategy that does hands the pacing
// to the step. Empty for any other group.
func batchStepWayOut(members []ratioMember) string {
	var engine, decoder bool
	var still []string
	for _, m := range members {
		if m.component != v1beta1.EngineComponent && m.component != v1beta1.DecoderComponent {
			continue
		}
		engine = engine || m.component == v1beta1.EngineComponent
		decoder = decoder || m.component == v1beta1.DecoderComponent
		if !m.steps() {
			still = append(still, string(m.component))
		}
	}
	if !engine || !decoder || len(still) == 0 {
		return ""
	}
	return fmt.Sprintf("give the %s a SurgeThenDrain or RecreatePod update strategy, which hands the pacing to the batch step", strings.Join(still, " and the "))
}

// waysOut words the refusal's alternatives as one list, empty ones left out:
// every one but the last after a comma, the last after "or".
func waysOut(ways ...string) string {
	kept := make([]string, 0, len(ways))
	for _, w := range ways {
		if w != "" {
			kept = append(kept, w)
		}
	}
	if len(kept) <= 1 {
		return strings.Join(kept, "")
	}
	return strings.Join(kept[:len(kept)-1], ", ") + ", or " + kept[len(kept)-1]
}

// ratioGroupMembers lists the group's distinct declared Components, in
// declaration order, each at the replica count the rollout would size it at.
func ratioGroupMembers(spec *v1beta1.InferenceServiceSpec, components []v1beta1.ComponentType, defaultMinReplicas *int) []ratioMember {
	seen := make(map[v1beta1.ComponentType]struct{}, len(components))
	members := make([]ratioMember, 0, len(components))
	for _, c := range components {
		if _, dup := seen[c]; dup {
			continue
		}
		seen[c] = struct{}{}
		replicas := componentReplicaBasis(spec, c, defaultMinReplicas)
		if replicas <= 0 {
			continue
		}
		strategy, gang := componentUpdateShape(spec, c)
		m := ratioMember{component: c, replicas: replicas, unit: "pod", surges: strategy == v1beta1.UpdateStrategySurgeThenDrain}
		if gang {
			m.unit = "gang"
		}
		switch {
		case strategy == v1beta1.UpdateStrategyRecreatePod:
			m.drainFirst = string(strategy)
		case gang && (strategy == v1beta1.UpdateStrategyInPlaceIfPossible || strategy == v1beta1.UpdateStrategyInPlaceOnly):
			m.drainFirst = fmt.Sprintf("%s on multi-node gangs, which rebuilds the whole gang", strategy)
		}
		members = append(members, m)
	}
	return members
}

// ratioDrainFeasible projects one drain of m against every peer exactly as
// the runtime ratio check does: the starting pairwise ratio is the larger
// count over the smaller, the projected ratio the same with m one unit down,
// and the projection must stay within twice the tolerance of the start. A
// member of one has no admissible drain at all.
func ratioDrainFeasible(groupIdx int, m ratioMember, members []ratioMember, tolerancePercent int32) error {
	if m.replicas == 1 {
		return fmt.Errorf("spec.rollout.groups[%d]: the %s rolls drain-first (%s) in a maintainRatio group with a single replica, so its only drain leaves it with nothing serving, which the ratio check refuses, and the rollout would park for good; %s (%s)",
			groupIdx, m.component, m.drainFirst,
			waysOut(fmt.Sprintf("give the %s the SurgeThenDrain update strategy", m.component), fmt.Sprintf("add %s replicas", m.component), batchStepWayOut(members)),
			ReasonRatioDrainInfeasible)
	}
	band := 2 * float64(tolerancePercent) / 100.0
	projected := m.replicas - 1
	for _, p := range members {
		if p.component == m.component {
			continue
		}
		if ratioWithinBand(m.replicas, p.replicas, projected, p.replicas, band) {
			continue
		}
		offPoints := ratioOffPoints(m.replicas, p.replicas, projected, p.replicas)
		return fmt.Errorf("spec.rollout.groups[%d]: the %s rolls drain-first (%s) in a maintainRatio group, and its first step could never be admitted: with %s %d and %s %d, one %s %s out reads %s %d : %s %d, %d points off the starting ratio, and the ratio check lets a single drain go at most %d points (twice the tolerance of %d), so the rollout would park for good; %s (%s)",
			groupIdx, m.component, m.drainFirst, m.component, m.replicas, p.component, p.replicas,
			m.component, m.unit, m.component, projected, p.component, p.replicas, offPoints,
			2*tolerancePercent, tolerancePercent,
			waysOut(fmt.Sprintf("widen maintainRatio.tolerance to at least %d", ratioToleranceNeeded(m.replicas, p.replicas, projected, p.replicas)),
				fmt.Sprintf("give the %s the SurgeThenDrain update strategy", m.component), fmt.Sprintf("add %s replicas", m.component), batchStepWayOut(members)),
			ReasonRatioDrainInfeasible)
	}
	return nil
}

// ratioWithinBand is the runtime ratio check's pairwise test on one pair:
// the starting ratio is the larger original count over the smaller, the
// projected ratio the larger projected count over the smaller, and the
// projection passes when it lies within start times one plus or minus band,
// bounds included. A side projected to nothing serving is out.
func ratioWithinBand(origA, origB, projA, projB int32, band float64) bool {
	if origA < origB {
		origA, origB = origB, origA
	}
	origRatio := float64(origA) / float64(origB)
	if projA < projB {
		projA, projB = projB, projA
	}
	if projB <= 0 {
		return false
	}
	projRatio := float64(projA) / float64(projB)
	return projRatio >= origRatio*(1-band) && projRatio <= origRatio*(1+band)
}

// ratioOffPoints is how far, in percent of the starting ratio, the projected
// pairwise ratio sits from it, rounded to the nearest point.
func ratioOffPoints(origA, origB, projA, projB int32) int32 {
	if origA < origB {
		origA, origB = origB, origA
	}
	if projA < projB {
		projA, projB = projB, projA
	}
	origRatio := float64(origA) / float64(origB)
	projRatio := float64(projA) / float64(projB)
	off := (projRatio - origRatio) / origRatio
	if off < 0 {
		off = -off
	}
	return int32(off*100 + 0.5)
}

// ratioToleranceNeeded is the smallest whole tolerance under which the
// projected pair passes the relaxed single-drain rule, found with the same
// band test the rejection used so the suggestion is one that admits.
func ratioToleranceNeeded(origA, origB, projA, projB int32) int32 {
	for t := int32(1); t <= 100; t++ {
		if ratioWithinBand(origA, origB, projA, projB, 2*float64(t)/100.0) {
			return t
		}
	}
	return 100
}

// componentReplicaBasis is the replica count the rollout sizes a Component
// at: minReplicas when set and positive, else maxReplicas when set, else the
// count the reconciler projects for a Component that sets neither. That
// projection fills an unset minReplicas from the configured default before
// it reads the field, and reads a still-unset or non-positive field as the
// API default of one. Zero only for a Component the spec does not declare.
func componentReplicaBasis(spec *v1beta1.InferenceServiceSpec, c v1beta1.ComponentType, defaultMinReplicas *int) int32 {
	role := declaredRoles(spec).find(c)
	if role == nil {
		return 0
	}
	ext := role.extension
	if ext.MinReplicas != nil && *ext.MinReplicas > 0 {
		return int32(*ext.MinReplicas)
	}
	if ext.MaxReplicas > 0 {
		return int32(ext.MaxReplicas)
	}
	if defaultMinReplicas != nil && *defaultMinReplicas > 0 {
		return int32(*defaultMinReplicas)
	}
	return int32(constants.DefaultMinReplicas)
}

// componentUpdateShape reports the Component's declared update strategy
// (empty when unset) and whether its Instances are multi-pod gangs.
func componentUpdateShape(spec *v1beta1.InferenceServiceSpec, c v1beta1.ComponentType) (v1beta1.UpdateStrategyType, bool) {
	role := declaredRoles(spec).find(c)
	if role == nil {
		return "", false
	}
	var strategy v1beta1.UpdateStrategyType
	if lc := role.extension.Lifecycle; lc != nil && lc.UpdateStrategy != nil {
		strategy = lc.UpdateStrategy.Type
	}
	return strategy, DeclaredMultiPod(role.leader, role.worker)
}

// DeclaredMultiPod reports whether a Component declares a Leader+Worker pair
// whose size is unset or positive; an unset size resolves to one worker at
// reconcile time, so every such Instance is a gang of more than one pod.
func DeclaredMultiPod(leader *v1beta1.LeaderSpec, worker *v1beta1.WorkerSpec) bool {
	return leader != nil && worker != nil && (worker.Size == nil || *worker.Size > 0)
}
