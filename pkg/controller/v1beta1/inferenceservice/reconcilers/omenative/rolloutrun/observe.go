package rolloutrun

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/rollout"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
	"sigs.k8s.io/ome/pkg/validation"
)

// targetPair is one grouped Component's observed revision state, read from its
// InferenceReplica: current is where the Component is, target where its spec
// points (empty values mean no IR, or no revision minted yet), and the
// replica counters that expose STRAGGLERS — instances not on the target
// revision even when current==target, which is exactly the state a mid-roll
// revert produces (target back to v1 while instances sit on v2).
type targetPair struct {
	current  string
	target   string
	replicas int32
	updated  int32
}

// straggling reports Instances still off the target revision. Paired with an
// agreeing revision pair it marks a Component that owes a roll, as opposed to
// one that has arrived.
func (t targetPair) straggling() bool {
	return t.replicas > 0 && t.updated < t.replicas
}

// observeGroupTargets reads the IR revision pair for every Component named in
// any spec rollout group, via the live reader (the run decision must not act
// on a cache-lagged target). fresh is false while any observed IR's status
// lags its generation — revision names from an older IR generation describe a
// different desired workload, so the caller waits for one consistent snapshot
// rather than opening a run against it.
func observeGroupTargets(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService) (map[v1beta1.ComponentType]targetPair, bool, error) {
	out := map[v1beta1.ComponentType]targetPair{}
	if isvc.Spec.Rollout == nil {
		return out, true, nil
	}
	fresh := true
	for gi := range isvc.Spec.Rollout.Groups {
		for _, comp := range isvc.Spec.Rollout.Groups[gi].Components {
			if _, seen := out[comp]; seen {
				continue
			}
			ir := &v1beta1.InferenceReplica{}
			key := irprojector.RoleReplicaKey(isvc, comp)
			if err := reads.Get(ctx, key, ir); err != nil {
				if apierrors.IsNotFound(err) {
					out[comp] = targetPair{}
					continue
				}
				return nil, false, err
			}
			if ir.Status.ObservedGeneration != ir.Generation || projectionTrails(isvc, ir) {
				fresh = false
			}
			current := query.RevisionFromName(ir.Status.CurrentRevision).Hash()
			target := query.RevisionFromName(ir.Status.UpdateRevision).Hash()
			if target == "" {
				target = current
			}
			out[comp] = targetPair{
				current:  current,
				target:   target,
				replicas: ir.Status.Replicas,
				updated:  ir.Status.UpdatedReplicas,
			}
		}
	}
	return out, fresh, nil
}

// groupKind is the progression kind a spec group declares, ref included.
// projectionTrails reports whether ir was last projected from an older
// generation of the service than the one it carries now: the projector
// stamps the parent generation on every pass, and until it stamps this
// one the member's revisions still describe the spec the service had
// before. A run judged against them would close on a roll the service
// has already withdrawn. A member with no stamp is read as current.
func projectionTrails(isvc *v1beta1.InferenceService, ir *v1beta1.InferenceReplica) bool {
	stamp, ok := ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey]
	if !ok {
		return false
	}
	projected, err := strconv.ParseInt(stamp, 10, 64)
	if err != nil {
		return false
	}
	return projected < isvc.Generation
}

func groupKind(g *v1beta1.RolloutGroup) v1beta1.RolloutProgressionKind {
	return g.DeclaredProgression()
}

// primaryCanaryComponent is the externally routed member whose scalar canary
// status mirrors the per-Component rollout record.
func primaryCanaryComponent(isvc *v1beta1.InferenceService) v1beta1.ComponentType {
	if isvc == nil || isvc.Spec.Rollout == nil {
		return ""
	}
	for gi := range isvc.Spec.Rollout.Groups {
		g := &isvc.Spec.Rollout.Groups[gi]
		if groupKind(g) != v1beta1.RolloutProgressionCanary {
			continue
		}
		if p := primaryOfGroup(g); p != "" {
			return p
		}
	}
	return ""
}

// primaryOfGroup is the Component a group's step machine and traffic weight run
// through: router > engine > decoder among the group's own members. A group
// owns one canary unit, so this is that unit's entrypoint and the key its run
// state is stored under.
func primaryOfGroup(g *v1beta1.RolloutGroup) v1beta1.ComponentType {
	if g == nil {
		return ""
	}
	for _, preferred := range []v1beta1.ComponentType{
		v1beta1.RouterComponent,
		v1beta1.EngineComponent,
		v1beta1.DecoderComponent,
	} {
		for _, comp := range g.Components {
			if comp == preferred {
				return comp
			}
		}
	}
	return ""
}

// stickyRejectHashes returns, per Component, the revision that Component's OWN
// canary group rejected — the hold a rolled-back ladder rests on.
//
// The attribution is per group, because a rollback belongs to the ladder that
// failed. Crediting one group's failure to every Component in the run would
// read an unrelated group's live target as a hold and strand it behind a
// failure it had no part in, which is the opposite of what independent groups
// promise.
func stickyRejectHashes(isvc *v1beta1.InferenceService, targets map[v1beta1.ComponentType]targetPair) map[v1beta1.ComponentType]string {
	rejected := map[v1beta1.ComponentType]string{}
	if isvc.Spec.Rollout == nil {
		return rejected
	}
	for gi := range isvc.Spec.Rollout.Groups {
		g := &isvc.Spec.Rollout.Groups[gi]
		if groupKind(g) != v1beta1.RolloutProgressionCanary {
			continue
		}
		primary := primaryOfGroup(g)
		if primary == "" {
			continue
		}
		cs := rollout.CanaryStatusFor(&isvc.Status, primary)
		if cs == nil || cs.RolledBackRevisionHash == "" {
			continue
		}
		addGroupReject(isvc, g, primary, cs.RolledBackRevisionHash, targets, rejected)
	}
	return rejected
}

// addGroupReject records the rejected revision for every member of one
// rolled-back canary group. A run whose pinned primary revision IS the
// rejected one supplies the exact member set; failing that (status predating
// the run model, or a later run that has since overwritten the record) only
// the primary's hash is known, and the members are reconstructed from their
// observed targets while those still point at the rejected roll.
func addGroupReject(
	isvc *v1beta1.InferenceService,
	g *v1beta1.RolloutGroup,
	primary v1beta1.ComponentType,
	hash string,
	targets map[v1beta1.ComponentType]targetPair,
	out map[v1beta1.ComponentType]string,
) {
	if isvc.Status.Rollout != nil {
		var records [][]v1beta1.RolloutRunTarget
		if active := isvc.Status.Rollout.ActiveRun; active != nil {
			records = append(records, active.TargetRevisions)
		}
		if last := isvc.Status.Rollout.LastRun; last != nil && last.Outcome == v1beta1.RolloutRunRolledBack {
			records = append(records, last.TargetRevisions)
		}
		for _, pinned := range records {
			if pinnedRevision(pinned, primary) != hash {
				continue
			}
			// Members only: the same run pins other groups' Components, and
			// their revisions were never the ones this ladder rejected.
			for _, member := range pinned {
				if groupHasComponent(g, member.Component) {
					out[member.Component] = member.Revision
				}
			}
			return
		}
	}
	out[primary] = hash
	if targets[primary].target != hash {
		return
	}
	for _, comp := range g.Components {
		if target := targets[comp].target; target != "" {
			out[comp] = target
		}
	}
}

// pinnedRevision is the revision a run pinned for one Component, "" when the
// run does not carry it.
func pinnedRevision(pinned []v1beta1.RolloutRunTarget, comp v1beta1.ComponentType) string {
	for _, t := range pinned {
		if t.Component == comp {
			return t.Revision
		}
	}
	return ""
}

func groupHasComponent(g *v1beta1.RolloutGroup, comp v1beta1.ComponentType) bool {
	for _, c := range g.Components {
		if c == comp {
			return true
		}
	}
	return false
}

// divergedMember reports whether any grouped Component needs a roll: its
// target differs from its current revision (excluding the canary sticky
// reject), or stragglers remain — instances not on the target even though
// the revision pair agrees, the state a mid-roll revert leaves behind.
// Without the straggler clause a revert-to-current would open no run, and
// the plan gate would then hold the straggler updates forever.
func divergedMember(isvc *v1beta1.InferenceService, targets map[v1beta1.ComponentType]targetPair) bool {
	rejected := stickyRejectHashes(isvc, targets)
	if isvc.Spec.Rollout == nil {
		return false
	}
	for gi := range isvc.Spec.Rollout.Groups {
		g := &isvc.Spec.Rollout.Groups[gi]
		for _, comp := range g.Components {
			t := targets[comp]
			// The rejected revision is a HOLD, not a pending roll — and it
			// keeps being the IR's spec target throughout the hold, so both
			// clauses below would otherwise re-open a run toward it forever.
			if groupKind(g) == v1beta1.RolloutProgressionCanary && t.target != "" && t.target == rejected[comp] {
				continue
			}
			if t.straggling() {
				return true
			}
			if t.target == "" || t.target == t.current {
				continue
			}
			return true
		}
	}
	return false
}

// canaryMidFlight reports whether the canary step machine holds in-progress
// state that a run must wrap (the adopt-in-place trigger: an upgrade or a
// status-loss recovery can find the engine mid-ladder with no pinned run,
// even when the IRs have fully converged — the traffic ladder outlives pod
// convergence). A revert still draining counts: the rejected target opens
// no run of its own, and the plan gate admits the roll back to stable only
// under a pinned run. A settled rolled-back hold is terminal, not
// in-progress. The done sentinel is read against the effective body; a
// group whose reference does not resolve has none, so any non-terminal
// state counts and the open then parks on the reference.
func canaryMidFlight(isvc *v1beta1.InferenceService, policies rollout.Policies) bool {
	if isvc == nil || isvc.Spec.Rollout == nil {
		return false
	}
	primary := primaryCanaryComponent(isvc)
	if primary == "" {
		return false
	}
	cs := rollout.CanaryStatusFor(&isvc.Status, primary)
	if cs == nil {
		return false
	}
	g := rollout.CanaryGroupFor(isvc, policies, primary)
	if cs.RolledBackRevisionHash != "" {
		if g == nil {
			g = declaredCanaryGroup(isvc)
		}
		return revertInFlight(isvc, g)
	}
	if g != nil {
		return int(cs.CurrentStep) < len(g.Canary.Steps)
	}
	return true
}

// declaredCanaryGroup is the first spec group declaring canary, by any form.
func declaredCanaryGroup(isvc *v1beta1.InferenceService) *v1beta1.RolloutGroup {
	for gi := range isvc.Spec.Rollout.Groups {
		if g := &isvc.Spec.Rollout.Groups[gi]; groupKind(g) == v1beta1.RolloutProgressionCanary {
			return g
		}
	}
	return nil
}

// revertInFlight reports whether a canary group's unit is still rolling
// back: its ladder rejected a revision and the primary has not yet reported
// the revert complete. The state is read the way the run's close reads it,
// so a run adopted around the revert is one the revert's completion closes.
func revertInFlight(isvc *v1beta1.InferenceService, g *v1beta1.RolloutGroup) bool {
	primary := primaryOfGroup(g)
	cs := rollout.CanaryStatusFor(&isvc.Status, primary)
	if cs == nil || cs.RolledBackRevisionHash == "" {
		return false
	}
	steps := 0
	if g.Canary != nil {
		steps = len(g.Canary.Steps)
	}
	return rollout.StateOf(cs, isvc.Status.Components[primary].RolloutPhase, steps) == rollout.CanaryStateRollingBack
}

// unitLadderInFlight reports whether a canary group's own ladder is one a
// run adopts: its primary carries a canary record short of the ladder's end,
// or a revert still draining. A group whose unit is idle, or finished, has
// nothing in flight whatever another group's ladder is doing.
func unitLadderInFlight(isvc *v1beta1.InferenceService, g *v1beta1.RolloutGroup) bool {
	if g == nil || g.Canary == nil {
		return false
	}
	cs := rollout.CanaryStatusFor(&isvc.Status, primaryOfGroup(g))
	if cs == nil {
		return false
	}
	if cs.RolledBackRevisionHash != "" {
		return revertInFlight(isvc, g)
	}
	return int(cs.CurrentStep) < len(g.Canary.Steps)
}

// derivedProvenance parses the derive-time plan-source annotation
// ("<idx>=<name>@<digest>;..."), which carries per-group policy identity on a
// derived ISVC whose refs the control plane inflated into inline groups.
//
// The annotation carries no progression, so every returned PolicyRef has
// Progression deliberately unset. That zero value is NOT writable to the
// status subresource (the field is a required CRD enum): a caller that puts
// one on status must fill Progression from the composed group body first.
func derivedProvenance(isvc *v1beta1.InferenceService) map[int]v1beta1.RolloutRunProvenance {
	raw := isvc.Annotations[constants.RolloutPlanSourceAnnotation]
	if raw == "" {
		return nil
	}
	out := map[int]v1beta1.RolloutRunProvenance{}
	for _, entry := range strings.Split(raw, ";") {
		eq := strings.IndexByte(entry, '=')
		at := strings.LastIndexByte(entry, '@')
		if eq <= 0 || at <= eq+1 {
			continue
		}
		idx, err := strconv.Atoi(entry[:eq])
		if err != nil || idx < 0 {
			continue
		}
		out[idx] = v1beta1.RolloutRunProvenance{
			Source:         v1beta1.RolloutPlanSourcePolicy,
			PolicyRef:      &v1beta1.RolloutPolicyRef{Name: entry[eq+1 : at]},
			PortableDigest: entry[at+1:],
		}
	}
	return out
}

// liveSourceDigest computes the portable digest of what a run opened NOW
// would pin for one spec group: the inline body's digest when an inline arm
// is set, else the referenced policy's. Returns "" with no error when the
// source is unresolvable (dangling ref) — the caller reports that separately.
func liveSourceDigest(g *v1beta1.RolloutGroup, policies rollout.Policies) (string, error) {
	if rolloutpolicy.GroupSource(g) == v1beta1.RolloutPlanSourceInline {
		return rolloutpolicy.ProgressionDigest(g)
	}
	policy := policies.ByName[g.PolicyRef.Name]
	if policy == nil {
		return "", nil
	}
	return rolloutpolicy.PortableDigest(&policy.Spec)
}

// observePolicies reads through reads every RolloutPolicy the spec's groups
// reference, shadowed references included so the resolution view can report
// what they would pin: the cached client once per pass for the set the
// executors' effective view resolves through, the live reader on the pass
// that pins a plan. The cached set is enough for the view because a body
// executes only through a pin: a stale body between runs is reported by
// drift and corrected at open, which composes from the live reading, and
// while a run is open the view reads the pin. A missing policy is absent
// from the set; a policy whose body fails validation is kept with the
// reason, so the opener parks on it as it parks on a missing one. Nothing
// is read while the policy surface is not installed.
func observePolicies(ctx context.Context, in Inputs, reads client.Reader) (rollout.Policies, error) {
	isvc := in.ISVC
	out := rollout.Policies{Namespace: isvc.Namespace, Enabled: in.FeatureEnabled}
	if !in.FeatureEnabled || isvc.Spec.Rollout == nil {
		return out, nil
	}
	out.ByName = map[string]*v1beta1.RolloutPolicy{}
	out.Invalid = map[string]error{}
	seen := map[string]bool{}
	for gi := range isvc.Spec.Rollout.Groups {
		ref := isvc.Spec.Rollout.Groups[gi].PolicyRef
		if ref == nil || seen[ref.Name] {
			continue
		}
		seen[ref.Name] = true
		policy := &v1beta1.RolloutPolicy{}
		err := reads.Get(ctx, types.NamespacedName{Namespace: isvc.Namespace, Name: ref.Name}, policy)
		switch {
		case err == nil:
			out.ByName[ref.Name] = policy
			if verr := validation.ValidateRolloutPolicySpec(&policy.Spec); verr != nil {
				out.Invalid[ref.Name] = verr
			}
		case apierrors.IsNotFound(err):
		default:
			return rollout.Policies{}, err
		}
	}
	return out, nil
}

// combinedPlanDigest folds the per-group digests into the one value the
// repin annotation CAS-compares against.
func combinedPlanDigest(digests []string) string {
	return rolloutpolicy.CombinedDigest(digests)
}

// runID names one run: a short stable hash over the pinned targets and plan
// digests plus the open timestamp, prefixed by the ISVC name for
// greppability.
func runID(isvc *v1beta1.InferenceService, targets []v1beta1.RolloutRunTarget, digests []string, openedAt string) string {
	var b strings.Builder
	for _, t := range targets {
		fmt.Fprintf(&b, "%s=%s;", t.Component, t.Revision)
	}
	b.WriteString(strings.Join(digests, ","))
	b.WriteString(openedAt)
	return isvc.Name + "-" + rolloutpolicy.ShortHash([]byte(b.String()))
}
