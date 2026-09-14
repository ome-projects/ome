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
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
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
			key := types.NamespacedName{Namespace: isvc.Namespace, Name: irprojector.InferenceReplicaName(isvc.Name, comp)}
			if err := reads.Get(ctx, key, ir); err != nil {
				if apierrors.IsNotFound(err) {
					out[comp] = targetPair{}
					continue
				}
				return nil, false, err
			}
			if ir.Status.ObservedGeneration != ir.Generation {
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

// groupKind mirrors validation's declared-kind resolution: the inline arm
// when present (inline outranks the ref), else the ref's declared kind, else
// the blueGreen default.
func groupKind(g *v1beta1.RolloutGroup) v1beta1.RolloutProgressionKind {
	switch {
	case g.Canary != nil:
		return v1beta1.RolloutProgressionCanary
	case g.BlueGreen != nil:
		return v1beta1.RolloutProgressionBlueGreen
	case g.RollingUpdate != nil:
		return v1beta1.RolloutProgressionRollingUpdate
	case g.PolicyRef != nil:
		return g.PolicyRef.Progression
	}
	return v1beta1.RolloutProgressionBlueGreen
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
	}
	return ""
}

// stickyRejectHashes returns the rejected target for each Component in the
// rolled-back canary group. Run status supplies the exact active or closed
// target set. For legacy status, the primary scalar identifies whether the
// current IR targets are still the rejected group; only then is it reconstructed.
func stickyRejectHashes(isvc *v1beta1.InferenceService, targets map[v1beta1.ComponentType]targetPair) map[v1beta1.ComponentType]string {
	rejected := map[v1beta1.ComponentType]string{}
	if isvc.Status.Canary == nil || isvc.Status.Canary.RolledBackRevisionHash == "" {
		return rejected
	}
	primary := primaryCanaryComponent(isvc)
	if primary == "" {
		return rejected
	}
	if isvc.Status.Rollout != nil {
		if active := isvc.Status.Rollout.ActiveRun; active != nil {
			for _, target := range active.TargetRevisions {
				if target.Component == primary && target.Revision == isvc.Status.Canary.RolledBackRevisionHash {
					for _, member := range active.TargetRevisions {
						rejected[member.Component] = member.Revision
					}
					return rejected
				}
			}
		}
		if last := isvc.Status.Rollout.LastRun; last != nil && last.Outcome == v1beta1.RolloutRunRolledBack {
			for _, target := range last.TargetRevisions {
				if target.Component == primary && target.Revision == isvc.Status.Canary.RolledBackRevisionHash {
					for _, member := range last.TargetRevisions {
						rejected[member.Component] = member.Revision
					}
					return rejected
				}
			}
		}
	}
	rejected[primary] = isvc.Status.Canary.RolledBackRevisionHash
	if targets[primary].target != isvc.Status.Canary.RolledBackRevisionHash {
		return rejected
	}
	for gi := range isvc.Spec.Rollout.Groups {
		g := &isvc.Spec.Rollout.Groups[gi]
		if groupKind(g) != v1beta1.RolloutProgressionCanary {
			continue
		}
		for _, comp := range g.Components {
			if target := targets[comp].target; target != "" {
				rejected[comp] = target
			}
		}
	}
	return rejected
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
			if t.replicas > 0 && t.updated < t.replicas {
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
// convergence). A rolled-back hold is terminal, not in-progress. With an
// inline canary body the done sentinel is exact; for a ref-sourced canary the
// body is not resolvable here, so any non-terminal state counts (a run that
// opens around an already-done canary just closes Completed on the next
// pass — harmless).
func canaryMidFlight(isvc *v1beta1.InferenceService) bool {
	cs := isvc.Status.Canary
	if cs == nil || cs.RolledBackRevisionHash != "" || isvc.Spec.Rollout == nil {
		return false
	}
	for gi := range isvc.Spec.Rollout.Groups {
		g := &isvc.Spec.Rollout.Groups[gi]
		if groupKind(g) != v1beta1.RolloutProgressionCanary {
			continue
		}
		if g.Canary != nil {
			return int(cs.CurrentStep) < len(g.Canary.Steps)
		}
		return true
	}
	return false
}

// derivedProvenance parses the derive-time plan-source annotation
// ("<idx>=<name>@<digest>;..."), which carries per-group policy identity on a
// derived ISVC whose refs the control plane inflated into inline groups.
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
// is set, else the referenced policy's (fetched through c — the caller
// chooses cached vs live). Returns "" with no error when the source is
// unresolvable (dangling ref) — the caller reports that separately.
func liveSourceDigest(ctx context.Context, c client.Client, isvc *v1beta1.InferenceService, g *v1beta1.RolloutGroup) (string, error) {
	if g.Canary != nil || g.BlueGreen != nil || g.RollingUpdate != nil || g.PolicyRef == nil {
		return rolloutpolicy.ProgressionDigest(g)
	}
	policy := &v1beta1.RolloutPolicy{}
	key := types.NamespacedName{Namespace: isvc.Namespace, Name: g.PolicyRef.Name}
	if err := c.Get(ctx, key, policy); err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		return "", err
	}
	return rolloutpolicy.PortableDigest(&policy.Spec)
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
