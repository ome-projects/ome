package placement

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
	"sigs.k8s.io/ome/pkg/validation"
)

// WorkloadClusterRolloutPolicyCapability is the WorkloadCluster label value
// (under constants.WorkloadClusterRolloutPolicyLabel) that marks a member as
// resolving the rollout-policy schema this control plane inflates against.
// Derived from the API group version rather than a free-standing literal so
// the capability gate can never drift from the API the placer fans out.
var WorkloadClusterRolloutPolicyCapability = v1beta1.SchemeGroupVersion.Version

// rolloutManualPauseWarningReason is the Warning event reason emitted when a
// placed effective plan contains a bare manual Pause step.
const rolloutManualPauseWarningReason = "RolloutManualPauseNotForwardable"

// resolvedRolloutPolicy is one control-plane RolloutPolicy resolved by the
// rollout preflight: the body inflation composes from and its portable digest
// (the provenance identity members report back).
type resolvedRolloutPolicy struct {
	spec   *v1beta1.RolloutPolicySpec
	digest string
}

// rolloutPreflight is the in-memory rollout-policy bookkeeping for the
// placement controller: the staged preflight condition, the per-source
// resolved policies the derive-time inflation consumes, the per-(source,home)
// lifted run provenance, and the manual-Pause warning dedupe. The API
// deliberately carries no such state; a control-plane restart clears it and
// the next pass rebuilds it from the spec (conservative: resolution reruns,
// warnings re-emit at most once per content).
type rolloutPreflight struct {
	mu sync.Mutex
	// preflight is the latest staged PlacementPolicyPreflight condition per
	// source, applied on the next status write (merged with the autoscaler
	// preflight's staged condition of the same type).
	preflight map[types.UID]policyCondition
	// plans is the latest successfully-resolved policy set per source; the
	// fan-out's derive reads it to inflate ref-only groups.
	plans map[types.UID]map[string]resolvedRolloutPolicy
	// homes is the latest lifted rollout-run observation per (source, home).
	homes map[types.UID]map[string]*v1beta1.CandidateRolloutStatus
	// warned is the last manual-Pause warning message emitted per source, so
	// the poll-cadence reconcile does not re-emit an unchanged warning.
	warned map[types.UID]string
}

func (p *rolloutPreflight) setPreflight(uid types.UID, c policyCondition) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.preflight == nil {
		p.preflight = make(map[types.UID]policyCondition)
	}
	p.preflight[uid] = c
}

func (p *rolloutPreflight) preflightFor(uid types.UID) (policyCondition, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.preflight[uid]
	return c, ok
}

func (p *rolloutPreflight) stagePlans(uid types.UID, plans map[string]resolvedRolloutPolicy) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.plans == nil {
		p.plans = make(map[types.UID]map[string]resolvedRolloutPolicy)
	}
	p.plans[uid] = plans
}

// plansFor returns the staged resolved policies for a source. The map is
// replaced wholesale on each stage and never mutated afterwards, so returning
// it without copying is safe.
func (p *rolloutPreflight) plansFor(uid types.UID) map[string]resolvedRolloutPolicy {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.plans[uid]
}

func (p *rolloutPreflight) recordHome(uid types.UID, cluster string, obs *v1beta1.CandidateRolloutStatus) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.homes == nil {
		p.homes = make(map[types.UID]map[string]*v1beta1.CandidateRolloutStatus)
	}
	if p.homes[uid] == nil {
		p.homes[uid] = make(map[string]*v1beta1.CandidateRolloutStatus)
	}
	p.homes[uid][cluster] = obs
}

func (p *rolloutPreflight) homeFor(uid types.UID, cluster string) *v1beta1.CandidateRolloutStatus {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.homes[uid][cluster]
}

func (p *rolloutPreflight) forgetHome(uid types.UID, cluster string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.homes[uid], cluster)
}

// warnOnce reports whether msg is new content for this source (and records
// it); an unchanged message returns false so the caller skips the re-emit.
func (p *rolloutPreflight) warnOnce(uid types.UID, msg string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.warned[uid] == msg {
		return false
	}
	if p.warned == nil {
		p.warned = make(map[types.UID]string)
	}
	p.warned[uid] = msg
	return true
}

// forget drops all per-source bookkeeping (source deleted or refs removed).
func (p *rolloutPreflight) forget(uid types.UID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.preflight, uid)
	delete(p.plans, uid)
	delete(p.homes, uid)
	delete(p.warned, uid)
}

// rolloutState returns the rollout-policy preflight state, lazily defaulted so
// a Reconciler assembled as a bare struct literal (tests, cmd wiring) works
// without a constructor.
func (r *Reconciler) rolloutState() *rolloutPreflight {
	r.rolloutOnce.Do(func() {
		if r.rollout == nil {
			r.rollout = &rolloutPreflight{}
		}
	})
	return r.rollout
}

// hasRolloutPolicyRefs is the zero-cost guard for the lift surface: true when
// any spec rollout group carries a PolicyRef.
func hasRolloutPolicyRefs(isvc *v1beta1.InferenceService) bool {
	if isvc.Spec.Rollout == nil {
		return false
	}
	for i := range isvc.Spec.Rollout.Groups {
		if isvc.Spec.Rollout.Groups[i].PolicyRef != nil {
			return true
		}
	}
	return false
}

// rolloutGroupRef pairs a ref-only rollout group's index with the policy it
// references (the groups derive-time inflation rewrites).
type rolloutGroupRef struct {
	index  int
	policy string
}

// refOnlyRolloutGroups lists the groups whose ONLY progression source is a
// PolicyRef — the groups whose resolution failure must hold placement. A
// group with an inline arm never appears here: inline wins, so a dangling
// shadowed ref cannot park a member.
func refOnlyRolloutGroups(spec *v1beta1.RolloutSpec) []rolloutGroupRef {
	var out []rolloutGroupRef
	for i := range spec.Groups {
		g := &spec.Groups[i]
		if g.PolicyRef != nil && !hasInlineProgression(g) {
			out = append(out, rolloutGroupRef{index: i, policy: g.PolicyRef.Name})
		}
	}
	return out
}

// distinctRolloutPolicyNames returns the sorted distinct policy names across refs.
func distinctRolloutPolicyNames(refs []rolloutGroupRef) []string {
	seen := make(map[string]struct{}, len(refs))
	var names []string
	for _, ref := range refs {
		if _, ok := seen[ref.policy]; ok {
			continue
		}
		seen[ref.policy] = struct{}{}
		names = append(names, ref.policy)
	}
	sort.Strings(names)
	return names
}

// rolloutPlanFacts summarizes the EFFECTIVE plan (each group's inline arm, or
// its resolved policy body) for the preflight's gates.
type rolloutPlanFacts struct {
	// needsCapability: the plan carries fields an older member's ISVC CRD
	// schema would prune (providerRef, readyTimeout), or any group carries a
	// PolicyRef at all — either way the member must declare the capability.
	needsCapability bool
	// manualPauses describes each bare manual Pause step (no Duration, no
	// Analysis) in the effective plan, e.g. "group 0 step 2".
	manualPauses []string
}

// effectiveRolloutFacts walks the effective plan. resolved supplies the policy
// bodies for ref-only groups; a missing entry is skipped (the caller has
// already held placement for it).
func effectiveRolloutFacts(spec *v1beta1.RolloutSpec, resolved map[string]resolvedRolloutPolicy) rolloutPlanFacts {
	facts := rolloutPlanFacts{}
	for i := range spec.Groups {
		g := &spec.Groups[i]
		if g.PolicyRef != nil {
			facts.needsCapability = true
		}
		canary := g.Canary
		if !hasInlineProgression(g) && g.PolicyRef != nil {
			if pol, ok := resolved[g.PolicyRef.Name]; ok && pol.spec != nil {
				canary = pol.spec.Canary
			}
		}
		if canary == nil {
			continue
		}
		if canary.ReadyTimeout != nil || (canary.Prometheus != nil && canary.Prometheus.ProviderRef != nil) {
			facts.needsCapability = true
		}
		for si := range canary.Steps {
			step := &canary.Steps[si]
			if step.Analysis == nil && step.Pause != nil && step.Pause.Duration == nil {
				facts.manualPauses = append(facts.manualPauses, fmt.Sprintf("group %d step %d", i, si))
			}
		}
	}
	return facts
}

// preflightRolloutPolicies gates a rollout-carrying source before fan-out. It
// resolves every ref-only group's RolloutPolicy against the control plane's
// own copy (the GitOps-synced anchor the derive-time inflation composes from)
// and holds placement fail-closed when any ref cannot resolve — a member must
// never receive a spec whose declared gate was silently replaced. Candidates
// are then gated on the rollout-policy capability label whenever the plan
// carries a ref or a schema-sensitive field (providerRef / readyTimeout, even
// from inline groups): an older member's ISVC CRD would prune those fields on
// the derived apply, silently detaching the gate's metrics source. Per-member
// provider BINDINGS are deliberately not checked here — an unbound member
// parks its own run loudly, which is the member's park to own. Returns nil for
// a source with no rollout spec (the zero-cost path). Only ever READS
// policies, and stages the shared PlacementPolicyPreflight condition for the
// next status write.
func (r *Reconciler) preflightRolloutPolicies(ctx context.Context, isvc *v1beta1.InferenceService, candidates []string, clusters []v1beta1.WorkloadCluster) *policyPreflightOutcome {
	spec := isvc.Spec.Rollout
	if spec == nil || len(spec.Groups) == 0 {
		return nil
	}
	rp := r.rolloutState()
	refs := refOnlyRolloutGroups(spec)

	// Resolve each referenced policy on the control plane. Read live
	// (APIReader) so a missing object surfaces immediately.
	resolved := make(map[string]resolvedRolloutPolicy, len(refs))
	for _, name := range distinctRolloutPolicyNames(refs) {
		pol := &v1beta1.RolloutPolicy{}
		if err := r.APIReader.Get(ctx, types.NamespacedName{Namespace: isvc.Namespace, Name: name}, pol); err != nil {
			if !apierrors.IsNotFound(err) && sourcePlaced(isvc) {
				// A transient control-plane read error is no verdict on the
				// policy. Hold actuation and re-read soon.
				r.Log.Error(err, "rollout preflight: policy read failed; holding existing placement",
					"policy", isvc.Namespace+"/"+name, "isvc", isvc.Namespace+"/"+isvc.Name)
				return &policyPreflightOutcome{holdAsIs: true, transient: true}
			}
			rp.setPreflight(isvc.UID, preflightCondition(corev1.ConditionFalse,
				v1beta1.RolloutPlanReasonPolicyNotFound,
				fmt.Sprintf("RolloutPolicy %s/%s has no readable copy on %s: %v; a ref-only rollout group must never fall back to the default progression, holding placement",
					isvc.Namespace, name, r.controlPlaneName(), err)))
			return &policyPreflightOutcome{hold: true, transient: !apierrors.IsNotFound(err)}
		}
		// The body must pass the same validation members enforce at admission:
		// an invalid body means every derived apply is denied.
		if err := validation.ValidateRolloutPolicySpec(&pol.Spec); err != nil {
			rp.setPreflight(isvc.UID, preflightCondition(corev1.ConditionFalse,
				v1beta1.PlacementPolicyPreflightReasonInvalidPolicy,
				fmt.Sprintf("RolloutPolicy %s/%s is invalid: %v; the inflated derived spec would be denied by every member webhook, holding placement",
					isvc.Namespace, name, err)))
			return &policyPreflightOutcome{hold: true}
		}
		digest, err := rolloutpolicy.PortableDigest(&pol.Spec)
		if err != nil {
			rp.setPreflight(isvc.UID, preflightCondition(corev1.ConditionFalse,
				v1beta1.PlacementPolicyPreflightReasonInvalidPolicy,
				fmt.Sprintf("RolloutPolicy %s/%s cannot be digested: %v; holding placement", isvc.Namespace, name, err)))
			return &policyPreflightOutcome{hold: true}
		}
		resolved[name] = resolvedRolloutPolicy{spec: pol.Spec.DeepCopy(), digest: digest}
	}

	// Compose each ref-only group exactly as the derive-time inflation will,
	// so a declared-kind lie (ProgressionMismatch) holds placement here rather
	// than failing every per-cluster apply.
	for _, ref := range refs {
		if _, err := rolloutpolicy.ComposeGroup(&spec.Groups[ref.index], resolved[ref.policy].spec); err != nil {
			reason := v1beta1.PlacementPolicyPreflightReasonInvalidPolicy
			if errors.Is(err, rolloutpolicy.ErrProgressionMismatch) {
				reason = v1beta1.RolloutPlanReasonProgressionMismatch
			}
			rp.setPreflight(isvc.UID, preflightCondition(corev1.ConditionFalse, reason,
				fmt.Sprintf("rollout group %d cannot be inflated: %v; holding placement", ref.index, err)))
			return &policyPreflightOutcome{hold: true}
		}
	}
	rp.stagePlans(isvc.UID, resolved)

	facts := effectiveRolloutFacts(spec, resolved)
	r.warnManualPauseGates(isvc, facts.manualPauses)

	if len(refs) == 0 && !facts.needsCapability {
		// Plain inline plan with no schema-sensitive fields: candidates flow
		// through untouched and no condition is staged (zero-cost for the
		// common inline-rollout source).
		return nil
	}

	labelsFor := make(map[string]map[string]string, len(clusters))
	for i := range clusters {
		labelsFor[clusters[i].Name] = clusters[i].Labels
	}
	winner := winnerCluster(isvc)
	var eligible []string
	var skips []candidateSkip
	for _, c := range candidates {
		if labelsFor[c][constants.WorkloadClusterRolloutPolicyLabel] == WorkloadClusterRolloutPolicyCapability {
			eligible = append(eligible, c)
			continue
		}
		if c == winner {
			// Capability ineligibility gates new fan-out without freezing the
			// standing winner's observed health.
			r.Log.Info("rollout preflight: standing winner lacks the capability label; holding actuation",
				"cluster", c, "isvc", isvc.Namespace+"/"+isvc.Name)
			rp.setPreflight(isvc.UID, preflightCondition(corev1.ConditionFalse,
				v1beta1.PlacementPolicyPreflightReasonCapabilityMissing,
				fmt.Sprintf("rollout policy: standing winner %s lacks %s=%s; holding placement",
					c, constants.WorkloadClusterRolloutPolicyLabel, WorkloadClusterRolloutPolicyCapability)))
			return &policyPreflightOutcome{holdAsIs: true}
		}
		skips = append(skips, candidateSkip{
			cluster: c,
			reason:  v1beta1.PlacementPolicyPreflightReasonCapabilityMissing,
			detail: fmt.Sprintf("cluster does not carry %s=%s; its InferenceService schema would prune the inflated plan's gate fields",
				constants.WorkloadClusterRolloutPolicyLabel, WorkloadClusterRolloutPolicyCapability),
		})
	}
	if len(eligible) == 0 {
		rp.setPreflight(isvc.UID, preflightCondition(corev1.ConditionFalse,
			v1beta1.PlacementPolicyPreflightReasonCapabilityMissing,
			"rollout policy: no eligible candidate remains; skipped: "+describeSkips(skips)))
		return &policyPreflightOutcome{hold: true}
	}
	msg := fmt.Sprintf("rollout policy: %d of %d candidates eligible", len(eligible), len(candidates))
	if len(skips) > 0 {
		msg += "; skipped: " + describeSkips(skips)
	}
	rp.setPreflight(isvc.UID, preflightCondition(corev1.ConditionTrue,
		v1beta1.PlacementPolicyPreflightReasonPassed, msg))
	return &policyPreflightOutcome{eligible: eligible}
}

// warnManualPauseGates emits one Warning event (deduped on content) when the
// effective plan contains bare manual Pause steps: the promote/rollback verbs
// are control-plane-owned and stripped from derived objects, and no forwarding
// mechanism exists, so a placed manual gate can only be advanced by annotating
// each member's derived InferenceService directly.
func (r *Reconciler) warnManualPauseGates(isvc *v1beta1.InferenceService, manualPauses []string) {
	if len(manualPauses) == 0 || r.Recorder == nil {
		return
	}
	msg := fmt.Sprintf("effective rollout plan contains bare manual Pause steps (%s): the ome.io/rollout-promote and ome.io/rollout-rollback verbs are stripped from derived objects and not forwarded, so a placed manual gate can only be advanced by annotating each member's derived InferenceService directly; prefer analysis or timed gates for placed canaries",
		strings.Join(manualPauses, ", "))
	if !r.rolloutState().warnOnce(isvc.UID, msg) {
		return
	}
	r.Recorder.Event(isvc, corev1.EventTypeWarning, rolloutManualPauseWarningReason, msg)
}

// observeDerivedRolloutStatus lifts a freshly read derived ISVC's rollout-run
// provenance into the in-memory home observations, from which the next status
// write populates candidates[].rollout. No-op for a source whose rollout
// groups reference no policy (the zero-cost path).
func (r *Reconciler) observeDerivedRolloutStatus(src *v1beta1.InferenceService, cluster string, derived *v1beta1.InferenceService) {
	if !hasRolloutPolicyRefs(src) {
		return
	}
	r.rolloutState().recordHome(src.UID, cluster, liftCandidateRollout(derived))
}

// liftCandidateRollout maps one derived ISVC's status.rollout into the bounded
// per-home CandidateRolloutStatus: the active run's ID and per-group
// provenance (source, policy name, portable digest) plus the last closed run's
// outcome and combined digest. Never the plan bodies. Nil when the home
// reports no run state yet.
func liftCandidateRollout(derived *v1beta1.InferenceService) *v1beta1.CandidateRolloutStatus {
	rs := derived.Status.Rollout
	if rs == nil || (rs.ActiveRun == nil && rs.LastRun == nil) {
		return nil
	}
	out := &v1beta1.CandidateRolloutStatus{}
	if run := rs.ActiveRun; run != nil {
		out.ActiveRunID = run.RunID
		for i := range run.Plan.Groups {
			g := &run.Plan.Groups[i]
			entry := v1beta1.CandidateRolloutGroup{Source: g.Source, PortableDigest: g.PortableDigest}
			if g.PolicyRef != nil {
				entry.PolicyName = g.PolicyRef.Name
			}
			out.ActiveGroups = append(out.ActiveGroups, entry)
		}
	}
	if last := rs.LastRun; last != nil {
		digests := make([]string, 0, len(last.Groups))
		for i := range last.Groups {
			digests = append(digests, last.Groups[i].PortableDigest)
		}
		out.LastRun = &v1beta1.CandidateRolloutLastRun{
			Outcome: last.Outcome,
			Digest:  rolloutpolicy.CombinedDigest(digests),
		}
	}
	return out
}

// cloneCandidateRollout hand-copies a lifted observation so the status write
// never aliases the in-memory bookkeeping.
func cloneCandidateRollout(in *v1beta1.CandidateRolloutStatus) *v1beta1.CandidateRolloutStatus {
	if in == nil {
		return nil
	}
	out := &v1beta1.CandidateRolloutStatus{ActiveRunID: in.ActiveRunID}
	if in.ActiveGroups != nil {
		out.ActiveGroups = make([]v1beta1.CandidateRolloutGroup, len(in.ActiveGroups))
		copy(out.ActiveGroups, in.ActiveGroups)
	}
	if in.LastRun != nil {
		lr := *in.LastRun
		out.LastRun = &lr
	}
	return out
}

// rolloutStatusForWrite decorates the pass's placement result right before the
// status write: it attaches the lifted per-home rollout provenance to the
// candidates and returns the staged PlacementPolicyPreflight condition (merged
// with the autoscaler preflight's by the caller). For a source whose rollout
// carries no policy ref and no schema-sensitive field it only drops stale
// bookkeeping; clearing the shared condition is the autoscaler side's job,
// which keys on the live status rather than in-memory state.
func (r *Reconciler) rolloutStatusForWrite(isvc *v1beta1.InferenceService, res *placementResult) []policyCondition {
	rp := r.rolloutState()
	if !rolloutPreflightActive(isvc) {
		rp.forget(isvc.UID)
		return nil
	}
	var conds []policyCondition
	if pre, ok := rp.preflightFor(isvc.UID); ok {
		conds = append(conds, pre)
	}
	for i := range res.candidates {
		if obs := rp.homeFor(isvc.UID, res.candidates[i].Cluster); obs != nil {
			res.candidates[i].Rollout = cloneCandidateRollout(obs)
		}
	}
	return conds
}

// rolloutPreflightActive reports whether the rollout preflight stages a
// condition for this source: any group carries a PolicyRef, or an inline
// canary uses a schema-sensitive field (providerRef / readyTimeout).
func rolloutPreflightActive(isvc *v1beta1.InferenceService) bool {
	spec := isvc.Spec.Rollout
	if spec == nil {
		return false
	}
	if hasRolloutPolicyRefs(isvc) {
		return true
	}
	return effectiveRolloutFacts(spec, nil).needsCapability
}

// mergePolicyConditions folds the autoscaler-policy and rollout-policy staged
// conditions into one list, resolving the shared PlacementPolicyPreflight
// type: a real condition beats a clear (the other preflight still owns
// content), a False verdict beats a True one (either preflight holding must
// surface), and two True verdicts join their messages.
func mergePolicyConditions(a, b []policyCondition) []policyCondition {
	if len(b) == 0 {
		return a
	}
	if len(a) == 0 {
		return b
	}
	out := make([]policyCondition, 0, len(a)+len(b))
	byType := make(map[string]int, len(a)+len(b))
	for _, c := range append(append([]policyCondition{}, a...), b...) {
		i, seen := byType[string(c.condType)]
		if !seen {
			byType[string(c.condType)] = len(out)
			out = append(out, c)
			continue
		}
		out[i] = preferPolicyCondition(out[i], c)
	}
	return out
}

// preferPolicyCondition picks the surviving entry for one condition type. x is
// the earlier-staged entry (the autoscaler side in reconcile order).
func preferPolicyCondition(x, y policyCondition) policyCondition {
	switch {
	case x.clear && !y.clear:
		return y
	case y.clear && !x.clear:
		return x
	case x.clear:
		return x
	case x.cond.Status == corev1.ConditionFalse:
		return x
	case y.cond.Status == corev1.ConditionFalse:
		return y
	default:
		x.cond.Message = x.cond.Message + "; " + y.cond.Message
		return x
	}
}
