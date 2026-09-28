package placement

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"knative.dev/pkg/apis"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/autoscalerpolicy/render"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/autoscaler"
	"sigs.k8s.io/ome/pkg/validation"
)

// WorkloadClusterAutoscalerPolicyCapability is the WorkloadCluster label value
// (under constants.WorkloadClusterAutoscalerPolicyLabel) that marks a member as
// resolving the AutoscalerPolicy schema this control plane compiles against.
// Derived from the API group version rather than a free-standing literal so the
// capability gate can never drift from the policy API the placer fans out.
var WorkloadClusterAutoscalerPolicyCapability = v1beta1.SchemeGroupVersion.Version

// policyPruneRevertThreshold is how many CONSECUTIVE re-applies must observe
// the live remote derived missing a ref the control plane stamped before the
// aggregate condition flips FieldPruned. One observation can be an apply race
// (webhook rewrite, concurrent update); repeated reverts across full re-stamp
// passes mean the member apiserver is pruning the field (old ISVC CRD) — the
// one skew case that would otherwise mis-scale silently.
const policyPruneRevertThreshold = 3

// policyCondition pairs a source-ISVC condition type with its content, so the
// preflight/aggregate writers can stage conditions for writePlacement to apply
// through the status condition manager.
type policyCondition struct {
	condType apis.ConditionType
	cond     apis.Condition
	// clear removes the condition type from the status outright (the last
	// policy ref was rolled back, so the final state must not stick forever).
	clear bool
}

func preflightCondition(status corev1.ConditionStatus, reason, message string) policyCondition {
	return policyCondition{
		condType: apis.ConditionType(v1beta1.PlacementPolicyPreflightCondition),
		cond:     apis.Condition{Status: status, Reason: reason, Message: message},
	}
}

func aggregateCondition(status corev1.ConditionStatus, reason, message string) policyCondition {
	return policyCondition{
		condType: apis.ConditionType(v1beta1.AutoscalerPolicyAggregateCondition),
		cond:     apis.Condition{Status: status, Reason: reason, Message: message},
	}
}

// applyPolicyConditions writes staged conditions through the ISVC condition
// manager (which preserves LastTransitionTime on unchanged content). A nil
// slice is the no-ref fast path: nothing touches the status. A clear entry
// removes the condition type instead.
func applyPolicyConditions(st *v1beta1.InferenceServiceStatus, conds []policyCondition) {
	for i := range conds {
		if conds[i].clear {
			removeCondition(st, conds[i].condType)
			continue
		}
		st.SetCondition(conds[i].condType, &conds[i].cond)
	}
}

// removeCondition deletes a condition type from the status wholesale. The
// condition-manager Set path can only mark, never remove, so ref rollback
// clears its conditions here.
func removeCondition(st *v1beta1.InferenceServiceStatus, t apis.ConditionType) {
	conds := st.Status.Conditions
	for i := range conds {
		if conds[i].Type == t {
			st.Status.Conditions = append(conds[:i:i], conds[i+1:]...)
			return
		}
	}
}

// policyHomeObservation is one home's lifted AutoscalerPolicy state, refreshed
// whenever the reconcile reads that home's derived ISVC.
type policyHomeObservation struct {
	// autoscaling is the per-home digest state mirrored onto
	// status.placement.candidates[].autoscaling.
	autoscaling *v1beta1.CandidateAutoscalingStatus
	// placedSince anchors the skew clock: the first observation of this
	// ref-carrying (source, home) pair, advanced to the derived's creation
	// time only when that is later (a recreated derived restarts the clock).
	placedSince time.Time
	// allReported: every ref-carrying component proves the member resolved the
	// policy — a policy-rendered resolved digest, or the shadow preview digest
	// when an inline block outranks the ref. The ResolveTimeout detector keys
	// on this, NOT on autoscaling.Ready, so a deliberate inline-precedence
	// (preview) home is never flagged as skew.
	allReported bool
	// failedClosed: the home reports AutoscalerResolved=False for at least one
	// ref-carrying component (member holds last-known-good, loudly).
	failedClosed bool
}

// policyPreflight holds the config-driven preflight tunables plus the
// in-memory per-(source,home) bookkeeping the multi-cluster policy detectors
// need. The API deliberately carries no such state; a control-plane restart
// clears it and every clock restarts conservatively (detectors only ever fire
// later, never spuriously).
type policyPreflight struct {
	memberGetTimeout time.Duration
	skewDeadline     time.Duration
	// now is injectable so tests drive the skew clock without sleeping.
	now func() time.Time

	mu sync.Mutex
	// preflight is the latest staged PlacementPolicyPreflight condition per
	// source, applied on the next status write.
	preflight map[types.UID]policyCondition
	// homes is the latest lifted observation per (source, home).
	homes map[types.UID]map[string]policyHomeObservation
	// prunes counts CONSECUTIVE ref-prune reverts per (source, home).
	prunes map[types.UID]map[string]int
	// eligibility is the last terminally-determined preflight verdict per
	// (source, candidate). A transient member read error keeps the candidate
	// in this last-known state instead of evicting it, so an auth blip or a
	// slow apiserver can never re-race a serving home; only NotFound and
	// digest skew (terminal verdicts) overwrite it.
	eligibility map[types.UID]map[string]bool
}

// newPolicyPreflight resolves the tunables, falling back to the controllerconfig
// operational defaults for unset values (graceful degradation, no magic
// literal at the call sites).
func newPolicyPreflight(cfg controllerconfig.AutoscalerPolicyPreflightConfig) *policyPreflight {
	get := cfg.MemberGetTimeoutSeconds
	if get <= 0 {
		get = controllerconfig.DefaultPolicyMemberGetTimeoutSeconds
	}
	skew := cfg.SkewDeadlineSeconds
	if skew <= 0 {
		skew = controllerconfig.DefaultPolicySkewDeadlineSeconds
	}
	return &policyPreflight{
		memberGetTimeout: time.Duration(get) * time.Second,
		skewDeadline:     time.Duration(skew) * time.Second,
	}
}

func (p *policyPreflight) clock() time.Time {
	if p.now != nil {
		return p.now()
	}
	return time.Now()
}

func (p *policyPreflight) setPreflight(uid types.UID, c policyCondition) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.preflight == nil {
		p.preflight = make(map[types.UID]policyCondition)
	}
	p.preflight[uid] = c
}

func (p *policyPreflight) preflightFor(uid types.UID) (policyCondition, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.preflight[uid]
	return c, ok
}

func (p *policyPreflight) recordHome(uid types.UID, cluster string, obs policyHomeObservation) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.homes == nil {
		p.homes = make(map[types.UID]map[string]policyHomeObservation)
	}
	byCluster := p.homes[uid]
	if byCluster == nil {
		byCluster = make(map[string]policyHomeObservation)
		p.homes[uid] = byCluster
	}
	// The skew clock anchors at the FIRST observation of this ref-carrying
	// (source, home) pair; the derived's creation time only ever moves the
	// anchor FORWARD (a recreated derived restarts the clock). Anchoring on
	// creation alone would start a long-placed derived already past the
	// deadline the moment a ref is added to its source.
	anchor := p.clock()
	if prev, ok := byCluster[cluster]; ok && !prev.placedSince.IsZero() {
		anchor = prev.placedSince
	}
	if obs.placedSince.After(anchor) {
		anchor = obs.placedSince
	}
	obs.placedSince = anchor
	byCluster[cluster] = obs
}

func (p *policyPreflight) homeFor(uid types.UID, cluster string) (policyHomeObservation, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	obs, ok := p.homes[uid][cluster]
	return obs, ok
}

func (p *policyPreflight) forgetHome(uid types.UID, cluster string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.homes[uid], cluster)
	delete(p.prunes[uid], cluster)
}

// recordEligibility stores a candidate's terminally-determined preflight
// verdict (member verified, or member NotFound/digest-skewed). Transient
// read errors never write here; they read the last verdict instead.
func (p *policyPreflight) recordEligibility(uid types.UID, cluster string, eligible bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.eligibility == nil {
		p.eligibility = make(map[types.UID]map[string]bool)
	}
	if p.eligibility[uid] == nil {
		p.eligibility[uid] = make(map[string]bool)
	}
	p.eligibility[uid][cluster] = eligible
}

// lastEligibility returns the candidate's last terminal verdict; known is
// false when no pass has ever terminally verified it (e.g. after a
// control-plane restart).
func (p *policyPreflight) lastEligibility(uid types.UID, cluster string) (eligible, known bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	eligible, known = p.eligibility[uid][cluster]
	return eligible, known
}

// recordPrune advances (pruned) or resets (intact) the consecutive
// prune-revert counter for one (source, home).
func (p *policyPreflight) recordPrune(uid types.UID, cluster string, pruned bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !pruned {
		delete(p.prunes[uid], cluster)
		return
	}
	if p.prunes == nil {
		p.prunes = make(map[types.UID]map[string]int)
	}
	if p.prunes[uid] == nil {
		p.prunes[uid] = make(map[string]int)
	}
	p.prunes[uid][cluster]++
}

// prunedHomes lists (sorted) the homes whose consecutive prune-revert count
// reached the FieldPruned threshold.
func (p *policyPreflight) prunedHomes(uid types.UID) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []string
	for cluster, n := range p.prunes[uid] {
		if n >= policyPruneRevertThreshold {
			out = append(out, cluster)
		}
	}
	sort.Strings(out)
	return out
}

// forget drops all per-source bookkeeping (source deleted).
func (p *policyPreflight) forget(uid types.UID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.preflight, uid)
	delete(p.homes, uid)
	delete(p.prunes, uid)
	delete(p.eligibility, uid)
}

// policyState returns the AutoscalerPolicy preflight state, lazily defaulted so
// a Reconciler assembled as a bare struct literal (tests, cmd wiring) works
// without a constructor; SetupWithManager installs the config-resolved state
// before the manager starts.
func (r *Reconciler) policyState() *policyPreflight {
	r.policyOnce.Do(func() {
		if r.policy == nil {
			r.policy = newPolicyPreflight(controllerconfig.AutoscalerPolicyPreflightConfig{})
		}
	})
	return r.policy
}

// loadPolicyPreflightConfig resolves the autoscalerPolicy preflight tunables
// once at setup from the operator ConfigMap. An unreadable block degrades to
// the controllerconfig operational fallbacks rather than blocking manager
// startup: a control plane without the block still places policy-less ISVCs
// completely untouched.
func loadPolicyPreflightConfig(mgr ctrl.Manager, log logr.Logger) controllerconfig.AutoscalerPolicyPreflightConfig {
	clientset, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err == nil {
		var cfg *controllerconfig.AutoscalerPolicyConfig
		if cfg, err = controllerconfig.NewAutoscalerPolicyConfig(clientset); err == nil {
			return cfg.Preflight
		}
	}
	log.Info("autoscalerPolicy config unavailable; placement preflight uses defaults", "error", err.Error())
	return controllerconfig.AutoscalerPolicyPreflightConfig{}
}

// componentPolicyRef pairs a source component with the AutoscalerPolicy name it
// references.
type componentPolicyRef struct {
	component v1beta1.ComponentType
	policy    string
}

// hasPolicyRefs is the allocation-free zero-cost guard: the entire policy
// preflight/lift surface is inert for the common ISVC that references no
// AutoscalerPolicy, keeping placement behavior byte-identical for it.
func hasPolicyRefs(isvc *v1beta1.InferenceService) bool {
	return (isvc.Spec.Engine != nil && isvc.Spec.Engine.AutoscalerPolicyRef != nil) ||
		(isvc.Spec.Decoder != nil && isvc.Spec.Decoder.AutoscalerPolicyRef != nil) ||
		(isvc.Spec.Router != nil && isvc.Spec.Router.AutoscalerPolicyRef != nil)
}

// referencedPolicyRefs lists the components carrying an autoscalerPolicyRef in
// declared-component order.
func referencedPolicyRefs(isvc *v1beta1.InferenceService) []componentPolicyRef {
	var out []componentPolicyRef
	for _, comp := range declaredComponents(isvc) {
		if ref := autoscaler.ComponentPolicyRef(isvc, comp); ref != nil && ref.Name != "" {
			out = append(out, componentPolicyRef{component: comp, policy: ref.Name})
		}
	}
	return out
}

// distinctPolicyNames returns the sorted distinct policy names across refs.
func distinctPolicyNames(refs []componentPolicyRef) []string {
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

// splitCeilingUnset reports whether Split placement lacks a per-cluster
// replica ceiling. Callers check the mode first (Split implies spec.placement
// is set).
func splitCeilingUnset(isvc *v1beta1.InferenceService) bool {
	sp := isvc.Spec.Placement.Split
	return sp == nil || sp.MaxReplicasPerCluster <= 0
}

// controlPlaneName names this control plane in operator-facing messages.
func (r *Reconciler) controlPlaneName() string {
	if r.ControlPlaneID != "" {
		return "control plane " + r.ControlPlaneID
	}
	return "the control plane"
}

// policyPreflightOutcome is the preflight verdict for one pass: the candidates
// still eligible for fan-out, or a full hold (Split hard gate, missing anchor,
// or no eligible candidate at all).
type policyPreflightOutcome struct {
	eligible []string
	// hold gates fan-out while the caller continues observing standing homes.
	hold bool
	// holdAsIs gates fan-out and Single re-racing while the caller continues
	// observing and publishing standing-home health.
	holdAsIs bool
	// transient marks a pass whose verdict rests on a transient read error,
	// so the caller re-checks at the poll cadence instead of the long
	// steady-state backstop.
	transient bool
}

// candidateSkip records one candidate the preflight made ineligible this pass.
type candidateSkip struct {
	cluster string
	reason  string
	detail  string
	// transient: the verdict rests on a read error that can heal by itself
	// (anything but NotFound), not on observed terminal skew.
	transient bool
}

// skipReasonPrecedence orders the False reason chosen when NO candidate
// remains eligible: a digest mismatch is the loudest genuine-skew signal,
// then a missing member copy (distribution lag), then an unreachable member,
// then a member that never declared the capability at all.
var skipReasonPrecedence = []string{
	v1beta1.PlacementPolicyPreflightReasonDigestMismatch,
	v1beta1.PlacementPolicyPreflightReasonPolicyMissing,
	v1beta1.PlacementPolicyPreflightReasonMemberGetTimeout,
	v1beta1.PlacementPolicyPreflightReasonCapabilityMissing,
}

func dominantSkipReason(skips []candidateSkip) string {
	present := make(map[string]bool, len(skips))
	for _, s := range skips {
		present[s.reason] = true
	}
	for _, reason := range skipReasonPrecedence {
		if present[reason] {
			return reason
		}
	}
	return v1beta1.PlacementPolicyPreflightReasonCapabilityMissing
}

func describeSkips(skips []candidateSkip) string {
	parts := make([]string, 0, len(skips))
	for _, s := range skips {
		parts = append(parts, fmt.Sprintf("%s (%s: %s)", s.cluster, s.reason, s.detail))
	}
	return strings.Join(parts, "; ")
}

// preflightPolicies gates the candidate set for a policy-referencing source
// before fan-out: member capability label, policy presence and portable-digest
// equality against the control-plane anchor copy, and the Split hard gate.
// Returns nil for a source with no refs (the zero-cost path: candidates flow
// through untouched). It only ever READS policies — on the control plane and,
// by name, on each candidate — and stages the PlacementPolicyPreflight
// condition for the next status write.
func (r *Reconciler) preflightPolicies(ctx context.Context, isvc *v1beta1.InferenceService, candidates []string, clusters []v1beta1.WorkloadCluster) *policyPreflightOutcome {
	if !hasPolicyRefs(isvc) {
		return nil
	}
	refs := referencedPolicyRefs(isvc)
	pp := r.policyState()
	names := distinctPolicyNames(refs)

	// The control plane runs no ISVC admission webhook, so the source-side
	// ref-shape validation happens here: a reserved ref kind would otherwise
	// pass preflight and then be denied by every member webhook on the
	// derived apply, wedging placement behind a Passed condition.
	if err := validation.ValidateAutoscalerPolicyRefs(isvc, true); err != nil {
		pp.setPreflight(isvc.UID, preflightCondition(corev1.ConditionFalse,
			v1beta1.PlacementPolicyPreflightReasonInvalidRef,
			fmt.Sprintf("autoscalerPolicyRef on the source is invalid: %v; every member webhook would deny the derived apply, holding placement", err)))
		return &policyPreflightOutcome{hold: true}
	}

	// Anchor copies: the GitOps-synced control-plane policy is the digest
	// anchor every member must match. Read live (APIReader) so a missing
	// object surfaces immediately. A missing/unreadable anchor holds placement
	// outright — no candidate can be verified against nothing.
	anchorDigests := make(map[string]string, len(names))
	anchorSpecs := make(map[string]*v1beta1.AutoscalerPolicySpec, len(names))
	for _, name := range names {
		pol := &v1beta1.AutoscalerPolicy{}
		if err := r.APIReader.Get(ctx, types.NamespacedName{Namespace: isvc.Namespace, Name: name}, pol); err != nil {
			if !apierrors.IsNotFound(err) && sourcePlaced(isvc) {
				// A transient control-plane read error is no verdict on the
				// anchor. Hold actuation and re-read soon.
				r.Log.Error(err, "policy preflight: anchor read failed; holding existing placement",
					"policy", isvc.Namespace+"/"+name, "isvc", isvc.Namespace+"/"+isvc.Name)
				return &policyPreflightOutcome{holdAsIs: true, transient: true}
			}
			pp.setPreflight(isvc.UID, preflightCondition(corev1.ConditionFalse,
				v1beta1.PlacementPolicyPreflightReasonPolicyMissing,
				fmt.Sprintf("AutoscalerPolicy %s/%s has no readable anchor copy on %s: %v; holding placement",
					isvc.Namespace, name, r.controlPlaneName(), err)))
			return &policyPreflightOutcome{hold: true, transient: !apierrors.IsNotFound(err)}
		}
		digest, err := render.PortableDigest(&pol.Spec)
		if err != nil {
			pp.setPreflight(isvc.UID, preflightCondition(corev1.ConditionFalse,
				v1beta1.PlacementPolicyPreflightReasonPolicyMissing,
				fmt.Sprintf("AutoscalerPolicy %s/%s anchor copy on %s cannot be digested: %v; holding placement",
					isvc.Namespace, name, r.controlPlaneName(), err)))
			return &policyPreflightOutcome{hold: true}
		}
		// The anchor spec must itself validate: members enforce the same
		// checks at admission, so an invalid anchor (reserved enforcement,
		// template or query errors) means every derived apply is denied.
		if issues := render.ValidateSpec(&pol.Spec); len(issues) > 0 {
			pp.setPreflight(isvc.UID, preflightCondition(corev1.ConditionFalse,
				v1beta1.PlacementPolicyPreflightReasonInvalidPolicy,
				fmt.Sprintf("AutoscalerPolicy %s/%s anchor copy on %s is invalid: %s; every member webhook would deny the rendered scaler, holding placement",
					isvc.Namespace, name, r.controlPlaneName(), joinIssues(issues))))
			return &policyPreflightOutcome{hold: true}
		}
		anchorDigests[name] = digest
		anchorSpecs[name] = &pol.Spec
	}

	// Split hard gate: with no per-cluster ceiling, every home renders the
	// GLOBAL MaxReplicas, so a fleet-wide metric outage would drive each home
	// to the full budget (N x max). Hold loudly; the fix is one spec field.
	if placementMode(isvc) == v1beta1.PlacementModeSplit && splitCeilingUnset(isvc) {
		for _, name := range names {
			consumes, err := render.ConsumesMaxReplicas(anchorSpecs[name])
			if err != nil || consumes {
				detail := "the policy derives from the component's MaxReplicas"
				if err != nil {
					detail = fmt.Sprintf("the policy template could not be proven MaxReplicas-free (%v)", err)
				}
				pp.setPreflight(isvc.UID, preflightCondition(corev1.ConditionFalse,
					v1beta1.PlacementPolicyPreflightReasonUnboundedSplitCeiling,
					fmt.Sprintf("Split placement with AutoscalerPolicy %s/%s held: %s and spec.placement.split.maxReplicasPerCluster is unset, so every home would keep the global replica ceiling",
						isvc.Namespace, name, detail)))
				return &policyPreflightOutcome{hold: true}
			}
		}
	}

	labelsFor := make(map[string]map[string]string, len(clusters))
	for i := range clusters {
		labelsFor[clusters[i].Name] = clusters[i].Labels
	}

	winner := winnerCluster(isvc)
	var eligible []string
	var skips []candidateSkip
	transient := false
	for _, c := range candidates {
		var skip *candidateSkip
		if labelsFor[c][constants.WorkloadClusterAutoscalerPolicyLabel] != WorkloadClusterAutoscalerPolicyCapability {
			skip = &candidateSkip{
				cluster: c,
				reason:  v1beta1.PlacementPolicyPreflightReasonCapabilityMissing,
				detail: fmt.Sprintf("cluster does not carry %s=%s",
					constants.WorkloadClusterAutoscalerPolicyLabel, WorkloadClusterAutoscalerPolicyCapability),
			}
		} else {
			cl, ok := r.Clusters.ClientFor(c)
			if !ok {
				// Not connected this pass: fan-out cannot reach it anyway, and the
				// sticky-winner logic owns transport flaps. Leave it eligible;
				// verification happens on the pass that can actually place.
				eligible = append(eligible, c)
				continue
			}
			skip = r.memberPolicySkew(ctx, cl, isvc.Namespace, names, anchorDigests)
		}
		if skip == nil {
			pp.recordEligibility(isvc.UID, c, true)
			eligible = append(eligible, c)
			continue
		}
		skip.cluster = c
		if skip.transient {
			transient = true
			if last, known := pp.lastEligibility(isvc.UID, c); known && last {
				// A transient read error keeps the candidate in its last
				// terminally-verified eligibility; only NotFound and digest
				// skew evict. Re-verified at the poll cadence.
				eligible = append(eligible, c)
				continue
			}
		} else {
			pp.recordEligibility(isvc.UID, c, false)
		}
		if c == winner {
			// Preflight ineligibility gates new fan-out without freezing the
			// standing winner's observed health.
			r.Log.Info("policy preflight: standing winner ineligible; holding actuation",
				"cluster", c, "isvc", isvc.Namespace+"/"+isvc.Name, "reason", skip.reason, "detail", skip.detail)
			if !skip.transient {
				pp.setPreflight(isvc.UID, preflightCondition(corev1.ConditionFalse, skip.reason,
					"standing winner is ineligible; skipped: "+describeSkips([]candidateSkip{*skip})))
			}
			return &policyPreflightOutcome{holdAsIs: true, transient: skip.transient}
		}
		skips = append(skips, *skip)
	}

	if len(eligible) == 0 {
		pp.setPreflight(isvc.UID, preflightCondition(corev1.ConditionFalse,
			dominantSkipReason(skips),
			"no eligible candidate remains; skipped: "+describeSkips(skips)))
		return &policyPreflightOutcome{hold: true, transient: transient}
	}
	msg := fmt.Sprintf("%d of %d candidates eligible", len(eligible), len(candidates))
	if len(skips) > 0 {
		msg += "; skipped: " + describeSkips(skips)
	}
	pp.setPreflight(isvc.UID, preflightCondition(corev1.ConditionTrue,
		v1beta1.PlacementPolicyPreflightReasonPassed, msg))
	return &policyPreflightOutcome{eligible: eligible, transient: transient}
}

// sourcePlaced reports whether the source currently publishes a Placed
// placement — a serving status/URL an errant Pending write would wipe.
func sourcePlaced(isvc *v1beta1.InferenceService) bool {
	return isvc.Status.Placement != nil && isvc.Status.Placement.Phase == v1beta1.PlacementPhasePlaced
}

// joinIssues renders validation issues for an operator-facing message.
func joinIssues(issues []render.Issue) string {
	parts := make([]string, 0, len(issues))
	for _, issue := range issues {
		parts = append(parts, issue.String())
	}
	return strings.Join(parts, "; ")
}

// memberPolicySkew live-GETs each referenced policy on one candidate (by name
// only — read-only access) and compares portable digests against the anchor.
// Each GET is bounded by the configured member-get timeout so one hung member
// apiserver cannot stall the whole preflight. Returns nil when the member
// matches on every policy.
func (r *Reconciler) memberPolicySkew(ctx context.Context, reads client.Reader, namespace string, names []string, anchorDigests map[string]string) *candidateSkip {
	pp := r.policyState()
	for _, name := range names {
		cctx, cancel := context.WithTimeout(ctx, pp.memberGetTimeout)
		remote := &v1beta1.AutoscalerPolicy{}
		err := reads.Get(cctx, types.NamespacedName{Namespace: namespace, Name: name}, remote)
		cancel()
		switch {
		case err == nil:
			digest, derr := render.PortableDigest(&remote.Spec)
			if derr != nil {
				return &candidateSkip{
					reason: v1beta1.PlacementPolicyPreflightReasonDigestMismatch,
					detail: fmt.Sprintf("policy %q member copy cannot be digested: %v", name, derr),
				}
			}
			if digest != anchorDigests[name] {
				return &candidateSkip{
					reason: v1beta1.PlacementPolicyPreflightReasonDigestMismatch,
					detail: fmt.Sprintf("policy %q digest %s on member differs from anchor %s", name, digest, anchorDigests[name]),
				}
			}
		case apierrors.IsNotFound(err):
			return &candidateSkip{
				reason: v1beta1.PlacementPolicyPreflightReasonPolicyMissing,
				detail: fmt.Sprintf("policy %q not found on member", name),
			}
		case errors.Is(err, context.DeadlineExceeded) || apierrors.IsTimeout(err) || apierrors.IsServerTimeout(err):
			return &candidateSkip{
				reason:    v1beta1.PlacementPolicyPreflightReasonMemberGetTimeout,
				detail:    fmt.Sprintf("policy %q get timed out after %s", name, pp.memberGetTimeout),
				transient: true,
			}
		default:
			return &candidateSkip{
				reason:    v1beta1.PlacementPolicyPreflightReasonMemberGetTimeout,
				detail:    fmt.Sprintf("policy %q get failed: %v", name, err),
				transient: true,
			}
		}
	}
	return nil
}

// observeDerivedPolicyStatus lifts a freshly read derived ISVC's per-component
// autoscaler-policy status into the in-memory home observations, from which the
// next status write populates candidates[].autoscaling and evaluates the
// aggregate detectors. No-op for the common no-ref source.
func (r *Reconciler) observeDerivedPolicyStatus(src *v1beta1.InferenceService, cluster string, derived *v1beta1.InferenceService) {
	if !hasPolicyRefs(src) {
		return
	}
	lifted, allReported, failedClosed := liftCandidateAutoscaling(referencedPolicyRefs(src), derived)
	r.policyState().recordHome(src.UID, cluster, policyHomeObservation{
		autoscaling:  lifted,
		placedSince:  derived.CreationTimestamp.Time,
		allReported:  allReported,
		failedClosed: failedClosed,
	})
}

// liftCandidateAutoscaling maps one derived ISVC's
// status.components.<c>.autoscaler policy fields into the per-home
// CandidateAutoscalingStatus: the portable digest observed per policy, the
// per-component resolved digest, and readiness (specSource=policy with a
// non-empty resolved digest). It also derives the two detector inputs: whether
// every ref-carrying component proved resolution (policy render OR the shadow
// preview under inline precedence), and whether any component fails closed.
func liftCandidateAutoscaling(refs []componentPolicyRef, derived *v1beta1.InferenceService) (*v1beta1.CandidateAutoscalingStatus, bool, bool) {
	out := &v1beta1.CandidateAutoscalingStatus{
		Components: make(map[v1beta1.ComponentType]v1beta1.CandidateComponentAutoscaling, len(refs)),
	}
	observedDigest := make(map[string]string, len(refs))
	ready, reported, failedClosed := true, true, false
	for _, ref := range refs {
		entry := v1beta1.CandidateComponentAutoscaling{}
		appliedRef := autoscaler.ComponentPolicyRef(derived, ref.component)
		if appliedRef == nil || appliedRef.Name != ref.policy {
			out.Components[ref.component] = entry
			ready = false
			reported = false
			continue
		}
		var as *v1beta1.ComponentAutoscalerStatus
		if cs, ok := derived.Status.Components[ref.component]; ok {
			as = cs.Autoscaler
		}
		compReported := false
		if as != nil {
			switch {
			case as.SpecSource == string(autoscaler.SpecSourcePolicy) && as.Policy != nil && as.Policy.Name == ref.policy:
				entry.ResolvedDigest = as.Policy.ResolvedDigest
				entry.Ready = entry.ResolvedDigest != ""
				compReported = entry.Ready
				if observedDigest[ref.policy] == "" {
					observedDigest[ref.policy] = as.Policy.PortableDigest
				}
			case as.ShadowedPolicyRef != nil && as.ShadowedPolicyRef.Name == ref.policy:
				// Inline outranks the ref: not policy-ready, but the shadow
				// preview proves the member resolved the policy, so the skew
				// detector must not count this home as unresponsive.
				compReported = as.ShadowedPolicyRef.WouldRenderDigest != ""
				if observedDigest[ref.policy] == "" {
					observedDigest[ref.policy] = as.ShadowedPolicyRef.PortableDigest
				}
			}
			resolved := apimeta.FindStatusCondition(as.Conditions, v1beta1.AutoscalerResolvedCondition)
			if resolved != nil && resolved.Status == metav1.ConditionFalse && derived.Generation > 0 &&
				resolved.ObservedGeneration == derived.Generation {
				failedClosed = true
			}
		}
		out.Components[ref.component] = entry
		ready = ready && entry.Ready
		reported = reported && compReported
	}
	out.Ready = ready
	for _, name := range distinctPolicyNames(refs) {
		out.Policies = append(out.Policies, v1beta1.CandidatePolicyDigest{
			Name: name, PortableDigest: observedDigest[name],
		})
	}
	return out, reported, failedClosed
}

// observePolicyRefStamp feeds the FieldPruned detector from the wholesale
// re-stamp in applyDerived: when the pre-apply live remote object exists but
// lacks a ref the desired derived spec carries, the member apiserver pruned the
// field on the previous apply (old ISVC CRD — the one silent-mis-scale skew).
// An intact live object resets the consecutive counter.
func (r *Reconciler) observePolicyRefStamp(src *v1beta1.InferenceService, cluster string, live, desired *v1beta1.InferenceService) {
	if !hasPolicyRefs(desired) {
		return
	}
	if live.ResourceVersion == "" {
		return // first create: no live object to have pruned anything
	}
	pruned := false
	for _, ref := range referencedPolicyRefs(desired) {
		if autoscaler.ComponentPolicyRef(live, ref.component) == nil {
			pruned = true
			break
		}
	}
	r.policyState().recordPrune(src.UID, cluster, pruned)
}

// policyStatusForWrite decorates the pass's placement result for a
// policy-referencing source right before the status write: it attaches the
// lifted per-home autoscaling state to the candidates and returns the staged
// PlacementPolicyPreflight condition plus the freshly evaluated
// AutoscalerPolicyReady aggregate. Nil (and untouched candidates) for the
// common no-ref source.
func (r *Reconciler) policyStatusForWrite(isvc *v1beta1.InferenceService, res *placementResult) []policyCondition {
	if !hasPolicyRefs(isvc) {
		// Ref rollback: a source whose last ref was removed must not keep the
		// final policy conditions frozen on its status. Clear both in the
		// same write and drop the per-source bookkeeping; a never-referencing
		// source has neither condition and stays on the zero-cost path.
		if isvc.Status.GetCondition(apis.ConditionType(v1beta1.PlacementPolicyPreflightCondition)) == nil &&
			isvc.Status.GetCondition(apis.ConditionType(v1beta1.AutoscalerPolicyAggregateCondition)) == nil {
			return nil
		}
		r.policyState().forget(isvc.UID)
		return []policyCondition{
			{condType: apis.ConditionType(v1beta1.PlacementPolicyPreflightCondition), clear: true},
			{condType: apis.ConditionType(v1beta1.AutoscalerPolicyAggregateCondition), clear: true},
		}
	}
	pp := r.policyState()
	var conds []policyCondition
	if pre, ok := pp.preflightFor(isvc.UID); ok {
		conds = append(conds, pre)
	}

	now := pp.clock()
	var failed, timedOut []string
	homes, readyHomes := 0, 0
	for i := range res.candidates {
		c := &res.candidates[i]
		obs, ok := pp.homeFor(isvc.UID, c.Cluster)
		if !ok {
			continue
		}
		homes++
		if obs.autoscaling != nil {
			c.Autoscaling = obs.autoscaling.DeepCopy()
			if obs.autoscaling.Ready {
				readyHomes++
			}
		}
		if obs.failedClosed {
			failed = append(failed, c.Cluster)
		}
		if !obs.allReported && now.Sub(obs.placedSince) > pp.skewDeadline {
			timedOut = append(timedOut, c.Cluster)
		}
	}
	// Only homes in THIS pass's candidate set can hold the aggregate False
	// for pruning: the counter resets solely on a successful re-apply to the
	// offending member, which never happens once it leaves the fleet, so an
	// un-intersected record would latch the condition forever.
	pruned := pp.prunedHomes(isvc.UID)
	if len(pruned) > 0 {
		current := make(map[string]struct{}, len(res.candidates))
		for i := range res.candidates {
			current[res.candidates[i].Cluster] = struct{}{}
		}
		pruned = slices.DeleteFunc(pruned, func(c string) bool {
			_, ok := current[c]
			return !ok
		})
	}
	sort.Strings(failed)
	sort.Strings(timedOut)

	var agg policyCondition
	switch {
	case len(pruned) > 0:
		agg = aggregateCondition(corev1.ConditionFalse, v1beta1.AutoscalerPolicyAggregateReasonFieldPruned,
			fmt.Sprintf("homes keep reverting the applied autoscalerPolicyRef (%d consecutive re-stamps pruned): %s",
				policyPruneRevertThreshold, strings.Join(pruned, ", ")))
	case len(failed) > 0:
		agg = aggregateCondition(corev1.ConditionFalse, v1beta1.AutoscalerPolicyAggregateReasonMemberFailedClose,
			"homes report AutoscalerResolved=False: "+strings.Join(failed, ", "))
	case len(timedOut) > 0:
		agg = aggregateCondition(corev1.ConditionFalse, v1beta1.AutoscalerPolicyAggregateReasonResolveTimeout,
			fmt.Sprintf("homes have not reported a resolved policy digest within %s: %s",
				pp.skewDeadline, strings.Join(timedOut, ", ")))
	case homes == 0:
		agg = aggregateCondition(corev1.ConditionTrue, v1beta1.AutoscalerPolicyAggregateReasonAllHomesResolved,
			"no policy-referencing home placed yet")
	case readyHomes == homes:
		agg = aggregateCondition(corev1.ConditionTrue, v1beta1.AutoscalerPolicyAggregateReasonAllHomesResolved,
			fmt.Sprintf("all %d homes report resolved policy digests", homes))
	default:
		agg = aggregateCondition(corev1.ConditionTrue, v1beta1.AutoscalerPolicyAggregateReasonAllHomesResolved,
			fmt.Sprintf("%d of %d homes resolved; the rest are within the skew deadline", readyHomes, homes))
	}
	conds = append(conds, agg)
	return conds
}
