package placement

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// preflightRecord is the bookkeeping the two policy preflights keep per
// source: the PlacementPolicyPreflight condition staged for the next status
// write and one observation per member cluster, behind one mutex. The API
// carries none of it; a control-plane restart starts it empty.
type preflightRecord[M any] struct {
	mu sync.Mutex
	// staged is the latest staged condition per source.
	staged map[types.UID]policyCondition
	// members is the latest observation per (source, member).
	members map[types.UID]map[string]M
}

func (p *preflightRecord[M]) setPreflight(uid types.UID, c policyCondition) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.staged == nil {
		p.staged = make(map[types.UID]policyCondition)
	}
	p.staged[uid] = c
}

func (p *preflightRecord[M]) preflightFor(uid types.UID) (policyCondition, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.staged[uid]
	return c, ok
}

func (p *preflightRecord[M]) recordMember(uid types.UID, cluster string, obs M) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.putMemberLocked(uid, cluster, obs)
}

// putMemberLocked stores one member's observation; the caller holds mu.
func (p *preflightRecord[M]) putMemberLocked(uid types.UID, cluster string, obs M) {
	if p.members == nil {
		p.members = make(map[types.UID]map[string]M)
	}
	if p.members[uid] == nil {
		p.members[uid] = make(map[string]M)
	}
	p.members[uid][cluster] = obs
}

func (p *preflightRecord[M]) memberFor(uid types.UID, cluster string) (M, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	obs, ok := p.members[uid][cluster]
	return obs, ok
}

func (p *preflightRecord[M]) forgetMember(uid types.UID, cluster string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.members[uid], cluster)
}

// forget drops all per-source bookkeeping (source deleted or refs removed).
func (p *preflightRecord[M]) forget(uid types.UID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dropSourceLocked(uid)
}

// dropSourceLocked removes a source's condition and members; the caller holds mu.
func (p *preflightRecord[M]) dropSourceLocked(uid types.UID) {
	delete(p.staged, uid)
	delete(p.members, uid)
}

// conditionStager is the part of a preflight record the shared steps write:
// the condition staged for a source's next status write.
type conditionStager interface {
	setPreflight(uid types.UID, c policyCondition)
}

// policyRef names a control-plane policy a source refers to; a component's
// autoscalerPolicyRef and a rollout group's policyRef both implement it.
type policyRef interface {
	policyName() string
}

// distinctPolicyNames returns the sorted distinct policy names across refs.
func distinctPolicyNames[R policyRef](refs []R) []string {
	seen := make(map[string]struct{}, len(refs))
	var names []string
	for _, ref := range refs {
		name := ref.policyName()
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// clusterLabels indexes the fleet's labels by cluster name.
func clusterLabels(clusters []v1beta1.WorkloadCluster) map[string]map[string]string {
	labelsFor := make(map[string]map[string]string, len(clusters))
	for i := range clusters {
		labelsFor[clusters[i].Name] = clusters[i].Labels
	}
	return labelsFor
}

// policyPreflightOutcome is what one preflight pass decided: the candidates
// still eligible for fan-out, or a full hold (Split hard gate, missing anchor,
// or no eligible candidate at all).
type policyPreflightOutcome struct {
	eligible []string
	// hold gates fan-out while the caller continues observing standing members.
	hold bool
	// holdAsIs gates fan-out and Single re-racing while the caller continues
	// observing and publishing standing-member health.
	holdAsIs bool
	// transient marks a pass whose outcome rests on a transient read error,
	// so the caller re-checks at the poll cadence instead of the long
	// steady-state backstop.
	transient bool
}

// candidateSkip records one candidate the preflight made ineligible this pass.
type candidateSkip struct {
	cluster string
	reason  string
	detail  string
	// transient: the skip rests on a read error that can clear by itself
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

// holdOnUnreadableAnchor turns a failed control-plane read of a referenced
// policy into the pass's hold. While the source is placed, an error other
// than NotFound says nothing about the policy: the pass logs it, keeps the
// placement as it is and reads again soon. Otherwise the condition is staged
// and placement holds, transient unless the policy is absent.
func (r *Reconciler) holdOnUnreadableAnchor(err error, isvc *v1beta1.InferenceService, name string, record conditionStager, readFailedLog string, unreadable policyCondition) *policyPreflightOutcome {
	if !apierrors.IsNotFound(err) && sourcePlaced(isvc) {
		r.Log.Error(err, readFailedLog,
			"policy", isvc.Namespace+"/"+name, "isvc", isvc.Namespace+"/"+isvc.Name)
		return &policyPreflightOutcome{holdAsIs: true, transient: true}
	}
	record.setPreflight(isvc.UID, unreadable)
	return &policyPreflightOutcome{hold: true, transient: !apierrors.IsNotFound(err)}
}

// candidateGate walks a source's candidates in order for one policy kind and
// turns them into the pass's outcome: the candidates still eligible for
// fan-out, a hold when none remains, or a hold that keeps the placement as it
// is when the cluster serving the source today is itself ineligible (its
// health keeps being observed and published either way).
type candidateGate struct {
	// record receives the condition the gate stages for the source.
	record conditionStager
	// check classifies one candidate: nil keeps it eligible; a skip removes
	// it unless keep says a transient skip leaves it as it was last verified.
	check func(cluster string) (skip *candidateSkip, keep bool)
	// holdPlaced reports the serving cluster's skip (a log line and, for a
	// terminal skip, the staged condition) before the gate holds as is.
	holdPlaced func(skip candidateSkip)
	// messagePrefix opens the two condition messages the gate itself stages.
	messagePrefix string
}

// run stages the source's condition and returns the outcome, transient when
// any skip rested on a read error that can clear by itself.
func (g candidateGate) run(uid types.UID, candidates []string, placedCluster string) *policyPreflightOutcome {
	var eligible []string
	var skips []candidateSkip
	transient := false
	for _, c := range candidates {
		skip, keep := g.check(c)
		if skip == nil {
			eligible = append(eligible, c)
			continue
		}
		skip.cluster = c
		transient = transient || skip.transient
		if keep {
			eligible = append(eligible, c)
			continue
		}
		if c == placedCluster {
			g.holdPlaced(*skip)
			return &policyPreflightOutcome{holdAsIs: true, transient: skip.transient}
		}
		skips = append(skips, *skip)
	}
	if len(eligible) == 0 {
		g.record.setPreflight(uid, preflightCondition(corev1.ConditionFalse, dominantSkipReason(skips),
			g.messagePrefix+"no eligible candidate remains; skipped: "+describeSkips(skips)))
		return &policyPreflightOutcome{hold: true, transient: transient}
	}
	msg := g.messagePrefix + fmt.Sprintf("%d of %d candidates eligible", len(eligible), len(candidates))
	if len(skips) > 0 {
		msg += "; skipped: " + describeSkips(skips)
	}
	g.record.setPreflight(uid, preflightCondition(corev1.ConditionTrue,
		v1beta1.PlacementPolicyPreflightReasonPassed, msg))
	return &policyPreflightOutcome{eligible: eligible, transient: transient}
}
