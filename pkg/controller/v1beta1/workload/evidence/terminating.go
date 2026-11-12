package evidence

import (
	"context"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TerminatingClass classifies one Terminating pod against the
// stuck-teardown force-delete predicate. Exactly four values are
// actionable; everything else leaves the pod alone.
type TerminatingClass int

const (
	// NotConfigured: no ForceDeletePolicy — the escalation does
	// not exist.
	NotConfigured TerminatingClass = iota
	// WithinGrace: the pod is inside its own graceful-deletion
	// window (plus the configured slack), and no early force-delete
	// applies to it.
	WithinGrace
	// ForeignFinalizers: the pod is pinned by finalizers.
	// Report-only; force-delete could not remove the object anyway.
	ForeignFinalizers
	// Unscheduled: the pod never landed on a node — there is no
	// node whose death could be proven.
	Unscheduled
	// NodeReadError: the live Node read failed (non-NotFound).
	// Fail-safe: not actionable this pass.
	NodeReadError
	// NodeHealthy: the node has no current unreachable evidence.
	NodeHealthy
	// NodeNotDeadLongEnough: unreachable taint / NotReady
	// condition present but younger than the threshold (or of unprovable
	// age).
	NodeNotDeadLongEnough
	// NodeGone: the Node object is NotFound. Actionable.
	NodeGone
	// NodeUnreachableTaint: node.kubernetes.io/unreachable with
	// TimeAdded at least the threshold old. Actionable.
	NodeUnreachableTaint
	// NodeNotReady: NodeReady False/Unknown with
	// LastTransitionTime at least the threshold old. Actionable.
	NodeNotReady
	// NodeReadyUnknown: NodeReady Unknown with LastTransitionTime at least
	// the threshold old, read for a pod still inside its grace period when
	// SkipGracePeriodOnUnreachableNode is set. Actionable.
	NodeReadyUnknown
)

// Actionable reports whether the evidence justifies a force-delete.
func (e TerminatingClass) Actionable() bool {
	switch e {
	case NodeGone, NodeUnreachableTaint, NodeNotReady, NodeReadyUnknown:
		return true
	}
	return false
}

// terminatingClassNames names every class for events and logs, by value.
var terminatingClassNames = [...]string{
	NotConfigured:         "not-configured",
	WithinGrace:           "within-grace",
	ForeignFinalizers:     "foreign-finalizers",
	Unscheduled:           "unscheduled",
	NodeReadError:         "node-read-error",
	NodeHealthy:           "node-healthy",
	NodeNotDeadLongEnough: "node-not-dead-long-enough",
	NodeGone:              "node-gone",
	NodeUnreachableTaint:  "node-unreachable-taint",
	NodeNotReady:          "node-not-ready",
	NodeReadyUnknown:      "node-ready-unknown",
}

// String names the class for events and logs.
func (e TerminatingClass) String() string {
	if e >= 0 && int(e) < len(terminatingClassNames) && terminatingClassNames[e] != "" {
		return terminatingClassNames[e]
	}
	return "unknown"
}

// TerminatingResult is the classification plus the supporting detail the
// escalation reports in its event and audit entry.
type TerminatingResult struct {
	Kind     TerminatingClass
	NodeName string
	// RequeueAt is the next time-based evidence boundary or live recheck.
	// Zero means progress depends on a resource event.
	RequeueAt time.Time
	// Overdue is how far past the pod's own DeletionTimestamp now is. It
	// is negative for a pod still inside its own grace period.
	Overdue time.Duration
	// NodeReadErr carries a non-NotFound Node read failure for the caller
	// to log: the one behind NodeReadError, or, on WithinGrace, the read
	// that could not end the grace period early.
	NodeReadErr error
	// GraceSkipped is set on an actionable result for a pod still inside
	// its deletion deadline plus OverdueSlack, which only
	// SkipGracePeriodOnUnreachableNode lets go.
	GraceSkipped bool
}

// StuckTerminating classifies one Terminating pod for the force-delete
// escalation, in this order: NotConfigured without a policy (no Node is
// read); WithinGrace until DeletionTimestamp plus OverdueSlack has passed
// (the timestamp already holds the pod's own grace period, so each pod
// gets its own window), unless SkipGracePeriodOnUnreachableNode is set
// and unreachableInsideGrace ends the wait early; ForeignFinalizers,
// report-only, while finalizers remain, since a force-delete cannot
// remove them; otherwise NodeDeath decides from the live node.
func StuckTerminating(ctx context.Context, readers types.NodeReaders, pod *corev1.Pod, policy *types.ForceDeletePolicy, now time.Time) TerminatingResult {
	if policy == nil {
		return TerminatingResult{Kind: NotConfigured}
	}
	if pod.DeletionTimestamp == nil {
		// Not Terminating — nothing to escalate. Defensive: callers only
		// pass Terminating pods.
		return TerminatingResult{Kind: WithinGrace}
	}
	overdueAt := pod.DeletionTimestamp.Add(policy.OverdueSlack)
	if !now.After(overdueAt) {
		// The policy is strictly "past" the deadline. One nanosecond is the
		// smallest representable wake after equality, not a polling cadence.
		res := TerminatingResult{Kind: WithinGrace, RequeueAt: overdueAt.Add(time.Nanosecond)}
		if policy.SkipGracePeriodOnUnreachableNode {
			return unreachableInsideGrace(ctx, readers, pod, policy, now, res)
		}
		return res
	}
	overdue := now.Sub(pod.DeletionTimestamp.Time)
	if len(pod.Finalizers) > 0 {
		return TerminatingResult{Kind: ForeignFinalizers, NodeName: pod.Spec.NodeName, Overdue: overdue}
	}
	return NodeDeath(ctx, readers.Live, pod, policy, now, overdue)
}

// unreachableInsideGrace reads a pod inside its grace period, the time
// its kubelet has to stop it. Only a node whose kubelet has stopped
// reporting for at least the threshold ends that wait early. The cached
// Node screens the pod at no API cost. Only a cached Node that already
// shows the threshold passed is read again live, and the live copy
// decides. A pod held by finalizers or bound to no node, a node that is
// gone or unreadable, and a node whose kubelet still reports all keep the
// pod waiting. Signs younger than the threshold move the requeue to the
// moment they come of age, if that is sooner.
func unreachableInsideGrace(ctx context.Context, readers types.NodeReaders, pod *corev1.Pod, policy *types.ForceDeletePolicy, now time.Time, wait TerminatingResult) TerminatingResult {
	if len(pod.Finalizers) > 0 || pod.Spec.NodeName == "" {
		return wait
	}
	wait.NodeName = pod.Spec.NodeName
	key := client.ObjectKey{Name: pod.Spec.NodeName}
	// The cached copy is only read, so the cache hands it over uncopied.
	cached := &corev1.Node{}
	if err := readers.Cache.Get(ctx, key, cached, client.UnsafeDisableDeepCopy); err != nil {
		return waitAfterFailedRead(wait, err)
	}
	if kind, at := unreachableClass(cached, now, policy.NodeUnreachableThreshold); !kind.Actionable() {
		return waitUntil(wait, at)
	}
	live := &corev1.Node{}
	if err := readers.Live.Get(ctx, key, live); err != nil {
		return waitAfterFailedRead(wait, err)
	}
	kind, at := unreachableClass(live, now, policy.NodeUnreachableThreshold)
	if !kind.Actionable() {
		return waitUntil(wait, at)
	}
	return TerminatingResult{Kind: kind, NodeName: live.Name, Overdue: now.Sub(pod.DeletionTimestamp.Time), GraceSkipped: true}
}

// waitAfterFailedRead keeps the pod waiting on a Node read that failed. A
// gone node carries no age, and any other failure is passed on for the
// caller to log.
func waitAfterFailedRead(wait TerminatingResult, err error) TerminatingResult {
	if !apierrors.IsNotFound(err) {
		wait.NodeReadErr = err
	}
	return wait
}

// waitUntil moves the requeue to at when that comes before it.
func waitUntil(wait TerminatingResult, at time.Time) TerminatingResult {
	if !at.IsZero() && at.Before(wait.RequeueAt) {
		wait.RequeueAt = at
	}
	return wait
}

// NodeDeath is the node half of the force-delete predicate for a pod
// whose kubelet has had its time: a Terminating pod past its whole grace
// period plus the slack, or a pod its kubelet stopped reporting that
// holds a name a rebuild needs. Such a pod may go once its node is gone,
// or has been unreachable or NotReady for at least the threshold. A
// NotReady node may still run a kubelet. The rule rests on the time that
// kubelet already had, not on proof that it is gone. overdue is carried
// through for the caller's event text.
//
// Reads the Node through the LIVE reader (never the informer cache) so a
// stale cache can neither hold a recovered node "NotReady" nor miss a
// fresh recovery.
func NodeDeath(ctx context.Context, reader client.Reader, pod *corev1.Pod, policy *types.ForceDeletePolicy, now time.Time, overdue time.Duration) TerminatingResult {
	if pod.Spec.NodeName == "" {
		return TerminatingResult{Kind: Unscheduled, Overdue: overdue}
	}
	node := &corev1.Node{}
	if err := reader.Get(ctx, client.ObjectKey{Name: pod.Spec.NodeName}, node); err != nil {
		if apierrors.IsNotFound(err) {
			return TerminatingResult{Kind: NodeGone, NodeName: pod.Spec.NodeName, Overdue: overdue}
		}
		return TerminatingResult{Kind: NodeReadError, NodeName: pod.Spec.NodeName, Overdue: overdue, NodeReadErr: err}
	}
	kind, requeueAt := nodeDeathClass(node, now, policy.NodeUnreachableThreshold)
	return TerminatingResult{Kind: kind, NodeName: node.Name, RequeueAt: requeueAt, Overdue: overdue}
}

// NodeDeathScreened is NodeDeath for a pod a pass visits again and again
// while an operation stays open: the informer cache screens the node at
// no API cost, and the live Node decides. A cached Node that reports
// Ready, or whose signs are younger than the threshold, is the reading,
// with the recheck time the cache gives. Only a cached Node whose signs
// have come of age is read again live, so a lagging cache can delay the
// reading but never cause it. A node the cache lacks reads NodeGone with
// no live read: a missing Node carries no age for this reading to
// measure, and the missing-node sweep confirms such a node live itself.
func NodeDeathScreened(ctx context.Context, readers types.NodeReaders, pod *corev1.Pod, policy *types.ForceDeletePolicy, now time.Time) TerminatingResult {
	if pod.Spec.NodeName == "" {
		return TerminatingResult{Kind: Unscheduled}
	}
	// The cached copy is only read, so the cache hands it over uncopied.
	cached := &corev1.Node{}
	if err := readers.Cache.Get(ctx, client.ObjectKey{Name: pod.Spec.NodeName}, cached, client.UnsafeDisableDeepCopy); err != nil {
		if apierrors.IsNotFound(err) {
			return TerminatingResult{Kind: NodeGone, NodeName: pod.Spec.NodeName}
		}
		return TerminatingResult{Kind: NodeReadError, NodeName: pod.Spec.NodeName, NodeReadErr: err}
	}
	if kind, at := nodeDeathClass(cached, now, policy.NodeUnreachableThreshold); !kind.Actionable() {
		return TerminatingResult{Kind: kind, NodeName: cached.Name, RequeueAt: at}
	}
	return NodeDeath(ctx, readers.Live, pod, policy, now, 0)
}

// nodeDeathClass reads a live Node in three steps. A current Ready=True
// vetoes everything else, a lingering unreachable taint included: the
// kubelet writes NodeStatus itself, so a dead node cannot post it, and
// the veto only ever shrinks what may be force-deleted. Then each
// unreachable taint and each NotReady condition, in that order, is
// actionable once older than threshold. Otherwise the node is healthy,
// rechecked after one threshold, or not dead long enough, rechecked when
// the earliest dated sign comes of age (never, if no sign carries a time).
func nodeDeathClass(node *corev1.Node, now time.Time, threshold time.Duration) (TerminatingClass, time.Time) {
	if nodeReportsReady(node) {
		return NodeHealthy, now.Add(threshold)
	}
	signs := append(unreachableTaintSigns(node), notReadySigns(node)...)
	if len(signs) == 0 {
		return NodeHealthy, now.Add(threshold)
	}
	return datedSignsClass(signs, now, threshold)
}

// unreachableClass reads a Node for a pod still inside its grace period,
// the time its kubelet has to stop it. A kubelet that posts NodeReady,
// True or False, is running and can still use that time, so either
// status reads as healthy here. The signs that the kubelet stopped
// reporting are the unreachable taint and NodeReady Unknown. The outage
// counts from the most recent dated sign, so a taint left from an earlier
// outage cannot make a new one look old. A node that still reports asks
// for no recheck, so OME reads a later loss on the pod's next change, an
// operation's poll, or the end of the grace period.
func unreachableClass(node *corev1.Node, now time.Time, threshold time.Duration) (TerminatingClass, time.Time) {
	signs := unreachableTaintSigns(node)
	for _, cond := range node.Status.Conditions {
		if cond.Type != corev1.NodeReady {
			continue
		}
		if cond.Status != corev1.ConditionUnknown {
			return NodeHealthy, time.Time{}
		}
		signs = append(signs, nodeDeathSign{kind: NodeReadyUnknown, since: cond.LastTransitionTime.Time, dated: true})
	}
	if len(signs) == 0 {
		return NodeHealthy, time.Time{}
	}
	return datedSignsClass(newestDatedSign(signs), now, threshold)
}

// newestDatedSign keeps only the most recent dated sign, the first of
// equals, so the outage counts from it. When no sign carries a time, the
// signs are kept as they are.
func newestDatedSign(signs []nodeDeathSign) []nodeDeathSign {
	newest := -1
	for i, sign := range signs {
		if sign.dated && (newest < 0 || sign.since.After(signs[newest].since)) {
			newest = i
		}
	}
	if newest < 0 {
		return signs
	}
	return signs[newest : newest+1]
}

// datedSignsClass reads a node's signs at now: the class of the first
// one at least threshold old, else the not-yet-actionable class and the
// instant the earliest dated sign comes of age (zero if none is dated).
func datedSignsClass(signs []nodeDeathSign, now time.Time, threshold time.Duration) (TerminatingClass, time.Time) {
	var nextAt time.Time
	for _, sign := range signs {
		if !sign.dated {
			continue
		}
		actionableAt := sign.since.Add(threshold)
		if !now.Before(actionableAt) {
			return sign.kind, time.Time{}
		}
		if nextAt.IsZero() || actionableAt.Before(nextAt) {
			nextAt = actionableAt
		}
	}
	return NodeNotDeadLongEnough, nextAt
}

// nodeDeathSign is one sign that a node is unhealthy, an unreachable
// taint or a NodeReady that is not True: the class it gives once older
// than the threshold, and when it appeared. A taint without TimeAdded is
// undated: it rules out a healthy node but can never prove its age.
type nodeDeathSign struct {
	kind  TerminatingClass
	since time.Time
	dated bool
}

// nodeReportsReady reports a current NodeReady=True condition.
func nodeReportsReady(node *corev1.Node) bool {
	for _, cond := range node.Status.Conditions {
		if cond.Type == corev1.NodeReady && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// unreachableTaintSigns lists the node's unreachable taints, dated by the
// taint's own TimeAdded, in the order the node carries them.
func unreachableTaintSigns(node *corev1.Node) []nodeDeathSign {
	var signs []nodeDeathSign
	for _, taint := range node.Spec.Taints {
		if taint.Key != corev1.TaintNodeUnreachable {
			continue
		}
		sign := nodeDeathSign{kind: NodeUnreachableTaint}
		if taint.TimeAdded != nil {
			sign.since, sign.dated = taint.TimeAdded.Time, true
		}
		signs = append(signs, sign)
	}
	return signs
}

// notReadySigns lists the node's NodeReady False or Unknown conditions,
// dated by the condition's own LastTransitionTime.
func notReadySigns(node *corev1.Node) []nodeDeathSign {
	var signs []nodeDeathSign
	for _, cond := range node.Status.Conditions {
		if cond.Type != corev1.NodeReady {
			continue
		}
		if cond.Status == corev1.ConditionFalse || cond.Status == corev1.ConditionUnknown {
			signs = append(signs, nodeDeathSign{kind: NodeNotReady, since: cond.LastTransitionTime.Time, dated: true})
		}
	}
	return signs
}

// AllPodsTerminatingOverdue reports whether pods is non-empty and every
// pod in it is Terminating past its own DeletionTimestamp — the deadline
// the pod itself declared (its deletion request plus its own grace),
// which a live kubelet meets within moments of it. A set in that state
// is a teardown no further delete can move: the pods are already
// deleted, and only their kubelet, a finalizer owner or the force-delete
// escalation can remove them. No slack is applied, because nothing
// destructive hangs on the reading — a teardown grazing its deadline on
// a slow node reads as stalled for a pass and then completes.
func AllPodsTerminatingOverdue(pods []*corev1.Pod, now time.Time) bool {
	if len(pods) == 0 {
		return false
	}
	for _, pod := range pods {
		if pod == nil || pod.DeletionTimestamp == nil || !now.After(pod.DeletionTimestamp.Time) {
			return false
		}
	}
	return true
}

// SingleLiveNode returns the one node name hosting every pod of the
// instance (single-pod, or a single-host gang), or "" when there are no
// pods, any pod is unscheduled, or pods span multiple nodes — a
// multi-node attempt has no single suspect node to record, so it takes
// the terminal branch; the never-scheduled case has nothing to record.
func SingleLiveNode(pods []*corev1.Pod) string {
	if len(pods) == 0 {
		return ""
	}
	var observed string
	for _, pod := range pods {
		if pod == nil || pod.Spec.NodeName == "" {
			return ""
		}
		if observed == "" {
			observed = pod.Spec.NodeName
			continue
		}
		if observed != pod.Spec.NodeName {
			return ""
		}
	}
	return observed
}

// AttemptSuspectNode returns the node a failed attempt is steered off:
// SingleLiveNode over the attempt's own pods. A live pod of another
// revision is not the attempt's — a single-pod surge shares its Instance
// index with the source it replaces, and that source's node says nothing
// about where the replacement failed — so it neither names nor vetoes
// the suspect node. A pod on its way out, or one with no revision label,
// stays in scope: a recreate that drained its old pod is still failing
// on that pod, and without the label ownership cannot be told apart.
func AttemptSuspectNode(pods []*corev1.Pod, attemptRev string) string {
	own := make([]*corev1.Pod, 0, len(pods))
	for _, pod := range pods {
		if pod != nil && pod.DeletionTimestamp == nil && !podOnAttemptRevision(pod, attemptRev) {
			continue
		}
		own = append(own, pod)
	}
	return SingleLiveNode(own)
}

// podReadyWithdrawn reports the second shape of a pod its kubelet has
// stopped reporting, beside phase Unknown: a Running pod whose Ready
// condition is False while everything its kubelet last wrote says Ready
// — ContainersReady True and every readiness gate it declares satisfied.
// The kubelet derives Ready from exactly those inputs, so the combination
// has one other writer, the node lifecycle controller withdrawing Ready
// from every pod of a node that stopped heartbeating, and one innocent
// cause, the kubelet's own lag between a readiness-gate write and the
// Ready it folds it into. A draining pod is excluded by its gate and a
// probe failure by ContainersReady; the live Node read tells the two
// remaining cases apart.
func podReadyWithdrawn(pod *corev1.Pod) bool {
	if pod == nil || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	containersReady, podReady := false, false
	for _, cond := range pod.Status.Conditions {
		switch cond.Type {
		case corev1.ContainersReady:
			containersReady = cond.Status == corev1.ConditionTrue
		case corev1.PodReady:
			podReady = cond.Status == corev1.ConditionTrue
		}
	}
	return containersReady && !podReady && len(unsatisfiedReadinessGates(pod)) == 0
}

// UnreportedTargetPods returns the pods occupying one of the target
// names that have no report from the kubelet, lowest name first. A
// phase-Unknown pod always qualifies. A pod unreported only by its
// withdrawn Ready (podReadyWithdrawn) qualifies only under a force-delete
// policy: without one no Node is ever read, and only that read separates
// a dead node from the kubelet's lag behind a readiness-gate write. Name
// order — not the informer's map iteration order — decides which pod a
// hold's evidence names, so re-observing the same wait writes the same
// record.
func UnreportedTargetPods(pods []*corev1.Pod, targets map[string]struct{}, policy *types.ForceDeletePolicy) []*corev1.Pod {
	if len(pods) == 0 || len(targets) == 0 {
		return nil
	}
	var unreported []*corev1.Pod
	for _, pod := range pods {
		if !UnreportedPod(pod, policy) {
			continue
		}
		if _, ok := targets[pod.Name]; ok {
			unreported = append(unreported, pod)
		}
	}
	sort.Slice(unreported, func(i, j int) bool { return unreported[i].Name < unreported[j].Name })
	return unreported
}

// UnreportedPod reports a pod with no report from its kubelet, as
// UnreportedTargetPods reads each pod: one in phase Unknown, or, under a
// force-delete policy, one whose Ready the control plane took away while
// its own last report says Ready.
func UnreportedPod(pod *corev1.Pod, policy *types.ForceDeletePolicy) bool {
	return types.PodPhaseUnknown(pod) || (policy != nil && podReadyWithdrawn(pod))
}

// AnyUnreportedPod reports whether one of pods has no report from its
// kubelet. A caller that would otherwise name the pods it is looking for
// asks this first: with every pod reporting there is nothing to find.
func AnyUnreportedPod(pods []*corev1.Pod, policy *types.ForceDeletePolicy) bool {
	for _, pod := range pods {
		if UnreportedPod(pod, policy) {
			return true
		}
	}
	return false
}

// ReadUnreportedPod is the force-delete sweep's reading of a pod with no
// report from its kubelet, taken by the hold ahead of the sweep so a hold
// names only the pods the sweep leaves behind: node death for a pod
// nobody has asked to delete, the stuck-Terminating classification for
// one already on its way out, NotConfigured when there is no policy to
// read a Node under. A read error is returned rather than read as
// "kept": the sweep fails the pass on it too.
func ReadUnreportedPod(ctx context.Context, readers types.NodeReaders, pod *corev1.Pod, policy *types.ForceDeletePolicy, now time.Time) (TerminatingClass, error) {
	if policy == nil || pod == nil {
		return NotConfigured, nil
	}
	var res TerminatingResult
	if pod.DeletionTimestamp == nil {
		res = NodeDeath(ctx, readers.Live, pod, policy, now, 0)
	} else {
		res = StuckTerminating(ctx, readers, pod, policy, now)
	}
	if res.Kind == NodeReadError {
		return res.Kind, fmt.Errorf("node-death evidence for pod %s: node %s read: %w", pod.Name, res.NodeName, res.NodeReadErr)
	}
	return res.Kind, nil
}

// UnreportedPodHeld reports whether a pod the sweep does not free on this
// reading still holds its name against the rebuild. A phase-Unknown pod
// does whatever its node says: the kubelet itself stopped vouching for
// it. A pod unreported only by its withdrawn Ready holds the name while
// its node shows unreachable evidence or is on its way out; on a node
// posting Ready, or with no node at all, the shape is the kubelet's lag
// behind a readiness-gate write, and nothing waits on it.
func UnreportedPodHeld(pod *corev1.Pod, kind TerminatingClass) bool {
	if kind.Actionable() {
		return false
	}
	if types.PodPhaseUnknown(pod) {
		return true
	}
	switch kind {
	case NodeHealthy, Unscheduled:
		return false
	}
	return true
}

// PodNeverRanAContainer reports a pod its kubelet has run nothing of: it
// is Pending, not on its way out, and no container status, init or
// regular, shows a run that started, ended or restarted. Such a pod
// served no traffic and holds no state of its own, so removing its
// object costs only the create that puts the name on another node. A
// pod in any other phase, or one with a run on record, may still hold a
// process on its node and is read by the other readers here.
func PodNeverRanAContainer(pod *corev1.Pod) bool {
	if pod == nil || pod.DeletionTimestamp != nil ||
		(pod.Status.Phase != corev1.PodPending && pod.Status.Phase != "") {
		return false
	}
	for _, statuses := range [][]corev1.ContainerStatus{pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses} {
		for _, cs := range statuses {
			if cs.State.Running != nil || cs.State.Terminated != nil || cs.LastTerminationState.Terminated != nil ||
				cs.RestartCount > 0 || (cs.Started != nil && *cs.Started) {
				return false
			}
		}
	}
	return true
}

// OlderThanThreshold reports a sign on a Node that came of age: an
// unreachable taint or a NotReady condition older than the configured
// threshold. A Node object that is missing has no age to measure and is
// not one of these.
func (e TerminatingClass) OlderThanThreshold() bool {
	return e == NodeUnreachableTaint || e == NodeNotReady
}
