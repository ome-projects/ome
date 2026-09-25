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
// stuck-teardown force-delete predicate. Exactly three values are
// actionable; everything else leaves the pod alone.
type TerminatingClass int

const (
	// NotConfigured: no ForceDeletePolicy — the escalation does
	// not exist.
	NotConfigured TerminatingClass = iota
	// WithinGrace: the pod is inside its own graceful-deletion
	// window (plus the configured slack).
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
)

// Actionable reports whether the evidence justifies a force-delete.
func (e TerminatingClass) Actionable() bool {
	switch e {
	case NodeGone, NodeUnreachableTaint, NodeNotReady:
		return true
	}
	return false
}

// String names the evidence branch for events and logs.
func (e TerminatingClass) String() string {
	switch e {
	case NotConfigured:
		return "not-configured"
	case WithinGrace:
		return "within-grace"
	case ForeignFinalizers:
		return "foreign-finalizers"
	case Unscheduled:
		return "unscheduled"
	case NodeReadError:
		return "node-read-error"
	case NodeHealthy:
		return "node-healthy"
	case NodeNotDeadLongEnough:
		return "node-not-dead-long-enough"
	case NodeGone:
		return "node-gone"
	case NodeUnreachableTaint:
		return "node-unreachable-taint"
	case NodeNotReady:
		return "node-not-ready"
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
	// Overdue is how far past the pod's own DeletionTimestamp now is.
	Overdue time.Duration
	// NodeReadErr carries the non-NotFound Node read failure when Kind
	// is NodeReadError, for the caller to log.
	NodeReadErr error
}

// StuckTerminating is the PURE classification behind the
// force-delete escalation. Safety contract, branch by branch:
//
//	NotConfigured        — nil policy: the escalation does not exist; no
//	                       node is ever read. Acting would be acting on
//	                       an in-code default, which this feature bans.
//	WithinGrace          — now <= DeletionTimestamp+OverdueSlack. The
//	                       pod's DeletionTimestamp ALREADY includes its
//	                       own terminationGracePeriodSeconds (the k8s
//	                       contract: deletionTimestamp = request time +
//	                       grace), so a 10-minute-drain pod gets its 10
//	                       minutes and a 30s pod gets 30s — that is why
//	                       the check is per-pod. Acting earlier would
//	                       race a healthy graceful shutdown.
//	ForeignFinalizers    — force-delete cannot remove a finalizer-pinned
//	                       object, and stripping another controller's
//	                       finalizer is off-limits. Report-only.
//	Unscheduled          — no NodeName: there is no node whose death
//	                       could prove the container isn't running.
//	NodeReadError        — the live read failed; without evidence the
//	                       fail-safe is to do nothing this pass.
//	NodeHealthy          — Ready=True, evaluated FIRST and vetoing every
//	                       node-death branch below, including a lingering
//	                       unreachable taint: the kubelet writes
//	                       NodeStatus itself, so a dead node cannot post
//	                       Ready=True — a current Ready=True always
//	                       outranks stale taint state, and the veto can
//	                       only shrink the fire surface, never grow it.
//	                       A Ready node means the kubelet is merely slow;
//	                       force-deleting risks a double-running
//	                       container. Never actionable.
//	NodeNotDeadLongEnough— taint/NotReady present but younger than the
//	                       threshold (or age unprovable): a blip, not a
//	                       death. Wait.
//	NodeGone             — Node object deleted: no kubelet exists to run
//	                       the container or acknowledge the termination.
//	                       Safe to remove the API object.
//	NodeUnreachableTaint — the node-lifecycle controller has marked the
//	                       node unreachable for >= threshold (age from
//	                       the taint's own TimeAdded — no observation
//	                       state to persist, restart-safe).
//	NodeNotReady         — NodeReady False/Unknown for >= threshold (age
//	                       from the condition's own LastTransitionTime).
//
// Every branch from Unscheduled down is NodeDeath's.
func StuckTerminating(ctx context.Context, reader client.Reader, pod *corev1.Pod, policy *types.ForceDeletePolicy, now time.Time) TerminatingResult {
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
		return TerminatingResult{Kind: WithinGrace, RequeueAt: overdueAt.Add(time.Nanosecond)}
	}
	overdue := now.Sub(pod.DeletionTimestamp.Time)
	if len(pod.Finalizers) > 0 {
		return TerminatingResult{Kind: ForeignFinalizers, NodeName: pod.Spec.NodeName, Overdue: overdue}
	}
	return NodeDeath(ctx, reader, pod, policy, now, overdue)
}

// NodeDeath is the node half of the force-delete predicate: does
// the cluster prove that no kubelet is left to run pod's containers. It
// is shared by every caller that may only remove a pod object once the
// node under it is provably gone — the stuck-Terminating sweep, and the
// rebuild paths whose target name is held by a pod its node has stopped
// reporting. overdue is carried through for the caller's event text.
//
// Reads the Node through the LIVE reader (never the informer cache) so
// a stale cache can neither hold a recovered node "NotReady" nor miss a
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

	// Ready=True vetoes everything below, including a lingering
	// unreachable taint: the kubelet writes NodeStatus itself, so a dead
	// node cannot post Ready=True — a current Ready=True always outranks
	// stale taint state. The veto can only shrink the fire surface,
	// never grow it.
	for _, cond := range node.Status.Conditions {
		if cond.Type == corev1.NodeReady && cond.Status == corev1.ConditionTrue {
			return TerminatingResult{
				Kind:      NodeHealthy,
				NodeName:  node.Name,
				RequeueAt: now.Add(policy.NodeUnreachableThreshold),
				Overdue:   overdue,
			}
		}
	}

	// Evidence present but younger than the threshold — remembered so
	// the fall-through distinguishes "dying, wait" from "healthy".
	young := false
	var nextEvidenceAt time.Time
	for _, taint := range node.Spec.Taints {
		if taint.Key != corev1.TaintNodeUnreachable {
			continue
		}
		// Age comes from the taint's own TimeAdded; a nil TimeAdded
		// cannot prove the threshold elapsed.
		if taint.TimeAdded != nil {
			actionableAt := taint.TimeAdded.Add(policy.NodeUnreachableThreshold)
			if !now.Before(actionableAt) {
				return TerminatingResult{Kind: NodeUnreachableTaint, NodeName: node.Name, Overdue: overdue}
			}
			if nextEvidenceAt.IsZero() || actionableAt.Before(nextEvidenceAt) {
				nextEvidenceAt = actionableAt
			}
		}
		young = true
	}
	for _, cond := range node.Status.Conditions {
		if cond.Type != corev1.NodeReady {
			continue
		}
		if cond.Status == corev1.ConditionFalse || cond.Status == corev1.ConditionUnknown {
			actionableAt := cond.LastTransitionTime.Add(policy.NodeUnreachableThreshold)
			if !now.Before(actionableAt) {
				return TerminatingResult{Kind: NodeNotReady, NodeName: node.Name, Overdue: overdue}
			}
			if nextEvidenceAt.IsZero() || actionableAt.Before(nextEvidenceAt) {
				nextEvidenceAt = actionableAt
			}
			young = true
		}
	}
	if young {
		res := TerminatingResult{Kind: NodeNotDeadLongEnough, NodeName: node.Name, Overdue: overdue}
		if !nextEvidenceAt.IsZero() {
			res.RequeueAt = nextEvidenceAt
		}
		return res
	}
	return TerminatingResult{
		Kind:      NodeHealthy,
		NodeName:  node.Name,
		RequeueAt: now.Add(policy.NodeUnreachableThreshold),
		Overdue:   overdue,
	}
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

// UnknownPhaseTargetPods returns the pods in phase Unknown occupying one
// of the target names, lowest name first. Name order — not the
// informer's map iteration order — decides which pod a hold's evidence
// names, so re-observing the same wait writes the same record.
func UnknownPhaseTargetPods(pods []*corev1.Pod, targets map[string]struct{}) []*corev1.Pod {
	if len(pods) == 0 || len(targets) == 0 {
		return nil
	}
	var quiet []*corev1.Pod
	for _, pod := range pods {
		if !types.PodPhaseUnknown(pod) {
			continue
		}
		if _, ok := targets[pod.Name]; ok {
			quiet = append(quiet, pod)
		}
	}
	sort.Slice(quiet, func(i, j int) bool { return quiet[i].Name < quiet[j].Name })
	return quiet
}

// SweepFreesUnknownPod reports whether the force-delete sweep will free
// the name a phase-Unknown pod holds on this pass. It is the sweep's own
// reading — node death for a pod nobody has asked to delete, the
// stuck-Terminating classification for one already on its way out — so
// a hold recorded ahead of the sweep names only the pods the sweep
// leaves behind. A read error is returned rather than read as "kept":
// the sweep fails the pass on it too.
func SweepFreesUnknownPod(ctx context.Context, reader client.Reader, pod *corev1.Pod, policy *types.ForceDeletePolicy, now time.Time) (bool, error) {
	if policy == nil || pod == nil {
		return false, nil
	}
	var res TerminatingResult
	if pod.DeletionTimestamp == nil {
		res = NodeDeath(ctx, reader, pod, policy, now, 0)
	} else {
		res = StuckTerminating(ctx, reader, pod, policy, now)
	}
	if res.Kind == NodeReadError {
		return false, fmt.Errorf("node-death evidence for pod %s: node %s read: %w", pod.Name, res.NodeName, res.NodeReadErr)
	}
	return res.Kind.Actionable(), nil
}
