package coordination

import (
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/pkg/apis"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/utils"
)

// BuildGroupStatus produces a RolloutCoordinationGroupStatus snapshot
// from the (resolved group, computed transition, ratio state) tuple.
// Pure compute; the caller diffs against the previously-written status
// before issuing an Update.
func BuildGroupStatus(group ResolvedGroup, tr GroupTransition, ratio *RatioState, now time.Time) v1beta1.RolloutCoordinationGroupStatus {
	out := v1beta1.RolloutCoordinationGroupStatus{
		Name:               group.Name,
		Components:         append([]v1beta1.ComponentType(nil), group.Components...),
		Policy:             group.Policy,
		Phase:              tr.Phase,
		CompositePhase:     tr.CompositePhase,
		CurrentComponent:   tr.CurrentComponent,
		PreviousComponent:  tr.PreviousComponent,
		Message:            tr.Message,
		LastTransitionTime: &metav1.Time{Time: now},
	}
	if len(group.Order) > 0 {
		out.Order = append([]v1beta1.ComponentType(nil), group.Order...)
	}
	if group.Pacing.MaxSurge != nil {
		surge := *group.Pacing.MaxSurge
		out.ObservedSurge = &surge
	}
	if ratio != nil && group.Pacing.Type == v1beta1.CoordinationPacingRatioBalanced {
		out.ObservedRatio = ratioStateToAPI(ratio)
	}
	out.MemberNotReady = tr.MemberNotReady
	if !tr.MemberFinished.IsZero() {
		out.MemberFinished = &v1beta1.RolloutCoordinationMemberFinish{
			Component: tr.MemberFinished.Component,
			At:        metav1.NewTime(tr.MemberFinished.At),
		}
	}
	return out
}

// ratioStateToAPI converts the in-memory snapshot into the
// API-on-disk representation.
func ratioStateToAPI(r *RatioState) *v1beta1.RolloutCoordinationRatio {
	out := &v1beta1.RolloutCoordinationRatio{}
	if len(r.Original) > 0 {
		out.Original = make(map[v1beta1.ComponentType]int32, len(r.Original))
		for k, v := range r.Original {
			out.Original[k] = v
		}
	}
	if len(r.Current) > 0 {
		out.Current = make(map[v1beta1.ComponentType]int32, len(r.Current))
		for k, v := range r.Current {
			out.Current[k] = v
		}
	}
	if len(r.NewPods) > 0 {
		out.NewPods = make(map[v1beta1.ComponentType]int32, len(r.NewPods))
		for k, v := range r.NewPods {
			out.NewPods[k] = v
		}
	}
	return out
}

// MergeGroupStatus merges a freshly-computed group status into the
// previously-observed RolloutCoordinationStatus. The merge preserves
// LastTransitionTime when Phase didn't change, since the status is
// supposed to reflect when the phase last *transitioned*, not when
// the controller last looked at it. Prior groups are carried through
// untouched; pruning entries absent from the full fresh set is the
// caller's job (mergeAndPersistGroupStatuses), which sees every
// declared group at once.
func MergeGroupStatus(prev *v1beta1.RolloutCoordinationStatus, next v1beta1.RolloutCoordinationGroupStatus) *v1beta1.RolloutCoordinationStatus {
	out := &v1beta1.RolloutCoordinationStatus{}
	if prev != nil {
		out.Groups = append(out.Groups, prev.Groups...)
	}
	for i := range out.Groups {
		if out.Groups[i].Name != next.Name {
			continue
		}
		if out.Groups[i].Phase == next.Phase && out.Groups[i].LastTransitionTime != nil {
			next.LastTransitionTime = out.Groups[i].LastTransitionTime
		}
		out.Groups[i] = next
		return out
	}
	out.Groups = append(out.Groups, next)
	return out
}

// SetRolloutCoordinationReady stamps the InferenceServiceStatus
// condition `RolloutCoordinationReady` from the per-group phases.
// True when every group is Idle; False with Reason=InProgress when any
// group is mid-rollout; False with Reason=Failed when any group has
// Failed; Unknown with Reason=Pending when groups are declared but
// none have a phase yet.
func SetRolloutCoordinationReady(isvc *v1beta1.InferenceService, groups []v1beta1.RolloutCoordinationGroupStatus, now time.Time) {
	if isvc == nil || len(groups) == 0 {
		return
	}
	cond := ComputeCoordinationReady(groups)
	if cond == nil {
		return
	}
	isvc.Status.SetCondition(apis.ConditionType(v1beta1.RolloutCoordinationReady), cond)
}

// ClearRolloutCoordinationReady resolves the RolloutCoordinationReady condition
// when an ISVC no longer has any coordination-style group — e.g. it switched to
// a canary-only rollout or dropped the group. Without this, a condition left at
// False/InProgress by a prior rollout stays stuck forever, because
// SetRolloutCoordinationReady only runs when groups are present (and contradicts
// a healthy canary). Acts only when the condition already exists, so an ISVC
// that never coordinated does not gain a spurious condition.
func ClearRolloutCoordinationReady(isvc *v1beta1.InferenceService) {
	if isvc == nil {
		return
	}
	if isvc.Status.GetCondition(apis.ConditionType(v1beta1.RolloutCoordinationReady)) == nil {
		return
	}
	isvc.Status.SetCondition(apis.ConditionType(v1beta1.RolloutCoordinationReady), &apis.Condition{
		Type:   apis.ConditionType(v1beta1.RolloutCoordinationReady),
		Status: corev1.ConditionTrue,
		Reason: "NoCoordinationGroups",
	})
}

// SetCoordinationAdvisory stamps the CoordinationAdvisory status condition
// (Status=True) when an ISVC has >=2 OMENative Components but no
// spec.rollout.coordination group. The condition is the "already advised"
// memory: callers emit the one-shot ConsiderCoordinationGroup event only
// when this returns true (the transition into the advisory state), never
// every reconcile. Returns false (no event, no status write) when the
// condition is already True, so the steady state neither re-emits the event
// nor re-PATCHes status.
func SetCoordinationAdvisory(isvc *v1beta1.InferenceService, components []v1beta1.ComponentType) bool {
	if isvc == nil {
		return false
	}
	existing := isvc.Status.GetCondition(apis.ConditionType(v1beta1.CoordinationAdvisory))
	if existing != nil && existing.Status == corev1.ConditionTrue {
		return false
	}
	isvc.Status.SetCondition(apis.ConditionType(v1beta1.CoordinationAdvisory), &apis.Condition{
		Type:   apis.ConditionType(v1beta1.CoordinationAdvisory),
		Status: corev1.ConditionTrue,
		Reason: "NoCoordinationGroup",
		Message: fmt.Sprintf(
			"%d OMENative Components (%v) roll independently; declare a spec.rollout.coordination group to coordinate them",
			len(components), components),
	})
	return true
}

// ClearCoordinationAdvisory resolves the CoordinationAdvisory condition once
// the ISVC no longer warrants it — a coord group was declared, or it dropped
// below 2 coordinated Components. Acts only when the condition exists and is
// not already resolved, so it neither adds a spurious condition to an ISVC
// that never tripped the advisory nor re-PATCHes status every reconcile.
// Resolving re-arms the one-shot event: if the group is later removed, the
// advisory (and its event) fires again.
func ClearCoordinationAdvisory(isvc *v1beta1.InferenceService) {
	if isvc == nil {
		return
	}
	existing := isvc.Status.GetCondition(apis.ConditionType(v1beta1.CoordinationAdvisory))
	if existing == nil || existing.Status == corev1.ConditionFalse {
		return
	}
	isvc.Status.SetCondition(apis.ConditionType(v1beta1.CoordinationAdvisory), &apis.Condition{
		Type:   apis.ConditionType(v1beta1.CoordinationAdvisory),
		Status: corev1.ConditionFalse,
		Reason: "CoordinationDeclaredOrNotApplicable",
	})
}

// ComputeCoordinationReady is the pure decision step used by
// SetRolloutCoordinationReady. Returns nil when no condition write is
// warranted (no groups declared).
func ComputeCoordinationReady(groups []v1beta1.RolloutCoordinationGroupStatus) *apis.Condition {
	if len(groups) == 0 {
		return nil
	}
	anyFailed := false
	anyMidRollout := false
	anyPending := false
	anyStaged := false
	failedGroups := make([]string, 0)
	midRolloutGroups := make([]string, 0)
	for _, g := range groups {
		switch g.Phase {
		case v1beta1.CoordinationPhaseFailed:
			anyFailed = true
			failedGroups = append(failedGroups, g.Name)
		case v1beta1.CoordinationPhaseIdle, v1beta1.CoordinationPhaseStaged, "":
			// Staged is a terminal RESTING state (converged to a static
			// partition, holding old pods by design) — ready, like Idle.
			if g.Phase == "" {
				anyPending = true
			}
			if g.Phase == v1beta1.CoordinationPhaseStaged {
				anyStaged = true
			}
		default:
			anyMidRollout = true
			midRolloutGroups = append(midRolloutGroups, g.Name+":"+string(g.Phase))
		}
	}
	switch {
	case anyFailed:
		return &apis.Condition{
			Type:    apis.ConditionType(v1beta1.RolloutCoordinationReady),
			Status:  corev1.ConditionFalse,
			Reason:  "Failed",
			Message: "rollout coordination failed in groups: " + strings.Join(failedGroups, ","),
		}
	case anyMidRollout:
		return &apis.Condition{
			Type:    apis.ConditionType(v1beta1.RolloutCoordinationReady),
			Status:  corev1.ConditionFalse,
			Reason:  "InProgress",
			Message: "rollout coordination in flight: " + strings.Join(midRolloutGroups, ","),
		}
	case anyPending:
		return &apis.Condition{
			Type:    apis.ConditionType(v1beta1.RolloutCoordinationReady),
			Status:  corev1.ConditionUnknown,
			Reason:  "Pending",
			Message: "rollout coordination not yet observed",
		}
	default:
		reason := "Idle"
		if anyStaged {
			reason = "Staged"
		}
		return &apis.Condition{
			Type:   apis.ConditionType(v1beta1.RolloutCoordinationReady),
			Status: corev1.ConditionTrue,
			Reason: reason,
		}
	}
}

// MaxSurgeBudget converts the group's pacing MaxSurge (IntOrString)
// into the absolute pod count budget for one Component's surge
// step, given its desired replica count. Strings are interpreted as
// percentages of replicas, rounded up. A nil MaxSurge defaults to 25%
// (the value pacingWithDefaults fills in; this fallback only matters for
// callers that bypass it). Delegates the int/percent math to
// utils.ScaledCountFromIntOrString.
func MaxSurgeBudget(pacing v1beta1.CoordinationPacing, replicas int32) int32 {
	if pacing.MaxSurge == nil {
		return utils.ScaledCountFromIntOrString(utils.PtrIntOrStringFromString("25%"), replicas, true)
	}
	return utils.ScaledCountFromIntOrString(pacing.MaxSurge, replicas, true)
}

// MaxUnavailableBudget converts the group's pacing MaxUnavailable
// (IntOrString) into the absolute pod count, given replicas. Mirror
// of MaxSurgeBudget except a nil MaxUnavailable yields 0 (no drain
// headroom) rather than a 25% default. Percent strings round up.
func MaxUnavailableBudget(pacing v1beta1.CoordinationPacing, replicas int32) int32 {
	if pacing.MaxUnavailable == nil {
		return 0
	}
	return utils.ScaledCountFromIntOrString(pacing.MaxUnavailable, replicas, true)
}
