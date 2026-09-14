package snapshot

import (
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// DefaultNodeSuspicionWindow is the recovery-quarantine window used by a
// zero-valued snapshot Options. Alfred's production config has the same safe
// default and passes its validated value explicitly.
const DefaultNodeSuspicionWindow = 30 * time.Minute

// Quarantined reports whether a node must be excluded from placement targets.
// The zero value is clear for synthetic-snapshot compatibility; unrecognized
// future states fail closed.
func (h NodeHealthObservation) Quarantined() bool {
	return h.State != "" && h.State != NodeHealthClear
}

func observeNodeHealth(
	conditions []corev1.NodeCondition,
	triggerConditions []string,
	now time.Time,
	suspicionWindow time.Duration,
) NodeHealthObservation {
	result := NodeHealthObservation{State: NodeHealthClear}
	configured := make(map[corev1.NodeConditionType]struct{}, len(triggerConditions))
	for _, conditionType := range triggerConditions {
		configured[corev1.NodeConditionType(conditionType)] = struct{}{}
	}

	var unhealthy bool
	var unknown bool
	var suspectUntil *time.Time
	for _, condition := range conditions {
		if _, ok := configured[condition.Type]; !ok {
			continue
		}
		transitioned := condition.LastTransitionTime.Time
		result.Conditions = append(result.Conditions, NodeConditionObservation{
			Type:               condition.Type,
			Status:             condition.Status,
			LastTransitionTime: transitioned,
		})

		switch condition.Status {
		case corev1.ConditionTrue:
			unhealthy = true
		case corev1.ConditionUnknown:
			unknown = true
		case corev1.ConditionFalse:
			if transitioned.IsZero() {
				unknown = true
				continue
			}
			deadline := transitioned.Add(suspicionWindow)
			if now.Before(deadline) && (suspectUntil == nil || deadline.After(*suspectUntil)) {
				suspectUntil = &deadline
			}
		default:
			unknown = true
		}
	}

	sort.Slice(result.Conditions, func(i, j int) bool {
		left, right := result.Conditions[i], result.Conditions[j]
		if left.Type != right.Type {
			return left.Type < right.Type
		}
		if left.Status != right.Status {
			return left.Status < right.Status
		}
		return left.LastTransitionTime.Before(right.LastTransitionTime)
	})

	switch {
	case unhealthy:
		result.State = NodeHealthUnhealthy
	case unknown:
		result.State = NodeHealthUnknown
	case suspectUntil != nil:
		result.State = NodeHealthSuspect
		result.SuspectUntil = suspectUntil
	}
	return result
}
