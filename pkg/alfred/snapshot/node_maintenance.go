package snapshot

import (
	"sort"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/ome/pkg/alfred/config"
)

// ObserveNodeMaintenance evaluates configured planned-work signals against the
// current Node at time now. The returned sorted trigger identities also let
// event handlers distinguish actionable changes from heartbeat and unrelated
// metadata updates.
func ObserveNodeMaintenance(node *corev1.Node, triggers []config.MaintenanceTrigger, now time.Time) NodeMaintenanceObservation {
	result := NodeMaintenanceObservation{}
	for _, trigger := range triggers {
		if maintenanceTriggerMatches(node, trigger, now) {
			result.Triggers = append(result.Triggers, trigger.Name)
		}
	}
	sort.Strings(result.Triggers)
	result.Requested = len(result.Triggers) != 0
	return result
}

func maintenanceTriggerMatches(node *corev1.Node, trigger config.MaintenanceTrigger, now time.Time) bool {
	if match := trigger.Condition; match != nil {
		for _, condition := range node.Status.Conditions {
			if condition.Type == match.Type && condition.Status == match.Status {
				return true
			}
		}
	}
	if match := trigger.Label; match != nil {
		value, present := node.Labels[match.Key]
		if !present {
			return false
		}
		if match.ValueIsStartTime {
			return startTimeReached(value, now)
		}
		return match.Value == nil || value == *match.Value
	}
	if match := trigger.Taint; match != nil {
		for _, taint := range node.Spec.Taints {
			if taint.Key == match.Key && (match.Value == nil || taint.Value == *match.Value) && (match.Effect == "" || taint.Effect == match.Effect) {
				return true
			}
		}
	}
	return false
}

// startTimeReached reports whether now is at or after the Unix time, in whole
// seconds, written in value. A value that is not a whole number counts as
// reached: the key alone already asks for maintenance, and the time may only
// delay it.
func startTimeReached(value string, now time.Time) bool {
	seconds, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return true
	}
	// Compare whole seconds: time.Unix overflows for values near the int64
	// limit and would turn a far-future time into a past one.
	return now.Unix() >= seconds
}
