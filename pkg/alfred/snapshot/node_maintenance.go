package snapshot

import (
	"sort"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/ome/pkg/alfred/config"
)

// ObserveNodeMaintenance evaluates configured planned-work signals against the
// current Node. The returned sorted trigger identities also let event handlers
// distinguish actionable changes from heartbeat and unrelated metadata updates.
func ObserveNodeMaintenance(node *corev1.Node, triggers []config.MaintenanceTrigger) NodeMaintenanceObservation {
	result := NodeMaintenanceObservation{}
	for _, trigger := range triggers {
		if maintenanceTriggerMatches(node, trigger) {
			result.Triggers = append(result.Triggers, trigger.Name)
		}
	}
	sort.Strings(result.Triggers)
	result.Requested = len(result.Triggers) != 0
	return result
}

func maintenanceTriggerMatches(node *corev1.Node, trigger config.MaintenanceTrigger) bool {
	if match := trigger.Condition; match != nil {
		for _, condition := range node.Status.Conditions {
			if condition.Type == match.Type && condition.Status == match.Status {
				return true
			}
		}
	}
	if match := trigger.Label; match != nil {
		value, present := node.Labels[match.Key]
		return present && (match.Value == nil || value == *match.Value)
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
