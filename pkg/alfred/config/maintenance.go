package config

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

// Maintenance configures externally supplied planned-maintenance signals.
type Maintenance struct {
	Triggers []MaintenanceTrigger `json:"triggers"`
}

// MaintenanceTrigger has a stable unique name and exactly one matcher.
// Triggers are ORed; all fields within a matcher must match the same signal.
type MaintenanceTrigger struct {
	Name      string                `json:"name"`
	Condition *MaintenanceCondition `json:"condition,omitempty"`
	Label     *MaintenanceLabel     `json:"label,omitempty"`
	Taint     *MaintenanceTaint     `json:"taint,omitempty"`
}

// MaintenanceCondition matches an explicit condition type and status.
type MaintenanceCondition struct {
	Type   corev1.NodeConditionType `json:"type"`
	Status corev1.ConditionStatus   `json:"status"`
}

// MaintenanceLabel matches a label key. A nil value accepts any present value;
// a pointer to an empty string matches only an explicitly empty label value.
type MaintenanceLabel struct {
	Key   string  `json:"key"`
	Value *string `json:"value,omitempty"`
}

// MaintenanceTaint matches one taint. An absent value or effect is unrestricted.
type MaintenanceTaint struct {
	Key    string             `json:"key"`
	Value  *string            `json:"value,omitempty"`
	Effect corev1.TaintEffect `json:"effect,omitempty"`
}

func (n *NodeHealth) validateTriggers() error {
	seenConditions := map[string]bool{}
	for _, name := range n.TriggerConditions {
		if len(validation.IsQualifiedName(name)) != 0 || seenConditions[name] {
			return fmt.Errorf("policies.nodeHealth.triggerConditions: invalid or duplicate condition %q", name)
		}
		seenConditions[name] = true
	}
	seenNames := map[string]bool{}
	for i, trigger := range n.Maintenance.Triggers {
		path := fmt.Sprintf("policies.nodeHealth.maintenance.triggers[%d]", i)
		if strings.TrimSpace(trigger.Name) == "" || strings.TrimSpace(trigger.Name) != trigger.Name || seenNames[trigger.Name] {
			return fmt.Errorf("%s.name must be nonempty and unique: %q", path, trigger.Name)
		}
		seenNames[trigger.Name] = true
		count := 0
		if trigger.Condition != nil {
			count++
		}
		if trigger.Label != nil {
			count++
		}
		if trigger.Taint != nil {
			count++
		}
		if count != 1 {
			return fmt.Errorf("%s must set exactly one of condition, label, or taint", path)
		}
		if condition := trigger.Condition; condition != nil {
			if len(validation.IsQualifiedName(string(condition.Type))) != 0 {
				return fmt.Errorf("%s.condition.type is invalid: %q", path, condition.Type)
			}
			if condition.Status != corev1.ConditionTrue && condition.Status != corev1.ConditionFalse && condition.Status != corev1.ConditionUnknown {
				return fmt.Errorf("%s.condition.status must be True, False, or Unknown", path)
			}
		}
		if label := trigger.Label; label != nil {
			if err := validateMaintenanceKeyValue(path+".label", label.Key, label.Value); err != nil {
				return err
			}
		}
		if taint := trigger.Taint; taint != nil {
			if err := validateMaintenanceKeyValue(path+".taint", taint.Key, taint.Value); err != nil {
				return err
			}
			switch taint.Effect {
			case "", corev1.TaintEffectNoSchedule, corev1.TaintEffectPreferNoSchedule, corev1.TaintEffectNoExecute:
			default:
				return fmt.Errorf("%s.taint.effect is invalid: %q", path, taint.Effect)
			}
		}
	}
	return nil
}

func validateMaintenanceKeyValue(path, key string, value *string) error {
	if len(validation.IsQualifiedName(key)) != 0 {
		return fmt.Errorf("%s.key is invalid: %q", path, key)
	}
	if value != nil && len(validation.IsValidLabelValue(*value)) != 0 {
		return fmt.Errorf("%s.value is invalid: %q", path, *value)
	}
	return nil
}
