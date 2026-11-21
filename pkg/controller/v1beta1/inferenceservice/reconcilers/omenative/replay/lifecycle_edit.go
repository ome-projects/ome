package replay

import (
	"context"
	"fmt"
	"slices"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// A lifecycle edit is the operator's patch of one field of a Component's
// lifecycle block, on a settled service or in the middle of a roll.
// spec.strategy rewrites lifecycle.updateStrategy.type and
// spec.restartPolicy rewrites lifecycle.restartPolicy. Each writes its one
// field, as a merge patch does: the budgets and the partition the Component
// declares stay beside a new strategy type. The template does not move, so
// the write mints no revision and moves the service's generation alone. The
// projection of the tick's pass carries the field to that Component's
// replica and to no other.

// unsetLifecycleField is how a trace names a lifecycle field the Component
// did not declare before the edit.
const unsetLifecycleField = "unset"

// updateStrategyTypes are the update strategy types the API admits.
var updateStrategyTypes = []v1beta1.UpdateStrategyType{
	v1beta1.UpdateStrategySurgeThenDrain,
	v1beta1.UpdateStrategyRecreatePod,
	v1beta1.UpdateStrategyInPlaceIfPossible,
	v1beta1.UpdateStrategyInPlaceOnly,
}

// restartPolicies are the restart policies the API admits.
var restartPolicies = []v1beta1.InstanceRestartPolicy{
	v1beta1.InstanceRestartPolicyNone,
	v1beta1.InstanceRestartPolicyRecreateInstance,
}

// validateLifecycleEdit refuses, when the scenario loads, a lifecycle edit
// that names a variant, no Component, or a value the API does not admit.
func validateLifecycleEdit(scenario string, tick int, ev TimelineEvent) error {
	if ev.Variant != "" {
		return fmt.Errorf("replay: %s tick %d: %s takes no variant", scenario, tick, ev.ID)
	}
	if ev.Args.Component == "" {
		return fmt.Errorf("replay: %s tick %d: %s needs a component", scenario, tick, ev.ID)
	}
	switch ev.ID {
	case StrategyEdit:
		if !slices.Contains(updateStrategyTypes, v1beta1.UpdateStrategyType(ev.Args.Strategy)) {
			return fmt.Errorf("replay: %s tick %d: %s: strategy %q is not one of %v", scenario, tick, ev.ID, ev.Args.Strategy, updateStrategyTypes)
		}
	case RestartPolicyEdit:
		if !slices.Contains(restartPolicies, v1beta1.InstanceRestartPolicy(ev.Args.Policy)) {
			return fmt.Errorf("replay: %s tick %d: %s: policy %q is not one of %v", scenario, tick, ev.ID, ev.Args.Policy, restartPolicies)
		}
	}
	return nil
}

// editedComponent is the declaration a lifecycle edit rewrites: the named
// Component's, while it runs in the OMENative mode. A Component that left
// the mode has no replica to carry its lifecycle.
func (d *driver) editedComponent(ev TimelineEvent) (*member, ComponentState, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return nil, ComponentState{}, err
	}
	if d.departed[m.component] {
		return nil, ComponentState{}, fmt.Errorf("replay: %s: %s has left the OMENative mode, so no replica carries its lifecycle", ev.Key(), m.component)
	}
	return m, d.service.Components[string(m.component)], nil
}

// applyStrategyEdit rewrites the update strategy type a Component declares
// and leaves the rest of its strategy as it stands. An edit to the type the
// Component already declares would write nothing, so it is refused.
func applyStrategyEdit(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, cs, err := d.editedComponent(ev)
	if err != nil {
		return "", err
	}
	to := v1beta1.UpdateStrategyType(ev.Args.Strategy)
	if !slices.Contains(updateStrategyTypes, to) {
		return "", fmt.Errorf("replay: %s: strategy %q is not one of %v", ev.Key(), ev.Args.Strategy, updateStrategyTypes)
	}
	from := unsetLifecycleField
	if cs.UpdateStrategy == nil {
		cs.UpdateStrategy = &v1beta1.UpdateStrategy{}
	} else if cs.UpdateStrategy.Type != "" {
		from = string(cs.UpdateStrategy.Type)
	}
	if from == string(to) {
		return "", fmt.Errorf("replay: %s: %s already declares the strategy %s, and an edit that changes nothing writes nothing", ev.Key(), m.component, to)
	}
	cs.UpdateStrategy.Type = to
	d.service.Components[string(m.component)] = cs
	generation, err := d.writeSpec(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("component=%s strategy=%s→%s generation=%d", m.component, from, to, generation), nil
}

// applyRestartPolicyEdit rewrites the restart policy a Component declares.
// An edit to the policy the Component already declares would write nothing,
// so it is refused.
func applyRestartPolicyEdit(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, cs, err := d.editedComponent(ev)
	if err != nil {
		return "", err
	}
	to := v1beta1.InstanceRestartPolicy(ev.Args.Policy)
	if !slices.Contains(restartPolicies, to) {
		return "", fmt.Errorf("replay: %s: policy %q is not one of %v", ev.Key(), ev.Args.Policy, restartPolicies)
	}
	from := unsetLifecycleField
	if cs.RestartPolicy != nil {
		from = string(*cs.RestartPolicy)
	}
	if from == string(to) {
		return "", fmt.Errorf("replay: %s: %s already declares the restart policy %s, and an edit that changes nothing writes nothing", ev.Key(), m.component, to)
	}
	cs.RestartPolicy = &to
	d.service.Components[string(m.component)] = cs
	generation, err := d.writeSpec(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("component=%s restartPolicy=%s→%s generation=%d", m.component, from, to, generation), nil
}
