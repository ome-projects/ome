package isvc

import (
	"fmt"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
)

// ReasonDeploymentModeChangeUnderOpenRun ends the refusal of a write that
// moves a Component off the OMENative backend while a rollout run is open
// on it.
const ReasonDeploymentModeChangeUnderOpenRun = "DeploymentModeChangeUnderOpenRun"

// validateDeploymentModeChangeUnderOpenRun refuses a write that moves a
// Component off the OMENative backend while the service's open rollout run
// names that Component. The run's plan waits for the Component's replica
// to reach the run's target; replacing the backend deletes that replica
// and clears the Component's revision record, so the run would wait on a
// Component that no longer runs under it. The operator lets the run finish
// and changes the mode afterwards. A Component the run does not name, and
// a service with no run open, change backend as before. The open run is
// read from the stored object's status; a Component absent from either
// spec is left to the rules that own Component removal.
func validateDeploymentModeChangeUnderOpenRun(oldIsvc, isvc *v1beta1.InferenceService) error {
	if !v1beta1.RolloutRunActive(oldIsvc) {
		return nil
	}
	run := oldIsvc.Status.Rollout.ActiveRun
	for _, component := range runComponents(run) {
		before, wasDeclared := componentDispatchMode(&oldIsvc.Spec, component)
		after, isDeclared := componentDispatchMode(&isvc.Spec, component)
		if !wasDeclared || !isDeclared || before != constants.OMENative || after == constants.OMENative {
			continue
		}
		return fmt.Errorf("%s cannot leave OMENative while rollout run %s is open on it: this write would run %s as %s; wait for the run to finish, or finish it yourself (promote or roll back a canary), then change the mode (%s)",
			component, run.RunID, component, after, ReasonDeploymentModeChangeUnderOpenRun)
	}
	return nil
}

// runComponents lists the Components the run's plan names, in plan order
// and once each.
func runComponents(run *v1beta1.RolloutRun) []v1beta1.ComponentType {
	var out []v1beta1.ComponentType
	seen := map[v1beta1.ComponentType]bool{}
	for _, group := range run.Plan.Groups {
		for _, component := range group.Group.Components {
			if seen[component] {
				continue
			}
			seen[component] = true
			out = append(out, component)
		}
	}
	return out
}

// componentDispatchMode resolves the backend a declared role runs under
// with the rule the controller dispatches by (the role's annotation, then
// spec.deploymentMode, then a leader and worker shape); the flag is false
// for a role the spec does not declare.
func componentDispatchMode(spec *v1beta1.InferenceServiceSpec, component v1beta1.ComponentType) (constants.DeploymentModeType, bool) {
	view := v1beta1.SpecOf(spec, component)
	if view == nil {
		return "", false
	}
	return isvcutils.RoleDeploymentMode(view, spec.DeploymentMode), true
}
