package isvc

import (
	"fmt"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"sigs.k8s.io/ome/pkg/validation"
)

// validateMultiPodReadyPolicyNone rejects writes that put
// lifecycle.readyPolicy=None on a Component whose effective shape at
// admission time is multi-pod OMENative. Instance readiness for those
// Components is always the AllPodReady aggregation — per-pod readiness
// reporting is not supported — so admitting None would silently behave
// like AllPodReady. Single-pod Components keep accepting None (identical
// to AllPodReady at size 1), and shapes that only become multi-pod
// through runtime resolution are not checked here: the check considers
// only the Leader+Worker pair the InferenceService itself declares.
//
// The check ratchets on UPDATE: a Component is rejected only when the
// violation is newly introduced by this write. A stored object already
// violating for that Component keeps admitting writes so it continues to
// reconcile. A nil oldIsvc (CREATE) rejects every violating Component.
func validateMultiPodReadyPolicyNone(oldIsvc, isvc *v1beta1.InferenceService) error {
	violations := multiPodReadyPolicyNoneComponents(isvc)
	if len(violations) == 0 {
		return nil
	}
	tolerated := map[string]bool{}
	if oldIsvc != nil {
		for _, name := range multiPodReadyPolicyNoneComponents(oldIsvc) {
			tolerated[name] = true
		}
	}
	for _, name := range violations {
		if tolerated[name] {
			continue
		}
		return fmt.Errorf(
			"%s.lifecycle.readyPolicy %q is not allowed on a multi-pod OMENative component: per-pod readiness reporting is not yet supported; set readyPolicy to %q or remove the field",
			name, v1beta1.InstanceReadyPolicyNone, v1beta1.InstanceReadyPolicyAllPodReady)
	}
	return nil
}

// multiPodReadyPolicyNoneComponents returns the names of Components that
// set lifecycle.readyPolicy=None while resolving to a multi-pod OMENative
// shape from the InferenceService spec alone (a declared Leader+Worker
// pair with positive Worker.Size). It reads every declared role in the
// order of v1beta1.Roles, the order a refusal names them; only a role
// whose block declares a Leader+Worker pair can qualify, so the encoder
// and the router, single pod by their API, are never named. The
// service-level mode is resolved as the controller resolves it
// (annotation, then spec.deploymentMode, then the declared shape), so the
// Components rejected here are exactly the ones the workload engine runs
// as AllPodReady.
func multiPodReadyPolicyNoneComponents(isvc *v1beta1.InferenceService) []string {
	serviceMode := isvcutils.InferenceServiceDeploymentMode(isvc, "")
	var names []string
	for _, role := range v1beta1.ServiceRoles(&isvc.Spec).Declared() {
		if validation.DeclaredMultiPod(role.Leader, role.Worker) &&
			componentReadyPolicyIsNone(role.Extension) &&
			componentResolvesToOMENative(role.Extension.Annotations, serviceMode) {
			names = append(names, string(role.Type))
		}
	}
	return names
}

// componentReadyPolicyIsNone reports whether the Component explicitly
// sets lifecycle.readyPolicy=None.
func componentReadyPolicyIsNone(ext *v1beta1.ComponentExtensionSpec) bool {
	return ext.Lifecycle != nil && ext.Lifecycle.ReadyPolicy != nil &&
		*ext.Lifecycle.ReadyPolicy == v1beta1.InstanceReadyPolicyNone
}

// componentResolvesToOMENative reports whether a Component dispatches to
// the OMENative backend: the per-Component ome.io/deploymentMode annotation
// wins, then the service-level mode; an empty mode names no backend.
func componentResolvesToOMENative(annotations map[string]string, serviceMode constants.DeploymentModeType) bool {
	if annotations[constants.DeploymentMode] != "" {
		return annotations[constants.DeploymentMode] == string(constants.OMENative)
	}
	return serviceMode == constants.OMENative
}
