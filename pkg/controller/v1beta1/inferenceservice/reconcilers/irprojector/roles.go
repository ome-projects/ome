package irprojector

import (
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/validation"
)

// roleOrder is the order the roles are walked: the engine first, since every
// service has one, then decoder and router.
var roleOrder = []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent}

// RoleReplicaRef returns the standalone InferenceReplica spec.replicaRefs
// names for a role, or "" when the service renders the role itself or does
// not serve it.
func RoleReplicaRef(isvc *v1beta1.InferenceService, c v1beta1.ComponentType) string {
	return validation.ReferencedReplica(&isvc.Spec, c)
}

// ReferencesReplicas reports whether the service fronts replicas it
// references instead of projecting its own; such a service renders nothing.
func ReferencesReplicas(isvc *v1beta1.InferenceService) bool {
	return RoleReplicaRef(isvc, v1beta1.EngineComponent) != ""
}

// ReferencedRoles returns the roles the service serves through referenced
// replicas, engine first.
func ReferencedRoles(isvc *v1beta1.InferenceService) []v1beta1.ComponentType {
	var roles []v1beta1.ComponentType
	for _, c := range roleOrder {
		if RoleReplicaRef(isvc, c) != "" {
			roles = append(roles, c)
		}
	}
	return roles
}

// RoleDeclared reports whether the service serves a role, through its own
// spec block or through a referenced replica. The ingress and external
// Service reconcilers route to the declared roles.
func RoleDeclared(isvc *v1beta1.InferenceService, c v1beta1.ComponentType) bool {
	if RoleReplicaRef(isvc, c) != "" {
		return true
	}
	switch c {
	case v1beta1.EngineComponent:
		return isvc.Spec.Engine != nil
	case v1beta1.DecoderComponent:
		return isvc.Spec.Decoder != nil
	case v1beta1.RouterComponent:
		return isvc.Spec.Router != nil
	}
	return false
}

// RoleReplicaName returns the name of the InferenceReplica behind a role:
// the referenced replica when the service names one, else the replica the
// service projects.
func RoleReplicaName(isvc *v1beta1.InferenceService, c v1beta1.ComponentType) string {
	if name := RoleReplicaRef(isvc, c); name != "" {
		return name
	}
	return InferenceReplicaName(isvc.Name, c)
}

// RoleReplicaKey returns the object key of the InferenceReplica behind a
// role. A replica always lives in its service's namespace.
func RoleReplicaKey(isvc *v1beta1.InferenceService, c v1beta1.ComponentType) types.NamespacedName {
	return types.NamespacedName{Namespace: isvc.Namespace, Name: RoleReplicaName(isvc, c)}
}

// RoleReplicaPrefix returns the ome.io/inferenceservice label value of a
// role's pods, which is its replica's name prefix: the referenced replica's
// own name, else the service's. The role's per-revision Services and
// ControllerRevisions derive their names from it, so every per-revision
// object the service names or selects for a role is keyed on it.
func RoleReplicaPrefix(isvc *v1beta1.InferenceService, c v1beta1.ComponentType) string {
	if name := RoleReplicaRef(isvc, c); name != "" {
		return name
	}
	return isvc.Name
}
