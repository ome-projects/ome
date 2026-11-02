package validation

import (
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// declaredRole is one Component a spec declares, with the fields the rules
// read off it. A rule that walks the roles ranges over a roleList instead of
// testing spec.Engine, spec.Decoder and spec.Router one after the other.
type declaredRole struct {
	kind v1beta1.ComponentType
	// field is the path that declares the role on its object, as a refusal
	// names it: "spec.engine" on a service, "engineConfig" on a runtime.
	field        string
	extension    *v1beta1.ComponentExtensionSpec
	podSpec      *v1beta1.PodSpec
	runner       *v1beta1.RunnerSpec
	leader       *v1beta1.LeaderSpec
	worker       *v1beta1.WorkerSpec
	topologyKey  *string
	routerConfig map[string]string
}

// deploymentModeAnnotation is the role's own deploymentMode annotation,
// empty when the role does not set one.
func (r *declaredRole) deploymentModeAnnotation() string {
	return r.extension.Annotations[constants.DeploymentMode]
}

// ComponentRoles lists the roles a service can declare, in the order every
// rule walks them and every refusal names them: the order of v1beta1.Roles.
var ComponentRoles = v1beta1.RoleTypes()

// roleList holds the declared roles in the order every rule walks them: the
// order of v1beta1.Roles.
type roleList []declaredRole

// find returns the declared role of kind c, nil when the spec does not
// declare it.
func (l roleList) find(c v1beta1.ComponentType) *declaredRole {
	for i := range l {
		if l[i].kind == c {
			return &l[i]
		}
	}
	return nil
}

// declaredRoles lists the roles an InferenceService spec declares.
func declaredRoles(spec *v1beta1.InferenceServiceSpec) roleList {
	return rolesOf(v1beta1.ServiceRoles(spec), func(r v1beta1.Role) string { return r.ServiceField })
}

// runtimeRoles lists the roles a ServingRuntime spec declares defaults for.
func runtimeRoles(spec *v1beta1.ServingRuntimeSpec) roleList {
	return rolesOf(v1beta1.RuntimeRoles(spec), func(r v1beta1.Role) string { return r.RuntimeField })
}

// rolesOf lists the declared roles of specs, each named by the field that
// declares it on its object.
func rolesOf(specs v1beta1.RoleSpecs, field func(v1beta1.Role) string) roleList {
	var roles roleList
	for _, r := range v1beta1.Roles {
		view := specs.View(r.Type)
		if view == nil {
			continue
		}
		roles = append(roles, declaredRole{
			kind:         r.Type,
			field:        field(r),
			extension:    view.Extension,
			podSpec:      view.PodSpec,
			runner:       view.Runner,
			leader:       view.Leader,
			worker:       view.Worker,
			topologyKey:  view.TopologyKey,
			routerConfig: view.RouterConfig,
		})
	}
	return roles
}
