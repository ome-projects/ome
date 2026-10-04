package validation

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// replicaRefRoles is the order the referenced-form rules walk the roles.
var replicaRefRoles = []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent}

// ReplicaRefsField returns the spec path of a role's list in spec.replicaRefs.
func ReplicaRefsField(c v1beta1.ComponentType) string {
	return "spec.replicaRefs." + string(c)
}

// roleReplicaRefs returns the names spec.replicaRefs carries for a role.
func roleReplicaRefs(refs *v1beta1.ReplicaRefs, c v1beta1.ComponentType) []string {
	if refs == nil {
		return nil
	}
	switch c {
	case v1beta1.EngineComponent:
		return refs.Engine
	case v1beta1.DecoderComponent:
		return refs.Decoder
	case v1beta1.RouterComponent:
		return refs.Router
	}
	return nil
}

// ReferencedReplica returns the standalone InferenceReplica spec.replicaRefs
// names for a role, or "" when the role is not referenced. The referenced
// form is in effect only when it names an engine: a block without one is
// ignored as a whole, so a stored block admission never saw cannot apply to
// some roles and not others.
func ReferencedReplica(spec *v1beta1.InferenceServiceSpec, c v1beta1.ComponentType) string {
	if spec == nil || spec.ReplicaRefs == nil || len(spec.ReplicaRefs.Engine) == 0 {
		return ""
	}
	if names := roleReplicaRefs(spec.ReplicaRefs, c); len(names) > 0 {
		return names[0]
	}
	return ""
}

// ProjectedBy returns the name of the InferenceService that projects a
// replica, or "" for a standalone one: a projected replica carries the
// service as parentRef and as its controller owner, and either alone marks
// it as not the user's to reference.
func ProjectedBy(ir *v1beta1.InferenceReplica) string {
	if parent := ir.ParentName(); parent != "" {
		return parent
	}
	if owner := metav1.GetControllerOf(ir); owner != nil && owner.Kind == "InferenceService" && owner.APIVersion == v1beta1.SchemeGroupVersion.String() {
		return owner.Name
	}
	return ""
}

// ValidateReplicaRefs checks the shape of spec.replicaRefs: the engine is
// named (a service has an engine), one entry per role, each entry has the
// form of a standalone replica's name (the prefix of its pods and Services),
// no replica serves two roles, and nothing rides along that the referenced
// form cannot honor: no inline role, no rollout, no model or runtime (the
// replicas carry their own), no scaling policy or accelerator selection (the
// service writes nothing on, and renders nothing for, a referenced replica),
// no placement or routing (the replicas exist in this cluster only), no
// deployment mode other than OMENative. The live checks (the replica exists,
// is standalone, serves the role, is fronted by no other service) run in the
// webhook.
func ValidateReplicaRefs(spec *v1beta1.InferenceServiceSpec) error {
	if spec == nil || spec.ReplicaRefs == nil {
		return nil
	}
	if len(spec.ReplicaRefs.Engine) == 0 {
		return fmt.Errorf("%s is required when spec.replicaRefs is set; a service has an engine", ReplicaRefsField(v1beta1.EngineComponent))
	}
	named := map[string]string{}
	for _, c := range replicaRefRoles {
		field := ReplicaRefsField(c)
		names := roleReplicaRefs(spec.ReplicaRefs, c)
		if len(names) > 1 {
			return fmt.Errorf("%s names %d replicas; one entry per role is accepted", field, len(names))
		}
		for _, name := range names {
			if err := ValidateInferenceServiceName(name); err != nil {
				return fmt.Errorf("%s[0] %q must match %q", field, name, IsvcNameFmt)
			}
			if prev, dup := named[name]; dup {
				return fmt.Errorf("InferenceReplica %s is named by both %s and %s; a replica serves one role", name, prev, field)
			}
			named[name] = field
		}
	}
	for _, inline := range []struct {
		field string
		set   bool
	}{{"spec.engine", spec.Engine != nil}, {"spec.decoder", spec.Decoder != nil}, {"spec.router", spec.Router != nil}} {
		if inline.set {
			return fmt.Errorf("%s and spec.replicaRefs cannot both be set; a service references all of its roles or none", inline.field)
		}
	}
	if spec.Rollout != nil {
		return fmt.Errorf("spec.rollout is not accepted with spec.replicaRefs; a referenced replica rolls on its own")
	}
	const (
		ownModelAndRuntime = "the referenced replicas carry their own model and runtime"
		nothingRendered    = "the service writes nothing on, and renders nothing for, a referenced replica"
		thisClusterOnly    = "the referenced replicas exist in this cluster only"
	)
	for _, f := range []struct {
		field  string
		set    bool
		reason string
	}{
		{"spec.model", spec.Model != nil, ownModelAndRuntime},
		{"spec.runtime", spec.Runtime != nil, ownModelAndRuntime},
		{"spec.scalingPolicy", spec.ScalingPolicy != nil, nothingRendered},
		{"spec.acceleratorSelector", spec.AcceleratorSelector != nil, nothingRendered},
		{"spec.placement", spec.Placement != nil, thisClusterOnly},
		{"spec.routing", spec.Routing != nil, thisClusterOnly},
	} {
		if f.set {
			return fmt.Errorf("%s is not accepted with spec.replicaRefs; %s", f.field, f.reason)
		}
	}
	if spec.DeploymentMode != nil && *spec.DeploymentMode != constants.OMENative {
		return fmt.Errorf("spec.deploymentMode %s is not accepted with spec.replicaRefs; a referenced replica is OMENative", *spec.DeploymentMode)
	}
	return nil
}

// ValidateReplicaRefsUpdate keeps spec.replicaRefs as it was at create. The
// Services and the route in front of the replicas are named for the service,
// so fronting other replicas is a new service: delete and recreate.
func ValidateReplicaRefsUpdate(oldSpec, newSpec *v1beta1.InferenceServiceSpec) error {
	if equality.Semantic.DeepEqual(oldSpec.ReplicaRefs, newSpec.ReplicaRefs) {
		return nil
	}
	return fmt.Errorf("spec.replicaRefs is fixed at create; delete and recreate the InferenceService to front other replicas")
}
