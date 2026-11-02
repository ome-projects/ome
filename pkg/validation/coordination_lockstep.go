// RollingUpdate update-time compare (spec.rollout.groups[]).
//
// A rollingUpdate group's members may change one at a time; the update-time
// warning names the members that keep their revision. The compare therefore
// has to detect ANY change that mints a new workload revision for a Component
// — not just an image bump. This file holds the revision-affecting view of a
// Component's ISVC spec and the change predicate
// rollingUpdatePartialChangeWarning consumes.
package validation

import (
	"k8s.io/apimachinery/pkg/api/equality"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// componentRevisionSpec is the subset of one Component's ISVC spec that shapes
// its rendered pod template — and therefore its workload revision hash. The
// pod spec, runner/leader/worker overrides, gang topology key, router config,
// and the pod-template labels/annotations all land on the rendered template;
// changing any of them rolls the Component exactly like an image bump.
//
// Scaling, pacing, and autoscaling fields (replica bounds, timeout, PDB
// budgets, deployment strategy, lifecycle, autoscaler, service port
// protocols) are deliberately absent: they change behavior without
// re-rendering pods, so a change to them is not a change of the member.
type componentRevisionSpec struct {
	PodSpec     *v1beta1.PodSpec
	Runner      *v1beta1.RunnerSpec
	Leader      *v1beta1.LeaderSpec
	Worker      *v1beta1.WorkerSpec
	TopologyKey *string
	Config      map[string]string
	Labels      map[string]string
	Annotations map[string]string
}

// componentRevisionSpecFor extracts the revision-affecting view for one
// Component. nil when the Component is not declared on the spec, so
// add/remove of a whole Component also reads as a change.
func componentRevisionSpecFor(spec *v1beta1.InferenceServiceSpec, c v1beta1.ComponentType) *componentRevisionSpec {
	role := declaredRoles(spec).find(c)
	if role == nil {
		return nil
	}
	return &componentRevisionSpec{
		PodSpec:     role.podSpec,
		Runner:      role.runner,
		Leader:      role.leader,
		Worker:      role.worker,
		TopologyKey: role.topologyKey,
		Config:      role.routerConfig,
		Labels:      role.extension.Labels,
		Annotations: role.extension.Annotations,
	}
}

// componentRevisionSpecChanged reports whether the Component's
// revision-affecting spec differs between the old and new ISVC specs.
// Semantic equality so resource quantities compare by value ("1" == "1000m").
func componentRevisionSpecChanged(oldSpec, newSpec *v1beta1.InferenceServiceSpec, c v1beta1.ComponentType) bool {
	return !equality.Semantic.DeepEqual(
		componentRevisionSpecFor(oldSpec, c),
		componentRevisionSpecFor(newSpec, c))
}
