// Package placement implements the multi-cluster control-plane fan-out controller:
// it places an InferenceService onto a workload cluster and mirrors its status.
package placement

import "sigs.k8s.io/ome/pkg/constants"

// PlacementControllerName names the fan-out placement controller — explicit so
// it doesn't collide with other InferenceService-watching controllers on the
// derived name "inferenceservice".
const PlacementControllerName = "placement"

const (
	// AcceleratorRequirementsAnnotation identifies obsolete placement intent.
	AcceleratorRequirementsAnnotation = constants.AcceleratorRequirements
	// ClusterSelectorAnnotation identifies obsolete placement intent.
	ClusterSelectorAnnotation = constants.ClusterSelector

	// PlacementFinalizer lets the controller delete the derived ISVC before the
	// source ISVC is removed.
	PlacementFinalizer = "ome.io/placement"

	// EndpointFinalizer coordinates endpoint and placement teardown. The endpoint
	// publisher removes it only after publication is gone; placement retains
	// derived workloads while it remains so routing never outlives its backends.
	EndpointFinalizer = "ome.io/placement-endpoint"
)

// LocalQueueAnnotation names the Kueue LocalQueue the derived workload's
// pods join on the target cluster. It overrides the operator-configured
// queue; with neither set, no queue label is stamped.
var LocalQueueAnnotation = constants.LocalQueue

// The placement provenance markers, aliased from pkg/constants so the revision
// layer can exclude them from the pod-template hash without importing this
// package. Vars rather than consts because the shared definitions are vars.
var (
	// PlacementOriginLabel marks a derived ISVC as created by this control
	// plane (for tracking + future GC). Value: the source ISVC's origin id.
	PlacementOriginLabel = constants.PlacementOrigin
	// PlacementOriginUIDAnnotation records the source ISVC UID on the derived ISVC.
	PlacementOriginUIDAnnotation = constants.PlacementOriginUID
	// PlacementControlPlaneLabel records WHICH control plane created a derived
	// ISVC. Its value is the operator-supplied control-plane identity
	// (config-driven via --placement-control-plane-id / the chart, no in-code
	// default). When multiple control planes share a workload cluster, the GC
	// sweep filters on this label so each control plane only reaps its OWN
	// deriveds and never another's. Empty identity degrades gracefully: nothing
	// is stamped and the GC keeps its single-control-plane (origin-UID-only)
	// behavior.
	PlacementControlPlaneLabel = constants.PlacementControlPlane
)

// controlPlaneOnlyAnnotations are ome.io directives that drive the CONTROL
// plane's placement decision and must NOT ride along onto the derived ISVC,
// where the worker cluster's reconciler would (re)interpret them: the
// placement selectors (candidate selection is the control plane's job), the
// traffic-drain override (one shared cross-cluster routing decision), the
// rollout operator-verbs (promote/rollback/repin/resume advance one shared
// rollout, owned by the control plane — a copy on every candidate would each
// consume the verb, and the source's still-live copy would re-derive it onto
// the member on the next sync, replaying it forever), and the rollout
// plan-source provenance (system-authored during derive-time policy
// inflation; a user-supplied value on the source must never masquerade as
// control-plane provenance on the derived). Stripped by DeriveISVC;
// inflation re-authors the plan-source entry afterwards.
//
// Nothing forwards these hub→member, and the hub runs no rollout of its own,
// so on a placed InferenceService a verb takes effect only when applied
// directly to the member object.
var controlPlaneOnlyAnnotations = []string{
	AcceleratorRequirementsAnnotation,
	ClusterSelectorAnnotation,
	constants.TrafficDrainAnnotation,
	constants.RolloutPromoteAnnotation,
	constants.RolloutPromoteForceAnnotation,
	constants.RolloutRollbackAnnotation,
	constants.RolloutRepinAnnotation,
	constants.RolloutResumeAnnotation,
	constants.RolloutPlanSourceAnnotation,
}

// controlPlaneOnlyAnnotationPrefixes are the bookkeeping families of whatever
// deploys the SOURCE object. A derived copy belongs to the placer.
//
// The hazard is ownership, not rollout: ArgoCD reads tracking-id to decide
// which application owns a resource, so copying the source's reassigns the
// member's object to an application on another cluster that does not manage
// it. (No rollout — an ISVC-level annotation is object-scoped and excluded
// from the revision hash; see TemplateMeta in workload/revision.)
//
// A prefix, not the one key: ArgoCD also writes sync-options, compare-options
// and sync-wave, equally meaningless on a derived object.
var controlPlaneOnlyAnnotationPrefixes = []string{
	"argocd.argoproj.io/",
}
