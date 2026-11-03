package constants

// Obsolete placement annotation keys are retained for migration validation and
// removal from derived services. They must not silently become local metadata.
const (
	// PlacementExecution carries allocation authority on an owned member copy.
	PlacementExecution = "ome.io/placement-execution"
	// PlacementPolicy carries the source policy across derived-spec stripping.
	PlacementPolicy         = "ome.io/placement-policy"
	AcceleratorRequirements = "ome.io/accelerator-requirements"
	ClusterSelector         = "ome.io/cluster-selector"
)

// PlacementReleaseCleanupWaitAnnotation is the operator request that ends the
// cleanup wait of one placement plan revision. While a plan pauses member
// growth, a finished move waits until the members' replicas, counting replicas
// reserved for rolling updates, fit within the plan's minimums. Autoscalers
// that keep more replicas can hold that wait indefinitely. Value: the plan
// revision to release, copied from status.placement.plan.revision, as a
// positive whole number. The revision guards against a stale request: a value
// that names any other revision does nothing.
//
// The request skips only that cleanup wait. A move that has not finished keeps
// waiting, and the request stays until the move finishes at the same revision.
// The controller removes the annotation once the named revision's pause has
// ended, and at once when the value names another revision or a plan that does
// not pause, or is not a whole number. An event says which, or that the
// removal failed.
//
// It is control-plane-only: set it on the source InferenceService. It is
// stripped at derive and left out of the plan's input digest, so setting or
// removing it changes no member and no plan revision.
var PlacementReleaseCleanupWaitAnnotation = OMEAPIGroupName + "/placement-release-cleanup-wait"
