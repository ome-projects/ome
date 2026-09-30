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
