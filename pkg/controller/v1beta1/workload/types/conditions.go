package types

// ConditionType / ConditionReason are workload-internal identifiers
// stamped on metav1.Condition entries. Values match the legacy
// omenative/status strings byte-for-byte so operator dashboards keep
// matching.
type ConditionType string

func (t ConditionType) String() string { return string(t) }

type ConditionReason string

func (r ConditionReason) String() string { return string(r) }

const (
	// ConditionGangSchedulingUnavailable is True when the Component
	// has at least one multi-pod Instance but the scheduler-plugins
	// PodGroup CRD is missing. The reconciler still creates pods —
	// gang scheduling is a soft requirement so workloads proceed
	// without blocking — but partial-gang placement is possible and
	// the runtime may hang.
	ConditionGangSchedulingUnavailable ConditionType = "GangSchedulingUnavailable"

	// ConditionMigrationPolicyUnconfigured is True when a migration
	// request is waiting because the operator configured no migration
	// capacity policy, so the pass has no bound to judge it against. It
	// reports the last capacity judgment made for a pending request:
	// the pass that judges one under a configured policy sets it False.
	ConditionMigrationPolicyUnconfigured ConditionType = "MigrationPolicyUnconfigured"

	// ConditionInstanceReadyTimeoutUnconfigured is True when neither the
	// Component's spec.lifecycle.instanceReadyTimeout nor the operator's
	// lifecycle.instanceReadyTimeout supplies a readiness backstop, so every
	// operation the Component opens runs with no deadline and an Instance
	// that never becomes Ready waits instead of being failed.
	ConditionInstanceReadyTimeoutUnconfigured ConditionType = "InstanceReadyTimeoutUnconfigured"
)

const (
	// ReasonPodGroupCRDNotInstalled stamps the
	// GangSchedulingUnavailable condition when the
	// scheduler-plugins PodGroup CRD is missing.
	ReasonPodGroupCRDNotInstalled ConditionReason = "PodGroupCRDNotInstalled"
	// ReasonGangSchedulingAvailable stamps Status=False (CRD present,
	// or Component is single-pod).
	ReasonGangSchedulingAvailable ConditionReason = "GangSchedulingAvailable"
	// ReasonMigrationCapacityUnconfigured stamps the
	// MigrationPolicyUnconfigured condition when a request waits for a
	// capacity policy the operator has not supplied.
	ReasonMigrationCapacityUnconfigured ConditionReason = "MigrationCapacityUnconfigured"
	// ReasonMigrationCapacityConfigured stamps Status=False: the pass
	// judged a request against the operator's caps.
	ReasonMigrationCapacityConfigured ConditionReason = "MigrationCapacityConfigured"
	// ReasonInstanceReadyTimeoutUnconfigured stamps the
	// InstanceReadyTimeoutUnconfigured condition when no readiness backstop
	// is supplied at either level.
	ReasonInstanceReadyTimeoutUnconfigured ConditionReason = "InstanceReadyTimeoutUnconfigured"
	// ReasonInstanceReadyTimeoutConfigured stamps Status=False: an effective
	// readiness window is in force for the Component.
	ReasonInstanceReadyTimeoutConfigured ConditionReason = "InstanceReadyTimeoutConfigured"
)
