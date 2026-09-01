package constants

import "time"

const (
	// OMENativeExecutorCapabilityRenewInterval is the manager heartbeat
	// cadence for the InferenceReplica executor capability Lease.
	OMENativeExecutorCapabilityRenewInterval = 10 * time.Second
	// OMENativeExecutorCapabilityMinStaleness leaves three heartbeat windows
	// before Alfred treats an executor capability Lease as stale.
	OMENativeExecutorCapabilityMinStaleness = 3 * OMENativeExecutorCapabilityRenewInterval
)
