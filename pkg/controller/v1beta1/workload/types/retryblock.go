package types

import (
	"math"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Workload-side mirror of the InferenceReplica RetryBlock status. The
// adapter converts field-for-field; workload code never sees the CRD
// type. Revision-scoped: one block per target revision.
type RetryBlockState string

const (
	RetryBlockBackoff         RetryBlockState = "Backoff"
	RetryBlockHeld            RetryBlockState = "Held"
	RetryBlockRetryInProgress RetryBlockState = "RetryInProgress"
)

type RetryBlock struct {
	TargetRevision  string
	State           RetryBlockState
	AttemptsStarted int32
	NextRetryAt     *metav1.Time
	FirstFailureAt  *metav1.Time
	LastFailureAt   *metav1.Time
	Reason          string
}

// RetryBlockDisposition is the MutateRetryBlock callback's verdict.
type RetryBlockDisposition int

const (
	RetryBlockUnchanged RetryBlockDisposition = iota // no write
	RetryBlockPersist                                // upsert the mutated block
	RetryBlockRemove                                 // delete the block (success prune)
)

// RetryPolicy is the operator's retry ladder: it bounds automatic
// same-target update retries and paces a parked repair's re-arms.
// Config-driven (chart values → inferenceservice-config); nil means
// unconfigured and fails safe: Exhausted is always true, so the first
// failure Holds and no parked repair re-arms.
type RetryPolicy struct {
	MaxAttempts  int32
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64
}

// NextRetryDelay returns the backoff before attempt attemptsStarted+1:
// InitialDelay * Multiplier^(attemptsStarted-1), capped at MaxDelay.
func (p *RetryPolicy) NextRetryDelay(attemptsStarted int32) time.Duration {
	d := time.Duration(float64(p.InitialDelay) * math.Pow(p.Multiplier, float64(attemptsStarted-1)))
	if d > p.MaxDelay || d <= 0 {
		return p.MaxDelay
	}
	return d
}

// Exhausted reports whether no automatic attempts remain. A nil policy
// (unconfigured) is always exhausted — fail-safe Held.
func (p *RetryPolicy) Exhausted(attemptsStarted int32) bool {
	if p == nil {
		return true
	}
	return attemptsStarted >= p.MaxAttempts
}

// FindRetryBlock returns a pointer to the block for targetRevision
// (aliasing the slice element), or nil.
func FindRetryBlock(blocks []RetryBlock, targetRevision string) *RetryBlock {
	for i := range blocks {
		if blocks[i].TargetRevision == targetRevision {
			return &blocks[i]
		}
	}
	return nil
}
