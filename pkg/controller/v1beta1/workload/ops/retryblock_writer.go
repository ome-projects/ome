package ops

import (
	"context"
	"fmt"

	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// recordUpdateFailureInRetryBlock delegates to the shared writer in the
// types package (workload.RecordUpdateFailureInRetryBlock) — one
// implementation for the gang abandon here and the workload-root
// deadline disposition. Kept as a package-local name so the ops-side
// call sites and invariant tests read unchanged.
func recordUpdateFailureInRetryBlock(ctx context.Context, input workload.ReconcileInput, targetRev, reason string, workloadCaused bool) error {
	return workload.RecordUpdateFailureInRetryBlock(ctx, input, targetRev, reason, workloadCaused)
}

// pruneRetryBlockOnPromote removes the RetryBlock for rev once the
// subject converges there (RunningRevision promoted to rev) — a
// converged subject leaves no active block. No-op when the adapter did
// not wire MutateRetryBlock or rev is empty.
func pruneRetryBlockOnPromote(ctx context.Context, input workload.ReconcileInput, rev string) error {
	if input.MutateRetryBlock == nil || rev == "" {
		return nil
	}
	if err := input.MutateRetryBlock(ctx, rev, func(_ *workload.RetryBlock) workload.RetryBlockDisposition {
		return workload.RetryBlockRemove
	}); err != nil {
		return fmt.Errorf("prune retry block (rev=%s): %w", rev, err)
	}
	return nil
}

// instanceFailureReason summarizes the failure evidence the escalators
// already stamped on the instance (LastFailure) for RetryBlock.Reason:
// the kubelet detail message when present, else the compact termination
// fragment, else fallback.
func instanceFailureReason(s *workload.InstanceStatus, fallback string) string {
	if s == nil || s.LastFailure == nil {
		return fallback
	}
	if s.LastFailure.Message != "" {
		return s.LastFailure.Message
	}
	if short := s.LastFailure.ShortString(); short != "" {
		return short
	}
	return fallback
}

// instanceFailureWorkloadCaused reports whether the failure evidence the
// escalators stamped on the instance (LastFailure.Reason) blames the
// revision itself. Only that evidence charges the revision's retry
// ladder; an elapsed deadline or an ambiguous kubelet reason paces the
// next attempt without counting toward Held.
func instanceFailureWorkloadCaused(s *workload.InstanceStatus) bool {
	return s != nil && s.LastFailure != nil && workload.IsWorkloadCausedReason(s.LastFailure.Reason)
}
