package observer

import (
	"context"
	"strings"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
)

const (
	omenativeFutureSkew = 5 * time.Second

	omenativeReasonLeaseAbsent        = "lease-absent"
	omenativeReasonLeaseUnreadable    = "lease-unreadable"
	omenativeReasonLeaseDeleting      = "lease-deleting"
	omenativeReasonHolderMissing      = "holder-missing"
	omenativeReasonRenewTimeMissing   = "renew-time-missing"
	omenativeReasonRenewTimeFuture    = "renew-time-future"
	omenativeReasonLeaseStale         = "lease-stale"
	omenativeReasonSchemaMissing      = "schema-missing"
	omenativeReasonSchemaIncompatible = "schema-incompatible"
	omenativeReasonReady              = "ready"
)

// OMENativeExecutorReader checks the direct, uncached executor capability
// Lease. It performs exactly one named Get per observation pass.
type OMENativeExecutorReader struct {
	Reader client.Reader
	Now    func() time.Time
}

// Read returns a structured, payload-free availability observation using the
// same immutable config generation as the surrounding snapshot build.
func (r *OMENativeExecutorReader) Read(ctx context.Context, cfg *config.Config) snapshot.OMENativeExecutorState {
	if r.Reader == nil || cfg == nil {
		return snapshot.OMENativeExecutorState{Reason: omenativeReasonLeaseUnreadable}
	}

	lease := &coordinationv1.Lease{}
	key := client.ObjectKey{
		Namespace: cfg.OMENativeCapabilityLeaseNamespace,
		Name:      cfg.OMENativeCapabilityLeaseName,
	}
	if err := r.Reader.Get(ctx, key, lease); err != nil {
		if apierrors.IsNotFound(err) {
			return snapshot.OMENativeExecutorState{Reason: omenativeReasonLeaseAbsent}
		}
		return snapshot.OMENativeExecutorState{Reason: omenativeReasonLeaseUnreadable}
	}
	if lease.DeletionTimestamp != nil {
		return snapshot.OMENativeExecutorState{Reason: omenativeReasonLeaseDeleting}
	}

	wireVersion, ok := lease.Annotations[constants.OMENativeExecutorCapabilitySchemaAnnotationKey]
	if !ok || strings.TrimSpace(wireVersion) == "" {
		return snapshot.OMENativeExecutorState{Reason: omenativeReasonSchemaMissing}
	}
	if wireVersion != audit.SchemaV1 {
		return snapshot.OMENativeExecutorState{Reason: omenativeReasonSchemaIncompatible}
	}

	if lease.Spec.HolderIdentity == nil || strings.TrimSpace(*lease.Spec.HolderIdentity) == "" {
		return snapshot.OMENativeExecutorState{Reason: omenativeReasonHolderMissing}
	}
	if lease.Spec.RenewTime == nil || lease.Spec.RenewTime.IsZero() {
		return snapshot.OMENativeExecutorState{Reason: omenativeReasonRenewTimeMissing}
	}

	renewTime := lease.Spec.RenewTime.Time
	now := r.now()
	if renewTime.After(now.Add(omenativeFutureSkew)) {
		return snapshot.OMENativeExecutorState{Reason: omenativeReasonRenewTimeFuture}
	}
	if now.Sub(renewTime) > cfg.OMENativeCapabilityMaxStaleness.Duration {
		return snapshot.OMENativeExecutorState{Reason: omenativeReasonLeaseStale}
	}

	return snapshot.OMENativeExecutorState{
		Available:   true,
		WireVersion: audit.SchemaV1,
		RenewTime:   renewTime,
		Reason:      omenativeReasonReady,
	}
}

func (r *OMENativeExecutorReader) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}
