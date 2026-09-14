package engine

import (
	"context"
	"reflect"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/alfred/config"
	v1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func (d *Dispatcher) reconcileDispatches(ctx context.Context, j *dispatchJournal) []dispatchEntry {
	var reports []dispatchEntry
	for i := range j.Entries {
		e := &j.Entries[i]
		if e.terminal() {
			continue
		}
		before := *e
		d.reconcileDispatch(ctx, e)
		if e.Phase == dispatchFailed || e.Phase == dispatchStalled {
			if before.Phase != e.Phase {
				until := d.now().Add(d.Options.FailureBackoff)
				j.BackoffUntil = &until
			}
		}
		if !reflect.DeepEqual(before, *e) || !e.terminal() {
			reports = append(reports, *e)
		}
	}
	return reports
}

func (d *Dispatcher) reconcileDispatch(ctx context.Context, e *dispatchEntry) {
	var owner v1beta1.InferenceService
	var ir v1beta1.InferenceReplica
	if err := d.Reader.Get(ctx, e.Workload, &owner); err != nil || owner.UID != e.WorkloadUID {
		d.stallAfterDeadline(e, "OwnerUnavailable")
		return
	}
	if err := d.Reader.Get(ctx, types.NamespacedName{Namespace: e.Workload.Namespace, Name: e.IRName}, &ir); err != nil || ir.UID != e.IRUID {
		d.stallAfterDeadline(e, "ReplicaUnavailable")
		return
	}
	owned := false
	for _, ref := range ir.OwnerReferences {
		if ref.Kind == "InferenceService" && ref.Name == owner.Name && ref.UID == owner.UID && ref.Controller != nil && *ref.Controller {
			owned = true
		}
	}
	if !owned || ir.Spec.Component != e.Component {
		d.stallAfterDeadline(e, "ReplicaIdentityChanged")
		return
	}
	var matched *v1beta1.MigrationStatus
	for i := range ir.Status.Migrations {
		m := &ir.Status.Migrations[i]
		if m.RequestUUID == e.UUID {
			if matched != nil {
				d.stallAfterDeadline(e, "MigrationStatusInvalid")
				return
			}
			matched = m
		}
	}
	if matched != nil {
		m := matched
		// metav1.Time is serialized at second precision by the API. The
		// write-ahead journal retains nanoseconds; compare at wire precision.
		if m.Trigger != v1beta1.MigrationTriggerManual || m.SourceInstance != e.Instance || m.FromNode != e.FromNode || m.StartedAt.IsZero() || m.StartedAt.After(d.now()) || m.StartedAt.Time.Before(e.CreatedAt.Truncate(time.Second)) {
			d.stallAfterDeadline(e, "MigrationStatusInvalid")
			return
		}
		switch m.Phase {
		case v1beta1.MigrationPhaseCompleted, v1beta1.MigrationPhaseFailed:
			if m.CompletedAt == nil || m.CompletedAt.Before(&m.StartedAt) || m.CompletedAt.After(d.now()) {
				d.stallAfterDeadline(e, "MigrationStatusInvalid")
				return
			}
			completed := m.CompletedAt.Time
			if completed.Before(e.CreatedAt) {
				completed = e.CreatedAt
			}
			e.CompletedAt = &completed
			if e.AcknowledgedAt == nil {
				ack := m.StartedAt.Time
				if ack.Before(e.CreatedAt) {
					ack = e.CreatedAt
				}
				e.AcknowledgedAt = &ack
			}
			e.Phase = dispatchCompleted
			if m.Phase == v1beta1.MigrationPhaseFailed {
				e.Phase = dispatchFailed
			}
			e.Reason = "TerminalStatusObserved"
			return
		case v1beta1.MigrationPhaseAccepted, v1beta1.MigrationPhaseSurgePending, v1beta1.MigrationPhaseSurgeReady, v1beta1.MigrationPhaseDraining:
			if m.Deadline.IsZero() || !m.Deadline.After(m.StartedAt.Time) || m.CompletedAt != nil {
				d.stallAfterDeadline(e, "MigrationStatusInvalid")
				return
			}
			if e.AcknowledgedAt == nil {
				ack := m.StartedAt.Time
				if ack.Before(e.CreatedAt) {
					ack = e.CreatedAt
				}
				e.AcknowledgedAt = &ack
			}
			e.Phase = dispatchAcknowledged
			e.Reason = "UUIDStatusObserved"
			if d.now().After(m.Deadline.Time) {
				e.Phase = dispatchStalled
				e.Reason = "ConsumerDeadlineExceeded"
			}
			return
		default:
			d.stallAfterDeadline(e, "MigrationStatusInvalid")
			return
		}
	}
	if e.AcknowledgedAt != nil {
		e.Phase = dispatchStalled
		e.Reason = "AcknowledgedStatusMissing"
		return
	}
	if payload, present := owner.Annotations[migrationRequestPrefix+e.UUID]; present {
		if payload != e.Payload {
			e.Phase = dispatchStalled
			e.Reason = "RequestPayloadChanged"
			return
		}
		if e.Phase != dispatchStalled {
			e.Phase = dispatchSubmitted
			e.Reason = "RequestAnnotationObserved"
		}
	}
	d.stallAfterDeadline(e, "AcknowledgementTimeout")
}

func (d *Dispatcher) stallAfterDeadline(e *dispatchEntry, reason string) {
	if d.now().Sub(e.CreatedAt) >= d.Options.AcknowledgementTimeout {
		e.Phase = dispatchStalled
		e.Reason = reason
	}
}

func (d *Dispatcher) attemptDispatch(ctx context.Context, cm *corev1.ConfigMap, j *dispatchJournal, index int, proof dispatchEvidence, cfg *config.Config) dispatchEntry {
	e := j.Entries[index]
	if err := d.executionEnabled(ctx, cfg); err != nil {
		e.Reason = err.Error()
		return e
	}
	// Check owner RV after the journal roundtrip too, so the actual patch is
	// conditioned on the same source object that passed final preflight.
	var owner v1beta1.InferenceService
	if err := d.Reader.Get(ctx, e.Workload, &owner); err != nil || owner.UID != proof.owner.UID || owner.ResourceVersion != proof.owner.ResourceVersion {
		e.Reason = "OwnerChanged"
		return e
	}
	now := d.now()
	e.LastAttempt = &now
	e.Reason = "SubmissionPrepared"
	j.Entries[index] = e
	if err := saveDispatchJournal(ctx, d.Client, cm, j); err != nil {
		e.Reason = "JournalUnavailable"
		return e
	}
	if err := d.executionEnabled(ctx, cfg); err != nil {
		e.Reason = err.Error()
		return e
	}
	err := submitDispatch(ctx, d.Client, &owner, e)
	if err == nil {
		e.Phase = dispatchSubmitted
		e.Reason = "RequestSubmitted"
	} else {
		e.Reason = "SubmissionUncertain"
	}
	// Failure to persist the response leaves the prior attempted intent durable.
	// Reconciliation reads the actual annotation/status before considering retry.
	fresh, state, loadErr := loadDispatchJournal(ctx, d.Reader, d.Namespace)
	if loadErr == nil && index < len(state.Entries) && state.Entries[index].UUID == e.UUID {
		state.Entries[index] = e
		if saveErr := saveDispatchJournal(ctx, d.Client, fresh, state); saveErr != nil {
			e.Reason = "SubmissionJournalUncertain"
		}
	}
	return e
}
