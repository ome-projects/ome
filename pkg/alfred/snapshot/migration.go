package snapshot

import (
	"fmt"
	"time"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
)

const (
	migrationStateReasonRequestInvalid = "migration request annotation is invalid"
	migrationStateReasonStatusInvalid  = "inference replica migration status is invalid"
)

func migrationAnnotationKey(uuid string) string {
	return audit.MigrationRequestAnnotationPrefix + uuid
}

// parsePendingMigration adapts the canonical workload request into Alfred's
// immutable snapshot model. The workload parser owns schema validation;
// Alfred additionally fences the request against the source state it is about
// to reason about.
func parsePendingMigration(uuid string, raw string, workload *Workload) (InFlight, error) {
	if uuid == "" {
		return InFlight{}, fmt.Errorf("migration request UUID must not be empty")
	}
	req, err := audit.ParseMigrationRequest(raw)
	if err != nil {
		return InFlight{}, err
	}
	if req.FromNode == "" {
		return InFlight{}, fmt.Errorf("migration request from_node must not be empty")
	}
	if req.Instance < 0 {
		return InFlight{}, fmt.Errorf("migration request instance must be non-negative: %d", req.Instance)
	}

	component := v1beta1.ComponentType(req.Component)
	switch component {
	case v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent:
	default:
		return InFlight{}, fmt.Errorf("migration request component %q is unknown", req.Component)
	}

	if !hasCurrentMigrationInstance(workload, component, req.Instance) {
		return InFlight{}, fmt.Errorf("migration request has no current instance %s/%d", component, req.Instance)
	}

	var requestedAt time.Time
	if req.RequestedAt != "" {
		requestedAt, err = time.Parse(time.RFC3339, req.RequestedAt)
		if err != nil {
			return InFlight{}, fmt.Errorf("migration request requested_at %q is invalid: %w", req.RequestedAt, err)
		}
	}

	return InFlight{
		UUID:        uuid,
		Component:   component,
		Instance:    req.Instance,
		FromNode:    req.FromNode,
		RequestedAt: requestedAt,
		RequestedBy: req.RequestedBy,
	}, nil
}

func hasCurrentMigrationInstance(workload *Workload, component v1beta1.ComponentType, index int32) bool {
	if workload == nil || workload.Components[component] == nil {
		return false
	}
	for _, instance := range workload.Components[component].Instances {
		if instance != nil && instance.Index == index {
			return true
		}
	}
	return false
}

func currentMigrationInstance(workload *Workload, component v1beta1.ComponentType, index int32) *Instance {
	if workload == nil || workload.Components[component] == nil {
		return nil
	}
	for _, instance := range workload.Components[component].Instances {
		if instance != nil && instance.Index == index {
			return instance
		}
	}
	return nil
}

func validMigrationStatus(status *v1beta1.MigrationStatus) bool {
	if status.RequestUUID == "" || status.SourceInstance < 0 || status.StartedAt.IsZero() {
		return false
	}
	if status.CompletedAt != nil && (status.CompletedAt.IsZero() || status.CompletedAt.Before(&status.StartedAt)) {
		return false
	}
	switch status.Trigger {
	case v1beta1.MigrationTriggerManual:
		switch status.Phase {
		case v1beta1.MigrationPhaseAccepted,
			v1beta1.MigrationPhaseSurgePending,
			v1beta1.MigrationPhaseSurgeReady,
			v1beta1.MigrationPhaseDraining,
			v1beta1.MigrationPhaseCompleted,
			v1beta1.MigrationPhaseFailed:
		default:
			return false
		}
	case v1beta1.MigrationTriggerAuto:
		if status.Phase != v1beta1.MigrationPhaseRelocated {
			return false
		}
	default:
		return false
	}
	return status.Phase.Terminal() || status.CompletedAt == nil
}

func invalidateMigrationState(workload *Workload, reason string) {
	workload.MigrationStateValid = false
	if workload.MigrationStateReason == "" {
		workload.MigrationStateReason = reason
	}
}
