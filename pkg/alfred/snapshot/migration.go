package snapshot

import (
	"encoding/json"
	"fmt"
	"time"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

const (
	migrationRequestAnnotationPrefix   = "ome.io/migration-request-v1-"
	migrationStateReasonRequestInvalid = "migration request annotation is invalid"
	migrationStateReasonStatusInvalid  = "inference replica migration status is invalid"
)

// observedMigrationRequest is the existing public v1 annotation format. Keep
// every known field typed, including those not projected into InFlight, so
// malformed values cannot silently become accepted as unknown fields.
type observedMigrationRequest struct {
	SchemaVersion   string   `json:"schemaVersion"`
	Component       string   `json:"component"`
	Instance        int32    `json:"instance"`
	FromNode        string   `json:"from_node"`
	HintTargetNodes []string `json:"hint_target_nodes,omitempty"`
	Reason          string   `json:"reason,omitempty"`
	RequestedAt     string   `json:"requested_at,omitempty"`
	RequestedBy     string   `json:"requested_by,omitempty"`
}

func migrationAnnotationKey(uuid string) string {
	return migrationRequestAnnotationPrefix + uuid
}

// parsePendingMigration observes the public annotation contract without
// depending on the executor's parser. Unknown additive fields are accepted for
// v1 version skew; unknown schemas and malformed known fields are rejected.
// Alfred additionally checks the request against the observed source instance.
func parsePendingMigration(uuid string, raw string, workload *Workload) (InFlight, error) {
	if uuid == "" {
		return InFlight{}, fmt.Errorf("migration request UUID must not be empty")
	}
	var req observedMigrationRequest
	if err := json.Unmarshal([]byte(raw), &req); err != nil {
		return InFlight{}, fmt.Errorf("parse migration request: %w", err)
	}
	if req.SchemaVersion != "v1" {
		return InFlight{}, fmt.Errorf("unsupported migration request schema version: %q", req.SchemaVersion)
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
		var err error
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
