// Package placement validates placement authority for Alfred's new migrations.
package placement

import (
	"fmt"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

// CheckNewSurge is a read-only gate for new requests, not reconciliation of
// submitted requests. Either observed authority can withhold new surge while
// the ISVC policy is being projected onto its IR. Existing allocated work must
// continue to be observed even when this gate rejects new work.
func CheckNewSurge(service *v1beta1.InferenceService, ir *v1beta1.InferenceReplica) error {
	if ir == nil {
		return fmt.Errorf("source InferenceReplica is missing")
	}
	if ir.Spec.Paused {
		return fmt.Errorf("source InferenceReplica is paused")
	}
	current := ir.Spec.PlacementExecution
	if current != nil {
		if err := protocol.Validate(current); err != nil {
			return fmt.Errorf("source placement policy: %w", err)
		}
		if current.PauseSurge {
			return fmt.Errorf("source placement policy pauses new surge")
		}
	}
	next, err := protocol.FromDerived(service)
	if err != nil {
		return fmt.Errorf("owner placement policy: %w", err)
	}
	if err := protocol.Authorize(current, next); err != nil {
		return fmt.Errorf("owner placement authority: %w", err)
	}
	if next != nil && next.PauseSurge {
		return fmt.Errorf("owner placement policy pauses new surge")
	}
	return nil
}
