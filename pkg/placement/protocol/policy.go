// Package protocol carries placement authority between source and member controllers.
package protocol

import (
	"encoding/json"
	"fmt"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// FromDerived reads policy only on origin-marked services. A similarly named
// annotation on an ordinary local service cannot change its execution policy.
func FromDerived(service *v1beta1.InferenceService) (*v1beta1.PlacementExecutionPolicy, error) {
	if service == nil {
		return nil, nil
	}
	origin := service.Annotations[constants.PlacementOriginUID]
	if origin == "" {
		origin = service.Labels[constants.PlacementOrigin]
	}
	if origin == "" {
		return nil, nil
	}
	raw, exists := service.Annotations[constants.PlacementExecution]
	if !exists {
		return nil, nil
	}
	var policy v1beta1.PlacementExecutionPolicy
	if err := json.Unmarshal([]byte(raw), &policy); err != nil {
		return nil, fmt.Errorf("invalid placement execution policy: %w", err)
	}
	if err := Validate(&policy); err != nil {
		return nil, err
	}
	if string(policy.SourceUID) != origin {
		return nil, fmt.Errorf("placement execution policy does not belong to the derived service's source")
	}
	return &policy, nil
}

// Validate rejects an incomplete authority envelope instead of treating it as
// an ordinary member policy that would permit uncoordinated surge.
func Validate(policy *v1beta1.PlacementExecutionPolicy) error {
	if policy == nil || policy.PlanID == "" || policy.Revision <= 0 || policy.SourceUID == "" || policy.ClusterUID == "" {
		return fmt.Errorf("placement execution policy requires plan, source, cluster identities and a positive revision")
	}
	return nil
}

// Encode produces the annotation value a source stamps on an owned member copy.
func Encode(policy *v1beta1.PlacementExecutionPolicy) (string, error) {
	if err := Validate(policy); err != nil {
		return "", err
	}
	data, err := json.Marshal(policy)
	return string(data), err
}

// Authorize prevents a stale projection from replacing a newer member policy.
// A policy remains authoritative until a later revision explicitly releases it.
func Authorize(current, next *v1beta1.PlacementExecutionPolicy) error {
	if current == nil && next == nil {
		return nil
	}
	if err := Validate(next); err != nil {
		return err
	}
	if current == nil {
		return nil
	}
	if err := Validate(current); err != nil {
		return err
	}
	if current.SourceUID != next.SourceUID || current.ClusterUID != next.ClusterUID {
		return fmt.Errorf("placement execution identity changed")
	}
	if next.Revision < current.Revision || next.Revision == current.Revision && *next != *current {
		return fmt.Errorf("placement execution revision is stale or conflicting")
	}
	return nil
}
