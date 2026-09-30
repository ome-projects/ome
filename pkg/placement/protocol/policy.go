// Package protocol carries placement authority between source and member controllers.
package protocol

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/equality"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// IsAffinityMember distinguishes opt-in members from ordinary local services
// and legacy derived copies. An execution envelope also establishes opt-in.
func IsAffinityMember(service *v1beta1.InferenceService) bool {
	if service == nil || (service.Annotations[constants.PlacementOriginUID] == "" && service.Labels[constants.PlacementOrigin] == "") {
		return false
	}
	_, selected := service.Annotations[constants.PlacementPolicy]
	_, planned := service.Annotations[constants.PlacementExecution]
	return selected || planned
}

// executionVersion identifies the annotation wire format. An envelope keeps
// readers that only understand flat policies from accepting partial authority.
const (
	executionVersion = 1
	demandVersion    = 2
	floorsVersion    = 3
)

type executionEnvelope struct {
	Version int                               `json:"version"`
	Policy  *v1beta1.PlacementExecutionPolicy `json:"policy"`
}

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
	policy, err := Decode(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid placement execution policy: %w", err)
	}
	if err := Validate(policy); err != nil {
		return nil, err
	}
	if string(policy.SourceUID) != origin {
		return nil, fmt.Errorf("placement execution policy does not belong to the derived service's source")
	}
	if policy.Demand != nil {
		if err := ValidateDemandComponents(service, policy.Demand); err != nil {
			return nil, err
		}
	}
	return policy, nil
}

// Decode parses one placement execution policy annotation value, either a
// versioned envelope or the bare policy of the first transport, and rejects
// unknown fields. It does not check the policy against a service; FromDerived
// does that for a derived member.
func Decode(raw string) (*v1beta1.PlacementExecutionPolicy, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &fields); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	_, hasVersion := fields["version"]
	_, hasPolicy := fields["policy"]
	if hasVersion || hasPolicy {
		var envelope executionEnvelope
		if err := decoder.Decode(&envelope); err != nil {
			return nil, err
		}
		if envelope.Version != executionVersion && envelope.Version != demandVersion && envelope.Version != floorsVersion {
			return nil, fmt.Errorf("unsupported execution policy version %d", envelope.Version)
		}
		hasDemand := envelope.Policy != nil && envelope.Policy.Demand != nil
		hasFloors := envelope.Policy != nil && len(envelope.Policy.ReplicaFloors) > 0
		if (envelope.Version == floorsVersion) != hasFloors {
			return nil, fmt.Errorf("execution policy version does not match its replica floor contract")
		}
		if envelope.Version != floorsVersion && (envelope.Version == demandVersion) != hasDemand {
			return nil, fmt.Errorf("execution policy version does not match its demand contract")
		}
		return envelope.Policy, nil
	}
	var policy v1beta1.PlacementExecutionPolicy
	if err := decoder.Decode(&policy); err != nil {
		return nil, err
	}
	if policy.Demand != nil || len(policy.ReplicaFloors) > 0 {
		return nil, fmt.Errorf("placement demand and replica floors require a versioned execution envelope")
	}
	return &policy, nil
}

// Validate rejects an incomplete authority envelope instead of treating it as
// an ordinary member policy that would permit uncoordinated surge.
func Validate(policy *v1beta1.PlacementExecutionPolicy) error {
	if policy == nil || policy.PlanID == "" || policy.Revision <= 0 || policy.SourceUID == "" || policy.ClusterUID == "" {
		return fmt.Errorf("placement execution policy requires plan, source, cluster identities and a positive revision")
	}
	if policy.Demand != nil {
		if err := ValidateDemand(policy.Demand); err != nil {
			return err
		}
	}
	if len(policy.ReplicaFloors) > 0 {
		if policy.PauseSurge {
			_, err := ValidatePositiveReplicaFloors(policy.ReplicaFloors)
			return err
		}
		_, err := ValidateReplicaFloors(policy.ReplicaFloors)
		return err
	}
	return nil
}

// ValidateDemand checks the complete contract independently of an allocation's
// execution revision, including before a source plan has been persisted.
func ValidateDemand(demand *v1beta1.PlacementDemandContract) error {
	if demand == nil || !validHash(demand.Fingerprint) || len(demand.Components) == 0 {
		return fmt.Errorf("placement demand requires a fingerprint and component renderings")
	}
	seen := map[v1beta1.ComponentType]bool{}
	for _, component := range demand.Components {
		if (component.Component != v1beta1.EngineComponent && component.Component != v1beta1.DecoderComponent) || seen[component.Component] || !validHash(component.RenderingHash) {
			return fmt.Errorf("placement demand has invalid or duplicate component %q", component.Component)
		}
		seen[component.Component] = true
	}
	return nil
}

// ValidateDemandComponents requires exactly the service's declared Engine and
// Decoder renderings. Router has a separate per-home replica policy.
func ValidateDemandComponents(service *v1beta1.InferenceService, demand *v1beta1.PlacementDemandContract) error {
	if err := ValidateDemand(demand); err != nil {
		return err
	}
	if service == nil {
		return fmt.Errorf("placement demand requires a service component inventory")
	}
	declared := map[v1beta1.ComponentType]bool{
		v1beta1.EngineComponent:  service.Spec.Engine != nil,
		v1beta1.DecoderComponent: service.Spec.Decoder != nil,
	}
	for _, component := range demand.Components {
		if !declared[component.Component] {
			return fmt.Errorf("placement demand names an undeclared component %q", component.Component)
		}
		delete(declared, component.Component)
	}
	for component, exists := range declared {
		if exists {
			return fmt.Errorf("placement demand is missing component %q", component)
		}
	}
	return nil
}

func validHash(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == strings.ToLower(value)
}

// Encode produces the annotation value a source stamps on an owned member copy.
func Encode(policy *v1beta1.PlacementExecutionPolicy) (string, error) {
	if err := Validate(policy); err != nil {
		return "", err
	}
	version := executionVersion
	if policy.Demand != nil {
		version = demandVersion
	}
	if len(policy.ReplicaFloors) > 0 {
		version = floorsVersion
	}
	data, err := json.Marshal(executionEnvelope{Version: version, Policy: policy})
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
	if next.Revision < current.Revision || next.Revision == current.Revision && !equality.Semantic.DeepEqual(next, current) {
		return fmt.Errorf("placement execution revision is stale or conflicting")
	}
	return nil
}
