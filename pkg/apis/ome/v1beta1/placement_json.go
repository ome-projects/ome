package v1beta1

import (
	"encoding/json"
)

// PlacementLegacyFields records obsolete fields whose zero values would be omitted.
// +k8s:openapi-gen=false
type PlacementLegacyFields struct {
	Requirements    bool
	ClusterSelector bool
	CapacityFactors bool
}

// SplitLegacyFields records obsolete fields whose zero values would be omitted.
// +k8s:openapi-gen=false
type SplitLegacyFields struct {
	Spread                bool
	MinReplicasPerCluster bool
}

// HasLegacyFields includes explicitly empty and null fields. Go zero values
// alone cannot distinguish obsolete intent from omission after API decoding.
func (p *PlacementSpec) HasLegacyFields() bool {
	return p != nil && (p.LegacyFields.Requirements || p.LegacyFields.ClusterSelector || p.LegacyFields.CapacityFactors || p.Requirements != "" || p.ClusterSelector != "" || p.CapacityFactors != nil)
}

// HasLegacyFields includes explicit false and zero, which carry packing intent.
func (s *SplitSpec) HasLegacyFields() bool {
	return s != nil && (s.LegacyFields.Spread || s.LegacyFields.MinReplicasPerCluster || s.Spread || s.MinReplicasPerCluster != 0)
}

// UnmarshalJSON preserves null affinity as an invalid empty list rather than an
// unconstrained omission, including when admission webhooks are unavailable.
func (p *PlacementSpec) UnmarshalJSON(data []byte) error {
	type plain PlacementSpec
	var decoded plain
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	_, decoded.LegacyFields.Requirements = fields["requirements"]
	decoded.LegacyFields.Requirements = decoded.LegacyFields.Requirements && decoded.Requirements == ""
	_, decoded.LegacyFields.ClusterSelector = fields["clusterSelector"]
	decoded.LegacyFields.ClusterSelector = decoded.LegacyFields.ClusterSelector && decoded.ClusterSelector == ""
	_, decoded.LegacyFields.CapacityFactors = fields["capacityFactors"]
	decoded.LegacyFields.CapacityFactors = decoded.LegacyFields.CapacityFactors && decoded.CapacityFactors == nil
	if _, exists := fields["clusterAffinity"]; exists && decoded.ClusterAffinity == nil {
		decoded.ClusterAffinity = []ClusterAffinityTerm{}
	}
	*p = PlacementSpec(decoded)
	return nil
}

// MarshalJSON keeps explicit invalid restrictions visible across a read/write
// round trip, so clearing them requires an intentional edit of the API object.
func (p PlacementSpec) MarshalJSON() ([]byte, error) {
	type plain PlacementSpec
	data, err := json.Marshal(plain(p))
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}
	if p.ClusterAffinity != nil && len(p.ClusterAffinity) == 0 {
		fields["clusterAffinity"] = json.RawMessage(`[]`)
	}
	if p.LegacyFields.Requirements && p.Requirements == "" {
		fields["requirements"] = json.RawMessage(`""`)
	}
	if p.LegacyFields.ClusterSelector && p.ClusterSelector == "" {
		fields["clusterSelector"] = json.RawMessage(`""`)
	}
	if p.LegacyFields.CapacityFactors && p.CapacityFactors == nil {
		fields["capacityFactors"] = json.RawMessage(`null`)
	} else if p.CapacityFactors != nil && len(p.CapacityFactors) == 0 {
		fields["capacityFactors"] = json.RawMessage(`{}`)
	}
	return json.Marshal(fields)
}

func (s *SplitSpec) UnmarshalJSON(data []byte) error {
	type plain SplitSpec
	var decoded plain
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	_, decoded.LegacyFields.Spread = fields["spread"]
	decoded.LegacyFields.Spread = decoded.LegacyFields.Spread && !decoded.Spread
	_, decoded.LegacyFields.MinReplicasPerCluster = fields["minReplicasPerCluster"]
	decoded.LegacyFields.MinReplicasPerCluster = decoded.LegacyFields.MinReplicasPerCluster && decoded.MinReplicasPerCluster == 0
	*s = SplitSpec(decoded)
	return nil
}

func (s SplitSpec) MarshalJSON() ([]byte, error) {
	type plain SplitSpec
	data, err := json.Marshal(plain(s))
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}
	if s.LegacyFields.Spread && !s.Spread {
		fields["spread"] = json.RawMessage(`false`)
	}
	if s.LegacyFields.MinReplicasPerCluster && s.MinReplicasPerCluster == 0 {
		fields["minReplicasPerCluster"] = json.RawMessage(`0`)
	}
	return json.Marshal(fields)
}
