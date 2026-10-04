package v1beta1

import (
	"encoding/json"
)

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
	return json.Marshal(fields)
}
