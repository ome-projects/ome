package runtimepreset

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"

	v1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// PresetsConfigKeyName is the inferenceservice-config key that holds the
// engine presets: one ServingRuntimeSpec per ome.io/engine value.
const PresetsConfigKeyName = "runtimePresets"

// Presets maps an ome.io/engine value to the spec it fills in under the
// runtime's own fields. The binary carries no preset of its own; the chart
// values supply them.
type Presets map[string]*v1beta1.ServingRuntimeSpec

// parsePresets reads the runtimePresets block. An absent block is an empty
// table, under which every engine annotation is refused.
func parsePresets(configMap *v1.ConfigMap) (Presets, error) {
	raw, ok := configMap.Data[PresetsConfigKeyName]
	if !ok || strings.TrimSpace(raw) == "" {
		return Presets{}, nil
	}
	presets := Presets{}
	if err := json.Unmarshal([]byte(raw), &presets); err != nil {
		return nil, fmt.Errorf("unable to unmarshal %v json string: %w", PresetsConfigKeyName, err)
	}
	return presets, nil
}

// names lists the configured engines in order, for a refusal to quote.
func (p Presets) names() []string {
	return slices.Sorted(maps.Keys(p))
}
