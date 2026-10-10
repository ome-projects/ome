package config

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

// TPUSlicePartitions names the labels Alfred reads to decide whether a TPU
// instance can move to a new OME-provisioned slice (tpuSliceMigrationEnabled).
// OME creates the replacement's slice and the slice scheduler places it, so
// Alfred only checks that a free healthy partition of the instance's topology
// exists on nodes it would accept as a destination. It does not simulate
// placement.
type TPUSlicePartitions struct {
	// TopologyLabel is the pod nodeSelector key that holds the slice topology.
	TopologyLabel string `json:"topologyLabel"`
	// IDLabel and StateLabel are node label keys with one "%s" that the
	// topology replaces, for example cloud.google.com/gke-tpu-partition-2x2x2-id.
	IDLabel    string `json:"idLabel"`
	StateLabel string `json:"stateLabel"`
	// HealthyState is the state label value that marks a partition usable.
	HealthyState string `json:"healthyState"`
}

func (t *TPUSlicePartitions) applyDefaults() {
	defaultStr(&t.TopologyLabel, "cloud.google.com/gke-tpu-topology")
	defaultStr(&t.IDLabel, "cloud.google.com/gke-tpu-partition-%s-id")
	defaultStr(&t.StateLabel, "cloud.google.com/gke-tpu-partition-%s-state")
	defaultStr(&t.HealthyState, "HEALTHY")
}

func (t *TPUSlicePartitions) validate() error {
	if len(validation.IsQualifiedName(t.TopologyLabel)) != 0 {
		return fmt.Errorf("tpuSlicePartitions.topologyLabel is invalid: %q", t.TopologyLabel)
	}
	for _, f := range []struct{ name, value string }{
		{"idLabel", t.IDLabel},
		{"stateLabel", t.StateLabel},
	} {
		if strings.Count(f.value, "%s") != 1 || strings.Count(f.value, "%") != 1 {
			return fmt.Errorf("tpuSlicePartitions.%s must contain exactly one %%s: %q", f.name, f.value)
		}
		if len(validation.IsQualifiedName(t.Label(f.value, "2x2x2"))) != 0 {
			return fmt.Errorf("tpuSlicePartitions.%s does not form a valid label key: %q", f.name, f.value)
		}
	}
	if len(validation.IsValidLabelValue(t.HealthyState)) != 0 || t.HealthyState == "" {
		return fmt.Errorf("tpuSlicePartitions.healthyState is invalid: %q", t.HealthyState)
	}
	return nil
}

// Label fills a partition label key format with a topology.
func (t *TPUSlicePartitions) Label(format, topology string) string {
	return strings.Replace(format, "%s", topology, 1)
}
