package tpudefrag

import (
	"encoding/json"
	"time"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
)

// Fence is the value of the defragmentation fence annotation on one host. It
// is the drain's only durable state: a restarted Alfred reads open drains back
// from it.
type Fence struct {
	// Drain names the drain that cordoned the host.
	Drain string `json:"drain"`
	// Unit is the drained partition shape, and Partition the id of the
	// partition of that shape this host belongs to.
	Unit      string `json:"unit"`
	Partition string `json:"partition"`
	// Deadline ends the drain if it has not emptied by then.
	Deadline time.Time `json:"deadline"`
	// FencedAt is when this host was cordoned.
	FencedAt time.Time `json:"fencedAt"`
	// FreeAtFence records that the host held no slice when it was fenced, so
	// a slice found on it later was placed inside the fence.
	FreeAtFence bool `json:"freeAtFence,omitempty"`
}

// Encode returns the annotation value.
func (f Fence) Encode() (string, error) {
	raw, err := json.Marshal(f)
	return string(raw), err
}

// DecodeFence parses an annotation value. It returns false for a value Alfred
// did not write, which the policy then treats as someone else's cordon.
func DecodeFence(value string) (Fence, bool) {
	var f Fence
	if err := json.Unmarshal([]byte(value), &f); err != nil {
		return Fence{}, false
	}
	if f.Drain == "" || f.Unit == "" || f.Partition == "" || f.Deadline.IsZero() || f.FencedAt.IsZero() {
		return Fence{}, false
	}
	return f, true
}

// Occupied reports whether a node carries a slice or holds an accelerator pod.
func Occupied(node *snapshot.Node, cfg *config.Config) bool {
	return node != nil && (node.Labels[cfg.TPUSlicePartitions.SliceLabel] != "" || holdsAccelerators(node))
}
