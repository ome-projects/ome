package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// TPUDefragmentation is the TPU partition defragmentation policy's block
// (policies.tpuDefragmentation). The policy empties sparsely used partitions
// of a target shape by fencing them with a cordon and moving their Instances
// onto new OME-provisioned slices, so whole free partitions of that shape
// exist for large workloads.
type TPUDefragmentation struct {
	Enabled *bool `json:"enabled"`
	// TargetShapes are TPU partition shapes such as "4x4x4". The largest is
	// the drain unit; the others are reported.
	TargetShapes []string `json:"targetShapes"`
	// MinGap is how far whole free partitions of the unit shape must fall
	// below the packing bound, in partitions, before a TPU pod is drained.
	MinGap int `json:"minGap"`
	// MaxDrainPartitions bounds the partitions one drain fences. A cluster
	// has at most one open drain.
	MaxDrainPartitions int `json:"maxDrainPartitions"`
	// MaxBusyHostsPerPartition skips partitions with more busy hosts.
	MaxBusyHostsPerPartition int `json:"maxBusyHostsPerPartition"`
	// MaxMovesPerWorkloadPerDrain bounds the Instances of one
	// InferenceService a drain moves, which keeps the drain inside its
	// deadline under the per-workload cooldown.
	MaxMovesPerWorkloadPerDrain int `json:"maxMovesPerWorkloadPerDrain"`
	// DrainDeadlineMinutes ends a drain that has not emptied in time.
	DrainDeadlineMinutes int `json:"drainDeadlineMinutes"`
	// FenceSettleSeconds is how long every host of a drain must have been
	// cordoned before its first move, so the slice scheduler sees the fence.
	FenceSettleSeconds int `json:"fenceSettleSeconds"`
	// FenceFailureBackoffMinutes pauses drains in a TPU pod after a new
	// slice landed inside one of its fences.
	FenceFailureBackoffMinutes int `json:"fenceFailureBackoffMinutes"`
}

func (t *TPUDefragmentation) applyDefaults() {
	if t.Enabled == nil {
		t.Enabled = boolPtr(false)
	}
	if len(t.TargetShapes) == 0 {
		t.TargetShapes = []string{"4x4x4", "2x2x4"}
	}
	defaultInt(&t.MinGap, 2)
	defaultInt(&t.MaxDrainPartitions, 4)
	defaultInt(&t.MaxBusyHostsPerPartition, 8)
	defaultInt(&t.MaxMovesPerWorkloadPerDrain, 4)
	defaultInt(&t.DrainDeadlineMinutes, 240)
	defaultInt(&t.FenceSettleSeconds, 60)
	defaultInt(&t.FenceFailureBackoffMinutes, 1440)
}

func (t *TPUDefragmentation) validate(c *Config) error {
	seen := map[string]bool{}
	for _, shape := range t.TargetShapes {
		if _, ok := ShapeChips(shape); !ok {
			return fmt.Errorf("policies.tpuDefragmentation.targetShapes has invalid shape %q (want AxBxC)", shape)
		}
		if seen[shape] {
			return fmt.Errorf("policies.tpuDefragmentation.targetShapes has duplicate shape %q", shape)
		}
		seen[shape] = true
	}
	for _, p := range []struct {
		name  string
		value int
	}{
		{"minGap", t.MinGap},
		{"maxDrainPartitions", t.MaxDrainPartitions},
		{"maxBusyHostsPerPartition", t.MaxBusyHostsPerPartition},
		{"maxMovesPerWorkloadPerDrain", t.MaxMovesPerWorkloadPerDrain},
		{"drainDeadlineMinutes", t.DrainDeadlineMinutes},
		{"fenceSettleSeconds", t.FenceSettleSeconds},
		{"fenceFailureBackoffMinutes", t.FenceFailureBackoffMinutes},
	} {
		if p.value <= 0 {
			return fmt.Errorf("policies.tpuDefragmentation.%s must be positive, got %d", p.name, p.value)
		}
	}
	// A drain hides free hosts from every scheduler while it runs, so in
	// execute mode it may only run inside an operator-chosen window.
	if *t.Enabled && c.Mode == ModeExecute && len(c.MaintenanceWindows) == 0 {
		return fmt.Errorf("policies.tpuDefragmentation in execute mode requires maintenanceWindows")
	}
	return nil
}

// DrainDeadline is the longest a drain stays open.
func (t *TPUDefragmentation) DrainDeadline() time.Duration { return minutes(t.DrainDeadlineMinutes) }

// FenceSettle is how long a fence must stand before the first move.
func (t *TPUDefragmentation) FenceSettle() time.Duration {
	return time.Duration(t.FenceSettleSeconds) * time.Second
}

// FenceFailureBackoff is how long a TPU pod is not drained after a fence
// was not honored there.
func (t *TPUDefragmentation) FenceFailureBackoff() time.Duration {
	return minutes(t.FenceFailureBackoffMinutes)
}

// ShapeChips returns the chip count of a TPU partition shape such as "2x2x4",
// or false when the shape is not three positive integers joined by "x".
func ShapeChips(shape string) (int, bool) {
	dims := strings.Split(shape, "x")
	if len(dims) != 3 {
		return 0, false
	}
	chips := 1
	for _, d := range dims {
		n, err := strconv.Atoi(d)
		if err != nil || n <= 0 || strconv.Itoa(n) != d {
			return 0, false
		}
		chips *= n
	}
	return chips, true
}

// MaintenanceWindowOpen reports whether now falls inside a configured weekly
// UTC maintenance window. With no windows configured it reports true.
func (c *Config) MaintenanceWindowOpen(now time.Time) bool {
	if len(c.MaintenanceWindows) == 0 {
		return true
	}
	utc := now.UTC()
	day := utc.Weekday().String()[:3]
	minute := utc.Hour()*60 + utc.Minute()
	for _, w := range c.MaintenanceWindows {
		dayMatch := false
		for _, d := range w.Days {
			if d == day {
				dayMatch = true
				break
			}
		}
		if !dayMatch {
			continue
		}
		start, errStart := time.Parse("15:04", w.Start)
		end, errEnd := time.Parse("15:04", w.End)
		if errStart != nil || errEnd != nil {
			continue
		}
		if minute >= start.Hour()*60+start.Minute() && minute < end.Hour()*60+end.Minute() {
			return true
		}
	}
	return false
}
