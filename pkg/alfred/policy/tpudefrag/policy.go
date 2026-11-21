// Package tpudefrag implements Alfred's TPU partition defragmentation policy
// (OEP-0008.1). On TPU pools where OME gives each Instance its own slice, the
// policy plans drains: partitions of a target shape whose Instances can all
// move onto new slices elsewhere. The engine's fencer cordons a drain's hosts
// so the slice scheduler places replacements outside it; once the fence has
// settled, this policy emits one move per remaining Instance. The policy is
// pure: it reads the snapshot, including the fence annotations, and writes
// nothing.
package tpudefrag

import (
	"sort"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
)

// PolicyName is the stable policy label used by metrics, reports and the
// dispatch journal.
const PolicyName = "tpudefragmentation"

// Policy is the TPU partition defragmentation policy.
type Policy struct{}

var _ policy.Policy = &Policy{}

// Name implements policy.Policy.
func (*Policy) Name() string { return PolicyName }

// Evaluate emits, for each settled drain inside an open maintenance window,
// one move per Instance still on it. Instances of drains that are planned or
// still settling are reported as advisories. Partitions that have emptied are
// left to the fencer.
func (*Policy) Evaluate(snap *snapshot.ClusterSnapshot, cfg *config.Config) []policy.Candidate {
	if snap == nil || cfg == nil || !Enabled(cfg) {
		return nil
	}
	plan := Build(snap, cfg, snap.Timestamp, nil)
	var out []policy.Candidate
	for _, d := range plan.Open {
		if d.End != "" {
			continue
		}
		remaining := remainingPerPartition(d.Moves)
		for _, m := range d.Moves {
			c := candidateFor(snap, cfg, m, remaining[m.Partition])
			if !d.Settled {
				c.Executable, c.TPUSlice, c.AdvisoryReason = false, nil, policy.AdvisoryTPUDrainFenceSettling
			}
			out = append(out, c)
		}
	}
	for _, d := range plan.Intents {
		remaining := remainingPerPartition(d.Moves)
		for _, m := range d.Moves {
			c := candidateFor(snap, cfg, m, remaining[m.Partition])
			c.Executable, c.TPUSlice, c.AdvisoryReason = false, nil, policy.AdvisoryTPUDrainPlanned
			out = append(out, c)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		a, b := &out[i], &out[j]
		if a.Executable != b.Executable {
			return a.Executable
		}
		if a.Score != b.Score {
			return a.Score > b.Score
		}
		if a.Workload.String() != b.Workload.String() {
			return a.Workload.String() < b.Workload.String()
		}
		if a.Component != b.Component {
			return a.Component < b.Component
		}
		return a.Instance < b.Instance
	})
	return out
}

// candidateFor classifies one move. Its score prefers partitions with fewer
// Instances left, so nearly empty partitions finish first.
func candidateFor(snap *snapshot.ClusterSnapshot, cfg *config.Config, m Move, remaining int) policy.Candidate {
	c := policy.Candidate{
		Policy:      PolicyName,
		Workload:    m.Key.Workload,
		Component:   m.Key.Component,
		Instance:    m.Key.Instance,
		Reason:      policy.ReasonTPUFragmentation,
		FromNode:    m.FromNode,
		SurgeShaped: true,
		Score:       -float64(remaining),
	}
	if _, comp, _ := lookup(snap, m.Key); comp != nil {
		c.Mode = comp.DeploymentMode
	}
	reason, plan := movableReason(snap, cfg, m.Key)
	if reason != "" {
		c.AdvisoryReason = reason
		return c
	}
	c.Executable = true
	c.TPUSlice = plan
	return c
}

func remainingPerPartition(moves []Move) map[string]int {
	out := map[string]int{}
	for _, m := range moves {
		out[m.Partition]++
	}
	return out
}
