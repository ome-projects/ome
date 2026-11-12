package coordination

import (
	"fmt"
	"strconv"
	"strings"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadstatus "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
)

// unavailableInstancesClause names, for the unavailable budget's hold, the
// Instances the budget counts as out of rotation: every ordinal below the
// count the spec asks for whose row serves fewer pods than an Instance
// needs, or that has no row yet, each with why. A scale-up mid-roll reads
// "; counted as unavailable: Instances 12 to 23 still being created"; an
// Instance whose lost pod is being built again reads "Instance 0 being
// rebuilt"; one down beside the roll reads "Instance 3 Failed". Empty when every
// ordinal serves, so a hold spent by this pass's own starts keeps its plain
// text.
func unavailableInstancesClause(rows []v1beta1.OMENativeInstanceStatus, desired, podsPerInstance int32) string {
	byOrdinal := make(map[int32]*v1beta1.OMENativeInstanceStatus, len(rows))
	for i := range rows {
		byOrdinal[rows[i].Index] = &rows[i]
	}
	var groups []unavailableGroup
	for ordinal := int32(0); ordinal < desired; ordinal++ {
		why, down := instanceNotServing(byOrdinal[ordinal], podsPerInstance)
		if !down {
			continue
		}
		placed := false
		for i := range groups {
			if groups[i].why == why {
				groups[i].ordinals = append(groups[i].ordinals, ordinal)
				placed = true
				break
			}
		}
		if !placed {
			groups = append(groups, unavailableGroup{why: why, ordinals: []int32{ordinal}})
		}
	}
	if len(groups) == 0 {
		return ""
	}
	parts := make([]string, 0, len(groups))
	for _, g := range groups {
		noun := "Instance"
		if len(g.ordinals) > 1 {
			noun = "Instances"
		}
		parts = append(parts, fmt.Sprintf("%s %s %s", noun, ordinalSpans(g.ordinals), g.why))
	}
	return "; counted as unavailable: " + strings.Join(parts, ", ")
}

// unavailableGroup is the ordinals one wording of why covers, in order.
type unavailableGroup struct {
	why      string
	ordinals []int32
}

// instanceNotServing says whether the budget counts the Instance at an
// ordinal as out of rotation and, when it does, why in the operator's
// words. An ordinal with no row, a row still Pending or Creating, or one
// whose open operation is its create is an Instance whose pods are being
// built: one that never ran a revision (an empty RunningRevision marks a
// first materialization, a scale-up's new Instance among them) is still
// being created, one that ran before is being rebuilt. A row with fewer
// pods in rotation than an Instance needs is down for the reason its phase
// gives.
func instanceNotServing(row *v1beta1.OMENativeInstanceStatus, podsPerInstance int32) (why string, down bool) {
	if row == nil {
		return "still being created", true
	}
	if row.Phase == v1beta1.OMENativeInstancePending || row.Phase == v1beta1.OMENativeInstanceCreating ||
		(row.Operation != nil && row.Operation.Type == v1beta1.InstanceOperationCreate) {
		if row.RunningRevision == "" {
			return "still being created", true
		}
		return "being rebuilt", true
	}
	if workloadstatus.InstanceMeetsThreshold(row.PodCount, row.ServingPodCount, podsPerInstance) {
		return "", false
	}
	switch row.Phase {
	case v1beta1.OMENativeInstanceUpdating:
		return "being updated", true
	case v1beta1.OMENativeInstanceRestarting:
		return "restarting", true
	case v1beta1.OMENativeInstanceMigrating:
		return "migrating", true
	case v1beta1.OMENativeInstanceDeleting:
		return "being deleted", true
	case v1beta1.OMENativeInstanceFailed:
		return "Failed", true
	}
	return "not serving", true
}

// ordinalSpans writes ordinals, given in ascending order, as a list in
// which three or more in a row read as a span: "3", "3, 7", "12 to 23",
// "3, 12 to 23".
func ordinalSpans(ordinals []int32) string {
	var parts []string
	for i := 0; i < len(ordinals); {
		j := i
		for j+1 < len(ordinals) && ordinals[j+1] == ordinals[j]+1 {
			j++
		}
		switch {
		case j-i >= 2:
			parts = append(parts, fmt.Sprintf("%d to %d", ordinals[i], ordinals[j]))
			i = j + 1
		default:
			parts = append(parts, strconv.Itoa(int(ordinals[i])))
			i++
		}
	}
	return strings.Join(parts, ", ")
}

// podsPerInstance is the pod count one Instance of the replica needs in
// rotation to count as serving: the sum of its runner sizes, one when the
// spec records no runner.
func podsPerInstance(ir *v1beta1.InferenceReplica) int32 {
	var n int32
	for _, r := range ir.Spec.Runners {
		n += r.Size
	}
	return max(n, 1)
}
