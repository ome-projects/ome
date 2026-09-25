package v1alpha1

import (
	"strconv"

	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
)

// RolloutActionDetails identifies the exact pinned run and combined
// progression-render digests selected by one guarded rollout mutation. It
// intentionally carries no arbitrary controller messages or policy bodies.
type RolloutActionDetails struct {
	RunID               string `json:"runID"`
	PinnedPlanDigest    string `json:"pinnedPlanDigest"`
	RequestedPlanDigest string `json:"requestedPlanDigest"`
	GroupCount          int    `json:"groupCount"`
}

func (r ActionResult) rolloutTable(wide bool) report.Table {
	details := r.Rollout
	rows := [][]string{
		{"action", r.Action},
		{"target", r.Target.displayName()},
	}
	if wide {
		rows = append(rows,
			[]string{"uid", orDash(r.Target.UID)},
			[]string{"resource-version", orDash(r.Target.ResourceVersion)},
		)
	}
	rows = append(rows,
		[]string{"dry-run", string(r.DryRun)},
		[]string{"accepted", yesNo(r.Accepted)},
		[]string{"applied", yesNo(r.Applied)},
		[]string{"run-id", details.RunID},
		[]string{"pinned-plan-digest", details.PinnedPlanDigest},
		[]string{"requested-plan-digest", details.RequestedPlanDigest},
		[]string{"group-count", strconv.Itoa(details.GroupCount)},
		[]string{"message", orDash(r.Message)},
		[]string{"follow-up", orDash(r.FollowUp)},
		[]string{"hint", "Use -o json or -o yaml for full values."},
	)
	for i := range rows {
		rows[i][1] = printers.BoundedCell(rows[i][1], 56)
	}
	return report.Table{Headers: []string{"FIELD", "VALUE"}, Rows: rows}
}
