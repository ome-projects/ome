package v1alpha1

import (
	"strconv"

	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
)

// TrafficActionDetails identifies the exact durable override changed by one
// guarded traffic action. Operator reason text remains preview-only.
type TrafficActionDetails struct {
	OverrideID      string `json:"overrideID"`
	Cluster         string `json:"cluster,omitempty"`
	OverridesBefore int    `json:"overridesBefore"`
	OverridesAfter  int    `json:"overridesAfter"`
}

func (r ActionResult) trafficTable(wide bool) report.Table {
	details := r.Traffic
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
		[]string{"override-id", details.OverrideID},
		[]string{"cluster", orDash(details.Cluster)},
		[]string{"overrides", strconv.Itoa(details.OverridesBefore) + " -> " + strconv.Itoa(details.OverridesAfter)},
		[]string{"message", orDash(r.Message)},
		[]string{"follow-up", orDash(r.FollowUp)},
		[]string{"hint", "Use -o json or -o yaml for full values."},
	)
	for i := range rows {
		rows[i][1] = printers.BoundedCell(rows[i][1], 56)
	}
	return report.Table{Headers: []string{"FIELD", "VALUE"}, Rows: rows}
}
