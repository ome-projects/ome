package v1alpha1

import (
	"strings"
	"time"

	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
)

const ActionResultKind = "ActionResult"

// DryRunMode identifies whether an action was submitted to the API server.
type DryRunMode string

const (
	DryRunNone   DryRunMode = "none"
	DryRunClient DryRunMode = "client"
	DryRunServer DryRunMode = "server"
)

// ActionTarget is the exact Kubernetes object identity used by an action.
type ActionTarget struct {
	Kind            string `json:"kind"`
	Namespace       string `json:"namespace,omitempty"`
	Name            string `json:"name"`
	UID             string `json:"uid,omitempty"`
	ResourceVersion string `json:"resourceVersion,omitempty"`
}

// ActionResult is the separate, versioned stdout contract shared by guarded
// mutating commands. Preview, confirmation, and warnings remain on stderr.
type ActionResult struct {
	APIVersion   string              `json:"apiVersion"`
	Kind         string              `json:"kind"`
	CollectedAt  time.Time           `json:"collectedAt"`
	Action       string              `json:"action"`
	Target       ActionTarget        `json:"target"`
	DryRun       DryRunMode          `json:"dryRun"`
	RequestID    string              `json:"requestID,omitempty"`
	RevisionHash string              `json:"revisionHash,omitempty"`
	Accepted     bool                `json:"accepted"`
	Applied      bool                `json:"applied"`
	Message      string              `json:"message,omitempty"`
	FollowUp     string              `json:"followUp,omitempty"`
	Scale        *ScaleActionDetails `json:"scale,omitempty"`
}

// NewActionResult creates an unapplied result using an injectable clock.
func NewActionResult(action string, target ActionTarget, dryRun DryRunMode, clock Clock) ActionResult {
	if clock == nil {
		clock = SystemClock{}
	}
	return (ActionResult{
		APIVersion:  APIVersion,
		Kind:        ActionResultKind,
		CollectedAt: clock.Now().UTC(),
		Action:      action,
		Target:      target,
		DryRun:      dryRun,
	}).Canonical()
}

// Canonical returns a deterministic copy and enforces dry-run invariants:
// neither dry-run mode may claim application, and client dry-run cannot claim
// API acceptance because it submits no request.
func (r ActionResult) Canonical() ActionResult {
	result := r
	result.APIVersion = APIVersion
	result.Kind = ActionResultKind
	result.CollectedAt = r.CollectedAt.UTC()
	if r.Scale != nil {
		copy := r.Scale.canonical()
		result.Scale = &copy
	}
	if result.DryRun != DryRunNone {
		result.Applied = false
	}
	if result.DryRun == DryRunClient {
		result.Accepted = false
	}
	return result
}

// Table derives the concise human view from the typed action result.
func (r ActionResult) Table() report.Table {
	if r.Scale != nil {
		return r.scaleTable(false)
	}
	rows := [][]string{{"action", r.Action}, {"target", r.Target.displayName()}, {"dry-run", string(r.DryRun)}, {"accepted", yesNo(r.Accepted)}, {"applied", yesNo(r.Applied)}, {"request-id", orDash(r.RequestID)}, {"revision-hash", orDash(r.RevisionHash)}, {"message", orDash(r.Message)}, {"follow-up", orDash(r.FollowUp)}, {"hint", "Use -o json or -o yaml for full values."}}
	for i := range rows {
		rows[i][1] = printers.BoundedCell(rows[i][1], 56)
	}
	return report.Table{Headers: []string{"FIELD", "VALUE"}, Rows: rows}
}

// WideTable derives a bounded expanded view from the same canonical value.
func (r ActionResult) WideTable() report.Table {
	if r.Scale != nil {
		return r.scaleTable(true)
	}
	return r.Table()
}

func (t ActionTarget) displayName() string {
	parts := []string{t.Kind}
	if t.Namespace != "" {
		parts = append(parts, t.Namespace)
	}
	parts = append(parts, t.Name)
	return strings.Join(parts, "/")
}

func yesNo(value bool) string {
	if value {
		return "Yes"
	}
	return "No"
}
