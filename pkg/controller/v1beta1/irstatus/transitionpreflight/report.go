package transitionpreflight

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// ReasonKind is the bounded classification of one no-go reason.
type ReasonKind string

const (
	// ReasonUnreachable: the cluster, or one of its endpoints, could not be
	// read at the transport level.
	ReasonUnreachable ReasonKind = "unreachable"
	// ReasonFailedPage: one InferenceReplica list page failed, so the
	// representation counts are incomplete.
	ReasonFailedPage ReasonKind = "failed_page"
	// ReasonUnexpectedImage: the manager Deployment is missing, has no
	// manager container, or runs an image other than the approved one.
	ReasonUnexpectedImage ReasonKind = "unexpected_image"
	// ReasonConfigMismatch: the omenativeStatus block is missing, invalid,
	// or differs from the expected values.
	ReasonConfigMismatch ReasonKind = "configuration_mismatch"
	// ReasonStaleSchema: the served InferenceReplica schema fails the
	// manager's startup schema preflight.
	ReasonStaleSchema ReasonKind = "stale_schema"
	// ReasonColumnarV2Present: ColumnarV2 objects remain while the target is
	// DenseV1.
	ReasonColumnarV2Present ReasonKind = "columnar_v2_present"
	// ReasonAboveBound: a ColumnarV2 object has more rows than the expected
	// maxDecodedInstances allows.
	ReasonAboveBound ReasonKind = "columnar_v2_above_bound"
	// ReasonUndecodable: a stored representation fails the codec for a
	// reason other than the bound.
	ReasonUndecodable ReasonKind = "columnar_v2_undecodable"
)

// Reason is one no-go finding for one cluster.
type Reason struct {
	Kind   ReasonKind `json:"kind"`
	Detail string     `json:"detail"`
}

// Report is the auditable outcome of one preflight run.
type Report struct {
	InventoryDigest string          `json:"inventoryDigest"`
	Expected        Expectation     `json:"expected"`
	Clusters        []ClusterReport `json:"clusters"`
	Go              bool            `json:"go"`
}

// Expectation echoes the inventory values every cluster was checked against.
type Expectation struct {
	ManagerImage    string               `json:"managerImage"`
	OMENativeStatus ExpectedStatusConfig `json:"omenativeStatus"`
}

// ClusterReport is the outcome for one cluster.
type ClusterReport struct {
	Name      string        `json:"name"`
	Context   string        `json:"context"`
	Reachable bool          `json:"reachable"`
	Manager   ManagerReport `json:"manager"`
	Config    ConfigReport  `json:"config"`
	Schema    SchemaReport  `json:"schema"`
	Replicas  ReplicaReport `json:"replicas"`
	Reasons   []Reason      `json:"reasons"`
}

// ManagerReport describes the manager Deployment as observed.
type ManagerReport struct {
	Found         bool   `json:"found"`
	Image         string `json:"image,omitempty"`
	ExpectedImage string `json:"expectedImage"`
	ImageMatches  bool   `json:"imageMatches"`
	Replicas      int32  `json:"replicas"`
	ReadyReplicas int32  `json:"readyReplicas"`
	Error         string `json:"error,omitempty"`
	transport     bool
}

// ConfigReport describes the omenativeStatus block as observed.
type ConfigReport struct {
	Found     bool                  `json:"found"`
	Observed  *ExpectedStatusConfig `json:"observed,omitempty"`
	Expected  ExpectedStatusConfig  `json:"expected"`
	Matches   bool                  `json:"matches"`
	Error     string                `json:"error,omitempty"`
	transport bool
}

// SchemaReport is the outcome of the schema preflight.
type SchemaReport struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
	stale bool
}

// ReplicaReport is the representation census of one cluster.
type ReplicaReport struct {
	Pages   int `json:"pages"`
	Objects int `json:"objects"`
	DenseV1 int `json:"denseV1"`
	// ColumnarV2 lists every object stored as ColumnarV2.
	ColumnarV2 []string `json:"columnarV2"`
	// AboveBound lists the ColumnarV2 objects whose row count exceeds the
	// expected bound.
	AboveBound []string `json:"columnarV2AboveBound"`
	// Undecodable lists every object whose stored representation fails the
	// codec, with the fixed-catalog reason.
	Undecodable []ObjectProblem `json:"undecodable"`
	// LargestColumnarV2Rows is the largest decoded ColumnarV2 cardinality,
	// the number a replacement bound must fit.
	LargestColumnarV2Rows int          `json:"largestColumnarV2Rows"`
	FailedPage            *PageFailure `json:"failedPage,omitempty"`
}

// ObjectProblem names one object and the reason it failed the codec.
type ObjectProblem struct {
	Object string `json:"object"`
	Reason string `json:"reason"`
}

// String renders the problem for the reason text.
func (p ObjectProblem) String() string { return p.Object + " (" + p.Reason + ")" }

// PageFailure records which list page failed and why.
type PageFailure struct {
	Page  int    `json:"page"`
	Error string `json:"error"`
}

// WriteJSON writes the report as indented JSON.
func (r *Report) WriteJSON(w io.Writer) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(r)
}

// WriteText writes the human-readable report.
func (r *Report) WriteText(w io.Writer) error {
	var b strings.Builder
	fmt.Fprintf(&b, "InferenceReplica status transition preflight\n")
	fmt.Fprintf(&b, "inventory digest: sha256:%s\n", r.InventoryDigest)
	fmt.Fprintf(&b, "expected: image=%s %s\n", r.Expected.ManagerImage, r.Expected.OMENativeStatus)
	failed := 0
	for _, cluster := range r.Clusters {
		verdict := "GO"
		if len(cluster.Reasons) != 0 {
			verdict = "NO-GO"
			failed++
		}
		fmt.Fprintf(&b, "\ncluster %s (context %s): %s\n", cluster.Name, cluster.Context, verdict)
		if !cluster.Reachable {
			for _, reason := range cluster.Reasons {
				fmt.Fprintf(&b, "  - %s: %s\n", reason.Kind, reason.Detail)
			}
			continue
		}
		fmt.Fprintf(&b, "  manager:  %s\n", cluster.Manager.summary())
		fmt.Fprintf(&b, "  config:   %s\n", cluster.Config.summary())
		fmt.Fprintf(&b, "  schema:   %s\n", cluster.Schema.summary())
		fmt.Fprintf(&b, "  replicas: %s\n", cluster.Replicas.summary())
		if len(cluster.Reasons) != 0 {
			fmt.Fprintf(&b, "  reasons:\n")
			for _, reason := range cluster.Reasons {
				fmt.Fprintf(&b, "  - %s: %s\n", reason.Kind, reason.Detail)
			}
		}
	}
	if r.Go {
		fmt.Fprintf(&b, "\nresult: GO (%d clusters)\n", len(r.Clusters))
	} else {
		fmt.Fprintf(&b, "\nresult: NO-GO (%d of %d clusters failed)\n", failed, len(r.Clusters))
	}
	_, err := io.WriteString(w, b.String())
	return err
}

func (m ManagerReport) summary() string {
	if m.Error != "" {
		return "error: " + m.Error
	}
	match := "mismatch"
	if m.ImageMatches {
		match = "match"
	}
	return fmt.Sprintf("image=%s (%s) replicas=%d ready=%d", m.Image, match, m.Replicas, m.ReadyReplicas)
}

func (c ConfigReport) summary() string {
	if c.Error != "" {
		return "error: " + c.Error
	}
	match := "mismatch"
	if c.Matches {
		match = "match"
	}
	return fmt.Sprintf("%s (%s)", c.Observed, match)
}

func (s SchemaReport) summary() string {
	if s.OK {
		return "ok"
	}
	return "error: " + s.Error
}

func (r ReplicaReport) summary() string {
	summary := fmt.Sprintf("pages=%d objects=%d denseV1=%d columnarV2=%d aboveBound=%d undecodable=%d largestColumnarV2Rows=%d",
		r.Pages, r.Objects, r.DenseV1, len(r.ColumnarV2), len(r.AboveBound), len(r.Undecodable), r.LargestColumnarV2Rows)
	if r.FailedPage != nil {
		summary += fmt.Sprintf(" (page %d failed)", r.FailedPage.Page)
	}
	return summary
}
