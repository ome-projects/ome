package v1alpha1

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
)

// ScaleSourceIdentity intentionally excludes private source resourceVersions.
type ScaleSourceIdentity struct {
	Kind       string `json:"kind"`
	Namespace  string `json:"namespace,omitempty"`
	Name       string `json:"name"`
	UID        string `json:"uid"`
	Generation int64  `json:"generation"`
}

func (s ScaleSourceIdentity) displayName() string {
	return (ActionTarget{Kind: s.Kind, Namespace: s.Namespace, Name: s.Name}).displayName()
}

type ScaleInstanceCounts struct {
	Replicas  int32 `json:"replicas"`
	Ready     int32 `json:"ready"`
	Serving   int32 `json:"serving"`
	Available int32 `json:"available"`
}

// ScaleSpecSource is a fixed ownership-origin label, not a Kubernetes spec.
type ScaleSpecSource string

// ScaleActionDetails is the allowlisted logical-Instance action summary. Raw
// PodSpecs, autoscaler payloads, selectors and API messages have no field here.
type ScaleActionDetails struct {
	Component             string                `json:"component"`
	Subresource           string                `json:"subresource"`
	Field                 string                `json:"field"`
	PriorReplicas         int32                 `json:"priorReplicas"`
	RequestedReplicas     int32                 `json:"requestedReplicas"`
	MinReplicas           int32                 `json:"minReplicas"`
	MaxReplicas           int32                 `json:"maxReplicas"`
	Class                 string                `json:"class"`
	ManagedBy             string                `json:"managedBy"`
	SpecSource            ScaleSpecSource       `json:"specSource"`
	Override              bool                  `json:"override"`
	Transient             bool                  `json:"transient"`
	Parent                ScaleSourceIdentity   `json:"parent"`
	Sources               []ScaleSourceIdentity `json:"sources"`
	ReplicaGeneration     int64                 `json:"replicaGeneration"`
	ParentGenerationStamp int64                 `json:"parentGenerationStamp"`
	ParentFreshness       string                `json:"parentFreshness"`
	Instances             ScaleInstanceCounts   `json:"instances"`
	Issues                []string              `json:"issues"`
	Warnings              []string              `json:"warnings"`
}

func (s ScaleActionDetails) canonical() ScaleActionDetails {
	s.Sources = append([]ScaleSourceIdentity(nil), s.Sources...)
	slices.SortFunc(s.Sources, func(a, b ScaleSourceIdentity) int {
		return strings.Compare(a.Kind+"/"+a.Namespace+"/"+a.Name, b.Kind+"/"+b.Namespace+"/"+b.Name)
	})
	s.Issues = append([]string(nil), s.Issues...)
	s.Warnings = append([]string(nil), s.Warnings...)
	slices.Sort(s.Issues)
	slices.Sort(s.Warnings)
	s.Issues = slices.Compact(s.Issues)
	s.Warnings = slices.Compact(s.Warnings)
	return s
}

func (r ActionResult) scaleTable(wide bool) report.Table {
	r = r.Canonical()
	s := r.Scale
	rows := [][]string{{"action", r.Action}, {"target", r.Target.displayName()}, {"component", s.Component},
		{"field", s.Subresource + " " + s.Field}, {"replicas", fmt.Sprintf("%d -> %d", s.PriorReplicas, s.RequestedReplicas)},
		{"bounds", fmt.Sprintf("%d..%d", s.MinReplicas, s.MaxReplicas)}, {"class / owner", s.Class + " / " + s.ManagedBy},
		{"source", string(s.SpecSource)}, {"override", yesNo(s.Override)}, {"transient", yesNo(s.Transient)},
		{"dry-run", string(r.DryRun)}, {"accepted", yesNo(r.Accepted)}, {"applied", yesNo(r.Applied)},
		{"instances", fmt.Sprintf("%d total; %d ready; %d serving; %d available", s.Instances.Replicas, s.Instances.Ready, s.Instances.Serving, s.Instances.Available)},
		{"parent freshness", s.ParentFreshness}, {"message", r.Message}, {"follow-up", r.FollowUp}, {"hint", "Use -o json or -o yaml for full values."}}
	if wide {
		rows = append(rows, []string{"IR UID", r.Target.UID}, []string{"IR RV", r.Target.ResourceVersion},
			[]string{"IR generation", strconv.FormatInt(s.ReplicaGeneration, 10)}, []string{"parent stamp", strconv.FormatInt(s.ParentGenerationStamp, 10)},
			[]string{"parent", s.Parent.displayName()}, []string{"parent UID", s.Parent.UID})
		for _, source := range s.Sources {
			rows = append(rows, []string{"source identity", source.displayName()}, []string{"source UID", source.UID}, []string{"source generation", strconv.FormatInt(source.Generation, 10)})
		}
		for _, issue := range s.Issues {
			rows = append(rows, []string{"issue", issue})
		}
		for _, warning := range s.Warnings {
			rows = append(rows, []string{"warning", warning})
		}
	}
	for i := range rows {
		rows[i][1] = printers.BoundedCell(rows[i][1], 56)
	}
	return report.Table{Headers: []string{"FIELD", "VALUE"}, Rows: rows}
}
