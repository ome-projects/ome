package v1alpha1

import (
	"fmt"
	"sigs.k8s.io/ome/pkg/cli/report"
)

const (
	QuotaStatusReportKind     = "QuotaStatusReport"
	QuotaValidationReportKind = "QuotaValidationReport"
)

// QuotaValidationContent asserts only the complete fetched declared topology.
type QuotaValidationContent struct {
	Valid    bool             `json:"valid"`
	Topology QuotaTreeContent `json:"topology"`
}

func (c QuotaValidationContent) Canonical() QuotaValidationContent {
	c.Topology = c.Topology.Canonical()
	return c
}
func (c QuotaValidationContent) Table() report.Table {
	t := c.Topology.Table()
	t.Headers = []string{"QUOTA VALIDATION (advisory)"}
	result := "ViolationsDetected"
	if c.Valid {
		result = "NoProblemsDetected"
	}
	t.Rows = append([][]string{{"Result: " + result + "; complete fetched snapshot only."}}, t.Rows...)
	return t
}

// QuotaStatusGroup records inspected and displayed counts independently.
// Invalid/oversize groups expose no prefix that could hide contradictory data.
type QuotaStatusGroup struct {
	State     string        `json:"state"`
	Reason    string        `json:"reason"`
	Evidence  EvidenceLevel `json:"evidence"`
	Observed  int           `json:"observed"`
	Displayed int           `json:"displayed"`
	Truncated bool          `json:"truncated"`
}

type QuotaStatusContent struct {
	Target       string              `json:"target,omitempty"`
	Snapshot     QuotaTreeSnapshot   `json:"snapshot"`
	ObjectsGroup QuotaStatusGroup    `json:"objectsGroup"`
	Objects      []QuotaStatusObject `json:"objects"`
}

// Zero optional nonpointer scalars lose wire presence during API decoding.
// Their text is Unknown/Defaulted, never proof of free capacity or no usage.
type QuotaStatusObject struct {
	Name                  string                      `json:"name"`
	Generation            int64                       `json:"generation"`
	ObservedGeneration    int64                       `json:"observedGeneration"`
	Freshness             string                      `json:"freshness"`
	ReportedParent        string                      `json:"reportedParent,omitempty"`
	ParentState           string                      `json:"parentState"`
	SourceGeneration      int64                       `json:"sourceGeneration"`
	SourceGenerationState string                      `json:"sourceGenerationState"`
	Evidence              EvidenceLevel               `json:"evidence"`
	BudgetsGroup          QuotaStatusGroup            `json:"budgetsGroup"`
	Budgets               []QuotaStatusBudget         `json:"budgets"`
	CapacityGroup         QuotaStatusGroup            `json:"capacityGroup"`
	Capacity              []QuotaStatusCapacity       `json:"capacity"`
	ProjectionsGroup      QuotaStatusGroup            `json:"projectionsGroup"`
	Projections           []QuotaStatusProjection     `json:"projections"`
	MaterializationGroup  QuotaStatusGroup            `json:"materializationGroup"`
	Materialization       *QuotaStatusMaterialization `json:"materialization,omitempty"`
	ConditionsGroup       QuotaStatusGroup            `json:"conditionsGroup"`
	Conditions            []QuotaStatusCondition      `json:"conditions"`
}

type QuotaStatusUsage struct {
	Nominal  string `json:"nominal"`
	Admitted string `json:"admitted"`
	Reserved string `json:"reserved"`
	Borrowed string `json:"borrowed"`
}
type QuotaStatusClusterUsage struct {
	Cluster string           `json:"cluster"`
	Usage   QuotaStatusUsage `json:"usage"`
}
type QuotaStatusBudget struct {
	ResourceName    string                    `json:"resourceName"`
	ResourceFlavor  string                    `json:"resourceFlavor"`
	Usage           QuotaStatusUsage          `json:"usage"`
	PerClusterGroup QuotaStatusGroup          `json:"perClusterGroup"`
	PerCluster      []QuotaStatusClusterUsage `json:"perCluster"`
}
type QuotaStatusClusterCapacity struct {
	Cluster       string `json:"cluster"`
	Allocatable   string `json:"allocatable"`
	HighWaterMark string `json:"highWaterMark"`
	ObservedAt    string `json:"observedAt,omitempty"`
	SampleState   string `json:"sampleState"`
}
type QuotaStatusCapacity struct {
	ResourceName    string                       `json:"resourceName"`
	ResourceFlavor  string                       `json:"resourceFlavor"`
	Allocatable     string                       `json:"allocatable"`
	HighWaterMark   string                       `json:"highWaterMark"`
	ObservedAt      string                       `json:"observedAt,omitempty"`
	SampleState     string                       `json:"sampleState"`
	PerClusterGroup QuotaStatusGroup             `json:"perClusterGroup"`
	PerCluster      []QuotaStatusClusterCapacity `json:"perCluster"`
}
type QuotaStatusProjection struct {
	Cluster                  string `json:"cluster"`
	AppliedGeneration        int64  `json:"appliedGeneration"`
	AppliedTime              string `json:"appliedTime,omitempty"`
	ProjectionFreshness      string `json:"projectionFreshness"`
	MaterializedGeneration   int64  `json:"materializedGeneration"`
	MaterializationFreshness string `json:"materializationFreshness"`
}
type QuotaStatusMaterialization struct {
	FreezeState           string `json:"freezeState"`
	FrozenAt              string `json:"frozenAt,omitempty"`
	Reason                string `json:"reason"`
	LastAppliedGeneration int64  `json:"lastAppliedGeneration"`
	LastAppliedTime       string `json:"lastAppliedTime,omitempty"`
	Freshness             string `json:"freshness"`
}
type QuotaStatusCondition struct {
	Type               string `json:"type"`
	Status             string `json:"status"`
	Reason             string `json:"reason"`
	ObservedGeneration int64  `json:"observedGeneration"`
	Freshness          string `json:"freshness"`
	LastTransitionTime string `json:"lastTransitionTime,omitempty"`
}

func (c QuotaStatusContent) Canonical() QuotaStatusContent {
	c.Objects = append([]QuotaStatusObject{}, c.Objects...)
	for i := range c.Objects {
		o := &c.Objects[i]
		o.Budgets = append([]QuotaStatusBudget{}, o.Budgets...)
		for j := range o.Budgets {
			b := &o.Budgets[j]
			b.PerCluster = append([]QuotaStatusClusterUsage{}, b.PerCluster...)
			quotaSort(b.PerCluster)
		}
		quotaSort(o.Budgets)
		o.Capacity = append([]QuotaStatusCapacity{}, o.Capacity...)
		for j := range o.Capacity {
			b := &o.Capacity[j]
			b.PerCluster = append([]QuotaStatusClusterCapacity{}, b.PerCluster...)
			quotaSort(b.PerCluster)
		}
		quotaSort(o.Capacity)
		o.Projections = append([]QuotaStatusProjection{}, o.Projections...)
		quotaSort(o.Projections)
		o.Conditions = append([]QuotaStatusCondition{}, o.Conditions...)
		quotaSort(o.Conditions)
		if o.Materialization != nil {
			value := *o.Materialization
			o.Materialization = &value
		}
	}
	quotaSort(c.Objects)
	return c
}

func (c QuotaStatusContent) Table() report.Table {
	c = c.Canonical()
	rows := [][]string{{"Reported evidence only; no enforcement or free-capacity claim."}, {fmt.Sprintf("Snapshot: %s; %d items / %d pages", c.Snapshot.Completeness, c.Snapshot.ObservedItems, c.Snapshot.ObservedPages)}}
	if len(c.Objects) == 0 {
		rows = append(rows, []string{"No AcceleratorQuotas observed."})
	}
	if c.ObjectsGroup.Truncated {
		rows = append(rows, []string{fmt.Sprintf("Objects truncated: showing %d of %d; use a named status GET.", c.ObjectsGroup.Displayed, c.ObjectsGroup.Observed)})
	}
	for _, o := range c.Objects {
		rows = append(rows, []string{fmt.Sprintf("%s: generation=%d observed=%d %s (Reported)", o.Name, o.Generation, o.ObservedGeneration, o.Freshness)}, []string{fmt.Sprintf("  parent=%s (%s); sourceGeneration=%d (%s)", o.ReportedParent, o.ParentState, o.SourceGeneration, o.SourceGenerationState)})
		for _, g := range []struct {
			name  string
			group QuotaStatusGroup
		}{{"budgets", o.BudgetsGroup}, {"capacity", o.CapacityGroup}, {"projections", o.ProjectionsGroup}, {"materialization", o.MaterializationGroup}, {"conditions", o.ConditionsGroup}} {
			line := fmt.Sprintf("  %s: %s/%s", g.name, g.group.State, g.group.Reason)
			if g.group.Truncated {
				line += fmt.Sprintf("; truncated %d/%d", g.group.Displayed, g.group.Observed)
			}
			rows = append(rows, []string{line})
		}
		for _, b := range o.Budgets {
			rows = append(rows, []string{fmt.Sprintf("  budget %s/%s nominal=%s admitted=%s", b.ResourceName, b.ResourceFlavor, b.Usage.Nominal, b.Usage.Admitted)}, []string{fmt.Sprintf("    reserved=%s borrowed=%s; clusters=%s/%s", b.Usage.Reserved, b.Usage.Borrowed, b.PerClusterGroup.State, b.PerClusterGroup.Reason)})
			if b.PerClusterGroup.Truncated {
				rows = append(rows, []string{fmt.Sprintf("    clusters truncated %d/%d", b.PerClusterGroup.Displayed, b.PerClusterGroup.Observed)})
			}
			for _, cluster := range b.PerCluster {
				rows = append(rows, []string{fmt.Sprintf("    cluster %s nominal=%s admitted=%s reserved=%s", cluster.Cluster, cluster.Usage.Nominal, cluster.Usage.Admitted, cluster.Usage.Reserved)}, []string{"      borrowed=" + cluster.Usage.Borrowed})
			}
		}
		for _, b := range o.Capacity {
			rows = append(rows, []string{fmt.Sprintf("  capacity %s/%s allocatable=%s highWater=%s", b.ResourceName, b.ResourceFlavor, b.Allocatable, b.HighWaterMark)}, []string{"    sampled=" + b.ObservedAt + " (" + b.SampleState + "); highWater is historical"}, []string{"    clusters=" + b.PerClusterGroup.State + "/" + b.PerClusterGroup.Reason})
			if b.PerClusterGroup.Truncated {
				rows = append(rows, []string{fmt.Sprintf("    clusters truncated %d/%d", b.PerClusterGroup.Displayed, b.PerClusterGroup.Observed)})
			}
			for _, cluster := range b.PerCluster {
				rows = append(rows, []string{fmt.Sprintf("    cluster %s allocatable=%s highWater=%s", cluster.Cluster, cluster.Allocatable, cluster.HighWaterMark)}, []string{"      sampled=" + cluster.ObservedAt + " (" + cluster.SampleState + ")"})
			}
		}
		for _, p := range o.Projections {
			rows = append(rows, []string{fmt.Sprintf("  projection %s applied=%d %s; materialized=%d %s", p.Cluster, p.AppliedGeneration, p.ProjectionFreshness, p.MaterializedGeneration, p.MaterializationFreshness)})
		}
		if m := o.Materialization; m != nil {
			rows = append(rows, []string{fmt.Sprintf("  output=%s reason=%s; lastApplied=%d %s", m.FreezeState, m.Reason, m.LastAppliedGeneration, m.Freshness)})
		}
		for _, condition := range o.Conditions {
			rows = append(rows, []string{fmt.Sprintf("  %s=%s reason=%s %s", condition.Type, condition.Status, condition.Reason, condition.Freshness)})
		}
	}
	if len(c.Objects) > 0 {
		rows = append(rows, []string{"Zero optional scalars: Unknown/Defaulted; not proof of zero usage."}, []string{"Compact identities/lines may be clipped; -o wide retains safe fields."}, []string{"Computed ancestry: quota tree; no reported status path exists."})
	}
	for i := range rows {
		rows[i][0] = quotaClip(rows[i][0])
	}
	return report.Table{Headers: []string{"QUOTA STATUS (reported)"}, Rows: rows}
}

func QuotaStatusWideTable(e Envelope[QuotaStatusContent]) report.Table {
	e = e.Canonical()
	rows := [][]string{{"apiVersion", e.APIVersion}, {"report", e.Kind}, {"metadata", quotaKey(e.Metadata)}, {"target", e.Content.Target}, {"collectedAt", e.CollectedAt.Format("2006-01-02T15:04:05.999999999Z07:00")}, {"snapshot", quotaKey(e.Content.Snapshot)}, {"objectsGroup", quotaKey(e.Content.ObjectsGroup)}}
	for _, o := range e.Content.Objects {
		rows = append(rows, []string{"object", quotaKey(o)})
	}
	for _, s := range e.Sources {
		rows = append(rows, []string{"source", quotaKey(s)})
	}
	for _, w := range e.Warnings {
		rows = append(rows, []string{"warning", quotaKey(w)})
	}
	return report.Table{Headers: []string{"FIELD", "VALUE"}, Rows: rows}
}

func QuotaValidationWideTable(e Envelope[QuotaValidationContent]) report.Table {
	topology := Envelope[QuotaTreeContent]{APIVersion: e.APIVersion, Kind: e.Kind, Metadata: e.Metadata, CollectedAt: e.CollectedAt, Sources: e.Sources, Content: e.Content.Topology, Warnings: e.Warnings}
	t := QuotaTreeWideTable(topology)
	t.Rows = append([][]string{{"valid", fmt.Sprint(e.Content.Valid)}}, t.Rows...)
	return t
}
