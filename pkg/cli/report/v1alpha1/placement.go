package v1alpha1

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"time"

	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
)

const (
	KindPlacementStatus   = "PlacementStatusReport"
	KindPlacementExplain  = "PlacementExplainReport"
	KindPlacementEndpoint = "PlacementEndpointReport"
)

// PlacementValue is a closed, CLI-produced evidence value, never raw config.
type PlacementValue string

// PlacementEvidence dates only its own source, not unrelated placement state.
type PlacementEvidence struct {
	Evidence  EvidenceLevel  `json:"evidence"`
	Freshness PlacementValue `json:"freshness"`
	Reason    PlacementValue `json:"reason,omitempty"`
}

// PlacementAcquisition records bounded source reads explicitly.
type PlacementAcquisition struct {
	State     PlacementValue `json:"state"`
	Reason    PlacementValue `json:"reason,omitempty"`
	Returned  int            `json:"returned"`
	Admitted  int            `json:"admitted"`
	Pages     int            `json:"pages"`
	Complete  bool           `json:"complete"`
	Truncated bool           `json:"truncated"`
}

// PlacementPreview distinguishes full validation from display clipping.
type PlacementPreview struct {
	State     PlacementValue `json:"state"`
	Total     int            `json:"total"`
	Kept      int            `json:"kept"`
	Truncated bool           `json:"truncated"`
}

// PlacementSelector never contains label values or a raw selector string.
type PlacementSelector struct {
	Present bool           `json:"present"`
	State   PlacementValue `json:"state"`
}

type PlacementInputs struct {
	Source                       PlacementValue    `json:"source"`
	Mode                         PlacementValue    `json:"mode"`
	ModeEvidence                 PlacementValue    `json:"modeEvidence"`
	Requirements                 PlacementSelector `json:"requirements"`
	ClusterSelector              PlacementSelector `json:"clusterSelector"`
	LegacyRequirementsPresent    bool              `json:"legacyRequirementsPresent"`
	LegacyClusterSelectorPresent bool              `json:"legacyClusterSelectorPresent"`
	RequirementsState            PlacementValue    `json:"requirementsState"`
	Split                        *PlacementSplit   `json:"split,omitempty"`
}

// PlacementCount does not turn optional scalar zero into observed absence.
type PlacementCount struct {
	Value *int32         `json:"value,omitempty"`
	State PlacementValue `json:"state"`
}

type PlacementSplit struct {
	Replicas              PlacementCount `json:"replicas"`
	Spread                PlacementValue `json:"spread"`
	MaxReplicasPerCluster PlacementCount `json:"maxReplicasPerCluster"`
	MinReplicasPerCluster PlacementCount `json:"minReplicasPerCluster"`
}

// PlacementAddress is intentionally origin-only, not a callable full URL.
type PlacementAddress struct {
	State          PlacementValue    `json:"state"`
	EndpointOrigin string            `json:"endpointOrigin,omitempty"`
	PathPresent    bool              `json:"pathPresent"`
	Source         PlacementEvidence `json:"source"`
}

type PlacementPolicy struct {
	Name           string         `json:"name"`
	PortableDigest string         `json:"portableDigest,omitempty"`
	DigestState    PlacementValue `json:"digestState"`
}

type PlacementComponent struct {
	Component      PlacementValue `json:"component"`
	ResolvedDigest string         `json:"resolvedDigest,omitempty"`
	DigestState    PlacementValue `json:"digestState"`
	Ready          PlacementValue `json:"ready"`
}

type PlacementRolloutGroup struct {
	Ordinal        int            `json:"ordinal"`
	Source         PlacementValue `json:"source"`
	PolicyName     string         `json:"policyName,omitempty"`
	PortableDigest string         `json:"portableDigest,omitempty"`
	DigestState    PlacementValue `json:"digestState"`
}

type PlacementProvenance struct {
	State            PlacementValue          `json:"state"`
	Source           PlacementEvidence       `json:"source"`
	Policies         []PlacementPolicy       `json:"policies"`
	PolicyPreview    PlacementPreview        `json:"policyPreview"`
	Components       []PlacementComponent    `json:"components"`
	AutoscalingReady PlacementValue          `json:"autoscalingReady"`
	ActiveRunPresent bool                    `json:"activeRunPresent"`
	ActiveGroups     []PlacementRolloutGroup `json:"activeGroups"`
	GroupPreview     PlacementPreview        `json:"groupPreview"`
	LastOutcome      PlacementValue          `json:"lastOutcome"`
	LastDigest       string                  `json:"lastDigest,omitempty"`
	LastDigestState  PlacementValue          `json:"lastDigestState"`
}

type PlacementHome struct {
	Cluster          string              `json:"cluster"`
	Phase            PlacementValue      `json:"phase"`
	Address          PlacementAddress    `json:"address"`
	AdmittedReplicas PlacementCount      `json:"admittedReplicas"`
	ReadyReplicas    PlacementCount      `json:"readyReplicas"`
	Provenance       PlacementProvenance `json:"provenance"`
	Source           PlacementEvidence   `json:"source"`
}

type PlacementReported struct {
	Phase             PlacementValue    `json:"phase"`
	ReportedCluster   string            `json:"reportedCluster,omitempty"`
	Source            PlacementEvidence `json:"source"`
	Address           PlacementAddress  `json:"address"`
	ServiceAddress    PlacementAddress  `json:"serviceAddress"`
	Homes             []PlacementHome   `json:"homes"`
	HomePreview       PlacementPreview  `json:"homePreview"`
	ProvenancePreview PlacementPreview  `json:"provenancePreview"`
}

type PlacementIssue struct {
	Group PlacementValue `json:"group"`
	Code  PlacementValue `json:"code"`
	Count int            `json:"count"`
}

type PlacementStatusContent struct {
	Inputs    PlacementInputs   `json:"inputs"`
	Placement PlacementReported `json:"placement"`
	Issues    []PlacementIssue  `json:"issues"`
}

type PlacementCondition struct {
	Type   PlacementValue    `json:"type"`
	Status PlacementValue    `json:"status"`
	Reason PlacementValue    `json:"reason"`
	Source PlacementEvidence `json:"source"`
}

type PlacementCluster struct {
	Name                       string             `json:"name"`
	ComputedSelectorCompatible PlacementValue     `json:"computedSelectorCompatible"`
	ReportedHome               PlacementValue     `json:"reportedHome"`
	ReportedReady              PlacementCondition `json:"reportedReady"`
	ConnectionSource           PlacementValue     `json:"connectionSource"`
	ConditionPreview           PlacementPreview   `json:"conditionPreview"`
}

// PlacementRoutingCapacityFactors describes where the declared per-home
// capacity overrides came from without copying cluster names or quantities.
type PlacementRoutingCapacityFactors struct {
	Source PlacementValue `json:"source"`
	Count  int            `json:"count"`
}

// PlacementRoutingProbeIntent is a closed, non-sensitive summary of the
// declared endpoint-probe contract. Path contents are deliberately omitted.
type PlacementRoutingProbeIntent struct {
	State             PlacementValue `json:"state"`
	PathPresent       bool           `json:"pathPresent"`
	Method            PlacementValue `json:"method"`
	AcceptStatusCount int            `json:"acceptStatusCount"`
	GateStatusCount   int            `json:"gateStatusCount"`
	Period            string         `json:"period,omitempty"`
	Timeout           string         `json:"timeout,omitempty"`
	FailureThreshold  int32          `json:"failureThreshold,omitempty"`
	SuccessThreshold  int32          `json:"successThreshold,omitempty"`
	AllFailedPolicy   PlacementValue `json:"allFailedPolicy"`
}

// PlacementRoutingCapacityIntent is a closed, non-sensitive summary of the
// declared capacity-poll contract. Paths, format names, keys, and values are
// never copied into a report.
type PlacementRoutingCapacityIntent struct {
	State         PlacementValue `json:"state"`
	PathPresent   bool           `json:"pathPresent"`
	Method        PlacementValue `json:"method"`
	FormatPresent bool           `json:"formatPresent"`
	OptionCount   int            `json:"optionCount"`
	Period        string         `json:"period,omitempty"`
	Timeout       string         `json:"timeout,omitempty"`
	Samples       int32          `json:"samples,omitempty"`
	Quorum        int32          `json:"quorum,omitempty"`
	MaxAge        string         `json:"maxAge,omitempty"`
}

// PlacementRoutingPublisherIntent reports only whether per-service publisher
// options were declared and how many; option keys and values remain private.
type PlacementRoutingPublisherIntent struct {
	State       PlacementValue `json:"state"`
	OptionCount int            `json:"optionCount"`
}

// PlacementRoutingIntent reports only declaration evidence. Operator-level
// defaults and data-plane realization require separate observations.
type PlacementRoutingIntent struct {
	State           PlacementValue                  `json:"state"`
	Enablement      PlacementValue                  `json:"enablement"`
	CapacityFactors PlacementRoutingCapacityFactors `json:"capacityFactors"`
	Probe           PlacementRoutingProbeIntent     `json:"probe"`
	Capacity        PlacementRoutingCapacityIntent  `json:"capacity"`
	Publisher       PlacementRoutingPublisherIntent `json:"publisher"`
}

type PlacementExplainContent struct {
	Status           PlacementStatusContent `json:"status"`
	Routing          PlacementRoutingIntent `json:"routing"`
	Fleet            PlacementAcquisition   `json:"fleet"`
	Clusters         []PlacementCluster     `json:"clusters"`
	UnobservedInputs []PlacementValue       `json:"unobservedInputs"`
	Issues           []PlacementIssue       `json:"issues"`
}

type PlacementCapacity struct {
	Allocated      PlacementCount `json:"allocated"`
	Ready          PlacementCount `json:"ready"`
	Factor         string         `json:"factor,omitempty"`
	FactorState    PlacementValue `json:"factorState"`
	Source         PlacementValue `json:"source"`
	Reported       PlacementCount `json:"reported"`
	FallbackReason PlacementValue `json:"fallbackReason"`
}

type PlacementProbe struct {
	Result              PlacementValue    `json:"result"`
	PolicyDigest        string            `json:"policyDigest,omitempty"`
	PolicyDigestState   PlacementValue    `json:"policyDigestState"`
	Gated               bool              `json:"gated"`
	GatedState          PlacementValue    `json:"gatedState"`
	LastAttemptTime     *time.Time        `json:"lastAttemptTime,omitempty"`
	ConsecutiveFailures PlacementCount    `json:"consecutiveFailures"`
	Source              PlacementEvidence `json:"source"`
}

type PlacementRoute struct {
	Cluster      string             `json:"cluster"`
	Address      PlacementAddress   `json:"address"`
	Weight       int32              `json:"weight"`
	Healthy      bool               `json:"healthy"`
	DrainRefs    []string           `json:"drainRefs"`
	DrainPreview PlacementPreview   `json:"drainPreview"`
	Capacity     *PlacementCapacity `json:"capacity,omitempty"`
	Probe        PlacementProbe     `json:"probe"`
	Source       PlacementEvidence  `json:"source"`
}

type PlacementGateway struct {
	Group     string         `json:"group,omitempty"`
	Kind      string         `json:"kind"`
	Namespace string         `json:"namespace"`
	Name      string         `json:"name"`
	State     PlacementValue `json:"state"`
}

type PlacementRouting struct {
	Acquisition           PlacementAcquisition `json:"acquisition"`
	Mode                  PlacementValue       `json:"mode"`
	Source                PlacementEvidence    `json:"source"`
	EntryPreview          PlacementPreview     `json:"entryPreview"`
	ProbePreview          PlacementPreview     `json:"probePreview"`
	Routable              PlacementCondition   `json:"routable"`
	OverrideActive        PlacementCondition   `json:"overrideActive"`
	CapacityFallback      PlacementCondition   `json:"capacityFallback"`
	Acknowledgement       PlacementValue       `json:"acknowledgement"`
	AcknowledgementSource PlacementEvidence    `json:"acknowledgementSource"`
	Published             PlacementCondition   `json:"published"`
	Gateway               *PlacementGateway    `json:"gateway,omitempty"`
}

type PlacementEndpointContent struct {
	Status           PlacementStatusContent `json:"status"`
	Routing          PlacementRouting       `json:"routing"`
	Entries          []PlacementRoute       `json:"entries"`
	Conditions       []PlacementCondition   `json:"conditions"`
	ConditionPreview PlacementPreview       `json:"conditionPreview"`
	Issues           []PlacementIssue       `json:"issues"`
}

type PlacementStatusReport = Envelope[PlacementStatusContent]
type PlacementExplainReport = Envelope[PlacementExplainContent]
type PlacementEndpointReport = Envelope[PlacementEndpointContent]

func (c PlacementStatusContent) Canonical() PlacementStatusContent {
	if c.Inputs.Split != nil {
		split := *c.Inputs.Split
		split.Replicas = placementCopyCount(split.Replicas)
		split.MaxReplicasPerCluster = placementCopyCount(split.MaxReplicasPerCluster)
		split.MinReplicasPerCluster = placementCopyCount(split.MinReplicasPerCluster)
		c.Inputs.Split = &split
	}
	c.Placement.Homes = append([]PlacementHome{}, c.Placement.Homes...)
	for i := range c.Placement.Homes {
		c.Placement.Homes[i].AdmittedReplicas = placementCopyCount(c.Placement.Homes[i].AdmittedReplicas)
		c.Placement.Homes[i].ReadyReplicas = placementCopyCount(c.Placement.Homes[i].ReadyReplicas)
		p := &c.Placement.Homes[i].Provenance
		p.Policies = append([]PlacementPolicy{}, p.Policies...)
		p.Components = append([]PlacementComponent{}, p.Components...)
		p.ActiveGroups = append([]PlacementRolloutGroup{}, p.ActiveGroups...)
		sort.Slice(p.Policies, func(i, j int) bool { return placementTypedKey(p.Policies[i]) < placementTypedKey(p.Policies[j]) })
		sort.Slice(p.Components, func(i, j int) bool { return placementTypedKey(p.Components[i]) < placementTypedKey(p.Components[j]) })
	}
	sort.Slice(c.Placement.Homes, func(i, j int) bool {
		return placementTypedKey(c.Placement.Homes[i]) < placementTypedKey(c.Placement.Homes[j])
	})
	c.Issues = placementCanonicalIssues(c.Issues)
	return c
}

func (c PlacementExplainContent) Canonical() PlacementExplainContent {
	c.Status = c.Status.Canonical()
	c.Routing = placementCanonicalRoutingIntent(c.Routing)
	c.Clusters = append([]PlacementCluster{}, c.Clusters...)
	sort.Slice(c.Clusters, func(i, j int) bool { return placementTypedKey(c.Clusters[i]) < placementTypedKey(c.Clusters[j]) })
	c.UnobservedInputs = append([]PlacementValue{}, c.UnobservedInputs...)
	c.Issues = placementCanonicalIssues(c.Issues)
	return c
}

func placementCanonicalRoutingIntent(in PlacementRoutingIntent) PlacementRoutingIntent {
	in.State = placementClosedValue(in.State, "Absent", "Declared", "Invalid", "BudgetExceeded")
	in.Enablement = placementClosedValue(in.Enablement, "Inherited", "OptIn", "OptOut") // codespell:ignore optin
	in.CapacityFactors.Source = placementClosedValue(
		in.CapacityFactors.Source, "Inherited", "Routing", "LegacyPlacement", "Conflict",
	)
	if in.CapacityFactors.Count < 0 {
		in.CapacityFactors.Count = 0
	}
	in.Probe.State = placementClosedValue(in.Probe.State, "Inherited", "Disabled", "Configured")
	in.Probe.Method = placementClosedValue(in.Probe.Method, "NotApplicable", "GET", "HEAD", "POST", "Unknown")
	in.Probe.AllFailedPolicy = placementClosedValue(
		in.Probe.AllFailedPolicy, "NotApplicable", "PreserveTraffic", "Drain", "Unknown",
	)
	if in.Probe.AcceptStatusCount < 0 {
		in.Probe.AcceptStatusCount = 0
	}
	if in.Probe.GateStatusCount < 0 {
		in.Probe.GateStatusCount = 0
	}
	in.Probe.Period = placementDuration(in.Probe.Period)
	in.Probe.Timeout = placementDuration(in.Probe.Timeout)
	in.Capacity.State = placementClosedValue(in.Capacity.State, "Inherited", "Disabled", "Configured")
	in.Capacity.Method = placementClosedValue(in.Capacity.Method, "NotApplicable", "GET", "HEAD", "POST", "Unknown")
	if in.Capacity.OptionCount < 0 {
		in.Capacity.OptionCount = 0
	}
	in.Capacity.Period = placementDuration(in.Capacity.Period)
	in.Capacity.Timeout = placementDuration(in.Capacity.Timeout)
	in.Capacity.MaxAge = placementDuration(in.Capacity.MaxAge)
	in.Publisher.State = placementClosedValue(in.Publisher.State, "Inherited", "Configured")
	if in.Publisher.OptionCount < 0 {
		in.Publisher.OptionCount = 0
	}
	return in
}

func placementClosedValue(value PlacementValue, allowed ...PlacementValue) PlacementValue {
	for _, candidate := range allowed {
		if value == candidate {
			return value
		}
	}
	return "Unknown"
}

func placementDuration(value string) string {
	if value == "" {
		return ""
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration == 0 {
		return ""
	}
	return duration.String()
}

func (c PlacementEndpointContent) Canonical() PlacementEndpointContent {
	c.Status = c.Status.Canonical()
	c.Entries = append([]PlacementRoute{}, c.Entries...)
	for i := range c.Entries {
		e := &c.Entries[i]
		e.DrainRefs = append([]string{}, e.DrainRefs...)
		sort.Strings(e.DrainRefs)
		if e.Capacity != nil {
			capacity := *e.Capacity
			capacity.Allocated = placementCopyCount(capacity.Allocated)
			capacity.Ready = placementCopyCount(capacity.Ready)
			capacity.Reported = placementCopyCount(capacity.Reported)
			e.Capacity = &capacity
		}
		e.Probe.ConsecutiveFailures = placementCopyCount(e.Probe.ConsecutiveFailures)
		if e.Probe.LastAttemptTime != nil {
			value := e.Probe.LastAttemptTime.UTC()
			e.Probe.LastAttemptTime = &value
		}
	}
	if c.Routing.Gateway != nil {
		gateway := *c.Routing.Gateway
		c.Routing.Gateway = &gateway
	}
	sort.Slice(c.Entries, func(i, j int) bool { return placementTypedKey(c.Entries[i]) < placementTypedKey(c.Entries[j]) })
	c.Conditions = append([]PlacementCondition{}, c.Conditions...)
	sort.Slice(c.Conditions, func(i, j int) bool { return placementTypedKey(c.Conditions[i]) < placementTypedKey(c.Conditions[j]) })
	c.Issues = placementCanonicalIssues(c.Issues)
	return c
}

func placementCopyCount(in PlacementCount) PlacementCount {
	if in.Value != nil {
		value := *in.Value
		in.Value = &value
	}
	return in
}

func placementCanonicalIssues(in []PlacementIssue) []PlacementIssue {
	out := append([]PlacementIssue{}, in...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Group != out[j].Group {
			return out[i].Group < out[j].Group
		}
		if out[i].Code != out[j].Code {
			return out[i].Code < out[j].Code
		}
		return out[i].Count < out[j].Count
	})
	return out
}

// Struct JSON frames every typed field without delimiter collisions.
func placementTypedKey[T any](value T) string { data, _ := json.Marshal(value); return string(data) }

func (c PlacementStatusContent) Table() report.Table { return c.placementStatusTable(false) }

func placementSummary(c PlacementStatusContent) [][]string {
	rows := [][]string{
		{"Mode", string(c.Inputs.Mode) + " (" + string(c.Inputs.ModeEvidence) + ")"},
		{"Placement", string(c.Placement.Phase) + " (reported)"},
		{"Freshness", string(c.Placement.Source.Freshness)},
		{"Input source", string(c.Inputs.Source)},
		{"Selectors", string(c.Inputs.RequirementsState)},
		{"Reported cluster", c.Placement.ReportedCluster},
		{"Endpoint origin", placementAddressCell(c.Placement.Address)},
		{"Service origin", placementAddressCell(c.Placement.ServiceAddress)},
		{"Home inspection", placementPreviewCell(c.Placement.HomePreview)},
		{"Provenance inspect", placementPreviewCell(c.Placement.ProvenancePreview)},
	}
	return append(rows, placementIssueRows(c.Issues)...)
}

func placementAddressCell(address PlacementAddress) string {
	if address.EndpointOrigin == "" {
		return string(address.State)
	}
	return address.EndpointOrigin
}
func placementPreviewCell(preview PlacementPreview) string {
	return string(preview.State) + " " + strconv.Itoa(preview.Kept) + "/" + strconv.Itoa(preview.Total) + "; truncated=" + strconv.FormatBool(preview.Truncated)
}
func placementIssueRows(issues []PlacementIssue) [][]string {
	rows := [][]string{}
	for _, issue := range placementCanonicalIssues(issues) {
		rows = append(rows, []string{"Issue", string(issue.Group) + ": " + string(issue.Code) + " (" + strconv.Itoa(issue.Count) + ")"})
	}
	return rows
}
func placementAcquisitionRows(prefix string, acquisition PlacementAcquisition) [][]string {
	return [][]string{{prefix + " state", string(acquisition.State)}, {prefix + " reason", string(acquisition.Reason)}, {prefix + " Returned", strconv.Itoa(acquisition.Returned)}, {prefix + " Admitted", strconv.Itoa(acquisition.Admitted)}, {prefix + " Pages", strconv.Itoa(acquisition.Pages)}, {prefix + " Complete", strconv.FormatBool(acquisition.Complete)}, {prefix + " Truncated", strconv.FormatBool(acquisition.Truncated)}}
}

func placementTable(rows [][]string) report.Table {
	for i := range rows {
		rows[i][0] = printers.BoundedCell(rows[i][0], 18)
		rows[i][1] = printers.BoundedCell(rows[i][1], 56)
	}
	return report.Table{Headers: []string{"FIELD", "VALUE"}, Rows: rows}
}

func (c PlacementStatusContent) WideTable() report.Table {
	return c.placementStatusTable(true)
}

func (c PlacementStatusContent) placementStatusTable(wide bool) report.Table {
	rows := placementSummary(c)
	homes := c.Canonical().Placement.Homes
	if !wide && len(homes) > 4 {
		rows = append(rows, []string{"Home preview", "4/" + strconv.Itoa(len(homes)) + "; use -o wide or json"})
		homes = homes[:4]
	}
	for _, h := range homes {
		rows = append(rows, []string{"Reported home", h.Cluster + " (" + string(h.Phase) + ")"}, []string{"Home origin", placementAddressCell(h.Address)})
		if wide {
			rows = append(rows, []string{"Admitted replicas", placementCountCell(h.AdmittedReplicas)}, []string{"Ready replicas", placementCountCell(h.ReadyReplicas)}, []string{"Provenance", string(h.Provenance.State) + "; freshness unverifiable"})
			rows = append(rows, []string{"Policy inspection", placementPreviewCell(h.Provenance.PolicyPreview)}, []string{"Group inspection", placementPreviewCell(h.Provenance.GroupPreview)})
		}
	}
	rows = append(rows, []string{"Hint", "Placed does not prove replica floor met"}, []string{"View", "Bounded cells; use -o json for complete identities"})
	return placementTable(rows)
}

func (c PlacementExplainContent) Table() report.Table {
	return c.placementExplainTable(false)
}

func (c PlacementExplainContent) placementExplainTable(wide bool) report.Table {
	canonical := c.Canonical()
	routing := canonical.Routing
	rows := placementSummary(canonical.Status)
	rows = append(rows,
		[]string{"Routing intent", string(routing.State)},
		[]string{"Routing enablement", string(routing.Enablement)},
		[]string{"Install routing", "Gate/defaults unobserved"},
	)
	if routing.State != "Absent" {
		rows = append(rows,
			[]string{"Capacity factors", string(routing.CapacityFactors.Source) + " (" + strconv.Itoa(routing.CapacityFactors.Count) + ")"},
			[]string{"Routing probe", string(routing.Probe.State)},
			[]string{"Capacity poll", string(routing.Capacity.State)},
			[]string{"Publisher options", string(routing.Publisher.State) + " (" + strconv.Itoa(routing.Publisher.OptionCount) + ")"},
		)
	}
	if wide {
		if routing.Probe.State == "Configured" {
			rows = append(rows,
				[]string{"Probe method", string(routing.Probe.Method) + "; path=" + strconv.FormatBool(routing.Probe.PathPresent)},
				[]string{"Probe statuses", "accept=" + strconv.Itoa(routing.Probe.AcceptStatusCount) + "; gate=" + strconv.Itoa(routing.Probe.GateStatusCount)},
				[]string{"Probe thresholds", "failure=" + placementOptionalInt32Cell(routing.Probe.FailureThreshold) + "; success=" + placementOptionalInt32Cell(routing.Probe.SuccessThreshold)},
				[]string{"Probe timing", "period=" + placementOptionalStringCell(routing.Probe.Period) + "; timeout=" + placementOptionalStringCell(routing.Probe.Timeout)},
				[]string{"All-failed policy", string(routing.Probe.AllFailedPolicy)},
			)
		}
		if routing.Capacity.State == "Configured" {
			rows = append(rows,
				[]string{"Capacity method", string(routing.Capacity.Method) + "; path=" + strconv.FormatBool(routing.Capacity.PathPresent) + "; format=" + strconv.FormatBool(routing.Capacity.FormatPresent)},
				[]string{"Capacity window", "samples=" + placementOptionalInt32Cell(routing.Capacity.Samples) + "; quorum=" + placementOptionalInt32Cell(routing.Capacity.Quorum) + "; options=" + strconv.Itoa(routing.Capacity.OptionCount)},
				[]string{"Capacity timing", "period=" + placementOptionalStringCell(routing.Capacity.Period) + "; timeout=" + placementOptionalStringCell(routing.Capacity.Timeout) + "; maxAge=" + placementOptionalStringCell(routing.Capacity.MaxAge)},
			)
		}
	}
	rows = append(rows, placementAcquisitionRows("Fleet", c.Fleet)...)
	clusters := canonical.Clusters
	if !wide && len(clusters) > 4 {
		rows = append(rows, []string{"Cluster preview", "4/" + strconv.Itoa(len(clusters)) + "; use -o wide or json"})
		clusters = clusters[:4]
	}
	for _, w := range clusters {
		rows = append(rows, []string{"Cluster", w.Name}, []string{"Selector compatible", string(w.ComputedSelectorCompatible)}, []string{"Reported WLC Ready", string(w.ReportedReady.Status) + " (" + string(w.ReportedReady.Source.Freshness) + ")"})
		rows = append(rows, []string{"Condition inspect", placementPreviewCell(w.ConditionPreview)})
		if wide {
			rows = append(rows, []string{"Reported home", string(w.ReportedHome)}, []string{"Connection source", string(w.ConnectionSource)}, []string{"Ready reason", string(w.ReportedReady.Reason)})
		}
	}
	rows = append(rows, placementIssueRows(c.Issues)...)
	rows = append(rows, []string{"Hint", "WLC Ready is reachability, not capacity"}, []string{"Hint", "Partial fleet: no global eligibility verdict"}, []string{"View", "Bounded cells; use -o json for complete identities"})
	return placementTable(rows)
}

func placementOptionalInt32Cell(value int32) string {
	if value == 0 {
		return "-"
	}
	return strconv.Itoa(int(value))
}

func placementOptionalStringCell(value string) string {
	if value == "" {
		return "-"
	}
	return value
}

func (c PlacementExplainContent) WideTable() report.Table { return c.placementExplainTable(true) }

func (c PlacementEndpointContent) Table() report.Table {
	return c.placementEndpointTable(false)
}

func (c PlacementEndpointContent) placementEndpointTable(wide bool) report.Table {
	rows := placementSummary(c.Status)
	rows = append(rows, placementAcquisitionRows("TrafficMap", c.Routing.Acquisition)...)
	rows = append(rows,
		[]string{"Entry inspection", placementPreviewCell(c.Routing.EntryPreview)},
		[]string{"Probe inspection", placementPreviewCell(c.Routing.ProbePreview)},
		[]string{"Condition inspect", placementPreviewCell(c.ConditionPreview)},
		[]string{"Routing freshness", string(c.Routing.Source.Freshness)},
		[]string{"Traffic override", placementConditionCell(c.Routing.OverrideActive)},
		[]string{"Capacity fallback", placementConditionCell(c.Routing.CapacityFallback)},
		[]string{"Routable", string(c.Routing.Routable.Status) + ": " + string(c.Routing.Routable.Reason)},
		[]string{"Routable freshness", string(c.Routing.Routable.Source.Freshness) + ": " + string(c.Routing.Routable.Source.Reason)},
		[]string{"Published", placementConditionCell(c.Routing.Published)},
		[]string{"Publisher", string(c.Routing.Acknowledgement)},
		[]string{"Publisher fresh", string(c.Routing.AcknowledgementSource.Freshness)},
	)
	entries := c.Canonical().Entries
	if !wide && len(entries) > 4 {
		rows = append(rows, []string{"Route preview", "4/" + strconv.Itoa(len(entries)) + "; use -o wide or json"})
		entries = entries[:4]
	}
	for _, e := range entries {
		rows = append(rows,
			[]string{"Route home", e.Cluster},
			[]string{"Endpoint origin", placementAddressCell(e.Address)},
			[]string{"Final weight", strconv.Itoa(int(e.Weight)) + "; healthy=" + strconv.FormatBool(e.Healthy)},
			[]string{"Recorded probe", string(e.Probe.Result)},
			[]string{"Route evidence", placementRouteEvidenceCell(e)},
			[]string{"Probe policy", placementProbePolicyCell(e.Probe)},
		)
		if wide {
			if e.Capacity != nil {
				rows = append(rows, []string{"Capacity provenance", string(e.Capacity.Source)}, []string{"Allocated count", placementCountCell(e.Capacity.Allocated)}, []string{"Ready count", placementCountCell(e.Capacity.Ready)})
			}
			rows = append(rows,
				[]string{"Probe freshness", string(e.Probe.Source.Freshness)},
				[]string{"Drain refs", placementDrainRefsCell(e)},
				[]string{"Probe gate", placementProbeGateCell(e.Probe)},
				[]string{"Home cap fallback", placementCapacityFallbackCell(e.Capacity)},
			)
		}
	}
	rows = append(rows, placementIssueRows(c.Issues)...)
	rows = append(rows, []string{"Hint", "Routing generation does not date placement"}, []string{"Hint", "Recorded probes only; no CLI network probe"}, []string{"View", "Bounded cells; use -o json for complete identities"})
	return placementTable(rows)
}

func (c PlacementEndpointContent) WideTable() report.Table { return c.placementEndpointTable(true) }

func placementCountCell(count PlacementCount) string {
	if count.Value == nil {
		return string(count.State)
	}
	return strconv.Itoa(int(*count.Value)) + " (" + string(count.State) + ")"
}

func placementConditionCell(condition PlacementCondition) string {
	return string(condition.Status) + ": " + string(condition.Reason) + " (" + string(condition.Source.Freshness) + ")"
}

func placementRouteEvidenceCell(route PlacementRoute) string {
	gate := string(route.Probe.GatedState)
	if route.Probe.GatedState == "Reported" {
		gate = placementBoolCell(route.Probe.Gated)
	}
	return "drains=" + placementDrainSummaryCell(route.DrainPreview) + "; gate=" + gate + "; fallback=" + placementCapacityFallbackCell(route.Capacity)
}

func placementDrainSummaryCell(preview PlacementPreview) string {
	switch preview.State {
	case "Validated":
		return strconv.Itoa(preview.Total)
	case "MalformedPayload", "BudgetExceeded", "Unavailable", "ConflictingDuplicates":
		return string(preview.State)
	case "", "NotRecorded":
		return "NotRecorded"
	default:
		return "Unavailable"
	}
}

func placementDrainRefsCell(route PlacementRoute) string {
	if len(route.DrainRefs) > 0 {
		return strings.Join(route.DrainRefs, ", ")
	}
	if route.DrainPreview.State != "" {
		return string(route.DrainPreview.State)
	}
	return "NotRecorded"
}

func placementProbePolicyCell(probe PlacementProbe) string {
	switch probe.PolicyDigestState {
	case "Reported":
		if probe.PolicyDigest != "" {
			return probe.PolicyDigest
		}
		return "Unavailable"
	case "NotRecorded", "Unavailable":
		return string(probe.PolicyDigestState)
	case "":
		return "NotRecorded"
	default:
		return "Unavailable"
	}
}

func placementProbeGateCell(probe PlacementProbe) string {
	if probe.GatedState == "Reported" {
		return placementBoolCell(probe.Gated) + " (Reported)"
	}
	if probe.GatedState != "" {
		return string(probe.GatedState)
	}
	return "NotRecorded"
}

func placementCapacityFallbackCell(capacity *PlacementCapacity) string {
	if capacity != nil && capacity.FallbackReason != "" {
		return string(capacity.FallbackReason)
	}
	return "NotRecorded"
}

func placementBoolCell(value bool) string {
	if value {
		return "True"
	}
	return "False"
}
