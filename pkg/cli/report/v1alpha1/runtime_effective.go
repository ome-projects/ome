package v1alpha1

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"time"

	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
)

const RuntimeEffectiveReportKind = "RuntimeEffectiveReport"

// RuntimeSelection reports how and, when observed, which runtime was selected.
type RuntimeSelection struct {
	Source  RuntimeSelectionSource  `json:"source"`
	Runtime *RuntimeObjectReference `json:"runtime,omitempty"`
}

// RuntimeInheritance reports the root-first runtime inheritance chain.
type RuntimeInheritance struct {
	State             InheritanceState         `json:"state"`
	Sources           []RuntimeObjectReference `json:"sources"`
	UnavailableReason UnavailableReason        `json:"unavailableReason,omitempty"`
}

// RuntimeStatusObservation reports bounded generation freshness evidence.
type RuntimeStatusObservation struct {
	Generation         int64           `json:"generation"`
	ObservedGeneration int64           `json:"observedGeneration"`
	Freshness          StatusFreshness `json:"freshness"`
}

// RuntimeDriftObservation reports the bounded controller drift condition.
type RuntimeDriftObservation struct {
	State DriftConditionState `json:"state"`
	Cause RuntimeDriftCause   `json:"cause,omitempty"`
}

// RuntimePin reports pin intent and bounded status relationships.
type RuntimePin struct {
	Mode              RuntimePinMode           `json:"mode"`
	State             RuntimePinState          `json:"state"`
	RequestedRevision string                   `json:"requestedRevision,omitempty"`
	ReportedRevision  string                   `json:"reportedRevision,omitempty"`
	Status            RuntimeStatusObservation `json:"status"`
	ReportedDrift     RuntimeDriftObservation  `json:"reportedDrift"`
	SyncState         RuntimeSyncState         `json:"syncState"`
}

// RuntimeConfiguration is an allowlisted runtime configuration summary.
type RuntimeConfiguration struct {
	State             ConfigurationState        `json:"state"`
	Origin            ConfigurationOrigin       `json:"origin,omitempty"`
	Source            *RuntimeObjectReference   `json:"source,omitempty"`
	Revision          *RuntimeRevisionReference `json:"revision,omitempty"`
	Hash              string                    `json:"hash,omitempty"`
	Components        []RuntimeComponent        `json:"components"`
	UnavailableReason UnavailableReason         `json:"unavailableReason,omitempty"`
}

// RuntimeEffectiveContent compares live and active runtime configuration.
type RuntimeEffectiveContent struct {
	Selection    RuntimeSelection     `json:"selection"`
	Inheritance  RuntimeInheritance   `json:"inheritance"`
	Pin          RuntimePin           `json:"pin"`
	Live         RuntimeConfiguration `json:"live"`
	Active       RuntimeConfiguration `json:"active"`
	LiveToActive RuntimeHashRelation  `json:"liveToActive"`
	Issues       []RuntimeIssue       `json:"issues"`
}

// NewRuntimeEffectiveReport creates a canonical effective report.
func NewRuntimeEffectiveReport(metadata Metadata, content RuntimeEffectiveContent, clock Clock) RuntimeEnvelope[RuntimeEffectiveContent] {
	return newRuntimeEnvelope(metadata, content, clock)
}

func (RuntimeEffectiveContent) runtimeReportKind() string {
	return RuntimeEffectiveReportKind
}

// Canonical returns a deeply copied deterministic effective report content.
func (c RuntimeEffectiveContent) Canonical() RuntimeEffectiveContent {
	result := c
	result.Selection.Runtime = copyRuntimeObjectReference(c.Selection.Runtime)
	result.Inheritance.Sources = append([]RuntimeObjectReference{}, c.Inheritance.Sources...)
	result.Live = c.Live.canonical()
	result.Active = c.Active.canonical()
	result.Issues = append([]RuntimeIssue{}, c.Issues...)
	sort.Slice(result.Issues, func(i, j int) bool {
		if result.Issues[i].Code != result.Issues[j].Code {
			return result.Issues[i].Code < result.Issues[j].Code
		}
		return result.Issues[i].Revision < result.Issues[j].Revision
	})
	return result
}

// Table returns a compact deterministic effective-configuration view. Each
// fact occupies one row so the default output remains readable without
// terminal metadata.
func (c RuntimeEffectiveContent) Table() report.Table {
	canonical := c.Canonical()
	rows := appendCompactConfigurationRows(nil, "Live", canonical.Live)
	rows = appendCompactConfigurationRows(rows, "Active", canonical.Active)
	rows = appendCompactRuntimeServiceRows(rows, canonical)
	return report.Table{
		Headers: []string{"SCOPE", "FIELD", "VALUE"},
		Rows:    rows,
	}
}

// WideTable returns the complete legacy effective-configuration table.
func (c RuntimeEffectiveContent) WideTable() report.Table {
	canonical := c.Canonical()
	table := report.Table{
		Headers: []string{
			"VIEW", "STATE", "REASON", "RUNTIME", "REVISION", "HASH",
			"COMPONENT", "MODE", "MODE-SOURCE", "PIN", "PIN-STATE", "SYNC",
			"STATUS", "DRIFT", "LIVE-RELATION", "ISSUES",
		},
		Rows: [][]string{},
	}
	table.Rows = appendConfigurationRows(
		table.Rows, "Live", canonical.Live, canonical.Pin, canonical.LiveToActive, canonical.Issues,
	)
	table.Rows = appendConfigurationRows(
		table.Rows, "Active", canonical.Active, canonical.Pin, canonical.LiveToActive, canonical.Issues,
	)
	return table
}

const compactRuntimeEffectiveValueWidth = 54

func appendCompactConfigurationRows(
	rows [][]string,
	scope string,
	configuration RuntimeConfiguration,
) [][]string {
	rows = appendCompactRuntimeEffectiveRow(rows, scope, "STATE", string(configuration.State))
	if configuration.UnavailableReason != "" {
		rows = appendCompactRuntimeEffectiveRow(
			rows, scope, "REASON", string(configuration.UnavailableReason),
		)
	}
	if configuration.Source != nil {
		rows = append(rows, []string{
			scope, "RUNTIME", compactRuntimeReferenceDisplay(configuration.Source),
		})
	}
	if configuration.Revision != nil {
		rows = append(rows, []string{
			scope, "REVISION", compactRuntimeEffectiveIdentity(revisionReferenceDisplay(configuration.Revision)),
		})
	}
	if configuration.Hash != "" {
		rows = appendCompactRuntimeEffectiveRow(rows, scope, "HASH", configuration.Hash)
	}
	for _, component := range configuration.Components {
		field := compactRuntimeComponentField(component.Type)
		if field == "" {
			rows = append(rows, []string{scope, "OTHER", "Unsupported component omitted"})
			continue
		}
		rows = appendCompactRuntimeEffectiveRow(
			rows, scope, field, compactRuntimeComponentValue(component),
		)
	}
	return rows
}

func appendCompactRuntimeServiceRows(
	rows [][]string,
	content RuntimeEffectiveContent,
) [][]string {
	if value := compactRuntimePin(content.Pin); value != "" {
		rows = appendCompactRuntimeEffectiveRow(rows, "Service", "PIN", value)
	}
	if content.Pin.SyncState != "" {
		rows = appendCompactRuntimeEffectiveRow(
			rows, "Service", "SYNC", string(content.Pin.SyncState),
		)
	}
	if content.Pin.Status.Freshness != "" {
		rows = appendCompactRuntimeEffectiveRow(
			rows, "Service", "STATUS", string(content.Pin.Status.Freshness),
		)
	}
	if content.Pin.ReportedDrift.State != "" {
		rows = appendCompactRuntimeEffectiveRow(
			rows, "Service", "DRIFT", runtimeDriftDisplay(content.Pin.ReportedDrift),
		)
	}
	if content.LiveToActive != "" {
		rows = appendCompactRuntimeEffectiveRow(
			rows, "Service", "LIVE-RELATION", string(content.LiveToActive),
		)
	}
	for _, issue := range content.Issues {
		rows = append(rows, []string{
			"Service", "ISSUE", compactRuntimeIssueDisplay(issue),
		})
	}
	return rows
}

func appendCompactRuntimeEffectiveRow(
	rows [][]string,
	scope string,
	field string,
	value string,
) [][]string {
	return append(rows, []string{
		scope, field, printers.BoundedCell(orDash(value), compactRuntimeEffectiveValueWidth),
	})
}

func compactRuntimeComponentField(component RuntimeComponentType) string {
	switch component {
	case RuntimeComponentEngine:
		return "ENGINE"
	case RuntimeComponentDecoder:
		return "DECODER"
	case RuntimeComponentRouter:
		return "ROUTER"
	default:
		return ""
	}
}

func compactRuntimeComponentValue(component RuntimeComponent) string {
	mode := orDash(string(component.DeploymentMode))
	source := orDash(string(component.DeploymentModeSource))
	switch {
	case mode == "-" && source == "-":
		return "-"
	case source == "-":
		return mode
	case mode == "-":
		return source
	default:
		return mode + " (" + source + ")"
	}
}

func compactRuntimePin(pin RuntimePin) string {
	parts := make([]string, 0, 2)
	if pin.Mode != "" {
		parts = append(parts, string(pin.Mode))
	}
	if pin.State != "" {
		parts = append(parts, string(pin.State))
	}
	return strings.Join(parts, "/")
}

func compactRuntimeEffectiveIdentity(value string) string {
	const normalizedIdentityLimit = 1024
	digest := sha256.Sum256([]byte(value))
	clean := printers.BoundedMiddleCell(value, normalizedIdentityLimit)
	if printers.BoundedMiddleCell(clean, compactRuntimeEffectiveValueWidth) == clean {
		return clean
	}
	prefix := printers.BoundedMiddleCell(
		clean, compactRuntimeEffectiveValueWidth-1-8,
	)
	return prefix + "#" + hex.EncodeToString(digest[:4])
}

func compactRuntimeIssueDisplay(issue RuntimeIssue) string {
	code := string(issue.Code)
	if issue.Revision == "" {
		return compactRuntimeEffectiveIdentity(code)
	}
	full := code + "(" + issue.Revision + ")"
	clean := printers.BoundedCell(full, 1024)
	if printers.BoundedCell(clean, compactRuntimeEffectiveValueWidth) == clean {
		return clean
	}
	const codeWidth = 28
	const revisionWidth = compactRuntimeEffectiveValueWidth - codeWidth - 2
	return compactRuntimeIdentityComponent(code, codeWidth) + "(" +
		compactRuntimeIdentityComponent(issue.Revision, revisionWidth) + ")"
}

func compactRuntimeReferenceDisplay(reference *RuntimeObjectReference) string {
	if reference == nil {
		return "-"
	}
	kind := compactRuntimeKind(reference.Kind)
	if reference.Namespace == "" {
		nameWidth := compactRuntimeEffectiveValueWidth - len(kind) - 1
		return kind + "/" + compactRuntimeIdentityComponent(reference.Name, nameWidth)
	}

	namespaceWidth := 20
	nameWidth := compactRuntimeEffectiveValueWidth - len(kind) - 2 - namespaceWidth
	return kind + "/" +
		compactRuntimeIdentityComponent(reference.Namespace, namespaceWidth) + "/" +
		compactRuntimeIdentityComponent(reference.Name, nameWidth)
}

func compactRuntimeKind(kind RuntimeKind) string {
	switch kind {
	case RuntimeKindServingRuntime:
		return "SR"
	case RuntimeKindClusterServingRuntime:
		return "CSR"
	default:
		return "Unknown"
	}
}

func compactRuntimeIdentityComponent(value string, width int) string {
	const normalizedIdentityLimit = 1024
	identity := orDash(value)
	digest := sha256.Sum256([]byte(identity))
	clean := printers.BoundedMiddleCell(identity, normalizedIdentityLimit)
	if printers.BoundedMiddleCell(clean, width) == clean {
		return clean
	}
	prefix := printers.BoundedMiddleCell(clean, width-1-8)
	return prefix + "#" + hex.EncodeToString(digest[:4])
}

func (c RuntimeConfiguration) canonical() RuntimeConfiguration {
	result := c
	result.Source = copyRuntimeObjectReference(c.Source)
	if c.Revision != nil {
		revision := *c.Revision
		revision.CreatedAt = canonicalTimePointer(revision.CreatedAt)
		result.Revision = &revision
	}
	result.Components = append([]RuntimeComponent{}, c.Components...)
	sort.Slice(result.Components, func(i, j int) bool {
		a, b := result.Components[i], result.Components[j]
		if componentOrder(a.Type) != componentOrder(b.Type) {
			return componentOrder(a.Type) < componentOrder(b.Type)
		}
		if a.Type != b.Type {
			return a.Type < b.Type
		}
		if a.DeploymentMode != b.DeploymentMode {
			return a.DeploymentMode < b.DeploymentMode
		}
		return a.DeploymentModeSource < b.DeploymentModeSource
	})
	return result
}

func canonicalTimePointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	utc := value.UTC()
	return &utc
}

func copyRuntimeObjectReference(source *RuntimeObjectReference) *RuntimeObjectReference {
	if source == nil {
		return nil
	}
	result := *source
	return &result
}

func componentOrder(component RuntimeComponentType) int {
	switch component {
	case RuntimeComponentEngine:
		return 0
	case RuntimeComponentDecoder:
		return 1
	case RuntimeComponentRouter:
		return 2
	default:
		return 3
	}
}

func appendConfigurationRows(
	rows [][]string,
	view string,
	configuration RuntimeConfiguration,
	pin RuntimePin,
	relation RuntimeHashRelation,
	issues []RuntimeIssue,
) [][]string {
	components := configuration.Components
	if len(components) == 0 {
		components = []RuntimeComponent{{}}
	}
	for _, component := range components {
		rows = append(rows, []string{
			view,
			orDash(string(configuration.State)),
			orDash(string(configuration.UnavailableReason)),
			runtimeReferenceDisplay(configuration.Source),
			revisionReferenceDisplay(configuration.Revision),
			orDash(configuration.Hash),
			orDash(string(component.Type)),
			orDash(string(component.DeploymentMode)),
			orDash(string(component.DeploymentModeSource)),
			orDash(string(pin.Mode)),
			orDash(string(pin.State)),
			orDash(string(pin.SyncState)),
			orDash(string(pin.Status.Freshness)),
			runtimeDriftDisplay(pin.ReportedDrift),
			orDash(string(relation)),
			joinRuntimeIssues(issues),
		})
	}
	return rows
}

func runtimeDriftDisplay(drift RuntimeDriftObservation) string {
	if drift.State == "" {
		return "-"
	}
	if drift.Cause == "" {
		return string(drift.State)
	}
	return string(drift.State) + "/" + string(drift.Cause)
}

func joinRuntimeIssues(issues []RuntimeIssue) string {
	values := make([]string, len(issues))
	for i, issue := range issues {
		values[i] = string(issue.Code)
		if issue.Revision != "" {
			values[i] += "(" + issue.Revision + ")"
		}
	}
	return orDash(strings.Join(values, ","))
}

func runtimeReferenceDisplay(reference *RuntimeObjectReference) string {
	if reference == nil {
		return "-"
	}
	parts := []string{string(reference.Kind)}
	if reference.Namespace != "" {
		parts = append(parts, reference.Namespace)
	}
	parts = append(parts, reference.Name)
	return strings.Join(parts, "/")
}

func revisionReferenceDisplay(reference *RuntimeRevisionReference) string {
	if reference == nil {
		return "-"
	}
	return orDash(reference.Name)
}
