package v1alpha1

import (
	"cmp"
	"sort"
	"strconv"
	"strings"

	"sigs.k8s.io/ome/pkg/cli/report"
)

const RuntimeTreeReportKind = "RuntimeTreeReport"

// RuntimeTreeSnapshotCompleteness describes whether every requested list was
// observed without a bounded-page cutoff or source failure.
type RuntimeTreeSnapshotCompleteness string

const (
	RuntimeTreeSnapshotComplete RuntimeTreeSnapshotCompleteness = "Complete"
	RuntimeTreeSnapshotPartial  RuntimeTreeSnapshotCompleteness = "Partial"
)

// RuntimeTreeCollectionKind identifies one collection contributing to the
// tree snapshot.
type RuntimeTreeCollectionKind string

const (
	RuntimeTreeCollectionClusterServingRuntime RuntimeTreeCollectionKind = "ClusterServingRuntime"
	RuntimeTreeCollectionServingRuntime        RuntimeTreeCollectionKind = "ServingRuntime"
	RuntimeTreeCollectionInferenceService      RuntimeTreeCollectionKind = "InferenceService"
)

// RuntimeTreeCollectionScope identifies the exact Kubernetes list scope used
// to collect one kind. Namespace is carried separately for Namespace scope.
type RuntimeTreeCollectionScope string

const (
	RuntimeTreeCollectionScopeCluster       RuntimeTreeCollectionScope = "Cluster"
	RuntimeTreeCollectionScopeAllNamespaces RuntimeTreeCollectionScope = "AllNamespaces"
	RuntimeTreeCollectionScopeNamespace     RuntimeTreeCollectionScope = "Namespace"
)

// RuntimeTreeCollectionStatus describes the outcome of one bounded list.
type RuntimeTreeCollectionStatus string

const (
	RuntimeTreeCollectionStatusComplete    RuntimeTreeCollectionStatus = "Complete"
	RuntimeTreeCollectionStatusTruncated   RuntimeTreeCollectionStatus = "Truncated"
	RuntimeTreeCollectionStatusUnavailable RuntimeTreeCollectionStatus = "Unavailable"
)

// RuntimeTreeCollection reports bounded collection evidence for one kind.
type RuntimeTreeCollection struct {
	Kind          RuntimeTreeCollectionKind   `json:"kind"`
	Scope         RuntimeTreeCollectionScope  `json:"scope"`
	Namespace     string                      `json:"namespace,omitempty"`
	Status        RuntimeTreeCollectionStatus `json:"status"`
	ObservedPages int                         `json:"observedPages"`
	ObservedItems int                         `json:"observedItems"`
}

// RuntimeTreeSnapshot reports the completeness of the inputs used to build a
// tree. Collections is additive so later dependency collectors can describe
// their own bounded reads without changing this schema.
type RuntimeTreeSnapshot struct {
	Completeness RuntimeTreeSnapshotCompleteness `json:"completeness"`
	Collections  []RuntimeTreeCollection         `json:"collections"`
}

// RuntimeTreeIdentity is the full kind/scope/name identity of one runtime.
type RuntimeTreeIdentity struct {
	Kind      RuntimeKind `json:"kind"`
	Namespace string      `json:"namespace,omitempty"`
	Name      string      `json:"name"`
}

// RuntimeTreeRuntime retains the declared and resolved inheritance edge for a
// runtime represented in the tree.
type RuntimeTreeRuntime struct {
	Identity       RuntimeTreeIdentity  `json:"identity"`
	ParentName     string               `json:"parentName,omitempty"`
	ResolvedParent *RuntimeTreeIdentity `json:"resolvedParent,omitempty"`
}

// RuntimeTreeDependentKind identifies a supported non-runtime leaf.
type RuntimeTreeDependentKind string

const (
	RuntimeTreeDependentInferenceService RuntimeTreeDependentKind = "InferenceService"
)

// RuntimeTreeDependent is an allowlisted identity-only dependency leaf.
type RuntimeTreeDependent struct {
	Kind      RuntimeTreeDependentKind `json:"kind"`
	Namespace string                   `json:"namespace"`
	Name      string                   `json:"name"`
	UID       string                   `json:"uid,omitempty"`
}

// RuntimeTreeIssueCode classifies an inheritance topology problem.
type RuntimeTreeIssueCode string

const (
	RuntimeTreeIssueParentMissing    RuntimeTreeIssueCode = "ParentMissing"
	RuntimeTreeIssueCycleDetected    RuntimeTreeIssueCode = "CycleDetected"
	RuntimeTreeIssueMaxDepthExceeded RuntimeTreeIssueCode = "MaxDepthExceeded"
)

// RuntimeTreeIssue preserves bounded graph diagnostics. Path retains the
// graph's subject-first order toward its ancestors.
type RuntimeTreeIssue struct {
	Code       RuntimeTreeIssueCode  `json:"code"`
	Subject    RuntimeTreeIdentity   `json:"subject"`
	ParentName string                `json:"parentName,omitempty"`
	Path       []RuntimeTreeIdentity `json:"path"`
}

// RuntimeTreeResolutionMode identifies the lookup policy used for an entire
// controller inheritance walk.
type RuntimeTreeResolutionMode string

const (
	RuntimeTreeResolutionModeCluster    RuntimeTreeResolutionMode = "Cluster"
	RuntimeTreeResolutionModeNamespaced RuntimeTreeResolutionMode = "Namespaced"
)

// RuntimeTreeResolutionContext identifies one fixed controller lookup scope.
type RuntimeTreeResolutionContext struct {
	Mode      RuntimeTreeResolutionMode `json:"mode"`
	Namespace string                    `json:"namespace,omitempty"`
}

// RuntimeTreePath is one exact controller inheritance walk. Runtimes remain
// ordered from the observed root or error boundary to Head. Dependents belong
// only to Head; Issue belongs only to this walk.
type RuntimeTreePath struct {
	Head       RuntimeTreeIdentity    `json:"head"`
	Runtimes   []RuntimeTreeRuntime   `json:"runtimes"`
	Dependents []RuntimeTreeDependent `json:"dependents"`
	Issue      *RuntimeTreeIssue      `json:"issue,omitempty"`
}

// RuntimeTreeContext groups exact head paths resolved under one fixed lookup
// context. ResolutionCompleteness describes only the runtime collections used
// by the controller lookup; dependent-list completeness remains in Snapshot.
type RuntimeTreeContext struct {
	Context                RuntimeTreeResolutionContext    `json:"context"`
	ResolutionCompleteness RuntimeTreeSnapshotCompleteness `json:"resolutionCompleteness"`
	Paths                  []RuntimeTreePath               `json:"paths"`
}

// RuntimeTreeContent is the typed body shared by terminal and machine output.
type RuntimeTreeContent struct {
	Target   RuntimeTreeIdentity  `json:"target"`
	Snapshot RuntimeTreeSnapshot  `json:"snapshot"`
	Contexts []RuntimeTreeContext `json:"contexts"`
}

// NewRuntimeTreeReport creates a canonical runtime inheritance tree report.
func NewRuntimeTreeReport(
	metadata Metadata,
	content RuntimeTreeContent,
	clock Clock,
) RuntimeEnvelope[RuntimeTreeContent] {
	return newRuntimeEnvelope(metadata, content, clock)
}

func (RuntimeTreeContent) runtimeReportKind() string {
	return RuntimeTreeReportKind
}

// Canonical returns a deeply copied and deterministically ordered report. It
// sorts contexts and heads without changing the order inside an exact path.
func (c RuntimeTreeContent) Canonical() RuntimeTreeContent {
	result := c
	result.Snapshot.Collections = append([]RuntimeTreeCollection{}, c.Snapshot.Collections...)
	sort.Slice(result.Snapshot.Collections, func(i, j int) bool {
		return compareRuntimeTreeCollections(result.Snapshot.Collections[i], result.Snapshot.Collections[j]) < 0
	})
	result.Contexts = make([]RuntimeTreeContext, len(c.Contexts))
	for i := range c.Contexts {
		result.Contexts[i] = c.Contexts[i].canonical(c.Target)
	}
	sort.Slice(result.Contexts, func(i, j int) bool {
		return compareRuntimeTreeContexts(result.Contexts[i], result.Contexts[j]) < 0
	})
	return result
}

func (c RuntimeTreeContext) canonical(target RuntimeTreeIdentity) RuntimeTreeContext {
	result := c
	result.Paths = make([]RuntimeTreePath, len(c.Paths))
	for i := range c.Paths {
		result.Paths[i] = c.Paths[i].canonical()
	}
	sort.Slice(result.Paths, func(i, j int) bool {
		leftSelected := result.Paths[i].Head == target
		rightSelected := result.Paths[j].Head == target
		if leftSelected != rightSelected {
			return leftSelected
		}
		return compareRuntimeTreePaths(result.Paths[i], result.Paths[j]) < 0
	})
	return result
}

func (p RuntimeTreePath) canonical() RuntimeTreePath {
	result := p
	result.Runtimes = make([]RuntimeTreeRuntime, len(p.Runtimes))
	for i := range p.Runtimes {
		result.Runtimes[i] = copyRuntimeTreeRuntime(p.Runtimes[i])
	}
	result.Dependents = append([]RuntimeTreeDependent{}, p.Dependents...)
	sort.Slice(result.Dependents, func(i, j int) bool {
		return compareRuntimeTreeDependents(result.Dependents[i], result.Dependents[j]) < 0
	})
	if p.Issue != nil {
		issue := *p.Issue
		issue.Path = append([]RuntimeTreeIdentity{}, p.Issue.Path...)
		result.Issue = &issue
	}
	return result
}

func copyRuntimeTreeRuntime(runtime RuntimeTreeRuntime) RuntimeTreeRuntime {
	result := runtime
	if runtime.ResolvedParent != nil {
		parent := *runtime.ResolvedParent
		result.ResolvedParent = &parent
	}
	return result
}

// Table returns a one-column view suitable for constrained terminals. Every
// context and head is a separate section so distinct controller walks are
// never visually merged.
func (c RuntimeTreeContent) Table() report.Table {
	return c.tableWithWarnings(nil)
}

func (c RuntimeTreeContent) tableWithWarnings(warnings []RuntimeWarning) report.Table {
	canonical := c.Canonical()
	rows := [][]string{{"Target: " + formatRuntimeTreeIdentity(canonical.Target)}}
	for _, context := range canonical.Contexts {
		rows = append(rows, []string{
			"Context: " + formatRuntimeTreeContext(context.Context) +
				" (resolution: " + string(context.ResolutionCompleteness) + ")",
		})
		for _, path := range context.Paths {
			rows = append(rows, []string{"Head: " + formatRuntimeTreeIdentityInContext(path.Head, context.Context)})
			for i, runtime := range path.Runtimes {
				prefix := ""
				if i > 0 {
					prefix = strings.Repeat("    ", i-1) + "`-- "
				}
				rows = append(rows, []string{
					prefix + formatRuntimeTreeIdentityInContext(runtime.Identity, context.Context) +
						selectedSuffix(runtime.Identity == canonical.Target),
				})
			}
			dependentPrefix := strings.Repeat("    ", max(0, len(path.Runtimes)-1))
			for i, dependent := range path.Dependents {
				branch := "|-- "
				if i == len(path.Dependents)-1 {
					branch = "`-- "
				}
				rows = append(rows, []string{
					dependentPrefix + branch + formatRuntimeTreeDependentInContext(dependent, context.Context),
				})
			}
			if path.Issue != nil {
				rows = append(rows, []string{formatRuntimeTreeIssue(*path.Issue, context.Context)})
				if len(path.Issue.Path) > 0 {
					rows = append(rows, []string{formatRuntimeTreeIssuePath(path.Issue.Path, context.Context)})
				}
			}
		}
	}
	rows = append(rows, []string{"Snapshot: " + string(canonical.Snapshot.Completeness)})
	for _, collection := range canonical.Snapshot.Collections {
		rows = append(rows, []string{formatRuntimeTreeCollection(collection)})
	}
	for _, warning := range warnings {
		rows = append(rows, []string{"Warning: " + string(warning.Code)})
	}
	return report.Table{Headers: []string{"RUNTIME TREE"}, Rows: rows}
}

func selectedSuffix(selected bool) string {
	if selected {
		return " [selected]"
	}
	return ""
}

func formatRuntimeTreeContext(context RuntimeTreeResolutionContext) string {
	if context.Namespace == "" {
		return string(context.Mode)
	}
	return strings.Join([]string{string(context.Mode), context.Namespace}, "/")
}

func formatRuntimeTreeIdentity(identity RuntimeTreeIdentity) string {
	parts := []string{string(identity.Kind)}
	if identity.Namespace != "" {
		parts = append(parts, identity.Namespace)
	}
	parts = append(parts, identity.Name)
	return strings.Join(parts, "/")
}

func formatRuntimeTreeIdentityInContext(
	identity RuntimeTreeIdentity,
	context RuntimeTreeResolutionContext,
) string {
	if context.Mode == RuntimeTreeResolutionModeNamespaced &&
		identity.Kind == RuntimeKindServingRuntime && identity.Namespace == context.Namespace {
		return strings.Join([]string{string(identity.Kind), identity.Name}, "/")
	}
	return formatRuntimeTreeIdentity(identity)
}

func formatRuntimeTreeDependentInContext(
	dependent RuntimeTreeDependent,
	context RuntimeTreeResolutionContext,
) string {
	if context.Mode == RuntimeTreeResolutionModeNamespaced && dependent.Namespace == context.Namespace {
		return strings.Join([]string{string(dependent.Kind), dependent.Name}, "/")
	}
	return strings.Join([]string{string(dependent.Kind), dependent.Namespace, dependent.Name}, "/")
}

func formatRuntimeTreeIssue(issue RuntimeTreeIssue, context RuntimeTreeResolutionContext) string {
	result := "Issue: " + string(issue.Code) +
		" subject=" + formatRuntimeTreeIdentityInContext(issue.Subject, context)
	if issue.ParentName != "" {
		result += " parent=" + issue.ParentName
	}
	return result
}

func formatRuntimeTreeIssuePath(
	path []RuntimeTreeIdentity,
	context RuntimeTreeResolutionContext,
) string {
	parts := make([]string, len(path))
	for i := range path {
		parts[i] = formatRuntimeTreeIdentityInContext(path[i], context)
	}
	return "Issue path: " + strings.Join(parts, " -> ")
}

func formatRuntimeTreeCollection(collection RuntimeTreeCollection) string {
	return "Collection: " + string(collection.Kind) +
		" scope=" + formatRuntimeTreeCollectionScope(collection) +
		" status=" + string(collection.Status) +
		" pages=" + strconv.Itoa(collection.ObservedPages) +
		" items=" + strconv.Itoa(collection.ObservedItems)
}

func formatRuntimeTreeCollectionScope(collection RuntimeTreeCollection) string {
	if collection.Namespace == "" {
		return string(collection.Scope)
	}
	return string(collection.Scope) + "/" + collection.Namespace
}

func compareRuntimeTreeCollections(a, b RuntimeTreeCollection) int {
	if result := cmp.Compare(runtimeTreeCollectionRank(a.Kind), runtimeTreeCollectionRank(b.Kind)); result != 0 {
		return result
	}
	for _, result := range []int{
		cmp.Compare(a.Kind, b.Kind),
		cmp.Compare(a.Scope, b.Scope),
		cmp.Compare(a.Namespace, b.Namespace),
		cmp.Compare(a.Status, b.Status),
		cmp.Compare(a.ObservedPages, b.ObservedPages),
		cmp.Compare(a.ObservedItems, b.ObservedItems),
	} {
		if result != 0 {
			return result
		}
	}
	return 0
}

func runtimeTreeCollectionRank(kind RuntimeTreeCollectionKind) int {
	switch kind {
	case RuntimeTreeCollectionClusterServingRuntime:
		return 0
	case RuntimeTreeCollectionServingRuntime:
		return 1
	case RuntimeTreeCollectionInferenceService:
		return 2
	default:
		return 3
	}
}

func compareRuntimeTreeIdentities(a, b RuntimeTreeIdentity) int {
	for _, result := range []int{
		cmp.Compare(runtimeTreeKindRank(a.Kind), runtimeTreeKindRank(b.Kind)),
		cmp.Compare(a.Kind, b.Kind),
		cmp.Compare(a.Namespace, b.Namespace),
		cmp.Compare(a.Name, b.Name),
	} {
		if result != 0 {
			return result
		}
	}
	return 0
}

func runtimeTreeKindRank(kind RuntimeKind) int {
	switch kind {
	case RuntimeKindClusterServingRuntime:
		return 0
	case RuntimeKindServingRuntime:
		return 1
	default:
		return 2
	}
}

func compareRuntimeTreeDependents(a, b RuntimeTreeDependent) int {
	for _, result := range []int{
		cmp.Compare(a.Kind, b.Kind),
		cmp.Compare(a.Namespace, b.Namespace),
		cmp.Compare(a.Name, b.Name),
		cmp.Compare(a.UID, b.UID),
	} {
		if result != 0 {
			return result
		}
	}
	return 0
}

func compareRuntimeTreeContexts(a, b RuntimeTreeContext) int {
	for _, result := range []int{
		cmp.Compare(runtimeTreeResolutionModeRank(a.Context.Mode), runtimeTreeResolutionModeRank(b.Context.Mode)),
		cmp.Compare(a.Context.Mode, b.Context.Mode),
		cmp.Compare(a.Context.Namespace, b.Context.Namespace),
		cmp.Compare(a.ResolutionCompleteness, b.ResolutionCompleteness),
		compareRuntimeTreePathSlices(a.Paths, b.Paths),
	} {
		if result != 0 {
			return result
		}
	}
	return 0
}

func runtimeTreeResolutionModeRank(mode RuntimeTreeResolutionMode) int {
	switch mode {
	case RuntimeTreeResolutionModeCluster:
		return 0
	case RuntimeTreeResolutionModeNamespaced:
		return 1
	default:
		return 2
	}
}

func compareRuntimeTreePaths(a, b RuntimeTreePath) int {
	for _, result := range []int{
		compareRuntimeTreeIdentities(a.Head, b.Head),
		compareRuntimeTreeRuntimeSlices(a.Runtimes, b.Runtimes),
		compareRuntimeTreeDependentSlices(a.Dependents, b.Dependents),
		compareRuntimeTreeIssuePointers(a.Issue, b.Issue),
	} {
		if result != 0 {
			return result
		}
	}
	return 0
}

func compareRuntimeTreePathSlices(a, b []RuntimeTreePath) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if result := compareRuntimeTreePaths(a[i], b[i]); result != 0 {
			return result
		}
	}
	return cmp.Compare(len(a), len(b))
}

func compareRuntimeTreeRuntimeSlices(a, b []RuntimeTreeRuntime) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		for _, result := range []int{
			compareRuntimeTreeIdentities(a[i].Identity, b[i].Identity),
			cmp.Compare(a[i].ParentName, b[i].ParentName),
			compareRuntimeTreeIdentityPointers(a[i].ResolvedParent, b[i].ResolvedParent),
		} {
			if result != 0 {
				return result
			}
		}
	}
	return cmp.Compare(len(a), len(b))
}

func compareRuntimeTreeIdentityPointers(a, b *RuntimeTreeIdentity) int {
	if a == nil {
		if b == nil {
			return 0
		}
		return -1
	}
	if b == nil {
		return 1
	}
	return compareRuntimeTreeIdentities(*a, *b)
}

func compareRuntimeTreeDependentSlices(a, b []RuntimeTreeDependent) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if result := compareRuntimeTreeDependents(a[i], b[i]); result != 0 {
			return result
		}
	}
	return cmp.Compare(len(a), len(b))
}

func compareRuntimeTreeIssues(a, b RuntimeTreeIssue) int {
	for _, result := range []int{
		cmp.Compare(a.Code, b.Code),
		compareRuntimeTreeIdentities(a.Subject, b.Subject),
		cmp.Compare(a.ParentName, b.ParentName),
		compareRuntimeTreeIdentitySlices(a.Path, b.Path),
	} {
		if result != 0 {
			return result
		}
	}
	return 0
}

func compareRuntimeTreeIssuePointers(a, b *RuntimeTreeIssue) int {
	if a == nil {
		if b == nil {
			return 0
		}
		return -1
	}
	if b == nil {
		return 1
	}
	return compareRuntimeTreeIssues(*a, *b)
}

func compareRuntimeTreeIdentitySlices(a, b []RuntimeTreeIdentity) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if result := compareRuntimeTreeIdentities(a[i], b[i]); result != 0 {
			return result
		}
	}
	return cmp.Compare(len(a), len(b))
}
