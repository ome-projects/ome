// Package runtimetreeprojection projects an already-resolved runtime graph
// into the versioned kubectl-ome runtime-tree report. It performs no cluster
// reads.
package runtimetreeprojection

import (
	"errors"
	"fmt"

	"k8s.io/apimachinery/pkg/util/validation"

	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/runtimegraph"
	"sigs.k8s.io/ome/pkg/constants"
)

var (
	// ErrInvalidProjection indicates graph evidence that cannot be represented
	// safely by the runtime-tree contract.
	ErrInvalidProjection = errors.New("runtime tree projection is invalid")
	// ErrInvalidSnapshot indicates contradictory or malformed collection
	// completeness evidence.
	ErrInvalidSnapshot = errors.New("runtime tree snapshot is invalid")
	// ErrInvalidDependent indicates an incomplete or unsupported dependency
	// leaf identity.
	ErrInvalidDependent = errors.New("runtime tree dependent is invalid")
	// ErrDependentRuntimeNotVisible indicates a leaf whose runtime is absent as
	// an exact direct head. An ancestor occurrence is intentionally insufficient.
	ErrDependentRuntimeNotVisible = errors.New("runtime tree dependent runtime is not a visible head")
)

// CollectionObservation is bounded pagination evidence for one collected
// object kind.
type CollectionObservation struct {
	Kind          reportv1alpha1.RuntimeTreeCollectionKind
	Scope         reportv1alpha1.RuntimeTreeCollectionScope
	Namespace     string
	Status        reportv1alpha1.RuntimeTreeCollectionStatus
	ObservedPages int
	ObservedItems int
}

// SnapshotObservation describes the graph and dependency reads used by a
// caller. Project derives all completeness and warnings from these bounded
// statuses; callers cannot provide contradictory summary fields.
type SnapshotObservation struct {
	Collections []CollectionObservation
}

// DependentLeaf attaches one normalized, identity-only object to the exact
// runtime it references. Kind is additive; v1alpha1 currently admits only an
// InferenceService leaf.
type DependentLeaf struct {
	Runtime   runtimegraph.Identity
	Kind      reportv1alpha1.RuntimeTreeDependentKind
	Namespace string
	Name      string
	UID       string
}

// Input contains already-collected evidence. Project never performs I/O or
// mutates these values.
type Input struct {
	Projection runtimegraph.Projection
	Snapshot   SnapshotObservation
	Dependents []DependentLeaf
}

// Project builds a canonical runtime-tree report from already-resolved graph
// evidence.
func Project(
	input Input,
	clock reportv1alpha1.Clock,
) (reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeTreeContent], error) {
	snapshot, statuses, warnings, err := projectSnapshot(input.Snapshot)
	if err != nil {
		return reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeTreeContent]{}, err
	}
	target, err := projectIdentity(input.Projection.Target)
	if err != nil {
		return reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeTreeContent]{}, err
	}
	contexts, heads, err := projectContexts(input.Projection, statuses)
	if err != nil {
		return reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeTreeContent]{}, err
	}
	if err := attachDependents(contexts, heads, input.Dependents); err != nil {
		return reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeTreeContent]{}, err
	}
	if err := validateVisibleCounts(snapshot, contexts); err != nil {
		return reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeTreeContent]{}, err
	}
	reportValue := reportv1alpha1.NewRuntimeTreeReport(
		reportv1alpha1.Metadata{
			Namespace: input.Projection.Target.Namespace,
			Name:      input.Projection.Target.Name,
		},
		reportv1alpha1.RuntimeTreeContent{Target: target, Snapshot: snapshot, Contexts: contexts},
		clock,
	)
	reportValue.Warnings = warnings
	return reportValue.Canonical(), nil
}

func projectSnapshot(
	snapshot SnapshotObservation,
) (
	reportv1alpha1.RuntimeTreeSnapshot,
	map[reportv1alpha1.RuntimeTreeCollectionKind]reportv1alpha1.RuntimeTreeCollectionStatus,
	[]reportv1alpha1.RuntimeWarning,
	error,
) {
	collections := make([]reportv1alpha1.RuntimeTreeCollection, 0, len(snapshot.Collections))
	seenKinds := make(map[reportv1alpha1.RuntimeTreeCollectionKind]struct{}, len(snapshot.Collections))
	statuses := make(map[reportv1alpha1.RuntimeTreeCollectionKind]reportv1alpha1.RuntimeTreeCollectionStatus, len(snapshot.Collections))
	truncated := false
	unavailable := false
	for _, collection := range snapshot.Collections {
		if !validCollectionKind(collection.Kind) || !validCollectionScope(collection) ||
			!validCollectionStatus(collection.Status) ||
			collection.ObservedPages < 0 || collection.ObservedItems < 0 {
			return reportv1alpha1.RuntimeTreeSnapshot{}, nil, nil, fmt.Errorf(
				"%w: malformed %q collection", ErrInvalidSnapshot, collection.Kind,
			)
		}
		if collection.ObservedPages == 0 &&
			(collection.ObservedItems > 0 || collection.Status != reportv1alpha1.RuntimeTreeCollectionStatusUnavailable) {
			return reportv1alpha1.RuntimeTreeSnapshot{}, nil, nil, fmt.Errorf(
				"%w: %q collection has impossible page evidence", ErrInvalidSnapshot, collection.Kind,
			)
		}
		if _, duplicate := seenKinds[collection.Kind]; duplicate {
			return reportv1alpha1.RuntimeTreeSnapshot{}, nil, nil, fmt.Errorf(
				"%w: duplicate %q collection", ErrInvalidSnapshot, collection.Kind,
			)
		}
		seenKinds[collection.Kind] = struct{}{}
		statuses[collection.Kind] = collection.Status
		truncated = truncated || collection.Status == reportv1alpha1.RuntimeTreeCollectionStatusTruncated
		unavailable = unavailable || collection.Status == reportv1alpha1.RuntimeTreeCollectionStatusUnavailable
		collections = append(collections, reportv1alpha1.RuntimeTreeCollection{
			Kind: collection.Kind, Scope: collection.Scope, Namespace: collection.Namespace,
			Status:        collection.Status,
			ObservedPages: collection.ObservedPages, ObservedItems: collection.ObservedItems,
		})
	}
	for _, kind := range requiredCollectionKinds() {
		if _, observed := seenKinds[kind]; !observed {
			return reportv1alpha1.RuntimeTreeSnapshot{}, nil, nil, fmt.Errorf(
				"%w: missing %q collection", ErrInvalidSnapshot, kind,
			)
		}
	}

	completeness := reportv1alpha1.RuntimeTreeSnapshotComplete
	warnings := []reportv1alpha1.RuntimeWarning{}
	if truncated || unavailable {
		completeness = reportv1alpha1.RuntimeTreeSnapshotPartial
		warnings = append(warnings, reportv1alpha1.RuntimeWarning{Code: reportv1alpha1.WarningPartialData})
	}
	if unavailable {
		warnings = append(warnings, reportv1alpha1.RuntimeWarning{Code: reportv1alpha1.WarningSourceUnavailable})
	}
	if truncated {
		warnings = append(warnings, reportv1alpha1.RuntimeWarning{Code: reportv1alpha1.WarningTruncated})
	}
	return reportv1alpha1.RuntimeTreeSnapshot{
		Completeness: completeness, Collections: collections,
	}, statuses, warnings, nil
}

func validCollectionScope(collection CollectionObservation) bool {
	switch collection.Kind {
	case reportv1alpha1.RuntimeTreeCollectionClusterServingRuntime:
		return collection.Scope == reportv1alpha1.RuntimeTreeCollectionScopeCluster &&
			collection.Namespace == ""
	case reportv1alpha1.RuntimeTreeCollectionServingRuntime,
		reportv1alpha1.RuntimeTreeCollectionInferenceService:
		switch collection.Scope {
		case reportv1alpha1.RuntimeTreeCollectionScopeAllNamespaces:
			return collection.Namespace == ""
		case reportv1alpha1.RuntimeTreeCollectionScopeNamespace:
			return len(validation.IsDNS1123Label(collection.Namespace)) == 0
		default:
			return false
		}
	default:
		return false
	}
}

func requiredCollectionKinds() []reportv1alpha1.RuntimeTreeCollectionKind {
	return []reportv1alpha1.RuntimeTreeCollectionKind{
		reportv1alpha1.RuntimeTreeCollectionClusterServingRuntime,
		reportv1alpha1.RuntimeTreeCollectionServingRuntime,
		reportv1alpha1.RuntimeTreeCollectionInferenceService,
	}
}

func validCollectionKind(kind reportv1alpha1.RuntimeTreeCollectionKind) bool {
	switch kind {
	case reportv1alpha1.RuntimeTreeCollectionClusterServingRuntime,
		reportv1alpha1.RuntimeTreeCollectionServingRuntime,
		reportv1alpha1.RuntimeTreeCollectionInferenceService:
		return true
	default:
		return false
	}
}

func validCollectionStatus(status reportv1alpha1.RuntimeTreeCollectionStatus) bool {
	switch status {
	case reportv1alpha1.RuntimeTreeCollectionStatusComplete,
		reportv1alpha1.RuntimeTreeCollectionStatusTruncated,
		reportv1alpha1.RuntimeTreeCollectionStatusUnavailable:
		return true
	default:
		return false
	}
}

func projectRuntime(value runtimegraph.Runtime) (reportv1alpha1.RuntimeTreeRuntime, error) {
	identity, err := projectIdentity(value.Identity)
	if err != nil {
		return reportv1alpha1.RuntimeTreeRuntime{}, err
	}
	result := reportv1alpha1.RuntimeTreeRuntime{Identity: identity, ParentName: value.ParentName}
	if value.ResolvedParent != nil {
		parent, err := projectIdentity(*value.ResolvedParent)
		if err != nil {
			return reportv1alpha1.RuntimeTreeRuntime{}, err
		}
		result.ResolvedParent = &parent
	}
	return result, nil
}

func projectIdentity(identity runtimegraph.Identity) (reportv1alpha1.RuntimeTreeIdentity, error) {
	if len(validation.IsDNS1123Subdomain(identity.Name)) != 0 {
		return reportv1alpha1.RuntimeTreeIdentity{}, fmt.Errorf(
			"%w: runtime name %q is invalid", ErrInvalidProjection, identity.Name,
		)
	}
	result := reportv1alpha1.RuntimeTreeIdentity{Namespace: identity.Namespace, Name: identity.Name}
	switch identity.Kind {
	case runtimegraph.KindClusterServingRuntime:
		if identity.Namespace != "" {
			return reportv1alpha1.RuntimeTreeIdentity{}, fmt.Errorf(
				"%w: ClusterServingRuntime cannot have a namespace", ErrInvalidProjection,
			)
		}
		result.Kind = reportv1alpha1.RuntimeKindClusterServingRuntime
	case runtimegraph.KindServingRuntime:
		if len(validation.IsDNS1123Label(identity.Namespace)) != 0 {
			return reportv1alpha1.RuntimeTreeIdentity{}, fmt.Errorf(
				"%w: ServingRuntime namespace %q is invalid", ErrInvalidProjection, identity.Namespace,
			)
		}
		result.Kind = reportv1alpha1.RuntimeKindServingRuntime
	default:
		return reportv1alpha1.RuntimeTreeIdentity{}, fmt.Errorf(
			"%w: unsupported runtime kind %q", ErrInvalidProjection, identity.Kind,
		)
	}
	return result, nil
}

type headLocation struct {
	context int
	path    int
}

type resolutionKey struct {
	context runtimegraph.ResolutionContext
	runtime runtimegraph.Identity
}

type parentResolution struct {
	found  bool
	parent runtimegraph.Identity
}

type projectionConsistency struct {
	declaredParents map[runtimegraph.Identity]string
	resolutions     map[resolutionKey]parentResolution
}

func newProjectionConsistency() *projectionConsistency {
	return &projectionConsistency{
		declaredParents: map[runtimegraph.Identity]string{},
		resolutions:     map[resolutionKey]parentResolution{},
	}
}

func (c *projectionConsistency) observePath(
	path runtimegraph.ResolutionPath,
	context runtimegraph.ResolutionContext,
) error {
	for i := range path.Runtimes {
		runtime := path.Runtimes[i]
		if previous, observed := c.declaredParents[runtime.Identity]; observed && previous != runtime.ParentName {
			return fmt.Errorf(
				"%w: runtime %s has inconsistent declared parents", ErrInvalidProjection, runtime.Identity.Name,
			)
		}
		c.declaredParents[runtime.Identity] = runtime.ParentName
		if runtime.ParentName == "" {
			continue
		}

		resolution, definitive := definitiveParentResolution(path, i)
		if !definitive {
			continue
		}
		key := resolutionKey{context: context, runtime: runtime.Identity}
		if previous, observed := c.resolutions[key]; observed && previous != resolution {
			return fmt.Errorf(
				"%w: runtime %s has inconsistent parent resolution", ErrInvalidProjection, runtime.Identity.Name,
			)
		}
		c.resolutions[key] = resolution
	}
	return nil
}

func definitiveParentResolution(path runtimegraph.ResolutionPath, runtimeIndex int) (parentResolution, bool) {
	runtime := path.Runtimes[runtimeIndex]
	if runtime.ResolvedParent != nil {
		return parentResolution{found: true, parent: *runtime.ResolvedParent}, true
	}
	if runtimeIndex == 0 && path.Issue != nil {
		switch path.Issue.Code {
		case runtimegraph.IssueParentMissing:
			return parentResolution{}, true
		case runtimegraph.IssueMaxDepthExceeded:
			return parentResolution{}, false
		}
	}
	return parentResolution{}, false
}

func projectContexts(
	projection runtimegraph.Projection,
	statuses map[reportv1alpha1.RuntimeTreeCollectionKind]reportv1alpha1.RuntimeTreeCollectionStatus,
) ([]reportv1alpha1.RuntimeTreeContext, map[runtimegraph.Identity]headLocation, error) {
	if len(projection.Contexts) == 0 {
		return nil, nil, fmt.Errorf("%w: at least one context is required", ErrInvalidProjection)
	}
	result := make([]reportv1alpha1.RuntimeTreeContext, 0, len(projection.Contexts))
	heads := make(map[runtimegraph.Identity]headLocation)
	seenContexts := make(map[runtimegraph.ResolutionContext]struct{}, len(projection.Contexts))
	consistency := newProjectionConsistency()
	directTargetHeads := 0
	for _, sourceContext := range projection.Contexts {
		if _, duplicate := seenContexts[sourceContext.Context]; duplicate {
			return nil, nil, fmt.Errorf("%w: duplicate resolution context", ErrInvalidProjection)
		}
		seenContexts[sourceContext.Context] = struct{}{}
		context, err := projectResolutionContext(sourceContext.Context)
		if err != nil {
			return nil, nil, err
		}
		if len(sourceContext.Paths) == 0 {
			return nil, nil, fmt.Errorf("%w: context has no paths", ErrInvalidProjection)
		}
		projectedContext := reportv1alpha1.RuntimeTreeContext{
			Context: context, ResolutionCompleteness: contextCompleteness(context.Mode, statuses),
			Paths: []reportv1alpha1.RuntimeTreePath{},
		}
		for _, sourcePath := range sourceContext.Paths {
			if _, duplicate := heads[sourcePath.Subject]; duplicate {
				return nil, nil, fmt.Errorf("%w: duplicate head %s", ErrInvalidProjection, sourcePath.Subject.Name)
			}
			path, err := projectPath(sourcePath, sourceContext.Context, projection.Target)
			if err != nil {
				return nil, nil, err
			}
			if err := consistency.observePath(sourcePath, sourceContext.Context); err != nil {
				return nil, nil, err
			}
			if sourcePath.Subject == projection.Target {
				directTargetHeads++
			}
			heads[sourcePath.Subject] = headLocation{context: len(result), path: len(projectedContext.Paths)}
			projectedContext.Paths = append(projectedContext.Paths, path)
		}
		result = append(result, projectedContext)
	}
	if directTargetHeads != 1 {
		return nil, nil, fmt.Errorf("%w: target must have exactly one direct head path", ErrInvalidProjection)
	}
	return result, heads, nil
}

func projectResolutionContext(
	context runtimegraph.ResolutionContext,
) (reportv1alpha1.RuntimeTreeResolutionContext, error) {
	switch context.Mode {
	case runtimegraph.ResolutionModeCluster:
		if context.Namespace != "" {
			return reportv1alpha1.RuntimeTreeResolutionContext{}, fmt.Errorf(
				"%w: cluster context cannot have a namespace", ErrInvalidProjection,
			)
		}
		return reportv1alpha1.RuntimeTreeResolutionContext{
			Mode: reportv1alpha1.RuntimeTreeResolutionModeCluster,
		}, nil
	case runtimegraph.ResolutionModeNamespaced:
		if len(validation.IsDNS1123Label(context.Namespace)) != 0 {
			return reportv1alpha1.RuntimeTreeResolutionContext{}, fmt.Errorf(
				"%w: namespaced context namespace %q is invalid", ErrInvalidProjection, context.Namespace,
			)
		}
		return reportv1alpha1.RuntimeTreeResolutionContext{
			Mode: reportv1alpha1.RuntimeTreeResolutionModeNamespaced, Namespace: context.Namespace,
		}, nil
	default:
		return reportv1alpha1.RuntimeTreeResolutionContext{}, fmt.Errorf(
			"%w: unsupported resolution mode %q", ErrInvalidProjection, context.Mode,
		)
	}
}

func contextCompleteness(
	mode reportv1alpha1.RuntimeTreeResolutionMode,
	statuses map[reportv1alpha1.RuntimeTreeCollectionKind]reportv1alpha1.RuntimeTreeCollectionStatus,
) reportv1alpha1.RuntimeTreeSnapshotCompleteness {
	required := []reportv1alpha1.RuntimeTreeCollectionKind{
		reportv1alpha1.RuntimeTreeCollectionClusterServingRuntime,
	}
	if mode == reportv1alpha1.RuntimeTreeResolutionModeNamespaced {
		required = append(required, reportv1alpha1.RuntimeTreeCollectionServingRuntime)
	}
	for _, kind := range required {
		if statuses[kind] != reportv1alpha1.RuntimeTreeCollectionStatusComplete {
			return reportv1alpha1.RuntimeTreeSnapshotPartial
		}
	}
	return reportv1alpha1.RuntimeTreeSnapshotComplete
}

func projectPath(
	path runtimegraph.ResolutionPath,
	context runtimegraph.ResolutionContext,
	target runtimegraph.Identity,
) (reportv1alpha1.RuntimeTreePath, error) {
	if err := validatePath(path, context, target); err != nil {
		return reportv1alpha1.RuntimeTreePath{}, err
	}
	head, err := projectIdentity(path.Subject)
	if err != nil {
		return reportv1alpha1.RuntimeTreePath{}, err
	}
	result := reportv1alpha1.RuntimeTreePath{
		Head: head, Runtimes: make([]reportv1alpha1.RuntimeTreeRuntime, len(path.Runtimes)),
		Dependents: []reportv1alpha1.RuntimeTreeDependent{},
	}
	for i := range path.Runtimes {
		result.Runtimes[i], err = projectRuntime(path.Runtimes[i])
		if err != nil {
			return reportv1alpha1.RuntimeTreePath{}, err
		}
	}
	if path.Issue != nil {
		issue, err := projectIssue(*path.Issue)
		if err != nil {
			return reportv1alpha1.RuntimeTreePath{}, err
		}
		result.Issue = &issue
	}
	return result, nil
}

func validatePath(
	path runtimegraph.ResolutionPath,
	context runtimegraph.ResolutionContext,
	target runtimegraph.Identity,
) error {
	if len(path.Runtimes) == 0 || path.Subject != path.Runtimes[len(path.Runtimes)-1].Identity {
		return fmt.Errorf("%w: head and final runtime must match", ErrInvalidProjection)
	}
	if len(path.Runtimes) > constants.RuntimeInheritMaxDepth {
		return fmt.Errorf("%w: path exceeds the controller depth bound", ErrInvalidProjection)
	}
	if err := validateHeadContext(path.Subject, context); err != nil {
		return err
	}
	seenNames := make(map[string]struct{}, len(path.Runtimes))
	targetOccurrences := 0
	for i := range path.Runtimes {
		runtime := path.Runtimes[i]
		if err := validateRuntimeInContext(runtime, context); err != nil {
			return err
		}
		if _, duplicate := seenNames[runtime.Identity.Name]; duplicate {
			return fmt.Errorf("%w: a path repeats runtime name %q", ErrInvalidProjection, runtime.Identity.Name)
		}
		seenNames[runtime.Identity.Name] = struct{}{}
		if runtime.Identity == target {
			targetOccurrences++
		}
		if i > 0 {
			expected := path.Runtimes[i-1].Identity
			if runtime.ResolvedParent == nil || *runtime.ResolvedParent != expected || runtime.ParentName != expected.Name {
				return fmt.Errorf("%w: structural parent disagrees for %s", ErrInvalidProjection, runtime.Identity.Name)
			}
		}
	}
	if targetOccurrences != 1 {
		return fmt.Errorf("%w: every path must visit the target exactly once", ErrInvalidProjection)
	}
	return validateBoundary(path)
}

func validateHeadContext(identity runtimegraph.Identity, context runtimegraph.ResolutionContext) error {
	if context.Mode == runtimegraph.ResolutionModeCluster {
		if identity.Kind != runtimegraph.KindClusterServingRuntime || identity.Namespace != "" {
			return fmt.Errorf("%w: cluster context head must be a ClusterServingRuntime", ErrInvalidProjection)
		}
		return nil
	}
	if context.Mode == runtimegraph.ResolutionModeNamespaced &&
		identity.Kind == runtimegraph.KindServingRuntime && identity.Namespace == context.Namespace {
		return nil
	}
	return fmt.Errorf("%w: namespaced context head must be a ServingRuntime in that namespace", ErrInvalidProjection)
}

func validateRuntimeInContext(runtime runtimegraph.Runtime, context runtimegraph.ResolutionContext) error {
	if _, err := projectIdentity(runtime.Identity); err != nil {
		return err
	}
	if !identityAllowedInContext(runtime.Identity, context) {
		return fmt.Errorf("%w: runtime is incompatible with its resolution context", ErrInvalidProjection)
	}
	if runtime.ParentName != "" && len(validation.IsDNS1123Subdomain(runtime.ParentName)) != 0 {
		return fmt.Errorf("%w: parent name %q is invalid", ErrInvalidProjection, runtime.ParentName)
	}
	if runtime.ResolvedParent != nil {
		if _, err := projectIdentity(*runtime.ResolvedParent); err != nil {
			return err
		}
		if !identityAllowedInContext(*runtime.ResolvedParent, context) ||
			runtime.ParentName == "" || runtime.ParentName != runtime.ResolvedParent.Name {
			return fmt.Errorf("%w: declared and resolved parent disagree for %s", ErrInvalidProjection, runtime.Identity.Name)
		}
	}
	return nil
}

func identityAllowedInContext(identity runtimegraph.Identity, context runtimegraph.ResolutionContext) bool {
	switch context.Mode {
	case runtimegraph.ResolutionModeCluster:
		return identity.Kind == runtimegraph.KindClusterServingRuntime && identity.Namespace == ""
	case runtimegraph.ResolutionModeNamespaced:
		return identity.Kind == runtimegraph.KindClusterServingRuntime && identity.Namespace == "" ||
			identity.Kind == runtimegraph.KindServingRuntime && identity.Namespace == context.Namespace
	default:
		return false
	}
}

func validateBoundary(path runtimegraph.ResolutionPath) error {
	boundary := path.Runtimes[0]
	if path.Issue == nil {
		if boundary.ParentName != "" || boundary.ResolvedParent != nil {
			return fmt.Errorf("%w: unresolved boundary has no issue", ErrInvalidProjection)
		}
		return nil
	}
	issue := path.Issue
	if issue.Subject != path.Subject || issue.ParentName == "" || issue.ParentName != boundary.ParentName {
		return fmt.Errorf("%w: issue does not describe its head boundary", ErrInvalidProjection)
	}
	childFirst := reverseRuntimeIdentities(path.Runtimes)
	for _, identity := range issue.Path {
		if !identityAllowedInContext(identity, resolutionContextForPath(path)) {
			return fmt.Errorf("%w: issue path identity is incompatible with context", ErrInvalidProjection)
		}
		if _, err := projectIdentity(identity); err != nil {
			return err
		}
	}
	switch issue.Code {
	case runtimegraph.IssueParentMissing:
		if len(path.Runtimes) >= constants.RuntimeInheritMaxDepth || boundary.ResolvedParent != nil ||
			!equalIdentitySlices(issue.Path, childFirst) {
			return fmt.Errorf("%w: malformed missing-parent boundary", ErrInvalidProjection)
		}
	case runtimegraph.IssueMaxDepthExceeded:
		if len(path.Runtimes) != constants.RuntimeInheritMaxDepth || boundary.ResolvedParent != nil ||
			!equalIdentitySlices(issue.Path, childFirst) {
			return fmt.Errorf("%w: malformed max-depth boundary", ErrInvalidProjection)
		}
	case runtimegraph.IssueCycleDetected:
		if len(path.Runtimes) >= constants.RuntimeInheritMaxDepth || boundary.ResolvedParent == nil ||
			len(issue.Path) != len(childFirst)+1 ||
			!equalIdentitySlices(issue.Path[:len(childFirst)], childFirst) ||
			issue.Path[len(issue.Path)-1] != *boundary.ResolvedParent ||
			!containsIdentity(childFirst, *boundary.ResolvedParent) {
			return fmt.Errorf("%w: malformed cycle boundary", ErrInvalidProjection)
		}
	default:
		return fmt.Errorf("%w: unsupported issue code %q", ErrInvalidProjection, issue.Code)
	}
	return nil
}

// resolutionContextForPath returns the only context compatible with the
// path's head. validatePath already checked the original context against it.
func resolutionContextForPath(path runtimegraph.ResolutionPath) runtimegraph.ResolutionContext {
	if path.Subject.Kind == runtimegraph.KindServingRuntime {
		return runtimegraph.ResolutionContext{Mode: runtimegraph.ResolutionModeNamespaced, Namespace: path.Subject.Namespace}
	}
	return runtimegraph.ResolutionContext{Mode: runtimegraph.ResolutionModeCluster}
}

func reverseRuntimeIdentities(runtimes []runtimegraph.Runtime) []runtimegraph.Identity {
	result := make([]runtimegraph.Identity, len(runtimes))
	for i := range runtimes {
		result[len(runtimes)-1-i] = runtimes[i].Identity
	}
	return result
}

func equalIdentitySlices(left, right []runtimegraph.Identity) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func containsIdentity(values []runtimegraph.Identity, candidate runtimegraph.Identity) bool {
	for _, value := range values {
		if value == candidate {
			return true
		}
	}
	return false
}

func projectIssue(issue runtimegraph.Issue) (reportv1alpha1.RuntimeTreeIssue, error) {
	result := reportv1alpha1.RuntimeTreeIssue{ParentName: issue.ParentName}
	switch issue.Code {
	case runtimegraph.IssueParentMissing:
		result.Code = reportv1alpha1.RuntimeTreeIssueParentMissing
	case runtimegraph.IssueCycleDetected:
		result.Code = reportv1alpha1.RuntimeTreeIssueCycleDetected
	case runtimegraph.IssueMaxDepthExceeded:
		result.Code = reportv1alpha1.RuntimeTreeIssueMaxDepthExceeded
	default:
		return reportv1alpha1.RuntimeTreeIssue{}, fmt.Errorf(
			"%w: unsupported issue code %q", ErrInvalidProjection, issue.Code,
		)
	}
	var err error
	result.Subject, err = projectIdentity(issue.Subject)
	if err != nil {
		return reportv1alpha1.RuntimeTreeIssue{}, err
	}
	result.Path = make([]reportv1alpha1.RuntimeTreeIdentity, len(issue.Path))
	for i := range issue.Path {
		result.Path[i], err = projectIdentity(issue.Path[i])
		if err != nil {
			return reportv1alpha1.RuntimeTreeIssue{}, err
		}
	}
	return result, nil
}

type dependentIdentity struct {
	kind      reportv1alpha1.RuntimeTreeDependentKind
	namespace string
	name      string
}

func attachDependents(
	contexts []reportv1alpha1.RuntimeTreeContext,
	heads map[runtimegraph.Identity]headLocation,
	values []DependentLeaf,
) error {
	seen := make(map[dependentIdentity]DependentLeaf, len(values))
	for _, value := range values {
		if err := validateDependent(value); err != nil {
			return err
		}
		key := dependentIdentity{kind: value.Kind, namespace: value.Namespace, name: value.Name}
		if previous, duplicate := seen[key]; duplicate {
			if previous == value {
				return fmt.Errorf("%w: duplicate dependent %s/%s", ErrInvalidDependent, value.Namespace, value.Name)
			}
			return fmt.Errorf("%w: ambiguous dependent %s/%s", ErrInvalidDependent, value.Namespace, value.Name)
		}
		seen[key] = value
		location, visible := heads[value.Runtime]
		if !visible {
			return fmt.Errorf("%w: %s", ErrDependentRuntimeNotVisible, value.Runtime.Name)
		}
		contexts[location.context].Paths[location.path].Dependents = append(
			contexts[location.context].Paths[location.path].Dependents,
			reportv1alpha1.RuntimeTreeDependent{
				Kind: value.Kind, Namespace: value.Namespace, Name: value.Name, UID: value.UID,
			},
		)
	}
	return nil
}

func validateDependent(value DependentLeaf) error {
	if value.Kind != reportv1alpha1.RuntimeTreeDependentInferenceService ||
		len(validation.IsDNS1123Label(value.Namespace)) != 0 ||
		len(validation.IsDNS1123Subdomain(value.Name)) != 0 {
		return fmt.Errorf("%w: incomplete or unsupported leaf identity", ErrInvalidDependent)
	}
	switch value.Runtime.Kind {
	case runtimegraph.KindClusterServingRuntime:
		if value.Runtime.Namespace != "" || len(validation.IsDNS1123Subdomain(value.Runtime.Name)) != 0 {
			return fmt.Errorf("%w: malformed ClusterServingRuntime identity", ErrInvalidDependent)
		}
	case runtimegraph.KindServingRuntime:
		if len(validation.IsDNS1123Label(value.Runtime.Namespace)) != 0 ||
			len(validation.IsDNS1123Subdomain(value.Runtime.Name)) != 0 ||
			value.Namespace != value.Runtime.Namespace {
			return fmt.Errorf("%w: malformed ServingRuntime identity", ErrInvalidDependent)
		}
	default:
		return fmt.Errorf("%w: unsupported runtime kind %q", ErrInvalidDependent, value.Runtime.Kind)
	}
	return nil
}

func validateVisibleCounts(
	snapshot reportv1alpha1.RuntimeTreeSnapshot,
	contexts []reportv1alpha1.RuntimeTreeContext,
) error {
	visibleRuntimes := map[reportv1alpha1.RuntimeTreeIdentity]struct{}{}
	visibleDependents := map[dependentIdentity]struct{}{}
	for _, context := range contexts {
		for _, path := range context.Paths {
			for _, runtime := range path.Runtimes {
				visibleRuntimes[runtime.Identity] = struct{}{}
			}
			for _, dependent := range path.Dependents {
				visibleDependents[dependentIdentity{
					kind: dependent.Kind, namespace: dependent.Namespace, name: dependent.Name,
				}] = struct{}{}
			}
		}
	}

	minimums := map[reportv1alpha1.RuntimeTreeCollectionKind]int{
		reportv1alpha1.RuntimeTreeCollectionInferenceService: len(visibleDependents),
	}
	for identity := range visibleRuntimes {
		switch identity.Kind {
		case reportv1alpha1.RuntimeKindClusterServingRuntime:
			minimums[reportv1alpha1.RuntimeTreeCollectionClusterServingRuntime]++
		case reportv1alpha1.RuntimeKindServingRuntime:
			minimums[reportv1alpha1.RuntimeTreeCollectionServingRuntime]++
		}
	}
	for _, collection := range snapshot.Collections {
		if collection.ObservedItems < minimums[collection.Kind] {
			return fmt.Errorf(
				"%w: %q observed item count is below visible report objects",
				ErrInvalidSnapshot, collection.Kind,
			)
		}
	}
	return nil
}
