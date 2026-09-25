package effective

import (
	"context"
	"errors"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	appstyped "k8s.io/client-go/kubernetes/typed/apps/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/runtimerevision"
	"sigs.k8s.io/ome/pkg/runtimeselector"
)

// ErrActiveRuntimeUnavailable indicates that controller evidence does not
// select an active runtime configuration.
var ErrActiveRuntimeUnavailable = errors.New("active runtime configuration is unavailable")

// ErrActiveRuntimeInconsistent indicates that the selected configuration is
// controller-readable but fails the stronger consistency contract.
var ErrActiveRuntimeInconsistent = errors.New("active runtime configuration is not consistency-safe")

type liveRuntimeResolver interface {
	ResolveLive(context.Context, *v1beta1.InferenceService) (*LiveConfiguration, error)
}

type revisionNamespace interface {
	Get(context.Context, string, metav1.GetOptions) (*appsv1.ControllerRevision, error)
	List(context.Context, metav1.ListOptions) (*appsv1.ControllerRevisionList, error)
}

type revisionNamespaceGetter func(string) revisionNamespace

type typedRevisionGetter struct {
	getter appstyped.ControllerRevisionsGetter
}

func (g typedRevisionGetter) namespace(namespace string) revisionNamespace {
	return g.getter.ControllerRevisions(namespace)
}

// ConfigurationOrigin identifies the snapshot used to form an active
// configuration.
type ConfigurationOrigin string

const (
	ConfigurationOriginLiveRuntime        ConfigurationOrigin = "LiveRuntime"
	ConfigurationOriginControllerRevision ConfigurationOrigin = "ControllerRevision"
)

// RuntimePinMode describes whether controller intent selects live runtime
// content, a managed status pin, or an explicit requested pin.
type RuntimePinMode string

const (
	RuntimePinModeAutoSync    RuntimePinMode = "AutoSync"
	RuntimePinModeManagedPin  RuntimePinMode = "ManagedPin"
	RuntimePinModeExplicitPin RuntimePinMode = "ExplicitPin"
	RuntimePinModeInvalidPin  RuntimePinMode = "InvalidPin"
)

// RuntimePinState summarizes the controller-usable outcome of pin resolution.
type RuntimePinState string

const (
	RuntimePinStateNotApplicable           RuntimePinState = "NotApplicable"
	RuntimePinStateAwaitingPin             RuntimePinState = "AwaitingPin"
	RuntimePinStateResolved                RuntimePinState = "Resolved"
	RuntimePinStateDesiredReportedMismatch RuntimePinState = "DesiredReportedMismatch"
	RuntimePinStateRevisionMissing         RuntimePinState = "RevisionMissing"
	RuntimePinStateRevisionInvalid         RuntimePinState = "RevisionInvalid"
	RuntimePinStateRevisionDisabled        RuntimePinState = "RevisionDisabled"
	RuntimePinStateUnavailable             RuntimePinState = "Unavailable"
	RuntimePinStateInvalidIntent           RuntimePinState = "InvalidIntent"
)

// StatusFreshness compares status.observedGeneration with object generation.
type StatusFreshness string

const (
	StatusFreshnessCurrent      StatusFreshness = "Current"
	StatusFreshnessStale        StatusFreshness = "Stale"
	StatusFreshnessInconsistent StatusFreshness = "Inconsistent"
	StatusFreshnessUnknown      StatusFreshness = "Unknown"
)

// SyncTokenState reports the relationship between private sync-token values
// without exposing either value.
type SyncTokenState string

const (
	SyncTokenStateAbsent       SyncTokenState = "Absent"
	SyncTokenStateAcknowledged SyncTokenState = "Acknowledged"
	SyncTokenStatePending      SyncTokenState = "Pending"
	SyncTokenStateStatusOnly   SyncTokenState = "StatusOnly"
)

// RuntimeDriftState is the bounded state of the RuntimeDrifted condition.
type RuntimeDriftState string

const (
	RuntimeDriftStateNotReported     RuntimeDriftState = "NotReported"
	RuntimeDriftStateReportedTrue    RuntimeDriftState = "ReportedTrue"
	RuntimeDriftStateReportedFalse   RuntimeDriftState = "ReportedFalse"
	RuntimeDriftStateReportedUnknown RuntimeDriftState = "ReportedUnknown"
	RuntimeDriftStateMalformed       RuntimeDriftState = "Malformed"
)

// RuntimeDriftReason is the allowlisted reason for reported runtime drift.
type RuntimeDriftReason string

const (
	RuntimeDriftReasonRevisionMismatch     RuntimeDriftReason = "RevisionMismatch"
	RuntimeDriftReasonRevisionMissing      RuntimeDriftReason = "RevisionMissing"
	RuntimeDriftReasonSourceRuntimeMissing RuntimeDriftReason = "SourceRuntimeMissing"
	RuntimeDriftReasonRuntimeMismatch      RuntimeDriftReason = "RuntimeMismatch"
	RuntimeDriftReasonPinAdvanced          RuntimeDriftReason = "PinAdvanced"
	RuntimeDriftReasonOther                RuntimeDriftReason = "Other"
)

// RuntimeHashRelation compares recomputed runtime content hashes.
type RuntimeHashRelation string

const (
	RuntimeHashRelationUnknown   RuntimeHashRelation = "Unknown"
	RuntimeHashRelationEqual     RuntimeHashRelation = "Equal"
	RuntimeHashRelationDifferent RuntimeHashRelation = "Different"
	RuntimeHashRelationAmbiguous RuntimeHashRelation = "Ambiguous"
)

// LiveRuntimeAvailability is the bounded outcome of independent live runtime
// resolution. Unavailable includes hard read failures and incomplete snapshots.
type LiveRuntimeAvailability string

const (
	LiveRuntimeAvailable   LiveRuntimeAvailability = "Available"
	LiveRuntimeNotFound    LiveRuntimeAvailability = "NotFound"
	LiveRuntimeDisabled    LiveRuntimeAvailability = "Disabled"
	LiveRuntimeUnavailable LiveRuntimeAvailability = "Unavailable"
)

// RevisionConsistencyState summarizes the revision writer-contract checks.
type RevisionConsistencyState string

const (
	RevisionConsistencyConsistent   RevisionConsistencyState = "Consistent"
	RevisionConsistencyInconsistent RevisionConsistencyState = "Inconsistent"
	RevisionConsistencyUnknown      RevisionConsistencyState = "Unknown"
)

// RevisionConsistencyCode identifies a bounded consistency or collection
// anomaly without exposing raw revision content.
type RevisionConsistencyCode string

const (
	RevisionConsistencyCreatedBy            RevisionConsistencyCode = "CreatedBy"
	RevisionConsistencySourceName           RevisionConsistencyCode = "SourceName"
	RevisionConsistencySourceKind           RevisionConsistencyCode = "SourceKind"
	RevisionConsistencySourceNamespace      RevisionConsistencyCode = "SourceNamespace"
	RevisionConsistencyHashLabelInvalid     RevisionConsistencyCode = "HashLabelInvalid"
	RevisionConsistencyHashLabelMismatch    RevisionConsistencyCode = "HashLabelMismatch"
	RevisionConsistencyNameHash             RevisionConsistencyCode = "NameHash"
	RevisionConsistencyOrdinal              RevisionConsistencyCode = "Ordinal"
	RevisionConsistencyPayloadCanonicality  RevisionConsistencyCode = "PayloadCanonicality"
	RevisionConsistencyUnexpectedDataObject RevisionConsistencyCode = "UnexpectedDataObject"
	RevisionConsistencyMalformedPayload     RevisionConsistencyCode = "MalformedPayload"
	RevisionConsistencyReturnedIdentity     RevisionConsistencyCode = "ReturnedIdentity"
	RevisionConsistencyDuplicateIdentity    RevisionConsistencyCode = "DuplicateIdentity"
	RevisionConsistencyConflictingIdentity  RevisionConsistencyCode = "ConflictingIdentity"
	RevisionConsistencyDuplicateContentHash RevisionConsistencyCode = "DuplicateContentHash"
	RevisionConsistencyShortHashCollision   RevisionConsistencyCode = "ShortHashCollision"
)

// RuntimeRevisionRole records why a revision observation was collected.
type RuntimeRevisionRole string

const (
	RuntimeRevisionRoleRequested RuntimeRevisionRole = "Requested"
	RuntimeRevisionRoleReported  RuntimeRevisionRole = "Reported"
	RuntimeRevisionRoleActive    RuntimeRevisionRole = "Active"
	RuntimeRevisionRoleHistory   RuntimeRevisionRole = "History"
)

// RuntimeSourceIssueCode classifies a bounded evidence-source failure.
type RuntimeSourceIssueCode string

const (
	RuntimeSourceIssueLiveNotFound       RuntimeSourceIssueCode = "LiveNotFound"
	RuntimeSourceIssueLiveDisabled       RuntimeSourceIssueCode = "LiveDisabled"
	RuntimeSourceIssueLiveUnavailable    RuntimeSourceIssueCode = "LiveUnavailable"
	RuntimeSourceIssueRevisionNotFound   RuntimeSourceIssueCode = "RevisionNotFound"
	RuntimeSourceIssueRevisionGetFailed  RuntimeSourceIssueCode = "RevisionGetFailed"
	RuntimeSourceIssueRevisionListFailed RuntimeSourceIssueCode = "RevisionListFailed"
)

// RuntimeSourceIssue retains a diagnostic cause for errors.Is/errors.As while
// exposing only a fixed code and fixed error text.
type RuntimeSourceIssue struct {
	Code         RuntimeSourceIssueCode
	RevisionName string
	cause        error
}

// Error returns fixed redacted text for the issue code.
func (i RuntimeSourceIssue) Error() string {
	switch i.Code {
	case RuntimeSourceIssueLiveNotFound:
		return "live runtime was not found"
	case RuntimeSourceIssueLiveDisabled:
		return "live runtime is disabled"
	case RuntimeSourceIssueLiveUnavailable:
		return "live runtime evidence is unavailable"
	case RuntimeSourceIssueRevisionNotFound:
		return "runtime revision was not found"
	case RuntimeSourceIssueRevisionGetFailed:
		return "runtime revision evidence read failed"
	case RuntimeSourceIssueRevisionListFailed:
		return "runtime revision history read failed"
	default:
		return "runtime source evidence is unavailable"
	}
}

// Unwrap preserves the diagnostic cause for errors.Is and errors.As.
func (i RuntimeSourceIssue) Unwrap() error { return i.cause }

// GoString returns fixed redacted text for %#v formatting.
func (RuntimeSourceIssue) GoString() string { return "<effective.RuntimeSourceIssue redacted>" }

// RuntimeResolveOptions controls optional bounded evidence collection.
type RuntimeResolveOptions struct {
	IncludeHistory bool
}

// ActiveConfiguration is the controller-selected configuration. spec is kept
// private because it may contain credentials and arbitrary pod configuration.
type ActiveConfiguration struct {
	Origin            ConfigurationOrigin
	RuntimeName       string
	RuntimeKind       string
	RuntimeNamespace  string
	RevisionName      string
	RevisionShortHash string
	components        []EffectiveComponent
	Consistency       RevisionConsistencyState
	spec              *v1beta1.ServingRuntimeSpec
}

// MarshalJSON prevents accidental serialization of private runtime content.
func (ActiveConfiguration) MarshalJSON() ([]byte, error) { return nil, ErrUnsafeRuntimeSerialization }

// MarshalYAML prevents accidental serialization of private runtime content.
func (ActiveConfiguration) MarshalYAML() (any, error) { return nil, ErrUnsafeRuntimeSerialization }

// String returns a fixed redacted representation.
func (ActiveConfiguration) String() string { return "<effective.ActiveConfiguration redacted>" }

// GoString returns a fixed redacted representation for %#v formatting.
func (ActiveConfiguration) GoString() string { return "<effective.ActiveConfiguration redacted>" }

// RuntimeRevisionObservation exposes allowlisted identity and consistency
// provenance while retaining decoded content and raw hashes privately.
type RuntimeRevisionObservation struct {
	Name              string
	Namespace         string
	UID               string
	ResourceVersion   string
	SourceName        string
	SourceKind        string
	SourceNamespace   string
	ShortHash         string
	CreationTimestamp metav1.Time
	Ordinal           int64
	roles             []RuntimeRevisionRole
	Available         bool
	Consistency       RevisionConsistencyState
	consistencyCodes  []RevisionConsistencyCode
	RelationToLive    RuntimeHashRelation
	Disabled          bool
	spec              *v1beta1.ServingRuntimeSpec
	fullHash          string
	computedShortHash string
	rawShortHash      string
	expectedName      string
	expectedNamespace string
	objectFingerprint string
	objectReturned    bool
	usable            bool
	readError         error
	notFound          bool
}

// MarshalJSON prevents accidental serialization of private revision content.
func (RuntimeRevisionObservation) MarshalJSON() ([]byte, error) {
	return nil, ErrUnsafeRuntimeSerialization
}

// MarshalYAML prevents accidental serialization of private revision content.
func (RuntimeRevisionObservation) MarshalYAML() (any, error) {
	return nil, ErrUnsafeRuntimeSerialization
}

// String returns a fixed redacted representation.
func (RuntimeRevisionObservation) String() string {
	return "<effective.RuntimeRevisionObservation redacted>"
}

// GoString returns a fixed redacted representation for %#v formatting.
func (RuntimeRevisionObservation) GoString() string {
	return "<effective.RuntimeRevisionObservation redacted>"
}

// InferenceServiceIdentity is the safe primary object identity associated with
// collected evidence. Exact snapshot metadata remains private to RuntimeState.
type InferenceServiceIdentity struct {
	Name      string
	Namespace string
	UID       string
}

type inferenceServiceBinding struct {
	identity        InferenceServiceIdentity
	resourceVersion string
}

// RuntimeState keeps unsafe resolution snapshots internal to the effective
// layer while exposing bounded provenance for allowlisted report projection.
type RuntimeState struct {
	Generation              int64
	ObservedGeneration      int64
	RuntimeName             string
	RuntimeKind             string
	RuntimeNamespace        string
	DeclaredSourceKind      string
	DeclaredSourceNamespace string
	SelectionSource         RuntimeSelectionSource
	PinMode                 RuntimePinMode
	PinState                RuntimePinState
	RequestedRevisionName   string
	ReportedRevisionName    string
	ActiveRevisionName      string
	StatusFreshness         StatusFreshness
	SyncTokenState          SyncTokenState
	DriftState              RuntimeDriftState
	DriftReason             RuntimeDriftReason
	LiveToActive            RuntimeHashRelation
	LiveShortHash           string
	live                    *LiveConfiguration
	active                  *ActiveConfiguration
	revisions               []RuntimeRevisionObservation
	issues                  []RuntimeSourceIssue
	HistoryRequested        bool
	HistoryPages            int
	HistoryPageLimit        int
	HistoryRequestLimit     int
	HistoryRequestedPages   int
	HistoryObservedPages    int
	HistoryComplete         bool
	HistoryTruncated        bool
	historyNamespace        string
	liveAvailability        liveAvailability
	inferenceService        inferenceServiceBinding
}

type liveAvailability uint8

const (
	liveUnreadable liveAvailability = iota
	liveAvailable
	liveNotFound
	liveDisabled
)

// MarshalJSON prevents accidental serialization of the aggregate unsafe state.
func (RuntimeState) MarshalJSON() ([]byte, error) { return nil, ErrUnsafeRuntimeSerialization }

// MarshalYAML prevents accidental serialization of the aggregate unsafe state.
func (RuntimeState) MarshalYAML() (any, error) { return nil, ErrUnsafeRuntimeSerialization }

// String returns a fixed redacted representation.
func (RuntimeState) String() string { return "<effective.RuntimeState redacted>" }

// GoString returns a fixed redacted representation for %#v formatting.
func (RuntimeState) GoString() string { return "<effective.RuntimeState redacted>" }

// RuntimePinResolver collects live and revision evidence without changing
// Kubernetes state.
type RuntimePinResolver struct {
	revisions    revisionNamespaceGetter
	live         liveRuntimeResolver
	omeNamespace string
	limits       paging.Limits
}

// NewRuntimePinResolver constructs a bounded runtime pin provenance resolver.
func NewRuntimePinResolver(revisions appstyped.ControllerRevisionsGetter, live *RuntimeResolver, omeNamespace string, limits paging.Limits) (*RuntimePinResolver, error) {
	if revisions == nil {
		return nil, errors.New("ControllerRevision client must not be nil")
	}
	if live == nil {
		return nil, errors.New("live runtime resolver must not be nil")
	}
	return newRuntimePinResolver(typedRevisionGetter{getter: revisions}.namespace, live, omeNamespace, limits)
}

func newRuntimePinResolver(revisions revisionNamespaceGetter, live liveRuntimeResolver, omeNamespace string, limits paging.Limits) (*RuntimePinResolver, error) {
	if revisions == nil {
		return nil, errors.New("ControllerRevision client must not be nil")
	}
	if live == nil {
		return nil, errors.New("live runtime resolver must not be nil")
	}
	if omeNamespace == "" {
		return nil, errors.New("OME namespace must not be empty")
	}
	if limits.PageSize <= 0 || limits.MaxItems <= 0 || limits.MaxPages <= 0 || limits.RequestTimeout <= 0 {
		return nil, errors.New("revision paging limits are invalid")
	}
	if err := limits.Validate(); err != nil {
		return nil, errors.New("revision paging limits are invalid")
	}
	return &RuntimePinResolver{revisions: revisions, live: live, omeNamespace: omeNamespace, limits: limits}, nil
}

// Resolve independently collects live, exact pin, and optional history
// evidence and returns a defensive-access state envelope.
func (r *RuntimePinResolver) Resolve(ctx context.Context, isvc *v1beta1.InferenceService, options RuntimeResolveOptions) (*RuntimeState, error) {
	if r == nil || r.revisions == nil || r.live == nil {
		return nil, errors.New("runtime pin resolver is not configured")
	}
	if ctx == nil {
		return nil, errors.New("context must not be nil")
	}
	if isvc == nil {
		return nil, errors.New("InferenceService must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	binding := inferenceServiceBinding{
		identity: InferenceServiceIdentity{
			Name: isvc.Name, Namespace: isvc.Namespace, UID: string(isvc.UID),
		},
		resourceVersion: isvc.ResourceVersion,
	}

	mode, declaredKind, declaredNamespace := runtimePinIntent(isvc)
	liveCtx, cancelLive := context.WithTimeout(ctx, r.limits.RequestTimeout)
	live, liveErr := r.live.ResolveLive(liveCtx, isvc)
	cancelLive()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	state := &RuntimeState{
		Generation: isvc.Generation, ObservedGeneration: isvc.Status.ObservedGeneration,
		PinMode: mode, PinState: RuntimePinStateNotApplicable,
		DeclaredSourceKind: declaredKind, DeclaredSourceNamespace: declaredNamespace,
		SelectionSource: RuntimeSelected, LiveToActive: RuntimeHashRelationUnknown,
		live: live, HistoryRequested: false, HistoryComplete: false,
		inferenceService: binding,
	}
	state.StatusFreshness = deriveStatusFreshness(state.Generation, state.ObservedGeneration)
	state.SyncTokenState = deriveSyncTokenState(
		isvc.Annotations[constants.RuntimeSyncAnnotationKey], isvc.Status.LastRuntimeSyncToken,
	)
	state.DriftState, state.DriftReason = deriveRuntimeDrift(isvc.Status.Conditions)
	if isvc.Spec.Runtime != nil && isvc.Spec.Runtime.Name != "" {
		state.RuntimeName = isvc.Spec.Runtime.Name
		state.SelectionSource = RuntimeExplicit
	}
	state.liveAvailability = classifyLiveAvailability(live, liveErr)
	if state.liveAvailability == liveUnreadable && liveErr == nil {
		liveErr = errors.New("live runtime snapshot is incomplete")
	}
	if liveErr != nil {
		code := RuntimeSourceIssueLiveUnavailable
		switch state.liveAvailability {
		case liveNotFound:
			code = RuntimeSourceIssueLiveNotFound
		case liveDisabled:
			code = RuntimeSourceIssueLiveDisabled
		}
		state.issues = append(state.issues, RuntimeSourceIssue{Code: code, cause: liveErr})
	}
	if live != nil {
		state.RuntimeName = live.Runtime.Name
		state.RuntimeKind = live.Runtime.Kind
		state.RuntimeNamespace = live.Runtime.Namespace
		state.SelectionSource = live.Runtime.SelectionSource
		if live.Runtime.spec != nil {
			_, state.LiveShortHash, _ = runtimerevision.Hash(live.Runtime.spec)
		}
	}
	if isvc.Spec.Runtime != nil && isvc.Spec.Runtime.Revision != nil {
		state.RequestedRevisionName = *isvc.Spec.Runtime.Revision
	}
	state.ReportedRevisionName = isvc.Status.PinnedRevisionName
	if isvc.Spec.Runtime != nil && isvc.Spec.Runtime.Name != "" {
		if err := r.collectExactRevision(ctx, state, state.RequestedRevisionName, RuntimeRevisionRoleRequested); err != nil {
			return nil, err
		}
		if err := r.collectExactRevision(ctx, state, state.ReportedRevisionName, RuntimeRevisionRoleReported); err != nil {
			return nil, err
		}
	}
	r.selectActive(isvc, state)
	if err := r.collectHistory(ctx, state, options); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return state, nil
}

func runtimePinIntent(isvc *v1beta1.InferenceService) (RuntimePinMode, string, string) {
	if isvc == nil || isvc.Spec.Runtime == nil || isvc.Spec.Runtime.Name == "" {
		return RuntimePinModeAutoSync, "", ""
	}
	ref := isvc.Spec.Runtime
	if !validDeclaredRuntimeKind(ref.Kind) {
		if runtimeAutoSyncEnabled(ref) {
			return RuntimePinModeAutoSync, "", ""
		}
		return RuntimePinModeInvalidPin, "", ""
	}
	kind := string(runtimerevision.KindClusterServingRuntime)
	namespace := ""
	if ref.Kind != nil && *ref.Kind == runtimeselector.KindServingRuntime {
		kind = string(runtimerevision.KindServingRuntime)
		namespace = isvc.Namespace
	}
	if runtimeAutoSyncEnabled(ref) {
		return RuntimePinModeAutoSync, kind, namespace
	}
	if ref.Revision != nil && *ref.Revision != "" {
		return RuntimePinModeExplicitPin, kind, namespace
	}
	return RuntimePinModeManagedPin, kind, namespace
}

func (r *RuntimePinResolver) selectActive(isvc *v1beta1.InferenceService, state *RuntimeState) {
	if state.liveAvailability == liveUnreadable || state.liveAvailability == liveDisabled {
		state.PinState = RuntimePinStateUnavailable
		return
	}
	switch state.PinMode {
	case RuntimePinModeAutoSync:
		if state.liveAvailability == liveAvailable {
			state.PinState = RuntimePinStateNotApplicable
			state.active = activeFromLive(state.live)
			state.LiveToActive = RuntimeHashRelationEqual
		} else {
			state.PinState = RuntimePinStateUnavailable
		}
	case RuntimePinModeInvalidPin:
		state.PinState = RuntimePinStateInvalidIntent
	case RuntimePinModeManagedPin:
		if state.ReportedRevisionName == "" {
			if state.liveAvailability == liveAvailable {
				state.PinState = RuntimePinStateAwaitingPin
				state.active = activeFromLive(state.live)
				state.LiveToActive = RuntimeHashRelationEqual
			} else {
				state.PinState = RuntimePinStateUnavailable
			}
			return
		}
		observation := findRevisionObservation(state.revisions, state.ReportedRevisionName)
		if observation == nil {
			state.PinState = RuntimePinStateRevisionInvalid
			return
		}
		if observation.notFound {
			state.PinState = RuntimePinStateRevisionMissing
			return
		}
		if observation.readError != nil {
			state.PinState = RuntimePinStateUnavailable
			return
		}
		if !observation.Available || observation.spec == nil {
			state.PinState = RuntimePinStateRevisionInvalid
			return
		}
		state.PinState = RuntimePinStateResolved
		state.activateRevision(isvc, observation)
	case RuntimePinModeExplicitPin:
		observation := findRevisionObservation(state.revisions, state.RequestedRevisionName)
		if observation == nil {
			state.PinState = RuntimePinStateRevisionInvalid
			return
		}
		if observation.notFound {
			state.PinState = RuntimePinStateRevisionMissing
			return
		}
		if observation.readError != nil {
			state.PinState = RuntimePinStateUnavailable
			return
		}
		if !observation.Available || observation.spec == nil || observation.SourceName != state.RuntimeName {
			state.PinState = RuntimePinStateRevisionInvalid
			return
		}
		state.PinState = RuntimePinStateResolved
		if state.ReportedRevisionName != "" && state.ReportedRevisionName != state.RequestedRevisionName {
			state.PinState = RuntimePinStateDesiredReportedMismatch
		}
		state.activateRevision(isvc, observation)
	}
}

func findRevisionObservation(observations []RuntimeRevisionObservation, name string) *RuntimeRevisionObservation {
	for i := range observations {
		if observations[i].expectedName == name {
			return &observations[i]
		}
	}
	return nil
}

func (s *RuntimeState) activateRevision(isvc *v1beta1.InferenceService, observation *RuntimeRevisionObservation) {
	if observation.spec.IsDisabled() {
		s.PinState = RuntimePinStateRevisionDisabled
		return
	}
	components, err := MergeEffectiveComponents(isvc, observation.spec)
	if err != nil {
		s.PinState = RuntimePinStateRevisionInvalid
		return
	}
	revisionName := observation.Name
	if observation.expectedName != "" {
		revisionName = observation.expectedName
	}
	s.ActiveRevisionName = revisionName
	s.active = &ActiveConfiguration{
		Origin:      ConfigurationOriginControllerRevision,
		RuntimeName: s.RuntimeName, RuntimeKind: s.DeclaredSourceKind,
		RuntimeNamespace: s.DeclaredSourceNamespace, RevisionName: revisionName,
		RevisionShortHash: observation.ShortHash, components: components,
		Consistency: observation.Consistency, spec: observation.spec.DeepCopy(),
	}
	observation.roles = appendRole(observation.roles, RuntimeRevisionRoleActive)
	if s.live != nil && s.live.Runtime.spec != nil {
		liveFull, liveShort, _ := runtimerevision.Hash(s.live.Runtime.spec)
		activeFull, activeShort, _ := runtimerevision.Hash(observation.spec)
		s.LiveToActive = compareRuntimeHashes(liveFull, liveShort, activeFull, activeShort)
	}
}

func compareRuntimeHashes(leftFull, leftShort, rightFull, rightShort string) RuntimeHashRelation {
	if leftFull == "" || rightFull == "" {
		return RuntimeHashRelationUnknown
	}
	if leftFull == rightFull {
		return RuntimeHashRelationEqual
	}
	if leftShort == rightShort {
		return RuntimeHashRelationAmbiguous
	}
	return RuntimeHashRelationDifferent
}

func appendRole(roles []RuntimeRevisionRole, role RuntimeRevisionRole) []RuntimeRevisionRole {
	for _, existing := range roles {
		if existing == role {
			return roles
		}
	}
	return append(roles, role)
}

func (r *RuntimePinResolver) collectExactRevision(ctx context.Context, state *RuntimeState, name string, role RuntimeRevisionRole) error {
	if name == "" {
		return nil
	}
	for i := range state.revisions {
		if state.revisions[i].expectedName == name {
			state.revisions[i].roles = appendRole(state.revisions[i].roles, role)
			return nil
		}
	}
	requestCtx, cancel := context.WithTimeout(ctx, r.limits.RequestTimeout)
	revision, err := r.revisions(r.omeNamespace).Get(requestCtx, name, metav1.GetOptions{})
	cancel()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if err == nil && revision == nil {
		err = errors.New("runtime revision read returned an empty response")
	}
	if err != nil {
		observation := RuntimeRevisionObservation{
			roles: []RuntimeRevisionRole{role}, Available: false,
			Consistency: RevisionConsistencyUnknown, expectedName: name,
			expectedNamespace: r.omeNamespace, readError: err,
		}
		code := RuntimeSourceIssueRevisionGetFailed
		if apierrors.IsNotFound(err) {
			observation.notFound = true
			code = RuntimeSourceIssueRevisionNotFound
		}
		state.revisions = append(state.revisions, observation)
		state.issues = append(state.issues, RuntimeSourceIssue{Code: code, RevisionName: name, cause: err})
		return nil
	}
	observation := inspectRuntimeRevision(
		revision, r.omeNamespace, name, state.RuntimeName,
		runtimerevision.SourceKind(state.DeclaredSourceKind), state.DeclaredSourceNamespace,
	)
	observation.roles = []RuntimeRevisionRole{role}
	state.revisions = append(state.revisions, observation)
	return nil
}

func activeFromLive(live *LiveConfiguration) *ActiveConfiguration {
	if live == nil || live.Runtime.spec == nil {
		return nil
	}
	return &ActiveConfiguration{
		Origin:           ConfigurationOriginLiveRuntime,
		RuntimeName:      live.Runtime.Name,
		RuntimeKind:      live.Runtime.Kind,
		RuntimeNamespace: live.Runtime.Namespace,
		components:       cloneEffectiveComponents(live.Components),
		Consistency:      RevisionConsistencyUnknown,
		spec:             live.Runtime.spec.DeepCopy(),
	}
}
