// Package runtimeprojection converts internal runtime evidence into the
// versioned, allowlisted kubectl-ome report contract.
package runtimeprojection

import (
	"errors"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/effective"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/runtimeselector"
)

// ErrInvalidEvidence is returned when collector evidence is nil,
// contradictory, or contains a value outside a closed enum.
var ErrInvalidEvidence = errors.New("runtime report projection input is invalid")

// ProjectEffective projects runtime collector evidence into the safe,
// versioned effective-runtime report contract.
func ProjectEffective(
	isvc *v1beta1.InferenceService,
	state *effective.RuntimeState,
	clock reportv1alpha1.Clock,
) (reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeEffectiveContent], error) {
	if !validInputs(isvc, state) {
		return reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeEffectiveContent]{}, ErrInvalidEvidence
	}
	if !validHistoryCollectionEvidence(state) {
		return reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeEffectiveContent]{}, ErrInvalidEvidence
	}

	selectionSource, ok := mapSelectionSource(state.SelectionSource)
	if !ok {
		return reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeEffectiveContent]{}, ErrInvalidEvidence
	}
	pinMode, ok := mapPinMode(state.PinMode)
	if !ok {
		return reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeEffectiveContent]{}, ErrInvalidEvidence
	}
	pinState, ok := mapPinState(state.PinState)
	if !ok {
		return reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeEffectiveContent]{}, ErrInvalidEvidence
	}
	freshness, ok := mapFreshness(state.StatusFreshness)
	if !ok {
		return reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeEffectiveContent]{}, ErrInvalidEvidence
	}
	syncState, ok := mapSyncState(state.SyncTokenState)
	if !ok {
		return reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeEffectiveContent]{}, ErrInvalidEvidence
	}
	driftState, ok := mapDriftState(state.DriftState)
	if !ok {
		return reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeEffectiveContent]{}, ErrInvalidEvidence
	}
	driftCause, ok := mapDriftCause(state.DriftReason)
	if !ok {
		return reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeEffectiveContent]{}, ErrInvalidEvidence
	}
	relation, ok := mapHashRelation(state.LiveToActive)
	if !ok {
		return reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeEffectiveContent]{}, ErrInvalidEvidence
	}

	live := state.LiveConfiguration()
	if !validStateEvidence(state, live) {
		return reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeEffectiveContent]{}, ErrInvalidEvidence
	}
	notConfigured := noRuntimeConfigurationDeclared(isvc)
	inheritance, runtimeIdentity, ok := projectInheritance(state, live, notConfigured)
	if !ok {
		return reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeEffectiveContent]{}, ErrInvalidEvidence
	}
	selectionRuntime := runtimeIdentity
	if state.RuntimeName == "" {
		selectionRuntime = nil
	}

	liveConfiguration, ok := projectLiveConfiguration(state, live, runtimeIdentity, notConfigured)
	if !ok {
		return reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeEffectiveContent]{}, ErrInvalidEvidence
	}
	activeConfiguration, ok := projectActiveConfiguration(state, runtimeIdentity, notConfigured)
	if !ok {
		return reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeEffectiveContent]{}, ErrInvalidEvidence
	}
	if !validPinConfiguration(state, activeConfiguration) {
		return reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeEffectiveContent]{}, ErrInvalidEvidence
	}

	issues, sources, warnings, ok := projectEffectiveDiagnostics(
		isvc, state, live, inheritance, activeConfiguration, notConfigured,
	)
	if !ok {
		return reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeEffectiveContent]{}, ErrInvalidEvidence
	}
	content := reportv1alpha1.RuntimeEffectiveContent{
		Selection:   reportv1alpha1.RuntimeSelection{Source: selectionSource, Runtime: selectionRuntime},
		Inheritance: inheritance,
		Pin: reportv1alpha1.RuntimePin{
			Mode:              pinMode,
			State:             pinState,
			RequestedRevision: state.RequestedRevisionName,
			ReportedRevision:  state.ReportedRevisionName,
			Status: reportv1alpha1.RuntimeStatusObservation{
				Generation:         state.Generation,
				ObservedGeneration: state.ObservedGeneration,
				Freshness:          freshness,
			},
			ReportedDrift: reportv1alpha1.RuntimeDriftObservation{State: driftState, Cause: driftCause},
			SyncState:     syncState,
		},
		Live:         liveConfiguration,
		Active:       activeConfiguration,
		LiveToActive: relation,
		Issues:       issues,
	}
	reportValue := reportv1alpha1.NewRuntimeEffectiveReport(
		reportv1alpha1.Metadata{Namespace: isvc.Namespace, Name: isvc.Name}, content, clock,
	)
	reportValue.Sources = sources
	reportValue.Warnings = warnings
	return reportValue.Canonical(), nil
}

func noRuntimeConfigurationDeclared(isvc *v1beta1.InferenceService) bool {
	if isvc == nil {
		return false
	}
	runtimeAbsent := isvc.Spec.Runtime == nil || isvc.Spec.Runtime.Name == ""
	modelAbsent := isvc.Spec.Model == nil || isvc.Spec.Model.Name == ""
	return runtimeAbsent && modelAbsent
}

func invalidDeclaredRuntimeKind(isvc *v1beta1.InferenceService) bool {
	if isvc == nil || isvc.Spec.Runtime == nil || isvc.Spec.Runtime.Name == "" ||
		isvc.Spec.Runtime.Kind == nil {
		return false
	}
	kind := *isvc.Spec.Runtime.Kind
	return kind != runtimeselector.KindServingRuntime && kind != runtimeselector.KindClusterServingRuntime
}

func validPinConfiguration(
	state *effective.RuntimeState,
	active reportv1alpha1.RuntimeConfiguration,
) bool {
	available := active.State == reportv1alpha1.ConfigurationStateAvailable
	if !validConfigurationRelation(state, active, available) {
		return false
	}
	if state.LiveAvailability() == effective.LiveRuntimeDisabled ||
		state.LiveAvailability() == effective.LiveRuntimeUnavailable {
		return state.PinState == effective.RuntimePinStateUnavailable && !available
	}
	switch state.PinMode {
	case effective.RuntimePinModeAutoSync:
		if state.LiveAvailability() == effective.LiveRuntimeAvailable {
			return state.PinState == effective.RuntimePinStateNotApplicable && available &&
				active.Origin == reportv1alpha1.ConfigurationOriginLiveRuntime
		}
		return state.PinState == effective.RuntimePinStateUnavailable && !available
	case effective.RuntimePinModeInvalidPin:
		return state.PinState == effective.RuntimePinStateInvalidIntent && !available
	case effective.RuntimePinModeManagedPin:
		if state.ReportedRevisionName == "" {
			if state.LiveAvailability() == effective.LiveRuntimeAvailable {
				return state.PinState == effective.RuntimePinStateAwaitingPin && available &&
					active.Origin == reportv1alpha1.ConfigurationOriginLiveRuntime
			}
			return state.PinState == effective.RuntimePinStateUnavailable && !available
		}
		if state.PinState == effective.RuntimePinStateResolved {
			return available && active.Origin == reportv1alpha1.ConfigurationOriginControllerRevision
		}
		return !available && failedRevisionPinState(state.PinState)
	case effective.RuntimePinModeExplicitPin:
		if state.RequestedRevisionName == "" {
			return false
		}
		if state.PinState == effective.RuntimePinStateResolved ||
			state.PinState == effective.RuntimePinStateDesiredReportedMismatch {
			if !available || active.Origin != reportv1alpha1.ConfigurationOriginControllerRevision {
				return false
			}
			mismatch := state.ReportedRevisionName != "" &&
				state.ReportedRevisionName != state.RequestedRevisionName
			return mismatch == (state.PinState == effective.RuntimePinStateDesiredReportedMismatch)
		}
		return !available && failedRevisionPinState(state.PinState)
	default:
		return false
	}
}

func validConfigurationRelation(
	state *effective.RuntimeState,
	active reportv1alpha1.RuntimeConfiguration,
	available bool,
) bool {
	if !available {
		return state.LiveToActive == effective.RuntimeHashRelationUnknown
	}
	switch active.Origin {
	case reportv1alpha1.ConfigurationOriginLiveRuntime:
		return state.LiveAvailability() == effective.LiveRuntimeAvailable &&
			state.LiveToActive == effective.RuntimeHashRelationEqual
	case reportv1alpha1.ConfigurationOriginControllerRevision:
		if state.LiveAvailability() != effective.LiveRuntimeAvailable {
			return state.LiveToActive == effective.RuntimeHashRelationUnknown
		}
		if state.LiveShortHash == "" || active.Hash == "" {
			return true
		}
		if state.LiveShortHash != active.Hash {
			return state.LiveToActive == effective.RuntimeHashRelationDifferent
		}
		return state.LiveToActive == effective.RuntimeHashRelationEqual ||
			state.LiveToActive == effective.RuntimeHashRelationAmbiguous
	default:
		return false
	}
}

func failedRevisionPinState(state effective.RuntimePinState) bool {
	switch state {
	case effective.RuntimePinStateRevisionMissing,
		effective.RuntimePinStateRevisionInvalid,
		effective.RuntimePinStateRevisionDisabled,
		effective.RuntimePinStateUnavailable:
		return true
	default:
		return false
	}
}

func validInputs(isvc *v1beta1.InferenceService, state *effective.RuntimeState) bool {
	if isvc == nil || state == nil || isvc.Name == "" || isvc.Namespace == "" {
		return false
	}
	if !state.MatchesInferenceService(isvc) {
		return false
	}
	if isvc.Generation != state.Generation || isvc.Status.ObservedGeneration != state.ObservedGeneration {
		return false
	}
	if state.StatusFreshness != expectedStatusFreshness(state.Generation, state.ObservedGeneration) ||
		state.SyncTokenState != expectedSyncTokenState(isvc) {
		return false
	}
	expectedDriftState, expectedDriftReason := expectedDriftObservation(isvc)
	if state.DriftState != expectedDriftState || state.DriftReason != expectedDriftReason {
		return false
	}
	requestedRevision := ""
	if isvc.Spec.Runtime != nil && isvc.Spec.Runtime.Revision != nil {
		requestedRevision = *isvc.Spec.Runtime.Revision
	}
	if requestedRevision != state.RequestedRevisionName || isvc.Status.PinnedRevisionName != state.ReportedRevisionName {
		return false
	}
	expectedMode, expectedDeclaredKind, expectedDeclaredNamespace := expectedPinIntent(isvc)
	if state.PinMode != expectedMode || state.DeclaredSourceKind != expectedDeclaredKind ||
		state.DeclaredSourceNamespace != expectedDeclaredNamespace {
		return false
	}
	explicitRuntimeName := ""
	if isvc.Spec.Runtime != nil {
		explicitRuntimeName = isvc.Spec.Runtime.Name
	}
	if state.SelectionSource == effective.RuntimeExplicit {
		return explicitRuntimeName != "" && explicitRuntimeName == state.RuntimeName
	}
	if state.SelectionSource == effective.RuntimeSelected {
		return explicitRuntimeName == ""
	}
	return true
}

func expectedStatusFreshness(generation, observedGeneration int64) effective.StatusFreshness {
	switch {
	case observedGeneration == 0:
		return effective.StatusFreshnessUnknown
	case observedGeneration == generation:
		return effective.StatusFreshnessCurrent
	case observedGeneration < generation:
		return effective.StatusFreshnessStale
	default:
		return effective.StatusFreshnessInconsistent
	}
}

func expectedSyncTokenState(isvc *v1beta1.InferenceService) effective.SyncTokenState {
	annotation := isvc.Annotations[constants.RuntimeSyncAnnotationKey]
	status := isvc.Status.LastRuntimeSyncToken
	switch {
	case annotation == "" && status == "":
		return effective.SyncTokenStateAbsent
	case annotation == "":
		return effective.SyncTokenStateStatusOnly
	case annotation == status:
		return effective.SyncTokenStateAcknowledged
	default:
		return effective.SyncTokenStatePending
	}
}

func expectedDriftObservation(
	isvc *v1beta1.InferenceService,
) (effective.RuntimeDriftState, effective.RuntimeDriftReason) {
	found := false
	status := ""
	reason := ""
	for _, condition := range isvc.Status.Conditions {
		if string(condition.Type) != constants.RuntimeDriftedConditionType {
			continue
		}
		if found {
			return effective.RuntimeDriftStateMalformed, ""
		}
		found = true
		status = string(condition.Status)
		reason = condition.Reason
	}
	if !found {
		return effective.RuntimeDriftStateNotReported, ""
	}
	classifiedReason := classifyExpectedDriftReason(reason)
	switch status {
	case "True":
		return effective.RuntimeDriftStateReportedTrue, classifiedReason
	case "False":
		return effective.RuntimeDriftStateReportedFalse, classifiedReason
	case "Unknown":
		return effective.RuntimeDriftStateReportedUnknown, classifiedReason
	default:
		return effective.RuntimeDriftStateMalformed, classifiedReason
	}
}

func classifyExpectedDriftReason(reason string) effective.RuntimeDriftReason {
	switch effective.RuntimeDriftReason(reason) {
	case effective.RuntimeDriftReasonRevisionMismatch,
		effective.RuntimeDriftReasonRevisionMissing,
		effective.RuntimeDriftReasonSourceRuntimeMissing,
		effective.RuntimeDriftReasonRuntimeMismatch,
		effective.RuntimeDriftReasonPinAdvanced:
		return effective.RuntimeDriftReason(reason)
	default:
		return effective.RuntimeDriftReasonOther
	}
}

func expectedPinIntent(isvc *v1beta1.InferenceService) (effective.RuntimePinMode, string, string) {
	if isvc.Spec.Runtime == nil || isvc.Spec.Runtime.Name == "" {
		return effective.RuntimePinModeAutoSync, "", ""
	}
	reference := isvc.Spec.Runtime
	kindValid := reference.Kind == nil || *reference.Kind == runtimeselector.KindClusterServingRuntime ||
		*reference.Kind == runtimeselector.KindServingRuntime
	autoSync := reference.AutoSync == nil || *reference.AutoSync
	if !kindValid {
		if autoSync {
			return effective.RuntimePinModeAutoSync, "", ""
		}
		return effective.RuntimePinModeInvalidPin, "", ""
	}
	kind := runtimeselector.KindClusterServingRuntime
	namespace := ""
	if reference.Kind != nil && *reference.Kind == runtimeselector.KindServingRuntime {
		kind = runtimeselector.KindServingRuntime
		namespace = isvc.Namespace
	}
	if autoSync {
		return effective.RuntimePinModeAutoSync, kind, namespace
	}
	if reference.Revision != nil && *reference.Revision != "" {
		return effective.RuntimePinModeExplicitPin, kind, namespace
	}
	return effective.RuntimePinModeManagedPin, kind, namespace
}

func validStateEvidence(state *effective.RuntimeState, live *effective.LiveConfiguration) bool {
	if !validClosedStateEnums(state) || !validActiveRevisionNameEvidence(state) {
		return false
	}
	if !validLiveEvidenceShape(state.LiveAvailability(), live != nil) {
		return false
	}
	if state.RuntimeKind != "" {
		if _, ok := mapRuntimeKind(state.RuntimeKind); !ok {
			return false
		}
	}
	if !validVerifiedShortHash(state.LiveShortHash) {
		return false
	}
	if state.DriftState == effective.RuntimeDriftStateNotReported && state.DriftReason != "" {
		return false
	}
	if live == nil {
		return state.RuntimeKind == "" && state.RuntimeNamespace == "" && state.LiveShortHash == ""
	}
	if live.Runtime.Name != state.RuntimeName || live.Runtime.Kind != state.RuntimeKind ||
		live.Runtime.Namespace != state.RuntimeNamespace || live.Runtime.SelectionSource != state.SelectionSource {
		return false
	}
	return true
}

func validActiveRevisionNameEvidence(state *effective.RuntimeState) bool {
	active, err := state.RequireActive()
	if err != nil {
		return state.ActiveRevisionName == ""
	}
	switch active.Origin {
	case effective.ConfigurationOriginLiveRuntime:
		return state.ActiveRevisionName == "" && active.RevisionName == ""
	case effective.ConfigurationOriginControllerRevision:
		return active.RevisionName != "" && state.ActiveRevisionName == active.RevisionName
	default:
		return false
	}
}

func validLiveEvidenceShape(availability effective.LiveRuntimeAvailability, livePresent bool) bool {
	switch availability {
	case effective.LiveRuntimeAvailable:
		return livePresent
	case effective.LiveRuntimeNotFound, effective.LiveRuntimeDisabled, effective.LiveRuntimeUnavailable:
		return !livePresent
	default:
		return false
	}
}

func validClosedStateEnums(state *effective.RuntimeState) bool {
	if _, ok := mapSelectionSource(state.SelectionSource); !ok {
		return false
	}
	if _, ok := mapPinMode(state.PinMode); !ok {
		return false
	}
	if _, ok := mapPinState(state.PinState); !ok {
		return false
	}
	if _, ok := mapFreshness(state.StatusFreshness); !ok {
		return false
	}
	if _, ok := mapSyncState(state.SyncTokenState); !ok {
		return false
	}
	if _, ok := mapDriftState(state.DriftState); !ok {
		return false
	}
	if _, ok := mapDriftCause(state.DriftReason); !ok {
		return false
	}
	if _, ok := mapHashRelation(state.LiveToActive); !ok {
		return false
	}
	switch state.DriftState {
	case effective.RuntimeDriftStateNotReported:
		return state.DriftReason == ""
	case effective.RuntimeDriftStateReportedTrue,
		effective.RuntimeDriftStateReportedFalse,
		effective.RuntimeDriftStateReportedUnknown:
		return state.DriftReason != ""
	case effective.RuntimeDriftStateMalformed:
		return true
	default:
		return false
	}
}

func validVerifiedShortHash(value string) bool {
	if value == "" {
		return true
	}
	if len(value) != 8 {
		return false
	}
	for _, character := range value {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func mapSelectionSource(value effective.RuntimeSelectionSource) (reportv1alpha1.RuntimeSelectionSource, bool) {
	switch value {
	case effective.RuntimeExplicit:
		return reportv1alpha1.RuntimeSelectionSourceExplicit, true
	case effective.RuntimeSelected:
		return reportv1alpha1.RuntimeSelectionSourceSelected, true
	default:
		return "", false
	}
}

func mapRuntimeKind(value string) (reportv1alpha1.RuntimeKind, bool) {
	switch value {
	case runtimeselector.KindServingRuntime:
		return reportv1alpha1.RuntimeKindServingRuntime, true
	case runtimeselector.KindClusterServingRuntime:
		return reportv1alpha1.RuntimeKindClusterServingRuntime, true
	case "":
		return reportv1alpha1.RuntimeKindUnknown, true
	default:
		return "", false
	}
}

func mapPinMode(value effective.RuntimePinMode) (reportv1alpha1.RuntimePinMode, bool) {
	switch value {
	case effective.RuntimePinModeAutoSync:
		return reportv1alpha1.RuntimePinModeAutoSync, true
	case effective.RuntimePinModeManagedPin:
		return reportv1alpha1.RuntimePinModeManagedPin, true
	case effective.RuntimePinModeExplicitPin:
		return reportv1alpha1.RuntimePinModeExplicitPin, true
	case effective.RuntimePinModeInvalidPin:
		return reportv1alpha1.RuntimePinModeInvalidPin, true
	default:
		return "", false
	}
}

func mapPinState(value effective.RuntimePinState) (reportv1alpha1.RuntimePinState, bool) {
	switch value {
	case effective.RuntimePinStateNotApplicable:
		return reportv1alpha1.RuntimePinStateNotApplicable, true
	case effective.RuntimePinStateAwaitingPin:
		return reportv1alpha1.RuntimePinStateAwaitingPin, true
	case effective.RuntimePinStateResolved:
		return reportv1alpha1.RuntimePinStateResolved, true
	case effective.RuntimePinStateDesiredReportedMismatch:
		return reportv1alpha1.RuntimePinStateDesiredReportedMismatch, true
	case effective.RuntimePinStateRevisionMissing:
		return reportv1alpha1.RuntimePinStateRevisionMissing, true
	case effective.RuntimePinStateRevisionInvalid:
		return reportv1alpha1.RuntimePinStateRevisionInvalid, true
	case effective.RuntimePinStateRevisionDisabled:
		return reportv1alpha1.RuntimePinStateRevisionDisabled, true
	case effective.RuntimePinStateUnavailable:
		return reportv1alpha1.RuntimePinStateUnavailable, true
	case effective.RuntimePinStateInvalidIntent:
		return reportv1alpha1.RuntimePinStateInvalidIntent, true
	default:
		return "", false
	}
}

func mapFreshness(value effective.StatusFreshness) (reportv1alpha1.StatusFreshness, bool) {
	switch value {
	case effective.StatusFreshnessCurrent:
		return reportv1alpha1.StatusFreshnessCurrent, true
	case effective.StatusFreshnessStale:
		return reportv1alpha1.StatusFreshnessStale, true
	case effective.StatusFreshnessUnknown:
		return reportv1alpha1.StatusFreshnessUnobserved, true
	case effective.StatusFreshnessInconsistent:
		return reportv1alpha1.StatusFreshnessInvalid, true
	default:
		return "", false
	}
}

func mapSyncState(value effective.SyncTokenState) (reportv1alpha1.RuntimeSyncState, bool) {
	switch value {
	case effective.SyncTokenStateAbsent:
		return reportv1alpha1.RuntimeSyncStateAbsent, true
	case effective.SyncTokenStateAcknowledged:
		return reportv1alpha1.RuntimeSyncStateAcknowledged, true
	case effective.SyncTokenStatePending:
		return reportv1alpha1.RuntimeSyncStatePending, true
	case effective.SyncTokenStateStatusOnly:
		return reportv1alpha1.RuntimeSyncStateStatusOnly, true
	default:
		return "", false
	}
}

func mapDriftState(value effective.RuntimeDriftState) (reportv1alpha1.DriftConditionState, bool) {
	switch value {
	case effective.RuntimeDriftStateNotReported:
		return reportv1alpha1.DriftConditionStateNotReported, true
	case effective.RuntimeDriftStateReportedTrue:
		return reportv1alpha1.DriftConditionStateReportedTrue, true
	case effective.RuntimeDriftStateReportedFalse:
		return reportv1alpha1.DriftConditionStateReportedFalse, true
	case effective.RuntimeDriftStateReportedUnknown:
		return reportv1alpha1.DriftConditionStateReportedUnknown, true
	case effective.RuntimeDriftStateMalformed:
		return reportv1alpha1.DriftConditionStateMalformed, true
	default:
		return "", false
	}
}

func mapDriftCause(value effective.RuntimeDriftReason) (reportv1alpha1.RuntimeDriftCause, bool) {
	switch value {
	case "":
		return "", true
	case effective.RuntimeDriftReasonRevisionMismatch:
		return reportv1alpha1.RuntimeDriftCauseRevisionMismatch, true
	case effective.RuntimeDriftReasonRevisionMissing:
		return reportv1alpha1.RuntimeDriftCauseRevisionMissing, true
	case effective.RuntimeDriftReasonSourceRuntimeMissing:
		return reportv1alpha1.RuntimeDriftCauseSourceRuntimeMissing, true
	case effective.RuntimeDriftReasonRuntimeMismatch:
		return reportv1alpha1.RuntimeDriftCauseRuntimeMismatch, true
	case effective.RuntimeDriftReasonPinAdvanced:
		return reportv1alpha1.RuntimeDriftCausePinAdvanced, true
	case effective.RuntimeDriftReasonOther:
		return reportv1alpha1.RuntimeDriftCauseOther, true
	default:
		return "", false
	}
}

func mapHashRelation(value effective.RuntimeHashRelation) (reportv1alpha1.RuntimeHashRelation, bool) {
	switch value {
	case effective.RuntimeHashRelationUnknown:
		return reportv1alpha1.RuntimeHashRelationUnknown, true
	case effective.RuntimeHashRelationEqual:
		return reportv1alpha1.RuntimeHashRelationEqual, true
	case effective.RuntimeHashRelationDifferent:
		return reportv1alpha1.RuntimeHashRelationDifferent, true
	case effective.RuntimeHashRelationAmbiguous:
		return reportv1alpha1.RuntimeHashRelationAmbiguous, true
	default:
		return "", false
	}
}

func projectInheritance(
	state *effective.RuntimeState,
	live *effective.LiveConfiguration,
	notConfigured bool,
) (reportv1alpha1.RuntimeInheritance, *reportv1alpha1.RuntimeObjectReference, bool) {
	runtimeKind := state.RuntimeKind
	runtimeNamespace := state.RuntimeNamespace
	if runtimeKind == "" && state.RuntimeName != "" {
		runtimeKind = state.DeclaredSourceKind
		runtimeNamespace = state.DeclaredSourceNamespace
	}
	runtimeIdentity, validIdentity := runtimeReference(state.RuntimeName, runtimeKind, runtimeNamespace)
	if !validIdentity {
		return reportv1alpha1.RuntimeInheritance{}, nil, false
	}
	if live == nil {
		reason := liveUnavailableReason(state)
		if notConfigured {
			reason = reportv1alpha1.UnavailableNotConfigured
		}
		return reportv1alpha1.RuntimeInheritance{
			State:             reportv1alpha1.InheritanceStateUnavailable,
			Sources:           []reportv1alpha1.RuntimeObjectReference{},
			UnavailableReason: reason,
		}, runtimeIdentity, true
	}
	observation := live.Runtime.DeclaredInheritance
	if observation.Validate() != nil {
		return reportv1alpha1.RuntimeInheritance{}, nil, false
	}
	switch observation.State() {
	case effective.InheritanceObserved:
		chain := observation.Chain()
		sources := make([]reportv1alpha1.RuntimeObjectReference, len(chain))
		for i := range chain {
			kind, ok := mapRuntimeKind(chain[i].Kind)
			if !ok || kind == reportv1alpha1.RuntimeKindUnknown || chain[i].Name == "" {
				return reportv1alpha1.RuntimeInheritance{}, nil, false
			}
			sources[i] = reportv1alpha1.RuntimeObjectReference{
				APIVersion: chain[i].APIVersion,
				Kind:       kind,
				Namespace:  chain[i].Namespace,
				Name:       chain[i].Name,
				UID:        string(chain[i].UID),
				Generation: chain[i].Generation,
			}
			if chain[i].Name == state.RuntimeName && chain[i].Kind == state.RuntimeKind && chain[i].Namespace == state.RuntimeNamespace {
				copy := sources[i]
				runtimeIdentity = &copy
			}
		}
		return reportv1alpha1.RuntimeInheritance{
			State:   reportv1alpha1.InheritanceStateObserved,
			Sources: sources,
		}, runtimeIdentity, true
	case effective.InheritanceUnavailable:
		reason, ok := mapInheritanceUnavailableReason(observation.UnavailableReason())
		return reportv1alpha1.RuntimeInheritance{
			State:             reportv1alpha1.InheritanceStateUnavailable,
			Sources:           []reportv1alpha1.RuntimeObjectReference{},
			UnavailableReason: reason,
		}, runtimeIdentity, ok
	default:
		return reportv1alpha1.RuntimeInheritance{}, nil, false
	}
}

func mapInheritanceUnavailableReason(value effective.InheritanceUnavailableReason) (reportv1alpha1.UnavailableReason, bool) {
	switch value {
	case effective.InheritanceNotFound:
		return reportv1alpha1.UnavailableNotFound, true
	case effective.InheritanceForbidden:
		return reportv1alpha1.UnavailableForbidden, true
	case effective.InheritanceCycle:
		return reportv1alpha1.UnavailableCycle, true
	case effective.InheritanceMaxDepthExceeded:
		return reportv1alpha1.UnavailableMaxDepthExceeded, true
	case effective.InheritanceMalformed:
		return reportv1alpha1.UnavailableMalformedPayload, true
	case effective.InheritanceUnreadable:
		return reportv1alpha1.UnavailableUnreadable, true
	default:
		return "", false
	}
}

func runtimeReference(name, kind, namespace string) (*reportv1alpha1.RuntimeObjectReference, bool) {
	if name == "" {
		return nil, true
	}
	reportKind, ok := mapRuntimeKind(kind)
	if !ok {
		return nil, false
	}
	return &reportv1alpha1.RuntimeObjectReference{
		APIVersion: v1beta1.SchemeGroupVersion.String(),
		Kind:       reportKind,
		Namespace:  namespace,
		Name:       name,
	}, true
}

func projectLiveConfiguration(
	state *effective.RuntimeState,
	live *effective.LiveConfiguration,
	runtimeIdentity *reportv1alpha1.RuntimeObjectReference,
	notConfigured bool,
) (reportv1alpha1.RuntimeConfiguration, bool) {
	if live == nil || state.LiveAvailability() != effective.LiveRuntimeAvailable {
		reason := liveUnavailableReason(state)
		if notConfigured {
			reason = reportv1alpha1.UnavailableNotConfigured
		}
		return reportv1alpha1.RuntimeConfiguration{
			State:             reportv1alpha1.ConfigurationStateUnavailable,
			Source:            configurationRuntimeSource(runtimeIdentity),
			Components:        []reportv1alpha1.RuntimeComponent{},
			UnavailableReason: reason,
		}, true
	}
	components, ok := projectComponents(live.Components)
	if !ok {
		return reportv1alpha1.RuntimeConfiguration{}, false
	}
	return reportv1alpha1.RuntimeConfiguration{
		State:      reportv1alpha1.ConfigurationStateAvailable,
		Origin:     reportv1alpha1.ConfigurationOriginLiveRuntime,
		Source:     copyRuntimeReference(runtimeIdentity),
		Hash:       state.LiveShortHash,
		Components: components,
	}, true
}

func projectActiveConfiguration(
	state *effective.RuntimeState,
	runtimeIdentity *reportv1alpha1.RuntimeObjectReference,
	notConfigured bool,
) (reportv1alpha1.RuntimeConfiguration, bool) {
	active, err := state.RequireActive()
	if err != nil {
		reason := activeUnavailableReason(state)
		revision, revisionReason, valid := projectUnavailableActiveRevision(state)
		if !valid {
			return reportv1alpha1.RuntimeConfiguration{}, false
		}
		if revisionReason != "" {
			reason = revisionReason
		}
		if notConfigured {
			reason = reportv1alpha1.UnavailableNotConfigured
		}
		return reportv1alpha1.RuntimeConfiguration{
			State:             reportv1alpha1.ConfigurationStateUnavailable,
			Source:            configurationRuntimeSource(runtimeIdentity),
			Revision:          revision,
			Components:        []reportv1alpha1.RuntimeComponent{},
			UnavailableReason: reason,
		}, true
	}
	origin, ok := mapConfigurationOrigin(active.Origin)
	if !ok {
		return reportv1alpha1.RuntimeConfiguration{}, false
	}
	components, ok := projectComponents(active.Components())
	if !ok {
		return reportv1alpha1.RuntimeConfiguration{}, false
	}
	source, ok := runtimeReference(active.RuntimeName, active.RuntimeKind, active.RuntimeNamespace)
	if !ok {
		return reportv1alpha1.RuntimeConfiguration{}, false
	}
	if sameRuntimeReference(source, runtimeIdentity) {
		source = copyRuntimeReference(runtimeIdentity)
	}
	hash := active.RevisionShortHash
	if active.Origin == effective.ConfigurationOriginLiveRuntime {
		hash = state.LiveShortHash
	}
	configuration := reportv1alpha1.RuntimeConfiguration{
		State:      reportv1alpha1.ConfigurationStateAvailable,
		Origin:     origin,
		Source:     source,
		Hash:       hash,
		Components: components,
	}
	if active.Origin == effective.ConfigurationOriginControllerRevision {
		revision, valid := projectActiveRevision(state, active)
		if !valid {
			return reportv1alpha1.RuntimeConfiguration{}, false
		}
		configuration.Revision = revision
	}
	return configuration, true
}

func mapConfigurationOrigin(value effective.ConfigurationOrigin) (reportv1alpha1.ConfigurationOrigin, bool) {
	switch value {
	case effective.ConfigurationOriginLiveRuntime:
		return reportv1alpha1.ConfigurationOriginLiveRuntime, true
	case effective.ConfigurationOriginControllerRevision:
		return reportv1alpha1.ConfigurationOriginControllerRevision, true
	default:
		return "", false
	}
}

func projectComponents(values []effective.EffectiveComponent) ([]reportv1alpha1.RuntimeComponent, bool) {
	result := make([]reportv1alpha1.RuntimeComponent, len(values))
	for i := range values {
		componentType, ok := mapComponentType(values[i].Type)
		if !ok {
			return nil, false
		}
		mode, ok := mapDeploymentMode(values[i].DeploymentMode)
		if !ok {
			return nil, false
		}
		source, ok := mapDeploymentModeSource(values[i].DeploymentModeSource)
		if !ok {
			return nil, false
		}
		result[i] = reportv1alpha1.RuntimeComponent{
			Type: componentType, DeploymentMode: mode, DeploymentModeSource: source,
		}
	}
	return result, true
}

func mapComponentType(value v1beta1.ComponentType) (reportv1alpha1.RuntimeComponentType, bool) {
	switch value {
	case v1beta1.EngineComponent:
		return reportv1alpha1.RuntimeComponentEngine, true
	case v1beta1.DecoderComponent:
		return reportv1alpha1.RuntimeComponentDecoder, true
	case v1beta1.RouterComponent:
		return reportv1alpha1.RuntimeComponentRouter, true
	default:
		return "", false
	}
}

func mapDeploymentMode(value constants.DeploymentModeType) (reportv1alpha1.DeploymentMode, bool) {
	switch value {
	case constants.RawDeployment:
		return reportv1alpha1.DeploymentModeRawDeployment, true
	case constants.MultiNode:
		return reportv1alpha1.DeploymentModeMultiNode, true
	case constants.VirtualDeployment:
		return reportv1alpha1.DeploymentModeVirtualDeployment, true
	case constants.OMENative:
		return reportv1alpha1.DeploymentModeOMENative, true
	default:
		return "", false
	}
}

func mapDeploymentModeSource(value effective.ComponentDeploymentModeSource) (reportv1alpha1.DeploymentModeSource, bool) {
	switch value {
	case effective.DeploymentModeComponentAnnotation:
		return reportv1alpha1.DeploymentModeSourceComponentAnnotation, true
	case effective.DeploymentModeServiceAnnotation:
		return reportv1alpha1.DeploymentModeSourceServiceAnnotation, true
	case effective.DeploymentModeServiceSpec:
		return reportv1alpha1.DeploymentModeSourceServiceSpec, true
	case effective.DeploymentModeLeaderWorkerShape:
		return reportv1alpha1.DeploymentModeSourceLeaderWorkerShape, true
	case effective.DeploymentModeDefault:
		return reportv1alpha1.DeploymentModeSourceDefault, true
	default:
		return "", false
	}
}

func mapLiveUnavailableReason(value effective.LiveRuntimeAvailability) reportv1alpha1.UnavailableReason {
	switch value {
	case effective.LiveRuntimeNotFound:
		return reportv1alpha1.UnavailableNotFound
	case effective.LiveRuntimeDisabled:
		return reportv1alpha1.UnavailableDisabled
	case effective.LiveRuntimeUnavailable:
		return reportv1alpha1.UnavailableUnreadable
	default:
		return reportv1alpha1.UnavailableUnreadable
	}
}

func liveUnavailableReason(state *effective.RuntimeState) reportv1alpha1.UnavailableReason {
	for _, issue := range state.SourceIssues() {
		switch issue.Code {
		case effective.RuntimeSourceIssueLiveNotFound,
			effective.RuntimeSourceIssueLiveDisabled,
			effective.RuntimeSourceIssueLiveUnavailable:
			_, reason, ok := projectSourceIssue(issue)
			if ok {
				return reason
			}
		}
	}
	return mapLiveUnavailableReason(state.LiveAvailability())
}

func activeUnavailableReason(state *effective.RuntimeState) reportv1alpha1.UnavailableReason {
	switch state.PinState {
	case effective.RuntimePinStateRevisionMissing:
		return reportv1alpha1.UnavailableNotFound
	case effective.RuntimePinStateRevisionDisabled:
		return reportv1alpha1.UnavailableDisabled
	case effective.RuntimePinStateInvalidIntent, effective.RuntimePinStateRevisionInvalid:
		return reportv1alpha1.UnavailableMalformedPayload
	case effective.RuntimePinStateUnavailable:
		if state.PinMode == effective.RuntimePinModeAutoSync {
			return liveUnavailableReason(state)
		}
		if state.LiveAvailability() != effective.LiveRuntimeAvailable &&
			state.LiveAvailability() != effective.LiveRuntimeNotFound {
			return liveUnavailableReason(state)
		}
		return reportv1alpha1.UnavailableUnreadable
	default:
		if state.PinMode == effective.RuntimePinModeAutoSync {
			return liveUnavailableReason(state)
		}
		return reportv1alpha1.UnavailableUnreadable
	}
}

func projectBaseSources(isvc *v1beta1.InferenceService, live *effective.LiveConfiguration) []reportv1alpha1.RuntimeSourceReference {
	sources := []reportv1alpha1.RuntimeSourceReference{{
		Kind: "InferenceService", Namespace: isvc.Namespace, Name: isvc.Name,
		UID: string(isvc.UID), Generation: isvc.Generation, Evidence: reportv1alpha1.EvidenceObserved,
	}}
	if live == nil {
		return sources
	}
	if live.Model != nil {
		sources = append(sources, reportv1alpha1.RuntimeSourceReference{
			Kind: live.Model.Kind, Namespace: live.Model.Namespace, Name: live.Model.Name,
			UID: string(live.Model.UID), Generation: live.Model.Generation, Evidence: reportv1alpha1.EvidenceObserved,
		})
	}
	inheritance := live.Runtime.DeclaredInheritance.Chain()
	runtimeObserved := false
	for _, source := range inheritance {
		if source.Kind == live.Runtime.Kind && source.Namespace == live.Runtime.Namespace &&
			source.Name == live.Runtime.Name {
			runtimeObserved = true
		}
		sources = append(sources, reportv1alpha1.RuntimeSourceReference{
			Kind: source.Kind, Namespace: source.Namespace, Name: source.Name,
			UID: string(source.UID), Generation: source.Generation, Evidence: reportv1alpha1.EvidenceObserved,
		})
	}
	if !runtimeObserved && live.Runtime.Name != "" {
		sources = append(sources, reportv1alpha1.RuntimeSourceReference{
			Kind: live.Runtime.Kind, Namespace: live.Runtime.Namespace, Name: live.Runtime.Name,
			Evidence: reportv1alpha1.EvidenceObserved,
		})
	}
	return deduplicateSources(sources)
}

func projectActiveRevision(
	state *effective.RuntimeState,
	active *effective.ActiveConfiguration,
) (*reportv1alpha1.RuntimeRevisionReference, bool) {
	if active.RevisionName == "" || !validVerifiedShortHash(active.RevisionShortHash) {
		return nil, false
	}
	var matched *effective.RuntimeRevisionObservation
	for _, observation := range state.RevisionObservations() {
		if observation.ExpectedName() != active.RevisionName || !hasRevisionRole(observation.Roles(), effective.RuntimeRevisionRoleActive) {
			continue
		}
		if matched != nil {
			return nil, false
		}
		copy := observation
		matched = &copy
	}
	if matched == nil {
		return nil, false
	}
	revision := expectedRevisionReference(*matched, active.RevisionName)
	return revision, true
}

func projectUnavailableActiveRevision(
	state *effective.RuntimeState,
) (*reportv1alpha1.RuntimeRevisionReference, reportv1alpha1.UnavailableReason, bool) {
	expectedName := ""
	switch state.PinMode {
	case effective.RuntimePinModeManagedPin:
		expectedName = state.ReportedRevisionName
	case effective.RuntimePinModeExplicitPin:
		expectedName = state.RequestedRevisionName
	case effective.RuntimePinModeAutoSync, effective.RuntimePinModeInvalidPin:
		return nil, "", true
	default:
		return nil, "", false
	}
	if expectedName == "" {
		return nil, "", true
	}
	var matched *effective.RuntimeRevisionObservation
	for _, observation := range state.RevisionObservations() {
		if observation.ExpectedName() != expectedName {
			continue
		}
		if matched != nil {
			return nil, "", false
		}
		copy := observation
		matched = &copy
	}
	if matched == nil {
		return nil, "", false
	}
	reason, _ := failedRevisionReason(state.SourceIssues(), expectedName)
	return expectedRevisionReference(*matched, expectedName), reason, true
}

func expectedRevisionReference(
	observation effective.RuntimeRevisionObservation,
	expectedName string,
) *reportv1alpha1.RuntimeRevisionReference {
	revision := &reportv1alpha1.RuntimeRevisionReference{
		Namespace: observation.ExpectedNamespace(),
		Name:      expectedName,
	}
	if observation.ObjectReturned() && observation.ReturnedName() == observation.ExpectedName() &&
		observation.ReturnedNamespace() == observation.ExpectedNamespace() {
		revision.UID = observation.UID
		if !observation.CreationTimestamp.IsZero() {
			createdAt := observation.CreationTimestamp.Time.UTC()
			revision.CreatedAt = &createdAt
		}
	}
	return revision
}

func hasRevisionRole(values []effective.RuntimeRevisionRole, target effective.RuntimeRevisionRole) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func deduplicateSources(values []reportv1alpha1.RuntimeSourceReference) []reportv1alpha1.RuntimeSourceReference {
	result := make([]reportv1alpha1.RuntimeSourceReference, 0, len(values))
	seen := make(map[reportv1alpha1.RuntimeSourceReference]struct{}, len(values))
	for _, value := range values {
		if _, found := seen[value]; found {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func copyRuntimeReference(value *reportv1alpha1.RuntimeObjectReference) *reportv1alpha1.RuntimeObjectReference {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func configurationRuntimeSource(
	value *reportv1alpha1.RuntimeObjectReference,
) *reportv1alpha1.RuntimeObjectReference {
	if value == nil || value.Kind == reportv1alpha1.RuntimeKindUnknown {
		return nil
	}
	return copyRuntimeReference(value)
}

func sameRuntimeReference(left, right *reportv1alpha1.RuntimeObjectReference) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.APIVersion == right.APIVersion && left.Kind == right.Kind &&
		left.Namespace == right.Namespace && left.Name == right.Name
}
