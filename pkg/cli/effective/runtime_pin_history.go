package effective

import (
	"context"
	"errors"
	"sort"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/runtimerevision"
)

func (r *RuntimePinResolver) collectHistory(
	ctx context.Context,
	state *RuntimeState,
	options RuntimeResolveOptions,
) error {
	state.HistoryRequested = options.IncludeHistory
	state.HistoryPageLimit = r.limits.MaxPages
	state.HistoryRequestLimit = r.limits.MaxRequestAttempts()
	if !options.IncludeHistory {
		state.HistoryComplete = false
		finalizeRevisionEvidence(state)
		return nil
	}
	state.HistoryComplete = false
	if state.RuntimeName == "" {
		finalizeRevisionEvidence(state)
		return nil
	}
	state.historyNamespace = r.omeNamespace

	base := metav1.ListOptions{LabelSelector: labels.Set{
		constants.RuntimeRevisionOfLabelKey: state.RuntimeName,
	}.AsSelector().String()}
	result, err := paging.ListBounded(ctx, base, r.limits, func(
		requestCtx context.Context,
		listOptions metav1.ListOptions,
	) (paging.Page[appsv1.ControllerRevision], error) {
		list, err := r.revisions(r.omeNamespace).List(requestCtx, listOptions)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return paging.Page[appsv1.ControllerRevision]{}, ctxErr
			}
			return paging.Page[appsv1.ControllerRevision]{}, err
		}
		if list == nil {
			return paging.Page[appsv1.ControllerRevision]{}, errors.New("runtime revision history returned an empty response")
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return paging.Page[appsv1.ControllerRevision]{}, ctxErr
		}
		return paging.Page[appsv1.ControllerRevision]{Items: list.Items, Continue: list.Continue}, nil
	})
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	state.HistoryPages = result.Pages
	state.HistoryRequestedPages = result.Pages
	state.HistoryObservedPages = result.ObservedPages
	state.HistoryTruncated = result.Truncated
	state.HistoryComplete = err == nil && !result.Truncated
	historyObservations := make([]RuntimeRevisionObservation, 0, len(result.Items))
	for i := range result.Items {
		revision := result.Items[i].DeepCopy()
		observation := inspectRuntimeRevision(
			revision, r.omeNamespace, "", state.RuntimeName,
			runtimerevision.SourceKind(state.DeclaredSourceKind), state.DeclaredSourceNamespace,
		)
		observation.roles = []RuntimeRevisionRole{RuntimeRevisionRoleHistory}
		historyObservations = append(historyObservations, observation)
	}
	sortRuntimeRevisionObservations(historyObservations)
	for i := range historyObservations {
		mergeHistoryObservation(state, historyObservations[i])
	}
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		state.issues = append(state.issues, RuntimeSourceIssue{
			Code: RuntimeSourceIssueRevisionListFailed, cause: err,
		})
	}
	finalizeRevisionEvidence(state)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return nil
}

func mergeHistoryObservation(state *RuntimeState, history RuntimeRevisionObservation) {
	for i := range state.revisions {
		exact := &state.revisions[i]
		if exact.expectedName == "" || exact.expectedName != history.Name ||
			!exact.objectReturned || !history.objectReturned ||
			exact.Name != history.Name || exact.Namespace != history.Namespace ||
			exact.objectFingerprint == "" || exact.objectFingerprint != history.objectFingerprint {
			continue
		}
		historyAlreadyMerged := false
		for _, role := range exact.roles {
			if role == RuntimeRevisionRoleHistory {
				historyAlreadyMerged = true
				break
			}
		}
		if historyAlreadyMerged {
			continue
		}
		for _, role := range history.roles {
			exact.roles = appendRole(exact.roles, role)
		}
		return
	}
	state.revisions = append(state.revisions, history)
}

func finalizeRevisionEvidence(state *RuntimeState) {
	var liveFullHash, liveShortHash string
	if state.live != nil && state.live.Runtime.spec != nil {
		var err error
		liveFullHash, liveShortHash, err = runtimerevision.Hash(state.live.Runtime.spec)
		if err != nil {
			liveFullHash, liveShortHash = "", ""
		}
	}
	for i := range state.revisions {
		setObservationLiveRelation(&state.revisions[i], liveFullHash, liveShortHash)
	}
	classifyRevisionCollectionAnomalies(state.revisions)
	sortRuntimeRevisionObservations(state.revisions)
	if state.active == nil || state.ActiveRevisionName == "" {
		return
	}
	for i := range state.revisions {
		if state.revisions[i].expectedName == state.ActiveRevisionName {
			state.active.Consistency = state.revisions[i].Consistency
			return
		}
	}
}

func classifyRevisionCollectionAnomalies(observations []RuntimeRevisionObservation) {
	identities := make(map[string][]int)
	fullHashes := make(map[string][]int)
	shortHashes := make(map[string][]int)
	for i := range observations {
		observation := &observations[i]
		if observation.objectReturned {
			key := observation.Namespace + "\x00" + observation.Name
			identities[key] = append(identities[key], i)
		}
		if observation.fullHash != "" {
			fullHashes[observation.fullHash] = append(fullHashes[observation.fullHash], i)
		}
		if observation.computedShortHash != "" && observation.fullHash != "" {
			shortHashes[observation.computedShortHash] = append(shortHashes[observation.computedShortHash], i)
		}
	}
	for _, indexes := range identities {
		if len(indexes) < 2 {
			continue
		}
		fingerprints := make(map[string][]int)
		unknownFingerprints := 0
		for _, index := range indexes {
			fingerprint := observations[index].objectFingerprint
			if fingerprint == "" {
				unknownFingerprints++
				continue
			}
			fingerprints[fingerprint] = append(fingerprints[fingerprint], index)
		}
		for _, duplicateIndexes := range fingerprints {
			if len(duplicateIndexes) < 2 {
				continue
			}
			for _, index := range duplicateIndexes {
				addObservationConsistencyCode(&observations[index], RevisionConsistencyDuplicateIdentity)
			}
		}
		if len(fingerprints)+unknownFingerprints > 1 {
			for _, index := range indexes {
				addObservationConsistencyCode(&observations[index], RevisionConsistencyConflictingIdentity)
			}
		}
	}
	for _, indexes := range fullHashes {
		if len(indexes) < 2 {
			continue
		}
		for _, index := range indexes {
			addObservationConsistencyCode(&observations[index], RevisionConsistencyDuplicateContentHash)
		}
	}
	for _, indexes := range shortHashes {
		if len(indexes) < 2 {
			continue
		}
		uniqueFullHashes := make(map[string]struct{})
		for _, index := range indexes {
			uniqueFullHashes[observations[index].fullHash] = struct{}{}
		}
		if len(uniqueFullHashes) < 2 {
			continue
		}
		for _, index := range indexes {
			addObservationConsistencyCode(&observations[index], RevisionConsistencyShortHashCollision)
		}
	}
}

func addObservationConsistencyCode(observation *RuntimeRevisionObservation, code RevisionConsistencyCode) {
	for _, existing := range observation.consistencyCodes {
		if existing == code {
			return
		}
	}
	observation.consistencyCodes = append(observation.consistencyCodes, code)
	observation.Consistency = RevisionConsistencyInconsistent
	sortConsistencyCodes(observation.consistencyCodes)
}

func setObservationLiveRelation(observation *RuntimeRevisionObservation, liveFullHash, liveShortHash string) {
	if observation == nil {
		return
	}
	if liveFullHash == "" || observation.fullHash == "" {
		observation.RelationToLive = RuntimeHashRelationUnknown
		return
	}
	observation.RelationToLive = compareRuntimeHashes(
		liveFullHash, liveShortHash, observation.fullHash, observation.computedShortHash,
	)
}

func sortRuntimeRevisionObservations(observations []RuntimeRevisionObservation) {
	sort.SliceStable(observations, func(i, j int) bool {
		left := &observations[i]
		right := &observations[j]
		if !left.CreationTimestamp.Time.Equal(right.CreationTimestamp.Time) {
			return left.CreationTimestamp.Time.After(right.CreationTimestamp.Time)
		}
		values := [...][2]string{
			{left.Name, right.Name},
			{left.Namespace, right.Namespace},
			{left.expectedName, right.expectedName},
			{left.fullHash, right.fullHash},
			{left.computedShortHash, right.computedShortHash},
			{left.objectFingerprint, right.objectFingerprint},
			{left.UID, right.UID},
			{left.ResourceVersion, right.ResourceVersion},
			{left.SourceName, right.SourceName},
			{left.SourceKind, right.SourceKind},
			{left.SourceNamespace, right.SourceNamespace},
			{left.rawShortHash, right.rawShortHash},
		}
		for _, pair := range values {
			if pair[0] != pair[1] {
				return pair[0] < pair[1]
			}
		}
		if left.Ordinal != right.Ordinal {
			return left.Ordinal < right.Ordinal
		}
		return false
	})
}
