// Package instancestatusprojection joins one authoritative InferenceReplica
// status row with bounded, identity-checked live Pod and Warning Event evidence.
package instancestatusprojection

import (
	"errors"
	"sort"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/instancecollection"
	"sigs.k8s.io/ome/pkg/cli/instanceprojection"
	"sigs.k8s.io/ome/pkg/cli/observation"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

var (
	ErrInvalidComponent = errors.New("component must be engine, decoder, or router")
	ErrInvalidIndex     = errors.New("instance index must be non-negative")
	ErrInvalidLimits    = errors.New("instance status projection limits must be positive")
)

type Limits struct {
	MaxInstances         int
	MaxPods              int
	MaxContainerStatuses int
	MaxPodConditions     int
	MaxEvents            int
}

type Input struct {
	InferenceService      *omev1beta1.InferenceService
	Collection            instancecollection.Result
	CollectionUnavailable reportv1alpha1.UnavailableReason
	Component             omev1beta1.ComponentType
	Index                 int32
	NotOMENative          bool
	DeploymentMode        reportv1alpha1.DeploymentMode
	DeploymentModeSource  reportv1alpha1.DeploymentModeSource
	DeploymentModeOrigin  string
	DeploymentUnavailable reportv1alpha1.UnavailableReason
	Pods                  observation.Collection[corev1.Pod]
	PodsUnavailable       reportv1alpha1.UnavailableReason
	Events                observation.EventCollection
}

// EventTargets returns exact selected Pods in deterministic cap priority.
// Parent and InferenceReplica events are intentionally excluded because the
// controller emits them for multiple logical instances on the same target.
func EventTargets(
	isvc *omev1beta1.InferenceService,
	ir *omev1beta1.InferenceReplica,
	index int32,
	pods []corev1.Pod,
	maxPodConditions int,
	maxTargets int,
) ([]observation.ObjectRef, int) {
	if isvc == nil || ir == nil || maxPodConditions <= 0 || maxTargets <= 0 {
		return []observation.ObjectRef{}, 0
	}
	result := []observation.ObjectRef{}
	expectedIncarnation, authoritativeRows := int64(0), 0
	for i := range ir.Status.InstanceStatuses {
		if ir.Status.InstanceStatuses[i].Index == index {
			expectedIncarnation = ir.Status.InstanceStatuses[i].Incarnation
			authoritativeRows++
		}
	}
	if authoritativeRows != 1 || expectedIncarnation < 0 {
		return []observation.ObjectRef{}, 0
	}
	valid := make([]*corev1.Pod, 0, len(pods))
	names, uids := map[string]int{}, map[types.UID]int{}
	for i := range pods {
		pod := &pods[i]
		if pod.Namespace == isvc.Namespace && len(validation.IsDNS1123Subdomain(pod.Name)) == 0 && validUID(pod.UID) {
			names[pod.Name]++
			uids[pod.UID]++
		}
		if !validSelectedPod(pod, isvc, ir, index) {
			continue
		}
		valid = append(valid, pod)
	}
	for _, pod := range valid {
		if names[pod.Name] != 1 || uids[pod.UID] != 1 {
			continue
		}
		ready, readyOK := podCondition(pod.Status.Conditions, corev1.PodReady, maxPodConditions)
		serving, servingOK := podCondition(pod.Status.Conditions, query.ServingConditionType, maxPodConditions)
		priority := 10
		if pod.DeletionTimestamp != nil {
			priority = 25
		} else if pod.Status.Phase != corev1.PodRunning || !readyOK || !servingOK ||
			ready != string(corev1.ConditionTrue) || serving != string(corev1.ConditionTrue) {
			priority = 20
		} else if incarnation, err := strconv.ParseInt(pod.Labels[query.LabelInstanceIncarnation], 10, 64); err != nil || incarnation != expectedIncarnation {
			priority = 20
		}
		result = append(result, observation.ObjectRef{
			Namespace: isvc.Namespace, Kind: "Pod", Name: pod.Name, UID: pod.UID, Priority: priority,
		})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Priority != result[j].Priority {
			return result[i].Priority > result[j].Priority
		}
		if result[i].Kind != result[j].Kind {
			return result[i].Kind < result[j].Kind
		}
		if result[i].Name != result[j].Name {
			return result[i].Name < result[j].Name
		}
		return result[i].UID < result[j].UID
	})
	skipped := 0
	if len(result) > maxTargets {
		skipped = len(result) - maxTargets
		result = result[:maxTargets]
	}
	return result, skipped
}

// Project derives all output forms from the same safe report. It delegates
// authoritative identity, generation, aggregate, and row validation to the
// instance-list projection rather than maintaining a second interpretation.
func Project(input Input, limits Limits, clock reportv1alpha1.Clock) (reportv1alpha1.InstanceStatusReport, error) {
	if !validComponent(input.Component) {
		return reportv1alpha1.InstanceStatusReport{}, ErrInvalidComponent
	}
	if input.Index < 0 {
		return reportv1alpha1.InstanceStatusReport{}, ErrInvalidIndex
	}
	if limits.MaxInstances <= 0 || limits.MaxPods <= 0 || limits.MaxContainerStatuses <= 0 ||
		limits.MaxPodConditions <= 0 || limits.MaxEvents <= 0 {
		return reportv1alpha1.InstanceStatusReport{}, ErrInvalidLimits
	}
	list, err := instanceprojection.Project(instanceprojection.Input{
		InferenceService: input.InferenceService, Collection: input.Collection,
		CollectionUnavailable: input.CollectionUnavailable, MaxInstances: limits.MaxInstances,
	}, clock)
	if err != nil {
		return reportv1alpha1.InstanceStatusReport{}, err
	}
	report := reportv1alpha1.NewInstanceStatusReport(list.Metadata, reportv1alpha1.InstanceStatusContent{
		Summary: reportv1alpha1.InstanceStatusSummary{
			State: reportv1alpha1.InstanceStatusStateReported, Component: reportv1alpha1.RuntimeComponentType(input.Component),
			Index: input.Index, Evidence: reportv1alpha1.InstanceEvidenceReported,
		},
		Deployment: reportv1alpha1.InstanceStatusDeployment{
			Mode: input.DeploymentMode, Source: input.DeploymentModeSource,
			Origin:   input.DeploymentModeOrigin,
			Evidence: reportv1alpha1.EvidenceReported, UnavailableReason: input.DeploymentUnavailable,
		},
		Encoding: reportv1alpha1.InstanceStatusEncoding{Evidence: reportv1alpha1.EvidenceUnavailable, UnavailableReason: reportv1alpha1.UnavailableUnsupportedAPI},
		Pods:     []reportv1alpha1.InstanceStatusPod{}, Events: []reportv1alpha1.InstanceStatusEvent{},
		Issues: []reportv1alpha1.InstanceStatusIssue{},
	}, reportv1alpha1.ClockFunc(func() time.Time { return list.CollectedAt }))
	report.Content.Issues = append(report.Content.Issues, reportv1alpha1.InstanceStatusIssue{Code: reportv1alpha1.InstanceStatusIssueEncodingUnsupported, UnavailableReason: reportv1alpha1.UnavailableUnsupportedAPI})
	if input.DeploymentUnavailable != "" || input.DeploymentMode == "" {
		report.Content.Deployment.Evidence = reportv1alpha1.EvidenceUnavailable
	}
	report.Sources = append(report.Sources, list.Sources...)
	if input.NotOMENative {
		report.Content.Summary.State = reportv1alpha1.InstanceStatusStateNotOMENative
		report.Content.Summary.Evidence = reportv1alpha1.InstanceEvidenceUnavailable
		addIssue(&report, reportv1alpha1.InstanceStatusIssueNotOMENative, reportv1alpha1.UnavailableNotConfigured)
		return finish(report), nil
	}
	if input.CollectionUnavailable != "" || input.Collection.Truncated {
		report.Content.Summary.State = reportv1alpha1.InstanceStatusStateUnavailable
		report.Content.Summary.Evidence = reportv1alpha1.InstanceEvidenceUnavailable
		if input.CollectionUnavailable != "" {
			addIssue(&report, reportv1alpha1.InstanceStatusIssueCollectionUnavailable, input.CollectionUnavailable)
		}
		if input.Collection.Truncated {
			report.Content.Summary.Truncated = true
			addIssue(&report, reportv1alpha1.InstanceStatusIssueCollectionTruncated, "")
		}
		copyListIssues(&report, list, input.Component, "", input.Index)
		return finish(report), nil
	}

	component, componentCount := findComponent(list, input.Component)
	if componentCount != 1 {
		report.Content.Summary.Evidence = reportv1alpha1.InstanceEvidenceUnavailable
		switch {
		case input.CollectionUnavailable != "" && componentCount == 0:
			report.Content.Summary.State = reportv1alpha1.InstanceStatusStateUnavailable
			addIssue(&report, reportv1alpha1.InstanceStatusIssueCollectionUnavailable, input.CollectionUnavailable)
		case componentCount == 0:
			report.Content.Summary.State = reportv1alpha1.InstanceStatusStateNotProjected
			addIssue(&report, reportv1alpha1.InstanceStatusIssueInstanceMissing, reportv1alpha1.UnavailableNotFound)
		default:
			report.Content.Summary.State = reportv1alpha1.InstanceStatusStatePartial
			addIssue(&report, reportv1alpha1.InstanceStatusIssueAuthoritativeInvalid, reportv1alpha1.UnavailableMalformedPayload)
		}
		copyListIssues(&report, list, input.Component, component.InferenceReplica, input.Index)
		return finish(report), nil
	}
	report.Content.Summary.Evidence = component.State
	if component.State == reportv1alpha1.InstanceEvidenceMalformed || component.State == reportv1alpha1.InstanceEvidenceUnavailable {
		report.Content.Summary.State = stateForInvalidEvidence(component.State)
		addIssue(&report, reportv1alpha1.InstanceStatusIssueAuthoritativeInvalid, evidenceUnavailableReason(component.State))
		copyListIssues(&report, list, input.Component, component.InferenceReplica, input.Index)
		return finish(report), nil
	}
	row, rowCount := findInstance(list, input.Component, component.InferenceReplica, input.Index)
	if rowCount != 1 {
		report.Content.Summary.State = reportv1alpha1.InstanceStatusStateMissing
		if rowCount > 1 {
			report.Content.Summary.State = reportv1alpha1.InstanceStatusStatePartial
			addIssue(&report, reportv1alpha1.InstanceStatusIssueAuthoritativeInvalid, reportv1alpha1.UnavailableMalformedPayload)
		} else {
			addIssue(&report, reportv1alpha1.InstanceStatusIssueInstanceMissing, reportv1alpha1.UnavailableNotFound)
		}
		copyListIssues(&report, list, input.Component, component.InferenceReplica, input.Index)
		return finish(report), nil
	}
	ir, irCount := findReplica(input.Collection, input.Component, component.InferenceReplica)
	if irCount != 1 {
		report.Content.Summary.State = reportv1alpha1.InstanceStatusStatePartial
		addIssue(&report, reportv1alpha1.InstanceStatusIssueAuthoritativeInvalid, reportv1alpha1.UnavailableMalformedPayload)
		return finish(report), nil
	}
	rawRow, rawRowCount := findRawRow(ir, input.Index)
	if rawRowCount != 1 {
		report.Content.Summary.State = reportv1alpha1.InstanceStatusStatePartial
		addIssue(&report, reportv1alpha1.InstanceStatusIssueAuthoritativeInvalid, reportv1alpha1.UnavailableMalformedPayload)
		return finish(report), nil
	}
	report.Content.Instance = projectAuthoritative(row, rawRow, ir, input.Index, &report)
	if component.State == reportv1alpha1.InstanceEvidenceStale {
		report.Content.Summary.State = reportv1alpha1.InstanceStatusStatePartial
	}
	copyListIssues(&report, list, input.Component, component.InferenceReplica, input.Index)
	copyDetailCompleteness(&report, input.Collection, ir.Name, input.Component, input.Index)

	acceptedPods := projectPods(input, ir, rawRow, limits, &report)
	report.Content.Pods = acceptedPods
	projectEvents(input, acceptedPods, limits, &report)
	return finish(report), nil
}

func projectAuthoritative(
	row reportv1alpha1.InstanceListInstance,
	raw *omev1beta1.OMENativeInstanceStatus,
	ir *omev1beta1.InferenceReplica,
	selectedIndex int32,
	report *reportv1alpha1.InstanceStatusReport,
) *reportv1alpha1.InstanceStatusInstance {
	result := &reportv1alpha1.InstanceStatusInstance{
		InferenceReplica: row.InferenceReplica, Index: row.Index, Incarnation: row.Incarnation,
		Phase: row.Phase, RunningRevision: row.RunningRevision, TargetRevision: row.TargetRevision,
		Pods: row.Pods, Admitted: row.Admitted, Conditions: []reportv1alpha1.InstanceStatusCondition{},
		ReadySince: metaTimePointerValue(raw.ReadySince),
		Migrations: []reportv1alpha1.InstanceStatusMigration{},
	}
	if raw.ActiveOrdinal == 0 || raw.ActiveOrdinal == 1 {
		result.ActiveOrdinal = copyInt32(&raw.ActiveOrdinal)
	} else {
		addIssue(report, reportv1alpha1.InstanceStatusIssueAuthoritativeInvalid, reportv1alpha1.UnavailableMalformedPayload)
	}
	contradictory := map[string]bool{}
	statuses := map[string]metav1.ConditionStatus{}
	for _, condition := range raw.Conditions {
		if previous, present := statuses[condition.Type]; present && previous != condition.Status {
			contradictory[condition.Type] = true
		} else {
			statuses[condition.Type] = condition.Status
		}
	}
	for _, condition := range raw.Conditions {
		if contradictory[condition.Type] {
			addIssue(report, reportv1alpha1.InstanceStatusIssueAuthoritativeInvalid, reportv1alpha1.UnavailableMalformedPayload)
			continue
		}
		if !validConditionStatus(condition.Status) {
			addIssue(report, reportv1alpha1.InstanceStatusIssueAuthoritativeInvalid, reportv1alpha1.UnavailableMalformedPayload)
			continue
		}
		evidence := reportv1alpha1.InstanceEvidenceReported
		if condition.ObservedGeneration != ir.Generation {
			evidence = reportv1alpha1.InstanceEvidenceStale
			addIssue(report, reportv1alpha1.InstanceStatusIssueConditionGenerationStale, "")
		}
		result.Conditions = append(result.Conditions, reportv1alpha1.InstanceStatusCondition{
			Type: condition.Type, Status: string(condition.Status), ObservedGeneration: condition.ObservedGeneration, Evidence: evidence, Reason: condition.Reason,
			LastTransitionTime: metaTimePointer(condition.LastTransitionTime),
		})
	}
	migrationIDs := make(map[string]int, len(ir.Status.Migrations))
	for i := range ir.Status.Migrations {
		migrationIDs[ir.Status.Migrations[i].RequestUUID]++
	}
	for i := range ir.Status.Migrations {
		migration := &ir.Status.Migrations[i]
		role := ""
		if migration.SourceInstance == selectedIndex {
			role = "Source"
		} else if migration.SurgeInstance != nil && *migration.SurgeInstance == selectedIndex {
			role = "Surge"
		} else {
			continue
		}
		if migrationIDs[migration.RequestUUID] != 1 || !validMigration(migration) {
			addIssue(report, reportv1alpha1.InstanceStatusIssueMigrationInvalid, reportv1alpha1.UnavailableMalformedPayload)
			continue
		}
		result.Migrations = append(result.Migrations, reportv1alpha1.InstanceStatusMigration{
			RequestUUID: migration.RequestUUID, Role: role, Trigger: string(migration.Trigger),
			SourceInstance: migration.SourceInstance, SurgeInstance: copyInt32(migration.SurgeInstance),
			Phase: string(migration.Phase), AllocatedAt: metaTimePointerValue(migration.AllocatedAt),
			FromNode: migration.FromNode, TargetNodeHints: append([]string{}, migration.HintTargetNodes...),
			Attempt: migration.Attempt, Reason: migration.Reason, Message: migration.Message,
			StartedAt: metaTimePointer(migration.StartedAt), Deadline: metaTimePointer(migration.Deadline),
			CompletedAt: metaTimePointerValue(migration.CompletedAt), Succeeded: copyBool(migration.Succeeded),
		})
	}
	if raw.Operation != nil {
		if validOperation(raw.Operation) {
			result.Operation = &reportv1alpha1.InstanceStatusOperation{
				ID: raw.Operation.ID, Type: string(raw.Operation.Type), Step: raw.Operation.Step,
				StartedAt: metaTimePointer(raw.Operation.StartedAt), LastProgressAt: metaTimePointer(raw.Operation.LastProgressAt),
				Deadline: metaTimePointer(raw.Operation.Deadline), RetryCount: raw.Operation.RetryCount,
				TargetRevision: raw.Operation.TargetRevision, Reason: raw.Operation.Reason,
				SurgeIndex: copyInt32(raw.Operation.SurgeIndex), FromNode: raw.Operation.FromNode,
				TargetNodeHints: append([]string{}, raw.Operation.HintTargetNodes...), RequestUUID: raw.Operation.RequestUUID,
			}
		} else {
			addIssue(report, reportv1alpha1.InstanceStatusIssueOperationInvalid, reportv1alpha1.UnavailableMalformedPayload)
		}
	}
	if raw.LastFailure != nil {
		if validFailure(raw.LastFailure) {
			result.LastFailure = &reportv1alpha1.InstanceStatusFailure{
				PodName: raw.LastFailure.PodName, ContainerName: raw.LastFailure.ContainerName,
				Reason: raw.LastFailure.Reason, ExitCode: copyInt32(raw.LastFailure.ExitCode),
				Time: metaTimePointer(raw.LastFailure.Time),
			}
		} else {
			addIssue(report, reportv1alpha1.InstanceStatusIssueLastFailureInvalid, reportv1alpha1.UnavailableMalformedPayload)
		}
	}
	return result
}

func projectPods(
	input Input,
	ir *omev1beta1.InferenceReplica,
	row *omev1beta1.OMENativeInstanceStatus,
	limits Limits,
	report *reportv1alpha1.InstanceStatusReport,
) []reportv1alpha1.InstanceStatusPod {
	report.Sources = append(report.Sources, reportv1alpha1.SourceReference{
		Kind: "PodList", Namespace: input.InferenceService.Namespace,
		Name:     input.InferenceService.Name + "/" + string(input.Component) + "/" + strconv.FormatInt(int64(input.Index), 10),
		Evidence: reportv1alpha1.EvidenceObserved, UnavailableReason: input.PodsUnavailable,
	})
	if input.PodsUnavailable != "" {
		report.Sources[len(report.Sources)-1].Evidence = reportv1alpha1.EvidenceUnavailable
		addIssue(report, reportv1alpha1.InstanceStatusIssuePodsUnavailable, input.PodsUnavailable)
	}
	type candidate struct {
		value    reportv1alpha1.InstanceStatusPod
		priority int
	}
	validPods := make([]*corev1.Pod, 0, min(len(input.Pods.Items), limits.MaxPods))
	names, uids := map[string]int{}, map[types.UID]int{}
	for i := range input.Pods.Items {
		pod := &input.Pods.Items[i]
		if pod.Namespace == input.InferenceService.Namespace && len(validation.IsDNS1123Subdomain(pod.Name)) == 0 && validUID(pod.UID) {
			names[pod.Name]++
			uids[pod.UID]++
		}
		if !validSelectedPod(pod, input.InferenceService, ir, input.Index) {
			addIssue(report, reportv1alpha1.InstanceStatusIssuePodIdentityRejected, reportv1alpha1.UnavailableMalformedPayload)
			continue
		}
		validPods = append(validPods, pod)
	}
	candidates := make([]candidate, 0, min(len(validPods), limits.MaxPods))
	for _, pod := range validPods {
		if names[pod.Name] != 1 || uids[pod.UID] != 1 {
			addIssue(report, reportv1alpha1.InstanceStatusIssuePodIdentityRejected, reportv1alpha1.UnavailableMalformedPayload)
			continue
		}
		ready, conditionsOK := podCondition(pod.Status.Conditions, corev1.PodReady, limits.MaxPodConditions)
		serving, servingOK := podCondition(pod.Status.Conditions, query.ServingConditionType, limits.MaxPodConditions)
		if !conditionsOK || !servingOK {
			addIssue(report, reportv1alpha1.InstanceStatusIssuePodDetailsTruncated, reportv1alpha1.UnavailableMalformedPayload)
			continue
		}
		restarts, statusesOK := restartTotal(pod, limits.MaxContainerStatuses)
		if !statusesOK {
			addIssue(report, reportv1alpha1.InstanceStatusIssuePodDetailsTruncated, reportv1alpha1.UnavailableMalformedPayload)
			continue
		}
		incarnation, _ := strconv.ParseInt(pod.Labels[query.LabelInstanceIncarnation], 10, 64)
		value := reportv1alpha1.InstanceStatusPod{
			Name: pod.Name, Runner: pod.Labels[query.LabelRunner], Revision: pod.Labels[query.LabelRevisionHash],
			Incarnation: incarnation, Phase: string(pod.Status.Phase), Ready: ready, ServingReady: serving,
			Node: pod.Spec.NodeName, RestartCount: restarts, Deleting: pod.DeletionTimestamp != nil,
		}
		priority := 2
		if pod.DeletionTimestamp != nil {
			priority = 0
		} else if pod.Status.Phase != corev1.PodRunning || ready != string(corev1.ConditionTrue) ||
			serving != string(corev1.ConditionTrue) || incarnation != row.Incarnation {
			priority = 1
		}
		candidates = append(candidates, candidate{value: value, priority: priority})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].priority != candidates[j].priority {
			return candidates[i].priority < candidates[j].priority
		}
		return candidates[i].value.Name < candidates[j].value.Name
	})
	if input.Pods.Truncated || len(candidates) > limits.MaxPods {
		addIssue(report, reportv1alpha1.InstanceStatusIssuePodsTruncated, "")
		report.Content.Summary.Truncated = true
	}
	if len(candidates) > limits.MaxPods {
		candidates = candidates[:limits.MaxPods]
	}
	result := make([]reportv1alpha1.InstanceStatusPod, len(candidates))
	for i := range candidates {
		result[i] = candidates[i].value
	}
	return result
}

func projectEvents(
	input Input,
	pods []reportv1alpha1.InstanceStatusPod,
	limits Limits,
	report *reportv1alpha1.InstanceStatusReport,
) {
	report.Sources = append(report.Sources, reportv1alpha1.SourceReference{
		Kind: "EventList", Namespace: input.InferenceService.Namespace,
		Name:     input.InferenceService.Name + "/" + string(input.Component) + "/" + strconv.FormatInt(int64(input.Index), 10),
		Evidence: reportv1alpha1.EvidenceObserved,
	})
	if len(input.Events.Failures) > 0 {
		report.Sources[len(report.Sources)-1].Evidence = reportv1alpha1.EvidenceUnavailable
		reasons := make([]reportv1alpha1.UnavailableReason, 0, len(input.Events.Failures))
		for _, failure := range input.Events.Failures {
			reasons = append(reasons, eventFailureReason(failure.Err))
		}
		sort.Slice(reasons, func(i, j int) bool { return reasons[i] < reasons[j] })
		for _, reason := range reasons {
			addIssue(report, reportv1alpha1.InstanceStatusIssueEventsUnavailable, reason)
		}
		report.Sources[len(report.Sources)-1].UnavailableReason = reasons[0]
	}
	acceptedPodUIDs := make(map[string]types.UID, len(pods))
	for _, projected := range pods {
		for i := range input.Pods.Items {
			if input.Pods.Items[i].Name == projected.Name {
				acceptedPodUIDs[projected.Name] = input.Pods.Items[i].UID
				break
			}
		}
	}
	items := make([]reportv1alpha1.InstanceStatusEvent, 0, min(len(input.Events.Items), limits.MaxEvents))
	for i := range input.Events.Items {
		event := &input.Events.Items[i]
		if !validSelectedEvent(event, input.InferenceService.Namespace, acceptedPodUIDs) {
			addIssue(report, reportv1alpha1.InstanceStatusIssueEventIdentityRejected, reportv1alpha1.UnavailableMalformedPayload)
			continue
		}
		items = append(items, reportv1alpha1.InstanceStatusEvent{
			TargetKind: event.InvolvedObject.Kind, TargetName: event.InvolvedObject.Name,
			Reason: event.Reason, Count: eventCount(event),
			FirstSeen: eventFirstSeen(event), LastSeen: eventLastSeen(event),
		})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].TargetKind != items[j].TargetKind {
			return items[i].TargetKind < items[j].TargetKind
		}
		if items[i].TargetName != items[j].TargetName {
			return items[i].TargetName < items[j].TargetName
		}
		if items[i].Reason != items[j].Reason {
			return items[i].Reason < items[j].Reason
		}
		if items[i].Count != items[j].Count {
			return items[i].Count < items[j].Count
		}
		if timePointerLess(items[i].FirstSeen, items[j].FirstSeen) {
			return true
		}
		if timePointerLess(items[j].FirstSeen, items[i].FirstSeen) {
			return false
		}
		return timePointerLess(items[i].LastSeen, items[j].LastSeen)
	})
	if input.Events.Truncated || input.Events.SkippedTargets > 0 || len(items) > limits.MaxEvents {
		addIssue(report, reportv1alpha1.InstanceStatusIssueEventsTruncated, "")
		report.Content.Summary.Truncated = true
	}
	if len(items) > limits.MaxEvents {
		items = items[:limits.MaxEvents]
	}
	report.Content.Events = items
}

func timePointerLess(left, right *time.Time) bool {
	if left == nil {
		return right != nil
	}
	return right != nil && left.Before(*right)
}

func findComponent(list reportv1alpha1.InstanceListReport, component omev1beta1.ComponentType) (reportv1alpha1.InstanceListComponent, int) {
	var found reportv1alpha1.InstanceListComponent
	count := 0
	for _, candidate := range list.Content.Components {
		if candidate.Type == reportv1alpha1.RuntimeComponentType(component) {
			found, count = candidate, count+1
		}
	}
	return found, count
}

func findInstance(list reportv1alpha1.InstanceListReport, component omev1beta1.ComponentType, ir string, index int32) (reportv1alpha1.InstanceListInstance, int) {
	var found reportv1alpha1.InstanceListInstance
	count := 0
	for _, candidate := range list.Content.Instances {
		if candidate.Component == reportv1alpha1.RuntimeComponentType(component) && candidate.InferenceReplica == ir && candidate.Index == index {
			found, count = candidate, count+1
		}
	}
	return found, count
}

func findReplica(collection instancecollection.Result, component omev1beta1.ComponentType, name string) (*omev1beta1.InferenceReplica, int) {
	var found *omev1beta1.InferenceReplica
	count := 0
	for i := range collection.Items {
		if collection.Items[i].Spec.Component == component && collection.Items[i].Name == name {
			found, count = &collection.Items[i], count+1
		}
	}
	return found, count
}

func findRawRow(ir *omev1beta1.InferenceReplica, index int32) (*omev1beta1.OMENativeInstanceStatus, int) {
	var found *omev1beta1.OMENativeInstanceStatus
	count := 0
	for i := range ir.Status.InstanceStatuses {
		if ir.Status.InstanceStatuses[i].Index == index {
			found, count = &ir.Status.InstanceStatuses[i], count+1
		}
	}
	return found, count
}

func copyListIssues(report *reportv1alpha1.InstanceStatusReport, list reportv1alpha1.InstanceListReport, component omev1beta1.ComponentType, ir string, index int32) {
	for _, issue := range list.Content.Issues {
		if issue.Component != "" && issue.Component != reportv1alpha1.RuntimeComponentType(component) {
			continue
		}
		if issue.Code == reportv1alpha1.InstanceIssueIdentityRejected {
			addIssue(report, reportv1alpha1.InstanceStatusIssueIdentityRejected, reportv1alpha1.UnavailableMalformedPayload)
			continue
		}
		if issue.InferenceReplica != "" && ir != "" && issue.InferenceReplica != ir {
			continue
		}
		if issue.Index != nil && *issue.Index != index {
			continue
		}
		if issue.Code == reportv1alpha1.InstanceIssueSparseIndices {
			continue
		}
		code := reportv1alpha1.InstanceStatusIssueCode(issue.Code)
		reason := issue.UnavailableReason
		addIssue(report, code, reason)
		switch issue.Code {
		case reportv1alpha1.InstanceIssueCollectionTruncated,
			reportv1alpha1.InstanceIssueStatusRowsTruncated,
			reportv1alpha1.InstanceIssueOutputTruncated:
			report.Content.Summary.Truncated = true
		}
	}
}

func copyDetailCompleteness(report *reportv1alpha1.InstanceStatusReport, collection instancecollection.Result, name string, component omev1beta1.ComponentType, index int32) {
	for _, truncation := range collection.DetailsTruncated {
		if truncation.Name != name || truncation.Component != component || truncation.Index != index {
			continue
		}
		switch truncation.Kind {
		case instancecollection.DetailConditions:
			addIssue(report, reportv1alpha1.InstanceStatusIssueConditionsTruncated, "")
		case instancecollection.DetailNodeHints:
			addIssue(report, reportv1alpha1.InstanceStatusIssueOperationDetailsTruncated, "")
		case instancecollection.DetailMigrations:
			addIssue(report, reportv1alpha1.InstanceStatusIssueMigrationsTruncated, "")
		}
		report.Content.Summary.Truncated = true
	}
}

func validSelectedPod(pod *corev1.Pod, isvc *omev1beta1.InferenceService, ir *omev1beta1.InferenceReplica, index int32) bool {
	if pod == nil || pod.Namespace != isvc.Namespace || len(validation.IsDNS1123Subdomain(pod.Name)) != 0 || !validUID(pod.UID) {
		return false
	}
	if pod.Labels[constants.InferenceServicePodLabelKey] != isvc.Name ||
		pod.Labels[constants.OMEComponentLabel] != string(ir.Spec.Component) ||
		pod.Labels[query.LabelManagedBy] != query.ManagedByOMENative ||
		pod.Labels[query.LabelInstanceIdx] != strconv.FormatInt(int64(index), 10) {
		return false
	}
	incarnation, err := strconv.ParseInt(pod.Labels[query.LabelInstanceIncarnation], 10, 64)
	if err != nil || incarnation < 0 || pod.Labels[query.LabelInstanceIncarnation] != strconv.FormatInt(incarnation, 10) {
		return false
	}
	if len(validation.IsDNS1123Label(pod.Labels[query.LabelRunner])) != 0 ||
		(pod.Labels[query.LabelRevisionHash] != "" && len(validation.IsDNS1123Label(pod.Labels[query.LabelRevisionHash])) != 0) {
		return false
	}
	if rawOrdinal, present := pod.Labels[query.LabelPodOrdinal]; present {
		ordinal, err := strconv.ParseInt(rawOrdinal, 10, 32)
		if err != nil || ordinal < 0 || rawOrdinal != strconv.FormatInt(ordinal, 10) ||
			pod.Name != query.PodName(isvc.Name, workload.ComponentType(ir.Spec.Component), index, pod.Labels[query.LabelRunner], int32(ordinal)) {
			return false
		}
	}
	return exactControllerOwner(pod.OwnerReferences, "InferenceReplica", ir.Name, ir.UID)
}

func validSelectedEvent(event *corev1.Event, namespace string, pods map[string]types.UID) bool {
	if event == nil || event.Namespace != namespace || event.Type != corev1.EventTypeWarning ||
		event.Count < 0 || (event.Series != nil && event.Series.Count < 0) {
		return false
	}
	ref := event.InvolvedObject
	if ref.Namespace != "" && ref.Namespace != namespace {
		return false
	}
	if ref.APIVersion != "v1" || ref.Kind != "Pod" {
		return false
	}
	uid, ok := pods[ref.Name]
	return ok && uid != "" && ref.UID == uid
}

func eventCount(event *corev1.Event) int32 {
	if event.Series != nil {
		return event.Series.Count
	}
	return event.Count
}

func eventFirstSeen(event *corev1.Event) *time.Time {
	if !event.EventTime.IsZero() {
		value := event.EventTime.Time.UTC()
		return &value
	}
	return metaTimePointer(event.FirstTimestamp)
}

func exactControllerOwner(owners []metav1.OwnerReference, kind, name string, uid types.UID) bool {
	count := 0
	matched := false
	for _, owner := range owners {
		if owner.Controller == nil || !*owner.Controller {
			continue
		}
		count++
		matched = owner.APIVersion == omev1beta1.SchemeGroupVersion.String() && owner.Kind == kind && owner.Name == name && owner.UID == uid
	}
	return count == 1 && matched
}

func podCondition(conditions []corev1.PodCondition, kind corev1.PodConditionType, max int) (string, bool) {
	if len(conditions) > max {
		return "", false
	}
	found := false
	value := string(corev1.ConditionUnknown)
	for _, condition := range conditions {
		if condition.Type == kind {
			if found {
				return "", false
			}
			if condition.Status != corev1.ConditionTrue && condition.Status != corev1.ConditionFalse && condition.Status != corev1.ConditionUnknown {
				return "", false
			}
			found, value = true, string(condition.Status)
		}
	}
	return value, true
}

func restartTotal(pod *corev1.Pod, max int) (int32, bool) {
	if len(pod.Status.ContainerStatuses)+len(pod.Status.InitContainerStatuses) > max {
		return 0, false
	}
	var total int64
	for _, statuses := range [][]corev1.ContainerStatus{pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses} {
		for _, status := range statuses {
			if status.RestartCount < 0 {
				return 0, false
			}
			total += int64(status.RestartCount)
			if total > int64(^uint32(0)>>1) {
				return 0, false
			}
		}
	}
	return int32(total), true
}

func eventFailureReason(err error) reportv1alpha1.UnavailableReason {
	switch {
	case apierrors.IsForbidden(err):
		return reportv1alpha1.UnavailableForbidden
	case apierrors.IsNotFound(err):
		return reportv1alpha1.UnavailableNotFound
	case apierrors.IsMethodNotSupported(err):
		return reportv1alpha1.UnavailableUnsupportedAPI
	default:
		return reportv1alpha1.UnavailableUnreadable
	}
}

func validConditionStatus(status metav1.ConditionStatus) bool {
	return status == metav1.ConditionTrue || status == metav1.ConditionFalse || status == metav1.ConditionUnknown
}

func validOperation(operation *omev1beta1.InstanceOperation) bool {
	if operation == nil || operation.ID == "" || operation.Step == "" || operation.RetryCount < 0 ||
		(operation.SurgeIndex != nil && *operation.SurgeIndex < 0) {
		return false
	}
	switch operation.Type {
	case omev1beta1.InstanceOperationCreate, omev1beta1.InstanceOperationUpdate,
		omev1beta1.InstanceOperationRestart, omev1beta1.InstanceOperationMigrate,
		omev1beta1.InstanceOperationDelete:
		return true
	default:
		return false
	}
}

func validFailure(failure *omev1beta1.InstanceTermination) bool {
	return failure != nil && len(validation.IsDNS1123Subdomain(failure.PodName)) == 0 &&
		(failure.ContainerName == "" || len(validation.IsDNS1123Label(failure.ContainerName)) == 0)
}

func validMigration(migration *omev1beta1.MigrationStatus) bool {
	if migration == nil || migration.RequestUUID == "" || migration.SourceInstance < 0 || migration.Attempt < 0 ||
		(migration.SurgeInstance != nil && (*migration.SurgeInstance < 0 || *migration.SurgeInstance == migration.SourceInstance)) {
		return false
	}
	if migration.Trigger != omev1beta1.MigrationTriggerManual && migration.Trigger != omev1beta1.MigrationTriggerAuto {
		return false
	}
	switch migration.Phase {
	case omev1beta1.MigrationPhaseAccepted, omev1beta1.MigrationPhaseSurgePending,
		omev1beta1.MigrationPhaseSurgeReady, omev1beta1.MigrationPhaseDraining,
		omev1beta1.MigrationPhaseCompleted, omev1beta1.MigrationPhaseFailed,
		omev1beta1.MigrationPhaseRelocated:
		return true
	default:
		return false
	}
}

func metaTimePointer(value metav1.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	result := value.Time.UTC()
	return &result
}

func metaTimePointerValue(value *metav1.Time) *time.Time {
	if value == nil {
		return nil
	}
	return metaTimePointer(*value)
}

func eventLastSeen(event *corev1.Event) *time.Time {
	if event.Series != nil && !event.Series.LastObservedTime.IsZero() {
		value := event.Series.LastObservedTime.Time.UTC()
		return &value
	}
	if !event.EventTime.IsZero() {
		value := event.EventTime.Time.UTC()
		return &value
	}
	return metaTimePointer(event.LastTimestamp)
}

func addIssue(report *reportv1alpha1.InstanceStatusReport, code reportv1alpha1.InstanceStatusIssueCode, reason reportv1alpha1.UnavailableReason) {
	report.Content.Issues = append(report.Content.Issues, reportv1alpha1.InstanceStatusIssue{Code: code, UnavailableReason: reason})
	if report.Content.Summary.State == reportv1alpha1.InstanceStatusStateReported {
		report.Content.Summary.State = reportv1alpha1.InstanceStatusStatePartial
	}
}

func finish(report reportv1alpha1.InstanceStatusReport) reportv1alpha1.InstanceStatusReport {
	if report.Content.Summary.Truncated {
		report.Warnings = append(report.Warnings, reportv1alpha1.InstanceStatusWarning{Code: reportv1alpha1.WarningTruncated})
	}
	if report.Content.Summary.State == reportv1alpha1.InstanceStatusStatePartial {
		report.Warnings = append(report.Warnings, reportv1alpha1.InstanceStatusWarning{Code: reportv1alpha1.WarningPartialData})
	}
	if report.Content.Summary.Evidence == reportv1alpha1.InstanceEvidenceStale {
		report.Warnings = append(report.Warnings, reportv1alpha1.InstanceStatusWarning{Code: reportv1alpha1.WarningStaleEvidence})
	}
	for _, issue := range report.Content.Issues {
		if issue.Code == reportv1alpha1.InstanceStatusIssuePodsUnavailable || issue.Code == reportv1alpha1.InstanceStatusIssueEventsUnavailable || issue.Code == reportv1alpha1.InstanceStatusIssueCollectionUnavailable {
			report.Warnings = append(report.Warnings, reportv1alpha1.InstanceStatusWarning{Code: reportv1alpha1.WarningSourceUnavailable})
		}
	}
	return report.Canonical()
}

func stateForInvalidEvidence(state reportv1alpha1.InstanceEvidenceState) reportv1alpha1.InstanceStatusState {
	if state == reportv1alpha1.InstanceEvidenceUnavailable {
		return reportv1alpha1.InstanceStatusStateUnavailable
	}
	return reportv1alpha1.InstanceStatusStatePartial
}

func evidenceUnavailableReason(state reportv1alpha1.InstanceEvidenceState) reportv1alpha1.UnavailableReason {
	if state == reportv1alpha1.InstanceEvidenceMalformed {
		return reportv1alpha1.UnavailableMalformedPayload
	}
	return reportv1alpha1.UnavailableUnreadable
}

func copyInt32(value *int32) *int32 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func copyBool(value *bool) *bool {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func validUID(uid types.UID) bool {
	if len(uid) == 0 || len(uid) > 128 {
		return false
	}
	for _, value := range []byte(uid) {
		if (value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z') ||
			(value >= '0' && value <= '9') || value == '-' || value == '_' || value == '.' {
			continue
		}
		return false
	}
	return true
}

func validComponent(component omev1beta1.ComponentType) bool {
	switch component {
	case omev1beta1.EngineComponent, omev1beta1.DecoderComponent, omev1beta1.RouterComponent:
		return true
	default:
		return false
	}
}
