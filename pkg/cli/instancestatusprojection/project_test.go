package instancestatusprojection_test

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/instancecollection"
	"sigs.k8s.io/ome/pkg/cli/instancestatusprojection"
	"sigs.k8s.io/ome/pkg/cli/observation"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

func TestProjectJoinsNormalMultiPodInstanceWithAuthoritativeDetails(t *testing.T) {
	t.Parallel()

	input := statusInput()
	row := &input.Collection.Items[0].Status.InstanceStatuses[0]
	row.PodCount, row.ServingPodCount, row.AvailablePodCount = 2, 1, 1
	row.Conditions = []metav1.Condition{{Type: "AllPodsReady", Status: metav1.ConditionFalse, ObservedGeneration: 2, Reason: "WorkerPending"}}
	row.Operation = &omev1beta1.InstanceOperation{ID: "op-1", Type: omev1beta1.InstanceOperationUpdate, Step: "WaitReady", Reason: "rolling", RetryCount: 2}
	exit := int32(137)
	row.LastFailure = &omev1beta1.InstanceTermination{PodName: "chat-engine-2-worker", ContainerName: "runner", Reason: "OOMKilled", ExitCode: &exit, Message: "never emit"}
	input.Pods.Items = []corev1.Pod{
		statusPod(input.InferenceService, &input.Collection.Items[0], "chat-engine-2-worker", "worker", false, true),
		statusPod(input.InferenceService, &input.Collection.Items[0], "chat-engine-2-leader", "leader", true, true),
	}
	input.Pods.Items[0].Status.ContainerStatuses = []corev1.ContainerStatus{{RestartCount: 3}, {RestartCount: 4}}
	input.Events.Items = []corev1.Event{
		warningEvent("event-pod", "Pod", input.Pods.Items[0].Name, input.Pods.Items[0].UID, "FailedMount"),
	}

	got, err := instancestatusprojection.Project(input, statusLimits(), fixedClock())

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.InstanceStatusStateReported, got.Content.Summary.State)
	require.NotNil(t, got.Content.Instance)
	assert.Equal(t, int64(7), got.Content.Instance.Incarnation)
	assert.Equal(t, reportv1alpha1.InstancePhaseUpdating, got.Content.Instance.Phase)
	assert.Equal(t, int32(2), got.Content.Instance.Pods.Total)
	require.Len(t, got.Content.Instance.Conditions, 1)
	require.NotNil(t, got.Content.Instance.Operation)
	assert.Equal(t, "WaitReady", got.Content.Instance.Operation.Step)
	require.NotNil(t, got.Content.Instance.LastFailure)
	assert.Equal(t, "OOMKilled", got.Content.Instance.LastFailure.Reason)
	require.Len(t, got.Content.Pods, 2)
	assert.Equal(t, "chat-engine-2-leader", got.Content.Pods[0].Name)
	assert.Equal(t, int32(7), got.Content.Pods[1].RestartCount)
	assert.Equal(t, "True", got.Content.Pods[0].Ready)
	assert.Equal(t, "True", got.Content.Pods[0].ServingReady)
	require.Len(t, got.Content.Events, 1)
	assert.NotContains(t, mustJSON(t, got), "never emit")
}

func TestProjectReportsSelectedLifecycleAndMigrationRoles(t *testing.T) {
	t.Parallel()
	input := statusInput()
	ready := metav1.NewTime(time.Date(2026, 9, 14, 20, 0, 0, 0, time.UTC))
	row := &input.Collection.Items[0].Status.InstanceStatuses[0]
	row.ReadySince, row.ActiveOrdinal = &ready, 1
	surge := int32(2)
	succeeded := true
	input.Collection.Items[0].Status.Migrations = []omev1beta1.MigrationStatus{
		{RequestUUID: "source", Trigger: omev1beta1.MigrationTriggerManual, SourceInstance: 2, Phase: omev1beta1.MigrationPhaseDraining, FromNode: "node-a", HintTargetNodes: []string{"node-b"}, Reason: "move", Message: "waiting", StartedAt: ready, Deadline: ready},
		{RequestUUID: "surge", Trigger: omev1beta1.MigrationTriggerManual, SourceInstance: 1, SurgeInstance: &surge, Phase: omev1beta1.MigrationPhaseCompleted, Succeeded: &succeeded, StartedAt: ready, Deadline: ready, CompletedAt: &ready},
		{RequestUUID: "other", Trigger: omev1beta1.MigrationTriggerManual, SourceInstance: 9, Phase: omev1beta1.MigrationPhaseAccepted},
	}
	got, err := instancestatusprojection.Project(input, statusLimits(), fixedClock())
	require.NoError(t, err)
	encoded := mustJSON(t, got)
	for _, literal := range []string{`"readySince":"2026-09-14T20:00:00Z"`, `"activeOrdinal":1`, `"requestUUID":"source"`, `"role":"Source"`, `"requestUUID":"surge"`, `"role":"Surge"`, `"succeeded":true`} {
		assert.Contains(t, encoded, literal)
	}
	assert.NotContains(t, encoded, `"requestUUID":"other"`)
}

func TestProjectDoesNotReportInvalidActiveOrdinal(t *testing.T) {
	t.Parallel()
	input := statusInput()
	input.Collection.Items[0].Status.InstanceStatuses[0].ActiveOrdinal = 7
	got, err := instancestatusprojection.Project(input, statusLimits(), fixedClock())
	require.NoError(t, err)
	assert.NotContains(t, mustJSON(t, got), `"activeOrdinal":7`)
	assert.Contains(t, issueCodes(got), reportv1alpha1.InstanceStatusIssueAuthoritativeInvalid)
}

func TestProjectRejectsDuplicateMigrationIdentityOrderIndependently(t *testing.T) {
	t.Parallel()
	project := func(reverse bool) reportv1alpha1.InstanceStatusReport {
		input := statusInput()
		input.Collection.Items[0].Status.Migrations = []omev1beta1.MigrationStatus{
			{RequestUUID: "duplicate", Trigger: omev1beta1.MigrationTriggerManual, SourceInstance: 2, Phase: omev1beta1.MigrationPhaseAccepted, Reason: "a"},
			{RequestUUID: "duplicate", Trigger: omev1beta1.MigrationTriggerManual, SourceInstance: 2, Phase: omev1beta1.MigrationPhaseDraining, Reason: "b"},
		}
		if reverse {
			slicesReverse(input.Collection.Items[0].Status.Migrations)
		}
		got, err := instancestatusprojection.Project(input, statusLimits(), fixedClock())
		require.NoError(t, err)
		return got
	}
	left, right := project(false), project(true)
	assert.Equal(t, left, right)
	require.NotNil(t, left.Content.Instance)
	assert.Empty(t, left.Content.Instance.Migrations)
	assert.Contains(t, issueCodes(left), reportv1alpha1.InstanceStatusIssueMigrationInvalid)
}

func TestProjectDoesNotGuessRemovedInstanceStatusEncoding(t *testing.T) {
	t.Parallel()
	got, err := instancestatusprojection.Project(statusInput(), statusLimits(), fixedClock())
	require.NoError(t, err)
	encoded := mustJSON(t, got)
	assert.Contains(t, encoded, `"encoding":{"evidence":"Unavailable","unavailableReason":"UnsupportedAPI"}`)
	assert.Contains(t, issueCodes(got), reportv1alpha1.InstanceStatusIssueEncodingUnsupported)
	assert.NotContains(t, encoded, "DenseV1")
}

func TestProjectRejectsSelectorBlindWrongNamespaceLabelsIndexAndOwner(t *testing.T) {
	t.Parallel()

	input := statusInput()
	valid := statusPod(input.InferenceService, &input.Collection.Items[0], "valid", "leader", true, true)
	wrongNamespace := valid.DeepCopy()
	wrongNamespace.Name, wrongNamespace.Namespace, wrongNamespace.UID = "wrong-namespace", "other", "uid-wrong-namespace"
	wrongLabel := valid.DeepCopy()
	wrongLabel.Name, wrongLabel.UID, wrongLabel.Labels[constants.InferenceServicePodLabelKey] = "wrong-label", "uid-wrong-label", "other"
	wrongIndex := valid.DeepCopy()
	wrongIndex.Name, wrongIndex.UID, wrongIndex.Labels[query.LabelInstanceIdx] = "wrong-index", "uid-wrong-index", "3"
	wrongOwner := valid.DeepCopy()
	wrongOwner.Name, wrongOwner.UID, wrongOwner.OwnerReferences[0].UID = "wrong-owner", "uid-wrong-owner", "foreign"
	ownerShaped := valid.DeepCopy()
	ownerShaped.Name = "owner-shaped"
	ownerShaped.UID = "uid-owner-shaped"
	delete(ownerShaped.Labels, query.LabelManagedBy)
	input.Pods.Items = []corev1.Pod{*wrongOwner, *wrongIndex, valid, *wrongLabel, *ownerShaped, *wrongNamespace}
	input.Events.Items = []corev1.Event{
		warningEvent("valid-event", "Pod", valid.Name, valid.UID, "FailedMount"),
		warningEvent("foreign-event", "Pod", wrongOwner.Name, wrongOwner.UID, "SECRET_EVENT"),
	}

	got, err := instancestatusprojection.Project(input, statusLimits(), fixedClock())

	require.NoError(t, err)
	require.Len(t, got.Content.Pods, 1)
	assert.Equal(t, "valid", got.Content.Pods[0].Name)
	require.Len(t, got.Content.Events, 1)
	assert.Equal(t, "FailedMount", got.Content.Events[0].Reason)
	assert.Contains(t, issueCodes(got), reportv1alpha1.InstanceStatusIssuePodIdentityRejected)
	assert.Contains(t, issueCodes(got), reportv1alpha1.InstanceStatusIssueEventIdentityRejected)
	assert.NotContains(t, mustJSON(t, got), "SECRET_EVENT")
}

func TestProjectNeverInfersMissingLogicalInstanceFromPods(t *testing.T) {
	t.Parallel()

	input := statusInput()
	input.Index = 99
	pod := statusPod(input.InferenceService, &input.Collection.Items[0], "orphan", "leader", true, true)
	pod.Labels[query.LabelInstanceIdx] = "99"
	input.Pods.Items = []corev1.Pod{pod}

	got, err := instancestatusprojection.Project(input, statusLimits(), fixedClock())

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.InstanceStatusStateMissing, got.Content.Summary.State)
	assert.Nil(t, got.Content.Instance)
	assert.Empty(t, got.Content.Pods)
	assert.Empty(t, got.Content.Events)
	assert.Contains(t, issueCodes(got), reportv1alpha1.InstanceStatusIssueInstanceMissing)
}

func TestProjectFailsClosedForMalformedDuplicateAndStaleAuthority(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*instancestatusprojection.Input)
		state  reportv1alpha1.InstanceStatusState
	}{
		{name: "malformed row", mutate: func(in *instancestatusprojection.Input) {
			in.Collection.Items[0].Status.InstanceStatuses[0].Phase = "Future"
		}, state: reportv1alpha1.InstanceStatusStatePartial},
		{name: "duplicate row", mutate: func(in *instancestatusprojection.Input) {
			in.Collection.Items[0].Status.InstanceStatuses = append(in.Collection.Items[0].Status.InstanceStatuses, in.Collection.Items[0].Status.InstanceStatuses[0])
		}, state: reportv1alpha1.InstanceStatusStatePartial},
		{name: "duplicate IR", mutate: func(in *instancestatusprojection.Input) {
			duplicate := in.Collection.Items[0].DeepCopy()
			duplicate.Name = "chat-engine-two"
			duplicate.UID = "ir-two"
			in.Collection.Items = append(in.Collection.Items, *duplicate)
		}, state: reportv1alpha1.InstanceStatusStatePartial},
		{name: "stale IR", mutate: func(in *instancestatusprojection.Input) { in.Collection.Items[0].Status.ObservedGeneration-- }, state: reportv1alpha1.InstanceStatusStatePartial},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			input := statusInput()
			input.Pods.Items = []corev1.Pod{statusPod(input.InferenceService, &input.Collection.Items[0], "live", "leader", true, true)}
			test.mutate(&input)
			got, err := instancestatusprojection.Project(input, statusLimits(), fixedClock())
			require.NoError(t, err)
			assert.Equal(t, test.state, got.Content.Summary.State)
			if test.name != "stale IR" {
				assert.Nil(t, got.Content.Instance)
				assert.Empty(t, got.Content.Pods)
			} else {
				require.NotNil(t, got.Content.Instance)
				require.Len(t, got.Content.Pods, 1)
				assert.Contains(t, warningCodes(got), reportv1alpha1.WarningStaleEvidence)
			}
			assert.NotEqual(t, reportv1alpha1.InstanceStatusStateReported, got.Content.Summary.State)
		})
	}
}

func TestProjectRepresentsCollectionRawDeploymentAndOptionalSourceFailures(t *testing.T) {
	t.Parallel()

	t.Run("collection unavailable", func(t *testing.T) {
		input := statusInput()
		input.Collection = instancecollection.Result{}
		input.CollectionUnavailable = reportv1alpha1.UnavailableForbidden
		got, err := instancestatusprojection.Project(input, statusLimits(), fixedClock())
		require.NoError(t, err)
		assert.Equal(t, reportv1alpha1.InstanceStatusStateUnavailable, got.Content.Summary.State)
		assert.Empty(t, got.Content.Pods)
	})
	t.Run("raw deployment", func(t *testing.T) {
		input := statusInput()
		input.NotOMENative = true
		got, err := instancestatusprojection.Project(input, statusLimits(), fixedClock())
		require.NoError(t, err)
		assert.Equal(t, reportv1alpha1.InstanceStatusStateNotOMENative, got.Content.Summary.State)
		assert.Empty(t, got.Content.Pods)
	})
	t.Run("pod and event failures preserve instance", func(t *testing.T) {
		input := statusInput()
		input.PodsUnavailable = reportv1alpha1.UnavailableForbidden
		input.Events.Failures = []observation.SourceFailure{{Err: context.DeadlineExceeded}}
		got, err := instancestatusprojection.Project(input, statusLimits(), fixedClock())
		require.NoError(t, err)
		assert.Equal(t, reportv1alpha1.InstanceStatusStatePartial, got.Content.Summary.State)
		require.NotNil(t, got.Content.Instance)
		assert.Contains(t, issueCodes(got), reportv1alpha1.InstanceStatusIssuePodsUnavailable)
		assert.Contains(t, issueCodes(got), reportv1alpha1.InstanceStatusIssueEventsUnavailable)
	})
}

func TestProjectNeverEstablishesAuthorityFromIncompleteCollection(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		truncated bool
		reason    reportv1alpha1.UnavailableReason
		issue     reportv1alpha1.InstanceStatusIssueCode
	}{
		{name: "truncated", truncated: true, issue: reportv1alpha1.InstanceStatusIssueCollectionTruncated},
		{name: "later page forbidden", reason: reportv1alpha1.UnavailableForbidden, issue: reportv1alpha1.InstanceStatusIssueCollectionUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := statusInput()
			input.Collection.Truncated = test.truncated
			input.CollectionUnavailable = test.reason
			pod := statusPod(input.InferenceService, &input.Collection.Items[0], "selected", "leader", true, true)
			input.Pods.Items = []corev1.Pod{pod}
			input.Events.Items = []corev1.Event{warningEvent("warning", "Pod", pod.Name, pod.UID, "FailedMount")}

			got, err := instancestatusprojection.Project(input, statusLimits(), fixedClock())
			require.NoError(t, err)
			assert.Equal(t, reportv1alpha1.InstanceStatusStateUnavailable, got.Content.Summary.State)
			assert.Equal(t, reportv1alpha1.InstanceEvidenceUnavailable, got.Content.Summary.Evidence)
			assert.Nil(t, got.Content.Instance)
			assert.Empty(t, got.Content.Pods)
			assert.Empty(t, got.Content.Events)
			assert.Contains(t, issueCodes(got), test.issue)
			if test.reason != "" {
				assert.Contains(t, got.Content.Issues, reportv1alpha1.InstanceStatusIssue{Code: test.issue, UnavailableReason: test.reason})
			}
		})
	}
}

func TestProjectPreservesPreciseAuthorityIssuesAndStaleWarnings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		code   reportv1alpha1.InstanceStatusIssueCode
		mutate func(*omev1beta1.InferenceReplica)
	}{
		{name: "parent missing", code: "ParentGenerationMissing", mutate: func(ir *omev1beta1.InferenceReplica) {
			delete(ir.Annotations, constants.InferenceReplicaParentGenerationAnnotationKey)
		}},
		{name: "parent invalid", code: "ParentGenerationInvalid", mutate: func(ir *omev1beta1.InferenceReplica) {
			ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] = "invalid"
		}},
		{name: "parent stale", code: "ParentGenerationStale", mutate: func(ir *omev1beta1.InferenceReplica) {
			ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] = "3"
		}},
		{name: "parent ahead", code: "ParentGenerationAhead", mutate: func(ir *omev1beta1.InferenceReplica) {
			ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] = "5"
		}},
		{name: "status unobserved", code: "StatusUnobserved", mutate: func(ir *omev1beta1.InferenceReplica) { ir.Status.ObservedGeneration = 0 }},
		{name: "observed invalid", code: "ObservedGenerationInvalid", mutate: func(ir *omev1beta1.InferenceReplica) { ir.Status.ObservedGeneration = 3 }},
		{name: "observed stale", code: "StaleGeneration", mutate: func(ir *omev1beta1.InferenceReplica) { ir.Status.ObservedGeneration = 1 }},
		{name: "rows missing", code: "StatusesNotReported", mutate: func(ir *omev1beta1.InferenceReplica) { ir.Status.InstanceStatuses = nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := statusInput()
			test.mutate(&input.Collection.Items[0])
			got, err := instancestatusprojection.Project(input, statusLimits(), fixedClock())
			require.NoError(t, err)
			assert.Contains(t, issueCodes(got), test.code)
			if got.Content.Summary.Evidence == reportv1alpha1.InstanceEvidenceStale {
				assert.Contains(t, warningCodes(got), reportv1alpha1.WarningStaleEvidence)
			}
		})
	}
}

func TestProjectPreservesStatusRowTruncationAndRefusesSupplements(t *testing.T) {
	t.Parallel()
	input := statusInput()
	input.Collection.Items[0].Status.InstanceStatuses = nil
	input.Collection.StatusRowsTruncated = []instancecollection.StatusRowsTruncation{{Name: "chat-engine", Component: omev1beta1.EngineComponent}}
	input.Pods.Items = []corev1.Pod{statusPod(input.InferenceService, &input.Collection.Items[0], "selected", "leader", true, true)}
	got, err := instancestatusprojection.Project(input, statusLimits(), fixedClock())
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.InstanceStatusStateUnavailable, got.Content.Summary.State)
	assert.Contains(t, issueCodes(got), reportv1alpha1.InstanceStatusIssueStatusRowsTruncated)
	assert.Empty(t, got.Content.Pods)
	assert.Empty(t, got.Content.Events)
}

func TestProjectReportsAuthoritativeDetailTruncationPrecisely(t *testing.T) {
	t.Parallel()

	input := statusInput()
	input.Collection.DetailsTruncated = []instancecollection.DetailTruncation{
		{Name: "chat-engine", Component: omev1beta1.EngineComponent, Index: 2, Kind: instancecollection.DetailConditions},
		{Name: "chat-engine", Component: omev1beta1.EngineComponent, Index: 2, Kind: instancecollection.DetailNodeHints},
	}
	input.Collection.Rejected = []instancecollection.Rejection{{Name: "hostile", Reason: instancecollection.RejectionMetadata}}

	got, err := instancestatusprojection.Project(input, statusLimits(), fixedClock())

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.InstanceStatusStatePartial, got.Content.Summary.State)
	assert.True(t, got.Content.Summary.Truncated)
	assert.Contains(t, issueCodes(got), reportv1alpha1.InstanceStatusIssueConditionsTruncated)
	assert.Contains(t, issueCodes(got), reportv1alpha1.InstanceStatusIssueOperationDetailsTruncated)
	assert.Contains(t, issueCodes(got), reportv1alpha1.InstanceStatusIssueIdentityRejected)
}

func TestProjectRejectsMalformedPodOperationalIdentities(t *testing.T) {
	t.Parallel()

	mutations := map[string]func(*corev1.Pod){
		"invalid name":         func(pod *corev1.Pod) { pod.Name = "INVALID_NAME" },
		"missing UID":          func(pod *corev1.Pod) { pod.UID = "" },
		"unsafe UID":           func(pod *corev1.Pod) { pod.UID = "uid/unsafe" },
		"oversized UID":        func(pod *corev1.Pod) { pod.UID = types.UID(strings.Repeat("a", 129)) },
		"bad incarnation":      func(pod *corev1.Pod) { pod.Labels[query.LabelInstanceIncarnation] = "invalid" },
		"bad runner":           func(pod *corev1.Pod) { pod.Labels[query.LabelRunner] = "INVALID_RUNNER" },
		"bad revision":         func(pod *corev1.Pod) { pod.Labels[query.LabelRevisionHash] = "INVALID_REV" },
		"second controller":    func(pod *corev1.Pod) { pod.OwnerReferences = append(pod.OwnerReferences, pod.OwnerReferences[0]) },
		"wrong owner API":      func(pod *corev1.Pod) { pod.OwnerReferences[0].APIVersion = "v1" },
		"wrong owner kind":     func(pod *corev1.Pod) { pod.OwnerReferences[0].Kind = "Pod" },
		"wrong owner name":     func(pod *corev1.Pod) { pod.OwnerReferences[0].Name = "other" },
		"non-controller owner": func(pod *corev1.Pod) { value := false; pod.OwnerReferences[0].Controller = &value },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			input := statusInput()
			pod := statusPod(input.InferenceService, &input.Collection.Items[0], "selected", "worker", true, true)
			mutate(&pod)
			input.Pods.Items = []corev1.Pod{pod}
			got, err := instancestatusprojection.Project(input, statusLimits(), fixedClock())
			require.NoError(t, err)
			assert.Empty(t, got.Content.Pods)
			assert.Contains(t, issueCodes(got), reportv1alpha1.InstanceStatusIssuePodIdentityRejected)
		})
	}
}

func TestProjectTreatsUnreportedAuthoritativeRowsAsUnavailable(t *testing.T) {
	t.Parallel()

	input := statusInput()
	input.Collection.Items[0].Status.InstanceStatuses = nil
	got, err := instancestatusprojection.Project(input, statusLimits(), fixedClock())
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.InstanceStatusStateUnavailable, got.Content.Summary.State)
	assert.Nil(t, got.Content.Instance)
	assert.Empty(t, got.Content.Pods)
}

func TestProjectBoundsPrioritizesAndSanitizesLiveEvidenceDeterministically(t *testing.T) {
	t.Parallel()

	project := func(reverse bool) reportv1alpha1.InstanceStatusReport {
		input := statusInput()
		pods := []corev1.Pod{
			statusPod(input.InferenceService, &input.Collection.Items[0], "healthy", "worker", true, true),
			statusPod(input.InferenceService, &input.Collection.Items[0], "unhealthy", "worker", false, false),
			statusPod(input.InferenceService, &input.Collection.Items[0], "terminating", "worker", true, true),
		}
		now := metav1.NewTime(time.Now())
		pods[2].DeletionTimestamp = &now
		pods[1].Status.ContainerStatuses = make([]corev1.ContainerStatus, 4)
		if reverse {
			slicesReverse(pods)
		}
		input.Pods.Items = pods
		input.Pods.Truncated = true
		input.Events.Truncated = true
		input.Events.Items = []corev1.Event{warningEvent("unsafe", "Pod", "terminating", podsUID(pods, "terminating"), "Bearer secret")}
		limits := statusLimits()
		limits.MaxPods = 2
		limits.MaxContainerStatuses = 2
		limits.MaxEvents = 1
		got, err := instancestatusprojection.Project(input, limits, fixedClock())
		require.NoError(t, err)
		return got
	}

	left, right := project(false), project(true)
	assert.Equal(t, left, right)
	require.Len(t, left.Content.Pods, 2)
	assert.Equal(t, []string{"healthy", "terminating"}, []string{left.Content.Pods[0].Name, left.Content.Pods[1].Name})
	assert.Equal(t, "[REDACTED]", left.Content.Events[0].Reason)
	assert.True(t, left.Content.Summary.Truncated)
	for _, code := range []reportv1alpha1.InstanceStatusIssueCode{
		reportv1alpha1.InstanceStatusIssuePodsTruncated,
		reportv1alpha1.InstanceStatusIssuePodDetailsTruncated,
		reportv1alpha1.InstanceStatusIssueEventsTruncated,
	} {
		assert.Contains(t, issueCodes(left), code)
	}
}

func TestEventTargetsKeepExactIRAndPrioritizeUnhealthyPodsDeterministically(t *testing.T) {
	t.Parallel()

	input := statusInput()
	ir := &input.Collection.Items[0]
	healthy := statusPod(input.InferenceService, ir, "a-healthy", "worker", true, true)
	unhealthy := statusPod(input.InferenceService, ir, "z-unhealthy", "worker", false, false)
	foreign := statusPod(input.InferenceService, ir, "foreign", "worker", false, false)
	foreign.OwnerReferences[0].UID = "other"
	targets, skipped := instancestatusprojection.EventTargets(
		input.InferenceService, ir, input.Index, []corev1.Pod{healthy, foreign, unhealthy}, 16, 2,
	)

	require.Len(t, targets, 2)
	assert.Zero(t, skipped)
	assert.Equal(t, []string{"z-unhealthy", "a-healthy"}, []string{targets[0].Name, targets[1].Name})
	assert.Greater(t, targets[0].Priority, targets[1].Priority)
}

func TestEventTargetsRejectInvalidInputsAndPrioritizeTerminatingPods(t *testing.T) {
	t.Parallel()

	input := statusInput()
	ir := &input.Collection.Items[0]
	terminating := statusPod(input.InferenceService, ir, "terminating", "worker", true, true)
	now := metav1.NewTime(time.Now())
	terminating.DeletionTimestamp = &now
	targets, _ := instancestatusprojection.EventTargets(nil, ir, input.Index, nil, 16, 1)
	assert.Empty(t, targets)
	targets, _ = instancestatusprojection.EventTargets(input.InferenceService, nil, input.Index, nil, 16, 1)
	assert.Empty(t, targets)
	targets, _ = instancestatusprojection.EventTargets(input.InferenceService, ir, input.Index, nil, 0, 1)
	assert.Empty(t, targets)

	targets, _ = instancestatusprojection.EventTargets(input.InferenceService, ir, input.Index, []corev1.Pod{terminating}, 16, 1)
	require.Len(t, targets, 1)
	assert.Equal(t, "terminating", targets[0].Name)
}

func TestEventTargetsApplyCapAfterUnhealthyPriority(t *testing.T) {
	t.Parallel()
	input := statusInput()
	ir := &input.Collection.Items[0]
	healthy := statusPod(input.InferenceService, ir, "a-healthy", "worker", true, true)
	unhealthy := statusPod(input.InferenceService, ir, "z-unhealthy", "worker", false, false)
	targets, skipped := instancestatusprojection.EventTargets(input.InferenceService, ir, input.Index, []corev1.Pod{healthy, unhealthy}, 16, 1)
	require.Len(t, targets, 1)
	assert.Equal(t, 1, skipped)
	assert.Equal(t, "z-unhealthy", targets[0].Name)
}

func TestEventTargetsPrioritizeStaleIncarnationBeforeCap(t *testing.T) {
	t.Parallel()
	input := statusInput()
	ir := &input.Collection.Items[0]
	current := statusPod(input.InferenceService, ir, "a-current", "worker", true, true)
	stale := statusPod(input.InferenceService, ir, "z-stale", "worker", true, true)
	stale.Labels[query.LabelInstanceIncarnation] = "6"
	targets, skipped := instancestatusprojection.EventTargets(input.InferenceService, ir, input.Index, []corev1.Pod{current, stale}, 16, 1)
	require.Len(t, targets, 1)
	assert.Equal(t, 1, skipped)
	assert.Equal(t, "z-stale", targets[0].Name)
}

func TestProjectRejectsUnscopedInferenceReplicaEvents(t *testing.T) {
	t.Parallel()
	input := statusInput()
	input.Events.Items = []corev1.Event{warningEvent("sibling", "InferenceReplica", "chat-engine", input.Collection.Items[0].UID, "SiblingFailed")}
	got, err := instancestatusprojection.Project(input, statusLimits(), fixedClock())
	require.NoError(t, err)
	assert.Empty(t, got.Content.Events)
	assert.NotContains(t, mustJSON(t, got), "SiblingFailed")
}

func TestProjectRejectsAmbiguousAndMalformedPodDetailsOrderIndependently(t *testing.T) {
	t.Parallel()
	project := func(reverse bool) reportv1alpha1.InstanceStatusReport {
		input := statusInput()
		duplicateName := statusPod(input.InferenceService, &input.Collection.Items[0], "duplicate", "leader", true, true)
		duplicateName2 := duplicateName.DeepCopy()
		duplicateName2.UID = "different-uid"
		duplicateUID := statusPod(input.InferenceService, &input.Collection.Items[0], "uid-a", "leader", true, true)
		duplicateUID2 := statusPod(input.InferenceService, &input.Collection.Items[0], "uid-b", "leader", true, true)
		duplicateUID2.UID = duplicateUID.UID
		negative := statusPod(input.InferenceService, &input.Collection.Items[0], "negative", "leader", true, true)
		negative.Labels[query.LabelInstanceIncarnation] = "-1"
		noncanonical := statusPod(input.InferenceService, &input.Collection.Items[0], "noncanonical", "leader", true, true)
		noncanonical.Labels[query.LabelInstanceIncarnation] = "07"
		badRestarts := statusPod(input.InferenceService, &input.Collection.Items[0], "restarts", "leader", true, true)
		badRestarts.Status.ContainerStatuses = []corev1.ContainerStatus{{RestartCount: -1}}
		overflow := statusPod(input.InferenceService, &input.Collection.Items[0], "overflow", "leader", true, true)
		overflow.Status.ContainerStatuses = []corev1.ContainerStatus{{RestartCount: math.MaxInt32}, {RestartCount: 1}}
		conflicting := statusPod(input.InferenceService, &input.Collection.Items[0], "conditions", "leader", true, true)
		conflicting.Status.Conditions = append(conflicting.Status.Conditions, corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionFalse})
		input.Pods.Items = []corev1.Pod{duplicateName, *duplicateName2, duplicateUID, duplicateUID2, negative, noncanonical, badRestarts, overflow, conflicting}
		if reverse {
			slicesReverse(input.Pods.Items)
		}
		got, err := instancestatusprojection.Project(input, statusLimits(), fixedClock())
		require.NoError(t, err)
		return got
	}
	left, right := project(false), project(true)
	assert.Equal(t, left, right)
	assert.Empty(t, left.Content.Pods)
	assert.Empty(t, left.Content.Events)
}

func TestProjectAcceptsOptionalRevisionPreservesUnknownAndRejectsBadOrdinalOrStatusCap(t *testing.T) {
	t.Parallel()
	input := statusInput()
	valid := statusPod(input.InferenceService, &input.Collection.Items[0], "chat-engine-2-worker-0", "worker", true, true)
	valid.UID = "uid-valid"
	delete(valid.Labels, query.LabelRevisionHash)
	valid.Labels[query.LabelPodOrdinal] = "0"
	valid.Status.Conditions[0].Status = corev1.ConditionUnknown
	badOrdinal := valid.DeepCopy()
	badOrdinal.Name, badOrdinal.UID, badOrdinal.Labels[query.LabelPodOrdinal] = "chat-engine-2-worker-9", "uid-ordinal", "0"
	tooManyStatuses := valid.DeepCopy()
	tooManyStatuses.Name, tooManyStatuses.UID = "chat-engine-2-worker-1", "uid-statuses"
	tooManyStatuses.Labels[query.LabelPodOrdinal] = "1"
	tooManyStatuses.Status.ContainerStatuses = make([]corev1.ContainerStatus, statusLimits().MaxContainerStatuses+1)
	input.Pods.Items = []corev1.Pod{valid, *badOrdinal, *tooManyStatuses}

	got, err := instancestatusprojection.Project(input, statusLimits(), fixedClock())
	require.NoError(t, err)
	require.Len(t, got.Content.Pods, 1)
	assert.Equal(t, "chat-engine-2-worker-0", got.Content.Pods[0].Name)
	assert.Equal(t, "Unknown", got.Content.Pods[0].Ready)
	assert.Contains(t, issueCodes(got), reportv1alpha1.InstanceStatusIssuePodIdentityRejected)
	assert.Contains(t, issueCodes(got), reportv1alpha1.InstanceStatusIssuePodDetailsTruncated)
}

func TestProjectClassifiesEventFailuresWithoutRawErrorsDeterministically(t *testing.T) {
	t.Parallel()
	project := func(reverse bool) reportv1alpha1.InstanceStatusReport {
		input := statusInput()
		failures := []observation.SourceFailure{
			{Err: apierrors.NewForbidden(schema.GroupResource{Resource: "events"}, "", errors.New("secret raw error"))},
			{Err: apierrors.NewNotFound(schema.GroupResource{Resource: "events"}, "")},
			{Err: apierrors.NewMethodNotSupported(schema.GroupResource{Resource: "events"}, "list")},
			{Err: context.DeadlineExceeded},
		}
		if reverse {
			slicesReverse(failures)
		}
		input.Events.Failures = failures
		got, err := instancestatusprojection.Project(input, statusLimits(), fixedClock())
		require.NoError(t, err)
		return got
	}
	left, right := project(false), project(true)
	assert.Equal(t, left, right)
	for _, reason := range []reportv1alpha1.UnavailableReason{reportv1alpha1.UnavailableForbidden, reportv1alpha1.UnavailableNotFound, reportv1alpha1.UnavailableUnsupportedAPI, reportv1alpha1.UnavailableUnreadable} {
		assert.Contains(t, left.Content.Issues, reportv1alpha1.InstanceStatusIssue{Code: reportv1alpha1.InstanceStatusIssueEventsUnavailable, UnavailableReason: reason})
	}
	assert.NotContains(t, mustJSON(t, left), "secret raw error")
}

func TestProjectRetainsExactPageOnePodEventsWhenLaterPodPageFails(t *testing.T) {
	t.Parallel()
	input := statusInput()
	pod := statusPod(input.InferenceService, &input.Collection.Items[0], "selected", "worker", true, true)
	input.Pods.Items = []corev1.Pod{pod}
	input.PodsUnavailable = reportv1alpha1.UnavailableForbidden
	input.Events.Items = []corev1.Event{warningEvent("warning", "Pod", pod.Name, pod.UID, "FailedMount")}
	got, err := instancestatusprojection.Project(input, statusLimits(), fixedClock())
	require.NoError(t, err)
	require.Len(t, got.Content.Pods, 1)
	require.Len(t, got.Content.Events, 1)
	assert.Equal(t, "FailedMount", got.Content.Events[0].Reason)
	assert.NotContains(t, issueCodes(got), reportv1alpha1.InstanceStatusIssueEventIdentityRejected)
	assert.Contains(t, issueCodes(got), reportv1alpha1.InstanceStatusIssuePodsUnavailable)
}

func TestProjectRejectsInvalidArguments(t *testing.T) {
	t.Parallel()

	input := statusInput()
	input.Component = "future"
	_, err := instancestatusprojection.Project(input, statusLimits(), fixedClock())
	assert.ErrorIs(t, err, instancestatusprojection.ErrInvalidComponent)

	input = statusInput()
	input.Index = -1
	_, err = instancestatusprojection.Project(input, statusLimits(), fixedClock())
	assert.ErrorIs(t, err, instancestatusprojection.ErrInvalidIndex)

	for _, mutate := range []func(*instancestatusprojection.Limits){
		func(value *instancestatusprojection.Limits) { value.MaxInstances = 0 },
		func(value *instancestatusprojection.Limits) { value.MaxPods = 0 },
		func(value *instancestatusprojection.Limits) { value.MaxContainerStatuses = 0 },
		func(value *instancestatusprojection.Limits) { value.MaxPodConditions = 0 },
		func(value *instancestatusprojection.Limits) { value.MaxEvents = 0 },
	} {
		limits := statusLimits()
		mutate(&limits)
		_, err = instancestatusprojection.Project(statusInput(), limits, fixedClock())
		assert.ErrorIs(t, err, instancestatusprojection.ErrInvalidLimits)
	}
}

func TestProjectRejectsMalformedAuthoritativeDetails(t *testing.T) {
	t.Parallel()

	input := statusInput()
	row := &input.Collection.Items[0].Status.InstanceStatuses[0]
	row.Conditions = []metav1.Condition{{Type: "Ready", Status: "Future"}}
	row.Operation = &omev1beta1.InstanceOperation{ID: "op", Type: "Future", Step: "run"}
	row.LastFailure = &omev1beta1.InstanceTermination{PodName: "INVALID_NAME"}

	got, err := instancestatusprojection.Project(input, statusLimits(), fixedClock())

	require.NoError(t, err)
	require.NotNil(t, got.Content.Instance)
	assert.Empty(t, got.Content.Instance.Conditions)
	assert.Nil(t, got.Content.Instance.Operation)
	assert.Nil(t, got.Content.Instance.LastFailure)
	assert.Contains(t, issueCodes(got), reportv1alpha1.InstanceStatusIssueAuthoritativeInvalid)
	assert.Contains(t, issueCodes(got), reportv1alpha1.InstanceStatusIssueOperationInvalid)
	assert.Contains(t, issueCodes(got), reportv1alpha1.InstanceStatusIssueLastFailureInvalid)
}

func TestProjectRejectsContradictoryConditionsAndMarksOldConditionGenerationStale(t *testing.T) {
	t.Parallel()
	input := statusInput()
	ir := &input.Collection.Items[0]
	ir.Generation = 5
	ir.Status.ObservedGeneration = 5
	ir.Status.InstanceStatuses[0].Conditions = []metav1.Condition{
		{Type: "Ready", Status: metav1.ConditionTrue, ObservedGeneration: 5},
		{Type: "Ready", Status: metav1.ConditionFalse, ObservedGeneration: 5},
		{Type: "Serving", Status: metav1.ConditionTrue, ObservedGeneration: 4},
	}
	got, err := instancestatusprojection.Project(input, statusLimits(), fixedClock())
	require.NoError(t, err)
	require.NotNil(t, got.Content.Instance)
	require.Len(t, got.Content.Instance.Conditions, 1)
	assert.Equal(t, "Serving", got.Content.Instance.Conditions[0].Type)
	assert.Contains(t, mustJSON(t, got), `"observedGeneration":4`)
	assert.Contains(t, issueCodes(got), reportv1alpha1.InstanceStatusIssueAuthoritativeInvalid)
	assert.Contains(t, issueCodes(got), reportv1alpha1.InstanceStatusIssueCode("ConditionGenerationStale"))
}

func TestProjectRejectsEveryUnsafeEventShape(t *testing.T) {
	t.Parallel()

	mutations := map[string]func(*corev1.Event){
		"wrong event namespace":  func(event *corev1.Event) { event.Namespace = "other" },
		"normal type":            func(event *corev1.Event) { event.Type = corev1.EventTypeNormal },
		"negative count":         func(event *corev1.Event) { event.Count = -1 },
		"wrong ref namespace":    func(event *corev1.Event) { event.InvolvedObject.Namespace = "other" },
		"wrong IR identity":      func(event *corev1.Event) { event.InvolvedObject.UID = "other" },
		"unknown target kind":    func(event *corev1.Event) { event.InvolvedObject.Kind = "Secret" },
		"wrong selected pod UID": func(event *corev1.Event) { event.InvolvedObject.UID = "other" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			input := statusInput()
			pod := statusPod(input.InferenceService, &input.Collection.Items[0], "selected", "worker", true, true)
			input.Pods.Items = []corev1.Pod{pod}
			kind, target, uid := "InferenceReplica", "chat-engine", input.Collection.Items[0].UID
			if name == "wrong selected pod UID" {
				kind, target, uid = "Pod", pod.Name, pod.UID
			}
			event := warningEvent("unsafe", kind, target, uid, "Failed")
			mutate(&event)
			input.Events.Items = []corev1.Event{event}
			got, err := instancestatusprojection.Project(input, statusLimits(), fixedClock())
			require.NoError(t, err)
			assert.Empty(t, got.Content.Events)
			assert.Contains(t, issueCodes(got), reportv1alpha1.InstanceStatusIssueEventIdentityRejected)
		})
	}
}

func TestProjectUsesSafeEventAndOperationTimes(t *testing.T) {
	t.Parallel()

	input := statusInput()
	now := time.Date(2026, 9, 14, 23, 0, 0, 0, time.FixedZone("offset", 3600))
	metaNow := metav1.NewTime(now)
	microNow := metav1.NewMicroTime(now)
	row := &input.Collection.Items[0].Status.InstanceStatuses[0]
	row.Conditions = []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue, LastTransitionTime: metaNow}}
	row.Operation = &omev1beta1.InstanceOperation{ID: "op", Type: omev1beta1.InstanceOperationMigrate, Step: "Move", StartedAt: metaNow, LastProgressAt: metaNow, Deadline: metaNow}
	input.Events.Items = []corev1.Event{
		warningEvent("series", "InferenceReplica", "chat-engine", input.Collection.Items[0].UID, "Series"),
		warningEvent("event-time", "InferenceReplica", "chat-engine", input.Collection.Items[0].UID, "EventTime"),
		warningEvent("last-time", "InferenceReplica", "chat-engine", input.Collection.Items[0].UID, "LastTime"),
	}
	input.Events.Items[0].Series = &corev1.EventSeries{LastObservedTime: microNow}
	input.Events.Items[1].EventTime = microNow
	input.Events.Items[2].LastTimestamp = metaNow

	got, err := instancestatusprojection.Project(input, statusLimits(), fixedClock())

	require.NoError(t, err)
	require.NotNil(t, got.Content.Instance.Operation)
	assert.Equal(t, time.UTC, got.Content.Instance.Operation.StartedAt.Location())
	for _, event := range got.Content.Events {
		require.NotNil(t, event.LastSeen)
		assert.Equal(t, time.UTC, event.LastSeen.Location())
	}
}

func TestProjectUsesCoreV1EventSeriesSemantics(t *testing.T) {
	t.Parallel()
	input := statusInput()
	pod := statusPod(input.InferenceService, &input.Collection.Items[0], "selected", "worker", true, true)
	input.Pods.Items = []corev1.Pod{pod}
	first := metav1.NewMicroTime(time.Date(2026, 9, 14, 20, 0, 0, 0, time.UTC))
	last := metav1.NewMicroTime(first.Add(time.Minute))
	event := warningEvent("series", "Pod", pod.Name, pod.UID, "Failed")
	event.InvolvedObject.APIVersion = "v1"
	event.Count = 2
	event.EventTime = first
	event.FirstTimestamp = metav1.NewTime(first.Add(time.Hour))
	event.Series = &corev1.EventSeries{Count: 7, LastObservedTime: last}
	input.Events.Items = []corev1.Event{event}

	got, err := instancestatusprojection.Project(input, statusLimits(), fixedClock())
	require.NoError(t, err)
	require.Len(t, got.Content.Events, 1)
	assert.Equal(t, int32(7), got.Content.Events[0].Count)
	assert.Equal(t, first.Time.UTC(), *got.Content.Events[0].FirstSeen)
	assert.Equal(t, last.Time.UTC(), *got.Content.Events[0].LastSeen)

	input.Events.Items[0].Series.Count = -1
	got, err = instancestatusprojection.Project(input, statusLimits(), fixedClock())
	require.NoError(t, err)
	assert.Empty(t, got.Content.Events)
	input.Events.Items[0].Series = nil
	input.Events.Items[0].InvolvedObject.APIVersion = "other/v1"
	got, err = instancestatusprojection.Project(input, statusLimits(), fixedClock())
	require.NoError(t, err)
	assert.Empty(t, got.Content.Events)
}

func TestProjectEventCapUsesTotalOrderIndependentOfInput(t *testing.T) {
	t.Parallel()
	project := func(reverse bool) reportv1alpha1.InstanceStatusReport {
		input := statusInput()
		pod := statusPod(input.InferenceService, &input.Collection.Items[0], "selected", "worker", true, true)
		input.Pods.Items = []corev1.Pod{pod}
		first := warningEvent("one", "Pod", pod.Name, pod.UID, "Failed")
		first.Count = 1
		first.FirstTimestamp = metav1.NewTime(time.Date(2026, 9, 14, 21, 0, 0, 0, time.UTC))
		second := warningEvent("two", "Pod", pod.Name, pod.UID, "Failed")
		second.Count = 1
		second.FirstTimestamp = metav1.NewTime(time.Date(2026, 9, 14, 20, 0, 0, 0, time.UTC))
		input.Events.Items = []corev1.Event{first, second}
		if reverse {
			slicesReverse(input.Events.Items)
		}
		limits := statusLimits()
		limits.MaxEvents = 1
		got, err := instancestatusprojection.Project(input, limits, fixedClock())
		require.NoError(t, err)
		return got
	}
	left, right := project(false), project(true)
	assert.Equal(t, left, right)
	require.Len(t, left.Content.Events, 1)
	assert.Equal(t, int32(1), left.Content.Events[0].Count)
	assert.Equal(t, time.Date(2026, 9, 14, 20, 0, 0, 0, time.UTC), *left.Content.Events[0].FirstSeen)
}

func TestProjectRejectsMalformedSelectedMigrations(t *testing.T) {
	t.Parallel()
	input := statusInput()
	same := int32(2)
	input.Collection.Items[0].Status.Migrations = []omev1beta1.MigrationStatus{
		{RequestUUID: "bad-trigger", Trigger: "Future", SourceInstance: 2, Phase: omev1beta1.MigrationPhaseAccepted},
		{RequestUUID: "bad-source", Trigger: omev1beta1.MigrationTriggerManual, SourceInstance: -1, SurgeInstance: &same, Phase: omev1beta1.MigrationPhaseAccepted},
		{RequestUUID: "bad-attempt", Trigger: omev1beta1.MigrationTriggerManual, SourceInstance: 2, Phase: omev1beta1.MigrationPhaseAccepted, Attempt: -1},
		{RequestUUID: "bad-phase", Trigger: omev1beta1.MigrationTriggerManual, SourceInstance: 2, Phase: "Future"},
	}
	got, err := instancestatusprojection.Project(input, statusLimits(), fixedClock())
	require.NoError(t, err)
	require.NotNil(t, got.Content.Instance)
	assert.Empty(t, got.Content.Instance.Migrations)
	assert.Contains(t, issueCodes(got), reportv1alpha1.InstanceStatusIssueMigrationInvalid)
}

func statusInput() instancestatusprojection.Input {
	isvc := &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "chat", Namespace: "prod", UID: "isvc-uid", Generation: 4,
	}}
	controller := true
	ir := omev1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name: "chat-engine", Namespace: "prod", UID: "ir-uid", Generation: 2,
			Annotations:     map[string]string{constants.InferenceReplicaParentGenerationAnnotationKey: "4"},
			Labels:          map[string]string{constants.InferenceServiceLabel: "chat", constants.OMEComponentLabel: "engine"},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: omev1beta1.SchemeGroupVersion.String(), Kind: "InferenceService", Name: "chat", UID: "isvc-uid", Controller: &controller}},
		},
		Spec: omev1beta1.InferenceReplicaSpec{ParentRef: omev1beta1.ParentReference{Name: "chat"}, Component: omev1beta1.EngineComponent},
		Status: omev1beta1.InferenceReplicaStatus{
			ObservedGeneration: 2, Replicas: 1, ReadyReplicas: 1, ServingReplicas: 1, AvailableReplicas: 1,
			CurrentRevision: "chat-engine-a", UpdateRevision: "chat-engine-b",
			InstanceStatuses: []omev1beta1.OMENativeInstanceStatus{{
				Index: 2, Incarnation: 7, Phase: omev1beta1.OMENativeInstanceUpdating,
				RunningRevision: "chat-engine-a", TargetRevision: "chat-engine-b",
				PodCount: 1, ServingPodCount: 1, AvailablePodCount: 1, Admitted: true,
			}},
		},
	}
	return instancestatusprojection.Input{
		InferenceService: isvc, Collection: instancecollection.Result{Items: []omev1beta1.InferenceReplica{ir}},
		Component: omev1beta1.EngineComponent, Index: 2,
		Pods:   observation.Collection[corev1.Pod]{Items: []corev1.Pod{}},
		Events: observation.EventCollection{Items: []corev1.Event{}, Failures: []observation.SourceFailure{}},
	}
}

func statusLimits() instancestatusprojection.Limits {
	return instancestatusprojection.Limits{MaxInstances: 100, MaxPods: 8, MaxContainerStatuses: 16, MaxPodConditions: 16, MaxEvents: 32}
}

func fixedClock() reportv1alpha1.Clock {
	return reportv1alpha1.ClockFunc(func() time.Time { return time.Date(2026, 9, 14, 22, 0, 0, 0, time.UTC) })
}

func statusPod(isvc *omev1beta1.InferenceService, ir *omev1beta1.InferenceReplica, name, runner string, ready, serving bool) corev1.Pod {
	controller := true
	conditions := []corev1.PodCondition{
		{Type: corev1.PodReady, Status: boolCondition(ready)},
		{Type: query.ServingConditionType, Status: boolCondition(serving)},
	}
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: isvc.Namespace, UID: types.UID("uid-" + name),
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: isvc.Name, constants.OMEComponentLabel: string(ir.Spec.Component),
				query.LabelManagedBy: query.ManagedByOMENative, query.LabelInstanceIdx: "2",
				query.LabelInstanceIncarnation: "7", query.LabelRunner: runner, query.LabelRevisionHash: "a1b2c3",
			},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: omev1beta1.SchemeGroupVersion.String(), Kind: "InferenceReplica", Name: ir.Name, UID: ir.UID, Controller: &controller}},
		},
		Spec:   corev1.PodSpec{NodeName: "node-a"},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: conditions},
	}
}

func warningEvent(name, kind, target string, uid types.UID, reason string) corev1.Event {
	return corev1.Event{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "prod", UID: types.UID("uid-" + name)}, Type: corev1.EventTypeWarning, Reason: reason, Count: 1, InvolvedObject: corev1.ObjectReference{APIVersion: "v1", Kind: kind, Namespace: "prod", Name: target, UID: uid}}
}

func boolCondition(value bool) corev1.ConditionStatus {
	if value {
		return corev1.ConditionTrue
	}
	return corev1.ConditionFalse
}

func issueCodes(report reportv1alpha1.InstanceStatusReport) []reportv1alpha1.InstanceStatusIssueCode {
	codes := make([]reportv1alpha1.InstanceStatusIssueCode, len(report.Content.Issues))
	for i := range report.Content.Issues {
		codes[i] = report.Content.Issues[i].Code
	}
	return codes
}

func warningCodes(report reportv1alpha1.InstanceStatusReport) []reportv1alpha1.WarningCode {
	codes := make([]reportv1alpha1.WarningCode, len(report.Warnings))
	for i := range report.Warnings {
		codes[i] = report.Warnings[i].Code
	}
	return codes
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	return strings.TrimSpace(string(mustMarshalJSON(t, value)))
}

func mustMarshalJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	require.NoError(t, err)
	return data
}

func slicesReverse[T any](values []T) {
	for left, right := 0, len(values)-1; left < right; left, right = left+1, right-1 {
		values[left], values[right] = values[right], values[left]
	}
}

func podsUID(pods []corev1.Pod, name string) types.UID {
	for _, pod := range pods {
		if pod.Name == name {
			return pod.UID
		}
	}
	return ""
}
