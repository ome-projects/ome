package runtimeprojection

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	knapis "knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/effective"
	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestProjectionRenderingIsIndependentOfHistoryListOrder(t *testing.T) {
	liveSpec := projectionRuntimeSpec("private.registry/live:secret")
	active := projectionRevision(t, "active-uid", "gpu-runtime", liveSpec)
	active.CreationTimestamp = metav1.NewTime(time.Date(2026, time.August, 31, 19, 0, 0, 0, time.UTC))
	older := projectionRevision(t, "older-uid", "gpu-runtime", projectionRuntimeSpec("private.registry/older:secret"))
	older.CreationTimestamp = metav1.NewTime(time.Date(2026, time.August, 30, 19, 0, 0, 0, time.UTC))

	firstClient := projectionClientWithHistoryOrder(active, older, []appsv1.ControllerRevision{
		*active.DeepCopy(), *active.DeepCopy(), *older.DeepCopy(),
	})
	secondClient := projectionClientWithHistoryOrder(active, older, []appsv1.ControllerRevision{
		*older.DeepCopy(), *active.DeepCopy(), *active.DeepCopy(),
	})

	firstISVC, firstState := resolveProjectionWithRevisionClient(t, firstClient, liveSpec, active.Name, nil)
	secondISVC, secondState := resolveProjectionWithRevisionClient(t, secondClient, liveSpec, active.Name, nil)
	firstEffective, err := ProjectEffective(firstISVC, firstState, projectionTestClock)
	require.NoError(t, err)
	secondEffective, err := ProjectEffective(secondISVC, secondState, projectionTestClock)
	require.NoError(t, err)
	firstHistory, err := ProjectHistory(firstISVC, firstState, projectionTestClock)
	require.NoError(t, err)
	secondHistory, err := ProjectHistory(secondISVC, secondState, projectionTestClock)
	require.NoError(t, err)

	for _, format := range []report.Format{report.FormatTable, report.FormatJSON, report.FormatYAML} {
		assert.Equal(t, renderProjectionDocument(t, format, firstEffective), renderProjectionDocument(t, format, secondEffective))
		assert.Equal(t, renderProjectionDocument(t, format, firstHistory), renderProjectionDocument(t, format, secondHistory))
	}
}

func TestRuntimeProjectionDoesNotLeakOrMutatePrivateEvidence(t *testing.T) {
	isvc, state, canaries := resolveProjectionLeakCanaryFixture(t)
	isvcBefore := isvc.DeepCopy()
	stateBefore := snapshotProjectionState(state)

	effectiveReport, err := ProjectEffective(isvc, state, projectionTestClock)
	require.NoError(t, err)
	historyReport, err := ProjectHistory(isvc, state, projectionTestClock)
	require.NoError(t, err)

	require.Equal(t, isvcBefore, isvc)
	require.Equal(t, stateBefore, snapshotProjectionState(state), "projection mutated collector evidence")
	require.NotEmpty(t, historyReport.Content.Revisions)
	require.Contains(t, historyReport.Content.Issues, reportv1alpha1.RuntimeIssue{
		Code: reportv1alpha1.RuntimeIssueHistoryUnavailable,
	})
	require.Contains(t, historyReport.Warnings, reportv1alpha1.RuntimeWarning{Code: reportv1alpha1.WarningPartialData})
	require.Contains(t, historyReport.Warnings, reportv1alpha1.RuntimeWarning{Code: reportv1alpha1.WarningSourceUnavailable})

	for _, format := range []report.Format{report.FormatTable, report.FormatJSON, report.FormatYAML} {
		outputs := []string{
			renderProjectionDocument(t, format, effectiveReport),
			renderProjectionDocument(t, format, historyReport),
		}
		for _, output := range outputs {
			for _, canary := range canaries {
				assert.NotContains(t, output, canary, "private evidence leaked in %s output", format)
			}
		}
	}
	for _, format := range []report.Format{report.FormatJSON, report.FormatYAML} {
		combined := renderProjectionDocument(t, format, effectiveReport) + renderProjectionDocument(t, format, historyReport)
		for _, forbiddenField := range []string{
			"resourceVersion", "lastRuntimeSyncToken", "annotations", "labels", "continue", "message",
		} {
			assert.NotContains(t, strings.ToLower(combined), strings.ToLower(forbiddenField))
		}
	}

	baselineEffective, err := ProjectEffective(isvc, state, projectionTestClock)
	require.NoError(t, err)
	baselineHistory, err := ProjectHistory(isvc, state, projectionTestClock)
	require.NoError(t, err)
	effectiveReport.Sources[0].Name = "mutated-output"
	effectiveReport.Content.Inheritance.Sources[0].Name = "mutated-output"
	if effectiveReport.Content.Active.Revision != nil {
		effectiveReport.Content.Active.Revision.Name = "mutated-output"
	}
	historyReport.Sources[0].Name = "mutated-output"
	historyReport.Content.Revisions[0].Roles[0] = reportv1alpha1.RuntimeRevisionRole("mutated-output")
	if historyReport.Content.Revisions[0].Revision.CreatedAt != nil {
		*historyReport.Content.Revisions[0].Revision.CreatedAt = time.Time{}
	}

	afterEffective, err := ProjectEffective(isvc, state, projectionTestClock)
	require.NoError(t, err)
	afterHistory, err := ProjectHistory(isvc, state, projectionTestClock)
	require.NoError(t, err)
	assert.Equal(t, baselineEffective, afterEffective, "mutating projected output must not alias collector input")
	assert.Equal(t, baselineHistory, afterHistory, "mutating projected output must not alias collector input")
}

func projectionClientWithHistoryOrder(
	active, older *appsv1.ControllerRevision,
	items []appsv1.ControllerRevision,
) *k8sfake.Clientset {
	clientset := k8sfake.NewSimpleClientset(active.DeepCopy(), older.DeepCopy())
	clientset.PrependReactor("list", "controllerrevisions", func(k8stesting.Action) (bool, runtime.Object, error) {
		copied := make([]appsv1.ControllerRevision, len(items))
		for i := range items {
			copied[i] = *items[i].DeepCopy()
		}
		return true, &appsv1.ControllerRevisionList{Items: copied}, nil
	})
	return clientset
}

func resolveProjectionLeakCanaryFixture(
	t *testing.T,
) (*v1beta1.InferenceService, *effective.RuntimeState, []string) {
	t.Helper()
	const (
		isvcResourceVersion = "CANARY_ISVC_RESOURCE_VERSION"
		isvcAnnotationValue = "CANARY_ISVC_ANNOTATION_VALUE"
		syncToken           = "CANARY_SYNC_TOKEN"
		conditionReason     = "CANARY_CONDITION_REASON"
		conditionMessage    = "CANARY_CONDITION_MESSAGE"
		runtimeResourceVer  = "CANARY_RUNTIME_RESOURCE_VERSION"
		runtimeLabelValue   = "CANARY_RUNTIME_LABEL_VALUE"
		runtimeAnnotation   = "CANARY_RUNTIME_ANNOTATION_VALUE"
		image               = "CANARY_PRIVATE_IMAGE"
		command             = "CANARY_PRIVATE_COMMAND"
		argument            = "CANARY_PRIVATE_ARGUMENT"
		environmentValue    = "CANARY_PRIVATE_ENV_VALUE"
		secretReference     = "CANARY_SECRET_REFERENCE"
		rawMapKey           = "CANARY_RAW_MAP_KEY"
		rawMapValue         = "CANARY_RAW_MAP_VALUE"
		revisionResourceVer = "CANARY_REVISION_RESOURCE_VERSION"
		revisionLabelValue  = "CANARY_REVISION_LABEL_VALUE"
		revisionAnnotation  = "CANARY_REVISION_ANNOTATION_VALUE"
		continueToken       = "CANARY_CONTINUE_TOKEN"
		listError           = "CANARY_LIST_ERROR"
	)

	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	liveSpec := projectionRuntimeSpec(image)
	liveSpec.EngineConfig.Runner.Command = []string{command}
	liveSpec.EngineConfig.Runner.Args = []string{argument}
	liveSpec.EngineConfig.Runner.Env = []corev1.EnvVar{
		{Name: "PRIVATE_LITERAL", Value: environmentValue},
		{
			Name: "PRIVATE_REFERENCE",
			ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: secretReference}, Key: "token",
			}},
		},
	}
	liveSpec.EngineConfig.Volumes = []corev1.Volume{{
		Name: "private-volume",
		VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
			SecretName: secretReference,
		}},
	}}
	liveSpec.RouterConfig = &v1beta1.RouterSpec{Config: map[string]string{rawMapKey: rawMapValue}}
	runtimeObject := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name: "gpu-runtime", UID: "runtime-uid", Generation: 5, ResourceVersion: runtimeResourceVer,
			Labels:      map[string]string{"private-label": runtimeLabelValue},
			Annotations: map[string]string{"private-annotation": runtimeAnnotation},
		},
		Spec: *liveSpec.DeepCopy(),
	}
	liveClient := ctrlclientfake.NewClientBuilder().WithScheme(scheme).WithObjects(runtimeObject).Build()
	revision := projectionRevision(t, "revision-uid", runtimeObject.Name, liveSpec)
	revision.CreationTimestamp = metav1.NewTime(time.Date(2026, time.August, 31, 19, 0, 0, 0, time.UTC))
	revision.ResourceVersion = revisionResourceVer
	revision.Labels["private-label"] = revisionLabelValue
	revision.Annotations["private-annotation"] = revisionAnnotation
	clientset := k8sfake.NewSimpleClientset(revision.DeepCopy())
	listCalls := 0
	clientset.PrependReactor("list", "controllerrevisions", func(k8stesting.Action) (bool, runtime.Object, error) {
		listCalls++
		if listCalls == 1 {
			return true, &appsv1.ControllerRevisionList{
				ListMeta: metav1.ListMeta{Continue: continueToken},
				Items:    []appsv1.ControllerRevision{*revision.DeepCopy()},
			}, nil
		}
		return true, nil, errors.New(listError)
	})
	autoSync := false
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "chat", Namespace: "workloads", UID: types.UID("isvc-uid"), Generation: 7,
			ResourceVersion: isvcResourceVersion,
			Annotations: map[string]string{
				constants.RuntimeSyncAnnotationKey: syncToken,
				"private-annotation":               isvcAnnotationValue,
			},
		},
		Spec: v1beta1.InferenceServiceSpec{
			Runtime: &v1beta1.ServingRuntimeRef{Name: runtimeObject.Name, AutoSync: &autoSync},
			Engine: &v1beta1.EngineSpec{Runner: &v1beta1.RunnerSpec{Container: corev1.Container{
				Name: "runner", Env: []corev1.EnvVar{{Name: "ISVC_PRIVATE", Value: environmentValue}},
			}}},
		},
	}
	isvc.Status.ObservedGeneration = 7
	isvc.Status.PinnedRevisionName = revision.Name
	isvc.Status.LastRuntimeSyncToken = syncToken
	isvc.Status.Conditions = duckv1.Conditions{{
		Type: knapis.ConditionType(constants.RuntimeDriftedConditionType), Status: corev1.ConditionUnknown,
		Reason: conditionReason, Message: conditionMessage,
	}}
	resolver, err := effective.NewRuntimePinResolver(
		clientset.AppsV1(), effective.NewRuntimeResolver(liveClient), "ome-system",
		paging.Limits{PageSize: 1, MaxItems: 2, MaxPages: 2, RequestTimeout: time.Second},
	)
	require.NoError(t, err)
	state, err := resolver.Resolve(t.Context(), isvc, effective.RuntimeResolveOptions{IncludeHistory: true})
	require.NoError(t, err)
	require.Equal(t, 2, listCalls)

	return isvc, state, []string{
		isvcResourceVersion, isvcAnnotationValue, syncToken, conditionReason, conditionMessage,
		runtimeResourceVer, runtimeLabelValue, runtimeAnnotation, image, command, argument,
		environmentValue, secretReference, rawMapKey, rawMapValue, revisionResourceVer,
		revisionLabelValue, revisionAnnotation, continueToken, listError,
	}
}

type projectionStateSnapshot struct {
	Generation              int64
	ObservedGeneration      int64
	RuntimeName             string
	RuntimeKind             string
	RuntimeNamespace        string
	DeclaredSourceKind      string
	DeclaredSourceNamespace string
	SelectionSource         effective.RuntimeSelectionSource
	PinMode                 effective.RuntimePinMode
	PinState                effective.RuntimePinState
	RequestedRevisionName   string
	ReportedRevisionName    string
	ActiveRevisionName      string
	StatusFreshness         effective.StatusFreshness
	SyncTokenState          effective.SyncTokenState
	DriftState              effective.RuntimeDriftState
	DriftReason             effective.RuntimeDriftReason
	LiveToActive            effective.RuntimeHashRelation
	LiveShortHash           string
	HistoryRequested        bool
	HistoryPages            int
	HistoryPageLimit        int
	HistoryRequestLimit     int
	HistoryRequestedPages   int
	HistoryObservedPages    int
	HistoryComplete         bool
	HistoryTruncated        bool
	HistoryNamespace        string
	LiveAvailability        effective.LiveRuntimeAvailability
	Identity                effective.InferenceServiceIdentity
	Live                    *effective.LiveConfiguration
	Active                  *effective.ActiveConfiguration
	Revisions               []effective.RuntimeRevisionObservation
	Issues                  []effective.RuntimeSourceIssue
}

func snapshotProjectionState(state *effective.RuntimeState) projectionStateSnapshot {
	active, _ := state.RequireActive()
	return projectionStateSnapshot{
		Generation: state.Generation, ObservedGeneration: state.ObservedGeneration,
		RuntimeName: state.RuntimeName, RuntimeKind: state.RuntimeKind, RuntimeNamespace: state.RuntimeNamespace,
		DeclaredSourceKind: state.DeclaredSourceKind, DeclaredSourceNamespace: state.DeclaredSourceNamespace,
		SelectionSource: state.SelectionSource, PinMode: state.PinMode, PinState: state.PinState,
		RequestedRevisionName: state.RequestedRevisionName, ReportedRevisionName: state.ReportedRevisionName,
		ActiveRevisionName: state.ActiveRevisionName, StatusFreshness: state.StatusFreshness,
		SyncTokenState: state.SyncTokenState, DriftState: state.DriftState, DriftReason: state.DriftReason,
		LiveToActive: state.LiveToActive, LiveShortHash: state.LiveShortHash,
		HistoryRequested: state.HistoryRequested, HistoryPages: state.HistoryPages,
		HistoryPageLimit: state.HistoryPageLimit, HistoryRequestLimit: state.HistoryRequestLimit,
		HistoryRequestedPages: state.HistoryRequestedPages,
		HistoryObservedPages:  state.HistoryObservedPages, HistoryComplete: state.HistoryComplete,
		HistoryTruncated: state.HistoryTruncated, HistoryNamespace: state.HistoryNamespace(),
		LiveAvailability: state.LiveAvailability(), Identity: state.InferenceServiceIdentity(),
		Live: state.LiveConfiguration(), Active: active, Revisions: state.RevisionObservations(), Issues: state.SourceIssues(),
	}
}

func renderProjectionDocument[T report.Document[T]](t *testing.T, format report.Format, document T) string {
	t.Helper()
	var output bytes.Buffer
	require.NoError(t, report.Write(&output, format, document))
	return output.String()
}

func setMalformedProjectionDrift(isvc *v1beta1.InferenceService) {
	condition := knapis.Condition{
		Type: knapis.ConditionType(constants.RuntimeDriftedConditionType), Status: corev1.ConditionTrue,
		Reason: string(effective.RuntimeDriftReasonRevisionMismatch),
	}
	isvc.Status.Conditions = duckv1.Conditions{condition, condition}
}
