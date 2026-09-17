// Package status implements `kubectl ome status`: the full readiness story
// of one InferenceService in a single view.
package status

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/acceleratorprojection"
	"sigs.k8s.io/ome/pkg/cli/apierror"
	"sigs.k8s.io/ome/pkg/cli/effective"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/namespace"
	"sigs.k8s.io/ome/pkg/cli/observation"
	"sigs.k8s.io/ome/pkg/cli/paging"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	omeclient "sigs.k8s.io/ome/pkg/client/clientset/versioned"
	"sigs.k8s.io/ome/pkg/constants"
)

const (
	maxStatusPods                    = 1000
	maxStatusEventsPerTarget         = 25
	maxStatusEventTargets            = 16
	maxStatusEvents                  = 100
	maxConcurrentStatusEventRequests = 8
)

type gatherLimits struct {
	pods      paging.Limits
	events    observation.EventLimits
	maxEvents int
}

var errStatusCancelled = errors.New("status read was cancelled or timed out")

// gatherTyped is the command's bounded read-only acquisition path. Legacy pure
// gather/render helpers remain available to their existing compatibility tests.
func gatherTyped(ctx context.Context, f factory.Factory, ns, name string) (*report, error) {
	return gatherTypedWithOMENamespace(ctx, f, ns, name, namespace.DefaultOMENamespace)
}

func gatherTypedWithOMENamespace(ctx context.Context, f factory.Factory, ns, name, omeNamespace string) (*report, error) {
	if ctx.Err() != nil {
		return nil, errStatusCancelled
	}
	var ome omeclient.Interface
	var kube kubernetes.Interface
	var err error
	owned, actionOwned := f.(factory.ActionReadClientsResolver)
	if actionOwned {
		ome, err = owned.OMEClientForAction(ctx)
	} else {
		ome, err = f.OMEClient()
	}
	if err != nil || ome == nil {
		if ctx.Err() != nil {
			return nil, errStatusCancelled
		}
		return nil, errors.New("status configuration is unavailable")
	}
	requestCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	v, err := ome.OmeV1beta1().InferenceServices(ns).Get(requestCtx, name, metav1.GetOptions{})
	requestErr := requestCtx.Err()
	cancel()
	if ctx.Err() != nil || requestErr != nil {
		return nil, errStatusCancelled
	}
	if err != nil {
		if kerrors.IsNotFound(err) {
			return nil, errors.New("status InferenceService was not found")
		}
		return nil, errors.New("status InferenceService could not be read")
	}
	if v == nil || v.Name != name || v.Namespace != ns || !statusPrivateIdentity(v.Name, string(v.UID)) || v.ResourceVersion == "" || len(v.ResourceVersion) > 253 {
		return nil, errStatusSource
	}
	if actionOwned {
		kube, err = owned.KubeClientForAction(ctx)
	} else {
		kube, err = f.KubeClient()
	}
	if err != nil || kube == nil {
		if ctx.Err() != nil {
			return nil, errStatusCancelled
		}
		return nil, errors.New("status configuration is unavailable")
	}
	limits := defaultGatherLimits()
	r := &report{ISVC: v, Pods: map[v1beta1.ComponentType][]corev1.Pod{}, PodObservation: reportv1alpha1.StatusCollection{State: "Reported"}, EventObservation: reportv1alpha1.StatusCollection{State: "Reported"}}
	pods, podErr := observation.CollectPods(ctx, kube.CoreV1(), ns, constants.InferenceServiceLabel+"="+name, limits.pods)
	if ctx.Err() != nil {
		return nil, errStatusCancelled
	}
	r.PodObservation.Observed = len(pods.Items)
	r.PodObservation.Truncated = pods.Truncated
	if podErr != nil {
		r.PodObservation.State = "Unavailable"
		if len(pods.Items) > 0 {
			r.PodObservation.State = "Partial"
		}
		r.PodObservation.Reason = reportv1alpha1.StatusSourceReason(warningEventFailureReason(podErr))
		r.PodObservation.Truncated = len(pods.Items) > 0 || pods.Truncated
	}
	if pods.Truncated && r.PodObservation.State == "Reported" {
		r.PodObservation.State = "Partial"
	}
	for _, p := range pods.Items {
		component := v1beta1.ComponentType(p.Labels[constants.OMEComponentLabel])
		if component != v1beta1.EngineComponent && component != v1beta1.DecoderComponent && component != v1beta1.RouterComponent {
			r.PodIssues = append(r.PodIssues, "UnsupportedComponent")
			continue
		}
		r.Pods[component] = append(r.Pods[component], p)
	}
	accepted, podIssues := acceptedStatusPods(r)
	r.PodIssues = append(r.PodIssues, podIssues...)
	if len(r.PodIssues) > 0 {
		r.PodObservation.State = "Partial"
		if r.PodObservation.Reason == "" {
			r.PodObservation.Reason = "MalformedPayload"
		}
	}
	targets, skipped := statusEventTargets(ns, name, v, accepted, limits.events.MaxTargets)
	events, eventErr := observation.CollectWarningEvents(ctx, kube.CoreV1(), targets, limits.events)
	if ctx.Err() != nil {
		return nil, errStatusCancelled
	}
	r.EventObservation.Truncated = events.Truncated || skipped > 0 || len(events.Items) > limits.maxEvents
	r.EventObservation.SkippedTargets = min(events.SkippedTargets+skipped, maxStatusPods)
	if eventErr != nil {
		r.EventObservation.State = "Unavailable"
		r.EventObservation.Reason = reportv1alpha1.StatusSourceReason(warningEventFailureReason(eventErr))
	}
	if len(events.Failures) > 0 {
		reasons := []string{}
		for _, failure := range events.Failures {
			reasons = append(reasons, warningEventFailureReason(failure.Err))
		}
		slices.Sort(reasons)
		r.EventObservation.Reason = reportv1alpha1.StatusSourceReason(reasons[0])
		r.EventObservation.State = "Partial"
		if len(events.Failures) == len(targets) {
			r.EventObservation.State = "Unavailable"
		}
	}
	if r.EventObservation.Truncated && r.EventObservation.State == "Reported" {
		r.EventObservation.State = "Partial"
	}
	r.Events = events.Items[:min(len(events.Items), limits.maxEvents)]
	r.EventObservation.Observed = len(r.Events)
	if err := gatherStatusIntegrations(ctx, f, ome, kube, r, omeNamespace); err != nil {
		return nil, err
	}
	return r, nil
}

func defaultGatherLimits() gatherLimits {
	return gatherLimits{
		pods: paging.Limits{
			PageSize:       paging.ChunkSize,
			MaxItems:       maxStatusPods,
			MaxPages:       2,
			RequestTimeout: 10 * time.Second,
		},
		events: observation.EventLimits{
			Paging: paging.Limits{
				PageSize:       maxStatusEventsPerTarget,
				MaxItems:       maxStatusEventsPerTarget,
				MaxPages:       1,
				RequestTimeout: 5 * time.Second,
			},
			MaxTargets:    maxStatusEventTargets,
			MaxConcurrent: maxConcurrentStatusEventRequests,
		},
		maxEvents: maxStatusEvents,
	}
}

type report struct {
	ISVC               *v1beta1.InferenceService
	RuntimeState       *effective.RuntimeState
	RuntimeReason      reportv1alpha1.StatusSummaryReason
	AcceleratorBase    *effective.AcceleratorBaseResolution
	AcceleratorClasses map[string]acceleratorprojection.AcceleratorClassEvidence
	AcceleratorReason  reportv1alpha1.StatusSummaryReason
	Pods               map[v1beta1.ComponentType][]corev1.Pod
	Events             []corev1.Event
	Warnings           []string
	PodObservation     reportv1alpha1.StatusCollection
	EventObservation   reportv1alpha1.StatusCollection
	PodIssues          []reportv1alpha1.StatusIssueCode
}

func gather(ctx context.Context, f factory.Factory, ns, name string) (*report, error) {
	return gatherWithLimits(ctx, f, ns, name, defaultGatherLimits())
}

func gatherWithLimits(ctx context.Context, f factory.Factory, ns, name string, limits gatherLimits) (*report, error) {
	if limits.maxEvents <= 0 {
		return nil, fmt.Errorf("event output limit must be positive")
	}
	ome, err := f.OMEClient()
	if err != nil {
		return nil, err
	}
	isvc, err := ome.OmeV1beta1().InferenceServices(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, apierror.Friendly(err)
	}
	kube, err := f.KubeClient()
	if err != nil {
		return nil, err
	}
	podSelector := fmt.Sprintf("%s=%s", constants.InferenceServiceLabel, name)
	podCollection, err := observation.CollectPods(ctx, kube.CoreV1(), ns, podSelector, limits.pods)
	if err != nil {
		return nil, err
	}
	pods := podCollection.Items

	r := &report{ISVC: isvc, Pods: map[v1beta1.ComponentType][]corev1.Pod{}}
	if podCollection.Truncated {
		r.Warnings = append(r.Warnings, fmt.Sprintf(
			"Pod observation truncated; collected pods: %d; component details may be incomplete",
			len(pods),
		))
	}
	for _, p := range pods {
		component := v1beta1.ComponentType(p.Labels[constants.OMEComponentLabel])
		r.Pods[component] = append(r.Pods[component], p)
	}

	targets, skippedPodTargets := statusEventTargets(ns, name, isvc, pods, limits.events.MaxTargets)
	eventCollection, err := observation.CollectWarningEvents(ctx, kube.CoreV1(), targets, limits.events)
	if err != nil {
		return nil, err
	}
	for _, failure := range eventCollection.Failures {
		r.Warnings = append(r.Warnings, fmt.Sprintf(
			"Warning Events unavailable for %s %s/%s (%s)",
			failure.Target.Kind,
			failure.Target.Namespace,
			failure.Target.Name,
			warningEventFailureReason(failure.Err),
		))
	}
	if eventCollection.Truncated || skippedPodTargets > 0 {
		warning := "Warning Event observation truncated"
		skippedTargets := eventCollection.SkippedTargets + skippedPodTargets
		if skippedTargets > 0 {
			warning += fmt.Sprintf("; object targets not queried: %d", skippedTargets)
		}
		if omitted := len(eventCollection.Items) - limits.maxEvents; omitted > 0 {
			warning += fmt.Sprintf("; events not shown: %d", omitted)
		}
		r.Warnings = append(r.Warnings, warning+"; recent events may be incomplete")
	}
	r.Events = eventCollection.Items
	if len(r.Events) > limits.maxEvents {
		r.Events = r.Events[:limits.maxEvents]
		if !eventCollection.Truncated && skippedPodTargets == 0 {
			r.Warnings = append(r.Warnings, fmt.Sprintf(
				"Warning Event observation truncated; events not shown: %d; recent events may be incomplete",
				len(eventCollection.Items)-limits.maxEvents,
			))
		}
	}
	return r, nil
}

func statusEventTargets(
	namespace string,
	name string,
	isvc *v1beta1.InferenceService,
	pods []corev1.Pod,
	maxTargets int,
) ([]observation.ObjectRef, int) {
	targets := []observation.ObjectRef{{
		Namespace: namespace,
		Kind:      "InferenceService",
		Name:      name,
		UID:       isvc.UID,
	}}
	orderedPods := append([]corev1.Pod{}, pods...)
	sort.SliceStable(orderedPods, func(i, j int) bool {
		leftNeedsEvents := statusPodNeedsEventObservation(orderedPods[i])
		rightNeedsEvents := statusPodNeedsEventObservation(orderedPods[j])
		if leftNeedsEvents != rightNeedsEvents {
			return leftNeedsEvents
		}
		if orderedPods[i].Namespace != orderedPods[j].Namespace {
			return orderedPods[i].Namespace < orderedPods[j].Namespace
		}
		if orderedPods[i].Name != orderedPods[j].Name {
			return orderedPods[i].Name < orderedPods[j].Name
		}
		return orderedPods[i].UID < orderedPods[j].UID
	})
	podLimit := max(maxTargets-1, 0)
	if len(orderedPods) > podLimit {
		orderedPods = orderedPods[:podLimit]
	}
	for _, pod := range orderedPods {
		targets = append(targets, observation.ObjectRef{
			Namespace: namespace,
			Kind:      "Pod",
			Name:      pod.Name,
			UID:       pod.UID,
		})
	}
	return targets, len(pods) - len(orderedPods)
}

func statusPodNeedsEventObservation(pod corev1.Pod) bool {
	if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
		return true
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status != corev1.ConditionTrue
		}
	}
	return true
}

func warningEventFailureReason(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded),
		kerrors.IsTimeout(err),
		kerrors.IsServerTimeout(err):
		return "Timeout"
	case kerrors.IsForbidden(err):
		return "Forbidden"
	case kerrors.IsUnauthorized(err):
		return "Unauthorized"
	case kerrors.IsNotFound(err):
		return "NotFound"
	case kerrors.IsTooManyRequests(err), kerrors.IsServiceUnavailable(err):
		return "Unavailable"
	default:
		return "Unreadable"
	}
}
