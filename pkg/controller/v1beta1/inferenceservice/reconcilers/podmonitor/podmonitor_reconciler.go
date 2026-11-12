package podmonitor

import (
	"context"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/constants"
)

var log = logf.Log.WithName("PodMonitorReconciler")

type PodMonitorReconciler struct {
	client     client.Client
	scheme     *runtime.Scheme
	PodMonitor *monitoringv1.PodMonitor
}

func NewPodMonitorReconciler(client client.Client,
	scheme *runtime.Scheme,
	componentMeta metav1.ObjectMeta,
	podSpec *corev1.PodSpec,
) *PodMonitorReconciler {
	return &PodMonitorReconciler{
		client:     client,
		scheme:     scheme,
		PodMonitor: createPodMonitor(componentMeta, podSpec),
	}
}

func metricsPortName(podSpec *corev1.PodSpec) string {
	if podSpec == nil || len(podSpec.Containers) == 0 {
		return "http"
	}
	ports := podSpec.Containers[0].Ports
	for _, port := range ports {
		if port.Name == "metrics" {
			return "metrics"
		}
	}
	if len(ports) > 0 && ports[0].Name != "" {
		return ports[0].Name
	}
	return "http"
}

func createPodMonitor(componentMeta metav1.ObjectMeta, podSpec *corev1.PodSpec) *monitoringv1.PodMonitor {
	appLabel := constants.TruncateNameWithMaxLength(componentMeta.Name, 63)
	portName := metricsPortName(podSpec)

	// Default /metrics endpoint, plus any extras from the annotation.
	endpoints := []monitoringv1.PodMetricsEndpoint{{
		Port:     &portName,
		Path:     "/metrics",
		Interval: "10s",
	}}
	endpoints = append(endpoints, ParseExtraEndpoints(componentMeta.Annotations, "10s")...)

	return &monitoringv1.PodMonitor{
		ObjectMeta: metav1.ObjectMeta{
			Name:      componentMeta.Name,
			Namespace: componentMeta.Namespace,
			Labels: map[string]string{
				"app": appLabel,
			},
		},
		Spec: monitoringv1.PodMonitorSpec{
			NamespaceSelector: monitoringv1.NamespaceSelector{
				MatchNames: []string{componentMeta.Namespace},
			},
			Selector: metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": appLabel,
				},
			},
			PodMetricsEndpoints: endpoints,
		},
	}
}

func (r *PodMonitorReconciler) checkPodMonitorExist() (constants.CheckResultType, *monitoringv1.PodMonitor, error) {
	existing := &monitoringv1.PodMonitor{}
	err := r.client.Get(context.TODO(), types.NamespacedName{
		Namespace: r.PodMonitor.Namespace,
		Name:      r.PodMonitor.Name,
	}, existing)
	if err != nil {
		if apierr.IsNotFound(err) {
			return constants.CheckResultCreate, nil, nil
		}
		log.Info("Failed to get existing PodMonitor", "Namespace", r.PodMonitor.Namespace, "Name", r.PodMonitor.Name)
		return constants.CheckResultUnknown, nil, err
	}

	if semanticPodMonitorEquals(r.PodMonitor, existing) {
		return constants.CheckResultExisted, existing, nil
	}
	return constants.CheckResultUpdate, existing, nil
}

// semanticPodMonitorEquals reports whether the live PodMonitor already matches
// the desired one for every field OME manages. The Prometheus operator defaults
// a number of PodMetricsEndpoint fields (scheme, interval, scrapeTimeout, path,
// honorLabels, etc.) onto the stored object whenever the desired endpoint omits
// them, so the live endpoints are always richer than the minimal ones OME
// builds. Comparing the full endpoint structs would therefore diff on every
// reconcile and trigger a perpetual no-op Update. We compare only OME's managed
// sub-fields rather than re-introducing the operator defaults in code.
func semanticPodMonitorEquals(desired, existing *monitoringv1.PodMonitor) bool {
	return equality.Semantic.DeepEqual(desired.Spec.Selector, existing.Spec.Selector) &&
		equality.Semantic.DeepEqual(desired.Spec.NamespaceSelector, existing.Spec.NamespaceSelector) &&
		equality.Semantic.DeepEqual(managedEndpoints(desired.Spec.PodMetricsEndpoints), managedEndpoints(existing.Spec.PodMetricsEndpoints))
}

// managedEndpoints projects each PodMetricsEndpoint down to only the fields OME
// sets in createPodMonitor (port/portName, path, interval), discarding every
// operator-defaulted field so the equality check ignores them.
func managedEndpoints(endpoints []monitoringv1.PodMetricsEndpoint) []monitoringv1.PodMetricsEndpoint {
	if endpoints == nil {
		return nil
	}
	managed := make([]monitoringv1.PodMetricsEndpoint, len(endpoints))
	for i, ep := range endpoints {
		managed[i] = monitoringv1.PodMetricsEndpoint{
			Port:     ep.Port,
			Path:     ep.Path,
			Interval: ep.Interval,
		}
	}
	return managed
}

func (r *PodMonitorReconciler) Reconcile() (*monitoringv1.PodMonitor, error) {
	checkResult, existing, err := r.checkPodMonitorExist()
	log.V(1).Info("PodMonitor reconcile", "checkResult", checkResult, "err", err)
	if err != nil {
		return nil, err
	}

	switch checkResult {
	case constants.CheckResultCreate:
		err = r.client.Create(context.TODO(), r.PodMonitor)
		if err != nil {
			log.Error(err, "Failed to create PodMonitor", "Namespace", r.PodMonitor.Namespace, "Name", r.PodMonitor.Name)
			return nil, err
		}
		return r.PodMonitor, nil
	case constants.CheckResultUpdate:
		if existing != nil {
			r.PodMonitor.SetResourceVersion(existing.GetResourceVersion())
		}
		err = r.client.Update(context.TODO(), r.PodMonitor)
		if err != nil {
			log.Info("Failed to update PodMonitor", "Namespace", r.PodMonitor.Namespace, "Name", r.PodMonitor.Name)
			return nil, err
		}
		return r.PodMonitor, nil
	default:
		return existing, nil
	}
}
