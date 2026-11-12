package pod

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"sync"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/ome/pkg/constants"
)

// +kubebuilder:webhook:path=/mutate-pods,mutating=true,failurePolicy=fail,groups="",resources=pods,verbs=create,versions=v1,name=inferenceservice.ome-webhook-server.pod-mutator,reinvocationPolicy=IfNeeded
// +kubebuilder:webhook:path=/mutate-training-pods,mutating=true,failurePolicy=fail,groups="",resources=pods,verbs=create,versions=v1,name=trainingjob.ome-webhook-server.pod-mutator,reinvocationPolicy=IfNeeded
var log = logf.Log.WithName(constants.PodMutatorWebhookName)

type Mutator struct {
	Client    client.Client
	Clientset kubernetes.Interface
	Decoder   admission.Decoder
	// reported holds the ConfigMap blocks already logged as absent or
	// incomplete, so each is reported once per process, not once per pod.
	reported sync.Map
}

func (mutator *Mutator) Handle(ctx context.Context, req admission.Request) admission.Response {
	pod := &v1.Pod{}
	if err := mutator.Decoder.Decode(req, pod); err != nil {
		log.Error(err, "Failed to decode pod", "namespace", req.Namespace, "name", req.Name)
		return admission.Errored(http.StatusBadRequest, err)
	}

	if !needMutate(pod) {
		return admission.ValidationResponse(true, "")
	}

	configMap, err := mutator.Clientset.CoreV1().ConfigMaps(constants.OMENamespace).Get(ctx,
		constants.InferenceServiceConfigMapName, metav1.GetOptions{})
	if err != nil {
		log.Error(err, "Failed to find config map", "name", constants.InferenceServiceConfigMapName)
		return admission.Errored(http.StatusInternalServerError, err)
	}

	// Diff against the decoded pod, not the raw request. Decode drops any field this
	// build's k8s.io/api doesn't know; a raw diff would then patch those fields away.
	// Snapshot before the namespace backfill so that op stays in the patch.
	original, err := json.Marshal(pod)
	if err != nil {
		log.Error(err, "Failed to marshal pod", "namespace", req.AdmissionRequest.Namespace, "name", getPodName(pod))
		return admission.Errored(http.StatusInternalServerError, err)
	}

	// Pod namespace is empty in the admission body; backfill from the
	// request so downstream injectors and the controller's pod-ownership
	// queries see the right value.
	pod.Namespace = req.AdmissionRequest.Namespace

	if err := mutator.mutate(pod, configMap); err != nil {
		log.Error(err, "Failed to mutate pod", "namespace", pod.Namespace, "name", getPodName(pod))
		return admission.Errored(http.StatusInternalServerError, err)
	}

	patch, err := json.Marshal(pod)
	if err != nil {
		log.Error(err, "Failed to marshal pod", "namespace", pod.Namespace, "name", getPodName(pod))
		return admission.Errored(http.StatusInternalServerError, err)
	}
	return admission.PatchResponseFromRaw(original, patch)
}

func (mutator *Mutator) mutate(pod *v1.Pod, configMap *v1.ConfigMap) error {

	modelInitInjector, err := newModelInitInjector(configMap)
	if err != nil {
		return err
	}

	metricsAggregator, err := newMetricsAggregator(configMap)
	if err != nil {
		return err
	}
	if !metricsAggregator.portsConfigured() {
		mutator.reportOnce(MetricsAggregatorConfigMapKeyName,
			"metrics aggregation and scraping inject nothing until containerPort, path, aggregatePort and podPrometheusPort are set")
	}

	fineTunedAdapterInjector, err := newFineTunedAdapterInjector(configMap, mutator.Client)
	if err != nil {
		return err
	}

	servingSidecarInjector, err := newServingSidecarInjector(configMap)
	if err != nil {
		return err
	}

	rdma, err := newRDMAConfig(configMap)
	if err != nil {
		return err
	}
	if !rdma.hasProfiles() {
		mutator.reportOnce(RDMAConfigKeyName,
			"pods that ask for RDMA injection get nothing until rdma.profiles names a profile; a chart install ships none, the cluster's values file carries its fabric table")
	}

	runtimeProfiles, err := newRuntimeProfilesConfig(configMap)
	if err != nil {
		return err
	}
	if runtimeProfiles == nil {
		mutator.reportOnce(RuntimeProfilesConfigKeyName,
			"probe profiles inject nothing, and observability profiles serve only pods that name a port, until it is set")
	}

	mutators := []func(pod *v1.Pod) error{
		modelInitInjector.InjectModelInit,
		metricsAggregator.InjectMetricsAggregator,
		fineTunedAdapterInjector.InjectFineTunedAdapter,
		servingSidecarInjector.InjectServingSidecar,
		func(pod *v1.Pod) error { return injectRDMA(pod, rdma) },
		injectShm,
		func(pod *v1.Pod) error { return injectProbes(pod, runtimeProfiles) },
		func(pod *v1.Pod) error { return injectObservability(pod, runtimeProfiles) },
	}

	for _, mutator := range mutators {
		if err := mutator(pod); err != nil {
			return err
		}
	}

	// The model-init container must run before the fine-tuned adapter
	// container; keep every other init container in its original order.
	sort.SliceStable(pod.Spec.InitContainers, func(i, j int) bool {
		if pod.Spec.InitContainers[i].Name == constants.ModelInitContainerName && pod.Spec.InitContainers[j].Name == constants.FineTunedAdapterContainerName {
			return true
		}
		if pod.Spec.InitContainers[i].Name == constants.FineTunedAdapterContainerName && pod.Spec.InitContainers[j].Name == constants.ModelInitContainerName {
			return false
		}
		return i < j
	})

	return nil
}

// reportOnce logs, once per process, that a block of the ConfigMap is
// absent or incomplete and what the webhook leaves out because of it. It
// reports whether this call wrote the line.
func (mutator *Mutator) reportOnce(block, effect string) bool {
	if _, seen := mutator.reported.LoadOrStore(block, struct{}{}); seen {
		return false
	}
	log.Info("inferenceservice-config block absent or incomplete", "block", block, "effect", effect)
	return true
}

func getPodName(pod *v1.Pod) string {
	var podName string
	_, ok := pod.Labels[constants.TrainingJobPodLabelKey]
	if ok {
		podName = pod.Labels[constants.TrainingJobPodLabelKey]
	} else {
		podName = pod.Labels[constants.InferenceServicePodLabelKey]
	}
	return podName
}

// needMutate reports whether the pod is owned by an InferenceService or
// a TrainingJob — only those carry the labels the injectors key off, so
// any other pod passes through unchanged.
func needMutate(pod *v1.Pod) bool {
	_, inferencePodLabel := pod.Labels[constants.InferenceServicePodLabelKey]
	_, trainingPodLabel := pod.Labels[constants.TrainingJobPodLabelKey]
	return inferencePodLabel || trainingPodLabel
}
