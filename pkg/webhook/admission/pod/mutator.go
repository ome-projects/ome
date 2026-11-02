package pod

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"

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

	fineTunedAdapterInjector, err := newFineTunedAdapterInjector(configMap, mutator.Client)
	if err != nil {
		return err
	}

	servingSidecarInjector, err := newServingSidecarInjector(configMap)
	if err != nil {
		return err
	}

	mutators := []func(pod *v1.Pod) error{
		modelInitInjector.InjectModelInit,
		metricsAggregator.InjectMetricsAggregator,
		fineTunedAdapterInjector.InjectFineTunedAdapter,
		servingSidecarInjector.InjectServingSidecar,
		injectRDMA,
		injectShm,
		injectProbes,
		injectObservability,
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
