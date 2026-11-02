package pod

import (
	"encoding/json"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/constants"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"sigs.k8s.io/ome/pkg/utils/storage"
)

const (
	fineTunedAdapterConfigMapKeyName = "fineTunedAdapter"
)

type FineTunedAdapterInjector struct {
	Image         string `json:"image" validate:"required"`
	MemoryRequest string `json:"memoryRequest"`
	MemoryLimit   string `json:"memoryLimit"`
	CpuRequest    string `json:"cpuRequest"`
	CpuLimit      string `json:"cpuLimit"`
	CompartmentId string `json:"compartmentId" validate:"required"`
	AuthType      string `json:"authType" validate:"required"`
	Region        string `json:"region"`
	client        client.Client
}

func newFineTunedAdapterInjector(configMap *v1.ConfigMap, client client.Client) (*FineTunedAdapterInjector, error) {
	fineTunedAdapterInjector := &FineTunedAdapterInjector{}
	if fineTunedAdapterConfigVal, ok := configMap.Data[fineTunedAdapterConfigMapKeyName]; ok {
		if err := json.Unmarshal([]byte(fineTunedAdapterConfigVal), fineTunedAdapterInjector); err != nil {
			return nil, fmt.Errorf("unable to unmarshal %v json string: %w", fineTunedAdapterConfigMapKeyName, err)
		}
	}
	fineTunedAdapterInjector.client = client
	return fineTunedAdapterInjector, nil
}

// InjectFineTunedAdapter injects the fine-tuned weight init container
// into the pod when the ome.io/fine-tuned-adapter-injection annotation
// names a FineTunedWeight to download.
func (fa *FineTunedAdapterInjector) InjectFineTunedAdapter(pod *v1.Pod) error {
	if fineTunedWeightName, ok := pod.ObjectMeta.Annotations[constants.FineTunedAdapterInjectionKey]; ok && len(fineTunedWeightName) > 0 {
		return fa.injectFineTunedAdapter(pod, fineTunedWeightName)
	}
	return nil
}

func (fa *FineTunedAdapterInjector) injectFineTunedAdapter(pod *v1.Pod, fineTunedWeightName string) error {
	if findContainer(pod.Spec.InitContainers, constants.FineTunedAdapterContainerName) != nil {
		return nil
	}
	if err := requireConfigured(fa, "FineTunedAdapterInjector"); err != nil {
		return err
	}
	if err := requireWorkloadIdentityServiceAccount(pod, fa.AuthType); err != nil {
		return err
	}

	fineTunedWeightUri, err := fa.getFineTunedWeightUri(pod, fineTunedWeightName)
	if err != nil {
		return fmt.Errorf("failed to resolve fine-tuned weight URI for %s: %w", fineTunedWeightName, err)
	}
	securityContext, err := mainContainerSecurityContext(pod)
	if err != nil {
		return err
	}

	envs := fa.getModelInitEnvs(pod, fineTunedWeightUri)
	mounts := fa.getVolumeMounts(pod)
	initContainer, err := agentContainer(fa.agentContainerSpec(), envs, mounts, securityContext)
	if err != nil {
		return err
	}
	pod.Spec.InitContainers = append(pod.Spec.InitContainers, *initContainer)
	return nil
}

func (fa *FineTunedAdapterInjector) getVolumeMounts(pod *v1.Pod) []v1.VolumeMount {
	return []v1.VolumeMount{
		{
			Name:      constants.ModelEmptyDirVolumeName,
			MountPath: constants.FineTunedWeightDownloadMountPath,
		},
		{
			Name:      constants.ModelEmptyDirVolumeName,
			MountPath: fa.getFineTunedWeightVolumeMountPath(pod),
			SubPath:   fa.getFineTunedWeightVolumeMountSubPath(pod),
		},
	}
}

// agentContainerSpec is the init container that downloads the weight:
// ome-agent's fine-tuned-adapter subcommand with this block's image and
// resources.
func (fa *FineTunedAdapterInjector) agentContainerSpec() agentContainerSpec {
	return agentContainerSpec{
		injector:      "fine-tuned adapter injector",
		name:          constants.FineTunedAdapterContainerName,
		subcommand:    "fine-tuned-adapter",
		image:         fa.Image,
		cpuRequest:    fa.CpuRequest,
		cpuLimit:      fa.CpuLimit,
		memoryRequest: fa.MemoryRequest,
		memoryLimit:   fa.MemoryLimit,
	}
}

func (fa *FineTunedAdapterInjector) getFineTunedWeightUri(pod *v1.Pod, fineTunedWeightName string) (*storage.OCIStorageComponents, error) {
	fineTunedWeight, err := isvcutils.GetFineTunedWeight(fa.client, fineTunedWeightName)
	if err != nil {
		return nil, err
	}
	if fineTunedWeight.Spec.Storage == nil || fineTunedWeight.Spec.Storage.StorageUri == nil {
		return nil, fmt.Errorf("fine-tuned weight %s has no storage URI", fineTunedWeightName)
	}
	osUri, err := storage.ParseOCIStorageURI(*fineTunedWeight.Spec.Storage.StorageUri)
	if err != nil {
		return nil, err
	}
	// Merged-weight serving downloads a single archive instead of the
	// adapter layout; suffix the object name so the agent fetches it.
	if pod.ObjectMeta.Annotations[constants.FTServingWithMergedWeightsAnnotationKey] == "true" {
		osUri.Prefix = fmt.Sprintf("%s%s", osUri.Prefix, constants.MergedModelWeightZippedFileSuffix)
		osUri.ObjectName = fmt.Sprintf("%s%s", osUri.ObjectName, constants.MergedModelWeightZippedFileSuffix)
	}
	return osUri, nil
}

func (fa *FineTunedAdapterInjector) getModelInitEnvs(pod *v1.Pod, fineTunedWeightUri *storage.OCIStorageComponents) []v1.EnvVar {
	return []v1.EnvVar{
		{Name: constants.AgentAuthTypeEnvVarKey, Value: fa.AuthType},
		{Name: constants.AgentCompartmentIDEnvVarKey, Value: fa.CompartmentId},
		{Name: constants.AgentRegionEnvVarKey, Value: fa.Region},
		{Name: constants.AgentUnzippedFineTunedWeightDirectory, Value: fa.getFineTunedWeightVolumeMountPath(pod)},
		{Name: constants.AgentZippedFineTunedWeightDirectory, Value: constants.FineTunedWeightDownloadMountPath},
		{Name: constants.AgentModelBucketNameEnvVarKey, Value: fineTunedWeightUri.Bucket},
		{Name: constants.AgentModelNamespaceEnvVarKey, Value: fineTunedWeightUri.Namespace},
		{Name: constants.AgentModelObjectName, Value: fineTunedWeightUri.Prefix},
	}
}

func (fa *FineTunedAdapterInjector) getFineTunedWeightVolumeMountPath(pod *v1.Pod) string {
	if isvcutils.IsCohereCommand1TFewFTServing(&pod.ObjectMeta) {
		return constants.CohereTFewFineTunedWeightVolumeMountPath
	} else {
		return constants.ModelDefaultMountPath
	}
}

func (fa *FineTunedAdapterInjector) getFineTunedWeightVolumeMountSubPath(pod *v1.Pod) string {
	if pod.ObjectMeta.Annotations[constants.BaseModelFormat] == constants.TensorRTLLM {
		return constants.TensorRTModelVolumeMountSubPath
	}
	if isvcutils.IsCohereCommand1TFewFTServing(&pod.ObjectMeta) {
		return constants.FineTunedWeightVolumeMountSubPath
	}
	return ""
}
