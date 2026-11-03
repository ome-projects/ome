package pod

import (
	"encoding/json"
	"fmt"
	"path/filepath"

	v1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/constants"
)

const (
	servingSidecarConfigMapKeyName = "servingSidecar"
)

type ServingSidecarInjector struct {
	Image                string `json:"image" validate:"required"`
	MemoryRequest        string `json:"memoryRequest"`
	MemoryLimit          string `json:"memoryLimit"`
	CpuRequest           string `json:"cpuRequest"`
	CpuLimit             string `json:"cpuLimit"`
	CompartmentId        string `json:"compartmentId" validate:"required"`
	AuthType             string `json:"authType" validate:"required"`
	Region               string `json:"region"`
	RealmDomainComponent string `json:"realmDomainComponent"`
}

func newServingSidecarInjector(configMap *v1.ConfigMap) (*ServingSidecarInjector, error) {
	servingSidecarInjector := &ServingSidecarInjector{}
	if servingSidecarConfigVal, ok := configMap.Data[servingSidecarConfigMapKeyName]; ok {
		if err := json.Unmarshal([]byte(servingSidecarConfigVal), servingSidecarInjector); err != nil {
			return nil, fmt.Errorf("unable to unmarshal %v json string: %w", servingSidecarConfigMapKeyName, err)
		}
	}
	return servingSidecarInjector, nil
}

// InjectServingSidecar injects the serving sidecar container into the
// pod when ome.io/enable-serving-sidecar=true.
func (ss *ServingSidecarInjector) InjectServingSidecar(pod *v1.Pod) error {
	if enableServingSidecar, ok := pod.ObjectMeta.Annotations[constants.ServingSidecarInjectionKey]; ok && enableServingSidecar == "true" {
		return ss.injectServingSidecar(pod)
	}
	return nil
}

func (ss *ServingSidecarInjector) injectServingSidecar(pod *v1.Pod) error {
	if findContainer(pod.Spec.Containers, constants.ServingSidecarContainerName) != nil {
		return nil
	}
	if err := requireConfigured(ss, "ServingSidecarInjector"); err != nil {
		return err
	}
	if err := requireWorkloadIdentityServiceAccount(pod, ss.AuthType); err != nil {
		return err
	}

	fineTunedWeightFTStrategy, err := ss.getFineTunedWeightFTStrategy(pod)
	if err != nil {
		return err
	}
	securityContext, err := mainContainerSecurityContext(pod)
	if err != nil {
		return err
	}

	mounts := ss.getVolumeMounts(pod, fineTunedWeightFTStrategy)
	envs := ss.getServingSidecarEnvs(fineTunedWeightFTStrategy)
	sidecarContainer, err := agentContainer(ss.agentContainerSpec(), envs, mounts, securityContext)
	if err != nil {
		return err
	}
	pod.Spec.Containers = append(pod.Spec.Containers, *sidecarContainer)
	return nil
}

func (ss *ServingSidecarInjector) getFineTunedWeightFTStrategy(pod *v1.Pod) (string, error) {
	if fineTunedWeightFTStrategy, ok := pod.ObjectMeta.Annotations[constants.FineTunedWeightFTStrategyKey]; ok {
		return fineTunedWeightFTStrategy, nil
	}
	return "", fmt.Errorf("failed to get the fine-tuned weight FT strategy for the serving sidecar")
}

func (ss *ServingSidecarInjector) getVolumeMounts(pod *v1.Pod, fineTunedWeightFTStrategy string) []v1.VolumeMount {
	servingSidecarMounts := []v1.VolumeMount{}

	fineTunedWeightMountPath := filepath.Join(constants.ModelDefaultMountPathPrefix, fineTunedWeightFTStrategy)
	fineTunedWeightVolumeMount := v1.VolumeMount{
		Name:      constants.ModelEmptyDirVolumeName,
		MountPath: fineTunedWeightMountPath,
		ReadOnly:  false,
		SubPath:   constants.FineTunedWeightVolumeMountSubPath,
	}
	fineTunedWeightDownloadMount := v1.VolumeMount{
		Name:      constants.ModelEmptyDirVolumeName,
		MountPath: constants.FineTunedWeightDownloadMountPath,
		ReadOnly:  false,
		SubPath:   constants.FineTunedWeightDownloadVolumeMountSubPath,
	}

	servingSidecarMounts = append(servingSidecarMounts, fineTunedWeightDownloadMount)
	servingSidecarMounts = append(servingSidecarMounts, fineTunedWeightVolumeMount)
	return servingSidecarMounts
}

func (ss *ServingSidecarInjector) getServingSidecarEnvs(fineTunedWeightFTStrategy string) []v1.EnvVar {
	envVars := []v1.EnvVar{
		{Name: constants.AgentAuthTypeEnvVarKey, Value: ss.AuthType},
		{Name: constants.AgentCompartmentIDEnvVarKey, Value: ss.CompartmentId},
		{Name: constants.AgentRegionEnvVarKey, Value: ss.Region},
		// The weight-info file path is intentionally not injected: the agent
		// reads it from its own config file, and an env var here would
		// override that value for every pod. The directory envs below are
		// injected because they must match the mounts created above.
		{Name: constants.AgentUnzippedFineTunedWeightDirectory, Value: filepath.Join(constants.ModelDefaultMountPathPrefix, fineTunedWeightFTStrategy)},
		{Name: constants.AgentZippedFineTunedWeightDirectory, Value: constants.FineTunedWeightDownloadMountPath},
	}

	return envVars
}

// agentContainerSpec is the sidecar that serves the weight: ome-agent's
// serving-agent subcommand with this block's image and resources.
func (ss *ServingSidecarInjector) agentContainerSpec() agentContainerSpec {
	return agentContainerSpec{
		injector:      "serving sidecar injector",
		name:          constants.ServingSidecarContainerName,
		subcommand:    "serving-agent",
		image:         ss.Image,
		cpuRequest:    ss.CpuRequest,
		cpuLimit:      ss.CpuLimit,
		memoryRequest: ss.MemoryRequest,
		memoryLimit:   ss.MemoryLimit,
	}
}
