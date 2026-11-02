package pod

import (
	"fmt"

	"github.com/go-playground/validator/v10"
	v1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/constants"
)

// The fine-tuned adapter injector and the serving sidecar injector each add
// an ome-agent container configured by a block of the inferenceservice-config
// ConfigMap. These are the rules the two share.

// agentContainerSpec says what an injector's ome-agent container is before
// the pod is read: the container's name, the agent subcommand it runs, and
// the image and resources its ConfigMap block names. injector is the
// injector's name as a refusal reads it.
type agentContainerSpec struct {
	injector      string
	name          string
	subcommand    string
	image         string
	cpuRequest    string
	cpuLimit      string
	memoryRequest string
	memoryLimit   string
}

// agentContainer builds the ome-agent container from its spec, the envs and
// mounts the injector prepared, and the main container's security context.
// A missing or malformed resource string is refused.
func agentContainer(spec agentContainerSpec, envs []v1.EnvVar, mounts []v1.VolumeMount, securityContext *v1.SecurityContext) (*v1.Container, error) {
	resources, err := newResourceRequirements(spec.cpuLimit, spec.memoryLimit, spec.cpuRequest, spec.memoryRequest)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", spec.injector, err)
	}
	return &v1.Container{
		Name:                     spec.name,
		Image:                    spec.image,
		TerminationMessagePolicy: v1.TerminationMessageFallbackToLogsOnError,
		Env:                      envs,
		VolumeMounts:             mounts,
		Args:                     []string{spec.subcommand, "--config", "/ome-agent.yaml", "--debug"},
		Resources:                resources,
		SecurityContext:          securityContext,
	}, nil
}

// requireConfigured refuses an injector whose ConfigMap block misses a
// required key; name is the injector's name in the refusal.
func requireConfigured(injector any, name string) error {
	if err := validator.New().Struct(injector); err != nil {
		return fmt.Errorf("failed to validate %s: %w", name, err)
	}
	return nil
}

// requireWorkloadIdentityServiceAccount applies under OKEWorkloadIdentity:
// the pod must name a service account, and its token is automounted because
// the agent obtains workload-identity credentials with it.
func requireWorkloadIdentityServiceAccount(pod *v1.Pod, authType string) error {
	if authType != constants.AuthtypeOKEWorkloadIdentity {
		return nil
	}
	if len(pod.Spec.ServiceAccountName) == 0 {
		return fmt.Errorf("a service account should be specified when using OKEWorkloadIdentity")
	}
	automount := true
	pod.Spec.AutomountServiceAccountToken = &automount
	return nil
}

// mainContainerSecurityContext returns a copy of the main container's
// security context.
func mainContainerSecurityContext(pod *v1.Pod) (*v1.SecurityContext, error) {
	main := findContainer(pod.Spec.Containers, constants.MainContainerName)
	if main == nil {
		return nil, fmt.Errorf("no main container %s specified", constants.MainContainerName)
	}
	return main.SecurityContext.DeepCopy(), nil
}
