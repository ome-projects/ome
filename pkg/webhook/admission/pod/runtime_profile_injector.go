package pod

import (
	"encoding/json"
	"fmt"
	"strconv"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"sigs.k8s.io/ome/pkg/constants"
)

// Runtime profile injectors factor out repetitive ServingRuntime YAML
// (probes, /dev/shm, prometheus annotations). Each is gated on its own
// profile annotation; profiles never overwrite operator-set values.

// RuntimeProfilesConfigKeyName is the inferenceservice-config key that
// holds the runtime profiles' port and probe timings.
const RuntimeProfilesConfigKeyName = "runtimeProfiles"

// RuntimeProfilesConfig is the runtimeProfiles block of the
// inferenceservice-config ConfigMap: the port the probe and observability
// profiles use when the pod names none, and the schedule of the three
// probes a probe profile injects.
type RuntimeProfilesConfig struct {
	DefaultPort int32        `json:"defaultPort"`
	Probes      ProbeTimings `json:"probes"`
}

// ProbeTimings is the schedule of each injected probe.
type ProbeTimings struct {
	Readiness ProbeTiming `json:"readiness"`
	Liveness  ProbeTiming `json:"liveness"`
	Startup   ProbeTiming `json:"startup"`
}

// ProbeTiming is one probe's schedule in the probe's own field names. A
// zero field is left to the API server's default.
type ProbeTiming struct {
	InitialDelaySeconds int32 `json:"initialDelaySeconds"`
	PeriodSeconds       int32 `json:"periodSeconds"`
	TimeoutSeconds      int32 `json:"timeoutSeconds"`
	FailureThreshold    int32 `json:"failureThreshold"`
	SuccessThreshold    int32 `json:"successThreshold"`
}

// newRuntimeProfilesConfig parses the runtimeProfiles block. An absent
// block is nil: a probe profile then injects nothing, and an observability
// profile serves only pods that name their port.
func newRuntimeProfilesConfig(configMap *v1.ConfigMap) (*RuntimeProfilesConfig, error) {
	raw, ok := configMap.Data[RuntimeProfilesConfigKeyName]
	if !ok {
		return nil, nil
	}
	config := &RuntimeProfilesConfig{}
	if err := json.Unmarshal([]byte(raw), config); err != nil {
		return nil, fmt.Errorf("unable to unmarshal %v json string: %w", RuntimeProfilesConfigKeyName, err)
	}
	return config, nil
}

func runtimeTargetContainer(pod *v1.Pod) *v1.Container {
	return findContainer(pod.Spec.Containers, targetContainerName(pod, constants.RuntimeContainerNameAnnotationKey))
}

// ----- shm ---------------------------------------------------------------

// ShmProfiles enumerates the recognized values for
// runtime.ome.io/shm-profile. Only "default" exists today: it injects
// a memory-backed /dev/shm emptyDir without a sizeLimit, leaving
// per-pod sizing under the kernel default. Runtimes that need an
// explicit sizeLimit set the volume directly in YAML — sizing is
// deployment-specific (single-tenant devbox vs multi-pod node) and
// shouldn't be baked into a one-size-fits-all profile.
var ShmProfiles = map[string]struct{}{
	"default": {},
}

func injectShm(pod *v1.Pod) error {
	profile, err := lookupProfile(pod, constants.RuntimeShmProfileAnnotationKey, ShmProfiles, "shm")
	if err != nil {
		return err
	}
	if profile == nil {
		return nil
	}
	target := runtimeTargetContainer(pod)
	if target == nil {
		return nil
	}

	ensureVolume(pod, v1.Volume{
		Name: DshmVolumeName,
		VolumeSource: v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{Medium: v1.StorageMediumMemory},
		},
	})
	ensureVolumeMount(target, v1.VolumeMount{Name: DshmVolumeName, MountPath: "/dev/shm"})
	return nil
}

// ----- probes ------------------------------------------------------------

type ProbeProfile struct {
	Path string
}

var ProbeProfiles = map[string]ProbeProfile{
	"sglang-http": {Path: "/health"},
	"vllm-http":   {Path: "/health"},
}

// injectProbes adds the probes the profile names, on the port the pod
// names or the configured default, with the configured timings. Without
// the runtimeProfiles block or a port it injects nothing.
func injectProbes(pod *v1.Pod, profiles *RuntimeProfilesConfig) error {
	profile, err := lookupProfile(pod, constants.RuntimeProbeProfileAnnotationKey, ProbeProfiles, "probe")
	if err != nil {
		return err
	}
	if profile == nil {
		return nil
	}
	port, err := resolvePort(pod, constants.RuntimeProbePortAnnotationKey, profiles)
	if err != nil {
		return err
	}
	if profiles == nil || port == 0 {
		return nil
	}
	target := runtimeTargetContainer(pod)
	if target == nil {
		return nil
	}

	// Inject only the probes the runtime hasn't declared, so a YAML can
	// override any single probe (e.g. liveness only) without losing the
	// other two.
	if target.ReadinessProbe == nil {
		target.ReadinessProbe = httpProbe(profile.Path, port, profiles.Probes.Readiness)
	}
	if target.LivenessProbe == nil {
		target.LivenessProbe = httpProbe(profile.Path, port, profiles.Probes.Liveness)
	}
	if target.StartupProbe == nil {
		target.StartupProbe = httpProbe(profile.Path, port, profiles.Probes.Startup)
	}
	return nil
}

// httpProbe is an HTTP GET probe on path and port with the timing; a zero
// successThreshold falls through to the apiserver default.
func httpProbe(path string, port int32, timing ProbeTiming) *v1.Probe {
	p := &v1.Probe{
		ProbeHandler: v1.ProbeHandler{
			HTTPGet: &v1.HTTPGetAction{
				Path: path,
				Port: intstr.FromInt32(port),
			},
		},
		InitialDelaySeconds: timing.InitialDelaySeconds,
		PeriodSeconds:       timing.PeriodSeconds,
		TimeoutSeconds:      timing.TimeoutSeconds,
		FailureThreshold:    timing.FailureThreshold,
	}
	if timing.SuccessThreshold > 0 {
		p.SuccessThreshold = timing.SuccessThreshold
	}
	return p
}

// ----- observability ----------------------------------------------------

type ObservabilityProfile struct {
	Path string
}

var ObservabilityProfiles = map[string]ObservabilityProfile{
	"prometheus": {Path: "/metrics"},
}

// injectObservability sets the scrape annotations the profile names, on
// the port the pod names or the configured default. Without a port it
// injects nothing.
func injectObservability(pod *v1.Pod, profiles *RuntimeProfilesConfig) error {
	profile, err := lookupProfile(pod, constants.RuntimeObservabilityProfileAnnotationKey, ObservabilityProfiles, "observability")
	if err != nil {
		return err
	}
	if profile == nil {
		return nil
	}
	port, err := resolvePort(pod, constants.RuntimeObservabilityPortAnnotationKey, profiles)
	if err != nil {
		return err
	}
	if port == 0 {
		return nil
	}
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	setIfAbsent(pod.Annotations, constants.PrometheusScrapeAnnotationKey, "true")
	setIfAbsent(pod.Annotations, constants.PrometheusPortAnnotationKey, strconv.Itoa(int(port)))
	setIfAbsent(pod.Annotations, constants.PrometheusPathAnnotationKey, profile.Path)
	return nil
}

// ----- helpers ----------------------------------------------------------

// resolvePort is the port the annotation names, else the configured
// default, else 0 for "none".
func resolvePort(pod *v1.Pod, key string, profiles *RuntimeProfilesConfig) (int32, error) {
	v, ok := pod.ObjectMeta.Annotations[key]
	if !ok || v == "" {
		if profiles == nil {
			return 0, nil
		}
		return profiles.DefaultPort, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 || n > 65535 {
		return 0, fmt.Errorf("invalid port %q (annotation %s)", v, key)
	}
	return int32(n), nil
}

func setIfAbsent(m map[string]string, k, v string) {
	if _, ok := m[k]; !ok {
		m[k] = v
	}
}
