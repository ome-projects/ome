package render

import (
	"context"
	"fmt"
	"path"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/kvcache"
)

// CacheBindingError holds this engine projection until its required cache can be configured.
type CacheBindingError struct{ Cause error }

func (e *CacheBindingError) Error() string { return e.Cause.Error() }
func (e *CacheBindingError) Unwrap() error { return e.Cause }

func bindCache(ctx context.Context, p *Piece, svc *ome.InferenceService, engine *ome.EngineSpec, pod *corev1.PodSpec, metadata *metav1.ObjectMeta) error {
	if engine.Leader != nil || engine.Worker != nil || p.DeploymentMode != constants.RawDeployment || engine.Runner == nil {
		return fmt.Errorf("KVCachePool requires a single-pod RawDeployment engine with a vLLM runner")
	}
	if p.Runtime == nil || !slices.ContainsFunc(p.Runtime.KVCacheConnectors, func(c ome.KVCacheConnectorSpec) bool {
		return c.Provider == ome.KVCacheProviderLMCache && slices.Contains(c.DeploymentModes, ome.KVCachePoolNodeLocal) && c.Components.Engine.ConnectorConfig.ConnectorClass == kvcache.ConnectorClass
	}) {
		return fmt.Errorf("runtime %q does not declare LMCacheMPConnector for NodeLocal", p.RuntimeName)
	}
	pool := &ome.KVCachePool{}
	if err := p.Client.Get(ctx, client.ObjectKey{Namespace: svc.Namespace, Name: svc.Spec.KVCachePool.Name}, pool); err != nil {
		return err
	}
	if pool.Spec.Provider.Name != ome.KVCacheProviderLMCache || pool.Spec.DeploymentMode != ome.KVCachePoolNodeLocal {
		return fmt.Errorf("KVCachePool %q requires LMCache NodeLocal", pool.Name)
	}
	cond := meta.FindStatusCondition(pool.Status.Conditions, "Ready")
	if !pool.DeletionTimestamp.IsZero() || cond == nil || cond.Status != metav1.ConditionTrue || cond.ObservedGeneration != pool.Generation || pool.Status.Connection == nil || len(pool.Spec.Workloads) != 1 {
		return fmt.Errorf("KVCachePool %q is waiting for ready servers and a current connection", pool.Name)
	}
	cm := &corev1.ConfigMap{}
	if err := p.Client.Get(ctx, client.ObjectKey{Namespace: svc.Namespace, Name: pool.Status.Connection.ConfigMapRef.Name}, cm); err != nil {
		return err
	}
	// Inline the pool's JSON as a single argv element. It participates in the
	// pod template hash, so a new connection rolls the engine automatically.
	raw, err := kvcache.Connection(cm, pool.UID)
	if err != nil {
		return err
	}
	if !pod.HostIPC {
		return fmt.Errorf("this LMCache connection requires hostIPC: true on the engine template")
	}
	if metadata.Annotations[constants.RuntimeShmProfileAnnotationKey] != "" {
		return fmt.Errorf("runtime.ome.io/shm-profile would shadow the host /dev/shm required by this LMCache connection")
	}
	if metadata.Labels == nil {
		metadata.Labels = make(map[string]string)
	}
	metadata.Labels[kvcache.PoolUIDLabel] = string(pool.UID)
	if pod.NodeSelector == nil {
		pod.NodeSelector = make(map[string]string)
	}
	for k, v := range pool.Spec.Workloads[0].NodeSelector {
		if old, ok := pod.NodeSelector[k]; ok && old != v {
			return fmt.Errorf("engine nodeSelector %q conflicts with KVCachePool %q", k, pool.Name)
		}
		pod.NodeSelector[k] = v
	}
	for i := range pod.Containers {
		c := &pod.Containers[i]
		if c.Name != engine.Runner.Name {
			continue
		}
		if len(c.Command) > 0 {
			exe := path.Base(c.Command[0])
			python := (exe == "python" || exe == "python3") && len(c.Command) >= 3 && c.Command[1] == "-m" && c.Command[2] == "vllm.entrypoints.openai.api_server"
			if exe != "vllm" && !python {
				return fmt.Errorf("KVCachePool requires a direct vLLM command or image entrypoint")
			}
		}
		for _, args := range [][]string{c.Command, c.Args} {
			for _, arg := range args {
				if arg == "--kv-transfer-config" || strings.HasPrefix(arg, "--kv-transfer-config=") {
					return fmt.Errorf("the vLLM runner already configures --kv-transfer-config")
				}
			}
		}
		for _, mount := range c.VolumeMounts {
			if path.Clean(mount.MountPath) == "/dev/shm" {
				return fmt.Errorf("a /dev/shm mount shadows the host IPC required by this LMCache connection")
			}
		}
		for _, env := range c.Env {
			if env.Name == "PYTHONHASHSEED" && (env.Value != "0" || env.ValueFrom != nil) {
				return fmt.Errorf("the vLLM runner requires PYTHONHASHSEED=0 for LMCache")
			}
		}
		if !slices.ContainsFunc(c.Env, func(e corev1.EnvVar) bool { return e.Name == "PYTHONHASHSEED" }) {
			c.Env = append(c.Env, corev1.EnvVar{Name: "PYTHONHASHSEED", Value: "0"})
		}
		c.Args = append(c.Args, "--kv-transfer-config", raw)
		return nil
	}
	return fmt.Errorf("vLLM runner %q is absent from the engine pod", engine.Runner.Name)
}
