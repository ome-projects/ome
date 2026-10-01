---
title: Deploy an InferenceService
description: Create a complete runtime-managed Qwen deployment, wait for readiness, and send a chat request without a BaseModel or model agent.
since: v1.3
---

Serve the ungated `Qwen/Qwen3-0.6B` model with SGLang. You create a namespaced [ServingRuntime](../../concepts/runtimes/serving-runtimes.md) and an [InferenceService](../../concepts/serving/inference-services.md); the runtime downloads its own weights. OME manages the pods and Service, but not the model files.

This is a complete runtime-only path, not a prerequisite for every other guide. For OME-managed downloads on the pinned v1.2.2 release, use [Serve your first model](../../getting-started/serve-your-first-model.md). To learn the controller without GPUs or model downloads, use the [CPU-only OMENative lab](../omenative/learn-omenative.md).

<div class="prerequisites" markdown>

- A source installation of OME from the same public checkout as these examples. This recipe targets `bc1f94db` (v1.3 development), not v1.2.2. Follow [Install from source](../../getting-started/install.md#install-from-source), using the controller-only profile.
- `kubectl` access to create a namespace, ServingRuntime and InferenceService, inspect pods, and port-forward a Service.
- An AMD64 node with a free NVIDIA GPU exposed as `nvidia.com/gpu`, a driver compatible with the CUDA 12.9 runtime image, and room for a pod reserving 10 CPUs and 30 GiB of memory. These are the public catalog's conservative reservations, not measured minimum requirements.
- Serving nodes can pull `docker.io/lmsysorg/sglang`; serving pods can download the model from Hugging Face and its artifact hosts. Allow sufficient node disk space for the image and weights.

</div>

No BaseModel, model agent, AcceleratorClass, PVC, Hugging Face token or ingress controller is required. This guide explicitly chooses OMENative, which can run without the PodMonitor CRD; do not substitute the default RawDeployment mode without checking its [additional dependency](../../getting-started/install.md#the-podmonitor-crd).

!!! note "Runtime-managed storage"
    SGLang downloads weights into the container's local cache. Replacing the pod can download them again. The image tag is pinned, but the model repository's default revision can change. For retained storage and a separately controlled artifact revision, follow [Stage model weights](stage-model-weights.md).

## Step 1: Create the namespace

Save the three manifests below in a new working directory. They are also maintained in `config/samples/docs/runtime-managed-qwen/` in the OME checkout.

```yaml title="namespace.yaml"
apiVersion: v1
kind: Namespace
metadata:
  name: qwen-demo
```

```bash
kubectl apply -f namespace.yaml
```

## Step 2: Define the runtime

Save this as `servingruntime.yaml`. It adapts the image, command, GPU reservation, probes and shared-memory mount from the public Qwen runtime in `config/runtimes/srt/Qwen/qwen3-0-6b-rt.yaml`. Unlike that model-managed runtime, it passes the Hugging Face repository directly to SGLang rather than relying on OME to inject `MODEL_PATH`.

```yaml title="servingruntime.yaml"
apiVersion: ome.io/v1beta1
kind: ServingRuntime
metadata:
  name: sglang-qwen3
  namespace: qwen-demo
spec:
  protocolVersions:
    - openAI
  engineConfig:
    nodeSelector:
      kubernetes.io/arch: amd64
    tolerations:
      - key: nvidia.com/gpu
        operator: Exists
        effect: NoSchedule
    volumes:
      - name: dshm
        emptyDir:
          medium: Memory
    runner:
      name: ome-container
      image: docker.io/lmsysorg/sglang:v0.5.5.post3-cu129-amd64
      command:
        - python3
        - -m
        - sglang.launch_server
        - --host
        - "0.0.0.0"
        - --port
        - "8080"
        - --model-path
        - Qwen/Qwen3-0.6B
        - --served-model-name
        - Qwen/Qwen3-0.6B
        - --tp-size
        - "1"
        - --mem-frac
        - "0.9"
      ports:
        - name: http1
          containerPort: 8080
      volumeMounts:
        - name: dshm
          mountPath: /dev/shm
      resources:
        requests:
          cpu: "10"
          memory: 30Gi
          nvidia.com/gpu: 1
        limits:
          cpu: "10"
          memory: 30Gi
          nvidia.com/gpu: 1
      startupProbe:
        httpGet:
          path: /health_generate
          port: 8080
        initialDelaySeconds: 60
        periodSeconds: 6
        timeoutSeconds: 30
        failureThreshold: 150
      readinessProbe:
        httpGet:
          path: /health_generate
          port: 8080
        periodSeconds: 60
        timeoutSeconds: 200
        failureThreshold: 3
```

```bash
kubectl apply -f servingruntime.yaml
```

The startup probe allows a cold model load before readiness checks take over. Change its budget if your download or startup time requires it; a longer timeout does not fix an image, driver or download failure.

## Step 3: Create the InferenceService

Save this as `inferenceservice.yaml`:

```yaml title="inferenceservice.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: qwen3
  namespace: qwen-demo
spec:
  deploymentMode: OMENative
  runtime:
    name: sglang-qwen3
    kind: ServingRuntime
  engine:
    minReplicas: 1
    maxReplicas: 1
    autoscaler:
      class: None
```

`runtime.kind: ServingRuntime` selects the runtime in `qwen-demo`, even if a ClusterServingRuntime has the same name. There is no `spec.model`, so OME does not select a runtime by model metadata, inject model storage or wait for model-agent node labels. Ordinary runtime resources, volumes and node selectors still apply.

`engine` declares the component to create. One OMENative replica is one Instance; this single-runner Instance contains one pod. Disabling autoscaling keeps replica changes under your control.

```bash
kubectl apply -f inferenceservice.yaml
kubectl wait --for=condition=Ready inferenceservice/qwen3 -n qwen-demo --timeout=30m
```

## Step 4: Inspect readiness

OME should create an InferenceReplica named `qwen3-engine`, its serving pod, and Services for the engine and public service endpoint. It does not create a Deployment for this OMENative engine.

```bash
kubectl get inferenceservice qwen3 -n qwen-demo
kubectl get inferencereplica qwen3-engine -n qwen-demo
kubectl get pods -n qwen-demo -l ome.io/inferenceservice=qwen3
kubectl get services -n qwen-demo
kubectl get inferenceservice qwen3 -n qwen-demo -o jsonpath='{.status.url}'
```

With ingress creation disabled, the endpoint is a cluster-internal Service, not a browser-accessible public URL. `Ready=True` means the controller's readiness checks passed; the next step checks that the model actually answers.

## Step 5: Send a request

Forward a local port to the Service and leave this terminal running:

```bash
kubectl port-forward svc/qwen3 18080:8080 -n qwen-demo
```

In a second terminal, confirm that SGLang lists the intended model:

```bash
curl --fail --silent --show-error http://localhost:18080/v1/models
```

Look for `Qwen/Qwen3-0.6B` in the response's `data` array. Then send a chat request:

```bash
curl --fail --silent --show-error http://localhost:18080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "Qwen/Qwen3-0.6B",
    "messages": [
      {"role": "user", "content": "What is Kubernetes? Answer in one sentence."}
    ],
    "max_tokens": 100,
    "chat_template_kwargs": {"enable_thinking": false}
  }'
```

A successful response is an OpenAI-compatible chat completion with text in `choices[0].message.content`. The exact generated text varies. HTTP errors, an empty model list or a failed generation are not a successful deployment even if the InferenceService reports Ready.

## Troubleshooting

Start with the service, pod events and runner logs:

```bash
kubectl describe inferenceservice qwen3 -n qwen-demo
kubectl describe pods -n qwen-demo -l ome.io/inferenceservice=qwen3
kubectl logs -n qwen-demo -l ome.io/inferenceservice=qwen3 -c ome-container --tail=100
```

| Symptom | Check and next action |
| --- | --- |
| Admission rejects the runtime-only service | Check the running controller image and installed CRDs. v1.2.2 still requires a model; use the matching source installation. Confirm `sglang-qwen3` exists in `qwen-demo`. |
| Pod stays Pending | Read scheduling events. Check allocatable GPUs, CPU and memory, AMD64 placement and node taints. Model-agent readiness labels are not involved in this recipe. |
| Image pull fails | Check registry access, image name, pull credentials and architecture. After fixing the cause, [reset an Instance](../omenative/reset-failed-instances.md) if OMENative has marked it Failed. |
| Pod runs but never becomes Ready | Read SGLang's startup logs for download, CUDA or out-of-memory errors. Confirm artifact-host egress and GPU compatibility before extending timeouts. |
| Port-forward or request fails | Confirm the Service has ready endpoints with `kubectl get endpointslices -n qwen-demo -l kubernetes.io/service-name=qwen3`. Keep the port-forward process alive and use the model name returned by `/v1/models`. |

For lifecycle failures, inspect the InferenceReplica with `kubectl get inferencereplica qwen3-engine -n qwen-demo -o yaml`. [Troubleshoot an InferenceService](../troubleshoot/troubleshoot-an-inferenceservice.md) explains the wider condition and event checks.

## Clean up

Stop port-forwarding with Ctrl-C. Delete the service first, then its runtime:

```bash
kubectl delete -f inferenceservice.yaml
kubectl wait --for=delete pod -n qwen-demo -l ome.io/inferenceservice=qwen3 --timeout=5m
kubectl delete -f servingruntime.yaml
```

Deleting the InferenceService garbage-collects its controller-owned resources. If `qwen-demo` was created only for this guide and contains nothing else you need, delete it with `kubectl delete -f namespace.yaml`. There is no PVC or model resource to retain.

## Next steps

| You want to… | Continue with |
| --- | --- |
| Practice scale and template updates without spending GPU capacity | [Learn OMENative without a GPU](../omenative/learn-omenative.md) |
| Stage and retain weights before starting a model server | [Stage model weights](stage-model-weights.md) |
| Let OME manage model downloads and metadata | [Serve your first model](../../getting-started/serve-your-first-model.md) |
| Understand explicit versus automatic runtime selection | [Reference a runtime explicitly](reference-a-runtime-explicitly.md) |
| Add accelerator selection | [Select accelerators](select-accelerators.md) |
