---
title: Deploy an InferenceService
description: Write the smallest InferenceService that serves a model, with only a model, a model and a runtime, or only a runtime, then wait for it, send it a request and see what OME created.
---

An [InferenceService](../../concepts/serving/inference-services.md) is the one resource you have to write to serve a model. At its smallest it names a model. OME picks a [runtime](../../concepts/runtimes/serving-runtimes.md) that supports the model, creates the serving pods and a Service, and reports when the service is ready. This guide writes the three smallest shapes an InferenceService takes, then follows one of them from `kubectl apply` to a request. The other guides in this section, accelerators, storage and a runtime you name, are variations on it.

<div class="prerequisites" markdown>

- OME, installed as in [Install OME](../../getting-started/install.md), with the [model agent](../operate-ome/model-agent.md) turned on and `modelAgent.hostPath` set to `/raid/models`, where the models in `config/models/` keep their weights.
- Since v1.3, the Prometheus Operator's PodMonitor CRD, which engines that run as a Deployment need. See [The PodMonitor CRD](../../getting-started/install.md#the-podmonitor-crd).
- The ClusterBaseModel `llama-3-2-1b-instruct` and the ClusterServingRuntime `vllm-llama-3-2-1b-instruct`, from `config/models/meta/Llama-3.2-1B-Instruct.yaml` and `config/runtimes/vllm/llama-3-2-1b-instruct-rt.yaml` in the OME repository. The model is gated on Hugging Face, so the model agent needs your token in the Secret `hf-token` in the `ome` namespace: see [Credentials](../../concepts/models/base-models.md#credentials). Wait until `kubectl get clusterbasemodel llama-3-2-1b-instruct` shows `Ready`; until then, no runtime matches the model and the webhook rejects the InferenceService.
- A namespace `llama-demo`. To create it, run `kubectl create namespace llama-demo`.
- A node with a free NVIDIA GPU that Kubernetes schedules as `nvidia.com/gpu`, and `kubectl` access to the cluster.

</div>

## Step 1: Choose the shape

An InferenceService names a model, a runtime, or both. Pick the shape that fits, and save it as `isvc.yaml`.

=== "Model only"

    OME picks the runtime: the one that best supports the model among the runtimes that allow automatic selection. `engine: {}` declares the engine, the component that runs the model, and lets the runtime fill it in.

    ```yaml title="isvc.yaml"
    apiVersion: ome.io/v1beta1
    kind: InferenceService
    metadata:
      name: llama-3-2-1b-instruct
      namespace: llama-demo
    spec:
      model:
        name: llama-3-2-1b-instruct
      engine: {}
    ```

    `model.kind` defaults to `ClusterBaseModel`. To serve a BaseModel from `llama-demo` instead, add `kind: BaseModel` under `model`.

=== "Model and runtime"

    Name the runtime when OME wouldn't pick it on its own, when several runtimes match the model and you want a particular one, or when you want the choice to stay fixed. [Reference a runtime explicitly](reference-a-runtime-explicitly.md) covers the details.

    ```yaml title="isvc.yaml"
    apiVersion: ome.io/v1beta1
    kind: InferenceService
    metadata:
      name: llama-3-2-1b-instruct
      namespace: llama-demo
    spec:
      model:
        name: llama-3-2-1b-instruct
      runtime:
        name: vllm-llama-3-2-1b-instruct
      engine:
        minReplicas: 1
        maxReplicas: 1
    ```

    `minReplicas` and `maxReplicas` keep the engine at one pod. Without them, the range comes from the runtime, then from the chart's [replica defaults](../operate-ome/set-replica-defaults.md), 1 to 3 for the engine.

=== "Runtime only"

    Since v1.3, you can leave out the model when the runtime loads the weights itself. OME then skips all model work: no runtime selection, no `MODEL_PATH`, no model volume, and no node selector for the model. This runtime passes `$(MODEL_PATH)` to vLLM, so the engine sets the variable to the Hugging Face repository and gives vLLM the token from a Secret `hf-token` in `llama-demo`, as [Credentials](../../concepts/models/base-models.md#credentials) creates it.

    ```yaml title="isvc.yaml"
    apiVersion: ome.io/v1beta1
    kind: InferenceService
    metadata:
      name: llama-runtime-only
      namespace: llama-demo
    spec:
      runtime:
        name: vllm-llama-3-2-1b-instruct
      engine:
        runner:
          env:
            - name: MODEL_PATH
              value: meta-llama/Llama-3.2-1B-Instruct
            - name: HF_TOKEN
              valueFrom:
                secretKeyRef:
                  name: hf-token
                  key: token
    ```

    This shape needs no BaseModel and no model agent. On v1.2.2, every InferenceService needs `spec.model`.

The webhook rejects an InferenceService that names neither: `at least one of spec.model or spec.runtime must be set`.

The rest of this guide uses the first shape. The commands work for the second one unchanged. For the third, use the name `llama-runtime-only` in place of `llama-3-2-1b-instruct`.

## Step 2: Apply it

```bash
kubectl apply -f isvc.yaml
```

```output
Warning: Runtime vllm-llama-3-2-1b-instruct will be auto-selected for model llama-3-2-1b-instruct
inferenceservice.ome.io/llama-3-2-1b-instruct created
```

The warning names the runtime OME picked. When you name a runtime that declares support for the model, there's no warning. If no runtime that allows automatic selection supports the model, the webhook rejects the InferenceService with a message that starts with `no supporting runtime found for model llama-3-2-1b-instruct`: see [Troubleshoot runtime selection](troubleshoot-runtime-selection.md).

## Step 3: Wait until it's ready

The first start pulls the runtime's image and loads the model, which can take several minutes:

```bash
kubectl wait --for=condition=Ready inferenceservice/llama-3-2-1b-instruct -n llama-demo --timeout=30m
```

```output
inferenceservice.ome.io/llama-3-2-1b-instruct condition met
```

`status.url` is where to send requests. With ingress creation off, the default, it's the address of the Service that OME puts in front of the model:

```bash
kubectl get inferenceservice llama-3-2-1b-instruct -n llama-demo -o jsonpath='{.status.url}'
```

```output
http://llama-3-2-1b-instruct.llama-demo.svc.cluster.local:8080
```

## Step 4: See what OME created

With the chart's default settings, OME creates:

- A Deployment and a Service named `llama-3-2-1b-instruct-engine`. The pods run only on nodes where the model is `Ready`, as [Node labels and status](../../concepts/models/base-models.md#node-labels-and-status) describes.
- A HorizontalPodAutoscaler and a PodDisruptionBudget of the same name. The autoscaler keeps CPU use at 80% with 1 to 3 replicas, and the budget lets one pod be unavailable at a time.
- The Service `llama-3-2-1b-instruct` in front of the engine, whose address is the URL above. See [Ingress and external access](../../concepts/rollouts-and-traffic/ingress.md).

The `RUNTIME` column of `kubectl get inferenceservice` shows `spec.runtime.name`, so it's empty when OME picks the runtime. The engine's workload always names the runtime OME used, in its `serving-runtime` label:

```bash
kubectl get deployment llama-3-2-1b-instruct-engine -n llama-demo -o jsonpath='{.metadata.labels.serving-runtime}'
```

```output
vllm-llama-3-2-1b-instruct
```

The engine runs as a Deployment because nothing chose another [deployment mode](../../concepts/architecture/deployment-modes.md). A multi-node engine runs on [OMENative](../../concepts/omenative/overview.md) by default, and [any component can opt in](../omenative/serve-a-model-on-omenative.md).

## Step 5: Send a request

The Service is a ClusterIP Service, so forward a local port to it, and leave the command running:

```bash
kubectl port-forward svc/llama-3-2-1b-instruct 8080:8080 -n llama-demo
```

```output
Forwarding from 127.0.0.1:8080 -> 8080
Forwarding from [::1]:8080 -> 8080
```

In a second terminal, ask the runtime which model names it serves:

```bash
curl -s http://localhost:8080/v1/models
```

The response lists them under `data`. Send a chat completion to `/v1/chat/completions` with one of those names as `model`, as [Step 7 of Serve your first model](../../getting-started/serve-your-first-model.md#step-7-send-a-request) does. The response is an OpenAI chat completion, with the answer in `choices[0].message.content`.

## Troubleshooting

[Troubleshoot an InferenceService](../troubleshoot/troubleshoot-an-inferenceservice.md) covers more symptoms.

### The webhook rejects the InferenceService

`kubectl` prints `admission webhook "inferenceservice.ome-webhook-server.validator" denied the request:`, then a message:

| The message includes | Fix |
| --- | --- |
| `referenced model "llama-3-2-1b-instruct" not found in namespace "llama-demo"` | Create the ClusterBaseModel, or add `kind: BaseModel` if the model is a BaseModel in `llama-demo`. |
| `model format name is required` | Wait for the model to become `Ready`: OME fills in its format when the model agent has read it. |
| `no supporting runtime found for model llama-3-2-1b-instruct` | No runtime with `autoSelect: true` supports the model. See [Troubleshoot runtime selection](troubleshoot-runtime-selection.md), or name a runtime. |
| `at least one of spec.model or spec.runtime must be set` | Add a `model` or a `runtime`. |

### The engine pod stays Pending

Look at the pod's events with `kubectl describe pod -n llama-demo -l ome.io/inferenceservice=llama-3-2-1b-instruct`. `Insufficient nvidia.com/gpu` means no node has a free GPU for the runtime's request. `didn't match Pod's node affinity/selector` means the model isn't `Ready` on any node with a free GPU: list the nodes that have it with `kubectl get nodes -l models.ome.io/clusterbasemodel.llama-3-2-1b-instruct=Ready`.

### The engine pod runs, but the InferenceService doesn't become Ready {since=v1.3}

Engines that run as a Deployment need the Prometheus Operator's PodMonitor CRD. Install it as in [The PodMonitor CRD](../../getting-started/install.md#the-podmonitor-crd), then restart the controller with `kubectl rollout restart deployment/ome-controller-manager -n ome`, because it looks for the CRD only when it starts.

## Clean up

Delete the InferenceService. The model and the runtime stay, for other InferenceServices to use:

```bash
kubectl delete inferenceservice llama-3-2-1b-instruct -n llama-demo
```

```output
inferenceservice.ome.io "llama-3-2-1b-instruct" deleted from llama-demo namespace
```

## Next steps

- [Select accelerators](select-accelerators.md): name an AcceleratorClass for the engine, or let a policy pick one.
- [Serve models from a PVC](serve-models-from-pvc.md) and [Serve models from node-local storage](serve-models-from-local-storage.md): serve weights that are already in the cluster, with no download.
- [Reference a runtime explicitly](reference-a-runtime-explicitly.md): the fields of `spec.runtime`, and how OME resolves the name.
- [Serve a model on OMENative](../omenative/serve-a-model-on-omenative.md): run the engine on OMENative instead of a Deployment. Since v1.3.
- [InferenceService](../../concepts/serving/inference-services.md): the engine, decoder and router components, and every field they take.
