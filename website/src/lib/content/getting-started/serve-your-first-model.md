---
title: Serve your first model
description: Create a ClusterBaseModel, a ClusterServingRuntime and an InferenceService for a small model, then send your first request to it.
---

Serve [Qwen3-0.6B](https://huggingface.co/Qwen/Qwen3-0.6B), a small language model, with SGLang on one GPU, and send it a chat request through its OpenAI-compatible API. You create a [ClusterBaseModel](../concepts/models/base-models.md) that says where the weights are, a [ClusterServingRuntime](../concepts/runtimes/serving-runtimes.md) that describes the serving pods, and an [InferenceService](../concepts/serving/inference-services.md) that serves the model with that runtime.

<div class="prerequisites" markdown>

- OME, installed as in [Install OME](install.md), with the [model agent](../guides/operate-ome/model-agent.md) and its default host directory, `/mnt/data/models`.
- Since v1.3, the Prometheus Operator's PodMonitor CRD. See [The PodMonitor CRD](install.md#the-podmonitor-crd).
- `kubectl`, with the rights to create namespaces and cluster-scoped OME resources.
- An amd64 node with an NVIDIA GPU that Kubernetes schedules as `nvidia.com/gpu`, through the NVIDIA device plugin or the GPU Operator. The engine pod requests 1 GPU, 10 CPUs and 30 GiB of memory. The model agent's pod on the same node requests another 10 CPUs and 100 GiB, which you can lower with `modelAgent.resources`.
- Nodes that can reach Hugging Face, to download the model, and Docker Hub, to pull the SGLang image. Qwen3-0.6B isn't gated, so you need no Hugging Face token.

</div>

This is the model-agent path for the pinned v1.2.2 installation. For a development build without the model agent, use [runtime-managed Qwen](../guides/deploy-models/deploy-an-inferenceservice.md); without a GPU, try the [CPU-only OMENative lab](../guides/omenative/learn-omenative.md). OMENative requires a [matching source installation](install.md#install-from-source), not just a new field on a v1.2.2 service.

## Step 1: Check that OME is running

Check that the OME controller and the model agent are ready:

```bash
kubectl wait --for=condition=Available deployment/ome-controller-manager -n ome --timeout=5m
kubectl rollout status daemonset/ome-model-agent-daemonset -n ome --timeout=5m
```

```output
deployment.apps/ome-controller-manager condition met
daemon set "ome-model-agent-daemonset" successfully rolled out
```

If the second command fails with `NotFound`, run [Step 3 of Install OME](install.md#step-3-install-ome) again: its values file turns on the model agent.

## Step 2: Create the model

A ClusterBaseModel is cluster-scoped, so InferenceServices in any namespace can serve it. Save this one as `qwen3-0-6b-model.yaml`:

```yaml title="qwen3-0-6b-model.yaml"
apiVersion: ome.io/v1beta1
kind: ClusterBaseModel
metadata:
  name: qwen3-0-6b
spec:
  storage:
    storageUri: hf://Qwen/Qwen3-0.6B
    path: /mnt/data/models/Qwen/Qwen3-0.6B
```

- `storageUri` says where the weights come from: `hf://`, then the model's repository on Hugging Face.
- `path` is the directory on each node that holds the weights. Keep it inside the model agent's host directory, `/mnt/data/models` by default, so that the weights land on the node's disk.

Every node that runs the model agent downloads the model. To keep it on some nodes only, see [Choose the nodes](../concepts/models/base-models.md#choose-the-nodes).

Create the model:

```bash
kubectl apply -f qwen3-0-6b-model.yaml
```

```output
clusterbasemodel.ome.io/qwen3-0-6b created
```

## Step 3: Watch the model become Ready

Watch the model's [state](../concepts/models/base-models.md#model-lifecycle):

```bash
kubectl get clusterbasemodel qwen3-0-6b -w -o custom-columns=NAME:.metadata.name,STATE:.status.state
```

```output
NAME         STATE
qwen3-0-6b   In_Transit
qwen3-0-6b   In_Transit
qwen3-0-6b   Ready
```

Press Ctrl+C once the state is `Ready`. `<none>` means that OME hasn't set a state yet.

Meanwhile, the model agent on each node downloads the weights into `/mnt/data/models/Qwen/Qwen3-0.6B`. The model is `Ready` as soon as one node has it. OME reads the model's format and architecture from its files, and fills them in for you. Check [what OME learned](../concepts/models/base-models.md#what-ome-learns-from-the-model):

```bash
kubectl get clusterbasemodel qwen3-0-6b \
  -o custom-columns=NAME:.metadata.name,FORMAT:.spec.modelFormat.name,ARCHITECTURE:.spec.modelArchitecture,STATE:.status.state
```

```output
NAME         FORMAT        ARCHITECTURE       STATE
qwen3-0-6b   safetensors   Qwen3ForCausalLM   Ready
```

## Step 4: Create the runtime

The ClusterServingRuntime `srt-qwen3-0-6b`, from OME's catalog, runs SGLang from the image `docker.io/lmsysorg/sglang:v0.5.5.post3-cu129-amd64`, which is built for amd64 and CUDA 12.9. SGLang serves the model on port 8080, under the name `Qwen/Qwen3-0.6B`. The runtime sets `autoSelect: false`, so an InferenceService must name it to use it.

Apply [the runtime](https://github.com/ome-projects/ome/blob/v1.2.2/config/runtimes/srt/Qwen/qwen3-0-6b-rt.yaml) from the v1.2.2 release:

```bash
kubectl apply -f https://raw.githubusercontent.com/ome-projects/ome/v1.2.2/config/runtimes/srt/Qwen/qwen3-0-6b-rt.yaml
```

```output
clusterservingruntime.ome.io/srt-qwen3-0-6b created
```

## Step 5: Create the InferenceService

Create a namespace for the InferenceService:

```bash
kubectl create namespace qwen3-0-6b
```

```output
namespace/qwen3-0-6b created
```

Save this InferenceService as `qwen3-0-6b-isvc.yaml`:

```yaml title="qwen3-0-6b-isvc.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: qwen3-0-6b
  namespace: qwen3-0-6b
spec:
  model:
    name: qwen3-0-6b
  runtime:
    name: srt-qwen3-0-6b
  engine:
    minReplicas: 1
    maxReplicas: 1
```

- `model.name` names the model from Step 2, and `model.kind` defaults to `ClusterBaseModel`. To serve a BaseModel from the InferenceService's namespace instead, set `model.kind: BaseModel`.
- `runtime.name` names the runtime from Step 4.
- `engine` is the component that runs the model. The runtime supplies its container, and `minReplicas` and `maxReplicas` keep it at one pod.

Create the InferenceService:

```bash
kubectl apply -f qwen3-0-6b-isvc.yaml
```

```output
inferenceservice.ome.io/qwen3-0-6b created
```

## Step 6: Watch the InferenceService become Ready

Wait for the InferenceService to be ready. The first start pulls the SGLang image, which can take several minutes:

```bash
kubectl wait --for=condition=Ready inferenceservice/qwen3-0-6b -n qwen3-0-6b --timeout=30m
```

```output
inferenceservice.ome.io/qwen3-0-6b condition met
```

Then look at the InferenceService:

```bash
kubectl get inferenceservice qwen3-0-6b -n qwen3-0-6b
```

```output
NAME         URL                                                   READY   BASEMODEL    RUNTIME          AGE
qwen3-0-6b   http://qwen3-0-6b.qwen3-0-6b.svc.cluster.local:8080   True    qwen3-0-6b   srt-qwen3-0-6b   7m12s
```

The chart turns off ingress creation by default, so OME puts the Service `qwen3-0-6b` in front of the engine, and `URL` is its address inside the cluster.

OME runs the engine as the Deployment `qwen3-0-6b-engine`, in the default RawDeployment [deployment mode](../concepts/architecture/deployment-modes.md). Its pod runs on a node where the model is `Ready`, and mounts the weights from that node. List it:

```bash
kubectl get pods -n qwen3-0-6b
```

```output
NAME                                 READY   STATUS    RESTARTS   AGE
qwen3-0-6b-engine-6d8f9c7b5d-x2k4p   1/1     Running   0          7m8s
```

## Step 7: Send a request

The Service `qwen3-0-6b` is a ClusterIP Service, so forward a local port to it, and leave the command running. If you turned on ingress creation, forward to `svc/qwen3-0-6b-engine` instead.

```bash
kubectl port-forward svc/qwen3-0-6b 8080:8080 -n qwen3-0-6b
```

```output
Forwarding from 127.0.0.1:8080 -> 8080
Forwarding from [::1]:8080 -> 8080
```

In a second terminal, send a chat completion request. `model` is the name that the runtime serves the model under. `chat_template_kwargs` turns off Qwen3's thinking mode, which can use up `max_tokens` on `<think>` reasoning before the answer.

```bash
curl -s http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model": "Qwen/Qwen3-0.6B", "messages": [{"role": "user", "content": "What is Kubernetes? Answer in one sentence."}], "max_tokens": 100, "chat_template_kwargs": {"enable_thinking": false}}'
```

The response is an OpenAI chat completion, with the model's answer in `choices[0].message.content`. If `finish_reason` is `length`, the answer ran out of tokens: raise `max_tokens`.

## Troubleshooting

[Troubleshoot an InferenceService](../guides/troubleshoot/troubleshoot-an-inferenceservice.md) covers more symptoms.

### The model doesn't become Ready

Check the model's [label](../concepts/models/base-models.md#node-labels-and-status) on each node:

```bash
kubectl get nodes -L models.ome.io/clusterbasemodel.qwen3-0-6b
```

```output
NAME         STATUS   ROLES    AGE   VERSION   CLUSTERBASEMODEL.QWEN3-0-6B
gpu-node-1   Ready    <none>   12d   v1.33.4   Failed
```

- Empty: no model agent runs on the node. List the agent's pods and their nodes with `kubectl get pods -n ome -l app.kubernetes.io/component=ome-model-agent-daemonset -o wide`. The agent tolerates the `nvidia.com/gpu` taint. For nodes with other taints, add tolerations to `modelAgent.tolerations`.
- `Updating`: the agent is still downloading or reading the model.
- `Failed`: the download or the reading failed, for example because the node can't reach Hugging Face.

For `Updating` and `Failed`, read the agent's logs with `kubectl logs -n ome -l app.kubernetes.io/component=ome-model-agent-daemonset --tail=100`. [Run the model agent](../guides/operate-ome/model-agent.md) covers the agent's settings.

### The InferenceService is rejected or gets no pods {#the-webhook-rejects-the-inferenceservice}

When the webhook rejects the InferenceService, kubectl's error includes `admission webhook "inferenceservice.ome-webhook-server.validator" denied the request:`, followed by a reason:

| The reason includes | Fix |
| --- | --- |
| `referenced model "qwen3-0-6b" not found` | Create the model, as in [Step 2](#step-2-create-the-model). |
| `model format name is required` | Wait for the model to be `Ready`, as in [Step 3](#step-3-watch-the-model-become-ready). |
| `runtime srt-qwen3-0-6b not found` | Create the runtime, as in [Step 4](#step-4-create-the-runtime). |

Then apply `qwen3-0-6b-isvc.yaml` again.

On v1.2.2, the webhook admits the InferenceService in the last two cases. OME creates no pods for it, and records the reason in `RuntimeValidationError` events:

```bash
kubectl get events -n qwen3-0-6b --field-selector reason=RuntimeValidationError
```

```output
LAST SEEN   TYPE      REASON                   OBJECT                        MESSAGE
8s          Warning   RuntimeValidationError   inferenceservice/qwen3-0-6b   Runtime srt-qwen3-0-6b does not support model qwen3-0-6b: runtime srt-qwen3-0-6b not found in namespace qwen3-0-6b or at cluster scope
```

Fix the cause, and OME creates the engine on its own.

### The engine pod stays Pending

Look at the pod's events with `kubectl describe pod -n qwen3-0-6b -l ome.io/inferenceservice=qwen3-0-6b`. The scheduler's message says why no node fits:

- `Insufficient nvidia.com/gpu`, `Insufficient cpu` or `Insufficient memory`: no node has a free GPU, 10 CPUs and 30 GiB of memory. On the GPU node, the model agent's pod already takes 10 CPUs and 100 GiB. Lower them with `modelAgent.resources`, or free the GPU.
- `didn't match Pod's node affinity/selector`: the model isn't `Ready` on any node with a free GPU. List the nodes that have it with `kubectl get nodes -l models.ome.io/clusterbasemodel.qwen3-0-6b=Ready`.
- `untolerated taint`: the GPU node has a taint other than `nvidia.com/gpu`, the only one that the runtime tolerates.

### The engine pod isn't ready or restarts

The runtime's startup probe gives SGLang about 16 minutes to load the model before Kubernetes restarts its container. Read SGLang's logs with `kubectl logs -n qwen3-0-6b -l ome.io/inferenceservice=qwen3-0-6b --tail=100`.

- `ErrImagePull` or `ImagePullBackOff`: the node can't pull the SGLang image from Docker Hub. If your nodes pull from a mirror, see [Install from a private registry](private-registries.md).
- SGLang can't find the model's files: check that the model's `path` is inside the model agent's host directory. With a path outside it, the model still becomes `Ready`, but the engine's pod mounts an empty directory.

### The engine pod runs, but the InferenceService doesn't become Ready {since=v1.3}

RawDeployment engines like this one need the Prometheus Operator's PodMonitor CRD. Install it as in [The PodMonitor CRD](install.md#the-podmonitor-crd). The controller looks for the CRD only when it starts, so then restart it with `kubectl rollout restart deployment/ome-controller-manager -n ome`.

## Clean up

[Serve a model on OMENative](../guides/omenative/serve-a-model-on-omenative.md) starts from this model and runtime. If you go on to it, delete only the InferenceService, which frees its GPU. To delete everything:

```bash
kubectl delete inferenceservice qwen3-0-6b -n qwen3-0-6b
kubectl delete namespace qwen3-0-6b
kubectl delete clusterservingruntime srt-qwen3-0-6b
kubectl delete clusterbasemodel qwen3-0-6b
```

```output
inferenceservice.ome.io "qwen3-0-6b" deleted from qwen3-0-6b namespace
namespace "qwen3-0-6b" deleted
clusterservingruntime.ome.io "srt-qwen3-0-6b" deleted
clusterbasemodel.ome.io "qwen3-0-6b" deleted
```

Deleting the model waits until the model agent has deleted `/mnt/data/models/Qwen/Qwen3-0.6B` from each node that has it. To keep the files, see [Deleting a model](../concepts/models/base-models.md#deleting-a-model).

## Next steps

- [Serve a model on OMENative](../guides/omenative/serve-a-model-on-omenative.md): run this model's engine on OMENative, OME's own workload controller, instead of a Deployment. Since v1.3.
- [Expose a service without ingress](../guides/networking/expose-without-ingress.md): reach the model from outside the cluster through a LoadBalancer or NodePort Service.
- [Pre-configured models and runtimes](pre-configured-models.md): serve more models with the `ome-serving` chart. Its `qwen3-0-6b` entry creates resources with the same names as yours, so clean up first if you turn it on.
- [Serving runtimes](../concepts/runtimes/serving-runtimes.md): how a runtime describes the pods that serve a model, and how OME picks one.
- [InferenceService](../concepts/serving/inference-services.md): the engine, decoder and router components, and what OME creates for each.
