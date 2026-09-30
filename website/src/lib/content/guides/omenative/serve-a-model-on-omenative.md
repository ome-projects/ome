---
title: Serve a model on OMENative
description: Opt a single-pod InferenceService into OMENative with spec.deploymentMode, then check the InferenceReplica, the Instance and the pod that OME creates.
since: v1.3
---

Run a single-pod model on [OMENative](../../concepts/omenative/overview.md), OME's own workload controller, by setting `spec.deploymentMode: OMENative` on its [InferenceService](../../concepts/serving/inference-services.md). OMENative runs each replica as an [Instance](../../concepts/omenative/instances.md) and reports its phase and last failure. It also stops retrying an image that can't be pulled, and lets you choose how an update replaces the pod.

An engine or decoder with a `leader` or `worker` runs on OMENative by default, and a single-pod component runs as a Deployment until you opt it in. Here you opt in the model from [Serve your first model](../../getting-started/serve-your-first-model.md), check what OME creates for it, and send it a request.

<div class="prerequisites" markdown>

- OME v1.3 or later, installed with the `ome-resources` chart, and `kubectl`, with the rights to create namespaces and InferenceServices. See [Install OME](../../getting-started/install.md).
- The ClusterBaseModel `qwen3-0-6b` in the `Ready` state, and the ClusterServingRuntime `srt-qwen3-0-6b`, both from [Serve your first model](../../getting-started/serve-your-first-model.md). Check with `kubectl get clusterbasemodel qwen3-0-6b` and `kubectl get clusterservingruntime srt-qwen3-0-6b`.
- A node where the model is `Ready`, with a free NVIDIA GPU, 10 CPUs and 30 GiB of memory for the engine pod. If the InferenceService from Serve your first model still holds the GPU, delete it with `kubectl delete inferenceservice qwen3-0-6b -n qwen3-0-6b`, and keep the model and the runtime.
- The [kubectl ome](../../reference/kubectl-ome/overview.md) plugin, for Step 3.

</div>

## Step 1: Create the InferenceService

Create a namespace for the InferenceService:

```bash
kubectl create namespace qwen3-native
```

```output
namespace/qwen3-native created
```

Save this InferenceService as `qwen3-0-6b-native.yaml`. It's the one from Serve your first model, with the new namespace and `spec.deploymentMode`:

```yaml title="qwen3-0-6b-native.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: qwen3-0-6b
  namespace: qwen3-native
spec:
  deploymentMode: OMENative
  model:
    name: qwen3-0-6b
  runtime:
    name: srt-qwen3-0-6b
  engine:
    minReplicas: 1
    maxReplicas: 1
```

`deploymentMode: OMENative` runs every component of the InferenceService on OMENative, here only the engine. `minReplicas` and `maxReplicas` count Instances, not pods. An Instance of this engine is one pod, so the engine runs one pod.

Create the InferenceService:

```bash
kubectl apply -f qwen3-0-6b-native.yaml
```

```output
inferenceservice.ome.io/qwen3-0-6b created
```

Wait for the InferenceService to be ready. Pulling the SGLang image and loading the model can take several minutes:

```bash
kubectl wait --for=condition=Ready inferenceservice/qwen3-0-6b -n qwen3-native --timeout=30m
```

```output
inferenceservice.ome.io/qwen3-0-6b condition met
```

## Step 2: Check the InferenceReplica

OME runs an OMENative component from an [InferenceReplica](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-InferenceReplica) named `{isvc}-{component}`, in place of a Deployment. List the InferenceReplicas, `irep` for short:

```bash
kubectl get inferencereplicas -n qwen3-native
```

```output
NAME                COMPONENT   DESIRED   CURRENT   READY   AVAILABLE   AGE
qwen3-0-6b-engine   engine      1         1         1       1           6m
```

The columns count Instances. `DESIRED` is the number that OME keeps, here the engine's `minReplicas`, and `READY` counts the Instances whose pods are all ready. [Readiness and availability](../../concepts/omenative/instances.md#readiness-and-availability) explains `AVAILABLE`.

No Deployment runs the engine:

```bash
kubectl get deployments -n qwen3-native
```

```output
No resources found in qwen3-native namespace.
```

Make your changes in the InferenceService. OME keeps the InferenceReplica's spec in line with the InferenceService and its runtime.

## Step 3: Check the Instance and its pod

List the Instances of the InferenceService with kubectl ome:

```bash
kubectl ome instance list qwen3-0-6b -n qwen3-native
```

```output
COMP     IDX/INC   PHASE   PODS    REVS          AOF   EVIDENCE
engine   0/1       Ready   1/1/1   qwen...9c7a   A--   OK
```

- `IDX/INC` is `0/1`: Instance 0, in its first [incarnation](../../concepts/omenative/instances.md#incarnation).
- `PHASE` is `Ready`. While the pod starts, it's `Creating`.
- `PODS` counts the Instance's serving, available and total pods.

[Output fields](../../reference/kubectl-ome/instance.md#list-output-fields) explains the other columns, and [Instance phases](../../concepts/omenative/instances.md#instance-phases) describes the phases.

OME names the pod `{isvc}-{component}-{index}-{runner}-{ordinal}`, where the runner of a single-pod Instance is `default`. List it with its `ome.io/serving` condition:

```bash
kubectl get pods -n qwen3-native -l ome.io/inferenceservice=qwen3-0-6b \
  -o 'custom-columns=NAME:.metadata.name,SERVING:.status.conditions[?(@.type=="ome.io/serving")].status'
```

```output
NAME                            SERVING
qwen3-0-6b-engine-0-default-0   True
```

`ome.io/serving` is a readiness gate that OME controls. The pod is ready, and receives requests, only while it's `True`. OME sets it to `False` to take the pod out of rotation without stopping it, for example to drain it. [Pod labels and environment](../../concepts/architecture/deployment-modes.md#pod-labels-and-environment) lists the labels and `OME_*` environment variables that OME adds to the pod.

## Step 4: Send a request

Forward a local port to the Service `qwen3-0-6b`, as in Serve your first model, and leave the command running. If you turned on ingress creation, forward to `svc/qwen3-0-6b-engine` instead.

```bash
kubectl port-forward svc/qwen3-0-6b 8080:8080 -n qwen3-native
```

```output
Forwarding from 127.0.0.1:8080 -> 8080
Forwarding from [::1]:8080 -> 8080
```

In a second terminal, send the chat completion request from Serve your first model. `chat_template_kwargs` turns off Qwen3's thinking mode, which can use up `max_tokens`:

```bash
curl -s http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model": "Qwen/Qwen3-0.6B", "messages": [{"role": "user", "content": "What is Kubernetes? Answer in one sentence."}], "max_tokens": 100, "chat_template_kwargs": {"enable_thinking": false}}'
```

The response is an OpenAI chat completion, with the model's answer in `choices[0].message.content`.

## Move a running InferenceService to OMENative

To move an InferenceService that runs as a Deployment, such as the one from Serve your first model, add `deploymentMode: OMENative` to its `spec` and apply it again. OME leaves the old Deployment running. Until you delete it with `kubectl delete deployment qwen3-0-6b-engine -n qwen3-0-6b`, its pod keeps its GPU, and the new pod can stay `Pending`.

## Troubleshooting

### The pod stays Pending

Read the scheduler's message in the pod's events with `kubectl describe pod -n qwen3-native -l ome.io/inferenceservice=qwen3-0-6b`. The reasons are the ones in [The engine pod stays Pending](../../getting-started/serve-your-first-model.md#the-engine-pod-stays-pending), in Serve your first model. Also check whether the InferenceService from that guide, or a Deployment left from a mode change, still holds the GPU.

### The Instance fails

When `kubectl ome instance list` shows the phase `Failed`, run `kubectl ome instance status qwen3-0-6b 0 --component engine -n qwen3-native`. It shows the Instance's last failure, its pods and their warning events. Read SGLang's logs with `kubectl logs -n qwen3-native -l ome.io/inferenceservice=qwen3-0-6b --tail=100`.

With the chart's settings, OME fails a new Instance when:

- a container of its pod is stuck, such as in `ImagePullBackOff` or `CrashLoopBackOff`, and the pod is more than 60 seconds old;
- its pod stays unschedulable for 15 minutes;
- it takes more than 30 minutes to become ready, excluding any time its pod spends unschedulable.

[Set Instance readiness deadlines](set-instance-readiness-deadlines.md) explains how to change these deadlines.

OME can rebuild a failed Instance on its own. When the image can't be pulled, the chart's settings give the revision three attempts in all. Then OME holds the revision and stops rebuilding the Instance. [What OME does with a failed Instance](../../concepts/omenative/instances.md#what-ome-does-with-a-failed-instance) describes the retries.

If your fix changes the pod template, such as the image, OME rolls the new revision out to every Instance of the engine, the failed one included. For any other fix, [release the held revision](../roll-out-changes/release-a-held-revision.md) if OME holds it, and otherwise [reset the failed Instance](reset-failed-instances.md).

### The engine runs as a Deployment

If `kubectl get deployments -n qwen3-native` lists `qwen3-0-6b-engine` and there's no InferenceReplica, look for an `ome.io/deploymentMode` annotation on the engine: it wins over `spec.deploymentMode`. Look in the InferenceService with `kubectl get inferenceservice qwen3-0-6b -n qwen3-native -o jsonpath='{.spec.engine.annotations}'`. Look in the runtime with `kubectl get clusterservingruntime srt-qwen3-0-6b -o jsonpath='{.spec.engineConfig.annotations}'`.

Set `ome.io/deploymentMode: OMENative` in the InferenceService's `spec.engine.annotations`, which wins over the runtime's annotation. Then delete the Deployment that OME leaves behind with `kubectl delete deployment qwen3-0-6b-engine -n qwen3-native`. [How OME resolves the mode](../../concepts/architecture/deployment-modes.md#how-ome-resolves-the-mode) gives the full order.

### No pod appears

If the InferenceReplica exists but its `CURRENT`, `READY` and `AVAILABLE` columns stay empty, check that the manager runs the InferenceReplica controller. Read the startup logs of every manager replica:

```bash
kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1 | grep 'InferenceReplica controller'
```

A replica that runs the controller logs `Setting up InferenceReplica controller`. If one logs `InferenceReplica controller disabled via --enable-inferencereplica-controller=false`, the `manager` container of the `ome-controller-manager` Deployment has that flag. The chart doesn't set it, so remove it where it was added.

### The webhook rejects the InferenceService

The webhook rejects the InferenceService when the model or the runtime doesn't exist, or when the model has no recorded format. [The InferenceService is rejected or gets no pods](../../getting-started/serve-your-first-model.md#the-webhook-rejects-the-inferenceservice), in Serve your first model, lists the messages and their fixes.

## Clean up

Delete the InferenceService and its namespace:

```bash
kubectl delete inferenceservice qwen3-0-6b -n qwen3-native
kubectl delete namespace qwen3-native
```

```output
inferenceservice.ome.io "qwen3-0-6b" deleted from qwen3-native namespace
namespace "qwen3-native" deleted
```

Deleting the namespace waits until OME has drained and deleted the pod. If a pod stays `Terminating`, such as on a node that's gone, see [Recover stuck deletions](recover-stuck-deletions.md).

The model and the runtime stay. To delete them too, follow [Clean up](../../getting-started/serve-your-first-model.md#clean-up) in Serve your first model.

## Next steps

- [Serve a multi-node model](serve-a-multi-node-model.md): give the engine a leader and workers, which OMENative runs as multi-pod Instances.
- [OMENative overview](../../concepts/omenative/overview.md): what OMENative does, and how its parts fit together.
- [Instances](../../concepts/omenative/instances.md): the phases, operations and incarnations of an Instance.
- [OMENative update strategies](../../concepts/architecture/omenative-update-strategies.md): how OME replaces an Instance's pods when you change the InferenceService. The default needs room for a second pod, so with one free GPU, choose `RecreatePod`.
- [Deployment modes and OMENative](../../concepts/architecture/deployment-modes.md): the modes compared, and everything OME creates in each.
- [kubectl ome instance](../../reference/kubectl-ome/instance.md): inspect Instances and their failures.
