---
title: Pace rollouts with minReadySeconds
description: Make a new OMENative pod stay Ready for a warm-up window before it counts as Available, so a rollout waits before draining the old pods.
since: v1.3
---

A model server can pass its readiness probe and still fail while it warms up under its first requests. `lifecycle.minReadySeconds` gives each new pod a warm-up window. In a rollout with the default strategy, OME keeps the old pods serving until the new ones have stayed Ready for the whole window. It works like a Deployment's `minReadySeconds`, for components that run on [OMENative](../../concepts/omenative/overview.md).

You can set it on an [InferenceService](../../concepts/serving/inference-services.md), or as a default in a runtime or for the whole cluster.

<div class="prerequisites" markdown>

- OME v1.3 or later, installed with the `ome-resources` chart as in [Install OME](../../getting-started/install.md), and `kubectl` access to the cluster. Until v1.3 is released, run [a build of `main`](../../getting-started/install.md#install-from-source).
- A namespace `llama-demo`.
- The ClusterBaseModel `llama-3-2-1b-instruct` in the `Ready` state, and the [ClusterServingRuntime](../../concepts/runtimes/serving-runtimes.md) `srt-llama-3-2-1b-instruct`. Both are in the [catalog of pre-configured models](../../getting-started/pre-configured-models.md). Check with `kubectl get clusterbasemodel llama-3-2-1b-instruct` and `kubectl get clusterservingruntime srt-llama-3-2-1b-instruct`.
- Room for one serving pod on an amd64 node, with an NVIDIA GPU, 10 CPUs and 30 GiB of memory. A rollout with the default strategy needs room for a second pod while it replaces the first.

</div>

## Ready versus available

Each replica of an OMENative component is an Instance: one pod, or a leader and its workers. A new pod is Ready, and receives requests, once its containers pass their readiness checks and OME puts it into rotation. It's Available once it has stayed Ready for `minReadySeconds`. The window delays what OME does next:

| When | What waits for the window |
| --- | --- |
| A `SurgeThenDrain` rollout, the default | Draining the old pods, which keep serving until then. |
| A `RecreatePod`, `InPlaceIfPossible` or `InPlaceOnly` rollout | The updated Instance's phase turning `Ready` on the new revision. |
| First deploy and scale-up | The new Instance's phase turning `Ready`. |
| An Instance migration | Draining the source Instance, which keeps serving until then. The window counts toward the migration's [deadline](../../concepts/omenative/migration-and-transient-scale.md#deadline). |

The InferenceService's `Ready` condition doesn't wait for the window. An Instance that's being replaced holds its place in the `maxSurge` or `maxUnavailable` budget for the whole window. So a rollout takes at least one window longer for each batch of Instances that move together. If a new pod stops being Ready during its window, the window starts over. [Readiness and availability](../../concepts/omenative/instances.md#readiness-and-availability) defines the states.

## Step 1: Set a window on an InferenceService

Create an InferenceService that runs in OMENative mode, with a 900-second window on its engine:

```yaml title="isvc.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-chat
  namespace: llama-demo
spec:
  deploymentMode: OMENative
  model:
    name: llama-3-2-1b-instruct
  runtime:
    name: srt-llama-3-2-1b-instruct
  engine:
    minReplicas: 1
    maxReplicas: 1
    lifecycle:
      minReadySeconds: 900
```

`spec.deploymentMode: OMENative` puts every component of the service on OMENative. In RawDeployment, MultiNode (deprecated) or any other mode, OME ignores `minReadySeconds`, and the webhook admits it without a warning. [How OME resolves the mode](../../concepts/architecture/deployment-modes.md#how-ome-resolves-the-mode) lists what else sets a component's mode.

Make the window longer than your readiness probe takes to catch a failure: about `failureThreshold` times the longer of `periodSeconds` and `timeoutSeconds`. Otherwise a pod can become Available before a failing probe marks it not Ready. The engine of `srt-llama-3-2-1b-instruct` probes every 60 seconds, waits up to 200 seconds for an answer and allows 3 failures. It catches a server that returns errors in about 3 minutes and one that hangs in about 10, so 900 seconds covers both.

Apply the file:

```bash
kubectl apply -f isvc.yaml
```

```output
inferenceservice.ome.io/llama-chat created
```

The first start pulls the SGLang image and loads the model, which can take several minutes.

## Step 2: Check the window in effect {#step-2-check-the-workload}

OME writes an InferenceReplica for each OMENative component, named `<service>-<component>`, here `llama-chat-engine`. Its `spec.minReadySeconds` is the window in effect, wherever it's set. Its status counts the Instances whose pods are all Ready, and those that are Available:

```bash
kubectl get inferencereplica llama-chat-engine -n llama-demo \
  -o custom-columns=NAME:.metadata.name,MIN-READY:.spec.minReadySeconds,READY:.status.readyReplicas,AVAILABLE:.status.availableReplicas
```

During the window, the output looks like this:

```output
NAME                MIN-READY   READY   AVAILABLE
llama-chat-engine   900         1       <none>
```

`<none>` means 0. Once the pod has been Ready for 900 seconds, `AVAILABLE` shows `1`. The InferenceService reports the same counts in `status.components.engine.lifecycle`. Its spec shows only the value you set there.

## Set a default for many services

OME takes each component's window from the first of these that sets it, and uses 0 when none does:

| Set in | Field |
| --- | --- |
| The InferenceService | `spec.engine.lifecycle.minReadySeconds`, or the same under `decoder` or `router`. It wins even when it's `0`. |
| The service's ServingRuntime or ClusterServingRuntime | `spec.engineConfig.lifecycle.minReadySeconds`, or the same under `decoderConfig` or `routerConfig`. |
| The cluster | `ome.controller.minReadySeconds` in the `ome-resources` chart, unset by default. |

The value is a whole number of seconds, 0 or more.

### In a runtime

Set a 1200-second default on the engine of `srt-llama-3-2-1b-instruct`:

```bash
kubectl patch clusterservingruntime srt-llama-3-2-1b-instruct --type merge \
  -p '{"spec":{"engineConfig":{"lifecycle":{"minReadySeconds":1200}}}}'
```

```output
clusterservingruntime.ome.io/srt-llama-3-2-1b-instruct patched
```

`llama-chat` sets its own value, so it keeps 900. Remove it, and the runtime's default takes over:

```bash
kubectl patch inferenceservice llama-chat -n llama-demo --type merge \
  -p '{"spec":{"engine":{"lifecycle":{"minReadySeconds":null}}}}'
```

```output
inferenceservice.ome.io/llama-chat patched
```

```bash
kubectl get inferencereplica llama-chat-engine -n llama-demo -o jsonpath='{.spec.minReadySeconds}'
```

```output
1200
```

A service can pin its runtime with `spec.runtime.autoSync: false`. When you change the runtime, OME sets that service's `RuntimeDrifted` condition to True and leaves its serving workload as it is until you move the pin. See [Runtime revisions and pinning](../../concepts/runtimes/runtime-revisions.md#when-the-runtime-changes).

### For the whole cluster

Add the default to the values file that you installed `ome-resources` with:

```yaml title="values.yaml"
ome:
  controller:
    minReadySeconds: 60
```

Keep your other values in the file: `helm upgrade` resets every value that isn't in it to the chart's default. Upgrade with the chart version that you already run, so that OME stays in step with the `ome-crd` chart. `helm list -n ome` shows it in its `CHART` column:

```bash
helm upgrade --install ome oci://ghcr.io/moirai-internal/charts/ome-resources \
  --namespace ome --version 1.3.0 -f values.yaml
```

Until v1.3 is released, upgrade from a checkout of `main` instead, as in [Install from source](../../getting-started/install.md#install-from-source).

Helm reports that the release has been upgraded. The upgrade restarts the controller, which then applies the new default. Check the `deploy` entry of the `inferenceservice-config` ConfigMap:

```bash
kubectl get configmap inferenceservice-config -n ome -o jsonpath='{.data.deploy}' | grep minReadySeconds
```

```output
  "minReadySeconds": 60,
```

If you manage the ConfigMap yourself, add `"minReadySeconds": 60` to the JSON in its `deploy` entry. A later `helm upgrade` can overwrite the edit. [Change ConfigMap settings](../operate-ome/configure-the-controller.md#tune-the-config-cache) explains when an edit takes effect, and how to apply it at once.

## Troubleshooting

### The controller crash-loops or services stop updating

Nothing checks the cluster default when you set it. With a negative value, such as `-30`, new controller pods exit with `invalid deploy config, minReadySeconds must be >= 0, got -30`. A running controller logs the same error for every InferenceService and stops creating or updating serving workloads. Read the logs of every replica, and look for that error:

```bash
kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1
```

Set the value to 0 or more in your values file or the ConfigMap, then upgrade again or restart the controller.

### The value doesn't seem to apply

Check `spec.minReadySeconds` on the component's InferenceReplica, as in [Step 2](#step-2-check-the-workload), then find your case:

| What you see | Cause and fix |
| --- | --- |
| kubectl reports that the InferenceReplica isn't found | The component doesn't run on OMENative. Set `spec.deploymentMode: OMENative`, and remove any `ome.io/deploymentMode` annotation that names another mode, on the component or in the runtime. |
| The value isn't the runtime's | The InferenceService sets its own value, even `0`. Or it pins its runtime with `spec.runtime.autoSync: false`, and gets the change once you move the pin. |
| Pods didn't restart | Expected: the window isn't part of the pod template. OME uses a new value from then on, in rollouts in progress too. |

## Clean up

Delete the InferenceService:

```bash
kubectl delete inferenceservice llama-chat -n llama-demo
```

```output
inferenceservice.ome.io "llama-chat" deleted from llama-demo namespace
```

If you set the runtime default, remove it:

```bash
kubectl patch clusterservingruntime srt-llama-3-2-1b-instruct --type merge \
  -p '{"spec":{"engineConfig":{"lifecycle":{"minReadySeconds":null}}}}'
```

```output
clusterservingruntime.ome.io/srt-llama-3-2-1b-instruct patched
```

If you set the cluster default, remove `minReadySeconds` from your values file, and run the `helm upgrade` command again with the same version.

## Next steps

- [OMENative update strategies](../../concepts/architecture/omenative-update-strategies.md): how SurgeThenDrain and the other strategies replace an Instance's pods, and how `partition`, `maxSurge` and `maxUnavailable` pace a rollout.
- [Rollout groups](../../concepts/rollouts-and-traffic/rollout-groups.md): roll the engine and the decoder out as one, by canary, blue-green or rolling update.
- [Pause and resume a rollout](pause-and-resume-a-rollout.md): hold a rollout in progress with the alpha `kubectl ome rollout pause` action, and continue it with `resume`.
- [LifecycleSpec](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-LifecycleSpec) in the API reference: the other lifecycle fields of a component.
