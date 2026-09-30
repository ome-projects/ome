---
title: Scale to zero with KEDA
description: "Scale an idle RawDeployment component to zero replicas with a KEDA autoscaler, and back up when a trigger becomes active."
since: v1.3
---

Scale an idle InferenceService component to zero replicas, so that it holds no GPUs, and bring it back when a [KEDA](https://keda.sh) trigger becomes active. Give the component its own `autoscaler` with `class: KEDA` and at least one trigger, and OME creates a KEDA ScaledObject that scales the component's Deployment. This works for components in the RawDeployment mode, the default for a component without a leader and workers: see [Deployment modes](../../concepts/architecture/deployment-modes.md). [OMENative components](#omenative-components-cant-scale-to-zero) keep at least one Instance. The `autoscaler` field is alpha, so its API can change between releases. It needs no feature flag.

<div class="prerequisites" markdown>

- OME, [installed](../../getting-started/install.md) with the Prometheus Operator's [PodMonitor CRD](../../getting-started/install.md#the-podmonitor-crd), which RawDeployment components need.
- [KEDA](https://keda.sh/docs/latest/deploy/), installed in the cluster. The OME controller looks for KEDA's CRDs only when it starts, so if you install KEDA after OME, restart the controller, as [No ScaledObject appears](#keda-isnt-installed) shows.
- The pre-configured model `llama-3-2-1b-instruct` and its runtimes, with the model [Ready](../../concepts/models/base-models.md#model-lifecycle). See [Pre-configured models and runtimes](../../getting-started/pre-configured-models.md).

</div>

## What happens at zero

While a component is at zero, no pod serves it, and requests to it fail. Requests don't wake it up: only a KEDA trigger does. The component then serves again once a new pod is ready, so plan for the time that the engine takes to start and load the model.

Pick triggers whose source keeps reporting while the component is at zero, such as a schedule, a queue, or a metric from a service that stays up. The component's own metrics stop with its pods, so they can't wake it. For a metric in the Prometheus that the `ome-resources` chart deploys, point a `prometheus` trigger at `http://ome-prometheus.ome.svc:9090`, for OME in the namespace `ome`. See [Collect metrics](../operate-ome/metrics.md).

## Step 1: Create an InferenceService that scales to zero

!!! warning "Switch a running component in two steps"
    Move a running component from the default `HPA` class to KEDA in two steps. First set `class: External`, keep `minReplicas` at 1 or more, and apply the file: OME deletes its HorizontalPodAutoscaler and keeps the replica count. Then set `class: KEDA` with its triggers and `minReplicas: 0`, and apply the file again. In a default KEDA install, a switch in one step never completes, as [Component autoscaling](../../concepts/serving/component-autoscaling.md#switch-from-hpa-to-keda) explains.

Create a namespace for the example:

```bash
kubectl create namespace llama-demo
```

```output
namespace/llama-demo created
```

Save this InferenceService as `llama-chat.yaml`. Its engine scales between zero and two replicas, and a KEDA `cron` trigger asks for one replica from 09:00 to 17:00 UTC each day:

```yaml title="llama-chat.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-chat
  namespace: llama-demo
spec:
  model:
    name: llama-3-2-1b-instruct
  engine:
    minReplicas: 0
    maxReplicas: 2
    autoscaler:
      class: KEDA
      keda:
        triggers:
          - type: cron
            metadata:
              timezone: UTC
              start: "0 9 * * *"
              end: "0 17 * * *"
              desiredReplicas: "1"
```

If it's between 09:00 and 17:00 UTC now, move `start` and `end` to earlier hours today, so that the engine scales to zero in Step 3.

OME copies the `keda` block into the ScaledObject as written, so you can use any trigger that your KEDA version supports: see the [ScaledObject specification](https://keda.sh/docs/latest/reference/scaledobject-spec/). The block's optional fields include these:

| Field | What it sets | Default |
| --- | --- | --- |
| `pollingInterval` | How often KEDA checks the triggers, in seconds. | 30 |
| `cooldownPeriod` | How long KEDA waits after the last active trigger before it scales the component to zero, in seconds. | 300 |
| `idleReplicaCount` | The replica count while no trigger is active. See [Keep a minimum while a trigger is active](#keep-a-minimum-while-a-trigger-is-active). | Unset |

Apply the file:

```bash
kubectl apply -f llama-chat.yaml
```

```output
Warning: Runtime vllm-llama-3-2-1b-instruct will be auto-selected for model llama-3-2-1b-instruct
inferenceservice.ome.io/llama-chat created
```

## Step 2: Check the ScaledObject

OME creates the Deployment `llama-chat-engine` and the ScaledObject `scaledobject-llama-chat-engine`. From then on, KEDA sets the Deployment's replica count. Check that the ScaledObject scales the Deployment between the engine's `minReplicas` and `maxReplicas`:

```bash
kubectl get scaledobject scaledobject-llama-chat-engine -n llama-demo -o jsonpath='{.spec.scaleTargetRef.kind}/{.spec.scaleTargetRef.name} {.spec.minReplicaCount}-{.spec.maxReplicaCount}{"\n"}'
```

```output
Deployment/llama-chat-engine 0-2
```

If the output names an `InferenceReplica` instead, the engine runs on OMENative, which [keeps at least one Instance](#omenative-components-cant-scale-to-zero).

## Step 3: Watch the engine scale to zero

Outside the trigger's window, KEDA scales a new engine to zero the first time it checks the trigger. Later, when the window closes, KEDA waits for `cooldownPeriod` before it scales the engine to zero. Check the Deployment's replica count:

```bash
kubectl get deployment llama-chat-engine -n llama-demo -o jsonpath='{.spec.replicas}{"\n"}'
```

```output
0
```

At zero, the InferenceService reports Ready, but its model status is pending:

```bash
kubectl get inferenceservice llama-chat -n llama-demo -o jsonpath='{.status.modelStatus.transitionStatus} {.status.modelStatus.modelRevisionStates.targetModelState}{"\n"}'
```

```output
InProgress Pending
```

When the window opens, KEDA scales the Deployment to one replica, and the engine serves again once its pod is ready.

## Keep a minimum while a trigger is active

To idle at zero but keep a minimum while a trigger is active, set `keda.idleReplicaCount` to 0 and `minReplicas` to that minimum. Change `llama-chat.yaml` to this:

```yaml title="llama-chat.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-chat
  namespace: llama-demo
spec:
  model:
    name: llama-3-2-1b-instruct
  engine:
    minReplicas: 2
    maxReplicas: 4
    autoscaler:
      class: KEDA
      keda:
        idleReplicaCount: 0
        triggers:
          - type: cron
            metadata:
              timezone: UTC
              start: "0 9 * * *"
              end: "0 17 * * *"
              desiredReplicas: "3"
```

Apply the file:

```bash
kubectl apply -f llama-chat.yaml
```

```output
Warning: Runtime vllm-llama-3-2-1b-instruct will be auto-selected for model llama-3-2-1b-instruct
inferenceservice.ome.io/llama-chat configured
```

Check the ScaledObject's counts:

```bash
kubectl get scaledobject scaledobject-llama-chat-engine -n llama-demo -o jsonpath='{.spec.minReplicaCount}-{.spec.maxReplicaCount} {.spec.idleReplicaCount}{"\n"}'
```

```output
2-4 0
```

While no trigger is active, KEDA scales the engine to zero. While the trigger is active, it scales the engine between 2 and 4 replicas. `idleReplicaCount` must be lower than `minReplicas`, so leave it out when `minReplicas` is 0.

## OMENative components can't scale to zero

An [OMENative](../../concepts/omenative/overview.md) component keeps at least one Instance. With `minReplicas: 0`, the webhook admits the InferenceService, but OME sets the ScaledObject's `minReplicaCount` to 1, and `kubectl ome autoscale explain` shows `unsupported` in the ZERO row. With `keda.idleReplicaCount: 0`, `explain` shows `yes` in the ZERO row, but while no trigger is active the component drops to one Instance. OME then sets the count back to `minReplicas`, so the component can grow back to `minReplicas` Instances.

A component with a leader and workers can't scale to zero either: it runs on OMENative, or in `MultiNode` (deprecated), which gets no autoscaler.

To scale to zero, run the component as a RawDeployment. The InferenceService's `spec.deploymentMode`, or an `ome.io/deploymentMode` annotation on the InferenceService or its runtime, can make a component OMENative: see [How OME resolves the mode](../../concepts/architecture/deployment-modes.md#how-ome-resolves-the-mode).

## Troubleshooting

### The webhook rejects the InferenceService

`kubectl apply` fails with a message that contains one of these reasons:

| Reason | Fix |
| --- | --- |
| `InvalidScaleToZero` | Give the component its own `autoscaler` with `class: KEDA` and a trigger, as in [Step 1](#step-1-create-an-inferenceservice-that-scales-to-zero). KEDA settings from the runtime or an [AutoscalerPolicy](../../concepts/serving/autoscaler-policy.md) don't count. |
| `KedaTriggersRequired` | Add `keda.triggers` with at least one trigger. |
| `KedaIdleBelowMin` | Set `minReplicas` above `keda.idleReplicaCount`, or leave out `idleReplicaCount` and set `minReplicas: 0`. |
| `AutoscalerAnnotationConflict` | Remove the `ome.io/autoscalerClass` annotation. The component's `autoscaler` replaces it. |

The `InvalidScaleToZero` message also suggests the `ome.io/autoscalerClass=keda` annotation. Use the component's own `autoscaler` instead: OME needs its triggers to create the ScaledObject.

### No ScaledObject appears {#keda-isnt-installed}

The webhook admits the InferenceService, but OME fails to create the ScaledObject, so the component never scales to zero. OME records no event, and leaves the InferenceService's status and ingress as they were. Read the logs of every manager replica:

```bash
kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1
```

Each failed attempt logs `Failed to reconcile component` with an error:

| In the logs | Cause | Fix |
| --- | --- | --- |
| `won't watch keda.sh/v1alpha1/ScaledObject resources` when the pod starts, then an error about the ScaledObject | KEDA isn't installed, or you installed it after OME. | Install KEDA, then restart the controller. |
| `is already managed by the hpa 'llama-chat-engine'` | The component switched to KEDA in one step, and OME's HorizontalPodAutoscaler still scales it. | Delete that HorizontalPodAutoscaler. |
| `minReplicas=0 requires typed KEDA with at least one trigger`, or `KEDA autoscaler requires at least one trigger` | The `ome.io/autoscalerClass: keda` annotation asks for KEDA, but carries no triggers. | Remove the annotation, and give the component its own `autoscaler`. |

To restart the controller after you install KEDA, so that it watches ScaledObjects:

```bash
kubectl rollout restart deployment/ome-controller-manager -n ome
```

```output
deployment.apps/ome-controller-manager restarted
```

To delete OME's HorizontalPodAutoscaler after a switch in one step:

```bash
kubectl delete hpa llama-chat-engine -n llama-demo
```

```output
horizontalpodautoscaler.autoscaling "llama-chat-engine" deleted from llama-demo namespace
```

With `class: KEDA`, OME doesn't create it again, and it creates the ScaledObject when it retries.

### The component doesn't wake up

KEDA scales the component up only when a trigger becomes active, and a trigger on the component's own metrics has nothing to read at zero. Use a trigger whose source keeps reporting at zero, as [What happens at zero](#what-happens-at-zero) describes. The ScaledObject's `Active` condition, in the InferenceService's `status.components.engine.autoscaler.conditions`, shows whether a trigger is active.

## Clean up

Delete the InferenceService and its namespace:

```bash
kubectl delete inferenceservice llama-chat -n llama-demo
kubectl delete namespace llama-demo
```

```output
inferenceservice.ome.io "llama-chat" deleted from llama-demo namespace
namespace "llama-demo" deleted
```

Deleting the InferenceService also deletes its ScaledObject.

## Next steps

- [Component autoscaling](../../concepts/serving/component-autoscaling.md): how OME picks and runs each component's autoscaler.
- [Autoscaler policy](../../concepts/serving/autoscaler-policy.md): share KEDA or HPA settings across InferenceServices with an alpha AutoscalerPolicy.
- [Bring your own autoscaler](bring-your-own-autoscaler.md): scale a component with an autoscaler that you run yourself.
- [kubectl ome autoscale](../../reference/kubectl-ome/autoscale.md): see the autoscaling in effect for each component, and where it came from.
