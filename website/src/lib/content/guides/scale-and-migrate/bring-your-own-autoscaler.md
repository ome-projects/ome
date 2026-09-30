---
title: Bring your own autoscaler
description: Turn off OME's autoscaler for a component with the External class, and scale it with your own autoscaler through the scaleTargetRef that OME publishes.
since: v1.3
---

Set a component's [`autoscaler.class`](../../concepts/serving/component-autoscaling.md) to `External` to scale it with an autoscaler that you run: a HorizontalPodAutoscaler or KEDA ScaledObject that you write, or a controller of your own. OME then creates no autoscaler for the component, and publishes the object for yours to scale in `status.components.<component>.scaleTargetRef`. The `autoscaler` field and the `autoscaler` and `scaleTargetRef` status fields are alpha, so they can change between releases, but they need no feature flag. On v1.2.2, the `ome.io/autoscalerClass: external` annotation stops OME from creating a HorizontalPodAutoscaler for a RawDeployment InferenceService.

<div class="prerequisites" markdown>

- The `llama-3-2-3b-instruct` [ClusterBaseModel](../../concepts/models/base-models.md) and the `srt-llama-3-2-3b-instruct` ClusterServingRuntime from [Pre-configured models and runtimes](../../getting-started/pre-configured-models.md), with the model downloaded.
- Three GPUs for the engine: the HorizontalPodAutoscaler in Step 3 keeps at least three Instances, and each takes one.
- A namespace named `prod`. To use an [InferenceService](../../concepts/serving/inference-services.md) that you already have instead of `chat`, add the `autoscaler` block from Step 1 to its component.
- A component that uses [OMENative](../../concepts/omenative/overview.md) or RawDeployment. A `MultiNode` (deprecated) component has no scale target: [move it to OMENative](../omenative/move-from-leaderworkerset.md) first.
- metrics-server, or another resource metrics API server, for the CPU-based HorizontalPodAutoscaler in Step 3.
- Permission to create InferenceServices, HorizontalPodAutoscalers, Roles and RoleBindings in `prod`.

</div>

## Step 1: Declare the class

Set `class: External` in the component's `autoscaler` block. Save this InferenceService as `chat.yaml`:

```yaml title="chat.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: chat
  namespace: prod
spec:
  deploymentMode: OMENative
  model:
    name: llama-3-2-3b-instruct
  runtime:
    name: srt-llama-3-2-3b-instruct
  engine:
    minReplicas: 2
    autoscaler:
      class: External
```

`External` needs no other settings. OME creates the InferenceReplica with the engine's `minReplicas`, 2 Instances here, and then your autoscaler sets the count. A RawDeployment component starts with 1 pod, whatever its `minReplicas`. On a running component, OME deletes the autoscaler that it created, and the count stays put until yours changes it. Add the same block under `decoder` or `router` for any other component that your autoscaler scales.

To hand off the engine of all the InferenceServices that use a runtime and set no autoscaling of their own, put the block in the runtime's `engineConfig` instead. To hold a component at a fixed count, use [`class: None`](../../concepts/serving/component-autoscaling.md#external-and-none).

Apply the file:

```bash
kubectl apply -f chat.yaml
```

```output
inferenceservice.ome.io/chat created
```

Check the class that OME applied to the engine:

```bash
kubectl get inferenceservice chat -n prod -o jsonpath='{.status.components.engine.autoscaler}'
```

```output
{"class":"External","managedBy":"external","specSource":"isvc"}
```

`specSource: isvc` says that the setting came from the InferenceService. An inline `autoscaler` block wins over all other autoscaling settings, as [Which setting wins](../../concepts/serving/component-autoscaling.md#which-setting-wins) shows.

## Step 2: Read the published scale target

Read the object to scale from the engine's status:

```bash
kubectl get inferenceservice chat -n prod -o jsonpath='{.status.components.engine.scaleTargetRef}'
```

```output
{"apiVersion":"ome.io/v1beta1","kind":"InferenceReplica","name":"chat-engine"}
```

The target depends on the component's deployment mode:

| Deployment mode | Scale target | What `spec.replicas` counts |
| --- | --- | --- |
| OMENative | The InferenceReplica `<InferenceService>-<component>`, in `ome.io/v1beta1` | [Instances](../../concepts/omenative/instances.md). An Instance is one pod, or a leader pod and its workers. |
| RawDeployment | The Deployment `<InferenceService>-<component>`, in `apps/v1` | Pods |

Your autoscaler writes the count to the target's `scale` subresource, as `kubectl scale` does, and OME adds or removes Instances to match. Write the count only that way: OME owns the rest of the InferenceReplica's spec and overwrites other changes to it.

## Step 3: Point your autoscaler at the target

Whichever autoscaler you run:

- Point one autoscaler at each target. Two overwrite each other's counts.
- Set the bounds in your autoscaler. OME passes `minReplicas` and `maxReplicas` only to the autoscalers that it creates.
- On OMENative, keep your autoscaler's minimum at 1 or more. OME sets a count of 0 back to `minReplicas`, or to 1 when `minReplicas` is unset or 0.

=== "HorizontalPodAutoscaler"

    This HorizontalPodAutoscaler keeps the engine's CPU use at 70%, with 3 to 8 Instances. It scales on CPU to keep the example simple, and a KEDA ScaledObject can scale on the engine's own metrics from [Prometheus](../operate-ome/metrics.md) instead. Save it as `chat-engine-byo.yaml`:

    ```yaml title="chat-engine-byo.yaml"
    apiVersion: autoscaling/v2
    kind: HorizontalPodAutoscaler
    metadata:
      name: chat-engine-byo
      namespace: prod
    spec:
      scaleTargetRef:
        apiVersion: ome.io/v1beta1
        kind: InferenceReplica
        name: chat-engine
      minReplicas: 3
      maxReplicas: 8
      metrics:
        - type: Resource
          resource:
            name: cpu
            target:
              type: Utilization
              averageUtilization: 70
    ```

    For a RawDeployment component, target `apiVersion: apps/v1`, `kind: Deployment` and `name: chat-engine` instead. Apply the file:

    ```bash
    kubectl apply -f chat-engine-byo.yaml
    ```

    ```output
    horizontalpodautoscaler.autoscaling/chat-engine-byo created
    ```

    The HorizontalPodAutoscaler needs no extra permissions. It raises a count below its `minReplicas` at once, so it takes the engine from 2 Instances to 3. It's named `chat-engine-byo` because OME gives `chat-engine` to its own HorizontalPodAutoscaler under `class: HPA`, and a clash would [block the hand-back](#switching-back-to-hpa-or-keda-fails).

=== "KEDA ScaledObject"

    Put the target from Step 2 in your ScaledObject's `spec.scaleTargetRef`: `apiVersion: ome.io/v1beta1`, `kind: InferenceReplica` and `name: chat-engine`. On OMENative, set its `minReplicaCount` to 1 or more, because it defaults to 0. To have OME create the ScaledObject instead, use [`class: KEDA`](../../concepts/serving/component-autoscaling.md#keda).

=== "Your own controller"

    Your controller needs to read the InferenceService, to find the target, and to update the target's `scale` subresource, which takes a rule of its own. This Role and RoleBinding grant both to the service account `chat-scaler` in `prod`. Save them as `chat-scaler-rbac.yaml`:

    ```yaml title="chat-scaler-rbac.yaml"
    apiVersion: rbac.authorization.k8s.io/v1
    kind: Role
    metadata:
      name: chat-scaler
      namespace: prod
    rules:
      - apiGroups: ["ome.io"]
        resources: ["inferenceservices"]
        verbs: ["get"]
      - apiGroups: ["ome.io"]
        resources: ["inferencereplicas/scale"]
        verbs: ["get", "update", "patch"]
    ---
    apiVersion: rbac.authorization.k8s.io/v1
    kind: RoleBinding
    metadata:
      name: chat-scaler
      namespace: prod
    subjects:
      - kind: ServiceAccount
        name: chat-scaler
        namespace: prod
    roleRef:
      apiGroup: rbac.authorization.k8s.io
      kind: Role
      name: chat-scaler
    ```

    For a RawDeployment component, grant `deployments/scale` in the `apps` API group instead. Apply the file:

    ```bash
    kubectl apply -f chat-scaler-rbac.yaml
    ```

    ```output
    role.rbac.authorization.k8s.io/chat-scaler created
    rolebinding.rbac.authorization.k8s.io/chat-scaler created
    ```

    Then run your controller as `chat-scaler`, in place of the HorizontalPodAutoscaler.

!!! warning "Count Instances, not pods"
    The InferenceReplica counts Instances, but a HorizontalPodAutoscaler works out most counts from the number of ready pods. On a multi-node component, where each Instance is a leader pod and its workers, it asks for too many Instances. Scale such a component on an `Object` or `External` metric with an `AverageValue` target. KEDA's triggers use that target by default, except `cpu` and `memory`.

## Step 4: Check the scaling

List the InferenceReplica, whose short name is `irep`:

```bash
kubectl get inferencereplica chat-engine -n prod
```

```output
NAME          COMPONENT   DESIRED   CURRENT   READY   AVAILABLE   AGE
chat-engine   engine      3         3         2       2           25m
```

`DESIRED` is the count that your autoscaler last wrote: here, the HorizontalPodAutoscaler's minimum of 3, not the engine's `minReplicas` of 2. `CURRENT` counts the Instances that exist, and `READY` and `AVAILABLE` catch up as the new Instance starts. For a RawDeployment component, `kubectl get deployment chat-engine -n prod` shows the same in pods. OME doesn't copy your autoscaler's decisions into its status, so read them from your autoscaler.

## Hand scaling back to OME

To hand a component back to OME, delete your autoscaler first. Then set another class, or remove the `autoscaler` block so that the next source in [Which setting wins](../../concepts/serving/component-autoscaling.md#which-setting-wins) applies.

## Troubleshooting

### Your count doesn't last

Something other than your autoscaler sets the count. Read the class that OME applied, which points to the cause:

```bash
kubectl get inferenceservice chat -n prod -o jsonpath='{.status.components.engine.autoscaler.class}'
```

- `None`: when `minReplicas` is 1 or more, as it is by default, OME sets the count back to it. Set `class: External`.
- `HPA` or `KEDA`: OME's own autoscaler scales the component too. Put the `autoscaler` block under the component that your autoscaler targets. `specSource` in the same status says where the class came from.
- `External`: a second autoscaler targets the same object, or yours writes 0 to an OMENative component. Keep one autoscaler per target, with a minimum of 1 or more.

### `scaleTargetRef` is empty

The Step 2 command prints nothing for a `MultiNode` (deprecated) component, which has no scale target: [move it to OMENative](../omenative/move-from-leaderworkerset.md). Otherwise, OME hasn't set up the component yet, or setting it up failed. Look for a `Reconciler error` line in the controller logs:

```bash
kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1
```

Fix the error that it names, and OME retries on its own.

### Switching back to HPA or KEDA fails

A switch to `class: HPA` or `class: KEDA` fails when an object of yours has the name that OME uses for its own: OME won't take it over. For this engine, those names are `chat-engine` for a HorizontalPodAutoscaler and `scaledobject-chat-engine` for a ScaledObject. Each attempt logs a `Reconciler error` line that names the object and says it `is not controlled by expected owner`. Delete or rename your object, and OME creates its own on the next attempt.

## Clean up

=== "HorizontalPodAutoscaler"

    Delete the HorizontalPodAutoscaler and the InferenceService:

    ```bash
    kubectl delete -f chat-engine-byo.yaml -f chat.yaml
    ```

    ```output
    horizontalpodautoscaler.autoscaling "chat-engine-byo" deleted from prod namespace
    inferenceservice.ome.io "chat" deleted from prod namespace
    ```

=== "KEDA ScaledObject"

    Delete your ScaledObject, then the InferenceService:

    ```bash
    kubectl delete -f chat.yaml
    ```

    ```output
    inferenceservice.ome.io "chat" deleted from prod namespace
    ```

=== "Your own controller"

    Stop your controller. Then delete the Role and RoleBinding, and the InferenceService:

    ```bash
    kubectl delete -f chat-scaler-rbac.yaml -f chat.yaml
    ```

    ```output
    role.rbac.authorization.k8s.io "chat-scaler" deleted from prod namespace
    rolebinding.rbac.authorization.k8s.io "chat-scaler" deleted from prod namespace
    inferenceservice.ome.io "chat" deleted from prod namespace
    ```

## Next steps

- [Component autoscaling](../../concepts/serving/component-autoscaling.md): every autoscaler class, including `None` for a fixed count, and how OME picks a component's settings.
- [Scale to zero with KEDA](scale-to-zero-with-keda.md): let OME's KEDA ScaledObject scale a component to zero.
- [Autoscaler policy](../../concepts/serving/autoscaler-policy.md): share HPA or KEDA settings across InferenceServices. It's alpha and off by default.
- [Request a transient scale](request-a-transient-scale.md): ask once for a different count with `kubectl ome scale`, which is alpha.
- [`kubectl ome autoscale`](../../reference/kubectl-ome/autoscale.md): see the autoscaling in effect for each component, and where it came from.
