---
title: Configure pod disruption budgets
description: Control how many of a component's pods a node drain can take down, with a budget on each component or defaults for each deployment mode.
---

A PodDisruptionBudget limits how many of a component's pods evictions can take down at once, as when `kubectl drain` empties a node. On a Helm install, OME gives each [RawDeployment](../../concepts/architecture/deployment-modes.md) and [OMENative](../../concepts/omenative/overview.md) component a default budget of `maxUnavailable: 1`, so evictions take its pods one at a time. You can set your own budget on a component, or change the default for all components in a mode.

A PodDisruptionBudget limits evictions only. Rolling updates and OME's own changes delete pods rather than evict them, so the budget doesn't slow them down.

<div class="prerequisites" markdown>

- OME installed, and `kubectl` access to the cluster. See [Install OME](../../getting-started/install.md).
- The InferenceService `qwen3-0-6b` from [Serve your first model](../../getting-started/serve-your-first-model.md), with its engine in the RawDeployment mode, and room in the cluster for three more engine pods. An engine pod needs a GPU, 10 CPUs and 30 GiB of memory.
- For Step 2, OME v1.3 or later. On a Helm install, you also need Helm and the values file of the `ome-resources` release `ome` in the namespace `ome`. If you've lost the file, `helm get values ome -n ome` prints the values you set.

</div>

## How OME picks a budget

The engine, the decoder and the router each take `minAvailable` or `maxUnavailable`, as in `spec.engine.minAvailable`. Set one of the two, to a number of pods or to a percentage of the component's pods from `0%` to `100%`. A runtime can set them too, in its `engineConfig`, `decoderConfig` or `routerConfig`, and a field that the InferenceService sets overrides the same field from the runtime.

A component that sets neither field gets its mode's default. Since v1.3, the `ome-resources` chart sets the RawDeployment and OMENative defaults to `maxUnavailable: 1`, and [Step 2](#step-2-set-the-operator-defaults-for-each-mode) changes them. A manifest install has no defaults, so only the components that set a field get a PodDisruptionBudget. On v1.2.2, a RawDeployment component that sets neither field always gets `maxUnavailable: 1`.

| Budget | Of four pods, evictions can take down | Notes |
| --- | --- | --- |
| `maxUnavailable: 1` | One at a time | The chart's default. A one-pod component can still lose its only pod. |
| `minAvailable: 50%` | Two at a time | A percentage follows the replica count, rounded up to whole pods. |
| `minAvailable: 0` | All four | Opts the component out of its mode's default. |
| `minAvailable: 100%` | None | Blocks evictions, so node drains stall. |

!!! warning "Upgrading a manifest install from v1.2.2"
    When you upgrade a manifest install from v1.2.2, OME deletes the PodDisruptionBudgets of the RawDeployment components that set neither field. To keep them, [move to the Helm charts](../operate-ome/move-to-the-helm-charts.md) before you upgrade, or set the defaults afterwards, as in the kubectl tab of [Step 2](#step-2-set-the-operator-defaults-for-each-mode).

## Step 1: Set a budget on a component

Give the engine four pods, and keep at least half of them available. In `qwen3-0-6b-isvc.yaml`, raise `minReplicas` and `maxReplicas`, and add `minAvailable`:

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
    minReplicas: 4
    maxReplicas: 4
    minAvailable: 50%
```

To cap how many pods can be down instead, set `maxUnavailable` in place of `minAvailable`. Apply the file:

```bash
kubectl apply -f qwen3-0-6b-isvc.yaml
```

```output
inferenceservice.ome.io/qwen3-0-6b configured
```

OME scales the Deployment `qwen3-0-6b-engine` to four pods, and creates or updates the PodDisruptionBudget `qwen3-0-6b-engine`. Wait for the four pods to be ready:

```bash
kubectl wait deployment qwen3-0-6b-engine -n qwen3-0-6b --for=jsonpath='{.status.readyReplicas}'=4 --timeout=20m
```

```output
deployment.apps/qwen3-0-6b-engine condition met
```

List the PodDisruptionBudgets of the InferenceService:

```bash
kubectl get pdb -n qwen3-0-6b -l ome.io/inferenceservice=qwen3-0-6b
```

```output
NAME                MIN AVAILABLE   MAX UNAVAILABLE   ALLOWED DISRUPTIONS   AGE
qwen3-0-6b-engine   50%             N/A               2                     45m
```

Kubernetes resolves `50%` against the Deployment's four replicas, so two pods must stay available. `ALLOWED DISRUPTIONS` is how many pods evictions can take down now: two, while all four pods are healthy. At `0`, Kubernetes refuses to evict the engine's pods.

OME sets the budget and selector back when you edit the PodDisruptionBudget, so change the budget in the InferenceService instead.

## Step 2: Set the defaults for each mode {#step-2-set-the-operator-defaults-for-each-mode since=v1.3}

A mode's default applies in every namespace. Like a component's budget, it takes `minAvailable` or `maxUnavailable`. Set the defaults through the chart on a Helm install, or in the ConfigMap on a manifest install.

=== "Helm"

    Set the defaults under `ome.controller.podDisruptionBudget` in your values file. This one sets the RawDeployment default to `maxUnavailable: 25%`, and keeps the chart's default for OMENative:

    ```yaml title="values.yaml"
    ome:
      controller:
        podDisruptionBudget:
          rawDeployment:
            maxUnavailable: 25%
    ```

    A mode that you leave out keeps the chart's `maxUnavailable: 1`. To turn a mode's default off, set the mode to `null`, as in `omeNative: null`.

    !!! warning "Clear `maxUnavailable` when you switch a mode to `minAvailable`"
        Helm merges your values with the chart's, so a mode that you give only `minAvailable` keeps the chart's `maxUnavailable: 1` too. With both fields in a mode, OME [stops updating every InferenceService](#every-inferenceservice-stops-reconciling). Set `maxUnavailable: null` in the same mode:

        ```yaml title="values.yaml"
        ome:
          controller:
            podDisruptionBudget:
              rawDeployment:
                minAvailable: 50%
                maxUnavailable: null
        ```

    Keep your other install values in the file, since `helm upgrade` resets any value that the file leaves out. The `CHART` column of `helm list -n ome` shows the chart version you run, such as `ome-resources-1.3.0`. Upgrade the release with that version:

    ```bash
    helm upgrade --install ome oci://ghcr.io/moirai-internal/charts/ome-resources \
      --namespace ome --version 1.3.0 -f values.yaml
    ```

    Helm prints `Release "ome" has been upgraded. Happy Helming!` and the release's new revision. The upgrade restarts the manager, which applies the new defaults.

=== "kubectl"

    Set the `podDisruptionBudget` key of the `inferenceservice-config` ConfigMap, in the manager's namespace `ome`, to the JSON that the chart would write. This value sets the RawDeployment default to `maxUnavailable: 25%`, and gives OMENative the chart's `maxUnavailable: 1`:

    ```bash
    kubectl patch configmap inferenceservice-config -n ome --type merge \
      -p '{"data":{"podDisruptionBudget":"{\"omeNative\":{\"maxUnavailable\":1},\"rawDeployment\":{\"maxUnavailable\":\"25%\"}}"}}'
    ```

    ```output
    configmap/inferenceservice-config patched
    ```

    Write a percentage as a JSON string, as in `"25%"`. A mode that you leave out has no default. Restart the manager so that it applies the new defaults now:

    ```bash
    kubectl rollout restart deployment ome-controller-manager -n ome
    ```

    ```output
    deployment.apps/ome-controller-manager restarted
    ```

    On a Helm install, the next `helm upgrade` writes the chart's value back over your patch.

Wait for the new manager pods:

```bash
kubectl rollout status deployment ome-controller-manager -n ome
```

When they're running, the command ends with:

```output
deployment "ome-controller-manager" successfully rolled out
```

Check the defaults that the manager reads:

```bash
kubectl get configmap inferenceservice-config -n ome -o jsonpath='{.data.podDisruptionBudget}'
```

```output
{"omeNative":{"maxUnavailable":1},"rawDeployment":{"maxUnavailable":"25%"}}
```

The engine keeps its own budget from Step 1. Other RawDeployment components that set neither field now get `maxUnavailable: 25%`.

## How OMENative counts pods {since=v1.3}

On OMENative, OME writes the budget as an integer `minAvailable`, counted in pods. An OMENative component runs [Instances](../../concepts/omenative/instances.md), and a multi-pod Instance has a leader and `worker.size` workers. OME multiplies the Instances that the component should run by the pods in each, and rounds a percentage up. For four Instances of a leader and two workers each, 12 pods in all:

| Budget that you set | `minAvailable` that OME writes |
| --- | --- |
| `minAvailable: 2` | 2 |
| `minAvailable: 90%` | 11 |
| `maxUnavailable: 1`, the chart's default | 11 |
| `maxUnavailable: 25%` | 9 |

`kubectl get pdb` shows the number under `MIN AVAILABLE`, and `N/A` under `MAX UNAVAILABLE`, whichever field you set. OME updates the number as the component scales, including when an [autoscaler](../../concepts/serving/component-autoscaling.md) scales it.

The budget counts pods, not Instances. By default, when a pod of a ready multi-pod Instance goes missing, OME [drains the Instance and recreates all its pods](../../concepts/omenative/instance-restart-policy.md). So one eviction can take the whole Instance down. For a multi-pod component, `maxUnavailable: 1` lets evictions take down one Instance at a time.

To pace OMENative's own updates, set [`lifecycle.updateStrategy.rollingUpdate.maxUnavailable`](../../concepts/architecture/omenative-update-strategies.md#pacing-with-rollingupdate), which counts Instances.

When a component moves between RawDeployment and OMENative, its PodDisruptionBudget protects only the old pods until the new workload's replicas are ready. OME then moves it to the new pods with the new mode's budget, or deletes it when that mode has none.

## Troubleshooting

### A node drain doesn't finish

`kubectl drain` keeps retrying an eviction that a PodDisruptionBudget refuses, and prints `Cannot evict pod as it would violate the pod's disruption budget.` List the InferenceService's PodDisruptionBudgets as in [Step 1](#step-1-set-a-budget-on-a-component).

A component whose `ALLOWED DISRUPTIONS` is `0` has no pod to spare. Either some of its pods are already unavailable, or its budget allows no disruption, as `minAvailable: 100%` does. Wait for the pods to be ready, lower the budget, or add replicas. On OMENative, the drain can also wait while OME recreates a multi-pod Instance after an eviction.

### No PodDisruptionBudget appears

When `kubectl get pdb -n qwen3-0-6b -l ome.io/inferenceservice=qwen3-0-6b` leaves out a component, check these:

- The component sets neither field, and its mode has no default, as on a manifest install. The check at the end of [Step 2](#step-2-set-the-operator-defaults-for-each-mode) prints the defaults, if any. Set a budget as in Step 1, or defaults as in Step 2.
- The component runs in the `MultiNode` mode (deprecated), which gets none. [Move it to OMENative](../omenative/move-from-leaderworkerset.md) to give it one.
- OME logs an error for the component, as [A component stops updating](#a-component-stops-updating) describes.

### The webhook rejects the budget {since=v1.3}

kubectl prints an error that includes `admission webhook "inferenceservice.ome-webhook-server.validator" denied the request:`, followed by a reason that names the field. For a component that sets both fields, the reason is:

```output
spec.engine.minAvailable and spec.engine.maxUnavailable cannot both be set
```

For a value other than a whole number or a percentage from `0%` to `100%`, it's:

```output
spec.engine.minAvailable must be a non-negative integer or a percentage from 0% to 100%
```

Keep one field, with a value such as `2` or `50%`, and apply the InferenceService again. A percentage is a whole number followed by `%`.

### A component stops updating {since=v1.3}

OME stops creating and updating the component's workload. It also skips the components after it, in the order engine, decoder, router. It records no event, so look for the error in the manager's logs:

```bash
kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1 | grep qwen3-0-6b
```

The lines include `Failed to reconcile component` and an error that starts with one of these:

| Error starts with | Cause | Fix |
| --- | --- | --- |
| `failed to resolve engine PodDisruptionBudget` | The runtime sets one field for the component and the InferenceService sets the other, or the runtime's value is invalid. | Set the runtime's field in the InferenceService in place of the other one, or fix it in the runtime. |
| `failed to preflight engine PodDisruptionBudget` | A PodDisruptionBudget that OME didn't create has the component's name. | Delete it with `kubectl delete pdb qwen3-0-6b-engine -n qwen3-0-6b`. |

For the decoder or the router, the error names that component. OME retries, so the components update once you fix the cause.

### Every InferenceService stops updating {#every-inferenceservice-stops-reconciling since=v1.3}

When the `podDisruptionBudget` defaults are invalid, OME stops creating and updating the workloads of all InferenceServices, including the ones with their own budgets. Deleting an InferenceService still works. OME records no event, so look for the error in the manager's logs:

```bash
kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1 | grep 'fails to create InferenceServicesConfig'
```

The error starts with `fails to create InferenceServicesConfig:`, followed by the reason. After a Helm change, the reason is likely `exactly one of podDisruptionBudget.rawDeployment.minAvailable or podDisruptionBudget.rawDeployment.maxUnavailable must be set`. It means your values gave a mode `minAvailable` without `maxUnavailable: null`, as the warning in [Step 2](#step-2-set-the-operator-defaults-for-each-mode) explains. Other reasons name an invalid value, an unknown key or malformed JSON. Keys are case-sensitive.

Fix the values or the ConfigMap as in Step 2, and restart the manager after a `kubectl` edit. The InferenceServices then update again.

## Clean up

Set the engine back to one pod without a budget: in `qwen3-0-6b-isvc.yaml`, remove `minAvailable`, and set `minReplicas` and `maxReplicas` back to `1`. Then apply the file:

```bash
kubectl apply -f qwen3-0-6b-isvc.yaml
```

```output
inferenceservice.ome.io/qwen3-0-6b configured
```

The engine goes back to its mode's default. When the mode has none, OME deletes the PodDisruptionBudget `qwen3-0-6b-engine`.

If you changed the defaults in Step 2, put them back:

=== "Helm"

    Remove `podDisruptionBudget` from your values file, and upgrade the release as in Step 2. The chart writes its defaults back and restarts the manager.

=== "kubectl"

    Remove the key:

    ```bash
    kubectl patch configmap inferenceservice-config -n ome --type json -p '[{"op":"remove","path":"/data/podDisruptionBudget"}]'
    ```

    ```output
    configmap/inferenceservice-config patched
    ```

    Then restart the manager as in Step 2.

## Next steps

- [Deployment modes and OMENative](../../concepts/architecture/deployment-modes.md): what OME creates for a component in each mode, and how it picks the mode.
- [Serve a model on OMENative](../omenative/serve-a-model-on-omenative.md): run the engine as OMENative Instances, where the budget counts pods.
- [Instances](../../concepts/omenative/instances.md): how an OMENative component runs its pods, as single-pod or multi-pod Instances.
- [Set replica defaults](../operate-ome/set-replica-defaults.md): the default `minReplicas` and `maxReplicas` for components.
- [Specifying a Disruption Budget for your Application](https://kubernetes.io/docs/tasks/run-application/configure-pdb/): how Kubernetes applies a PodDisruptionBudget.
