---
title: Set replica defaults
description: Choose the minReplicas and maxReplicas that components get when the InferenceService and its runtime leave them unset, with the ome-resources chart's ome.controller.replicas values.
since: v1.3
---

The `ome-resources` chart's defaults are 1 to 3 replicas for the engine and the decoder, and 1 to 2 for the router. A [component](../../concepts/serving/inference-services.md#components) takes a default for each bound that the InferenceService and its [runtime](../../concepts/runtimes/serving-runtimes.md) leave unset. Change the defaults in your values file, then check the range that a component gets.

<div class="prerequisites" markdown>

- OME installed from the `ome-crd` and `ome-resources` charts, 1.3.0 or later, with the `ome-resources` release named `ome` in the namespace `ome`. See [Install OME](../../getting-started/install.md).
- Helm and `kubectl` access to the cluster.
- The values file that you installed `ome-resources` with. If you don't have it, `helm get values ome -n ome -o yaml > values.yaml` saves the values that you set.
- The InferenceService `qwen3-0-6b` from [Serve your first model](../../getting-started/serve-your-first-model.md), and the file `qwen3-0-6b-isvc.yaml` that you created it from. Step 2 changes it.

</div>

## Where a component's replicas come from

OME takes each bound, `minReplicas` and `maxReplicas`, from the first of these that sets it:

| Order | Where | Example |
| --- | --- | --- |
| 1 | The component in the InferenceService: `spec.engine`, `spec.decoder` or `spec.router`. | `spec.engine.maxReplicas: 2` |
| 2 | The runtime's [`engineConfig`, `decoderConfig` or `routerConfig`](../../concepts/runtimes/serving-runtimes.md#the-engine-decoder-and-router). | `engineConfig.maxReplicas: 2` |
| 3 | The replica defaults. | `defaultMaxReplicas.engine: 3` |

So a component can take `minReplicas` from the InferenceService and `maxReplicas` from the defaults. When a default `maxReplicas` is below the component's `minReplicas`, OME raises it to match. On v1.2.2, the webhook wrote both bounds into the InferenceServices that you created or updated, and they win over the defaults until you [remove them](#a-component-doesnt-get-the-defaults).

`minReplicas: 0` counts as set, while `maxReplicas: 0` counts as unset and takes the default. [Scale to zero with KEDA](../scale-and-migrate/scale-to-zero-with-keda.md) shows when a component can use `minReplicas: 0`.

A manifest install, with `make install` or `kubectl apply -k config/default`, [sets no defaults](#set-the-defaults-without-helm), so a component with no bounds anywhere runs one replica.

!!! warning "Set `minReplicas` on every component in a canary group"
    In a [canary group](../../concepts/rollouts-and-traffic/rollout-groups.md), a step's wait for ready new instances reads only the InferenceService, which doesn't hold the defaults. Without `minReplicas` there, the step can wait for instances that never start, or record its traffic before any new instance is ready. [New-revision capacity](../../reference/rollouts/canary-progression.md#new-revision-capacity) has the details.

## Step 1: Set the defaults

Set the defaults under `ome.controller.replicas` in your values file. This example raises the default `maxReplicas` of the engine and the decoder from 3 to 4:

```yaml title="values.yaml"
ome:
  controller:
    replicas:
      defaultMinReplicas: 1
      defaultMaxReplicas:
        engine: 4
        decoder: 4
        router: 2
```

| Key | Fills in | Chart default |
| --- | --- | --- |
| `defaultMinReplicas` | `minReplicas` of every component | `1` |
| `defaultMaxReplicas.engine` | `maxReplicas` of the engine | `3` |
| `defaultMaxReplicas.decoder` | `maxReplicas` of the decoder | `3` |
| `defaultMaxReplicas.router` | `maxReplicas` of the router | `2` |

Each value must be a whole number above 0. A key that you leave out keeps the chart's default. To remove a default, set its key to `null`: `router: null` removes the router's, and `replicas: null` removes them all.

New defaults apply to running components, except those with the alpha `External` autoscaler class. Raising `defaultMinReplicas` adds replicas to every component that takes it, so check that the cluster has capacity first. Lowering a `defaultMaxReplicas` removes replicas above the new maximum.

Keep your other install values in this file, because `helm upgrade` resets any value that the file leaves out. The `CHART` column of `helm list -n ome` shows your chart version. Upgrade the release with that version, 1.3.0 in this example:

```bash
helm upgrade --install ome oci://ghcr.io/moirai-internal/charts/ome-resources \
  --namespace ome --version 1.3.0 -f values.yaml
```

Helm reports that the release `ome` has been upgraded, and the manager restarts. Wait for it:

```bash
kubectl rollout status deployment ome-controller-manager -n ome
```

When the new pods are running, the command ends with:

```output
deployment "ome-controller-manager" successfully rolled out
```

## Step 2: Check what a component got

The engine of `qwen3-0-6b` sets `minReplicas: 1` and `maxReplicas: 1`, and its runtime, `srt-qwen3-0-6b`, sets neither. Remove `maxReplicas` from `qwen3-0-6b-isvc.yaml`, so that the engine takes it from the defaults:

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
```

Apply it:

```bash
kubectl apply -f qwen3-0-6b-isvc.yaml
```

```output
inferenceservice.ome.io/qwen3-0-6b configured
```

`kubectl apply` removes `maxReplicas`, because the file that it applied before set it. The engine keeps `minReplicas: 1` from the InferenceService and takes `maxReplicas: 4` from the defaults. It runs in the [RawDeployment mode](../../concepts/architecture/deployment-modes.md#the-deployment-modes) with the default autoscaler: a [HorizontalPodAutoscaler](../../concepts/serving/component-autoscaling.md) named after the component, which scales the Deployment `qwen3-0-6b-engine`. Print the autoscaler's range:

```bash
kubectl get hpa qwen3-0-6b-engine -n qwen3-0-6b \
  -o jsonpath='min={.spec.minReplicas} max={.spec.maxReplicas}{"\n"}'
```

```output
min=1 max=4
```

On [OMENative](../../concepts/architecture/deployment-modes.md#omenative), the HorizontalPodAutoscaler has the same name and scales the component's InferenceReplica. With `KEDA` in the alpha `autoscaler` block, the range is in the ScaledObject `scaledobject-qwen3-0-6b-engine`, as `spec.minReplicaCount` and `spec.maxReplicaCount`. With `None`, the component runs `minReplicas` replicas. With `External`, set the range in your own autoscaler: see [Bring your own autoscaler](../scale-and-migrate/bring-your-own-autoscaler.md).

OME doesn't write the defaults into the InferenceService. So `kubectl get inferenceservice qwen3-0-6b -n qwen3-0-6b -o yaml` still shows only `minReplicas: 1` for the engine, and [`kubectl ome autoscale explain`](../../reference/kubectl-ome/autoscale.md) shows its `RANGE` as `1..1`. Check the autoscaler instead.

## Set the defaults without Helm

To add defaults to a manifest install, [move to the Helm charts](move-to-the-helm-charts.md). Or add a `replicas` object with the keys from Step 1 to the `deploy` entry of the `inferenceservice-config` ConfigMap, in the manager's namespace, so that the entry reads:

```json
{
  "defaultDeploymentMode": "RawDeployment",
  "replicas": {
    "defaultMinReplicas": 1,
    "defaultMaxReplicas": {"engine": 3, "decoder": 3, "router": 2}
  }
}
```

Then restart the manager, as [Change ConfigMap settings](configure-the-controller.md#tune-the-config-cache) shows. On a Helm install, use Step 1 instead: the next `helm upgrade` can undo a `kubectl` edit.

## Troubleshooting

### A component doesn't get the defaults

Check the component in the InferenceService, and the runtime's `engineConfig`, `decoderConfig` or `routerConfig`: a bound set there wins over the default.

An InferenceService that you created or updated on v1.2.2 holds the values that the webhook wrote: `minReplicas: 1`, and `maxReplicas: 3`, or `2` for the router. To make a component follow the defaults, remove them with a merge patch, as here for the engine of `qwen3-0-6b`:

```bash
kubectl patch inferenceservice qwen3-0-6b -n qwen3-0-6b --type merge \
  -p '{"spec":{"engine":{"minReplicas":null,"maxReplicas":null}}}'
```

```output
inferenceservice.ome.io/qwen3-0-6b patched
```

Removing the values from your manifest isn't enough: a client-side `kubectl apply`, the default, leaves in place values that it didn't set. In a canary group, patch out only `maxReplicas`.

Then check the defaults that the manager reads:

```bash
kubectl get configmap inferenceservice-config -n ome -o jsonpath='{.data.deploy}' | \
  grep '"replicas"'
```

With the values from Step 1, the output is:

```output
  "replicas": {"defaultMaxReplicas":{"decoder":4,"engine":4,"router":2},"defaultMinReplicas":1},
```

An empty result means that the config sets no defaults. OME ignores a key that it doesn't know, so check the spelling of each key. If the line is right but a component keeps its old range after a `kubectl` edit, restart the manager, as [Change ConfigMap settings](configure-the-controller.md#tune-the-config-cache) shows.

### The manager doesn't start after you change a default

A default that isn't a whole number above 0 makes the new manager pods exit, and `kubectl rollout status` keeps waiting. Soon after, the running manager reads the bad value too, and new, changed and deleted InferenceServices all stall, with no event. Find the error:

```bash
kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1 | \
  grep -e 'invalid deploy config' -e 'unable to parse deploy config'
```

| Message | Cause |
| --- | --- |
| `invalid deploy config, replicas.<key> must be > 0, got <n>` | The key's value is 0 or below. |
| `unable to parse deploy config json` | A value isn't a whole number, such as `4.5`, or is quoted, such as `"4"`. |

Fix the value and upgrade again, as in Step 1.

### The controller logs `minReplicas must not exceed maxReplicas` {#reconciles-fail-with-minreplicas-must-not-exceed-maxreplicas}

A default `minReplicas` can end up above a `maxReplicas` that the InferenceService or its runtime sets, such as `defaultMinReplicas: 3` for an engine with `maxReplicas: 2`. On OMENative, OME raises `maxReplicas` to match, so the component can run more replicas than you set. On RawDeployment, OME stops updating the component's autoscaler, the components after it and the InferenceService's status, and records no event. It updates the engine first, then the decoder, then the router. Look for the error:

```bash
kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1 | \
  grep 'minReplicas must not exceed maxReplicas'
```

The error names the component, such as `raw engine`, and ends with both values, such as `3 > 2`. Set `minReplicas` on the component, or lower `defaultMinReplicas`.

## Clean up

To put the engine back at one replica, add `maxReplicas: 1` to `qwen3-0-6b-isvc.yaml` again, and apply it.

## Next steps

- [Component autoscaling](../../concepts/serving/component-autoscaling.md): how each autoscaler class scales a component within its range.
- [Scale to zero with KEDA](../scale-and-migrate/scale-to-zero-with-keda.md): let a component scale down to no replicas.
- [Canary progression](../../reference/rollouts/canary-progression.md): how a canary step counts a component's replicas.
- [Configure the controller](configure-the-controller.md): tune the manager's flags and ConfigMap settings.
