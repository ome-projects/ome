---
title: Spread Instances across fault domains
description: Spread an OMENative engine's or decoder's Instances across racks, fabric slices or zones with topologySpread and topologySpreadKey, as a preference or a requirement.
since: v1.3
---

Spread the [Instances](../../concepts/omenative/instances.md) of an engine or decoder across zones, racks or fabric slices, so that when one domain fails, the Instances in the others keep serving. Set `topologySpread` on a component that runs on [OMENative](../../concepts/omenative/overview.md), and OME asks the scheduler to spread its Instances across the values of a node label. Here you spread the two Instances of a Qwen3-0.6B engine across zones, then check where they landed.

<div class="prerequisites" markdown>

- OME v1.3 or later, and `kubectl`, with the rights to create namespaces and [InferenceServices](../../concepts/serving/inference-services.md).
- The [ClusterBaseModel](../../concepts/models/base-models.md) `qwen3-0-6b`, in the `Ready` state, and the [ClusterServingRuntime](../../concepts/runtimes/serving-runtimes.md) `srt-qwen3-0-6b`, created as in [Serve your first model](../../getting-started/serve-your-first-model.md).
- GPU nodes where the model is `Ready`, with room for one engine pod in each of two zones: 1 GPU, 10 CPUs and 30 GiB of memory. To list them with their zones, run `kubectl get nodes -l models.ome.io/clusterbasemodel.qwen3-0-6b=Ready -L topology.kubernetes.io/zone`.

</div>

## How the spread works

OME adds a Kubernetes [topology spread constraint](https://kubernetes.io/docs/concepts/scheduling-eviction/topology-spread-constraints/) to each Instance's leader pod, or to its only pod, so the scheduler counts the component's Instances per domain. Set these fields on the engine or the decoder:

| Field | What it sets | When unset |
| --- | --- | --- |
| `topologySpread` | `Required` or `Preferred`. | No spread. |
| `topologySpreadKey` | The node label whose values are the domains, such as `topology.kubernetes.io/zone`. | OME uses `topologyKey`. With neither key, there's no spread. |
| `topologyKey` | The node label that keeps a multi-pod Instance's workers in their leader's domain. | Nothing keeps the workers in their leader's domain. |

A runtime can set the same fields in its `engineConfig` or `decoderConfig`, as defaults that the InferenceService's values [override](../../concepts/runtimes/serving-runtimes.md#the-engine-decoder-and-router). The engine and the decoder spread their Instances separately. Other deployment modes ignore the fields, and the webhook doesn't warn.

### Required and Preferred

| `topologySpread` | `whenUnsatisfiable` | Where a new Instance goes | Pick it when |
| --- | --- | --- | --- |
| `Required` | `DoNotSchedule` | Only to a domain with the fewest of the component's Instances, so counts differ by at most one. When those are full, the pod stays `Pending`, even if another domain has room. | An Instance should wait for room rather than break the spread. |
| `Preferred` | `ScheduleAnyway` | Preferably to a domain with the fewest Instances, and to another one when those are full. | An Instance should start even if it breaks the spread. |

The scheduler counts only the domains that have a node the pods select. The example engine's pods select nodes where the model is `Ready`, so if the model is `Ready` in a single zone, `Required` puts all the Instances there.

### Choosing the key

Set the keys by the Instance's shape and the domain you expect to fail:

| Instance | Domain that fails | Keys to set |
| --- | --- | --- |
| One pod | Any | `topologySpreadKey` |
| A leader and workers | The one that holds the Instance together, such as an NVLink rack | `topologyKey` only |
| A leader and workers | A larger one, such as a zone of racks | `topologyKey` to the rack label, and `topologySpreadKey` to the zone label |

For an engine with a leader and workers, these fields keep each Instance's pods in one NVLink domain and spread the Instances across zones:

```yaml
engine:
  topologyKey: nvidia.com/gpu.clique
  topologySpread: Required
  topologySpreadKey: topology.kubernetes.io/zone
```

## Step 1: Set the spread on an InferenceService

Create a namespace for the InferenceService:

```bash
kubectl create namespace spread-demo
```

```output
namespace/spread-demo created
```

Save this InferenceService as `qwen3-0-6b-isvc.yaml`:

```yaml title="qwen3-0-6b-isvc.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: qwen3-0-6b
  namespace: spread-demo
spec:
  deploymentMode: OMENative
  model:
    name: qwen3-0-6b
  runtime:
    name: srt-qwen3-0-6b
  engine:
    minReplicas: 2
    maxReplicas: 2
    topologySpread: Required
    topologySpreadKey: topology.kubernetes.io/zone
```

- `deploymentMode: OMENative` runs the engine on OMENative. A single-pod engine runs as a Deployment by default.
- `minReplicas` and `maxReplicas` keep the engine at two Instances, each a single pod.
- `topologySpread: Required` keeps the two Instances in different zones.
- `topologySpreadKey` names the zone label.

Apply the file:

```bash
kubectl apply -f qwen3-0-6b-isvc.yaml
```

```output
inferenceservice.ome.io/qwen3-0-6b created
```

Wait for the InferenceService to be ready. The first start pulls the SGLang image, which can take several minutes:

```bash
kubectl wait --for=condition=Ready inferenceservice/qwen3-0-6b -n spread-demo --timeout=30m
```

```output
inferenceservice.ome.io/qwen3-0-6b condition met
```

## Step 2: Check where the Instances landed

The engine's [InferenceReplica](../../concepts/architecture/deployment-modes.md#what-ome-creates), `qwen3-0-6b-engine`, shows the spread settings in effect, with the runtime's defaults merged in:

```bash
kubectl get inferencereplica qwen3-0-6b-engine -n spread-demo \
  -o 'custom-columns=NAME:.metadata.name,SPREAD:.spec.topologySpread,SPREAD-KEY:.spec.topologySpreadKey,TOPOLOGY-KEY:.spec.topologyKey'
```

```output
NAME                SPREAD     SPREAD-KEY                    TOPOLOGY-KEY
qwen3-0-6b-engine   Required   topology.kubernetes.io/zone   <none>
```

List the engine's pods with their spread constraint and their node:

```bash
kubectl get pods -n spread-demo -l ome.io/inferenceservice=qwen3-0-6b \
  -o 'custom-columns=NAME:.metadata.name,SPREAD-KEY:.spec.topologySpreadConstraints[*].topologyKey,WHEN:.spec.topologySpreadConstraints[*].whenUnsatisfiable,NODE:.spec.nodeName'
```

```output
NAME                            SPREAD-KEY                    WHEN            NODE
qwen3-0-6b-engine-0-default-0   topology.kubernetes.io/zone   DoNotSchedule   gpu-node-1
qwen3-0-6b-engine-1-default-0   topology.kubernetes.io/zone   DoNotSchedule   gpu-node-2
```

Each pod is one Instance. On a component with workers, only the leaders carry the constraint, so the workers show `<none>`.

Show the nodes' zones, using the names from your `NODE` column:

```bash
kubectl get nodes gpu-node-1 gpu-node-2 \
  -o 'custom-columns=NAME:.metadata.name,ZONE:.metadata.labels.topology\.kubernetes\.io/zone'
```

```output
NAME         ZONE
gpu-node-1   zone-a
gpu-node-2   zone-b
```

The two Instances run in different zones, so losing one zone leaves one Instance serving.

## Add the spread to a running InferenceService

Add the fields to the engine or decoder of an InferenceService that runs on OMENative, and apply it. Running Instances stay where they are. New pods get the constraint: those of a scale-up, a repair, a migration or a rollout. To move a running Instance now, [request an Instance migration](../scale-and-migrate/request-an-instance-migration.md).

Changing `topologyKey` on a component with workers is different: OME rolls the component onto a new revision.

## Spread under the OME scheduler {#preferred-under-the-ome-scheduler}

The [OME scheduler](../../concepts/scheduling/ome-scheduler.md) is an optional second scheduler that places each opted-in OMENative [gang](../../concepts/serving/gang-scheduling.md) in one accelerator domain, all or nothing. It's alpha, and runs only on Kubernetes 1.35. Use `Required` with it: when the domain it picks for a gang would break the spread, it tries another. With the scheduler chart's defaults, `Preferred` has no effect.

## Troubleshooting

### An Instance stays Pending

Run `kubectl describe pod qwen3-0-6b-engine-1-default-0 -n spread-demo`. When the spread holds the pod, its `FailedScheduling` event counts nodes that `didn't match pod topology spread constraints`:

| Case | Why the pod waits | What to do |
| --- | --- | --- |
| The count ends in `(missing required label)` | Those nodes lack the key's label, so `Required` skips them. | Label the nodes, or correct `topologySpreadKey`. |
| The count has no suffix | The domains with the fewest of the component's Instances are full. | Free room in one of them, or run fewer Instances. |
| The pod is new in a migration, or in a rollout with the default `SurgeThenDrain` strategy | The old pod counts, and keeps serving, until the new one is ready. | Free room in a domain with the fewest Instances, counting the old pod. See [SurgeThenDrain](../../concepts/architecture/omenative-update-strategies.md#surgethendrain). |
| Every node in one domain is cordoned, or has a taint the pods don't tolerate | That domain still counts, and its low count holds new Instances back from the others. | The pod schedules once those nodes accept pods again. |

### The Instances landed in one domain

Run the commands in [Step 2](#step-2-check-where-the-instances-landed), and find what you see:

| What Step 2 shows | Cause | What to do |
| --- | --- | --- |
| `SPREAD` is `<none>`, or the component has no InferenceReplica | The component doesn't run on OMENative, or nothing sets `topologySpread`. | [Turn on OMENative](../../concepts/omenative/overview.md#turn-on-omenative) for the component, and set `topologySpread`. |
| `SPREAD-KEY` and `TOPOLOGY-KEY` are both `<none>` | OME has no key to spread on. | Set `topologySpreadKey`. |
| The pods have no `SPREAD-KEY`, or a `SPREAD-KEY` or `WHEN` from before your change | The pods are older than the change. | [Request an Instance migration](../scale-and-migrate/request-an-instance-migration.md) to move them. |
| `WHEN` is `ScheduleAnyway` | `Preferred` used another domain while the ones with the fewest Instances were full. Under the OME scheduler's defaults, it has no effect. | Use `Required`. |
| `WHEN` isn't what you set, on a pod created after your change | Your runtime or InferenceService already sets a spread constraint on that key, and OME keeps it. | Remove that constraint. |
| All the values are right | Only one domain has a node the pods select, such as one where the model is `Ready`. | Put the model on nodes in a second domain, as [Choose the nodes](../../concepts/models/base-models.md#choose-the-nodes) shows. |

## Clean up

Delete the InferenceService and its namespace:

```bash
kubectl delete inferenceservice qwen3-0-6b -n spread-demo
kubectl delete namespace spread-demo
```

```output
inferenceservice.ome.io "qwen3-0-6b" deleted from spread-demo namespace
namespace "spread-demo" deleted
```

The model and the runtime stay. [Serve your first model](../../getting-started/serve-your-first-model.md) shows how to delete them.

## Next steps

- [Request an instance migration](../scale-and-migrate/request-an-instance-migration.md): move a running Instance, whose new pods get the spread.
- [Serve a multi-node model](serve-a-multi-node-model.md): run Instances with a leader and workers.
- [Use the OME scheduler](../operate-ome/ome-scheduler.md): install the alpha gang scheduler, and opt a workload in.
