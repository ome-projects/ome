---
title: "Spread Instances Across Fault Domains"
linkTitle: "Spread Across Fault Domains"
weight: 20
date: 2026-09-27
description: >
  Use topologySpread and topologySpreadKey on an OMENative engine or decoder to spread its Instances across racks, fabric slices, or zones, and understand what Required and Preferred each guarantee.
---

This page shows you how to use `topologySpread` and `topologySpreadKey` to spread an OMENative component's Instances across fault domains, so losing one domain — a rack, an NVLink clique, a TPU cube — cannot take out every Instance of the component at once.

The fields live directly on the `engine` and `decoder` component specs (the router has no spreading fields), and they apply only to components whose deployment mode resolves to **OMENative** — see [Deployment Modes and OMENative](/ome/docs/concepts/omenative/). Unset means no spreading: placement stays pure bin-packing.

```yaml
spec:
  engine:
    topologySpread: Required   # or Preferred
    topologySpreadKey: topology.kubernetes.io/rack   # optional, see below
```

## Before you begin

You need to have the following:

- A Kubernetes cluster with OME installed
- An InferenceService component running in OMENative deployment mode
- Node labels that mark your fault-domain boundaries (e.g. `topology.kubernetes.io/rack`, an NVLink clique label, or a TPU sub-block label)
- `kubectl` configured to communicate with your cluster

## How spreading is rendered

OME renders the policy as one standard Kubernetes [`topologySpreadConstraint`](https://kubernetes.io/docs/concepts/scheduling-eviction/topology-spread-constraints/) with `maxSkew: 1` on each Instance's **anchor pod** — the leader of a multi-node Instance, or the sole pod of a single-pod Instance:

```yaml
# On the rendered leader (or single) pod:
topologySpreadConstraints:
  - maxSkew: 1
    topologyKey: nvidia.com/gpu.clique      # the resolved spread key
    whenUnsatisfiable: DoNotSchedule        # Required (Preferred renders ScheduleAnyway)
    labelSelector:
      matchLabels:
        ome.io/inferenceservice: llama-chat
        component: engine
        ome.io/runner: leader
```

The selector matches only sibling anchors of the same component of the same InferenceService, so matching-pod counts equal Instance counts: spreading anchors spreads Instances. Worker pods deliberately carry **no** constraint of their own — their generated worker→leader required podAffinity already makes them follow their leader, so the constraint-gated leader always decides the fault domain and whole gangs stay co-located while spreading.

Because the constraint is a vanilla pod-spec field, it works with any scheduler; nothing about it requires the OME gang scheduler.

If you author your own `topologySpreadConstraints` entry on the same label key in the component's pod spec, OME leaves it alone and injects nothing — your constraint wins.

## Required vs Preferred

`topologySpread` takes one of two values (the CRD enum rejects anything else):

| Value | Renders as | Guarantee |
|-------|-----------|-----------|
| `Required` | `whenUnsatisfiable: DoNotSchedule` | **Balanced spreading.** With `maxSkew: 1`, anchor counts per fault domain may differ by at most one. A placement that would over-skew is refused: the anchor holds `Pending` until a compliant domain has capacity. Enforced by any scheduler that runs PodTopologySpread's Filter — including the OME gang scheduler. |
| `Preferred` | `whenUnsatisfiable: ScheduleAnyway` | **Best-effort spreading.** The constraint only influences placement through PodTopologySpread's *Score*, so it is honored by the default kube-scheduler but never blocks a placement. Currently without effect under the OME gang scheduler — see below. |

Note what `Required` does and does not promise: it never blocks a *balanced* placement (if counts are 1/1 across two racks, a third Instance may land in either rack that has room), but it trades utilization for fault isolation — when the only domain with free capacity would over-skew, the new Instance waits instead of packing in. The held placement is retried as the cluster changes (pod churn, freed capacity), which is ordinary PodTopologySpread behavior.

### Why Preferred is inert under the OME gang scheduler

The [`ome-scheduler`](/ome/docs/administration/ome-scheduler/) packing profile disables PodTopologySpread's **scoring** (`scheduler.disablePodTopologySpreadScore`, default `true`) because soft spreading is the exact opposite of packing — and scoring is the only mechanism a `ScheduleAnyway` constraint acts through. On top of that, the OMEGangPack plugin's PreFilter has already best-fit-pinned each gang to a single domain before scoring runs. So under the packing profile, `Preferred` changes nothing.

`Required` is different: PodTopologySpread's Filter path stays enabled for hard `DoNotSchedule` constraints. When the gang scheduler's packing choice would over-skew, the filter vetoes it and the retry re-plans the gang into a domain that satisfies the constraint. Use `Required` when you run `ome-scheduler`; reserve `Preferred` for components scheduled by the default kube-scheduler (or another profile that keeps the spread score).

## Choosing the spread key

`topologySpreadKey` names the node label whose values are your fault domains. It defaults to the component's co-location `topologyKey`, and it is ignored unless `topologySpread` is set. Three shapes:

- **Default — the gang domain is the failure zone.** On fabrics where the co-location domain is also the thing that fails (an NVLink rack), leave `topologySpreadKey` unset: Instances co-locate within a domain by `topologyKey` and spread across domains by the same label.
- **Split keys — the failure zone is coarser than the gang domain.** When one failure zone contains many gang domains, co-locate by the fine label and spread by the coarse one. Example: TPU gangs co-locate by a gang-sized partition label while spreading across `cloud.google.com/gce-topology-subblock`, so no sub-block failure takes every Instance.
- **Single-pod components.** A single-pod component has no gang and typically no co-location `topologyKey`, so there is no default to fall back to — set `topologySpreadKey` explicitly (for example `topology.kubernetes.io/zone`). If neither key resolves, no constraint is rendered and the policy is a no-op.

## Set it on an InferenceService

A multi-node engine that co-locates each gang inside an NVLink clique and requires Instances to balance across cliques:

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-chat
  namespace: llama-demo
spec:
  deploymentMode: OMENative
  model:
    name: llama-3-70b-instruct
  engine:
    minReplicas: 3
    maxReplicas: 3
    topologyKey: nvidia.com/gpu.clique   # co-locate each leader+worker gang
    topologySpread: Required             # balance Instances across cliques
    leader:
      # ...
    worker:
      size: 1
      # ...
```

The split-key shape adds the coarser fault-domain label:

```yaml
  engine:
    topologyKey: cloud.google.com/gke-tpu-partition-2x2x2-id   # gang domain
    topologySpread: Required
    topologySpreadKey: cloud.google.com/gce-topology-subblock  # failure zone
```

The same fields work under `spec.decoder`; each component resolves independently.

A runtime author can also set them in the ServingRuntime's `engineConfig` or `decoderConfig` as a default for every service using the runtime. The InferenceService component's value overrides the runtime's; unset on both means no spreading.

## Turning it on does not roll the fleet

Unlike `topologyKey` — which participates in the component's revision hash, so changing it mints a new revision and rolls the component — `topologySpread` and `topologySpreadKey` are deliberately **excluded from the revision hash**. The constraint is injected when a pod is rendered, not baked into the revision. Consequences:

- Setting, changing, or removing the policy on a healthy fleet replaces nothing. It shapes **future** placements only: scale-ups, repairs, migrations, and the new pods of a rollout.
- Existing pods are never evicted to fix skew — topology spread constraints are scheduling-time only. A fleet that was packed before you enabled `Required` stays packed until its pods are naturally recreated (for example by the next rollout).

## Verify

The resolved policy is projected onto the component's InferenceReplica:

```bash
kubectl get inferencereplica llama-chat-engine -n llama-demo \
  -o jsonpath='{.spec.topologySpread}{" "}{.spec.topologySpreadKey}{"\n"}'
```

Inspect a rendered anchor pod — the leader (or single) pod carries the constraint, workers do not:

```bash
kubectl get pod llama-chat-engine-0-leader-0 -n llama-demo \
  -o jsonpath='{.spec.topologySpreadConstraints}' | jq
```

Then confirm the spread itself by listing which fault domain each anchor landed on:

```bash
kubectl get pods -n llama-demo -l ome.io/inferenceservice=llama-chat,component=engine,ome.io/runner=leader \
  -o custom-columns='POD:.metadata.name,NODE:.spec.nodeName'
kubectl get nodes -L nvidia.com/gpu.clique
```

Under `Required`, an Instance that stays `Pending` while other domains sit full is usually the constraint doing its job: placing it in the only domain with capacity would push the skew above 1. `kubectl describe pod` on the pending anchor shows the scheduler's `didn't match pod topology spread constraints` message. It schedules once a compliant domain has room.

## Next steps

- [Gang Scheduling](/ome/docs/concepts/gang_scheduling/) — how multi-pod Instances become PodGroups and why co-location needs a gang-aware scheduler
- [OME Scheduler](/ome/docs/administration/ome-scheduler/) — installing the packing scheduler whose profile keeps the spread Filter but disables the spread Score
- [Deployment Modes and OMENative](/ome/docs/concepts/omenative/) — how a component resolves to OMENative and what an Instance is
