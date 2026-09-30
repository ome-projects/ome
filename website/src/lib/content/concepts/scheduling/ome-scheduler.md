---
title: The OME scheduler
description: An alpha second scheduler that places each gang of pods in one accelerator domain, and binds its pods only when the whole gang fits.
since: v1.3
---

The OME scheduler, `ome-scheduler`, places each gang of pods in one accelerator domain, and binds the gang's pods only when all of them have a node. The leader and workers of a multi-node model replica then share a fast interconnect, and a gang that fits in no domain waits without holding any node.

The OME scheduler is alpha, and it runs only on Kubernetes 1.35. It's the upstream kube-scheduler with one extra plugin, OMEGangPack. You install it with its own Helm chart, `ome-scheduler`, as a second scheduler next to your default one, and only pods that set `schedulerName: ome-scheduler` use it. OME's releases publish the chart but not the scheduler's image, so you build the image yourself. [Use the OME scheduler](../../guides/operate-ome/ome-scheduler.md) installs it and opts a workload in.

## Why gangs need their own scheduler

A [gang](../serving/gang-scheduling.md) is a set of pods that are useful only together: the leader of a multi-node model replica can't serve without its workers. The pods run one model, so they belong in one accelerator domain. That's a set of nodes joined by a fast interconnect, such as an NVLink domain, a TPU slice or a rack.

The default Kubernetes scheduler places pods one at a time and ignores the PodGroups that declare gangs. It can split a gang across domains, so the leader and its workers talk over slower links. It can also bind part of a gang while the rest waits. The bound pods then hold accelerators they can't use, and two gangs can each take part of a domain that only one of them could fill.

When you set a component's `topologyKey`, OME adds a required pod affinity that keeps each worker in its leader's domain. But the leader still picks its node alone, so when its domain has no room for the workers, they stay `Pending`. The OME scheduler places the whole gang as a unit instead.

For gang scheduling without domain packing, the scheduler-plugins Coscheduling plugin also works with OME's PodGroups. See [Bring a gang-aware scheduler](../serving/gang-scheduling.md#bring-a-gang-aware-scheduler).

## Accelerator domains

The scheduler finds domains by a topology label: a node label whose value is the same on every node of one domain, such as `nvidia.com/gpu.clique`. OME copies a component's `topologyKey` into the `ome.io/topology-key` annotation of the component's PodGroups, and the scheduler reads the gang's label from there. For a PodGroup without the annotation, it uses the chart's `scheduler.plugin.topologyKey`, which is empty by default. With neither, the gang's pods stay `Pending`.

Gang members land only on nodes that carry the label. A domain has room for a gang when it has a separate node that can run each member. This rule only picks the domain, so small members can still share a node.

## How it places a gang

A gang's members are the pods whose `scheduling.x-k8s.io/pod-group` label names the same PodGroup, and the PodGroup's `minMember` is the gang's size. For each gang, the scheduler:

1. Waits until at least `minMember` members exist.
2. Picks, of the domains with room for the gang, the one with the fewest nodes that can run a member. This leaves larger domains for larger gangs.
3. Holds that domain until the last member has a node. Meanwhile, no other gang or pod on the OME scheduler can use its nodes, even the ones the gang doesn't need. The default scheduler can still place its pods there.
4. Binds the members together. They wait at the scheduler's Permit stage until `minMember` of them have a node.

A member waits up to the PodGroup's `scheduleTimeoutSeconds`. When the PodGroup sets none, the chart's `scheduler.plugin.defaultPermitTimeoutSeconds` applies, 600 seconds by default. If a member fails after it has a node, for example when its wait times out, the scheduler releases the domain and retries the members that haven't bound. Members that already bound keep their nodes, and the gang stays in their domain.

## How OME workloads use it

OME creates the gangs, but you choose their scheduler. When the scheduler-plugins PodGroup CRD is installed, OME creates a PodGroup for each [OMENative](../omenative/overview.md) [Instance](../omenative/instances.md) that has more than one pod. The PodGroup's `minMember` is the Instance's pod count, so the gang is the whole Instance. With the ome-resources chart's defaults, its `scheduleTimeoutSeconds` is 600 seconds, as [Bound the gang schedule timeout](../serving/gang-scheduling.md#bound-the-gang-schedule-timeout) explains.

OME looks for the CRD only when its manager starts, so restart the manager if you install the CRD after OME. Until then, a multi-pod component gets no PodGroups, and its InferenceReplica's `GangSchedulingUnavailable` condition is `True`.

To put a workload on the OME scheduler, set `schedulerName: ome-scheduler` on the [serving runtime](../runtimes/serving-runtimes.md) or on the [InferenceService](../serving/inference-services.md). For a multi-node component, also set a `topologyKey`:

=== "ServingRuntime"

    ```yaml
    spec:
      schedulerName: ome-scheduler
      engineConfig:
        topologyKey: nvidia.com/gpu.clique
    ```

=== "InferenceService"

    ```yaml
    spec:
      engine:
        schedulerName: ome-scheduler
        topologyKey: nvidia.com/gpu.clique
    ```

The runtime's `schedulerName` applies to every pod spec that doesn't set its own, leaders and workers included. Set `topologyKey` on the runtime's `engineConfig` or `decoderConfig`, or on the InferenceService's `engine` or `decoder`. Settings on the InferenceService win over the runtime's. When OME creates PodGroups for pods on the default scheduler, it records a `MaybeNoGangScheduler` warning event on the InferenceService.

A single-pod Instance forms no gang, so the OME scheduler places its pod on its own. When the chart's `scheduler.plugin.topologyKey` is set, the scheduler also steers these pods toward partly used domains by default, which keeps whole domains free for gangs. See [Pack standalone pods](../../guides/operate-ome/ome-scheduler.md#pack-standalone-pods).

## What you see

Pods that the OME scheduler places get a `Scheduled` event from `ome-scheduler`. While a gang forms, its pods are `Pending` with no node. When the scheduler can't place a pod, the pod's `FailedScheduling` events give the reason. These are the common ones:

| Reason | What it means |
| --- | --- |
| `no domain has room for gang {namespace}/{name}` | No domain has room for the whole gang. The gang waits until one does. |
| `PodGroup {namespace}/{name} must declare ome.io/topology-key or the scheduler must configure topologyKey` | The gang has no domain label. Set the component's `topologyKey`, or the chart's `scheduler.plugin.topologyKey`. |
| `PodGroup {namespace}/{name} not resolvable yet` | The PodGroup doesn't exist, usually because OME found no PodGroup CRD when its manager started. Install the CRD, and restart the manager. |

[Scheduling events](../../reference/scheduling/ome-scheduler-configuration.md#scheduling-events) lists all the reasons.

## Limitations

The scheduler packs each gang into one domain. To also spread a component's Instances across domains for fault isolation, set the component's `topologySpread` to `Required`. With the chart's defaults, `Preferred` has no effect, as [Spread under the OME scheduler](../../guides/omenative/spread-instances-across-fault-domains.md#preferred-under-the-ome-scheduler) explains.

By default, the chart turns preemption off for the OME scheduler, with `scheduler.disablePreemption: true`, because Kubernetes' built-in preemption can evict pods without freeing a whole domain. So a gang waits for capacity instead. The default scheduler still preempts for the pods it places.

## Next steps

- [Use the OME scheduler](../../guides/operate-ome/ome-scheduler.md): install the chart, opt a workload in, and read the scheduler's events and metrics.
- [OME scheduler configuration](../../reference/scheduling/ome-scheduler-configuration.md): the chart's values, the PodGroup fields that the scheduler reads, and its events and metrics.
- [Gang scheduling](../serving/gang-scheduling.md): the PodGroups OME creates for multi-pod Instances.
- [Serve a multi-node model](../../guides/omenative/serve-a-multi-node-model.md): give an engine a leader and workers, which make up the gangs this scheduler places.
- [Alfred](alfred.md): OME's alpha GPU cluster caretaker, which recommends Instance migrations when GPU capacity fragments or nodes turn unhealthy.
