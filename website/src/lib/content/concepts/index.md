---
title: Concepts
description: "Learn how OME's parts fit together: its architecture, OMENative, scheduling, models, runtimes, serving, rollouts and traffic."
---

New to OME? Read [How OME works](architecture/how-ome-works.md) first; the other pages build on it.

## Architecture

<div class="grid cards" markdown>

-   **[How OME works](architecture/how-ome-works.md)**

    How the OME manager, the model agent and OME's custom resources turn a model and a runtime into serving pods.

-   **[Deployment modes and OMENative](architecture/deployment-modes.md)**

    OME runs InferenceService components as Kubernetes Deployments or on OMENative, which creates, updates, repairs and moves each replica as one unit. Opt in to OMENative, see how OME picks a component's mode, and read what OMENative creates and reports.

</div>

## OMENative

<div class="grid cards" markdown>

-   **[OMENative overview](omenative/overview.md)**

    OMENative runs each replica of an InferenceService component as an Instance, one pod or a leader and its workers, that OME creates, updates, repairs, moves and removes as one unit. Multi-node engines and decoders use it by default, and any component can opt in.

-   **[Instances](omenative/instances.md)**

    An Instance is one replica of an OMENative component, a single pod or a leader and its workers, whose identity, phase, operation and failures OME records on the component's InferenceReplica.

-   **[OMENative update strategies](architecture/omenative-update-strategies.md)**

    Choose how an OMENative rollout replaces an Instance's pods, by starting new ones first, recreating them or updating them in place, and pace it with partition, maxSurge and maxUnavailable.

-   **[Instance restart policy](omenative/instance-restart-policy.md)**

    Choose whether OMENative repairs only a failed pod or rebuilds its whole Instance, and see how a restart runs.

-   **[Migration and transient scale](omenative/migration-and-transient-scale.md)**

    OMENative moves an Instance to another node by starting its replacement first, and can run more or fewer Instances for a while without an edit to the InferenceService.

</div>

## Scheduling and capacity

The OME scheduler and Alfred are alpha. Their configuration and behavior can change between releases. The scheduler needs Kubernetes 1.35.

<div class="grid cards" markdown>

-   **[Gang scheduling](serving/gang-scheduling.md)**

    With the scheduler-plugins PodGroup CRD installed, OME creates a PodGroup for each multi-pod OMENative Instance, so a gang-aware scheduler places its leader and workers together or not at all.

-   **[The OME scheduler](scheduling/ome-scheduler.md)**

    An alpha second scheduler that places each gang of pods in one accelerator domain, and binds its pods only when the whole gang fits.

-   **[Alfred](scheduling/alfred.md)**

    Alfred, OME's alpha GPU cluster caretaker, recommends Instance moves when free GPUs fragment or a node turns unhealthy and, when you let it, asks OMENative to carry them out.

-   **[Alfred policies](scheduling/alfred-policies.md)**

    How Alfred's defragmentation and node-health policies find Instances to move, and how its arbiter and safety bounds decide which become recommendations or migration requests.

</div>

To divide a cluster's GPUs among teams with AcceleratorQuota, see [Set accelerator quotas](../guides/operate-ome/accelerator-quota.md).

## Models

<div class="grid cards" markdown>

-   **[Base models](models/base-models.md)**

    BaseModel and ClusterBaseModel tell OME where a model's weights live and which nodes get them; OME parses the model's architecture, size and capabilities.

-   **[Fine-tuned weights](models/fine-tuned-weights.md)**

    Serve weights fine-tuned from a base model, such as a LoRA adapter, from a zip file in OCI Object Storage.

</div>

## Runtimes

<div class="grid cards" markdown>

-   **[Serving runtimes](runtimes/serving-runtimes.md)**

    A ServingRuntime or ClusterServingRuntime describes how to run a model server: its pods, and optionally the models it supports, so OME can choose it for an InferenceService.

-   **[Runtime inheritance](runtimes/runtime-inheritance.md)**

    Write one runtime per engine, and keep what differs for each model or GPU in small child runtimes that inherit from it.

-   **[Runtime revisions and pinning](runtimes/runtime-revisions.md)**

    Pin an InferenceService to a snapshot of its runtime, choose when runtime changes reach it, and roll back to an earlier snapshot.

-   **[Accelerator classes](runtimes/accelerator-classes.md)**

    An AcceleratorClass names one type of GPU and the nodes that have it, so an InferenceService can run its engine and decoder on that type, picked by name or by policy.

</div>

## Serving

<div class="grid cards" markdown>

-   **[InferenceService](serving/inference-services.md)**

    An InferenceService names a model, a serving runtime or both, and declares the engine, decoder and router components that OME turns into workloads.

-   **[Component autoscaling](serving/component-autoscaling.md)**

    Each InferenceService component can carry its own autoscaler block, which picks HPA, KEDA, External or None and the metrics or triggers to scale on.

-   **[Autoscaler policy](serving/autoscaler-policy.md)**

    An AutoscalerPolicy is an alpha, reusable set of KEDA or HPA autoscaling settings that InferenceService components share by name.

-   **[Benchmarks](serving/benchmarks.md)**

    A BenchmarkJob runs genai-bench against an InferenceService with the traffic scenarios you choose, and stores the results.

</div>

## Rollouts and traffic

<div class="grid cards" markdown>

-   **[Rollout groups](rollouts-and-traffic/rollout-groups.md)**

    Rollout groups roll an InferenceService's OMENative components out as one, so prefill and decode change together, by canary, blue-green or rolling update, with metric-gated canary steps.

-   **[Rollout policy](rollouts-and-traffic/rollout-policy.md)**

    A RolloutPolicy is an alpha, reusable canary, blue-green or rolling-update progression that InferenceService rollout groups attach by reference.

-   **[Ingress and external access](rollouts-and-traffic/ingress.md)**

    How clients outside the cluster reach an InferenceService: the Service that OME creates by default, or the Kubernetes Ingress or Gateway API HTTPRoutes it creates when you turn on ingress.

-   **[Traffic policy](rollouts-and-traffic/traffic-policy.md)**

    Set an InferenceService's load balancing, session affinity and endpoint override in spec.traffic. OME writes them into the gateway's backend policy and reports the result in status.

-   **[Traffic map](rollouts-and-traffic/traffic-map.md)**

    A TrafficMap is the alpha routing table that OME writes for a multi-cluster InferenceService: each workload cluster's share of the requests, and the reasons behind it.

</div>
