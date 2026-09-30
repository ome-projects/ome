---
title: Guides
description: Deploy models, run them on OMENative, set up networking, roll out changes, scale and schedule workloads, try alpha multi-cluster routing, and operate and troubleshoot OME.
---

New to OME? Start by [installing it](../getting-started/install.md) and [serving your first model](../getting-started/serve-your-first-model.md). If an InferenceService isn't ready, go to [Troubleshoot an InferenceService](troubleshoot/troubleshoot-an-inferenceservice.md).

## Deploy models

Start with the smallest InferenceService, then add what you need: a runtime you name, weights that are already in the cluster, or an accelerator class.

<div class="grid cards" markdown>

-   **[Deploy an InferenceService](deploy-models/deploy-an-inferenceservice.md)**

    Write the smallest InferenceService that serves a model, with only a model, a model and a runtime, or only a runtime, then wait for it, send it a request and see what OME created.

-   **[Reference a runtime explicitly](deploy-models/reference-a-runtime-explicitly.md)**

    Name the ServingRuntime or ClusterServingRuntime that an InferenceService uses instead of letting OME choose one, then check that OME used it.

-   **[Troubleshoot runtime selection](deploy-models/troubleshoot-runtime-selection.md)**

    Find out why OME can't auto-select a runtime for your model, from kubectl's error or the RuntimeReady condition, then fix the runtime or name one.

-   **[Serve models from a PVC](deploy-models/serve-models-from-pvc.md)**

    Serve model weights that already live on a PersistentVolumeClaim by pointing a BaseModel at a pvc:// URI, with no download to nodes.

-   **[Serve models from node-local storage](deploy-models/serve-models-from-local-storage.md)**

    Serve model weights already on your nodes' disks with a local:// storage URI, which OME validates in place and mounts read-only.

-   **[Select accelerators](deploy-models/select-accelerators.md)**

    Pick the AcceleratorClass for the engine and decoder by naming a class, or let OME choose one with the BestFit, Cheapest, MostCapable or FirstAvailable policy.

-   **[Configure pod disruption budgets](deploy-models/configure-pod-disruption-budgets.md)**

    Control how many of a component's pods a node drain can take down, with a budget on each component or defaults for each deployment mode.

-   **[Run benchmarks](deploy-models/run-benchmarks.md)**

    Run a BenchmarkJob against an InferenceService with realistic traffic scenarios, and collect its throughput and latency results from storage.

</div>

## OMENative

[OMENative](../concepts/omenative/overview.md) runs each replica as an Instance, one pod or a leader and its workers, that OME creates, updates, repairs, moves and removes as one unit.

<div class="grid cards" markdown>

-   **[Serve a model on OMENative](omenative/serve-a-model-on-omenative.md)**

    Opt a single-pod InferenceService into OMENative with spec.deploymentMode, then check the InferenceReplica, the Instance and the pod that OME creates.

-   **[Serve a multi-node model](omenative/serve-a-multi-node-model.md)**

    Run one copy of a model across several nodes by giving its engine a leader and workers, which OMENative creates, updates and repairs together as one Instance.

-   **[Serve a prefill-decode model](omenative/serve-a-prefill-decode-model.md)**

    Serve a model with prefill on the engine and decode on the decoder, each with its own InferenceReplica on OMENative, and scale prefill on its own.

-   **[Move from LeaderWorkerSet to OMENative](omenative/move-from-leaderworkerset.md)**

    Move a multi-node InferenceService off the deprecated MultiNode mode, which runs on LeaderWorkerSet, onto OMENative, and delete the LeaderWorkerSet that OME leaves behind.

-   **[Spread Instances across fault domains](omenative/spread-instances-across-fault-domains.md)**

    Spread an OMENative engine's or decoder's Instances across racks, fabric slices or zones with topologySpread and topologySpreadKey, as a preference or a requirement.

-   **[Set Instance readiness deadlines](omenative/set-instance-readiness-deadlines.md)**

    Bound how long OMENative waits for a new Instance to become Ready with instanceReadyTimeout and the stuck-pod and unschedulable grace periods, and see what OME does when one runs out.

-   **[Reset failed Instances](omenative/reset-failed-instances.md)**

    Rebuild the OMENative Instances that OME marked Failed, once you've fixed the cause, with the ome.io/reset-instances annotation.

-   **[Recover stuck deletions](omenative/recover-stuck-deletions.md)**

    Recover OMENative pods stuck Terminating on dead nodes with the opt-in forceDelete escalation, and bound how long a deleting InferenceReplica holds its teardown finalizer.

-   **[Change the Instance status encoding](omenative/change-the-status-encoding.md)**

    Choose how OME stores per-Instance status on InferenceReplicas, as compact ColumnarV2 columns or a DenseV1 list, and switch a cluster between them.

</div>

## Networking

<div class="grid cards" markdown>

-   **[Configure ingress](networking/configure-ingress.md)**

    Configure how OME exposes InferenceServices through Kubernetes Ingress or Gateway API with the ingress block of the inferenceservice-config ConfigMap.

-   **[Expose a service without ingress](networking/expose-without-ingress.md)**

    Put an InferenceService behind a LoadBalancer or NodePort Service with the ome.io/service-type annotation, without an Ingress controller or a Gateway.

-   **[Choose a Gateway API host scheme](networking/gateway-host-schemes.md)**

    Choose between one shared hostname with per-service path prefixes and a subdomain per InferenceService for the HTTPRoutes OME generates.

-   **[Use multiple gateways](networking/multiple-gateways.md)**

    Attach OME's HTTPRoutes to more than one Gateway, such as an internal and an external one, and read the gateways' addresses in status.

-   **[Use per-namespace gateways](networking/namespace-gateways.md)**

    Route each namespace's InferenceServices through that namespace's own Gateway, and override the gateway for a single service with annotations.

-   **[Configure route timeouts](networking/configure-route-timeouts.md)**

    Set the request timeout on OME's HTTPRoutes for one InferenceService or the whole cluster, or turn it off so long generations can finish.

-   **[Set appProtocol on services](networking/set-service-app-protocols.md)**

    Set Kubernetes appProtocol values on the Service ports OME generates, so gateways and service meshes use the right protocol for your model server.

</div>

## Roll out changes

These guides are for components on [OMENative](../concepts/omenative/overview.md). To set up a canary, blue-green or rolling update, see [Rollout groups](../concepts/rollouts-and-traffic/rollout-groups.md#choose-a-progression).

<div class="grid cards" markdown>

-   **[Pause and resume a rollout](roll-out-changes/pause-and-resume-a-rollout.md)**

    Pause in-flight rollout work on an OMENative InferenceService with the alpha kubectl ome rollout pause action, and continue it with resume.

-   **[Promote or roll back a canary](roll-out-changes/promote-or-roll-back-a-canary.md)**

    Advance a gated canary step with kubectl ome rollout promote, or abort the canary back to the stable revisions with rollback; both are alpha.

-   **[Release a held revision](roll-out-changes/release-a-held-revision.md)**

    Get OME to retry a revision it stopped retrying, once you've fixed what made it fail, with the alpha kubectl ome instance release-held command.

-   **[Repin a drifted rollout plan](roll-out-changes/repin-a-drifted-rollout-plan.md)**

    Apply an edit to spec.rollout or a RolloutPolicy to the active rollout run, keeping its progress, with the alpha kubectl ome rollout repin action.

-   **[Pace rollouts with minReadySeconds](roll-out-changes/pace-rollouts-with-min-ready-seconds.md)**

    Make a new OMENative pod stay Ready for a warm-up window before it counts as Available, so a rollout waits before draining the old pods.

</div>

## Scale and migrate

To autoscale a component with HPA or KEDA, see [Component autoscaling](../concepts/serving/component-autoscaling.md).

<div class="grid cards" markdown>

-   **[Request a transient scale](scale-and-migrate/request-a-transient-scale.md)**

    Change how many Instances one OMENative component runs with the alpha kubectl ome scale command, until its autoscaler or OME sets the count again.

-   **[Request an Instance migration](scale-and-migrate/request-an-instance-migration.md)**

    Move one OMENative Instance to another node, with its replacement serving before the old one drains, using the alpha kubectl ome migration start command.

-   **[Scale to zero with KEDA](scale-and-migrate/scale-to-zero-with-keda.md)**

    Scale an idle RawDeployment component to zero replicas with a KEDA autoscaler, and back up when a trigger becomes active.

-   **[Bring your own autoscaler](scale-and-migrate/bring-your-own-autoscaler.md)**

    Turn off OME's autoscaler for a component with the External class, and scale it with your own autoscaler through the scaleTargetRef that OME publishes.

</div>

## Scheduling and capacity

The OME scheduler and Alfred are alpha. Their configuration and behavior can change between releases.

<div class="grid cards" markdown>

-   **[Use the OME scheduler](operate-ome/ome-scheduler.md)**

    Install the alpha ome-scheduler as a second scheduler and opt workloads into topology-packed gang placement; it requires Kubernetes 1.35.

-   **[Run Alfred in recommend-only mode](scheduling/run-alfred.md)**

    Install Alfred and read the Instance moves it recommends when free GPUs fragment or a node turns unhealthy, then tune its policies.

-   **[Set up scheduler simulation for Alfred](scheduling/set-up-scheduler-simulation.md)**

    Load your schedulers' configurations into Alfred, so that it predicts where the replacement pods of each move would land.

-   **[Let Alfred migrate Instances](scheduling/let-alfred-migrate-instances.md)**

    Turn on Alfred's execute mode, so that it asks OMENative to migrate Instances off fragmented or unhealthy nodes within the limits you set.

-   **[Set accelerator quotas](operate-ome/accelerator-quota.md)**

    Install the ome-quota-manager, split a cluster's GPUs among teams with an AcceleratorQuota tree, and charge workloads to each team's Kueue budget.

</div>

## Multi-cluster

Multi-cluster placement and routing are alpha, still in development, and off by default. Fields and behavior can change between releases. [Turn on routing](../concepts/rollouts-and-traffic/traffic-map.md#turn-on-routing) before you follow these guides.

<div class="grid cards" markdown>

-   **[Publish a global endpoint](multi-cluster/publish-a-global-endpoint.md)**

    Give an alpha multi-cluster InferenceService one global hostname that splits its traffic across workload clusters by its TrafficMap's weights.

-   **[Configure routing health probes](multi-cluster/routing-health-probes.md)**

    Configure the alpha end-to-end probe that sets a workload cluster's TrafficMap weight to zero after repeated failures, for every InferenceService or for one.

-   **[Drain a workload cluster](multi-cluster/drain-a-workload-cluster.md)**

    Use the alpha kubectl ome traffic drain to take a workload cluster out of an InferenceService's traffic for maintenance, and undrain to give it back.

-   **[Weight traffic for heterogeneous clusters](multi-cluster/weight-traffic-for-heterogeneous-clusters.md)**

    Give a workload cluster whose replicas serve more requests a larger share of an alpha multi-cluster InferenceService's traffic, with capacity factors in spec.routing.capacityFactors.

-   **[Tune routing capacity polling](multi-cluster/tune-routing-capacity-polling.md)**

    Cap a workload cluster's TrafficMap weight at the replicas that its endpoint reports it can serve, with an alpha poll set in the chart or for one InferenceService.

</div>

## Operate OME

<div class="grid cards" markdown>

-   **[Configure the controller](operate-ome/configure-the-controller.md)**

    Tune the OME controller manager's command-line flags, including reconcile concurrency, leader election and runtime-revision garbage collection.

-   **[Set replica defaults](operate-ome/set-replica-defaults.md)**

    Choose the minReplicas and maxReplicas that components get when the InferenceService and its runtime leave them unset, with the ome-resources chart's ome.controller.replicas values.

-   **[Run the model agent](operate-ome/model-agent.md)**

    Turn on the optional model agent for models stored on the nodes, and configure how it prepares them.

-   **[Keep downloaded model files](operate-ome/configure-model-artifact-retention.md)**

    Keep a model's downloaded files on the nodes after you delete the model, with the models.ome/reserve-model-artifact label.

-   **[Share Hugging Face artifacts](operate-ome/shared-hf-artifacts.md)**

    Keep one downloaded copy of a Hugging Face snapshot per node and share it across BaseModels and ClusterBaseModels with downloadPolicy ReuseIfExists.

-   **[Collect metrics](operate-ome/metrics.md)**

    Tune or turn off the short-retention Prometheus that the ome-resources chart deploys for KEDA autoscaling and canary analysis, or scrape OME with your own.

-   **[Set up alerting](operate-ome/alerting.md)**

    Enable the ome-resources chart's five P0 PrometheusRule alerts, which are off by default, and tune their thresholds and selectors.

-   **[Move a manifest install to the Helm charts](operate-ome/move-to-the-helm-charts.md)**

    Move an OME install from make install or kubectl apply -k to the ome-crd and ome-resources Helm charts, and keep your InferenceServices, models and runtimes in place.

</div>

## Troubleshoot

<div class="grid cards" markdown>

-   **[Troubleshoot an InferenceService](troubleshoot/troubleshoot-an-inferenceservice.md)**

    Find out why an InferenceService isn't ready or serving by checking its status, model, runtime and logs, then look up the fix for what you see.

</div>
