---
title: Reference
description: Look up the OME API, kubectl-ome commands, runtime matching and rollout rules, labels, annotations and storage formats when you need exact details.
---

## API

<div class="grid cards" markdown>

-   **[OME API](api/ome.v1beta1.md)**

    Look up the fields of every type in the ome.io/v1beta1 API, such as InferenceService, BaseModel and ClusterServingRuntime, in this generated reference.

-   **[Labels and annotations](api/labels-and-annotations.md)**

    OME reads and sets these labels and annotations on InferenceServices, models, runtimes, nodes and the resources it generates.

-   **[Traffic annotations](api/traffic-annotations.md)**

    These ome.io annotations tune circuit breaking, retries and timeouts for an InferenceService's backends, with pass-through prefixes for Envoy Gateway and Istio.

</div>

## kubectl ome

<div class="grid cards" markdown>

-   **[kubectl-ome overview and install](kubectl-ome/overview.md)**

    Install the kubectl-ome plugin, and look up its commands, global flags, output formats, exit codes and the RBAC it needs.

-   **[kubectl ome accelerator](kubectl-ome/accelerator.md)**

    Show the accelerator class or policy that the engine and decoder of an InferenceService ask for, and the resource requests their pods start from.

-   **[kubectl ome admin](kubectl-ome/admin.md)**

    Check that a cluster serves the APIs OME needs with admin doctor, and read the moves that Alfred, which is alpha, recommends.

-   **[kubectl ome autoscale](kubectl-ome/autoscale.md)**

    Show the autoscaling state the controller reports for each InferenceService component, and explain which layer supplies its autoscaler.

-   **[kubectl ome cluster](kubectl-ome/cluster.md)**

    Show whether OME reports each alpha WorkloadCluster as connected, from the Ready condition that its controller writes.

-   **[kubectl ome get](kubectl-ome/get.md)**

    List OME resources with model-centric columns, including merged views of BaseModels with ClusterBaseModels and ServingRuntimes with ClusterServingRuntimes.

-   **[kubectl ome instance](kubectl-ome/instance.md)**

    List the Instances of an InferenceService's OMENative components, inspect one Instance, show a component's retry blocks, and release a held revision (alpha).

-   **[kubectl ome logs](kubectl-ome/logs.md)**

    Stream logs from the pods behind an InferenceService, narrowed to one component, OMENative Instance or revision.

-   **[kubectl ome migration](kubectl-ome/migration.md)**

    See an InferenceService's running and past OMENative migrations, and request one with the alpha start action.

-   **[kubectl ome placement](kubectl-ome/placement.md)**

    Inspect the alpha multi-cluster placement of an InferenceService: its reported status, the workload clusters its selectors match and its routing table.

-   **[kubectl ome quota](kubectl-ome/quota.md)**

    Show the declared AcceleratorQuota tree, reported budgets and materialization, and validate the quota topology with a scriptable exit code.

-   **[kubectl ome rollout](kubectl-ome/rollout.md)**

    Show rollout progress, the pinned plan and bounded history for an InferenceService, validate its configuration, and run the alpha rollout actions.

-   **[kubectl ome runtime](kubectl-ome/runtime.md)**

    Explain which runtimes match a model, and show the effective runtime, revision history and inheritance tree behind an InferenceService.

-   **[kubectl ome scale](kubectl-ome/scale.md)**

    Request a temporary Instance count for one component of an OMENative InferenceService (alpha).

-   **[kubectl ome status](kubectl-ome/status.md)**

    Show why an InferenceService is or isn't ready, in one report: conditions, pods, model, runtime, rollout, autoscaling, traffic and warning events.

-   **[kubectl ome traffic](kubectl-ome/traffic.md)**

    Show the traffic routes, weights and canary split the controller reports for an InferenceService, check declared behavior, and drain a cluster (alpha).

-   **[kubectl ome version](kubectl-ome/version.md)**

    Print the kubectl-ome plugin version and the OME version that the cluster's manager Deployment declares.

-   **[kubectl ome wait](kubectl-ome/wait.md)**

    Block until an InferenceService reaches the state you name, and exit with a code that scripts can branch on.

-   **[Guarded actions](kubectl-ome/guarded-actions.md)**

    Every mutating kubectl-ome command follows one safety contract: dry runs, confirmation, a guarded patch, the ActionResult, exit codes and RBAC.

</div>

## Matching

<div class="grid cards" markdown>

-   **[Runtime selection scoring](matching/runtime-selection-scoring.md)**

    How OME picks a runtime for a model: namespace runtimes first, then by score, size range and name.

-   **[Model version matching](matching/model-version-matching.md)**

    How a runtime's supportedModelFormats entries accept a model's format and framework versions: the operators, the version syntax and the mismatch reasons.

-   **[Model size range matching](matching/model-size-range-matching.md)**

    Auto-selection skips a runtime when the model's parameter count is outside the runtime's modelSizeRange.

-   **[Runtime accelerator-class matching](matching/runtime-accelerator-class-matching.md)**

    Auto-selection rejects a runtime that does not list every AcceleratorClass your InferenceService names; an explicitly named runtime only gets a warning.

-   **[Runtime deployment-mode matching](matching/runtime-deployment-mode-matching.md)**

    Auto-selection skips a runtime when it and your InferenceService declare different deployment modes for the same component.

-   **[Diffusion pipeline runtime matching](matching/diffusion-pipeline-runtime-matching.md)**

    The diffusionPipeline field on a runtime's supportedModelFormats limits auto-selection to the diffusers pipelines and components it can serve.

</div>

## Rollouts

<div class="grid cards" markdown>

-   **[Canary progression](rollouts/canary-progression.md)**

    Canary steps move a share of each component's Instances to a new revision, then advance at once, on a timer, on a promote or on Prometheus analysis.

-   **[Canary metric analysis](rollouts/canary-analysis.md)**

    Metric-gated canary steps sample Prometheus each interval, roll back after failureLimit failing samples, and hold or roll back when metrics can't be read.

</div>

## Scheduling and capacity

The OME scheduler and Alfred are alpha. Their configuration and behavior can change between releases.

<div class="grid cards" markdown>

-   **[OME scheduler configuration](scheduling/ome-scheduler-configuration.md)**

    The ome-scheduler Helm chart's values, the PodGroup fields that the scheduler reads, and its events and metrics.

-   **[Alfred configuration](scheduling/alfred-configuration.md)**

    Alfred's config.yaml keys, ome-alfred Helm chart values and workload annotations, with their defaults and limits.

-   **[Alfred metrics and events](scheduling/alfred-metrics-and-events.md)**

    The Prometheus metrics, Kubernetes events and recommendations ConfigMap that Alfred publishes, and what each one tells you.

</div>

## Operate OME

<div class="grid cards" markdown>

-   **[Controller manager flags](operate-ome/controller-manager-flags.md)**

    Look up the OME controller manager's command-line flags, their defaults and the ome-resources Helm values that set them.

-   **[ome-serving chart values](operate-ome/ome-serving-values.md)**

    Look up the ome-serving Helm chart's values for each catalog model, with their defaults, the GPU presets and the PD mode settings.

</div>

## Storage

<div class="grid cards" markdown>

-   **[Benchmark output storage](storage/benchmark-output-storage.md)**

    A BenchmarkJob's outputLocation accepts these storageUri formats and parameters for OCI, S3, Azure Blob, Google Cloud Storage, GitHub Releases and PVCs.

</div>
