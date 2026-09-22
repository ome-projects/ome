---
title: Labels and Annotations
linkTitle: Labels and Annotations
weight: 2
description: >
  Reference of labels and annotations used by OME.
---

This document serves as a reference of the various labels and annotations used throughout OME.

## Annotations

### InferenceService Annotations

These annotations are used to configure InferenceService behavior:

| Annotation                           | Description                                                                                                                                               |
|--------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `ome.io/enable-tag-routing`          | Enables tag-based routing for the InferenceService                                                                                                        |
| `ome.io/autoscalerClass`             | Specifies the autoscaler class to use. Valid values: `hpa`, `keda`, `external`                                                                            |
| `ome.io/metrics`                     | Defines the scaling metric type. Valid values: `cpu`, `memory`                                                                                            |
| `ome.io/targetUtilizationPercentage` | Sets the target utilization percentage for autoscaling                                                                                                    |
| `ome.io/deprecation-warning`         | Displays deprecation warnings for legacy configurations                                                                                                   |
| `ome.io/enable-metric-aggregation`   | Enables metric aggregation for the InferenceService                                                                                                       |
| `ome.io/enable-prometheus-scraping`  | Enables Prometheus scraping for metrics collection                                                                                                        |
| `ome.io/volcano-queue`               | Specifies the Volcano queue name for job scheduling                                                                                                       |

### Model and Runtime Annotations

| Annotation                                      | Description                                          |
|-------------------------------------------------|------------------------------------------------------|
| `ome.io/inject-model-init`                      | Enables injection of model initialization containers |
| `ome.io/inject-fine-tuned-adapter`              | Enables injection of fine-tuned adapter containers   |
| `ome.io/inject-serving-sidecar`                 | Enables injection of serving sidecar containers      |
| `ome.io/fine-tuned-weight-ft-strategy`          | Specifies the fine-tuning strategy for weights       |
| `ome.io/base-model-name`                        | Specifies the base model name                        |
| `ome.io/base-model-vendor`                      | Specifies the base model vendor                      |
| `ome.io/serving-runtime`                        | Specifies the serving runtime to use                 |
| `ome.io/base-model-format`                      | Specifies the base model format                      |
| `ome.io/base-model-format-version`              | Specifies the base model format version              |
| `ome.io/fine-tuned-serving-with-merged-weights` | Enables fine-tuned serving with merged weights       |

### Model Security Annotations

These annotations control model encryption and decryption:

| Annotation                                 | Description                                                 |
|--------------------------------------------|-------------------------------------------------------------|
| `ome.io/base-model-decryption-key-name`    | Specifies the decryption key name for the base model        |
| `ome.io/base-model-decryption-secret-name` | Specifies the secret name containing decryption credentials |
| `ome.io/disable-model-decryption`          | Disables model decryption                                   |

### Service Configuration Annotations

| Annotation                    | Description                           |
|-------------------------------|---------------------------------------|
| `ome.io/service-type`         | Specifies the Kubernetes service type |
| `ome.io/load-balancer-ip`     | Sets the load balancer IP address     |

### RDMA Annotations

| Annotation                   | Description                                         |
|------------------------------|-----------------------------------------------------|
| `rdma.ome.io/auto-inject`    | Enables automatic RDMA injection                    |
| `rdma.ome.io/profile`        | Specifies the RDMA profile to use                   |
| `rdma.ome.io/container-name` | Specifies the container name for RDMA configuration |

The profiles are `oci-roce` (the default), `cks-gb-sglang` and `cks-gb-rdma`.
`oci-roce` injects NCCL/UCX environment variables. `cks-gb-sglang` injects NCCL
environment variables. Both profiles inject a `/dev/shm` emptyDir, a
`/dev/infiniband` hostPath mount and the `IPC_LOCK` capability; `cks-gb-rdma`
adds Multus network attachments only. None of them run the container
privileged: a privileged container bypasses the device cgroup and sees every
GPU on the node rather than the ones allocated to it.

A profile does not by itself grant access to the HCA device nodes. Request
your cluster's RDMA device-plugin resource in the runtime's container
resources (for example `rdma/hca_shared_devices_a: 1`), or set
`privileged: true` on the container explicitly. Values already set on the
container win over the profile.


### Runtime Revision and Pinning Annotations

These annotations drive [runtime revision pinning](/ome/docs/concepts/runtime-revision), which lets an InferenceService pin to a content-addressed snapshot of its ServingRuntime instead of always tracking the live runtime.

| Annotation                 | Description                                                                                                                                                                                    |
|----------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `ome.io/runtime-sync`      | Set/bump to a new value on a pinned InferenceService (`spec.runtime.autoSync: false`) to acknowledge runtime drift and advance the pin to a fresh runtime snapshot.                             |
| `ome.io/gc-eligible-since` | Set by the controller on an OME-managed ControllerRevision (RFC3339 timestamp) when it first becomes unreferenced and over the retention count; the garbage collector uses it. Not user-set.   |

### Deployment Configuration Annotations

| Annotation                    | Description                                                                                            |
|-------------------------------|-------------------------------------------------------------------------------------------------------|
| `ome.io/deploymentMode`       | Selects the deployment strategy (e.g. `RawDeployment`, `MultiNode`, `PDDisaggregated`). |
| `ome.io/dedicated-ai-cluster` | Associates the InferenceService with a dedicated AI cluster.                                           |
| `ome.io/entrypoint-component` | Identifies the entrypoint component of a multi-component InferenceService.                             |
| `ome.io/accelerator-class`    | Selects the accelerator class used for runtime matching and scheduling.                                |

### Ingress Annotations

See [Ingress administration](/ome/docs/administration/ingress) for usage details.

| Annotation                                 | Description                                                          |
|--------------------------------------------|--------------------------------------------------------------------|
| `ome.io/ingress-domain`                    | Overrides the ingress domain for the InferenceService.             |
| `ome.io/ingress-domain-template`           | Template used to construct the ingress domain.                     |
| `ome.io/ingress-additional-domains`        | Additional domains to expose the InferenceService on.              |
| `ome.io/ingress-url-scheme`                | URL scheme (`http`/`https`) for generated ingress URLs.            |
| `ome.io/ingress-path-template`             | Template used to construct the ingress path.                       |
| `ome.io/ingress-disable-istio-virtualhost` | Disables creation of the Istio VirtualHost for the InferenceService. |
| `ome.io/ingress-disable-creation`          | Disables ingress creation entirely for the InferenceService.       |

### Observability Annotations

| Annotation               | Description                                          |
|--------------------------|------------------------------------------------------|
| `prometheus.ome.io/port` | Container port Prometheus should scrape for metrics. |
| `prometheus.ome.io/path` | HTTP path Prometheus should scrape for metrics.      |

### Model Management Annotations

| Annotation                           | Description                                                                                     |
|--------------------------------------|-------------------------------------------------------------------------------------------------|
| `ome.oracle.com/skip-config-parsing` | Set on a BaseModel/ClusterBaseModel to skip automatic model-config parsing by the model agent.  |

## Labels

### Model and Runtime Labels

| Label                                    | Description                                  |
|------------------------------------------|----------------------------------------------|
| `base-model-name`                        | Base model name label                        |
| `base-model-size`                        | Base model size label                        |
| `base-model-type`                        | Base model type label                        |
| `base-model-vendor`                      | Base model vendor label                      |
| `fine-tuned-serving`                     | Fine-tuned serving label                     |
| `fine-tuned-serving-with-merged-weights` | Fine-tuned serving with merged weights label |
| `serving-runtime`                        | Serving runtime label                        |
| `fine-tuned-weight-ft-strategy`          | Fine-tuning strategy label                   |

### Scheduling Labels

| Label                   | Description        |
|-------------------------|--------------------|
| `volcano.sh/queue-name` | Volcano queue name |
| `volcano.sh/job-name`   | Volcano job name   |

### Kueue Labels

| Label                           | Description                    |
|---------------------------------|--------------------------------|
| `kueue.x-k8s.io/queue-name`     | Kueue queue name               |
| `kueue.x-k8s.io/priority-class` | Kueue workload priority class  |
| `kueue-enabled`                 | Enables Kueue for the resource |

### Model Agent Labels

| Label                                  | Description                       |
|----------------------------------------|-----------------------------------|
| `node.kubernetes.io/instance-type`     | Node instance shape               |
| `models.ome/{uid}`                     | Model label with UID              |
| `models.ome.io/target-instance-shapes` | Target instance shapes for models |
| `models.ome/basemodel-status`          | Base model status                 |

### Component Labels

| Label                     | Description                             |
|---------------------------|-----------------------------------------|
| `component`               | KService component label                |
| `endpoint`                | KService endpoint label                 |
| `ome.io/inferenceservice` | InferenceService label for TrainedModel |
| `ome.io/inferenceservice` | InferenceService pod label              |

### Network Visibility Labels

| Label                               | Description                      |
|-------------------------------------|----------------------------------|
| `networking.ome.io/visibility`      | Network visibility configuration |
| `networking.knative.dev/visibility` | Cluster-local network visibility  |
| `sidecar.istio.io/inject`           | Istio sidecar injection          |


### Runtime Revision Labels

Applied by the controller to the OME-managed ControllerRevisions used for [runtime pinning](/ome/docs/concepts/runtime-revision). Not user-set.

| Label                         | Description                                                                                          |
|-------------------------------|-----------------------------------------------------------------------------------------------------|
| `ome.io/runtime-of`           | Name of the source ServingRuntime/ClusterServingRuntime the revision snapshots.                     |
| `ome.io/runtime-of-kind`      | Kind of the source runtime (`ServingRuntime` or `ClusterServingRuntime`).                           |
| `ome.io/runtime-of-namespace` | Namespace of the source runtime (empty for cluster-scoped).                                          |
| `ome.io/revision-hash`        | Content hash of the snapshotted runtime spec; used to find-or-create revisions.                     |
| `ome.io/created-by`           | Marks the revision as OME-created (value `ome-controller`); gates the immutability webhook and GC.  |

### Model Management Labels

| Label                               | Description                                                       |
|-------------------------------------|-------------------------------------------------------------------|
| `models.ome.io/category`            | Category of the model.                                            |
| `models.ome.io/node-name`           | Node the model artifact is managed on (set by the model agent).   |
| `models.ome.io/managed-by`          | Identifies the manager of the model artifact.                     |
| `models.ome/reserve-model-artifact` | Marks a model artifact to be reserved (retained, not reclaimed).  |

## Special Values

### Autoscaler Classes

- `hpa`: Horizontal Pod Autoscaler
- `keda`: Kubernetes Event-driven Autoscaling
- `external`: External autoscaler

### Scale Metrics

- `cpu`: CPU utilization
- `memory`: Memory utilization
- `concurrency`: Request concurrency
- `rps`: Requests per second


### Priority Classes

- `volcano-scheduling-high-priority`: High priority for Volcano scheduling
- `kueue-scheduling-high-priority`: High priority for Kueue workload scheduling
