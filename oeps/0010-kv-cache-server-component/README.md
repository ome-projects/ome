# OEP-0010: KVCachePool Custom Resource

<!--
This OEP proposes a provider-neutral KVCachePool custom resource for OME. A
KVCachePool is an independently managed, namespace-scoped resource parallel to
model and runtime in the InferenceService dependency model. InferenceService
references the pool; ServingRuntime declares how its serving engine can connect
to the pool.
-->

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [Resource Model](#resource-model)
  - [Naming](#naming)
  - [Deployment Modes](#deployment-modes)
  - [Provider and Backend Model](#provider-and-backend-model)
  - [External Provider Inputs](#external-provider-inputs)
  - [User Stories](#user-stories)
    - [Story 1: Pre-create a Node-local LMCache Pool](#story-1-pre-create-a-node-local-lmcache-pool)
    - [Story 2: Bind an InferenceService to an Existing Pool](#story-2-bind-an-inferenceservice-to-an-existing-pool)
    - [Story 3: Share a Pool Across Services](#story-3-share-a-pool-across-services)
    - [Story 4: Deploy a Mooncake Distributed Store](#story-4-deploy-a-mooncake-distributed-store)
    - [Story 5: Configure Runtime-side Connector Behavior](#story-5-configure-runtime-side-connector-behavior)
  - [Notes/Constraints/Caveats](#notesconstraintscaveats)
  - [Risks and Mitigations](#risks-and-mitigations)
- [Design Details](#design-details)
  - [API Specifications](#api-specifications)
    - [KVCachePool](#kvcachepool)
    - [InferenceService Extension](#inferenceservice-extension)
    - [ServingRuntime Extension](#servingruntime-extension)
    - [Status](#status)
  - [Example Configuration](#example-configuration)
    - [LMCache NodeLocal Pool](#lmcache-nodelocal-pool)
    - [Mooncake DistributedStore Pool](#mooncake-distributedstore-pool)
    - [ServingRuntime with KV Cache Connectors](#servingruntime-with-kv-cache-connectors)
    - [InferenceService Reference](#inferenceservice-reference)
  - [Reconciliation Flow](#reconciliation-flow)
    - [KVCachePool Controller](#kvcachepool-controller)
    - [InferenceService Controller](#inferenceservice-controller)
  - [Connector Merge Rules](#connector-merge-rules)
  - [Provider Adapter Contracts](#provider-adapter-contracts)
  - [Scheduling and Resource Management](#scheduling-and-resource-management)
  - [Readiness, Status, and Failure Behavior](#readiness-status-and-failure-behavior)
  - [Observability](#observability)
  - [Security](#security)
  - [Backward Compatibility](#backward-compatibility)
  - [Implementation Plan](#implementation-plan)
  - [Test Plan](#test-plan)
    - [Prerequisite Testing Updates](#prerequisite-testing-updates)
    - [Unit Tests](#unit-tests)
    - [Integration Tests](#integration-tests)
  - [Graduation Criteria](#graduation-criteria)
- [Implementation History](#implementation-history)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
<!-- /toc -->

## Summary

This OEP introduces `KVCachePool`, a namespace-scoped OME custom resource for
managing distributed KV cache capacity and transfer services. A `KVCachePool`
is not an `InferenceService` component. It is parallel to model and runtime in
the serving dependency model:

```text
InferenceService
  -> model
  -> runtime
  -> kvCachePool
```

Users create a `KVCachePool` independently. An `InferenceService` can later
reference the pool through `spec.kvCachePool`, which is reference-only in
alpha. The selected `ServingRuntime` declares `spec.kvCacheConnectors`, which
describes how that runtime's engine and decoder components can attach to pools
provided by LMCache, Mooncake, or future providers.

The `KVCachePool` API is provider-neutral. It separates:

1. pool-wide cache intent, such as capacity and eviction policy;
2. provider identity and provider-scoped configuration;
3. backend identity and backend-scoped configuration;
4. pool workloads, where pod and container configuration lives; and
5. status connection information consumed by runtime-side connector injection.

The first release supports LMCache `NodeLocal`. OME runs one cache server per
selected node. `RawDeployment` and `DistributedStore` describe possible future
pool implementations.

## Motivation

LLM serving systems increasingly rely on KV cache reuse, prefix reuse,
prefill-decode transfer, and cache-aware runtime integration to improve latency
and throughput. A single serving pod's local KV cache is not sufficient when
requests are distributed across replicas, when prefill and decode are
separated, or when serving pods restart and lose reusable state.

Early design work treated KV cache as another `InferenceService` component
beside engine, decoder, and router. That was too narrow. Real KV cache systems
are often deployed and operated independently from any one inference endpoint:

1. LMCache multiprocess deployments can provide node-local cache services that
   serving pods attach to.
2. LMCache recommends a DaemonSet for shared node-local multiprocess servers.
3. Mooncake provides a distributed store model with master and store/client
   roles.
4. NIXL can appear as a transfer or storage backend below another provider
   rather than as an OME-managed server process.

OME therefore needs a pool-level resource that can be created first, managed
independently, and referenced by any compatible `InferenceService` in the same
namespace.

### Goals

1. Add a namespace-scoped `KVCachePool` CRD.
2. Make `KVCachePool` independently managed and not owned by
   `InferenceService`.
3. Add `InferenceService.spec.kvCachePool` as a reference-only binding in
   alpha.
4. Add `ServingRuntime.spec.kvCacheConnectors` for runtime-side connector
   support.
5. Keep the pool API provider-neutral and extensible to LMCache, Mooncake,
   NIXL-backed configurations, and future providers.
6. Reuse existing OME workload configuration types where appropriate,
   including `PodSpec`, `RunnerSpec`, and `ComponentExtensionSpec`.
7. Keep all pool pod/container configuration under `spec.workloads[]`.
8. Publish normalized connection information in `KVCachePool.status`.
9. Preserve existing `InferenceService` behavior when `spec.kvCachePool` is
   absent.

### Non-Goals

1. Add `kvCache` as an `InferenceService` component parallel to engine,
   decoder, and router.
2. Add `ClusterKVCachePool` in the initial design.
3. Add externally managed pool binding in alpha.
4. Add router-level KV-cache-aware routing in alpha.
5. Guarantee durable KV entry persistence across provider workload replacement.
6. Validate model-level compatibility between a pool and an
   `InferenceService`.
7. Make every runtime automatically use a KV cache pool.
8. Replace engine-local prefix caching or provider-native local cache features.
9. Expose provider-specific configuration as top-level OME API fields.

## Proposal

### Resource Model

`KVCachePool` is a namespace-scoped resource:

```yaml
apiVersion: ome.io/v1beta1
kind: KVCachePool
metadata:
  name: shared-kv-cache
  namespace: llm
spec:
  provider:
    name: LMCache
    config:
      l1CapacityPerNode: 60Gi
  deploymentMode: NodeLocal
  workloads:
    - name: server
      nodeSelector:
        node-type: gpu
```

`InferenceService` references the pool:

```yaml
spec:
  model:
    name: qwen3-14b
  runtime:
    name: vllm-lmcache
  kvCachePool:
    name: shared-kv-cache
```

The pool does not know which serving engine will use it. Runtime-side connector
metadata lives in `ServingRuntime.spec.kvCacheConnectors`.

### Naming

Use `KVCachePool`, not `KVCacheServer`.

`KVCachePool` describes the user-facing resource: a managed pool of distributed
KV cache capacity and transfer endpoints. `KVCacheServer` sounds like one
process or one Kubernetes Service. A pool can span nodes or coordinate
multiple store roles.

Recommended names:

- CRD kind: `KVCachePool`
- API field on `InferenceService`: `spec.kvCachePool`
- Go ref type: `KVCachePoolRef`
- Go spec type: `KVCachePoolSpec`
- Status condition: `KVCachePoolReady`
- Runtime connector field: `ServingRuntime.spec.kvCacheConnectors`
- Internal controller name: `KVCachePoolReconciler`

The term "cache server" may be used only when describing a provider workload
that actually runs a server process.

### Deployment Modes

`KVCachePool` uses the JSON field name `deploymentMode`, following OME API
vocabulary. It uses a separate enum from `InferenceService` because the valid
pool modes differ from serving endpoint modes.

```go
// +kubebuilder:validation:Enum=NodeLocal
type KVCachePoolDeploymentMode string

const KVCachePoolNodeLocal KVCachePoolDeploymentMode = "NodeLocal"
```

Current and proposed modes:

- `RawDeployment` (proposed): OME directly reconciles simple Kubernetes Deployment-backed
  pool workloads.
- `NodeLocal`: OME directly reconciles one pool workload per selected node.
  The implementation may use a DaemonSet, but the API names the pool semantics,
  not the Kubernetes object.
- `DistributedStore` (proposed): OME directly reconciles coordinated store roles such as
  master and store/client workloads.

`External` is deferred until OME defines a clear status, ownership, and
validation contract for infrastructure it does not manage.

### Provider and Backend Model

The API distinguishes provider from backend.

`provider.name` identifies the primary integration layer used by the pool. A
provider integration defines the pool workloads and the connection settings
that compatible engines consume.

`provider.backends[]` identifies storage or transfer backends used underneath
the provider. For example, LMCache may be the provider while Mooncake or NIXL
is configured as a backend.

Provider-specific configuration belongs under `provider.config`.
Backend-specific configuration belongs under `provider.backends[].config`.
There is no giant top-level `providerConfig` bag.

### External Provider Inputs

The API shape is influenced by these provider designs:

1. [LMCache multiprocess deployment](https://docs.lmcache.ai/mp/index.html)
   motivates `NodeLocal` and node-local connection discovery.
2. [LMCache storage backends](https://docs.lmcache.ai/kv_cache/storage_backends/mooncake.html)
   motivate separating **provider** from **backend** because LMCache can use
   Mooncake or NIXL beneath the LMCache integration layer.
3. [Mooncake Store](https://kvcache-ai.github.io/Mooncake/design/mooncake-store.html)
   motivates `DistributedStore` and named pool workloads such as `master` and
   `store`.
4. NIXL motivates backend extensibility without assuming every KV cache
   capability is a standalone OME-managed server process.

### User Stories

#### Story 1: Pre-create a Node-local LMCache Pool

Alice is a platform operator. She creates a namespace-scoped LMCache-backed
`KVCachePool` with `deploymentMode: NodeLocal` before any
`InferenceService` exists. OME reconciles node-local pool workloads and
publishes connection information in `status.connection`.

#### Story 2: Bind an InferenceService to an Existing Pool

Bob owns a model endpoint. He selects a runtime that advertises LMCache
connector support and references Alice's pool from `spec.kvCachePool`. He does
not configure provider workloads or connector args on the `InferenceService`.
The inference controller derives connector injection from the pool status and
the selected runtime.

#### Story 3: Share a Pool Across Services

Carol binds two services in one namespace to the same NodeLocal pool. On a
shared node, the second engine can reuse compatible KV entries written by the
first. When she deletes the first service, the second keeps using the same
cache server.

#### Story 4: Deploy a Mooncake Distributed Store

Dave deploys a Mooncake-backed `KVCachePool` with
`deploymentMode: DistributedStore`. The pool declares named workloads such as
`master` and `store`. OME reconciles those workloads and publishes the master
RPC and metadata endpoints in status.

#### Story 5: Configure Runtime-side Connector Behavior

Emma owns a `ServingRuntime` for vLLM. She declares
`spec.kvCacheConnectors` with component-specific connector settings for engine
and decoder. In prefill-decode serving, engine and decoder can receive
different connector roles, args, and environment variables.

### Notes/Constraints/Caveats

1. **Independent lifecycle:** Deleting an `InferenceService` does not delete a
   referenced `KVCachePool`.
2. **Namespace scope:** `KVCachePool` is namespace-scoped in alpha.
3. **Reference-only binding:** `InferenceService.spec.kvCachePool` carries only
   reference fields in alpha.
4. **Engine-agnostic pool:** `KVCachePool` does not mention vLLM, SGLang,
   engine, decoder, or router. Runtime-side integration belongs to
   `ServingRuntime`.
5. **OME-managed alpha:** OME directly manages the NodeLocal DaemonSet,
   Service and connection ConfigMap. Pure
   external pools are future work.
6. **Workloads only:** Pod and container configuration for a pool lives only in
   `spec.workloads[]`.
7. **Compatibility responsibility:** The user is responsible for choosing a
   `ServingRuntime` compatible with the referenced `KVCachePool`.
8. **Controller validation:** OME validates that the referenced pool exists,
   is ready, exposes usable connection information, and has a matching runtime
   connector. It does not prove model-level safety.

### Risks and Mitigations

**Risk 1: Provider APIs leak into the portable OME API**

- *Mitigation:* Keep common pool intent in typed fields. Keep provider-native
  settings scoped under `provider.config` or `backends[].config`.

**Risk 2: Users select an incompatible runtime and pool**

- *Mitigation:* Keep `InferenceService.spec.kvCachePool` reference-only, but
  require the selected `ServingRuntime` to advertise a matching
  `kvCacheConnectors` entry. Surface clear events and readiness conditions when
  no matching connector exists.

**Risk 3: Connector injection overrides user service configuration**

- *Mitigation:* Define an explicit merge order:
  runtime component config, generated connector config, connector overrides,
  then `InferenceService` component config. Service values remain highest
  precedence.

**Risk 4: Pool deployment modes become Kubernetes-object names**

- *Mitigation:* Use semantic mode names where appropriate. For example,
  `NodeLocal` rather than `DaemonSet`, and `DistributedStore` rather than
  `StatefulSet`.

**Risk 5: Premature deletion interrupts shared consumers**

- *Mitigation:* Hold the pool finalizer until referring services, their active
  ReplicaSets and their draining Pods release the pool. Delete pool resources
  explicitly so foreground cascading deletion also respects this ordering.

## Design Details

### API Specifications

#### KVCachePool

Add a namespace-scoped `KVCachePool` resource:

```go
// KVCachePool is the Schema for distributed KV cache pools.
// +k8s:openapi-gen=true
// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Provider",type="string",JSONPath=".spec.provider.name"
// +kubebuilder:printcolumn:name="Mode",type="string",JSONPath=".spec.deploymentMode"
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:resource:path=kvcachepools,shortName=kvcp
type KVCachePool struct {
    metav1.TypeMeta   `json:",inline"`
    metav1.ObjectMeta `json:"metadata,omitempty"`

    Spec   KVCachePoolSpec   `json:"spec,omitempty"`
    Status KVCachePoolStatus `json:"status,omitempty"`
}
```

Pool spec:

```go
type KVCachePoolSpec struct {
    // Provider identifies the primary KV cache integration layer.
    // +required
    Provider KVCacheProviderSpec `json:"provider"`

    // DeploymentMode describes how OME reconciles this pool.
    // +required
    DeploymentMode KVCachePoolDeploymentMode `json:"deploymentMode"`

    // Cache describes provider-neutral cache policy.
    // +optional
    Cache *KVCachePolicySpec `json:"cache,omitempty"`

    // Workloads contains pod and container configuration for OME-managed pool
    // roles. This is the only place where pool pod/container config appears.
    // +optional
    // +listType=map
    // +listMapKey=name
    Workloads []KVCachePoolWorkloadSpec `json:"workloads,omitempty"`
}
```

Provider and backend specs:

```go
// +kubebuilder:validation:Enum=LMCache;Mooncake;NIXL
type KVCacheProvider string

type KVCacheProviderSpec struct {
    // Name identifies the primary provider.
    // +required
    Name KVCacheProvider `json:"name"`

    // Version optionally constrains the provider implementation version.
    // +optional
    Version *string `json:"version,omitempty"`

    // Backends identifies storage or transfer backends used by the provider.
    // +optional
    // +listType=map
    // +listMapKey=name
    Backends []KVCacheBackendSpec `json:"backends,omitempty"`

    // Config contains provider-scoped settings that are not portable OME API.
    // +optional
    Config *runtime.RawExtension `json:"config,omitempty"`
}

// +kubebuilder:validation:Enum=Local;Mooncake;NIXL;Redis;Filesystem
type KVCacheBackendType string

type KVCacheBackendSpec struct {
    // Name identifies this backend entry.
    // +required
    Name string `json:"name"`

    // Type identifies the backend implementation.
    // +required
    Type KVCacheBackendType `json:"type"`

    // Config contains backend-scoped settings that are not portable OME API.
    // +optional
    Config *runtime.RawExtension `json:"config,omitempty"`
}
```

Cache policy:

```go
// +kubebuilder:validation:Enum=LRU;LFU;FIFO;ProviderDefault
type KVCacheEvictionPolicy string

type KVCachePolicySpec struct {
    // Capacity is the intended total pool capacity. Providers translate it to
    // native size, memory, segment, or storage settings.
    // +optional
    Capacity *resource.Quantity `json:"capacity,omitempty"`

    // TTLSeconds optionally limits cache entry lifetime.
    // +optional
    TTLSeconds *int64 `json:"ttlSeconds,omitempty"`

    // EvictionPolicy is the desired eviction behavior.
    // +optional
    EvictionPolicy *KVCacheEvictionPolicy `json:"evictionPolicy,omitempty"`

    // ChunkSize is a provider-neutral chunk/page/block size hint.
    // +optional
    ChunkSize *resource.Quantity `json:"chunkSize,omitempty"`

    // Keyspace optionally scopes generated cache keys.
    // +optional
    Keyspace *string `json:"keyspace,omitempty"`
}
```

Workload spec:

```go
type KVCachePoolWorkloadSpec struct {
    // Name identifies the provider or backend role, such as server, master, or
    // store.
    // +required
    Name string `json:"name"`

    // PodSpec provides pod-level customization for this pool workload.
    // +optional
    PodSpec `json:",inline"`

    // ComponentExtensionSpec reuses OME workload knobs such as replicas,
    // autoscaling, labels, annotations, PDB, and deployment strategy.
    // +optional
    ComponentExtensionSpec `json:",inline"`

    // Runner customizes the primary container for this workload.
    // +optional
    Runner *RunnerSpec `json:"runner,omitempty"`
}
```

#### InferenceService Extension

Add `KVCachePool *KVCachePoolRef` to `InferenceServiceSpec`:

```go
type InferenceServiceSpec struct {
    // Existing fields omitted.

    // KVCachePool references a namespace-scoped KVCachePool. In alpha this is
    // reference-only; connector behavior is derived from the pool and runtime.
    // +optional
    KVCachePool *KVCachePoolRef `json:"kvCachePool,omitempty"`
}

type KVCachePoolRef struct {
    // Name of the KVCachePool being referenced.
    // +required
    Name string `json:"name"`

    // Kind of the referenced resource. Defaults to KVCachePool.
    // +optional
    // +kubebuilder:default="KVCachePool"
    Kind *string `json:"kind,omitempty"`

    // APIGroup of the referenced resource. Defaults to ome.io.
    // +optional
    // +kubebuilder:default="ome.io"
    APIGroup *string `json:"apiGroup,omitempty"`
}
```

`KVCachePoolRef` does not contain connector args, target components, roles, or
provider config in alpha.

#### ServingRuntime Extension

Add `KVCacheConnectors []KVCacheConnectorSpec` to `ServingRuntimeSpec`:

```go
type ServingRuntimeSpec struct {
    // Existing fields omitted.

    // KVCacheConnectors describes runtime-side support for attaching serving
    // components to referenced KVCachePools.
    // +optional
    // +listType=map
    // +listMapKey=provider
    KVCacheConnectors []KVCacheConnectorSpec `json:"kvCacheConnectors,omitempty"`
}
```

Connector spec:

```go
type KVCacheConnectorSpec struct {
    // Provider identifies the pool provider this connector supports.
    // +required
    Provider KVCacheProvider `json:"provider"`

    // DeploymentModes lists supported pool deployment modes. Empty means all
    // modes supported by this provider adapter.
    // +optional
    // +listType=atomic
    DeploymentModes []KVCachePoolDeploymentMode `json:"deploymentModes,omitempty"`

    // Components provides component-specific connector configuration.
    // +optional
    Components map[ComponentType]KVCacheConnectorComponentSpec `json:"components,omitempty"`
}

type KVCacheConnectorComponentSpec struct {
    // ConnectorConfig is typed connector intent interpreted by the
    // provider/runtime adapter.
    // +optional
    ConnectorConfig *KVCacheConnectorConfig `json:"connectorConfig,omitempty"`

    // RuntimeArgsOverride provides connector-specific runtime args. Matching
    // args replace existing values; missing args are appended.
    // +optional
    // +listType=atomic
    RuntimeArgsOverride []string `json:"runtimeArgsOverride,omitempty"`

    // EnvironmentOverride provides connector-specific environment variables.
    // +optional
    EnvironmentOverride map[string]string `json:"environmentOverride,omitempty"`
}

type KVCacheConnectorConfig struct {
    // ConnectorClass names the runtime connector implementation, such as
    // LMCacheConnectorV1.
    // +optional
    ConnectorClass *string `json:"connectorClass,omitempty"`

    // Role describes the component role understood by the runtime adapter, such
    // as kv_both, kv_producer, or kv_consumer.
    // +optional
    Role *string `json:"role,omitempty"`

    // ConnectionRefName optionally selects a named connection entry published
    // by KVCachePool status.
    // +optional
    ConnectionRefName *string `json:"connectionRefName,omitempty"`
}
```

`KVCacheConnectorConfig` is intentionally typed. It is not a
`runtime.RawExtension`. Provider-specific escaping remains on the pool provider
and backend specs.

#### Status

`KVCachePool` publishes normalized connection information in status:

```go
type KVCachePoolStatus struct {
    duckv1.Status `json:",inline"`

    // Phase is a coarse lifecycle summary.
    // +optional
    Phase KVCachePoolPhase `json:"phase,omitempty"`

    // Connection contains normalized connection information consumed by
    // ServingRuntime connector adapters.
    // +optional
    Connection *KVCachePoolConnectionStatus `json:"connection,omitempty"`

    // Workloads reports provider workload status.
    // +optional
    // +listType=map
    // +listMapKey=name
    Workloads []KVCachePoolWorkloadStatus `json:"workloads,omitempty"`
}

type KVCachePoolConnectionStatus struct {
    // Endpoint is the primary in-cluster endpoint when one exists.
    // +optional
    Endpoint *apis.URL `json:"endpoint,omitempty"`

    // Ports lists named connection ports.
    // +optional
    // +listType=map
    // +listMapKey=name
    Ports []KVCachePoolPortStatus `json:"ports,omitempty"`

    // ConfigMapRef points to provider-generated connection config when needed.
    // +optional
    ConfigMapRef *corev1.LocalObjectReference `json:"configMapRef,omitempty"`

    // SecretRef points to provider-generated credentials when needed.
    // +optional
    SecretRef *corev1.LocalObjectReference `json:"secretRef,omitempty"`

    // ProviderStatus contains provider-scoped observed state, not desired
    // configuration.
    // +optional
    ProviderStatus *runtime.RawExtension `json:"providerStatus,omitempty"`
}

type KVCachePoolPortStatus struct {
    Name string `json:"name"`
    Port int32  `json:"port"`
}

type KVCachePoolWorkloadStatus struct {
    Name string `json:"name"`
    ReadyReplicas int32 `json:"readyReplicas,omitempty"`
    DesiredReplicas int32 `json:"desiredReplicas,omitempty"`
}
```

The controller should set the standard `Ready` condition and a
`KVCachePoolReady` condition alias if OME wants a domain-specific condition.

### Example Configuration

#### LMCache NodeLocal Pool

```yaml
apiVersion: ome.io/v1beta1
kind: KVCachePool
metadata:
  name: lmcache-node-local
spec:
  provider:
    name: LMCache
    config:
      l1CapacityPerNode: 60Gi
  deploymentMode: NodeLocal
  workloads:
    - name: server
      runner:
        image: lmcache/standalone:v0.5.1-cu129@sha256:c64d5704427e0c21e8b12d460f01a6cf33971ee2fdee2fbced268e4aa10d7d12
      nodeSelector:
        node-type: gpu
      tolerations:
        - key: nvidia.com/gpu
          operator: Exists
          effect: NoSchedule
```

A `60Gi` L1 budget applies independently to each selected node. Two ready
servers supply 120 GiB in aggregate. Engines use the pool's ClusterIP Service
with `internalTrafficPolicy: Local`; a node without a ready local endpoint
cannot use a remote server. OME publishes the generated vLLM connection in
`status.connection.configMapRef`. See the [runnable example](../../config/samples/kvcachepool/).

#### Mooncake DistributedStore Pool

```yaml
apiVersion: ome.io/v1beta1
kind: KVCachePool
metadata:
  name: mooncake-store
  namespace: llm
spec:
  provider:
    name: Mooncake
    config:
      metadata:
        mode: http
  deploymentMode: DistributedStore
  cache:
    capacity: 640Gi
    chunkSize: 256
  workloads:
    - name: master
      minReplicas: 1
      runner:
        image: mooncake/mooncake-transfer-engine:latest
        command:
          - mooncake_master
        args:
          - --enable_http_metadata_server=true
          - --http_metadata_server_host=0.0.0.0
          - --http_metadata_server_port=8080
          - --rpc_port=50051
          - --metrics_port=9003
        ports:
          - name: rpc
            containerPort: 50051
          - name: metadata
            containerPort: 8080
          - name: metrics
            containerPort: 9003
        resources:
          requests:
            cpu: "4"
            memory: 8Gi
    - name: store
      minReplicas: 4
      runner:
        image: mooncake/mooncake-transfer-engine:latest
        command:
          - mooncake_client
        ports:
          - name: rpc
            containerPort: 50052
        resources:
          requests:
            cpu: "8"
            memory: 180Gi
      volumes:
        - name: cache-data
          emptyDir: {}
```

#### ServingRuntime with KV Cache Connectors

```yaml
apiVersion: ome.io/v1beta1
kind: ServingRuntime
metadata:
  name: vllm-lmcache-mp
spec:
  supportedModelFormats:
    - modelFormat:
        name: safetensors
        version: "1.0.0"
      modelArchitecture: Qwen2ForCausalLM
      autoSelect: false
  kvCacheConnectors:
    - provider: LMCache
      deploymentModes: [NodeLocal]
      components:
        engine:
          connectorConfig:
            connectorClass: LMCacheMPConnector
  engineConfig:
    hostIPC: true
    tolerations:
      - key: nvidia.com/gpu
        operator: Exists
        effect: NoSchedule
    runner:
      name: ome-container
      image: lmcache/vllm-openai:v0.5.1-cu129
      command: [vllm, serve]
      args:
        - $(MODEL_PATH)
        - --served-model-name
        - qwen
        - --port
        - "8080"
        - --max-model-len
        - "4096"
      resources:
        limits:
          nvidia.com/gpu: "1"
      ports:
        - name: http1
          containerPort: 8080
      readinessProbe:
        httpGet:
          path: /health
          port: 8080
```

#### InferenceService Reference

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: qwen-cache
spec:
  deploymentMode: RawDeployment
  model:
    kind: BaseModel
    name: qwen2-5-0-5b
  runtime:
    kind: ServingRuntime
    name: vllm-lmcache-mp
  kvCachePool:
    name: lmcache-node-local
  engine:
    minReplicas: 1
    maxReplicas: 1
```

### Reconciliation Flow

#### KVCachePool Controller

1. Watch `KVCachePool` resources.
2. Validate provider, deployment mode, backend entries, cache policy, and
   workload names.
3. Select a provider adapter based on `spec.provider.name`.
4. Validate provider and deployment-mode compatibility.
5. Reconcile implementation resources:
   - `RawDeployment`: Deployment, Service, HPA, PDB as needed.
   - `NodeLocal`: DaemonSet, node-local Service and connection ConfigMap.
   - `DistributedStore`: provider role workloads such as master and store.
6. Build normalized `status.connection`.
7. Update `Ready` and workload status.

#### InferenceService Controller

When `spec.kvCachePool` is absent, existing behavior is unchanged.

When `spec.kvCachePool` is present:

1. Reconcile model as today.
2. Select or validate `ServingRuntime` as today.
3. Fetch the referenced namespaced `KVCachePool`.
4. Require `KVCachePool` to be ready and to publish usable connection status.
5. Find a `ServingRuntime.spec.kvCacheConnectors` entry matching:
   - pool provider;
   - pool deployment mode; and
   - target serving component.
6. Generate connector args and env from typed `connectorConfig` plus
   `KVCachePool.status.connection`.
7. Apply connector merge rules.
8. Reconcile engine, decoder, and router through existing workload paths.
9. Surface clear conditions and events when connector injection cannot be
   derived.

### Connector Merge Rules

Connector injection uses a three-way merge:

```text
ServingRuntime component config
  -> generated connector config
  -> connector runtimeArgsOverride / environmentOverride
  -> InferenceService component config
```

`InferenceService` component values have highest precedence, matching OME's
existing runtime/service merge model.

`RuntimeArgsOverride` follows the existing OME argument merge behavior:

1. If an argument key exists, override its value.
2. If an argument key does not exist, append it.

Environment merge should preserve the same precedence. Generated connector env
and connector `EnvironmentOverride` should not overwrite explicit
`InferenceService` component env values.

### Provider Adapter Contracts

Introduce two internal adapter boundaries.

Pool provider adapter:

```go
type KVCachePoolProviderAdapter interface {
    Provider() v1beta1.KVCacheProvider
    ValidatePool(pool *v1beta1.KVCachePool) field.ErrorList
    ReconcilePool(ctx context.Context, pool *v1beta1.KVCachePool) (*KVCachePoolConnectionStatus, error)
}
```

Runtime connector adapter:

```go
type KVCacheRuntimeConnectorAdapter interface {
    Provider() v1beta1.KVCacheProvider
    BuildConnectorPatch(
        connector v1beta1.KVCacheConnectorSpec,
        component v1beta1.ComponentType,
        pool *v1beta1.KVCachePool,
    ) (*KVCacheConnectorPatch, error)
}
```

The pool adapter owns provider resource reconciliation and pool status. The
runtime connector adapter owns serving-runtime-specific injection details.

### Scheduling and Resource Management

`KVCachePool` reuses OME workload configuration patterns inside
`spec.workloads[]`.

Pool workloads:

1. use their own resources, node selectors, affinity, tolerations,
   scheduler name, priority class, security context, volumes, and image pull
   secrets;
2. do not inherit `InferenceService` model volume mounts;
3. do not inherit engine or decoder accelerator placement by default;
4. may run on CPU, memory-optimized, GPU, or RDMA-capable nodes depending on
   provider needs and user scheduling config; and
5. may use OME scaling, PDB, and deployment strategy fields where applicable.

### Readiness, Status, and Failure Behavior

`KVCachePool` is ready when:

1. provider resources are successfully reconciled;
2. required workloads are available;
3. provider health checks pass where available; and
4. `status.connection` contains the data required for runtime connector
   injection.

An `InferenceService` that references an unready pool should be marked not ready
with an actionable event. It should not silently serve without the requested
pool, because the reference declares a desired serving dependency.

Provider validation should fail fast for:

1. unsupported provider;
2. unsupported deployment mode for provider;
3. duplicate workload names;
4. missing required workload roles for a mode;
5. missing runner image when a workload requires one;
6. provider config that cannot be decoded by the selected adapter; and
7. missing or stale pool connection information.

### Observability

Controller-level metrics:

1. `ome_kvcachepool_reconcile_total`
2. `ome_kvcachepool_reconcile_duration_seconds`
3. `ome_kvcachepool_ready`
4. `ome_kvcachepool_connection_ready`
5. `ome_kvcache_connector_injection_total`
6. `ome_kvcache_provider_errors_total`

Pool workloads should preserve provider metrics ports configured in
`workloads[].runner.ports`.

Recommended labels:

```text
ome.io/kvcachepool=<pool-name>
ome.io/kvcachepool-workload=<workload-name>
ome.io/kvcache-provider=<provider-name>
```

### Security

1. Pool services should default to internal cluster access.
2. Provider management endpoints should not be exposed through inference
   ingress by default.
3. Provider credentials should use Kubernetes Secret references, not inline
   provider config values.
4. NetworkPolicy integration should be possible through stable labels.
5. Cross-namespace pool references are not supported in alpha.
6. Restrict permission to create and update LMCache pools to administrators
   trusted to deploy GPU-visible servers with host IPC.

### Backward Compatibility

This proposal is backward compatible:

1. Existing `InferenceService` objects omit `spec.kvCachePool` and behave the
   same.
2. Existing `ServingRuntime` objects omit `spec.kvCacheConnectors` and behave
   the same.
3. No existing component spec is removed or renamed.
4. No existing deployment mode enum is changed.
5. `KVCachePoolDeploymentMode` is a separate enum from
   `constants.DeploymentModeType`.

### Implementation Plan

1. Add API types:
   - `KVCachePool`
   - `KVCachePoolSpec`
   - `KVCacheProviderSpec`
   - `KVCacheBackendSpec`
   - `KVCachePolicySpec`
   - `KVCachePoolWorkloadSpec`
   - `KVCachePoolStatus`
   - `KVCachePoolRef`
   - `KVCacheConnectorSpec`

2. Add code generation updates:
   - `make generate`
   - `make manifests`

3. Add `KVCachePool` controller:
   - provider adapter registry
   - direct workload reconciliation
   - status connection normalization

4. Add `InferenceService` integration:
   - fetch referenced pool
   - validate readiness and connection status
   - match runtime connector
   - apply connector merge rules

5. Add provider adapters:
   - LMCache `NodeLocal`
   - Mooncake `DistributedStore` design/validation

6. Add samples:
   - LMCache NodeLocal
   - Mooncake DistributedStore
   - vLLM runtime with `kvCacheConnectors`
   - InferenceService referencing a pool

7. Add docs:
   - API reference
   - operational guide
   - troubleshooting for readiness and connector injection

### Test Plan

[x] I/we understand that component owners may require updates to existing tests
before accepting changes necessary for this enhancement.

##### Prerequisite Testing Updates

Existing `InferenceService` tests assume the dependency model only includes
model and runtime. Tests must be updated to cover optional `kvCachePool`
references without changing behavior when the field is absent.

#### Unit Tests

- API validation for `KVCachePool` required fields, enum values, workload list
  semantics, and deepcopy generation.
- API validation for `InferenceService.spec.kvCachePool`.
- API validation for `ServingRuntime.spec.kvCacheConnectors`.
- Provider adapter selection and validation.
- LMCache `NodeLocal` resource construction.
- Mooncake `DistributedStore` workload validation.
- Status connection normalization.
- Runtime connector matching by provider and deployment mode.
- Connector merge order:
  runtime config -> generated connector -> connector overrides -> service
  config.
- Argument merge behavior where missing args are appended.
- Environment merge behavior where service env has highest precedence.

#### Integration Tests

1. Create `KVCachePool` with `deploymentMode: NodeLocal` and verify OME creates
   provider workloads and status connection.
2. Create `InferenceService` referencing a ready pool and a compatible runtime;
   verify engine connector injection.
3. Create engine and decoder runtime connector config with different roles;
   verify component-specific args/env.
4. Reference a missing pool; verify actionable warning and not-ready status.
5. Reference an unready pool; verify serving reconciliation waits or reports
   not-ready.
6. Reference a ready pool with a runtime lacking matching
   `kvCacheConnectors`; verify clear error condition.
7. Delete an `InferenceService`; verify referenced `KVCachePool` remains.
8. Delete a `KVCachePool`; verify implementation resources are cleaned up.

### Graduation Criteria

#### Alpha

1. `KVCachePool` CRD is available.
2. `InferenceService.spec.kvCachePool` reference is available.
3. `ServingRuntime.spec.kvCacheConnectors` is available.
4. LMCache `NodeLocal` works end-to-end for vLLM.
5. Pool status publishes connection information.
6. Connector injection respects merge precedence.
7. Samples and basic troubleshooting documentation exist.

#### Beta

1. LMCache NodeLocal rollout, deletion and server restart behavior are validated.
2. Mooncake `DistributedStore` design is implemented or explicitly deferred
   with a validated sample.
3. Engine and decoder role injection is covered by integration tests.
4. Provider status and metrics are documented.
5. Upgrade and rollback behavior is documented.

#### Stable

1. At least two provider or provider/backend combinations are validated.
2. Failure-mode guidance is documented for production use.
3. API fields have proven sufficient for the supported pool implementations.
4. External pool binding is either added in a follow-up OEP or explicitly left
   out of scope.

## Implementation History

- 2026-05-09: Initial OEP drafted as an `InferenceService` `kvCache`
  component.
- 2026-05-10: Reworked design to introduce namespace-scoped `KVCachePool` CRD,
  reference-only `InferenceService.spec.kvCachePool`, and runtime-side
  `ServingRuntime.spec.kvCacheConnectors`.

## Drawbacks

Adding a separate CRD increases the number of resources users must understand.
Users now need to manage the lifecycle of a pool and choose compatible runtimes.
The upside is a cleaner ownership model and the ability to create pools before
serving endpoints exist.

Runtime connector injection adds complexity to the inference controller. The
merge order must be carefully tested to avoid overwriting user-provided engine
or decoder settings.

## Alternatives

### Inline `InferenceService.spec.kvCache`

The initial design added `kvCache` as a component parallel to engine, decoder,
and router.

This proposal rejects that approach because KV cache pools may be created and
operated independently. Multiple services can share a pool and release their
references at different times.

### `KVCacheServer` CRD

OME could expose a `KVCacheServer` resource.

This proposal rejects that name because many valid implementations are not one
server process. A `KVCachePool` can run a server on each selected node or
coordinate a distributed store.

### Cluster-scoped `ClusterKVCachePool`

OME could add both namespaced and cluster-scoped variants.

This proposal defers cluster scope. The initial resource is namespaced to keep
ownership, RBAC, service discovery, and connection status straightforward.

### Top-level Runner Fields

OME could put `runner`, `PodSpec`, and scaling fields directly on
`KVCachePoolSpec`.

This proposal rejects that shape because it conflicts with multi-role providers
such as Mooncake. All pod and container configuration lives under
`spec.workloads[]`.

### Separate `managementMode`

OME could add a `managementMode` field to distinguish OME-managed pools
from external pools.

This proposal rejects that field. Following the existing `InferenceService`
pattern, `deploymentMode` carries the reconciliation strategy. The first release implements `NodeLocal`.

### External Pools in Alpha

OME could allow `InferenceService` to bind to arbitrary existing endpoints or
ConfigMaps.

This proposal defers that. External pools need a clear ownership, readiness,
and validation contract, and should be added only after OME-managed pools are
well understood.
