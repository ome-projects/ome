# OEP-007: Multi-Instance Serving

<!--
This OEP describes how OME adds support for NVIDIA Multi-Instance GPU (MIG)
serving using node-level MIG reconfiguration driven by InferenceService demand
and applied through NVIDIA mig-parted.
-->

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [User Stories](#user-stories)
    - [Story 1: Platform operator enables MIG-backed serving](#story-1-platform-operator-enables-mig-backed-serving)
    - [Story 2: Application owner deploys a MIG-sized model](#story-2-application-owner-deploys-a-mig-sized-model)
    - [Story 3: Accelerator-aware runtime selection chooses a MIG profile](#story-3-accelerator-aware-runtime-selection-chooses-a-mig-profile)
    - [Story 4: OME cleans up MIG assignments safely](#story-4-ome-cleans-up-mig-assignments-safely)
  - [Notes/Constraints/Caveats](#notesconstraintscaveats)
  - [Risks and Mitigations](#risks-and-mitigations)
- [Design Details](#design-details)
  - [Background](#background)
  - [Architecture Overview](#architecture-overview)
  - [Configuration Model](#configuration-model)
  - [Controller Behavior](#controller-behavior)
  - [MIG Manager Behavior](#mig-manager-behavior)
  - [Resource and Scheduling Flow](#resource-and-scheduling-flow)
  - [Operational Workflow](#operational-workflow)
  - [API and Configuration Examples](#api-and-configuration-examples)
  - [Test Plan](#test-plan)
    - [Prerequisite testing updates](#prerequisite-testing-updates)
    - [Unit Tests](#unit-tests)
    - [Integration Tests](#integration-tests)
  - [Graduation Criteria](#graduation-criteria)
- [Implementation History](#implementation-history)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
<!-- /toc -->

## Summary

This OEP introduces multi-instance serving in OME by adding support for NVIDIA
Multi-Instance GPU (MIG) backed workloads. The design allows OME to recognize
MIG resource requests in serving runtimes, map those requests to a desired MIG
configuration, label an appropriate node with the requested configuration, and
apply that configuration on the node through a dedicated `mig-manager`
component that uses NVIDIA `mig-parted`.

The feature targets an important gap in GPU utilization. Today, OME can deploy
GPU-backed inference workloads, but it does not manage how a physical GPU is
partitioned for smaller or more predictable serving units. With MIG, a single
GPU can be split into multiple isolated GPU instances, each with dedicated
compute and memory resources. That enables denser placement for smaller models,
reduces over-allocation of full GPUs, and creates a more consistent serving
shape for runtimes that are tuned to a specific GPU slice size. NVIDIA
positions MIG as a hardware-based partitioning mechanism that provides strong
isolation and predictable quality of service, while `mig-parted` provides a
supported way to declaratively manage the desired MIG layout on a node.

In OME, this capability is implemented as an operator-driven workflow rather
than a new top-level API. Runtime authors request a specific MIG extended
resource such as `nvidia.com/mig-2g.20gb`, the controller derives the
corresponding MIG config name, selects an eligible node, persists the
assignment in `InferenceService` annotations, and updates node labels so the
node-local manager can converge the physical device state. The resulting design
keeps the user-facing API small while fitting into Kubernetes-native scheduling
and OME’s existing reconciliation model.

## Motivation

Many inference workloads do not need an entire high-end GPU. Small and
medium-sized models often fit comfortably inside a fraction of an A100 or H100,
while still benefiting from GPU acceleration. Without MIG-aware orchestration,
operators must either waste large GPUs on undersized workloads or maintain
manually partitioned clusters with out-of-band processes for reconfiguration.
That increases operational burden and reduces effective cluster utilization.

NVIDIA’s MIG model exposes partitioned slices as distinct schedulable resources
through the device plugin and GPU operator stack. Different profiles, such as
`1g`, `2g`, `3g`, or `7g`, trade off memory and compute slices, and the set of
supported profiles depends on the GPU SKU. OME needs a way to connect those
schedulable resources to actual node reconfiguration so that an inference
workload can request a MIG slice declaratively and be placed onto a node that
is made ready for that slice shape.

This OEP is motivated by three practical needs:

1. **Higher GPU utilization** by placing right-sized inference workloads on MIG
   instances rather than whole GPUs.
2. **Operator-managed reconfiguration** so MIG state is converged by OME rather
   than a separate manual workflow.
3. **Minimal API expansion** by reusing standard Kubernetes resource requests,
   node labels, and annotations instead of introducing a new CRD for the first
   iteration.

### Goals

1. Enable OME runtimes to request NVIDIA MIG resources using standard
   Kubernetes extended resources.
2. Add controller logic that detects MIG resource requests and assigns them to
   compatible nodes.
3. Add a node-local MIG manager daemonset (provided out-of-box with OME) that
   applies desired MIG configuration through `mig-parted`.
4. Persist assignment state so OME can reconcile, update, and clean up MIG
   configurations safely across `InferenceService` lifecycle events.
5. Support an initial set of MIG profiles through a simple deterministic mapping
   from requested resource name to node configuration name.
6. Keep the design compatible with NVIDIA GPU Operator and device plugin based
   clusters.
7. Provide an option to disable OME-managed dynamic allocation entirely and use
   only preconfigured MIG nodes.
8. Ensure dynamic allocation can target only nodes explicitly marked as
   eligible for MIG reconfiguration.

### Non-Goals

1. Introduce a general-purpose GPU partitioning API independent of NVIDIA MIG,
   such as Guest Instance Management (GIM) or multi-virtual GPU (From AMD).
2. Introduce cloud-provider-specific or stage-addon specific dynamic
   allocation mechanisms as part of this initial design.
3. Support dynamic synthesis of arbitrary valid MIG layouts from many mixed
   requests in this first iteration.
4. Solve bin-packing across multiple MIG profiles on the same node beyond the
   initial assignment heuristic.
5. Replace NVIDIA GPU Operator, device plugin management, or cluster-level GPU
   provisioning flows.
6. Add MIG support for non-NVIDIA accelerators or vendor-neutral partitioning.
7. Guarantee zero-disruption reconfiguration for nodes currently serving active
   workloads; this design focuses first on correctness and safe convergence.

## Proposal

OME will support multi-instance serving by treating MIG as a node-level
configuration concern driven by workload resource requests.

At a high level, the proposal is:

1. A `ServingRuntime` or generated workload pod template requests a MIG extended
   resource such as `nvidia.com/mig-2g.20gb`.
2. During `InferenceService` reconciliation, OME inspects owned deployments,
   detects MIG resource requests, and derives a desired MIG config name from the
   requested resource.
3. Depending on configuration, the controller either selects a compatible node
   that is explicitly marked as eligible for MIG dynamic allocation and updates
   it with the desired MIG configuration labels, or restricts placement to
   nodes that are already preconfigured with the requested MIG resource.
4. When dynamic allocation is enabled, a `mig-manager` daemon running on each
   node watches that node, applies the requested config with
   `nvidia-mig-parted`, and reports progress and outcome back through node
   labels and events.
5. OME stores MIG assignments in an annotation on the `InferenceService` so it
   can preserve intent, handle updates, and release configurations during
   cleanup.
6. When dynamic allocation is enabled and a service is removed or changes away
   from a given MIG profile, OME releases the assignment and resets node labels
   when no other service still depends on that node/config pairing.

This creates a closed reconciliation loop between desired serving shape,
controller assignment, node-level application, and lifecycle cleanup.

### User Stories

#### Story 1: Platform operator enables MIG-backed serving

A platform operator who needs MIG enablement can choose between two operating
modes. In dynamic-allocation mode, the operator deploys the OME-provided
`mig-manager` DaemonSet and provides a `mig-parted` configuration map with
allowed profiles. The operator also marks the subset of nodes that OME is
allowed to reconfigure dynamically. After that, OME can reconfigure only those
opted-in nodes on demand instead of requiring the operator to log into nodes
and manually repartition GPUs.

The operator can also disable dynamic allocation entirely. In that mode, OME
does not change node MIG layout and instead schedules workloads only onto nodes
that are already partitioned with the required MIG resources. The operator can
choose to define a static configuration to support MIG static assignments.

#### Story 2: Application owner creates InferenceService for a MIG-sized model

An application owner selects a MIG enabled serving runtime tuned for a model. That
runtime requests one `nvidia.com/mig-2g.20gb` device. The user does not need to
set node labels manually. OME identifies the MIG request, selects a node,
applies the corresponding MIG configuration, and schedules the model onto that
node once the resource becomes available.

#### Story 3: Accelerator-aware runtime selection chooses a MIG pr ofile

A platform team publishes runtimes for the same model family, with one
runtime targeting MIG instances targetting different GPU types (A100 vs H100).
When accelerator-aware runtime selection is used, OME can select the runtime
whose accelerator requirements match the available MIG-backed node pool. For
example, a smaller model can resolve to a runtime that requests
`nvidia.com/mig-1g.10gb` for H100 or `nvidia.com/mig-2g.10gb` for A100, allowing
the same predictable performance from runtime irrespective of type of MIG instances.

After runtime selection completes, MIG handling follows the selected runtime’s
resource requirements. If the chosen runtime requests a MIG resource, OME
resolves the corresponding MIG config, selects or validates a compatible node,
and either dynamically applies the MIG layout on an explicitly opted-in node or
relies on a preconfigured MIG node, depending on the operating mode.

#### Story 4: OME cleans up MIG assignments safely

A team deletes an `InferenceService` that previously caused a node to be
configured for a `2g.20gb` profile. OME inspects assignments from other active
services before removing node labels or resetting the configuration. If another
service still depends on the same node/profile pairing, OME leaves that
configuration in place.

### Notes/Constraints/Caveats

- MIG support depends on NVIDIA hardware and a cluster environment that exposes
  MIG resources as Kubernetes extended resources.
- Supported profile names and allowable combinations depend on the underlying
  GPU model. The initial OME design assumes administrators provide valid
  `mig-parted` configuration entries for the targeted hardware.
- Reconfiguring MIG can require that active GPU consumers are drained first. The
  implementation includes placeholders for drain-aware operation, lock-based
  serialization, and optional reboot handling, but the operational experience is
  still constrained by node state and NVIDIA driver requirements.
- The current implementation uses a fixed mapping from requested resource name
  to config name, such as `nvidia.com/mig-2g.20gb` -> `all-2g.20gb`. This keeps
  the first iteration simple but does not yet model mixed-profile layouts.
- Scheduling convergence depends on the device plugin refreshing allocatable
  resources after MIG reconfiguration.
- When dynamic allocation is disabled, OME does not perform node relabeling for
  MIG reconfiguration and relies on preconfigured MIG nodes.
- When dynamic allocation is enabled, OME selects only nodes explicitly marked
  as eligible for MIG reconfiguration.

### Risks and Mitigations

**Risk: invalid or unsupported MIG profile on a given GPU SKU**

If OME requests a config that is not valid for the hardware in the node,
`mig-parted` application fails and the node can remain unavailable for that
workload.

**Mitigation:** keep profile-to-config mapping explicit, require administrators
to ship only valid `mig-parted` config entries for their hardware, surface node
error labels and Kubernetes events, and limit initial scope to known-good
profiles.

**Risk: disruption to workloads on a node during reconfiguration**

Changing MIG layout can require stopping GPU consumers, so an unsafe change
could interrupt running inference traffic.

**Mitigation:** serialize changes with a node lock, gate actions through node
state labels, support GPU client detection, and make drain behavior explicit in
manager configuration. Operators can further reduce risk by using dedicated MIG
serving pools.

**Risk: controller assigns the same node inconsistently across services**

Without persisted assignment state, repeated reconciliations could oscillate or
tear down a config still used by another service.

**Mitigation:** persist assignment metadata on the `InferenceService`, collect
other active assignments before reset, and only remove node-level config or
resource labels when no other service still references them.

**Risk: limited packing efficiency**

A simple mapping like `all-2g.20gb` is easy to reason about but may underuse a
GPU versus mixed-profile packing.

**Mitigation:** explicitly scope the first phase to homogeneous per-node config
selection. Future work can add richer packing or a scheduler extension if this
becomes a bottleneck.

## Design Details

### Background

NVIDIA MIG partitions a single physical GPU into isolated GPU instances with
fixed fractions of compute and memory. Those instances are exposed to workloads
as independent resources, enabling multiple inference servers to share one GPU
with stronger isolation than time-slicing alone. `mig-parted` is NVIDIA’s
declarative utility for applying named MIG layouts on supported nodes.

In Kubernetes environments, MIG resources are typically surfaced as extended
resource names like `nvidia.com/mig-2g.20gb`. OME builds on that model rather
than inventing a parallel resource abstraction.

### Architecture Overview

The design introduces two cooperating control loops:

1. **InferenceService reconciliation loop** in the controller manager.
2. **Node-local MIG convergence loop** in `mig-manager`.

The second loop is optional and is used only when dynamic allocation is
enabled.

The controller loop is responsible for discovering MIG demand from workload
specifications, selecting a target node, and recording desired configuration.
The node-local loop is responsible for turning that desired configuration into
actual node device state.

The main components are:

- `pkg/controller/v1beta1/inferenceservice/mig_profile.go`: derives MIG demand,
  tracks assignments, and manages node labels.
- `internal/migmanager/*`: node-local reconciler, `mig-parted` invocation,
  locking, GPU client awareness, and state reporting.
- `config/mig-manager/*`: deployment manifests, RBAC, and seed config maps.
- Sample runtime and service manifests showing MIG-sized serving.

### Configuration Model

The design uses existing Kubernetes objects plus node labels and annotations.

The design supports two operating modes:

- **Dynamic allocation enabled**: OME selects only a node explicitly marked
  for MIG dynamic allocation, writes desired MIG configuration labels, and
  relies on `mig-manager` plus `mig-parted` to converge node state.
- **Dynamic allocation disabled**: OME does not mutate node MIG configuration
  and schedules only onto nodes that already advertise the required MIG
  extended resources.

**Serving workload declaration**

A runtime declares MIG demand through standard resource requests and limits,
for example:

```yaml
resources:
  requests:
    nvidia.com/mig-2g.20gb: "1"
  limits:
    nvidia.com/mig-2g.20gb: "1"
```

**Node desired state**

The controller applies labels to the selected node, including:

- desired MIG config label, based on `constants.NvidiaMigConfigLabel`
- MIG strategy label
- state and error labels used by the node-local manager
- an additional label to reflect the requested MIG resource class for selection

Nodes are also expected to carry an explicit opt-in label or equivalent marker
that indicates they are eligible for OME-managed MIG dynamic allocation. Nodes
without that marker are not selected for dynamic reconfiguration.

**InferenceService assignment state**

OME persists the chosen node, requested resource, desired config, and whether it
was applied in an annotation on the owning `InferenceService`. This allows OME
to distinguish new, updated, and stale assignments during reconciliation and
cleanup.

**mig-parted configuration**

Administrators provide named MIG configurations through a ConfigMap consumed by
`mig-manager`. A minimal example in the current repo includes entries such as:

```yaml
version: v1
mig-configs:
  all-disabled:
  - devices: all
    mig-enabled: false

  all-2g.20gb:
  - devices: all
    mig-enabled: true
    mig-devices:
      "2g.20gb": 7
```

This format matches the `mig-parted` configuration style of named layouts and
lets OME reference a config by name during node reconciliation.

### Controller Behavior

The controller implementation detects and manages MIG-backed deployments as part
of `InferenceService` reconciliation.

1. **Collect requests**
   - List deployments owned by the `InferenceService`.
   - Inspect pod specs for resource requests whose names match a MIG resource
     pattern.
   - Produce one or more MIG requests containing deployment name, resource,
     quantity, and scheduling hints.

2. **Load existing assignments**
   - Read the MIG assignment annotation from the `InferenceService`.
   - Compare desired deployment/resource pairs with previously stored entries.

3. **Resolve config name**
   - Map the MIG resource name to a desired config, for example
     `nvidia.com/mig-2g.20gb` -> `all-2g.20gb`.
   - Reuse an existing assignment when the requested resource and config still
     match.

4. **Select node**
   - Find a compatible node for the request, considering current node labels and
     placement constraints.
   - When dynamic allocation is enabled, only consider nodes explicitly marked as
     eligible for MIG dynamic allocation, then apply the desired MIG config
     label and any companion selection label on the chosen node if needed.
   - When dynamic allocation is disabled, only consider nodes that already
     satisfy the requested MIG resource.

5. **Persist assignment**
   - Store the chosen node, resource, config, and whether the config label was
     newly applied in the `InferenceService` annotation.

6. **Release stale assignments**
   - When assignments disappear or change, inspect other live services.
   - When dynamic allocation is enabled, reset node config or resource labels
     only when no other service still uses that node/config or node/resource
     combination.

This approach intentionally keeps assignment state at the `InferenceService`
layer instead of introducing a separate scheduling CRD.

### MIG Manager Behavior

`mig-manager` is deployed as a privileged DaemonSet and reconciles only the node
on which it runs.

This component is optional when dynamic allocation is disabled.

Its responsibilities are:

1. Watch the local node state and desired labels.
2. Reconcile only when the node is marked as eligible for MIG dynamic
   allocation.
3. Load the allowed config names from the configured `mig-parted` file.
4. Detect whether a node needs reconfiguration.
5. Serialize changes with a local lock file so only one change runs at a time.
6. Optionally inspect configured GPU clients before applying changes.
7. Execute `nvidia-mig-parted apply -f <config> -c <desired>` with the host root
   and NVML library path configured in the container environment.
8. Verify the result with `nvidia-smi -L`.
9. Update node labels and annotations with success, failure, timestamps, and
   error details.

The current implementation exposes configurable paths and label keys such as:

- `--config-file`
- `--gpu-clients-file`
- `--host-root`
- `--mig-parted-path`
- `--nvidia-smi-path`
- `--lock-file`
- `--default-config`
- `--mig-config-label`
- `--mig-state-label`
- `--mig-force-label`
- `--mig-drain-label`

Operationally, the DaemonSet mounts host paths and the config maps required to
run `mig-parted` against host drivers and device state.

### Resource and Scheduling Flow

The end-to-end flow is:

1. A runtime requests a MIG extended resource.
2. OME creates or updates the workload deployment.
3. The controller sees the MIG request and either assigns a node/config on a
   node explicitly marked for dynamic reconfiguration or targets a node that is
   already preconfigured.
4. When dynamic allocation is enabled, the node label change triggers
   `mig-manager` on the selected node.
5. When dynamic allocation is enabled, `mig-manager` applies the named MIG
   layout with `mig-parted`.
6. NVIDIA software on the node advertises the resulting MIG resources, or they
   are already advertised on preconfigured nodes.
7. Kubernetes schedules the workload onto a node that satisfies the MIG
   resource request.
8. On deletion or reconfiguration, OME removes the assignment and, when dynamic
   allocation is enabled, resets labels only when safe.

This design deliberately separates **desired config assignment** from **actual
resource advertisement**. OME does not fabricate resources; it converges the
node so the NVIDIA stack can advertise them.

### Operational Workflow

A cluster operator enables the feature by:

1. Choosing whether OME should manage MIG allocation dynamically or rely only
   on preconfigured MIG nodes.
2. If dynamic allocation is enabled, marking the subset of nodes that are
   eligible for OME-managed MIG reconfiguration.
3. If dynamic allocation is enabled, deploying the `mig-manager` manifests.
4. If dynamic allocation is enabled, providing a `mig-parted` config map with
   valid named layouts for the target GPU types.
5. Ensuring the cluster’s NVIDIA stack supports MIG and exposes MIG resources.
6. Creating runtimes that request MIG resources.
7. Optionally dedicating node pools to MIG-backed inference to reduce
   reconfiguration disruption.

A runtime author then publishes a MIG-specific runtime, such as the included
`config/runtimes/srt/meta/llama-3-1-8b-mig-serving.yaml`, which requests
`nvidia.com/mig-2g.20gb: 1`.

An application owner deploys an `InferenceService` referencing that runtime,
such as `config/samples/isvc/meta/llama-3-1-8b-instruct-mig.yaml`.

### API and Configuration Examples

**Sample runtime**
Sample runtime requesting 

```
apiVersion: ome.io/v1beta1
kind: ClusterServingRuntime
.
.
.
  engineConfig:
    runner:
      name: ome-container
      image: docker.io/lmsysorg/sglang:v0.5.10.post1-cu129-amd64
      ports:
        - containerPort: 8080
          name: http1
          protocol: TCP
      command:
        - python3
        - -m
        - sglang.launch_server
        - --model-path
        - $(MODEL_PATH)
        - --tp-size
        - "1"
      resources:
        requests:
          cpu: 10
          memory: 30Gi
          nvidia.com/mig-2g.20gb: "1"
        limits:
          cpu: 10
          memory: 30Gi
          nvidia.com/mig-2g.20gb: "1"

```

### Test Plan

[ ] I/we understand that component owners may require updates to existing tests
before accepting changes necessary for this enhancement.

##### Prerequisite testing updates

- Add or extend controller tests that cover MIG request extraction from owned
  deployment pod specs.
- Add or extend manager unit tests around config parsing, desired/apply state
  transitions, and failure reporting.

#### Integration Tests

- Deploy a MIG runtime and verify the controller labels a node with the desired
  config.
- Verify `mig-manager` transitions node state from pending/applying to success
  when a valid named config exists.
- Verify invalid config selection produces failure labels and warning events.
- Verify deleting an `InferenceService` clears assignments only when no other
  service still references the same node/config.
- Verify sample MIG runtime scheduling succeeds once the node advertises the
  requested MIG resource.

### Graduation Criteria

**Alpha**

- MIG-backed runtimes can be declared using Kubernetes MIG extended resources.
- OME can assign nodes, invoke `mig-parted`, and persist MIG assignments.
- Basic failure reporting is available through node labels and events.
- Sample runtime and service manifests demonstrate end-to-end behavior.

**Beta**

- Integration tests validate end-to-end reconfiguration and cleanup.
- Drain behavior and GPU client coordination are validated in realistic node
  conditions.
- Operator guidance covers supported hardware and required NVIDIA stack setup.
- More than one validated MIG profile is documented and tested.

**Stable**

- The design is proven across supported NVIDIA GPU families and common OME
  serving runtimes.
- Recovery behavior for interrupted or partially applied reconfiguration is
  well-tested.
- Operational playbooks exist for upgrades, rollback, and troubleshooting.

## Implementation History

- April 2026: initial OEP drafted from the existing design draft PDF and the
  current `mig-manager` and controller implementation on top of OME.

## Drawbacks

- The initial mapping from MIG resource name to config name is intentionally
  simple and limits packing flexibility.
- Node-level reconfiguration adds operational complexity and can disrupt active
  GPU users if not isolated carefully.
- The design depends on NVIDIA-specific tooling and profile semantics.
- Troubleshooting spans multiple layers: OME controller, node labels,
  `mig-manager`, `mig-parted`, NVIDIA drivers, and device plugin refresh.

## Alternatives

1. **Manual MIG preconfiguration only**

   Operators could pre-slice nodes outside OME and let workloads consume the
   exposed MIG resources directly. This is simpler, but it pushes lifecycle
   management and cleanup entirely to operators and does not let OME reconcile
   desired state.

2. **Introduce a dedicated MIG CRD**

   OME could model desired MIG layouts as a first-class API. This may become
   attractive later, especially for richer packing logic, but it adds API and
   controller complexity before the basic workflow is proven.

3. **Scheduler-driven packing first**

   OME could invest in a custom scheduler or scheduler extension for globally
   optimal MIG placement. That is more powerful, but also significantly more
   complex than the current node-label-driven design.

4. **Support mixed-profile layouts immediately**

   OME could attempt to compute valid composite layouts from multiple services.
   That would be more efficient in some clusters, but it requires profile-aware
   packing logic and deeper validation against NVIDIA’s hardware-specific MIG
   constraints. The current design intentionally starts with homogeneous,
   operator-provided named layouts.
