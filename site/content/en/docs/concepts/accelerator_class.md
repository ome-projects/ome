---
title: "Accelerator Class"
date: 2026-09-26
weight: 27
description: >
  AcceleratorClass catalogs a GPU type for your cluster — how to author one, which discovery fields actually drive node matching, and how to read and troubleshoot its status.
---

## What is an AcceleratorClass?

`AcceleratorClass` is a **cluster-scoped** resource that describes one class of accelerator hardware in your cluster — for example "NVIDIA H100 80GB" — in terms of its identity (vendor, family, model), the node labels that identify it, its capabilities, the extended resources it exposes, and its cost. It is a catalog entry, not a workload: creating one deploys nothing.

Two consumers read it:

- **The AcceleratorClass controller** (part of the OME manager) continuously matches cluster nodes against each class and records the matching node names in the class's status. That status backs the `NODES` printer column:

  ```bash
  kubectl get acceleratorclasses
  ```

  ```
  NAME              VENDOR   FAMILY   MEMORY   NODES
  nvidia-a100-80g   nvidia   ampere   80Gi     4
  nvidia-h100       nvidia   hopper   80Gi     2
  nvidia-h200       nvidia   hopper   141Gi    0
  ```

- **InferenceService accelerator selection** reads the class **spec** — never the status — to pick a class per component and shape the generated pods (node selector, node affinity, GPU resource requests). See [Select Accelerators for an InferenceService](/ome/docs/tasks/run-workloads/select-accelerators/) for the policies and constraints, and [Explain Accelerator Selection](/ome/docs/tasks/kubectl-ome-accelerator-explain/) for the CLI that reports what was selected.

Because selection reads only the spec, discovery and selection are deliberately decoupled: a class showing `NODES: 0` can still be selected, and its pods will simply stay `Pending` until a matching node appears (useful with cluster autoscalers that scale GPU pools from zero).

## Authoring an AcceleratorClass

A complete class for an H100 node pool:

```yaml
apiVersion: ome.io/v1beta1
kind: AcceleratorClass
metadata:
  name: nvidia-h100
spec:
  vendor: nvidia
  family: hopper
  model: h100
  discovery:
    nodeSelector:                # drives node discovery; also merged into pods
      nvidia.com/gpu.product: NVIDIA-H100-80GB-HBM3
  capabilities:
    memoryGB: 80Gi
    computeCapability: "9.0"
    memoryBandwidthGBps: "3350"
    features: ["tensor-cores", "fp8", "nvlink"]
    performance:
      fp16Tflops: 989
      int8Tops: 1979
  resources:                     # drives node discovery; also applied to pods
    - name: nvidia.com/gpu
      quantity: "8"
  cost:
    perHour: "4.5"
    spotPerHour: "2.1"
    tier: high
```

`discovery` and `capabilities` are required blocks in the schema, but all of their members are optional — `discovery: {}` and `capabilities: {}` are valid (and a discovery block with no `nodeSelector` matches every node, see below).

What each block is actually used for:

| Block | Read by | Effect |
|-------|---------|--------|
| `vendor`, `family` | Selection constraints | Matched (case-insensitively) against the `architectureFamilies` constraint as `<vendor>-<family>` or `family` alone. `model` is informational. |
| `discovery.nodeSelector` | Discovery controller **and** pod generation | Every key=value must match a node's labels for the node to count in status. Also merged into the generated pod's `nodeSelector` when the class is selected. |
| `discovery.affinity` | Pod generation only | **Not consulted for discovery.** When the selected class has one and the component spec sets no `affinity`, the node-affinity part is copied to the pod. Pod (anti-)affinity terms are never copied. |
| `discovery.pciVendorID`, `discovery.deviceIDs` | Nothing | Declared in the API but not consumed by any controller today. They do not influence discovery. |
| `capabilities.*` | Selection constraints and policies | `memoryGB`, `computeCapability`, `features`, `memoryBandwidthGBps`, and `performance` feed constraint filtering and policy scoring. **None of them influence node discovery.** |
| `resources` | Discovery controller **and** pod generation | Each entry's `name` must exist with a non-zero quantity in a node's capacity for the node to count in status. On selection, each entry is set as both request and limit on the runner container (`quantity` defaults to `"1"`; `divisible` is declared but not consumed today). |
| `cost` | The `Cheapest` selection policy | Not used in discovery. |
| `integration` | Nothing | `kueueResourceFlavor` and `volcanoGPUType` are declared in the API but not consumed by any controller today. |

## How node discovery works

The AcceleratorClass controller lists all nodes on every reconcile and admits a node into `status.nodes` when **both** checks pass:

1. **Label match** — every key=value pair in `discovery.nodeSelector` matches the node's labels exactly. An empty or missing `nodeSelector` passes every node.
2. **Capacity match** — every entry in `spec.resources` names a resource that is present **and non-zero** in the node's `status.capacity`. A class with no `resources` skips this check.

That is the complete algorithm. In particular, discovery does **not**:

- Compare quantities. A node with 1 GPU passes a class that declares `quantity: "8"` — only presence and non-zero are checked, against `capacity` (not `allocatable`).
- Check node readiness or schedulability. Cordoned and `NotReady` nodes still count.
- Check current allocation. A node whose GPUs are fully occupied by other pods still counts.
- Evaluate `discovery.affinity`, `discovery.pciVendorID`, `discovery.deviceIDs`, or anything under `capabilities`.

A class with an empty `nodeSelector` **and** no `resources` therefore matches every node in the cluster.

### When status updates

The controller reconciles a class, and recomputes its node list, whenever:

- The class itself is created or updated.
- Any node is created or deleted.
- Any node's **labels or capacity** change. Other node updates (conditions, heartbeats, images) are filtered out and do not trigger reconciles.

Every node event fans out to a reconcile of every AcceleratorClass, so a label or capacity change is reflected in status promptly — there is no polling interval to wait for.

## Status reference

| Field | Populated? | Meaning |
|-------|------------|---------|
| `status.nodes` | Yes | Names of all nodes that pass discovery, sorted alphabetically. |
| `status.availableNodes` | Yes | `len(status.nodes)`. Backs the `NODES` printer column. |
| `status.lastUpdated` | Yes | Stamped **only when `nodes`/`availableNodes` actually change** — a reconcile that computes the same node list does not bump it. It answers "when did membership last change", not "when did the controller last look". |
| `status.totalAccelerators` | **No** | Declared in the API but never populated by the controller. Always 0. |
| `status.availableAccelerators` | **No** | Declared but never populated. Always 0 — do not alert on it. |
| `status.conditions` | **No** | Declared but never populated. |

Keep in mind that `availableNodes` counts nodes matching the discovery rules, not free capacity: it does not decrease as pods consume the GPUs.

## Why does my class report zero nodes?

Work through these in order:

1. **A `discovery.nodeSelector` label doesn't match.** Matching is exact string equality on every pair. Compare against the actual node labels:

   ```bash
   kubectl get acceleratorclass nvidia-h100 -o jsonpath='{.spec.discovery.nodeSelector}'
   kubectl get nodes -L nvidia.com/gpu.product
   ```

   Watch for near-misses in values populated by [GPU Feature Discovery](https://github.com/NVIDIA/k8s-device-plugin) (e.g. `NVIDIA-H100-80GB-HBM3` vs `NVIDIA-H100-PCIE-80GB`).

2. **A `spec.resources` entry names a resource the nodes don't expose.** Each declared name must appear with a non-zero quantity in node capacity, which requires the vendor's device plugin to be running on the node:

   ```bash
   kubectl get node <node-name> -o jsonpath='{.status.capacity}'
   ```

   If `nvidia.com/gpu` is absent or `"0"` here, fix the device plugin — the class definition is not the problem.

3. **You expected `pciVendorID`, `deviceIDs`, `affinity`, or `capabilities` to drive discovery.** They don't (see the table above). If those are your only discovery hints, the class matches on `nodeSelector`/`resources` alone — add a `nodeSelector` that identifies the hardware.

4. **No matching nodes currently exist.** With autoscaled GPU pools scaled to zero this is normal; the class is still selectable, and scheduling the generated pods is what brings nodes up.

## Deleting a class

The controller manages a finalizer (`acceleratorclasses.ome.io/finalizer`) on every class. There is no external cleanup behind it — on deletion the controller simply removes the finalizer, so deletes complete promptly.

Deleting a class does not touch running pods, but it affects the next reconcile of InferenceServices that reference it: a service that pins the class by name fails with an `AcceleratorClassError` warning event, while policy-based selection silently skips the missing name in the runtime's candidate list.

## Next steps

- [Select Accelerators for an InferenceService](/ome/docs/tasks/run-workloads/select-accelerators/) — the selection policies, constraints, and how a selected class shapes the generated pods
- [Explain Accelerator Selection](/ome/docs/tasks/kubectl-ome-accelerator-explain/) — read-only CLI report of declared vs. selected classes
- [Serving Runtime concepts](/ome/docs/concepts/serving_runtime/#accelerator-requirements) — declaring which classes a runtime supports
- [OME v1beta1 API reference](/ome/docs/reference/ome.v1beta1) — the full `AcceleratorClass` field reference
