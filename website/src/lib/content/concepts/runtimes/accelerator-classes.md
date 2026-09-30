---
title: Accelerator classes
description: "An AcceleratorClass names one type of GPU and the nodes that have it, so an InferenceService can run its engine and decoder on that type, picked by name or by policy."
---

An AcceleratorClass names one type of GPU in your cluster, such as the NVIDIA H100, and says which nodes have it. An [InferenceService](../serving/inference-services.md) picks a class for its engine and decoder, by name or with a policy that finds the best-fitting, cheapest or most capable one. OME then runs those pods on the class's nodes. A [serving runtime](serving-runtimes.md) lists the classes it runs on, and can tune its settings for each.

OME installs no classes, and they're optional: without one, the pods keep the runtime's settings. Create a class for each GPU type when a runtime runs on more than one, or when you want OME to choose the GPU. A class is cluster-scoped, and creating one deploys nothing.

## Write an AcceleratorClass

These two classes describe nodes with eight NVIDIA H100 or eight L40S GPUs. Both find their nodes by the `nvidia.com/gpu.product` label, which NVIDIA GPU Feature Discovery sets, and by the `nvidia.com/gpu` resource from the NVIDIA device plugin. Take the label values from your nodes, with `kubectl get nodes -L nvidia.com/gpu.product`. The runtime in [Per-accelerator configuration](serving-runtimes.md#per-accelerator-configuration) runs on both:

```yaml title="accelerator-classes.yaml"
apiVersion: ome.io/v1beta1
kind: AcceleratorClass
metadata:
  name: nvidia-h100
spec:
  vendor: nvidia
  family: hopper
  model: h100
  discovery:
    nodeSelector:
      nvidia.com/gpu.product: NVIDIA-H100-80GB-HBM3
  capabilities:
    memoryGB: 80Gi
    computeCapability: "9.0"
    memoryBandwidthGBps: "3350"
    features:
      - tensor-cores
      - fp8
      - nvlink
    performance:
      fp16Tflops: 989
      int8Tops: 1979
  resources:
    - name: nvidia.com/gpu
      quantity: "8"
  cost:
    perHour: "4.5"
    spotPerHour: "2.1"
    tier: high
---
apiVersion: ome.io/v1beta1
kind: AcceleratorClass
metadata:
  name: nvidia-l40s
spec:
  vendor: nvidia
  family: ada
  model: l40s
  discovery:
    nodeSelector:
      nvidia.com/gpu.product: NVIDIA-L40S
  capabilities:
    memoryGB: 48Gi
    computeCapability: "8.9"
    memoryBandwidthGBps: "864"
    features:
      - tensor-cores
      - fp8
    performance:
      fp16Tflops: 362
      int8Tops: 733
  resources:
    - name: nvidia.com/gpu
      quantity: "8"
  cost:
    perHour: "1.8"
    spotPerHour: "0.8"
    tier: medium
```

Apply the file:

```bash
kubectl apply -f accelerator-classes.yaml
```

```output
acceleratorclass.ome.io/nvidia-h100 created
acceleratorclass.ome.io/nvidia-l40s created
```

List the classes. This cluster has two H100 nodes, an L40S node and a node without GPUs:

```bash
kubectl get acceleratorclasses
```

```output
NAME          VENDOR   FAMILY   MEMORY   NODES
nvidia-h100   nvidia   hopper   80Gi     2
nvidia-l40s   nvidia   ada      48Gi
```

NODES counts the nodes that match the class, and is blank when none do; see [Why a class reports zero nodes](#why-a-class-reports-zero-nodes).

Only `discovery` and `capabilities` are required, and both can be `{}`, but a class without `memoryGB` or `computeCapability` fails the constraints that read them. Constraints and policies are InferenceService settings; see [Select accelerators](../../guides/deploy-models/select-accelerators.md).

| Field | What OME uses it for |
| --- | --- |
| `vendor`, `family` | The VENDOR and FAMILY columns, and the `architectureFamilies` constraint, which matches `hopper` or `nvidia-hopper`, ignoring case. Keep hyphens out of `family`, or only the `<vendor>-<family>` form matches. |
| `discovery.nodeSelector` | Finding matching nodes, and the pods' node selector. |
| `discovery.affinity` | The pods' node affinity. Node matching ignores it. |
| `capabilities.memoryGB` | The MEMORY column, the `minMemory` and `maxMemory` constraints, and the BestFit and MostCapable policies. |
| `capabilities.computeCapability` | The `minArchitectureVersion` constraint. Versions compare as text, so `10.0` is lower than `9.0`. |
| `capabilities.memoryBandwidthGBps` | The MostCapable policy. |
| `capabilities.features` | The `requiredFeatures` constraint, ignoring case. |
| `capabilities.performance` | The compute scores of the BestFit and MostCapable policies. |
| `resources` | Finding matching nodes, and the runner's requests and limits. |
| `cost` | The Cheapest policy. |

OME ignores `model`, `discovery.pciVendorID`, `discovery.deviceIDs`, `capabilities.levelZeroVersion`, `capabilities.clockSpeedMHz`, `capabilities.performance.latency`, `resources[].divisible` and `integration`. The [AcceleratorClassSpec](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-AcceleratorClassSpec) reference lists all the fields.

!!! warning "Write memoryGB in Gi"
    The `minMemory` and `maxMemory` constraints read `memoryGB` in whole gibibytes, rounded down: `80Gi` is 80, `80G` is 74, and `80`, which means 80 bytes, is 0.

!!! warning "Set the quantity to the GPUs in each pod"
    Unless an InferenceService sets its own runner resources, its pods get the class's `resources` in place of the runtime's, and `quantity` defaults to `1`. Set `quantity` to the GPUs each pod needs, `8` in these classes, to match the runtimes that list them.

## How OME finds matching nodes

A node matches a class when it has all the labels in `discovery.nodeSelector`, with the same values. Since v1.3, the node's capacity must also list all the resources in `resources`, above zero. A class that sets neither matches all nodes. On v1.2.2, OME ignores `resources` and instead requires the node's system memory to be at least `capabilities.memoryGB`, so a class can match nodes without its GPUs.

NODES counts matching nodes, not free GPUs. It includes nodes that are cordoned, not ready or busy, and a node with one GPU matches a class whose `quantity` is `8`.

The class's `status.nodes` lists the matching nodes, and `status.lastUpdated` says when that list last changed. OME leaves `totalAccelerators`, `availableAccelerators` and `conditions` unset. Print the matching nodes:

```bash
kubectl get acceleratorclasses \
  -o 'custom-columns=NAME:.metadata.name,NODES:.status.nodes,UPDATED:.status.lastUpdated'
```

```output
NAME          NODES                     UPDATED
nvidia-h100   [gpu-h100-1 gpu-h100-2]   2026-09-28T15:41:05Z
nvidia-l40s   <none>                    <none>
```

## Why a class reports zero nodes

A class reports zero nodes when no node has both its labels and its resources. Print what `nvidia-l40s` looks for:

```bash
kubectl get acceleratorclass nvidia-l40s \
  -o jsonpath='{.spec.discovery.nodeSelector}{"\n"}{.spec.resources}{"\n"}'
```

```output
{"nvidia.com/gpu.product":"NVIDIA-L40S"}
[{"name":"nvidia.com/gpu","quantity":"8"}]
```

Then print the nodes' product labels and GPU capacity:

```bash
kubectl get nodes \
  -o 'custom-columns=NAME:.metadata.name,PRODUCT:.metadata.labels.nvidia\.com/gpu\.product,GPUS:.status.capacity.nvidia\.com/gpu'
```

```output
NAME         PRODUCT                 GPUS
cpu-1        <none>                  <none>
gpu-h100-1   NVIDIA-H100-80GB-HBM3   8
gpu-h100-2   NVIDIA-H100-80GB-HBM3   8
gpu-l40s-1   NVIDIA-L40S             <none>
```

The output points to the cause:

| What the output shows | What to do |
| --- | --- |
| No node has the class's PRODUCT. Values must match exactly, so `NVIDIA-H100-80GB-HBM3` doesn't match `NVIDIA-H100-PCIe`. | Fix the class's `nodeSelector`, or the node labels. |
| A node has the PRODUCT, but its GPUS is `<none>` or `0`, as for `gpu-l40s-1`. | Fix the NVIDIA device plugin on the node. If the node reports its GPUs under another resource name, list that name in the class's `resources`. |
| No GPU node exists, because a cluster autoscaler scaled the pool to zero. | Nothing. InferenceServices can still pick the class, and their pending pods can make the autoscaler add a node. |

Once the device plugin reports the GPUs on `gpu-l40s-1`, the class counts it:

```bash
kubectl get acceleratorclasses
```

```output
NAME          VENDOR   FAMILY   MEMORY   NODES
nvidia-h100   nvidia   hopper   80Gi     2
nvidia-l40s   nvidia   ada      48Gi     1
```

## How InferenceServices use a class {#how-services-choose-a-class}

OME picks a class for the engine and the decoder when their runtime lists classes in `acceleratorRequirements.acceleratorClasses`. If the InferenceService names a class, in `spec.acceleratorSelector` or the component's `acceleratorOverride`, the component gets it. If it sets a `policy` there instead, the policy picks one of the runtime's classes that pass the constraints. With neither, the component gets no class: there's no default policy. [Select accelerators](../../guides/deploy-models/select-accelerators.md) shows both ways.

A policy ignores free capacity, so it can pick a class whose nodes are full or that no node matches yet. Constraints in an `acceleratorOverride` have no effect, so set them in `spec.acceleratorSelector`. With `BestFit`, always set at least one there, to avoid a [known bug](../../guides/deploy-models/select-accelerators.md#bestfit-doesnt-create-or-update-the-workload).

When OME picks the runtime itself, it considers only runtimes that list all the classes the InferenceService names; see [Runtime accelerator-class matching](../../reference/matching/runtime-accelerator-class-matching.md).

OME then changes the component's pods, including leader and worker pods:

| Pod setting | What OME does |
| --- | --- |
| Node selector | Adds `discovery.nodeSelector`, except for [merged fine-tuned weights](../models/fine-tuned-weights.md#what-the-pods-get). It wins over the runtime's node selectors, and the component's `nodeSelector` wins over it. |
| Resources | Sets each resource in `resources` as the runner's request and limit, overriding the runtime's. To keep your own, set `resources` in the component's `runner`, plus `leader.runner` and `worker.runner` if multi-node. |
| Node affinity | Since v1.3, adds `discovery.affinity.nodeAffinity`. The component's `affinity`, or the runtime's node affinity, takes its place. On v1.2.2, `discovery.affinity` replaces the pod's whole affinity when the component sets none. |
| Runtime settings | Applies the runtime's `acceleratorConfig` entry for the class; see [Per-accelerator configuration](serving-runtimes.md#per-accelerator-configuration). |

The InferenceService's status doesn't record the class. To see which class a component got, check its pods' node selector, as [Step 4 of Select accelerators](../../guides/deploy-models/select-accelerators.md#step-4-check-the-selected-class) shows.

## Change or delete a class

A change to a class reaches the InferenceServices that use it, even those pinned to a [runtime revision](runtime-revisions.md), because a revision doesn't include the class. Since v1.3, OME applies the change right away. On v1.2.2, an InferenceService picks it up the next time OME processes it for another reason.

You can delete a class that runtimes and InferenceServices use, and it goes away at once. For an InferenceService that names the class, OME then records an `AcceleratorClassError` Warning event and stops updating the serving workload. The message reads `Failed to get accelerator class for engine: accelerator class nvidia-l40s not found at cluster scope`. The running pods keep serving, and OME retries until the class exists again or you name another one.

Since v1.3, for an InferenceService with a policy, OME picks again from the runtime's other classes, or runs the pods without a class if none qualifies. On v1.2.2, the policy picks no class while any class that the runtime lists is missing.

Runtimes that list a deleted class keep working, but the admission webhook rejects creating or updating an enabled runtime that lists it:

```text
unknown accelerator classes referenced in AcceleratorRequirements: [nvidia-l40s]
```

Remove the class from the runtime's `acceleratorRequirements.acceleratorClasses`, or create it again. For the same reason, create classes before the runtimes that list them.

## Next steps

- [Select accelerators](../../guides/deploy-models/select-accelerators.md): pick a class by name, or let a policy choose one.
- [Serving runtimes](serving-runtimes.md): list the classes a runtime runs on, and tune it for each.
- [Runtime accelerator-class matching](../../reference/matching/runtime-accelerator-class-matching.md): how named classes narrow runtime selection, and why the classes that a runtime lists must exist.
