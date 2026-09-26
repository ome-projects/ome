---
title: "Select Accelerators for an InferenceService"
linkTitle: "Select Accelerators"
weight: 20
date: 2026-09-26
description: >
  Let OME pick the GPU class for each component declaratively — by naming an AcceleratorClass, or by a policy that filters and scores the runtime's accelerator classes.
---

This page shows you how to let OME choose an `AcceleratorClass` for an InferenceService instead of hard-coding a `nodeSelector` and GPU resource requests. You'll learn the precedence between `spec.acceleratorSelector` and the per-component `acceleratorOverride`, how the `BestFit`, `Cheapest`, `MostCapable`, and `FirstAvailable` policies choose among the runtime's accelerator classes, and how the selected class shapes the generated pods.

## Before you begin

You need to have the following:

- A Kubernetes cluster with OME installed
- `kubectl` configured to communicate with your cluster
- `AcceleratorClass` resources describing the GPU types in your cluster (cluster-scoped, usually installed by your platform team)
- A ServingRuntime or ClusterServingRuntime whose `acceleratorRequirements.acceleratorClasses` lists the classes it supports

That last point is the key precondition: **accelerator selection only runs when the resolved runtime lists at least one class in `acceleratorRequirements.acceleratorClasses`**. If the runtime has no `acceleratorRequirements`, everything you put in `spec.acceleratorSelector` — including an explicit class name — is ignored.

## How selection works

For each component that serves the model (**engine** and **decoder**; the router never gets an accelerator class), OME resolves the class in this order:

1. The component's `acceleratorOverride.acceleratorClass` (explicit name)
2. The top-level `spec.acceleratorSelector.acceleratorClass` (explicit name)
3. The component's `acceleratorOverride.policy`
4. The top-level `spec.acceleratorSelector.policy`

An explicit class name at either level always beats a policy at any level. So if the top level names a class and a component override only sets a policy, the named class still wins for that component.

When a policy applies, the candidate pool is **only** the classes named in the runtime's `acceleratorRequirements.acceleratorClasses`, in that list order (duplicates removed, names that don't exist in the cluster skipped). The candidates are then filtered by `spec.acceleratorSelector.constraints` and the policy picks one of the survivors.

If neither a class name nor a policy is set anywhere, no class is selected — constraints on their own do nothing, and the pods are generated from the runtime's own `nodeSelector` and container resources.

## Step 1: Check the available AcceleratorClasses

`AcceleratorClass` is a cluster-scoped resource. The AcceleratorClass controller continuously matches cluster nodes against each class's discovery rules, so the `NODES` column tells you whether a class currently maps to any nodes:

```bash
kubectl get acceleratorclasses
```

```
NAME              VENDOR   FAMILY   MEMORY   NODES
nvidia-a100-80g   nvidia   ampere   80Gi     4
nvidia-h100       nvidia   hopper   80Gi     2
nvidia-h200       nvidia   hopper   141Gi    0
```

The fields the selection policies actually read look like this:

```yaml
apiVersion: ome.io/v1beta1
kind: AcceleratorClass
metadata:
  name: nvidia-h100
spec:
  vendor: nvidia            # with family, forms "nvidia-hopper" for architectureFamilies matching
  family: hopper
  model: h100
  discovery:
    nodeSelector:            # merged into pods that select this class
      nvidia.com/gpu.product: NVIDIA-H100-80GB-HBM3
  capabilities:
    memoryGB: 80Gi           # byte quantity; compared against minMemory/maxMemory constraints
    computeCapability: "9.0" # compared against minArchitectureVersion
    memoryBandwidthGBps: "3350"
    features: ["tensor-cores", "fp8", "nvlink"]
    performance:
      fp16Tflops: 989
      int8Tops: 1979         # also used when a preferred precision is "fp8"
  resources:                 # applied to the runner container as requests/limits
    - name: nvidia.com/gpu
      quantity: "1"
  cost:
    perHour: "4.5"
    spotPerHour: "2.1"       # preferred over perHour by the Cheapest policy
    tier: high
```

> **Note:** the `NODES` column is informational. The selection policies score classes purely on their spec — they do **not** check whether a class currently has available nodes, and `FirstAvailable` in particular does not mean "first class with free capacity".

## Step 2: Confirm the runtime's candidate list

Policy-based selection can only choose among the classes the runtime declares:

```bash
kubectl get clusterservingruntime srt-llama-fp8 \
  -o jsonpath='{.spec.acceleratorRequirements.acceleratorClasses}'
```

```
["nvidia-h100","nvidia-h200"]
```

If the class you expect is missing here, add it to the runtime — constraints and policies cannot reach outside this list. See [Accelerator Requirements](/ome/docs/concepts/serving_runtime/#accelerator-requirements) for the runtime-side fields.

## Option A: Pin a class by name

Set `spec.acceleratorSelector.acceleratorClass` (or the per-component equivalent) to use one specific class:

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-70b
spec:
  model:
    name: llama-3-3-70b-instruct
  acceleratorSelector:
    acceleratorClass: nvidia-h100
  engine:
    minReplicas: 1
    maxReplicas: 3
```

An explicit name skips constraint filtering and policy scoring entirely. Two things to know:

- The named class is **not** validated against the runtime's `acceleratorRequirements.acceleratorClasses` list — any existing AcceleratorClass works, as long as the runtime lists at least one class (otherwise selection never runs, see above).
- If the named class does not exist in the cluster, reconciliation fails and OME emits a Warning event with reason `AcceleratorClassError` on the InferenceService. Check with `kubectl describe inferenceservice llama-70b`.

## Option B: Select by policy

Set a `policy` and, optionally, `constraints` to filter the runtime's candidates first:

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-70b
spec:
  model:
    name: llama-3-3-70b-instruct
  acceleratorSelector:
    policy: Cheapest
    constraints:
      minMemory: 80                # at least 80 GB of accelerator memory
      architectureFamilies:
        - nvidia-hopper
        - nvidia-ampere
      preferredPrecisions:
        - fp8
        - fp16
  engine:
    minReplicas: 1
    maxReplicas: 3
```

### How constraints filter candidates

Constraints are hard filters applied before the policy scores anything. A candidate is dropped when:

| Constraint               | Drops a candidate when                                                                                                   |
|--------------------------|---------------------------------------------------------------------------------------------------------------------------|
| `excludedClasses`        | The class name is in the list.                                                                                             |
| `architectureFamilies`   | The class matches none of the entries. An entry with a dash matches `<vendor>-<family>` (e.g. `nvidia-hopper`); an entry without a dash matches `family` alone (e.g. `hopper`). Case-insensitive. |
| `minMemory` / `maxMemory` | The class's `capabilities.memoryGB` (converted from bytes to GB) falls outside the range — or the class doesn't set `memoryGB` at all. |
| `requiredFeatures`       | Any listed feature is missing from `capabilities.features` (case-insensitive; all must be present).                         |
| `minArchitectureVersion` | The class's `capabilities.computeCapability` compares lower as a string (e.g. `"8.0"` < `"9.0"`) — or is unset.            |

Two fields behave differently than you might expect:

- `minComputePerformanceTFLOPS` is **not** a filter. It only influences the `BestFit` score: a class below the requirement gets a proportionally lower compute score but is not disqualified.
- `preferredPrecisions` is **not** a filter either. It steers the `BestFit` and `MostCapable` scoring (see below).

If every candidate is filtered out, no class is selected and the pods are generated without accelerator settings — this is silent (logged by the controller, but no error and no event), so double-check your constraints if the pods come out without the expected `nodeSelector`.

### What each policy picks

| Policy           | Chooses                                                                                             |
|------------------|-----------------------------------------------------------------------------------------------------|
| `BestFit`        | Highest score of `0.7 × memory-fit + 0.3 × compute`. Right-sizes: penalizes classes much larger than `minMemory`. |
| `Cheapest`       | Lowest cost, using the best cost data available: spot/hourly price first, then per-million-tokens, then tier. |
| `MostCapable`    | Highest score of `0.5 × memory + 0.3 × bandwidth + 0.2 × TFLOPS`, each normalized to the best candidate. |
| `FirstAvailable` | The first candidate in the runtime's `acceleratorClasses` list order that passed the constraints.     |

**BestFit** targets the smallest class that satisfies your requirements. The memory-fit score is `minMemory / classMemory` — an exact match scores 1.0, a class twice as large scores 0.5. Without a `minMemory` constraint every class gets a perfect memory score and only compute decides. The compute score walks your `preferredPrecisions` in order: the first precision the class has TFLOPS data for is scored against `minComputePerformanceTFLOPS` (capped at 1.0; a perfect 1.0 when no minimum is set), and each precision skipped for lack of data halves the result. If none of the preferred precisions has data, fp16 is tried as a heavily-penalized fallback; without any `preferredPrecisions`, the class's best TFLOPS value across all precisions is used. Classes without any `capabilities.performance` data score 0 on compute.

**Cheapest** groups candidates by the kind of cost data they carry and only compares within the highest-priority non-empty group: hourly prices first (`spotPerHour` when set, otherwise `perHour`), then `perMillionTokens`, then `tier` (`low` < `medium` < `high`; an unrecognized tier counts as `medium`). Classes with no `cost` block at all are skipped, and if no candidate has cost data, nothing is selected.

**MostCapable** normalizes each candidate's memory, memory bandwidth, and TFLOPS against the best value in the pool and picks the highest composite score. TFLOPS is read at your first preferred precision (default fp16), falling through the rest of the list when the first has no data. Note that a preferred precision of `fp8` reads the class's `int8Tops` value.

**FirstAvailable** simply keeps the runtime's list order, so it doubles as "let the runtime author rank the classes". As noted above, it does not check node availability.

## Per-component override

Both `spec.engine` and `spec.decoder` accept an `acceleratorOverride` of the same shape as `spec.acceleratorSelector`. This is mainly useful for disaggregated prefill/decode serving where the two phases want different hardware:

```yaml
spec:
  model:
    name: deepseek-r1
  acceleratorSelector:
    policy: MostCapable          # default for all components
    constraints:
      architectureFamilies:
        - nvidia-hopper
  engine:
    acceleratorOverride:
      acceleratorClass: nvidia-h200   # engine pinned; decoder still uses MostCapable
    minReplicas: 1
    maxReplicas: 3
  decoder:
    minReplicas: 1
    maxReplicas: 3
```

Two precedence details to keep in mind:

- Only the override's `acceleratorClass` and `policy` are consulted. **Constraints are always read from the top-level `spec.acceleratorSelector.constraints`** — a `constraints` block inside `acceleratorOverride` has no effect on filtering today.
- Because an explicit name anywhere beats a policy anywhere, an override that only sets `policy` cannot displace a top-level `acceleratorClass` name. To give one component a different class by policy, remove the top-level name and pin the *other* component instead.

## How the selected class shapes the pod

Once a class is selected for a component, OME applies it when generating that component's workload:

- **Node selector.** The class's `discovery.nodeSelector` is merged into the pod's `nodeSelector` on top of the runtime's, and your component-level `nodeSelector` overrides both on conflicting keys (runtime < AcceleratorClass < InferenceService).
- **Node affinity.** If the component spec sets no `affinity` of its own, the node-affinity part of the class's `discovery.affinity` is copied to the pod. Pod (anti-)affinity terms from the class are never copied.
- **Resources.** If you didn't set resources on the component's `runner`, each entry in the class's `resources` (for example `nvidia.com/gpu: "1"`) is set as both request and limit on the runner container, overriding the runtime container's values for those resource names. Resources you set explicitly always win.
- **Per-class model configuration.** If the matched entry in the runtime's `supportedModelFormats` has an `acceleratorConfig` keyed by the selected class name, its `environmentOverride` is merged into the runner's environment, its `runtimeArgsOverride` into the runner's args, and its `tensorParallelismOverride` rewrites the tensor/pipeline-parallel flags (and takes over from OME's automatic parallelism computation). See [Per-Accelerator Model Configuration](/ome/docs/concepts/serving_runtime/#per-accelerator-model-configuration).

Verify the result on the generated pod:

```bash
kubectl get pod -l ome.io/inferenceservice=llama-70b \
  -o jsonpath='{.items[0].spec.nodeSelector}' | jq
kubectl get pod -l ome.io/inferenceservice=llama-70b \
  -o jsonpath='{.items[0].spec.containers[0].resources}' | jq
```

## Troubleshooting

**Pods come out with no accelerator `nodeSelector` or GPU resources.** In order of likelihood:

1. The runtime doesn't declare `acceleratorRequirements.acceleratorClasses` — selection never ran (Step 2).
2. You set `constraints` but no `policy` and no `acceleratorClass` — constraints alone select nothing.
3. All candidates were filtered out by the constraints, or (with `Cheapest`) none of the surviving candidates carries cost data. Both cases are silent; loosen the constraints or fill in the class specs.

**Reconciliation fails with an `AcceleratorClassError` event.** The explicitly named class doesn't exist in the cluster, or an API read of the candidate classes failed. `kubectl describe inferenceservice <name>` shows the event; `kubectl get acceleratorclasses` shows what exists.

**The pods select the class but stay `Pending`.** The class's `discovery.nodeSelector` matches no schedulable node. Compare it against your node labels, and check the class's `NODES` column — selection does not verify availability, so a class with 0 nodes can still be chosen.

## Next steps

- [Inference Service concepts](/ome/docs/concepts/inference_service) — the `acceleratorSelector` and `acceleratorOverride` field reference
- [Serving Runtime concepts](/ome/docs/concepts/serving_runtime/#accelerator-requirements) — declaring `acceleratorRequirements` and per-class `acceleratorConfig` on a runtime
- [OME v1beta1 API reference](/ome/docs/reference/ome.v1beta1) — the full `AcceleratorClass`, `AcceleratorSelector`, and `AcceleratorConstraints` field reference
