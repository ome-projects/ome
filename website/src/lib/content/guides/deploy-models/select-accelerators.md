---
title: Select accelerators
description: Pick the AcceleratorClass for the engine and decoder by naming a class, or let OME choose one with the BestFit, Cheapest, MostCapable or FirstAvailable policy.
---

Run an InferenceService's engine on the GPU type you want. Pin an [AcceleratorClass](../../concepts/runtimes/accelerator-classes.md) by name, or set a policy that picks the best-fitting, cheapest or most capable class that the serving runtime lists. The engine's pods then run on that class's nodes, and the runtime can tune the model server for the class. In a prefill-decode deployment, the decoder can use a class of its own.

<div class="prerequisites" markdown>

- OME installed, and `kubectl` access to the cluster. See [Install OME](../../getting-started/install.md).
- Permission to create AcceleratorClasses and to change ClusterServingRuntimes. Both are cluster-scoped.
- The ClusterBaseModel `llama-3-3-70b-instruct` in the `Ready` state. If it's missing, apply [`config/models/meta/Llama-3.3-70B-instruct.yaml`](https://github.com/ome-projects/ome/blob/main/config/models/meta/Llama-3.3-70B-instruct.yaml) from a clone of the OME repository. The model agent downloads it under `/raid/models`, which needs the chart values `modelAgent.enabled=true` and `modelAgent.hostPath=/raid/models`; see [Install OME](../../getting-started/install.md#step-3-install-ome). The model is gated, so the download needs a Hugging Face token in the Secret `hf-token` in the `ome` namespace; see [Credentials](../../concepts/models/base-models.md#credentials).
- The [ClusterServingRuntime](../../concepts/runtimes/serving-runtimes.md) `srt-llama-3-3-70b-instruct`. If it's missing, apply [`config/runtimes/srt/meta/llama-3-3-70b-instruct-rt.yaml`](https://github.com/ome-projects/ome/blob/main/config/runtimes/srt/meta/llama-3-3-70b-instruct-rt.yaml) from the same clone.
- The namespace `llama-demo`.
- Nodes with NVIDIA H100 GPUs, and H200 GPUs to try `MostCapable` or the prefill-decode example. The runtime's SGLang image is built for amd64, and each engine pod requests 4 GPUs, 10 CPUs and 160 GiB of memory.

</div>

## How OME picks a class

A serving runtime lists the classes it runs on in `spec.acceleratorRequirements.acceleratorClasses`. If the list is empty, no component gets a class, even one that the InferenceService names. Otherwise, OME picks a class for the engine and for the decoder from what the InferenceService sets in `spec.acceleratorSelector`, or in the component's [`acceleratorOverride`](#override-the-class-for-one-component):

| The InferenceService sets | The component gets |
| --- | --- |
| `acceleratorClass` | That class, even when a policy is also set. |
| `policy` | The class that the [policy](#policies) picks from the runtime's list. |
| Neither | No class. Its pods keep the runtime's settings. |

The router never gets a class.

### Policies

A policy picks one of the runtime's classes that pass the [constraints](#constraints). Policies ignore free capacity, so a policy can pick a class whose nodes are full or that has no nodes.

| Policy | Picks |
| --- | --- |
| `BestFit` | The class whose memory comes closest to `minMemory`, with compute performance counting for less. |
| `Cheapest` | The class with the lowest `cost`: `spotPerHour`, else `perHour`, with `perMillionTokens` and then `tier` as fallbacks. Classes without `cost` are left out. |
| `MostCapable` | The class with the most memory, memory bandwidth and TFLOPS, with memory counting most. |
| `FirstAvailable` | The first class in the runtime's list that passes the constraints. |

## Step 1: Create the AcceleratorClasses

OME doesn't install any AcceleratorClasses, so create one for each GPU type. If your cluster already has classes for your GPUs, skip this step, and use their names in place of `nvidia-h100` and `nvidia-h200`.

Save these two classes. Replace the `nvidia.com/gpu.product` values with the ones on your nodes, from `kubectl get nodes -L nvidia.com/gpu.product`. The costs are examples, for `Cheapest` to compare:

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
      - fp8
      - nvlink
    performance:
      fp16Tflops: 989
  cost:
    perHour: "10"
---
apiVersion: ome.io/v1beta1
kind: AcceleratorClass
metadata:
  name: nvidia-h200
spec:
  vendor: nvidia
  family: hopper
  model: h200
  discovery:
    nodeSelector:
      nvidia.com/gpu.product: NVIDIA-H200
  capabilities:
    memoryGB: 141Gi
    computeCapability: "9.0"
    memoryBandwidthGBps: "4800"
    features:
      - fp8
      - nvlink
    performance:
      fp16Tflops: 989
  cost:
    perHour: "12"
```

Apply the file:

```bash
kubectl apply -f accelerator-classes.yaml
```

```output
acceleratorclass.ome.io/nvidia-h100 created
acceleratorclass.ome.io/nvidia-h200 created
```

List the classes:

```bash
kubectl get acceleratorclasses
```

```output
NAME          VENDOR   FAMILY   MEMORY   NODES
nvidia-h100   nvidia   hopper   80Gi     2
nvidia-h200   nvidia   hopper   141Gi    1
```

`NODES` is the number of nodes that match the class. If it's blank, see [Why a class reports zero nodes](../../concepts/runtimes/accelerator-classes.md#why-a-class-reports-zero-nodes).

## Step 2: Add the classes to the runtime {#step-2-check-the-runtimes-candidates}

Check the runtime's list of classes:

```bash
kubectl get clusterservingruntime srt-llama-3-3-70b-instruct \
  -o jsonpath='{.spec.acceleratorRequirements.acceleratorClasses}'
```

The output is empty, because the catalog runtime lists no classes. Add the two classes:

```bash
kubectl patch clusterservingruntime srt-llama-3-3-70b-instruct --type merge \
  -p '{"spec":{"acceleratorRequirements":{"acceleratorClasses":["nvidia-h100","nvidia-h200"]}}}'
```

```output
clusterservingruntime.ome.io/srt-llama-3-3-70b-instruct patched
```

Check the list again:

```bash
kubectl get clusterservingruntime srt-llama-3-3-70b-instruct \
  -o jsonpath='{.spec.acceleratorRequirements.acceleratorClasses}'
```

```output
["nvidia-h100","nvidia-h200"]
```

The order sets what `FirstAvailable` picks. Other InferenceServices that use this runtime get a class only if they name one or set a policy. Those pinned with `spec.runtime.autoSync: false` report `RuntimeDrifted`, and OME stops updating them until you roll them forward or [clean up](#clean-up); see [When the runtime changes](../../concepts/runtimes/runtime-revisions.md#when-the-runtime-changes).

## Step 3: Pin a class or choose a policy

Pin a class when the model should run on one GPU type. Set a policy when the runtime runs well on several, and you'd rather OME choose by fit, cost or capability. Both go in `spec.acceleratorSelector`:

=== "Pin a class"

    ```yaml title="isvc.yaml"
    apiVersion: ome.io/v1beta1
    kind: InferenceService
    metadata:
      name: llama-3-3-70b-instruct
      namespace: llama-demo
    spec:
      model:
        name: llama-3-3-70b-instruct
      runtime:
        name: srt-llama-3-3-70b-instruct
      engine:
        minReplicas: 1
        maxReplicas: 1
      acceleratorSelector:
        acceleratorClass: nvidia-h100
    ```

=== "Choose a policy"

    ```yaml title="isvc.yaml"
    apiVersion: ome.io/v1beta1
    kind: InferenceService
    metadata:
      name: llama-3-3-70b-instruct
      namespace: llama-demo
    spec:
      model:
        name: llama-3-3-70b-instruct
      runtime:
        name: srt-llama-3-3-70b-instruct
      engine:
        minReplicas: 1
        maxReplicas: 1
      acceleratorSelector:
        policy: BestFit
        constraints:
          minMemory: 80
    ```

`spec.model` refers to a ClusterBaseModel unless you add `kind: BaseModel`. The example names the runtime, because the catalog runtime turns off auto-selection for the model.

With the classes from Step 1, the policies pick:

| Policy | Picks |
| --- | --- |
| `BestFit`, with `minMemory: 80` | `nvidia-h100`, whose 80 GiB fits exactly. |
| `Cheapest` | `nvidia-h100`, which has the lower `perHour`. |
| `MostCapable` | `nvidia-h200`, which has more memory and bandwidth. |
| `FirstAvailable` | `nvidia-h100`, which is first in the runtime's list. |

!!! warning "Set constraints with BestFit"
    With `BestFit`, always set `spec.acceleratorSelector.constraints`, also when `BestFit` is in an `acceleratorOverride`. If you leave them out and two or more classes qualify, [OME doesn't create or update the serving workload](#bestfit-doesnt-create-or-update-the-workload).

Apply the file:

```bash
kubectl apply -f isvc.yaml
```

```output
inferenceservice.ome.io/llama-3-3-70b-instruct created
```

## Step 4: Check the selected class

The engine's pods show the selected class, through its node selector. Print the node selector of an engine pod:

```bash
kubectl get pods -n llama-demo \
  -l ome.io/inferenceservice=llama-3-3-70b-instruct,component=engine \
  -o jsonpath='{.items[0].spec.nodeSelector}'
```

```output
{"models.ome.io/clusterbasemodel.llama-3-3-70b-instruct":"Ready","nvidia.com/gpu.product":"NVIDIA-H100-80GB-HBM3"}
```

The `nvidia.com/gpu.product` entry is `nvidia-h100`'s node selector from Step 1, so the engine got that class. The other entry keeps the pod on nodes where the model is ready. A `Pending` pod already has its node selector, so you can check the class before a node is free.

The InferenceService's status leaves the class out, so [`kubectl ome accelerator explain`](../../reference/kubectl-ome/accelerator.md) can't show it.

A class can also set the pods' requests and limits: its `resources` replace the runtime's values for those resources, unless the InferenceService's runner sets its own. The classes from Step 1 set none, so the pods keep the runtime's 4 GPUs. [How InferenceServices use a class](../../concepts/runtimes/accelerator-classes.md#how-services-choose-a-class) lists everything a class changes on the pods.

## Narrow the choice with constraints {#constraints}

Constraints limit the classes that a policy picks from. Set them in `spec.acceleratorSelector.constraints`. They apply to every component's policy, and constraints in an `acceleratorOverride` have no effect. A named class skips them.

| Field | A class passes when |
| --- | --- |
| `excludedClasses` | Its name isn't in the list. |
| `architectureFamilies` | The list has its `family`, or its vendor and family joined by a hyphen: `hopper` or `nvidia-hopper`. |
| `minMemory`, `maxMemory` | Its `capabilities.memoryGB`, rounded down to whole GiB, is in the range. Write `memoryGB` in `Gi`: `80G` counts as 74. |
| `requiredFeatures` | Its `capabilities.features` has every entry. |
| `minArchitectureVersion` | Its `capabilities.computeCapability` is at least this version. The two compare as text, so `"10.0"` is lower than `"9.0"`. |
| `minComputePerformanceTFLOPS` | Always. `BestFit` scores the classes below it lower. |
| `preferredPrecisions` | Always. `BestFit` and `MostCapable` read the TFLOPS of the first listed precision that a class has. `fp8` and `int8` read `int8Tops`. |

[AcceleratorConstraints](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-AcceleratorConstraints) in the API reference lists the field types.

## Override the class for one component

Set `acceleratorOverride` on `spec.engine` or `spec.decoder` to give that component its own `acceleratorClass` or `policy`. The override wins over `spec.acceleratorSelector` for the same field.

This InferenceService serves the model in prefill-decode mode. The engine, which runs prefill, gets the class that `Cheapest` picks. The decoder is pinned to `nvidia-h200`, for its higher memory bandwidth:

```yaml title="isvc-pd.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-3-3-70b-instruct-pd
  namespace: llama-demo
spec:
  model:
    name: llama-3-3-70b-instruct
  runtime:
    name: srt-llama-3-3-70b-instruct-pd
  acceleratorSelector:
    policy: Cheapest
  engine:
    minReplicas: 1
    maxReplicas: 1
  decoder:
    minReplicas: 1
    maxReplicas: 1
    acceleratorOverride:
      acceleratorClass: nvidia-h200
  router:
    minReplicas: 1
    maxReplicas: 1
```

To try it, apply the catalog runtime [`config/runtimes/srt/meta/llama-3-3-70b-instruct-pd-rt.yaml`](https://github.com/ome-projects/ome/blob/main/config/runtimes/srt/meta/llama-3-3-70b-instruct-pd-rt.yaml), and add both classes to its list, as in [Step 2](#step-2-check-the-runtimes-candidates). Its engine and decoder pods run privileged on the host network, and need RDMA networking with the profile `oci-roce`; see [PD mode](../../reference/operate-ome/ome-serving-values.md#pd-mode).

A class name wins over any policy, so an override's `policy` has no effect while `spec.acceleratorSelector.acceleratorClass` is set. To give one component a policy, set the policy in `spec.acceleratorSelector`, and name classes in the other components' overrides, as this example does.

## Troubleshooting

### The webhook denies the runtime

The patch in Step 2 fails when a listed class is missing. The message ends with the missing classes, such as `unknown accelerator classes referenced in AcceleratorRequirements: [nvidia-h200]`. Create the classes first, or fix the names.

### kubectl apply warns that the runtime doesn't declare support for the model {since=v1.3}

`kubectl apply` warns when the InferenceService names a class that's missing from the runtime's list. The reason in parentheses ends with `runtime does not support the required accelerator class`:

```output
Warning: runtime "srt-llama-3-3-70b-instruct" does not declare support for model "llama-3-3-70b-instruct" (runtime srt-llama-3-3-70b-instruct does not support model : runtime does not support the required accelerator class); proceeding because the runtime was named explicitly
inferenceservice.ome.io/llama-3-3-70b-instruct created
```

OME still uses the runtime, and the pods get the named class if the runtime lists at least one class. Add the class to the runtime's list, as in [Step 2](#step-2-check-the-runtimes-candidates), to stop the warning. For any other reason in the warning, see [Reference a runtime explicitly](reference-a-runtime-explicitly.md).

### The InferenceService has an AcceleratorClassError event

List the events:

```bash
kubectl get events -n llama-demo --field-selector reason=AcceleratorClassError
```

```output
LAST SEEN   TYPE      REASON                  OBJECT                                    MESSAGE
12s         Warning   AcceleratorClassError   inferenceservice/llama-3-3-70b-instruct   Failed to get accelerator class for engine: accelerator class nvidia-h100 not found at cluster scope
```

The class that the InferenceService names is missing from the cluster. Create the class or fix the name. Until then, OME leaves the serving workload as it is, and keeps retrying.

### BestFit doesn't create or update the workload

This happens with `BestFit`, no `spec.acceleratorSelector.constraints`, and two or more classes to pick from. OME records no event, but the controller log shows `invalid memory address or nil pointer dereference`:

```bash
kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1 | grep "invalid memory address"
```

Add `constraints` to `spec.acceleratorSelector`, such as `minMemory: 80`. This is a known bug.

### The pods don't get the class's node selector

The component got no class. OME records no event for this, so check that:

- The runtime lists classes, as in [Step 2](#step-2-check-the-runtimes-candidates).
- The InferenceService names a class or sets a policy. Constraints and the `ome.io/accelerator-class` annotation don't pick a class on their own.
- At least one class passes the [constraints](#constraints). Compare each class's `memoryGB`, `computeCapability` and `features` with them.
- For `Cheapest`, at least one class sets `cost`.

### The pods stay Pending

An engine pod needs a free node of its class where the model is ready, as its node selector in [Step 4](#step-4-check-the-selected-class) shows. Check the class's `NODES` in [Step 1](#step-1-create-the-acceleratorclasses). If the class's nodes are full, pin a class that has free nodes, or exclude the full one with `excludedClasses`. For other causes, see [Pods stay Pending](../troubleshoot/troubleshoot-an-inferenceservice.md#pods-stay-pending).

## Clean up

Delete the InferenceService, and remove the class list from the runtime:

```bash
kubectl delete inferenceservice llama-3-3-70b-instruct -n llama-demo
kubectl patch clusterservingruntime srt-llama-3-3-70b-instruct --type json \
  -p '[{"op":"remove","path":"/spec/acceleratorRequirements"}]'
```

```output
inferenceservice.ome.io "llama-3-3-70b-instruct" deleted from llama-demo namespace
clusterservingruntime.ome.io/srt-llama-3-3-70b-instruct patched
```

If you tried the prefill-decode example, delete `llama-3-3-70b-instruct-pd` as well, and remove the class list from `srt-llama-3-3-70b-instruct-pd` the same way.

Removing the list clears `RuntimeDrifted` from the pinned InferenceServices that drifted in Step 2. Any that you rolled forward since then drift again, until you roll them forward once more.

If you created the two classes in Step 1, delete them after you remove them from every runtime's list:

```bash
kubectl delete acceleratorclass nvidia-h100 nvidia-h200
```

```output
acceleratorclass.ome.io "nvidia-h100" deleted
acceleratorclass.ome.io "nvidia-h200" deleted
```

## Next steps

- Learn what an AcceleratorClass describes, and how OME finds its nodes, in [Accelerator classes](../../concepts/runtimes/accelerator-classes.md).
- Tune the model server for each class in [Per-accelerator configuration](../../concepts/runtimes/serving-runtimes.md#per-accelerator-configuration).
- See how named classes narrow runtime auto-selection in [Runtime accelerator-class matching](../../reference/matching/runtime-accelerator-class-matching.md).
- Look up the `kubectl ome accelerator` commands in [kubectl ome accelerator](../../reference/kubectl-ome/accelerator.md).
