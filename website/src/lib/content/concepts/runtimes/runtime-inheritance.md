---
title: Runtime inheritance
description: "Write one runtime per engine, and keep what differs for each model or GPU in small child runtimes that inherit from it."
since: v1.3
---

Runtime inheritance lets you write one [serving runtime](serving-runtimes.md) per engine and reuse it for many models. A child runtime names its parent in the `ome.io/inherit-from` annotation. It gets the parent's spec, and sets only what differs for a model or a GPU type, such as its flags and resources. Settings for a single [InferenceService](../serving/inference-services.md) can go in the InferenceService instead. OME applies inheritance when an InferenceService names the child runtime, not when OME chooses a runtime itself; see [When the merged spec applies](#when-the-merged-spec-applies).

## Define a profile and inherit from it

Any runtime can be a parent. One that exists only to be inherited from is a profile: set `disabled: true` on it so that no InferenceService uses it directly. You can also mark it with the `ome.io/runtime-profile: "true"` annotation, and the admission webhook then requires `disabled: true`.

The example uses the `llama-3-2-3b-instruct` ClusterBaseModel from [`config/models/meta/Llama-3.2-3B-Instruct.yaml`](https://github.com/ome-projects/ome/blob/main/config/models/meta/Llama-3.2-3B-Instruct.yaml). Apply it with the [model agent](../../guides/operate-ome/model-agent.md) turned on and a Hugging Face token for the gated model, as [What's in a runtime](serving-runtimes.md#anatomy-of-a-runtime) describes, and wait until it's `Ready`. If you ran the examples there, delete the `llama-demo` namespace first, because this example reuses their names. Then create a namespace for the example:

```bash
kubectl create namespace llama-demo
```

```output
namespace/llama-demo created
```

This ClusterServingRuntime is a profile for SGLang. It holds what all SGLang runtimes share, including the model-independent server flags in `command`:

```yaml title="profile.yaml"
apiVersion: ome.io/v1beta1
kind: ClusterServingRuntime
metadata:
  name: sglang-profile
  annotations:
    ome.io/runtime-profile: "true"
spec:
  disabled: true
  engineConfig:
    tolerations:
      - key: nvidia.com/gpu
        operator: Exists
        effect: NoSchedule
    volumes:
      - name: dshm
        emptyDir:
          medium: Memory
    runner:
      name: ome-container
      image: docker.io/lmsysorg/sglang:v0.5.5.post3-cu129-amd64
      command:
        - python3
        - -m
        - sglang.launch_server
        - --host=0.0.0.0
        - --port=8080
        - --model-path=$(MODEL_PATH)
        - "--served-model-name={{.Name}}"
      ports:
        - containerPort: 8080
          name: http1
          protocol: TCP
      volumeMounts:
        - name: dshm
          mountPath: /dev/shm
      readinessProbe:
        httpGet:
          path: /health_generate
          port: 8080
        periodSeconds: 60
        timeoutSeconds: 200
      startupProbe:
        httpGet:
          path: /health_generate
          port: 8080
        initialDelaySeconds: 60
        periodSeconds: 6
        timeoutSeconds: 30
        failureThreshold: 150
```

Apply the file:

```bash
kubectl apply -f profile.yaml
```

```output
clusterservingruntime.ome.io/sglang-profile created
```

This ServingRuntime is the one from [What's in a runtime](serving-runtimes.md#anatomy-of-a-runtime), rebuilt on the profile. It names the profile in `ome.io/inherit-from`, and adds only what's specific to Llama 3.2 3B:

```yaml title="runtime.yaml"
apiVersion: ome.io/v1beta1
kind: ServingRuntime
metadata:
  name: sglang-llama-3-2-3b
  namespace: llama-demo
  annotations:
    ome.io/inherit-from: sglang-profile
spec:
  disabled: false
  supportedModelFormats:
    - modelFormat:
        name: safetensors
        version: "1.0.0"
      modelFramework:
        name: transformers
        version: "4.45.0.dev0"
      modelArchitecture: LlamaForCausalLM
      autoSelect: false
  modelSizeRange:
    min: 2B
    max: 4B
  engineConfig:
    runner:
      name: ome-container
      args:
        - --tp-size=1
        - --mem-frac=0.9
      resources:
        requests:
          cpu: "10"
          memory: 30Gi
          nvidia.com/gpu: "1"
        limits:
          cpu: "10"
          memory: 30Gi
          nvidia.com/gpu: "1"
```

- `disabled: false` overrides the profile's `disabled: true`, which the child would otherwise inherit.
- Every runner needs a `name`, so the runtime repeats `ome-container`.
- A child's `command` or `args` replaces the parent's whole list. So the profile holds the shared flags in `command`, and the child adds its own in `args`. An InferenceService's `args` replaces the child's in turn; see [The engine, decoder and router](serving-runtimes.md#the-engine-decoder-and-router).
- `autoSelect: false`, the same as leaving it out, keeps OME from choosing the runtime on its own; see [When the merged spec applies](#when-the-merged-spec-applies).

Apply the file:

```bash
kubectl apply -f runtime.yaml
```

```output
servingruntime.ome.io/sglang-llama-3-2-3b created
```

This InferenceService names the runtime:

```yaml title="isvc.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-3-2-3b-instruct
  namespace: llama-demo
spec:
  model:
    name: llama-3-2-3b-instruct
  runtime:
    name: sglang-llama-3-2-3b
  engine:
    minReplicas: 1
    maxReplicas: 1
```

Apply the file:

```bash
kubectl apply -f isvc.yaml
```

```output
inferenceservice.ome.io/llama-3-2-3b-instruct created
```

Print the image, command and flags that the engine's Deployment runs:

```bash
kubectl get deployment llama-3-2-3b-instruct-engine -n llama-demo \
  -o jsonpath='{range .spec.template.spec.containers[?(@.name=="ome-container")]}{.image}{"\n"}{range .command[*]}{@}{"\n"}{end}{range .args[*]}{@}{"\n"}{end}{end}'
```

```output
docker.io/lmsysorg/sglang:v0.5.5.post3-cu129-amd64
python3
-m
sglang.launch_server
--host=0.0.0.0
--port=8080
--model-path=$(MODEL_PATH)
--served-model-name=llama-3-2-3b-instruct
--tp-size=1
--mem-frac=0.9
```

The image and the command come from the profile, and the last two flags from the child. OME filled in the `{{.Name}}` [placeholder](serving-runtimes.md#placeholders) with the InferenceService's name. To serve another model or GPU type on the same engine, add another child that sets its own formats, `args` and resources.

## What merges

OME merges a chain from the root down, laying each runtime's spec over the result for its parent:

| Field | How the child's value combines with the parent's |
| --- | --- |
| Single values, such as `image` and `disabled` | The child's value replaces the parent's. |
| Objects, such as `runner` and `modelSizeRange` | Merge field by field, so a child can set `modelSizeRange.max` and keep its parent's `min`. |
| `nodeSelector`, resource `requests` and `limits`, and the spec's `labels` and `annotations` | Merge by key. |
| `env`, `volumes`, `imagePullSecrets`, `containers` and `initContainers` | Merge item by item, by `name`. |
| `ports` | Merge item by item, by `containerPort`. |
| `volumeMounts` | Merge item by item, by `mountPath`. |
| `topologySpreadConstraints` | Merge item by item, by `topologyKey`. |
| Most other lists, such as `command`, `args` and `tolerations` | The child's list replaces the parent's. |

A child inherits only the spec, so the parent's `ome.io/runtime-profile` annotation and other metadata stay on the parent. Watch for three things:

- A child can't remove what its parent sets. Setting a field to `[]`, `{}` or `""` counts as leaving it unset.
- `requests` and `limits` merge by key, so set both. If a child raises a request above the limit it inherits, Kubernetes rejects the pod spec.
- Objects, and list items with the same key, merge field by field. A child that switches a volume, an environment variable or a probe to another kind of source or check ends up with both, which Kubernetes rejects. Change it in the parent, or give the child's volume a new name and mount that.

## When the merged spec applies

When an InferenceService names a runtime, OME uses the merged spec to render the pods, and to check that the runtime is enabled and [declares support for the model](serving-runtimes.md#when-you-name-a-runtime). When OME chooses a runtime itself, it matches and renders each runtime's own spec, without inheritance. So name a child runtime instead of setting `autoSelect: true` on it.

`kubectl ome runtime effective llama-3-2-3b-instruct -n llama-demo` shows the runtime that an InferenceService uses, and whether the InferenceService is pinned to a snapshot or has drifted from it. With `-o json` or `-o yaml`, it also lists the chain, root first.

### When a parent changes

!!! warning "A profile edit reaches every InferenceService built on it"
    Before you edit a profile, see what the edit reaches with `kubectl ome runtime tree sglang-profile`. It shows the runtimes that inherit from the profile, and the InferenceServices that name the profile or one of those runtimes; see [kubectl ome runtime](../../reference/kubectl-ome/runtime.md#tree).

What happens to an InferenceService built on the changed runtime depends on its `spec.runtime.autoSync`:

- With `autoSync: true`, the default, OME renders pods from the new merged spec, and rolls them out as the component's [deployment mode](../architecture/deployment-modes.md) does.
- With `autoSync: false`, the InferenceService keeps its snapshot of the merged spec. OME sets its `RuntimeDrifted` condition to `True`, with the reason `RevisionMismatch`, and leaves its serving workload as it is until you roll forward or undo the change. One pinned with `spec.runtime.revision` stays on that revision. See [Runtime revisions and pinning](runtime-revisions.md).

## Chain limits

A parent can inherit from another runtime in turn. A chain holds at most five runtimes, the runtime and four ancestors, and a name can appear in it only once. Names alone count, so a ServingRuntime can't inherit from a ClusterServingRuntime with its own name.

`ome.io/inherit-from` holds only the parent's name. OME looks up the parents in a runtime's chain like this:

| Runtime | Where OME looks for the parents in its chain |
| --- | --- |
| ClusterServingRuntime | ClusterServingRuntimes |
| ServingRuntime | ServingRuntimes in its namespace, then ClusterServingRuntimes |

## What admission checks

When you create or update a runtime, the admission webhook checks that its chain resolves, and that a profile sets `disabled: true`. When it rejects the runtime, kubectl prints `admission webhook "servingruntime.ome-webhook-server.validator" denied the request:`, or `clusterservingruntime.ome-webhook-server.validator` for a ClusterServingRuntime, and then the reason. Deleting or changing a parent later can still break the chains below it. OME then reports the problem in the `InheritanceReady` condition of each runtime affected; see [When a chain breaks](#chain-health-in-status).

| Problem | Reason in the webhook's error | `InheritanceReady` reason |
| --- | --- | --- |
| A parent doesn't exist, as when you apply `runtime.yaml` before `profile.yaml` | `inherit-from parent "sglang-profile" not found (chain so far: [sglang-llama-3-2-3b])` | `ParentNotFound` |
| The chain loops, as when a runtime names itself, or a ServingRuntime names a ClusterServingRuntime with its own name | `inheritance cycle detected: [sglang-llama-3-2-3b sglang-llama-3-2-3b]` | `InheritanceCycle` |
| The chain holds more than five runtimes | Begins `inheritance chain exceeds max depth 5` | `MaxDepthExceeded` |
| A profile doesn't set `disabled: true` | `runtimes carrying ome.io/runtime-profile="true" must also set spec.disabled: true` | None |

Chains in these messages start at the runtime you wrote.

## When a chain breaks {#chain-health-in-status}

A runtime's status lists its chain, root first, and whether the chain resolves. For the example's runtime:

```bash
kubectl get servingruntime sglang-llama-3-2-3b -n llama-demo \
  -o 'custom-columns=NAME:.metadata.name,CHAIN:.status.inheritanceChain[*],READY:.status.conditions[?(@.type=="InheritanceReady")].status,REASON:.status.conditions[?(@.type=="InheritanceReady")].reason'
```

```output
NAME                  CHAIN                                READY   REASON
sglang-llama-3-2-3b   sglang-profile,sglang-llama-3-2-3b   True    Resolved
```

If you delete `sglang-profile`, for example, the chain breaks and `InheritanceReady` turns `False`, with a reason from [What admission checks](#what-admission-checks), or `ResolverError` for any other failure. OME also records a warning event on the runtime, and `inheritanceChain` keeps the last chain that resolved. When you create the missing parent, the runtimes below it return to `Resolved`. The [ServingRuntimeStatus](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-ServingRuntimeStatus) reference lists the fields.

While the chain of an InferenceService's runtime is broken, OME leaves the serving workload as it is and retries. The running pods keep serving. OME records a `RuntimeValidationError` warning event on the InferenceService, whose message begins `Runtime sglang-llama-3-2-3b does not support model llama-3-2-3b-instruct:` and ends with the chain's error. The event is `RuntimePinError` for an InferenceService with `autoSync: false`, and `RuntimeFetchError` for one that follows its live runtime without `spec.model`. The admission webhook also rejects creating or updating an InferenceService that names the runtime, with the chain's error.

## Next steps

- [Serving runtimes](serving-runtimes.md): the fields of a runtime, and how OME selects one.
- [Runtime revisions and pinning](runtime-revisions.md): pin an InferenceService to an immutable snapshot of its merged runtime.
- [Reference a runtime explicitly](../../guides/deploy-models/reference-a-runtime-explicitly.md): name a runtime in the InferenceService, so that OME uses what it inherits.
- [kubectl ome runtime](../../reference/kubectl-ome/runtime.md): see a runtime's inheritance tree, and the runtime behind an InferenceService.
