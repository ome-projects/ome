---
title: Model version matching
description: "How a runtime's supportedModelFormats entries accept a model's format and framework versions: the operators, the version syntax and the mismatch reasons."
---

Each entry in a [serving runtime's](../../concepts/runtimes/serving-runtimes.md) `supportedModelFormats` can require versions of the model's format and framework, such as `safetensors` `1.0.0` and `transformers` `4.45.0.dev0`. The entry matches a model only when the format and framework names are equal and the entry's operators accept the model's versions. Watch for three rules:

- A version that only one side sets never matches. When neither side sets one, the versions match.
- `GreaterThan` and `GreaterThanOrEqual` set an upper limit on the model's version, not a lower one.
- A version with a suffix, such as the `.dev0` in `4.45.0.dev0`, matches only an equal version, whatever the operator.

The entry must also match the model's architecture, quantization and diffusion pipeline, and the runtime must pass the other checks that [Troubleshoot runtime selection](../../guides/deploy-models/troubleshoot-runtime-selection.md#step-3-read-the-exclusion-reasons) lists.

## Where versions and operators are declared

A model declares its versions, and a runtime entry declares the versions it accepts, with an operator for each:

| Version | On a [BaseModel](../../concepts/models/base-models.md) or ClusterBaseModel | On a ServingRuntime or ClusterServingRuntime entry |
| --- | --- | --- |
| Format | `spec.modelFormat.version` | `spec.supportedModelFormats[].modelFormat.version` and `.operator` |
| Framework | `spec.modelFramework.version` | `spec.supportedModelFormats[].modelFramework.version` and `.operator` |

Runtime selection ignores the entry's top-level `name` and `version`, and the model's `operator` fields and `spec.version`. The API reference lists every field of an entry under [SupportedModelFormat](../api/ome.v1beta1.md#ome-io-v1beta1-SupportedModelFormat).

This BaseModel has the format `safetensors` `1.0.0` and the framework `transformers` `4.45.0.dev0`:

```yaml title="model.yaml"
apiVersion: ome.io/v1beta1
kind: BaseModel
metadata:
  name: llama-3-2-3b-instruct
  namespace: llama-demo
  annotations:
    ome.oracle.com/skip-config-parsing: "true"
spec:
  modelType: llama
  modelArchitecture: LlamaForCausalLM
  modelFormat:
    name: safetensors
    version: "1.0.0"
  modelFramework:
    name: transformers
    version: "4.45.0.dev0"
  modelParameterSize: "3.21B"
  modelCapabilities:
    - TEXT_TO_TEXT
  storage:
    storageUri: hf://meta-llama/Llama-3.2-3B-Instruct
    path: /mnt/data/models/meta/llama-3-2-3b-instruct
    key: hf-token
```

The catalog runtime [`srt-llama-3-2-3b-instruct`](https://github.com/ome-projects/ome/blob/main/config/runtimes/srt/meta/llama-3-2-3b-instruct-rt.yaml) accepts them with this entry:

```yaml
spec:
  supportedModelFormats:
    - modelFramework:
        name: transformers
        version: "4.45.0.dev0"
      modelFormat:
        name: safetensors
        version: "1.0.0"
      modelArchitecture: LlamaForCausalLM
      autoSelect: false
      priority: 1
```

The entry sets no operator, so both versions must equal the model's. It also sets `autoSelect: false`, so OME uses this runtime only for an [InferenceService](../../concepts/serving/inference-services.md) that names it.

When OME reads a model's files, it [fills in `modelFormat` and `modelFramework`](../../concepts/models/base-models.md#what-ome-learns-from-the-model), and the annotation on the BaseModel above turns that off. It keeps a `modelFramework` that you set, even one without a `version`, but adds the format version it read to a `modelFormat` that has none. A model whose `config.json` has no `transformers_version` gets no framework version.

Format and framework names are case-sensitive: `SafeTensors` doesn't match `safetensors`. When the names are equal, the versions decide:

| Model's version | Entry's version | Result |
| --- | --- | --- |
| Set | Set | OME compares them with the entry's `operator`. |
| Not set | Not set | They match. |
| Not set | Set | No match. |
| Set | Not set | No match. |

The API requires a `modelFramework` on every entry, so a model needs one to match any entry.

## Operators

The entry's `operator` compares the entry's version, on the left, with the model's:

| Operator | Matches when |
| --- | --- |
| `Equal`, the default | The versions are equal. A missing part counts as `0` and the `v` is ignored. Suffixes must be identical, the build included. |
| `GreaterThan` | The entry's version is greater than the model's. |
| `GreaterThanOrEqual` | The entry's version is greater than or equal to the model's. |
| Any other value, such as `LessThan` or `equal` | As for `Equal`, but two versions without suffixes must also have the same number of parts and the same `v`. The API accepts any value. |

So the entry's version is the newest one that `GreaterThanOrEqual` accepts. When either version has a suffix, the ordering operators match only an equal version. Otherwise, they need both versions to have the same number of parts, and the `v` on both or neither.

This entry accepts transformers versions up to `4.46.0` that have three parts, no `v` and no suffix, such as `4.44.2`. It refuses the BaseModel above, whose `4.45.0.dev0` has a suffix:

```yaml
spec:
  supportedModelFormats:
    - modelFramework:
        name: transformers
        version: "4.46.0"
        operator: GreaterThanOrEqual
      modelFormat:
        name: safetensors
        version: "1.0.0"
      modelArchitecture: LlamaForCausalLM
```

## Accepted version formats

A version has one to three numbers separated by dots, with an optional lowercase `v` in front. Suffixes can follow the third number:

| Version | Parses | Why |
| --- | --- | --- |
| `1`, `v1.12`, `0.6.0`, `v0.8.0` | Yes | One to three numbers. |
| `4.51.3-SAM-HQ-preview`, `4.43.0+build`, `4.45.0.dev0`, `1.2.3.4` | Yes, with a suffix | A pre-release after `-`, a build after `+`, or a dev part after `.`. |
| `1.08.0`, `V1.0.0`, `1.2-beta`, `1.0.0-`, `""` | No | A leading zero, a capital `V`, a suffix before the third number, an empty suffix, or an empty version. |

Nothing checks a version when you apply a model or a runtime. A version that doesn't parse never matches, and `version: ""` still counts as set, so leave the field out instead.

## Examples

| Entry's version | Operator | Model's version | Matches | Why |
| --- | --- | --- | --- | --- |
| `1.0.0` | `Equal` | `1.0.0` | Yes | The versions are equal. |
| `1.0.0` | Not set | `1.0` | Yes | The API sets `Equal`, and the missing patch counts as `0`. |
| `1.8.0` | `GreaterThan` | `1.7.0` | Yes | The entry's version is greater. |
| `1.8.0` | `GreaterThan` | `1.8.0` | No | Equal versions aren't greater. |
| `1.8.0` | `GreaterThanOrEqual` | `1.8.0` | Yes | Equal versions match. |
| `4.50.0` | `GreaterThanOrEqual` | `4.36.2` | Yes | The model's version is lower than the entry's. |
| `4.50.0` | `GreaterThanOrEqual` | `4.51.0` | No | The model's version is higher than the entry's. |
| `1.8.0-dev` | `GreaterThan` | `1.8.0-dev` | Yes | With a suffix, only an equal version matches. |
| `4.45.0.dev0` | `GreaterThanOrEqual` | `4.44.0` | No | The entry's version has a suffix, so only an equal version matches. |
| `4.50.0` | `GreaterThanOrEqual` | `4.45.0.dev0` | No | The model's version has a suffix, so only an equal version matches. |
| `1.8` | `GreaterThan` | `1.7.0` | No | The versions have different numbers of parts. |

## Mismatch reasons

When no entry matches, the runtime's reason is `model format '<label>' not in supported formats:` followed by each entry's differences, separated by `; `. [Troubleshoot runtime selection](../../guides/deploy-models/troubleshoot-runtime-selection.md#step-3-read-the-exclusion-reasons) explains the label. These are the differences in names and versions, with example values:

| Reason | When |
| --- | --- |
| `format name mismatch (model=gguf, runtime=safetensors)` or `framework name mismatch (model=diffusers, runtime=transformers)` | The names differ. |
| `format version mismatch (model=2.0.0, runtime=1.0.0)` or `framework version mismatch (model=4.46.0, runtime=4.45.0.dev0)` | The versions fail the entry's operator, or one of them is [malformed](#accepted-version-formats). |
| `model has no format version but runtime requires 1.0.0` or `model has no framework version but runtime requires 4.45.0.dev0` | Only the entry has a version. |
| `model has format version but runtime has no version requirement` or `model has framework version but runtime has no version requirement` | Only the model has a version. |
| `model has no framework but runtime requires transformers` | The model has no `modelFramework`. |

If the BaseModel above had the framework version `4.46.0`, `srt-llama-3-2-3b-instruct` would give this reason:

```text
model format 'mt:safetensors:1.0.0:LlamaForCausalLM:transformers:4.46.0' not in supported formats: framework version mismatch (model=4.46.0, runtime=4.45.0.dev0)
```

### Where a mismatch shows up

| When | What you see |
| --- | --- |
| Another runtime matches | OME picks from the runtimes that match, with no event. Since v1.3, [`kubectl ome runtime explain`](../kubectl-ome/runtime.md#explain) shows this one as `No`, with the reason. |
| No runtime matches | The webhook rejects the InferenceService. After admission, OME sets `RuntimeReady` to `False` and records a Warning event, both with reason `RuntimeNotFound` (since v1.3). Running pods keep serving. See [Troubleshoot runtime selection](../../guides/deploy-models/troubleshoot-runtime-selection.md). |
| The InferenceService names the runtime | OME uses it anyway. Since v1.3, the webhook warns `does not declare support for model`. OME records a `RuntimeCompatibilityAdvisory` Warning event, unless `spec.runtime.autoSync` is `false`. See [Reference a runtime explicitly](../../guides/deploy-models/reference-a-runtime-explicitly.md#what-ome-checks-when-you-apply). |

The webhook rejects only an InferenceService that has an `engine` section and no runner image of its own, in `engine.runner.image` or in both `engine.leader.runner.image` and `engine.worker.runner.image`.

To fix a mismatch, change the entry's version or [operator](#operators). If OME read no version from the model's files, [set it yourself](../../concepts/models/base-models.md#set-the-metadata-yourself). Or name the runtime in `spec.runtime.name`.

## Related pages

- [Troubleshoot runtime selection](../../guides/deploy-models/troubleshoot-runtime-selection.md): find out why OME can't choose a runtime for a model.
- [Reference a runtime explicitly](../../guides/deploy-models/reference-a-runtime-explicitly.md): name the runtime instead of letting OME pick one.
- [`kubectl ome runtime`](../kubectl-ome/runtime.md): see which runtimes match a model, and why.
- [Serving runtimes](../../concepts/runtimes/serving-runtimes.md): how a runtime declares the models it supports.
- [Diffusion pipeline runtime matching](diffusion-pipeline-runtime-matching.md): how an entry matches a diffusion model's pipeline.
- [Runtime selection scoring](runtime-selection-scoring.md): how OME ranks the runtimes that match.
- [Runtime accelerator-class matching](runtime-accelerator-class-matching.md), [Runtime deployment-mode matching](runtime-deployment-mode-matching.md) and [Model size range matching](model-size-range-matching.md): other checks that can exclude a runtime.
