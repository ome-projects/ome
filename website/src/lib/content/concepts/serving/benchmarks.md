---
title: Benchmarks
description: A BenchmarkJob runs genai-bench against an InferenceService with the traffic scenarios you choose, and stores the results.
---

A BenchmarkJob measures how an [InferenceService](inference-services.md) performs under load. OME runs [genai-bench](https://github.com/sgl-project/genai-bench) against it with the traffic shapes and concurrency levels you choose, and genai-bench saves its throughput and latency results to the storage you name. Use one to see how much load a replica takes before latency climbs, or to compare runtimes and GPU types. [Run benchmarks](../../guides/deploy-models/run-benchmarks.md) walks through a run from start to results.

The examples use the `llama-demo` namespace and the `llama-3-2-1b-instruct` InferenceService from [A minimal InferenceService](inference-services.md#a-minimal-inferenceservice).

## The BenchmarkJob resource

A benchmark runs in iterations. Each iteration sends one traffic scenario at one concurrency level, and stops after `maxTimePerIteration` minutes or `maxRequestsPerIteration` requests, whichever comes first.

This file creates a PersistentVolumeClaim for the results, and a BenchmarkJob that runs two scenarios at three concurrency levels: six iterations of up to 10 minutes.

```yaml title="benchmark.yaml"
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: benchmark-results
  namespace: llama-demo
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
---
apiVersion: ome.io/v1beta1
kind: BenchmarkJob
metadata:
  name: llama-3-2-1b-instruct-bench
  namespace: llama-demo
spec:
  endpoint:
    inferenceService:
      name: llama-3-2-1b-instruct
      namespace: llama-demo
  task: text-to-text
  trafficScenarios:
    - "D(100,100)"
    - "N(480,240)/(300,150)"
  numConcurrency:
    - 1
    - 4
    - 16
  maxTimePerIteration: 10
  maxRequestsPerIteration: 200
  outputLocation:
    storageUri: pvc://benchmark-results/llama-3-2-1b-instruct
  podOverride:
    image: ghcr.io/sgl-project/genai-bench:0.1.132
```

OME releases don't publish the genai-bench image that the `ome-resources` chart sets by default, so the example uses the image that the genai-bench project publishes. To make it the default, set the chart values `ome.benchmarkJob.image` to `ghcr.io/sgl-project/genai-bench` and `ome.benchmarkJob.tag` to `0.1.132`. The claim uses your cluster's default StorageClass. Apply the file:

```bash
kubectl apply -f benchmark.yaml
```

```output
persistentvolumeclaim/benchmark-results created
benchmarkjob.ome.io/llama-3-2-1b-instruct-bench created
```

The spec's fields:

| Field | What it sets |
| --- | --- |
| `endpoint` | Required. The InferenceService or URL to benchmark. See [Endpoints](#endpoints). |
| `task` | Required. The kind of requests: `text-to-text`, `image-to-text`, `text-to-embeddings` or `image-to-embeddings`. |
| `maxTimePerIteration` | Required. How many minutes an iteration runs at most. |
| `maxRequestsPerIteration` | Required. How many requests an iteration sends at most. |
| `outputLocation` | Required. Where the results go. See [Where results go](#where-results-go). |
| `trafficScenarios` | The sizes of the requests. See [Traffic scenarios](#traffic-scenarios). Without it, genai-bench uses its defaults. |
| `numConcurrency` | How many requests genai-bench keeps in flight, one level per iteration. Without it, genai-bench uses its defaults. |
| `resultFolderName` | The folder that holds the results. See [Where results go](#where-results-go). |
| `serviceMetadata` | Labels for the results: `engine` (`vLLM`, `SGLang` or `TGI`), `version`, `gpuType` (`H100`, `A100`, `MI300` or `A10`) and `gpuCount`. Set all four. |
| `huggingFaceSecretReference` | A Secret in the BenchmarkJob's namespace. Its `HUGGINGFACE_API_KEY` key sets the pod's `HUGGINGFACE_API_KEY` variable. |
| `podOverride` | Changes to the benchmark pod. See [How a benchmark runs](#how-a-benchmark-runs). |

`additionalRequestParams` and `dataset` have no effect: OME doesn't pass them to genai-bench. The [API reference](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-BenchmarkJobSpec) describes every field.

## Endpoints

`spec.endpoint` names one target: an `inferenceService` or an `endpoint` URL. Admission rejects a BenchmarkJob that sets neither or both. You can benchmark an InferenceService whose model is stored on the nodes.

### An InferenceService

`name` and `namespace` are required, and the InferenceService can be in another namespace. OME fills in the rest from it:

| genai-bench gets | From the InferenceService |
| --- | --- |
| Base URL | The scheme and host of `status.url`, such as `http://llama-3-2-1b-instruct.llama-demo.svc.cluster.local:8080` |
| API format | `openai`, or the value of `PROTOCOL_VERSION` in `spec.engine.runner.env` |
| Model name | Since v1.3, the engine's `--served-model-name`, or else its `--model` or `--model-path`: `vllm-model` in the example. On v1.2.2, always `vllm-model`, so the runtime must serve the model under that name. |
| Tokenizer | The model's `storage.path`, mounted read-only from the node |
| API key | The placeholder `sample-key` |

The pod runs only on nodes where the model is `Ready`, as [Node labels and status](../models/base-models.md#node-labels-and-status) describes. OME can't benchmark a model with a `pvc://` storage URI, because no node holds a copy of it. Since v1.3, it also can't benchmark an InferenceService that names only a runtime.

### An external endpoint

`endpoint.endpoint` takes a base `url`, an `apiFormat` (`openai`, `oci-cohere` or `cohere`) and the `modelName` the endpoint serves. A BenchmarkJob that targets a URL becomes `Failed`: genai-bench needs a tokenizer, and an API key for `openai`, and a BenchmarkJob has no field for either. This is a known bug.

## Traffic scenarios

A traffic scenario sets the size of each request, in tokens or images. The forms for each task:

| Scenario | Tasks | Requests |
| --- | --- | --- |
| `D(<input>,<output>)` | `text-to-text` | Fixed counts of input and output tokens. |
| `N(<mean>,<sd>)/(<mean>,<sd>)` | `text-to-text` | Input and output token counts from normal distributions, given as mean and standard deviation. |
| `U(<min>,<max>)/(<min>,<max>)` | `text-to-text` | Input and output token counts from uniform distributions, given as bounds. |
| `I(<width>,<height>)` or `I(<width>,<height>,<images>)` | `image-to-text`, `image-to-embeddings` | Images of `<width>` by `<height>` pixels, and optionally how many images a request carries. |
| `E(<n>,<n>)` | `text-to-embeddings` | The form that admission accepts for this task. |

For example, `N(480,240)/(300,150)` sends around 480 input tokens and asks for around 300 output tokens, with standard deviations of 240 and 150.

Admission rejects a scenario that doesn't fit the task's forms, such as `D(100)`. The `text-to-text` check is loose, so a misspelled scenario can pass admission and fail in genai-bench.

For `text-to-embeddings`, admission accepts only two-number scenarios, `E(<n>,<n>)`, and rejects a BenchmarkJob that leaves `trafficScenarios` out. Check that your genai-bench image runs the two-number form before you rely on it. This is a known bug.

## Where results go

`outputLocation.storageUri` says where genai-bench writes the results. A `pvc://<claim>/<sub-path>` URI writes them to that directory of a claim in the BenchmarkJob's namespace, and the claim must exist before OME creates the Job. In the example, that's the `llama-3-2-1b-instruct` directory of the claim `benchmark-results`. [Run benchmarks](../../guides/deploy-models/run-benchmarks.md) shows how to read them. An `oci://`, `s3://`, `az://`, `gs://` or `github://` URI uploads the results to that store, with the settings in `outputLocation.parameters`. [Benchmark output storage](../../reference/storage/benchmark-output-storage.md) lists each URI format and its parameters.

`resultFolderName` names the folder that holds the results. Without it, genai-bench picks the name.

!!! warning "Credentials in parameters"
    OME passes `parameters` to genai-bench as command-line arguments, so anyone who can read the BenchmarkJob, its Job or its pod can read them. For OCI Object Storage and Google Cloud Storage, mount the credentials file from a Secret through `podOverride`, and pass its path in `config_file` or `gcp_credentials_path`. See [Credentials appear on the pod command line](../../reference/storage/benchmark-output-storage.md#credentials-appear-on-the-pod-command-line).

## How a benchmark runs

OME creates a Job with the BenchmarkJob's name and the label `benchmark: <name>`, in the BenchmarkJob's namespace. For an InferenceService, OME waits until it's `Ready`, and creates the Job up to a minute later. The Job runs genai-bench once, and doesn't retry it. Read genai-bench's output with `kubectl logs job/llama-3-2-1b-instruct-bench -n llama-demo`.

The pod's defaults, and what `podOverride` does to each:

| Field | Default | `podOverride` |
| --- | --- | --- |
| `image` | The chart's `ome.benchmarkJob` image | Replaces it. |
| `resources` | 2 CPUs and 2Gi of memory, as requests and limits | Replaces the requests and limits it names, and keeps the others. |
| `env` | `HUGGINGFACE_API_KEY` when you set `huggingFaceSecretReference`, and `MODEL_PATH` for an InferenceService | Adds variables, and replaces those with the same name. |
| `envFrom` | Not set | Sets it. |
| `volumes` and `volumeMounts` | The model's `storage.path`, and the claim of a `pvc://` URI | Adds them. A volume with the same name, or a mount at the same `mountPath`, replaces OME's. |
| `nodeSelector` | For an InferenceService, nodes where the model is `Ready` | Adds labels, and replaces those with the same key. |
| `tolerations` | The `nvidia.com/gpu` taint with the `NoSchedule` effect. The pod requests no GPUs. | Replaces the default. |
| `affinity` | Not set | Sets it. |

The `ome-resources` chart's `ome.benchmarkJob` values set the default image and resources, and a change applies to the Jobs that OME creates afterwards. The pod runs as the namespace's `default` ServiceAccount, so give that ServiceAccount any pull secrets it needs, as [BenchmarkJob pods](../../getting-started/private-registries.md#benchmarkjob-pods) describes.

When you raise a request, raise its limit too. With a request above its limit, OME can't create the Job, and the BenchmarkJob stays `Pending`:

```yaml
spec:
  podOverride:
    resources:
      requests:
        cpu: "4"
        memory: 8Gi
      limits:
        cpu: "4"
        memory: 8Gi
```

Editing a BenchmarkJob doesn't change its Job. To run the benchmark again, [delete the Job](../../guides/deploy-models/run-benchmarks.md#run-the-benchmark-again), and OME creates a new one from the current spec. The Job and its pod stay until you delete the BenchmarkJob, and the results stay where genai-bench wrote them.

## Status

`kubectl get` shows the BenchmarkJob's state:

```bash
kubectl get benchmarkjob llama-3-2-1b-instruct-bench -n llama-demo
```

```output
NAME                          AGE   STATUS
llama-3-2-1b-instruct-bench   12m   Running
```

`STATUS` is `status.state`, which follows the Job:

| State | When |
| --- | --- |
| `Pending` | OME hasn't created the Job, or the Job was deleted. |
| `Running` | The Job exists and hasn't finished. |
| `Completed` | The Job succeeded. |
| `Failed` | The Job failed. |

The other status fields:

| Field | What it holds |
| --- | --- |
| `startTime` | When the Job started. It can be empty while the state is `Running`. |
| `completionTime` | When the Job succeeded or failed. |
| `failureMessage` | The message of the Job's `Failed` condition. genai-bench's own errors are in the pod's logs. |
| `lastReconcileTime` | When the state last changed. |

When a BenchmarkJob stays `Pending`, the controller logs say why:

```bash
kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1 | grep llama-3-2-1b-instruct-bench
```

[The BenchmarkJob stays Pending](../../guides/deploy-models/run-benchmarks.md#the-benchmarkjob-stays-pending) lists the common messages and their fixes. A request above its limit, or an output URI from [URIs OME can't write to](../../reference/storage/benchmark-output-storage.md#uris-ome-cant-write-to), also keeps a BenchmarkJob `Pending`.

## Next steps

- [Run benchmarks](../../guides/deploy-models/run-benchmarks.md): run a BenchmarkJob, and read its results.
- [Benchmark output storage](../../reference/storage/benchmark-output-storage.md): the URI formats and parameters for each backend.
