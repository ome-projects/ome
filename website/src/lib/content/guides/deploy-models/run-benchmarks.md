---
title: Run benchmarks
description: Run a BenchmarkJob against an InferenceService with realistic traffic scenarios, and collect its throughput and latency results from storage.
---

A [BenchmarkJob](../../concepts/serving/benchmarks.md) measures the throughput and latency of an [InferenceService](../../concepts/serving/inference-services.md) under load. OME runs [genai-bench](https://github.com/sgl-project/genai-bench) in a Kubernetes Job with the traffic scenarios and concurrency levels you choose, and genai-bench saves the results to storage. Here you benchmark the `qwen3-0-6b` InferenceService from [Serve your first model](../../getting-started/serve-your-first-model.md), and copy the results from a PersistentVolumeClaim.

<div class="prerequisites" markdown>

- OME v1.3 or later, and `kubectl` access to the namespace `qwen3-0-6b`. Until v1.3 is released, run [a build of `main`](../../getting-started/install.md#install-from-source).
- The InferenceService `qwen3-0-6b`, `Ready`, from [Serve your first model](../../getting-started/serve-your-first-model.md). You can't benchmark an InferenceService whose model is on a PVC, or one that names only a runtime.
- 2 CPUs and 2 GiB of memory free on a node where the model is `Ready`. The benchmark pod runs there, because it reads the model's tokenizer from the node.
- A default StorageClass, for the claim that holds the results. `kubectl get storageclass` marks it `(default)`.
- Nodes that can pull `ghcr.io/sgl-project/genai-bench:0.1.132`, and `busybox` from Docker Hub. To pull from your own registry instead, see [BenchmarkJob pods](../../getting-started/private-registries.md#benchmarkjob-pods).

</div>

## Step 1: Create a benchmark

Save this file as `qwen3-0-6b-bench.yaml`. It creates a claim for the results, and a BenchmarkJob that runs two traffic scenarios at two concurrency levels, four iterations in all:

```yaml title="qwen3-0-6b-bench.yaml"
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: benchmark-results
  namespace: qwen3-0-6b
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 1Gi
---
apiVersion: ome.io/v1beta1
kind: BenchmarkJob
metadata:
  name: qwen3-0-6b-bench
  namespace: qwen3-0-6b
spec:
  endpoint:
    inferenceService:
      name: qwen3-0-6b
      namespace: qwen3-0-6b
  task: text-to-text
  trafficScenarios:
    - "D(100,100)"
    - "D(2000,200)"
  numConcurrency:
    - 1
    - 8
  maxTimePerIteration: 5
  maxRequestsPerIteration: 100
  outputLocation:
    storageUri: pvc://benchmark-results/qwen3-0-6b
  resultFolderName: baseline
  podOverride:
    image: ghcr.io/sgl-project/genai-bench:0.1.132
```

| Field | What it does here |
| --- | --- |
| `endpoint.inferenceService` | Names the InferenceService. OME takes its URL, API format and model name from it. |
| `task` | `text-to-text` sends text prompts and reads text answers. |
| `trafficScenarios` | `D(100,100)` sends 100 input tokens and asks for 100 output tokens. `D(2000,200)` sends long prompts for short answers. See [Choose traffic scenarios](#choose-traffic-scenarios). |
| `numConcurrency` | Runs each scenario with 1, then 8, requests in flight. |
| `maxTimePerIteration`, `maxRequestsPerIteration` | End each iteration after 5 minutes or 100 requests, whichever limit it reaches first. |
| `outputLocation.storageUri` | Writes the results to the directory `qwen3-0-6b` on the claim `benchmark-results`, in the BenchmarkJob's namespace. |
| `resultFolderName` | Names the folder in that directory that holds this run's results. Without it, genai-bench names the folder. |
| `podOverride.image` | Sets the genai-bench image. Set it in every BenchmarkJob: OME releases don't publish the chart's default image. |

OME starts the benchmark once the InferenceService is `Ready`. The benchmark pod must reach the InferenceService's URL. With ingress creation off, the default, that's the Service's address inside the cluster. Each request also names the model. OME reads the name from the engine pods: here it's `Qwen/Qwen3-0.6B`, from the runtime's `--served-model-name`, the name you used in [Step 7 of Serve your first model](../../getting-started/serve-your-first-model.md#step-7-send-a-request).

`endpoint` can name a URL instead of an InferenceService, but such a benchmark fails, as [An external endpoint](../../concepts/serving/benchmarks.md#an-external-endpoint) explains. [The BenchmarkJob resource](../../concepts/serving/benchmarks.md#the-benchmarkjob-resource) lists every field.

Apply the file:

```bash
kubectl apply -f qwen3-0-6b-bench.yaml
```

```output
persistentvolumeclaim/benchmark-results created
benchmarkjob.ome.io/qwen3-0-6b-bench created
```

## Step 2: Watch it run

Check the BenchmarkJob's state:

```bash
kubectl get benchmarkjob qwen3-0-6b-bench -n qwen3-0-6b
```

```output
NAME               AGE   STATUS
qwen3-0-6b-bench   3m    Running
```

`STATUS` is `Pending` until OME creates the Job, then `Running`, and then `Completed` or `Failed`. If it stays `Pending`, see [The BenchmarkJob stays Pending](#the-benchmarkjob-stays-pending).

The Job that runs genai-bench has the BenchmarkJob's name. Follow its output:

```bash
kubectl logs -f job/qwen3-0-6b-bench -n qwen3-0-6b
```

It prints genai-bench's output until genai-bench exits.

Or wait for the state `Completed`. The four iterations take up to about 20 minutes, plus the time to pull the image and start genai-bench:

```bash
kubectl wait --for=jsonpath='{.status.state}'=Completed benchmarkjob/qwen3-0-6b-bench -n qwen3-0-6b --timeout=40m
```

```output
benchmarkjob.ome.io/qwen3-0-6b-bench condition met
```

If the state becomes `Failed`, the command waits until the timeout. See [The BenchmarkJob fails](#the-benchmarkjob-fails).

## Step 3: Read the results

genai-bench wrote the results to `qwen3-0-6b/baseline` on the claim. The benchmark pod has stopped, so read them from a pod that mounts the claim. Save this file as `benchmark-results-reader.yaml`:

```yaml title="benchmark-results-reader.yaml"
apiVersion: v1
kind: Pod
metadata:
  name: benchmark-results-reader
  namespace: qwen3-0-6b
spec:
  containers:
    - name: reader
      image: busybox:1.37
      command: ["sleep", "3600"]
      volumeMounts:
        - name: results
          mountPath: /results
          readOnly: true
  volumes:
    - name: results
      persistentVolumeClaim:
        claimName: benchmark-results
        readOnly: true
```

Create the pod, which mounts the claim read-only at `/results`, and wait for it to be ready:

```bash
kubectl apply -f benchmark-results-reader.yaml
kubectl wait --for=condition=Ready pod/benchmark-results-reader -n qwen3-0-6b --timeout=5m
```

```output
pod/benchmark-results-reader created
pod/benchmark-results-reader condition met
```

List the results:

```bash
kubectl exec -n qwen3-0-6b benchmark-results-reader -- ls /results/qwen3-0-6b/baseline
```

The files, and what they hold, depend on the genai-bench version: see [genai-bench's documentation](https://github.com/sgl-project/genai-bench).

Copy the folder to your machine:

```bash
kubectl cp qwen3-0-6b/benchmark-results-reader:/results/qwen3-0-6b/baseline ./baseline
```

kubectl copies the folder to `baseline` in your working directory.

## Run the benchmark again

OME runs a BenchmarkJob's Job once, and doesn't change it when you edit the BenchmarkJob. To run again, edit the BenchmarkJob, with a new `resultFolderName` so the results go to a new folder. Then delete the Job:

```bash
kubectl delete job qwen3-0-6b-bench -n qwen3-0-6b
```

```output
job.batch "qwen3-0-6b-bench" deleted from qwen3-0-6b namespace
```

OME creates a new Job from the current spec. The BenchmarkJob is `Pending` until it does.

## Choose traffic scenarios

A BenchmarkJob runs each traffic scenario at each concurrency level. For `text-to-text`, a scenario sets how many tokens each request sends and asks for:

| Scenario | Requests |
| --- | --- |
| `D(100,100)` | Short prompts and short answers, like chat turns. |
| `D(2000,200)` | Long prompts and short answers, like summaries or retrieval-augmented generation. |
| `D(100,1000)` | Short prompts and long answers, like writing tasks. |
| `N(480,240)/(300,150)` | Lengths that vary around 480 input and 300 output tokens, with standard deviations of 240 and 150. |

[Traffic scenarios](../../concepts/serving/benchmarks.md#traffic-scenarios) lists the forms for every task. Admission checks `text-to-text` scenarios loosely, so a typo can pass `kubectl apply` and fail later in genai-bench.

`numConcurrency` sets how many requests genai-bench keeps in flight. Run a few levels, from 1 to beyond the load you expect, to see how throughput and latency change as the load grows.

If you leave out `trafficScenarios` or `numConcurrency`, genai-bench uses its own defaults for that setting.

For a `text-to-embeddings` model, set `trafficScenarios` in the form `E(<n>,<n>)`, the only form admission accepts. Read [Traffic scenarios](../../concepts/serving/benchmarks.md#traffic-scenarios) first.

## Troubleshooting

### Admission rejects the BenchmarkJob

For the scenario `D(100)`, `kubectl apply` prints this line:

```output
Error from server (Forbidden): error when creating "qwen3-0-6b-bench.yaml": admission webhook "benchmarkjob.ome-webhook-server.validator" denied the request: invalid traffic scenarios: failed to validate scenario 'D(100)': invalid scenario format for task 'text-to-text': D(100)
```

The webhook's reason, after `denied the request:`, starts with the part of the spec that's wrong:

| Reason starts with | Fix |
| --- | --- |
| `invalid endpoint:` | Set exactly one of `endpoint.inferenceService` and `endpoint.endpoint`. |
| `invalid traffic scenarios:` | Write each scenario in a form for the task, as in [Choose traffic scenarios](#choose-traffic-scenarios). |
| `invalid storage:` | Fix `outputLocation.storageUri`. A `pvc://` URI needs a directory after the claim's name, as in `pvc://benchmark-results/qwen3-0-6b`. |

### The BenchmarkJob stays Pending

The reason is in the controller's logs, not in the BenchmarkJob's status. Search the logs of every controller replica for the BenchmarkJob's name. The command assumes that OME runs in the `ome` namespace:

```bash
kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1 | grep qwen3-0-6b-bench
```

Look for these messages:

| Message | Fix |
| --- | --- |
| `InferenceService is not ready, re-queuing` | Wait. OME checks again every minute. If the InferenceService doesn't become `Ready`, see [Troubleshooting](../../getting-started/serve-your-first-model.md#troubleshooting) in Serve your first model. |
| `failed to get InferenceService` | Fix `endpoint.inferenceService`: no InferenceService has that name in that namespace. |
| `PVC benchmark-results not found` | Create the claim in the BenchmarkJob's namespace. |
| `cannot determine the served model name` | OME found no engine pod with a container that passes `--served-model-name`, `--model` or `--model-path`. List the engine pods with `kubectl get pods -n qwen3-0-6b -l ome.io/inferenceservice=qwen3-0-6b,component=engine`, and check the runtime's arguments. |
| `has no Model defined` | The InferenceService names only a runtime, and can't be benchmarked. |
| `has missing Storage or Path information` | The model has no `storage.path`, as with a model on a PVC, and can't be benchmarked. |

[Status](../../concepts/serving/benchmarks.md#status) lists the other causes. OME retries with a growing delay. After you fix the cause, annotate the BenchmarkJob to make it try again right away:

```bash
kubectl annotate benchmarkjob qwen3-0-6b-bench -n qwen3-0-6b retry=1 --overwrite
```

```output
benchmarkjob.ome.io/qwen3-0-6b-bench annotated
```

### The benchmark pod doesn't start

The BenchmarkJob is `Running`, but its pod stays `Pending` or can't pull its image. The events at the end of the pod's description say why:

```bash
kubectl describe pod -n qwen3-0-6b -l benchmark=qwen3-0-6b-bench
```

- `FailedScheduling`: no node where the model is `Ready` has room for the pod, or the node has a taint that the pod doesn't tolerate. Free room on such a node, or set `podOverride.resources` or `podOverride.tolerations`. Your tolerations replace the default `nvidia.com/gpu` one, so list every taint that the pod must tolerate.
- `ErrImagePull` or `ImagePullBackOff`: check that `podOverride.image` is set as in [Step 1](#step-1-create-a-benchmark). If the registry refuses access, give the `default` ServiceAccount in `qwen3-0-6b` a pull secret, as [BenchmarkJob pods](../../getting-started/private-registries.md#benchmarkjob-pods) describes.

The pod picks up `podOverride` changes and pull secrets only when it's created, so after those fixes, [run the benchmark again](#run-the-benchmark-again).

### The BenchmarkJob fails

genai-bench exited with an error, and the Job doesn't retry it. The end of the pod's log holds the error:

```bash
kubectl logs job/qwen3-0-6b-bench -n qwen3-0-6b
```

Fix the cause in the BenchmarkJob or at the endpoint, then [run the benchmark again](#run-the-benchmark-again).

## Clean up

Delete the BenchmarkJob, which also deletes its Job and pod, then the reader pod:

```bash
kubectl delete benchmarkjob qwen3-0-6b-bench -n qwen3-0-6b
kubectl delete pod benchmark-results-reader -n qwen3-0-6b
```

```output
benchmarkjob.ome.io "qwen3-0-6b-bench" deleted from qwen3-0-6b namespace
pod "benchmark-results-reader" deleted from qwen3-0-6b namespace
```

The results stay on the claim. Delete the claim when you no longer need them. With a StorageClass whose reclaim policy is `Delete`, that deletes the volume, and the results with it:

```bash
kubectl delete pvc benchmark-results -n qwen3-0-6b
```

```output
persistentvolumeclaim "benchmark-results" deleted from qwen3-0-6b namespace
```

To delete the InferenceService, the runtime and the model too, follow [Clean up](../../getting-started/serve-your-first-model.md#clean-up) in Serve your first model.

## Next steps

- [Benchmarks](../../concepts/serving/benchmarks.md): every field of a BenchmarkJob, the rules for endpoints and traffic scenarios, and the status.
- [Benchmark output storage](../../reference/storage/benchmark-output-storage.md): write the results to OCI Object Storage, Amazon S3, Azure Blob Storage, Google Cloud Storage or GitHub Releases.
