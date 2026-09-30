---
title: Serve a multi-node model
description: Run one copy of a model across several nodes by giving its engine a leader and workers, which OMENative creates, updates and repairs together as one Instance.
since: v1.3
---

Run one copy of a model across several nodes, as a leader pod and worker pods that serve it together. [OMENative](../../concepts/omenative/overview.md) runs each leader and its workers as one [Instance](../../concepts/omenative/instances.md), which OME creates, updates, repairs and removes as a unit. By default, an update brings a new Instance up and serving before the old one drains. A `leader` and a `worker` on the engine are all it takes to run on OMENative.

You'll serve Qwen3 0.6B, the model from [Serve your first model](../../getting-started/serve-your-first-model.md), on two pods with one GPU each, so you can try the steps on modest hardware. A larger model takes the same steps, with more GPUs in each pod or more workers, and often a longer [readiness deadline](#control-how-instances-update-and-recover).

<div class="prerequisites" markdown>

- OME v1.3 or later, and `kubectl`, with the rights to create namespaces and cluster-scoped OME resources.
- The ClusterBaseModel `qwen3-0-6b` from [Serve your first model](../../getting-started/serve-your-first-model.md), downloaded and `Ready`.
- Two amd64 nodes with a free NVIDIA GPU each, exposed as `nvidia.com/gpu`, and the model `Ready` on both. `kubectl get nodes -l models.ome.io/clusterbasemodel.qwen3-0-6b=Ready` lists them. The leader and the worker each request 1 GPU, 10 CPUs and 30 GiB of memory. The scheduler can also place both pods on one node with two free GPUs.
- Pod-to-pod network traffic between those nodes.
- Recommended: the scheduler-plugins PodGroup CRD and a gang-aware scheduler, such as the alpha [OME scheduler](../../concepts/scheduling/ome-scheduler.md), which needs Kubernetes 1.35. With them, the leader and its worker are scheduled together or not at all. [Gang scheduling](../../concepts/serving/gang-scheduling.md) shows the setup. Without them, the example still runs when both GPUs are free.

</div>

## Step 1: Prepare a multi-node runtime

Save this runtime as `srt-qwen3-0-6b-multi-node.yaml`. It's the `srt-qwen3-0-6b` runtime from Serve your first model, split into a `leader` and a `worker` in its `engineConfig`:

```yaml title="srt-qwen3-0-6b-multi-node.yaml"
apiVersion: ome.io/v1beta1
kind: ClusterServingRuntime
metadata:
  name: srt-qwen3-0-6b-multi-node
spec:
  supportedModelFormats:
    - modelFramework:
        name: transformers
        version: "4.51.0"
      modelFormat:
        name: safetensors
        version: "1.0.0"
      modelArchitecture: Qwen3ForCausalLM
      autoSelect: false
      priority: 1
  protocolVersions:
    - openAI
  modelSizeRange:
    min: 0.5B
    max: 1B
  engineConfig:
    tolerations:
      - key: nvidia.com/gpu
        operator: Exists
        effect: NoSchedule
    volumes:
      - name: dshm
        emptyDir:
          medium: Memory
    leader:
      runner:
        name: ome-container
        image: docker.io/lmsysorg/sglang:v0.5.5.post3-cu129-amd64
        ports:
          - containerPort: 8080
            name: http1
            protocol: TCP
        command:
          - python3
          - -m
          - sglang.launch_server
          - --host
          - "0.0.0.0"
          - --port
          - "8080"
          - --model-path
          - $(MODEL_PATH)
          - --served-model-name
          - Qwen/Qwen3-0.6B
          - --mem-frac
          - "0.9"
          - --tp-size
          - $(PARALLELISM_SIZE)
          - --dist-init-addr
          - $(OME_LEADER_ADDRESS):5000
          - --nnodes
          - $(OME_INSTANCE_POD_COUNT)
          - --node-rank
          - $(OME_INSTANCE_POD_RANK)
        volumeMounts:
          - mountPath: /dev/shm
            name: dshm
        resources:
          requests:
            cpu: 10
            memory: 30Gi
            nvidia.com/gpu: 1
          limits:
            cpu: 10
            memory: 30Gi
            nvidia.com/gpu: 1
        startupProbe:
          httpGet:
            path: /health_generate
            port: 8080
          initialDelaySeconds: 60
          periodSeconds: 6
          timeoutSeconds: 30
          failureThreshold: 150
        readinessProbe:
          httpGet:
            path: /health_generate
            port: 8080
          periodSeconds: 60
          timeoutSeconds: 200
          failureThreshold: 3
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          periodSeconds: 60
          timeoutSeconds: 60
          failureThreshold: 5
    worker:
      size: 1
      runner:
        name: ome-container
        image: docker.io/lmsysorg/sglang:v0.5.5.post3-cu129-amd64
        command:
          - python3
          - -m
          - sglang.launch_server
          - --host
          - "0.0.0.0"
          - --port
          - "8080"
          - --model-path
          - $(MODEL_PATH)
          - --served-model-name
          - Qwen/Qwen3-0.6B
          - --mem-frac
          - "0.9"
          - --tp-size
          - $(PARALLELISM_SIZE)
          - --dist-init-addr
          - $(OME_LEADER_ADDRESS):5000
          - --nnodes
          - $(OME_INSTANCE_POD_COUNT)
          - --node-rank
          - $(OME_INSTANCE_POD_RANK)
        volumeMounts:
          - mountPath: /dev/shm
            name: dshm
        resources:
          requests:
            cpu: 10
            memory: 30Gi
            nvidia.com/gpu: 1
          limits:
            cpu: 10
            memory: 30Gi
            nvidia.com/gpu: 1
```

`leader.runner` and `worker.runner` each define the pod's container, `ome-container`, and run the same SGLang command. OME sets variables that give each pod its place in the Instance:

| Flag | Variable | Value |
| --- | --- | --- |
| `--tp-size` | `PARALLELISM_SIZE` | The pod's GPUs times the pods in the Instance: `2` here |
| `--dist-init-addr` | `OME_LEADER_ADDRESS`, with port 5000 | The leader's DNS name, which resolves once the leader pod has an IP address |
| `--nnodes` | `OME_INSTANCE_POD_COUNT` | `1` + `worker.size`: `2` here |
| `--node-rank` | `OME_INSTANCE_POD_RANK` | `0` on the leader, `k + 1` on worker `k` |

[Pod labels and environment](../../concepts/architecture/deployment-modes.md#pod-labels-and-environment) lists the other variables, and [Serving runtimes](../../concepts/runtimes/serving-runtimes.md#the-engine-decoder-and-router) explains when OME leaves `PARALLELISM_SIZE` unset.

- `tolerations` and `volumes` on `engineConfig` apply to both pods.
- The leader is the pod that serves requests, so only the leader declares the HTTP port and has probes. Keep serving probes off the workers: a failing readiness probe keeps the Instance from becoming ready, and a failing liveness probe can make OME recreate the whole Instance.
- `worker.size: 1` gives each Instance one worker.
- `supportedModelFormats` and `modelSizeRange` match those of `srt-qwen3-0-6b`. With `autoSelect: false`, OME uses this runtime only when an InferenceService names it.

Create the runtime:

```bash
kubectl apply -f srt-qwen3-0-6b-multi-node.yaml
```

```output
clusterservingruntime.ome.io/srt-qwen3-0-6b-multi-node created
```

!!! warning "LeaderWorkerSet is deprecated"
    The LeaderWorkerSet-backed `MultiNode` mode is deprecated. The catalog's multi-node runtimes, `srt-deepseek-rdma`, `srt-deepseek-rdma-pd` and `srt-kimi-k2-pd`, still pass its `LWS_*` variables, which OMENative pods don't get. To run one of them on OMENative, [make an OMENative copy](move-from-leaderworkerset.md#step-2-make-an-omenative-copy-of-the-runtime) first.

## Step 2: Create the InferenceService

Create a namespace for the InferenceService:

```bash
kubectl create namespace qwen3-multi-node
```

```output
namespace/qwen3-multi-node created
```

Save this InferenceService as `qwen3-multi-node-isvc.yaml`:

```yaml title="qwen3-multi-node-isvc.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: qwen3-multi-node
  namespace: qwen3-multi-node
spec:
  model:
    name: qwen3-0-6b
  runtime:
    name: srt-qwen3-0-6b-multi-node
  engine:
    minReplicas: 1
    maxReplicas: 1
    leader: {}
    worker:
      size: 1
```

- `runtime.name` names the runtime from Step 1.
- `leader: {}` and `worker` make the engine's Service send requests only to leaders, and `{}` keeps the runtime's leader as it is. Declare both: the webhook rejects an engine that declares only one.
- `worker.size` is the number of workers in each Instance. It defaults to the runtime's `worker.size`, or 1.
- `minReplicas` and `maxReplicas` count Instances, and each Instance needs the GPUs of a leader and all its workers. Here, that's one Instance of two pods.
- To use a gang-aware scheduler, add its name as `schedulerName` under `engine`, as [Bring a gang-aware scheduler](../../concepts/serving/gang-scheduling.md#bring-a-gang-aware-scheduler) shows.

Create the InferenceService:

```bash
kubectl apply -f qwen3-multi-node-isvc.yaml
```

```output
inferenceservice.ome.io/qwen3-multi-node created
```

## Step 3: Check the Instance

Wait for the InferenceService to be ready. The first start pulls the SGLang image onto both nodes, which can take several minutes:

```bash
kubectl wait --for=condition=Ready inferenceservice/qwen3-multi-node -n qwen3-multi-node --timeout=30m
```

```output
inferenceservice.ome.io/qwen3-multi-node condition met
```

OME created the [InferenceReplica](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-InferenceReplica) `qwen3-multi-node-engine`, which runs the engine's Instances. Its counts are Instances, not pods:

```bash
kubectl get inferencereplica qwen3-multi-node-engine -n qwen3-multi-node
```

```output
NAME                      COMPONENT   DESIRED   CURRENT   READY   AVAILABLE   AGE
qwen3-multi-node-engine   engine      1         1         1       1           9m
```

List the Instance's pods with their ranks and nodes:

```bash
kubectl get pods -n qwen3-multi-node -l ome.io/inferenceservice=qwen3-multi-node \
  -o 'custom-columns=NAME:.metadata.name,RANK:.spec.containers[0].env[?(@.name=="OME_INSTANCE_POD_RANK")].value,NODE:.spec.nodeName,READY:.status.conditions[?(@.type=="Ready")].status'
```

```output
NAME                                 RANK   NODE         READY
qwen3-multi-node-engine-0-leader-0   0      gpu-node-1   True
qwen3-multi-node-engine-0-worker-0   1      gpu-node-2   True
```

Your node names differ. If the PodGroup CRD is installed, check the Instance's PodGroup:

```bash
kubectl get podgroup qwen3-multi-node-engine-0 -n qwen3-multi-node \
  -o custom-columns=NAME:.metadata.name,MIN-MEMBER:.spec.minMember
```

```output
NAME                        MIN-MEMBER
qwen3-multi-node-engine-0   2
```

`MIN-MEMBER` is the Instance's two pods, so a gang-aware scheduler binds both or neither.

## Step 4: Send a request

The engine's Service, `qwen3-multi-node-engine`, sends requests only to the leader. Forward a local port to it, and leave the command running:

```bash
kubectl port-forward svc/qwen3-multi-node-engine 8080:8080 -n qwen3-multi-node
```

```output
Forwarding from 127.0.0.1:8080 -> 8080
Forwarding from [::1]:8080 -> 8080
```

With ingress off, the default, don't send requests to the InferenceService's URL. It points to the Service `qwen3-multi-node`, which also selects the worker. To reach a multi-node engine from outside the cluster, see [Expose a multi-node engine](../networking/expose-without-ingress.md#expose-a-multi-node-engine).

In a second terminal, send a chat completion request. `model` is the name that the runtime serves the model under, and `chat_template_kwargs` turns off Qwen3's thinking mode:

```bash
curl -s http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model": "Qwen/Qwen3-0.6B", "messages": [{"role": "user", "content": "What is Kubernetes? Answer in one sentence."}], "max_tokens": 100, "chat_template_kwargs": {"enable_thinking": false}}'
```

The response is an OpenAI chat completion, with the model's answer in `choices[0].message.content`.

## Control how Instances update and recover

The engine's `lifecycle` sets how OME updates and repairs its Instances. This version of `qwen3-multi-node-isvc.yaml` sets it explicitly and runs two Instances, which need four GPUs:

```yaml title="qwen3-multi-node-isvc.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: qwen3-multi-node
  namespace: qwen3-multi-node
spec:
  model:
    name: qwen3-0-6b
  runtime:
    name: srt-qwen3-0-6b-multi-node
  engine:
    minReplicas: 2
    maxReplicas: 2
    leader: {}
    worker:
      size: 1
    lifecycle:
      restartPolicy: RecreateInstanceOnPodRestart
      readyPolicy: AllPodReady
      instanceReadyTimeout: 45m
      updateStrategy:
        type: SurgeThenDrain
        rollingUpdate:
          maxSurge: 1
          maxUnavailable: 1
```

| Field | Default | What it does |
| --- | --- | --- |
| `restartPolicy` | `RecreateInstanceOnPodRestart` | When a pod of a ready Instance fails, disappears or restarts its `ome-container`, OME recreates the whole Instance. |
| `readyPolicy` | `AllPodReady` | An Instance is ready when all its pods are. An engine with a leader and a worker must use this value. |
| `instanceReadyTimeout` | `30m` in the chart | How long a new Instance has to become ready before OME marks it `Failed`. |
| `updateStrategy.type` | `SurgeThenDrain` | An update brings up a whole new Instance, then drains an old one once the new one is ready. |
| `maxSurge` | `25%` of the Instances, rounded up, in the chart | How many new Instances an update brings up at once. |
| `maxUnavailable` | No limit under `SurgeThenDrain` | How many Instances OME rebuilds at once when a pod is stuck, for example in `CrashLoopBackOff`. See [The crash-loop repair](../../concepts/omenative/instance-restart-policy.md#the-crash-loop-repair). |

Apart from `instanceReadyTimeout` and `maxUnavailable`, these values are the defaults. `maxUnavailable: 1` lets OME rebuild one stuck Instance at a time, which `0` would block. It also keeps the engine `Ready` while one Instance is out of service. With `maxSurge: 1`, an update replaces one Instance at a time, so it needs two more GPUs while the new Instance starts. Apply the file:

```bash
kubectl apply -f qwen3-multi-node-isvc.yaml
```

```output
inferenceservice.ome.io/qwen3-multi-node configured
```

[Instance restart policy](../../concepts/omenative/instance-restart-policy.md) and [OMENative update strategies](../../concepts/architecture/omenative-update-strategies.md) explain the other policies and strategies.

## Troubleshooting

### Pods stay Pending

`kubectl describe pod` on a `Pending` pod shows `FailedScheduling` events that say what the pod lacks, such as a free GPU or a node where the model is `Ready`. If the pod has no events at all, check that the scheduler in its `spec.schedulerName` is running.

If all the Instance's pods stay `Pending` under a gang-aware scheduler, the whole gang doesn't fit yet. Free GPUs for the leader and all its workers on nodes where the model is `Ready`. The OME scheduler also needs a topology key, as [Bring a gang-aware scheduler](../../concepts/serving/gang-scheduling.md#bring-a-gang-aware-scheduler) explains.

If some pods run while others stay `Pending`, the running pods keep their GPUs while they wait. There are two causes:

| Cause | Sign | Fix |
| --- | --- | --- |
| The PodGroup CRD is missing, so OME creates no PodGroup. | The engine's `GangSchedulingUnavailable` condition has the reason `PodGroupCRDNotInstalled`. | Install the CRD and restart the controller, as [Install the PodGroup CRD](../../concepts/serving/gang-scheduling.md#when-the-podgroup-crd-is-missing) shows. |
| The pods use the default scheduler, which ignores PodGroups. | A `MaybeNoGangScheduler` Warning event on the InferenceService. | Name a gang-aware scheduler in the engine's `schedulerName` or the runtime's `spec.schedulerName`. |

Read the condition's reason:

```bash
kubectl get inferenceservice qwen3-multi-node -n qwen3-multi-node \
  -o jsonpath='{.status.components.engine.lifecycle.conditions[?(@.type=="GangSchedulingUnavailable")].reason}'
```

```output
PodGroupCRDNotInstalled
```

Read the event's message:

```bash
kubectl get events -n qwen3-multi-node --field-selector reason=MaybeNoGangScheduler \
  -o jsonpath='{range .items[*]}{.message}{"\n"}{end}'
```

```output
OMENative component=engine created scheduler-plugins PodGroup objects but pod template's spec.schedulerName is "" (default kube-scheduler does not enforce PodGroup gang); set runtime.spec.schedulerName to a gang-aware scheduler (e.g. scheduler-plugins-scheduler) or install scheduler-plugins as a plugin in the default scheduler
```

If you added the Coscheduling plugin to your default scheduler, the warning is a false positive.

With the chart's settings, a pod that stays unschedulable for 15 minutes makes OME fail the Instance, with an `InstanceFailed` Warning event that carries the scheduler's message.

### Workers can't reach the leader

The pods run, but the leader never becomes ready, and the engine's logs show that it can't connect to the other ranks. Read them with `kubectl logs -n qwen3-multi-node qwen3-multi-node-engine-0-worker-0`, or with the leader's pod name. Check the leader address that the worker connects to:

```bash
kubectl get pod qwen3-multi-node-engine-0-worker-0 -n qwen3-multi-node \
  -o jsonpath='{.spec.containers[0].env[?(@.name=="OME_LEADER_ADDRESS")].value}'
```

```output
qwen3-multi-node-engine-0-leader-0.qwen3-multi-node-engine-headless
```

Then check these causes:

- The runtime still passes `LWS_*` variables, which reach the engine as literal text. [Make an OMENative copy](move-from-leaderworkerset.md#step-2-make-an-omenative-copy-of-the-runtime) of the runtime.
- The pods run with `hostNetwork: true` but without `dnsPolicy: ClusterFirstWithHostNet`, so they use the node's DNS, which can't resolve the leader's name. The catalog's multi-node runtimes set both.
- A NetworkPolicy blocks traffic between the pods. Allow it between the pods with the label `ome.io/inferenceservice=qwen3-multi-node`.

### The whole Instance keeps restarting

The Instance's pods keep terminating together and coming back. Each time OME recreates the Instance, it records a `RestartTriggered` Warning event on the InferenceService that names the cause. Read the causes:

```bash
kubectl get events -n qwen3-multi-node --field-selector reason=RestartTriggered \
  -o jsonpath='{range .items[*]}{.message}{"\n"}{end}'
```

For a worker whose container ran out of memory, the output is:

```output
OMENative component=engine instance=0 restart triggered: pod qwen3-multi-node-engine-0-worker-0 container ome-container restarted after Ready: OOMKilled (exit 137) (incarnation=2)
```

| Cause | Fix |
| --- | --- |
| `OOMKilled` | The container used more memory than its limit. Raise `resources.limits.memory` in the runtime. |
| A failed liveness probe | Loosen the probe, or leave probes off the workers, as the runtime in Step 1 does. |
| `pod count 1 below desired 2`, or `gang member lost: 1 of 2 pods present` | A pod went missing, for example because someone deleted it or its node went away. |

[Observe a restart](../../concepts/omenative/instance-restart-policy.md#observe-a-restart) shows the other restart events, and how to read an Instance's last failure.

## Clean up

Delete the InferenceService, its namespace and the runtime:

```bash
kubectl delete inferenceservice qwen3-multi-node -n qwen3-multi-node
kubectl delete namespace qwen3-multi-node
kubectl delete clusterservingruntime srt-qwen3-0-6b-multi-node
```

```output
inferenceservice.ome.io "qwen3-multi-node" deleted
namespace "qwen3-multi-node" deleted
clusterservingruntime.ome.io "srt-qwen3-0-6b-multi-node" deleted
```

The ClusterBaseModel `qwen3-0-6b` stays. [Serve your first model](../../getting-started/serve-your-first-model.md) shows how to delete it.

## Next steps

- [Move from LeaderWorkerSet to OMENative](move-from-leaderworkerset.md): move an engine that runs as a LeaderWorkerSet onto OMENative.
- [Use the OME scheduler](../operate-ome/ome-scheduler.md): place each Instance's leader and workers in one accelerator domain. It's alpha and needs Kubernetes 1.35.
- [Gang scheduling](../../concepts/serving/gang-scheduling.md): schedule the pods of each Instance together.
- [Set Instance readiness deadlines](set-instance-readiness-deadlines.md): decide when a slow or stuck Instance fails.
