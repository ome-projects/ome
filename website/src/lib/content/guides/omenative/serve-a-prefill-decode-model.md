---
title: Serve a prefill-decode model
description: Serve a model with prefill on the engine and decode on the decoder, each with its own InferenceReplica on OMENative, and scale prefill on its own.
since: v1.3
---

Run a model's prefill and decode phases on separate pods, so that you can size and scale each phase on its own. The engine runs prefill, the decoder runs decode, and a router sends each request's prefill to an engine pod and its decode to a decoder pod. Prefill-decode, or PD, is a shape rather than a [deployment mode](../../concepts/architecture/deployment-modes.md): any [InferenceService](../../concepts/serving/inference-services.md) with an `engine` and a `decoder` is PD. On [OMENative](../../concepts/omenative/overview.md), the engine and the decoder each get their own InferenceReplica, and they can roll out together as one [rollout group](../../concepts/rollouts-and-traffic/rollout-groups.md).

Here you serve `meta-llama/Llama-3.2-1B-Instruct` on OMENative with two prefill Instances and one decode Instance. Then you add a third prefill Instance while the decoder keeps running as it is.

<div class="prerequisites" markdown>

- OME v1.3 or later, installed with the `ome-resources` chart, and `kubectl`, with the rights to create namespaces, Secrets in `ome`, ClusterBaseModels, ClusterServingRuntimes and InferenceServices. See [Install OME](../../getting-started/install.md).
- The model agent, to download the weights to your nodes. It's off by default: turn it on with the chart values `modelAgent.enabled=true` and `modelAgent.hostPath=/raid/models`, as [Install OME](../../getting-started/install.md#step-3-install-ome) shows. Or put the weights on a PersistentVolumeClaim, as in Steps 1 to 3 of [Serve models from a PVC](../deploy-models/serve-models-from-pvc.md). Then create only the runtime in Step 1, skip the namespace in Step 2, and add `kind: BaseModel` under `spec.model`.
- For the model agent, a Hugging Face token with access to `meta-llama/Llama-3.2-1B-Instruct`, which is gated, in the environment variable `HF_TOKEN`.
- Three amd64 nodes with NVIDIA GPUs and RDMA NICs, and a fourth for Step 5. An engine or decoder pod takes a GPU, 10 CPUs, 30 GiB of memory and port 8080 on its node's network, so a node runs at most one of them. The runtime uses the RDMA device `mlx5_0` and OME's [`oci-roce` RDMA profile](../../reference/api/labels-and-annotations.md#pod-keys-you-set), and expects nodes set up for them.
- For the router, 1 CPU and 2 GiB of memory on a node without the `nvidia.com/gpu` taint.
- Optionally, the [kubectl ome](../../reference/kubectl-ome/overview.md) plugin, to inspect the Instances.

</div>

## Step 1: Create the model and the runtime

Check whether the ClusterBaseModel and the PD ClusterServingRuntime exist:

```bash
kubectl get clusterbasemodel llama-3-2-1b-instruct
kubectl get clusterservingruntime srt-llama-3-2-1b-instruct-pd
```

```output
Error from server (NotFound): clusterbasemodels.ome.io "llama-3-2-1b-instruct" not found
Error from server (NotFound): clusterservingruntimes.ome.io "srt-llama-3-2-1b-instruct-pd" not found
```

Create the ones that are missing. The model agent reads the model's token from the Secret `hf-token` in the `ome` namespace; see [Credentials](../../concepts/models/base-models.md#credentials). Create the Secret, unless it exists already:

```bash
kubectl create secret generic hf-token -n ome --from-literal=token="$HF_TOKEN"
```

```output
secret/hf-token created
```

From a clone of the OME repository, apply the model and the runtime:

```bash
kubectl apply -f config/models/meta/Llama-3.2-1B-Instruct.yaml
kubectl apply -f config/runtimes/srt/meta/llama-3-2-1b-instruct-pd-rt.yaml
```

```output
clusterbasemodel.ome.io/llama-3-2-1b-instruct created
clusterservingruntime.ome.io/srt-llama-3-2-1b-instruct-pd created
```

Wait for the model to become `Ready`, which it does once one node has it:

```bash
kubectl wait --for=jsonpath='{.status.state}'=Ready clusterbasemodel/llama-3-2-1b-instruct --timeout=30m
```

```output
clusterbasemodel.ome.io/llama-3-2-1b-instruct condition met
```

OME runs the engine and decoder pods only on nodes that have the model. Before you go on, check that `kubectl get nodes -l models.ome.io/clusterbasemodel.llama-3-2-1b-instruct=Ready -o name` lists at least three nodes, or four for Step 5.

## Step 2: Create the InferenceService

Create a namespace for the InferenceService:

```bash
kubectl create namespace llama-demo
```

```output
namespace/llama-demo created
```

The engine and decoder pods are privileged and use their node's network, so the namespace needs the `privileged` Pod Security level. If the cluster's default level is stricter, label the namespace `pod-security.kubernetes.io/enforce=privileged`.

Save this InferenceService as `llama-pd.yaml`:

```yaml title="llama-pd.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-pd
  namespace: llama-demo
spec:
  deploymentMode: OMENative
  model:
    name: llama-3-2-1b-instruct
  runtime:
    name: srt-llama-3-2-1b-instruct-pd
  engine:
    minReplicas: 2
    maxReplicas: 2
  decoder:
    minReplicas: 1
    maxReplicas: 1
  router:
    minReplicas: 1
    maxReplicas: 1
```

- `engine` and `decoder` make the InferenceService PD. The runtime starts SGLang in the engine with `--disaggregation-mode prefill`, and in the decoder with `--disaggregation-mode decode`.
- `deploymentMode: OMENative` runs all three components on OMENative: the engine, the decoder and the router.
- `minReplicas` and `maxReplicas` count Instances, and each component has its own. Prefill gets two Instances, twice as many as decode.
- An InferenceService names a model, a runtime or both. This one names the model because the runtime loads the weights from the model's path.

Create the InferenceService:

```bash
kubectl apply -f llama-pd.yaml
```

```output
Warning: runtime "srt-llama-3-2-1b-instruct-pd" does not declare support for model "llama-3-2-1b-instruct" (runtime srt-llama-3-2-1b-instruct-pd does not support model safetensors: model format 'mt:safetensors:1.0.0:LlamaForCausalLM:transformers:4.45.0.dev0' not in supported formats: framework version mismatch (model=4.45.0.dev0, runtime=4.43.0)); proceeding because the runtime was named explicitly
inferenceservice.ome.io/llama-pd created
```

The InferenceService is created despite the warning. The runtime declares Transformers `4.43.0`, and OME recorded `4.45.0.dev0` from the model's `config.json`. Because the InferenceService names the runtime, OME uses it anyway; see [kubectl prints a `does not declare support` warning](../deploy-models/reference-a-runtime-explicitly.md#what-ome-checks-when-you-apply).

With ingress creation off, the default, the InferenceService's `Ready` condition follows the engine alone, so wait for `DecoderReady` and `RouterReady` too; see [Status](../../concepts/serving/inference-services.md#status). Pulling the images and loading the model can take several minutes:

```bash
kubectl wait --for=condition=Ready inferenceservice/llama-pd -n llama-demo --timeout=30m
kubectl wait --for=condition=DecoderReady inferenceservice/llama-pd -n llama-demo --timeout=30m
kubectl wait --for=condition=RouterReady inferenceservice/llama-pd -n llama-demo --timeout=30m
```

```output
inferenceservice.ome.io/llama-pd condition met
inferenceservice.ome.io/llama-pd condition met
inferenceservice.ome.io/llama-pd condition met
```

## Step 3: Check the InferenceReplicas and the pods

OME creates one InferenceReplica per component, named `{isvc}-{component}`. List them:

```bash
kubectl get inferencereplicas -n llama-demo
```

```output
NAME               COMPONENT   DESIRED   CURRENT   READY   AVAILABLE   AGE
llama-pd-decoder   decoder     1         1         1       1           12m
llama-pd-engine    engine      2         2         2       2           12m
llama-pd-router    router      1         1         1       1           12m
```

An InferenceReplica runs one component's Instances, and its columns count them. OME writes the InferenceReplicas from the InferenceService, so make your changes there, and get or describe the InferenceReplicas to see what's happening.

List the pods, with their component and their `ome.io/serving` condition, which is `True` while a pod is in rotation:

```bash
kubectl get pods -n llama-demo -l ome.io/inferenceservice=llama-pd \
  -o 'custom-columns=NAME:.metadata.name,COMPONENT:.metadata.labels.component,SERVING:.status.conditions[?(@.type=="ome.io/serving")].status'
```

```output
NAME                           COMPONENT   SERVING
llama-pd-decoder-0-default-0   decoder     True
llama-pd-engine-0-default-0    engine      True
llama-pd-engine-1-default-0    engine      True
llama-pd-router-0-default-0    router      True
```

Here, an Instance is one serving replica, with one pod. OME names the pod `{isvc}-{component}-{index}-{runner}-{ordinal}`, where the runner of a single-pod Instance is `default`.

The router runs with `--pd-disaggregation` and finds the pods by their labels: `component=engine` for prefill and `component=decoder` for decode, together with `ome.io/inferenceservice=llama-pd`. To see the Instances' phases, run `kubectl ome instance list llama-pd -n llama-demo`. [Instances](../../concepts/omenative/instances.md) explains the phases.

## Step 4: Send a request

Forward a local port to the Service `llama-pd`, which sends requests to the router, and leave the command running. If you turned on ingress creation, forward to `svc/llama-pd-router` instead.

```bash
kubectl port-forward svc/llama-pd 8080:8080 -n llama-demo
```

```output
Forwarding from 127.0.0.1:8080 -> 8080
Forwarding from [::1]:8080 -> 8080
```

In a second terminal, send a chat completion request. The runtime serves the model under its Hugging Face name:

```bash
curl -s http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model": "meta-llama/Llama-3.2-1B-Instruct", "messages": [{"role": "user", "content": "What is Kubernetes? Answer in one sentence."}], "max_tokens": 100}'
```

The response is an OpenAI chat completion, with the model's answer in `choices[0].message.content`. The router ran the request's prefill on one of the engine pods, and its decode on the decoder pod.

## Step 5: Scale prefill

Each component scales on its own. To add a prefill Instance, raise the engine's `minReplicas` and `maxReplicas` to 3 in the InferenceService:

```bash
kubectl patch inferenceservice llama-pd -n llama-demo --type merge \
  -p '{"spec":{"engine":{"minReplicas":3,"maxReplicas":3}}}'
```

```output
Warning: runtime "srt-llama-3-2-1b-instruct-pd" does not declare support for model "llama-3-2-1b-instruct" (runtime srt-llama-3-2-1b-instruct-pd does not support model safetensors: model format 'mt:safetensors:1.0.0:LlamaForCausalLM:transformers:4.45.0.dev0' not in supported formats: framework version mismatch (model=4.45.0.dev0, runtime=4.43.0)); proceeding because the runtime was named explicitly
inferenceservice.ome.io/llama-pd patched
```

kubectl prints the warning from Step 2 again. The engine's autoscaler, a HorizontalPodAutoscaler by default, raises the engine's InferenceReplica to the new minimum, and OME adds a third engine Instance; see [Scaling](../../concepts/serving/inference-services.md#scaling). Wait until it's ready:

```bash
kubectl wait --for=jsonpath='{.status.readyReplicas}'=3 inferencereplica/llama-pd-engine -n llama-demo --timeout=30m
```

```output
inferencereplica.ome.io/llama-pd-engine condition met
```

List the InferenceReplicas again:

```bash
kubectl get inferencereplicas -n llama-demo
```

```output
NAME               COMPONENT   DESIRED   CURRENT   READY   AVAILABLE   AGE
llama-pd-decoder   decoder     1         1         1       1           25m
llama-pd-engine    engine      3         3         3       3           25m
llama-pd-router    router      1         1         1       1           25m
```

The engine has three Instances, and the decoder still has one. OME added engine Instance 2, with the pod `llama-pd-engine-2-default-0` on a fourth node, and left the running pods alone. The router finds the new pod by its labels.

## Troubleshooting

### A pod stays Pending

Read the scheduler's message in the pod's events with `kubectl describe pod -n llama-demo -l ome.io/inferenceservice=llama-pd`:

- `node(s) didn't have free ports for the requested pod ports`: port 8080 is taken on every node that has the model. An engine or decoder pod holds it, or another pod on the node's network. Add a node, or free one. An update needs a spare node too, because by default OME starts an Instance's new pod before it drains the old one.
- `node(s) didn't match Pod's node affinity/selector`: too few nodes have the model. Check them as in Step 1.
- `node(s) had untolerated taint`, for the router pod: the router needs a node without the `nvidia.com/gpu` taint.
- `Insufficient nvidia.com/gpu`, `Insufficient cpu` or `Insufficient memory`: the nodes lack room for what the runtime requests.

### An engine or decoder pod fails

Read SGLang's logs with `kubectl logs -n llama-demo -l component=engine --tail=100`, or `-l component=decoder`. For an Instance's last failure, run `kubectl ome instance status llama-pd 0 --component engine -n llama-demo`. If your nodes' RDMA device isn't `mlx5_0`, change `--disaggregation-ib-device` in the runtime's `engineConfig` and `decoderConfig`, and apply the runtime again. OME rolls the change out to both components.

### The router can't pull its image

If your nodes can't reach the registry of the router's image, copy the image to a registry they can reach, and set it as `routerConfig.runner.image` in the runtime. See [Serving pods](../../getting-started/private-registries.md#serving-pods).

## Clean up

Delete the InferenceService:

```bash
kubectl delete inferenceservice llama-pd -n llama-demo
```

```output
inferenceservice.ome.io "llama-pd" deleted from llama-demo namespace
```

OME drains the Instances before it deletes their pods. If a pod stays `Terminating`, see [Recover stuck deletions](recover-stuck-deletions.md). The namespace, the model, the runtime and the Secret stay. If the namespace holds nothing else you need, such as a PVC, delete it with `kubectl delete namespace llama-demo`.

## Next steps

- [Rollout groups](../../concepts/rollouts-and-traffic/rollout-groups.md): roll the engine and the decoder out together, by canary, blue-green or rolling update.
- [Canary progression](../../reference/rollouts/canary-progression.md): the steps and gates of a canary.
- [Request an instance migration](../scale-and-migrate/request-an-instance-migration.md): move one prefill or decode Instance off its node.
- [Request a transient scale](../scale-and-migrate/request-a-transient-scale.md): ask once for a different Instance count, which the component's autoscaler or OME can soon overwrite.

The `kubectl ome` actions on these pages, such as `migration start` and `scale`, are alpha.
