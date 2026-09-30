---
title: Pre-configured models and runtimes
description: "Install the ome-serving Helm chart to deploy ready-made ClusterBaseModels, SGLang runtimes and InferenceServices from OME's model catalog."
---

The `ome-serving` Helm chart deploys models from OME's catalog, a set of ready-made model and runtime settings that ships in the chart. For each model that you turn on in a values file, it creates:

- a [ClusterBaseModel](../concepts/models/base-models.md) named after the model's catalog key, such as `qwen2-5-3b-instruct`, which the [model agent](../guides/operate-ome/model-agent.md) downloads from Hugging Face
- a [ClusterServingRuntime](../concepts/runtimes/serving-runtimes.md) named `srt-` followed by the key, which serves the model with SGLang
- a namespace named after the key, with an [InferenceService](../concepts/serving/inference-services.md) of the same name. OME picks its runtime.

Here you deploy Qwen2.5-3B-Instruct and Phi-4-mini-instruct, and send each one a request. If another page sent you here for one model and its runtime, go to [Install a model without serving it](#install-a-model-without-serving-it).

<div class="prerequisites" markdown>

- OME, installed as in [Install OME](install.md), with the model agent on. Since v1.3, the agent is off by default, and the values file in [Step 3 of Install OME](install.md#step-3-install-ome) turns it on.
- `git`, Helm and `kubectl`, with the rights to create namespaces and cluster-scoped OME resources.
- Two free NVIDIA GPUs on amd64 nodes that Kubernetes schedules as `nvidia.com/gpu`. Each model's engine pod requests 1 GPU, 10 CPUs and 30 GiB of memory.
- Room for both models under `/mnt/data/models`, the model agent's default host directory, on the nodes that run the agent: they all download both.
- Nodes that can reach Hugging Face, for the models, and Docker Hub, for the SGLang image.
- No ClusterBaseModel, ClusterServingRuntime or namespace named after a model that you turn on, such as one that you applied from `config/models/`. See [Helm refuses to install over existing resources](#helm-refuses-to-install-over-existing-resources).

</div>

## Step 1: Get the chart

The chart ships in the OME repository. Clone the repository at the tag of the OME release that you run, so that the chart matches your OME. To find your release, run `helm list -n ome`: `ome-resources-1.2.2` in the `CHART` column is v1.2.2. Run the rest of the commands from the clone's root directory:

```bash
git clone --depth 1 --branch v1.2.2 https://github.com/ome-projects/ome.git
cd ome
```

If you installed OME from source, `helm list` shows `ome-resources-0.1.0`: use that checkout instead.

The chart's registry, `charts/ome-serving/templates/_helpers.tpl`, holds the runtime settings of the catalog models. The chart's InferenceServices leave the runtime to OME, which picks only runtimes that set `autoSelect: true`. Most registry entries set `false`. List the models whose entry sets `true`:

```bash
awk 'NR == FNR { if (/^[a-z0-9.-]+:$/) key = $1; if (/autoSelect: true/) auto[key] = 1; next }
     /^  [a-z0-9.-]+:$/ && auto[$1] { print substr($1, 1, length($1) - 1) }' \
  charts/ome-serving/templates/_helpers.tpl charts/ome-serving/values.yaml
```

The list includes `qwen2-5-3b-instruct` and `phi-4-mini-instruct`. To deploy a catalog model from outside the list, change `autoSelect` to `true` in the model's registry entry, in your clone. If the model has no entry, add one as in [Add a model outside the catalog](#add-a-model-outside-the-catalog).

## Step 2: Install the chart

Turn on the two models in a values file of your own. Save this one as `my-models.yaml`:

```yaml title="my-models.yaml"
models:
  qwen2-5-3b-instruct:
    enabled: true
    path: /mnt/data/models/Qwen/Qwen2.5-3B-Instruct
  phi-4-mini-instruct:
    enabled: true
    path: /mnt/data/models/microsoft/Phi-4-mini-instruct
```

Set a `path` for each model that you turn on, because the catalog sets none. It's the directory where the model agent puts the weights on the nodes, and the serving pods mount it. Keep it inside the agent's host directory, `/mnt/data/models` by default. Helm merges your file into the chart's `values.yaml`, so the models keep their other catalog settings.

Install the chart as the release `ome-serving`. `--namespace ome` sets only where Helm keeps the release's records: the models' resources go in their own namespaces or at cluster scope.

```bash
helm upgrade --install ome-serving ./charts/ome-serving \
  --namespace ome -f my-models.yaml
```

!!! warning "The first install fails"
    Helm marks the release as failed: until the model agent has read the models, the webhook rejects the InferenceServices with `model format name is required`. This is a known bug. Wait for the models in Step 3, then run the command again in Step 4, and leave out `--atomic` and `--rollback-on-failure`, which uninstall a failed release.

Check that the models and the runtimes exist:

```bash
kubectl get clusterbasemodel qwen2-5-3b-instruct phi-4-mini-instruct -o name
kubectl get clusterservingruntime srt-qwen2-5-3b-instruct srt-phi-4-mini-instruct -o name
```

```output
clusterbasemodel.ome.io/qwen2-5-3b-instruct
clusterbasemodel.ome.io/phi-4-mini-instruct
clusterservingruntime.ome.io/srt-qwen2-5-3b-instruct
clusterservingruntime.ome.io/srt-phi-4-mini-instruct
```

The model agent starts to download the models.

## Step 3: Wait for the models to become Ready

OME reports a model as `Ready` as soon as one node has it. Wait for both models:

```bash
kubectl wait clusterbasemodel/qwen2-5-3b-instruct clusterbasemodel/phi-4-mini-instruct \
  --for=jsonpath='{.status.state}'=Ready --timeout=30m
```

```output
clusterbasemodel.ome.io/qwen2-5-3b-instruct condition met
clusterbasemodel.ome.io/phi-4-mini-instruct condition met
```

If the command times out, see [The model doesn't become Ready](#the-model-doesnt-become-ready).

## Step 4: Create the InferenceServices

Run the same command again:

```bash
helm upgrade --install ome-serving ./charts/ome-serving \
  --namespace ome -f my-models.yaml
```

Helm creates the two InferenceServices, and reports the release's status as `deployed`. If Helm fails again, see [The webhook rejects the InferenceServices](#the-webhook-rejects-the-inferenceservices).

SGLang loads the models after their engine pods start. Wait for both InferenceServices to become ready:

```bash
kubectl wait --for=condition=Ready inferenceservice/qwen2-5-3b-instruct -n qwen2-5-3b-instruct --timeout=30m
kubectl wait --for=condition=Ready inferenceservice/phi-4-mini-instruct -n phi-4-mini-instruct --timeout=30m
```

```output
inferenceservice.ome.io/qwen2-5-3b-instruct condition met
inferenceservice.ome.io/phi-4-mini-instruct condition met
```

If one times out, see [The engine pods aren't ready](#the-engine-pods-arent-ready).

## Step 5: Send a request

The model has a ClusterIP Service named after it, so forward a local port to it, and leave the command running. If you turned on ingress creation, forward to `svc/qwen2-5-3b-instruct-engine` instead.

```bash
kubectl port-forward svc/qwen2-5-3b-instruct 8080:8080 -n qwen2-5-3b-instruct
```

```output
Forwarding from 127.0.0.1:8080 -> 8080
Forwarding from [::1]:8080 -> 8080
```

In a second terminal, send a chat completion request. `model` is the name that SGLang serves the model under, which the catalog sets to the model's Hugging Face repository:

```bash
curl -s http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model": "Qwen/Qwen2.5-3B-Instruct", "messages": [{"role": "user", "content": "What is Kubernetes? Answer in one sentence."}], "max_tokens": 100}'
```

The response is an OpenAI chat completion, with the model's answer in `choices[0].message.content`. To try Phi-4-mini-instruct, forward another local port, such as `8081:8080`, to `svc/phi-4-mini-instruct` in the namespace `phi-4-mini-instruct`. Then send the request to that port, with `"model": "microsoft/Phi-4-mini-instruct"`.

## Install a model without serving it

Examples on other pages use a catalog model, and sometimes its runtime, in InferenceServices of their own. For those, install only the model and its runtime: render their two templates, and apply them with kubectl.

Those examples use Llama models, such as `llama-3-2-3b-instruct`, which are gated on Hugging Face, so the model agent needs your access token. Put it in a Secret in `ome`, under the key `token`, as [Credentials](../concepts/models/base-models.md#credentials) describes:

```bash
kubectl create secret generic hf-token -n ome --from-literal=token="$HF_TOKEN"
```

```output
secret/hf-token created
```

Turn on the model in a values file, and name the Secret in `key`:

```yaml title="llama.yaml"
models:
  llama-3-2-3b-instruct:
    enabled: true
    path: /mnt/data/models/meta-llama/Llama-3.2-3B-Instruct
    key: hf-token
```

Render the ClusterBaseModel and the ClusterServingRuntime, and apply them:

```bash
helm template ome-serving ./charts/ome-serving -f llama.yaml \
  -s templates/clusterbasemodel.yaml -s templates/clusterservingruntime.yaml \
  | kubectl apply -f -
```

```output
clusterbasemodel.ome.io/llama-3-2-3b-instruct created
clusterservingruntime.ome.io/srt-llama-3-2-3b-instruct created
```

Wait for the model as in [Step 3](#step-3-wait-for-the-models-to-become-ready), with its name in the `kubectl wait` command, before you create an InferenceService that uses it.

The runtimes of the Llama models that those pages use set `autoSelect: false`, so OME uses them only for an InferenceService that names them, as most of the examples do. If yours leaves the runtime to OME, change `autoSelect` to `true` in the model's registry entry before you render the templates.

kubectl, not Helm, manages these resources. To delete them, pipe the same `helm template` command to `kubectl delete -f -`.

## Change a model's settings

Set a model's other values under its key in your values file, then run `helm upgrade --install` again. The values cover where the weights come from, GPUs, images, replicas and PD mode: see [ome-serving chart values](../reference/operate-ome/ome-serving-values.md).

## Add a model outside the catalog

To deploy a model from outside the catalog, add it to the registry in your clone, then to your values file. Its key names the model's resources: use a short name of lowercase letters, digits and hyphens that starts with a letter.

Add the model's entry at the end of the registry, just before the last line of `charts/ome-serving/templates/_helpers.tpl`, which is `{{- end }}`:

```yaml title="charts/ome-serving/templates/_helpers.tpl (excerpt)"
my-model-8b:
  architecture: LlamaForCausalLM
  transformersVersion: "4.46.0"
  autoSelect: true
  priority: 1
  sizeRange: ["7B", "9B"]
  servedName: example-org/my-model-8b
```

- `architecture` is the model's architecture, as in the `architectures` list of its `config.json`.
- `transformersVersion` is the `transformers_version` in its `config.json`. The chart's runtimes match only that exact version.
- `sizeRange` is a range that includes the model's number of parameters.
- `servedName` is the name that requests send as `model`.
- `autoSelect: true` lets OME pick the runtime for the chart's InferenceService, and `priority` ranks it against other runtimes that support the model.

The chart's runtimes match only unquantized models that the SGLang image supports. Then add the model to your values file:

```yaml title="my-models.yaml"
models:
  my-model-8b:
    enabled: true
    vendor: example-org
    capabilities: [TEXT_TO_TEXT]
    hfModelId: example-org/my-model-8b
    path: /mnt/data/models/example-org/my-model-8b
    runtime:
      gpus: 1
```

Keep the `runtime` key: the chart fails to render without one. Install the model as in Steps 2 to 4. If the webhook rejects its InferenceService, see [The webhook rejects the InferenceServices](#the-webhook-rejects-the-inferenceservices).

## Troubleshooting

### The webhook rejects the InferenceServices

Helm reports that the admission webhook `inferenceservice.ome-webhook-server.validator` denied the request, and marks the release as failed. It keeps what it created, so fix the cause and run `helm upgrade --install` again.

| The reason contains | What to do |
| --- | --- |
| `model format name is required`, or `referenced model "qwen2-5-3b-instruct" not found` | Wait for the model to become `Ready`, as in [Step 3](#step-3-wait-for-the-models-to-become-ready). With `createModel: false`, create the model first. |
| `no runtime found to support model` | No runtime with `autoSelect: true` matches. Fix the model's registry entry, or set its `autoSelect` to `true`; see [Read the exclusion reasons](../guides/deploy-models/troubleshoot-runtime-selection.md#step-3-read-the-exclusion-reasons). |

Since v1.3, `kubectl ome runtime explain --model qwen2-5-3b-instruct -n qwen2-5-3b-instruct` lists the runtimes that OME considered, and why they match or not. See [`kubectl ome runtime`](../reference/kubectl-ome/runtime.md). Until v1.3 is released, [build the plugin from `main`](../reference/kubectl-ome/overview.md#build-from-source).

### Helm refuses to install over existing resources

Helm stops, before it creates anything, when one of the chart's resources already exists outside the release. That happens with a model or runtime that you applied with kubectl, such as `qwen3-0-6b` and `srt-qwen3-0-6b` from [Serve your first model](serve-your-first-model.md). It also happens with a namespace named after a model's key. Delete the resource, or turn the model off. With Helm 4, or Helm 3.17 or newer, `--take-ownership` adopts the resource into the release and changes it to match the chart.

### The model doesn't become Ready

Check the model's label on the nodes:

```bash
kubectl get nodes -L models.ome.io/clusterbasemodel.qwen2-5-3b-instruct
```

| Value | What it means |
| --- | --- |
| `Updating` | The model agent is still downloading or reading the model. |
| `Failed` | The agent couldn't download or read the model. A gated model needs your Hugging Face token: see [Install a model without serving it](#install-a-model-without-serving-it). |
| None | No model agent runs on the node, or the model's `nodeSelector` leaves it out. Since v1.3, the agent is off by default: see [Step 3 of Install OME](install.md#step-3-install-ome). |

Read the model agent's logs with `kubectl logs -n ome -l app.kubernetes.io/component=ome-model-agent-daemonset --tail=100`. On v1.2.2, the agent also crashes and restarts on a model without a `path`. [The model doesn't become Ready](serve-your-first-model.md#the-model-doesnt-become-ready) in Serve your first model covers more causes.

### The engine pods aren't ready

Check the pods of the model's InferenceService with `kubectl get pods -n qwen2-5-3b-instruct`.

- A pod that stays `Pending` doesn't fit on any node; see [The engine pod stays Pending](serve-your-first-model.md#the-engine-pod-stays-pending). With `gpus` above 1, a node needs that many free GPUs.
- `ErrImagePull` or `ImagePullBackOff`: the node can't pull the SGLang image from Docker Hub, or the router's image from `fra.ocir.io`. Point `image` or `routerImage` at a copy in your own registry, as [Defaults](../reference/operate-ome/ome-serving-values.md#defaults) describes.
- SGLang can't find the model's files: check that the model has a `path` inside the model agent's host directory, as in [Step 2](#step-2-install-the-chart).
- The pod restarts before it's ready: the startup probe gives SGLang about 16 minutes to load the model. Read SGLang's logs with `kubectl logs -n qwen2-5-3b-instruct -l ome.io/inferenceservice=qwen2-5-3b-instruct --tail=100`.
- The pods run, but the InferenceService never becomes Ready: since v1.3, the chart's InferenceServices need the Prometheus Operator's PodMonitor CRD. Install it, then restart the controller, as [The PodMonitor CRD](install.md#the-podmonitor-crd) shows.

### Another runtime serves the model

OME picks from every runtime that supports the model and sets `autoSelect: true`. Among those, a ServingRuntime in the InferenceService's namespace wins over ClusterServingRuntimes. The engine pod's `serving-runtime` label names the runtime that OME picked:

```bash
kubectl get pods -n qwen2-5-3b-instruct -L serving-runtime
```

With the chart's runtime, the `SERVING-RUNTIME` column shows `srt-qwen2-5-3b-instruct`. The chart's InferenceServices can't name a runtime. To make OME pick the chart's, set `autoSelect: false` on the other runtime, or, if it's a ClusterServingRuntime, raise `priority` in the model's registry entry. Since v1.3, `kubectl ome runtime explain --isvc qwen2-5-3b-instruct -n qwen2-5-3b-instruct` shows how OME ranked the runtimes. See [Troubleshoot runtime selection](../guides/deploy-models/troubleshoot-runtime-selection.md).

## Clean up

Uninstall the release:

```bash
helm uninstall ome-serving --namespace ome
```

```output
release "ome-serving" uninstalled
```

Helm deletes the models' namespaces and everything in them, the runtimes and the models. A model is deleted once the model agent has removed its files from the nodes; see [Deleting a model](../concepts/models/base-models.md#deleting-a-model). OME, and any Secrets that you created in `ome`, stay.

To remove one model instead, set its `enabled` to `false` in your values file, and run `helm upgrade --install` again.

## Next steps

- [ome-serving chart values](../reference/operate-ome/ome-serving-values.md): every value that you can set for a model.
- [Serving runtimes](../concepts/runtimes/serving-runtimes.md): how a runtime describes the pods that serve a model, and how OME picks one.
- [Base models](../concepts/models/base-models.md): how OME downloads a model to the nodes, what it learns from the model's files, and how it tracks the model on the nodes.
- [Troubleshoot runtime selection](../guides/deploy-models/troubleshoot-runtime-selection.md): find out why OME can't pick a runtime for a model.
- [InferenceServices](../concepts/serving/inference-services.md#the-runtime): since v1.3, serve a model with a runtime that loads its own weights, with no base model or model agent. Until v1.3 is released, that needs [a build of `main`](install.md#install-from-source).
