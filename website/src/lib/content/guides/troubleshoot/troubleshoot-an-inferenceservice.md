---
title: Troubleshoot an InferenceService
description: Find out why an InferenceService isn't ready or serving by checking its status, model, runtime and logs, then look up the fix for what you see.
since: v1.3
---

When an [InferenceService](../../concepts/serving/inference-services.md) fails to become ready, serve requests or pick up a change, check its status, model, runtime and logs. Then look up what you see in [Troubleshooting](#troubleshooting).

The examples use the InferenceService `qwen2-5-7b` in the namespace `demo`. It serves the ClusterBaseModel `qwen2-5-7b` with a runtime that OME selected, and its engine runs on [OMENative](../../concepts/omenative/overview.md) as the InferenceReplica `qwen2-5-7b-engine` and its [Instances](../../concepts/omenative/instances.md). OME runs in the namespace `ome`.

<div class="prerequisites" markdown>

- The kubectl-ome plugin, and a kubeconfig for the cluster. See [kubectl-ome overview and install](../../reference/kubectl-ome/overview.md).
- Read access to the InferenceService, its pods, pod logs, events and InferenceReplicas; to base models and serving runtimes, cluster-scoped ones included; and to pod logs in OME's namespace.
- If OME runs in another namespace, use it wherever a command names `ome`, and pass it to `kubectl ome status` and `kubectl ome instance status` with `--ome-namespace`.

</div>

## Symptoms and where to start

Unless kubectl rejected the InferenceService, start with [Step 1](#step-1-get-the-overall-status), whose report often points at the cause. Then find what you see:

| Symptom | Where to look |
| --- | --- |
| The admission webhook denied the request | [The webhook rejects the InferenceService](#the-webhook-rejects-the-inferenceservice) |
| The InferenceService has no pods | [Step 2](#step-2-check-the-model), [Step 3](#step-3-check-the-runtime), then [The InferenceService gets no pods](#the-inferenceservice-gets-no-pods) |
| Pods stay `Pending` | [Pods stay Pending](#pods-stay-pending) |
| Pods crash or keep restarting | [Step 4](#step-4-read-the-logs) |
| Pods run, but the InferenceService doesn't become Ready | [Pods run, but the InferenceService isn't Ready](#pods-run-but-the-inferenceservice-isnt-ready) |
| An InferenceService that uses fine-tuned weights doesn't become Ready | [When the InferenceService doesn't become ready](../../concepts/models/fine-tuned-weights.md#when-the-inferenceservice-doesnt-become-ready), on the Fine-tuned weights page |
| A change doesn't roll out | [A change doesn't roll out](#step-4-check-the-rollout) |
| The InferenceService runs too many or too few pods | [The number of pods is wrong](#the-number-of-pods-is-wrong) |
| The InferenceService has no URL | [The InferenceService has no URL](#the-inferenceservice-has-no-url) |
| Requests fail or don't arrive | [Requests fail](#requests-fail) |
| Some pods of an Instance run while others stay `Pending` | [Part of an Instance stays Pending](#part-of-an-instance-stays-pending) |
| An Instance is `Failed`, or never becomes ready | [Instances fail or never become ready](#instances-fail-or-never-become-ready) |
| Pods stay `Terminating` after you delete the InferenceService | [Pods stay Terminating after a delete](#pods-stay-terminating-after-a-delete) |

## Step 1: Get the overall status

`kubectl ome status` reports on the InferenceService, its pods and their Warning events. Just after you create the InferenceService, the report looks like this:

```bash
kubectl ome status qwen2-5-7b -n demo
```

```output
FIELD                VALUE
Name                 qwen2-5-7b
Namespace            demo
Ready                NotRecorded / Unavailable
Ready reason
Declared runtime
Model                qwen2-5-7b
Generation           1 observed=0; advisory Unverifiable
Pod observation      Reported count=0 truncated=false
Event observation    Reported count=0 truncated=false
engine               NotRecorded / Unavailable; Ready pods=0/0 restarts=0
Rollout              Unknown reported=Unknown
Rollout evidence     Reported / Unverifiable
Autoscaling          Unavailable / Unavailable parent status
Placement            NotConfigured / NotApplicable / NotRecorded
Traffic              Unavailable / Unavailable parent status
Runtime active       Unavailable / Unavailable AutoSelectionNotProbed
Accelerator          Unavailable / Unavailable AutoSelectionNotProbed
Full safe values     Use -o json or -o yaml
Rollout detail       kubectl ome rollout status NAME
Autoscale detail     kubectl ome autoscale status NAME
Placement detail     kubectl ome placement status NAME
Traffic detail       kubectl ome traffic status NAME
Runtime detail       kubectl ome runtime effective NAME
Accelerator detail   kubectl ome accelerator explain NAME
```

In a healthy report, `Ready` and each component row start with `True / Valid`, all pods are ready, and no `Issue` row appears. Otherwise, read these rows:

| Row | What it tells you |
| --- | --- |
| `Ready`, `Ready reason` | The InferenceService's `Ready` condition and its reason. If it stays `NotRecorded / Unavailable`, see [The InferenceService gets no pods](#the-inferenceservice-gets-no-pods). |
| `engine`, `decoder`, `router` | A component's `Ready` condition, ready and total pods, and container restarts. For restarts, see [Step 4](#step-4-read-the-logs). |
| `Runtime active`, `Accelerator` | Whether OME reports an active runtime and accelerator. `AutoSelectionNotProbed` means that OME selects the runtime and the report doesn't work out which one. [Step 3](#step-3-check-the-runtime) does. |
| `Warning Inference...`, `Warning Pod` | A recent Warning event on the InferenceService or a pod, and its reason. It can remain after a recovery. |
| `Issue` | Evidence that the report couldn't collect or trust. |

Add `-o wide` for the condition and event messages. The report leaves out Normal events and events on InferenceReplicas: list those with `kubectl get events`. [kubectl ome status](../../reference/kubectl-ome/status.md#output-fields) describes every row.

## Step 2: Check the model

Steps 2 and 3 apply when the InferenceService names a base model in `spec.model`. An InferenceService without one names its runtime in `spec.runtime`, and OME skips the model fetch and runtime selection: go to [Step 4](#step-4-read-the-logs).

Unless the model's weights come from a PVC, OME schedules the pods only onto nodes that hold the weights. Check the model's state and which nodes have it:

```bash
kubectl get clusterbasemodel qwen2-5-7b \
  -o 'custom-columns=STATE:.status.state,READY:.status.nodesReady[*],FAILED:.status.nodesFailed[*]'
```

```output
STATE   READY                   FAILED
Ready   gpu-node-1,gpu-node-2   <none>
```

For a BaseModel, run `kubectl get basemodel qwen2-5-7b -n demo` with the same columns. Then act on `STATE`:

- `Ready`: at least one node has the model. Other nodes may still be downloading it, so some pods can stay `Pending`.
- `In_Transit`: no node has the model yet. The model agent downloads it, and the `ome-resources` chart installs the agent only when you set `modelAgent.enabled: true`. See [Run the model agent](../operate-ome/model-agent.md).
- `Failed`: no node has the model. `FAILED` lists the nodes whose download failed. See [Model lifecycle](../../concepts/models/base-models.md#model-lifecycle).

A `pvc://` model needs no agent: a metadata Job sets its state. See [Serve models from a PVC](../deploy-models/serve-models-from-pvc.md#troubleshooting).

## Step 3: Check the runtime

[`kubectl ome runtime explain`](../../reference/kubectl-ome/runtime.md#explain) runs OME's runtime selection for the model and lists every runtime it considered:

```bash
kubectl ome runtime explain --isvc qwen2-5-7b -n demo
```

The first row with `Yes` in `COMPATIBLE` is the runtime that OME selects, and each `No` row gives its reason in `REASON`. When every row says `No`, OME leaves the serving workload as it is: read the reasons as [Troubleshoot runtime selection](../deploy-models/troubleshoot-runtime-selection.md#step-3-read-the-exclusion-reasons) explains.

The command finds the model whatever `spec.model.kind` says, so a `Yes` row doesn't prove that the kind is right. See [An InferenceService that serves a BaseModel gets no pods](#an-inferenceservice-that-serves-a-basemodel-gets-no-pods).

## Step 4: Read the logs

[`kubectl ome logs`](../../reference/kubectl-ome/logs.md) reads the main container, `ome-container`, of the InferenceService's pods. Read the engine's logs from the last 10 minutes:

```bash
kubectl ome logs qwen2-5-7b -n demo --component engine --since 10m
```

If the command fails with `no pods found for InferenceService "qwen2-5-7b" in namespace "demo"`, check the name and the namespace, then see [The InferenceService gets no pods](#the-inferenceservice-gets-no-pods).

For a restarted pod's previous run, use kubectl. List the engine's pods:

```bash
kubectl get pods -n demo -l ome.io/inferenceservice=qwen2-5-7b,component=engine
```

On OMENative, a pod's name includes its Instance, as in `qwen2-5-7b-engine-0-default-0` for Instance `0`. A Deployment's pods have names like `qwen2-5-7b-engine-6c9f8d7b5-x2kqp`. Read the previous run of `ome-container`:

```bash
kubectl logs qwen2-5-7b-engine-0-default-0 -n demo -c ome-container --previous
```

The log usually ends with the error that stopped the container. When a pod of a multi-pod Instance fails, OME recreates all the Instance's pods by default, and their logs are gone: see [Instance restart policy](../../concepts/omenative/instance-restart-policy.md). OME still records the Instance's last failure: `kubectl ome instance status qwen2-5-7b 0 --component engine -n demo -o wide` shows its pod, container, reason and exit code.

Some failures show only in the controller's logs. Read every replica, because only the leader acts, and pass `--tail=-1`, because a label selector limits kubectl to the last 10 lines per pod:

```bash
kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1 | grep qwen2-5-7b
```

## Troubleshooting

### The webhook rejects the InferenceService

kubectl prints `admission webhook "inferenceservice.ome-webhook-server.validator" denied the request:` and a message. The webhook checks every create and update, so it can reject an InferenceService that it admitted before, even for a label change. Find the first row that matches:

| The message includes | Cause and fix |
| --- | --- |
| `at least one of spec.model or spec.runtime must be set` | Set `spec.model`, or name the runtime in `spec.runtime`. |
| `referenced model "qwen2-5-7b" not found in namespace "demo"` | Check `spec.model.name`, or create the model as a BaseModel in `demo` or a ClusterBaseModel. |
| `is disabled` | The named model or runtime sets `spec.disabled: true`. Set it to `false`, or use another. |
| `model format name is required` | OME hasn't recorded the model's format. See [The message says the model format name is required](../deploy-models/troubleshoot-runtime-selection.md#the-message-says-the-model-format-name-is-required). |
| `no model cache provider is configured for sharded model loading` | The model sets `distribution: Sharded`, which can't be served in this release. See [Distribution](../../concepts/models/base-models.md#distribution). |
| `srt-qwen2-5-7b not found in namespace demo` | The runtime that the InferenceService names is missing. See [The runtime you named isn't found](../deploy-models/troubleshoot-runtime-selection.md#the-runtime-you-named-isnt-found). |
| `no supporting runtime found for model qwen2-5-7b` | No runtime supports the model. See [Troubleshoot runtime selection](../deploy-models/troubleshoot-runtime-selection.md). |

The warning `Runtime srt-qwen2-5-7b will be auto-selected for model qwen2-5-7b` is expected. A warning that includes `does not declare support for model` means that the runtime you named doesn't list the model's format: see [kubectl prints a `does not declare support` warning](../deploy-models/reference-a-runtime-explicitly.md#what-ome-checks-when-you-apply).

### The InferenceService gets no pods

`kubectl ome status` shows `Pod observation` as `Reported count=0 truncated=false`. Look for these causes in order, and fix the first that applies. The first three apply when the InferenceService names a base model.

1. `spec.model.kind` differs from the model's kind. See the next section.
2. A `ModelReconcileError` Warning event, such as `specified base model qwen2-5-7b is disabled`. Fix the model that it names.
3. A Normal `ModelNotReady` event, which the status report leaves out: the model is Sharded, which can't be served in this release. List these events with `kubectl get events -n demo --field-selector reason=ModelNotReady`. See [Distribution](../../concepts/models/base-models.md#distribution).
4. A `RuntimeNotFound` Warning event: no runtime supports the model. See [Troubleshoot runtime selection](../deploy-models/troubleshoot-runtime-selection.md). An InferenceService that was already serving keeps its pods.
5. An `AcceleratorClassError` Warning event, or the `BestFit` accelerator policy. See [The InferenceService has an AcceleratorClassError event](../deploy-models/select-accelerators.md#the-inferenceservice-has-an-acceleratorclasserror-event) and [BestFit doesn't create or update the workload](../deploy-models/select-accelerators.md#bestfit-doesnt-create-or-update-the-workload).
6. The InferenceService lacks an `engine` section, so OME creates no engine. Add one: a section that sets only `minReplicas` is enough.

Otherwise, read the controller's logs, as in [Step 4](#step-4-read-the-logs).

### An InferenceService that serves a BaseModel gets no pods

`spec.model.kind` defaults to `ClusterBaseModel`. Suppose `qwen2-5-7b` is a BaseModel in `demo`, and the InferenceService leaves out the kind. The webhook admits it, but OME leaves the serving workload as it is and records no event or condition. The status report shows `Ready` as `NotRecorded / Unavailable`, and the controller's logs show `Failed to get referenced ClusterBaseModel`. The reverse, `kind: BaseModel` for a ClusterBaseModel, logs `Failed to get referenced BaseModel`. Check the kind:

```bash
kubectl get inferenceservice qwen2-5-7b -n demo -o custom-columns=KIND:.spec.model.kind,MODEL:.spec.model.name
```

```output
KIND               MODEL
ClusterBaseModel   qwen2-5-7b
```

Set the kind to the model's kind, here `BaseModel`:

```bash
kubectl patch inferenceservice qwen2-5-7b -n demo --type=merge -p '{"spec":{"model":{"kind":"BaseModel"}}}'
```

```output
inferenceservice.ome.io/qwen2-5-7b patched
```

### Pods stay Pending

List the scheduler's events in the namespace:

```bash
kubectl get events -n demo --field-selector reason=FailedScheduling
```

Their messages point at one of these causes:

- The InferenceService names a base model, and not enough nodes have it. Unless the model is served from a PVC, OME schedules the pods only onto nodes labeled `models.ome.io/clusterbasemodel.qwen2-5-7b=Ready`, or `models.ome.io/demo.basemodel.qwen2-5-7b=Ready` for a BaseModel. List them:

    ```bash
    kubectl get nodes -l models.ome.io/clusterbasemodel.qwen2-5-7b=Ready -o name
    ```

    ```output
    node/gpu-node-1
    node/gpu-node-2
    ```

    An empty list means that no node has the model yet: go back to [Step 2](#step-2-check-the-model).

- The nodes lack free GPUs, CPU or memory for the pod's requests.
- The pods of a multi-pod Instance wait to be scheduled together: see [Gang scheduling](../../concepts/serving/gang-scheduling.md).

On OMENative, pods that stay unschedulable for too long fail their Instance: see [Instances fail or never become ready](#instances-fail-or-never-become-ready).

### Pods run, but the InferenceService isn't Ready

RawDeployment and `MultiNode` (deprecated) components need the Prometheus Operator's PodMonitor CRD. RawDeployment is the default for a component without a leader and workers. If the CRD is missing, their pods run, but the InferenceService never becomes Ready. OME records no event about it. Check for the CRD:

```bash
kubectl get crd podmonitors.monitoring.coreos.com
```

```output
Error from server (NotFound): customresourcedefinitions.apiextensions.k8s.io "podmonitors.monitoring.coreos.com" not found
```

Install it as in [The PodMonitor CRD](../../getting-started/install.md#the-podmonitor-crd), then restart the controller, which looks for the CRD only when it starts:

```bash
kubectl rollout restart deployment/ome-controller-manager -n ome
```

```output
deployment.apps/ome-controller-manager restarted
```

### A change doesn't roll out {#step-4-check-the-rollout}

`kubectl ome rollout explain` shows the rollout plan and what holds it:

```bash
kubectl ome rollout explain qwen2-5-7b -n demo
```

Read the `Effective` view, the plan that OME follows:

| Row | Healthy | Otherwise |
| --- | --- | --- |
| `READY` | Starts with `True/Pinned` or `True/NoActiveRun`. | `False` means that the rollout is parked, and the old revision keeps serving. The reason says why. |
| `DRIFT` | Starts with `False/InSync`. | `True/SpecNewerThanRun` or `True/PolicyNewerThanRun` means that your edit waits for the next rollout run. See [Repin a drifted rollout plan](../roll-out-changes/repin-a-drifted-rollout-plan.md). |
| `HOLD` | No row. | Each row names a hold. `GlobalPause` means that the rollout is paused: see [Pause and resume a rollout](../roll-out-changes/pause-and-resume-a-rollout.md). |

[kubectl ome rollout explain](../../reference/kubectl-ome/rollout.md#explain-output-fields) lists the other rows. Two other causes appear outside the plan:

- With `spec.runtime.autoSync: false`, the InferenceService pins its runtime, so runtime changes don't reach it. See [Runtime revisions and pinning](../../concepts/runtimes/runtime-revisions.md).
- A `RetryHeld` Warning event means that OME stopped retrying a failed update of an OMENative component. See [Release a held revision](../roll-out-changes/release-a-held-revision.md).

### The number of pods is wrong

`kubectl ome autoscale status` shows the autoscaling that the controller reports for each component:

```bash
kubectl ome autoscale status qwen2-5-7b -n demo
```

In a component's column, `CLASS` names the autoscaler, `TARGET-NAME` the object it scales, and `CURRENT` and `DESIRED` its replica counts. Add `--live-scaler` to also read the HorizontalPodAutoscaler or KEDA ScaledObject. [Component autoscaling](../../concepts/serving/component-autoscaling.md) explains where the settings come from, and [kubectl ome autoscale](../../reference/kubectl-ome/autoscale.md) describes every row.

### The InferenceService has no URL

If the InferenceService also lacks pods, start with [The InferenceService gets no pods](#the-inferenceservice-gets-no-pods). If its pods run, see [Pods run, but the InferenceService isn't Ready](#pods-run-but-the-inferenceservice-isnt-ready): a missing PodMonitor CRD also keeps OME from setting the URL.

The `ome-resources` chart turns off ingress creation by default. OME then points the URL at a Service named after the InferenceService, as in `http://qwen2-5-7b.demo.svc.cluster.local:8080`, and the `IngressReady` condition says so:

```bash
kubectl get inferenceservice qwen2-5-7b -n demo -o jsonpath='{.status.conditions[?(@.type=="IngressReady")].reason}'
```

```output
IngressDisabled
```

OME skips the Service and the URL for an InferenceService labeled `networking.knative.dev/visibility: cluster-local`, and for one without an engine or a router. To reach the service from outside the cluster, see [Expose a service without ingress](../networking/expose-without-ingress.md) or [Configure ingress](../networking/configure-ingress.md).

### Requests fail

Send the InferenceService a test request, as in [Step 7 of Serve your first model](../../getting-started/serve-your-first-model.md#step-7-send-a-request). If it fails, read the engine's logs, as in [Step 4](#step-4-read-the-logs).

When the InferenceService sets a traffic policy, a traffic annotation or a canary, `kubectl ome traffic explain` compares it with what the controller reports. Suppose `qwen2-5-7b` is exposed through Envoy Gateway and sets `spec.traffic.algorithm: RoundRobin`:

```bash
kubectl ome traffic explain qwen2-5-7b -n demo
```

```output
LAYER       STATE           VALUE           SOURCE
SUMMARY     Consistent      -               Computed/Current
INTENT      Declared        RoundRobin      Declared/Current
SUPPORT     Honored         -               Computed/Current
TRANSLATE   Computed        envoy-gateway   Computed/Current
REALIZE     Reported        r=1 e=0 w=0     Reported/Current
CHECK       Match           algorithm       Computed/Current
CHECK       Match           policy          Computed/Current
CHECK       NotApplicable   canary-weight   Computed/Current
```

`SUMMARY` is `Consistent` when the declared and reported policy agree, and [kubectl ome traffic](../../reference/kubectl-ome/traffic.md#explain-output-fields) explains `Mismatch` and `Unsupported`. The command reads only the InferenceService, so `Consistent` doesn't prove that requests reach the pods. See [Traffic policy](../../concepts/rollouts-and-traffic/traffic-policy.md).

### Part of an Instance stays Pending

OME gang-schedules a multi-pod Instance through a PodGroup, which needs the scheduler-plugins PodGroup CRD. If the CRD is missing, OME creates the pods anyway, so the scheduler can place some of them, and sets `GangSchedulingUnavailable` to `True` on the component. Check its reason:

```bash
kubectl get inferenceservice qwen2-5-7b -n demo -o jsonpath='{.status.components.engine.lifecycle.conditions[?(@.type=="GangSchedulingUnavailable")].reason}'
```

```output
PodGroupCRDNotInstalled
```

Install the CRD and restart the controller, as [Install the PodGroup CRD](../../concepts/serving/gang-scheduling.md#when-the-podgroup-crd-is-missing) shows. With the CRD in place, a `MaybeNoGangScheduler` Warning event means that the pods use the default scheduler, which ignores PodGroups unless you added the Coscheduling plugin. See [Bring a gang-aware scheduler](../../concepts/serving/gang-scheduling.md#bring-a-gang-aware-scheduler).

### Instances fail or never become ready

When OME moves an Instance to the `Failed` phase, it records an `InstanceFailed` Warning event on the InferenceService. List these events:

```bash
kubectl get events -n demo --field-selector reason=InstanceFailed
```

The message names the Instance and its pod, and ends with one of these causes:

| The cause starts with | What happened | What to do |
| --- | --- | --- |
| A kubelet reason, such as `CrashLoopBackOff` or `ImagePullBackOff` | A container stayed waiting for longer than the stuck-pod grace period, `60s` with the `ome-resources` chart. | Read the logs, as in [Step 4](#step-4-read-the-logs). |
| `Unschedulable:` | The pods stayed unschedulable for longer than the unschedulable grace period, `15m` with the chart. | See [Pods stay Pending](#pods-stay-pending). |
| `DeadlineExceeded:` | The Instance wasn't ready by its readiness deadline, `30m` by default. Waiting for the scheduler, a scheduling gate or quota doesn't count. | Read the logs. If the model needs longer to load, raise the deadline: see [Set Instance readiness deadlines](../omenative/set-instance-readiness-deadlines.md). |

To find the failed Instances and read their last failures, follow [Step 1 of Reset failed Instances](../omenative/reset-failed-instances.md#step-1-find-the-parked-instances).

Fix the cause. A fix to the pod template, such as a new image, rolls out to every Instance, failed ones included. After any other fix, [release the held revision](../roll-out-changes/release-a-held-revision.md) if the InferenceService has a `RetryHeld` Warning event, or else [reset the failed Instances](../omenative/reset-failed-instances.md).

When an Instance stays unready but never fails, check the engine's deadline:

```bash
kubectl get inferenceservice qwen2-5-7b -n demo -o jsonpath='{.status.components.engine.lifecycle.conditions[?(@.type=="InstanceReadyTimeoutUnconfigured")].message}'
```

```output
Instance readiness deadline is 30m0s
```

If the message starts with `no instanceReadyTimeout is set`, OME waits for you instead of failing the Instance. [The Instance is stuck and never fails](../omenative/set-instance-readiness-deadlines.md#the-instance-is-stuck-and-never-fails) covers this and the other causes.

### Pods stay Terminating after a delete

Deleting an InferenceService deletes its InferenceReplicas, and each keeps the finalizer `ome.io/ir-teardown` until its pods and PodGroups are gone. A pod on a node that stopped responding holds that up. OME records `TeardownBlocked` or `TeardownDeadlineExceeded` events on the InferenceReplica, not the InferenceService:

```bash
kubectl get events -n demo --field-selector involvedObject.kind=InferenceReplica,involvedObject.name=qwen2-5-7b-engine
```

Both messages end with a `kubectl patch` command that removes the finalizer. Read [Recover stuck deletions](../omenative/recover-stuck-deletions.md) before you run it.

## Next steps

- [kubectl ome status](../../reference/kubectl-ome/status.md): every row of the status report.
- [Troubleshoot runtime selection](../deploy-models/troubleshoot-runtime-selection.md): when no runtime matches the model.
- [Observe OMENative in status](../../concepts/architecture/deployment-modes.md#observe-omenative-in-status): the status that OMENative components report.
