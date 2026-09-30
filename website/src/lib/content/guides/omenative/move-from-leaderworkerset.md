---
title: Move from LeaderWorkerSet to OMENative
description: Move a multi-node InferenceService off the deprecated MultiNode mode, which runs on LeaderWorkerSet, onto OMENative, and delete the LeaderWorkerSet that OME leaves behind.
since: v1.3
---

Move a multi-node component off the deprecated `MultiNode` [deployment mode](../../concepts/architecture/deployment-modes.md), which runs it on a LeaderWorkerSet, and onto [OMENative](../../concepts/omenative/overview.md), OME's own way of running an inference component. OMENative runs each replica as an [Instance](../../concepts/omenative/instances.md): a leader pod and its workers, which OME creates, updates, repairs, moves and removes as one unit. On OMENative:

- [Updates](../../concepts/architecture/omenative-update-strategies.md) bring a new Instance up and serving before the old one drains, by default. [Rollout groups](../../concepts/rollouts-and-traffic/rollout-groups.md) can roll out prefill and decode together, by canary, blue-green or rolling update.
- A revision that keeps failing is [held](../roll-out-changes/release-a-held-revision.md) until you change the component or release the revision.
- When one of an Instance's pods fails, OME [rebuilds the whole Instance](../../concepts/omenative/instance-restart-policy.md), by default.
- You can [move an Instance off a node](../../concepts/omenative/migration-and-transient-scale.md), and OME starts its replacement first. The alpha [Alfred](../../concepts/scheduling/alfred.md) recommends moves and, when you let it, requests them. On `MultiNode`, it only advises, with the reason `LWSMigrationUnsupported`.
- The component reports [per-Instance status](../../concepts/architecture/deployment-modes.md#observe-omenative-in-status), and can have an autoscaler and a PodDisruptionBudget.
- [Gang scheduling](../../concepts/serving/gang-scheduling.md) works with the PodGroup CRD and a gang-aware scheduler, such as the alpha [OME scheduler](../../concepts/scheduling/ome-scheduler.md), which needs Kubernetes 1.35.

The steps move the `engine` of the [InferenceService](../../concepts/serving/inference-services.md) `deepseek-r1`, then delete its old LeaderWorkerSet.

!!! danger "Upgrading from v1.2.2 moves multi-node components to OMENative"
    When you upgrade from v1.2.2, each engine or decoder that runs on a LeaderWorkerSet moves to OMENative, unless it has an `ome.io/deploymentMode` annotation of its own. The annotation that v1.2.2 added to the InferenceService's `metadata.annotations` doesn't count. The component serves nothing until its new pods are ready, and nothing at all if its runtime uses `LWS_*` variables, as the catalog's `srt-deepseek-rdma` does. To move on your own schedule, [pin each component to `MultiNode`](#pin-a-component-to-multinode) before you upgrade.

<div class="prerequisites" markdown>

- OME v1.3, with the InferenceReplica controller enabled, as it is by default.
- A component that runs on a LeaderWorkerSet. The examples use the catalog sample `deepseek-r1`, in the namespace `deepseek-r1`, with its engine [pinned to `MultiNode`](#pin-a-component-to-multinode). Its runtime, the ClusterServingRuntime `srt-deepseek-rdma`, gives the engine one leader and one worker.
- Permission to create ClusterServingRuntimes, and to patch InferenceServices and delete LeaderWorkerSets in the InferenceService's namespace.
- A time when the component can stop serving, from Step 3 until its new pods are ready. If the move fails, you can [go back](#pin-a-component-to-multinode).

</div>

## What changes when a component moves

When a component's `ome.io/deploymentMode` annotation changes from `MultiNode` to `OMENative`, OME:

- Creates the [InferenceReplica](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-InferenceReplica) `deepseek-r1-engine`, which runs the component's Instances.
- Points the component's Service, `deepseek-r1-engine`, at the new pods at once, so the component serves nothing until they're ready.
- Leaves the LeaderWorkerSet `lws-deepseek-r1-engine` running until you delete it. Its pods keep their accelerators.
- Gives the component the ome-resources chart's default PodDisruptionBudget.

The new pods get none of LeaderWorkerSet's `LWS_*` variables or labels. If the runtime uses them, the pods can't find their leader, so Step 2 makes a copy of the runtime that uses OME's names.

## Step 1: Find the components that run on LeaderWorkerSet

List the LeaderWorkerSets that OME created, in every namespace:

```bash
kubectl get leaderworkersets --all-namespaces -l ome.io/inferenceservice \
  -o custom-columns='NAMESPACE:.metadata.namespace,NAME:.metadata.name,INFERENCESERVICE:.metadata.labels.ome\.io/inferenceservice,COMPONENT:.metadata.labels.component,RUNTIME:.metadata.labels.serving-runtime'
```

```output
NAMESPACE     NAME                     INFERENCESERVICE   COMPONENT   RUNTIME
deepseek-r1   lws-deepseek-r1-engine   deepseek-r1        engine      srt-deepseek-rdma
```

Do Step 2 once for each runtime in the list, and Steps 3 to 5 for each InferenceService. A component that also has an InferenceReplica, named `<InferenceService>-<component>`, has already moved: finish its move from Step 2. List the InferenceReplicas:

```bash
kubectl get inferencereplicas --all-namespaces
```

```output
No resources found
```

## Step 2: Make an OMENative copy of the runtime

Check whether the runtime from Step 1 uses LeaderWorkerSet's variables or labels. For a namespaced [serving runtime](../../concepts/runtimes/serving-runtimes.md), use `servingruntime` and `-n` with the InferenceService's namespace in this step's commands.

```bash
kubectl get clusterservingruntime srt-deepseek-rdma -o yaml \
  | grep -oE 'LWS_[A-Z_]+|leaderworkerset\.sigs\.k8s\.io/[a-z-]+' | LC_ALL=C sort -u
```

```output
LWS_GROUP_SIZE
LWS_LEADER_ADDRESS
LWS_WORKER_INDEX
leaderworkerset.sigs.k8s.io/worker-index
```

If the command prints nothing, the runtime works on OMENative as it is: go to Step 3, and leave `spec.runtime` out of the change.

Don't edit the runtime in place: by default, a runtime change reaches every InferenceService that uses it, including those still on a LeaderWorkerSet. Copy it to a file instead:

```bash
kubectl get clusterservingruntime srt-deepseek-rdma -o yaml > srt-deepseek-rdma-omenative.yaml
```

The command prints nothing. OMENative pods use OME's names, which [Pod labels and environment](../../concepts/architecture/deployment-modes.md#pod-labels-and-environment) lists:

| LeaderWorkerSet | OMENative |
| --- | --- |
| `$(LWS_LEADER_ADDRESS)` | `$(OME_LEADER_ADDRESS)` |
| `$(LWS_GROUP_SIZE)` | `$(OME_INSTANCE_POD_COUNT)` |
| `$(LWS_WORKER_INDEX)` | `$(OME_INSTANCE_POD_RANK)` |
| The label `leaderworkerset.sigs.k8s.io/worker-index=0`, to select leaders | The label `ome.io/runner=leader` |

OME sets its variables in the pod's containers only, so an init container can't use them. Edit `srt-deepseek-rdma-omenative.yaml`:

1. Replace `metadata` with a new name:

    ```yaml
    metadata:
      name: srt-deepseek-rdma-omenative
    ```

2. Delete `status`, if the file has one.
3. Make the replacements in the table. In `srt-deepseek-rdma`, the variables are in the leader and worker `command`, and the label is in the router's `--selector`. The catalog's PD runtimes, `srt-deepseek-rdma-pd` and `srt-kimi-k2-pd`, have the label in `--prefill-selector` and `--decode-selector`.
4. Delete any other `leaderworkerset.sigs.k8s.io` key, which OMENative ignores. In place of `exclusive-topology`, the component's `topologyKey` keeps each Instance's pods in one domain, which other pods can share.
5. Delete `ome.io/deploymentMode: MultiNode` from `engineConfig.annotations` and `decoderConfig.annotations`, if it's there.
6. Set `autoSelect: false` on each entry in `supportedModelFormats`, as `srt-deepseek-rdma` already does, so that OME picks the copy only for InferenceServices that name it.

Check that nothing from LeaderWorkerSet is left:

```bash
grep -c 'LWS_\|leaderworkerset' srt-deepseek-rdma-omenative.yaml
```

```output
0
```

Create the new runtime:

```bash
kubectl create -f srt-deepseek-rdma-omenative.yaml
```

```output
clusterservingruntime.ome.io/srt-deepseek-rdma-omenative created
```

## Step 3: Move the component to OMENative

Change three things in the InferenceService:

- `spec.runtime.name`: the runtime from Step 2.
- The `ome.io/deploymentMode` annotation in `spec.engine.annotations`: set it to `OMENative`, in place of the `MultiNode` pin.
- `spec.engine.leader` and `spec.engine.worker`: add them as empty blocks if they're missing. Empty blocks take their settings from the runtime, and make the Service select only the leaders.

Some InferenceServices need more in the same change:

| If the InferenceService | Also |
| --- | --- |
| Has a `decoder` | Set the decoder's annotation to `OMENative` too, and add its empty `leader` and `worker` blocks if they're missing. The webhook rejects the change when only one component is `OMENative`. |
| Sets `spec.runtime.autoSync: false` | Set its `ome.io/runtime-sync` annotation to a new value, so that it [rolls forward](../../concepts/runtimes/runtime-revisions.md#roll-forward-to-the-latest-runtime) to the new runtime. |
| Sets `spec.runtime.revision` | Delete it, with `null` in a merge patch. The webhook rejects a revision that belongs to another runtime. |

Use the tab for how you manage the InferenceService:

=== "Patch"

    ```bash
    kubectl patch inferenceservice deepseek-r1 -n deepseek-r1 --type merge \
      -p '{"spec":{"runtime":{"name":"srt-deepseek-rdma-omenative"},"engine":{"annotations":{"ome.io/deploymentMode":"OMENative"},"leader":{},"worker":{}}}}'
    ```

    ```output
    inferenceservice.ome.io/deepseek-r1 patched
    ```

=== "Manifest"

    ```yaml title="deepseek-r1.yaml"
    apiVersion: ome.io/v1beta1
    kind: InferenceService
    metadata:
      name: deepseek-r1
      namespace: deepseek-r1
    spec:
      model:
        name: deepseek-r1
      runtime:
        name: srt-deepseek-rdma-omenative
      engine:
        annotations:
          ome.io/deploymentMode: OMENative
        minReplicas: 1
        maxReplicas: 1
        leader: {}
        worker: {}
    ```

    ```bash
    kubectl apply -f deepseek-r1.yaml
    ```

    ```output
    inferenceservice.ome.io/deepseek-r1 configured
    ```

## Step 4: Check the new Instances

List the component's InferenceReplica. Its columns count Instances, not pods:

```bash
kubectl get inferencereplicas -n deepseek-r1
```

```output
NAME                 COMPONENT   DESIRED   CURRENT   READY   AVAILABLE   AGE
deepseek-r1-engine   engine      1         1         1       1           3m
```

List the component's pods. The LeaderWorkerSet's pods, which have no `ome.io/runner` label, still run next to the new ones:

```bash
kubectl get pods -n deepseek-r1 -l ome.io/inferenceservice=deepseek-r1,component=engine \
  -o custom-columns='NAME:.metadata.name,RUNNER:.metadata.labels.ome\.io/runner,PHASE:.status.phase'
```

```output
NAME                            RUNNER   PHASE
deepseek-r1-engine-0-leader-0   leader   Running
deepseek-r1-engine-0-worker-0   worker   Running
lws-deepseek-r1-engine-0        <none>   Running
lws-deepseek-r1-engine-0-1      <none>   Running
```

If the new pods stay `Pending` with a `FailedScheduling` event, the old pods hold the accelerators: do Step 5 now. Then wait for the InferenceService to be ready:

```bash
kubectl wait --for=condition=Ready inferenceservice/deepseek-r1 -n deepseek-r1 --timeout=60m
```

```output
inferenceservice.ome.io/deepseek-r1 condition met
```

## Step 5: Delete the old LeaderWorkerSet

Delete the LeaderWorkerSet:

```bash
kubectl delete leaderworkerset lws-deepseek-r1-engine -n deepseek-r1
```

```output
leaderworkerset.leaderworkerset.x-k8s.io "lws-deepseek-r1-engine" deleted from deepseek-r1 namespace
```

Kubernetes deletes its pods with it. Once they've terminated, none are left:

```bash
kubectl get pods -n deepseek-r1 -l leaderworkerset.sigs.k8s.io/name=lws-deepseek-r1-engine
```

```output
No resources found in deepseek-r1 namespace.
```

## Uninstall LeaderWorkerSet

Once no component runs on a LeaderWorkerSet, you can uninstall LeaderWorkerSet as [its docs](https://lws.sigs.k8s.io) describe. The manager checks for its CRD only when it starts, so restart the manager afterwards:

```bash
kubectl rollout restart deployment ome-controller-manager -n ome
```

```output
deployment.apps/ome-controller-manager restarted
```

## Pin a component to `MultiNode`

A component stays on its LeaderWorkerSet while its own `ome.io/deploymentMode` annotation says `MultiNode`, as [How OME resolves the mode](../../concepts/architecture/deployment-modes.md#how-ome-resolves-the-mode) explains. Because `MultiNode` is deprecated, a pin is a stopgap. Pin components before you upgrade from v1.2.2, or to go back after a move. To pin the engine of `deepseek-r1`:

```bash
kubectl patch inferenceservice deepseek-r1 -n deepseek-r1 --type merge \
  -p '{"spec":{"engine":{"annotations":{"ome.io/deploymentMode":"MultiNode"}}}}'
```

```output
inferenceservice.ome.io/deepseek-r1 patched
```

Pin a decoder the same way, under `decoder`. To pin the InferenceServices that use a runtime, add the annotation to the runtime's `engineConfig.annotations` or `decoderConfig.annotations`. This reaches only those with `spec.runtime.autoSync: true`, the default. If you keep InferenceServices in files, add the annotation there too.

On v1.2.2, the pin restarts the LeaderWorkerSet's pods, unless the InferenceService's `metadata.annotations` already has `ome.io/deploymentMode: MultiNode`. After the upgrade, the pods of each pinned LeaderWorkerSet restart once.

To go back after a move, pin the component and, if you changed it, set `spec.runtime.name` back to the old runtime, handling `autoSync: false` as in Step 3. OME points the Service back at the LeaderWorkerSet, which it creates again if you deleted it. If the component moved when you upgraded, the LeaderWorkerSet's pods restart once. The InferenceReplica's pods keep their accelerators until you delete it:

```bash
kubectl delete inferencereplica deepseek-r1-engine -n deepseek-r1
```

```output
inferencereplica.ome.io "deepseek-r1-engine" deleted from deepseek-r1 namespace
```

## Troubleshooting

### The new pods never become ready

The usual cause is a runtime that still passes `LWS_*` variables. Check which ones the leader pod uses:

```bash
kubectl get pod deepseek-r1-engine-0-leader-0 -n deepseek-r1 -o yaml \
  | grep -oE 'LWS_[A-Z_]+' | LC_ALL=C sort -u
```

```output
LWS_GROUP_SIZE
LWS_LEADER_ADDRESS
LWS_WORKER_INDEX
```

Point the component at a runtime that uses OME's names, as in Steps 2 and 3, or [go back to the LeaderWorkerSet](#pin-a-component-to-multinode).

### The Service sends requests to the workers

When only the runtime declares `worker`, the component's Service selects the workers too. The catalog's `srt-deepseek-rdma` gives its workers no readiness probe, so they get requests as soon as they start. Check the Service's selector:

```bash
kubectl get service deepseek-r1-engine -n deepseek-r1 -o jsonpath='{.spec.selector}{"\n"}'
```

```output
{"component":"engine","ome.io/inferenceservice":"deepseek-r1","ome.io/managed-by":"OMENative"}
```

If the selector has no `ome.io/runner`, add empty `leader` and `worker` blocks to `spec.engine`, as in Step 3. OME then adds `"ome.io/runner":"leader"` to it.

### The webhook rejects the change

kubectl prints `admission webhook "inferenceservice.ome-webhook-server.validator" denied the request:`, followed by this message:

```text
InvalidDeploymentModeCombination: engine deploymentMode="OMENative" does not match decoder deploymentMode="MultiNode"; when either is OMENative, both must use the same value
```

The webhook reads each component's mode from the InferenceService, not from the runtime. Give the engine and the decoder the same annotation, in one patch.

### The component still runs on the LeaderWorkerSet

After Step 3, `kubectl get inferencereplicas -n deepseek-r1` shows no InferenceReplica for the component. Check the component's annotation in the InferenceService:

```bash
kubectl get inferenceservice deepseek-r1 -n deepseek-r1 \
  -o jsonpath='{.spec.engine.annotations.ome\.io/deploymentMode}{"\n"}'
```

```output
MultiNode
```

| Output | Fix |
| --- | --- |
| `MultiNode` | Set it to `OMENative`, as in Step 3. |
| An empty line | The runtime's `engineConfig.annotations` says `MultiNode`. Set the InferenceService's annotation to `OMENative`, which wins over the runtime's. |
| `OMENative` | If `spec.runtime.autoSync` is `false`, OME still uses the old runtime. Set a new `ome.io/runtime-sync` value, as in Step 3. |

### A component moved to OMENative when you upgraded

The component had no `ome.io/deploymentMode` annotation of its own when you upgraded from v1.2.2, so it now runs on OMENative next to its old LeaderWorkerSet. Finish the move from Step 2, or [pin it to `MultiNode`](#pin-a-component-to-multinode) until you're ready.

## Next steps

- [Serve a multi-node model](serve-a-multi-node-model.md): run a new multi-node model on OMENative from the start.
- [OMENative and LeaderWorkerSet](../../concepts/omenative/overview.md#omenative-and-leaderworkerset): everything a component gains on OMENative.
- [OMENative update strategies](../../concepts/architecture/omenative-update-strategies.md): choose how OME replaces the Instances' pods when the component changes.
- [Let Alfred migrate Instances](../scheduling/let-alfred-migrate-instances.md): let the alpha Alfred move Instances off fragmented or unhealthy nodes.
