---
title: Recover stuck deletions
description: Recover OMENative pods stuck Terminating on dead nodes with the opt-in forceDelete escalation, and bound how long a deleting InferenceReplica holds its teardown finalizer.
since: v1.3
---

When a node dies, its pods can stay `Terminating`, and stall any scale-down, update, migration or teardown of an [OMENative](../../concepts/omenative/overview.md) component that waits for them. Turn on the force-delete escalation, and OME deletes such a pod once the cluster shows that its node is dead. The teardown deadline, `30m` in the chart, lets a deleted InferenceService's [InferenceReplicas](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-InferenceReplica) go even when pods remain. If a pod is stuck now, go to [Manual recovery](#manual-recovery).

<div class="prerequisites" markdown>

- OME v1.3 installed with the `ome-crd` and `ome-resources` charts, as the releases `ome-crd` and `ome` in the namespace `ome`. See [Install OME](../../getting-started/install.md).
- Helm, and `kubectl` access with permission to read nodes, events and ConfigMaps. [Manual recovery](#manual-recovery) also needs permission to delete pods and patch InferenceReplicas.
- The values file that you installed `ome-resources` with. If you don't have it, `helm get values ome -n ome` prints the values that you set.
- An OMENative InferenceService. The examples use `qwen3-0-6b` in the namespace `qwen3-native`, from [Serve a model on OMENative](serve-a-model-on-omenative.md). OME writes the InferenceReplica `qwen3-0-6b-engine` for its engine.
- `jq`, to read the JSON that OME keeps in ConfigMaps.

</div>

## How OME recovers a stuck pod {#when-it-fires}

OMENative gives each pod a [stable name](../../concepts/omenative/instances.md#what-an-instance-is), and reuses it when it recreates the pod. So while an old pod stays `Terminating`, the recreated pod waits.

With the escalation on, OME force-deletes a `Terminating` pod when all three hold:

1. It's more than `overdueSlack` past its `deletionTimestamp`, which already includes its grace period.
2. It has no finalizers.
3. Its node is dead, by one of these checks:

| Evidence | The node |
| --- | --- |
| `node-gone` | Its Node object is gone |
| `node-unreachable-taint` | Has had the `node.kubernetes.io/unreachable` taint for at least `nodeUnreachableThreshold` |
| `node-not-ready` | Has had its `Ready` condition `False` or `Unknown` for at least `nodeUnreachableThreshold` |

A node whose `Ready` condition is `True` never counts as dead, whatever its taints. OME looks for such pods when it scales a component down or tears it down, and when it updates or [migrates](../../concepts/omenative/migration-and-transient-scale.md) an [Instance](../../concepts/omenative/instances.md). Once the pod is gone, the work that waited for it goes on.

## Step 1: Turn on the force-delete escalation

These are operator settings for every OMENative component in the cluster, not fields of a component's `lifecycle` block. The chart writes them from `ome.controller.lifecycle` into the `lifecycle` key of the `inferenceservice-config` ConfigMap in `ome`:

| Value under `ome.controller.lifecycle` | Chart default | What it sets |
| --- | --- | --- |
| `forceDelete.overdueSlack` | Not set | How far past its `deletionTimestamp` a pod must be |
| `forceDelete.nodeUnreachableThreshold` | Not set | How long the pod's node must have looked dead |
| `teardown.deadline` | `30m` | How long a deleted InferenceReplica waits for its pods. See [Change the teardown deadline](#change-the-teardown-deadline) |

The escalation is on when both `forceDelete` durations are set and valid. Add them to your values file:

```yaml title="values.yaml"
ome:
  controller:
    lifecycle:
      forceDelete:
        overdueSlack: 2m
        nodeUnreachableThreshold: 5m
```

Give each duration a unit: a bare number, like `120`, stops the new manager pods from starting (see [The controller doesn't start after the upgrade](#the-controller-doesnt-start-after-the-upgrade)). To turn the escalation off, remove the block and upgrade.

!!! warning "A force-deleted pod can keep running"
    Force deletion removes only the pod object. On a node that's cut off rather than dead, the containers run until the kubelet reconnects and stops them, while OME runs a replacement with the same name elsewhere. Set `nodeUnreachableThreshold` longer than the outages that your nodes recover from.

If you installed from the `config/` manifests, add the block as JSON to the ConfigMap's `lifecycle` key instead, with `kubectl edit configmap inferenceservice-config -n ome`. [OME picks up the edit](../operate-ome/configure-the-controller.md#tune-the-config-cache) without a restart. OME ignores the whole key while it holds invalid JSON, and new manager pods exit until you fix it.

On a chart install, keep your other values in the file, since `helm upgrade` resets any that are missing. Upgrade with the chart version that you run, from the `CHART` column of `helm list -n ome`:

```bash
helm upgrade --install ome oci://ghcr.io/moirai-internal/charts/ome-resources \
  --namespace ome --version 1.3.0 -f values.yaml
```

Helm reports that the release `ome` has been upgraded, and the manager pods restart. Wait for the rollout:

```bash
kubectl rollout status deployment ome-controller-manager -n ome
```

```output
deployment "ome-controller-manager" successfully rolled out
```

Check the block that OME reads:

```bash
kubectl get configmap inferenceservice-config -n ome -o jsonpath='{.data.lifecycle}' | jq .forceDelete
```

```output
{
  "nodeUnreachableThreshold": "5m",
  "overdueSlack": "2m"
}
```

## Step 2: Check the recovery

When OME force-deletes a pod, it records a `PodForceDeleted` Warning event on the InferenceService, or on the InferenceReplica once the InferenceService is gone:

```bash
kubectl get events -n qwen3-native --field-selector reason=PodForceDeleted \
  -o jsonpath='{range .items[*]}{.message}{"\n"}{end}'
```

```output
OMENative component=engine instance=0: force-deleted stuck-Terminating pod qwen3-0-6b-engine-0-default-0 on node gpu-node-3 (evidence=node-unreachable-taint, 2m41s past the pod's own deletion deadline)
```

Then wait for the InferenceReplica to be `Ready`:

```bash
kubectl wait --for=condition=Ready inferencereplica/qwen3-0-6b-engine -n qwen3-native --timeout=15m
```

```output
inferencereplica.ome.io/qwen3-0-6b-engine condition met
```

??? note "Find force-deletions after their events expire"
    Events expire after an hour by default. While the InferenceService exists, OME also records each force-deletion in its audit ConfigMap, `qwen3-0-6b-ome-migration-audit`:

    ```bash
    kubectl get configmap qwen3-0-6b-ome-migration-audit -n qwen3-native \
      -o jsonpath='{.data.history\.json}' | jq '.entries[] | select(.outcome == "force-delete-unreachable")'
    ```

    ```output
    {
      "requestUUID": "0b6f4f59-3d0e-4c8a-9a52-6f1d2c7e8a41",
      "component": "engine",
      "sourceInstance": 0,
      "phase": "Completed",
      "reason": "ForceDelete",
      "fromNode": "gpu-node-3",
      "startedAt": "2026-09-28T10:14:03Z",
      "completedAt": "2026-09-28T10:14:03Z",
      "outcome": "force-delete-unreachable"
    }
    ```

## Change the teardown deadline

When you delete an OMENative InferenceService, Kubernetes deletes its InferenceReplicas too. Each keeps the finalizer `ome.io/ir-teardown` until OME has drained and deleted the component's pods and [PodGroups](../../concepts/serving/gang-scheduling.md).

`teardown.deadline` caps that wait, counted from the InferenceReplica's delete. At the deadline, OME removes the finalizer, and Kubernetes deletes what's left in the background. A pod on a dead node can then stay `Terminating` until you [force-delete it by hand](#manual-recovery).

Pick a deadline longer than your pods take to drain, plus their grace period, `overdueSlack` and `nodeUnreachableThreshold`, so that the escalation removes pods on dead nodes first. The chart's `30m` fits the default 30-second grace period and the values from Step 1. For a long grace period, like `terminationGracePeriodSeconds: 1800`, raise it:

```yaml title="values.yaml"
ome:
  controller:
    lifecycle:
      forceDelete:
        overdueSlack: 2m
        nodeUnreachableThreshold: 5m
      teardown:
        deadline: 1h
```

To hold the finalizer until the teardown is done, with no deadline, set `teardown: null` instead. Installs from the `config/` manifests set no deadline.

Upgrade the release and wait for the rollout, as in Step 1. Then check the block:

```bash
kubectl get configmap inferenceservice-config -n ome -o jsonpath='{.data.lifecycle}' | jq .teardown
```

```output
{
  "deadline": "1h"
}
```

When a teardown reaches the deadline, OME records a `TeardownDeadlineExceeded` Warning event on the InferenceReplica, with a count of the pods left:

```bash
kubectl get events -n qwen3-native --field-selector reason=TeardownDeadlineExceeded \
  -o jsonpath='{range .items[*]}{.message}{"\n"}{end}'
```

```output
teardown deadline 1h0m0s exceeded (1 owned pod(s) observed; selector: parent="qwen3-0-6b", component="engine"); releasing finalizer ome.io/ir-teardown to background GC; manual escape: kubectl patch inferencereplica qwen3-0-6b-engine -n qwen3-native --type=merge -p '{"metadata":{"finalizers":null}}'
```

## Manual recovery

Force-delete a stuck pod yourself when the escalation is off or doesn't act on it, or once the teardown deadline has passed. First make sure that the node is down for good: force-deleting a pod on a node that's only cut off leaves its containers running.

```bash
kubectl delete pod qwen3-0-6b-engine-0-default-0 -n qwen3-native --grace-period=0 --force
```

```output
Warning: Immediate deletion does not wait for confirmation that the running resource has been terminated. The resource may continue to run on the cluster indefinitely.
pod "qwen3-0-6b-engine-0-default-0" force deleted from qwen3-native namespace
```

A pod with finalizers stays until they're removed. Once the pod is gone, the work that waited for it goes on.

If a deleted InferenceReplica still stays, release its finalizer. OME stops cleaning up the component, and Kubernetes deletes what's left in the background:

```bash
kubectl patch inferencereplica qwen3-0-6b-engine -n qwen3-native --type=merge -p '{"metadata":{"finalizers":null}}'
```

```output
inferencereplica.ome.io/qwen3-0-6b-engine patched
```

Patch only an InferenceReplica that's being deleted: on any other, OME adds the finalizer back.

## Troubleshooting

### The controller doesn't start after the upgrade

`kubectl rollout status` doesn't finish, and the new manager pods exit with `Failed to initialize lifecycle scale configuration` in their logs. A duration in the `lifecycle` values is a bare number, like `120`, or, on a `config/` install, the key holds invalid JSON. Fix the value as in Step 1. Until then, the manager pods that were already running ignore the whole `lifecycle` key, the escalation and the teardown deadline included.

### A pod stays Terminating with the escalation on

List the component's pods with their nodes, deletion times and finalizers:

```bash
kubectl get pods -n qwen3-native -l ome.io/inferenceservice=qwen3-0-6b,component=engine \
  -o custom-columns=NAME:.metadata.name,NODE:.spec.nodeName,DELETION:.metadata.deletionTimestamp,FINALIZERS:.metadata.finalizers
```

```output
NAME                            NODE         DELETION               FINALIZERS
qwen3-0-6b-engine-0-default-0   gpu-node-3   2026-09-28T10:11:22Z   <none>
```

Read the node's `Ready` condition and when it last changed:

```bash
kubectl get node gpu-node-3 \
  -o jsonpath='{range .status.conditions[?(@.type=="Ready")]}{.status} since {.lastTransitionTime}{"\n"}{end}'
```

```output
Unknown since 2026-09-28T10:02:47Z
```

Then find the cause:

| Cause | What to do |
| --- | --- |
| Less than `overdueSlack` has passed since the time in `DELETION` | Wait |
| The pod has finalizers | See [A PodDeleteBlockedByFinalizer event](#a-poddeleteblockedbyfinalizer-event) |
| The node's `Ready` condition is `True` | Its kubelet is up: find out on the node why the containers don't stop |
| The node has been `False` or `Unknown` for less than `nodeUnreachableThreshold` | Wait |
| The `forceDelete` block isn't valid | Set both durations, above zero and with a unit. OME logs a bad block only at debug verbosity |
| `kubectl ome instance list` shows the Instance as `Restarting` | A restart waits for its old pods without the escalation. [Force-delete the pod by hand](#manual-recovery) |
| No scale-down, teardown, update or migration waits for the pod | [Force-delete the pod by hand](#manual-recovery) |

### A PodDeleteBlockedByFinalizer event

The pod is more than `overdueSlack` past its `deletionTimestamp`, but it has finalizers, which OME leaves to the controllers that own them. The event names them:

```bash
kubectl get events -n qwen3-native --field-selector reason=PodDeleteBlockedByFinalizer \
  -o jsonpath='{range .items[*]}{.message}{"\n"}{end}'
```

```output
OMENative component=engine instance=0: pod qwen3-0-6b-engine-0-default-0 is 3m5s past its own deletion deadline but pinned by finalizers [example.com/cleanup]; OME never strips another controller's finalizer — the finalizer owner must resolve it
```

Fix the controller that owns the finalizer. Remove the finalizer yourself only if its controller is gone for good, since its cleanup then never runs.

### An Instance waits with `NodeUnknown`

A pod that holds a name the Instance needs is in phase `Unknown`: its node stopped reporting, and its containers may still run. With the escalation on, OME force-deletes the pod once its node counts as dead, and the `PodForceDeleted` message says `held in phase Unknown`. A pod that's also `Terminating` follows [the usual rules](#when-it-fires) instead. Without the escalation, the Instance waits until the node comes back, or until you [force-delete the pod by hand](#manual-recovery).

### A deleting InferenceReplica has TeardownBlocked events

The teardown has no valid deadline, and pods or PodGroups remain. OME records this event while it waits, so it also shows during an ordinary teardown. List the messages:

```bash
kubectl get events -n qwen3-native --field-selector reason=TeardownBlocked \
  -o jsonpath='{range .items[*]}{.message}{"\n"}{end}'
```

```output
teardown blocked: 1 owned pod(s) and 0 owned PodGroup(s) remain (selector: parent="qwen3-0-6b", component="engine"); finalizer ome.io/ir-teardown holds until cleanup completes (no lifecycle.teardown.deadline configured); manual escape: kubectl patch inferencereplica qwen3-0-6b-engine -n qwen3-native --type=merge -p '{"metadata":{"finalizers":null}}'
```

If the part in parentheses says `lifecycle.teardown.deadline configured but invalid`, fix the deadline as in [Change the teardown deadline](#change-the-teardown-deadline). Otherwise, check the pods that remain as in [A pod stays Terminating with the escalation on](#a-pod-stays-terminating-with-the-escalation-on).

### DrainOverdue on the InferenceReplica

An Instance that OME is removing is still draining past its deadline: the start of the delete, plus the component's [`instanceReadyTimeout`](set-instance-readiness-deadlines.md). `DrainOverdue` is only a warning, and the drain goes on. The event names the pods that are still `Terminating` or draining:

```bash
kubectl get events -n qwen3-native --field-selector reason=DrainOverdue \
  -o jsonpath='{range .items[*]}{.message}{"\n"}{end}'
```

```output
OMENative component=engine instance=0: drain overdue by 4m12s (deadline 2026-09-28T10:44:03Z); 1 pod(s) still Terminating (qwen3-0-6b-engine-0-default-0)
```

For a pod that's still `Terminating`, check it as in [A pod stays Terminating with the escalation on](#a-pod-stays-terminating-with-the-escalation-on).

## Next steps

- [Set Instance readiness deadlines](set-instance-readiness-deadlines.md), which also set the drain deadlines behind `DrainOverdue`.
- [Reset failed Instances](reset-failed-instances.md).
- [Request an Instance migration](../scale-and-migrate/request-an-instance-migration.md), to move an Instance off a node before you take the node down.
- [Troubleshoot an InferenceService](../troubleshoot/troubleshoot-an-inferenceservice.md).
