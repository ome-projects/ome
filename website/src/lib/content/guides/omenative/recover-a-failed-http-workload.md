---
title: Recover a failed HTTP workload
description: Break the CPU lab's readiness probe, inspect the failed attempt, and recover by publishing a corrected OMENative revision.
since: v1.3
---

Extend [Learn OMENative with an HTTP workload](learn-omenative.md) by deliberately pointing its readiness probe at the wrong port. The container still runs the same HTTP server, but the replacement cannot become ready. Inspect the failure, freeze the rollout, then correct the template and verify the new revision and response.

This is a controller-learning exercise, not model inference or an availability test. Run it only in the disposable lab environment. The lab's `SurgeThenDrain` strategy keeps a serving source until its replacement is available, but this exercise does not establish an outage-free guarantee.

<div class="prerequisites" markdown>

- Complete the HTTP lab through its update step, leaving `http-lab` in `ome-http-lab` with three ready Instances and the `hello from OMENative v2` response. Do not clean it up yet.
- Use the same v1.3 development controller, CRDs and webhooks as that lab. See [Install from source](../../getting-started/install.md#install-from-source); this path is not supported by v1.2.2.
- Keep room for the fourth, replacement pod. The namespace must not be waiting on an unmet quota, admission or placement prerequisite.
- Have permission to patch the InferenceService and read its InferenceReplica, pods, events and ControllerRevisions.
- Optionally, build `kubectl-ome` from the same source for the detailed Instance reports below. See [Build from source](../../reference/kubectl-ome/overview.md#build-from-source). The main exercise uses plain `kubectl`.

</div>

Run commands from the matching OME checkout. The JSON below matches the patch files in `config/samples/docs/omenative-http`. Outputs are illustrative, not a captured cluster run.

## Step 1: Record the working revision

Check the starting point before introducing a failure:

```bash
kubectl get inferencereplica http-lab-engine -n ome-http-lab \
  -o custom-columns='GENERATION:.metadata.generation,OBSERVED:.status.observedGeneration,CURRENT:.status.currentRevision,TARGET:.status.updateRevision,READY:.status.conditions[?(@.type=="Ready")].status,UPDATED-READY:.status.updatedReadyReplicas,AVAILABLE:.status.availableReplicas'
```

`CURRENT` and `TARGET` must match, `OBSERVED` must equal `GENERATION`, `READY` must be `True`, and both counts must be `3`. If not, finish or diagnose the preceding lab first.

Save the working target:

```bash
WORKING_REVISION=$(kubectl get inferencereplica http-lab-engine -n ome-http-lab \
  -o jsonpath='{.status.updateRevision}')
printf '%s\n' "$WORKING_REVISION"
```

## Step 2: Introduce a readiness failure

The server listens on port 8080. This merge patch directs only its readiness probe to unused port 8081. It keeps the same image, arguments and small resource requests.

```json title="recovery-fail.patch.json"
{
  "spec": {
    "engine": {
      "runner": {
        "name": "ome-container",
        "readinessProbe": {
          "httpGet": {
            "path": "/",
            "port": 8081
          }
        }
      },
      "lifecycle": {
        "instanceReadyTimeout": "30s",
        "migrationPolicy": {
          "mode": "Never"
        }
      }
    }
  }
}
```

`instanceReadyTimeout` bounds each active readiness wait; it is not a 30-second wall-clock deadline for the entire experiment. Admission gates and operator holds can pause that clock. `migrationPolicy: Never` prevents relocating the broken probe to another node. See [Set Instance readiness deadlines](set-instance-readiness-deadlines.md).

There is no per-service retry-count setting in this patch. The revision retry policy is operator-wide, and this lab does not change it. Readiness failure is not evidence that charges a revision's retry ladder toward `Held`; do not expect an exact retry count or a `RetryHeld` event here.

```bash
kubectl patch inferenceservice http-lab -n ome-http-lab --type=merge \
  --patch-file=config/samples/docs/omenative-http/recovery-fail.patch.json
```

Wait for a reported failed Instance, then **freeze the experiment**. The following block freezes it even if the diagnostic wait expires:

```bash
if ! kubectl wait inferencereplica/http-lab-engine -n ome-http-lab \
  --for='jsonpath={.status.conditions[?(@.type=="Ready")].reason}=InstanceFailed' \
  --timeout=2m; then
  printf '%s\n' 'Failure was not observed within two minutes; freezing for inspection.'
fi
kubectl annotate inferenceservice http-lab -n ome-http-lab \
  ome.io/rollout-paused=freeze --overwrite
kubectl wait inferencereplica/http-lab-engine -n ome-http-lab \
  --for=jsonpath='{.spec.pauseMode}'=Freeze --timeout=60s
```

Set the pause on the **InferenceService**, not by editing its projected replica. Freeze stops new rollout and repair work; it does not stop the kubelet or undo work already underway. Keep the service frozen while investigating. If you need to stop early, leave it frozen or go directly to [Clean up](#clean-up).

## Step 3: Read the failure and the hold separately

These summaries work with either Instance status encoding:

```bash
kubectl get inferencereplica http-lab-engine -n ome-http-lab \
  -o jsonpath='{range .status.conditions[*]}{.type}{": "}{.status}{" ("}{.reason}{")\n"}{end}{.status.rolloutHold}{"\n"}{.status.retryBlocks}{"\n"}'
kubectl get events -n ome-http-lab \
  --field-selector involvedObject.name=http-lab --sort-by=.metadata.creationTimestamp
kubectl describe pods -n ome-http-lab -l ome.io/inferenceservice=http-lab
```

Look for the replacement pod's readiness probe failure on port 8081. A running-but-unready target can leave `ContainersNotReady` in its last failure; the timeout event can mention `DeadlineExceeded`. The aggregate `Ready` condition reports `InstanceFailed` when a failed row is observed. In-flight state may move again before the freeze takes effect, so retain the events as well as the current snapshot.

The pause is an operator hold, not a revision retry block. `status.rolloutHold` explains why work is currently gated; `status.retryBlocks` records revision retry authority. A `Held` block is not expected from this wrong-port probe alone. If you see an image-pull or container-configuration failure, diagnose that different cause instead.

With the optional source-built plugin, inspect the attempt without decoding the status representation yourself:

```bash
kubectl ome instance list http-lab -n ome-http-lab
kubectl ome instance retry-blocks http-lab --component engine -n ome-http-lab
```

Find the affected engine index in the list or events, then substitute it for `0` in:

```bash
kubectl ome instance status http-lab 0 --component engine -n ome-http-lab -o wide
```

Read its running and target revisions, operation and deadline if still present, and last-failure reason/pod. OME can clear the failed operation while retaining the failure record, so an absent operation does not mean that no attempt ran. These are lifecycle attempts, not kubelet container restart counts.

Record the broken target and confirm that it differs from the working revision:

```bash
BROKEN_REVISION=$(kubectl get inferencereplica http-lab-engine -n ome-http-lab \
  -o jsonpath='{.status.updateRevision}')
printf 'working: %s\nbroken:  %s\n' "$WORKING_REVISION" "$BROKEN_REVISION"
```

If there is no new target or no wrong-port probe failure, the intended failure has not been demonstrated. Leave the service frozen and diagnose its events before continuing.

## Step 4: Correct the template and resume

Restore the probe to port 8080, change the response to `v3`, and restore the lab's two-minute readiness timeout. Setting `migrationPolicy` to `null` removes the exercise's override; the original HTTP runtime defines none.

```json title="recovery-fix.patch.json"
{
  "spec": {
    "engine": {
      "runner": {
        "name": "ome-container",
        "args": [
          "-listen=:8080",
          "-text=hello from OMENative v3"
        ],
        "readinessProbe": {
          "httpGet": {
            "path": "/",
            "port": 8080
          }
        }
      },
      "lifecycle": {
        "instanceReadyTimeout": "2m",
        "migrationPolicy": null
      }
    }
  }
}
```

Apply the correction while frozen, then remove the pause:

```bash
kubectl patch inferenceservice http-lab -n ome-http-lab --type=merge \
  --patch-file=config/samples/docs/omenative-http/recovery-fix.patch.json
kubectl annotate inferenceservice http-lab -n ome-http-lab ome.io/rollout-paused-
```

You changed the desired template, so OME targets a new immutable revision. This is not a retry of the broken revision, and it needs no release of that revision's retry authority.

Watch until `TARGET` differs from `BROKEN_REVISION`, `CURRENT` equals `TARGET`, `OBSERVED` equals `GENERATION`, `READY` is `True`, and both counts are `3`. Then press Ctrl-C:

```bash
kubectl get inferencereplica http-lab-engine -n ome-http-lab --watch \
  -o custom-columns='GENERATION:.metadata.generation,OBSERVED:.status.observedGeneration,CURRENT:.status.currentRevision,TARGET:.status.updateRevision,READY:.status.conditions[?(@.type=="Ready")].status,UPDATED-READY:.status.updatedReadyReplicas,AVAILABLE:.status.availableReplicas'
```

Do not use a previously true `Ready` condition alone as proof that the correction was applied. A retained last-failure record is history; the current phase, target and convergence counters determine whether recovery finished.

## Step 5: Verify the response

Stop any old port-forward with Ctrl-C and open a new one:

```bash
kubectl port-forward service/http-lab-engine -n ome-http-lab 8080:8080
```

In another terminal:

```bash
curl --fail --silent --show-error http://127.0.0.1:8080/
```

```output
hello from OMENative v3
```

The new revision and updated-ready count prove template convergence; the request checks one forwarded pod. A port-forward is not a load-balancing or uninterrupted-traffic test.

## Why not reset or roll back?

Choose recovery based on what changed:

| Situation | Recovery |
| --- | --- |
| The desired pod template is wrong, as in this lab | Correct the InferenceService or runtime. OME rolls toward the corrected revision. |
| The desired template is still correct, but an external fault left a non-serving Instance `Failed` | After fixing the fault, consider [Reset failed Instances](reset-failed-instances.md). A reset rebuilds the same target; it does not fix the template. |
| The same target revision has a `Held` retry block | After fixing its cause, [release that exact held revision](../roll-out-changes/release-a-held-revision.md). Reset alone does not release it. |
| You want the previous HTTP response back | Explicitly restore the runner arguments and readiness probe in the InferenceService. This lab has no canary or blue-green rollout group to roll back. |

A reset request is the `ome.io/reset-instances` annotation on the **InferenceReplica**, unlike the pause annotation on the InferenceService. Its value is `all` or selected non-negative Instance indices. OME skips an Instance that still has a serving pod, and does not clear operations owned by Update, Migrate or Delete. This protects the source of a failed surge. Annotation removal acknowledges the request; it does not prove that a rebuild became ready. There is no `kubectl ome instance reset` command.

To restore an earlier template, edit the owning InferenceService, not a ControllerRevision or the generated pods. Reapplying the original lab YAML alone may retain runner fields introduced by merge patches; explicitly remove or restore those fields in your desired manifest.

## Clean up

Stop the port-forward. If you are finished with both HTTP exercises, delete the lab service and wait for its replica to disappear:

```bash
kubectl delete inferenceservice http-lab -n ome-http-lab --wait=true --timeout=2m
kubectl wait --for=delete inferencereplica/http-lab-engine -n ome-http-lab --timeout=2m
```

Deletion is allowed while the rollout is frozen. If draining takes longer, inspect the remaining pods and events; do not force-remove finalizers.

Delete the namespace only if it contains no work outside this lab:

```bash
kubectl delete namespace ome-http-lab --wait=true --timeout=2m
```

## Next steps

- [Pause and resume a rollout](../roll-out-changes/pause-and-resume-a-rollout.md): pause modes and what can continue while paused.
- [Set Instance readiness deadlines](set-instance-readiness-deadlines.md): active clocks, scheduling gates and failure evidence.
- [Reset failed Instances](reset-failed-instances.md): same-target rebuilds and skipped requests.
