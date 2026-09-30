---
title: kubectl ome logs
description: Stream logs from the pods behind an InferenceService, narrowed to one component, OMENative Instance or revision.
since: v1.3
---

`kubectl ome logs` prints the logs of every pod behind an [InferenceService](../../concepts/serving/inference-services.md), given only the service's name. Narrow it to one component, one [OMENative](../../concepts/omenative/overview.md) [Instance](../../concepts/omenative/instances.md) or one revision, and add `--follow` to stream new lines as they arrive.

```text
kubectl ome logs INFERENCESERVICE [flags]
```

The command needs `list` on pods and `get` on `pods/log` in the InferenceService's namespace, as [Required RBAC](overview.md#required-rbac) shows.

## Pods it reads

The command reads the pods that carry the label `ome.io/inferenceservice=<name>`: those of the service's engine, decoder and router. These flags narrow the selection, and you can combine them:

| Flag | Labels it adds | Pods it selects |
| --- | --- | --- |
| `--component engine` | `component=engine` | The pods of one component: `engine`, `decoder` or `router`. |
| `--instance 0` | `ome.io/managed-by=OMENative` and `ome.io/instance-index=0` | The pods of one OMENative Instance. |
| `--revision 1a2b3c4d` | `ome.io/managed-by=OMENative` and `ome.io/revision-hash=1a2b3c4d`, plus `component` for a full revision name | The OMENative pods of one revision. |

In prefill-decode serving, the engine runs prefill and the decoder runs decode.

### Instances and revisions

`--instance` and `--revision` select only the pods that OME runs on OMENative, so they match nothing on a component that runs as a Deployment. See [Deployment modes](../../concepts/architecture/deployment-modes.md).

`--instance` takes an Instance's [index](../../concepts/omenative/instances.md#index), with `--component` or a full `--revision`. `--revision` takes a revision of a component's pod template: its 8-character hash, with `--component`, or its full name, `<inferenceservice>-<component>-<hash>`, which names the component. It isn't a [runtime revision](../../concepts/runtimes/runtime-revisions.md).

To find the indexes and hashes, list the pods with those labels as columns:

```bash
kubectl get pods -n prod -l ome.io/inferenceservice=chat -L component,ome.io/instance-index,ome.io/revision-hash
```

The output adds the columns `COMPONENT`, `INSTANCE-INDEX` and `REVISION-HASH`.

### Containers

The command reads `ome-container`, OME's main container, which runs the engine or the decoder in the shipped runtimes. In a pod without it, such as a router pod, it reads the pod's only container, or fails when the pod has several. `--container` picks another container in every selected pod, so narrow the pods with `--component` too. Unlike in `kubectl logs`, `-c` here is `--component`, and `--container` has no short form.

The command reads the container's current run, or its last run while it's in `CrashLoopBackOff`. For the run before a restart, use `kubectl logs chat-engine-0-leader-0 -n prod -c ome-container --previous`.

## Flags

| Flag | Default | Description |
| --- | --- | --- |
| `-c`, `--component` | None | Only this component: engine, decoder or router |
| `--container` | None | Container name (default: the OME main container, falling back to the pod's first container) |
| `-f`, `--follow` | `false` | Stream new log lines as they arrive |
| `--instance` | `-1` | Only this OMENative instance index (requires `--component` or a full `--revision`) |
| `--limit-bytes` | `0` | Maximum bytes of logs to request per pod for one-shot reads (`0` for no limit) |
| `--max-log-requests` | `5` | Maximum number of concurrent log streams to follow |
| `--revision` | None | Only this OMENative revision hash or full ControllerRevision name |
| `--since` | `0` | Only logs newer than this duration (e.g. `10m`) |
| `--tail` | `-1` | Lines of recent log to show per pod (`-1` for all) |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

The `--instance` default, `-1`, means every Instance: leave the flag out to read them all. The container fallback in the `--container` help is wrong: see [Containers](#containers).

## Output

Logs go to stdout. When more than one pod matches, each line starts with `[<component>/<pod>]`, such as `[engine/chat-engine-0-leader-0]`. By default, each pod's whole log prints, even with `--follow`. `--tail`, `--since` and `--limit-bytes` apply to each pod: `--tail 100` over three pods prints up to 300 lines.

| Behavior | Without `--follow` | With `--follow` |
| --- | --- | --- |
| Reads | One pod at a time, in name order | All the pods at once, up to `--max-log-requests` |
| Prints | One pod's log after another | Lines as they arrive, interleaved but whole |
| A pod not on a node yet | Prints nothing | Its stream ends at once and isn't reopened |
| A failed log request | Stops the command. Earlier pods have printed. | Closes all streams and stops the command |
| Ends | After the last pod | When all streams have ended, or on Ctrl-C |

`--follow` stays with the pods that matched when it started, and doesn't reopen a stream after its container restarts. Run the command again to pick up a new revision's pods.

When more pods match than `--max-log-requests` allows, `--follow` fails before it opens a stream:

```output
error: you are attempting to follow 6 log streams, but maximum allowed concurrency is 5, use --max-log-requests to increase the limit
```

Raise the limit, or narrow the selection.

## Examples

Read the logs of `chat`'s pods once:

```bash
kubectl ome logs chat -n prod
```

It prints the pods' whole logs, then exits.

Read the last 10 minutes of the engine's logs, at most 100 lines from each pod:

```bash
kubectl ome logs chat -n prod -c engine --since 10m --tail 100
```

Read the router's logs. Router pods have one container in the shipped runtimes, so `--container` isn't needed:

```bash
kubectl ome logs chat -n prod -c router
```

Follow the engine, starting from the last 20 lines of each pod:

```bash
kubectl ome logs chat -n prod -c engine -f --tail 20
```

It prints new lines as they arrive, until you press Ctrl-C or all its streams end.

Follow all of `chat`'s pods, up to 12:

```bash
kubectl ome logs chat -n prod -f --max-log-requests 12
```

Read the pods of the engine's Instance `0`, its leader and its workers:

```bash
kubectl ome logs chat -n prod -c engine --instance 0
```

Read the pods of revision `chat-engine-1a2b3c4d`. The full name sets the component:

```bash
kubectl ome logs chat -n prod --revision chat-engine-1a2b3c4d
```

During a [SurgeThenDrain](../../concepts/architecture/omenative-update-strategies.md#surgethendrain) update, a single-pod Instance runs its old and new pods at the same index. Read the pod of the decoder's single-pod Instance `0` that runs revision `5e6f7a8b`:

```bash
kubectl ome logs chat -n prod -c decoder --instance 0 --revision 5e6f7a8b
```

Read the OME controller's logs, from every replica, with kubectl. Use your OME namespace in place of `ome`:

```bash
kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1
```

## Errors {#errors}

| Error | Meaning |
| --- | --- |
| `no pods found for InferenceService "chat" in namespace "prod" (selector component=engine,ome.io/inferenceservice=chat)` | No pod matches. Check the name and the namespace, or pass the selector to `kubectl get pods -n prod -l`. See [The InferenceService gets no pods](../../guides/troubleshoot/troubleshoot-an-inferenceservice.md#the-inferenceservice-gets-no-pods). |
| `pod discovery was truncated after <n> requests; narrow the query with --component, --instance, or --revision` | More than 200 pods match. |
| `discover pods: <reason>` | The pod list failed, for example because you can't list pods. |
| `streaming logs for pod <pod>: container "ome-container" in pod "<pod>" is waiting to start: ContainerCreating` | The pod's container is still starting. Wait, or narrow the selection. |
| `streaming logs for pod <pod>: container <container> is not valid for pod <pod>` | A selected pod has no container of that name. Add `--component`. |
| `streaming logs for pod <pod>: a container name must be specified for pod <pod>, choose one of: [<containers>]` | The pod has several containers and no `ome-container`. Pass `--container`. |
| `bufio.Scanner: token too long` | A log line is 1 MiB or longer. Read that pod with `kubectl logs`. |
| `invalid component "<component>" (valid: engine, decoder, router)` | `-c` takes a component. Name a container with `--container`. |
| `--instance requires --component` | Add `--component`, or give a full `--revision`. |
| `hash-only --revision requires --component` | Add `--component`, or give the revision's full name. |
| `--limit-bytes cannot be used with --follow` | `--limit-bytes` works only on one-shot reads. |

The other validation errors name the flag or value at fault.

## Exit codes

| Code | Meaning | Returned by |
| --- | --- | --- |
| `0` | Success | Every read that ends without an error. With `--follow`, once all streams have ended. |
| `1` | General error | Every run that fails, such as no matching pods, a failed log request or Ctrl-C. |

Errors print to stderr as `error: <message>`.

## Related guides

- [Troubleshoot an InferenceService](../../guides/troubleshoot/troubleshoot-an-inferenceservice.md)
