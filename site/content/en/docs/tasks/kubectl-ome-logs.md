---
title: "Stream InferenceService Logs"
linkTitle: "kubectl-ome logs"
weight: 21
date: 2026-09-26
description: >
  Stream logs from the pods behind an InferenceService and narrow them to one component, one OMENative instance, or one revision.
---

`kubectl ome logs` streams logs from every pod behind an InferenceService, so
you don't have to look up pod names first. This page shows how to narrow the
stream to one component, one OMENative instance index, or one revision, and
which bounds apply to how much log data the command reads.

## Before you begin

- Install the [kubectl-ome plugin](/ome/docs/tasks/kubectl-ome/).
- The command lists `pods` and gets `pods/log` in the InferenceService's
  namespace; the [minimal reader role](/ome/docs/tasks/kubectl-ome/#required-rbac)
  already grants both.

The command takes exactly one InferenceService name and accepts the standard
kubectl connection flags (`--kubeconfig`, `--context`, `-n`):

```bash
kubectl ome logs my-isvc -n team-a
```

When more than one pod matches, every line is prefixed with
`[component/pod-name] ` so interleaved output stays attributable; with a
single matching pod there is no prefix.

## Stream one component

`--component` (`-c`) limits the stream to one of the service's components —
`engine`, `decoder`, or `router`:

```bash
kubectl ome logs my-isvc -c engine -f
```

`--follow` (`-f`) keeps the streams open and prints new lines as they arrive.

## Stream one OMENative instance

For components running in the OMENative deployment mode — where the OME
controller manages pods directly and labels them with
`ome.io/managed-by: OMENative` — `--instance` selects the single instance
with that index (the pods' `ome.io/instance-index` label). The index must be
between 0 and 2147483647, and the flag requires `--component` (or a full
`--revision`, which pins the component by itself):

```bash
kubectl ome logs my-isvc -c engine --instance 2 -f
```

## Stream one revision

`--revision` limits the stream to the pods of one OMENative workload
revision. It accepts either form:

- The 8-character lowercase hexadecimal revision hash (the pods'
  `ome.io/revision-hash` label). This form requires `--component`:

  ```bash
  kubectl ome logs my-isvc -c engine --revision 1a2b3c4d
  ```

- The full ControllerRevision name,
  `<inferenceservice>-<component>-<hash>`. The component is inferred from
  the name, so `--component` is optional (if given, it must match):

  ```bash
  kubectl ome logs my-isvc --revision my-isvc-engine-1a2b3c4d
  ```

A full revision name must belong to the InferenceService you named —
`kubectl ome logs my-isvc --revision other-engine-1a2b3c4d` is rejected.
`--instance` and `--revision` combine, for example to follow one instance of
the outgoing revision during a rollout.

These are the OMENative **workload** revisions, stored as ControllerRevisions
in the service's own namespace — not the
[runtime pinning](/ome/docs/concepts/runtime-revision/) snapshots kept in the
OME namespace.

To find instance indexes and revision hashes, read them off the pod labels:

```bash
kubectl get pods -l ome.io/inferenceservice=my-isvc \
  -L component,ome.io/instance-index,ome.io/revision-hash
```

Both flags narrow the pod selector to `ome.io/managed-by=OMENative`, so on a
service whose components run as plain Deployments (RawDeployment) or
LeaderWorkerSets (MultiNode) they match nothing and the command reports
`no pods found`.

## Choose the container

Each pod contributes one container's logs. `--container` names it explicitly;
without the flag, the command uses the OME main container (`ome-container`)
when the pod has one. Otherwise the request is sent without a container name,
which the Kubernetes API resolves only for single-container pods — pods with
several containers and no `ome-container` need an explicit `--container`.

## Bound the output

| Flag                 | Default | Effect                                                                                                          |
|----------------------|---------|-----------------------------------------------------------------------------------------------------------------|
| `--tail`             | `-1`    | Lines of recent log to show **per pod**; `-1` means all available lines.                                          |
| `--since`            | `0`     | Only logs newer than this duration (for example `10m`, `2h`); `0` means no time bound.                            |
| `--limit-bytes`      | `0`     | Maximum bytes to request **per pod** for one-shot reads; `0` means no limit. Cannot be combined with `--follow`.  |
| `--max-log-requests` | `5`     | With `--follow`: maximum number of log streams followed concurrently. Must be greater than 0.                     |

`--tail`, `--since`, and `--limit-bytes` apply per pod, not to the combined
output — `--tail 100` against three pods can print up to 300 lines.

Without `--follow`, pods are read one at a time in name order, so
`--max-log-requests` places no bound on one-shot reads. With `--follow`, the
command refuses to start when more pods match than the limit allows:

```
you are attempting to follow 8 log streams, but maximum allowed concurrency is 5, use --max-log-requests to increase the limit
```

Either raise `--max-log-requests` or narrow the pod set with `--component`,
`--instance`, or `--revision`.

Pod discovery itself is also bounded: the command pages through matching pods
50 at a time, up to 10 pages or 200 pods, with a 10-second timeout per list
request. If the list is truncated — more matching pods than the bounds allow —
the command fails instead of streaming from an incomplete pod set:

```
pod discovery was truncated after 10 requests; narrow the query with --component, --instance, or --revision
```

## Validation errors

| Error                                              | Cause                                                                                                     |
|----------------------------------------------------|-----------------------------------------------------------------------------------------------------------|
| `--instance requires --component`                  | `--instance` given without `--component` or a full `--revision`.                                            |
| `hash-only --revision requires --component`        | An 8-hex-character `--revision` without `--component`.                                                      |
| `--instance must be between 0 and 2147483647`      | Negative or too-large instance index.                                                                       |
| `invalid revision "..."`                           | `--revision` is neither 8 lowercase hex characters nor `<inferenceservice>-<component>-<hash>`; uppercase hashes are rejected. |
| `revision "..." does not belong to InferenceService "..."` | The full revision name names a different service.                                                   |
| `revision component "..." does not match --component "..."` | The full revision name and `--component` disagree.                                                 |
| `--limit-bytes cannot be used with --follow`       | Byte limits apply only to one-shot reads.                                                                   |
| `no pods found for InferenceService ...`           | The selector matched nothing — including `--instance`/`--revision` used against non-OMENative components.   |
