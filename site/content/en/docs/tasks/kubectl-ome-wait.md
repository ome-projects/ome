---
title: "Wait for Reported InferenceService State"
linkTitle: "kubectl-ome wait"
weight: 21
date: 2026-09-26
description: >
  Block a script until an InferenceService reports a requested state: every --for predicate, the flags each one requires, and what exit codes 0, 1 and 2 mean.
---

`kubectl ome wait INFERENCESERVICE --for=PREDICATE` blocks until the named
InferenceService reports a requested state, writes one typed **WaitReport**,
and exits with a code a script can branch on — no polling loops around
`kubectl get`, no parsing of human-readable output. The command is part of
the [kubectl-ome plugin](/ome/docs/tasks/kubectl-ome/) and is read-only: it
never mutates anything, and it never prints raw API objects.

## Usage

```bash
kubectl ome wait chat --for=condition=Ready -n prod
kubectl ome wait chat --for=rollout=stable --timeout=10m -o json
```

The command takes exactly one InferenceService name and the standard kubectl
connection flags (`--kubeconfig`, `--context`, `-n`). Three flags apply to
every predicate:

| Flag | Default | Meaning |
| --- | --- | --- |
| `--for` | *required* | The predicate to wait for — one of the seven values below |
| `--timeout` | `60s` | How long to wait; must be positive and at most `24h` |
| `-o` / `--output` | `table` | Final report format: `table`, `wide`, `json` or `yaml` |

When the timeout elapses the command does not fail with a generic error: it
writes its report with outcome `TimedOut` and exits `2` (see
[exit codes](#exit-codes)).

## The `--for` predicates

`--for` takes one of seven exact, case-sensitive values
(`condition=Ready=true` is rejected). Some predicates require extra flags;
supplying a predicate's flag with any other predicate is an invalid
invocation, rejected before a single API request is made.

| `--for` value | Extra flags | Matches when the service reports |
| --- | --- | --- |
| `condition=Ready[=True\|False\|Unknown]` | none | A Ready condition with exactly that status |
| `rollout=stable` / `=failed` / `=rolled-back` | none | Aggregate rollout state `Succeeded` / `Failed` / `RolledBack` |
| `migration=terminal` | `--request-id` | That migration request reached a terminal phase |
| `replicas=ready` | `--component`, `--replicas` | Exact `status.readyReplicas` on the component's InferenceReplica |
| `replicas=current` | `--component`, `--replicas`; optional `--ir-name` + `--ir-uid` | Exact `spec.replicas` and reported `status.replicas` |
| `runtime-sync=acknowledged` | `--request-id` | That sync token acknowledged, pin eligible, no drift |
| `held-revision=unheld` | `--component`, `--revision`, `--ir-name`, `--ir-uid` | That exact revision no longer Held, mailbox absent |

### `condition=Ready`

An omitted status means `True`, so `--for=condition=Ready` and
`--for=condition=Ready=True` are the same wait. A service with **no** usable
Ready condition is observed as `NotRecorded`, which is not the explicit
`Unknown` status — `--for=condition=Ready=Unknown` matches only a Ready
condition whose status is literally `Unknown`, and times out on a service
that never records one.

```bash
kubectl ome wait chat --for=condition=Ready=False --timeout=2m -o json
```

### `rollout=stable|failed|rolled-back`

These match the controller-reported aggregate rollout state — the same
`reportedState` that the `kubectl ome rollout status` summary shows, computed
by the same projection. `stable` means it is `Succeeded` — a service
whose rollout is `NotConfigured` or `Staged` does not match, so this
predicate is for confirming a rollout you started, not for asserting "no
rollout is happening". `failed` matches `Failed`; `rolled-back` matches
`RolledBack`. Missing or invalid rollout evidence never matches.

```bash
kubectl ome wait chat --for=rollout=stable --timeout=15m
```

### `migration=terminal`

Requires `--request-id` with the canonical-form UUID from the
`kubectl ome migration start` result. The wait matches only when that exact
request is reported terminal in a complete, current status snapshot of the
service's own InferenceReplica. **Terminal is not success**: `Completed`,
`Failed` and `Relocated` all end the wait with exit `0` — read the
`migration` block of the report (phase and outcome) to learn which.

```bash
kubectl ome wait chat --for=migration=terminal \
  --request-id=12345678-1234-4234-8234-123456789abc -n prod --timeout=30m
```

### `replicas=ready`

Requires `--component` (`engine`, `decoder` or `router`) and a nonnegative
`--replicas=N`. It matches an **exact** `status.readyReplicas` count — not
"at least N" — on the current InferenceReplica owned by the service for that
component. A count is only accepted when the IR snapshot is current
(`status.observedGeneration` positive and equal to the IR's generation): an
IR that simply omits the count never matches, even when you asked for zero.
Reads admit at most 32 related InferenceReplicas and 2,048 status rows; an
incomplete or truncated snapshot never satisfies the wait.

```bash
kubectl ome wait chat --for=replicas=ready --component=engine --replicas=2 -n prod
```

A matched ready count is not serving health, availability, or `/scale`
convergence — it is the controller's reported count, nothing more.

### `replicas=current`

Requires `--component` and a **positive** `--replicas=N`; it matches when
the component's InferenceReplica reports exactly N in both `spec.replicas`
and the logical `status.replicas`. Readiness is reported in the result but
not required for the match — use `replicas=ready` for that. `--ir-name` and
`--ir-uid` are optional here but must be given together: copied from a
`kubectl ome scale` result's `target.name` and `target.uid`, they bind the
wait to the original target, so a replacement IR cannot satisfy it. Without
them, the parent's current scale target selects the IR.

```bash
kubectl ome wait chat --for=replicas=current --component=engine --replicas=4 -n prod
```

### `runtime-sync=acknowledged`

Requires `--request-id` with the **version-4** UUID printed by
`kubectl ome runtime sync` — a manually written timestamp token cannot be
waited on (see
[runtime pinning](/ome/docs/concepts/runtime-revision/#confirming-the-controller-acknowledged-the-request)).
The wait matches only when the same service reports that exact token
acknowledged in status, an eligible managed pin, and no `RuntimeDrifted`
condition. Token acknowledgment is not live-runtime convergence or serving
readiness.

```bash
kubectl ome wait chat --for=runtime-sync=acknowledged \
  --request-id=123e4567-e89b-42d3-a456-426614174000 -n prod
```

### `held-revision=unheld`

Requires all four of `--component`, `--revision`, `--ir-name` and
`--ir-uid`, copied from a
[`kubectl ome instance release-held`](/ome/docs/tasks/release-a-held-revision/)
result: `--revision` is the full `<isvc>-<component>-<hash>` name built from
its `revisionHash`, and `--ir-name`/`--ir-uid` are its `target.name` and
`target.uid`. The wait matches when that exact revision is no longer Held
and the release mailbox is absent on the same-UID InferenceReplica. The very
first poll may already match, and a match is not proof your release request
caused the state — natural pruning or a later re-hold can race.

```bash
kubectl ome wait chat --for=held-revision=unheld \
  --component=engine --revision=chat-engine-1f2a3b4c \
  --ir-name=chat-engine --ir-uid=6c9c2f6e-... -n prod --timeout=2m
```

## How the command observes

`condition=Ready` and `rollout=...` use a named GET of the InferenceService
followed by an exact-name WATCH, falling back to bounded named-GET polling
every 5 seconds when the watch cannot be sustained. The other five predicates
are poll-only from the start, re-reading their bounded evidence every
5 seconds: `runtime-sync` reads only the parent, `replicas=current` and
`held-revision` add an exact GET of the one target InferenceReplica, and
`migration` and `replicas=ready` collect a bounded page of the service's
related InferenceReplicas — none of them watch, because an IR-only status
update need not change the parent. The report records how the final
observation was made (`sourceMethod`, `pollingFallback`, and GET/WATCH/poll
counts), so a script can tell an event-driven match from a polled one.

The [minimal reader RBAC](/ome/docs/tasks/kubectl-ome/#required-rbac) is
sufficient for every predicate: when the `watch` verb is missing, the
Ready and rollout waits silently degrade to 5-second polling rather than
failing. Grant `watch` on `inferenceservices` to make them event-driven.

Cancellation (Ctrl-C) and deadlines are cooperative — external credential
plugins or custom transports may not honor them promptly.

## Exit codes

`wait` uses the plugin's
[shared exit-code mapping](/ome/docs/tasks/kubectl-ome/#exit-codes):

| Code | Meaning for `wait` |
| --- | --- |
| 0 | The requested state was observed on the same object |
| 1 | The observation could not complete — invalid flags or predicate/flag pairing, kubeconfig or API or output failure, cancellation |
| 2 | The wait completed but the predicate was left unmet — timeout, target absent, deleted, or replaced |

Exit `2` covers four distinct outcomes, all named in the report's
`content.outcome` field: `TimedOut` (the predicate never matched within
`--timeout`), `NotFound` (the InferenceService does not exist), `Deleted`
(it was deleted mid-wait), and `Replaced` (an object with the same name but
a different UID appeared). The same-object guarantee behind exit `0` comes
from that UID pinning: a service deleted and recreated during the wait ends
it as `Deleted` or `Replaced` rather than matching against the impostor.

On exit `2` the full report has already been written to stdout, so capture
`-o json` output and branch on the code:

```bash
report=$(kubectl ome wait chat --for=condition=Ready --timeout=5m -o json -n prod)
case $? in
  0) echo "Ready reported" ;;
  2) echo "unmet: $(jq -r '.content.outcome' <<<"$report")" >&2; exit 1 ;;
  *) echo "wait could not complete" >&2; exit 1 ;;
esac
```

Exit `1` means the command could not complete its observation — do not
treat any partial output as a report. A failed command writes a single
`error: <message>` line to stderr.

## What a match does not mean

Exit `0` is **state observation, not attribution**. The command verifies
that the same object reported the requested state; it does not verify that
your action caused it, and the report's generation-freshness field always
says `Unverifiable` — a reported Ready is not proof the controller has
converged on your latest spec change. When you need that distinction, read
the report's per-predicate observation block instead of relying on the exit
code alone. And as everywhere in the plugin, the `table`/`wide` output is
not a stable scripting interface — script against `-o json`.
