---
title: "kubectl-ome Plugin"
linkTitle: "kubectl-ome"
weight: 20
description: >
  Install and use the official OME CLI as a kubectl plugin
---

## Install

Via [krew](https://krew.sigs.k8s.io/) once the plugin is accepted into the index:

```bash
kubectl krew install ome
```

Until then, install straight from a GitHub release:

```bash
kubectl krew install --manifest-url \
  https://github.com/ome-projects/ome/releases/latest/download/ome.yaml
```

To pin a release instead of tracking the newest one, replace `latest/download`
with `download/` and the release tag:

```bash
kubectl krew install --manifest-url \
  https://github.com/ome-projects/ome/releases/download/vX.Y.Z/ome.yaml
```

## Commands

| Command | What it shows |
| --- | --- |
| `kubectl ome get isvc` | InferenceServices with model, runtime, readiness and URL |
| `kubectl ome get models -A` | BaseModels and ClusterBaseModels in one listing |
| `kubectl ome get runtimes` | ServingRuntimes and ClusterServingRuntimes in one listing |
| `kubectl ome status my-isvc` | Conditions, per-component pods, model status and warning events |
| `kubectl ome runtime explain --model my-model` | Which runtimes match the model, ranked, with rejection reasons |
| `kubectl ome logs my-isvc -c engine -f` | Live logs from the engine pods |
| `kubectl ome version` | Client and operator versions |

Every command accepts the standard kubectl connection flags
(`--kubeconfig`, `--context`, `-n`); listings accept `-o json|yaml|wide`,
`-A` and `-l`. Human-readable output is not a stable scripting interface
before GA — script against `-o json` and the [exit codes](#exit-codes)
below.

## Exit codes

Every command exits through one shared mapping, so scripts can branch on
the exit code without parsing error text:

| Code | Meaning |
| --- | --- |
| 0 | Success |
| 1 | General error — invalid invocation, kubeconfig, API, RBAC or output failure, or cancellation |
| 2 | Assertion unmet — the command completed and wrote its report, but the asserted state does not hold |
| 3 | Guarded mutation rejected — the target changed after its precondition was verified; the mutation was not applied |

Exit 2 is the read-only "checked, and it is not so" signal. `wait` returns
it when timeout, absence, deletion or replacement leaves the `--for`
predicate unmet; `rollout validate` when validation is Invalid or
Unverifiable; `admin doctor` when a required API is provably not
discoverable (a forbidden discovery response is not proof and does not by
itself cause exit 2); and `quota validate` when the complete snapshot
contains violations. In every exit-2 case the full report has already been
written to stdout, so a script can capture `-o json` output and still
branch on the code. Exit 1 means the command could not complete its
observation — do not treat any partial output as a report.

Exit 3 comes only from the action commands — `rollout
pause/resume/promote/rollback/repin`, `traffic drain/undrain`,
`migration start`, `scale`, `runtime sync` and `instance release-held` —
and means the mutation was not applied: either the refreshed target no
longer matched the preview, or the server rejected the guarded patch.
Re-inspect current state and issue a new request rather than retrying the
old one. Exit 1 from an action command carries no such guarantee — the
request outcome may be unknown.

Whatever the code, a failed command writes a single `error: <message>`
line to stderr (action previews and confirmation prompts also go to
stderr, not stdout).

## Required RBAC

The inspection commands are read-only. A minimal ClusterRole:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: kubectl-ome-reader
rules:
  - apiGroups: ["ome.io"]
    resources: ["*"]
    verbs: ["get", "list"]
  - apiGroups: [""]
    resources: ["pods", "events"]
    verbs: ["get", "list"]
  - apiGroups: [""]
    resources: ["pods/log"]
    verbs: ["get"]
  - apiGroups: ["apps"]
    resources: ["deployments"]
    verbs: ["get"]
  - apiGroups: ["apps"]
    resources: ["controllerrevisions"]
    verbs: ["get", "list"]
```

`admin recommendations`, `migration history` and `migration start` each read
one derived ConfigMap in the namespace they are pointed at. Bound
cluster-wide, a ConfigMap rule grants `get` on every ConfigMap in the
cluster, so keep it out of the baseline role and add it only where those
commands are used — as an extra rule, or through a namespace-scoped `Role`
and `RoleBinding` for users confined to one namespace:

```yaml
  - apiGroups: [""]
    resources: ["configmaps"]
    verbs: ["get"]
```

Full `autoscale status` output also reads `horizontalpodautoscalers`
(`autoscaling/v2`) and KEDA `scaledobjects` (`keda.sh/v1alpha1`). Without
them the command still succeeds but reports scaler evidence as unavailable,
which is easy to mistake for a broken autoscaler:

```yaml
  - apiGroups: ["autoscaling"]
    resources: ["horizontalpodautoscalers"]
    verbs: ["get"]
  - apiGroups: ["keda.sh"]
    resources: ["scaledobjects"]
    verbs: ["get"]
```

The action commands — `rollout pause/resume/promote/rollback/repin`,
`traffic drain/undrain`, `migration start`, `scale`, `runtime sync` and
`instance release-held` — patch their target, so they need one more rule.
`scale` patches the `scale` subresource, which RBAC matches only when it is
named explicitly. Grant this rule only to users who should be able to change
rollout, traffic, migration and scale state:

```yaml
  - apiGroups: ["ome.io"]
    resources:
      - inferenceservices
      - inferencereplicas
      - inferencereplicas/scale
    verbs: ["patch"]
```
