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
with `download/<tag>`:

```bash
kubectl krew install --manifest-url \
  https://github.com/ome-projects/ome/releases/download/<tag>/ome.yaml
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
before GA — script against `-o json`.

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
    resources: ["pods/log", "configmaps"]
    verbs: ["get"]
  - apiGroups: ["apps"]
    resources: ["deployments"]
    verbs: ["get"]
  - apiGroups: ["apps"]
    resources: ["controllerrevisions"]
    verbs: ["get", "list"]
```

The action commands — `rollout pause/resume/promote/rollback`,
`migration start`, `scale`, `runtime sync` and `instance release-held` —
patch their target, so they need one more rule. Grant it only to users who
should be able to change rollout, migration and scale state:

```yaml
  - apiGroups: ["ome.io"]
    resources: ["inferenceservices", "inferencereplicas"]
    verbs: ["patch"]
```
