---
title: kubectl-ome overview and install
description: Install the kubectl-ome plugin, and look up its commands, global flags, output formats, exit codes and the RBAC it needs.
since: v1.3
---

kubectl-ome is the OME command-line tool, a kubectl plugin that you run as `kubectl ome`. Most commands read your OME resources and explain what they find. [`status`](status.md) shows why an [InferenceService](../../concepts/serving/inference-services.md) is or isn't ready, and [`runtime explain`](runtime.md#explain) shows which runtimes match a model. A few alpha actions, such as `rollout pause`, change a service. Each runs the checks in [Guarded actions](guarded-actions.md). The plugin doesn't install OME: see [Install OME](../../getting-started/install.md).

## Install {#install}

kubectl-ome is new in v1.3. Until v1.3.0 is released, build it from source. From v1.3.0, you can also install a release with krew. kubectl runs the `kubectl-ome` binary on your `PATH` when you type `kubectl ome`.

### Build from source {#build-from-source}

You need git, make and Go 1.26 or later.

```bash
git clone --depth 1 https://github.com/ome-projects/ome.git
cd ome
make kubectl-ome
```

This builds `bin/kubectl-ome` from the latest commit of `main`. To build a release instead, add its tag to the clone, such as `--branch v1.3.0`. Put the binary on your `PATH`:

```bash
export PATH="$PWD/bin:$PATH"
```

To keep the plugin after you close the shell, copy `bin/kubectl-ome` to a directory on your `PATH`.

### Install with krew {#install-with-krew}

From v1.3.0, releases ship the plugin for Linux, macOS and Windows, on amd64 and arm64, with a [krew](https://krew.sigs.k8s.io/) manifest. Install it with krew:

```bash
kubectl krew install --manifest-url=https://github.com/ome-projects/ome/releases/download/v1.3.0/ome.yaml
```

krew downloads the archive for your platform and installs `kubectl-ome`. For another release, replace `v1.3.0` in the URL with its tag.

### Check the install {#check-the-install}

Check that kubectl finds the plugin, and which version of OME the cluster is set to run:

```bash
kubectl ome version
```

Once v1.3.0 is released, with the plugin and OME both from it, the output is:

```output
COMPONENT   VERSION
Client      v1.3.0 (commit 9c41e7a2d85b3f60e1a7c4d29b8f305e6d7a1c42)
Operator    v1.3.0
```

Client is the version and commit of the plugin. A build from the shallow clone of `main` above shows the commit's short hash as its version. Operator is the version of OME that the manager Deployment declares, in the namespace `ome` unless you pass `--ome-namespace`. When the CLI can't find it, Operator is `unknown` with the reason, and the command still exits `0`. Without the [RBAC](#required-rbac) below, the reason is `Forbidden`. [`kubectl ome version`](version.md) explains both values.

## Commands {#commands}

| Command | What it does |
| --- | --- |
| [`accelerator`](accelerator.md) | Shows the [accelerator class](../../concepts/runtimes/accelerator-classes.md) or policy that the engine and decoder of an InferenceService ask for, and the resource requests their pods start from. |
| [`admin`](admin.md) | Checks that the cluster serves the APIs OME needs, and reads the Instance moves that [Alfred](../../concepts/scheduling/alfred.md), OME's alpha GPU cluster caretaker, recommends. |
| [`autoscale`](autoscale.md) | Shows each component's autoscaling state, and which layer supplies its autoscaler. |
| [`cluster`](cluster.md) | Alpha. Shows the WorkloadClusters in the cluster of your current context, and whether OME reports each as connected. |
| [`get`](get.md) | Lists OME resources, including merged views of [BaseModels](../../concepts/models/base-models.md) with ClusterBaseModels and [ServingRuntimes](../../concepts/runtimes/serving-runtimes.md) with ClusterServingRuntimes. |
| [`instance`](instance.md) | Lists and inspects the [OMENative Instances](../../concepts/omenative/instances.md) of an InferenceService. The alpha `release-held` action releases a held revision. |
| [`logs`](logs.md) | Streams logs from the pods behind an InferenceService. |
| [`migration`](migration.md) | Shows the running and past OMENative migrations of an InferenceService. The alpha `start` action requests one. |
| [`placement`](placement.md) | Inspects the alpha multi-cluster placement of an InferenceService. |
| [`quota`](quota.md) | Shows the AcceleratorQuota tree (since v1.3) with its reported budgets, and validates the topology. |
| [`rollout`](rollout.md) | Shows rollout progress, the pinned plan and history, and validates rollout settings. The alpha actions pause, resume, promote, roll back and repin a rollout. |
| [`runtime`](runtime.md) | Explains which runtimes match a model, and shows a service's runtime, revisions and inheritance tree. The alpha `sync` action moves a pinned service to the runtime's current spec. |
| [`scale`](scale.md) | Alpha. Requests a transient replica count for one OMENative component. |
| [`status`](status.md) | Shows why an InferenceService is or isn't ready, in one report: its conditions, pods, model, runtime, rollout, autoscaling, placement, traffic and warning events. |
| [`traffic`](traffic.md) | Shows an InferenceService's traffic routes, weights and canary split, and checks them against your settings. The alpha `drain` and `undrain` actions take a cluster out of multi-cluster routing and back. |
| [`version`](version.md) | Prints the kubectl-ome and OME operator versions. |
| [`wait`](wait.md) | Waits until an InferenceService reaches a condition, rollout outcome, migration result, replica state or runtime sync, or a held revision is released. |

!!! note "Alpha"
    The commands and actions marked alpha can change their flags and behavior between releases.

## Global flags {#global-flags}

All commands take kubectl's standard connection flags, which pick the cluster, the identity and the namespace:

| Flag | What it does |
| --- | --- |
| `-n`, `--namespace` | The namespace to use. The default is your kubeconfig context's namespace, or `default`. |
| `--context`, `--kubeconfig`, `--cluster`, `--user` | The kubeconfig context, file, cluster or user to use. |
| `-s`, `--server`, `--token` | The API server's address and port, and a bearer token for it. |
| `--certificate-authority`, `--client-certificate`, `--client-key`, `--tls-server-name` | The TLS files, and the server name to check the certificate against. |
| `--insecure-skip-tls-verify` | Don't check the server's certificate. The connection is then insecure. |
| `--as`, `--as-uid`, `--as-group`, `--as-user-extra` | Impersonate a user, a UID, groups or user extras. You can repeat `--as-group` and `--as-user-extra`. |
| `--request-timeout` | How long to wait for each API request, as in `10s`. The default, `0`, sets no limit, but the [guarded actions](guarded-actions.md#timeouts-and-response-bounds) and some other commands set their own. |
| `--cache-dir`, `--disable-compression` | The cache directory, `.kube/cache` in your home directory by default, and a switch that turns off response compression. |

Commands that read objects in the OME namespace also take `--ome-namespace`, which defaults to `ome`. `-h` or `--help` prints any command's help.

## Output formats {#output-formats}

`-o` or `--output` sets the format. Most commands take these formats:

| Format | What it prints |
| --- | --- |
| `table` | The default. On a narrow terminal, long cells wrap, or rows print as lists of fields. A file or pipe gets one line per row. |
| `wide` | More detail, on commands that have it. The command's page says what it adds. |
| `json`, `yaml` | The whole report, with `apiVersion: cli.ome.io/v1alpha1` and a kind such as `RolloutStatusReport`. The action commands print an `ActionResult` with the same `apiVersion`. |

These commands take fewer formats, or print something else:

| Command | Formats |
| --- | --- |
| `autoscale explain`, `instance list`, `instance retry-blocks`, `migration status` | All but `wide`. |
| `logs`, `runtime explain`, `version` | No `-o` flag. |
| `get` | All four, but `json` and `yaml` print the object you name, or a `List` of objects, instead of a report. |

In scripts, read `-o json` or `-o yaml` and the [exit code](#exit-codes), not the table layout.

### Evidence levels {#evidence-levels}

Many reports say how the CLI got their values:

| Evidence | Meaning |
| --- | --- |
| `Declared` | Read from the spec or annotations of the resource. |
| `Reported` | Read from the resource's status. |
| `Observed` | Read from the API server by this command. |
| `Computed` | Worked out by the CLI from other values. |
| `Unavailable` | The CLI couldn't get the value. |

`instance`, `autoscale` and `migration` also use levels of their own, which their pages explain.

## Exit codes {#exit-codes}

kubectl-ome exits with one of these codes:

| Code | Meaning | Returned by |
| --- | --- | --- |
| `0` | Success | Any command that finishes. |
| `1` | General error | Any command, for an invalid argument or flag, a failed API request, a refusal, an action you didn't confirm, Ctrl-C, or an action whose outcome is unknown. |
| `2` | Assertion unmet | `rollout validate`, `quota validate` and `admin doctor` when their check fails, and `wait` when it ends without a match. |
| `3` | Mutation conflict | An action whose target or another input changed after the CLI read it, and some `migration start` refusals. The action didn't apply. [Guarded actions](guarded-actions.md#exit-codes) lists the cases. |

`version`, `admin` and some other read commands show a failed read in their output, and exit `0`. A command that exits `2` prints its report first, so a script can read the `-o json` output and still branch on the exit code. An action that exits `1` might have applied, so check the target before you run it again, as [When the outcome is unknown](guarded-actions.md#when-the-outcome-is-unknown) shows. Errors print to stderr as `error: <message>`.

## Required RBAC {#required-rbac}

kubectl-ome runs as the identity in your kubeconfig. The read commands need the rules in this ClusterRole, and a few need an [extra rule](#extra-rules). The actions also need `patch`, as [Guarded actions](guarded-actions.md#required-rbac) lists.

```yaml title="kubectl-ome-reader.yaml"
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
    verbs: ["list"]
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

| Rule | What needs it |
| --- | --- |
| `get` and `list` on every `ome.io` resource | Most commands. `*` also covers subresources, including the `inferencereplicas/scale` that `autoscale status --live-scale` reads. |
| `list` on pods and events | `status`, `logs`, `instance status` and `migration start` list the pods of an InferenceService. `status` and `instance status` list warning events. |
| `get` on `pods/log` | `logs` streams the pods' logs. |
| `get` on deployments | `version` and `admin doctor` read the `ome-controller-manager` Deployment in the OME namespace. |
| `get` and `list` on controllerrevisions | `status`, `runtime effective`, `runtime history` and the other commands that read a service's [runtime revisions](../../concepts/runtimes/runtime-revisions.md) in the OME namespace, and `migration start`, in the service's namespace. |

Bind it with a ClusterRoleBinding, since the commands read cluster-scoped resources and objects in the OME namespace:

```bash
kubectl apply -f kubectl-ome-reader.yaml
kubectl create clusterrolebinding kubectl-ome-reader --clusterrole=kubectl-ome-reader --group=ome-operators
```

```output
clusterrole.rbac.authorization.k8s.io/kubectl-ome-reader created
clusterrolebinding.rbac.authorization.k8s.io/kubectl-ome-reader created
```

Replace `ome-operators` with your group, or bind a user with `--user` or a service account with `--serviceaccount=NAMESPACE:NAME`. To check a permission, ask the API server: `kubectl auth can-i list inferencereplicas.ome.io -n prod` prints `yes` or `no`.

### Extra rules {#extra-rules}

These commands need one more rule, in the namespace the table names:

| Command | Rule | Namespace | Without it |
| --- | --- | --- | --- |
| `migration start` | `get` on configmaps | The service's | The preview warns that it can't read the audit ConfigMap, `<name>-ome-migration-audit`, and the action goes on. With `--request-id`, the command fails. |
| `migration history` | `get` on configmaps | The service's | The report leaves out the audit record and lists an `AuditUnavailable` issue. |
| `admin recommendations` | `get` on configmaps | Alfred's: `--alfred-namespace`, the OME namespace by default | The report shows the state `Forbidden`. |
| `autoscale status --live-scaler` | `get` on `horizontalpodautoscalers` in `autoscaling`, or on `scaledobjects` in `keda.sh`, whichever scales the component | The service's | SCALER-EVIDENCE shows `Forbidden`. |
| `wait --for condition=...` and `wait --for rollout=...` | `watch` on `inferenceservices` in `ome.io` | The service's | The command polls the InferenceService instead of watching it. |

Grant `get` on configmaps with a Role in each namespace where people use these commands. Bound with a ClusterRoleBinding, the rule reads every ConfigMap in the cluster. This Role grants the extra rules in the namespace `prod`:

```yaml title="kubectl-ome-extras.yaml"
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: kubectl-ome-extras
  namespace: prod
rules:
  - apiGroups: [""]
    resources: ["configmaps"]
    verbs: ["get"]
  - apiGroups: ["autoscaling"]
    resources: ["horizontalpodautoscalers"]
    verbs: ["get"]
  - apiGroups: ["keda.sh"]
    resources: ["scaledobjects"]
    verbs: ["get"]
  - apiGroups: ["ome.io"]
    resources: ["inferenceservices"]
    verbs: ["watch"]
```

```bash
kubectl apply -f kubectl-ome-extras.yaml
kubectl create rolebinding kubectl-ome-extras --role=kubectl-ome-extras --group=ome-operators -n prod
```

```output
role.rbac.authorization.k8s.io/kubectl-ome-extras created
rolebinding.rbac.authorization.k8s.io/kubectl-ome-extras created
```

### The view role {#the-view-role}

Instead of the roles above, you can bind the built-in `view` ClusterRole with a ClusterRoleBinding. The `ome-resources` Helm chart installs a ClusterRole, `ome-supplemental-viewer`, that Kubernetes aggregates into `view`. It grants `get`, `list` and `watch` on every `ome.io` resource and on nodes. With it, `view` grants every rule above except `get` on KEDA ScaledObjects, but it also grants read access to every ConfigMap in the cluster.

## Related guides {#related-guides}

- [Troubleshoot an InferenceService](../../guides/troubleshoot/troubleshoot-an-inferenceservice.md)
- [Serve a model on OMENative](../../guides/omenative/serve-a-model-on-omenative.md)
- [Set instance readiness deadlines](../../guides/omenative/set-instance-readiness-deadlines.md)
- [Select accelerators](../../guides/deploy-models/select-accelerators.md)
- [Set accelerator quotas](../../guides/operate-ome/accelerator-quota.md)
- [Pause and resume a rollout](../../guides/roll-out-changes/pause-and-resume-a-rollout.md)
- [Promote or roll back a canary](../../guides/roll-out-changes/promote-or-roll-back-a-canary.md)
- [Repin a drifted rollout plan](../../guides/roll-out-changes/repin-a-drifted-rollout-plan.md)
- [Release a held revision](../../guides/roll-out-changes/release-a-held-revision.md)
- [Request a transient scale](../../guides/scale-and-migrate/request-a-transient-scale.md)
- [Request an instance migration](../../guides/scale-and-migrate/request-an-instance-migration.md)
- [Drain a workload cluster](../../guides/multi-cluster/drain-a-workload-cluster.md), which uses the alpha multi-cluster routing
