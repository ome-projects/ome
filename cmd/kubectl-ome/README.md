# kubectl-ome

`kubectl-ome` is the OME command-line interface. Use it as `kubectl ome` to
inspect models, runtimes, inference services, logical instances, and controller
evidence; diagnose rollout, autoscaling, placement, quota, and traffic; stream
logs; and submit guarded operational requests.

The current command tree has **43 operational leaf commands: 32 read-only
commands and 11 guarded actions**. Help and shell-completion commands are not
included in that count. The implementation and each command's `--help` are the
source of truth; the tree is checked by
[`pkg/cli/root_test.go`](../../pkg/cli/root_test.go).

Multi-cluster APIs and CLI diagnostics are alpha. Their presence does not mean
multi-cluster reconciliation is a finished feature. Cluster, placement, and
traffic reports inspect evidence in the selected Kubernetes context; they do
not establish remote connectivity or end-to-end serving health.

## Contents

- [Build and install](#build-and-install), [quickstart](#quickstart), and
  [context/authentication](#context-authentication-and-namespaces).
- [Resource discovery](#resource-discovery-get),
  [service/runtime diagnostics](#service-runtime-and-accelerator-diagnostics),
  [instances/migrations](#instances-and-migrations), and [rollouts](#rollouts).
- [Autoscaling/runtime sync](#autoscaling-and-runtime-synchronization),
  [placement/traffic/quota](#placement-cluster-traffic-and-quota), and
  [control-plane diagnostics/logs](#control-plane-diagnostics-and-logs).
- [Action safety](#guarded-actions-and-dry-runs),
  [waits](#waiting-for-reported-state), [output](#human-output-and-automation),
  and [permissions/limits/errors](#permissions-limits-and-exit-codes).
- [Troubleshooting](#troubleshooting),
  [development](#development-and-verification), and
  [captured Moirai examples](#captured-moirai-examples).

## Build and install

Build from the repository root with Go **1.26 or newer**, matching `go.mod`:

```sh
make kubectl-ome
export PATH="$PWD/bin:$PATH"
kubectl plugin list
kubectl ome --help
```

The Makefile builds `bin/kubectl-ome` with CGO disabled and supplies version
metadata. This target does not require the Rust/Xet build used by `ome-agent`.
The binary can also run directly:

```sh
./bin/kubectl-ome --help
./bin/kubectl-ome version --request-timeout=10s
```

To install from the checkout into Go's configured binary directory:

```sh
CGO_ENABLED=0 go install ./cmd/kubectl-ome
```

Ensure that directory is on `PATH`. A plain `go install` does not add the
Makefile's version metadata. `make kubectl-ome-cross` builds Linux, macOS, and
Windows binaries for both amd64 and arm64 under `bin/cross/`.

Installing the CLI does not install or upgrade the OME controller, CRDs,
scheduler, Alfred, or quota manager. Read commands need the corresponding APIs
and permissions; guarded actions also require the controller evidence described
below.

## Quickstart

These quickstart commands are illustrative. Replace `dev-cluster`, `team-a`,
`chat`, and other example names with resources in your cluster. Actual recorded
outcomes are labeled separately under [captured Moirai examples](#captured-moirai-examples).

```sh
# Select the intended cluster and discover services.
kubectl ome get isvc --context dev-cluster -n team-a
kubectl ome get models --context dev-cluster -n team-a
kubectl ome get runtimes --context dev-cluster -n team-a

# Follow one service from parent status into detailed evidence.
kubectl ome status chat --context dev-cluster -n team-a
kubectl ome runtime effective chat --context dev-cluster -n team-a
kubectl ome instance list chat --context dev-cluster -n team-a
kubectl ome rollout status chat --context dev-cluster -n team-a

# Bound log volume and use an explicit wait predicate.
kubectl ome logs chat --context dev-cluster -n team-a \
  --component engine --tail=100 --limit-bytes=65536
kubectl ome wait chat --context dev-cluster -n team-a \
  --for=condition=Ready --timeout=2m -o json
```

Start with `admin doctor --isvc chat` when the API or installation is unclear.
Use the detailed family report when `status` shows unavailable, partial, stale,
or conflicting evidence. `status` describes what it observed; a successful
exit does not assert that the service is healthy.

## Context, authentication, and namespaces

The CLI uses client-go's standard kubectl configuration flags and kubeconfig
loading. Existing kubeconfig authentication, including credential plugins,
applies. It does not log in separately or change your current context.

```sh
kubectl ome status chat \
  --kubeconfig /path/to/kubeconfig \
  --context dev-cluster -n team-a --request-timeout=10s
```

| Setting | Meaning |
|---|---|
| `--kubeconfig`, `--context` | Select the connection and credentials. Prefer explicit context selection in automation. |
| `-n`, `--namespace` | Workload namespace; otherwise taken from the selected kubeconfig context. |
| `--request-timeout` | Kubernetes request timeout; command-specific bounds may shorten it. It is not a universal whole-command timeout. |
| `--ome-namespace` | OME control-plane/revision namespace, default `ome`, on commands that need it. |
| `admin --alfred-namespace` | Alfred namespace; defaults to `--ome-namespace`. |
| `admin --alfred-config-name` | Alfred configuration ConfigMap, default `alfred-config`. |
| `admin --alfred-config-key` | Configuration key, default `config.yaml`. |

`--ome-namespace` is available on `status`, `version`, `accelerator explain`,
`autoscale explain`, `runtime explain/effective/history/sync`,
`instance status/release-held`, `migration start`, `scale`, and
`rollout pause/resume/promote/rollback`. The `admin` family inherits the OME
and Alfred options. Other commands do not accept it. For example,
`migration history` reads an optional audit ConfigMap beside the workload,
not in the OME namespace.

For `traffic drain`, `--workload-cluster` identifies the WorkloadCluster to
drain. The inherited `--cluster` retains its standard kubeconfig API-cluster
selection meaning; it is not an alias for the drain target. Prefer explicit
`--context` for the intended connection and `--workload-cluster` for the target.

## Resource discovery: `get`

```sh
kubectl ome get isvc -A
kubectl ome get ir -n team-a -o wide
kubectl ome get models -n team-a -l app=chat
kubectl ome get isvc chat -n team-a -o yaml
```

| Resource | Aliases | Scope |
|---|---|---|
| `inferenceservices` | `inferenceservice`, `isvc`, `isvcs` | Namespaced |
| `models` | `model` | Merged BaseModel and ClusterBaseModel view |
| `basemodels` | `basemodel`, `bm` | Namespaced |
| `clusterbasemodels` | `clusterbasemodel`, `cbm` | Cluster |
| `runtimes` | `runtime` | Merged ServingRuntime and ClusterServingRuntime view |
| `servingruntimes` | `servingruntime`, `srt` | Namespaced |
| `clusterservingruntimes` | `clusterservingruntime`, `csrt` | Cluster |
| `acceleratorclasses` | `acceleratorclass`, `ac` | Cluster |
| `benchmarkjobs` | `benchmarkjob`, `bj` | Namespaced |
| `finetunedweights` | `finetunedweight`, `ftw` | Cluster |
| `inferencereplicas` | `inferencereplica`, `ir` | Namespaced |
| `workloadclusters` | `workloadcluster`, `wc` | Cluster |
| `acceleratorquotas` | `acceleratorquota`, `aq` | Cluster |
| `rolloutpolicies` | `rolloutpolicy`, `rp` | Namespaced |
| `trafficmaps` | `trafficmap`, `tm`, `tmap` | Namespaced |
| `autoscalerpolicies` | `autoscalerpolicy`, `ap` | Namespaced |

`get RESOURCE [NAME]` accepts `-A/--all-namespaces` and `-l/--selector` for
lists. A named lookup cannot combine with either option. Cluster-scoped
resources ignore `-A` with a warning. Merged views include cluster resources
alongside the selected namespaced resources; a named merged lookup tries the
namespaced object first.

Omit `-o` for the default table, or explicitly use `table`, `wide`, `json`, or
`yaml`. JSON/YAML returns the Kubernetes object for a
named lookup, or a `v1/List` containing raw resource objects for a list.
An empty list succeeds; human output writes its empty-result notice to stderr.

## Service, runtime, and accelerator diagnostics

| Command | What it answers |
|---|---|
| `status SERVICE` | Parent readiness, bounded pod/events evidence, and rollout, autoscale, runtime, accelerator, traffic, and placement summaries. |
| `runtime explain --model MODEL` | Which live runtimes match a model according to the operator's runtime-selection engine, including rejection reasons. |
| `runtime explain --isvc SERVICE` | The same selection explanation for a service's model. |
| `runtime effective SERVICE` | Live versus controller-active runtime evidence, inheritance, pins, and drift. |
| `runtime history SERVICE` | Retention-bounded runtime ControllerRevision evidence. |
| `runtime tree RUNTIME` | Parent paths, inheriting descendants, and explicit InferenceService users. |
| `accelerator explain SERVICE` | Declared accelerator policy, reported selection, verified AcceleratorClass identity, and base/applied requests. |

The two `runtime explain` forms are one command and require exactly one of
`--model` or `--isvc`. `--with-effective` is valid only with `--isvc`; it appends
independent effective-runtime evidence without changing the selector verdict.
This command has human output only, with no `-o` flag.

`runtime tree --kind ServingRuntime|ClusterServingRuntime` resolves an
otherwise ambiguous name. `--show-unattributed-users` adds unresolved/invalid
references separately. A cluster-scoped runtime tree can require namespaced
runtime and service lists across the cluster.

```sh
kubectl ome runtime explain --isvc chat --with-effective -n team-a
kubectl ome runtime tree shared-runtime --kind ClusterServingRuntime -o json
kubectl ome runtime history chat -n team-a -o wide
kubectl ome accelerator explain chat -n team-a -o json
```

Runtime history is retained evidence, not a complete audit log. Compact
history tables may use display fingerprints such as `PREFIX#DIGEST`; copy
real revision identities from `-o wide` or structured output for operations.
Accelerator explanation does not rerun accelerator selection or prove free
capacity. Pod counts in `status` are observed pods, not logical instance counts.

## Instances and migrations

| Command | Selection and behavior |
|---|---|
| `instance list SERVICE` | Lists controller-reported logical instances. |
| `instance status SERVICE INDEX --component COMPONENT` | One instance plus bounded live pod and Warning Event evidence. |
| `instance retry-blocks SERVICE --component COMPONENT` | Controller-reported retry authority and Held revisions. |
| `instance release-held SERVICE --component COMPONENT --revision REVISION` | Guarded request to release one exact Held revision. |
| `migration status SERVICE` | Authoritative migration records from owned InferenceReplicas. |
| `migration history SERVICE` | Separate InferenceReplica, parent-history, and optional audit windows. |
| `migration start SERVICE --component COMPONENT --instance INDEX` | Guarded request for one OMENative migration. |

Components are `engine`, `decoder`, or `router`. Migration status/history
accept an optional `--component` filter. `instance status` supports both
DenseV1 and ColumnarV2 status evidence; live pods cannot manufacture an instance
missing from authoritative status. RawDeployment components are reported as
`NotOMENative`.

```sh
kubectl ome instance status chat 0 --component engine -n team-a -o wide
kubectl ome instance retry-blocks chat --component engine -n team-a -o json
kubectl ome migration history chat --component engine -n team-a -o wide

# Preview only; live safety reads and confirmation are still required.
kubectl ome migration start chat --component engine --instance 0 \
  --from-node worker-a --hint-node worker-b --reason maintenance \
  -n team-a --dry-run=client --yes -o json
```

`migration start` requires a canonical nonnegative instance index and a
complete, owned current-revision pod set. A gang spanning nodes requires
`--from-node`. `--hint-node` accepts at most eight ordered soft preferences;
it does not reserve or force a destination. `--reason` is optional, and
`--requested-by` defaults to `kubectl-ome`; that label is advisory, not the
authenticated user identity. Keep both free of secrets.

A new migration request UUID is generated once. Supplying `--request-id`
performs lookup of an identical retained request; it does not choose a UUID
for a new request or replay one. An unseen/pruned UUID or insufficient history
is refused.

`release-held` accepts a full scoped revision or an unambiguous eight-character
lowercase hexadecimal hash. It requires current owned OMENative evidence,
the parent-generation stamp, `controller-write=true`, a Held target, and an
absent mailbox. Acceptance does not mean the controller released or retried
it, and it does not resume a paused workload. Later absence of the block is a
state observation, not proof that this request caused it.

## Rollouts

| Command | Purpose |
|---|---|
| `rollout status SERVICE` | Report rollout progress. |
| `rollout explain SERVICE` | Explain declared intent, effective plan, and reported progress. |
| `rollout history SERVICE` | Inspect bounded retained rollout evidence. |
| `rollout validate SERVICE` | Assert rollout, traffic, and autoscaling configuration validity. |
| `rollout pause SERVICE` | Request a service-wide OMENative lifecycle pause. |
| `rollout resume SERVICE` | Clear a recognized pause depth. |
| `rollout promote SERVICE` | Advance the current eligible pinned canary gate. |
| `rollout rollback SERVICE` | Abort canary members toward their own reported stable revisions. |
| `rollout repin SERVICE` | Request a guarded progression-plan repin after eligible drift. |

```sh
kubectl ome rollout explain chat -n team-a -o wide
kubectl ome rollout validate chat -n team-a -o json
kubectl ome rollout pause chat -n team-a --dry-run=server --yes -o json
kubectl ome rollout repin chat -n team-a --dry-run=client --yes -o json
```

Pause sets the `true` depth: Update/Create/Migration work holds while
RestartPolicy repair continues. It does not request `freeze` or downgrade an
existing freeze. Resume clears either recognized depth. Timed canary gates
continue aging, and scale-down/deletion can still proceed. Pause therefore
does not freeze all workload changes.

Resume's `--discard-pending-actions --yes` explicitly discards pending
promote/rollback mailboxes. Ordinary promote advances an indefinite manual
gate; bypassing an active analysis gate requires `--override-analysis --yes`
and bypasses that step's health checks, warm-up, and bake. Rollback holds the
rejected target; it does not undo the service specification or authorize a
retry. These actions refuse ineligible, stale, placement-owned, or conflicting
work instead of guessing an operation.

Repin requires an active nonempty plan, current Ready/drift evidence, matching
live/pinned topology, and at most one canary group. It submits the exact
combined progression-render digest, never the unsafe literal `now`.
The controller retains run identity/progress and may clamp a canary into a
pre-step hold. Repin is not a topology-change command.

`rollout validate` writes its report before returning: `Valid` exits 0;
`Invalid` or `Unverifiable` exits 2.

## Autoscaling and runtime synchronization

| Command | Purpose |
|---|---|
| `autoscale status SERVICE` | Inspect parent-reported autoscaling state. |
| `autoscale explain SERVICE` | Compare active-runtime autoscaling configuration with parent evidence. |
| `scale SERVICE --component COMPONENT --replicas N` | Guarded transient positive logical-instance count on the selected InferenceReplica `/scale`. |
| `runtime sync SERVICE` | Request advancement of an eligible managed runtime pin with `autoSync=false`. |

By default, `autoscale status` does not read HPA, KEDA, Deployment, or
InferenceReplica objects. Add `--live-scale` for the exact selected IR and its
`/scale`, or `--live-scaler` for the exact selected HPA/KEDA ScaledObject.
Both flags can be combined and require additional read permissions. The KEDA
option does not inspect KEDA's generated HPA. Matching counts or generations
alone do not prove scaler health.

```sh
kubectl ome autoscale status chat -n team-a --live-scale --live-scaler -o json
kubectl ome autoscale explain chat -n team-a -o json
kubectl ome scale chat -n team-a --component engine --replicas=2 \
  --override-autoscaler --yes --dry-run=client -o json
kubectl ome runtime sync chat -n team-a --dry-run=client --yes -o json
```

Every `scale` ownership class, including `None`, requires
`--override-autoscaler --yes`, including dry-run. `N` must be a positive
base-10 integer, at most 2147483647; zero is not supported. This changes the
selected IR's scale subresource, not durable parent intent. Reconciliation or
an autoscaler can overwrite it. Scale-down may drain/delete instances even
while rollout is paused. Policies, scalers, and other components are not edited.

`runtime sync` requires a bound source and managed pin, drift, complete bounded
history, and no conflicting rollback, Held, pending, or active work. Portable
services do not need fictitious IRs. Its token requests the latest live runtime
when the controller consumes it; it does not lock the previewed runtime
contents. Use its returned request ID with the acknowledgment wait below.
Disabled inheritance profiles are valid sources when the merged runtime is
enabled; profile identity and revision-history checks still apply.

## Placement, cluster, traffic, and quota

| Command | Evidence or operation |
|---|---|
| `cluster status [WORKLOADCLUSTER]` | Current-context declared/reported cluster registry evidence. |
| `placement status SERVICE` | Controller-reported placement state. |
| `placement explain SERVICE` | Label-selector compatibility and declared routing overrides. |
| `placement endpoint SERVICE` | Reported origins and routing evidence. |
| `traffic status SERVICE` | Controller-reported traffic status. |
| `traffic explain SERVICE` | Declared and reported traffic behavior. |
| `traffic drain SERVICE --id ID --workload-cluster CLUSTER --reason REASON` | Guarded addition of one traffic-drain override. |
| `traffic undrain SERVICE --id ID` | Guarded removal of exactly one existing override ID. |
| `quota tree [ACCELERATORQUOTA]` | Declared ancestry and budget leaves. |
| `quota status [ACCELERATORQUOTA]` | Reported budgets, capacity, and materialization evidence. |
| `quota validate [ACCELERATORQUOTA]` | Advisory validation of the complete quota topology. |

```sh
kubectl ome cluster status -o json
kubectl ome placement endpoint chat -n team-a -o wide
kubectl ome traffic explain chat -n team-a -o json
kubectl ome quota tree
kubectl ome quota validate -o json
kubectl ome traffic drain chat -n team-a --id maintenance-a \
  --workload-cluster member-a --reason maintenance --dry-run=client --yes -o json
```

These cluster/placement diagnostics do not open remote kubeconfigs, resolve
remote credentials, or probe endpoints. WorkloadCluster Ready is reported
evidence, not proof of capacity, quota, eligibility, or serving health.
Placement endpoint displays origins rather than full URLs; reported weights,
publisher acknowledgment, and recorded probes are separate facts.

Traffic actions target an eligible control-plane source InferenceService.
Drain IDs are independently removable DNS-1123 identifiers. The patch retains
unrelated annotations and other override IDs; malformed or conflicting
annotation state is refused. Accepted overrides still need reconciliation and
data-plane realization; inspect `traffic status` afterward.

Quota commands read cluster-scoped AcceleratorQuota objects, without Kueue,
node, or remote capacity verification. `quota tree NAME` selects ancestry and
descendants for display. `quota validate NAME` still validates the whole
snapshot; an empty topology fails because it lacks the reserved root. Quota
validation violations return exit 2. A named `quota status` uses an exact GET
and is useful when an all-object report reaches its display limit.

## Control-plane diagnostics and logs

| Command | Behavior |
|---|---|
| `admin doctor [--isvc SERVICE]` | Fixed API-discovery reads, the named manager Deployment, and optionally one exact service. |
| `admin recommendations` | Alfred's selected configuration and latest recommendation record. |
| `version` | Client build version and best-effort operator Deployment image version. |
| `logs SERVICE` | Logs from the service's selected pods. |

```sh
kubectl ome admin doctor --isvc chat -n team-a --ome-namespace ome -o json
kubectl ome admin recommendations --alfred-namespace caretaker -o wide
kubectl ome version --ome-namespace ome --request-timeout=10s
kubectl ome logs chat -n team-a --component engine --instance=0 \
  --follow --tail=100 --max-log-requests=5
```

Doctor performs at most nine sequential GETs: seven fixed API discovery
documents, one manager Deployment, and an optional service. It makes no LIST,
access-review, Secret, or remote-cluster request and performs no remediation.
API discovery does not prove read authorization or operator compatibility.
Proven required API non-discoverability returns exit 2; a forbidden discovery
request alone does not establish that an API is missing.

Recommendations reads at most two named ConfigMaps. It reports only
`last-cycle.json`, not complete history. Advisory recommendations or recorded
dispatch do not authorize execution or prove convergence. Declared ConfigMap
settings can differ from Alfred's last-known-good running configuration.

`version` selects the Deployment container named `manager`, with a fallback
only when there is exactly one container. It displays the image tag, digest,
or `tag@digest`; an untagged image is unknown, and a registry port is not a
version. This is declared Deployment image evidence, not a running-pod check.
Dynamic version fields are bounded and sanitized, and lookup errors use safe
classifications rather than raw kubeconfig/server text. Operator lookup failure
becomes `unknown` and does not cause a nonzero exit. Do not use it as a health
check. Output-write failures do return an error. Actual terminals use a
width-aware table that wraps long image identities; redirected output keeps
the two `Client Version:` and `Operator Version:` lines.

Logs accepts `--component/-c`, `--instance`, `--revision`, `--container`,
`--follow/-f`, `--tail`, `--since`, `--max-log-requests`, and `--limit-bytes`.
An instance filter needs a component or a full scoped revision. A short
eight-hex revision hash needs a component. The default container is OME's main
container, falling back to the pod's first container.

The default tail is `-1` (all available lines), not a small recent window;
follow concurrency defaults to 5. `--limit-bytes` defaults to 0 (no limit),
applies per pod to one-shot reads, and cannot combine with `--follow`.
Use explicit tail/byte limits for automation. Log content is workload output,
not the bounded diagnostic-report schema.

## Guarded actions and dry-runs

All 11 actions default to `--dry-run=none`: they can write after confirmation.
Select a dry-run explicitly for a preview workflow.

| Option | Contract |
|---|---|
| `--dry-run=client` | Live safety reads, eligibility checks, preview, confirmation, and applicable revalidation; no PATCH. This is not an offline manifest render. |
| `--dry-run=server` | The guarded PATCH is submitted with Kubernetes `dryRun=All`; admission/RBAC still apply, but the change is not persisted. |
| `--dry-run=none` | Submit the guarded operation after confirmation. |
| `--yes` | Confirm without a terminal prompt. It does not bypass eligibility, bounds, or identity checks. |

An interactive prompt defaults to no. Noninteractive input requires `--yes`;
piping `yes` into stdin is not a substitute. Both dry-run modes also require
confirmation. Strong overrides such as `--override-autoscaler`,
`--override-analysis`, and `--discard-pending-actions` explicitly require
`--yes`.

Previews and prompts go to **stderr**. Successful action output is one
`cli.ome.io/v1alpha1` `ActionResult` on **stdout**, with `target`, `dryRun`,
`accepted`, `applied`, and applicable `requestID`, `revisionHash`, and
follow-up fields. Client dry-run never claims API acceptance; neither dry-run
claims application. Even `accepted=true` and `applied=true` describe the API
operation, not controller convergence.

Actions verify exact target identities and guard patches with UID and
resourceVersion checks. Related-object checks are not a transaction across
the service, runtime, replicas, and pods. Incomplete, stale, malformed, or
oversized safety evidence can refuse an action. An idle or ineligible fixture
may therefore refuse even a dry-run.

Actions do not automatically wait or replay requests. After an unknown outcome
or conflict, inspect current status and retained request evidence before
preparing another request. A previous preview is not a reusable authorization
token: each invocation reads and checks the current target again.

## Waiting for reported state

`wait SERVICE --for=PREDICATE` returns one final typed report. The timeout
defaults to **60 seconds** and must be positive and no greater than 24 hours.

| Predicate | Additional arguments and meaning |
|---|---|
| `condition=Ready` | Same as `condition=Ready=True`; also accepts explicit `False` or `Unknown`. Missing Ready is not Unknown. |
| `rollout=stable` | Qualified canonical reported state is `Succeeded`; NotConfigured/Staged do not match. |
| `rollout=failed` | Qualified reported state is `Failed`. |
| `rollout=rolled-back` | Qualified reported state is `RolledBack`. |
| `migration=terminal` | Requires canonical UUID `--request-id`; Completed, Failed, and Relocated are all terminal. |
| `replicas=ready` | Requires `--component` and nonnegative `--replicas`; exact current owned-IR ready count, including an explicitly observed zero. |
| `replicas=current` | Requires `--component` and positive `--replicas`; exact IR spec and reported logical count. Optional paired `--ir-name`/`--ir-uid` binds the original scale target. |
| `runtime-sync=acknowledged` | Requires the canonical v4 `--request-id` from runtime sync; exact token acknowledgment, eligible pin, and no reported RuntimeDrifted condition. |
| `held-revision=unheld` | Requires `--component`, full `--revision=SERVICE-COMPONENT-HASH`, and original `--ir-name`/`--ir-uid`; exact Held block and mailbox are absent. |

Ready and rollout use a named GET and exact-name WATCH with polling fallback.
Migration, replica, runtime-sync, and Held-revision waits poll bounded reads
every five seconds. They remain bound to the original object identity; deletion
or replacement does not count as success.

Copy action request IDs and target identities from the actual JSON result:

```sh
kubectl ome wait chat -n team-a --for=migration=terminal \
  --request-id "$migration_request_id" --timeout=5m -o json
kubectl ome wait chat -n team-a --for=replicas=current \
  --component engine --replicas=2 --ir-name "$scale_ir_name" \
  --ir-uid "$scale_ir_uid" --timeout=2m -o json
kubectl ome wait chat -n team-a --for=runtime-sync=acknowledged \
  --request-id "$sync_request_id" --timeout=2m -o json
```

A matching wait proves its stated reported-state predicate on the same object.
Migration terminal may mean failure; replica count need not mean readiness;
Ready does not prove current-spec convergence. Acknowledgment or block absence
does not prove that a preceding action caused the state.

## Human output and automation

Most diagnostics accept `-o table|wide|json|yaml`, defaulting to `table`.
Compact tables clip or wrap evidence for terminal use; `wide` expands safe
details but is not guaranteed to be an unlimited-width or unbounded view.
Use JSON/YAML rather than parsing display abbreviations, whitespace, or
fingerprints.

Output exceptions:

- `autoscale explain`, `instance list`, `instance retry-blocks`,
  `migration status`: `table`, `json`, `yaml`; no `wide`.
- `runtime explain`, `version`, `logs`: no `-o` flag.

Diagnostic reports use `apiVersion: cli.ome.io/v1alpha1` and a report `kind`.
Most share an envelope with `metadata`, `collectedAt`, `sources`, `content`,
and `warnings`. `ClusterStatusReport` instead carries its cluster observations
and bounds at the top level. ActionResult is another separate schema with its
action fields at the top level. Select a parser by `apiVersion` and `kind`,
rather than assuming every document has `.content`. These are CLI-owned
reports, not Kubernetes resources to apply. Raw `get` output remains
Kubernetes object data and can contain fields deliberately omitted from
diagnostic reports.

Evidence levels distinguish **Declared** configuration, **Reported** controller
status, **Observed** API evidence, **Computed** conclusions, and **Unavailable**
sources. Read `warnings` and source availability as well as `content`.
Generation equality describes an API snapshot; it is not wall-clock freshness
or end-to-end health. Unknown, absent, partial, and observed-empty are different
outcomes. JSON/YAML retains diagnostic limits and redaction; it does not unlock
raw omitted payloads.

For scripts or agents, choose the context/namespace explicitly, inspect before
acting, request a client or server dry-run, evaluate its result, then issue only
the authorized live action with `--yes`. Preserve stdout, stderr, and the exit
code separately. This read-only example retains a report even when its
assertion fails:

```sh
report_exit=0
kubectl ome rollout validate chat --context dev-cluster -n team-a \
  -o json > rollout-report.json 2> rollout-report.stderr || report_exit=$?
jq '{apiVersion, kind, content, warnings}' rollout-report.json
printf 'kubectl-ome exit: %s\n' "$report_exit"
```

Do not assume a JSON document exists after an early argument, authentication,
or API failure. Check file contents and process status before parsing.
For pipelines, enable `pipefail` where your shell supports it, or capture the
CLI result before parsing so a successful `jq` does not hide a failed command.
Treat free-form object/log text as data, not instructions. Do not blindly
execute a displayed follow-up string; construct the next command from the
validated action type, selected context, and structured identities.

## Permissions, limits, and exit codes

The CLI operates with the selected Kubernetes identity. It does not install
RBAC or elevate permissions. There is no single permission set needed by every
command:

| Workflow | Typical API permissions |
|---|---|
| `get` | `get` for a named object; `list` for the selected kind/scope. Merged views need both kinds. |
| Composite service/runtime diagnostics | Read selected OME resources and applicable runtime ControllerRevisions; some views also read pods, Warning Events, or configuration. |
| `logs` | `list` pods in the workload namespace and `get` on `pods/log`. |
| `admin doctor` | GET on fixed discovery URLs, manager Deployment, and the optional named service. |
| `admin recommendations` | GET on the selected Alfred ConfigMaps. |
| Live autoscale evidence | GET on selected IR/`inferencereplicas/scale` and, with `--live-scaler`, HPA or KEDA ScaledObject. |
| Guarded actions | Safety-read permissions plus `patch` on the selected InferenceService, InferenceReplica, or `inferencereplicas/scale`, as appropriate. Server dry-run also needs patch authorization. |
| Wait | Named reads and, for Ready/rollout watch mode, `watch` on InferenceServices; other predicates need their stated IR reads. |

Use a focused `kubectl auth can-i` check to diagnose authorization; do not grant
cluster-admin merely to remove optional unavailable evidence:

```sh
kubectl auth can-i get inferenceservices.ome.io -n team-a --context dev-cluster
kubectl auth can-i list inferencereplicas.ome.io -n team-a --context dev-cluster
kubectl auth can-i get pods --subresource=log -n team-a --context dev-cluster
```

Commands collect bounded evidence rather than promising an exhaustive cluster
scan. Examples of the current bounds:

- `get` follows pages of 500 until complete; it restarts once after an expired
  continuation and discards the incomplete snapshot.
- `status` has a 30-second command budget, at most 1,000 pods/two pages, and
  100 Warning Events across at most 16 targets.
- Runtime selection uses at most 1,000 candidates/four pages;
  runtime history reads at most 1,000 revisions/two pages.
- `cluster status` lists at most 64 objects/two pages; quota topology admits
  at most 1,000 objects/two pages within a 10-second command budget.
- Doctor caps the command at 30 seconds and each request at 10 seconds.
- Guarded actions have a 45-second budget, including confirmation, with each
  API request capped at 10 seconds while retaining shorter request timeouts.
- Logs discovery admits at most 200 pods; follow streams also honor
  `--max-log-requests`.

Some diagnostics keep partial evidence with explicit warnings. Operations
requiring a complete snapshot, including quota topology and action eligibility,
refuse incomplete evidence. Many limits apply after JSON decoding; they are
not universal network response-size or memory guarantees. There is no global
flag to disable these limits. Credential plugins or custom transports may not
honor cooperative cancellation. Ctrl-C/SIGTERM cancels the command; a second
signal uses normal process signal handling.

| Exit | Meaning |
|---|---|
| `0` | Command completed; or the requested assertion/wait matched. Diagnostic success can include optional unavailable evidence. |
| `1` | Argument/configuration, API, output, confirmation, or cancellation failure. |
| `2` | Explicit assertion unmet: wait timeout/absence/deletion/replacement, invalid or unverifiable rollout validation, quota violations, or doctor-proven missing required API. |
| `3` | Typed guarded-mutation precondition conflict. Reinspect before preparing a fresh request. |

Not every safety refusal is exit 3: local eligibility failures can return 1.
A matching `migration=terminal` returns 0 even for a reported Failed outcome;
read the report. A nonzero result or ambiguous transport outcome is not a
reason to replay a mutation automatically. The main entry point writes one
`error: ...` line to stderr for returned errors.

## Troubleshooting

| Symptom | Next check |
|---|---|
| `kubectl` does not recognize `ome` | Confirm the executable is named `kubectl-ome`, executable, and on `PATH`; inspect `kubectl plugin list`. |
| Service not found | Check `--context`, `--kubeconfig`, and workload `-n`; `--ome-namespace` does not select the workload. |
| Optional report source is Forbidden/Unavailable | Read its source/warning details and check only the relevant API permission or optional CRD. |
| `version` shows operator unknown | Check the manager Deployment name/namespace and read authorization; client version output alone is not proof of cluster health. |
| Ready appears missing rather than Unknown | The condition was not recorded; `condition=Ready=Unknown` requires an explicit Unknown record. |
| `--output wide` or `--ome-namespace` is rejected | Consult the command-specific output exceptions and namespace list above. These flags are not universal. |
| Action refuses in client dry-run | Safety reads and eligibility checks still run. Inspect rollout, runtime, instance, and migration evidence for the exact target. |
| Confirmation fails in a pipeline | Use an explicit `--yes` only for the authorized operation; piped input cannot confirm. |
| Scale is refused despite autoscaler class None | Supply both required transient-override flags and a positive count; inspect bounds and current target with `autoscale status --live-scale`. |
| Wait times out after an accepted action | Read the final report and corresponding status. API acceptance and the requested reported-state predicate are separate. |
| Request outcome is unknown or exit 3 occurs | Inspect current target identity/status and existing mailboxes or history; do not replay automatically. |
| History/tree is truncated or collection exceeds bounds | Use a named diagnostic where supported, narrow workload scope, and retain explicit incomplete evidence. Increasing wait timeout does not enlarge collection bounds. |
| Log follow exceeds its concurrency bound | Narrow by component/instance/revision or deliberately raise `--max-log-requests`. |

## Development and verification

Run the focused CLI suites from the repository root:

```sh
go test ./cmd/kubectl-ome ./pkg/cli/...
make kubectl-ome
./bin/kubectl-ome --help
./bin/kubectl-ome rollout repin --help
```

The CLI suites include command registration, flags, typed reports, fake API
clients, bounded transport, pagination, confirmation, waits, and mutation
preconditions. Help/build checks need no cluster. Follow
[CONTRIBUTING.md](../../CONTRIBUTING.md) and repository checks for code changes;
the focused suite does not replace required project quality gates.

For live validation, record the CLI commit, actual controller image, context,
namespace, fixture names, stdout/stderr, and exit codes. Start with read-only
commands and isolated fixtures. Test action previews with both dry-run modes
before an explicitly authorized write. Verify controller state separately
after acceptance, and distinguish an intended safety refusal from a successful
operational workflow. Never put kubeconfig contents, credentials, or Secret
payloads into a transcript.

## Captured Moirai examples

These are **2026-09-25 observations**, not promises about current cluster state.
The manually dispatched [nightly build run 36159961578](https://github.com/ome-projects/ome/actions/runs/36159961578)
used `express=true` on `main` at commit
`8d9f609d36fe59389b67617e932bbb16186b45cb`. The deployed amd64 image tag was
`dev-8d9f609-amd64`; the published chart version was
`0.0.0-dev.20260925162646+8d9f609`. Helm release revisions were OME 19,
scheduler 6, and Alfred 8. Fourteen CRDs, including TrafficMap, were upgraded
separately and retained their existing non-Helm ownership.

The control plane used that nightly build. The CLI was built locally from the
same base with the CLI fixes in this checkout. In particular, the
runtime-sync example below uses the fix for disabled inheritance profiles.
Commands are written in plugin form instead of the temporary binary paths
used by the recorder. Set `KUBECONFIG` to your authorized configuration and
retain explicit context and namespace selection. Do not copy the historical
fixture names or mutation targets into another environment without inspection.

### Discovery and logical instances

```sh
kubectl ome get isvc --context dev-fra -n ome-cli-verify-20260925 -o table
```

Captured stdout, exit 0; this redirected table includes full URLs:

```text
NAME           MODEL   RUNTIME                READY   URL                                                                  AGE
cli-native     -       cli-native-runtime     True    http://cli-native.ome-cli-verify-20260925.svc.cluster.local:8080     3m33s
cli-portable   -       cli-portable-runtime   True    http://cli-portable.ome-cli-verify-20260925.svc.cluster.local:8080   9m32s
cli-traffic    -       cli-portable-runtime   True    http://cli-traffic.ome-cli-verify-20260925.svc.cluster.local:8080    9m29s
```

The following projection is extracted from the captured instance report after
a completed migration:

```sh
kubectl ome instance list cli-native --context dev-fra \
  -n ome-cli-verify-20260925 -o json \
  | jq '.content.instances[0] | {component,index,phase,runningRevision,pods}'
```

```json
{
  "component": "engine",
  "index": 1,
  "phase": "Ready",
  "runningRevision": "cli-native-engine-f491b2e9",
  "pods": {
    "total": 1,
    "serving": 1,
    "available": 1
  }
}
```

The full report also records `indexSet: Sparse` and a `SparseIndices` issue.
One ready instance does not imply its logical index is zero. Inspect returned
indices before running `instance status` or `migration start`.

### Inheritance and runtime drift

Before runtime sync, the portable service had a valid managed pin whose active
revision differed from the live inherited runtime. This is an exact `jq`
projection of that captured report:

```sh
kubectl ome runtime effective cli-portable --context dev-fra \
  -n ome-cli-verify-20260925 -o json \
  | jq '.content | {pinMode:.pin.mode,drift:.pin.reportedDrift,
      activeHash:.active.hash,liveHash:.live.hash,liveToActive}'
```

```json
{
  "pinMode": "ManagedPin",
  "drift": {
    "state": "ReportedTrue",
    "cause": "RevisionMismatch"
  },
  "activeHash": "4f4202ce",
  "liveHash": "ebef1f64",
  "liveToActive": "Different"
}
```

The tree command makes the inheritance and explicit users visible:

```sh
kubectl ome runtime tree cli-cpu-base --kind ServingRuntime \
  --context dev-fra -n ome-cli-verify-20260925
```

Excerpt from the recorded 80-column terminal output, with terminal framing
removed. This capture renders each head's path independently, so shared
ancestors repeat; these are not duplicate runtime objects:

```text
RUNTIME TREE
Target: ServingRuntime/ome-cli-verify-20260925/cli-cpu-base
Context: Namespaced/ome-cli-verify-20260925 (resolution: Complete)
Head: ServingRuntime/cli-cpu-base
ServingRuntime/cli-cpu-base [selected]
Head: ServingRuntime/cli-cpu-profile
ServingRuntime/cli-cpu-base [selected]
`-- ServingRuntime/cli-cpu-profile
Head: ServingRuntime/cli-native-runtime
ServingRuntime/cli-cpu-base [selected]
`-- ServingRuntime/cli-cpu-profile
    `-- ServingRuntime/cli-native-runtime
        `-- InferenceService/cli-native
Head: ServingRuntime/cli-portable-runtime
ServingRuntime/cli-cpu-base [selected]
`-- ServingRuntime/cli-cpu-profile
    `-- ServingRuntime/cli-portable-runtime
        |-- InferenceService/cli-portable
        `-- InferenceService/cli-traffic
Snapshot: Complete
```

Both profile ancestors were deliberately disabled; the leaf explicitly enabled
the merged runtime. Successful `runtime sync` client/server previews were
followed by one authorized live request. A separate acknowledgment wait, using
the exact UUID returned by that request, produced this captured projection:

```sh
# For a new operation, use its returned requestID, not this historical UUID.
kubectl ome wait cli-portable --context dev-fra -n ome-cli-verify-20260925 \
  --for=runtime-sync=acknowledged \
  --request-id=d43704fd-2386-45c9-a06f-706622bdcb4b --timeout=45s -o json \
  | jq '.content | {requested,outcome,reason,runtimeSync}'
```

```json
{
  "requested": "RuntimeSync=Acknowledged",
  "outcome": "Matched",
  "reason": "RuntimeSyncObserved",
  "runtimeSync": {
    "requestID": "d43704fd-2386-45c9-a06f-706622bdcb4b",
    "tokenState": "Acknowledged",
    "driftState": "Clear",
    "pinState": "Managed",
    "placementState": "Direct",
    "validity": "Valid",
    "inspectedConditions": 3,
    "generationFreshness": "Unverifiable"
  }
}
```

This wait exited 0. A separate `--for=condition=Ready` wait also matched;
the acknowledgment report's global generation freshness remains explicitly
`Unverifiable`, not silently upgraded to current.

### Traffic: acceptance is not convergence

The fixture used an intentionally nonexistent workload-cluster name. It tested
the guarded annotation-request protocol, not remote traffic movement. Client
dry-run, server dry-run, and the authorized live drain each exited 0:

| Captured mode | `accepted` | `applied` |
|---|---|---|
| `client` | `false` | `false` |
| `server` | `true` | `false` |
| `none` | `true` | `true` |

The live command was:

```sh
kubectl ome traffic drain cli-traffic --context dev-fra \
  -n ome-cli-verify-20260925 \
  --workload-cluster cli-traffic-no-cluster-20260925-0qlhzi \
  --id cli-fixture-drain --reason 'isolated CLI validation' --yes -o json
```

Selected fields from its captured `ActionResult` (other top-level fields
omitted, values unchanged):

```json
{
  "action": "traffic drain",
  "target": {
    "kind": "InferenceService",
    "namespace": "ome-cli-verify-20260925",
    "name": "cli-traffic",
    "uid": "9d3030b0-9737-4997-a55b-3712942d3be7",
    "resourceVersion": "1284688372"
  },
  "dryRun": "none",
  "accepted": true,
  "applied": true,
  "message": "API accepted traffic annotation request; not TrafficMap convergence.",
  "traffic": {
    "overrideID": "cli-fixture-drain",
    "cluster": "cli-traffic-no-cluster-20260925-0qlhzi",
    "overridesBefore": 0,
    "overridesAfter": 1
  }
}
```

The subsequent `traffic status` still reported `state: Unavailable` with
`TrafficStatusMissing`. The matching live `traffic undrain` was accepted and
applied, changing `overridesBefore: 1` to `overridesAfter: 0`. Neither response
proves TrafficMap convergence or a remote endpoint change.

### Coverage and limits of these observations

The final rebuilt-CLI read-only matrix had 295 expected exits, 110 parse-checked
JSON/YAML documents, and 90 terminal captures at 80/60 columns without width
failures. Its 18 expected exit-2 results included negative waits, empty-quota
validation, and unverifiable rollout validation. Empty or unavailable evidence
counts as API/output coverage, not successful feature execution.

Separate isolated-fixture runs observed native scale `1 → 2 → 1`, a migration
reported `Completed` with its exact-request wait matched, and the portable
runtime acknowledgment/readiness above. Native repin and promotion also passed
client dry-run, server dry-run, and authorized live submission. After the
manual-gated canary plan's first-step traffic changed from 50% to 25%, repin's
follow-up explanation reported `planDrift.state: False`, `reason: InSync`,
and observed traffic 25%. Promotion of target `bd999874` from stable
`f491b2e9` was followed by history reporting `outcome: Completed` and
`activeRuns: 0`. That history remained `Partial`/`RetentionBounded` with
`EpochUnverifiable`; it was not a complete rollout audit.

Native rollback subsequently passed client dry-run, server dry-run, and live
submission. History reported `outcome: RolledBack`, closed at
`2026-09-25T17:14:02Z`, with `activeRuns: 0`; logical instance 1 was then
reported Ready on stable revision `cli-native-engine-bd999874`. The same
retention and epoch caveats applied. However, the explicit
`wait --for=rollout=rolled-back` returned exit 1 with
`error: RolloutInspectionLimit`; rollback-wait success was not verified.
Retry-block evidence was empty with
`held: 0`, so release-held had no applicable target; its success was not
established. These examples do not assert that all 43 commands or every
guarded-action success path completed successfully.

AutoscalerPolicy and RolloutPolicy gates remained disabled; there was no quota
manager or multi-cluster deployment. Alfred was recommend-only. Policy-gated
reconciliation, quota enforcement, remote traffic movement, and multi-cluster
convergence were not established by this validation.

## Source references

- [Command registration](../../pkg/cli/root.go) and
  [command implementations](../../pkg/cli/cmd/).
- [Resource registry](../../pkg/cli/cmd/get/registry.go) and
  [AutoscalerPolicy registration](../../pkg/cli/cmd/get/autoscalerpolicy.go).
- [Namespace options](../../pkg/cli/namespace/options.go),
  [report schemas](../../pkg/cli/report/v1alpha1/), and
  [exit-code contract](../../pkg/cli/exitcode/exitcode.go).
- [Confirmation](../../pkg/cli/mutate/confirmation.go),
  [action result](../../pkg/cli/report/v1alpha1/action_result.go), and
  [wait predicates](../../pkg/cli/cmd/wait/wait.go).
