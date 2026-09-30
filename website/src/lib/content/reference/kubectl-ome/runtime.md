---
title: kubectl ome runtime
description: Explain which runtimes match a model, and show the effective runtime, revision history and inheritance tree behind an InferenceService.
since: v1.3
---

`kubectl ome runtime` shows how OME picks the [serving runtime](../../concepts/runtimes/serving-runtimes.md) for a model or an [InferenceService](../../concepts/serving/inference-services.md), and what that runtime is made of. `explain` runs OME's [runtime selection](../matching/runtime-selection-scoring.md) and says why each runtime qualifies or doesn't. `effective` shows the runtime an InferenceService uses and the [revision](../../concepts/runtimes/runtime-revisions.md) it's pinned to, and `history` lists the revisions kept for that runtime. `tree` shows a runtime's [inheritance chain](../../concepts/runtimes/runtime-inheritance.md), the runtimes that inherit from it, and the InferenceServices that name them. The alpha `sync` action asks the controller to move a pinned InferenceService to its runtime's current spec.

```text
kubectl ome runtime explain (--model NAME | --isvc NAME) [flags]
kubectl ome runtime effective INFERENCESERVICE [flags]
kubectl ome runtime tree RUNTIME [flags]
kubectl ome runtime history INFERENCESERVICE [flags]
kubectl ome runtime sync INFERENCESERVICE [flags]
```

| Subcommand | What it does |
| --- | --- |
| [`explain`](#explain) | Lists the runtimes OME can choose for a model, in rank order, and why the others don't qualify. |
| [`effective`](#effective) | Shows the runtime an InferenceService uses, and the revision it's pinned to. |
| [`tree`](#tree) | Shows a runtime's inheritance chain, the runtimes that inherit from it, and the InferenceServices that name them. |
| [`history`](#history) | Shows the revisions kept for an InferenceService's runtime. |
| [`sync`](#sync) | Alpha. Asks the controller to move a pinned InferenceService to its runtime's current spec. |

`effective`, `history` and `sync` take the name of one InferenceService, and so does `explain --isvc`. The CLI looks for it in the namespace you give with `-n`, then in the namespace of your kubeconfig context, then in `default`. `explain --model` looks in that namespace for a BaseModel with the name, then for a ClusterBaseModel. `tree` takes the name of a runtime: a ServingRuntime in that namespace, or a ClusterServingRuntime.

`effective`, `history` and `sync` read the InferenceService, its model, the runtime it names with the runtimes that runtime inherits from, and runtime revisions in the OME namespace. When the InferenceService names no runtime, `effective` and `history` also list the ServingRuntimes in its namespace and the ClusterServingRuntimes, and select one the way OME does. `effective` reads only the revisions named in `spec.runtime.revision` and `status.pinnedRevisionName`, while `history` and `sync` list every revision of the runtime. `sync` also lists the InferenceService's InferenceReplicas when a component runs on OMENative. `explain` reads the model, or the InferenceService and its model, and lists the ServingRuntimes in the namespace and the ClusterServingRuntimes. `--with-effective` adds what `effective` reads. `tree` lists runtimes and InferenceServices, and reads no revisions. The [kubectl-ome overview](overview.md) lists the RBAC each command needs, and [Guarded actions](guarded-actions.md) has the rule `sync` adds.

`effective` and `history` print a table, and `tree` prints a tree. For all three, `-o wide` prints more detail, and `-o json` and `-o yaml` print the whole report, with `apiVersion: cli.ome.io/v1alpha1` and the kind `RuntimeEffectiveReport`, `RuntimeHistoryReport` or `RuntimeTreeReport`. `sync` prints its result, an ActionResult, as a table, or in full with `-o json` and `-o yaml`. Its `-o wide` prints the same table. On a terminal narrower than a table, the CLI wraps long cells, or prints each row as a list of fields when even the headers don't fit. Output to a file or pipe keeps one line per row. `explain` has no `-o` flag: it prints one table, which it always wraps to fit 80 columns, or a narrower terminal.

## `explain`

```text
kubectl ome runtime explain (--model NAME | --isvc NAME) [flags]
```

`explain` runs the runtime selection that OME runs for an InferenceService that names no runtime, and prints a row for every ServingRuntime in the namespace and every ClusterServingRuntime. With `--model`, it matches the runtimes against a BaseModel or ClusterBaseModel. With `--isvc`, it matches them against the InferenceService's model, and also checks the accelerator class and the deployment modes the InferenceService asks for. It takes the model's name from `spec.model.name`, and looks the model up the same way as `--model`, whatever `spec.model.kind` says.

When the InferenceService names a runtime in `spec.runtime`, OME doesn't run selection for it, as [When you name a runtime](../../concepts/runtimes/serving-runtimes.md#when-you-name-a-runtime) describes. `explain` still prints the table, and writes `Note: spec.runtime is explicit; automatic selection below is hypothetical.` to stderr.

### Flags {#explain-flags}

| Flag | Default | Description |
| --- | --- | --- |
| `--isvc` | None | Explain runtime selection for this InferenceService's model |
| `--model` | None | Explain runtime selection for this BaseModel/ClusterBaseModel |
| `--ome-namespace` | `ome` | Namespace where the OME control plane is installed |
| `--with-effective` | `false` | Append bounded effective context for --isvc (auto scan: 1,000 items/2 pages) |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

Give exactly one of `--model` and `--isvc`, or the command fails with `exactly one of --model or --isvc is required`. `--with-effective` without `--isvc` fails with `--with-effective requires --isvc`. `--ome-namespace` matters only with `--with-effective`, which reads runtime revisions there.

### Output fields {#explain-output-fields}

| Column | What it shows |
| --- | --- |
| RUNTIME | The runtime's name. |
| SCOPE | `Namespaced` for a ServingRuntime, `Cluster` for a ClusterServingRuntime. |
| COMPATIBLE | `Yes` when automatic selection can choose the runtime, `No` when it can't. |
| PRIORITY | For a `Yes` row, the `priority` of the runtime's first `supportedModelFormats` entry that matches the model, or `1` when the entry sets none. `-` for a `No` row. |
| WEIGHT | For a `Yes` row, that entry's `modelFormat` weight plus its `modelFramework` weight, each times the priority, leaving out weights of 0 or less. `-` for a `No` row. |
| REASON | For a `No` row, why selection can't choose the runtime. `-` for a `Yes` row. |

`Yes` rows come first, in the order OME [ranks them](../matching/runtime-selection-scoring.md#the-ranking-order), so the first `Yes` row is the runtime OME would select. `No` rows follow, ServingRuntimes first, each group by name.

WEIGHT looks only at the first matching entry, and leaves out weights of 0 or less. The [score](../matching/runtime-selection-scoring.md#how-the-score-is-computed) OME ranks by takes the runtime's best entry, and counts a weight of 0 as 10 or 5. So WEIGHT equals the score only when the first matching entry gives the score and sets no weight of 0 or less. The command doesn't show the score itself.

A `No` row shows the first check the runtime fails, in this order:

| Reason | Cause |
| --- | --- |
| `runtime is disabled` | The runtime sets `disabled: true`. |
| `runtime does not support the required accelerator class` | With `--isvc`, the InferenceService asks for an accelerator class the runtime doesn't list. See [Runtime accelerator-class matching](../matching/runtime-accelerator-class-matching.md#where-a-rejection-shows-up). |
| `runtime engine deployment mode <mode> does not match requested engine deployment mode <mode>` | With `--isvc`, the runtime and the InferenceService both declare a deployment mode for the engine, and the modes differ. For the decoder, the message says `decoder`. See [Runtime deployment-mode matching](../matching/runtime-deployment-mode-matching.md#where-a-rejection-shows-up). |
| `model format '<label>' not in supported formats: <reasons>` | No `supportedModelFormats` entry matches the model. The message gives a reason for each entry, as [Model version matching](../matching/model-version-matching.md#mismatch-reasons) describes. A runtime with no entries gets `no supported formats defined` after the colon. |
| `model size <size> is outside supported range <range>` | The model's parameter count is outside the runtime's size range. See [Model size range matching](../matching/model-size-range-matching.md#where-a-rejection-shows-up). |
| `supports the model but has no supportedModelFormats[].autoSelect=true entry, so automatic selection skips it (pin it explicitly via spec.runtime.name instead)` | The runtime matches, but none of its entries sets `autoSelect: true`. An InferenceService can still name it. |
| `matching format <format> is not autoSelect-enabled (a different supportedModelFormats entry on this runtime has autoSelect=true, but not the one that matches this model)` | The runtime matches, and another entry sets `autoSelect: true`, but the runtime's score is 0 or less. The score leaves out entries that set `autoSelect: false`, so when the matching entry sets it, that's the cause. When the matching entry leaves `autoSelect` unset, the score counts it, and the cause is elsewhere in the score, such as a negative weight. Selection doesn't need the matching entry to set `autoSelect: true`. |
| `auto-select score is 0` | The runtime matches, and its matching entry sets `autoSelect: true`, but its score is 0 or less, as with a negative weight. |

REASON wraps to fit the table in 80 columns, and breaks inside a word when it has to.

If the InferenceService or the model doesn't exist, the command fails with the API server's error, such as `clusterbasemodels.ome.io "llama-3-2-3b-instruct" not found` when the name matches neither a BaseModel nor a ClusterBaseModel. An InferenceService without `spec.model` fails with `InferenceService "<name>" has no spec.model; pass --model instead`. `explain` reads at most 1,000 runtimes, the namespace's ServingRuntimes and the ClusterServingRuntimes together. With more, it fails with `runtime candidate snapshot exceeds the CLI collection limit`. When there are no runtimes at all, it prints no table, writes `No serving runtimes found in the selected namespace or at cluster scope.` to stderr, and exits `0`.

### Effective context {#explain-effective-context}

With `--isvc`, `--with-effective` adds a second table after a blank line and the heading `Effective context (separate observation; selector verdict unchanged):`. The CLI collects it separately from the selection above, and it doesn't change the verdicts. It starts with these rows:

| SCOPE | FIELD | What it shows |
| --- | --- | --- |
| `Service` | SELECTION | `Explicit` when `spec.runtime` names the runtime, `Selected` when the CLI selected it the way OME does. |
| `Service` | SOURCE | The runtime, as `CSR/<name>` for a ClusterServingRuntime or `SR/<namespace>/<name>` for a ServingRuntime. |
| `Source` | INHERITANCE | `Observed` when the CLI resolved the runtime's inheritance chain, `Unavailable` when it couldn't. |
| `Source` | REASON | Only when INHERITANCE is `Unavailable`: why, such as `NotFound` when the runtime or a parent is missing, or `Cycle` or `MaxDepthExceeded` for a chain OME doesn't accept. See [Chain limits](../../concepts/runtimes/runtime-inheritance.md#chain-limits). |
| `Source` | ROOT-FIRST | One row for each runtime in the chain, from the root to the runtime itself. |
| `Service` | CAVEAT | `Independent snapshot; not rollout convergence`. |

The rows that [`effective`](#effective-output-fields) prints follow. When the InferenceService names no runtime, the context selects one from at most 1,000 runtimes in all. With more, the Live view and INHERITANCE are `Unavailable`. If the CLI can't collect the context, it prints only two rows, `Service EVIDENCE Unavailable` and `Service CAVEAT Selector verdict above is unchanged`, and the command still exits `0`. If you interrupt the command, it prints no context and fails with the error instead. A read that times out doesn't fail it: the view that needs the read is `Unavailable`.

When there are no runtimes at all, `--with-effective` writes `No serving runtimes found for selector.` to stderr instead, and still prints the context.

### Examples {#explain-examples}

List the runtimes OME can choose for the BaseModel `llama-3-2-3b-instruct` in `team-a`:

```bash
kubectl ome runtime explain --model llama-3-2-3b-instruct -n team-a
```

```output
RUNTIME        SCOPE        COMPATIBLE   PRIORITY   WEIGHT   REASON
team-llama     Namespaced   Yes          1          2        -
vllm-llama     Cluster      Yes          2          6        -
srt-llama-1b   Cluster      No           -          -        model size 3.21B is
                                                             outside supported
                                                             range [500M, 2B]
vllm-base      Cluster      No           -          -        supports the model
                                                             but has no
                                                             supportedModelForma
                                                             ts[].
                                                             autoSelect=true
                                                             entry, so automatic
                                                             selection skips it
                                                             (pin it explicitly
                                                             via
                                                             spec.runtime.name
                                                             instead)
vllm-legacy    Cluster      No           -          -        runtime is disabled
```

OME would select the ServingRuntime `team-llama`, although its WEIGHT is lower, because every ServingRuntime in the namespace ranks above every ClusterServingRuntime. `team-llama`'s entry has weights of 1 and a priority of 1, so its WEIGHT is (1 + 1) × 1 = 2, and `vllm-llama`'s is (2 + 1) × 2 = 6. `srt-llama-1b` serves only models from 500M to 2B parameters. `vllm-base`, the runtime `vllm-llama` inherits from, matches the model, but none of its entries sets `autoSelect: true`, so OME uses it only for an InferenceService that names it. `vllm-legacy` is disabled.

Explain the selection for the InferenceService `llama-chat`, which serves the same model:

```bash
kubectl ome runtime explain --isvc llama-chat -n team-a
```

It prints the same table, since `llama-chat` asks for no accelerator class or deployment mode. `llama-chat` names the ClusterServingRuntime `vllm-llama` in `spec.runtime`, so OME never runs this selection for it, and the command writes `Note: spec.runtime is explicit; automatic selection below is hypothetical.` to stderr.

Add the effective context. The command prints the same table, then a blank line and this:

```bash
kubectl ome runtime explain --isvc llama-chat -n team-a --with-effective
```

```output
Effective context (separate observation; selector verdict unchanged):
SCOPE     FIELD           VALUE
Service   SELECTION       Explicit
Service   SOURCE          CSR/vllm-llama
Source    INHERITANCE     Observed
Source    ROOT-FIRST      CSR/vllm-base
Source    ROOT-FIRST      CSR/vllm-llama
Service   CAVEAT          Independent snapshot; not rollout convergence
Live      STATE           Available
Live      RUNTIME         CSR/vllm-llama
Live      HASH            3c18f4a0
Live      ENGINE          RawDeployment (Default)
Active    STATE           Available
Active    RUNTIME         CSR/vllm-llama
Active    REVISION        cr-vllm-llama-1baf272d
Active    HASH            1baf272d
Active    ENGINE          RawDeployment (Default)
Service   PIN             ManagedPin/Resolved
Service   SYNC            Absent
Service   STATUS          Current
Service   DRIFT           ReportedTrue/RevisionMismatch
Service   LIVE-RELATION   Different
```

`llama-chat` names `vllm-llama`, which inherits from `vllm-base`. The rest is what [`effective`](#effective-examples) shows for `llama-chat`.

## `effective`

```text
kubectl ome runtime effective INFERENCESERVICE [flags]
```

`effective` shows the runtime behind an InferenceService in two views, which [Live and pinned runtimes](../../concepts/runtimes/runtime-revisions.md#live-and-pinned-runtimes) explains:

- The Live view is the runtime as it's defined now: the one `spec.runtime` names, or the one OME selects for the model, merged with every runtime it [inherits from](../../concepts/runtimes/runtime-inheritance.md).
- The Active view is the runtime spec the InferenceService is meant to run: the revision it's pinned to, or the Live view when it isn't pinned.

The report also shows the pin, the runtime sync request and the RuntimeDrifted condition. It shows hashes of the specs, never the specs themselves. When `spec.runtime` names no runtime, the CLI selects one the way [`explain`](#explain) does, from at most 1,000 runtimes in all. With more, the Live view is `Unavailable`.

### Flags {#effective-flags}

| Flag | Default | Description |
| --- | --- | --- |
| `--ome-namespace` | `ome` | Namespace where the OME control plane is installed |
| `-o`, `--output` | `table` | Output format: table, wide, json or yaml |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

The CLI reads the runtime revisions in `--ome-namespace`.

### Output fields {#effective-output-fields}

The table has one row for each value: Live rows first, then Active rows, then Service rows. STATE always shows, and the other rows show only when they have a value.

| SCOPE | FIELD | What it shows |
| --- | --- | --- |
| `Live`, `Active` | STATE | `Available`, or `Unavailable` when the CLI can't produce the view. |
| `Live`, `Active` | REASON | Why the view is `Unavailable`. |
| `Live`, `Active` | RUNTIME | The runtime, as `CSR/<name>` or `SR/<namespace>/<name>`. |
| `Active` | REVISION | The revision the InferenceService is pinned to. |
| `Live`, `Active` | HASH | The short hash of the view's runtime spec. |
| `Live`, `Active` | ENGINE, DECODER, ROUTER | Each component's deployment mode and where it comes from, as `MODE (SOURCE)`. |
| `Service` | PIN | The pin mode and its state, as `<mode>/<state>`. |
| `Service` | SYNC | The state of the runtime sync request. |
| `Service` | STATUS | How `status.observedGeneration` compares with the InferenceService's generation. |
| `Service` | DRIFT | The RuntimeDrifted condition, as `<state>/<cause>`. |
| `Service` | LIVE-RELATION | How the Live hash compares with the Active hash. |
| `Service` | ISSUE | One row for each issue the CLI found. |

REASON is one of:

| Reason | Meaning |
| --- | --- |
| `NotConfigured` | The InferenceService names neither a runtime nor a model. |
| `NotFound` | The runtime, or for the Active view the revision, doesn't exist. |
| `Disabled` | The runtime, or for the Active view the revision's spec, is disabled. |
| `MalformedPayload` | The Active view can't be used: the pin is `InvalidIntent` or `RevisionInvalid`. |
| `Forbidden` | The API server refused the read, as forbidden or unauthorized. |
| `UnsupportedAPI` | The API server doesn't serve the resource. |
| `Unreadable` | The read failed for another reason. |

The component rows show each component the InferenceService has. The modes are `RawDeployment`, `OMENative`, `VirtualDeployment` and `MultiNode` (deprecated; [move to OMENative](../../guides/omenative/move-from-leaderworkerset.md)). The source says where the mode comes from, in the order OME checks them, as [How OME resolves the mode](../../concepts/architecture/deployment-modes.md#how-ome-resolves-the-mode) describes:

| Source | Where the mode comes from |
| --- | --- |
| `ServiceAnnotation` | The InferenceService's `ome.io/deploymentMode: VirtualDeployment` annotation, which applies to every component. |
| `ComponentAnnotation` | An `ome.io/deploymentMode` annotation on the component, from the InferenceService or its runtime. |
| `ServiceSpec` | `spec.deploymentMode`. |
| `LeaderWorkerShape` | The component sets a leader or workers, which makes it `OMENative`. The router never gets this source. |
| `Default` | Nothing sets a mode, so the component is a `RawDeployment`. |

The pin mode comes from `spec.runtime`, as [How pinning works](../../concepts/runtimes/runtime-revisions.md#how-pinning-works) describes:

| Mode | When |
| --- | --- |
| `AutoSync` | `spec.runtime` names no runtime, or leaves `autoSync` unset or `true`. The InferenceService follows the live runtime. |
| `ManagedPin` | `spec.runtime` names a runtime and sets `autoSync: false` without a `revision`. OME pins the InferenceService to a revision it chooses. |
| `ExplicitPin` | `spec.runtime` sets `autoSync: false` and a `revision`. |
| `InvalidPin` | `spec.runtime` sets `autoSync: false` and a `kind` that isn't `ServingRuntime` or `ClusterServingRuntime`. |

The pin state is one of:

| State | Meaning |
| --- | --- |
| `NotApplicable` | The mode is `AutoSync`, so there's no pin. |
| `AwaitingPin` | A managed pin that the controller hasn't reported in `status.pinnedRevisionName` yet. The Active view shows the live runtime. |
| `Resolved` | The CLI read the pinned revision, and can use it. |
| `DesiredReportedMismatch` | An explicit pin whose `spec.runtime.revision` differs from the `status.pinnedRevisionName` the controller reports. |
| `RevisionMissing` | The pinned revision doesn't exist. |
| `RevisionInvalid` | The pinned revision fails the CLI's checks, or its spec can't be merged with the InferenceService's components. |
| `RevisionDisabled` | The pinned revision's spec is disabled. |
| `Unavailable` | The live runtime is disabled or the CLI couldn't read it, or the CLI couldn't read the pinned revision. With `AutoSync`, or a managed pin not reported yet, a missing live runtime gives this state too. |
| `InvalidIntent` | The mode is `InvalidPin`. |

SYNC compares the `ome.io/runtime-sync` annotation with `status.lastRuntimeSyncToken`, where the controller records the last token it used, as [Roll forward to the latest runtime](../../concepts/runtimes/runtime-revisions.md#roll-forward-to-the-latest-runtime) describes:

| SYNC | Meaning |
| --- | --- |
| `Absent` | Neither is set. |
| `StatusOnly` | Only `status.lastRuntimeSyncToken` is set. |
| `Acknowledged` | Both hold the same token: the controller has used the last request. |
| `Pending` | The annotation holds a token the status doesn't: the controller hasn't moved the pin for it. |

STATUS compares `status.observedGeneration` with `metadata.generation`:

| STATUS | Meaning |
| --- | --- |
| `Unobserved` | `status.observedGeneration` isn't set. |
| `Current` | The two are equal. |
| `Stale` | `status.observedGeneration` is lower. |
| `Invalid` | `status.observedGeneration` is higher. |

STATUS compares two numbers and says nothing more: it doesn't tell you whether the controller has acted on the current spec. The controller doesn't record the InferenceService's generation in `status.observedGeneration`. It copies the `observedGeneration` of a component's workload, such as its Deployment, which counts that workload's own changes, and components on OMENative don't set it at all. So `Current` means only that the copied number equals the generation, `Stale` and `Invalid` that it's lower or higher, and `Unobserved` that nothing has set it, which is usual when every component runs on OMENative. `sync` doesn't rely on it: its preview shows the freshness as `Unverifiable`.

DRIFT shows the InferenceService's [RuntimeDrifted condition](../../concepts/runtimes/runtime-revisions.md#the-runtimedrifted-condition). OME sets the condition only to `True`, and removes it when the reason no longer applies. The state is one of:

| State | Meaning |
| --- | --- |
| `NotReported` | The InferenceService has no RuntimeDrifted condition. |
| `ReportedTrue`, `ReportedFalse`, `ReportedUnknown` | The condition's status is `True`, `False` or `Unknown`. |
| `Malformed` | The condition has another status, or appears more than once. When it appears more than once, DRIFT shows no cause. |

The cause is the condition's reason: `RevisionMismatch`, `RevisionMissing`, `SourceRuntimeMissing` or `RuntimeMismatch`, or `Other` for an empty or unknown reason.

LIVE-RELATION compares the full hashes of the Live and Active specs. It's `Equal` when they're the same, `Different` when they differ, `Ambiguous` when they differ but their eight-character short forms, which HASH shows, are the same, and `Unknown` when either view has no hash.

The ISSUE rows show these codes:

| Code | Meaning |
| --- | --- |
| `InvalidDeclaredKind` | `spec.runtime.kind` isn't `ServingRuntime` or `ClusterServingRuntime`. |
| `DeclaredCompatibilityMismatch` | The InferenceService names a runtime and follows it live, and the runtime doesn't pass OME's checks for the model. |
| `InheritanceUnavailable` | The CLI couldn't resolve the runtime's inheritance chain. |
| `LiveRuntimeUnavailable` | The live runtime is missing, disabled or unreadable. |
| `StatusUnobserved`, `StatusStale`, `StatusInvalid` | STATUS isn't `Current`. |
| `ReportedDriftConflict` | DRIFT is `Malformed`. |
| `ActiveRevisionUnreported` | The pin state is `AwaitingPin`. |
| `ActiveRevisionUnavailable` | The InferenceService is pinned, and the Active view is `Unavailable`. |
| `RevisionNotFound`, `RevisionUnavailable` | A revision doesn't exist, or the CLI couldn't read it. |
| `RevisionDisabled` | A revision's spec is disabled. |
| `RevisionNotOMEManaged`, `RevisionSourceMismatch` | A revision wasn't created by OME, or belongs to another runtime. |
| `RevisionHashInvalid`, `RevisionHashMismatch`, `RevisionNameMismatch`, `RevisionOrdinalUnexpected` | A revision's hash label is invalid or doesn't match its spec, its name isn't the one OME derives from its runtime and hash, or its revision number isn't 1. |
| `RevisionPayloadNonCanonical`, `RevisionDataObjectPresent`, `RevisionPayloadMalformed` | A revision's stored spec isn't in the form OME writes, or doesn't parse. |
| `RevisionIdentityMismatch`, `DuplicateRevision`, `ConflictingRevision` | The API server returned a revision other than the one asked for, or the same revision more than once, or two different revisions with the same name. |
| `DuplicateRevisionContent`, `RevisionHashCollision` | Two revisions hold the same spec, or different specs with the same short hash. |

A revision code shows the revision in parentheses, as in `RevisionNotFound(<revision>)`, and `effective` reports it only for the revisions that the InferenceService uses, requests in `spec.runtime.revision` or reports in `status.pinnedRevisionName`. [`history`](#history-output-fields) adds two codes of its own.

The table keeps each value within 54 characters. A long runtime name or namespace, revision name or issue keeps its start and end with `...` between them, followed by `#` and eight hex digits that tell it apart from similar names. Other long values keep their first 51 characters and end in `...`.

`-o wide` prints one row for each view and component, under VIEW, STATE, REASON, RUNTIME, REVISION, HASH, COMPONENT, MODE, MODE-SOURCE, PIN, PIN-STATE, SYNC, STATUS, DRIFT, LIVE-RELATION and ISSUES. It shows the runtime's full kind and name, and repeats the Service fields on each row. `-o json` and `-o yaml` add the runtime selection and the inheritance chain, which the tables leave out.

`effective` fails with `InferenceService name "<name>" is invalid: ...` for a name that isn't valid, with `get InferenceService: ...` when it can't read the InferenceService, and with `collect runtime evidence: ...` when it can't collect the rest.

### Examples {#effective-examples}

Show the runtime behind `llama-chat`:

```bash
kubectl ome runtime effective llama-chat -n team-a
```

```output
SCOPE     FIELD           VALUE
Live      STATE           Available
Live      RUNTIME         CSR/vllm-llama
Live      HASH            3c18f4a0
Live      ENGINE          RawDeployment (Default)
Active    STATE           Available
Active    RUNTIME         CSR/vllm-llama
Active    REVISION        cr-vllm-llama-1baf272d
Active    HASH            1baf272d
Active    ENGINE          RawDeployment (Default)
Service   PIN             ManagedPin/Resolved
Service   SYNC            Absent
Service   STATUS          Current
Service   DRIFT           ReportedTrue/RevisionMismatch
Service   LIVE-RELATION   Different
```

`llama-chat` names the ClusterServingRuntime `vllm-llama` with `autoSync: false` and no `revision`, so OME manages its pin, and the pinned revision `cr-vllm-llama-1baf272d` resolves. `vllm-llama` changed after OME pinned it: the Live view hashes to `3c18f4a0`, so LIVE-RELATION is `Different`, and OME reports RuntimeDrifted `True` with the reason `RevisionMismatch`. Nothing has set `ome.io/runtime-sync` yet. The engine is a `RawDeployment`, since nothing sets its mode. [`sync`](#sync) can move `llama-chat` to the live spec.

Show the same report with a row for each view and component:

```bash
kubectl ome runtime effective llama-chat -n team-a -o wide
```

```output
VIEW     STATE       REASON   RUNTIME                            REVISION                 HASH       COMPONENT   MODE            MODE-SOURCE   PIN          PIN-STATE   SYNC     STATUS    DRIFT                           LIVE-RELATION   ISSUES
Live     Available   -        ClusterServingRuntime/vllm-llama   -                        3c18f4a0   engine      RawDeployment   Default       ManagedPin   Resolved    Absent   Current   ReportedTrue/RevisionMismatch   Different       -
Active   Available   -        ClusterServingRuntime/vllm-llama   cr-vllm-llama-1baf272d   1baf272d   engine      RawDeployment   Default       ManagedPin   Resolved    Absent   Current   ReportedTrue/RevisionMismatch   Different       -
```

`llama-chat` has only an engine, so each view takes one row.

## `tree`

```text
kubectl ome runtime tree RUNTIME [flags]
```

`tree` shows how a runtime fits into [runtime inheritance](../../concepts/runtimes/runtime-inheritance.md): the chain of runtimes it inherits from, the runtimes that inherit from it, and the InferenceServices that name any of them in `spec.runtime`. It lists runtimes and InferenceServices, and prints no specs, status, labels, annotations or resourceVersions.

Without `--kind`, the CLI looks for a ServingRuntime with the name in the namespace and a ClusterServingRuntime with the name. When both exist, it fails with `runtime "<name>" is ambiguous; pass --kind ServingRuntime or --kind ClusterServingRuntime: runtime target is ambiguous: "<name>" matched 2 runtimes`. For a ServingRuntime, it lists the ClusterServingRuntimes, and the ServingRuntimes and InferenceServices in its namespace. For a ClusterServingRuntime, it lists the ServingRuntimes and InferenceServices in every namespace, since any of them can use it. Each list holds at most 1,000 objects, read in two pages of 500.

### Flags {#tree-flags}

| Flag | Default | Description |
| --- | --- | --- |
| `--kind` | None | Runtime kind: ServingRuntime or ClusterServingRuntime (auto-detected when omitted) |
| `-o`, `--output` | `table` | Output format: table, wide, json or yaml |
| `--show-unattributed-users` | `false` | Show unattributed and defensive duplicate-key evidence separately from runtime users |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

### Output fields {#tree-output-fields}

The tree starts with `RUNTIME TREE` and a `Target:` line, and prints its lines in contexts:

| Line | What it shows |
| --- | --- |
| `Target:` | The runtime you named, as `ServingRuntime/<namespace>/<name>` or `ClusterServingRuntime/<name>`. |
| `Context:` | `Cluster` for the ClusterServingRuntimes and the InferenceServices that name them, or `Namespaced/<namespace>` for the ServingRuntimes of one namespace and the InferenceServices that name them. It always ends in `(resolution: Complete)`. |
| `Head:` | The runtime a path ends at: the target, or a runtime that inherits from it. A context has a path for each such runtime. |
| `Issue:` | A problem in the chain: `ParentMissing`, `CycleDetected` or `MaxDepthExceeded`, with the runtime as `subject=` and its parent as `parent=`. |
| `Issue path:` | The runtimes the problem involves, joined by ` -> `. |
| `Snapshot:` | `Complete`, or `Partial` when the CLI couldn't list every InferenceService. |
| `Collection:` | One line for each list the CLI read: the kind, the scope (`Cluster`, `AllNamespaces` or `Namespace/<namespace>`), the status (`Complete`, `Truncated` or `Unavailable`), and the pages and items it got. |
| `Warning:` | A warning code, such as `PartialData`, `SourceUnavailable` or `Truncated`. |

Under each `Head:` line, the chain prints root first: the root runtime at the left, then on each line the runtime that inherits from the one above it, after `` `-- `` and indented four more spaces. The target has ` [selected]` after it. The InferenceServices that name the head follow it, one level deeper, after `|-- `, or `` `-- `` for the last one. Inside a `Namespaced` context, the names leave out the namespace.

The CLI attributes an InferenceService to a runtime by its `spec.runtime`. With `kind: ServingRuntime`, it looks only for a ServingRuntime in the InferenceService's namespace. With `kind: ClusterServingRuntime`, it looks for the ClusterServingRuntime, then for a ServingRuntime in the namespace. With no kind, it looks for a ServingRuntime in the namespace, then for the ClusterServingRuntime.

With `--show-unattributed-users`, the tree adds `Unattributed users (not attributed to runtime tree):` before the `Snapshot:` line, with the listed InferenceServices whose runtime the CLI can't tell from their spec. Each shows as `[not attributed] InferenceService/<namespace>/<name>`, then its state and reason:

| State and reason | Cause |
| --- | --- |
| `state=Unresolved reason=AutomaticSelection` | The InferenceService names no runtime, so OME selects one. |
| `state=Unresolved reason=RuntimeNotFound` | The runtime it names doesn't exist. A `declared runtime=<name>` line follows. |
| `state=Invalid reason=InvalidRuntimeName` | The name in `spec.runtime` isn't a valid runtime name. |
| `state=Ambiguous reason=DuplicateInferenceService` | The list returned the same InferenceService more than once, which the CLI checks for defensively. |

When there are none, the block shows `none observed`.

`tree` needs complete runtime lists. When a runtime list is truncated or fails, it fails with `runtime tree requires complete runtime evidence: <kind> collection is <unavailable or truncated> (pages=<n> items=<n>); restore list access or narrow the runtime scope, then retry`. When the InferenceService list is incomplete or fails, it prints the tree with `Snapshot: Partial` and warnings. It fails with `collect InferenceServices: ...` only when it runs out of time or you interrupt it.

The default output keeps each line within 80 columns. It moves the end of a long `Issue:` or `Collection:` line to an indented line below, and shortens a long name, adding a short fingerprint so that different names stay distinct. `-o wide` prints full names, except in the unattributed block. `-o json` and `-o yaml` print the RuntimeTreeReport.

`tree` also fails with `runtime name "<name>" is invalid: ...` for a name that isn't valid, with `unsupported runtime kind "<kind>" (supported: ServingRuntime, ClusterServingRuntime)` for another `--kind`, and with `project runtime inheritance tree: runtime target was not found: "<name>"` when the runtime doesn't exist.

### Examples {#tree-examples}

Show the tree around the ClusterServingRuntime `vllm-llama`:

```bash
kubectl ome runtime tree vllm-llama -n team-a
```

```output
RUNTIME TREE
Target: ClusterServingRuntime/vllm-llama
Context: Cluster (resolution: Complete)
Head: ClusterServingRuntime/vllm-llama
ClusterServingRuntime/vllm-base
`-- ClusterServingRuntime/vllm-llama [selected]
    |-- InferenceService/team-a/llama-chat
    `-- InferenceService/team-b/llama-batch
Context: Namespaced/team-a (resolution: Complete)
Head: ServingRuntime/team-llama
ClusterServingRuntime/vllm-base
`-- ClusterServingRuntime/vllm-llama [selected]
    `-- ServingRuntime/team-llama
        `-- InferenceService/llama-dev
Snapshot: Complete
Collection: ClusterServingRuntime scope=Cluster status=Complete pages=1 items=4
Collection: ServingRuntime scope=AllNamespaces status=Complete pages=1 items=1
Collection: InferenceService scope=AllNamespaces status=Complete pages=1 items=4
```

`vllm-llama` inherits from `vllm-base`, so each chain starts there. In the Cluster context, `llama-chat` in `team-a` and `llama-batch` in `team-b` name `vllm-llama`. The ServingRuntime `team-llama` inherits from `vllm-llama`, so it heads a path in `team-a`, where `llama-dev` names it. `vllm-llama` is a ClusterServingRuntime, so the CLI listed ServingRuntimes and InferenceServices in every namespace. The fourth InferenceService, `llama-auto` in `team-b`, names no runtime, so the tree leaves it out.

Show the InferenceServices the CLI can't attribute as well. The command prints the same tree, with these lines before `Snapshot: Complete`:

```bash
kubectl ome runtime tree vllm-llama -n team-a --show-unattributed-users
```

```output
Unattributed users (not attributed to runtime tree):
  [not attributed] InferenceService/team-b/llama-auto
  state=Unresolved reason=AutomaticSelection
```

Show the tree around the ServingRuntime `team-llama`:

```bash
kubectl ome runtime tree team-llama -n team-a
```

```output
RUNTIME TREE
Target: ServingRuntime/team-a/team-llama
Context: Namespaced/team-a (resolution: Complete)
Head: ServingRuntime/team-llama
ClusterServingRuntime/vllm-base
`-- ClusterServingRuntime/vllm-llama
    `-- ServingRuntime/team-llama [selected]
        `-- InferenceService/llama-dev
Snapshot: Complete
Collection: ClusterServingRuntime scope=Cluster status=Complete pages=1 items=4
Collection: ServingRuntime scope=Namespace/team-a
  status=Complete pages=1 items=1
Collection: InferenceService scope=Namespace/team-a
  status=Complete pages=1 items=2
```

For a ServingRuntime, the CLI lists only in its namespace. `llama-chat` names `vllm-llama`, not `team-llama`, so this tree leaves it out. The two longer `Collection:` lines continue on indented lines to stay within 80 columns.

## `history`

```text
kubectl ome runtime history INFERENCESERVICE [flags]
```

`history` lists the revisions of the runtime behind an InferenceService: the ControllerRevisions labeled `ome.io/runtime-of=<runtime name>` in the OME namespace, newest first. It finds the runtime the way `effective` does. OME deletes unused revisions beyond the newest ones, as [Garbage collection](../../concepts/runtimes/runtime-revisions.md#garbage-collection) describes, so the list shows what OME kept. The CLI lists at most 1,000 revisions, in two pages of 500.

Runtimes with the same name share the label, so revisions of a same-named runtime in another namespace or scope show up too, and fail the check with `RevisionSourceMismatch`.

### Flags {#history-flags}

| Flag | Default | Description |
| --- | --- | --- |
| `--ome-namespace` | `ome` | Namespace where the OME control plane is installed |
| `-o`, `--output` | `table` | Output format: table, wide, json or yaml |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

### Output fields {#history-output-fields}

The table keeps each line within 80 columns:

| Column | What it shows |
| --- | --- |
| WINDOW | What the CLI saw, as `OBS/BOUND/SEEN/ASKED`. OBS is `C` when the list is complete, `P` when it's partial, and `U` when it's unavailable. BOUND is `B` when the list is complete within what OME keeps, and `I` when it's incomplete. SEEN is the number of pages the API server returned, and ASKED the number of list requests the CLI sent. |
| REVISION | The revision's name, when it fits in 14 characters. A longer name shows its first two characters, `...`, `#` and eight hex digits of a digest of the full name. The digest only tells names apart: it isn't the runtime hash. |
| CREATED | When the revision was created, as `YY-MM-DDTHH:MMZ`. |
| ROLES | `A` when the revision is the Active view, `Q` when `spec.runtime.revision` names it, `R` when `status.pinnedRevisionName` names it, and `H` for every listed revision. |
| CHECK | `OK` when the revision is consistent with what OME writes, `BAD` when it isn't, and `?` when the CLI can't tell. |
| LIVE | `MATCH` when the revision holds the live spec, `DIFF` when it doesn't, `AMB` when only the short hash matches, and `?` when the CLI can't tell. |
| ISSUES | The number of issues for the revision and for the whole report, as `R<n>/G<n>`, with `9+` for more than nine. |

The command's help also lists `N` for OBS, which `history` never prints, since it always asks for the list. When the CLI lists no revisions, as when it can't find a runtime name, the table has one row with the window, `-` in the next five columns, and the issues.

Use `-o wide` for the name to put in `spec.runtime.revision`. It prints OBSERVATION, COMPLETENESS, PAGES, REVISION, CREATED, HASH, ROLES, SOURCE, CONSISTENCY, RELATION, REVISION-ISSUES and REPORT-ISSUES, with full names, times and hashes, and the issue codes that [`effective`](#effective-output-fields) uses. It adds two report issues: `HistoryUnavailable` when the list failed, and `HistoryTruncated` when it holds more than the CLI read. `-o json` and `-o yaml` print the RuntimeHistoryReport.

`history` fails the same ways as [`effective`](#effective-output-fields).

### Examples {#history-examples}

List the revisions of the runtime behind `llama-chat`:

```bash
kubectl ome runtime history llama-chat -n team-a
```

```output
WINDOW    REVISION         CREATED           ROLES   CHECK   LIVE    ISSUES
C/B/1/1   cr...#1c63d586   26-09-20T14:30Z   H       OK      MATCH   R0/G0
C/B/1/1   cr...#fd54e47d   26-09-01T08:00Z   ARH     OK      DIFF    R0/G0
```

The CLI read one page with one request, and the list is complete within what OME keeps. The newer revision holds the live spec. The older one is the revision `llama-chat` is pinned to: the Active view, and the one `status.pinnedRevisionName` reports. Both pass the check. The names are too long for the column, so the table shows digests of them.

Show the full names and hashes:

```bash
kubectl ome runtime history llama-chat -n team-a -o wide
```

```output
OBSERVATION   COMPLETENESS       PAGES   REVISION                 CREATED                HASH       ROLES                     SOURCE                             CONSISTENCY   RELATION          REVISION-ISSUES   REPORT-ISSUES
Complete      RetentionBounded   1/1     cr-vllm-llama-3c18f4a0   2026-09-20T14:30:00Z   3c18f4a0   History                   ClusterServingRuntime/vllm-llama   Consistent    MatchesLive       -                 -
Complete      RetentionBounded   1/1     cr-vllm-llama-1baf272d   2026-09-01T08:00:00Z   1baf272d   Active,Reported,History   ClusterServingRuntime/vllm-llama   Consistent    DiffersFromLive   -                 -
```

`cr-vllm-llama-3c18f4a0` holds the live spec, with the hash `3c18f4a0` that `effective` shows for the Live view.

## `sync`

!!! note "Alpha"
    This command is alpha. Its flags and behavior can change between releases.

```text
kubectl ome runtime sync INFERENCESERVICE [flags]
```

`sync` rolls a pinned InferenceService forward to its runtime's live spec. It sets the `ome.io/runtime-sync` annotation to `cli-runtime-sync-` followed by a new request ID, a random UUID. When the controller uses the token, it pins the InferenceService to a revision that holds the merged live spec, and records the token in `status.lastRuntimeSyncToken`, as [Roll forward to the latest runtime](../../concepts/runtimes/runtime-revisions.md#roll-forward-to-the-latest-runtime) describes. It records the token only when it moves the pin. If the live spec matches the pinned revision again before the controller gets to the token, the controller clears the drift, doesn't record the token, and SYNC stays `Pending`. The next change to the live spec then moves the pin without a new request. The token asks for the live spec at the time the controller uses it: the annotation doesn't lock the spec that the preview showed.

Nothing removes the annotation. Once the controller records the token, the annotation and `status.lastRuntimeSyncToken` hold the same value, and the next `sync` replaces it. While a token waits, `sync` refuses, and OME moves the pin as soon as the live spec differs from the pinned revision. To withdraw a token that the controller hasn't recorded, remove the annotation:

```bash
kubectl annotate inferenceservice llama-chat -n team-a ome.io/runtime-sync-
```

```output
inferenceservice.ome.io/llama-chat annotated
```

SYNC then shows `StatusOnly` or `Absent`, and the pin stays where it is.

Unlike `kubectl annotate`, `sync` checks first that rolling forward is safe. It works only on an InferenceService that `effective` shows as `ManagedPin/Resolved`, with DRIFT `ReportedTrue/RevisionMismatch` and LIVE-RELATION `Different`. [Refusals](#sync-refusals) lists every check. [kubectl ome runtime sync](../../concepts/runtimes/runtime-revisions.md#kubectl-ome-runtime-sync) shows the command in use.

### Flags {#sync-flags}

| Flag | Default | Description |
| --- | --- | --- |
| `--dry-run` | `none` | Dry-run: none, client or server |
| `--ome-namespace` | `ome` | Namespace where the OME control plane is installed |
| `-o`, `--output` | `table` | Output: table, wide, json or yaml |
| `--yes` | `false` | Confirm the exact preview without an interactive prompt |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

### How the action runs {#how-the-action-runs}

`sync` changes the InferenceService in a guarded way:

1. The CLI checks the flags and the name, reads what the action needs, and refuses if the action isn't safe now. A refusal changes nothing and exits `1`.
2. It prints a preview of the exact change to stderr.
3. It asks `Confirm this exact action? [y/N]` on stderr, and goes ahead only on `y` or `yes`. `--yes` confirms without the prompt, and without a terminal you must pass it.
4. It reads the InferenceService, the runtimes, the revisions and the InferenceReplicas again. If anything it checked has changed, or it can't read it all again, it fails with `runtime sync safety snapshot changed; refresh runtime effective and retry explicitly` and exits `3`.
5. It sends a JSON Patch that tests the UID and resourceVersion from the preview, and any earlier `ome.io/runtime-sync` value, before it sets the annotation. If the InferenceService changed after the CLI read it, the API server rejects the patch, and the CLI fails with `guarded annotation patch rejected; refresh runtime effective and retry explicitly` and exits `3`.
6. It prints the result, an ActionResult, to stdout.

`--dry-run client` runs every check, prints the preview, asks for confirmation and reads everything again, then stops without sending the patch. `--dry-run server` sends the same patch with `dryRun=All`, so the API server runs its checks and stores nothing.

The command has 45 seconds in all, including the time the prompt waits for your answer, and each API request has at most 10 seconds, or less if you set a shorter `--request-timeout`. When the time runs out before the patch, the command fails with `context deadline exceeded` and exits `1`. Without `--yes`, it fails with `action not confirmed; noninteractive input requires --yes` when you answer anything but `y` or `yes`, press Ctrl-D, or run it without a terminal. Ctrl-C at the prompt fails with `context canceled`. A declined or interrupted prompt sends no patch.

The preview starts with `ALPHA runtime sync preview (acceptance is not convergence)`. It lists the context, the namespaces, and the InferenceService with its UID, resourceVersion, generation and reported generation. It then lists the pinned revision, each runtime in the chain, root first, with its UID, resourceVersion and generation, and the revision's UID and resourceVersion. It ends with the current and target hashes, the revision the CLI expects OME to pin, the pause state, the annotation and the new token, and warnings about what the action doesn't do. Some values need a note:

- `Freshness` is always `Unverifiable (advisory global status)`, because `sync` doesn't rely on STATUS.
- A ClusterServingRuntime shows as `ClusterServingRuntime//<name>`, with an empty namespace between the slashes.
- `Target evidence` is `Existing` when a revision with the target hash exists, and then `Target UID` and `Target RV` follow. It's `NotFound` when the controller has to create the revision.
- `Old sync token` is the SYNC state from `effective`: `Absent`, `StatusOnly` or `Acknowledged`.
- `Pause state` is `Paused`, `Freeze` or `Not recognized`, from the `ome.io/rollout-paused` annotation. A pause doesn't make `sync` refuse, and stays in effect for the rollout that follows.

The ActionResult table shows:

| Field | What it shows |
| --- | --- |
| `action` | `runtime sync`. |
| `target` | The InferenceService, as `InferenceService/<namespace>/<name>`. |
| `dry-run` | `none`, `client` or `server`. |
| `accepted` | Whether the API server accepted the patch. Always `No` for a client dry run. |
| `applied` | Whether the annotation was stored. Always `No` for a dry run. |
| `request-id` | The request ID in the token, `requestID` in JSON. |
| `revision-hash` | The target hash from the preview: the hash of the live spec. |
| `message` | `Validated locally; no patch sent.` for a client dry run, `API dry-run accepted; no changes persisted.` for a server dry run, and otherwise `API accepted annotation request; consumption and convergence not observed.` |
| `follow-up` | The command that shows the result, `kubectl ome runtime effective <name> -n <namespace> --context=<context> --ome-namespace=<OME namespace>`. |
| `hint` | `Use -o json or -o yaml for full values.` |

The table cuts each value to 56 characters: a longer value keeps its first 53 characters and ends in `...`. So the accepted message prints as `API accepted annotation request; consumption and conv...`, and the follow-up command always prints cut. `-o json` and `-o yaml` print every value in full. `-o wide` prints the same table as the default.

`applied` means the API server stored the annotation, not that the controller used it. Run the follow-up command, or [`kubectl ome wait`](wait.md) with the request ID, to see what the controller did.

When the CLI can't tell whether the patch went through, it says the outcome is unknown. A server error, a timeout or a network error fails with `runtime sync request outcome unknown; check runtime effective; do not replay`. A response that's too large or doesn't match the request fails with `runtime sync response is unbound or oversized; outcome unknown, check runtime effective; do not replay`, or `runtime sync response is unbound; outcome unknown, check runtime effective; do not replay`. If the CLI can't write the result, it fails with `write runtime sync result failed; request outcome may be unknown, check runtime effective; do not replay`. Check `effective` before you run `sync` again: a second `sync` refuses while an earlier token waits for the controller. Other API errors, such as a denied request, fail with `required Kubernetes API request failed; check access and connectivity`, and so does a failure to read the InferenceService at the start, even when it doesn't exist. The CLI never retries on its own. Each of these errors exits `1`.

[Guarded actions](guarded-actions.md) describes the contract every mutating kubectl-ome command follows. Like the other actions, `sync` refuses, and changes nothing, in these cases:

| Message | Cause |
| --- | --- |
| `action refused: target identity is missing or unsafe` | The InferenceService's name, namespace, UID, resourceVersion or generation is missing, invalid or unsafe to show. |
| `action refused: target is being deleted` | The InferenceService is being deleted. |
| `action refused: placement sources and derived services cannot be mutated` | The InferenceService takes part in placement: it has `spec.placement`, `status.placement` or the `ome.io/placement` finalizer, or one of the annotations or labels `ome.io/accelerator-requirements`, `ome.io/cluster-selector`, `ome.io/placement-origin`, `ome.io/placement-origin-uid` and `ome.io/placement-control-plane`. |
| `action refused: safety inputs exceed inspection bounds` | There's more to inspect than the CLI checks safely, such as more than 256 annotations. When a component runs on OMENative, this includes more than 32 InferenceReplicas, more than 2048 instances or 256 migrations in one of them, or replica evidence the CLI couldn't read in full. |

### Refusals {#sync-refusals}

Besides the refusals [every action shares](#how-the-action-runs), `sync` refuses in these cases:

| Message | Cause |
| --- | --- |
| `action refused: managed runtime sync safety evidence is unavailable or inconsistent` | The runtime evidence doesn't show a managed pin that drifted from the live runtime. The conditions follow this table. |
| `action refused: active runtime is unavailable, inconsistent or unbound` | The runtime evidence doesn't match the InferenceService the CLI read, or the components that run on OMENative changed between reads. |
| `action refused: controller safety evidence is stale or inconsistent` | A rollout run is active, the last run was rolled back, or the canary holds a rolled-back revision. When a component runs on OMENative, also when that component has no InferenceReplica, or when any InferenceReplica of the InferenceService has lifecycle work, a transient scale, an operation or a migration under way, sets `spec.pacing.rollbackToRevision`, reports retry blocks, is being deleted, isn't named `<name>-<component>`, doesn't name this InferenceService in `spec.parentRef` and as its only controller, lags, or reports a status that doesn't add up, such as more ready replicas than replicas. An InferenceReplica lags when its `status.observedGeneration` differs from its generation, or its `ome.io/parent-generation` annotation differs from the InferenceService's generation. |
| `action refused: a rollout promote or rollback mailbox is present` | The InferenceService has the `ome.io/rollout-promote`, `ome.io/rollout-rollback` or `ome.io/rollout-repin` annotation, or one of its InferenceReplicas has `ome.io/release-held-revision`. After a [rollback](rollout.md#promote-and-rollback), the rollback annotation stays until a different target revision appears. |
| `action refused: a logical annotation value is unsafe to preview` | A value in the preview holds a tab or a line break, is longer than 768 characters, or is an identity the CLI can't show safely. |

`sync` goes ahead only when all of these hold:

- `spec.runtime` names the runtime with a valid name, sets `autoSync: false` and no `revision`, and sets `kind` and `apiGroup`, if at all, to `ServingRuntime` or `ClusterServingRuntime` and `ome.io`.
- PIN is `ManagedPin/Resolved`, and the Active view is the revision in `status.pinnedRevisionName`.
- DRIFT is `ReportedTrue/RevisionMismatch`, and LIVE-RELATION is `Different`.
- SYNC isn't `Pending`, so no earlier token waits for the controller.
- The runtime isn't disabled, and its chain has at most five runtimes, none of them twice.
- The runtime has at most 32 revisions, all consistent with what OME writes and none disabled, and at most one of them has the target hash.

Because runtimes with the same name share their revisions' label, a same-named runtime in another namespace or scope that has revisions makes `sync` refuse.

### Examples {#sync-examples}

Run every check and see the preview, without sending the patch. `--yes` skips the prompt:

```bash
kubectl ome runtime sync llama-chat -n team-a --dry-run client --yes
```

The preview goes to stderr:

```output
ALPHA runtime sync preview (acceptance is not convergence)
FIELD             VALUE
Action            runtime sync
Context           prod-us-east
Workload NS       team-a
OME NS            ome
Target            InferenceService/llama-chat
UID               7d3f9b2e-5c1a-4e8f-9a6b-2f4c8d1e0a37
ResourceVersion   4711
Dry-run           client
Generation        3
Reported gen      3
Freshness         Unverifiable (advisory global status)
Pin               cr-vllm-llama-1baf272d
Pin source        ClusterServingRuntime//vllm-llama
Source            ClusterServingRuntime//vllm-base
Source UID        a41c7e9d-2b5f-4c83-9e6a-1f8d3b7c05e2
Source RV         187
Source gen        1
Source            ClusterServingRuntime//vllm-llama
Source UID        3b8e1f6a-9c2d-4a7e-8f15-6d0b4c9a2e81
Source RV         212
Source gen        2
Pin UID           c8f4a2e6-1d9b-4b7a-8e3c-5a0f7d2b9e64
Pin RV            101
Current hash      1baf272d
Target hash       3c18f4a0
Predicted pin     cr-vllm-llama-3c18f4a0
Target evidence   Existing
Old sync token    Absent
Target UID        0e6b9d3a-4f2c-4e1b-b7a8-3c5d9f1e8a20
Target RV         102
Pause state       Not recognized
Request UUID      ddf745b1-4bf6-4de0-9933-16e97855fb55
Set annotation    ome.io/runtime-sync
New token         cli-runtime-sync-ddf745b1-4bf6-4de0-9933-16e97855fb55
API acceptance is not convergence or serving-revision readiness.
Requests latest merged live runtime when controller consumes the token.
Previewed content is not locked by this annotation.
Parent CAS is not an atomic transaction across runtimes, revisions and IRs.
Global observed generation is advisory; freshness is Unverifiable.
Existing pause/freeze and lifecycle/rollout policies remain in effect.
```

The result goes to stdout:

```output
FIELD           VALUE
action          runtime sync
target          InferenceService/team-a/llama-chat
dry-run         client
accepted        No
applied         No
request-id      ddf745b1-4bf6-4de0-9933-16e97855fb55
revision-hash   3c18f4a0
message         Validated locally; no patch sent.
follow-up       kubectl ome runtime effective llama-chat -n team-a --...
hint            Use -o json or -o yaml for full values.
```

The CLI checked the chain from `vllm-base` to `vllm-llama`, and expects OME to pin `cr-vllm-llama-3c18f4a0`, a revision that already exists with the target hash, as [`history`](#history-examples) shows. The client dry run sent nothing, so `accepted` and `applied` are `No`, and the next run gets a new request ID.

Roll `llama-chat` forward, confirming at the prompt:

```bash
kubectl ome runtime sync llama-chat -n team-a
```

After you answer `y`, the CLI reads everything again and sends the patch. The result shows `accepted` and `applied` as `Yes`, a new `request-id`, and the message `API accepted annotation request; consumption and conv...`.

Have the API server check the patch without storing it:

```bash
kubectl ome runtime sync llama-chat -n team-a --dry-run server --yes
```

The result shows `accepted` as `Yes`, `applied` as `No`, and the message `API dry-run accepted; no changes persisted.`

Wait until the controller has used the token, passing the request ID from the result:

```bash
kubectl ome wait llama-chat -n team-a --for=runtime-sync=acknowledged --request-id=5b2e7c1a-9d4f-4e8b-a6c3-0f1d2e3b4a59
```

`wait` checks every five seconds. It exits `0` once the annotation and `status.lastRuntimeSyncToken` both hold the token, the InferenceService has no RuntimeDrifted condition, and its pin is still managed. It exits `2` if that doesn't happen within 60 seconds, or the `--timeout` you set, or if the InferenceService is missing, deleted or replaced, and `1` on an error. The new pods can still be rolling out then.

## Exit codes

| Code | Meaning | Returned by |
| --- | --- | --- |
| `0` | Success | Every subcommand that finishes, including `explain` when there are no runtimes. |
| `1` | General error | Every subcommand, for an invalid name or flag, a failed API request, or evidence `tree` needs and can't collect. `sync` also returns it for a refusal, an action you didn't confirm, Ctrl-C, its 45 seconds running out, or an outcome it can't tell. |
| `3` | Mutation conflict | `sync`, when the second read after confirmation finds a change, or when the API server rejects the guarded patch because the InferenceService changed after the CLI read it. |

No `runtime` subcommand returns `2`. Errors print to stderr as `error: <message>`.

## Related guides

- [Reference a runtime explicitly](../../guides/deploy-models/reference-a-runtime-explicitly.md)
- [Troubleshoot an InferenceService](../../guides/troubleshoot/troubleshoot-an-inferenceservice.md)
- [Troubleshoot runtime selection](../../guides/deploy-models/troubleshoot-runtime-selection.md)
