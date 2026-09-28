# OEP-0011.1: Comprehensive kubectl-ome Operations

<!--
This OEP is an additive follow-on to OEP-0011. It expands the existing
visibility-first kubectl plugin into an operational interface for OME's
current APIs without changing those APIs or their controllers.
-->

<!-- toc -->
- [Summary](#summary)
- [Relationship to OEP-0011](#relationship-to-oep-0011)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [Operating Principles](#operating-principles)
  - [Command Surface](#command-surface)
  - [User Stories](#user-stories)
    - [Diagnose a Mixed-Mode Service](#diagnose-a-mixed-mode-service)
    - [Safely Advance a Manual Canary](#safely-advance-a-manual-canary)
    - [Submit and Track a Migration](#submit-and-track-a-migration)
    - [Explain an Autoscaling Discrepancy](#explain-an-autoscaling-discrepancy)
    - [Diagnose Placement without Remote Credentials](#diagnose-placement-without-remote-credentials)
    - [Consume Diagnostics in Automation](#consume-diagnostics-in-automation)
  - [Mutation Safety Contract](#mutation-safety-contract)
  - [Output Contract](#output-contract)
  - [Exit Code Contract](#exit-code-contract)
  - [Version Skew and Feature Availability](#version-skew-and-feature-availability)
  - [Security and RBAC](#security-and-rbac)
  - [Risks and Mitigations](#risks-and-mitigations)
- [Design Details](#design-details)
  - [Implementation Scope](#implementation-scope)
  - [CLI Architecture](#cli-architecture)
  - [Namespace Model](#namespace-model)
  - [Target Resolution](#target-resolution)
  - [Observation and Evidence Model](#observation-and-evidence-model)
  - [Report Objects](#report-objects)
  - [OMENative and Instance Diagnostics](#omenative-and-instance-diagnostics)
  - [Rollout Diagnostics and Controls](#rollout-diagnostics-and-controls)
  - [Migration Diagnostics and Requests](#migration-diagnostics-and-requests)
  - [Autoscaling Diagnostics and Manual Scale](#autoscaling-diagnostics-and-manual-scale)
  - [Traffic Diagnostics](#traffic-diagnostics)
  - [Runtime and Accelerator Diagnostics](#runtime-and-accelerator-diagnostics)
  - [Accelerator Quota Diagnostics](#accelerator-quota-diagnostics)
  - [Placement and Workload Cluster Diagnostics](#placement-and-workload-cluster-diagnostics)
  - [Alfred Recommendations](#alfred-recommendations)
  - [Wait and Doctor Workflows](#wait-and-doctor-workflows)
  - [Implementation Roadmap](#implementation-roadmap)
    - [Wave 0: Proposal](#wave-0-proposal)
    - [Wave 1: Foundations and explicit dependencies](#wave-1-foundations-and-explicit-dependencies)
    - [Wave 2: Inventory and core service diagnosis](#wave-2-inventory-and-core-service-diagnosis)
    - [Wave 3: Parallel operational reads](#wave-3-parallel-operational-reads)
    - [Wave 4: Guarded operations](#wave-4-guarded-operations)
    - [Wave 5: Wait and doctor workflows](#wave-5-wait-and-doctor-workflows)
    - [Wave 6: Top-level status composition](#wave-6-top-level-status-composition)
    - [Final audit](#final-audit)
  - [Test Plan](#test-plan)
    - [Unit and Contract Tests](#unit-and-contract-tests)
    - [Per-PR Verification](#per-pr-verification)
    - [Integration Tests](#integration-tests)
  - [Graduation Criteria](#graduation-criteria)
- [Implementation History](#implementation-history)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
  - [Extend OEP-0011 in Place](#extend-oep-0011-in-place)
  - [Use Only a Sequential New OEP Number](#use-only-a-sequential-new-oep-number)
  - [One Monolithic CLI PR](#one-monolithic-cli-pr)
  - [Generic Imperative CRUD](#generic-imperative-crud)
  - [Direct InferenceReplica Mutation](#direct-inferencereplica-mutation)
  - [Duplicate Controller Logic in the CLI](#duplicate-controller-logic-in-the-cli)
  - [Raw kubectl and Shell Scripts](#raw-kubectl-and-shell-scripts)
<!-- /toc -->

## Summary

[OEP-0011](../0011-kubectl-ome-plugin/README.md) established a read-only
`kubectl-ome` plugin and the framework used by its `get`, `status`, `logs`,
`runtime explain`, and `version` commands. Since that proposal, OME has gained
substantial operational behavior: the OMENative deployment backend and
InferenceReplica lifecycle, dense and columnar instance status, component
autoscaling, migration requests, canary and coordinated rollouts, traffic
translation, accelerator quota APIs, multi-cluster placement status, and
Alfred recommendations.

This OEP expands the plugin into the primary operational interface for those
features. It proposes rich, evidence-based inspection commands and a small set
of guarded actions that use control points already supported by the API
server and controllers. It does not add or modify an OME API, controller,
webhook, deployment manifest, or generated client.

The result is a CLI that lets an operator answer what OME declared, what each
controller reported, what Kubernetes currently observes, and what safe action
is available without manually correlating CRDs, annotations, scale
subresources, pods, events, and ConfigMaps.

## Relationship to OEP-0011

This document is an amendment-style, additive follow-on. OEP-0011 remains the
source of truth for the plugin binary, kubeconfig handling, client factory,
IOStreams discipline, original read-only commands, release packaging, and
krew distribution. OEP-0011 deliberately deferred imperative and admin
workflows while reserving package boundaries for them.

OEP-0011.1 owns the post-v1 operational command families, shared diagnostic
report contract, and guarded mutation rules. It does not replace OEP-0011 or
change the behavior of its existing raw `get -o json|yaml` output.

## Motivation

The plugin's original status view predates the current control plane. For
several new status blocks it can only report that data is present and tell the
operator to inspect YAML. That leaves common incidents dependent on long,
error-prone sequences of `kubectl get`, JSONPath, annotation patches, scale
requests, and knowledge of controller internals.

Examples include:

1. An OMENative service is unavailable, but the useful answer is split across
   the InferenceService, one InferenceReplica per component, either DenseV1 or
   ColumnarV2 instance status, pods, revisions, and events.
2. A canary is paused or failed, but the current step, coordination gate,
   stable and canary revisions, observed traffic, metric analysis, and valid
   operator action are not presented together.
3. An autoscaler reports one desired count while the InferenceReplica scale
   subresource and live instance population show another.
4. A manual migration is operationally necessary, but submitting the
   annotation mailbox request correctly requires knowledge of its versioned
   key and JSON payload.
5. Placement or quota data exists, but operators cannot distinguish authored
   intent, controller-reported materialization, remote projection lag, and
   currently unavailable evidence.

These are operational bottlenecks rather than missing desired-state APIs. A
first-party CLI can remove them while remaining a Kubernetes API client.

### Goals

- Make `kubectl ome` the coherent operational view for current OME serving
  features.
- Explain effective OMENative deployment mode per component and diagnose its
  InferenceReplica and logical instance state.
- Present declared and observed rollout, migration, autoscaling, traffic,
  runtime, accelerator, quota, and placement state without conflating them.
- Add narrowly guarded rollout, migration, and scale actions only where the
  server already exposes a supported control point.
- Diagnose runtime pinning and lifecycle RetryBlocks, including their existing
  guarded runtime-sync and held-revision release control points.
- Provide deterministic table output for humans and versioned JSON/YAML report
  objects for automation.
- Degrade explicitly when a CRD, status field, optional controller, translator,
  or permission is unavailable.
- Preserve OEP-0011 command compatibility and the pure-Go, six-platform build.
- Deliver the enhancement as dependency-ordered, independently reviewable PRs
  with strong unit, golden, race, and mutation-request coverage.

### Non-Goals

- No changes outside `cmd/kubectl-ome/**` and `pkg/cli/**` for implementation.
  The OEP document itself is the only proposal-time exception.
- No CRD, Go API type, generated client, controller, webhook, RBAC manifest,
  chart, site, release workflow, or Makefile change.
- No generic create, deploy, edit, apply, or delete workflow for
  InferenceServices or other OME resources.
- No direct InferenceReplica spec or status mutation. Its spec is projected by
  the InferenceService controller and its status is controller-owned. The only
  IR writes are its supported `/scale` subresource and the controller-consumed
  held-revision release annotation.
- No arbitrary traffic-weight setter, rollout-spec editor, autoscaler-object
  editor, quota mutator, placement mutator, or remote-cluster executor.
- No canary retry command. Canary failure or rollback re-arms only when
  supported server behavior produces a genuinely new target. This is distinct
  from releasing an OMENative lifecycle RetryBlock in `Held` state, for which
  the IR controller exposes an explicit mailbox.
- No claim that alpha APIs or partially deployed controllers are stable or
  available on every cluster.
- No claim that multi-cluster reconciliation is complete. Placement commands
  are observational and operate only against the current kubeconfig context.
- No implicit retrieval or use of credentials referenced by a WorkloadCluster.
- No execution of Alfred recommendations. The initial CLI support is advisory.
- No implementation of `Proportional` or `Pinned` scaling policy modes. Current
  admission rejects both because no controller applies them; legacy stored
  values are diagnosed rather than treated as active behavior.

## Proposal

### Operating Principles

The expanded CLI follows four principles:

1. **Evidence before inference.** Output labels data as declared configuration,
   controller-reported status, live observed state, or unavailable evidence.
   The CLI does not turn an absent status field into a healthy result.
2. **Read broadly, write narrowly.** Diagnostics may join related resources.
   Actions use only supported InferenceService annotations, the narrow
   InferenceReplica held-release mailbox, or the InferenceReplica `/scale`
   subresource.
3. **Preview and guard every action.** A mutation shows the exact context,
   namespace, target, current evidence, and patch; it requires confirmation or
   `--yes`, supports dry-run, and carries an optimistic concurrency guard.
4. **Report request acceptance separately from convergence.** A successful
   mutation means the API server accepted the request. `status` and `wait`
   commands report whether controllers subsequently converged.

### Command Surface

The exact aliases and table columns may be refined during alpha, but the
command families and safety classification are part of this proposal.

| Command | Class | Purpose |
| --- | --- | --- |
| `get inferencereplicas` | Read | Enrich IR lifecycle, replica, revision, migration, and encoding columns. |
| `get acceleratorquotas` (`aq`) | Read | Add the currently missing AcceleratorQuota inventory entry with flattened budget rows. |
| `get workloadclusters` | Read | Enrich connection, generation, and endpoint evidence without claiming capacity. |
| `status ISVC` | Read | Replace placeholder sections with joined OMENative, rollout, autoscaling, traffic, accelerator, and placement diagnosis. |
| `instance list ISVC` | Read | Normalize DenseV1 and ColumnarV2 into logical per-instance rows. |
| `instance status ISVC INDEX --component COMPONENT` | Read | Show one instance's revisions, pods, readiness, lifecycle, failure, and migration evidence. |
| `instance retry-blocks ISVC --component COMPONENT` | Read | Show retry blocks, target revisions, state, reason, and release eligibility. |
| `instance release-held ISVC --component COMPONENT --revision REVISION` | Write | Submit the established IR held-revision release mailbox after exact identity checks. |
| `logs ISVC` | Read | Extend the existing streamer with OMENative instance and revision filters. |
| `rollout status ISVC` | Read | Show component and coordination phases, revisions, steps, gates, analysis, and traffic. |
| `rollout explain ISVC` | Read | Explain ordered groups, progression, budgets, soak, ratio gates, and active sequence. |
| `rollout validate ISVC` | Read/assert | Diagnose invalid or unsupported stored rollout specifications. |
| `rollout history ISVC` | Read | Show the bounded revision and status evidence currently retained by the cluster. |
| `rollout pause ISVC` / `rollout resume ISVC` | Write | Set or clear the lifecycle circuit-breaker annotation with an active-rollout guard and global-effects preview. |
| `rollout promote ISVC` | Write | Advance an active manual gate; analysis override is an explicit stronger action. |
| `rollout rollback ISVC` | Write | Submit the established rollback annotation for an active canary. |
| `migration status ISVC` | Read | Show active and terminal IR migration records and capacity indicators. |
| `migration history ISVC` | Read | Join bounded IR history with the optional migration audit ConfigMap. |
| `migration start ISVC` | Write | Submit one schema-v1 migration mailbox annotation after validating the target instance. |
| `autoscale status ISVC` | Read | Show per-component class, owner, source, target, desired/current counts, and conditions. |
| `autoscale explain ISVC` | Read | Explain declared policy, resolved ownership, scale-to-zero eligibility, and observed discrepancies. |
| `scale ISVC --component COMPONENT --replicas N` | Write | Patch only the resolved InferenceReplica `/scale` subresource with autoscaler-aware guards. |
| `traffic status ISVC` | Read | Show effective translator, endpoints, observed weights, conditions, and unsupported fields. |
| `traffic explain ISVC` | Read | Compare requested traffic intent with controller-reported support and observed realization. |
| `runtime effective ISVC` | Read | Show requested, selected, inherited, pinned, and drift-safe runtime configuration. |
| `runtime history ISVC` | Read | Summarize safe revision provenance without printing embedded pod templates or credentials. |
| `runtime sync ISVC` | Write | Advance an eligible autoSync=false pin through the established runtime-sync annotation. |
| `runtime explain` | Read | Extend the existing selector explanation with effective configuration evidence. |
| `accelerator explain ISVC` | Read | Explain selected AcceleratorClass, policy, requests, and controller-reported reasons. |
| `quota tree [AQ]` | Read | Render AcceleratorQuota ancestry, leaves, budgets, and detected structural problems. |
| `quota validate [AQ]` | Read/assert | Validate the fetched tree without claiming admission or controller enforcement. |
| `quota status [AQ]` | Read | Report status, materialization, capacity, and projection evidence when present. |
| `cluster status [WLC]` | Read | Diagnose current-context WorkloadCluster reachability and reported connection state. |
| `placement status ISVC` | Read | Show placement phase, candidates, winner, endpoints, and admitted/ready counts. |
| `placement explain ISVC` | Read | Explain candidate matching from available local objects and mark unavailable inputs. |
| `placement endpoint ISVC` | Read | Inspect the published endpoint evidence without contacting the remote cluster. |
| `admin doctor` | Read/assert | Check CLI/operator skew, CRDs, permissions, optional APIs, and feature availability. |
| `admin recommendations` | Read | Resolve Alfred's config and decode its latest recommendation record as advisory evidence only. |
| `wait ISVC` | Read/assert | Wait for an explicit condition, rollout, migration, or replica predicate with a timeout. |

Read/assert commands remain non-mutating but return a distinct unmet-predicate
exit code. Mutation commands are visibly labeled alpha in help output.

### User Stories

#### Diagnose a Mixed-Mode Service

An operator runs `kubectl ome status chat -n prod` and sees that engine and
decoder resolve to OMENative while the router resolves to RawDeployment. The
report shows which precedence rung selected each mode, which InferenceReplica
backs each OMENative component, and why one decoder instance is unavailable.

#### Safely Advance a Manual Canary

An operator runs `kubectl ome rollout promote chat`. The CLI fetches the live
canary status, previews its current step, traffic, and exact revision hash,
then asks for confirmation. If the resource changes before the patch, the
operation fails as stale instead of promoting a different step.

#### Submit and Track a Migration

An operator previews `kubectl ome migration start chat --component engine
--instance 3 --hint-node gpu-12`. The CLI validates the authoritative IR
instance, submits one versioned mailbox request, prints its request UUID, and
directs the operator to `migration status` or `wait` for convergence.

#### Explain an Autoscaling Discrepancy

`kubectl ome autoscale explain chat` shows that the desired policy is KEDA,
the controller reports KEDA ownership, the scale target is an
InferenceReplica, and the current instance count has not reached the desired
count. It reports missing ScaledObject permission as `Not readable`, not as
evidence that no scaler exists.

#### Diagnose Placement without Remote Credentials

`kubectl ome placement status chat` shows the management-plane placement
phase, candidates, winner, and published endpoint. It does not read a
WorkloadCluster Secret or silently connect to the selected cluster.

#### Consume Diagnostics in Automation

An incident system runs `kubectl ome rollout status chat -o json`, receives a
versioned CLI report object, and keys on explicit evidence state instead of
parsing a human table or assuming raw CRD shape.

### Mutation Safety Contract

All action commands share one contract:

- Resolve the effective context and namespace before showing the preview.
- Fetch the target and all data needed for precondition checks before asking
  for confirmation.
- Fail closed when the target is a placement source or placement-derived ISVC.
  A source is identified by structured or legacy placement requirements or
  placement status, or the `ome.io/placement` finalizer that may precede both;
  a derived object is identified by its origin markers. Current placement does
  not route operator actions safely to one remote owner, and `--force` cannot
  bypass this guard.
- Print the target, current operational state, expected revision or
  resourceVersion, and exact logical action.
- Require interactive confirmation unless `--yes` is present. Refusal makes no
  request and returns a non-zero invocation result. A non-interactive stdin
  fails closed unless `--yes` is supplied.
- Support `--dry-run=client`, `--dry-run=server`, and the default `none`.
  Client dry-run performs reads and validation but sends no patch. Server
  dry-run sends the same request with Kubernetes dry-run semantics.
- Use a narrow JSON Patch or scale-subresource request with resourceVersion or
  equivalent test preconditions. A conflict is surfaced and never silently
  retried against a new rollout or instance state.
- Never replace an annotation map, patch spec fields incidentally, or write a
  status subresource.
- Never print Secret data or include credentials in a report.
- State that API acceptance is not controller convergence and print the
  matching status or wait command.

Automation can combine `--yes` with either server dry-run or a real request;
there is no hidden environment variable that bypasses safeguards.

### Output Contract

Human-oriented diagnostic commands default to deterministic tables and
sections. They support `-o json` and `-o yaml` using a versioned report schema
owned by the CLI, initially `cli.ome.io/v1alpha1`. The report includes source
references, collection time, evidence levels, warnings, and normalized rows.

Actions use a versioned `ActionResult` on stdout. It records the operation,
target kind/namespace/name/UID/resourceVersion, dry-run mode, request ID or
revision hash when applicable, whether a real request was accepted, and the
follow-up status/wait hint. Both dry-run modes set `applied: false`; server
dry-run separately records API acceptance. Preview, prompt, and warnings go to
stderr so `-o json|yaml` produces exactly one machine-readable stdout object.

`kubectl ome get -o json|yaml` remains different by design: it continues to
emit unmodified Kubernetes API objects or list envelopes. This preserves the
OEP-0011 contract and prevents a diagnostic report from masquerading as a
Kubernetes object intended for apply.

Human output is alpha and may gain columns. Report schemas follow additive
compatibility within a version; breaking changes require a new report version.
All ordering is deterministic so golden tests and automation remain stable.

The CLI uses these evidence labels consistently:

| Evidence | Meaning |
| --- | --- |
| `Declared` | Read from desired spec or user-authored metadata. |
| `Reported` | Written by an OME or related controller into status. |
| `Observed` | Read from a live Kubernetes object or subresource. |
| `Computed` | Derived locally from identified inputs by a named CLI or shared operator rule; not a controller assertion. |
| `Unavailable` | The source is absent, unsupported, forbidden, or unreadable; the reason is retained. |

Health values such as `False`, `Failed`, or `Degraded` are not transport
errors. A report can be produced successfully about an unhealthy workload.

### Exit Code Contract

| Code | Meaning |
| --- | --- |
| `0` | A report was produced, an optional source was recorded as unavailable, a client/server dry-run validated, or the API server accepted a real action. Workload health may still be degraded. |
| `1` | Invalid invocation, missing/unreadable primary target or required source, transport/API/render failure, unsafe non-interactive use, or declined confirmation. |
| `2` | Doctor/validation found violations, or an explicit wait/assert predicate was not satisfied before completion or timeout. |
| `3` | A guarded mutation was rejected because its live precondition was stale or conflicting; no action was applied. |

The root command maps typed CLI errors to these codes. Existing commands keep
their current exit behavior unless they opt into a new assertion explicitly.
An optional Forbidden, NotFound, or unsupported source is represented in an
otherwise successful report and does not become exit `1`; the command's
documented primary target and safety-critical mutation inputs remain required.

### Version Skew and Feature Availability

The existing OEP-0011 minor-version skew policy remains. New commands must
also tolerate clusters where:

- a new CRD is not installed;
- an alpha status block is absent;
- an optional translator, autoscaler, KEDA API, Kueue API, placement
  controller, quota controller, or Alfred deployment is unavailable;
- an IR uses DenseV1 or ColumnarV2 instance status;
- a mixed-mode service has only some components backed by IRs; or
- the client knows fields that the server does not, or vice versa.

Absence is reported with source-specific guidance. It is never filled with a
guessed healthy value. Mutating commands require the exact server-side
prerequisites they depend on and fail closed when those cannot be established.

### Security and RBAC

Read commands request only the resources needed for their selected detail.
Optional evidence is fetched lazily so basic status remains useful under
restricted RBAC. Expected access includes:

| API group | Resources | Verbs |
| --- | --- | --- |
| `ome.io` | InferenceServices, InferenceReplicas, models, runtimes, AcceleratorClasses, AcceleratorQuotas, WorkloadClusters | `get`, `list` |
| core | Pods, Events, ConfigMaps | `get`, `list` as required by the selected command |
| `apps` | Deployments, ControllerRevisions | `get`, `list` |
| `autoscaling` | HorizontalPodAutoscalers | optional `get`, `list` |
| `keda.sh` | ScaledObjects | optional `get`, `list` |

Mutation access is limited to:

| API group | Resource | Verbs | Commands |
| --- | --- | --- | --- |
| `ome.io` | InferenceServices | `get`, `patch` | rollout actions, migration start, and runtime sync |
| `ome.io` | InferenceReplicas | `get`, `patch` | held-revision release annotation only |
| `ome.io` | InferenceReplicas/scale | `get`, `patch` | manual scale only |

No command needs an InferenceReplica spec/status write, Secret read, remote
cluster credential, workload deletion, or newly forged controller-write
annotation. The release mailbox patch preserves the controller-owned
annotation already present on the IR and changes only the supported mailbox
key. The CLI never escalates privileges or treats a SubjectAccessReview as
proof that an optional controller is installed.

### Risks and Mitigations

- **A convenient CLI can make unsafe actions easier.** Writes are limited to
  established server control points and protected by preview, confirmation,
  dry-run, exact-target validation, and optimistic concurrency.
- **Placement copies do not provide a safe mutation route.** Every action
  rejects placement sources and derived copies until the server defines
  single-owner routing and annotation removal semantics.
- **Joined data can be internally inconsistent.** Every report records object
  generation/resourceVersion and evidence level. Stale projection is shown,
  not silently merged into one authoritative state.
- **Alpha server behavior can drift.** Commands render unknown and absent data
  gracefully, avoid reimplementing controllers, and version their composite
  report schema.
- **Human output can become an accidental API.** Automation is directed to
  report JSON/YAML; deterministic goldens detect accidental changes.
- **A broad program can become an unreviewable change.** Foundations and
  feature families are split into focused, dependency-ordered PRs with a
  scope audit and tests on every PR.
- **Optional APIs can widen permissions.** Optional reads occur only for
  requested detail and preserve `Not readable` separately from `Not found`.
- **Multi-cluster commands could expose credentials.** They inspect only
  current-context objects and reported endpoints and never dereference remote
  credential Secrets.
- **Effective configuration may itself contain literal secrets.** Runtime and
  revision views use an allowlisted, redacted field set and never emit complete
  pod templates, environment values, headers, or arbitrary extension payloads.
  [OEP-0011.2](../0011.2-kubectl-ome-render/README.md) scopes this rule to
  diagnostic reports and adds `runtime render` as the one separately
  documented command that prints effective component specs unredacted.

## Design Details

### Implementation Scope

Every implementation PR is restricted to:

```text
cmd/kubectl-ome/**
pkg/cli/**
```

Tests and golden fixtures live under the same paths. No implementation PR may
change `go.mod`, `go.sum`, generated clients, APIs, controllers, manifests,
charts, site content, workflows, or repository-wide tooling. The design uses
dependencies already present in the module. If implementation discovers that
a server or generated-client change is required, that command is deferred
rather than expanding scope implicitly.

### CLI Architecture

OEP-0011's thin binary, root command, client factory, typed clients,
controller-runtime client, IOStreams, printers, paging, and per-command options
structs remain the foundation. This OEP adds focused CLI-local boundaries:

```text
pkg/cli/
  target/       # ISVC/component/IR resolution and identity checks
  observation/  # immutable snapshots and evidence metadata
  report/       # versioned report objects and table/JSON/YAML rendering
  mutate/       # previews, confirmation, dry-run, guarded patches
  transport/    # testable JSON Patch, dry-run, watch, and scale requests
  cmd/
    instance/
    rollout/
    migration/
    autoscale/
    scale/
    traffic/
    accelerator/
    quota/
    cluster/
    placement/
    admin/
    wait/
```

Names are illustrative package boundaries, not permission to create a generic
framework before its consumers need it. Foundations are introduced with the
first concrete command and kept narrow. The existing `factory.Factory`
interface is extended only when an approved command cannot use its current
typed clients or REST configuration; production and static fake
implementations change together.

Collection and rendering remain separate. A collector returns an immutable
snapshot plus source errors. Renderers never issue API requests. Mutators
consume freshly resolved targets and typed preconditions; they do not accept
unverified arbitrary object names from render output.

### Namespace Model

The workload namespace remains the global kubectl namespace selected by
`--namespace` or kubeconfig context. Control-plane resources use a separate
`--ome-namespace` option, defaulting compatibly to `ome`. Runtime
pinning ControllerRevisions and operator Deployment checks use the OME
namespace. InferenceServices, IRs, pods, events, autoscalers, and the
OMENative workload ControllerRevisions owned by an ISVC use the workload
namespace.

Alfred has its own deployment flags, so its ConfigMaps use an independent
`--alfred-namespace` that defaults to the effective OME namespace plus
`--alfred-config-name` and `--alfred-config-key`, defaulting to
`alfred-config` and `config.yaml`. Explicit values make a nonstandard Alfred
deployment deterministic; the CLI does not assume Alfred must share the OME
namespace.

Reports retain the namespace of every source instead of applying one namespace
label to all evidence. A command that requires a control-plane resource prints
the effective OME namespace in its preview or source list. Namespace discovery
may be added only when it is deterministic; it never scans all namespaces and
silently chooses one.

### Target Resolution

One shared resolver establishes the authoritative relationships used by all
commands:

1. Fetch the named InferenceService in the effective namespace.
2. Resolve the selected live or pinned runtime, its inheritance, and the
   runtime/ISVC component merge with existing operator helpers. If the inputs
   are unavailable, report effective configuration as unavailable rather than
   calculating from the raw ISVC alone.
3. Resolve each merged component's effective deployment mode with the
   operator's precedence: component annotation, `spec.deploymentMode`, merged
   leader/worker shape to OMENative, then RawDeployment default.
4. Enumerate InferenceReplicas and validate namespace,
   `spec.parentRef.name`, component, and the controller OwnerReference's ISVC
   GVK/name/UID instead of trusting a guessed IR name. ParentRef itself has no
   UID.
5. Treat only OMENative components as IR-backed. Mixed-mode services are
   normal and keep per-component evidence.
6. Normalize DenseV1 and ColumnarV2 instance payloads through one checked
   decoder. Malformed or contradictory encodings return explicit evidence
   errors rather than partial invented rows.
7. Verify namespace, GroupVersionKind, ParentRef, OwnerReference, component,
   placement safety, and status target
   before any mutation.

The canonical `<isvc>-<component>` name may be shown as expected projection,
but ParentRef and component identity determine authority.

### Observation and Evidence Model

Collectors preserve source boundaries instead of flattening everything into
one boolean. Each evidence item contains:

- source object kind, namespace, name, UID, generation, and resourceVersion;
- evidence level (`Declared`, `Reported`, `Observed`, `Computed`, or
  `Unavailable`);
- collection timestamp;
- value or normalized rows;
- absence/error reason, including NotFound, Forbidden, unsupported API, stale
  generation, malformed payload, or not configured.

The model distinguishes:

- desired replica count from autoscaler-reported desired count, IR scale
  value, IR status counters, and normalized live instances;
- rollout spec from component rollout phase, canary status, coordination
  status, traffic status, and live revisions;
- placement spec from controller-reported candidates and endpoint;
- quota authored tree from status/materialization/capacity evidence; and
- recommendation production from action execution.

The CLI may summarize disagreement, but it preserves the underlying values so
an operator can see which source is stale.

Collection is bounded. Service-level status lists pods once with selectors and
uses paged APIs; `instance list` does not fetch one Event stream per instance.
Detailed pod and Event joins are limited to an explicitly selected instance or
failed target, with documented caps, bounded concurrency, context/request
timeouts, and truncation metadata in reports. Optional-source failure or
truncation preserves the primary report and emits a warning.

### Report Objects

Composite commands use a shared envelope similar to:

```yaml
apiVersion: cli.ome.io/v1alpha1
kind: RolloutReport
metadata:
  namespace: prod
  name: chat
collectedAt: "2026-08-31T18:00:00Z"
sources: []
summary: {}
components: []
warnings: []
```

Each report kind owns typed content; the envelope is not an unstructured bag.
JSON and YAML encode the same object. Empty slices remain empty slices, source
ordering and component ordering are deterministic, and timestamps are
injectable in tests. Reports contain no Kubernetes `spec` intended for apply.

The report layer owns format parsing and output errors. Existing `get`
printers continue to own raw API serialization. Human tables are derived from
the same typed report used for machine output so the two views cannot drift in
meaning.

`ActionResult` is a separate report kind shared by mutators. In table mode it
prints one concise result after any interactive preview. In JSON/YAML mode the
preview remains on stderr and stdout contains only the result object. Literal
credentials, complete runtime templates, arbitrary annotations, Secret
selectors, and authentication headers are never serialized.

### OMENative and Instance Diagnostics

`status`, `instance list`, and `instance status` expose:

- effective deployment mode and the precedence source for each component;
- expected and observed IR identity and projection generation;
- desired, current, ready, serving, available, and updated replica counts;
- current and update revisions;
- lifecycle and coordination conditions;
- logical instance index, incarnation, phase, revisions, admission, pod counts,
  failure, and migration state;
- bounded live pod summaries, with detailed readiness, node, restarts, and
  warning events only for explicitly selected or failed instances; and
- whether DenseV1 or ColumnarV2 supplied the normalized rows.

IR status is authoritative for logical instances. Pod observations supplement
it but do not replace it. A missing IR for an OMENative component is reported
as `Not projected`; a RawDeployment component is reported as `Not OMENative`,
not as a missing resource.

RetryBlock diagnostics show target revision, state, reason, timestamps, and
whether a block is eligible for manual release. `instance release-held`
requires an exact IR identity and a matching `status.retryBlocks` entry in
`Held` state, then patches only
`ome.io/release-held-revision=<targetRevision>` with a resourceVersion guard.
The controller consumes this mailbox. An unknown or non-Held block is rejected
client-side rather than submitted as the controller's documented no-op.

No command directly restarts, deletes, or edits IR spec/status or its pods.
Controller ownership and reconciliation make those actions unsafe as generic
CLI verbs. The held-revision metadata mailbox is the narrow exception exposed
by the IR controller.
The existing log command gains optional instance and revision selection using
the same normalized identity model, while preserving its current component,
container, follow, tail, and since behavior.

### Rollout Diagnostics and Controls

`rollout status` joins the rollout spec, per-component phase, stable/current/
update revisions, canary status, coordination groups, observed traffic, and
analysis results. It distinguishes terminal success, failure, and rolled-back
states. For step-based rollout, it shows the current step and its capacity,
traffic, pause, and analysis gates without deriving success from spec alone.

`rollout explain` and `rollout validate` cover the complete grouped surface:
ordered single-component blue-green sequences, active group and sequence
position, defaulted blue-green progression, rolling-update surge and
unavailable budgets, post-group soak, multi-component maintain-ratio gates,
canary capacity/traffic independence, and manual/timed/analysis/immediate step
gates. Validation diagnoses an invalid legacy stored spec or a combination the
current controller cannot sequence; it never claims that desired shape is
already observed.

`rollout history` is explicitly bounded by objects and status retained by the
cluster. It must not promise a durable audit trail when only current/previous
revision evidence exists.

The supported actions are:

- pause: require at least one active rollout, then set
  `ome.io/rollout-paused=true`;
- resume: remove the paused annotation after verifying it is currently active
  and no rollout action is queued;
- promote: for an active indefinite manual pause, read the live canary hash and
  set `ome.io/rollout-promote=<canaryRevisionHash>`; and
- rollback: set `ome.io/rollout-rollback=true` for an active canary.

Despite its historical name, the paused annotation is a service-wide
OMENative lifecycle circuit breaker projected to every IR. It blocks Create,
Restart, Migration, and Update planning and parks applicable in-flight
IR lifecycle deadlines; scale-down and deletion teardown can still proceed.
Canary timed-gate clocks are not parked and continue aging, so a timed step may
advance immediately after resume. The pause preview enumerates these effects
and the affected components, and never calls the result a full workload
freeze. An idle service cannot be newly paused by this command.

Canary reconciliation does not consume promote or rollback mailboxes while
globally paused. Pause refuses if either mailbox is already pending, and
promote/rollback refuse whenever the service is paused. Resume also refuses a
pending mailbox because a queued boolean rollback can bind to a changed canary
target after resume. The explicit
`rollout resume --discard-pending-actions --yes` variant atomically removes the
pause plus pending promote/rollback annotations after previewing their values;
ordinary resume never activates or silently discards them.

Promote fails closed for an absent, terminal, failed, rolled-back, or hashless
canary and for timed or immediate steps. A matching promote can bypass the
controller's metric analysis, so an analysis step requires the separately
visible `--override-analysis --yes` action and a preview of every current
metric/gate result; ordinary `rollout promote` never bypasses analysis. An
override annotation is submitted only while that exact analysis step is
active, so it cannot linger into a later gate. Rollback requires an applicable
live canary. Actions use the status read in the preview as a patch precondition.
They report patch acceptance and do not wait implicitly for the controller to
consume the annotation.

### Migration Diagnostics and Requests

InferenceReplica `status.migrations` is the authoritative work state. The CLI
shows request ID, source, component, instance, phase, source/surge identity,
node hints, deadline, timestamps, outcome, and message. The bounded
InferenceService `status.migrationHistory` is reported as a parent summary.
The optional `<isvc>-ome-migration-audit` ConfigMap is labeled audit evidence
and may itself be bounded or absent; neither history source replaces IR
status.

`migration start` applies only to an OMENative/IR-backed component and writes
exactly one annotation whose key is
`ome.io/migration-request-v1-<uuid>`. Its JSON pins the schema actually consumed
by the workload controller: `schemaVersion`, `component`, `instance`, required
`from_node`, optional `hint_target_nodes`, `reason`, `requested_at`, and
`requested_by`. The CLI derives `from_node` only from an unambiguous current
instance observation; otherwise `--from-node` is required. A generated UUID is
printed as the tracking handle. New requests always use a CLI-generated UUID.
A caller-supplied `--request-id` is lookup-only for a bounded retry of an
already retained request; the CLI never submits an unseen caller-supplied ID.

Before patching, the CLI verifies the ISVC and IR relationship, component,
normalized target instance, current source node, migration policy is not
`Never`, and absence of an incompatible nonterminal request. UUID lookup spans
pending annotations, every sibling IR's migration status, the parent history,
and readable audit history. A pending annotation retains the canonical full
payload: an identical request returns an existing result with no write, while
a mismatch conflicts. Status and audit records can be lossy; if they prove the
UUID existed but cannot prove full payload equality, retry lookup fails closed
without a write. An unreadable required lookup source also fails the retry.

This is bounded retry detection, not durable exactly-once delivery. The
controller trims terminal status after time and count limits, and audit
history may also be bounded. An ID absent after pruning is not safe to reuse;
the unseen-ID rule requires a new generated UUID instead. Controller pacing
and capacity can change after the read, so capacity pressure is a warning
rather than a promise of admission. Success means the mailbox annotation was
accepted, not that migration started.

### Autoscaling Diagnostics and Manual Scale

Autoscaling commands show, per component:

- declared scaling class and relevant bounds;
- controller-reported `ManagedBy` and `SpecSource`;
- canonical scale target reference;
- desired/current counts, last scale time, and conditions;
- live IR `/scale` value when readable; and
- optional HPA or KEDA evidence when requested and permitted.

External ownership does not imply that OME can report external scaler
conditions. `Proportional` and `Pinned` scaling policies are both reported as
unimplemented; current admission rejects them because no controller applies
them, while a legacy unchanged stored value may remain through update
ratcheting.

`scale` requires `--component` and resolves its target from current component
status. It accepts only a same-namespace `ome.io/v1beta1` InferenceReplica whose
parent/component/owner identity match the requested ISVC. It patches only
`/scale` `spec.replicas`. Non-positive values are rejected because current IR
projection treats them as fallback-to-one rather than stable scale-to-zero.
Known effective minimum/maximum bounds are displayed and enforced.

By default, the command refuses an OME-owned HPA or KEDA target because the
autoscaler can immediately overwrite manual intent. It also refuses `External`:
that class is operator-owned, OME cannot observe the external scaler's state,
and the scaler may immediately overwrite a one-shot request. An explicit
`--override-autoscaler --yes` allows a transient request after a prominent
warning for any of these owners. `None` also fails closed by default because
projection restores ISVC-derived desired state; an explicit override uses the
same strong confirmation and is described as transient. KEDA scale-to-zero
eligibility remains diagnostic only on the OMENative path until the server
preserves zero IR replicas. Scale never creates or edits an HPA, ScaledObject,
or autoscaler policy.

### Traffic Diagnostics

Traffic commands compare declared intent with controller-reported policy
reference, routes/endpoints, revision weights, conditions, and unsupported
fields. TrafficStatus does not publish a translator name. The CLI may label a
translator as `Computed` from a recognized policy GVK; otherwise it is
`Unavailable`. It never parses human condition messages into structured
honored fields. Observed traffic status is the authority for realized policy;
the CLI does not assume support from a desired spec.

A noop or absent translator is surfaced as unavailable realization, including
`NoTranslatorAvailable` when reported. Raw extension or passthrough fields are
displayed as declared data and are not claimed valid unless the controller
reports them as honored. This OEP adds no traffic mutation command.

### Runtime and Accelerator Diagnostics

`runtime effective` explains namespaced-versus-cluster resolution, explicit
versus selected runtime, inheritance/defaulting visible in current API data,
per-component effective values, `autoSync`, pinned revision, last sync token,
and the `RuntimeDrifted` condition. The existing `runtime explain` continues to
reuse the operator's selector logic and gains source and effective-state
context without changing its selection verdict.

`runtime history` lists only safe revision provenance: revision name, runtime
identity, hash/labels, creation time, live/pinned role, and drift relationship.
It never emits raw ControllerRevision data, container environment, arguments,
pod templates, arbitrary annotations, headers, or extension payloads.

`runtime sync` applies only when `spec.runtime.autoSync=false`, no explicit
revision is set, the live source runtime is readable, a current pinned revision
exists, and live status establishes drift. It patches a fresh unique value into
`ome.io/runtime-sync` with the normal placement and resourceVersion guards. The
result reports request acceptance; a later status/wait confirms that
`status.lastRuntimeSyncToken` advanced and drift cleared. Explicit revision
rollback remains a desired-spec edit and is outside this OEP.

`accelerator explain` shows declared policy, selected AcceleratorClass,
selection reason/status, and effective resource requests. It may reuse a
pure, exported operator selector when doing so preserves exact behavior; it
does not copy controller algorithms into an independently drifting CLI rule.
Absent status remains unknown rather than inferred selection success.

### Accelerator Quota Diagnostics

The CLI first adds AcceleratorQuota to generic inventory. `quota tree`
assembles parent edges, detects missing parents, cycles, duplicate or invalid
roots, and renders budget leaves deterministically. `quota validate` performs
client-side structural and cross-object checks against the fetched snapshot
and exits `2` on violations.

These checks are advisory. They do not claim that an admission webhook,
controller, Kueue integration, materialization, or enforcement is installed.
`quota status` displays observed generation, paths, budgets, capacity,
per-cluster projection, materialization, and conditions only when the API
server has supplied them, with an evidence label for each source.

### Placement and Workload Cluster Diagnostics

Placement commands are alpha and observational. They show declared mode and
constraints, controller-reported phase, candidates, winner, admitted/ready
replicas, and published endpoints. `placement explain` can evaluate matching
against WorkloadCluster objects available in the current context and clearly
separates a locally computed candidate explanation from controller-reported
placement state.

WorkloadCluster diagnostics report connection/reachability and status
generation. They do not call connection status capacity, do not treat a
profile reference as a working connection, and do not imply that every
placement mode is implemented. No command reads the referenced Secret or
kubeconfig, contacts a remote API server, or mutates placement state.

Every mutating command rejects both placement-source signals and the derived
origin markers. This is necessary because current fan-out does not provide a
safe single target for migration, rollout, runtime-sync, scale, or IR mailbox
actions, and annotation removal is not guaranteed to propagate to derived
objects.

### Alfred Recommendations

`admin recommendations` first reads Alfred's selected configuration ConfigMap
and key from the effective Alfred namespace. It parses
`recommendationsConfigMapEnabled` and `recommendationsConfigMapName`, then
reads that resolved recommendations ConfigMap and decodes the latest cycle
record. It shows policy, workload, component, reason, executability,
admission/rejection outcome, and advisory actions such as migrating an
unsupported workload to OMENative.

The command never dispatches a recommendation. An unreadable or missing Alfred
configuration, disabled ConfigMap reporting, an absent resolved ConfigMap, and
malformed/old records are distinct results. The implementation uses a
CLI-local wire representation so the CLI does not import an execution engine
merely to read JSON.

### Wait and Doctor Workflows

`wait` makes convergence explicit after an action. Initial predicates cover
InferenceService Ready, rollout stable/failed/rolled-back, migration terminal
by request ID, runtime sync-token observation, held-revision release, and IR
replica counts. IR predicates require `--component`. It uses watch where
reliable with a bounded relist fallback, honors context cancellation and
timeout, and returns `2` when the predicate is unmet. It does not change the
success semantics of the action that preceded it.

`admin doctor` checks discoverability of required and optional APIs, effective
workload and OME namespaces/context, CLI/operator skew, basic read permissions,
and availability of feature-specific evidence. It performs no writes by
default. Any future server dry-run permission probe must be explicit and
follow the mutation safety contract.

### Implementation Roadmap

Implementation is a finite dependency graph of focused PRs. Parallel work
starts only after its shared dependency is merged.

#### Wave 0: Proposal

1. Merge OEP-0011.1.

#### Wave 1: Foundations and explicit dependencies

2. Add root composition, a testable executable/exit-code seam, and full
   command-tree tests. No dependency.
3. Add versioned diagnostic reports, `ActionResult`, and deterministic
   table/JSON/YAML rendering. No dependency.
4. Add the workload-versus-OME namespace model and shared option plumbing. No
   dependency.
5. Add model/runtime reference resolution, inheritance, and effective
   runtime/ISVC component merging. No dependency.
6. Add safe runtime-pin revision provenance and redaction primitives. Depends
   on 4 and 5.
7. Add authoritative ISVC/component/IR identity, effective deployment-mode,
   and placement-source classification. Depends on 5.
8. Add DenseV1/ColumnarV2 instance normalization. No dependency.
9. Add an HTTP/REST transport seam for guarded JSON Patch, server dry-run,
   watches, and `/scale` contract tests. No dependency.
10. Extract typed paging and bounded pod/Event observation. No dependency.

Items with no dependency run in parallel. A consumer starts as soon as all
listed predecessors merge; a wave is a concurrency frontier, not a requirement
that unrelated foundations wait for one another.

#### Wave 2: Inventory and core service diagnosis

11. Add AcceleratorQuota inventory. Depends on 3.
12. Enrich InferenceReplica inventory. Depends on 3 and 8.
13. Enrich WorkloadCluster inventory. Depends on 3.
14. Expand `status` with effective modes and IR summaries. Depends on 3, 5,
    7, 8, and 10.
15. Add instance list. Depends on 3, 7, and 8.
16. Add bounded instance status with pod/Event joins. Depends on 3, 7, 8,
    and 10.
17. Add RetryBlock and held-release eligibility diagnostics. Depends on 3,
    7, and 8.
18. Add OMENative instance and revision filters to logs. Depends on 7, 8,
    and 10.

#### Wave 3: Parallel operational reads

19. Add rollout status. Depends on 3 and 7.
20. Add full rollout explain and validation. Depends on 5 and 7.
21. Add redacted, bounded rollout history. Depends on 4, 6, and 7.
22. Add migration status and history. Depends on 3, 4, 7, 8, and 10.
23. Add autoscale status. Depends on 3, 5, and 7.
24. Add autoscale explain, including unsupported stored policies. Depends on
    23.
25. Add traffic status and explain. Depends on 3 and 7.
26. Add runtime effective/status and extend selector explain. Depends on 3, 4,
    5, and 6.
27. Add safe runtime history. Depends on 4, 6, and 26.
28. Add accelerator explain. Depends on 3, 5, and 7.
29. Add quota tree. Depends on 3 and 11.
30. Add quota validation and status. Depends on 29.
31. Add cluster status. Depends on 3 and 13.
32. Add placement status, explain, and endpoint inspection. Depends on 3, 7,
    and 13.
33. Add config-aware Alfred recommendation inspection and independent Alfred
    namespace/config flags. Depends on 3 and 4.

#### Wave 4: Guarded operations

34. Add active-rollout-guarded pause/resume, pending-mailbox protection, timer
    warnings, and the global lifecycle preview as the first concrete consumer
    of mutation preview, confirmation, placement guards, dry-run, conflict,
    transport, and `ActionResult` helpers. Depends on 2, 3, 7, 9, 19, and 20.
35. Add safe manual promote, explicit analysis override, and rollback. Depends
    on 34, 19, and 20.
36. Add generated-ID migration start and bounded retry lookup using the exact
    server wire contract. Depends on 34 and 22.
37. Add held-revision release. Depends on 34 and 17.
38. Add guarded runtime sync. Depends on 34, 26, and 27.
39. Add positive, bounded, autoscaler-aware manual scale. Depends on 34, 23,
    and 24.

#### Wave 5: Wait and doctor workflows

40. Add the shared watch/relist wait engine and ISVC Ready predicate. Depends
    on 2, 3, and 9.
41. Add rollout wait predicates. Depends on 19 and 40.
42. Add migration wait predicates. Depends on 22 and 40.
43. Add component-required IR, held-release, and scale predicates. Depends on
    15, 17, 23, and 40.
44. Add runtime-sync observation predicates. Depends on 26 and 40.
45. Add admin doctor with workload/OME namespace and API/RBAC checks. Depends
    on 2, 3, 4, and 9.

#### Wave 6: Top-level status composition

Every feature read PR above produces a reusable collector. The primary
integrator then makes small, sequential changes to the shared `status` package
so parallel feature branches do not fight over one gather/render file:

46. Integrate rollout summary. Depends on 14 and 19.
47. Integrate migration summary. Depends on 14 and 22.
48. Integrate autoscaling summary. Depends on 14 and 23.
49. Integrate traffic, runtime, and accelerator summaries. Depends on 14, 25,
    26, and 28.
50. Integrate placement summary. Depends on 14 and 32.

#### Final audit

After item 50, run the final compatibility, coverage, race, cross-platform,
history, and allowed-path audit. Fuzzing, wire contracts, and feature-specific
compatibility tests land with the package they exercise; there is no catch-all
code PR that hides unrelated fixes.

Each PR rebases on current main, modifies only the allowed implementation
paths, uses the repository PR template, carries DCO sign-off, adds focused
tests, and passes required CI before merge. Closely coupled items may be
combined when reviewability improves, but an implementation PR may not span
unrelated command families merely to reduce PR count.
The primary integrator owns root/factory/status integration hotspots and
rebases those PRs sequentially. Parallel workers own disjoint command and
support packages until their declared dependency is merged.

### Test Plan

[x] I/we understand that component owners may require updates to existing tests
before accepting changes necessary for this enhancement.

The focused CLI baseline on 2026-08-31 is 67.2% weighted statement coverage
(647 of 963 statements) across `cmd/kubectl-ome` and `pkg/cli/...`. The
program raises the final aggregate to at least 80%. New diagnostic packages
target at least 85%; mutation helpers and action packages target at least 90%.
Coverage thresholds supplement, rather than replace, behavioral assertions.

#### Unit and Contract Tests

- Full root-tree registration, global flags, IOStreams, and error-to-exit-code
  mapping, including the distinction between unavailable optional evidence,
  missing required evidence, unmet assertions, and stale mutations.
- `ActionResult` table/JSON/YAML output, strict stdout/stderr separation,
  non-TTY confirmation refusal, preview behavior, and accepted-versus-applied
  semantics for both dry-run modes.
- Generated OME and Kubernetes fake-client collectors, including pagination,
  selectors, RBAC errors, absent CRDs, and stale generations.
- `httptest` API-server wire contracts for JSON Patch content types,
  `PatchOptions.DryRun`, resourceVersion `test` operations, and the IR scale
  subresource. Generated fakes alone are insufficient for these paths.
- DenseV1 and ColumnarV2 parity, malformed payloads, ordering, empty sets, and
  normalization fuzz/property tests.
- OMENative log instance/revision selection, legacy component-only behavior,
  and exact pod/container filtering.
- Workload namespace and OME control-plane namespace separation for runtime
  pin revisions, workload revisions, and operator diagnostics, plus Alfred's
  independent namespace/config flags and recommendation-name resolution.
- Golden table, JSON, and YAML reports for healthy, degraded, failed,
  unsupported, forbidden, and partially available evidence.
- Exact JSON Patch escaping, payload, resourceVersion test, dry-run option,
  confirmation, conflict, requirements/status/finalizer-only placement-source
  refusal, derived-service refusal, and no-write-on-failure assertions.
- Migration schema-v1 wire payload and parser tests, required or unambiguously
  derived source-node behavior, and UUID lookup across pending annotations,
  active and terminal status, sibling IRs, and bounded audit history. Cases
  cover exact pending matches, mismatches, lossy history, unreadable history,
  pruned ledgers, and refusal to submit an unseen caller-supplied UUID.
- Rollout tests cover RollingUpdate, Recreate, and ordered blue-green groups;
  pause/resume, rollback, promotion hashes, manual/timed/analysis gates,
  explicit analysis overrides, component coordination, and terminal states.
  Pause cases cover idle refusal plus startup, restart, migration, update,
  parked IR deadlines, permitted scale-down, deletion teardown, queued-action
  refusal/discard, canary retargeting, and timed-gate aging across resume.
- Autoscaler ownership, scale target identity, override warning, and `/scale`
  subresource request tests, including legacy or invalid Proportional/Pinned
  policy values, HPA/KEDA/External/None override cases, and the OMENative
  zero-replica limitation.
- Runtime effective/pin/history/redaction tests and guarded sync tests for
  unique and ambiguous ControllerRevisions, drift, and auto-sync refusal.
- RetryBlock normalization and guarded held-revision release tests, including
  mailbox identity, eligibility, dry-run, conflict, duplicate behavior, and
  webhook-compatible preservation without forging the controller-write stamp.
- Quota tree cycle/missing-parent/root/budget tests and placement evidence
  tests without credential reads.
- Wait timeout, cancellation, relist, terminal-success, and terminal-failure
  tests with an injected clock/watch seam.
- Bounded observation tests assert request-count limits, per-call timeouts,
  cancellation, concurrency caps, deterministic truncation, and truncation
  metadata for large services.
- Race tests for concurrent log, watch, and collection paths.

#### Per-PR Verification

Every implementation PR runs focused tests for changed packages plus:

```shell
cli_verify_dir="$(mktemp -d)"
go test ./cmd/kubectl-ome ./pkg/cli/...
go test ./cmd/kubectl-ome ./pkg/cli/... \
  -covermode=atomic -coverprofile="${cli_verify_dir}/coverage.out"
go tool cover -func="${cli_verify_dir}/coverage.out"
go test -race ./pkg/cli/...
go vet ./cmd/kubectl-ome ./pkg/cli/...
go build -o "${cli_verify_dir}/kubectl-ome" ./cmd/kubectl-ome
```

The final audit enforces 80% across every statement block in the combined
profile, including top-level package initializers:

```shell
awk 'NR > 1 {
  total += $2
  if ($3 > 0) {
    covered += $2
  }
}
END {
  percentage = 100 * covered / total
  printf "total: %.1f%% (%d/%d statements)\n", percentage, covered, total
  exit percentage < 80
}' "${cli_verify_dir}/coverage.out"
```

`go tool cover -func` remains useful for function-level review, but its total
does not include statements that cannot be attributed to a function. A new
diagnostic package must independently report at least 85%, and a new mutation
helper or action package at least 90%, using a package-specific coverage
profile. A PR may not satisfy a package threshold by importing unrelated,
already-covered code.

The CLI is also built with `CGO_ENABLED=0` for linux, darwin, and windows on
amd64 and arm64. Every build uses `-o` with a unique path under a temporary
directory; verification never writes `kubectl-ome` or cross-build artifacts
into the repository. Changed Go files pass `gofmt`; diffs pass
`git diff --check`. An explicit path audit rejects any implementation diff
outside the two allowed roots and verifies that `go.mod` and `go.sum` are
unchanged.

Repository-wide CI remains authoritative. Commands that generate or format
unrelated repository paths are run only in a disposable verification worktree,
and any resulting tracked drift fails the scope audit instead of being added
to a CLI PR.

No PR is declared complete from an earlier branch state: its template records
fresh verification results from the final commit that is submitted for CI.

#### Integration Tests

Before beta, a compatible test cluster verifies read commands against real
CRDs and API-server serialization and verifies action request submission with
server dry-run. The cluster suite exercises JSON Patch preconditions and
content types, the IR scale subresource, and namespace separation.
Controller-backed smoke tests may validate consumption of migration, rollout,
runtime-sync, and held-revision-release mailboxes and observation through
status, but they do not weaken the rule that API acceptance and convergence
are separate results.

The implementation scope restriction means any repository-level integration
harness change needs separate authorization and is not part of this OEP's CLI
PR program.

### Graduation Criteria

- **Alpha:** all proposed read commands and guarded actions implemented under
  the allowed paths; report schema marked `v1alpha1`; final focused coverage at
  least 80%; mutation packages at least 90%; race, vet, focused build, and six
  cross-builds pass; every mutation has preview, confirmation, both dry-run
  modes, stale-state tests, placement-source guards, non-TTY tests, and a
  versioned `ActionResult` contract.
- **Beta:** operational usage across supported clusters confirms the report
  schema; optional-feature and RBAC behavior is documented by help output;
  compatible-cluster smoke coverage exists; no unresolved high-severity safety
  or output-contract issue remains.
- **Stable:** only after the corresponding server APIs graduate sufficiently;
  report schema and command semantics are additive, skew policy is validated
  across supported releases, and operational experience shows the safeguards
  prevent wrong-target actions.

Human table columns can graduate independently from alpha server features, but
the CLI cannot make an alpha, unavailable, or unimplemented server capability
stable by presenting it.

## Implementation History

- 2026-08-31: Provisional OEP-0011.1 created as an operational follow-on to
  OEP-0011.

## Drawbacks

- The CLI grows substantially and inherits maintenance work whenever alpha
  status fields evolve.
- Composite reports introduce a second, CLI-owned schema alongside raw CRDs.
- Strong mutation safeguards add flags and interaction that are more verbose
  than a direct `kubectl annotate` or `kubectl scale` command.
- Dozens of focused PRs require dependency coordination and repeated review.
- Some diagnostics remain incomplete when users lack optional RBAC or when a
  controller does not publish status.

These costs are accepted because the alternative leaves operational safety and
correlation to ad hoc shell commands without a versioned contract.

## Alternatives

### Extend OEP-0011 in Place

Rejected because OEP-0011 documents a visibility-first v1 that is already
implemented. Rewriting it would obscure the historical boundary between the
read-only plugin and later operational actions.

### Use Only a Sequential New OEP Number

A standalone sequential OEP was considered. This proposal uses the explicitly
requested OEP-0011.1 identifier to communicate that it extends the CLI design
and package architecture of OEP-0011 rather than defining an unrelated OME
subsystem. The metadata value is quoted so YAML preserves the identifier as a
string.

### One Monolithic CLI PR

Rejected because foundations, independent read commands, guarded actions, and
hardening have different review and failure domains. A dependency-ordered PR
graph supports parallelism after stable interfaces merge and keeps rollback
and ownership clear.

### Generic Imperative CRUD

Rejected because a generic editor cannot encode the ownership and stale-state
rules of OMENative, rollout, migration, autoscaling, quota, or placement. It
would make unsafe operations easier while adding little over `kubectl`.

### Direct InferenceReplica Mutation

Rejected because the InferenceService controller owns projected IR spec and
the IR controller owns status. Supported user operations already have safer
control points: parent annotations, the scale subresource, and the established
held-revision release mailbox.

### Duplicate Controller Logic in the CLI

Rejected because copied rollout, placement, traffic, or selector algorithms
would drift. The CLI reads authoritative status, uses exported pure helpers
when appropriate, and labels any local explanation separately from observed
controller results.

### Raw kubectl and Shell Scripts

Rejected as the primary interface. Raw commands remain useful escape hatches,
but they do not provide cross-resource identity checks, DenseV1/ColumnarV2
normalization, evidence labeling, guarded patches, deterministic reports, or a
shared operational contract.
