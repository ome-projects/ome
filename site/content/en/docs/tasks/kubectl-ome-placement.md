---
title: "Inspect Reported Placement Evidence with kubectl ome placement"
linkTitle: "kubectl-ome placement"
weight: 21
date: 2026-09-27
description: >
  How kubectl ome placement status, explain and endpoint report controller-written multi-cluster placement evidence — and what these read-only reports deliberately do not prove.
---

The `kubectl ome placement` family — `status`, `explain` and `endpoint` —
inspects the multi-cluster placement evidence recorded for one
InferenceService, using nothing but bounded reads against your current
kubeconfig context. It is the drill-down behind the one-line `Placement` row
in [`kubectl ome status`](/ome/docs/tasks/kubectl-ome-status). For installing
the plugin and the RBAC it needs, see
[kubectl-ome Plugin](/ome/docs/tasks/kubectl-ome).

> **Note:** multi-cluster reconciliation in OME is under active development,
> and these reports are an alpha contract (`apiVersion: cli.ome.io/v1alpha1`).
> Everything the commands print is *evidence recorded on control-plane
> objects* — what a controller wrote at some reconcile — never verified
> placement, capacity or serving health. The commands' own help text carries
> the same contract.

Do not confuse this family with
[`traffic status`](/ome/docs/tasks/kubectl-ome-traffic-status) and
[`traffic explain`](/ome/docs/tasks/kubectl-ome-traffic-explain), which cover
revision-level gateway traffic *within* a cluster. `placement endpoint` reads
the cross-cluster routing table instead — the
[TrafficMap](/ome/docs/concepts/traffic_map/).

## Usage

```bash
kubectl ome placement status chat -n prod     # reported placement of the parent
kubectl ome placement explain chat -n prod    # + selector fit per WorkloadCluster
kubectl ome placement endpoint chat -n prod   # + TrafficMap routing evidence
```

Each command takes exactly one argument — the InferenceService name,
validated as a DNS-1123 subdomain before any API request — plus the standard
kubectl connection flags (`--kubeconfig`, `--context`, `-n`). Output is
selected with `-o table` (default), `-o wide`, `-o json` or `-o yaml`; any
other value is rejected before anything is read. There is no watch mode and
no mutating flag of any kind.

`-o json` and `-o yaml` emit one typed report
(`apiVersion: cli.ome.io/v1alpha1`, kind `PlacementStatusReport`,
`PlacementExplainReport` or `PlacementEndpointReport`) with `metadata`,
`collectedAt`, `sources`, `content` and `warnings`. Script against `-o json`
— the two-column tables are not a stable interface, and their cells are
clipped to 18/56 characters with a literal `...` (which is why some field
names below end that way).

## The read-only contract

Every subcommand observes only the current-context control plane, inside a
30-second collection budget with a 10-second cap per request and **no
retries** — not even on a 429. There are no remote-cluster clients, no
credential or ClusterProfile resolution, no endpoint probes, no mutations
and no watches. That boundary is what makes the reports safe to run
anywhere, and it is also exactly what they cannot prove:

- **Placement freshness is unverifiable.** `status.placement` on the
  InferenceService carries no `observedGeneration`, so the report can never
  say whether the recorded placement reflects your current spec. The
  `Freshness` row always reads `Unverifiable` — a property of the API, not a
  defect of your service.
- **A reported address does not prove success.** Endpoints are echoed from
  status as sanitized **origins** (`scheme://host[:port]` only — never a
  path, query, or credential; `pathPresent` in JSON says a path existed).
  Nothing is ever contacted.
- **`explain` computes only label-selector compatibility.** It parses the
  declared requirements and cluster selector as standard Kubernetes label
  selectors and matches them against each WorkloadCluster's labels (plus its
  `metadata.name`). It does not predict scheduling: admission, quota,
  capacity, policy distribution, rollout preflight, sticky placement and
  operator routing defaults are all unobserved, and the JSON report lists
  them verbatim under `unobservedInputs`.
- **WLC `Ready` is reported control-plane reachability.** A `True` Ready
  condition on a WorkloadCluster means the registry controller reported it
  could reach that cluster's control plane — not that the cluster has
  capacity, free quota, or would admit this service.
- **`endpoint` shows origins and verbatim weights.** Traffic weights are
  echoed exactly as the TrafficMap records them; the publisher's
  acknowledgement and any recorded probe results are reported as separate
  facts, never combined into a health verdict. No CLI network probe ever
  runs.
- **Missing optional sources are diagnostics, not failures.** If the
  WorkloadCluster list or the TrafficMap is absent, forbidden, or unreadable,
  the command still succeeds (exit 0) and records the acquisition state
  instead. Only a failed read of the InferenceService itself is fatal.

## What each command reads

| Command | Reads | RBAC verbs |
| --- | --- | --- |
| `placement status` | one `get` of the InferenceService | `get` `inferenceservices` |
| `placement explain` | + a bounded cluster-scoped list of WorkloadClusters (at most 2 pages of 32) | + `list` `workloadclusters` |
| `placement endpoint` | + one `get` of the TrafficMap named after the service, in its namespace | + `get` `trafficmaps` |

The baseline read-only role on the
[kubectl-ome page](/ome/docs/tasks/kubectl-ome/#required-rbac) grants all
three. If a binding is narrower, note the sharp edge: a forbidden *optional*
read exits 0 and reports `Forbidden` as the fleet or TrafficMap acquisition
reason — easy to mistake for "no clusters" or "no routing" if you only skim
the table.

On a single-cluster installation all three commands work and report the
absence honestly: `status` shows a `NotRecorded` phase, `explain` an empty
fleet (or `UnsupportedAPI` when the WorkloadCluster CRD is not installed),
and `endpoint` a `NotFound` TrafficMap read — `NotFound` covers both a
missing map and a missing TrafficMap CRD.

## placement status

One read, echoing what the OME controller recorded under
`status.placement`. A Single-mode service fanned out to two candidates,
one admitted:

```
FIELD               VALUE
Mode                Single (Declared)
Placement           Placed (reported)
Freshness           Unverifiable
Input source        Structured
Selectors           Valid
Reported cluster    worker-a
Endpoint origin     https://chat.worker-a.example.com
Service origin      https://chat.prod.example.com
Home inspection     Validated 2/2; truncated=false
Provenance inspect  Validated 2/2; truncated=false
Reported home       worker-a (Admitted)
Home origin         https://chat.worker-a.example.com
Reported home       worker-b (Admitting)
Home origin         NotRecorded
Hint                Placed does not prove replica floor met
View                Bounded cells; use -o json for complete identities
```

- **`Mode`** — the placement mode (`Single`, `All`, `Split`) with its
  evidence: `Declared` when `spec.placement.mode` is set, `Defaulted`
  otherwise.
- **`Placement`** — the reported phase: `Pending`, `Admitting`, `Placed`,
  `Failed`, the legacy `Racing`, `NotRecorded` when no placement status
  exists (normal for single-cluster services), or `Unknown` for a value
  outside that set.
- **`Input source`** — `Structured` when `spec.placement` exists,
  `LegacyAnnotations` otherwise (the `ome.io/accelerator-requirements` and
  `ome.io/cluster-selector` annotations remain observable during their
  compatibility window; JSON flags their presence separately).
- **`Selectors`** — combined validity of the requirements and cluster
  selector: `Valid`, `NoRequirements`, `InvalidSelector` or `BudgetExceeded`
  (a selector string over 4096 bytes). Selector *contents* are never copied
  into the report.
- **`Reported home`** — one row per validated candidate from
  `status.placement.candidates`, with its per-cluster phase (`Admitting`,
  `Admitted`, legacy `Placed`) and reported origin. As the hint row says,
  `Placed` does not prove any replica floor was met.
- **`Home inspection` / `Provenance inspect`** — preview rows in
  `<state> <kept>/<total>; truncated=` form (see
  [Bounds](#bounds-previews-and-issues)).

The compact table shows at most 4 homes; `-o wide` shows all of them and
adds per-home `Admitted replicas` and `Ready replicas` counts (echoed only
in `Single`, `All` and `Split` modes; a zero is reported as `Unknown`, not
as an observed zero), plus rollout/policy provenance rows.

## placement explain

Adds two things to the status content: the **declared routing-override
summary** from `spec.routing` and, from a bounded WorkloadCluster list, a
**per-cluster selector-compatibility row**. After the same ten summary rows
as `placement status`, the table continues:

```
Routing intent      Declared
Routing enablement  Inherited
Install routing     Gate/defaults unobserved
Capacity factors    Routing (2)
Routing probe       Configured
Capacity poll       Inherited
Publisher options   Inherited (0)
Fleet state         Observed
Fleet reason
Fleet Returned      3
Fleet Admitted      3
Fleet Pages         1
Fleet Complete      true
Fleet Truncated     false
Cluster             worker-a
Selector compat...  True
Reported WLC Ready  True (Current)
Condition inspect   Validated 1/1; truncated=false
Cluster             worker-b
Selector compat...  True
Reported WLC Ready  False (Current)
Condition inspect   Validated 1/1; truncated=false
Cluster             worker-c
Selector compat...  False
Reported WLC Ready  True (Stale)
Condition inspect   Validated 1/1; truncated=false
Hint                WLC Ready is reachability, not capacity
Hint                Partial fleet: no global eligibility verdict
View                Bounded cells; use -o json for complete identities
```

### Routing intent rows

These summarize only what `spec.routing` (and the legacy
`spec.placement.capacityFactors` field) *declares* — operator-level defaults
and the install-wide routing gate are unobserved, which is what the constant
`Install routing  Gate/defaults unobserved` row reminds you of:

- **`Routing intent`** — `Absent`, `Declared`, `Invalid` (declared but
  failing the same validation the webhook applies) or `BudgetExceeded`.
- **`Routing enablement`** — `OptIn`, `OptOut`, <!-- codespell:ignore optin -->
  or `Inherited` when `spec.routing.enabled` is unset.
- **`Capacity factors`** — where declared per-home capacity overrides came
  from (`Routing`, `LegacyPlacement`, or `Conflict` when both fields are
  set) and how many, without copying cluster names or quantities.
- **`Routing probe` / `Capacity poll` / `Publisher options`** —
  `Configured`, `Disabled` or `Inherited` (publisher options are only ever
  `Configured` or `Inherited`). `-o wide` adds the declared probe
  method, thresholds, timing and all-failed policy, and the capacity-poll
  window — but never paths, format names, or option keys and values; only
  their presence and counts.

### Fleet and cluster rows

- **`Fleet ...`** — the acquisition record for the WorkloadCluster list:
  state (`Observed`, `Partial`, `Unavailable`), reason, items returned and
  admitted, pages read, and whether the read was complete. The read is
  deliberately bounded to 2 pages of 32; a bigger fleet is reported
  `Partial` with `PageBudgetExceeded` or `ItemBudgetExceeded` rather than
  enumerated. A partial fleet means **no global eligibility verdict** — a
  compatible cluster may simply not have been read.
- **`Selector compat...`** — the one thing `explain` computes:
  whether the cluster's labels (plus its `metadata.name`) match the
  service's combined requirements and cluster selector. `NotApplicable` when
  the service declares no selectors, `Unknown` when a selector or the
  cluster's labels are unusable.
- **`Reported WLC Ready`** — the cluster's reported `Ready` condition with
  its freshness against the cluster's own generation. Reachability only.
- `-o wide` adds `Reported home` (whether this cluster appears among the
  service's validated candidates), `Connection source`
  (`KubeConfigReferenceNotResolved` or `ClusterProfileReferenceNotResolved`
  — the CLI names the declared credential source but never resolves it) and
  the Ready condition's reason.

## placement endpoint

Reads the [TrafficMap](/ome/docs/concepts/traffic_map/) named after the
service and reports the recorded cross-cluster routing evidence. The map
must actually belong to the service — same name and namespace,
`spec.service` matching, and a single controller owner reference to this
InferenceService's UID — otherwise the read is discarded as
`OwnershipUnbound`. After the same ten summary rows as `placement status`:

```
TrafficMap state    Observed
TrafficMap reason
TrafficMap Retu...  1
TrafficMap Admi...  1
TrafficMap Pages    1
TrafficMap Comp...  true
TrafficMap Trun...  false
Entry inspection    Validated 2/2; truncated=false
Probe inspection    Validated 2/2; truncated=false
Condition inspect   Validated 4/4; truncated=false
Routing freshness   Current
Traffic override    False: NoOverrides (Current)
Capacity fallback   True: EndpointCapacityUnavailable (Current)
Routable            True: Routable
Routable freshness  Current:
Published           True: Published (Current)
Publisher           ReportedTrue
Publisher fresh     Current
Route home          worker-a
Endpoint origin     https://chat.worker-a.example.com
Final weight        1; healthy=true
Recorded probe      Passing
Route evidence      drains=0; gate=False; fallback=NotRecorded
Probe policy        NotRecorded
Route home          worker-b
Endpoint origin     https://chat.worker-b.example.com
Final weight        0; healthy=true
Recorded probe      Passing
Route evidence      drains=0; gate=False; fallback=NoReport
Probe policy        NotRecorded
Hint                Routing generation does not date placement
Hint                Recorded probes only; no CLI network probe
View                Bounded cells; use -o json for complete identities
```

- **`Routing freshness`** — whether the map's
  `spec.observedISVCGeneration` matches the InferenceService's current
  generation. This dates the *routing table*, not the placement — hence the
  hint row.
- **`Routable` / `Published` / `Traffic override` / `Capacity fallback`** —
  the TrafficMap's four conditions, each with an allowlisted reason
  (`Routable`, `NotPlaced`, `NoAddressableHome`, `AllHomesUnready`,
  `NoRoutableCapacity`, `AllHomesProbeFailed`, `TrafficDrain` for
  `Routable`; anything else renders as `OtherReportedReason`) and its own
  generation-bound freshness. Arbitrary condition messages are never copied.
- **`Publisher`** — the acknowledgement derived from `status.published`,
  the `Published` condition and `status.observedTrafficMapGeneration`:
  `NoAcknowledgement`, `ReportedTrue`, `ReportedFalse`, `Unknown`, or
  `Invalid` when those three contradict each other. `ReportedTrue` is the
  publisher's *claim* to have written external route objects — it is a
  separate fact from the weights above it, and from the gateway actually
  splitting traffic.
- **`Final weight`** — the entry's weight and `healthy` flag, verbatim.
  A weight of 0 with `healthy=true` is a normal state (for example, no
  allocated capacity); the `Routable` condition says why traffic flows or
  does not.
- **`Recorded probe` / `Probe policy`** — the last recorded
  [routing health probe](/ome/docs/administration/routing-health-probes/)
  result (`Passing`, `Failing`, `Unknown`) and the `sha256:` digest of the
  probe policy it ran under, exactly as recorded. `Route evidence`
  summarizes the same entry: how many
  [drain overrides](/ome/docs/tasks/drain-traffic-from-a-workload-cluster/)
  hold it, whether the probe gated traffic, and any capacity fallback
  reason (a closed vocabulary such as `NoReport`, `Unreachable`,
  `StaleReport`, `AwaitingQuorum` — raw fallback text is never copied).
- `-o wide` adds per-route capacity rows (`Allocated count`, `Ready count`,
  `Capacity provenance`: `ControlPlane` or `Endpoint`), probe freshness and
  gate rows, and up to 16 drain reference IDs. A reported `GatewayRef`
  appears in JSON as state `ReportedReferenceNotResolved` — echoed, never
  read back.

## Evidence and freshness

Every value in the JSON reports carries an evidence level and a freshness:

- **Evidence** — `Reported` for values echoed from controller-written
  status, `Observed` for objects this command itself read (the
  WorkloadCluster list, the TrafficMap), `Unavailable` when there is no
  backing evidence.
- **Freshness** — `Current` or `Stale` where the value is bound to a
  generation (`observedGeneration` on conditions,
  `observedISVCGeneration` on the TrafficMap spec), `Unverifiable` where
  the API records no generation marker (all of `status.placement`), and
  `Invalid` where the recorded generations are impossible (for example, an
  observed generation greater than the current one, or a condition
  transition time in the future).

## Bounds, previews and issues

All lists are validated, deduplicated, sorted and bounded before display;
whatever exceeds a bound is reported as a state, never silently dropped:

| Input | Validation bound | Displayed |
| --- | --- | --- |
| WorkloadClusters (`explain`) | 2 pages × 32 items; a larger fleet is `Partial` | 4 in the compact table, all in `-o wide`/JSON |
| Placement candidates | 256 | 64 (4 in the compact table) |
| TrafficMap entries | 256 | 64 (4 in the compact table) |
| Conditions per object | 64 | the recognized types only |
| Drain refs per entry | 64 | 16 |
| Selector strings | 4096 bytes each | never displayed |

Preview rows (`Home inspection`, `Entry inspection`, `Condition inspect`, …)
render as `<state> <kept>/<total>; truncated=<bool>`: `Validated` means
every item passed inspection, while `MalformedPayload`,
`ConflictingDuplicates` and `BudgetExceeded` mean the affected section is
withheld rather than partially trusted. `Issue` rows aggregate the same
findings as typed, message-free `<group>: <code> (count)` pairs — for
example `TrafficMapConditions: MalformedPayload (1)` or
`CandidatePhase: UnknownValue (2)`. Raw reasons, messages and names from a
malformed payload never reach the output, and API error text (which can
embed server URLs) is reduced to the closed reason set
(`Forbidden`, `NotFound`, `UnsupportedAPI`, `Timeout`, `Cancelled`,
`Unreadable`).

## Exit codes

All three subcommands are pure reads: exit 0 when the observation
completed — including every degraded-but-diagnosed case above — and exit 1
when it could not (bad invocation, unresolvable namespace, or a failed read
of the InferenceService itself). They never exit 2 or 3; there is no
assertion mode and no mutation. See the
[shared exit-code table](/ome/docs/tasks/kubectl-ome/#exit-codes).
