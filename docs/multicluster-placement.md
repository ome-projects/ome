# Multicluster placement

An InferenceService declares multicluster placement with
`spec.placement.policy: ClusterAffinity` and an explicit supported `mode`. The
mode is chosen at creation and stays for the life of the service.
An ordinary service without placement intent stays local. Placement does not
adopt an unrelated member service with the same name. Omitting affinity matches
every registration; empty or null affinity is invalid.

Legacy placement is unsupported. Migrate sources, derived copies, and writers
on a compatibility release before installing this schema or controller. The
[upgrade procedure](#retiring-legacy-placement) below describes that prerequisite.

`Single` races eligible members for admission and retains one winner. `All`
places the full service on every eligible member. `Split` assigns exact shares
of a fleet floor across all matching registrations. Readiness and quota gate
execution; they do not change those shares. `SplitByCapacity` uses nominal
hardware weights for the same apportionment and bounded transition.

Every placed member requires OMENative for its resolved Engine, Decoder,
and Router components. This includes modes selected by a runtime or a component
annotation. An unsupported backend sets the advisory
`PlacementBackendReady=False` condition and holds workload and mounted model
configuration updates. Existing serving resources remain intact; correcting the
runtime or service resumes reconciliation. VirtualDeployment cannot participate.
Member deletion still permits cleanup. Local services retain all deployment
modes and do not gain this condition. An origin-marked service without an
execution policy, including during direct ownership handoff, keeps reconciling
as a local service and reports `PlacementReplicaFloorsReady=Unknown` with reason
`ExecutionPolicyPending` until the source writes the policy.

The source checks each target through its identified direct member connection
before granting workload updates, then verifies runtime dependencies and source
identity again at application. Missing runtime inputs produce
`PlacementBackendReady=Unknown`. Single and All continue on verified eligible
members; a standing Single winner whose backend is unverified remains held.
Static Split leaves a blocked member's assigned share pending. SplitByCapacity
holds the entire accepted allocation when any matched backend cannot be verified.
Backend conditions are advisory: standing serving health continues to be observed.
Hardware reports are not required for Single, All, or static Split. Upgrade the
member access role so the placement credential can read runtimes, models, pinned
ControllerRevisions, and the named operator ConfigMap. These reads grant no
runtime, model, quota, or configuration writes.

## Component weights

In `Split`, every component runs on the clusters that hold an Engine. Each
affinity term can carry a `weights` block that shares the Decoder and the
Router out differently from the Engine. For example, a service with 4 Engines
and 12 Decoders wants three quarters of its Engines on `cluster-a` but its
Decoders shared evenly:

```yaml
engine:  {minReplicas: 4}
decoder: {minReplicas: 12}
placement:
  policy: ClusterAffinity
  mode: Split
  clusterAffinity:
  - weights: {engine: 3, decoder: 1}
    matchFields: [{key: metadata.name, operator: In, values: [cluster-a]}]
  - weights: {engine: 1, decoder: 1}
    matchFields: [{key: metadata.name, operator: In, values: [cluster-b]}]
```

`cluster-a` runs 3 Engines and 6 Decoders, and `cluster-b` runs 1 Engine and 6
Decoders. Each term gives each component a weight:

| Component | Weight a term gives it |
|---|---|
| Engine | `weights.engine`, else `weights.default` |
| Decoder | `weights.decoder`, else `weights.default`, else the Engine's weight |
| Router | `weights.router`, else `weights.default`, else the Engine's weight; read only when routers are divided |

A cluster's weight for a component is the sum over the terms it matches, as
in [Static shares](#static-shares). If any term gives a component a weight, a
term that gives it none contributes one.

A term weighs components only through `weights`; it has no scalar `weight`
field. Under strict field validation, which `kubectl` uses by default, the API
server rejects a term that sets `weight`. Other clients get a warning by
default, and the server drops the field, so that term counts as unweighted.

A Decoder whose weights are proportional to the Engine's follows the Engine
shares as described in [Static shares](#static-shares). It always does when no
term sets `decoder` and no term sets `default` beside `engine`. A Decoder with
different weights is split by them over the clusters that hold an Engine. Each
such cluster gets at least one Decoder, and the plan holds when there are fewer
Decoders than clusters. A Decoder weight on a cluster that holds no Engine goes
unused. A Decoder share above its cap (see
[Per-cluster caps](#per-cluster-caps)) holds the plan. A cluster that no longer matches any term has no Decoder
weight, so while it drains its share can fall to the one Decoder every cluster
with Engines receives. Placement counts the Decoders it still runs against the
larger of that share and the Decoder minimum its copy holds, so they do not
look like extra Engines that use up the move's allowance. During a move, the
Decoder total, and the Decoders of a cluster whose Engine share is not falling,
follow [Static shares](#static-shares).

When any term sets `weights`, the accepted plan records the Decoder's weight,
and the Router's when routers are divided, under
`status.placement.candidates[].allocation.componentMatches`. A label change
that alters a weight therefore produces a new plan before any member changes.
Each allocation and each component match also records the weight as
`minWeight`, which replaces `weight` over time. When `minWeight` is present, it
equals `weight`. A plan accepted before `minWeight` existed carries only
`weight`, so readers fall back to `weight` when `minWeight` is absent. `weight`
stays until every stored plan carries `minWeight`. Upgrade the CRDs before the
controller. A new controller on the old CRD writes a plan whose `minWeight` the
API server drops. It reports `PlanNotPersisted` for one reconcile, then
continues with plans that carry only `weight`.

Traffic follows what each cluster can serve. When a Decoder is split by its
own weights, each cluster's ready count is measured against the service's
Engine-to-Decoder ratio rather than its own floors: in the example, 3 Engines
with 6 Decoders and 1 Engine with 6 Decoders serve in the ratio 2:1. Moving
Decoders between clusters follows [Moving replicas](#moving-replicas): a
cluster keeps its Decoders until their replacements elsewhere are ready.

A Split source whose terms set no weights divides `router.minReplicas` across
the clusters that hold an Engine, by the Engine shares, as `weights.default: 1`
on every term would. With 4 Engines on two clusters and `router.minReplicas: 3`,
the clusters run 2 Engines each, and 2 and 1 routers; the extra router goes to
the cluster whose name sorts first. Setting `weights.default` or
`weights.router` on any term divides the routers by the same rules as the
Decoder. With default weights 3 and 1 and `router.minReplicas: 3`, the
clusters run 2 and 1 routers. Weights that name only the Engine or the Decoder
keep the full router policy on every cluster: to weigh the Engine without
dividing routers, set `weights.engine` instead of `weights.default`. The
Decoder then follows the Engine's weight. Each cluster gets at least one
router, and the plan holds when there are fewer routers than clusters. Prefer
two or more per cluster, because a cluster whose only router restarts serves
nothing until it returns. Dividing routers requires a positive
`router.minReplicas`. Each cluster's routers are
pinned at its share, or bounded by their cap (see
[Per-cluster caps](#per-cluster-caps)).

`weights` is valid only in static `Split`. `Single`, `All`, and
`SplitByCapacity` reject it; `SplitByCapacity` shares every component by
capacity and runs the full router policy on each cluster.

Status shows how the split came out. In both Split modes, each candidate with
a Decoder or a divided router lists `componentFloors`: per component, the
`current` floor on its member copy and the `desired` floor at the accepted
plan's shares. A `current` floor above `desired` is held until its
replacements on other clusters are ready. When any term sets `weights`, the
advisory `PlacementComponentShares` condition names, per component, whether it
follows the Engine shares or its own weights (reason `OwnWeights` when any
component does), and lists weights that go unused because their cluster holds
no Engine. Neither changes placement or readiness.

## Static shares

```yaml
spec:
  placement:
    policy: ClusterAffinity
    mode: Split
    clusterAffinity:
      - weights: {default: 3}
        matchExpressions:
          - key: region
            operator: In
            values: [west]
      - weights: {default: 1}
        matchExpressions:
          - key: region
            operator: In
            values: [east]
    split:
      replicas: 8
    maxSurge: 1
```

With one matching registration per region, this assigns six replicas to the
west member and two to the east member. Multiple registrations in either region
each receive that term's weight. Terms are ORed, and requirements within a term
are ANDed. When weights are present, all matching term weights add; an omitted
weight contributes one. With no explicit weights, every matching registration
has equal weight, including one matched by several terms.

`matchExpressions` supports `In`, `NotIn`, `Exists`, `DoesNotExist`, `Gt`, and
`Lt` with Kubernetes node-selector semantics. `matchFields` supports `In` and
`NotIn` against the actual `metadata.name`. Weights are valid only for `Split`
and have no upper bound. Omitting affinity is unconstrained. An explicit empty
or null affinity is invalid.

Shares use largest remainders, with cluster-name order breaking ties. Three
replicas across two equally weighted registrations produce two and one, never
two each. A zero share creates no member workload. An unreachable matched
registration retains its desired share; spare quota elsewhere cannot absorb it.

In Split, the ratios in [Per-cluster replica ratios](#per-cluster-replica-ratios)
are an alternative to `split.replicas`, `split.maxPerCluster` and the integer
weights. Each service uses one form or the other.

The fleet floor comes from `split.replicas`, or an explicitly positive
`engine.minReplicas` when that override is absent. An unresolved floor holds the
plan. One replica unit is a complete Engine instance; a multi-pod instance stays
together on its member. Routers are divided by the Engine shares unless the
terms set weights that name only the Engine or the Decoder (see
[Component weights](#component-weights)).

A declared Decoder follows the Engine in the ratio of their minimums: each
home's Decoder floor is its share of `decoder.minReplicas`, apportioned by
largest remainder over the homes' Engine shares so the per-home Decoder floors
sum to the declared minimum exactly; while a move holds more or fewer Engine
replicas than the fleet floor, the Decoder total follows the Engine total in
the same ratio, rounded up. While Engines above the minimum are in flight, a
member whose Engine share is not falling gets no more Decoders than the larger
of what its copy holds and its share at the new Engine shares, so it does not
start a Decoder that the move then removes.
When `split.replicas` sizes the fleet, the
Decoder minimum scales with it in the declared ratio, rounded up; without an
Engine minimum it is used as declared. A home with a positive Engine share always
receives at least one Decoder when the Decoder minimum is positive; a Decoder
minimum smaller than the number of homes with a positive share holds the plan
with a message that says so. A zero Decoder minimum places no Decoder. An
undeclared Decoder minimum pairs one Decoder with each Engine. The ratio is a
floor, not a cap: within their bands the two components scale independently on
the member. Placement reads live counts only to delay lowering a Decoder floor;
see [Moving replicas](#moving-replicas).

A proportional `scalingPolicy` keeps the counts placement writes on a Split
home. The scaling coordinator clamps each follower to that component's bounds,
and placement pins them, so a home whose share differs from the policy's ratio
reports the follower as `Clamped` in `status.scalingPolicy`. For example, a
home with 1 Engine and 6 Decoders under a 1:3 policy does this. On a Split home
the clamp raises no `ScalingTargetClamped` warning, because placement made it
on purpose. Single and All homes carry the source's own bounds and warn like a
local service.

Traffic weights count a home's ready replicas in Engine units against its own
floors, rounding down. A home that has admitted at least one Engine together
with its share of Decoders keeps at least one unit while it has a ready
instance of every component. For example, a home with 1 Engine and 3 Decoders
still serves traffic while one Decoder restarts, instead of dropping out of
the routing table.

### Per-cluster caps

`split.maxPerCluster` caps how many replicas of each component one member may
run. A Split service in the ratio form uses the max ratios of
[Per-cluster replica ratios](#per-cluster-replica-ratios) instead. Its keys are the
components, plus `default`:

| Component | Cap |
|---|---|
| Engine | `engine`, else `default` |
| Decoder | `decoder`, else `default`, else the Engine's cap scaled by the Decoder-to-Engine ratio of minimums, rounded up |
| Router | `router`, else `default`, else the Engine's cap scaled by the router-to-Engine ratio of minimums, rounded up |

Without an Engine, the Decoder takes the Engine's place in this table. A
member's maximum for a component is its cap, so its autoscaler can grow up to
it. A component without a cap is fixed at its share. A share above its cap
holds the whole plan: `PlacementConverged=False`, reason
`AllocationUnresolved`.

For example, take a service with `maxPerCluster: {engine: 18, router: 8}`. A
member whose shares are 6 Engines and 2 routers can autoscale to 18 Engines and
8 routers. Its Decoders can grow to 18 scaled by their ratio of minimums.
`maxPerCluster: {default: 18}` caps all three at 18, without scaling.

`router` applies only when Split divides the routers (see
[Component weights](#component-weights)). A router that keeps the full router
policy keeps the source's own bounds, so validation rejects a `router` key
there. It also rejects a key for a component the service does not declare.

`split.maxReplicasPerCluster` is removed. `maxPerCluster: {engine: 18}` caps
exactly as `maxReplicasPerCluster: 18` did; for a service without an Engine, use
`decoder`. The API server rejects the old field under strict field validation.
Other clients get a warning and the field is dropped, so that source is planned
without a cap.

## Per-cluster replica ratios

In Split, a service can give each cluster its own minimum and its own maximum
for each component. The values are ratios, not replica counts. The service's
own `minReplicas` and `maxReplicas` are the fleet totals, and OME divides each
total across the matched clusters by the ratios.

Take a service whose Engine declares `minReplicas: 12` and `maxReplicas: 40`.
cluster-a is the larger cluster. It should run two thirds of the Engines and
may grow to 30 of them. cluster-b should run the rest and stop at 10.

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: example-service
  namespace: team-a
spec:
  engine:
    minReplicas: 12
    maxReplicas: 40
  placement:
    policy: ClusterAffinity
    mode: Split
    clusterAffinity:
      - weights:
          engineReplicas: {min: 2, max: 3}
        matchFields:
          - {key: metadata.name, operator: In, values: [cluster-a]}
      - weights:
          engineReplicas: {min: 1, max: 1}
        matchFields:
          - {key: metadata.name, operator: In, values: [cluster-b]}
```

The cluster-a member gets 8 to 30 Engines: 12 times 2/3, and 40 times 3/4. The
cluster-b member gets 4 to 10. The members' minimums add up to the service's 12
and their maximums add up to its 40.

### Keys

Each term's `weights` block takes four optional keys. Each holds a ratio
`{min, max}`. Both numbers are optional, and a number that is set is at least 1.

| Key | Applies to |
|---|---|
| `engineReplicas` | Engine |
| `decoderReplicas` | Decoder |
| `routerReplicas` | Router |
| `defaultReplicas` | Any component without its own key |

- `min` divides the component's `minReplicas`. `max` divides its `maxReplicas`.
- `{min: 2}` on one cluster and `{min: 1}` on another give the first cluster
  twice the second's share, whatever the totals are.
- A null `min` counts as 1, and a null `max` counts as 1. Each defaults on its
  own: `{min: 2}` is `{min: 2, max: 1}`, `{max: 3}` is `{min: 1, max: 3}`, and
  `{}` is `{min: 1, max: 1}`. So `{min: 2}` beside `{min: 1}` divides the
  minimum 2 to 1 but the maximum 1 to 1. Write `max` to divide the maximum too.
- Each component looks up its ratio in the order it uses for integer weights.
  Engine uses `engineReplicas`, then `defaultReplicas`. Decoder uses
  `decoderReplicas`, then `defaultReplicas`, then the Engine's. Router uses
  `routerReplicas`, then `defaultReplicas`, then the Engine's.
- When several terms match one cluster, their ratios add. A matching term with
  no ratio for the component counts as `{min: 1, max: 1}`.
- Routers are divided when any term sets `defaultReplicas` or `routerReplicas`.
  Otherwise every cluster runs the service's full router policy.

### How a member's bounds are set

- A member's `minReplicas` is its share of the service's `minReplicas`, split
  by the `min` ratios in whole replicas with largest remainders.
- A member's `maxReplicas` is its share of the service's `maxReplicas`, split by
  the `max` ratios across the clusters that receive a member. A cluster whose
  minimum share rounds to zero gets no member and no part of the maximum.
- When a cluster's maximum share comes out below its minimum share, OME raises
  the maximum to the minimum and emits a `SplitCeilingRaised` Warning event. The
  members' maximums can then add up to more than the service's `maxReplicas`.
- When the service leaves a component's `maxReplicas` unset, each member is fixed
  at its share and its autoscaler cannot grow it. If such a component
  references an AutoscalerPolicy, admission returns an `UnboundedSplitCeiling`
  warning that names the component.
- Each cluster's allocation in `status.placement.candidates` records its min
  ratio as `minWeight` and its max ratio as `maxWeight`.

### Recipes

Equal clusters that autoscale to a fleet ceiling. Each cluster runs 3 to 10
Engines.

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: example-service
  namespace: team-a
spec:
  engine:
    minReplicas: 9
    maxReplicas: 30
  placement:
    policy: ClusterAffinity
    mode: Split
    clusterAffinity:
      - weights:
          defaultReplicas: {min: 1}
        matchFields:
          - {key: metadata.name, operator: In, values: [cluster-a, cluster-b, cluster-c]}
```

A small Decoder fleet that is weighted differently from the Engines. cluster-a
runs 8 to 16 Engines and 3 to 6 Decoders. cluster-b runs 4 to 8 Engines and
exactly 3 Decoders. The terms name only the Engine and the Decoder, so every
cluster runs the full router policy.

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: example-service
  namespace: team-a
spec:
  engine:
    minReplicas: 12
    maxReplicas: 24
  decoder:
    minReplicas: 6
    maxReplicas: 9
  placement:
    policy: ClusterAffinity
    mode: Split
    clusterAffinity:
      - weights:
          engineReplicas: {min: 2, max: 2}
          decoderReplicas: {min: 1, max: 2}
        matchFields:
          - {key: metadata.name, operator: In, values: [cluster-a]}
      - weights:
          engineReplicas: {min: 1, max: 1}
          decoderReplicas: {min: 1, max: 1}
        matchFields:
          - {key: metadata.name, operator: In, values: [cluster-b]}
```

### One form per service

A service uses either the integer form or the ratio form, never both. The API
server refuses a service that:

- sets a `*Replicas` key and an integer weight (`default`, `engine`, `decoder`
  or `router`), on one term or on different terms;
- sets a `*Replicas` key together with any `spec.placement.split` block, even an
  empty one, because the ratios replace `spec.placement.split`;
- sets a `*Replicas` key outside `mode: Split`.

Admission applies the same check in Go. Every YAML example in this page is
tested against it, so a copied example never mixes the forms.

### Moving from the integer form

A service keeps its members when each integer weight becomes a `min` ratio,
every `max` is 1, and each `maxReplicas` is the old cap times the number of
matched clusters.

| Integer form | Ratio form |
|---|---|
| `weights: {default: 3, decoder: 2, router: 1}` | `weights: {defaultReplicas: {min: 3, max: 1}, decoderReplicas: {min: 2, max: 1}, routerReplicas: {min: 1, max: 1}}` |
| `split.replicas: R` | the Engine's `minReplicas: R` |
| `split.maxPerCluster: {engine: 6, decoder: 4, router: 6}` over 3 clusters | `engine.maxReplicas: 18`, `decoder.maxReplicas: 12`, `router.maxReplicas: 18` |
| no `maxPerCluster` for a component | leave that component's `maxReplicas` unset |

Two cases need more care:

- When `split.replicas` differs from the Engine's `minReplicas`, rescale each
  other component's `minReplicas` by the same factor, so their shares stay the
  same.
- When two terms match one cluster, their ratios add, so with every `max` at 1
  that cluster gets twice the maximum. Rewrite the terms so each cluster
  matches exactly one, or set each term's `max` so every cluster's sum is equal.

## Moving replicas

All four modes use the shared surge and member acknowledgement protocol for
moves between homes. Single's initial admission race is separate from moving
an existing winner.

`Single` persists its nominated homes before creating workloads, then commits
the observed winner before cleaning up race losers. One admitted instance of
each declared component suffices to win; admission need not fill the requested
floor. Simultaneously observed winners are ordered by cluster name. A restart
retains accepted nominations and the committed winner. Only that winner can be
published for routing, even while loser resources await cleanup.

A matching Single winner remains sticky. An affinity change that excludes it
can start a bounded replacement race while the current winner keeps serving.
Unknown member health retains the winner's identity and allocation; a missing
connection does not prove that its workloads have stopped. A terminally failed
winner remains selected and reports Failed. Until the terminal condition clears,
later source changes are not applied to that winner.

`maxSurge` is a whole-replica allowance shared by placement and member rollout
surge. An omitted allowance blocks moves between homes. Zero permits no excess;
a move may wait indefinitely if it needs a replacement before removing a
serving replica.

Placement persists each allocation before applying it. Participating members
pause new rollout surge and defer autoscaler growth while a move owns the
allowance. Existing rollout reservations and occupied replicas consume that
same allowance. Placement-authorized floor growth and cleanup remain possible.
Members must acknowledge the current policy and their replica limits before
placement can spend capacity.

A replacement must be ready and routable before it can justify reducing a
serving home. Removing a home also requires publication of its zero traffic
weight for the exact plan. A missing or stale publication holds the move.
A settled positive allocation can release its pause without global routing;
this does not authorize any further replica movement.

The same rule applies to Decoder floors in the Split modes. A home's Decoder
floor can fall while its Engine share is unchanged, for example when a new
Engine minimum re-apportions the Decoders. The home keeps its current Decoder
floor until ready Decoders on serving homes exceed the Decoder minimum by the
replicas it would release; Decoders that are not ready leave at once. While a
floor is held, `PlacementConverged` is False with reason `ReplacementNotReady`
and a message that names each held home.

A cluster that OME holds above its new Decoder share keeps those Decoders until
the replacements serve. The move's surge allowance counts them as part of that
cluster's own share, at the Engine-to-Decoder ratio of its member copy, so they
never stop other clusters from growing to replace them. A finished move still
waits for them to leave. A higher Decoder ratio can also raise the share of a
cluster that the plan removes above the minimum its member copy sets. OME stops
writing that copy, so the cluster acknowledges the move at the copy's minimum,
and the move does not wait for Decoders that OME never asks the cluster to add.

### Single replacements

A replacement probe is a complete candidate home. Its full resolved floor must
fit the shared allowance: with an original floor of two and `maxSurge: 2`, only
one additional two-replica home can race at a time. A smaller allowance cannot
create a partial home. Existing rollout occupancy and reservations also consume
this allowance.

One admitted instance of every component selects a replacement. The original
winner remains protected until that replacement reaches its complete Engine,
Decoder, and Router floors and has published routing. The serving winner changes
when the original is requested to drain, or after its absence is verified.
Unselected probes receive no traffic. A cancelled replacement that already
serves retains its route until it can be safely drained. Physical cleanup of
the original and losing probes must finish before the surge pause can be released.

An optional positive `spec.placement.replacementTimeout` bounds how long a
probe may wait for component admission. Expired probes are retired and their
resources removed before another probe can spend their allowance. The persisted
start time survives controller restarts. This timeout does not apply to a
selected replacement waiting for full readiness. A terminally failed selected
replacement retains the original winner and reports `ReplacementFailed` until
its configuration is repaired or the move is cancelled.

If the original disappears during an accepted move, placement verifies that its
derived service, InferenceReplicas, and Pods are absent before releasing its
current allocation. Its original floor remains the budget baseline. Cancelling
the move can restore an eligible original home within the same allowance; a
serving replacement stays protected until the restored home is fully ready and
routable. Unknown inventory holds this recovery. After a handoff, complete loss
of every home requires verified empty inventories and the configured winner-loss
grace before a fresh initial race can begin.

## Full-policy homes

`All` retains the complete source policy on each eligible member; `Single`
applies it to nominated members and its retained winner. Placement
resolves component minimums using source, runtime, then operator configuration
precedence. It requires a nonnegative resolved floor for every component; Engine
and Decoder floors may differ and the Engine count is the home's replica unit,
with Router keeping its own floor. An unresolved floor holds that home's
allocation without changing local-service behavior, and the `PlacementConverged`
message names the home and the cause. Operator defaults require the configured
member operator namespace and an identified ConfigMap.

An unpaused zero-floor home retains its service and autoscaler through scale to
zero. Single still needs actual admission to select a winner; an idle established
winner remains selected. All keeps each eligible full home, including homes with
different inherited floors. Zero floors grant no ready or routable capacity.

Retiring a home or adding a positive All allocation after initial provisioning
requires positive component floors. `PositiveFloorRequired` retains the accepted
plan while those guarantees are absent. Configuring positive minimums refreshes
retained homes before shared movement accounting begins; observed autoscaled
replicas do not substitute for these minimums. Outside a move, a retained home's
minimum can change to or from zero. A zero-floor update during a paused move
holds that move until positive floors are restored.

While member growth is paused, a finished move waits until the members' replicas,
with rollout reservations, fit within the plan's minimums. `PlacementConverged`
is False with reason `AwaitingMemberCleanup` during that wait. The wait can last
as long as autoscalers keep replicas above the new minimums. The two cases below
end the pause without waiting for the autoscalers, and an operator can end the
wait of one plan revision with an annotation, as the paragraphs after them say.

An `All` change that keeps every member and needs no more Engine replicas on any
member than when the pause began moves no replicas between members. It ends the
pause once every member has applied its new minimums, even while autoscalers keep
replicas above them or a rolling update holds a surge replica. Only Engine
minimums are compared with where the pause began: a raised Decoder minimum with a
lowered Engine minimum also ends the pause. In `All`, a raised Engine minimum, or
a member that joins, leaves or drains, keeps the wait.

In `Split`, an Engine cap (see [Per-cluster caps](#per-cluster-caps)) lets
autoscalers keep a member above its share. A change in which no member leaves or drains then ends
the pause once every member serves at least its new Engine and Decoder shares,
even while autoscalers keep more replicas than those shares. This applies whether
the change raises, lowers or moves the shares, through `split.replicas`, the
Engine minimum or a `weights` block, and when a member joins: the new
member must serve its shares too. With a `weights` block, each member must serve
its Decoder share as the weights split it (see
[Component weights](#component-weights)). When routers are divided, each
member must also serve its Router share. A member that already has enough ready
replicas takes a higher share at once, so its autoscaled replicas do not use up
the move's surge
allowance. For example, two members at equal weights and a ceiling of 18 each
get 6 of 12 Engines, and each autoscaler runs 9. Raising `engine.minReplicas` to
16 raises each share to 8. The pause ends once both members have applied the new
shares, because each already serves at least 8 Engines. A replica that a rolling
update is replacing does not count as serving, so a member at exactly its new
share waits until that update finishes. A member that is not eligible, for
example one OME cannot reach, keeps the wait. So does a member that keeps its
old Decoder or Router count until new ones on another member serve, and a member
that leaves or drains. `SplitByCapacity` keeps the wait too. Without an Engine
cap, OME sets each member's maximum to its share, so its autoscaler cannot
keep extra replicas.

An operator can end the wait for one plan revision without changing the
service. Use it when the move is done but the pause defers autoscaler growth the
service needs, for example under load that keeps members above their new
minimums. Read the current revision, then set it as the value of
`ome.io/placement-release-cleanup-wait` on the source InferenceService on the
control plane:

```sh
revision="$(kubectl get isvc example-model -n team-a -o jsonpath='{.status.placement.plan.revision}')"
kubectl annotate --overwrite isvc example-model -n team-a "ome.io/placement-release-cleanup-wait=${revision}"
```

If that revision's move has finished and only this wait holds the pause, OME
ends the pause on its next reconcile, with a new plan revision. Autoscalers and
rollouts on the members may add replicas again. The release itself removes no
replicas, and a member with a smaller share may still be removing some, so for
a while the members can hold more replicas than the plan assigns. OME then
removes the annotation and records a `CleanupWaitReleased` event that names the
revision and the replicas the members held. The `PlacementConverged` message it
writes when it ends the pause names the release too.

The annotation ends only this wait. A move keeps waiting while members have not
acknowledged the pause or applied their targets, while a member that leaves
still holds replicas, or while a replacement does not serve yet. The annotation
stays while that move runs at the same revision and takes effect when it
finishes. A move that takes another step makes a new revision instead: OME
removes the annotation, and the operator sets it again with the new revision if
the wait returns. To cancel a request that is still waiting for its move,
remove the annotation:
`kubectl annotate isvc example-model -n team-a ome.io/placement-release-cleanup-wait-`.
If the pause ends on its own first, OME removes the annotation with a
`CleanupReleaseSkipped` event. A value that names another revision, or a plan
that does not pause, is removed at once with a `CleanupReleaseSkipped` event. A
value that is not a revision number, such as `latest` or `0`, is removed with a
`CleanupReleaseRejected` warning event. Every new plan revision makes an older
value stale, so a request can never end a later pause.

If OME cannot remove a request, it records a `CleanupReleaseNotRemoved` warning
event with the error and tries again on its next reconcile. A request for a
revision the plan has not reached yet then stops placement for the service until
the annotation is gone, so the request cannot end that revision's pause. Any
other request can no longer act, and placement goes on.

Set the annotation on the live object with `kubectl`, not in the service's
manifest in Git. OME removes it once it has answered it, so a copy kept in Git
makes the live object differ from the manifest. Each sync then writes back a
revision that is no longer current, and OME removes it again. Placement strips
the annotation from member copies and leaves it out of the plan's inputs, so
the annotation itself changes no member and starts no rollout. Only the end of
the pause reaches the members, as it does when a pause ends on its own.

The plan stores each current and desired home policy separately. A replacement
must fit its entire floor within the shared allowance: a two-replica home cannot
start with only one surge replica available. Outgoing homes retain their accepted
floors and templates until replacement readiness and traffic drain permit
removal. During a pause, inherited minimums are fixed to the accepted values;
applying the approved source policy restores inheritance.

Initial provisioning inventories standing member resources before allowing
retirement. Missing matched members can be retried independently while that
initial source intent stays unchanged. Unknown unmatched homes, changed intent
during incomplete inventory, and unidentified registrations hold movement.
`AwaitingHomeInputs` reports incomplete inventory or unresolved full-home floors.
`spec.placement.mode` is fixed for the life of the service: admission refuses an
update that changes it, or that adds or removes the `spec.placement` block, and
names both values. Delete and recreate the service to place it another way. A
stored plan whose mode differs from the spec (a change that reached storage
without admission) holds with the condition reason `PlacementModeChangeBlocked`
until the spec names the accepted mode again.

## Capacity input contract

The capacity adapter reads each matched member through its identified direct
connection, resolves the declared Engine and Decoder from that member's runtime
and configuration, and joins the result to the management root's fleet report.
Report UID, version, flavor mapping, and registration UID must agree. An empty
member can supply a complete sample; admission and running workloads are not
prerequisites for computing nominal weights.

One unit is one Engine instance plus the Decoder demand that the ratio of their
minimums attaches to it, including all leader and worker pods; equal minimums
use one instance of each. Each required resource/flavor pool contributes its
whole-unit quotient of allocatable hardware divided by that demand; the smallest
quotient is the member's weight. Quota budgets, usage, routing weights, and
high-water marks do not enter this calculation. Ambiguous flavors, stale
reports, and unavailable members return no partial allocation or equal-share
fallback.

The adapter requires OMENative for every declared component because placement
observes InferenceReplica execution and shared surge accounting. Local services
retain their existing backend choices. Required node constraints are projected
onto the complete flavor catalog's label keys for nominal attribution; topology,
model readiness, and other scheduling constraints remain member admission
checks. These weights do not guarantee physical fit.

Configure the input adapter through
`ome.multicluster.config.placement` in the resources chart, emitted as
`multicluster.placement` in `inferenceservice-config`:

```yaml
memberOperatorNamespace: operator-system
capacity:
  rootName: capacity-root
  maxAge: 5m
  stabilityWindow: 1m
  refreshInterval: 10s
```

These are example values, not binary defaults. The block defaults to null; every
field is required when present, and `refreshInterval` must be shorter than
`maxAge`. The root name must match the management and member quota collectors.
All members must expose their configuration in
`multicluster.placement.memberOperatorNamespace`. This setting also locates
pinned runtimes for Single, All, and static
Split. An absent namespace holds resolution when those dependencies are needed.
When upgrading, move it from `placement.capacity.memberOperatorNamespace` to
`placement.memberOperatorNamespace`.
The adapter uses the existing placement `fanoutTimeout` for remote operations.

The resources chart grants the control-plane manager read access to fleet quota
reports only when capacity placement is configured. Member access also requires
read access to quota roots, ResourceFlavors, and RuntimeClasses.

Capacity collection must provide identified, complete per-cluster flavor reports.
The placement credential needs reads for the local quota root, ResourceFlavors,
configuration, runtimes and their dependencies, and RuntimeClasses. Before
applying a contracted allocation, the controller resolves through the exact
write connection again and checks the accepted rendering, fresh hardware totals,
and root identity. A heartbeat alone may advance the report version without
invalidating an accepted allocation.

Set `spec.placement.mode: SplitByCapacity` and provide the fleet floor with
`split.replicas` or a positive Engine minimum. The first allocation can use a
complete fresh snapshot immediately. A hardware, flavor mapping, or member
identity change must remain consistent for `stabilityWindow` before the
controller accepts it. Unknown reports restart that observation window. A
controller restart also restarts an unaccepted window; the persisted allocation
remains authoritative throughout. Source demand changes on unchanged hardware
do not wait for a hardware stability window.

Hardware events enqueue participating services; heartbeat versions and usage
do not. `refreshInterval` bounds checks for stale reports, runtime/configuration
changes, and missed events. Missing configuration holds this mode pending.
An outgoing home's accepted floor can decrease while preserving its member
specification and rendering contract; retirement does not apply a new source
template or require hardware weights for a home outside the matched set.

Upgrade member CRDs, RBAC, and controllers before enabling contracted
allocations; missing support cannot acknowledge the accepted demand.

## Status and rollout

The routing metrics describe per-member
weights, retained readiness during failed reads, and alert queries for the hub.

`PlacementInputValid` reports syntax and obsolete selector annotations. `PlacementSatisfied`
requires every assigned floor to be admitted; extra admission on one home does
not compensate for another home's shortfall. `PlacementConverged` reports
whether members have applied the allocation. Serving readiness remains
independent, so a held allocation can continue serving.

For valid affinity, the `PlacementInputValid` message lists zero-based term
indexes with no matching registered WorkloadCluster. An unready matching member
still counts as a match. Unmatched terms do not invalidate the request or change
allocation; they help distinguish inventory or selector mistakes from member
availability problems.

The accepted plan records source and cluster identities, desired and current
floors, matching terms, and weights. Unknown identity or member observations
hold existing authority. `PlacementCapacityFresh` is true when the matched
hardware reports and demand are usable, false with `CapacityUnknown` when any
required input is unavailable, and unknown when this reconcile has not checked
them. `CapacityStabilizing` holds a changed hardware snapshot through the
configured window. Both holds preserve the accepted plan and member floors;
capacity placement never falls back to equal shares.

Upgrade member CRDs and controllers before expecting moves to progress. Missing
fields or missing member acknowledgements hold movement. Complete or stop an
active transition before downgrading to a member binary that does not enforce
placement pauses and replica limits. Ordinary local services do not acquire
these guards.

Execution annotations use an explicit versioned envelope. Members can still
read existing flat policies, but reject unsupported versions or fields before
reconciliation writes. Upgrade members before the source emits envelopes;
flat-only policy readers cannot establish allocation authority from them.
Deleting a service can still follow finalizer cleanup, and ordinary local
services ignore placement execution annotations.

## Retiring Legacy placement

Complete migration using a compatibility release **before** installing this
schema or binary. Audit all sources, derived copies, and writers, including
resources managed outside GitOps. Update each source in one write:

- Set `policy: ClusterAffinity` and an explicit mode.
- Replace `requirements`, `clusterSelector`, and both selector annotations with
  `clusterAffinity`. Combine requirements that were ANDed in the same term.
- Remove `split.spread` and `split.minReplicasPerCluster`, including explicit
  false, zero, empty, or null values. Choose equal or weighted static shares.
  Packed placement has no equivalent.
- Move placement `capacityFactors` to `spec.routing.capacityFactors`.

Wait for accepted plans, placement convergence, and Ready status. Every derived
copy must retain its source ownership and carry `ome.io/placement-policy:
ClusterAffinity` plus a valid `ome.io/placement-execution` envelope. Compare ISVC,
InferenceReplica, and Pod identities to detect unintended replacement. Require
the explicit policy in writer validation so a later promotion cannot restore
Legacy intent.

The retired fields are absent from the schema. Strict field validation rejects
them; non-strict clients may have them pruned before admission sees them. Do not
rely on an upgrade to preserve or translate their restrictions. Selector
annotations are rejected, and policy and mode are required.

Neither the policy nor the mode can change on an existing source: changing
placement ownership or the mode requires draining and recreating it. Rollback
must use a ClusterAffinity-capable release. Restoring a schema does not restore
removed restrictions or make a Legacy writer safe.

Execution envelope versions 1, 2, and 3 and existing flat policies remain
readable. Status recovery and endpoint-publisher cleanup are separate
compatibility contracts. The policy metric now carries `policy` and `mode`;
selector/spread labels and the per-cluster minimum metric are removed.
