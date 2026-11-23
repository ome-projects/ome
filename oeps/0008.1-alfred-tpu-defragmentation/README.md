# OEP-0008.1: Alfred TPU partition defragmentation

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Terminology](#terminology)
  - [The failure: free chips, no whole cube](#the-failure-free-chips-no-whole-cube)
  - [Why free space stays scattered](#why-free-space-stays-scattered)
  - [Why Alfred does not help today](#why-alfred-does-not-help-today)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [What must be true](#what-must-be-true)
  - [The unit of work is a drain, not a move](#the-unit-of-work-is-a-drain-not-a-move)
  - [Steering replacements with a fence](#steering-replacements-with-a-fence)
  - [What changes in Alfred](#what-changes-in-alfred)
  - [User stories](#user-stories)
  - [Risks and mitigations](#risks-and-mitigations)
  - [Expected effect](#expected-effect)
- [Design Details](#design-details)
  - [Observation](#observation)
  - [Choosing a drain](#choosing-a-drain)
  - [Predicting where a replacement lands](#predicting-where-a-replacement-lands)
  - [Running a drain](#running-a-drain)
  - [Removing the fence](#removing-the-fence)
  - [Throughput and budgets](#throughput-and-budgets)
  - [Quota for the replacement](#quota-for-the-replacement)
  - [Permissions](#permissions)
  - [Configuration](#configuration)
  - [Observability](#observability)
- [Test Plan](#test-plan)
- [Graduation Criteria](#graduation-criteria)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
<!-- /toc -->

## Summary

Tl;dr: on a TPU cluster that cuts slices on demand, free hosts end up scattered
across many cubes, so a workload that needs a whole cube cannot start even when
the cluster has room for dozens. Alfred's defragmentation policy only reads GPU
nodes, so nothing ever gathers that free space back together. This OEP adds a
TPU defragmentation policy to Alfred. It picks the emptiest cubes, cordons them,
moves the OMENative Instances on them onto new slices elsewhere (the replacement
serves before the original stops), and uncordons each cube once it is empty.

The policy reuses OEP-0008's arbiter budgets and cooldowns, the rule that only a
steady Component may move, maintenance windows, the opt-out annotation,
recommend-only mode, and the TPU slice move the node-health policy already
dispatches. It adds a TPU model of cubes and partitions, a decision unit that is
a set of cubes rather than one move, an engine component that sets and always
removes its own cordons, and a quota check for the replacement. The dispatcher
needs a small change to tell TPU moves from GPU ones.

## Motivation

### Terminology

- **Chip, host.** A TPU host carries a fixed number of chips (four in the
  examples here). Kubernetes sees each host as one node.
- **Partition.** A block of hosts that the hardware can hand to one workload as
  a unit, named by its shape in chips, e.g. `2x2x1` (one host), `2x2x2` (two
  hosts), `2x2x4` (four), `2x4x4` (eight), `4x4x4` (sixteen). Each host carries
  one partition-id label per shape (`cloud.google.com/gke-tpu-partition-<shape>-id`)
  and a matching state label. Partitions nest: a `4x4x4` holds two `2x4x4`,
  each of which holds two `2x2x4`, and so on down to single hosts.
- **Cube.** The largest single partition, `4x4x4`: sixteen hosts. A workload
  larger than one cube (e.g. `4x4x8`) takes whole cubes from one *TPU pod*, the
  set of cubes the optical fabric can join.
- **Slice.** The object (`slices.accelerator.gke.io`) that claims one partition
  for one workload. When an InferenceService sets
  `ome.io/tpu-slice-provisioning: "true"`, OME creates one slice per Instance on
  pools in `PROVISION_ONLY` mode and binds the Instance's pods to it.
- **slice-scheduler.** The external controller that picks which partition a
  slice gets. OME and Alfred do not choose the partition; they only create the
  slice.
- **Whole free partition.** A partition whose hosts are all healthy,
  schedulable, carry no slice and run no accelerator pod. Only a whole free
  partition can take a new slice of that shape.
- **Packing bound.** For a shape of `h` hosts in one TPU pod: the number of free
  hosts divided by `h`, rounded down. It is how many whole free partitions of
  that shape the pod would hold if its free hosts were packed perfectly.
- **Instance.** As in OEP-0008: the atomic unit of an OMENative
  Component, one or more pods that start, stop and move together. On these
  pools, one Instance holds exactly one slice.

### The failure: free chips, no whole cube

Example. A TPU pod has 144 cubes (2,304 hosts). About 710 hosts are free, so
the packing bound for `4x4x4` is 44 cubes. Only 20 cubes are wholly free. The
other 390 free hosts sit in 60 cubes that each run between one and fifteen
small slices. An operator deploys a model whose Instance needs one `4x4x4`.
Replica 21 stays pending: slice-scheduler finds no whole free cube, and it will
not take a partly used one. Nothing in the cluster will ever create one, short
of small Instances happening to leave.

The same gap appears one level down, smaller but real: in that pod, whole free
`2x2x4` partitions reach about 80% of their bound.

### Why free space stays scattered

Three facts combine. Each is a property of a component working as designed.

1. **Removals ignore placement.** When OME scales a Component down, rolls it, or
   moves an Instance, the Instance it removes is chosen by revision and health,
   not by where its slice sits. Each removal leaves a hole in an arbitrary cube.
2. **New slices fill holes, but not the fullest cube first.** slice-scheduler
   prefers the partition whose largest wholly free ancestor is smallest (a
   best-fit rule). That keeps a new `2x2x1` out of a free cube while any one-host
   hole exists. Among holes of equal size, however, it takes the first in its own
   iteration order. A hole in a nearly empty cube is as likely to be filled as a
   hole in a nearly full one, so a nearly empty cube rarely finishes emptying.
3. **Nothing moves a running slice to consolidate.** OME creates and deletes
   slices but never moves one to free a partition. slice-scheduler takes a
   partition from a running slice only by preemption, which evicts the pods
   first and only for a higher-priority slice.

Therefore the scatter is the steady state, not an incident. Placement fixes for
(1) and (2) slow its growth; they do not undo the scatter already present. That
takes moving Instances, which is this proposal.

### Why Alfred does not help today

Alfred (OEP-0008) already moves TPU Instances, but only to evacuate a node:

- **The defragmentation policy only reads GPU nodes.** It builds its candidates
  per GPU pool and skips any Instance with zero GPUs. Turning it on for a TPU
  cluster does nothing.
- **The TPU move exists, but only the node-health policy uses it.** When a node
  is unhealthy or carries a maintenance trigger, the node-health policy asks
  OMENative to move each Instance off it. For a TPU Instance, Alfred first checks
  that some whole free partition of the Instance's shape exists, and then lets
  slice-scheduler place the new slice.
- **Alfred cannot pick the destination.** It never names a partition. If the
  source cube still has free hosts, the replacement may land right back in it,
  and the cube never empties.

An operator can drain a cube today by putting a maintenance trigger on its
sixteen hosts. That works once, but it borrows the meaning of "this node is
being retired", Alfred never uncordons the hosts afterwards, and nobody chooses
which cubes are worth draining. See [Alternatives](#alternatives).

### Goals

1. Bring whole free partitions of an operator-chosen target shape (e.g.
   `4x4x4`) close to the packing bound, and keep them there. Smaller shapes
   (e.g. `2x2x4`) gain as a side effect and are reported.
2. Move only OMENative Instances on OME-provisioned slices, and only with a
   replacement that is serving before the original stops.
3. Spend a bounded, configurable number of moves, inside the arbiter's existing
   global budgets and inside configured maintenance windows.
4. Never leave a host cordoned by this policy after a drain ends for any
   reason, including across Alfred restarts. There are two exceptions. A host
   that became unhealthy or entered maintenance during the drain stays cordoned
   and passes to the node-health policy or the operator. A host carrying
   `alfred.ome.io/hold-cordon` stays cordoned for the operator.
5. Publish the gap between whole free partitions and the packing bound, so an
   operator can see whether the policy is needed before enabling it.

### Non-Goals

- **Holding capacity back for large shapes.** This policy creates whole free
  cubes; it does not stop small workloads from growing into them later. A
  reservation is a separate proposal.
- **Changing slice-scheduler.** Its tie-break and its preemption rules are
  inputs here. Changes there are worth proposing upstream, separately.
- **Moving work across clusters.** Each cluster is planned on its own.
- **Static TPU pools.** Pools not in `PROVISION_ONLY` mode have fixed slices
  and are skipped. GPU defragmentation (Policy #1) is unchanged.
- **Moving non-OME slices or non-OMENative workloads.** A cube holding one of
  those is never drained.
- **Running more than one move at a time.** Alfred dispatches serially today.
  Relaxing that is a separate, safety-critical change (Open Question 1).

## Proposal

### What must be true

1. **A drain must not cost more whole free cubes than it creates.** Every
   predicted replacement placement must avoid wholly free cubes anywhere in the
   cluster, and every cube in the drain must be predicted to empty. *Failure mode
   if skipped:* Alfred spends moves and breaks one free cube to free another.
2. **Every move is place-then-free.** The Instance keeps serving until its
   replacement on a new slice is ready, which the existing OMENative move
   already guarantees.
3. **The fence is proven before the first move.** Alfred dispatches no drain
   move until it has observed every fenced host cordoned and a settle interval
   has passed. *Failure mode if skipped:* the first replacement lands inside the
   fence, the cube never empties, and a move is wasted.
4. **Alfred removes exactly the cordons it set.** A host it cordons carries an
   annotation naming the drain. Alfred uncordons only hosts that carry the
   annotation, and every such host is either uncordoned or passed on when the
   drain ends.
   *Failure mode if skipped:* capacity silently disappears, one cube at a time.
5. **Budgets are global.** Drain moves go through the arbiter like every other
   candidate, so they count against the same caps and cooldowns, and node-health
   candidates win any contest for the next dispatch.
6. **Quiescent by default.** The policy acts only when the gap to the packing
   bound reaches a threshold, and only inside a configured maintenance window.
   Enabling `execute` with no window configured is a configuration error.

### The unit of work is a drain, not a move

Policy #1 scores one move at a time: benefit (fragmentation before minus after)
minus cost. That fails for cubes. A cube with four small Instances becomes free
only on the fourth move, so the first three moves score zero benefit and the
greedy loop never starts. Worse, scoring individual moves favours moves that
nudge a score without ever finishing a cube.

Therefore the TPU policy plans a **drain**: a set of partitions of the target
shape (cubes, for `4x4x4`) in one TPU pod, plus every Instance whose slice sits
on them. A drain either empties all of its cubes or it is not proposed. Its moves
are then emitted as candidates, one Instance each.

### Steering replacements with a fence

Alfred cannot tell slice-scheduler where to put a replacement. It can only take
partitions out of the running. The **fence** is a cordon on every host of the
drain's cubes, set before the first move. A cordon adds a `NoSchedule` taint to
the node. The partition-tree controller copies node taints onto the partition,
and slice-scheduler, run with taint filtering, skips any partition that carries
one. Every replacement therefore lands outside the drain.

A fenced host carries the annotation
`alfred.ome.io/cordoned-for-defragmentation`. Its value names the drain, the
partition of the unit shape the host belongs to, and the drain's deadline. It
also records when the host was fenced and whether the host was free then. The
annotation is the drain's only durable state: it survives Alfred restarts, and
the policy reads open drains back from it, including how long their fences have
stood.

The fence also keeps *other* new slices out of the drain while it
runs. That is intended: a scale-up landing in a cube being emptied would undo the
drain. The cost is bounded by the drain size (default four cubes, 64 hosts) and
its deadline.

### What changes in Alfred

OEP-0008 requires a policy to be a pure function of the snapshot: no client, no
Events, no metrics. The dispatcher re-runs a policy before each dispatch to check
that its candidate still holds. So the work splits like this:

| Piece | Kind | Does |
|---|---|---|
| TPU defragmentation policy | new policy, pure | Reads the snapshot, including fence annotations. Returns *drain intents* (cubes to fence) and, for drains whose fence is proven, one move candidate per remaining Instance. |
| Defragmentation fencer | new engine component, next to the maintenance cordoner | Turns drain intents into cordons and annotations, proves the fence, and removes or passes on the fence when a drain ends. It is the only writer of the annotation. |
| Snapshot | changed | Carries each node's fence annotation and TPU pod id. For the quota check, it also carries each pod's Kueue queue label and the quota status of the queues those labels name. |
| Dispatcher | changed | Maps TPU defragmentation moves to their own policy name in the journal. Skips the GPU placement re-check for them, and runs the TPU feasibility re-check instead. |
| Arbiter | unchanged | Applies the existing caps, cooldowns and priority rule. |

### User stories

**Story 1: the operator looks before enabling.** An operator turns the policy on
in `recommend-only` mode. Alfred publishes, per TPU pod, the whole free target
partitions and the packing bound (here 20 and 44 cubes), and records each drain
it would run, with its cubes and Instances. Nothing is cordoned.

**Story 2: a pod reaches its bound over a few windows.** In `execute` mode,
inside the maintenance window, Alfred opens a drain of up to four cubes, fences
their 64 hosts, waits for the fence to settle, and moves their Instances one at
a time. Each cube is uncordoned the moment it empties. The pod in the example
needs about 80 moves. Alfred runs one move at a time, so at 10–15 minutes per
move (`TBD`: measured move time), that is 14–20 hours of dispatch, spread over as
many windows as it takes.

**Story 3: a Component scales during a drain.** New slices from the scale-up
cannot land on fenced hosts, so they go elsewhere. Alfred moves an Instance only
while its whole Component is steady (every Instance ready and serving, one
revision), so it skips that Component's Instances until scaling ends. If the
drain's deadline passes first, the remaining cubes are uncordoned.

**Story 4: a replacement never becomes ready.** OMENative fails the move at its
ready timeout and the original Instance keeps serving. The drain marks that cube
as not freeable and removes its fence. Repeated failures trip the arbiter's
circuit breaker, which stops all moves for an hour.

**Story 5: a service must not move.** Its InferenceService carries
`alfred.ome.io/movable: "false"`. Any cube holding one of its slices is never
chosen for a drain.

### Risks and mitigations

| Risk | Mitigation |
|---|---|
| Each move cold-starts a model server. | Moves are place-then-free, serial, capped per hour, and run only in maintenance windows. The opt-out covers services whose start-up is too slow or costly. |
| A drain move in flight delays a node-health evacuation, because Alfred dispatches serially. | The delay is at most one move's duration; the health candidate takes the next dispatch. Open Question 1 covers relaxing serial dispatch. |
| The fence makes free hosts in drained cubes unusable for new work while it runs. | Default drain size is four cubes (64 hosts). Each cube is uncordoned as soon as it empties. A drain ends at its deadline or at the window's close, whichever comes first. |
| slice-scheduler does not honour cordons (taint filtering off, or a version without it). | Detect it: if any replacement lands inside the fence, end the drain, remove the fence, and stop draining that TPU pod for a backoff period. Emit `FenceNotHonored`. |
| A replacement cannot start because its queue has no quota left. | Check quota headroom before choosing a cube (see [Quota for the replacement](#quota-for-the-replacement)). |
| An operator cordons a fenced host by hand during the drain; Alfred cannot tell, and uncordons it at the end. | Accepted and documented. An operator who must hold a fenced host sets `alfred.ome.io/hold-cordon`, which the fencer honours (see [Removing the fence](#removing-the-fence)). |
| A replacement lands in a cube a later drain will empty, so an Instance moves twice. | Accepted. In replay, draining four cubes at a time cost 7–8% more moves than fencing every target cube at once. |
| Churn scatters free space again. | Drains open whenever the gap reaches the threshold. Placement-side fixes (Alternatives 3 and 4) reduce how often. |
| Alfred restarts mid-drain. | The annotation survives on the node. On start-up, the policy reads open drains back; the fencer ends any whose deadline has passed. |

### Expected effect

The numbers below come from a replay of recorded snapshots of production TPU
pods against an emulation of slice-scheduler's best-fit rule. The replay
tooling is outside this repository. To check the emulation, we replayed the
slices that were actually created and deleted over 18 hours. The deletion times
were unknown, so the replay brackets them: all deletions before all creations,
and the reverse. The observed count of whole free cubes fell inside that range,
or within two cubes of it, in every pod. Treat the figures as estimates, to a
first approximation.

| | Whole free `4x4x4`, share of bound | Whole free `2x2x4`, share of bound |
|---|---|---|
| Before | 45% | 82% |
| After one pass (all drains run) | 100% | over 99% |

- **One pass** moved about 6% of the Instances on the replayed pods.
- **Keeping it** cost about 2% of Instances per day. After 18 hours of real
  churn with no drains, the count of whole free cubes held roughly steady, but
  the bound grew because removals outnumbered arrivals, so the share fell to
  87–93%. Re-draining restored 98%.
- **The binding constraint after a pass is the number of free hosts, not their
  layout.** The policy cannot exceed the packing bound.

These assume every Instance is movable, every Component is steady when Alfred
wants to move it, quota allows every replacement, and every move succeeds. One
18-hour window may not represent a day with a large rollout. `TODO`: re-measure
over a week once alpha runs on a test pod, and measure move duration.

## Design Details

### Observation

Alfred's snapshot already carries every node's labels and every accelerator pod
on it. The policy derives, per cluster and per TPU pod:

- For each node: its partition id and state for each shape, using the existing
  `tpuSlicePartitions.idLabel` and `stateLabel` templates; whether it carries a
  slice (`tpuSlicePartitions.sliceLabel`); cordon and readiness; its fence
  annotation (new snapshot field); and the pods on it.
- For each partition: its hosts, and whether it is wholly free.
- For each cube: the slices on it, the Instances that own them, whether every
  one of those Instances is movable, and its TPU pod.

The TPU pod comes from a node label (`tpuSlicePartitions.podLabel`, new). On
GKE, `cloud.google.com/gce-topology-block` takes one value per TPU pod, and
`cloud.google.com/gce-topology-subblock` equals the host's `4x4x4` partition id.
Hosts without the label are left out of planning and of the gauges, because
they may belong to different TPU pods.

Alfred does not read slice objects, and this policy does not need to. Today's
TPU move check treats a host with no slice label and no accelerator pod as free;
this policy makes the same trade. `TBD`: whether reading slice objects is worth
the RBAC, to catch a slice claimed before its pods start.

### Choosing a drain

```
unit = the largest shape in targetShapes          # 4x4x4 by default

if any drain is open in the cluster: stop

for each TPU pod where packingBound(unit) - wholeFree(unit) >= minGap,
    largest gap first, ties to the lower pod id:
  skip the pod if it is in fence-failure backoff
  candidates = partitions of the unit shape where:
      every host is healthy, schedulable and carries no fence,
      at least one host is busy, at most maxBusyHostsPerPartition are,
      every slice belongs to a movable OMENative Instance,
      every owning Component is steady and has quota headroom
  sort candidates by (Instances to move, busy hosts, partition id)

  D = none
  for c in candidates, while len(D) < maxDrainPartitions:
      plan = predict(D + c)                          # next section
      if every Instance has a placement, no placement breaks a wholly free
         partition of the unit shape, and plan moves at most
         maxMovesPerWorkloadPerDrain Instances of one InferenceService:
          D = D + c

  if D: emit a drain intent for D, and stop
```

At most one drain is open in a cluster. A drain's replacements may be predicted
to land in another TPU pod, so a second drain could fence the very hosts the
first one counts on. Each would then wait for its deadline, holding both fences.
A second drain would also add no throughput: Alfred runs one migration at a time
per cluster, so drains cannot move Instances in parallel. They would only hide
more free hosts at once.

Every partition in a feasible drain is predicted to empty, so a larger feasible
drain always frees more of them. The loop therefore adds each candidate that
keeps the drain feasible, up to `maxDrainPartitions`, and skips one that does
not. Stopping at the first infeasible candidate instead would let one partition
whose Instances fit nowhere block every drain behind it, which is common on a
nearly full pod. Sorting by Instances to move first, not by busy hosts,
keeps the move count down: two hosts held by one `2x2x2` Instance cost one move,
while two hosts held by two `2x2x1` Instances cost two.

`maxMovesPerWorkloadPerDrain` exists because the arbiter's cooldown is per
InferenceService (30 minutes by default). A drain that needs twelve moves of one
InferenceService would take at least five and a half hours, longer than the
default deadline. With the default of four, a drain leaves out each partition
that would take one InferenceService past the limit, and a later drain takes it.
A single partition holding more Instances of one InferenceService than the
limit is never drained. To include such partitions, raise the limit, and the
deadline with it.

### Predicting where a replacement lands

The prediction copies slice-scheduler's documented rule for shapes up to one
cube. Among wholly free partitions of the Instance's shape outside the fence, it
takes the one whose largest wholly free ancestor is smallest; ties go to the
lowest partition id. It places the drain's Instances in the order their moves
are submitted (the cube with the fewest moves left first, then by name), each
against the state the previous placements left.

The prediction covers every TPU pod in the cluster that the Instance's pods can
run on, not only the drain's pod, because slice-scheduler may place a
replacement in any of them. The "never break a wholly free cube" check is
cluster-wide for the same reason.

Note the limit: the prediction decides whether a drain is *feasible*, not where
each replacement *will* land. slice-scheduler's real tie order follows its own
iteration and can differ. That is why the contract in
[What must be true](#what-must-be-true) is checked against the prediction before
the drain, and against the observed slice after each move.

### Running a drain

1. **Fence.** The fencer cordons every host of the drain's cubes and writes the
   annotation. It fences only when a migration API is configured, since
   otherwise no move could follow. A failed cordon ends that drain before any
   move. It does not
   block other dispatch: unlike the maintenance cordon, a missing defragmentation
   fence only means no drain move is emitted.
2. **Prove the fence.** The policy emits move candidates for a drain only when
   the snapshot shows every host of it cordoned with the annotation, every cube
   that still holds a slice has all its hosts, and at least
   `fenceSettleSeconds` (default 60) have passed since the latest fence time
   recorded in those annotations. The clock therefore survives restarts.
   The settle interval covers the partition-tree controller copying the taint.
   `TBD`: read the partition objects' taints directly instead (Open Question 4).
3. **Emit candidates.** Each pass emits one candidate per remaining Instance,
   with reason `TPUFragmentation` and the existing TPU slice plan. The arbiter
   admits them under its usual rules, one dispatch at a time.
4. **Check each outcome.** When OMENative reports a move done, the policy reads
   the new slice's hosts from the next snapshot. If any is inside the fence (a
   host recorded as free when fenced now holds a slice), the drain ends (see
   the risk table). If the new slice broke a wholly free cube, the policy
   records it; two such outcomes in one TPU pod start the backoff.
5. **Release cubes as they empty.** When no slice is left on a cube, the fencer
   removes that cube's fence at once, without waiting for the rest of the drain.
   An emptied cube is only ever released: if part of a release fails, its other
   hosts are not fenced again.

### Removing the fence

A drain ends when any of these happens:

- every cube in it is empty (success);
- its deadline passes (`drainDeadlineMinutes`, default 240), or the maintenance
  window that opened it closes, whichever comes first;
- a replacement lands inside the fence, or someone uncordons a fenced host
  (both end it as `fence_not_honored` and start the backoff);
- a cordon fails during fencing;
- the mode leaves `execute`, the policy is disabled, or no migration API is
  configured.

When a drain ends, the fencer visits every host that still carries its
annotation:

- **Pass on** if the host is unhealthy, or a maintenance trigger now matches it.
  The fencer removes its annotation and leaves the cordon. If the matching
  maintenance rule cordons, the fencer writes that rule's maintenance annotation,
  as the maintenance cordoner would have, because the maintenance cordoner skips
  hosts that are already cordoned.
- **Hold** if the host carries `alfred.ome.io/hold-cordon`. The fencer removes
  its annotation and leaves the cordon to the operator.
- **Uncordon** otherwise, and remove the annotation.

*Failure mode this leaves open:* a plain `kubectl cordon` on a host that is
already fenced changes nothing on the node, so Alfred cannot see it, and
uncordons the host when the drain ends. Goal 4 takes priority here, because the
alternative is fences that are never removed. The `hold-cordon` annotation is the
documented way to keep such a host.

On start-up, the policy reads open drains back from the annotations. The fencer
ends any drain whose deadline has passed, by the rules above.

### Throughput and budgets

Three existing limits decide how fast a drain runs:

- **Serial dispatch.** Alfred has at most one unresolved migration per cluster,
  and submits on a decision pass every five minutes. A move therefore takes
  the longer of its own duration and the next pass. At 10–15 minutes per move,
  that is four to six moves an hour, below the default hourly cap of ten.
- **Per-InferenceService cooldown (30 minutes).** It applies across all of an
  InferenceService's Components. `maxMovesPerWorkloadPerDrain` keeps a drain
  inside its deadline.
- **Steady Components only.** A Component that is scaling or rolling out is not
  moved. A drain that depends on a Component which never settles inside the
  window ends at its deadline.

Upkeep fits easily. At about 2% of Instances per day, a pod of about a thousand
Instances needs about one move an hour. A first pass on a badly scattered pod
takes most of a day of dispatch time.

### Quota for the replacement

A move starts the replacement Instance next to the original, so for a while the
Component holds one extra Instance. When the Component's pods are admitted by
Kueue, the replacement needs quota for that Instance. Without headroom, Kueue
keeps the replacement's pods waiting, so the move waits out its ready timeout
and fails. Enough failures trip the circuit breaker, which stops health
evacuations too.

Therefore the policy treats a Component as movable only if its ClusterQueue has
unused quota of at least one Instance's chips, read from the queue's status.
This needs read access to Kueue's `clusterqueues` and `localqueues`, granted
in Alfred's main ClusterRole, because recommend-only mode makes the same check
when it plans drains. If a queue's status cannot be read, a Component whose
pods name that queue is not movable. A Component whose pods carry no queue
label needs no check.

### Permissions

Cordoning and uncordoning need permission to patch nodes. Today only the
opt-in maintenance cordon has it, behind the `--enable-maintenance-cordon` flag,
and its stated contract is that Alfred never uncordons. This policy gets its own
flag (`--enable-defragmentation-fence`) and its own ClusterRole: get and patch
on nodes, nothing else. Without the flag, the policy can run only in
recommend-only mode.

This OEP amends OEP-0008 on one point. Alfred uncordons a node, but only a node
carrying the defragmentation annotation, which Alfred itself wrote.

### Configuration

```yaml
policies:
  tpuDefragmentation:
    enabled: false
    # The largest shape is the drain unit; smaller shapes are reported.
    targetShapes: ["4x4x4", "2x2x4"]
    # Act only when packing bound minus whole free partitions of the unit shape
    # reaches this, counted in partitions of the unit shape.
    minGap: 2
    # Partitions of the unit shape fenced by one drain; one open drain per cluster.
    maxDrainPartitions: 4
    # Never drain a partition with more busy hosts than this.
    maxBusyHostsPerPartition: 8
    # Keeps a drain inside its deadline under the per-InferenceService cooldown.
    maxMovesPerWorkloadPerDrain: 4
    drainDeadlineMinutes: 240
    fenceSettleSeconds: 60
    # After a fence is not honoured in a TPU pod, wait this long before
    # draining there again.
    fenceFailureBackoffMinutes: 1440
tpuSlicePartitions:
  # One value per TPU pod. TBD: confirm the key is stable across node image
  # versions before making it the default.
  podLabel: cloud.google.com/gce-topology-block
```

`maintenanceWindows`, the caps, the cooldowns and the opt-out annotation are the
existing OEP-0008 settings and apply unchanged. Validation rejects
`enabled: true` with mode `execute` and no `maintenanceWindows`. Windows cannot
wrap midnight today, so an overnight window is written as two windows.

### Observability

- `alfred_tpu_free_partitions{tpu_pod, shape}` and
  `alfred_tpu_packing_bound{tpu_pod, shape}`: the gap the policy acts on. The
  label is `tpu_pod` because Prometheus attaches its own `pod` label to every
  scraped series. The engine publishes them from the snapshot even when the
  policy is disabled, so an operator can decide whether to enable it.
- `alfred_tpu_drains_total{outcome}`: `completed`, `deadline`, `window_closed`,
  `fence_not_honored`, `cordon_failed`, `aborted`.
- `alfred_tpu_fenced_hosts`: hosts carrying the defragmentation annotation right
  now. Should read 0 outside a drain; an alert on a non-zero value lasting longer
  than the drain deadline catches a leak.
- Events on the InferenceService for each move (reason `TPUFragmentation`), and
  on each node when the fencer fences it, uncordons it or passes it on.
- Each drain intent, with its cubes, Instances and predicted gain, in the
  existing recommendations ConfigMap.

## Test Plan

- **Unit.**
  - Drain selection on synthetic pods: adds each candidate that keeps the drain
    feasible; opens no drain while one is open in the cluster, and picks the
    TPU pod with the largest gap when two qualify; never
    selects a cube with a non-movable, non-OMENative or unhealthy occupant, or
    a Component without quota headroom; respects
    `maxMovesPerWorkloadPerDrain`; returns nothing under `minGap`.
  - Prediction: matches the best-fit rule on hand-built trees, including a
    replacement that would land in another TPU pod and a case where the only
    placement breaks a wholly free cube (drain rejected).
  - Purity: the policy returns the same result when re-run on the same snapshot,
    as the dispatcher's pre-dispatch check does.
  - Fencer: success, deadline, window close, fence not honoured, cordon failure,
    disable mid-drain, restart with a stale annotation, a host that turns
    unhealthy (passed on), a host that gains a cordoning maintenance trigger
    (maintenance annotation written), and a host with `hold-cordon` (held).
  - Dispatcher: a TPU defragmentation move is journalled under its own policy
    name and skips the GPU placement re-check.
- **Integration (the existing Alfred kind harness).** Nodes carry partition
  labels; a stub stands in for slice-scheduler and places slices by the best-fit
  rule, honouring cordons. Assert: no move before the fence settles,
  replacements land outside the fence, cubes empty, every annotation is gone at
  the end, and the global caps hold.
- **Replay.** Feed recorded snapshots and slice churn into the policy with the
  emulated scheduler. Acceptance for alpha: at least 95% of the `4x4x4` packing
  bound after one pass, zero broken free cubes, zero leaked fences.

## Graduation Criteria

- **Alpha:** the policy behind `enabled` and the new flag, recommend-only by
  default, on one test TPU pod. Metrics published. Replay acceptance met. Move
  duration measured.
- **Beta:** in `execute` on production pods for two weeks: whole free `4x4x4` at
  or above 90% of the bound, upkeep at or below 3% of Instances per day, zero
  leaked fences, and no move failure attributable to the policy. Open
  Question 1 answered.
- **Stable:** default-on wherever `tpuSliceMigrationEnabled` is on, with the
  placement-side fixes (Alternatives 3 and 4) landed or explicitly declined.

## Drawbacks

- **It treats a symptom.** Better placement would make most moves unnecessary.
  This policy is the only thing that repairs scatter already present, but on its
  own it pays for scatter forever, at about 2% of Instances per day.
- **Every move is a cold start.** For services with long model loads, the cost
  is real even though capacity never dips.
- **Alfred reads more of GKE's partition labels.** It already reads one shape's
  partition labels to count free partitions for TPU slice moves. This policy
  reads every shape's, plus the TPU pod label. All of them stay behind the
  `tpuSlicePartitions` label templates.
- **Alfred starts uncordoning nodes,** which OEP-0008 deliberately avoided. The
  annotation limits it to nodes Alfred fenced, but a bug in the fencer now
  touches node scheduling.
- **The fence briefly hides free capacity** from every scheduler, not only from
  replacements.

## Alternatives

1. **Drain cubes with the existing maintenance trigger, driven by an external
   script.** Works today with no Alfred change. *Tradeoff:* it reuses the
   "node is being retired" meaning, so anything else reading that label
   misreads it; Alfred never uncordons; and the script plans outside Alfred's
   view of budgets and cooldowns. Fine for a one-off pass, wrong for a
   recurring job.
2. **Use slice-scheduler preemption.** Give large slices a higher priority and
   let slice-scheduler evict small ones. *Tradeoff:* preemption stops the small
   Instance before it is replaced, so serving capacity dips, and it only frees a
   cube when a large slice is already waiting. It defragments on demand at the
   cost of an outage for every Instance it evicts.
3. **Placement-aware scale-down in OME.** When a Component scales down, remove
   the Instance in the emptiest cube among equally ranked ones. *Tradeoff:* none
   in disruption, since the removal happens anyway. It slows new scatter but
   never repairs existing scatter, and it acts only when Components shrink.
   Worth doing regardless; a separate small change.
4. **Fullest-cube tie-break in slice-scheduler.** Among equally tight holes,
   prefer the one in the fullest cube. *Tradeoff:* an upstream change to a
   component OME does not own. Like Alternative 3, it slows scatter but does
   not repair it.
5. **Extend Policy #1 in place** (treat each cube as a bin). *Tradeoff:* its
   per-move scoring cannot see that the last of four moves frees a cube, and its
   target-node hints mean nothing when slice-scheduler picks the partition. The
   shared parts (arbiter, dispatch, windows) are reused either way.
6. **Do it in the OME controller.** *Tradeoff:* the controller reconciles one
   InferenceService at a time and has no view of a whole cube.

I recommend this proposal together with Alternatives 3 and 4 as separate
changes: the policy repairs what is scattered now, and the two placement fixes
cut how often it has to run.

## Open Questions

1. **Should TPU slice moves be allowed to run in parallel?** Serial dispatch
   makes a first pass take 14–20 hours of dispatch on the example pod, and makes
   a drain move delay a health evacuation. Allowing *k* concurrent slice moves
   per cluster (each to a different InferenceService) would cut both, but it
   changes the dispatcher's one-unresolved-request invariant, which every
   policy relies on. I think it is worth a separate proposal after alpha
   measures move duration. *Owner: TBD.*
2. **Should a drain also fence the next cubes it will drain?** Fencing more
   cubes avoids moving an Instance twice, at the cost of hiding more free hosts.
   Replay says the saving is about 7% of moves; I think it is not worth it at
   alpha. *Owner: TBD.*
3. **How does this interact with a future reservation of whole cubes** (a pool
   of pre-provisioned large slices, say)? A reserved cube must be invisible to
   drain selection and must count towards the gap. *Owner: TBD.*
4. **Should the fence be proven by reading partition objects?** The settle
   interval is a guess about how fast the partition-tree controller copies a
   taint. Reading the partition objects proves it exactly, at the cost of read
   access to a provider CRD. The fence-not-honoured detection bounds the damage
   of a guess that is too short to one wasted move per TPU pod per backoff.
   *Owner: TBD.*
5. **Multi-cube shapes** (`4x4x8` and up) take any whole free cubes in a TPU
   pod, so raising whole free cubes serves them too. Whether the gap metric
   should also report "largest multi-cube shape that fits" is open.

**Scope:** OMENative Instances on OME-provisioned slices in `PROVISION_ONLY`
TPU pools. GPU defragmentation, static TPU pools, and RawDeployment or LWS
workloads are unaffected.

**See also:** [OEP-0008](../0008-alfred-gpu-cluster-caretaker/),
the engine this extends, in particular its section on TPU instances on
per-Instance slices.
