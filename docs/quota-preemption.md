# Quota preemption

A queue can borrow idle accelerator quota without evicting any workload. When
the owner needs that quota back, reclamation determines whether its pending
workloads can evict eligible borrowers or must wait for them to finish.

Set `spec.preemption` on a `ClusterQueue` leaf `AcceleratorQuota` to configure
these decisions. The management controller copies the policy to every member
receiving that leaf. Each member renders it into its Kueue `ClusterQueue`.
`Cohort` nodes cannot set this field; policies are not inherited from parents.

For example, add this fragment to an existing leaf to allow reclamation while
keeping the other preemption mechanisms disabled:

```yaml
spec:
  preemption:
    reclaimWithinCohort: Any
    withinClusterQueue: Never
    borrowWithinCohort:
      policy: Never
```

The policy belongs to the **incoming workload's queue**. Setting `Never` on a
borrowing queue does not protect its running workloads from another queue's
reclamation policy.

## Policy fields

| Field | Accepted values | Meaning |
|---|---|---|
| `reclaimWithinCohort` | `Never`, `LowerPriority`, `Any` | Recover entitlement from eligible borrowers. `Any` permits selecting workloads even at equal or higher priority. |
| `withinClusterQueue` | `Never`, `LowerPriority`, `LowerOrNewerEqualPriority` | Replace workloads inside the same queue. The last value also permits newer equal-priority workloads. |
| `borrowWithinCohort.policy` | `Never`, `LowerPriority` | Evict eligible lower-priority borrowers to obtain additional borrowed quota. |
| `borrowWithinCohort.maxPriorityThreshold` | Signed 32-bit integer | Restrict workloads selected by borrowing-driven preemption to priorities at or below this value. Zero is a valid threshold. |

`borrowWithinCohort.policy: LowerPriority` requires `reclaimWithinCohort` to be
`Any` or `LowerPriority`. Kueue does not support borrowing-driven preemption
with Fair Sharing enabled. The threshold has no effect when borrowing-driven
preemption is disabled.

All fields are optional. OME renders only the supplied settings; Kueue's
default for each omitted policy is `Never`. Installing the feature with no
authored policy preserves the existing behavior. To explicitly disable quota
preemption, set all three policies to `Never`.

Removing a policy field previously owned by OME restores Kueue's default for
that field without replacing the queue. Fields owned by another manager keep
their values; omission does not override an external policy.

## Borrowing and ownership

Preemption does not enable borrowing by itself. A borrower needs a positive
`spec.budgets[].borrowingLimit`, available lendable quota, and a compatible
resource/flavor in the same member's cohort tree. OME preserves its existing
zero borrowing limit when that budget field is omitted.

The preemption policy applies to the whole leaf, across its budgets and member
clusters. It does not change nominal quota, per-cluster shares, replica counts,
or workload priority. A workload's priority and the incoming queue's policy
together determine whether it may be selected for eviction.

Author the settings on the source `AcceleratorQuota`. The quota controllers
apply their objects without taking ownership of conflicting Kueue fields by
force. If another controller owns a different preemption value, reconcile that
ownership before enabling the same field through OME. A leaf that never sets
`preemption` leaves externally owned policy fields alone.

## Serving impact and rollout

Install the matching CRDs and quota controllers on the management plane and
members before authoring policies. Policy changes update existing queues and
can permit eviction as soon as Kueue observes them. They do not require a
serving Pod-template change or a fleetwide model restart.

Kueue selects whole Kueue Workloads. Under standalone Pod integration, that can
mean one leader or worker Pod. OME's multi-Pod recovery policy can then rebuild
the affected instance's entire Pod set. Quota admission and eviction do not
preserve a model's prefill/decode ratio or individual replica minimum. A queue's
nominal entitlement is aggregate, not protection for particular Pods inside it.

Validate reclamation and service recovery in a test environment before using
it for interruptible serving capacity. Check the actual group selected for eviction, survivor
cleanup, replacement admission, and serving availability. Kubernetes scheduler
preemption, PodDisruptionBudget handling, and Kueue readiness-timeout eviction
remain separate mechanisms.
