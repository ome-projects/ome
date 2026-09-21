# Model-agent download scheduling

## Select the policy

The agent accepts two startup policies:

| Policy | Remote-download order | Priority-only model updates |
|---|---|---|
| `priority` (default) | `High`, then `Standard`, then `Background` | Change the priority of existing queued downloads, without restarting downloads. |
| `fifo` | Queue arrival order, independent of model priority or serving demand | Do not reorder queued work, cancel active transfers, or start new downloads. |

Set the policy in the OME Helm chart:

```yaml
modelAgent:
  downloadSchedulingPolicy: fifo
```

For a directly configured agent, use `--download-scheduling-policy=fifo`.
Use `priority` to restore priority scheduling. Other values cause a startup or chart-render error.

The policy applies to every model on that agent, for both BYOR and non-BYOR nodes.
Configure each agent deployment separately, including CPU agents when present.
The policy does not select workloads by DAC type.

## Rollout requirement

Changing the Helm value changes the DaemonSet Pod arguments and requires an agent rollout.
The policy cannot change through model events or a live configuration reload.
An old Pod keeps its startup policy until replacement.
During a rolling update, different nodes can temporarily use different policies.

Deploy an image that supports this flag before selecting FIFO.
An older binary rejects the unknown flag.

## What FIFO disables

FIFO ignores both `spec.storage.downloadPriority` and `status.downloadScheduling.servingDemand` when it orders downloads.
It also places startup revalidation work in the same download queue.
Existing priority fields can remain on model resources.
Changes to those fields do not restart downloads or change queue order.

The manager can continue to project serving demand for other agents that use priority scheduling.
Disabling manager-side serving demand alone does not disable explicit model priorities.
FIFO disables both priority sources for the selected agent.

## What remains active

- Dedicated cleanup and local reuse workers.
- UID checks and protection for same-name model recreation.
- Shared-path coordination and protection against unsafe deletion.
- Cancellation of obsolete transfers on deletion.
- Download retries, pending-task retention, and task coalescing.
- Model/node eligibility checks and normal artifact updates.

FIFO is not a rollback to an older model-agent implementation.
Actual URI, storage, deletion, or eligibility changes still take effect.
These changes are not priority-only updates.

## Worker and ordering boundaries

`--num-download-worker` controls remote-download workers.
`--num-high-priority-worker` controls dedicated cleanup and reuse workers, not serving-demand downloads.
Both OCI and Hugging Face remote transfers stay out of the cleanup worker pool.

Cleanup and local reuse can run before downloads under either policy.
A reuse attempt that needs a remote transfer enters the download queue when that need becomes known.
Retries also enter the queue when eligible to run.
FIFO therefore describes ready download-queue order, not the order of all Kubernetes events.
Multiple workers can finish downloads in a different order.

`--task-scheduler-capacity` bounds the runnable queue, not the total retained pending work.
`--same-path-reuse-wait-timeout` limits the wait for another task that populates the same local path.
This timeout does not limit a remote download.
FIFO does not change either setting.

## Local validation

Run these commands from the repository root, with the Rust xet library available:

```fish
go test -mod=readonly ./pkg/modelagent ./cmd/model-agent
go test -mod=readonly -race ./pkg/modelagent ./cmd/model-agent
bash charts/ome-resources/tests/render_test.sh
```

FIFO regression tests cover both model types, explicit and serving-demand priorities, pending capacity, active downloads, UID isolation, cancellation, and reuse.
