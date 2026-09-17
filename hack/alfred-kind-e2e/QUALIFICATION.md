# Alfred control-plane qualification

Original production code under test: `ec58fc07fc2cc3ea354cfdd927d16bfea3050aa6`
(merged main, PR #924). The September 15 qualification changed only tests and
their harness/documentation. These historical results do not qualify later
upstream changes or the subsequent Alfred compact-status compatibility fix.
The preserved dedicated kind control plane was reused
with no inference workloads; all four production processes were rebuilt,
redeployed and verified against loaded image identities. Both workload scheduler
profiles were probed against their mounted configuration.

This is not a production-readiness declaration. KWOK provides virtual GPUs and
synthetic kubelet/readiness behavior. OME, Alfred, the API server, EndpointSlice
controller and both scheduler profiles are real. Pause containers do not serve
inference traffic. Single/gang continuity is sampled, sequential API evidence,
not an atomic cross-resource history or a zero-outage measurement.

## Results

Run date: 2026-09-15. Artifact names below are relative to the private run's
`artifacts/` directory. Each passing migration used a real Alfred request and
OME controller status, not a manually inserted request or migration record.

| Scenario | Observed result | Artifact directory |
| --- | --- | --- |
| Maintenance single | PASS: source on GPU node d stayed Ready/Serving/routed while replacement on c was held; original request and Alfred dispatch completed. | `maintenance-single-20260915T122516Z-84915` |
| Alfred restart single | PASS: changed Alfred Pod UID and recovered leadership; same request completed, one request/migration/dispatch during recorded recovery. | `restart-single-20260915T122724Z-87035` |
| Unhealthy single | PASS: 60-second recovery quarantine without migration, stable unhealthy transition across heartbeats, then completed evacuation. | `unhealthy-single-20260915T122945Z-90808` |
| Whole gang, OME scheduler | PASS: two 8-GPU members moved from zone A to B; held source gang remained healthy and routed; original request and dispatch completed. | `maintenance-gang-20260915T123309Z-99268` |
| Partial gang + manager restart | PASS: 100 samples retained the healthy source, exactly two replacement UIDs and one SurgePending request with only the leader's containers ready; new manager recovered and original request completed. | `partial-restart-20260915T123806Z-7503` |
| No capacity | PASS for bounded abstention: 11 samples over 16 seconds independently proved all three destinations full; no migration/request, healthy routed source. Worker replay returned Unsupported, not Infeasible. | `no-capacity-20260915T124201Z-13913` |
| Useful defragmentation | PASS: free GPUs `[7,1,0,0]`; Alfred moved the 1-GPU source from a to b, and the same previously unschedulable 8-GPU beneficiary became Ready on a. OME recorded Completed. | `useful-defrag-20260915T125114Z-18755` |
| Hinted targets fill after submission | PASS for fallback behavior: while the real request remained unconsumed, both hints filled with scheduler-bound 8-GPU Pods; a previously cordoned non-hinted node became eligible. The resume barrier was verified within 80 seconds; OME completed there with sampled source/handoff safety, and Alfred dispatch completed. | `hint-exhaustion-single-20260915T125344Z-20614` |
| Target health changes after submission | REPRODUCED missing destination guarantee, not a safety pass: both hints acquired custom unhealthy signals after submission while the real request remained unconsumed. The standard scheduler placed the replacement on now-unhealthy node a; OME and Alfred recorded completion. Sampled source/handoff continuity still held. | `target-health-race-single-20260915T125812Z-25556` |

The health-race request `f9e6ebe4-9b11-4ca9-948a-8bb67e080378` was submitted
at 13:00:55.073060776 UTC. Node a's `GpuUnhealthy=True` transition followed at
13:01:06, and node b's at 13:01:16. Both were Node Ready and had no unhealthy
signal before submission (a had no such condition; b had False). The replacement
landed on a while its custom condition remained True. The unused fallback node
remained cordoned. This demonstrates that preflight simulation plus v1 placement
hints do not enforce Alfred's later custom-health exclusions. A strict guarantee
needs scheduling-time enforcement of the relevant health policy; a captured
target list alone would not exclude a listed node that becomes unhealthy later.
Any production/API or scheduler integration change requires a separate proposal.

For partial restart, separate final IR and owner/IR-UID-filtered journal snapshots
each contain one completed migration/dispatch. This does not establish continuous
duplicate-free behavior after recovery. Useful-defrag retained five safe handoff
summaries but only the latest raw Pod/EndpointSlice snapshots; its final Alfred
journal was still acknowledged, so only OME completion is claimed for that case.
The other migration runs retain raw handoff samples (7-9 samples per run).

KWOK lifecycle smoke also passed. The suite exercises readiness gates,
EndpointSlices, held/delayed readiness and termination behavior; the qualification
log retains the smoke result, not a full independently replayable event history.

Two failed harness attempts were retained, not counted as passes: partial restart
`20260915T123449Z-1934` exceeded the original 75-second manager-recovery wait
(shutdown plus the real 60-second leader lease required longer), and useful-defrag
`20260915T124449Z-15608` passed Helm a string threshold that Alfred rejected.
The harness now allows 105 seconds for manager recovery within the existing
two-minute surge budget, and passes/verifies the defrag threshold as a number.
Both corrected cases were rerun above; production code was unchanged.

The health-race assertions completed, but that runner exited 1 during cleanup:
node a's healthy transition appeared after its 30-second KWOK cleanup wait.
The cleanup wait is now 90 seconds with a named timeout diagnostic and an
offline delayed-transition regression (failed before, passed after the change).
A separate scoped cleanup retry succeeded and verified original health
annotations, cordons and manager arguments, with no unhealthy/maintenance signal
remaining. The complete health scenario was not rerun after this timeout-only
change; its retained migration evidence remains the run reported above.

## Reproduction and offline verification

Follow [README.md](README.md) for the exact build/deploy/scenario commands,
versions, serial fixture cleanup and private evidence paths. All binaries were
rebuilt and their deployed image identities checked; both mounted scheduler
profiles were probed. Baselines used the original 30-second acknowledgement
timeout; deliberate mailbox-delay tests use the committed test value of three
minutes. Policy cooldowns and recovery quarantine are one minute in this fixture,
not production defaults.

Local verification passed: `make test` (including Xet; most unchanged packages
used Go's cache), `make ci-lint`, pre-commit, all harness shell regression tests
and `go test ./hack/alfred-kind-e2e/worker-result`. Fresh `-count=1` race runs
passed for `./pkg/alfred/...`, `./cmd/alfred/...`, the nested simulator module,
and the new controller characterization tests. The latter also passed with
surrounding migration acceptance/execution and source-node guard tests.

```bash
go test ./pkg/controller/v1beta1/inferencereplica ./pkg/controller/v1beta1/workload/ops \
  -run '^(TestConsumeMigrationRequests_SourcePodRecreatedBeforeConsumption|TestMigrate_SourcePodRecreatedAfterAcceptance)$' -count=1
```

## Confirmed migration-v1 boundary

New controller-boundary characterization tests delete and recreate an actual
fake-client Pod object under the same name with a different UID. They cover
request-to-consumption and accepted-record-to-execution separately. An old
request is accepted and can migrate the same-instance, same-node successor.
Changing the successor's node causes execution to reject before taking source
ownership or allocating a surge.

This is a limitation of the current v1 contract: it expresses instance/source-node
intent, not the exact requesting-time Pod UIDs. Existing IR-owner and in-flight
source/surge operation ownership checks are distinct, useful protections. The
tests preserve them and do not add API fields or change controller behavior.
These deterministic tests use fake Kubernetes clients and existing readiness
fixtures; they are not live-apiserver source recreation or cache-race evidence.

## Remaining qualification boundaries

- No actual inference traffic, model downloads, GPU processes or production
  container packaging was validated.
- No exhaustive scheduling matrix: PVC/CSI attachment and topology, device/DRA
  resources, custom plugins, admission mutation, affinity/anti-affinity and
  topology spread combinations need dedicated supported-shape tests.
- Non-OMENative eviction, PDB exhaustion, priority/preemption and mixed-profile
  workloads are not live-qualified by this OMENative campaign. Likewise,
  `GpuUnhealthy` is a synthetic signal on a still-Ready virtual node, not a real
  lost kubelet, unreachable node, failed GPU or interrupted inference process.
- No large-cluster performance, API-pressure budget, prolonged outage, upgrade,
  multi-replica controller chaos, security/RBAC abuse, or 24-hour soak evidence.
- A useful-defrag positive example does not establish general beneficiary proof
  or optimal packing. Current policy scoring includes GPU-bin heuristics; real
  simulation checks replacement placement with source occupancy retained.
- A validated `Unsupported` worker response is not a proof of infeasibility.
  The no-capacity scenario independently checks full destinations and observed
  abstention. Its captured worker output is an out-of-band replay through the
  deployed worker, not interception of Alfred's dispatch-preflight response.

Follow-up production/API proposals should cite reproduced failures or explicit
missing guarantees from this qualification, not infer safety from passing unit
tests or these bounded local scenarios.

## September 17: compact-status compatibility

The branch was rebased onto main `f6e1f7f7f47cb8ce58e2cd4f4e55bddfdc63e4a8`
(including the ColumnarV2 codec and default). Production compatibility commit:
`2a6d4647` (the tested binaries were built from the same source before commit).
The compatibility patch adds one
Alfred-owned read-only adapter around the shared IR wire codec and updates five
Alfred readers. Both stored encodings retain Alfred's 10,000-row limit; malformed,
mixed, unknown or oversized representations fail closed. Raw captured objects
are not rewritten. The dependency guard permits only the shared codec package,
not controller or migration implementations. Outside Alfred, changes are harness
and audit/characterization tests; no workload, OMENative, API, status-writer or
scheduler production behavior is changed.

The preserved isolated cluster was empty of inference workloads before this
run. The manager, scheduler and simulator were rebuilt from current main; Alfred
was built before and after the patch. Loaded and running image identities were
verified, and both live scheduler profiles were probed. The manager and scheduler
images were identical across the comparison. CRI image IDs:

| Process | Image ID |
| --- | --- |
| OME manager | `sha256:3e10ec2a89236e323426ea48d8441deca90fb7fac676c4c83a2b1008448d5bf9` |
| OME scheduler | `sha256:b3cc94adce9d8bc6cef23f68ca6c551efbe33a1edb9da6b7adcb257daef5e73c` |
| Alfred before patch | `sha256:b3220fe8e72efa001b2b575819dfcfae332baa2a9f9e8ebaeba54f99ac0c5a2d` |
| Alfred with patch | `sha256:ac6f3a17a9b80d67b8c06f7219d9744246bf6f5af31b21e5f9a4e01d169f2ef3` |

The new `maintenance-columnar` fixture has four instances spread across four
virtual nodes. It requires actual controller-written ColumnarV2 without a dense
field before triggering maintenance; selecting the default alone is insufficient
because the controller retains dense encoding when it is smaller. Raw IRs and
separate decoded views are retained before the trigger and at completion.

| Case | Observed result | Artifact directory |
| --- | --- | --- |
| Pre-fix compact negative control | EXPECTED FAILURE: four Ready/Serving instances, raw ColumnarV2 members `0-3`; maintenance produced `OMENativeObservationInvalid` and no migration request within 30 seconds. | `maintenance-columnar-20260917T194322Z-7757` |
| Fixed compact migration | PASS: source instance 0 on c remained healthy/routed during a three-second held surge; replacement instance 4 bound on a. One request, OME Completed and Alfred completed/drained, eight safe handoff samples. Raw status remained ColumnarV2, members changed from `0-3` to `1-4`. | `maintenance-columnar-20260917T194704Z-23909` |
| Dense fallback migration | PASS: one dense row with no encoding marker before and after; source on c, replacement on d, held-source protection and eight safe handoff samples, OME and Alfred completion. | `maintenance-single-20260917T194855Z-30402` |
| Whole gang, real OME scheduler | PASS: both 8-GPU members replaced as a gang, held source remained healthy/routed, and OME plus Alfred recorded completion and drain. | `maintenance-gang-20260917T195019Z-34018` |
| Useful defragmentation | PASS: the 1-GPU source moved from a to b; the same initially unschedulable 8-GPU beneficiary became Ready on a, and OME recorded Completed. | `useful-defrag-20260917T195139Z-30400` |

The fixed compact request UUID was `be78bb95-0f60-46ad-adff-cbf778d312d7`.
The watch is checked throughout observation and before sealing evidence; an
early exit invalidates request-count evidence. This remains sampled control-plane
evidence, not a claim of uninterrupted inference traffic or production readiness.
The September 15 migration-v1 limitations above are not fixed by this patch.

Fresh verification on the compatibility source passed: full `make test`
(including Xet/envtest; unchanged tests may be cached), `make ci-lint`,
pre-commit, all harness shell regressions, and race runs for Alfred, the two Go
harness helpers, the nested simulator, and the migration source-identity
characterization tests. New semantic regressions failed before the fix for
compact observation, malformed empty inventory, source selection, sparse
occupied indexes, prediction identity and representation-independent dispatch
fingerprints. Upstream reader/field audit tables were updated without relaxing
their scope, access-count or stale-approval checks.

Only cases listed in this September 17 section were rerun on the compact-status
compatibility source. The other September 15 cases remain historical results.
