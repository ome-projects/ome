# Alfred local kind acceptance tests

This harness runs the real Alfred, OME manager, and scheduler binaries against a
local Kubernetes API server. KWOK supplies virtual GPU nodes and simulated
kubelet lifecycle responses. It is a control-plane acceptance test, not an
inference-traffic, GPU, model-download, or production-container-image test.

The test must obtain migration requests from Alfred and migration status from
the real OME controllers. It must not fabricate either. Kubernetes owns the
EndpointSlices; KWOK models pod readiness, including OME's serving readiness
gate, and termination on its virtual nodes only.

## Isolation and versions

- Dedicated Colima profile and Docker context: `alfred-e2e` /
  `colima-alfred-e2e`. No activation of the default Docker context is needed.
- Dedicated kind cluster: `alfred-e2e`. Every Kubernetes operation uses the
  private `${STATE_DIR}/kubeconfig` and explicit `kind-alfred-e2e` context.
  The complete scenario suite supports this default name only; do not override
  `CLUSTER_NAME` when running it.
- kind node image: Kubernetes 1.35.8, digest pinned in `cluster.sh`.
- Workload schedulers and Alfred's simulator: the repository's Kubernetes
  1.35.4 modules. The standard test profile is `alfred-default-scheduler`;
  gangs use `ome-scheduler`. The kind built-in scheduler handles infrastructure
  pods, not the test workloads.
- cert-manager: 1.21.2, for the unchanged OME chart's admission webhook.

The minimal test images package unmodified, CGO-disabled cross-compiled Go
binaries. This avoids a Rust/model-download build for a test that does not run
model downloads. It does not validate the production Dockerfiles.

## Run on an Apple Silicon Mac

Requires Go, Homebrew, Helm 4, kubectl, jq, Mike Farah yq v4, Colima,
Docker CLI/buildx, and kind. The no-benefit runner and offline raw-evidence
verifier use yq v4 to check the original YAML policy.
The namespace-churn runner also uses Perl's `Time::HiRes` monotonic clock.
Provisioning downloads public images; it does not require a registry login.
Helm 4 is required for the explicit `--server-side=false` install option that
lets cert-manager retain ownership of the webhook's CA bundle.

```bash
HOMEBREW_NO_AUTO_UPDATE=1 HOMEBREW_NO_INSTALL_CLEANUP=1 \
  brew install colima docker docker-buildx kind
colima start alfred-e2e --cpu 8 --memory 12 --disk 80 \
  --runtime docker --vm-type vz --mount none \
  --activate=false --ssh-config=false
export STATE_DIR="$(mktemp -d /private/tmp/alfred-kind-e2e.XXXXXX)"
bash hack/alfred-kind-e2e/cluster.sh
bash hack/alfred-kind-e2e/build.sh
bash hack/alfred-kind-e2e/kwok.sh
bash hack/alfred-kind-e2e/deploy.sh
bash hack/alfred-kind-e2e/scenario.sh maintenance-single
bash hack/alfred-kind-e2e/scenario.sh maintenance-columnar
bash hack/alfred-kind-e2e/scenario.sh placement-pause-single
bash hack/alfred-kind-e2e/scenario.sh semantic-retry-single
bash hack/alfred-kind-e2e/scenario.sh restart-single
bash hack/alfred-kind-e2e/scenario.sh unhealthy-single
```

Keep the printed state-directory path: it holds the private kubeconfig,
compiled binaries, and acceptance-test evidence. Do not put it in Git.
`build.sh` also records the loaded CRI image identities in `image-digests.json`.
`deploy.sh` restarts the four test deployments, verifies those running image
identities, and restarts the schedulers after configuration changes before
probing their exact profiles. This prevents reruns from testing stale binaries
or stale scheduler configuration behind the reused `:local` image tags.
The deployment also probes real webhook admission with a server dry-run:
Pod readiness alone can precede the manager's leader election and cache sync.

The single-workload scenarios reset only their own `single` fixture. Gang and
no-capacity tests require otherwise empty virtual GPU nodes, so remove that
fixture and wait for controller-owned children before continuing:

```bash
k=(kubectl --kubeconfig "${STATE_DIR}/kubeconfig" --context kind-alfred-e2e)
"${k[@]}" -n alfred-e2e delete inferenceservice single --wait=true
"${k[@]}" -n alfred-e2e wait --for=delete inferencereplica/single-engine --timeout=90s
"${k[@]}" -n alfred-e2e wait --for=delete pod \
  -l ome.io/inferenceservice=single --timeout=90s
bash hack/alfred-kind-e2e/gang.sh
"${k[@]}" -n alfred-e2e delete inferenceservice gang --wait=true
"${k[@]}" -n alfred-e2e wait --for=delete inferencereplica/gang-engine --timeout=90s
"${k[@]}" -n alfred-e2e wait --for=delete pod \
  -l ome.io/inferenceservice=gang --timeout=90s
bash hack/alfred-kind-e2e/no-capacity.sh
```

`maintenance-columnar` uses four initial Instances and soft topology spread
across the four virtual nodes. It refuses to trigger maintenance unless those
Instances occupy distinct nodes and the real InferenceReplica stores
`ColumnarV2` with columns and no dense status list. KWOK holds nonzero indices;
the runner releases only initial indices 1–3, then holds the migration surge
until the same source/readiness/routing checks used by `maintenance-single`
pass. Replacement detection excludes every baseline Pod UID. Both the healthy
baseline and completed migration must report four ready, serving, available
replicas, and the annotation watch must observe exactly one request UUID.

The runner builds `ir-status` once per scenario into `${STATE_DIR}` for the host
platform. It uses Alfred's bounded status decoder and stops on decode errors.
`ir-before-trigger.raw.json` and `ir-completed.raw.json` retain the API wire
representations; separate `.decoded.json` files hold logical rows and the raw
encoding marker. The evidence verifier requires actual ColumnarV2 at both
snapshots, even when the manager's configured target permits dense fallback.

`placement-pause-single` starts an origin-marked service with a valid public
placement pause envelope. After maintenance is triggered, three distinct Alfred
decision cycles must report an advisory with no request, journal intent,
migration or replacement for that fresh owner/replica. Source readiness and real
routing must remain intact. The harness releases the pause at revision 2 through
ISVC metadata and waits for real IR projection. Once the resulting migration has
allocated a held replacement, it publishes pause revision 3. That same request
must finish through the normal source-preserving handoff and Alfred journal
reconciliation, with no duplicate across three further decision cycles. Raw API
samples and the annotation watch are retained beside the combined evidence.
This tests the public envelope on a local member, not end-to-end multi-cluster
placement, real GPU execution, or retry recovery across a generation change.

`semantic-retry-single` tests the prepared-intent retry path across a real IR
generation change. A temporary admission policy targets only Alfred's additions
of migration request annotations to the fresh fixture UID. After maintenance,
Alfred must persist one prepared UUID/payload, attempt publication, and receive
an API rejection. The witness includes an increase in the API server's counter
for this unique policy, measured after all dry-run rejection probes; a prepared
journal entry alone is insufficient. The canonical Alfred guard stays intact.

The harness publishes placement pause revision 2, removes the temporary binding
while paused, and confirms the exact prepared payload passes server dry-run.
Three paused decision cycles must preserve the source and prepared intent with
no request or replacement. Released revision 3 must then let that same UUID and
byte-identical payload migrate through the usual held-surge/routing checks, with
no duplicate across three completed cycles. ISVC metadata changes its resource
version; the real OME projection advances IR generation while the source Pod and
revision remain unchanged. The old generation-based retry fence instead produces
a bounded three-cycle `SourceChanged` failure witness before acknowledgement
timeout.

This case requires the harness's `ome` Alfred namespace, three-minute migration
acknowledgement timeout, API-server metrics access, and administrator permission
to create/delete the narrowly scoped admission resources and impersonate Alfred
for server dry-run only. It does not change Alfred RBAC. Raw policy objects,
probe responses, metrics, API snapshots and watches remain in the run artifacts.
Cleanup uses recorded admission-object UIDs; if it cannot confirm a safe pause
after a failure, it reports the retained narrow deny policy instead of silently
unblocking a live pending intent. The offline shell/verifier tests run under the
existing `hack/alfred-kind-e2e/*_test.sh` CI loop; no live cluster is used in CI.

`namespace-churn.sh single` and `namespace-churn.sh gang` exercise unrelated
Namespace metadata changes inside the final preflight window, with the real
default-profile worker and OME gang worker respectively. Start with no fixture
ISVCs, IRs or Pods in `alfred-e2e`; remove only the previous completed fixture
before switching between the two cases. The runner wraps the ordinary maintenance
scenario, retaining its source-hold, readiness, routing and completion checks.

A test-image-only wrapper runs the real simulator first, holds its exact output,
and exposes a localhost release barrier. During each of three negative holds the
runner changes labels on a fresh empty Namespace: Alfred must report
`SchedulingStateChanged` without a request, intent or replacement. During the
fourth hold only annotations change: that attempt must submit within 30 seconds,
complete one migration, and remain duplicate-free across three new decision
cycles. Label controls preserve the affinity-input fence but do not exercise an
active cross-namespace affinity rule. No placements or migration status are
fabricated. This does not establish progress under Pod, membership or arbitrary
cluster churn.

The temporary registry uses a 20-second whole-worker timeout and a maximum
10-second post-simulation hold, under the unchanged dispatch deadline. A slow
attempt can still fail closed; no timeout authorizes output. The wrapper is not
included in production images. The runner pins its maintenance marker, retains
raw requests/results and hashes, API mutation responses, release receipts, the
full annotation watch and completion samples. Re-run `verify-namespace-churn.sh`
with the printed artifact directory to verify them offline. Cleanup clears the
fixture trigger before restoring the registry and refuses to overwrite changed
Deployment arguments or volumes. Failure retains diagnostic resources/evidence;
success deletes only the temporary Namespace and ConfigMap by UID.

OME can consume the annotation before a subsequent GET. Publication therefore
requires the exact watched key/value and a matching published journal entry,
not a prepared intent or an annotation that happens to remain visible. The
runner's own watch spans the first hold through all completion cycles. Its
30-second budget starts on the host's monotonic clock before release and ends
after the complete API capture and watched-publication check; host time is never
compared with the VM's wall clock.

The gang fixture has a leader and worker, each requesting eight GPUs, scheduled
by the real OME scheduler into one zone. Migration must replace both members in
the other zone. The no-capacity case fills the three destination nodes with
ordinarily scheduled GPU pods. It independently checks that the worker's actual
snapshot has no spare destination GPU capacity, then requires a validated
non-feasible result with no placements and an unchanged source. The current
worker deliberately returns `Unsupported` when a single-pod scheduling attempt
does not complete. This is a fail-closed safety check, not proof that the worker
distinguishes capacity exhaustion from every other scheduling failure. Neither
`Unsupported` nor the generic `SimulationNotFeasible` report is itself accepted
as a capacity diagnosis.

Each no-capacity run uses unique DNS names and creates its Namespace, runtime,
InferenceService, and blocker Pods one at a time. The API response for every
CREATE supplies the recorded UID; a missing, malformed, conflicting, or wrong
response stops the run without lookup-based adoption. Namespaced writes check
the recorded Namespace UID before and after the request. Kubernetes cannot make
a child CREATE conditional on its parent Namespace UID, so these checks detect
replacement but do not claim cross-resource atomicity.

Failures retain the fixture and diagnostics. The source Node marker is added and
cleared with UID, resourceVersion, and label-state JSON Patch tests. Only an
accepted scenario with a successfully cleared trigger may delete fixtures:
cleanup deletes the recorded Namespace by UID, waits for that exact identity to
disappear, then deletes the recorded runtime by UID. A same-name successor is
reported and left untouched.

To inspect only this test cluster:

```bash
kubectl --kubeconfig "${STATE_DIR}/kubeconfig" --context kind-alfred-e2e get pods -A
```

To release the VM's CPU and memory while retaining the cluster for another run:

```bash
colima stop alfred-e2e
```

## What a pass must establish

A ready source exists before the node trigger. Alfred submits the existing OME
migration API request. The real controllers create the replacement and the
real scheduler binds it away from the source node. The source remains healthy
and routed during a controlled replacement-readiness hold. After release, each
sample must contain either the healthy, routed source or the exact healthy,
routed replacement. Routing checks use the per-revision Service's actual
EndpointSlices, not headless peer discovery. The controller completes the same
request UUID, and Alfred subsequently observes the evacuated node. These are
sampled control-plane checks, not a measurement of gap-free inference traffic.

The pause-container fixtures declare a serving port so OME can create its real
per-revision routing Service. They do not actually listen on that port. Missing
the declaration correctly prevents migration's pre-drain routing gate from
passing; the harness must not bypass that gate or fabricate EndpointSlices.

Unit tests of the evidence checker exercise its rejection paths; they are not
a substitute for running the live scenario. Likewise, installing the cluster
and obtaining ready controller pods alone is not a successful migration test.

## Qualification and adversarial cases

See [QUALIFICATION.md](QUALIFICATION.md) for the tested production revision,
observed limitations, and what these tests do not establish.

The baseline has five live scenarios. Additional test-only variants exercise
partial gang readiness across OME manager replacement, an intentionally delayed
migration mailbox, and a real pending workload benefiting from defragmentation.
Run each capacity-dependent scenario with the preceding `single`/`gang` fixture
removed as shown above. They intentionally refuse to displace unrelated pods.

```bash
bash hack/alfred-kind-e2e/gang.sh partial-restart
# Remove the gang fixture and await its children before the next command.
bash hack/alfred-kind-e2e/useful-defrag.sh
bash hack/alfred-kind-e2e/no-benefit-defrag.sh
bash hack/alfred-kind-e2e/scenario.sh hint-exhaustion-single
bash hack/alfred-kind-e2e/scenario.sh target-health-race-single
```

The delayed-mailbox tests pause only the InferenceReplica controller via its
existing manager flag; webhooks and the InferenceService controller stay active.
The test chart allows three minutes for acknowledgement during the deliberate
consumer pause. Capacity blockers are scheduled by the real standard scheduler.
The non-hinted fallback is cordoned before simulation and becomes eligible only
after request submission. The health-race variant intentionally characterizes
placement onto a target whose custom health signal changed after submission;
reproducing this behavior is **not** a safety qualification pass.

Each runner retains evidence under `${STATE_DIR}/artifacts/`. On failure,
single/gang fixtures remain for diagnosis; remove only those fixtures before
another run. Delayed helpers restore manager arguments and their own node changes
and blockers. A cleared unhealthy signal remains in Alfred's one-minute recovery
quarantine, so allow that interval before running another capacity-sensitive case.
Useful-defrag restores the original Alfred configuration before removing capacity.
It retains failed fixtures; successful cleanup uses UID-fenced deletes and waits
for disappearance. Its proof keeps the same 8-GPU beneficiary Pod and unchanged
scheduling spec: initially unschedulable, still unbound while replacement readiness
is held, then bound to the vacated source node. Raw Pod and EndpointSlice samples
prove the source-to-replacement handoff; raw Nodes and all Pods prove the capacity
distribution and unchanged blockers. A complete owner-fenced annotation watch,
matching completed OME migration and Alfred journal entry, and three newer
completed decision cycles guard against duplicate or merely attempted migrations.

The sealed `evidence.json` includes these raw observations. Recheck it offline:

```bash
jq -e -L hack/alfred-kind-e2e -f hack/alfred-kind-e2e/verify-useful-defrag.jq \
  "${STATE_DIR}/artifacts/<useful-defrag-run>/evidence.json"
```

This is an observed useful outcome in the controlled fixture. Scheduler simulation
is only a prediction: migration-v1 node hints do not reserve or bind destinations,
and KWOK does not demonstrate real inference availability or GPU behavior.

The no-benefit test is the complementary negative control. Four identical 8-GPU
nodes have free capacity `[7,1,8,0]`. Moving the 1-GPU source into the 1-GPU hole
would create another 8-GPU slot. Its unchanged required node affinity, however,
allows only nodes a and c. The real standard scheduler therefore places the
simulated replacement on c, producing `[8,1,7,0]`: no additional 8-GPU slot.
Alfred must reject this feasible but unhelpful placement before requesting any
migration. Three distinct completed decisions must report a positive initial
candidate withheld as `PolicyNoLongerEligible`; absent candidates, unsupported
simulation, other rejection reasons, or stale reports do not pass.

The test uses the existing real-worker barrier, not a fabricated scheduler
result or a separate replay. It saves exact request/result bytes and release
receipts, stable node/occupant identities, raw source/readiness/routing samples,
and full owner and Pod watches. The installed policy uses a pure 8-GPU prior;
its API response is checked against the claimed configuration. The negative
runner copies the disabled policy to a temporary mutable ConfigMap and points
only its test deployment at that copy, leaving the Helm-managed policy and its
field ownership untouched. Cleanup disables the temporary policy with
UID/exact-key preconditions and verifies its reload in the same Alfred Pod
before restoring the original deployment and removing the barrier. It also
checks that the original policy bytes, UID, and field ownership did not change.
Deletion records include kind and UID so same-name objects cannot overwrite
each other's evidence. Failures retain evidence and
fixtures; uncertainty about disabling defrag retains the barrier too. It needs
an empty `alfred-e2e` namespace and the same dedicated cluster as the other tests.

```bash
bash hack/alfred-kind-e2e/verify-no-benefit-defrag.sh \
  "${STATE_DIR}/artifacts/<no-benefit-defrag-run>"
```

This small geometry proves actual-placement benefit gating for the standard
scheduler profile. It does not establish exhaustive gang, demand, topology,
competition, scale, or real model-serving qualification.

Run offline harness checks with:

```bash
for test in hack/alfred-kind-e2e/*_test.sh; do bash "$test" || exit; done
go test ./hack/alfred-kind-e2e/worker-result ./hack/alfred-kind-e2e/ir-status ./hack/alfred-kind-e2e/simulator-barrier
```

## Alfred-only change boundary

For a PR intentionally limited to Alfred, check its committed diff against the
intended base. Run this in Bash with `pipefail` so a failed Git command cannot
be mistaken for an empty, allowed diff:

```bash
set -o pipefail
git diff --name-only -z --no-renames origin/main...HEAD |
  bash hack/alfred-kind-e2e/check-change-scope.sh
```

The checker permits only Alfred code, deployment/build assets, this harness,
its dedicated CI workflow and OEP 8. Shared APIs, controllers (including the
read-only status codec), scheduler implementation and root dependency files
require separate approval. Disabling rename detection checks both ends of a
move. This path check complements the resolved dependency test in `cmd/alfred`;
it does not prove semantic independence or cover uncommitted files.

The dedicated CI workflow runs offline harness and compiled-worker checks, not
live kind qualification. Before presenting a revision as locally qualified,
run the applicable live scenarios from that exact checkout and retain its SHA,
image identities, scheduler profiles and evidence. A later relevant change or
rebase requires fresh evidence. A skipped or unavailable test is not a pass.
