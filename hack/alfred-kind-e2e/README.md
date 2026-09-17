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

Requires Go, Homebrew, Helm 4, kubectl, jq, Colima, Docker CLI/buildx, and kind.
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

Run offline harness checks with:

```bash
for test in hack/alfred-kind-e2e/*_test.sh; do bash "$test" || exit; done
go test ./hack/alfred-kind-e2e/worker-result ./hack/alfred-kind-e2e/ir-status
```
