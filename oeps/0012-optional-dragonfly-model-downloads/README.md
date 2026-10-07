# OEP-0012: Optional Dragonfly Model Download Transport

<!-- toc -->
- [Summary](#summary)
- [Terminology and Scope](#terminology-and-scope)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [Compatibility Contract](#compatibility-contract)
  - [Configuration](#configuration)
  - [User Stories](#user-stories)
    - [Accelerate a Multi-node Rollout](#accelerate-a-multi-node-rollout)
    - [Run OME Without Dragonfly](#run-ome-without-dragonfly)
    - [Continue During a Dragonfly Outage](#continue-during-a-dragonfly-outage)
  - [Operational Ownership](#operational-ownership)
  - [Risks and Mitigations](#risks-and-mitigations)
- [Design Details](#design-details)
  - [Architecture](#architecture)
  - [Version and Deployment Baseline](#version-and-deployment-baseline)
  - [Transport Boundary](#transport-boundary)
  - [Eligibility and Routing](#eligibility-and-routing)
  - [Immutable Identity and Cache Partitioning](#immutable-identity-and-cache-partitioning)
  - [Endpoint Normalization](#endpoint-normalization)
  - [Durable Download Ownership](#durable-download-ownership)
  - [Dragonfly Download Flow](#dragonfly-download-flow)
  - [Exact File Set and Guarded Recovery](#exact-file-set-and-guarded-recovery)
  - [Fallback and Failure Behavior](#fallback-and-failure-behavior)
  - [Credentials and Security](#credentials-and-security)
  - [Cancellation, Updates, and Deletion](#cancellation-updates-and-deletion)
  - [Storage Behavior](#storage-behavior)
  - [Observability](#observability)
  - [Component Changes](#component-changes)
  - [Rollout and Rollback](#rollout-and-rollback)
  - [Implementation Plan](#implementation-plan)
  - [Test Plan](#test-plan)
    - [Prerequisite Testing Updates](#prerequisite-testing-updates)
    - [Unit Tests](#unit-tests)
    - [Integration and End-to-end Tests](#integration-and-end-to-end-tests)
  - [Graduation Criteria](#graduation-criteria)
- [Open Questions](#open-questions)
- [Implementation History](#implementation-history)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
<!-- /toc -->

## Summary

This OEP adds Dragonfly as an optional download transport for Hugging Face
models materialized by the node-level model agent. When enabled, model-agent
asks the node-local Dragonfly client to fetch the files of an immutable model
snapshot. Dragonfly can obtain file pieces from other peers, a seed peer, or
the Hugging Face origin. The completed and verified snapshot is stored at the
same node-local path used today.

The integration is opt-in and preserves OME's current behavior:

- `direct` remains the default transport and uses OME's existing Xet-backed
  Hugging Face downloader.
- OME does not install, own, or require Dragonfly components.
- A cluster with no Dragonfly installation continues to run OME unchanged.
- A cluster that enables Dragonfly can configure direct download fallback.
- No BaseModel, ClusterBaseModel, ServingRuntime, or InferenceService API
  changes are introduced.

This proposal accelerates `PerNode` distribution. Every selected node still
ends with a complete local model snapshot. It does not implement
`Distribution=Sharded`, external-cache serving, lazy loading, or reduced
per-node model storage.

## Terminology and Scope

This OEP distinguishes storage layout from download transport:

| Term | Meaning |
| --- | --- |
| `PerNode` distribution | A complete model snapshot is materialized on every selected node. This remains the only serving layout affected by this OEP. |
| Direct transport | The existing OME downloader obtains files from Hugging Face using the Xet-backed client. |
| Dragonfly transport | Model-agent invokes `dfget`, which communicates with the node-local `dfdaemon` over a Unix-domain socket. |
| Peer | A Dragonfly client that can receive and upload cached file pieces. |
| Fallback | One direct transport attempt after a Dragonfly transport failure, when configured. |
| Shared HF artifact | OME's existing `ReuseIfExists` parent snapshot shared by model resources on one node. It is independent from Dragonfly's cross-node piece cache. |

The alpha scope is deliberately narrow:

1. `hf://` sources only.
2. `Distribution=PerNode`, including the backwards-compatible nil value.
3. Both `AlwaysDownload` and `ReuseIfExists` download policies.
4. Cluster-level model-agent configuration, not a per-model API.
5. An externally operated Dragonfly installation.

## Motivation

The model agent is a DaemonSet. When a model targets many nodes, every
eligible agent independently downloads the same files from Hugging Face. A
large rollout therefore multiplies origin traffic, consumes repeated external
bandwidth, and can encounter rate limits or long-tail download latency.

`ReuseIfExists` addresses a different duplication mode. It lets multiple OME
model resources that identify the same immutable Hugging Face snapshot share
one parent copy on a single node. It does not share bytes across nodes. The
`ome-agent replica` command can create a closer storage mirror, but every node
still downloads from that mirror independently.

Dragonfly is a CNCF project designed for peer-to-peer distribution. Its
[`dfget` command][dfget] and [native Hugging Face integration][dragonfly-hf]
support individual `hf://` files, authenticated repositories, immutable
revisions, and communication with a node-local `dfdaemon`. This makes it a
suitable optional transport for the high-fan-out case without changing OME's
model placement or runtime contracts.

[dfget]: https://d7y.io/docs/next/reference/commands/client/dfget/
[dragonfly-hf]: https://d7y.io/docs/next/operations/integrations/hugging-face/

### Goals

1. Reduce repeated Hugging Face origin traffic during concurrent multi-node
   model downloads.
2. Allow nodes to obtain model file pieces from Dragonfly peers while
   preserving a complete, verified model snapshot on each selected node.
3. Keep direct download as the default and fully supported transport.
4. Let an operator disable Dragonfly by configuration and roll back without
   editing model resources.
5. Preserve existing node eligibility, file locking, shared-artifact,
   metadata parsing, status, and ready-label behavior.
6. Pin downloads to the immutable Hugging Face commit already resolved by
   model-agent.
7. Preserve support for public, private, gated, and custom-endpoint Hugging
   Face repositories without exposing tokens in command arguments or logs.
8. Provide bounded-cardinality OME metrics that distinguish direct,
   Dragonfly, and fallback outcomes.
9. Verify that OME works in CI and supported deployments with no Dragonfly
   components installed.

### Non-Goals

1. Implementing `Distribution=Sharded` or wiring the reserved
   `ModelCacheProvider` API.
2. Serving model files directly from Dragonfly cache or through FUSE.
3. Reducing the final complete model copy required on each serving node.
4. Installing or reconciling Dragonfly manager, scheduler, seed-peer, client,
   Redis, or MySQL resources from OME.
5. Accelerating Oracle Cloud Infrastructure Object Storage (`oci://`) in
   alpha. Dragonfly's OCI registry support is unrelated to Oracle Object
   Storage.
6. Accelerating PVC, local, vendor, S3, Azure, GCS, GitHub, or ModelScope
   sources in alpha.
7. Selecting a transport automatically from cluster size or model size.
8. Adding per-BaseModel transport selection to the OME API.
9. Preheating, pinning, or garbage-collecting Dragonfly cache from OME.
10. Guaranteeing fully air-gapped Hugging Face repository discovery.
11. Hard-linking OME output to Dragonfly cache or allowing dfdaemon to write
    directly to OME's model filesystem in alpha.

## Proposal

Model-agent gains an opt-in routing wrapper around its existing Hugging Face
download callback. The original callback remains the direct implementation.
An eligible `dragonfly` attempt uses a pinned `dfget` binary and a mounted
node-local `dfdaemon` socket, streaming output into model-agent's filesystem.

The transport is selected once for the model-agent process. It is an
operational property of a model-agent deployment, not model intent, and
therefore belongs in command-line and Helm configuration rather than the
BaseModel API.

### Compatibility Contract

The following behavior is normative:

1. Omitting all new configuration produces the current direct-download
   behavior, including best-effort revision resolution for ordinary fresh
   downloads and the existing lazy manifest checks for shared artifacts.
2. `downloadTransport.type=direct` neither mounts a Dragonfly socket nor
   checks for a running Dragonfly service.
3. Existing model manifests require no edits when transport configuration
   changes.
4. Dragonfly is never a dependency of manager, admission webhooks,
   controllers, serving runtimes, or the CRDs.
5. A Dragonfly execution or content-integrity failure does not mark a model
   failed until the configured direct fallback has also failed or fallback is
   disabled. Filesystem safety failures remain fail-closed.
6. Cancellation and deletion never start a fallback download.
7. Switching back to `direct` requires only a model-agent DaemonSet rollout.
   Existing ready model directories remain valid and are not downloaded
   again solely because the transport changed.
8. Dragonfly and direct transports produce the same model directory contract
   before model-agent publishes Ready status.
9. Direct mode gains no mandatory manifest lookup, immutable-revision
   precondition, exact-file-set cleanup, or new destination ownership rule.

### Configuration

The proposed Helm values are:

```yaml
modelAgent:
  downloadTransport:
    # direct or dragonfly. direct is the default.
    type: direct

    # direct makes one direct transport attempt after a Dragonfly failure.
    # fail never sends an eligible request through OME's direct fallback.
    fallbackPolicy: direct

    dragonfly:
      # Socket created by the separately installed Dragonfly client DaemonSet.
      socketPath: /var/run/dragonfly/dfdaemon.sock

      # Per-file dfget timeout and per-snapshot file concurrency.
      timeout: 2h
      maxConcurrentFiles: 5
```

Equivalent model-agent flags are:

```text
--download-transport=direct|dragonfly
--download-fallback=direct|fail
--dragonfly-socket-path=/var/run/dragonfly/dfdaemon.sock
--dragonfly-download-timeout=2h
--dragonfly-max-concurrent-files=5
```

The defaults are `direct`, `direct`, the standard Dragonfly socket path, two
hours, and five files. Dragonfly-specific values are validated but have no
runtime effect when the selected transport is `direct`.

When `type=dragonfly`, the chart mounts the socket's parent directory from the
host into the model-agent container. The chart does not create any Dragonfly
workload or add a Dragonfly chart dependency. The standard model-agent image
contains a checksum-pinned `dfget` version listed in its SBOM. Direct mode
does not execute it.

The socket-directory hostPath uses `DirectoryOrCreate`, so a missing
Dragonfly installation cannot prevent model-agent from starting. Socket
preflight is bounded and advisory for pod readiness: a missing socket is
handled per download using the configured fallback policy, not by a blocking
init container or a Dragonfly-dependent readiness check. Every invocation
sets `--transfer-from-dfdaemon`; no OME model-path mount is added to dfdaemon.

`fallbackPolicy=direct` is the availability-oriented default once an operator
opts into Dragonfly. `fallbackPolicy=fail` is available to clusters that must
protect an origin from OME fallback fan-out during a Dragonfly outage. It does
not disable Dragonfly's own back-to-source behavior or change routing for
ineligible requests, which still use their existing direct path.

### User Stories

#### Accelerate a Multi-node Rollout

Alice operates a GPU cluster with Dragonfly already installed. She enables
the Dragonfly transport in the OME Helm values and rolls the model-agent
DaemonSet. When a pinned Hugging Face model targets several nodes, each agent
requests the same immutable files. Dragonfly schedules peer transfers, and
model-agent validates the completed snapshot before marking each node Ready.

#### Run OME Without Dragonfly

Bob installs OME with the default chart values. No Dragonfly components or
socket are present. Model-agent follows its existing direct path, and all
model CRs and serving behavior work as before.

#### Continue During a Dragonfly Outage

Carol enabled Dragonfly with `fallbackPolicy=direct`. During a scheduler or
peer outage, model-agent records a transport failure, applies bounded jitter,
and uses the existing direct downloader. The logical model download succeeds
if the origin remains available. Carol can set `fallbackPolicy=fail` to stop
OME fallback fan-out, including while its circuit breaker is open. Dragonfly
can still contact the origin on a cache miss unless configured otherwise by
its operator.

### Operational Ownership

OME owns:

- selecting and invoking the configured transport;
- immutable revision resolution;
- credentials read from OME model configuration;
- destination locking and lifecycle;
- post-download verification, metadata parsing, status, labels, and metrics;
- direct fallback behavior; and
- compatibility testing against documented Dragonfly versions.

The cluster operator owns:

- installing and upgrading Dragonfly;
- scheduler, seed-peer, client, and optional manager availability;
- Dragonfly mTLS, authorization, network policy, cache sizing, and cache GC;
- ensuring every peer and local socket caller belongs to the same trusted
  content confidentiality domain, or deploying separately enforced domains;
- deploying the qualified corrected build, origin trust stores, and source-side
  bounded metric-label mode in every required role;
- ensuring `dfdaemon` runs on every node where Dragonfly-enabled model-agent
  can run; and
- Dragonfly-native dashboards for peer hit ratio, origin traffic, and cache
  capacity.

### Risks and Mitigations

| Risk | Mitigation |
| --- | --- |
| Dragonfly becomes an accidental hard dependency | Default to `direct`; do not install Dragonfly from OME; keep direct CI and release qualification mandatory. |
| A Dragonfly outage causes all nodes to hit the origin | Provide `fallbackPolicy=fail`; use existing startup jitter plus bounded fallback jitter and a process-local circuit breaker; expose fallback metrics. |
| A branch changes while nodes download | Resolve the branch once through OME and pass the immutable commit SHA to every file request. |
| Cached private content crosses authorization boundaries | Treat the Dragonfly deployment and its socket callers as one trusted confidentiality domain. Tags partition tasks, not access. Use separate deployments or enforced data-plane isolation for mutually untrusted tenants. |
| Secret rotation reuses an old cache scope | Derive the tag from endpoint, Secret namespace, UID, resourceVersion, and data key read atomically with the actual token. Inline or process-configured tokens stay direct in alpha. |
| Tokens leak through process listings or logs | Pass tokens only through the child process environment (`DFGET_HF_TOKEN`), never command arguments; redact environment and subprocess errors; test that logs and metrics contain no token. |
| Back-to-source TLS accepts an impersonated origin | Reject unpatched client v1.5.7; qualify certificate-chain, hostname, expiry, custom-CA, and redirect behavior in every origin-fetching peer role before release. Peer mTLS and content digests do not protect origin credentials. |
| A malicious or corrupted peer supplies bad content | Verify the manifest even after successful dfget exit; classify size/digest mismatch as transport integrity failure and apply the selected fallback policy before Ready. |
| `dfget` writes an unexpected path | Download only filenames from OME's validated manifest, create destinations beneath a locked root, reject symlinks and unsafe names, and compare the resulting file set to the manifest. |
| A partial download loses ownership on restart | Persist a path claim and operation receipt before any snapshot mutation; recover independently of optional model parsing, and never reroute a known failed operation as an unowned legacy download. |
| A custom endpoint loses its path prefix | Derive a directory-form Dragonfly base URL from the canonical OME endpoint; qualify actual prefixed HEAD/GET requests, not only cache tags. |
| Secret rotations create unbounded native metric series | Keep generation-aware task tags, but require source-side bounded tag labels in the qualified Dragonfly build. Scrape relabeling is insufficient to bound exporter memory. |
| Dragonfly cache plus model output doubles node storage | Alpha streams a separate full OME copy; budget for both stores, temporary files, direct/Xet caches, and operator-controlled Dragonfly GC. No hard-link savings are assumed. |
| Dragonfly and `dfget` versions are incompatible | Pin `dfget` in the image, publish a tested version matrix, log the client version, and run real-version end-to-end tests before upgrades. |
| Xet-backed Hugging Face repositories behave differently | Include public and gated Xet-backed repositories in conformance tests; retain direct fallback; do not graduate without passing them. |
| Added binary increases image size and supply-chain surface | Pin its digest, record it in the SBOM, scan it with the standard image pipeline, and update it through normal dependency review. |

## Design Details

### Architecture

```text
                           optional, operated separately
                        +-------------------------------+
                        | Dragonfly scheduler/seed peer |
                        +---------------+---------------+
                                        |
  BaseModel event                       | peer scheduling
        |                               |
        v                               v
  +-----------+     UDS      +-------------------+       +-----------+
  | model-    |------------->| node-local        |<----->| peer node |
  | agent     |   dfget      | dfdaemon/cache    | pieces| cache     |
  +-----+-----+              +---------+---------+       +-----------+
        |                              |
        | direct fallback              | back-to-source
        +------------------------------+----------------> Hugging Face
        |
        v
  complete verified snapshot at the existing node-local model path
```

Model placement does not change. Scout continues to decide whether a model
belongs on the node using storage type, node selector, node affinity, and the
existing eligibility rules. Gopher continues to own task serialization,
locks, verification, metadata extraction, status, and node labels.

The UDS carries both control messages and file contents. `dfget` runs inside
model-agent with `--transfer-from-dfdaemon`, writes OME-owned output, and never
asks dfdaemon to open an OME destination path. The two pods need only share
the socket directory, not a mount namespace or model filesystem. Dragonfly's
[container download documentation][dfget-container] describes this distinction.

[dfget-container]: https://d7y.io/docs/next/reference/commands/client/dfget/#download-in-container

### Version and Deployment Baseline

The investigated upstream baseline is concrete, but is **not releasable
unmodified**:

| Component | Pinned qualification target |
| --- | --- |
| Bundled `dfget` | A reviewed corrected client build, Linux amd64 and arm64, with source revision and per-artifact SHA-256 verification. Unpatched `v1.5.7` is rejected. |
| Node-local dfdaemon and seed peers | The same qualified corrected client build in all peer roles, including every back-to-source role; pin image digests. |
| Scheduler and manager, when deployed | Dragonfly server `v2.5.2`, with image digests recorded in the E2E fixture. |
| Pod integration | Separate client DaemonSet, shared UDS directory, streamed output; no shared model-path mount or hard-link mode. |

The investigated client tag `v1.5.7` resolves to
`ada717934e8676e5a9d016fc8ae2762a60bdd8b7`,
and the server tag to `dbc47411cf062184725a585f4cdea7dfb0a72d86`.
The [pinned dfget source][dfget-source] supplies native `hf://` files,
`--hf-revision`, `DFGET_HF_TOKEN`, `DFGET_HF_BASE_URL`, digest input, and
`--transfer-from-dfdaemon`. The server target is the published
[v2.5.2 release][dragonfly-server-release]. Do not infer support for older
clients from the rolling `next` documentation or from a Helm chart's defaults;
the E2E fixture must explicitly pin its client and seed-peer images.

The [v1.5.7 Hugging Face backend][hf-backend-source] unconditionally installs
`NoVerifier` for origin HTTPS. Its [certificate verifier][tls-source] accepts
the server certificate without chain, hostname, or expiry validation. A
manifest digest may detect changed model bytes, but cannot prevent a bearer
token from reaching an impersonated origin. Changing the server release,
enabling peer mTLS, or updating only the bundled dfget does not fix this.

Before implementing the alpha image/fixture, select either a reviewed fixed
upstream client release or an explicitly reviewed patch set on the investigated
commit. Record the resulting immutable source revision, patch digests, build
provenance, and artifact/image digests; no corrected build is qualified by this
OEP. The build must enforce origin TLS verification and source-side bounded
native metric labels as specified below. Both are release gates, not optional
operator workarounds. If no corrected build qualifies, Dragonfly alpha does
not ship; default direct OME remains supported and unchanged.

Source inspection establishes the API target and blockers, not a claim that
OME has tested a safe combination. Alpha release is blocked until the real
multi-node tests below pass for both bundled architectures, including private
and Xet-backed file downloads and the separate-pod UDS topology. The
implementation PR records artifact checksums, image digests, fixture chart
version, and results before advertising this combination as supported.
Floating tags and unqualified mixed client versions are not supported in
alpha. Direct mode does not check external Dragonfly versions.

[dfget-source]: https://github.com/dragonflyoss/client/blob/v1.5.7/dragonfly-client/src/bin/dfget/main.rs
[dragonfly-server-release]: https://github.com/dragonflyoss/dragonfly/releases/tag/v2.5.2
[hf-backend-source]: https://github.com/dragonflyoss/client/blob/v1.5.7/dragonfly-client-backend/src/hugging_face.rs
[tls-source]: https://github.com/dragonflyoss/client/blob/v1.5.7/dragonfly-client-util/src/tls/mod.rs

### Transport Boundary

Keep the existing callback contract used by `directHfSource.download`:

```go
type HFDownloadFunc func(context.Context, *GopherTask, *xet.DownloadConfig) error
```

An opt-in routing wrapper retains the original function and configuration.
Only after eligibility preparation succeeds does it construct a stricter
request for Dragonfly and its pinned direct fallback, equivalent to:

```go
type ResolvedHFSnapshotRequest struct {
    RepositoryID   string
    CommitSHA      string
    Endpoint       string
    Credential     HFCredential
    Manifest       HFSnapshotManifest
    Destination    string
}

type DragonflySnapshotTransport interface {
    Download(context.Context, ResolvedHFSnapshotRequest, ProgressHandler) error
}
```

The exact Go names are implementation details. The stricter request is not a
new prerequisite for the original direct callback. Routing must satisfy:

1. Direct configuration invokes the original callback with existing Xet
   arguments and does not perform additional preparation or Hub requests.
2. Ordinary fresh direct downloads retain best-effort revision resolution;
   `AlwaysDownload` does not suddenly require a manifest. Existing shared
   artifact resolution, lazy validation, and failure/retry rules stay intact.
3. Only a Dragonfly request accepts a mandatory immutable commit and validated
   manifest. Preparation failure routes to the original callback when the
   existing lifecycle permits an ordinary download and no known unfinished
   opt-in operation requires recovery. It never bypasses a durable ownership
   error, existing shared-artifact error, safety check, or retry.
4. The transport does not own Kubernetes status, labels, or ConfigMaps and
   writes only under the destination owned by the current Gopher task.
5. It returns context cancellation without converting it to a transport
   failure.
6. A successful return means the manifest was completely materialized; the
   caller still performs authoritative post-download validation.
7. A direct fallback calls the original Xet function with a copied
   configuration pinned to the prepared commit, endpoint, effective token,
   and destination. It must then pass the same authoritative manifest and
   exact-file-set checks as the Dragonfly attempt. This stricter fallback
   contract applies only to a prepared eligible request.

The Dragonfly routing wrapper is inserted at the existing direct-HF snapshot
download callback. Its opt-in exact-file validator is also supplied to the
existing shared-parent validation/repair callback for eligible snapshots, so
stale files can authorize guarded repair rather than leave a parent permanently
invalid. This keeps `ReuseIfExists` parent ownership, recovery, locking, and
status publication common to both transports. In direct mode both callbacks
remain unchanged.

### Eligibility and Routing

The Dragonfly transport is eligible only when all of the following hold:

1. the configured transport is `dragonfly`;
2. the source storage type is Hugging Face;
3. distribution is nil or `PerNode`;
4. model-agent resolved a valid 40-character immutable commit SHA;
5. the manifest passed OME's safety and metadata checks;
6. the actual effective credential has supported provenance;
7. the destination is fresh/empty and can be durably claimed, or its exclusive
   OME snapshot ownership is durably established, with no unrelated descendants
   or conflicting artifact owner; and
8. the endpoint has a qualified HTTPS URL shape with preserved path semantics.

An ineligible request uses the original direct callback regardless of fallback
policy. This includes unavailable revision/manifest metadata, unsupported
credentials, and legacy directories whose ownership cannot be established.
Invalid preparation metadata must never be used to construct Dragonfly paths;
discard it and retain the existing direct lifecycle, including any safety or
shared-parent checks that already fail or requeue today. A known claimed or
interrupted operation is not a legacy directory: inspect its durable state
before routing, and fail closed on missing/corrupt state, owner conflict, or
failed recovery. It cannot escape `fallbackPolicy=fail` by becoming nonempty
or losing parser metadata. Cancellation always stops preparation and never
routes to a new download.

For an eligible request, the fallback policy governs preflight, attempted
downloads, and circuit-open decisions, even when no dfget process starts. This
makes cluster-level enablement safe for models outside the alpha scope without
silently weakening `fail` for models inside it. PVC and local sources retain
their existing behavior. OCI Object Storage continues to use the OCI SDK
downloader.

No routing decision reads `SupportedModelFormat.modelCacheProviders`, the
reserved `DragonFly` constant, or `ModelCacheStatus`; those belong to the
separate unfinished sharded-serving design.

### Immutable Identity and Cache Partitioning

Dragonfly task identity must distinguish the same filename at different
revisions, endpoints, and authorization scopes.

OME prepares an eligible `hf://org/repo@branch` request with a commit SHA before
invoking Dragonfly. Each `dfget` request receives:

- URL: `hf://org/repo/<manifest filename>`;
- revision: the resolved commit SHA;
- application: `ome-model-agent`; and
- tag: a deterministic cache-scope identifier.

The tag is a hash of versioned, unambiguously encoded non-secret identity
material, for example canonical JSON with explicit field names:

```text
ome-v1:<sha256({"version":1,"endpoint":<canonical endpoint>,
               "credential":<credential provenance>})>
```

Use the logical endpoint defined below in the tag; the directory-form value
passed to Dragonfly is derived from it, not a second cache identity. For an
anonymous request, credential provenance is `anonymous`,
and the manifest lookup must have succeeded without an effective token. For a
Secret-backed token, provenance contains the Secret namespace, UID,
`resourceVersion`, and selected data key. No token bytes, hash of token bytes,
or reversible token encoding enter the tag. The same Secret version and key
can share tasks; rotation, recreation, or a different endpoint produces a
different task partition. Metadata-only Secret updates may also reduce sharing
because resourceVersion is deliberately conservative.

The credential resolver returns the effective token and its provenance
together. Both must come from the same Secret GET, not two reads that can race
with rotation. Preserve current lookup precedence: BaseModel namespace or
`ome` for ClusterBaseModel, `storageKey`, the default `token` key or configured
`secretKey`, then existing parameter/process configuration fallback. Eligibility
is based on the credential actually selected, not merely the presence of a
Secret reference in the CR. Inline parameter tokens, inherited Xet/process
tokens, and fallback tokens after a failed/empty Secret lookup use direct in
alpha because they lack a supported non-secret generation identity. Never
classify an inherited effective token as anonymous.

Tags prevent accidental cache-task collisions; they are not authentication,
authorization, or protection against a caller that knows or guesses a tag.
This OEP permits private-model sharing only within an externally enforced,
trusted Dragonfly confidentiality domain. Mutually untrusted tenants require
separate deployments or independently reviewed isolation of peers, cache,
control plane, and local socket access. mTLS authenticates participating
components but does not by itself authorize a Hugging Face repository.

The tag is passed to `dfget`; OME does not override
`--content-for-calculating-task-id`. Dragonfly therefore retains its normal
per-file URL and revision identity while adding the OME cache scope.

### Endpoint Normalization

Normalize the endpoint once during eligible-request preparation. The logical
OME endpoint has no trailing path delimiter; derive `DFGET_HF_BASE_URL` by
appending exactly one `/`, making it a directory base for relative URL joins.
For example, both `https://mirror.example/hf` and
`https://mirror.example/hf/` yield logical endpoint
`https://mirror.example/hf` and Dragonfly base
`https://mirror.example/hf/`. OME discovery and pinned direct fallback use the
logical form, preserving `/hf/api/...` and `/hf/<repo>/resolve/...`; Dragonfly
file HEAD/GET requests must preserve the same prefix. Root endpoints likewise
derive a base ending in `/`.

Preserve escaped path segments, path case, and content-affecting origin/path
distinctions; do not decode `%2F`, collapse repeated internal slashes, or clean
dot segments into a different repository namespace. Reject ambiguous dot
segments, multiple trailing path delimiters, userinfo, queries, fragments,
and unsupported URL forms before constructing requests. Alpha Dragonfly
endpoints require HTTPS. HTTP or other
unsupported existing direct endpoints stay ineligible; this does not change
the default direct endpoint contract. Only documented safe equivalences,
including a single optional trailing delimiter, share a cache scope.

The investigated backend joins file paths relative to its base, so passing
raw `/hf` loses that prefix. It also constructs its API base with an absolute
`/api/` join. Alpha never uses Dragonfly recursive/API listing: OME owns all
metadata discovery and supplies individual manifest files. Qualification must
confirm file-only calls perform no unexpected root-level API requests, and
that the corrected build preserves encoded paths. An endpoint shape that
cannot satisfy this contract is ineligible before invoking dfget, under both
fallback policies; silently downloading from the wrong root is forbidden.

### Durable Download Ownership

Ordinary download ownership cannot depend on `Artifact` attached by model
parsing: the current callback returns on download error before parsing, and
parsing may be disabled or fail without persisting that metadata. The opt-in
path introduces an internal, durable write-ahead ownership store, independent
of model config, progress, and Ready status. It is not a CRD field.

Use a reserved, agent-owned `.ome-download-state` directory on the persistent
model volume, outside every final snapshot tree. Use restrictive directory/file
permissions (0700/0600), rooted no-follow access, and exclusive claim creation
that never overwrites another owner. Reject overlapping snapshot
destinations and unsafe state-root ancestors; never invent a `.cache` or
receipt exemption inside a snapshot. Index records by a versioned hash of the
canonical destination, not the CR UID alone, so a recreated CR cannot obtain a
second claim on the same path. State and staging roots must be discoverable
after pod restart; staging remains on the destination filesystem for rename.

Under the existing destination lock, before the first staged output, cleanup,
or final-file write:

1. Create an exclusive path claim binding schema version, canonical path,
   model key, CR UID, and node UID. Revalidate the current CR and existing
   artifact/descendant ownership before claiming; a fresh/empty path alone is
   not a claim. Write atomically and fsync the file and parent directory.
2. Persist an operation receipt binding that claim to repository, immutable
   commit, canonical endpoint, manifest identity, attempt ID, guarded staging
   location, and state (`prepared`, `running`, `failed`, or `content_verified`).
   Include no token or Secret bytes. Use the same atomic/fsync discipline.
3. Only then permit mutation. Persist a new receipt before an authorized
   revision replacement or retry; the path claim survives attempts. Record
   content verification before returning success, independently of parsing.
   A receipt is never authority to publish Ready.

Startup/retry recovery reacquires the same lock, reads the path claim and
receipt, verifies owner/UID/path/current task identity, and reaps any known
writer before reconciling staging and partial content. A crash after claim
creation but before receipt creation leaves a known incomplete claim, not a
legacy path; fail closed until an authorized recovery reconstructs the receipt
without adopting unrelated contents. Missing/corrupt receipts, conflicting
claims, state I/O/fsync failures, or inability to validate lifecycle ownership
permit no snapshot writes, deletion, or direct fallback. A known unfinished
attempt also retains its selected-policy contract while Dragonfly is configured
if fresh metadata is unavailable: retry/fail, never reclassify it as ordinary
ineligible work.

For shared snapshots, the existing canonical-parent repository and LockID
remain ownership and lifecycle authorities. Bind the staging receipt to that
parent and operation rather than creating a competing ordinary path claim.
The shared handler alone invalidates/writes the ready marker and controls
child failure ordering. Neither receipt type adopts an unowned legacy tree.

Deletion first stops writers and performs existing authorized cleanup, then
removes the receipt and finally the ordinary path claim under the lock. If
data is retained by existing policy, retain its ownership state too. CR
recreation, path changes, or node identity changes require explicit authorized
cleanup rather than automatic claim adoption. Rollback leaves ready files and
these records intact; direct mode neither requires new records for legacy
downloads nor uses them to enable new exact-set cleanup. Explicit disablement
authorizes the original direct routing, after writers stop and existing safe
lifecycle/lock checks pass; this is distinct from a silent ineligible retry
while Dragonfly remains configured.

### Dragonfly Download Flow

For an eligible snapshot, model-agent performs the following steps:

1. Acquire the existing destination or shared-parent operation lock. Inspect
   durable state before any ineligible-routing decision for a known operation.
2. Prepare the immutable revision, effective credential and provenance, and
   validated manifest. Reuse resolution/manifest results already available
   from the existing lifecycle; do not duplicate them. Route ineligible work
   as described above without changing the original callback's contract.
3. Establish/recover the durable claim and receipt before permitting a writer,
   including direct fallback. State failure is a safety error, not a transport
   failure. Check the local socket and circuit state. Apply the fallback policy
   on preflight failure or an open circuit.
4. Inventory and reconcile the owned snapshot as specified below. For shared
   parents, the existing handler must authorize repair, fail affected children,
   and remove the old ready marker before the callback changes any files.
5. For each missing or invalid manifest file, create a bounded download task:
   - validate the relative filename before constructing a path;
   - create an operation-owned temporary output under a guarded staging root
     outside the final snapshot tree, on the destination filesystem;
   - invoke `dfget --transfer-from-dfdaemon` for that single `hf://` file;
   - pass the commit, endpoint, application, cache-scope tag, socket, and
     timeout, deriving the directory-form base URL as specified above;
   - pass an LFS SHA-256 digest when the manifest supplies one; and
   - pass the token through `DFGET_HF_TOKEN` in the child environment.
   Verify and fsync the staged regular file, then move it into its validated
   final path and fsync the destination directory under the same lock. Already
   valid expected files are retained. Output paths handed to dfget must not
   already exist; alpha does
   not depend on its default overwrite behavior or enable force-hard-link.
6. Run at most `maxConcurrentFiles` file tasks for the snapshot. Existing
   model-agent worker limits still bound concurrent snapshots.
7. On cancellation, terminate all child processes and wait for them before
   releasing the destination lock.
8. Compare the resulting regular-file set, sizes, and digests with the
   manifest. Reject symlinks, special files, missing files, and unexpected
   files.
9. Persist `content_verified` after durable final-file publication, then
   continue through existing model parsing, status, progress cleanup, and Ready
   publication.

`--transfer-from-dfdaemon` is mandatory, not an operator-tunable flag in alpha.
Without it, dfdaemon attempts to write the output in its own mount namespace;
a shared socket alone does not make OME's destination visible there. Alpha
neither grants dfdaemon model-path access nor assumes shared-cache hard-links.

Per-file invocation is preferred over `dfget --recursive` in alpha because
OME already owns a validated manifest. It gives OME exact path control,
per-file digest input, bounded concurrency, and deterministic accounting
without relying on a second repository listing or Dragonfly's recursive file
limit.

`dfget` runs with its progress UI disabled. OME calculates snapshot progress
from the known manifest and completed output files at the existing reporting
interval. Progress is advisory; manifest validation remains the completion
authority.

### Exact File Set and Guarded Recovery

The current manifest helpers check expected files but do not enumerate or
remove all stale files. In particular, `removeInvalidFiles` does not remove a
file that disappeared between revisions. The opt-in path therefore needs an
explicit exact-file-set inventory and reconciliation helper; calling today's
validator alone is insufficient. Do not apply this new cleanup policy to
default direct downloads or adopt an unowned legacy directory.

Under the operation lock, enumerate without following symlinks and compare
regular files to the prepared manifest. Reject symlinked directories, special
files, ownership conflicts, and unsafe names before writing. Once exclusive
OME ownership and lifecycle mutation authorization are established, unlink
stale regular files absent from the new manifest and invalid expected files
through a rooted/no-follow filesystem API. Remove obsolete empty directories
only inside that owned root. Never recursively clear a configured path or
remove unrelated descendants. A pre-existing unsafe object fails closed; it
is not permission to fall back to a writer that could follow it.

Temporary output lives outside the snapshot and is attributable to the current
operation using its durable receipt. Failure/cancellation cleanup removes only
that operation's files and retains the ownership claim and failed receipt;
startup recovery must reacquire the destination lock and validate abandoned
operation ownership before cleaning staging. Guarded destination checks are
required again before each final-file publication, not just at initial scan.

Shared-parent validation distinguishes snapshot content from the one reserved
OME ready marker. The marker is not a manifest file and cannot be supplied by
a repository or transport. Read-only reuse validation may accept the existing
handler-owned marker, but a mutating callback requires it to be absent. Only
the shared handler writes a new marker, after exact content validation, and
uses the current LockID for recovery as today. An extra regular file causes
repair under the existing child-failure/status protocol, not endless
validation-only failure or deletion while consumers are still marked Ready.

Before direct fallback, reap every child, discard temporary output, and remove
any invalid or unexpected regular files using the same guarded helper. This
also prevents Xet's same-size skip behavior from accepting corrupt Dragonfly
output. Retain only expected files that pass digest validation. The fallback
uses the same commit and must produce the exact expected set; unexpected
post-transfer regular files, missing files, wrong sizes, or digests are content
integrity failures. There is no second fallback if the direct attempt fails.
OME ready markers and staging are the only explicitly managed exceptions;
arbitrary dotfiles or a whole `.cache` subtree are not exempt from validation.
Configure any downloader cache outside the snapshot root.

### Fallback and Failure Behavior

Fallback composes transports rather than duplicating Gopher lifecycle logic:

```text
preparation ineligible, no unfinished claim -> existing direct lifecycle
known unfinished claim + preparation/recovery failure -> retry/fail closed
Dragonfly execution + content valid ---------------------------> Ready
Dragonfly execution/integrity failure + direct -> clean -> direct -> verify
Dragonfly execution/integrity failure + fail -------------------> Failed
preflight/circuit unavailable + direct ------------> direct -> verify
preflight/circuit unavailable + fail ---------------------------> Failed
context canceled or model deleted -----------------> stop; never fall back
unsafe filesystem or lifecycle mutation failure ---> fail closed; no fallback
```

Successful dfget exit is not successful content validation. Missing files,
size/digest mismatch, or unexpected regular files produced by a Dragonfly
attempt are `transport_integrity` failures, including cache poisoning detected
only by OME. They use the same fallback policy as subprocess errors. Metadata
preparation failures are routing decisions only when there is no known
unfinished operation requiring recovery. Unsafe paths, symlinks, special
files, missing/corrupt durable state, loss of ownership, or lifecycle failures
are fail-closed safety errors and never trigger a new writer.

With `fallbackPolicy=direct`, any non-cancellation dfget execution or transport
integrity failure permits one direct transport attempt after child processes
exit and guarded cleanup completes. OME does not retry Dragonfly within that
logical attempt or parse human log text to decide whether to fall back. If the
direct path encounters the same origin, authorization, or not-found error, or
its output fails verification, the logical download fails with both errors
preserved in sanitized diagnostics. The existing direct downloader's internal
retry policy is unchanged. Metadata parsing failure after verified transfer
does not trigger fallback.

A process-local circuit breaker prevents every retry from invoking a known
unavailable socket. Consecutive local availability failures detected by
bounded preflight/probes open it; content, origin, or authorization errors do
not count as proof that the local service is unavailable. Generic subprocess
errors still follow the fallback policy, but must not be classified by stderr
string matching as local availability failures. During the bounded cooldown:

- `direct` sends eligible work to one verified direct attempt, records
  `circuit_open`, and applies the same bounded fallback jitter;
- `fail` fails eligible work fast with no direct attempt or OME origin
  fallback, also recording a bounded circuit-open error; and
- after cooldown, one bounded half-open probe runs. Concurrent work obeys its
  configured policy while the probe is pending; probe success closes the
  circuit and failure reopens it.

Constants and transitions must be covered by deterministic tests. The breaker
is process-local so OME does not create a distributed coordination dependency.

Fallback uses bounded node-derived jitter before contacting the origin. The
metric and structured log record the fallback reason, but neither includes a
token, signed URL, or Secret name.

`fallbackPolicy=fail` never silently changes to direct for an eligible
request, including missing socket and open-circuit cases. It does not promise
zero origin access: discovery still uses the Hub, Dragonfly may back-to-source,
and ineligible requests retain their direct behavior. Failed eligible work
uses the existing status path; a later model event or retry can recover it.

### Credentials and Security

Dragonfly expands the trusted computing base for an enabled cluster. The
node-local `dfdaemon`, seed peers that perform back-to-source requests, and
the Dragonfly control plane must be operated as infrastructure trusted to
handle the configured model content and credentials. Operators should follow
Dragonfly's [security guidance][dragonfly-security], including mTLS and
network restrictions.

[dragonfly-security]: https://d7y.io/docs/next/operations/security/

OME applies these controls:

1. Extend the existing credential resolver to return provenance with the
   selected effective token, preserving lookup precedence and Secret RBAC.
   Read token, UID, resourceVersion, namespace, and key from one Secret GET.
2. Build a minimal child environment, removing inherited `DFGET_*` settings
   before adding the explicit request values. Inherited task-ID, overwrite,
   token, or hard-link settings must not override OME's contract. Set
   `DFGET_HF_TOKEN` only there; never put a token in `argv`, the global
   model-agent environment, log fields, errors, or metrics.
3. Pass the normalized directory-form endpoint through `DFGET_HF_BASE_URL`;
   never interpolate it into a shell command.
4. Invoke `dfget` directly with `exec.CommandContext` or equivalent. Do not
   invoke a shell and do not accept arbitrary flags from a BaseModel.
5. Use a constant binary path selected by the image, not a CR field.
6. Redact child environment and stderr before recording diagnostics. Keep
   dfget's own log level/configuration controlled by the runner, and include
   its file logs in sentinel-secret checks; redacting only OME stderr is not
   sufficient. Tests assert no token reaches any logs or returned errors.
7. Partition private cache tasks by endpoint and credential generation as
   described above. Do not advertise tags as access control; restrict peers
   and socket callers to the same trusted confidentiality domain.
8. Preserve OME's digest verification even when Dragonfly reports success.
9. Document that rotating/revoking a remote token does not delete previously
   cached or materialized bytes or authorize OME to reuse an old scope. New
   prepared requests must obtain authorized metadata using their actual
   effective credential. Operators needing immediate content revocation must
   remove local copies, expire/purge peer tasks, and restrict old-domain
   access. ResourceVersion partitioning is not remote token-revocation
   enforcement and does not itself trigger model redownload.
10. Require normal certificate-chain, hostname, and validity verification for
    every HTTPS metadata, HEAD, range, and file request, including redirected
    downloads. A custom CA must be explicitly trusted in each relevant client
    role's trust store; peer mTLS roots are not automatically origin roots.
    No insecure-skip-verify option or `NoVerifier` escape hatch is permitted in
    the qualified HF path. Reject invalid TLS before sending HTTP authorization
    or accepting model bytes; content verification is not a substitute.
11. Bound redirect chains, reject HTTPS-to-HTTP downgrade, and never forward
    the HF bearer token across origins, including CDN/storage redirects.
    Redirected signed URLs are credentials too and must remain redacted. Test
    both node-local and seed-peer back-to-source paths against controlled
    endpoints; a secure dfget binary alone does not qualify the daemon fleet.

The socket mount is read-only at the filesystem mount level when supported,
but connecting to it grants access to the dfdaemon API. Pod and node security
reviews must treat that socket as a privileged local service endpoint.

### Cancellation, Updates, and Deletion

The current Gopher task context remains authoritative:

- Model deletion cancels `dfget` children, waits for exit, and then executes
  existing cleanup.
- A node becoming ineligible cancels the download and does not fall back.
- A source or revision update cannot publish the old task as Ready because
  existing CR UID and stale-task checks remain outside the transport.
- `DownloadOverride` uses the newly resolved immutable revision.
- Shared-artifact parent deletion waits on the same operation locks as direct
  downloads.
- Model-agent shutdown terminates child processes and does not orphan a
  writer holding the logical destination lock.
- An interrupted ordinary opt-in operation retains its durable claim and
  receipt even when parsing is skipped or fails. Retry and deletion discover
  it without relying on `ModelConfig.Artifact`.

The subprocess runner must terminate the whole child process group, not only
the immediate `dfget` process, and must always reap it. Cancellation tests are
required on Linux, the production platform.

### Storage Behavior

Dragonfly changes network transfer, not the serving layout:

1. Every selected node still has a full snapshot at the OME model path.
2. `ReuseIfExists` still creates one OME parent snapshot per node and symlinks
   local consumers to it.
3. Dragonfly maintains its own node cache under the operator-configured
   `dfdaemon` storage directory.
4. Alpha always streams contents over the socket to dfget and creates a
   separate OME output copy. It does not rely on shared inodes or access to the
   same model mount inside dfdaemon, even when cache and output use one disk.
5. Account for Dragonfly cache plus the full OME snapshot, temporary file
   outputs, and existing Xet/direct cache during fallback. Valid staged files
   are renamed, not copied again, into the final snapshot on the same
   filesystem. Stale content is reconciled only after ownership authorization.
6. Dragonfly cache eviction/GC cannot delete the independent served OME copy;
   qualify this separation using real deployments, distinct mounts, and GC.

Disk-space admission and reservation are outside this OEP. Documentation must
warn that enabling alpha can increase peak disk usage. No hard-link or final
storage reduction is promised. A future direct-output/hard-link design would
need identical destination mounts in both pods, compatible filesystem
semantics, and a separate write-safety/security review; it is not this alpha
integration.

### Observability

Existing logical download success, failure, duration, and progress metrics
retain their meaning. They count one model-agent download regardless of the
selected transport or fallback sequence. Existing source-specific byte
accounting is not reinterpreted as peer or origin traffic.

The following bounded-cardinality metrics are added:

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `model_agent_download_transport_requests_total` | Counter | `transport`, `source`, `result` | Transport attempts and outcomes. |
| `model_agent_download_transport_fallbacks_total` | Counter | `from`, `to`, `reason` | Direct fallbacks grouped by a bounded reason enum. |
| `model_agent_download_transport_duration_seconds` | Histogram | `transport`, `source` | Time spent in each transport attempt. |
| `model_agent_download_transport_available` | Gauge | `transport` | Whether the configured local transport passed its latest preflight/probe. |

`source` is a bounded storage kind such as `huggingface`; it is not a URI.
Model name, repository ID, node name, endpoint, Secret, and error strings are
not labels.

The initial bounded values are `direct|dragonfly` for `transport`/`from`/`to`,
`huggingface` for `source`, `success|error|canceled` for `result`, and
`preflight|execution|transport_integrity|circuit_open` for `reason`. Adding
values requires a metrics-cardinality review. The availability gauge describes
local service preflight/probe results, not success of every model or origin
authorization. A fail-policy rejection is an error, not a fallback counter;
requests rejected before invocation do not count as subprocess attempts.

OME metrics cannot accurately distinguish peer bytes from origin bytes. That
information remains in Dragonfly-native metrics. The operations guide must
show how to correlate OME transport attempts with Dragonfly peer-hit and
back-to-source dashboards.

Bounded OME labels alone are insufficient. The investigated
[Dragonfly metric implementation][dragonfly-metrics-source] labels native
download, failure, concurrency, prefetch, and traffic vectors with the raw
task `tag`. Every Secret resourceVersion therefore creates new retained label
sets, even for metadata-only updates. Cache GC does not remove those metric
children; an explicit metrics reset or process restart is not an acceptable
steady-state cardinality policy.

The corrected client build must provide a source-side mode that substitutes
the single constant `redacted` for task tags at **every native metric
observation**, including start/completion/failure, upload/prefetch/traffic,
and node/seed roles. Enable and qualify this mode throughout the deployment.
The full generation-specific tag remains unchanged in requests and task-ID
calculation; do not weaken credential partitioning to control metrics. Audit
the selected server build for corresponding task-derived labels as well and
require the same bounded-label property wherever it exports them.

OME uses one fixed application and a fixed qualified priority. With task tags
bounded at the source, each affected counter/gauge family must have at most
32 OME-attributed label sets per exporter, independent of repository count or
Secret generations; histogram buckets are accounted for separately. Publish
the exact family/label inventory and budget with the qualification fixture.
Rotate at least 1,000 Secret versions at fixed task types/application/priority:
after warming the finite combinations, the number of exporter metric children
and exposed OME label sets must not grow. Verify valid exposition and a stable
metric-registry memory footprint, separating cache/task storage from metrics.

Scrape-side relabeling or aggregation alone does not bound the exporter heap.
Simply dropping `tag` can also create duplicate samples rather than aggregate
them. Dashboards may use recording-rule aggregation, but the source-side mode
and rotation stress test are alpha release gates. Document scrape sample-count
and exporter RSS monitoring in addition to OME metrics and cache disk alerts.

[dragonfly-metrics-source]: https://github.com/dragonflyoss/client/blob/v1.5.7/dragonfly-client-metric/src/lib.rs

Structured logs include transport, immutable revision prefix, duration,
fallback decision, and bounded reason. They do not include credentials or a
private repository URI at default log level.

### Component Changes

| Component | Change |
| --- | --- |
| `cmd/model-agent` | Add transport, fallback, socket, timeout, and concurrency flags; build the selected transport. |
| `pkg/modelagent` | Add opt-in callback routing, credential provenance, endpoint normalization, durable ownership/operation receipts, Dragonfly subprocess runner, exact-file reconciliation, fallback composition, cache partition derivation, metrics, and tests; preserve default direct callbacks. |
| Model-agent image | Add a checksum-pinned corrected `dfget`; publish immutable source/patch provenance, version, and SBOM metadata. |
| External Dragonfly qualification | Select and review a client build with verified origin TLS and bounded source-side metric tags in all node/seed roles; qualify server metric labels. OME does not install or reconcile it. |
| `ome-resources` chart | Add values, flags, and the conditional socket-directory hostPath mount. |
| Documentation | Add installation prerequisites, enable/disable steps, version matrix, dashboards, security guidance, disk planning, and troubleshooting. |
| APIs and generated clients | No change. |
| Manager, webhooks, and serving renderers | No change. |

### Rollout and Rollback

Recommended rollout:

1. Install the qualified corrected Dragonfly combination independently and
   confirm a client peer on each canary node. Override chart defaults as needed
   to use the documented client and seed-peer build. Verify origin trust roots
   and source-side bounded metric labels; unpatched v1.5.7 is not qualified.
2. Confirm socket ownership and network policy from a canary model-agent pod.
   Verify streamed output with separate pod mount namespaces; do not grant
   dfdaemon write access to OME's model directory.
3. Enable Dragonfly for a canary model-agent node pool with direct fallback.
4. Download a pinned public test model concurrently and compare OME and
   Dragonfly metrics.
5. Exercise a private Secret-backed model, Secret rotation, and task partition
   separation; confirm all participating peers and callers share the intended
   trusted confidentiality domain. Check native metric series remain bounded.
6. Stop scheduler or dfdaemon components and verify the selected fallback
   policy, including fail-fast behavior during the circuit-open cooldown.
7. Expand the DaemonSet rollout after origin bytes, latency, disk, and errors
   meet the operator's objectives.

Rollback sets `modelAgent.downloadTransport.type: direct` and rolls the
DaemonSet. No CR, model directory, node label, or runtime migration is needed.
The separately installed Dragonfly system can then be removed after its
clients are no longer in use. Ready model files remain valid; reserved
ownership records remain outside snapshots and are not a new prerequisite
for ordinary direct downloads. Recover or cancel unfinished owned operations
under their locks before permitting a replacement writer.

### Implementation Plan

Implementation is divided into independently reviewable changes:

1. Merge this OEP.
2. Select/review a corrected immutable Dragonfly client build and source-side
   metric mode. Record source and patch provenance, and prove negative origin
   TLS and native-cardinality tests for every back-to-source role. Do not ship
   the investigated unpatched baseline while these gates are unresolved.
3. Add opt-in routing around the existing direct callback, preserving its
   signature and default behavior. Add regression tests for best-effort
   resolution, lazy shared manifests, and ordinary downloads without manifest
   metadata. Extend credential resolution with atomic effective provenance,
   normalize endpoints, and add durable ownership/recovery before any opt-in
   writer, independent of parsing.
4. Add the Dragonfly subprocess runner with mandatory UDS content transfer,
   secure credential environment, generation-aware cache partitioning,
   cancellation, owned-root exact-file reconciliation, per-file staging,
   post-validation, and integrity-aware fallback composition.
5. Add transport metrics and failure/circuit-breaker tests.
6. Add corrected pinned `dfget` to the image, SBOM/scanning coverage, Helm
   values, and the conditional socket mount.
7. Add fake-client integration tests and a real Dragonfly end-to-end job
   qualified against the pinned baseline; record architecture, checksums,
   image digests, chart overrides, and mount topology.
8. Publish operator documentation, tested version matrix, benchmark method,
   and rollback procedure.

Each implementation PR must keep direct-mode tests runnable without
Dragonfly, a Dragonfly socket, or network access.

### Test Plan

[x] I/we understand that component owners may require updates to existing
tests before accepting changes necessary for this enhancement.

#### Prerequisite Testing Updates

- Preserve fixtures for immutable Hugging Face revision resolution and
  manifest validation.
- Provide a test-only fake `dfget` executable that records sanitized argv and
  selected environment names, supports deterministic success/failure, and
  blocks until canceled. Include successful exit with wrong-size, same-size
  corrupt, missing, and extra-file output.
- Add chart snapshots for both direct and Dragonfly values.
- Define a version-pinned, non-blocking nightly environment for real
  Dragonfly tests; ordinary unit tests must not require external services.
- Provide controlled HTTPS origins with invalid/expired/hostname-mismatched
  certificates, a trusted custom CA, redirect sinks, and prefixed/escaped base
  paths. Include all origin-fetching peer roles in qualification.
- Provide crash-injection points around claim/receipt persistence, first-file
  publication, cleanup, and content verification, plus native metric rotation
  stress fixtures for the corrected build.

#### Unit Tests

- `cmd/model-agent`: default configuration selects direct transport; invalid
  enums, timeouts, concurrency, and socket paths fail clearly.
- `pkg/modelagent` direct callback: existing Xet arguments, progress, errors,
  and cancellation remain unchanged. Ordinary fresh downloads can still run
  after revision-resolution failure and without a usable manifest; shared
  parent metadata failures retain their existing retry/failure behavior.
- Dragonfly eligibility: storage type, nil/PerNode distribution, revision,
  effective credential provenance, manifest availability, and owned versus
  unowned destination cases. Ineligible routing is independent of fallback
  policy and never converts cancellation into direct work.
- Endpoint normalization: `/hf` and `/hf/` have the same logical scope and
  directory-form Dragonfly base; different prefixes/origins remain distinct.
  Preserve escaped paths, reject ambiguous/unsupported forms, and give pinned
  direct fallback the logical endpoint rather than Dragonfly's directory form.
- Command construction: one validated file per invocation; immutable revision,
  endpoint, application, tag, timeout, and digest are correct. Require
  `--transfer-from-dfdaemon`, nonexistent staging outputs, and no shell,
  force-hard-link, task-ID override, or daemon model-path dependency.
  Inject conflicting inherited `DFGET_*` settings and verify they cannot
  change output transfer, cache identity, overwrite, or credentials.
- Secret safety: a sentinel token never appears in argv, logs, returned
  errors, metrics, or cache-scope material.
- Cache partitioning: anonymous, different Secret UIDs/resourceVersions/keys,
  different endpoints, and different revisions do not collide; the same
  Secret generation does. Tokens and provenance come from one read. Test
  rotation/recreation, custom secret keys, inline/process tokens, empty or
  failed Secret reads with fallback credentials, and no false anonymous scope.
- Filesystem safety: traversal, absolute paths, symlinked directories,
  special files, and lost ownership fail closed without fallback. Test stale
  regular-file removal and invalid same-size-file removal only in authorized
  owned roots, operation-specific staging cleanup, and no writes into unowned
  legacy paths or unrelated descendants. Ready markers are handler-owned
  exceptions, not downloadable files or stale files to delete before repair.
- Durable ownership: crash before/after claim and receipt fsync, after the
  first rename, and during revision replacement. Restart ordinary downloads
  with skipped/failed parsing and verify the original owner and partial files
  remain recoverable. Under `fail`, retries invoke zero direct callbacks.
  Missing/corrupt receipts with a known claim, conflicting UID/path/node,
  state-store failure, and unavailable metadata for an unfinished operation
  fail closed without cleanup or fallback. Genuine legacy directories retain
  original direct routing; deletion/retention and rollback respect claims.
- Concurrency: per-snapshot file limit and existing snapshot-worker limit are
  both honored.
- Fallback: execution failure and successful exit followed by size/digest or
  exact-set failure each cause exactly one verified direct attempt under
  `direct` and none under `fail`. Test direct verification failure, cleanup
  failure, metadata parsing failure, cancellation at every boundary, and
  jitter. No return path may publish Ready before content verification.
- Circuit breaker: missing socket and all closed/open/half-open transitions
  under both policies; concurrent half-open work; direct-policy jitter and
  fail-policy zero direct calls. Integrity/auth/origin errors must not open a
  global local-service breaker. Avoid stderr-string classification.
- Cancellation: no fallback, all process-group children terminate, and stale
  tasks cannot publish Ready.
- `ReuseIfExists`: the Dragonfly callback preserves parent ownership,
  attachment, recovery, and last-reference deletion. Exact-set repair fails
  children and invalidates the old marker before mutation, writes a current
  marker only after verification, and recovers an interrupted operation
  without stale Ready publication.
- Metrics: label sets are bounded and logical success/failure counters are not
  double-counted during fallback.
- Helm: direct rendering has no Dragonfly volume or mount; Dragonfly rendering
  has the configured socket directory and flags; all existing values remain
  compatible. No missing-socket startup/readiness gate or OME model-path mount
  in Dragonfly is required.

Modified packages must record their baseline and resulting coverage in the
implementation PRs. New transport and fallback packages target at least 85%
statement coverage.

#### Integration and End-to-end Tests

1. **No Dragonfly:** install the chart with default values and download a
   pinned Hugging Face model through the direct path. Cover ordinary
   best-effort resolution/manifest unavailability and existing shared-parent
   failure behavior without requiring a socket or dfget process.
2. **Fake client success:** run model-agent against a fake executable and
   verify the final directory, metadata, ConfigMap state, and node Ready label.
3. **Fallback and integrity:** fail the fake Dragonfly attempt or return
   success with wrong-size/same-size corrupt output, succeed through direct,
   and verify one logical success plus one fallback metric. With `fail`, the
   same cases must not call direct or publish Ready. Include a poisoned
   Dragonfly cache detected by OME despite successful dfget exit.
4. **Cancellation:** delete or retarget the model during download and verify
   no child process, partial Ready state, or fallback remains.
5. **Real P2P:** in a multi-node Kind or equivalent cluster, install a pinned
   baseline Dragonfly combination, download the same public immutable snapshot
   on at least three nodes, and verify peer-task activity and identical file
   digests. Use separate dfdaemon/model-agent pods with no shared model-path
   mount; require UDS content transfer. Run Linux amd64 and arm64 qualification.
6. **Outage:** remove access to the local socket or scheduler and verify both
   `direct` and `fail` policies, including cooldown and half-open concurrency.
   With `fail`, count zero OME direct-origin downloads while the circuit is
   open; with `direct`, observe bounded attempts/jitter and unchanged pod
   startup. Distinguish this from Dragonfly back-to-source and discovery.
7. **Private partitions:** use a mock Hugging Face endpoint with two Secret
   generations and verify task separation, fresh metadata authorization after
   rotation, and token redaction. Test failed Secret lookup falling back to
   inline/process credentials routes direct. Do not interpret task separation
   as a cross-tenant authorization test or cached-content revocation.
8. **Xet conformance:** download representative LFS-backed and Xet-backed
   public repositories; include a gated Xet-backed repository in a protected
   periodic test.
9. **Storage and recovery:** test independent cache/output filesystems and
   same-disk separate copies, then run Dragonfly GC without changing the served
   digest. Update an owned ordinary snapshot to a revision that removes a
   shard; inject stale files into a shared parent; verify exact-set repair and
   ready-marker ordering. Interrupt ordinary download after its first file
   rename with parsing disabled, restart the pod, and recover using durable
   ownership under both policies; `fail` must not become legacy direct work.
   Inject state loss/corruption and CR recreation; preserve unrelated data.
   Interrupt shared repair and recover only owned staging using the parent
   repository/LockID, preserving unowned legacy data.
10. **Upgrade and rollback:** upgrade the tested Dragonfly version, then switch
    OME back to direct without editing model resources or redownloading a ready
    model.
11. **Origin TLS and redirects:** force node-local dfdaemon and seed-peer
    back-to-source requests independently. Invalid CA, wrong hostname, and
    expired certificates must reject the request before the sink receives an
    HF token or model HTTP request. A trusted custom CA succeeds. Cross-origin
    redirects receive no HF bearer token, downgrades fail, and signed URLs
    remain absent from logs. Compare both fallback policies; any permitted
    direct fallback must retain verified HTTPS. Unpatched v1.5.7 is a negative
    reference fixture, never the successful qualification build.
12. **Prefixed endpoints:** use mock origins at `/hf` and `/hf/` and an encoded
    base path. Assert OME discovery uses the prefix, dfdaemon/seed HEAD/GET
    uses it, no native root `/api/` lookup occurs, and final digests match the
    manifest. Under `direct` fallback assert Xet uses that same logical
    endpoint; under `fail` assert zero direct calls on transport failure.
13. **Native cardinality:** perform at least 1,000 Secret-version changes,
    including metadata-only updates, with public/private file tasks. Prove
    task partitions change while native label sets and registry children
    plateau within the stated per-family budget. Inspect all exporters and
    validate exposition without duplicate samples; capture metric-memory,
    RSS, cache/task size, and scrape sample counts separately.

Performance tests compare direct and Dragonfly runs at increasing node counts.
They publish aggregate origin bytes, peer bytes, p50/p95 model-ready duration,
node disk consumption, fallback count, and cache hit ratio. The OEP does not
promise a universal speedup; topology, model shape, origin, and disk throughput
all affect results.

### Graduation Criteria

**Alpha**

- Direct remains the default and passes all existing model-agent tests with no
  Dragonfly installation.
- The opt-in Hugging Face path, immutable revision, atomic Secret-generation
  provenance, prefixed endpoint normalization, durable ordinary/shared recovery,
  verification, cancellation, and both fallback policies pass, including
  skipped parsing, crash recovery, integrity errors, and circuit-open cases.
- A reviewed corrected immutable client build passes origin TLS/redirect tests
  in every back-to-source role and the source-side native metric cardinality
  stress gate. Unpatched v1.5.7 is explicitly excluded from support.
- The corrected pinned baseline passes multi-node, separate-pod UDS tests on
  Linux amd64 and arm64, including public, private/gated, LFS, and Xet-backed
  files. Only then is the combination advertised as supported.
- Security review covers token handling, cache partitioning versus enforced
  confidentiality domains, origin trust/redirects, socket permissions,
  subprocess execution, durable ownership, and image/patch supply chain.
- Documentation includes enable, disable, fallback, disk, version, and
  troubleshooting guidance.

**Beta**

- At least one complete minor release has alpha feedback with no unresolved
  model-corruption, credential-disclosure, or direct-mode regression issue.
- Published benchmarks at 4 and 16 concurrent nodes show lower aggregate
  origin bytes than direct download for the selected representative model set.
- The supported Dragonfly version matrix and upgrade tests cover every
  version advertised by OME.
- Dashboards and alerts cover transport availability, fallbacks, origin
  traffic, peer hits, cache disk pressure, native series count, and exporter RSS.
- Xet-backed and private gated model periodic tests are stable.

**Stable**

- At least two minor releases of operational experience exist across multiple
  cluster sizes.
- Rollback to direct has been exercised without CR changes or loss of ready
  model data.
- No unresolved high-severity security finding exists in the OME integration
  or bundled client.
- Upgrade, compatibility, failure, and performance documentation is complete.

The transport remains opt-in at every maturity stage unless a future OEP
proposes changing the default.

## Open Questions

1. Which corrected upstream release or reviewed patch set will satisfy the
   alpha origin-TLS and bounded-native-label gates? Record the immutable build
   before implementation qualification; none is approved by this draft.
2. Which additional Dragonfly combinations should be qualified after the
   corrected alpha baseline? Each extension must pass the same tests before the
   supported matrix grows.
3. Should `fallbackPolicy=direct` remain the default after alpha feedback, or
   should large clusters prefer `fail` to protect origins during a regional
   Dragonfly outage?
4. Which Dragonfly-native metrics are stable enough to name in OME's operator
   dashboard documentation?

None of these questions changes the compatibility requirement that direct
mode works without Dragonfly.

## Implementation History

- 2026-10-02: Initial provisional draft.
- 2026-10-02: Address design review with UDS streamed output, integrity-aware
  fallback, policy-aware circuit breaking, explicit trust domains and Secret
  generation provenance, unchanged direct contracts, guarded exact-file
  recovery, and a pinned alpha qualification baseline.
- 2026-10-05: Address follow-up review: reject unpatched client v1.5.7,
  require verified origin TLS and bounded native metric tags in a corrected
  build, add write-ahead ordinary ownership independent of parsing, and define
  directory-form endpoint normalization and release-gating conformance tests.

## Drawbacks

1. The model-agent image gains another pinned binary and associated CVE and
   upgrade responsibility.
2. Operators choosing Dragonfly run additional node and control-plane
   infrastructure.
3. Enabled clusters have two caches to size and observe: Dragonfly's piece
   cache and OME's final model directories.
4. Fallback favors availability but can create an origin traffic spike during
   a broad Dragonfly outage.
5. Per-file subprocesses are simpler and safer for alpha but add process
   startup overhead, especially for repositories containing many small files.
6. Credential-generation partitioning can reduce deduplication after Secret
   rotation or when teams use different credentials for the same repository.
   It does not supply tenant authorization; trusted-domain enforcement remains
   an operator responsibility.
7. The feature does not solve the per-node final-storage cost that motivates
   true sharded or lazy model loading.
8. Alpha depends on qualifying a corrected external client build for TLS and
   native metrics; a reviewed patch set adds maintenance until an upstream
   release satisfies those gates. Direct-only OME has no such dependency.
9. Write-ahead ownership requires durable state I/O and recovery tests; it
   adds no record requirement or new cleanup policy to ordinary direct mode.

## Alternatives

**Keep direct downloads only.** This has the smallest operational surface and
is appropriate for small clusters or infrequent rollouts. It does not address
origin fan-out at larger scale and remains the default choice.

**Use only regional object-storage mirrors.** `ome-agent replica` can move a
model closer to the cluster and reduce internet exposure. Every node still
downloads the full model independently from the mirror. Mirrors and P2P can be
complementary, but Oracle OCI Object Storage acceleration is outside alpha.

**Use a shared RWX PVC.** This can eliminate per-node copies, but changes the
serving I/O and availability model and depends on a suitable storage system.
OME already supports PVC-backed models; it is not a replacement for fast
node-local copies in every deployment.

**Implement `Distribution=Sharded` first.** This requires external-cache
configuration, runtime support, status, rendering, failure semantics, and
likely a mount or lazy-loading layer. Ordinary `dfget` still materializes
files, so presenting it as sharded serving would violate the existing API
description. That work needs a separate OEP.

**Proxy the existing Xet client through Dragonfly.** This appears less
invasive, but Xet CAS traffic and range behavior are not the same contract as
ordinary Hugging Face file downloads. Native `hf://` support gives Dragonfly
stable repository and revision semantics and is the supported alpha path.

**Use `dfget --recursive`.** This is shorter to invoke, but duplicates
repository discovery, applies a separate file-count limit, gives OME less path
and digest control, and complicates exact progress and validation. The
manifest-driven per-file approach is safer for alpha.

**Let dfdaemon write or hard-link directly into OME output.** This can reduce
socket copying and physical duplication, but a UDS mount does not expose the
destination inside the daemon pod. It would require identical model-path
mounts, filesystem/inode compatibility, and granting another service write
access to model storage. Alpha chooses mandatory streamed output and separate
copies; such an optimization requires a later, separately reviewed design.

**Publish a separate Dragonfly model-agent image.** This keeps `dfget` out of
the default image but doubles image qualification and makes configuration-only
rollback depend on selecting another image. A single scanned image with a
dormant optional client keeps the operational path simpler; direct mode still
has no Dragonfly service dependency.

**Make Dragonfly a required OME subchart.** This would simplify one-command
installation but couples OME availability and upgrades to a substantial
external system, forces it on small clusters, and conflicts with operators
that already run Dragonfly. This OEP explicitly rejects that coupling.

**Add transport selection to BaseModel.** Per-model policy is flexible, but it
adds API surface before cluster operators have deployment experience and lets
tenants select infrastructure behavior. Alpha keeps transport selection at
the model-agent deployment boundary.
