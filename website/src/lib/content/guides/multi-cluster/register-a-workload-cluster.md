---
title: Register a workload cluster
description: Connect an alpha OME control plane to a member cluster with a dedicated identity, then verify registration and a small OMENative placement separately.
status: preview
since: v1.3
---

Register a member Kubernetes cluster as a [WorkloadCluster](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-WorkloadCluster), then place a small CPU HTTP workload on it. The control plane creates a derived InferenceService on the member; the member's own OME controller creates its InferenceReplica, pods and Service. This separates three checks: the API connection works, placement can execute, and the workload can answer a request.

Multi-cluster serving is **alpha, still in development and off by default**. This guide uses matching source builds, not the v1.2.2 release. It does not configure accelerator capacity, quota admission or a global endpoint. The HTTP server is a connectivity test, not a model server.

The public-source behavior described here was checked at `bc1f94db`. The example files accompany this guide; use a checkout containing them rather than assuming they already exist at that earlier commit, and build its matching controller, CRDs and charts.

<div class="prerequisites" markdown>

- Two disposable Kubernetes clusters, with explicit kubeconfig contexts for their administrative setup. Do not convert an existing serving cluster into a control plane: that role disables its local serving controllers.
- The same OME source image, CRDs and charts on both clusters. Prepare the manager image and `source-values.yaml` from [Install from source](../../getting-started/install.md#install-from-source), including its `OME_SOURCE_TAG` and `OME_IMAGE_REGISTRY` variables. Both clusters must be able to pull that image on their worker architectures.
- cert-manager installed and ready on both clusters. This OMENative-only example needs no model agent, GPU, Kueue, PodMonitor CRD or Gateway API installation.
- Helm, `kubectl`, `jq` and `curl`. Setup permissions must cover the Helm resources, a dedicated member ServiceAccount and its TokenRequest, and a Secret on the control plane.
- The member API's HTTPS endpoint and its trusted CA certificate in a local PEM file, supplied by the cluster administrator. The endpoint must be reachable from the control-plane manager pods, not just your laptop.
- Member workers able to pull `docker.io/hashicorp/http-echo:1.0.0`. Verify that this versioned image supports their architecture. The test pod requests 25 millicores and 32 MiB of memory.

</div>

Commands run from the repository root. Example files are under `config/samples/docs/multi-cluster-registration/`. Choose your contexts explicitly; their names need not match the WorkloadCluster name:

```bash
OME_HUB_CONTEXT=hub
OME_MEMBER_CONTEXT=worker-a
```

## Step 1: Install the member and control-plane profiles

The roles are different even though they use the same build:

| Cluster | Values | Responsibility |
| --- | --- | --- |
| Member | `ome.multicluster.enabled: false`, `role: ""` | Runs the ordinary local serving controllers. `multiclusterAccess` separately grants the remote placement identity its permissions. |
| Control plane | `ome.multicluster.enabled: true`, `role: control-plane` | Connects to registered members and places source InferenceServices. It does not run their local serving workloads. |

The member overlay binds the chart's `ome-multicluster-access` ClusterRole to a dedicated ServiceAccount:

```yaml title="member-values.yaml"
ome:
  multicluster:
    enabled: false
    role: ""
  multiclusterAccess:
    enabled: true
    subjects:
      - kind: ServiceAccount
        name: ome-placement-lab
        namespace: ome
```

This is not `cluster-admin`, but it is powerful: the role can create, update and delete InferenceServices across member namespaces and read the runtime, model, pod, replica and endpoint information placement needs. It has no direct permission to create namespaces, read Secrets or change RBAC. However, creating workloads can cause pods to use member ServiceAccounts or mount namespace Secrets. This is trusted-controller access, **not tenant isolation**. Use a dedicated member for this lab; review these permissions and member admission controls before allowing a control plane onto a shared cluster. An empty `subjects` list installs only the role, for platforms that manage their own bindings.

The control-plane overlay gives the controller a stable ownership ID. Choose a different ID for each control plane that shares members, and keep it stable across upgrades so orphan cleanup stays scoped to the correct owner:

```yaml title="control-plane-values.yaml"
ome:
  multicluster:
    enabled: true
    role: control-plane
    placementControlPlaneID: registration-lab-hub
    config:
      placement:
        memberOperatorNamespace: ome
        localQueue: ""
        capacity: null
      routing:
        enabled: false
  multiclusterAccess:
    enabled: false
```

For this new lab, apply the CRD chart on both clusters before the resources chart:

```bash
helm --kube-context "$OME_MEMBER_CONTEXT" upgrade --install ome-crd ./charts/ome-crd \
  --namespace ome --create-namespace
helm --kube-context "$OME_HUB_CONTEXT" upgrade --install ome-crd ./charts/ome-crd \
  --namespace ome --create-namespace
```

Install the member first, with the manager-only base values from the source installation:

```bash
helm --kube-context "$OME_MEMBER_CONTEXT" upgrade --install ome ./charts/ome-resources \
  --namespace ome -f source-values.yaml \
  -f config/samples/docs/multi-cluster-registration/member-values.yaml \
  --set-string global.hub="$OME_IMAGE_REGISTRY" \
  --set-string ome.controller.tag="$OME_SOURCE_TAG" \
  --set-string ome.omeAgent.tag="$OME_SOURCE_TAG" \
  --set-string modelAgent.image.tag="$OME_SOURCE_TAG"
```

Then install the control plane:

```bash
helm --kube-context "$OME_HUB_CONTEXT" upgrade --install ome ./charts/ome-resources \
  --namespace ome -f source-values.yaml \
  -f config/samples/docs/multi-cluster-registration/control-plane-values.yaml \
  --set-string global.hub="$OME_IMAGE_REGISTRY" \
  --set-string ome.controller.tag="$OME_SOURCE_TAG" \
  --set-string ome.omeAgent.tag="$OME_SOURCE_TAG" \
  --set-string modelAgent.image.tag="$OME_SOURCE_TAG"
```

On Helm 4, add `--server-side=false` to both `ome-resources` commands. If default-runtime admission races webhook startup, use [the conditional troubleshooting steps](../../getting-started/install.md#the-first-install-fails-with-a-webhook-error). Keep these overlays and image overrides on every upgrade. When upgrading an existing fleet, update member CRDs and controllers before enabling source placement behavior that requires their newer execution contract.

```bash
kubectl --context "$OME_MEMBER_CONTEXT" rollout status deployment/ome-controller-manager -n ome --timeout=5m
kubectl --context "$OME_HUB_CONTEXT" rollout status deployment/ome-controller-manager -n ome --timeout=5m
```

## Step 2: Create a dedicated member credential

The chart creates the role and binding, **not** the ServiceAccount or a token. Create the lab account; `automountServiceAccountToken: false` does not prevent an explicit TokenRequest:

```yaml title="serviceaccount.yaml"
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ome-placement-lab
  namespace: ome
automountServiceAccountToken: false
```

```bash
kubectl --context "$OME_MEMBER_CONTEXT" apply \
  -f config/samples/docs/multi-cluster-registration/serviceaccount.yaml
```

For this short lab, request a time-limited token for that account. For ongoing use, have your platform issue and rotate the dedicated credential; OME does not renew a bearer token stored in a Secret. Do not copy an administrator's kubeconfig into the control plane.

Replace these two values with the member endpoint and its trusted CA file. Disable shell tracing and keep the temporary credential directory out of logs, shared storage and version control:

```bash
OME_MEMBER_API=https://api.worker-a.example.com:6443
OME_MEMBER_CA_FILE=/absolute/path/to/worker-a-ca.crt
OME_CREDENTIAL_DIR="$(mktemp -d)"
OME_MEMBER_KUBECONFIG="$OME_CREDENTIAL_DIR/worker-a.kubeconfig"
umask 077

kubectl --context "$OME_MEMBER_CONTEXT" create token ome-placement-lab \
  --namespace ome --duration=1h --output=json \
  > "$OME_CREDENTIAL_DIR/token-request.json"

jq --exit-status --arg server "$OME_MEMBER_API" --rawfile ca "$OME_MEMBER_CA_FILE" '
  if (.status.token | type) != "string" or (.status.token | length) == 0
  then error("TokenRequest returned no token")
  else {
    apiVersion: "v1", kind: "Config",
    clusters: [{name: "worker-a", cluster: {
      server: $server, "certificate-authority-data": ($ca | @base64)
    }}],
    contexts: [{name: "worker-a", context: {
      cluster: "worker-a", user: "ome-placement-lab"
    }}],
    "current-context": "worker-a",
    users: [{name: "ome-placement-lab", user: {token: .status.token}}]
  } end
' "$OME_CREDENTIAL_DIR/token-request.json" > "$OME_MEMBER_KUBECONFIG"

jq -r '.status.expirationTimestamp' "$OME_CREDENTIAL_DIR/token-request.json"
```

The final command prints only the expiration time. `1h` is a requested lifetime; the API server decides the actual expiration. Finish the lab and cleanup before it expires, or renew the token and replace the control-plane Secret. Stop if either credential command fails; do not register an empty file.

OME accepts an HTTPS endpoint with inline CA data and an inline bearer token or client certificate/key. It rejects disabled TLS verification, local CA/certificate/key/token paths, basic authentication and legacy `auth-provider` plugins. Use a minimal kubeconfig containing only this member and identity; some safety checks also inspect unused entries. `exec` credential plugins are off by default; a platform using them must explicitly enable and allow-list the command, install that binary in the manager image, and provide its workload identity. See [controller flags](../../reference/operate-ome/controller-manager-flags.md#multi-cluster).

Check the new identity before registering it. These commands use only the dedicated credential, not the administrative member context:

```bash
kubectl --kubeconfig "$OME_MEMBER_KUBECONFIG" auth can-i create inferenceservices.ome.io -n ome-registration-lab
kubectl --kubeconfig "$OME_MEMBER_KUBECONFIG" auth can-i get servingruntimes.ome.io -n ome-registration-lab
kubectl --kubeconfig "$OME_MEMBER_KUBECONFIG" auth can-i list inferencereplicas.ome.io -n ome-registration-lab
kubectl --kubeconfig "$OME_MEMBER_KUBECONFIG" auth can-i list pods -n ome-registration-lab
kubectl --kubeconfig "$OME_MEMBER_KUBECONFIG" auth can-i get secrets -n ome
kubectl --kubeconfig "$OME_MEMBER_KUBECONFIG" auth can-i create clusterrolebindings.rbac.authorization.k8s.io
```

The first four checks should say `yes`; the last two should say `no`. Broader access means the identity has another binding that needs review. These test direct API permissions only: a `no` for Secrets does not prevent indirect access through workload creation. They also do not prove that the control-plane pods can reach the member API.

## Step 3: Register the member on the control plane

Store only the dedicated kubeconfig in the control-plane namespace `ome`:

```bash
kubectl --context "$OME_HUB_CONTEXT" create secret generic worker-a-kubeconfig \
  --namespace ome --from-file=kubeconfig="$OME_MEMBER_KUBECONFIG"
```

Restrict who can read or update this Secret and who can create or change WorkloadClusters. The credential authorizes cross-cluster workload changes. Kubernetes Secrets are not a substitute for access controls or encryption at rest.

The WorkloadCluster is cluster-scoped; its Secret reference includes a namespace:

```yaml title="workloadcluster.yaml"
apiVersion: ome.io/v1beta1
kind: WorkloadCluster
metadata:
  name: worker-a
  labels:
    docs.ome.io/lab: registration
spec:
  clusterSource:
    kubeConfig:
      secretRef:
        name: worker-a-kubeconfig
        namespace: ome
      key: kubeconfig
```

```bash
kubectl --context "$OME_HUB_CONTEXT" apply \
  -f config/samples/docs/multi-cluster-registration/workloadcluster.yaml
kubectl --context "$OME_HUB_CONTEXT" wait workloadcluster/worker-a \
  --for=condition=Ready --timeout=5m
kubectl --context "$OME_HUB_CONTEXT" get workloadcluster worker-a \
  -o jsonpath='{.metadata.generation}{"\n"}{range .status.conditions[*]}{.type}{"="}{.status}{" reason="}{.reason}{" observedGeneration="}{.observedGeneration}{"\n"}{end}'
```

`Ready=True` with reason `Reachable` means OME validated the kubeconfig and reached the member's `/version` endpoint. Check that `observedGeneration` matches the registration's generation. It does not prove workload permissions, available GPUs, quota admission, a running InferenceService or a reachable model endpoint. A previously healthy connection can remain `Ready=True` with reason `ProbeFailedRetrying` during the connection grace period.

`clusterProfileRef` is present in the API but **not implemented**: the controller reports `ClusterProfileUnsupported`. Use `kubeConfig` here. The alpha `kubectl ome cluster status` command is observational; it does not register clusters, read credentials or probe the remote API.

For credential rotation, generate a fresh dedicated kubeconfig, then replace the Secret data without printing it:

```bash
kubectl --context "$OME_HUB_CONTEXT" create secret generic worker-a-kubeconfig \
  --namespace ome --from-file=kubeconfig="$OME_MEMBER_KUBECONFIG" \
  --dry-run=client -o yaml \
  | kubectl --context "$OME_HUB_CONTEXT" apply -f -
```

OME watches Secret changes and reconnects. Re-run the dedicated-credential checks, then confirm controller reconnection and a subsequent successful member observation. Updating Secret data does not change the WorkloadCluster generation, and its status does not record the observed Secret version: an existing `Ready=True`, even with a matching `observedGeneration`, is not proof that the new credential was used. Do not delete and recreate the WorkloadCluster merely to change a token, because recreation changes its identity.

## Step 4: Verify one placement with a CPU workload

Registration does not replicate namespaces, runtimes, models, PVCs or image-pull Secrets. Provision the test namespace on both clusters and the namespaced runtime only on the member:

```yaml title="namespace.yaml"
apiVersion: v1
kind: Namespace
metadata:
  name: ome-registration-lab
```

```yaml title="servingruntime.yaml"
apiVersion: ome.io/v1beta1
kind: ServingRuntime
metadata:
  name: registration-http
  namespace: ome-registration-lab
spec:
  engineConfig:
    terminationGracePeriodSeconds: 10
    runner:
      name: ome-container
      image: docker.io/hashicorp/http-echo:1.0.0
      args:
        - -listen=:8080
        - -text=hello from worker-a
      ports:
        - name: http
          containerPort: 8080
      readinessProbe:
        httpGet:
          path: /
          port: http
        periodSeconds: 2
      resources:
        requests:
          cpu: 25m
          memory: 32Mi
        limits:
          cpu: 100m
          memory: 64Mi
```

```bash
kubectl --context "$OME_HUB_CONTEXT" apply \
  -f config/samples/docs/multi-cluster-registration/namespace.yaml
kubectl --context "$OME_MEMBER_CONTEXT" apply \
  -f config/samples/docs/multi-cluster-registration/namespace.yaml
kubectl --context "$OME_MEMBER_CONTEXT" apply \
  -f config/samples/docs/multi-cluster-registration/servingruntime.yaml
```

The runtime runs `hashicorp/http-echo:1.0.0` on port 8080 with an HTTP readiness probe. Like the [single-cluster HTTP lab](../omenative/learn-omenative.md), it needs no BaseModel or storage. Create the source InferenceService **only on the control plane**:

```yaml title="inferenceservice.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: registration-smoke
  namespace: ome-registration-lab
spec:
  deploymentMode: OMENative
  runtime:
    apiGroup: ome.io
    kind: ServingRuntime
    name: registration-http
  engine:
    minReplicas: 1
    maxReplicas: 1
    autoscaler:
      class: None
    lifecycle:
      minReadySeconds: 5
  placement:
    policy: ClusterAffinity
    mode: Single
    maxSurge: 1
    clusterAffinity:
      - matchFields:
          - key: metadata.name
            operator: In
            values:
              - worker-a
```

`ClusterAffinity` is an explicit policy opt-in and requires an explicit mode. This field match selects the real WorkloadCluster name, independently of its labels. Placement currently verifies an OMENative backend for each participating component; a RawDeployment runtime is not an interchangeable backend for this path.

```bash
kubectl --context "$OME_HUB_CONTEXT" apply \
  -f config/samples/docs/multi-cluster-registration/inferenceservice.yaml
kubectl --context "$OME_HUB_CONTEXT" get inferenceservice registration-smoke -n ome-registration-lab \
  -o jsonpath='{.status.placement}{"\n"}{range .status.conditions[*]}{.type}{"="}{.status}{" reason="}{.reason}{"\n"}{end}'
```

Look for `worker-a` in the accepted plan and candidates. `PlacementConverged` reports member execution of the allocation; `PlacementSatisfied` reports its admitted floor. Neither means the HTTP server is ready. Wait for serving readiness on the member and its source:

```bash
kubectl --context "$OME_MEMBER_CONTEXT" wait inferenceservice/registration-smoke \
  -n ome-registration-lab --for=condition=Ready --timeout=5m
kubectl --context "$OME_MEMBER_CONTEXT" wait inferencereplica/registration-smoke-engine \
  -n ome-registration-lab --for=jsonpath='{.status.availableReplicas}'=1 --timeout=5m
kubectl --context "$OME_HUB_CONTEXT" wait inferenceservice/registration-smoke \
  -n ome-registration-lab --for=condition=Ready --timeout=5m
```

If the member object has not been created yet, `kubectl wait` can return `NotFound`; inspect source conditions and retry after placement creates it. Inspect the derived object's provenance without changing its controller-owned placement fields:

```bash
kubectl --context "$OME_MEMBER_CONTEXT" get inferenceservice registration-smoke -n ome-registration-lab \
  -o jsonpath='{.metadata.labels.ome\.io/placement-control-plane}{"\n"}{.metadata.annotations.ome\.io/placement-origin-uid}{"\n"}'
```

The control-plane label should be `registration-lab-hub`, and the origin UID should match the source InferenceService's UID. Test the member Service in a separate terminal:

```bash
kubectl --context "$OME_MEMBER_CONTEXT" port-forward service/registration-smoke-engine \
  -n ome-registration-lab 8080:8080
```

```bash
curl --fail --silent --show-error http://127.0.0.1:8080/
```

The response should be `hello from worker-a`. This checks one pod through a local port-forward, not cross-cluster client routing or a global endpoint.

## Connection, admission and capacity are separate

The profile deliberately leaves `placement.localQueue` empty and gives the source no `ome.io/local-queue` annotation. OME therefore does not stamp a Kueue queue label on the derived components; this example does not request Kueue quota gating. Any other admission controls on the member still apply. Do not interpret successful placement as evidence that accelerator quota enforcement is configured.

| Evidence | What it proves |
| --- | --- |
| WorkloadCluster `Ready` | Validated connection and API reachability, subject to the connection grace period. No capacity is stored here. |
| Source placement conditions and member InferenceReplica | The runtime resolves, the allocation executes and the requested instances are admitted or ready, as each condition states. |
| Fresh AcceleratorQuota capacity reports | Hardware observations used by separately configured capacity-based placement. These are not free GPU counts or workload readiness. |
| TrafficMap publication and a client request | Routing publication and data-plane behavior, after configuring gateways and reachability separately. |

`SplitByCapacity` is a separate setup, not the next value to substitute into this smoke test. It requires a configured capacity provider with a root name and freshness/stability/refresh settings, current quota-manager hardware reports on the member and control plane, matching resource-flavor identities, and resolved accelerator demand. `PlacementCapacityFresh` is its source-side check. This CPU-only runtime has no accelerator demand and is not a capacity-placement fixture. See [Set accelerator quotas](../operate-ome/accelerator-quota.md) for the quota subsystem; verify its hardware-report prerequisites before enabling capacity-based placement.

## Troubleshooting

| Evidence | Check |
| --- | --- |
| `SecretNotFound` | The referenced Secret is on the control plane, in the declared namespace. |
| `BadKubeConfig` | The Secret contains the declared key and a supported, self-contained kubeconfig. Keep TLS verification enabled. |
| `ConnectionFailed` | Token expiry, API DNS/routing/firewalls, trusted CA and endpoint hostname. Test reachability from the manager's network, not only your workstation. |
| `ClusterProfileUnsupported` | Replace `clusterProfileRef` with the supported Secret-backed source. |
| Registration is Ready, but remote reads or writes are forbidden | Check the dedicated identity's binding and permissions; `/version` access is insufficient. |
| `PlacementBackendReady` is false or unknown | Provision the named runtime on the member, check its namespace/kind and confirm every participating component resolves to OMENative. |
| No candidate matches | Check the registered WorkloadCluster name and the source's affinity. |
| Member pods remain Pending | Inspect pod events and admission gates. CPU/memory, image pulls and local scheduling are separate from registration. |

## Cleanup

Keep both controllers, the registration and the credential working until derived workloads are gone. Deleting the WorkloadCluster or credential first removes the transport needed for cleanup. A traffic drain alone does not delete serving workloads.

For this isolated lab, delete the source first and wait for its finalizer and the member's cleanup:

```bash
kubectl --context "$OME_HUB_CONTEXT" delete inferenceservice registration-smoke \
  -n ome-registration-lab --wait=true --timeout=5m
kubectl --context "$OME_MEMBER_CONTEXT" wait inferenceservice/registration-smoke \
  -n ome-registration-lab --for=delete --timeout=5m
kubectl --context "$OME_MEMBER_CONTEXT" delete \
  -f config/samples/docs/multi-cluster-registration/namespace.yaml --wait=true --timeout=5m
kubectl --context "$OME_HUB_CONTEXT" delete \
  -f config/samples/docs/multi-cluster-registration/namespace.yaml --wait=true --timeout=5m
```

If routing was added, source teardown waits for its publication cleanup before removing the backends. Do not force-remove finalizers to bypass an unreachable member; restore the connection and inspect the outstanding work.

Only after all sources using this registration are removed or safely moved, unregister the member and revoke the lab credential:

```bash
kubectl --context "$OME_HUB_CONTEXT" delete workloadcluster worker-a
kubectl --context "$OME_HUB_CONTEXT" delete secret worker-a-kubeconfig -n ome
kubectl --context "$OME_MEMBER_CONTEXT" delete serviceaccount ome-placement-lab -n ome
```

Remove this account from `member-values.yaml` and disable `ome.multiclusterAccess.enabled` if no other placement identity needs that role, then repeat the member Helm command with the revised values. This removes the chart-owned binding through Helm. Delete the two local credential files in `$OME_CREDENTIAL_DIR` using your normal secure credential-handling procedure. The OME installations and cert-manager remain; uninstall them separately only if the clusters are dedicated to the lab.

## Next steps

- [Publish a global endpoint](publish-a-global-endpoint.md) after adding another member, selecting the desired placement mode, and configuring member gateways and network reachability.
- [Configure routing health probes](routing-health-probes.md) to distinguish API reachability from serving-endpoint health.
- [Drain a workload cluster](drain-a-workload-cluster.md) for traffic maintenance after routing is configured; draining traffic does not unregister the member.
