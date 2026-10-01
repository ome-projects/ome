---
title: Stage model weights
description: Download a Hugging Face model to a shared PVC with ome-agent replica, then register the completed copy and serve it.
---

Copy [Qwen3-0.6B](https://huggingface.co/Qwen/Qwen3-0.6B) to a PersistentVolumeClaim with a one-time `ome-agent replica` Job. After the copy completes, register a [BaseModel](../../concepts/models/base-models.md) and serve the weights from the claim. The copying step needs neither OME's controller nor its model-agent DaemonSet, and uses no GPU.

<div class="prerequisites" markdown>

- A Kubernetes cluster, `kubectl`, and permission to create a namespace, PVC, ConfigMap and Job.
- A default StorageClass that supports `ReadWriteMany`, with space for the model and permission for UID/GID 65532 to write. The example requests 10 GiB. If your default class supports only `ReadWriteOnce`, select a shared-storage class with `storageClassName` in the PVC below. The copy, metadata and serving pods can run on different nodes.
- An `ome-agent` image built from public commit `bc1f94db`, the source baseline for this guide, in a registry your nodes can pull from. [Build from source](../../getting-started/install.md#install-from-source) covers the toolchain. From that checkout, `make push-ome-agent-image REGISTRY=registry.example.com/ome TAG=bc1f94db` builds and pushes it; replace the registry in that command and in the Job below with yours. The build needs Docker, Go and Rust/Xet.
- Nodes that can reach Hugging Face and its download endpoints. Qwen3-0.6B is ungated, so a Hugging Face token is optional.
- For registration and serving in Step 4: OME [installed from the same source baseline](../../getting-started/install.md#install-from-source), with its metadata Jobs using your `ome-agent` image, and an amd64 NVIDIA GPU node with 10 CPUs and 30 GiB of memory free. The model agent isn't required. See [Configure the metadata Job](serve-models-from-pvc.md#configure-the-metadata-job) for image settings.

</div>

## Step 1: Create a place for the weights

Save this as `model-storage.yaml` and apply it:

```yaml title="model-storage.yaml"
apiVersion: v1
kind: Namespace
metadata:
  name: qwen3-pvc
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: model-storage
  namespace: qwen3-pvc
spec:
  accessModes: [ReadWriteMany]
  resources:
    requests:
      storage: 10Gi
```

```bash
kubectl apply -f model-storage.yaml
kubectl get pvc model-storage -n qwen3-pvc
```

The claim should become `Bound`. A class with `WaitForFirstConsumer` can leave it `Pending` until you create the Job in Step 2; don't wait for binding before creating that consumer. Other provisioning errors appear in `kubectl describe pvc model-storage -n qwen3-pvc`.

Use an empty destination directory for a new model revision. The agent writes directly to it: this is not an atomic publication, and failed copies can leave partial files. Keep serving pods away from the destination until the Job completes. Never point two copying Jobs at the same PVC directory.

## Step 2: Run the copy

Save the ConfigMap and Job below as `stage-qwen3.yaml`. Replace `registry.example.com/ome/ome-agent:bc1f94db` with the image you built. If the registry requires credentials, configure the Job's `imagePullSecrets` as in [Install from a private registry](../../getting-started/private-registries.md).

The example downloads `@main`, which can change. For a repeatable artifact, replace `main` in `source.storage_uri` with the commit ID you selected from the model's Hugging Face history, and use a new destination directory for a different revision. No model snapshot is pinned by the OME source commit.

```yaml title="stage-qwen3.yaml"
apiVersion: v1
kind: ConfigMap
metadata:
  name: stage-qwen3
  namespace: qwen3-pvc
data:
  replica.yaml: |
    local_path: /models
    cache_dir: /tmp/hf-cache
    download_size_limit_gb: 8
    enable_size_limit_check: true
    hf_download_timeout: 30m
    hf_download_stale_progress_timeout: 10m
    source:
      storage_uri: hf://Qwen/Qwen3-0.6B@main
    target:
      storage_uri: pvc://model-storage/qwen3-0-6b
      pvc:
        enabled: true
---
apiVersion: batch/v1
kind: Job
metadata:
  name: stage-qwen3
  namespace: qwen3-pvc
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      automountServiceAccountToken: false
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        runAsGroup: 65532
        fsGroup: 65532
        seccompProfile:
          type: RuntimeDefault
      containers:
        - name: replica
          image: registry.example.com/ome/ome-agent:bc1f94db
          args: [replica, --config, /config/replica.yaml]
          securityContext:
            allowPrivilegeEscalation: false
            capabilities:
              drop: [ALL]
          env:
            - name: OME_AGENT_HF_TOKEN
              valueFrom:
                secretKeyRef:
                  name: hf-token
                  key: token
                  optional: true
          volumeMounts:
            - name: config
              mountPath: /config
              readOnly: true
            - name: models
              mountPath: /models
            - name: cache
              mountPath: /tmp
      volumes:
        - name: config
          configMap:
            name: stage-qwen3
        - name: models
          persistentVolumeClaim:
            claimName: model-storage
        - name: cache
          emptyDir:
            sizeLimit: 10Gi
```

For a gated or private source, first create `hf-token` in `qwen3-pvc`, using a token with access to that repository. With the token already in your `HF_TOKEN` environment variable, run `kubectl create secret generic hf-token -n qwen3-pvc --from-literal=token="$HF_TOKEN"`. The Job passes it as `OME_AGENT_HF_TOKEN`; don't put credentials in the ConfigMap. Skip this for the public Qwen example.

The PVC must be mounted at `local_path`. Here the agent writes to `/models/qwen3-0-6b`: `/models` is the volume mount, and `qwen3-0-6b` is the target URI's sub-path. The URI names the destination but doesn't mount it or create the claim. `target.pvc.enabled: true` is required.

The Job runs as UID/GID 65532, which also reads the files in OME's metadata Job. The storage driver must honor `fsGroup`, or the volume must already allow that user to write. The separate `emptyDir` holds the temporary download cache; deleting the Job removes the cache, not the PVC's weights.

```bash
kubectl apply -f stage-qwen3.yaml
kubectl get jobs,pods -n qwen3-pvc
```

Check that the Job's pod starts. If it stays `Pending`, inspect the pod and PVC events before continuing.

## Step 3: Confirm the copy completed

Follow the Job's log once its pod is running:

```bash
kubectl logs -f job/stage-qwen3 -n qwen3-pvc
```

Then check its outcome:

```bash
kubectl get job stage-qwen3 -n qwen3-pvc \
  -o 'custom-columns=SUCCEEDED:.status.succeeded,FAILED:.status.failed,CONDITIONS:.status.conditions[*].type'
```

Continue only when `SUCCEEDED` is `1` and the Job has the `Complete` condition. A replication error or interruption before completion exits nonzero. This Job sets `backoffLimit: 0`, so a failed attempt stops for you to inspect; it doesn't repeatedly rewrite the destination.

If the pod fails, read its log and termination details with `kubectl describe pod -n qwen3-pvc -l batch.kubernetes.io/job-name=stage-qwen3`. A completed copy establishes that the transfer finished; metadata parsing and a serving request in Step 4 check that OME can use it.

## Step 4: Register and serve the completed copy

Save this as `qwen3-pvc-model.yaml`. The BaseModel and its consumers belong in the same namespace as the PVC:

```yaml title="qwen3-pvc-model.yaml"
apiVersion: ome.io/v1beta1
kind: BaseModel
metadata:
  name: qwen3-0-6b
  namespace: qwen3-pvc
spec:
  storage:
    storageUri: pvc://model-storage/qwen3-0-6b
```

```bash
kubectl apply -f qwen3-pvc-model.yaml
kubectl wait --for=jsonpath='{.status.state}'=Ready basemodel/qwen3-0-6b -n qwen3-pvc --timeout=10m
```

OME starts a metadata Job that mounts the sub-path and reads the files as user 65532. Wait for the model to become `Ready` before creating an InferenceService. If it fails, see [The metadata Job fails](serve-models-from-pvc.md#the-metadata-job-fails).

From the same OME checkout, install the catalog runtime if `srt-qwen3-0-6b` doesn't already exist:

```bash
kubectl apply -f config/runtimes/srt/Qwen/qwen3-0-6b-rt.yaml
```

It uses `docker.io/lmsysorg/sglang:v0.5.5.post3-cu129-amd64` and serves `Qwen/Qwen3-0.6B` on port 8080. Save this as `qwen3-pvc-isvc.yaml`:

```yaml title="qwen3-pvc-isvc.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: qwen3-0-6b
  namespace: qwen3-pvc
spec:
  deploymentMode: OMENative
  model:
    name: qwen3-0-6b
    kind: BaseModel
  runtime:
    name: srt-qwen3-0-6b
  engine:
    minReplicas: 1
    maxReplicas: 1
    autoscaler:
      class: None
    nodeSelector:
      kubernetes.io/arch: amd64
```

```bash
kubectl apply -f qwen3-pvc-isvc.yaml
kubectl wait --for=condition=Ready inferenceservice/qwen3-0-6b -n qwen3-pvc --timeout=30m
kubectl port-forward svc/qwen3-0-6b-engine 8080:8080 -n qwen3-pvc
```

Leave the port-forward running. In another terminal, send a request:

```bash
curl --fail-with-body http://localhost:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"Qwen/Qwen3-0.6B","messages":[{"role":"user","content":"What is Kubernetes? Answer in one sentence."}],"max_tokens":100,"chat_template_kwargs":{"enable_thinking":false}}'
```

A successful response contains the generated text in `choices[0].message.content`. The serving pod mounts the model directory read-only at `/opt/ml/model`; its `MODEL_PATH` points there. The claim remains `ReadWriteMany`: mounting it read-only in consumers doesn't require changing its access mode. This recipe uses OMENative with autoscaling disabled; it needs neither PodMonitor nor KEDA. The AMD64 selector matches the catalog image; the GPU driver must support its CUDA 12.9 build.

## Other sources and destinations

`replica` supports Hugging Face to OCI or PVC, OCI to OCI or PVC, and PVC to OCI or PVC. A `local://` endpoint isn't supported; use [the model agent](../operate-ome/model-agent.md) for downloads onto nodes, or [stage files yourself](serve-models-from-local-storage.md#step-1-stage-the-weights-and-label-the-nodes).

For OCI, use `oci://n/{namespace}/b/{bucket}/o/{prefix}` and enable `oci.enabled` on the source or target block with its authentication settings. OCI-to-PVC copies preserve the full source object path, including its prefix, so point the BaseModel sub-path at the actual directory containing `config.json`. The public [replication configuration](https://github.com/ome-projects/ome/blob/bc1f94db/site/content/en/docs/administration/model-replication.md) describes the supported pairs, mount layouts, checksums and OCI upload locks.

## Troubleshooting and retrying

| Symptom | Check or fix |
| --- | --- |
| PVC or pod stays `Pending` | Check the StorageClass, available capacity and mount events. `ReadWriteOnce` doesn't permit arbitrary writer/reader placement across nodes. |
| `ImagePullBackOff` | Check the image name, architecture and registry credentials. The example registry must be replaced with yours. |
| Hugging Face returns 401 or 403 | Check the repository name, accepted model license and token access. The optional Secret belongs in `qwen3-pvc`. |
| `permission denied` | Check write access to the mounted directory for UID/GID 65532 and the driver's `fsGroup` behavior. |
| Size limit or timeout failure | Read the Job's log. Increase space or the config limits only after checking the source and destination. The 8 GiB source-size limit doesn't reserve PVC or cache capacity. |

A failed copy leaves its PVC data. After correcting the cause, delete the failed Job, update the ConfigMap and create the Job again. Reuse the destination only for the same pinned snapshot with no readers; for a different revision, choose a fresh sub-path. Copying doesn't remove obsolete files already at the destination. Keep the BaseModel pointed at the previous completed directory until the new one is ready.

## Clean up

After successful staging, remove the copy Job and its configuration:

```bash
kubectl delete job stage-qwen3 -n qwen3-pvc
kubectl delete configmap stage-qwen3 -n qwen3-pvc
```

The BaseModel, InferenceService and weights remain. To stop serving while retaining the weights, delete the InferenceService first, then the BaseModel. OME removes the model's metadata Jobs and leaves the PVC intact; see [PVC cleanup](serve-models-from-pvc.md#clean-up). Keep a shared runtime if other services use it.

Delete `hf-token` only if you created it for this copy and no other Job uses it. Delete `model-storage` only when you intend to release the stored weights: depending on the volume's reclaim policy, deleting the PVC can delete its backing storage. Deleting the namespace also deletes the PVC.
