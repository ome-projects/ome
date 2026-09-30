---
title: Benchmark output storage
description: "A BenchmarkJob's outputLocation accepts these storageUri formats and parameters for OCI, S3, Azure Blob, Google Cloud Storage, GitHub Releases and PVCs."
---

A [BenchmarkJob](../../concepts/serving/benchmarks.md) writes its results to the location in `spec.outputLocation`, a required [StorageSpec](../api/ome.v1beta1.md#ome-io-v1beta1-StorageSpec). OME reads two of its fields, and turns both into arguments for genai-bench, the benchmark tool that runs in [the BenchmarkJob's Job](../../concepts/serving/benchmarks.md#how-a-benchmark-runs):

- `storageUri`, whose scheme picks the backend;
- `parameters`, a map of settings and credentials for that backend.

OME ignores the other StorageSpec fields. None of them, `key` included, points `outputLocation` at a Secret.

!!! warning "Credentials are readable in the pod spec"
    OME passes each parameter it knows to genai-bench as a command-line argument, keys and tokens included. Anyone who can read the BenchmarkJob, its Job or its pod can read them, including holders of the built-in `view` role, which can't read Secrets. See [Credentials appear on the pod command line](#credentials-appear-on-the-pod-command-line).

| URI | Backend | Results |
| --- | --- | --- |
| `oci://n/<namespace>/b/<bucket>/o/<prefix>` | [OCI Object Storage](#oci-object-storage) | Uploaded |
| `s3://<bucket>/<prefix>` or `s3://<bucket>@<region>/<prefix>` | [AWS S3](#aws-s3) | Uploaded |
| `az://<account>/<container>/<path>` or `az://<account>.blob.core.windows.net/<container>/<path>` | [Azure Blob Storage](#azure-blob-storage) | Uploaded |
| `gs://<bucket>/<path>` | [Google Cloud Storage](#google-cloud-storage) | Uploaded |
| `github://<owner>/<repository>` or `github://<owner>/<repository>@<tag>` | [GitHub Releases](#github-releases) | Uploaded to a release |
| `pvc://<claim>/<sub-path>` | [Persistent volume claims](#persistent-volume-claims) | Written to a claim in the BenchmarkJob's namespace |

For a claim, OME mounts it in the pod, and genai-bench writes to it directly. For the other five backends, OME passes `--upload-results` and the location, and genai-bench uploads the results there.

How OME checks a URI and its parameters:

- OME's BenchmarkJob webhook checks the URI's form when you create or update a BenchmarkJob, and rejects an unknown scheme or a malformed URI. Its message starts with `invalid storage: error parsing storage URI:`, followed by the reason. For an unknown scheme, the reason is `unknown storage type for URI: <uri>`. Each section below lists the reasons for its backend.
- Nothing checks that the bucket, container, repository or credentials work. genai-bench finds out when it writes.
- OME matches parameter keys exactly, case included. It ignores keys it doesn't know, and the keys of other backends, without a warning. Admission doesn't check them either.
- Admission also accepts well-formed `hf://`, `vendor://` and `local://` URIs, which name model sources elsewhere in OME, but OME can't write results to them. See [URIs OME can't write to](#uris-ome-cant-write-to).

## Credentials appear on the pod command line

OME has no Secret reference for storage credentials. It copies the value of each parameter it knows, as it is, into the arguments of the genai-bench container. These parameters hold credentials:

| Backend | Parameters that hold credentials |
| --- | --- |
| OCI Object Storage | `security_token` |
| AWS S3 | `aws_access_key_id`, `aws_secret_access_key` |
| Azure Blob Storage | `azure_account_key`, `azure_connection_string`, `azure_sas_token` |
| GitHub Releases | `github_token` |

A value you set is in three objects in the BenchmarkJob's namespace:

- the BenchmarkJob, in `spec.outputLocation.parameters`;
- its Job, in the container `args` of the pod template;
- the Job's pod, in the container `args`.

OME sets no `ttlSecondsAfterFinished` on the Job, so the Job and its pod keep the values after the benchmark ends, until you delete the BenchmarkJob.

Anyone who can read one of these objects can read the values:

- The built-in `view` ClusterRole reads pods and Jobs, although it leaves Secrets out.
- Since v1.3, the `ome-resources` chart adds read access to every ome.io resource to `view`, through its `ome-supplemental-viewer` ClusterRole, so `view` reads BenchmarkJobs too. On v1.2.2, `view` doesn't read BenchmarkJobs, but it still reads the Job and the pod.

To see what the pod spec holds, print the container arguments of the Job:

```bash
kubectl get job llama-3-2-1b-instruct-bench -n llama-demo \
  -o jsonpath='{.spec.template.spec.containers[0].args}'
```

It prints the arguments as a JSON array: the benchmark settings, then the storage arguments that the sections below list, with each parameter value after its flag.

To keep a credential out of the pod spec:

- Where a parameter takes a path, `config_file` for OCI Object Storage or `gcp_credentials_path` for Google Cloud Storage, mount a Secret as a file with the `volumes` and `volumeMounts` of [`podOverride`](../../concepts/serving/benchmarks.md#how-a-benchmark-runs), and pass the file's path. The [Google Cloud Storage](#google-cloud-storage) example does this. OME doesn't check that the file exists.
- Or leave the credential parameters out. OME then passes no credentials, and whether the upload works depends on what genai-bench finds in the pod. The pod runs as the `default` ServiceAccount of the BenchmarkJob's namespace, and `podOverride` has no field for another one: see [BenchmarkJob pods](../../getting-started/private-registries.md#benchmarkjob-pods).

## OCI Object Storage

```text
oci://n/<namespace>/b/<bucket>/o/<prefix>
```

`<namespace>` is your tenancy's Object Storage namespace, not a Kubernetes namespace. The prefix can span several path segments. OME passes:

- `--upload-results`;
- `--namespace <namespace>`, `--storage-bucket <bucket>` and `--storage-prefix <prefix>`, even when the prefix is empty.

Unlike for the other backends that upload, OME passes no `--storage-provider`.

| Parameter | genai-bench argument | Value |
| --- | --- | --- |
| `auth` | `--auth` | The authentication type, such as `instance_principal`, which OME's sample BenchmarkJobs use |
| `config_file` | `--config-file` | The path of an OCI config file in the pod |
| `profile` | `--profile` | A profile in the config file |
| `security_token` | `--security-token` | A security token. It's readable in the pod spec. |
| `region` | `--region` | A region, such as `eu-frankfurt-1` |

Admission rejects a URI that has fewer than six path segments after `oci://`, or doesn't have `n`, `b` and `o` as its first, third and fifth, with `invalid OCI storage URI format. Expected: oci://n/{namespace}/b/{bucket}/o/{object_path}`.

This `outputLocation` uploads to the bucket `benchmark-results`:

```yaml
spec:
  outputLocation:
    storageUri: oci://n/examplens/b/benchmark-results/o/llama-3-2-1b-instruct
    parameters:
      auth: instance_principal
      region: eu-frankfurt-1
```

OME appends these arguments to the `args` of the genai-bench container:

```text
--upload-results
--namespace examplens
--storage-bucket benchmark-results
--storage-prefix llama-3-2-1b-instruct
--auth instance_principal
--region eu-frankfurt-1
```

## AWS S3

```text
s3://<bucket>/<prefix>
s3://<bucket>@<region>/<prefix>
```

The prefix is optional, as in `s3://benchmark-results`. OME passes:

- `--upload-results` and `--storage-provider aws`;
- `--storage-bucket <bucket>`, and `--storage-prefix <prefix>` when the URI has a prefix;
- `--storage-aws-region`, from the `aws_region` parameter, or else from `@<region>`. With neither, OME passes no region.

OME reads the first `@` in the URI as the start of the region, wherever it is, so don't put `@` in the prefix: `s3://benchmark-results/runs@2026/llama` gives the bucket `benchmark-results/runs`, the region `2026` and the prefix `llama`.

| Parameter | genai-bench argument | Value |
| --- | --- | --- |
| `aws_access_key_id` | `--storage-aws-access-key-id` | An access key ID. It's readable in the pod spec. |
| `aws_secret_access_key` | `--storage-aws-secret-access-key` | The secret access key. It's readable in the pod spec. |
| `aws_profile` | `--storage-aws-profile` | A named AWS profile |
| `aws_region` | `--storage-aws-region` | A region. It wins over `@<region>` in the URI. |

Admission rejects `s3://` with `invalid S3 storage URI format: missing bucket name`, and an empty bucket, as in `s3:///llama-3-2-1b-instruct`, with `invalid S3 storage URI format: bucket name cannot be empty`.

This `outputLocation` uploads to the bucket `benchmark-results` in `us-west-2`:

```yaml
spec:
  outputLocation:
    storageUri: s3://benchmark-results/llama-3-2-1b-instruct
    parameters:
      aws_region: us-west-2
```

OME appends these arguments to the `args` of the genai-bench container:

```text
--upload-results
--storage-provider aws
--storage-bucket benchmark-results
--storage-prefix llama-3-2-1b-instruct
--storage-aws-region us-west-2
```

## Azure Blob Storage

```text
az://<account>/<container>/<path>
az://<account>.blob.core.windows.net/<container>/<path>
```

The two forms name the same location, and the path is optional. OME passes:

- `--upload-results` and `--storage-provider azure`;
- `--storage-bucket <container>`, and `--storage-prefix <path>` when the URI has a path;
- always `--storage-azure-account-name`, from the `azure_account_name` parameter, or else from the URI.

| Parameter | genai-bench argument | Value |
| --- | --- | --- |
| `azure_account_name` | `--storage-azure-account-name` | The storage account. It wins over the account in the URI. |
| `azure_account_key` | `--storage-azure-account-key` | An account key. It's readable in the pod spec. |
| `azure_connection_string` | `--storage-azure-connection-string` | A connection string. It's readable in the pod spec. |
| `azure_sas_token` | `--storage-azure-sas-token` | A SAS token. It's readable in the pod spec. |

Admission rejects:

- `az://`, with `invalid Azure storage URI format: missing account name`;
- a URI without a container, as in `az://benchmarkstore`, with `invalid Azure storage URI format: missing container name`;
- an empty account or container, as in `az://benchmarkstore//llama-3-2-1b-instruct`, with `invalid Azure storage URI format: account name and container name are required`.

This `outputLocation` uploads to the container `benchmark-results` of the storage account `benchmarkstore`:

```yaml
spec:
  outputLocation:
    storageUri: az://benchmarkstore/benchmark-results/llama-3-2-1b-instruct
```

OME appends these arguments to the `args` of the genai-bench container:

```text
--upload-results
--storage-provider azure
--storage-bucket benchmark-results
--storage-prefix llama-3-2-1b-instruct
--storage-azure-account-name benchmarkstore
```

## Google Cloud Storage

```text
gs://<bucket>/<path>
```

The path is optional. OME passes `--upload-results`, `--storage-provider gcp` and `--storage-bucket <bucket>`, and `--storage-prefix <path>` when the URI has a path.

| Parameter | genai-bench argument | Value |
| --- | --- | --- |
| `gcp_project_id` | `--storage-gcp-project-id` | The Google Cloud project |
| `gcp_credentials_path` | `--storage-gcp-credentials-path` | The path of a service account key file in the pod. OME doesn't mount the file. |

Admission rejects `gs://` with `invalid GCS storage URI format: missing bucket name`, and an empty bucket, as in `gs:///llama-3-2-1b-instruct`, with `invalid GCS storage URI format: bucket name cannot be empty`.

This BenchmarkJob uploads to the bucket `benchmark-results`. It mounts a service account key from the `key.json` key of the Secret `gcs-benchmark-writer`, and passes only the key's path:

```yaml title="benchmark-gcs.yaml"
apiVersion: ome.io/v1beta1
kind: BenchmarkJob
metadata:
  name: llama-3-2-1b-instruct-bench
  namespace: llama-demo
spec:
  endpoint:
    inferenceService:
      name: llama-3-2-1b-instruct
      namespace: llama-demo
  task: text-to-text
  maxTimePerIteration: 10
  maxRequestsPerIteration: 200
  outputLocation:
    storageUri: gs://benchmark-results/llama-3-2-1b-instruct
    parameters:
      gcp_project_id: llama-demo-project
      gcp_credentials_path: /var/run/secrets/gcs/key.json
  podOverride:
    volumes:
      - name: gcs-credentials
        secret:
          secretName: gcs-benchmark-writer
    volumeMounts:
      - name: gcs-credentials
        mountPath: /var/run/secrets/gcs
        readOnly: true
```

OME appends these arguments to the `args` of the genai-bench container:

```text
--upload-results
--storage-provider gcp
--storage-bucket benchmark-results
--storage-prefix llama-3-2-1b-instruct
--storage-gcp-project-id llama-demo-project
--storage-gcp-credentials-path /var/run/secrets/gcs/key.json
```

## GitHub Releases

```text
github://<owner>/<repository>
github://<owner>/<repository>@<tag>
```

The tag names the release. OME passes:

- `--upload-results` and `--storage-provider github`;
- `--github-owner <owner>` and `--github-repo <repository>`;
- `--github-tag <tag>`, unless the tag is `latest`. A URI without a tag counts as `latest`, so for both, OME passes no tag and leaves the choice of release to genai-bench.

| Parameter | genai-bench argument | Value |
| --- | --- | --- |
| `github_token` | `--github-token` | A GitHub token with write access to the repository. It's readable in the pod spec. |

Admission rejects:

- `github://`, with `invalid GitHub storage URI format: missing owner/repository`;
- a URI without a slash, as in `github://acme`, with `invalid GitHub storage URI format: expected owner/repository`;
- an empty owner or repository, as in `github://acme/`, with `invalid GitHub storage URI format: owner and repository are required`.

This `outputLocation` uploads to the release `llama-3-2-1b-instruct` of the repository `acme/benchmarks`:

```yaml
spec:
  outputLocation:
    storageUri: github://acme/benchmarks@llama-3-2-1b-instruct
```

OME appends these arguments to the `args` of the genai-bench container:

```text
--upload-results
--storage-provider github
--github-owner acme
--github-repo benchmarks
--github-tag llama-3-2-1b-instruct
```

With `github_token` set, OME also appends `--github-token` and the token.

## Persistent volume claims

```text
pvc://<claim>/<sub-path>
```

For a claim, OME:

- looks up the claim in the BenchmarkJob's namespace before it creates the Job;
- adds a volume named `benchmark-output-storage` for the claim, and mounts the claim's `<sub-path>` directory at `/<sub-path>` in the genai-bench container;
- passes `--experiment-base-dir /<sub-path>` and no `--upload-results`, so genai-bench writes the results to the claim, and nothing is uploaded;
- ignores `parameters`.

The parser also accepts `pvc://<namespace>:<claim>/<sub-path>`, but OME ignores the namespace, since a pod can mount only claims in its own namespace.

The kubelet creates the sub-path directory in the claim when it's missing. The mount hides whatever the image has at `/<sub-path>`, so pick a sub-path that doesn't name one of the image's directories, such as `usr`. A `podOverride` volume named `benchmark-output-storage`, or a `podOverride` mount at `/<sub-path>`, overrides OME's.

This `outputLocation`, from the example on the [Benchmarks](../../concepts/serving/benchmarks.md#the-benchmarkjob-resource) page, writes to the `llama-3-2-1b-instruct` directory of the claim `benchmark-results`:

```yaml
spec:
  outputLocation:
    storageUri: pvc://benchmark-results/llama-3-2-1b-instruct
```

OME mounts that directory at `/llama-3-2-1b-instruct`, and appends this argument to the `args` of the genai-bench container:

```text
--experiment-base-dir /llama-3-2-1b-instruct
```

These URIs are rejected:

| URI | Rejected by | Message |
| --- | --- | --- |
| `pvc://` | Admission | `invalid PVC storage URI format: missing content after prefix` |
| `pvc://benchmark-results` or `pvc://benchmark-results/` | Admission | `invalid PVC storage URI format: missing subpath` |
| `pvc:///llama-3-2-1b-instruct` | Admission | `invalid PVC storage URI format: missing PVC name` |
| `pvc://benchmark-results/runs/../llama-3-2-1b-instruct` | Admission. Since v1.3. | `invalid PVC storage URI format: subpath must not contain '..' segments` |
| `pvc://benchmark-results//llama-3-2-1b-instruct` | The API server, when OME creates the Job | `must be a relative path` |
| `pvc://llama-demo:/llama-3-2-1b-instruct`, or another malformed `<namespace>:` part | Admission | `empty PVC name after colon`, `empty namespace before colon`, `multiple colons not allowed in namespace:pvc-name` or `invalid namespace "<namespace>" (must be lowercase alphanumeric with hyphens, max 63 chars)`, after `invalid PVC storage URI format:` |

Since v1.3, admission rejects a sub-path with a `..` segment. On v1.2.2, admission accepts it, the API server rejects the Job with `must not contain '..'`, and the BenchmarkJob stays `Pending`, as for the sub-path that starts with `/` in [URIs OME can't write to](#uris-ome-cant-write-to).

## URIs OME can't write to

Admission accepts these URIs, but OME can't create a Job for them:

| URI | What the controller logs |
| --- | --- |
| A well-formed `hf://`, `vendor://` or `local://` URI | `failed to build storage args: unsupported storage type: HUGGINGFACE`, with `VENDOR` or `LOCAL` for the other two |
| `pvc://` with a claim that isn't in the BenchmarkJob's namespace | `PVC <claim> not found`, followed by the API error |
| `pvc://` with a sub-path that starts with `/` | `failed to reconcile benchmark job:`, followed by the API server's error, which ends with `must be a relative path` |

In each case:

- OME creates no Job, and the BenchmarkJob stays `Pending`. The status doesn't say why, and OME records no event.
- Each attempt logs a `Reconciler error` line with the error, and OME tries again after a growing delay. [Status](../../concepts/serving/benchmarks.md#status) on the Benchmarks page shows how to read the logs of every controller replica.
- For an [InferenceService](../../concepts/serving/inference-services.md) endpoint, OME builds the Job only once the InferenceService is `Ready`, so the error appears after that.

While no Job exists, you can fix `outputLocation` in place, or create the missing claim, and OME's next attempt uses it. An edit to the BenchmarkJob starts an attempt right away. Once the Job exists, OME doesn't update it. To apply an edited `outputLocation`, delete the Job, as on the [Benchmarks](../../concepts/serving/benchmarks.md#how-a-benchmark-runs) page: OME creates a new one from the current spec.

A URI that OME accepts can still point at storage that genai-bench can't write to, such as a bucket that doesn't exist, credentials without access, or an endpoint the pod can't reach. OME checks none of these. genai-bench's errors are in the pod's logs, which `kubectl logs job/llama-3-2-1b-instruct-bench -n llama-demo` prints. If genai-bench exits with an error, the Job fails without a retry, and the BenchmarkJob's state becomes `Failed`.

## Related pages

- [Where results go](../../concepts/serving/benchmarks.md#where-results-go): how a BenchmarkJob uses `outputLocation`, and the `resultFolderName` that names the results folder.
- [How a benchmark runs](../../concepts/serving/benchmarks.md#how-a-benchmark-runs): the Job and pod that OME creates, and what `podOverride` changes.
- [Run benchmarks](../../guides/deploy-models/run-benchmarks.md): run a BenchmarkJob against an InferenceService, and collect its results.
- [Serve models from node-local storage](../../guides/deploy-models/serve-models-from-local-storage.md): `local://` URIs, which name model sources, not benchmark output.
- [OME API](../api/ome.v1beta1.md#ome-io-v1beta1-StorageSpec): the `StorageSpec` type.
