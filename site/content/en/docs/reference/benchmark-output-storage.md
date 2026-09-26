---
title: Benchmark Output Storage
linkTitle: Benchmark Output Storage
weight: 3
description: >
  Reference for the storage backends a BenchmarkJob's spec.outputLocation can
  target — the storageUri format and the parameters keys each backend accepts.
---

Every [BenchmarkJob](/ome/docs/concepts/benchmark/) must declare where its
results go: `spec.outputLocation` is a **required** field of type
[`StorageSpec`](/ome/docs/reference/ome.v1beta1/#ome-io-v1beta1-StorageSpec).
For benchmark output, only two of its fields are consulted:

- `storageUri` (required) — a URI whose scheme selects the storage backend.
- `parameters` (optional) — a string map of backend-specific authentication
  and configuration keys, listed per backend below.

The controller translates these two fields into command-line flags on the
[genai-bench](https://docs.sglang.ai/genai-bench/) container it launches. Six
backends are supported:

| Backend | URI format |
|---------|------------|
| [OCI Object Storage](#oci-object-storage) | `oci://n/{namespace}/b/{bucket}/o/{prefix}` |
| [AWS S3](#aws-s3) | `s3://{bucket}[@{region}]/{prefix}` |
| [Azure Blob Storage](#azure-blob-storage) | `az://{account}/{container}/{path}` |
| [Google Cloud Storage](#google-cloud-storage) | `gs://{bucket}/{object-path}` |
| [GitHub Releases](#github-releases) | `github://{owner}/{repo}[@{tag}]` |
| [PVC](#persistent-volume-claim-pvc) | `pvc://{pvc-name}/{sub-path}` |

The five remote backends make genai-bench **upload** results after each run
(`--upload-results`). The PVC backend is different: nothing is uploaded —
the claim is mounted into the benchmark pod and genai-bench writes its
experiment folders there directly.

Any other scheme — including URIs that are valid elsewhere in OME, such as
`hf://`, `vendor://`, or `local://` — is rejected when the controller builds
the benchmark Job: reconciliation fails with `unsupported storage type` and
no Job is created. Malformed URIs of a supported scheme fail the same way.
Parameter keys not listed below are silently ignored.

## Credentials appear on the pod command line

Every `parameters` value is passed **verbatim as a command-line argument** to
the genai-bench container. Anyone who can read the benchmark pod (or the Job
that owns it) can see them:

```shell
kubectl get pod -l batch.kubernetes.io/job-name=<benchmark-name> -o yaml
```

Prefer authentication that needs no inline secret — OCI instance principals,
AWS IAM roles or profiles, Azure AD, GCP application default credentials — and
reserve keys like `aws_secret_access_key`, `azure_account_key`, and
`github_token` for clusters where pod specs are adequately protected.

## OCI Object Storage

```yaml
outputLocation:
  storageUri: "oci://n/my-namespace/b/my-bucket/o/benchmark-results"
  parameters:
    auth: "instance_principal"
```

The URI must contain all three markers (`n/`, `b/`, `o/`); the object prefix
may span multiple path segments. Namespace, bucket, and prefix are passed to
genai-bench as `--namespace`, `--storage-bucket`, and `--storage-prefix`.

| Parameter | genai-bench flag | Purpose |
|-----------|------------------|---------|
| `auth` | `--auth` | Authentication mode: `user_principal`, `instance_principal`, `security_token`, or `instance_obo_user` |
| `config_file` | `--config-file` | OCI config file path, for `user_principal` |
| `profile` | `--profile` | Profile name within the config file |
| `security_token` | `--security-token` | Token, for `security_token` auth |
| `region` | `--region` | Region, for `security_token` auth |

## AWS S3

```yaml
outputLocation:
  storageUri: "s3://my-bucket@us-west-2/benchmarks/2025"
  parameters:
    aws_profile: "production"
```

The bucket may carry an optional `@{region}` suffix. The bucket is passed as
`--storage-bucket`; the path, if present, as `--storage-prefix`.

| Parameter | genai-bench flag | Purpose |
|-----------|------------------|---------|
| `aws_access_key_id` | `--storage-aws-access-key-id` | Static access key |
| `aws_secret_access_key` | `--storage-aws-secret-access-key` | Static secret key |
| `aws_profile` | `--storage-aws-profile` | Named AWS profile |
| `aws_region` | `--storage-aws-region` | Region; **takes precedence** over the URI's `@{region}` suffix |

If neither `aws_region` nor an `@{region}` suffix is given, no region flag is
passed. With no credential parameters at all, genai-bench falls back to the
standard AWS credential chain (environment variables, IAM role) inside the
benchmark pod.

## Azure Blob Storage

```yaml
outputLocation:
  storageUri: "az://myaccount/mycontainer/benchmarks"
  parameters:
    azure_account_key: "..."
```

Two URI forms are accepted: `az://{account}/{container}/{path}` and the full
endpoint form `az://{account}.blob.core.windows.net/{container}/{path}`. The
account and container are both required; the blob path is optional. The
container is passed as `--storage-bucket` and the path as `--storage-prefix`.

The account name is always passed (`--storage-azure-account-name`), taken from
the `azure_account_name` parameter when set, otherwise from the URI.

| Parameter | genai-bench flag | Purpose |
|-----------|------------------|---------|
| `azure_account_name` | `--storage-azure-account-name` | Overrides the account name from the URI |
| `azure_account_key` | `--storage-azure-account-key` | Storage account key |
| `azure_connection_string` | `--storage-azure-connection-string` | Connection string |
| `azure_sas_token` | `--storage-azure-sas-token` | SAS token |

With no credential parameters, genai-bench falls back to Azure default
credentials available in the benchmark pod.

## Google Cloud Storage

```yaml
outputLocation:
  storageUri: "gs://my-bucket/benchmarks"
  parameters:
    gcp_project_id: "my-project-123"
    gcp_credentials_path: "/secrets/sa.json"
```

The bucket is passed as `--storage-bucket`; the object path, if present, as
`--storage-prefix`.

| Parameter | genai-bench flag | Purpose |
|-----------|------------------|---------|
| `gcp_project_id` | `--storage-gcp-project-id` | GCP project |
| `gcp_credentials_path` | `--storage-gcp-credentials-path` | Path to a service-account JSON file **inside the benchmark pod** |

`gcp_credentials_path` is not resolved by OME — mount the service-account key
into the pod yourself via `spec.podOverride.volumes` and
`spec.podOverride.volumeMounts`. Without parameters, genai-bench uses
application default credentials.

## GitHub Releases

```yaml
outputLocation:
  storageUri: "github://myorg/myrepo@v1.0.0"
  parameters:
    github_token: "ghp_..."
```

Results are uploaded to a release of the given repository. Both owner and
repository are required (`--github-owner`, `--github-repo`). The `@{tag}`
suffix selects the release; when omitted it defaults to `latest`, in which
case no `--github-tag` flag is passed (the same applies to an explicit
`@latest`).

| Parameter | genai-bench flag | Purpose |
|-----------|------------------|---------|
| `github_token` | `--github-token` | Personal access token with write access to the repository |

## Persistent Volume Claim (PVC)

```yaml
outputLocation:
  storageUri: "pvc://benchmark-results-pvc/my-benchmark"
```

`pvc://{pvc-name}/{sub-path}` writes results to an **existing**
PersistentVolumeClaim instead of uploading them:

- The PVC must already exist **in the BenchmarkJob's namespace**; OME does not
  create it. If the claim is missing, reconciliation fails with
  `PVC {name} not found` and no Job is created.
- The controller mounts the claim's `{sub-path}` directory at `/{sub-path}`
  in the benchmark container (as a volume named `benchmark-output-storage`)
  and passes `--experiment-base-dir /{sub-path}`, so genai-bench writes its
  experiment folders straight to the volume.
- The sub-path is required (`pvc://my-pvc` alone is invalid) and must not
  contain `..` segments.
- `parameters` is ignored for this backend.

The parser also accepts a namespaced form, `pvc://{namespace}:{pvc-name}/{sub-path}`,
used by model storage URIs — but for benchmark output the namespace component
is **ignored**: the claim is always looked up in the BenchmarkJob's own
namespace. Don't rely on it to reach a PVC in another namespace.
