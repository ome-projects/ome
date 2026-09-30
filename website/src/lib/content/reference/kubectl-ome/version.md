---
title: kubectl ome version
description: Print the kubectl-ome plugin version and the OME version that the cluster's manager Deployment declares.
since: v1.3
---

`kubectl ome version` prints the version of your kubectl-ome plugin, and the version of OME that the [OME manager](../../concepts/architecture/how-ome-works.md#components) Deployment in your cluster declares. Use it to check which build of the plugin you have, and which release of OME the cluster is set to run. To compare the two, run [`kubectl ome admin doctor`](admin.md#doctor).

```text
kubectl ome version [flags]
```

`version` needs `get` on Deployments in the OME namespace. See [Required RBAC](overview.md#required-rbac).

## Flags

| Flag | Default | Description |
| --- | --- | --- |
| `--ome-namespace` | `ome` | Namespace where the OME control plane is installed |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

When OME runs in another namespace, set `--ome-namespace`. `-n` doesn't change the namespace `version` reads.

## Output fields

On a terminal, `version` prints a table with the columns COMPONENT and VERSION, and a row each for Client and Operator. In a pipe or a file, it prints a `Client Version:` line and an `Operator Version:` line instead. It has no `-o` flag, so scripts read these lines. On a narrow terminal, long values wrap, as [Output formats](overview.md#output-formats) describes.

### Client

Client is `<version> (commit <commit>)`, stamped into the plugin when it was built. The commit is the full hash. The version depends on how you got the plugin:

| Build | Version | Example |
| --- | --- | --- |
| A release archive, which krew installs, or `make kubectl-ome` at a release tag | The tag | `v1.3.0` |
| `make kubectl-ome` in a shallow clone of `main`, as in [Build from source](overview.md#build-from-source) | The short commit hash | `6502319` |
| `make kubectl-ome` in a full clone of `main` | The last tag, the number of commits since it, and `g` plus the short hash | `v1.2.2-376-g6502319d` |
| `make kubectl-ome` with uncommitted changes to tracked files | The version with `-dirty` | `v1.3.0-dirty` |
| `go build` or `go install` | `unknown`, and the commit is `unknown` too. Build with `make kubectl-ome` to stamp a version. | `unknown` |

### Operator

Operator is the image tag of the `manager` container in the `ome-controller-manager` Deployment, such as `v1.3.0` for `ghcr.io/moirai-internal/ome-manager:v1.3.0`. For an image pinned by digest, it's `<tag>@<digest>`, or the digest alone when the image has no tag. `version` shows any tag as written, even `latest`.

Operator shows the image the Deployment declares, not the one its running pods have. During an upgrade, or when new pods can't start, the two can differ. To check that the pods run the declared image, wait for the Deployment's rollout:

```bash
kubectl rollout status deployment/ome-controller-manager -n ome
```

```output
deployment "ome-controller-manager" successfully rolled out
```

### When Operator is unknown

When `version` can't read the operator version, Operator is `unknown`, with the reason in parentheses, and the command still exits `0`:

| Operator | When |
| --- | --- |
| `unknown (get deployments.apps ome/ome-controller-manager: <reason>)` | The CLI couldn't read the Deployment. The next table lists the reasons. |
| `unknown (resolve kubernetes-client: Unavailable)` | The CLI couldn't use your kubeconfig, such as for an unknown `--context`. |
| `unknown (manager container unavailable)` | The pod template has more than one container, and none is named `manager`. |
| `unknown (image has no tag or digest)` | The image names no tag or digest. |

`<reason>` is one of these:

| Reason | Cause |
| --- | --- |
| `NotFound` | The namespace has no `ome-controller-manager` Deployment. Set `--ome-namespace` to OME's namespace, or install OME. |
| `Forbidden` | You may not `get` Deployments in the namespace. |
| `Unauthorized` | The API server rejected your credentials, such as an expired token. |
| `Unavailable` | The CLI couldn't reach the API server. |
| `TimedOut` | The read took longer than `--request-timeout`, or the API server or a proxy timed out. |
| `Canceled` | You pressed Ctrl-C during the read. |
| `TooManyRequests`, `ServiceUnavailable`, `ServerError` | The server answered 429, 503 or another 5xx error. |
| `OMEAPIMissing (install OME first)` | A proxy, or a server that isn't the Kubernetes API, answered 404. Despite the hint, check the server in your kubeconfig context. |
| `MalformedResponse` | The CLI couldn't use the server's answer. |
| `Invalid`, `Conflict`, `UnsupportedAPI`, `Expired`, `ResponseTooLarge` | An unusual answer for this read, most likely from a proxy. |

When the text in parentheses would get too long, and always for `OMEAPIMissing`, the CLI leaves out the namespace and name, as in `unknown (get deployments.apps: NotFound)`.

## Examples

Show the versions, with the plugin from the v1.3.0 release and OME v1.3.0 installed:

```bash
kubectl ome version
```

```output
COMPONENT   VERSION
Client      v1.3.0 (commit 9c41e7a2d85b3f60e1a7c4d29b8f305e6d7a1c42)
Operator    v1.3.0
```

When OME runs in `ome-system`, the default read finds no Deployment, and Operator is `unknown (get deployments.apps ome/ome-controller-manager: NotFound)`. Set `--ome-namespace`:

```bash
kubectl ome version --ome-namespace ome-system
```

```output
COMPONENT   VERSION
Client      v1.3.0 (commit 9c41e7a2d85b3f60e1a7c4d29b8f305e6d7a1c42)
Operator    v1.3.0
```

Save the versions to a file, with a plugin built from a shallow clone of `main`:

```bash
kubectl ome version | tee versions.txt
```

```output
Client Version: 6502319 (commit 6502319dfe243a9d379178ef5ff215a8731a7c68)
Operator Version: v1.3.0
```

Client is the short commit hash, since the shallow clone has no tags.

## Exit codes

| Code | Meaning | Returned by |
| --- | --- | --- |
| `0` | Success | `version`, whenever it prints both values, even when Operator is `unknown`. |
| `1` | General error | `version`, for an argument or a bad flag, such as `-o`, or when it can't write its output. |

Errors print to stderr as `error: <message>`.

## Related guides

- [Troubleshoot an InferenceService](../../guides/troubleshoot/troubleshoot-an-inferenceservice.md)
