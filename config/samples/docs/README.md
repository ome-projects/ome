# Documentation workflow fixtures

These examples target the public OME implementation at `bc1f94db`
(v1.3 development), with matching controller, charts and CRDs. They are not
compatible with the v1.2.2 controller.

| Directory | Workflow | Requirements |
| --- | --- | --- |
| `omenative-http/` | HTTP request, scale, update and readiness-failure recovery | CPU workers; no model or GPU |
| `runtime-managed-qwen/` | SGLang downloads Qwen3-0.6B and answers a chat request | AMD64 NVIDIA GPU, compatible CUDA driver, image/model egress |
| `standalone-http/` | Directly owned InferenceReplica, Service, scale and update; optional runtime reference | CPU workers; no InferenceService, model or GPU |
| `multi-cluster-registration/` | Dedicated member credential, registration and one CPU placement | Two disposable clusters; trusted control plane; alpha/in development |

The corresponding website guides contain prerequisites, the complete YAML,
commands, observations, recovery and cleanup:

- `website/src/lib/content/guides/omenative/learn-omenative.md`
- `website/src/lib/content/guides/deploy-models/deploy-an-inferenceservice.md`
- `website/src/lib/content/guides/omenative/run-a-standalone-replica.md`
- `website/src/lib/content/guides/omenative/recover-a-failed-http-workload.md`
- `website/src/lib/content/guides/multi-cluster/register-a-workload-cluster.md`

Follow each guide's application order; do not apply a whole directory of
alternative examples or failure patches. Do not use an ambient production
context. Read the guide before applying;
the Qwen example reserves 10 CPUs, 30 GiB of memory and one GPU.

From the repository root, run the cluster-free fixture tests:

```bash
go test ./hack/docs-examples -run 'Test(RuntimeOnly|Standalone|Recovery)Documentation'
```

They exercise public admission validators, runtime resolution, supported
patches, standalone ownership constraints, and the recovery exercise's
readiness-failure disposition. The registration runtime and source spec are
checked, but no remote registration or placement is executed. Website
`pnpm test` checks that the copyable YAML matches these fixtures, while
`make docs-examples` uses an isolated envtest API server to check the docs'
complete objects against CRD schemas.

These checks do not pull or run the images. Image availability, model
download, GPU compatibility, actual readiness and HTTP behavior must also
be verified in a disposable environment before calling a workflow
end-to-end validated. The Qwen image tag is pinned, but its default model
revision is mutable; see the staging guide for controlled artifact storage.
