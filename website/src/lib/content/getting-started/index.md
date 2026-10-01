---
title: Getting Started
description: "Start here if you are new to OME: install the operator, serve your first model, and add pre-configured models and runtimes."
---

Choose a first workflow that matches your installation and available hardware:

| Workflow | OME version | What you need |
| --- | --- | --- |
| [Model-agent-managed Qwen](serve-your-first-model.md) | Pinned v1.2.2 release | One NVIDIA GPU, model-agent capacity and node-local storage |
| [Runtime-managed Qwen](../guides/deploy-models/deploy-an-inferenceservice.md) | v1.3 development source | One NVIDIA GPU; no model agent or pre-staged weights |
| [CPU-only OMENative lab](../guides/omenative/learn-omenative.md) | v1.3 development source | CPU workers; no GPU or model download; HTTP echo, not inference |

For retained model storage, [stage weights onto a PVC](../guides/deploy-models/stage-model-weights.md) before registering and serving them. Do not mix released controllers with development CRDs or manifests.

<div class="grid cards" markdown>

-   **[Introduction](introduction.md)**

    OME is a Kubernetes operator for serving large language models: it manages models and runtimes as resources, matches them, and runs the workloads.

-   **[Install OME](install.md)**

    Install cert-manager, then the ome-crd and ome-resources Helm charts, and check that the OME controller and the optional model agent are running.

-   **[Serve your first model](serve-your-first-model.md)**

    Create a ClusterBaseModel, a ClusterServingRuntime and an InferenceService for a small model, then send your first request to it.

-   **[Pre-configured models and runtimes](pre-configured-models.md)**

    Install the ome-serving Helm chart to deploy ready-made ClusterBaseModels, SGLang runtimes and InferenceServices from OME's model catalog.

-   **[Install from a private registry](private-registries.md)**

    Run OME from your own or a mirrored registry: copy its images, set global.hub, and give pods pull credentials with imagePullSecrets.

</div>

## Next steps

- To learn how OME's resources fit together, read [Concepts](../concepts/index.md).
- To deploy models, roll out changes and operate OME, follow the [Guides](../guides/index.md).
- To look up a field, a command or a matching rule, see the [Reference](../reference/index.md).
- To contribute code or docs, see [Contributing](../contributing/index.md).
