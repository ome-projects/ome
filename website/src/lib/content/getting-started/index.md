---
title: Getting Started
description: "Start here if you are new to OME: install the operator, serve your first model, and add pre-configured models and runtimes."
---

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
