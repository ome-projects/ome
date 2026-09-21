# OME (Open Model Engine) — Kubernetes Operator for LLM Serving

[![codecov](https://codecov.io/gh/ome-projects/ome/graph/badge.svg)](https://codecov.io/gh/ome-projects/ome)
[![Latest Release](https://img.shields.io/github/v/release/ome-projects/ome?include_prereleases)](https://github.com/ome-projects/ome/releases/latest)
[![API Reference](https://img.shields.io/badge/API-v1beta1-blue)](https://ome-projects.github.io/ome/docs/reference/ome.v1beta1/)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/ome-projects/ome)

<div align="center">
  <a href="https://github.com/ome-projects/ome">
    <img src="site/assets/icons/logo-clear-background.png" alt="OME Logo" width="300" height="auto" style="max-width: 100%;">
  </a>
</div>

## What is OME?
OME (Open Model Engine) is a Kubernetes operator for enterprise-grade management and serving of Large Language Models (LLMs). It optimizes the deployment and operation of LLMs by automating model management, intelligent runtime selection, efficient resource utilization, and sophisticated deployment patterns.

Read the [documentation](https://ome-projects.github.io/ome/docs/) to learn more about OME capabilities and features.

## Features Overview

- **Model Management:** Models are first-class citizen custom resources in OME. Sophisticated model parsing extracts architecture, parameter count, and capabilities directly from model files. Supports distributed storage with automated repair, double encryption, namespace scoping, and multiple formats (SafeTensors, PyTorch, TensorRT, ONNX). See the [supported models reference](config/models/SUPPORTED_MODELS.md) for the comprehensive catalog of 200+ pre-configured models, including the Llama, Qwen, DeepSeek, Gemma, and Phi families.

- **Intelligent Runtime Selection:** Automatic matching of models to optimal runtime configurations through weighted scoring based on architecture, format, quantization, parameter size, and framework compatibility.

- **Optimized Deployments:** Supports multiple deployment patterns including prefill-decode disaggregation, multi-node inference, and traditional Kubernetes deployments, with canary and blue-green rollout strategies and advanced scaling controls.

- **Resource Optimization:** Specialized GPU bin-packing scheduling with dynamic re-optimization to maximize cluster efficiency while ensuring high availability.

- **Runtime Integrations:** First-class support for [**SGLang**](https://github.com/sgl-project/sglang) - the most advanced inference engine with cache-aware load balancing, multi-node deployment, prefill-decode disaggregated serving, multi-LoRA adapter serving, and much more. Also supports [**vLLM**](https://github.com/vllm-project/vllm) for high-throughput inference.

- **Accelerator Management:** Hardware-aware scheduling through AcceleratorClass resources that define GPU capabilities, discovery patterns, and cost information. Enables intelligent accelerator selection with policies like BestFit, Cheapest, or MostCapable.

- **Web Console:** Modern web interface for managing models, serving runtimes, and inference services with real-time updates and HuggingFace model search integration. Developed separately in [ome-projects/ome-console](https://github.com/ome-projects/ome-console).

- **Kubernetes Ecosystem Integration:** Deep integration with modern Kubernetes components including [Kueue](https://kueue.sigs.k8s.io/) for gang scheduling of multi-pod workloads, [LeaderWorkerSet](https://github.com/kubernetes-sigs/lws) for resilient multi-node deployments, [KEDA](https://keda.sh/) for advanced custom metrics-based autoscaling, [K8s Gateway API](https://gateway-api.sigs.k8s.io/) for sophisticated traffic routing, and [Gateway API Inference Extension](https://gateway-api-inference-extension.sigs.k8s.io/) for standardized inference endpoints.

- **Automated Benchmarking:** Built-in performance evaluation through the BenchmarkJob custom resource, supporting configurable traffic patterns, concurrent load testing, and comprehensive result storage. Enables systematic performance comparison across models and service configurations.

## Production Readiness Status

- ✅ API version: v1beta1
- ✅ Comprehensive [documentation](https://ome-projects.github.io/ome/docs/)
- ✅ Unit and integration test coverage
- ✅ Production deployments with large-scale LLM workloads
- ✅ Monitoring via standard metrics and Kubernetes events
- ✅ Security: RBAC-based access control and model encryption
- ✅ High availability mode with redundant model storage

## Installation

**Requires Kubernetes 1.28 or newer**

### Option 1: OCI Registry (Recommended)

Install OME directly from the OCI registry:

```bash
# Install OME CRDs
helm upgrade --install ome-crd oci://ghcr.io/moirai-internal/charts/ome-crd --namespace ome --create-namespace

# Install OME resources
helm upgrade --install ome oci://ghcr.io/moirai-internal/charts/ome-resources --namespace ome
```

Moving an existing manifest install to these charts? See [Move a manifest install to the Helm charts](https://ome-projects.github.io/ome/docs/installation/#move-a-manifest-install-to-the-helm-charts) first.

### Option 2: Install from Source

For development or customization:

```bash
# Clone the repository
git clone https://github.com/ome-projects/ome.git
cd ome

# Install from local charts
helm install ome-crd charts/ome-crd --namespace ome --create-namespace
helm install ome charts/ome-resources --namespace ome
```

### Optional: Serving Resources

The `ome-serving` chart deploys a set of pre-configured ClusterBaseModels, ClusterServingRuntimes, and InferenceServices on top of the core installation:

```bash
helm upgrade --install ome-serving oci://ghcr.io/moirai-internal/charts/ome-serving --namespace ome
```

Read the [installation guide](https://ome-projects.github.io/ome/docs/installation/) for more options and advanced configurations.

Learn more about:
- OME [concepts](https://ome-projects.github.io/ome/docs/concepts/)
- Common [tasks](https://ome-projects.github.io/ome/docs/tasks/)

## Architecture

OME uses a component-based architecture built on Kubernetes custom resources:

- **BaseModel/ClusterBaseModel:** Define model sources and metadata with automatic parsing of architecture, parameters, and capabilities
- **FineTunedWeight:** Define LoRA adapters and fine-tuned weights that extend base models
- **ServingRuntime/ClusterServingRuntime:** Define how models are served with runtime-specific configurations
- **InferenceService:** Connects models to runtimes for deployment with support for prefill-decode disaggregation and multi-node inference
- **AcceleratorClass:** Define GPU hardware classes with capabilities, discovery patterns, and cost information for intelligent scheduling
- **BenchmarkJob:** Measures model performance under different workloads with configurable traffic patterns

OME's controller automatically:
1. Downloads and parses models to understand their characteristics
2. Selects the optimal runtime configuration for each model
3. Matches models to appropriate accelerators based on requirements
4. Generates Kubernetes resources for efficient deployment
5. Continuously optimizes resource utilization across the cluster

## Roadmap

High-level overview of the main priorities:

- Unified multi-cluster workload management (APIs merged; reconciliation in development)
- Multi-cloud model storage and authentication
- PVC-backed model storage
- KV cache pooling
- Model Context Protocol (MCP) gateway support
- Accelerator-aware runtime selection for heterogeneous GPU clusters
- A kubectl plugin for managing OME resources from the command line

## Community and Support

- [GitHub Issues](https://github.com/ome-projects/ome/issues) for bug reports and feature requests
- [Documentation](https://ome-projects.github.io/ome/docs/) for guides and reference

## License

OME is licensed under the [Apache License 2.0](LICENSE).
