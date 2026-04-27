# Hugging Face Model Configuration Parser

This package provides Go utilities for loading, parsing, and analyzing Hugging Face model configurations. It allows you to extract important information from model configuration files without needing to know the specific model type in advance.

## Features

- **Automatic model detection:**
  Detects and loads configuration for any supported model type.
- **Model metadata extraction:**
  Extracts key information, including:
  - Parameter count
  - Context window size
  - Model architecture
  - Vision capabilities (for multimodal models)
- **Extensible architecture:**
  Easy to add support for new model families.
- **Accurate parameter counting:**
  Handles complex cases such as Mixture of Experts (MoE) and multi-file safetensors models.
- **Comprehensive test coverage:**
  Unit tests and real-world test data for all supported models.

## Supported Model Families

### Language Models
- **LLaMA Family**: Llama-3, Llama-3.1, Llama-4, Maverick, Scout, SmolLM
- **Mistral Family**: Mistral, Mixtral (MoE)
- **DeepSeek**: DeepSeek V3
- **Phi Family**: Phi-3, Phi-3 Vision
- **Qwen Family**: Qwen2, Qwen2.5
- **Gemma Family**: Gemma, Gemma2, Gemma4
- **ChatGLM Family**: ChatGLM3, GLM-4
- **Other Models**:
  - StableLM (StabilityAI)
  - MiniCPM, MiniCPM3
  - InternLM, InternLM2
  - Baichuan, Baichuan2
  - XVERSE
  - ExaONE 3
  - Command-R (Cohere)
  - DBRX (Databricks)

### Multimodal Models
- **Qwen2-VL**: Vision-language models
- **Phi-3 Vision**: Multimodal Phi models
- **MLlama**: Multimodal Llama models (Llama 3.2 Vision)
- **DeepSeek-VL**: DeepSeek VL2 and Janus multimodal models
- **LLaVA**: Large Language and Vision Assistant models

### Embedding Models
- **BERT-based**: BGE, E5, and other BERT architectures
- **Mistral-based**: E5-Mistral embedding models
- **Qwen-based**: GTE-Qwen2 embedding models

### Reward Models
- Models using Llama, Gemma, Qwen, and InternLM architectures for reward modeling

## Usage

```go
import "path/to/hf_model_config"

config, err := hf_model_config.LoadConfig("path/to/config.json")
if err != nil {
    // handle error
}
fmt.Println("Parameters:", config.GetParameterCount())
fmt.Println("Context window:", config.GetContextLength())
fmt.Println("Architecture:", config.GetArchitecture())
```

See the `examples/` directory for more detailed usage patterns.

## Directory Structure

- `interface.go` – Central interface and loader logic
- `llama.go`, `llama4.go`, `mllama.go` – LLaMA family implementations
- `mistral.go`, `mixtral.go` – Mistral and Mixtral implementations
- `deepseek_v3.go` – DeepSeek V3 implementation
- `phi.go`, `phi3_v.go` – Phi family implementations
- `qwen2.go`, `qwen2_vl.go` – Qwen family implementations
- `gemma.go` – Gemma family implementation
- `chatglm.go` – ChatGLM/GLM-4 implementation
- `stablelm.go` – StableLM implementation
- `minicpm.go` – MiniCPM implementation
- `internlm.go` – InternLM implementation
- `baichuan.go` – Baichuan implementation
- `xverse.go` – XVERSE implementation
- `exaone.go` – ExaONE implementation
- `bert.go` – BERT-based models (embeddings)
- `deepseek_vl.go` – DeepSeek VL multimodal models
- `llava.go` – LLaVA multimodal models
- `command_r.go` – Command-R implementation
- `dbrx.go` – DBRX implementation
- `safetensors.go` – Utilities for parameter counting from safetensors files
- `*_test.go` – Unit tests
- `examples/` – Example code
- `testdata/` – Real model configuration files for testing

## Future Work

- **Additional model families to support:**
  - OLMoE (Allen AI)
  - LLaVA-NeXT, LLaVA-OneVision (extended LLaVA variants)
  - CLIP (standalone vision encoders)
  - Additional embedding models (Voyage, Cohere embeddings)
  - More reward models

- **Enhanced features:**
  - Automatic model file downloading
  - Model quantization detection and analysis
  - Memory usage estimation
  - Performance benchmarking utilities

- Continue expanding support for new and emerging Hugging Face model architectures.
- Add more robust error handling and config validation for edge cases.
- Improve documentation and provide more real-world examples.
