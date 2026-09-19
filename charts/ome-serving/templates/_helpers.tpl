{{/*
Expand the name of the chart.
*/}}
{{- define "ome-serving.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "ome-serving.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "ome-serving.labels" -}}
helm.sh/chart: {{ include "ome-serving.chart" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Model Registry - Maps model names to their supportedModelFormats configuration.
This hides architecture details from users - they only need to specify model name.
The catalog below is the canonical in-tree reference; vendor runtime
YAMLs live in operators' own GitOps repos.
*/}}
{{- define "ome-serving.modelRegistry" -}}
# Qwen3 models
qwen3-0-6b:
  architecture: Qwen3ForCausalLM
  transformersVersion: "4.51.0"
  autoSelect: false
  priority: 1
  sizeRange: ["0.5B", "1B"]
  servedName: Qwen/Qwen3-0.6B
qwen3-30b-a3b:
  architecture: Qwen3MoeForCausalLM
  transformersVersion: "4.51.0"
  autoSelect: false
  priority: 1
  sizeRange: ["25B", "35B"]
  servedName: Qwen/Qwen3-30B-A3B
qwen3-32b:
  architecture: Qwen3ForCausalLM
  transformersVersion: "4.51.0"
  autoSelect: true
  priority: 1
  sizeRange: ["30B", "34B"]
  servedName: Qwen/Qwen3-32B
qwen3-4b:
  architecture: Qwen3ForCausalLM
  transformersVersion: "4.51.0"
  autoSelect: false
  priority: 1
  sizeRange: ["3B", "5B"]
  servedName: Qwen/Qwen3-4B
qwen3-8b:
  architecture: Qwen3ForCausalLM
  transformersVersion: "4.51.0"
  autoSelect: false
  priority: 1
  sizeRange: ["7B", "9B"]
  servedName: Qwen/Qwen3-8B
qwen3-embedding-0-6b:
  architecture: Qwen3ForCausalLM
  transformersVersion: "4.51.3"
  autoSelect: true
  priority: 1
  sizeRange: ["500M", "700M"]
  servedName: Qwen/Qwen3-Embedding-0.6B
qwen3-embedding-4b:
  architecture: Qwen3ForCausalLM
  transformersVersion: "4.51.2"
  autoSelect: false
  priority: 1
  sizeRange: ["3B", "5B"]
  servedName: Qwen/Qwen3-Embedding-4B
qwen3-next-80b-a3b-instruct:
  architecture: Qwen3NextForCausalLM
  transformersVersion: "4.57.0.dev0"
  autoSelect: false
  priority: 1
  sizeRange: ["70B", "90B"]
  servedName: Qwen/Qwen3-Next-80B-A3B-Instruct
qwen3-vl-235b-a22b-instruct:
  architecture: Qwen3VLMoeForConditionalGeneration
  transformersVersion: "4.57.0"
  autoSelect: false
  priority: 1
  sizeRange: ["230B", "240B"]
  servedName: Qwen/Qwen3-VL-235B-A22B-Instruct

# Qwen models
deepseek-r1-distill-qwen-1-5b:
  architecture: Qwen2ForCausalLM
  transformersVersion: "4.44.0"
  autoSelect: false
  priority: 1
  sizeRange: ["1B", "100B"]
  servedName: deepseek-ai/DeepSeek-R1-Distill-Qwen-1.5B
deepseek-r1-distill-qwen-14b:
  architecture: Qwen2ForCausalLM
  transformersVersion: "4.43.1"
  autoSelect: true
  priority: 1
  sizeRange: ["1B", "100B"]
  servedName: deepseek-ai/DeepSeek-R1-Distill-Qwen-14B
deepseek-r1-distill-qwen-32b:
  architecture: Qwen2ForCausalLM
  transformersVersion: "4.43.1"
  autoSelect: true
  priority: 1
  sizeRange: ["1B", "100B"]
  servedName: deepseek-ai/DeepSeek-R1-Distill-Qwen-32B
deepseek-r1-distill-qwen-7b:
  architecture: Qwen2ForCausalLM
  transformersVersion: "4.44.0"
  autoSelect: false
  priority: 1
  sizeRange: ["1B", "100B"]
  servedName: deepseek-ai/DeepSeek-R1-Distill-Qwen-7B
gme-qwen2-vl-2b-instruct:
  architecture: Qwen2VLForConditionalGeneration
  transformersVersion: "4.45.0.dev0"
  autoSelect: false
  priority: 1
  sizeRange: ["1B", "3B"]
  servedName: Alibaba-NLP/gme-Qwen2-VL-2B-Instruct
gte-qwen2-7b-instruct:
  architecture: Qwen2ForCausalLM
  transformersVersion: "4.41.2"
  autoSelect: false
  priority: 1
  sizeRange: ["6B", "8B"]
  servedName: Alibaba-NLP/gte-Qwen2-7B-instruct
llava-next-72b:
  architecture: LlavaQwenForCausalLM
  transformersVersion: "4.39.0"
  autoSelect: false
  priority: 1
  sizeRange: ["70B", "75B"]
  servedName: lmms-lab/llava-next-72b
llava-onevision-qwen2-7b-ov:
  architecture: LlavaQwenForCausalLM
  transformersVersion: "4.40.0"
  autoSelect: false
  priority: 1
  sizeRange: ["6B", "8B"]
  servedName: lmms-lab/llava-onevision-qwen2-7b-ov
mimo-vl-7b-rl:
  architecture: Qwen2_5_VLForConditionalGeneration
  transformersVersion: "4.41.2"
  autoSelect: false
  priority: 1
  sizeRange: ["6B", "8B"]
  servedName: XiaomiMiMo/MiMo-VL-7B-RL
qwen1-5-110b-chat:
  architecture: Qwen2ForCausalLM
  transformersVersion: "4.37.2"
  autoSelect: true
  priority: 1
  sizeRange: ["105B", "115B"]
  servedName: Qwen/Qwen1.5-110B-Chat
qwen1-5-32b-chat:
  architecture: Qwen2ForCausalLM
  transformersVersion: "4.37.2"
  autoSelect: true
  priority: 1
  sizeRange: ["30B", "34B"]
  servedName: Qwen/Qwen1.5-32B-Chat
qwen1-5-72b-chat:
  architecture: Qwen2ForCausalLM
  transformersVersion: "4.37.0"
  autoSelect: true
  priority: 1
  sizeRange: ["70B", "75B"]
  servedName: Qwen/Qwen1.5-72B-Chat
qwen1-5-7b-chat:
  architecture: Qwen2ForCausalLM
  transformersVersion: "4.37.0"
  autoSelect: true
  priority: 1
  sizeRange: ["5B", "9B"]
  servedName: Qwen/Qwen1.5-7B-Chat
qwen2-5-1-5b:
  architecture: Qwen2ForCausalLM
  transformersVersion: "4.40.1"
  autoSelect: false
  priority: 1
  sizeRange: ["1B", "2B"]
  servedName: Qwen/Qwen2.5-1.5B
qwen2-5-14b:
  architecture: Qwen2ForCausalLM
  transformersVersion: "4.43.1"
  autoSelect: false
  priority: 1
  sizeRange: ["10B", "16B"]
  servedName: Qwen/Qwen2.5-14B
qwen2-5-14b-instruct:
  architecture: Qwen2ForCausalLM
  transformersVersion: "4.43.1"
  autoSelect: true
  priority: 1
  sizeRange: ["12B", "16B"]
  servedName: Qwen/Qwen2.5-14B-Instruct
qwen2-5-32b-instruct:
  architecture: Qwen2ForCausalLM
  transformersVersion: "4.43.1"
  autoSelect: true
  priority: 1
  sizeRange: ["30B", "34B"]
  servedName: Qwen/Qwen2.5-32B-Instruct
qwen2-5-3b:
  architecture: Qwen2ForCausalLM
  transformersVersion: "4.40.1"
  autoSelect: false
  priority: 1
  sizeRange: ["2B", "5B"]
  servedName: Qwen/Qwen2.5-3B
qwen2-5-3b-instruct:
  architecture: Qwen2ForCausalLM
  transformersVersion: "4.43.1"
  autoSelect: true
  priority: 1
  sizeRange: ["2B", "4B"]
  servedName: Qwen/Qwen2.5-3B-Instruct
qwen2-5-72b-instruct:
  architecture: Qwen2ForCausalLM
  transformersVersion: "4.43.1"
  autoSelect: true
  priority: 1
  sizeRange: ["70B", "75B"]
  servedName: Qwen/Qwen2.5-72B-Instruct
qwen2-5-7b:
  architecture: Qwen2ForCausalLM
  transformersVersion: "4.40.1"
  autoSelect: false
  priority: 1
  sizeRange: ["5B", "9B"]
  servedName: Qwen/Qwen2.5-7B
qwen2-5-coder-32b-instruct:
  architecture: Qwen2ForCausalLM
  transformersVersion: "4.43.1"
  autoSelect: true
  priority: 1
  sizeRange: ["30B", "34B"]
  servedName: Qwen/Qwen2.5-Coder-32B-Instruct
qwen2-5-coder-7b-instruct:
  architecture: Qwen2ForCausalLM
  transformersVersion: "4.44.0"
  autoSelect: false
  priority: 1
  sizeRange: ["5B", "9B"]
  servedName: Qwen/Qwen2.5-Coder-7B-Instruct
qwen2-5-vl-7b-instruct:
  architecture: Qwen2_5_VLForConditionalGeneration
  transformersVersion: "4.41.2"
  autoSelect: true
  priority: 1
  sizeRange: ["5B", "9B"]
  servedName: Qwen/Qwen2.5-VL-7B-Instruct
qwen2-72b-instruct:
  architecture: Qwen2ForCausalLM
  transformersVersion: "4.40.1"
  autoSelect: true
  priority: 1
  sizeRange: ["70B", "75B"]
  servedName: Qwen/Qwen2-72B-Instruct
qwen2-7b-instruct:
  architecture: Qwen2ForCausalLM
  transformersVersion: "4.41.2"
  autoSelect: true
  priority: 1
  sizeRange: ["5B", "9B"]
  servedName: Qwen/Qwen2-7B-Instruct
qwen2-vl-7b-instruct:
  architecture: Qwen2VLForConditionalGeneration
  transformersVersion: "4.41.2"
  autoSelect: true
  priority: 1
  sizeRange: ["5B", "9B"]
  servedName: Qwen/Qwen2-VL-7B-Instruct
skywork-or1-7b-preview:
  architecture: Qwen2ForCausalLM
  transformersVersion: "4.45.2"
  autoSelect: true
  priority: 1
  sizeRange: ["5B", "9B"]
  servedName: Skywork/Skywork-OR1-7B-Preview

# Llama4 models
llama-4-maverick-17b-128e-instruct:
  architecture: Llama4ForConditionalGeneration
  transformersVersion: "4.51.0.dev0"
  autoSelect: false
  priority: 1
  sizeRange: ["400B", "402B"]
  servedName: meta-llama/Llama-4-Maverick-17B-128E-Instruct
llama-4-maverick-17b-128e-instruct-fp8:
  architecture: Llama4ForConditionalGeneration
  transformersVersion: "4.51.0.dev0"
  autoSelect: true
  priority: 2
  sizeRange: ["400B", "402B"]
  servedName: meta-llama/Llama-4-Maverick-17B-128E-Instruct-FP8
llama-4-maverick-17b-128e-instruct-fp8-grpc:
  architecture: Llama4ForConditionalGeneration
  transformersVersion: "4.51.0"
  autoSelect: false
  priority: 2
  sizeRange: ["400B", "402B"]
  servedName: meta-llama/Llama-4-Maverick-17B-128E-Instruct-FP8
llama-4-maverick-17b-128e-instruct-fp8-pd-grpc:
  architecture: Llama4ForConditionalGeneration
  transformersVersion: "4.51.0"
  autoSelect: false
  priority: 2
  sizeRange: ["400B", "402B"]
  servedName: meta-llama/Llama-4-Maverick-17B-128E-Instruct-FP8
llama-4-scout-17b-16e-instruct:
  architecture: Llama4ForConditionalGeneration
  transformersVersion: "4.51.0.dev0"
  autoSelect: true
  priority: 2
  sizeRange: ["100B", "109B"]
  servedName: meta-llama/Llama-4-Scout-17B-16E-Instruct

# Llama models
deepseek-coder-7b-instruct-v1-5:
  architecture: LlamaForCausalLM
  transformersVersion: "4.35.2"
  autoSelect: false
  priority: 1
  sizeRange: ["1B", "100B"]
  servedName: deepseek-ai/deepseek-coder-7b-instruct-v1.5
deepseek-llm-7b-chat:
  architecture: LlamaForCausalLM
  transformersVersion: "4.33.1"
  autoSelect: true
  priority: 1
  sizeRange: ["1B", "100B"]
  servedName: deepseek-ai/deepseek-llm-7b-chat
deepseek-r1-distill-llama-70b:
  architecture: LlamaForCausalLM
  transformersVersion: "4.47.0.dev0"
  autoSelect: true
  priority: 1
  sizeRange: ["65B", "75B"]
  servedName: deepseek-ai/DeepSeek-R1-Distill-Llama-70B
deepseek-r1-distill-llama-8b:
  architecture: LlamaForCausalLM
  transformersVersion: "4.43.0.dev0"
  autoSelect: false
  priority: 1
  sizeRange: ["7B", "9B"]
  servedName: deepseek-ai/DeepSeek-R1-Distill-Llama-8B
falcon3-10b-instruct:
  architecture: LlamaForCausalLM
  transformersVersion: "4.46.1"
  autoSelect: true
  priority: 1
  sizeRange: ["10B", "12B"]
  servedName: tiiuae/Falcon3-10B-Instruct
hermes-2-pro-llama-3-8b:
  architecture: LlamaForCausalLM
  transformersVersion: "4.42.3"
  autoSelect: false
  priority: 1
  sizeRange: ["7B", "9B"]
  servedName: NousResearch/Hermes-2-Pro-Llama-3-8B
llama-2-13b:
  architecture: LlamaForCausalLM
  transformersVersion: "4.32.0.dev0"
  autoSelect: true
  priority: 1
  sizeRange: ["10B", "15B"]
  servedName: meta-llama/Llama-2-13b-hf
llama-2-13b-chat-hf:
  architecture: LlamaForCausalLM
  transformersVersion: "4.31.0.dev0"
  autoSelect: true
  priority: 1
  sizeRange: ["10B", "15B"]
  servedName: meta-llama/Llama-2-13b-chat-hf
llama-2-70b:
  architecture: LlamaForCausalLM
  transformersVersion: "4.32.0.dev0"
  autoSelect: true
  priority: 1
  sizeRange: ["65B", "75B"]
  servedName: meta-llama/Llama-2-70b-hf
llama-2-70b-chat-hf:
  architecture: LlamaForCausalLM
  transformersVersion: "4.31.0.dev0"
  autoSelect: true
  priority: 1
  sizeRange: ["65B", "75B"]
  servedName: meta-llama/Llama-2-70b-chat-hf
llama-2-7b:
  architecture: LlamaForCausalLM
  transformersVersion: "4.31.0.dev0"
  autoSelect: true
  priority: 1
  sizeRange: ["5B", "10B"]
  servedName: meta-llama/Llama-2-7b-hf
llama-2-7b-chat-hf:
  architecture: LlamaForCausalLM
  transformersVersion: "4.32.0.dev0"
  autoSelect: true
  priority: 1
  sizeRange: ["5B", "10B"]
  servedName: meta-llama/Llama-2-7b-chat-hf
llama-3-1-405b-instruct-fp8:
  architecture: LlamaForCausalLM
  transformersVersion: "4.43.0"
  autoSelect: false
  priority: 1
  sizeRange: ["400B", "410B"]
  servedName: meta-llama/Llama-3.1-405B-Instruct-FP8
llama-3-1-70b-instruct:
  architecture: LlamaForCausalLM
  transformersVersion: "4.42.3"
  autoSelect: false
  priority: 1
  sizeRange: ["60B", "75B"]
  servedName: meta-llama/Meta-Llama-3.1-70B-Instruct
llama-3-1-8b-instruct:
  architecture: LlamaForCausalLM
  transformersVersion: "4.42.3"
  autoSelect: false
  priority: 1
  sizeRange: ["7B", "9B"]
  servedName: meta-llama/Llama-3.1-8B-Instruct
llama-3-1-8b-instruct-grpc:
  architecture: LlamaForCausalLM
  transformersVersion: "4.43.0"
  autoSelect: false
  priority: 1
  sizeRange: ["7B", "9B"]
  servedName: meta-llama/Llama-3.1-8B-Instruct
llama-3-1-nemotron-70b-instruct-hf:
  architecture: LlamaForCausalLM
  transformersVersion: "4.40.0"
  autoSelect: true
  priority: 1
  sizeRange: ["68B", "75B"]
  servedName: nvidia/Llama-3.1-Nemotron-70B-Instruct-HF
llama-3-1-nemotron-nano-8b-v1:
  architecture: LlamaForCausalLM
  transformersVersion: "4.47.1"
  autoSelect: false
  priority: 1
  sizeRange: ["7B", "9B"]
  servedName: nvidia/Llama-3.1-Nemotron-Nano-8B-v1
llama-3-1-nemotron-ultra-253b-v1:
  architecture: LlamaForCausalLM
  transformersVersion: "4.48.3"
  autoSelect: false
  priority: 1
  sizeRange: ["250B", "260B"]
  servedName: nvidia/Llama-3.1-Nemotron-70B-Instruct
llama-3-2-11b-vision-instruct:
  architecture: MllamaForConditionalGeneration
  transformersVersion: "4.45.0"
  autoSelect: false
  priority: 1
  sizeRange: ["10B", "12B"]
  servedName: meta-llama/Llama-3.2-11B-Vision-Instruct
llama-3-2-1b-instruct:
  architecture: LlamaForCausalLM
  transformersVersion: "4.45.0.dev0"
  autoSelect: false
  priority: 1
  sizeRange: ["500M", "2B"]
  servedName: meta-llama/Llama-3.2-1B-Instruct
llama-3-2-3b-instruct:
  architecture: LlamaForCausalLM
  transformersVersion: "4.45.0.dev0"
  autoSelect: false
  priority: 1
  sizeRange: ["2B", "4B"]
  servedName: meta-llama/Llama-3.2-3B-Instruct
llama-3-2-90b-vision-instruct:
  architecture: MllamaForConditionalGeneration
  transformersVersion: "4.45.0"
  autoSelect: false
  priority: 1
  sizeRange: ["85B", "95B"]
  servedName: meta-llama/Llama-3.2-90B-Vision-Instruct
llama-3-2-90b-vision-instruct-fp8:
  architecture: MllamaForConditionalGeneration
  transformersVersion: "4.45.0"
  autoSelect: false
  priority: 1
  sizeRange: ["85B", "95B"]
  servedName: RedHatAI/Llama-3.2-90B-Vision-Instruct-FP8-dynamic
llama-3-3-70b-instruct:
  architecture: LlamaForCausalLM
  transformersVersion: "4.47.0.dev0"
  autoSelect: false
  priority: 1
  sizeRange: ["60B", "75B"]
  servedName: meta-llama/Llama-3.3-70B-Instruct
llama-3-3-70b-instruct-fp8-dynamic:
  architecture: LlamaForCausalLM
  transformersVersion: "4.45.0"
  autoSelect: false
  priority: 1
  sizeRange: ["60B", "75B"]
  servedName: RedHatAI/Llama-3.3-70B-Instruct-FP8-dynamic
llama-3-70b-instruct:
  architecture: LlamaForCausalLM
  transformersVersion: "4.40.0.dev0"
  autoSelect: false
  priority: 1
  sizeRange: ["65B", "75B"]
  servedName: meta-llama/Meta-Llama-3-70B-Instruct
llama-3-8b-instruct:
  architecture: LlamaForCausalLM
  transformersVersion: "4.40.0.dev0"
  autoSelect: false
  priority: 1
  sizeRange: ["7B", "9B"]
  servedName: meta-llama/Meta-Llama-3-8B-Instruct
llama-guard-3-8b:
  architecture: LlamaForCausalLM
  transformersVersion: "4.43.0.dev0"
  autoSelect: true
  priority: 1
  sizeRange: ["7B", "9B"]
  servedName: meta-llama/Llama-Guard-3-8B
llava-v1-5-13b:
  architecture: LlavaLlamaForCausalLM
  transformersVersion: "4.31.0"
  autoSelect: false
  priority: 1
  sizeRange: ["12B", "14B"]
  servedName: liuhaotian/llava-v1.5-13b
nvila-8b:
  architecture: LlavaLlamaModel
  transformersVersion: "4.46.0"
  autoSelect: true
  priority: 1
  sizeRange: ["7B", "9B"]
  servedName: Efficient-Large-Model/NVILA-8B
smollm-1-7b:
  architecture: LlamaForCausalLM
  transformersVersion: "4.39.3"
  autoSelect: false
  priority: 1
  sizeRange: ["1B", "2B"]
  servedName: HuggingFaceTB/SmolLM-1.7B
smollm2-1-7b-instruct:
  architecture: LlamaForCausalLM
  transformersVersion: "4.42.3"
  autoSelect: false
  priority: 1
  sizeRange: ["1B", "2B"]
  servedName: HuggingFaceTB/SmolLM2-1.7B-Instruct
solar-10-7b-instruct-v1-0:
  architecture: LlamaForCausalLM
  transformersVersion: "4.35.0"
  autoSelect: false
  priority: 1
  sizeRange: ["10B", "12B"]
  servedName: upstage/SOLAR-10.7B-Instruct-v1.0
vicuna-13b-v1-5:
  architecture: LlamaForCausalLM
  transformersVersion: "4.55.1"
  autoSelect: false
  priority: 1
  sizeRange: ["7B", "15B"]
  servedName: lmsys/vicuna-13b-v1.5
vicuna-7b-v1-5:
  architecture: LlamaForCausalLM
  transformersVersion: "4.55.1"
  autoSelect: false
  priority: 1
  sizeRange: ["3B", "9B"]
  servedName: lmsys/vicuna-7b-v1.5

# DeepSeek models
deepseek-rdma:
  architecture: DeepseekV3ForCausalLM
  transformersVersion: "4.33.1"
  autoSelect: false
  priority: 1
  sizeRange: ["650B", "700B"]
  servedName: deepseek-rdma
deepseek-v2-lite-chat:
  architecture: DeepseekV2ForCausalLM
  transformersVersion: "4.33.1"
  autoSelect: true
  priority: 1
  sizeRange: ["1B", "100B"]
  servedName: deepseek-ai/DeepSeek-V2-Lite-Chat
deepseek-v3:
  architecture: DeepseekV3ForCausalLM
  transformersVersion: "4.33.1"
  autoSelect: false
  priority: 1
  sizeRange: ["600B", "700B"]
  servedName: deepseek-ai/DeepSeek-V3
deepseek-v3-0324:
  architecture: DeepseekV3ForCausalLM
  transformersVersion: "4.46.3"
  autoSelect: false
  priority: 1
  sizeRange: ["600B", "700B"]
  servedName: deepseek-ai/DeepSeek-V3-0324
deepseek-vl2:
  architecture: DeepseekVLV2ForCausalLM
  transformersVersion: "4.38.2"
  autoSelect: false
  priority: 1
  sizeRange: ["25B", "30B"]
  servedName: deepseek-ai/deepseek-vl2
kimi-k2-instruct:
  architecture: DeepseekV3ForCausalLM
  transformersVersion: "4.48.3"
  autoSelect: false
  priority: 1
  sizeRange: ["900B", "1100B"]
  servedName: moonshotai/Kimi-K2-Instruct

# Mistral models
e5-mistral-7b-instruct:
  architecture: MistralModel
  transformersVersion: "4.34.0"
  autoSelect: true
  priority: 1
  sizeRange: ["6B", "8B"]
  servedName: intfloat/e5-mistral-7b-instruct
mistral-7b-instruct:
  architecture: MistralForCausalLM
  transformersVersion: "4.36.0"
  autoSelect: false
  priority: 1
  sizeRange: ["5B", "9B"]
  servedName: mistralai/Mistral-7B-Instruct-v0.2
mistral-7b-instruct-v0-2:
  architecture: MistralForCausalLM
  transformersVersion: "4.36.0"
  autoSelect: false
  priority: 1
  sizeRange: ["6B", "8B"]
  servedName: mistralai/Mistral-7B-Instruct-v0.2
mistral-7b-instruct-v0-3:
  architecture: MistralForCausalLM
  transformersVersion: "4.42.0.dev0"
  autoSelect: false
  priority: 1
  sizeRange: ["6B", "8B"]
  servedName: mistralai/Mistral-7B-Instruct-v0.3
mistral-nemo-instruct-2407:
  architecture: MistralForCausalLM
  transformersVersion: "4.43.0.dev0"
  autoSelect: false
  priority: 1
  sizeRange: ["10B", "14B"]
  servedName: mistralai/Mistral-Nemo-Instruct-2407
mistral-small-3-1-24b-instruct-2503:
  architecture: Mistral3ForConditionalGeneration
  transformersVersion: "4.50.0.dev0"
  autoSelect: false
  priority: 1
  sizeRange: ["20B", "28B"]
  servedName: mistralai/Mistral-Small-3.1-24B-Instruct-2503
mixtral-8x22b:
  architecture: MixtralForCausalLM
  transformersVersion: "4.38.0"
  autoSelect: true
  priority: 1
  sizeRange: ["135B", "145B"]
  servedName: mistralai/Mixtral-8x22B-v0.1
mixtral-8x7b:
  architecture: MixtralForCausalLM
  transformersVersion: "4.36.0.dev0"
  autoSelect: true
  priority: 1
  sizeRange: ["40B", "50B"]
  servedName: mistralai/Mixtral-8x7B-v0.1
mixtral-8x7b-instruct:
  architecture: MixtralForCausalLM
  transformersVersion: "4.36.0.dev0"
  autoSelect: false
  priority: 1
  sizeRange: ["40B", "50B"]
  servedName: mistralai/Mixtral-8x7B-Instruct-v0.1

# Gemma models
gemma-2-27b-it:
  architecture: Gemma2ForCausalLM
  transformersVersion: "4.42.0.dev0"
  autoSelect: false
  priority: 1
  sizeRange: ["25B", "30B"]
  servedName: google/gemma-2-27b-it
gemma-2-2b-it:
  architecture: Gemma2ForCausalLM
  transformersVersion: "4.42.4"
  autoSelect: false
  priority: 1
  sizeRange: ["1B", "3B"]
  servedName: google/gemma-2-2b-it
gemma-2-9b-it:
  architecture: Gemma2ForCausalLM
  transformersVersion: "4.42.0.dev0"
  autoSelect: false
  priority: 1
  sizeRange: ["5B", "12B"]
  servedName: google/gemma-2-9b-it
gemma-3-12b-it:
  architecture: Gemma3ForConditionalGeneration
  transformersVersion: "4.50.0.dev0"
  autoSelect: false
  priority: 1
  sizeRange: ["10B", "15B"]
  servedName: google/gemma-3-12b-it
gemma-3-1b-it:
  architecture: Gemma3ForCausalLM
  transformersVersion: "4.50.0.dev0"
  autoSelect: false
  priority: 1
  sizeRange: ["0.5B", "2B"]
  servedName: google/gemma-3-1b-it
gemma-3-4b-it:
  architecture: Gemma3ForConditionalGeneration
  transformersVersion: "4.50.0.dev0"
  autoSelect: false
  priority: 1
  sizeRange: ["3B", "5B"]
  servedName: google/gemma-3-4b-it

# Phi models
phi-2:
  architecture: PhiForCausalLM
  transformersVersion: "4.37.0"
  autoSelect: true
  priority: 1
  sizeRange: ["2B", "3B"]
  servedName: microsoft/phi-2
phi-3-5-mini-instruct:
  architecture: Phi3ForCausalLM
  transformersVersion: "4.43.3"
  autoSelect: true
  priority: 1
  sizeRange: ["2B", "5B"]
  servedName: microsoft/Phi-3.5-mini-instruct
phi-3-5-moe-instruct:
  architecture: PhiMoEForCausalLM
  transformersVersion: "4.43.3"
  autoSelect: true
  priority: 1
  sizeRange: ["40B", "45B"]
  servedName: microsoft/Phi-3.5-MoE-instruct
phi-3-mini-4k-instruct:
  architecture: Phi3ForCausalLM
  transformersVersion: "4.40.2"
  autoSelect: true
  priority: 1
  sizeRange: ["2B", "5B"]
  servedName: microsoft/Phi-3-mini-4k-instruct
phi-3-vision-128k-instruct:
  architecture: Phi3VForCausalLM
  transformersVersion: "4.38.1"
  autoSelect: false
  priority: 1
  sizeRange: ["4B", "5B"]
  servedName: microsoft/Phi-3-vision-128k-instruct
phi-4:
  architecture: Phi3ForCausalLM
  transformersVersion: "4.47.0"
  autoSelect: true
  priority: 1
  sizeRange: ["14B", "16B"]
  servedName: microsoft/phi-4
phi-4-mini-instruct:
  architecture: Phi3ForCausalLM
  transformersVersion: "4.45.0"
  autoSelect: true
  priority: 1
  sizeRange: ["3B", "4B"]
  servedName: microsoft/Phi-4-mini-instruct
phi-4-multimodal-instruct:
  architecture: Phi4MMForCausalLM
  transformersVersion: "4.46.1"
  autoSelect: true
  priority: 1
  sizeRange: ["5B", "6B"]
  servedName: microsoft/Phi-4-multimodal-instruct

# GLM models
chatglm2-6b:
  architecture: ChatGLMModel
  transformersVersion: "4.27.1"
  autoSelect: false
  priority: 1
  sizeRange: ["5B", "7B"]
  servedName: THUDM/chatglm2-6b
glm-4-5v:
  architecture: Glm4vMoeForConditionalGeneration
  transformersVersion: "4.57.1"
  autoSelect: false
  priority: 1
  sizeRange: ["8B", "10B"]
  servedName: zai-org/GLM-4.5V
glm-4-9b-chat:
  architecture: GlmForCausalLM
  transformersVersion: "4.46.0.dev0"
  autoSelect: false
  priority: 1
  sizeRange: ["8B", "10B"]
  servedName: ZhipuAI/glm-4-9b-chat

# Intern models
internlm2-20b:
  architecture: InternLM2ForCausalLM
  transformersVersion: "4.41.0"
  autoSelect: false
  priority: 1
  sizeRange: ["15B", "25B"]
  servedName: internlm/internlm2-20b
internlm2-7b:
  architecture: InternLM2ForCausalLM
  transformersVersion: "4.41.0"
  autoSelect: false
  priority: 1
  sizeRange: ["6B", "8B"]
  servedName: internlm/internlm2-7b
internlm2-7b-reward:
  architecture: InternLM2ForRewardModel
  transformersVersion: "4.41.0"
  autoSelect: false
  priority: 1
  sizeRange: ["6B", "8B"]
  servedName: internlm/internlm2-7b-reward
internvl2-5-8b:
  architecture: InternVLChatModel
  transformersVersion: "4.37.2"
  autoSelect: true
  priority: 1
  sizeRange: ["7B", "9B"]
  servedName: OpenGVLab/InternVL2_5-8B

# Embedding models
bge-large-en-v1-5:
  architecture: BertModel
  transformersVersion: "4.30.0"
  autoSelect: true
  priority: 1
  sizeRange: ["300M", "400M"]
  servedName: BAAI/bge-large-en-v1.5
bge-m3:
  architecture: XLMRobertaModel
  transformersVersion: "4.33.0"
  autoSelect: true
  priority: 1
  sizeRange: ["500M", "700M"]
  servedName: BAAI/bge-m3
bge-reranker-v2-m3:
  architecture: XLMRobertaForSequenceClassification
  transformersVersion: "4.38.1"
  autoSelect: true
  priority: 1
  sizeRange: ["500M", "700M"]
  servedName: BAAI/bge-reranker-v2-m3

# Code models
starcoder2-15b:
  architecture: Starcoder2ForCausalLM
  transformersVersion: "4.37.0.dev0"
  autoSelect: false
  priority: 1
  sizeRange: ["14B", "16B"]
  servedName: bigcode/starcoder2-15b
starcoder2-7b:
  architecture: Starcoder2ForCausalLM
  transformersVersion: "4.37.0.dev0"
  autoSelect: false
  priority: 1
  sizeRange: ["6B", "8B"]
  servedName: bigcode/starcoder2-7b

# MiniCPM models
minicpm-2b-sft-bf16:
  architecture: MiniCPMForCausalLM
  transformersVersion: "4.41.0"
  autoSelect: false
  priority: 1
  sizeRange: ["2B", "3B"]
  servedName: openbmb/MiniCPM-2B-sft-bf16
minicpm-v-2-6:
  architecture: MiniCPMV
  transformersVersion: "4.40.0"
  autoSelect: false
  priority: 1
  sizeRange: ["7B", "9B"]
  servedName: openbmb/MiniCPM-V-2_6
minicpm3-4b:
  architecture: MiniCPM3ForCausalLM
  transformersVersion: "4.41.0"
  autoSelect: false
  priority: 1
  sizeRange: ["3B", "5B"]
  servedName: openbmb/MiniCPM3-4B

# Falcon models
falcon-7b-instruct:
  architecture: FalconForCausalLM
  transformersVersion: "4.27.4"
  autoSelect: false
  priority: 1
  sizeRange: ["5B", "9B"]
  servedName: tiiuae/falcon-7b-instruct

# Bloom models
bloomz-7b1:
  architecture: BloomForCausalLM
  transformersVersion: "4.21.0.dev0"
  autoSelect: false
  priority: 1
  sizeRange: ["5B", "10B"]
  servedName: bigscience/bloomz-7b1

# GPT models
dolly-v2-12b:
  architecture: GPTNeoXForCausalLM
  transformersVersion: "4.25.1"
  autoSelect: false
  priority: 1
  sizeRange: ["10B", "13B"]
  servedName: databricks/dolly-v2-12b
gpt-j-6b:
  architecture: GPTJForCausalLM
  transformersVersion: "4.42.3"
  autoSelect: false
  priority: 1
  sizeRange: ["5B", "7B"]
  servedName: EleutherAI/gpt-j-6b
gpt-oss-120b:
  architecture: GptOssForCausalLM
  transformersVersion: "4.55.0"
  autoSelect: false
  priority: 1
  sizeRange: ["115B", "125B"]
  servedName: openai/gpt-oss-120b
gpt-oss-120b-bf16:
  architecture: GptOssForCausalLM
  transformersVersion: "4.55.1"
  autoSelect: false
  priority: 1
  sizeRange: ["115B", "125B"]
  servedName: lmsys/gpt-oss-120b-bf16
gpt-oss-120b-grpc:
  architecture: GptOssForCausalLM
  transformersVersion: "4.55.0"
  autoSelect: true
  priority: 1
  sizeRange: ["115B", "125B"]
  servedName: openai/gpt-oss-120b
gpt-oss-20b:
  architecture: GptOssForCausalLM
  transformersVersion: "4.55.0"
  autoSelect: true
  priority: 1
  sizeRange: ["10B", "15B"]
  servedName: openai/gpt-oss-20b
gpt-oss-20b-bf16:
  architecture: GptOssForCausalLM
  transformersVersion: "4.55.1"
  autoSelect: false
  priority: 1
  sizeRange: ["18B", "22B"]
  servedName: lmsys/gpt-oss-20b-bf16
gpt-oss-20b-grpc:
  architecture: GptOssForCausalLM
  transformersVersion: "4.55.0"
  autoSelect: true
  priority: 1
  sizeRange: ["115B", "125B"]
  servedName: openai/gpt-oss-20b
stablelm-tuned-alpha-7b:
  architecture: GPTNeoXForCausalLM
  transformersVersion: "4.28.1"
  autoSelect: false
  priority: 1
  sizeRange: ["6B", "8B"]
  servedName: stabilityai/stablelm-tuned-alpha-7b
xgen-7b-8k-inst:
  architecture: GPTNeoXForCausalLM
  transformersVersion: "4.40.0.dev0"
  autoSelect: false
  priority: 1
  sizeRange: ["5B", "9B"]
  servedName: Salesforce/xgen-7b-8k-inst

# Cohere models
c4ai-command-r-v01:
  architecture: CohereForCausalLM
  transformersVersion: "4.38.2"
  autoSelect: false
  priority: 1
  sizeRange: ["30B", "40B"]
  servedName: CohereForAI/c4ai-command-r-v01

# Granite models
granite-3-0-3b-a800m-instruct:
  architecture: GraniteMoeForCausalLM
  transformersVersion: "4.46.0.dev0"
  autoSelect: false
  priority: 1
  sizeRange: ["2B", "4B"]
  servedName: ibm-granite/granite-3.0-3b-a800m-instruct
granite-3-1-8b-instruct:
  architecture: GraniteForCausalLM
  transformersVersion: "4.47.0"
  autoSelect: false
  priority: 1
  sizeRange: ["7B", "9B"]
  servedName: ibm-granite/granite-3.1-8b-instruct

# Exaone models
exaone-3-5-7-8b-instruct:
  architecture: ExaoneForCausalLM
  transformersVersion: "4.43.0"
  autoSelect: false
  priority: 1
  sizeRange: ["7B", "9B"]
  servedName: LGAI-EXAONE/EXAONE-3.5-7.8B-Instruct

# OLMo models
olmo-2-1124-7b-instruct:
  architecture: Olmo2ForCausalLM
  transformersVersion: "4.47.0.dev0"
  autoSelect: false
  priority: 1
  sizeRange: ["6B", "8B"]
  servedName: allenai/OLMo-2-1124-7B-Instruct
olmoe-1b-7b-0924:
  architecture: OlmoeForCausalLM
  transformersVersion: "4.43.0.dev0"
  autoSelect: false
  priority: 1
  sizeRange: ["6B", "8B"]
  servedName: allenai/OLMoE-1B-7B-0924

# Baichuan models
baichuan2-13b-chat:
  architecture: BaichuanForCausalLM
  transformersVersion: "4.29.2"
  autoSelect: false
  priority: 1
  sizeRange: ["12B", "14B"]
  servedName: baichuan-inc/Baichuan2-13B-Chat
baichuan2-7b-chat:
  architecture: BaichuanForCausalLM
  transformersVersion: "4.29.2"
  autoSelect: false
  priority: 1
  sizeRange: ["5B", "9B"]
  servedName: baichuan-inc/Baichuan2-7B-Chat

# Other models
afm-4-5b-base:
  architecture: ArceeForCausalLM
  transformersVersion: "4.53.2"
  autoSelect: false
  priority: 1
  sizeRange: ["4B", "5B"]
  servedName: arcee-ai/AFM-4.5B-Base
clip-vit-large-patch14-336:
  architecture: CLIPModel
  transformersVersion: "4.21.3"
  autoSelect: false
  priority: 1
  sizeRange: ["300M", "500M"]
  servedName: openai/clip-vit-large-patch14-336
dbrx-instruct:
  architecture: DbrxForCausalLM
  transformersVersion: "4.40.0"
  autoSelect: false
  priority: 1
  sizeRange: ["130B", "135B"]
  servedName: databricks/dbrx-instruct
dots-ocr:
  architecture: DotsOCRForConditionalGeneration
  transformersVersion: "4.42.0"
  autoSelect: false
  priority: 1
  sizeRange: ["1B", "3B"]
  servedName: rednote-hilab/dots.ocr
dots-vlm1-inst:
  architecture: DotsVLMForConditionalGeneration
  transformersVersion: "4.42.0"
  autoSelect: false
  priority: 1
  sizeRange: ["1B", "3B"]
  servedName: rednote-hilab/dots.vlm1.inst
ernie-4-5-21b-a3b-pt:
  architecture: Ernie4_5_MoeForCausalLM
  transformersVersion: "4.54.0.dev0"
  autoSelect: false
  priority: 1
  sizeRange: ["20B", "25B"]
  servedName: baidu/ERNIE-4.5-21B-A3B-PT
grok-1:
  architecture: Grok1ModelForCausalLM
  transformersVersion: "4.35.0"
  autoSelect: false
  priority: 1
  sizeRange: ["300B", "320B"]
  servedName: xai-org/grok-1
grok-2:
  architecture: Grok1ForCausalLM
  transformersVersion: "4.35.0"
  autoSelect: true
  priority: 1
  sizeRange: ["310B", "320B"]
  servedName: xai-org/grok-2
janus-pro-7b:
  architecture: JanusMultiModalityCausalLM
  transformersVersion: "4.33.1"
  autoSelect: false
  priority: 1
  sizeRange: ["1B", "100B"]
  servedName: deepseek-ai/Janus-Pro-7B
jet-nemotron-2b:
  architecture: JetNemotronForCausalLM
  transformersVersion: "4.51.3"
  autoSelect: false
  priority: 1
  sizeRange: ["1B", "3B"]
  servedName: jet-ai/Jet-Nemotron-2B
kimi-vl-a3b-instruct:
  architecture: KimiVLForConditionalGeneration
  transformersVersion: "4.50.3"
  autoSelect: false
  priority: 1
  sizeRange: ["10B", "15B"]
  servedName: moonshotai/Kimi-VL-A3B-Instruct
ling-lite:
  architecture: BailingMoeForCausalLM
  transformersVersion: "4.36.0"
  autoSelect: false
  priority: 1
  sizeRange: ["6B", "10B"]
  servedName: inclusionAI/Ling-lite
ling-plus:
  architecture: BailingMoeForCausalLM
  transformersVersion: "4.36.0"
  autoSelect: false
  priority: 1
  sizeRange: ["35B", "45B"]
  servedName: inclusionAI/Ling-plus
llama-3-3-nemotron-super-49b-v1:
  architecture: DeciLMForCausalLM
  transformersVersion: "4.48.3"
  autoSelect: true
  priority: 1
  sizeRange: ["45B", "55B"]
  servedName: nvidia/Llama-3.3-Nemotron-Super-49B-v1
mimo-7b-rl:
  architecture: MiMoForCausalLM
  transformersVersion: "4.40.1"
  autoSelect: false
  priority: 1
  sizeRange: ["6B", "8B"]
  servedName: XiaomiMiMo/MiMo-7B-RL
minimax-m2:
  architecture: MiniMaxM2ForCausalLM
  transformersVersion: "4.57.1"
  autoSelect: false
  priority: 1
  sizeRange: ["40B", "50B"]
  servedName: minimax/MiniMax-M2
mpt-7b:
  architecture: MPTForCausalLM
  transformersVersion: "4.28.1"
  autoSelect: false
  priority: 1
  sizeRange: ["1B", "100B"]
  servedName: mosaicml/mpt-7b
nvidia-nemotron-nano-12b-v2-vl-bf16:
  architecture: NemotronVLForConditionalGeneration
  transformersVersion: "4.51.3"
  autoSelect: false
  priority: 1
  sizeRange: ["11B", "13B"]
  servedName: nvidia/NVIDIA-Nemotron-Nano-12B-v2-VL-BF16
nvidia-nemotron-nano-9b-v2:
  architecture: NemotronHForCausalLM
  transformersVersion: "4.47.0"
  autoSelect: false
  priority: 1
  sizeRange: ["8B", "10B"]
  servedName: nvidia/NVIDIA-Nemotron-Nano-9B-v2
nvidia-nemotron-3-nano-30b-a3b-base-bf16:
  architecture: NemotronHForCausalLM
  transformersVersion: "4.57.1"
  autoSelect: false
  priority: 1
  sizeRange: ["28B", "32B"]
  servedName: nvidia/NVIDIA-Nemotron-3-Nano-30B-A3B-Base-BF16
nvidia-nemotron-3-nano-30b-a3b-bf16:
  architecture: NemotronHForCausalLM
  transformersVersion: "4.55.4"
  autoSelect: false
  priority: 1
  sizeRange: ["28B", "32B"]
  servedName: nvidia/NVIDIA-Nemotron-3-Nano-30B-A3B-BF16
nvidia-nemotron-3-nano-30b-a3b-fp8:
  architecture: NemotronHForCausalLM
  transformersVersion: "4.53.2"
  autoSelect: false
  priority: 1
  sizeRange: ["28B", "32B"]
  servedName: nvidia/NVIDIA-Nemotron-3-Nano-30B-A3B-FP8
nvidia-nemotron-nano-12b-v2-vl-fp8:
  architecture: NemotronH_Nano_VL_V2
  transformersVersion: "4.51.3"
  autoSelect: false
  priority: 1
  sizeRange: ["11B", "13B"]
  servedName: nvidia/NVIDIA-Nemotron-Nano-12B-v2-VL-FP8
orion-14b-base:
  architecture: OrionForCausalLM
  transformersVersion: "4.34.0"
  autoSelect: true
  priority: 1
  sizeRange: ["12B", "16B"]
  servedName: OrionStarAI/Orion-14B-Base
persimmon-8b-chat:
  architecture: PersimmonForCausalLM
  transformersVersion: "4.34.0.dev0"
  autoSelect: false
  priority: 1
  sizeRange: ["7B", "9B"]
  servedName: adept/persimmon-8b-chat
qwen-7b-chat:
  architecture: QWenLMHeadModel
  transformersVersion: "4.32.0"
  autoSelect: true
  priority: 1
  sizeRange: ["5B", "9B"]
  servedName: Qwen/Qwen-7B-Chat
stablelm-2-12b-chat:
  architecture: StableLmForCausalLM
  transformersVersion: "4.40.0"
  autoSelect: false
  priority: 1
  sizeRange: ["10B", "15B"]
  servedName: stabilityai/stablelm-2-12b-chat
tele-flm:
  architecture: TeleFLMModel
  transformersVersion: "4.40.0"
  autoSelect: false
  priority: 1
  sizeRange: ["50B", "55B"]
  servedName: CofeAI/Tele-FLM
xverse-moe-a36b:
  architecture: XverseMoeForCausalLM
  transformersVersion: "4.30.0"
  autoSelect: false
  priority: 1
  sizeRange: ["200B", "260B"]
  servedName: xverse/XVERSE-MoE-A36B

{{- end }}
