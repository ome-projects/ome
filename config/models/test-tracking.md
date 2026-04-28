# Model Test Tracking

**Last Updated**: 2025-12-05
**Total Models**: 203 | **Total Runtimes**: 187
**Cluster**: 14x H100 nodes (8 GPUs each, 80GB/GPU)

## Summary

| Status        | Count | Percentage |
|---------------|-------|------------|
| ✅ Passed      | 95    | 46.8%      |
| ❌ Failed      | 40    | 19.7%      |
| ⏭️ Skipped    | 5     | 2.5%       |
| 🔲 Not Tested | 63    | 31.0%      |

---

## Master Model List

| Vendor                | Model                                    | Size  | Type         | GPUs | Runtime                                      | Status        | Owner  | Notes                                        |
|-----------------------|------------------------------------------|-------|--------------|------|----------------------------------------------|---------------|--------|----------------------------------------------|
| adept                 | persimmon-8b-chat                        | 8B    | Chat         | 1    | srt-persimmon-8b-chat                        | ✅ Passed      | simo   | Completions only                             |
| Alibaba-NLP           | gme-Qwen2-VL-2B-Instruct                 | 2B    | Vision+Embed | 1    | srt-gme-qwen2-vl-2b-instruct                 | ❌ Failed      | simo   | Vision warmup requires image input           |
| Alibaba-NLP           | gte-Qwen2-7B-instruct                    | 7B    | Embedding    | 1    | srt-gte-qwen2-7b-instruct                    | ✅ Passed      | simo   | Embeddings endpoint                          |
| allenai               | OLMo-2-1124-7B-Instruct                  | 7B    | Instruct     | 1    | srt-olmo-2-1124-7b-instruct                  | ✅ Passed      | simo   | Chat completions                             |
| allenai               | OLMoE-1B-7B-0924                         | 6.92B | MoE          | 1    | srt-olmoe-1b-7b-0924                         | ✅ Passed      | simo   | Completions only                             |
| arcee-ai              | AFM-4.5B-Base                            | 4.5B  | Base         | 1    | srt-afm-4-5b-base                            | ✅ Passed      | simo   | Completions only                             |
| BAAI                  | bge-large-en-v1.5                        | 335M  | Embedding    | 1    | srt-bge-large-en-v1-5                        | ✅ Passed      | simo   | Embeddings endpoint, triton backend          |
| BAAI                  | bge-m3                                   | 567M  | Embedding    | 1    | srt-bge-m3                                   | ✅ Passed      | simo   | Embeddings endpoint                          |
| BAAI                  | bge-reranker-v2-m3                       | 567M  | Reranker     | 1    | srt-bge-reranker-v2-m3                       | ✅ Passed      | simo   | Rerank endpoint                              |
| baichuan-inc          | Baichuan2-7B-Chat                        | 7B    | Chat         | 1    | srt-baichuan2-7b-chat                        | ✅ Passed      | simo   | Completions only                             |
| baichuan-inc          | Baichuan2-13B-Chat                       | 13B   | Chat         | 2    | srt-baichuan2-13b-chat                       | ❌ Failed      | simo   | Warmup timeout with TP=2                     |
| baidu                 | ERNIE-4.5-21B-A3B-PT                     | 21B   | MoE          | -    | srt-ernie-4-5-21b-a3b-pt                     | 🔲 Not Tested | simo   |                                              |
| bigcode               | starcoder2-3b                            | 3B    | Code         | -    | -                                            | 🔲 Not Tested | simo   | No runtime                                   |
| bigcode               | starcoder2-7b                            | 7B    | Code         | 1    | srt-starcoder2-7b                            | ✅ Passed      | simo   | Completions endpoint                         |
| bigcode               | starcoder2-15b                           | 15B   | Code         | 1    | srt-starcoder2-15b                           | ✅ Passed      | simo   | Completions endpoint, CUDA graph disabled    |
| bigscience            | bloomz-7b1                               | 7B    | Base         | 1    | srt-bloomz-7b1                               | ❌ Failed      | simo   | BloomForCausalLM not supported               |
| CofeAI                | Tele-FLM                                 | -     | -            | -    | srt-tele-flm                                 | 🔲 Not Tested | simo   |                                              |
| CohereForAI           | c4ai-command-r-v01                       | 35B   | Chat         | 4    | srt-c4ai-command-r-v01                       | ❌ Failed      | simo   | Controller not reconciling                   |
| databricks            | dbrx-instruct                            | 132B  | MoE          | 8    | srt-dbrx-instruct                            | ❌ Failed      | simo   | Download timeout (262GB)                     |
| databricks            | dolly-v2-12b                             | 12B   | Instruct     | 2    | srt-dolly-v2-12b                             | ❌ Failed      | simo   | GPTNeoXForCausalLM not supported             |
| deepseek-ai           | deepseek-coder-7b-instruct-v1.5          | 7B    | Code         | 1    | srt-deepseek-coder-7b-instruct-v1-5          | ✅ Passed      | simo   | Chat completions                             |
| deepseek-ai           | deepseek-llm-7b-chat                     | 7B    | Chat         | 1    | srt-deepseek-llm-7b-chat                     | ✅ Passed      | simo   | Chat completions                             |
| deepseek-ai           | deepseek-v2-lite-chat                    | 16B   | MoE          | 2    | srt-deepseek-v2-lite-chat                    | ✅ Passed      | simo   | MoE, 2 GPUs                                  |
| deepseek-ai           | DeepSeek-V2                              | 236B  | MoE          | -    | -                                            | 🔲 Not Tested | simo   | No runtime                                   |
| deepseek-ai           | DeepSeek-V2.5                            | 236B  | MoE          | -    | -                                            | 🔲 Not Tested | simo   | No runtime                                   |
| deepseek-ai           | DeepSeek-V3                              | 671B  | MoE          | 32+  | srt-deepseek-v3                              | ❌ Failed      | simo   | CUDA OOM, needs 32+ GPUs                     |
| deepseek-ai           | DeepSeek-V3-0324                         | 671B  | MoE          | -    | srt-deepseek-v3-0324                         | 🔲 Not Tested | simo   |                                              |
| deepseek-ai           | DeepSeek-R1                              | 671B  | MoE          | -    | -                                            | 🔲 Not Tested | simo   | No runtime                                   |
| deepseek-ai           | DeepSeek-R1-Zero                         | 671B  | MoE          | -    | -                                            | 🔲 Not Tested | simo   | No runtime                                   |
| deepseek-ai           | DeepSeek-R1-Distill-Qwen-1.5B            | 1.5B  | Distill      | 1    | srt-deepseek-r1-distill-qwen-1-5b            | ✅ Passed      | simo   | Reasoning model                              |
| deepseek-ai           | DeepSeek-R1-Distill-Qwen-7B              | 7B    | Distill      | 1    | srt-deepseek-r1-distill-qwen-7b              | ✅ Passed      | simo   | Reasoning model                              |
| deepseek-ai           | DeepSeek-R1-Distill-Qwen-14B             | 14B   | Distill      | 2    | srt-deepseek-r1-distill-qwen-14b             | ✅ Passed      | simo   | Reasoning model, TP=2                        |
| deepseek-ai           | DeepSeek-R1-Distill-Qwen-32B             | 32B   | Distill      | 2    | srt-deepseek-r1-distill-qwen-32b             | ✅ Passed      | simo   | Reasoning model                              |
| deepseek-ai           | DeepSeek-R1-Distill-Llama-8B             | 8B    | Distill      | 1    | srt-deepseek-r1-distill-llama-8b             | ✅ Passed      | simo   | Reasoning model                              |
| deepseek-ai           | DeepSeek-R1-Distill-Llama-70B            | 70B   | Distill      | 4    | srt-deepseek-r1-distill-llama-70b            | ✅ Passed      | simo   | Reasoning model, TP=4                        |
| deepseek-ai           | deepseek-vl2                             | -     | VLM          | -    | srt-deepseek-vl2                             | 🔲 Not Tested | simo   |                                              |
| deepseek-ai           | Janus-Pro-7B                             | 7B    | VLM          | 1    | srt-janus-pro-7b                             | ✅ Passed      | simo   | Vision-language model                        |
| Efficient-Large-Model | NVILA-8B                                 | 8B    | VLM          | -    | srt-nvila-8b                                 | 🔲 Not Tested | simo   |                                              |
| EleutherAI            | gpt-j-6b                                 | 6B    | Base         | 1    | srt-gpt-j-6b                                 | ❌ Failed      | simo   | GPTJForCausalLM not supported                |
| google                | gemma-2b                                 | 2B    | Base         | -    | -                                            | 🔲 Not Tested | simo   | No runtime                                   |
| google                | gemma-7b                                 | 7B    | Base         | -    | -                                            | 🔲 Not Tested | simo   | No runtime                                   |
| google                | gemma-2-2b                               | 2B    | Base         | -    | -                                            | 🔲 Not Tested | simo   | No runtime                                   |
| google                | gemma-2-2b-it                            | 2B    | Instruct     | 1    | srt-gemma-2-2b-it                            | ✅ Passed      | simo   | Chat completions                             |
| google                | gemma-2-9b                               | 9B    | Base         | -    | -                                            | 🔲 Not Tested | simo   | No runtime                                   |
| google                | gemma-2-9b-it                            | 9B    | Instruct     | 1    | srt-gemma-2-9b-it                            | ✅ Passed      | simo   | Chat completions                             |
| google                | gemma-2-27b                              | 27B   | Base         | -    | -                                            | 🔲 Not Tested | simo   | No runtime                                   |
| google                | gemma-2-27b-it                           | 27B   | Instruct     | 2    | srt-gemma-2-27b-it                           | ✅ Passed      | simo   | TP=2, generate endpoint                      |
| google                | gemma-3-1b-it                            | 1B    | Instruct     | 1    | srt-gemma-3-1b-it                            | ✅ Passed      | simo   | Chat completions                             |
| google                | gemma-3-4b-it                            | 4B    | Instruct     | 1    | srt-gemma-3-4b-it                            | ✅ Passed      | simo   | Chat completions                             |
| google                | gemma-3-12b-it                           | 12B   | Instruct     | 2    | srt-gemma-3-12b-it                           | ✅ Passed      | simo   | TP=2, mem-frac 0.75                          |
| google                | gemma-3-27b-it                           | 27B   | Instruct     | -    | -                                            | 🔲 Not Tested | simo   | No runtime                                   |
| HuggingFaceTB         | SmolLM-135M                              | 135M  | Base         | -    | -                                            | 🔲 Not Tested | keyang | No runtime                                   |
| HuggingFaceTB         | SmolLM-360M                              | 360M  | Base         | -    | -                                            | 🔲 Not Tested | keyang | No runtime                                   |
| HuggingFaceTB         | SmolLM-1.7B                              | 1.7B  | Base         | 1    | srt-smollm-1-7b                              | ✅ Passed      | keyang | Completions endpoint                         |
| HuggingFaceTB         | SmolLM2-1.7B-Instruct                    | 1.7B  | Instruct     | -    | srt-smollm2-1-7b-instruct                    | 🔲 Not Tested | keyang |                                              |
| ibm-granite           | granite-3.0-2b-instruct                  | 2B    | Instruct     | -    | -                                            | 🔲 Not Tested | keyang | No runtime                                   |
| ibm-granite           | granite-3.0-3b-a800m-instruct            | 3B    | MoE          | 1    | srt-granite-3-0-3b-a800m-instruct            | ✅ Passed      | keyang | Native /generate                             |
| ibm-granite           | granite-3.0-8b-instruct                  | 8B    | Instruct     | -    | -                                            | 🔲 Not Tested | keyang | No runtime                                   |
| ibm-granite           | granite-3.1-2b-instruct                  | 2B    | Instruct     | -    | -                                            | 🔲 Not Tested | keyang | No runtime                                   |
| ibm-granite           | granite-3.1-8b-instruct                  | 8B    | Instruct     | 1    | srt-granite-3-1-8b-instruct                  | ✅ Passed      | keyang | Chat completions                             |
| inclusionAI           | Ling-lite                                | -     | -            | -    | srt-ling-lite                                | 🔲 Not Tested | keyang |                                              |
| inclusionAI           | Ling-plus                                | -     | -            | -    | srt-ling-plus                                | 🔲 Not Tested | keyang |                                              |
| internlm              | internlm2-7b                             | 7B    | Base         | 1    | srt-internlm2-7b                             | ✅ Passed      | keyang | Completions only                             |
| internlm              | internlm2-7b-reward                      | 7B    | Reward       | 1    | srt-internlm2-7b-reward                      | ❌ Failed      | keyang | Download timeout                             |
| internlm              | internlm2-20b                            | 20B   | Base         | 2    | srt-internlm2-20b                            | ✅ Passed      | keyang | Completions only                             |
| intfloat              | e5-mistral-7b-instruct                   | 7B    | Embedding    | -    | srt-e5-mistral-7b-instruct                   | 🔲 Not Tested | keyang |                                              |
| jason9693             | Qwen2.5-1.5B-apeach                      | 1.5B  | Instruct     | -    | -                                            | 🔲 Not Tested | keyang | No runtime                                   |
| jet-ai                | Jet-Nemotron-2B                          | 2B    | Chat         | 1    | srt-jet-nemotron-2b                          | ✅ Passed      | keyang | Chat completions                             |
| LGAI-EXAONE           | EXAONE-3.5-7.8B-Instruct                 | 7.8B  | Instruct     | 1    | srt-exaone-3-5-7-8b-instruct                 | ❌ Failed      | keyang | Download timeout                             |
| liuhaotian            | llava-v1.5-7b                            | 7B    | VLM          | -    | -                                            | 🔲 Not Tested | keyang | No runtime                                   |
| liuhaotian            | llava-v1.5-13b                           | 13B   | VLM          | -    | srt-llava-v1-5-13b                           | 🔲 Not Tested | keyang |                                              |
| liuhaotian            | llava-v1.6-vicuna-7b                     | 7B    | VLM          | -    | -                                            | 🔲 Not Tested | keyang | No runtime                                   |
| liuhaotian            | llava-v1.6-vicuna-13b                    | 13B   | VLM          | -    | -                                            | 🔲 Not Tested | keyang | No runtime                                   |
| lmms-lab              | llava-next-8b                            | 8B    | VLM          | -    | -                                            | 🔲 Not Tested | keyang | No runtime                                   |
| lmms-lab              | llava-next-72b                           | 72B   | VLM          | -    | srt-llava-next-72b                           | 🔲 Not Tested | keyang |                                              |
| lmms-lab              | llava-onevision-qwen2-7b-ov              | 7B    | VLM          | -    | srt-llava-onevision-qwen2-7b-ov              | 🔲 Not Tested | keyang |                                              |
| lmsys                 | vicuna-7b-v1-5                           | 7B    | Chat         | 1    | srt-vicuna-7b-v1-5                           | ✅ Passed      | keyang | Chat completions                             |
| lmsys                 | vicuna-13b-v1-5                          | 13B   | Chat         | 2    | srt-vicuna-13b-v1-5                          | ✅ Passed      | keyang | Chat completions, TP=2                       |
| meta                  | Llama-2-7b-hf                            | 7B    | Base         | 1    | srt-llama-2-7b                               | ✅ Passed      | keyang | Completions endpoint                         |
| meta                  | Llama-2-7b-chat-hf                       | 7B    | Chat         | 1    | srt-llama-2-7b-chat-hf                       | ✅ Passed      | keyang | Chat completions                             |
| meta                  | Llama-2-13b-hf                           | 13B   | Base         | 1    | srt-llama-2-13b                              | ✅ Passed      | keyang | Completions endpoint                         |
| meta                  | Llama-2-13b-chat-hf                      | 13B   | Chat         | 1    | srt-llama-2-13b-chat-hf                      | ✅ Passed      | keyang | Chat completions                             |
| meta                  | Llama-2-70b-hf                           | 70B   | Base         | 4    | srt-llama-2-70b                              | ✅ Passed      | keyang | Completions, TP=4                            |
| meta                  | Llama-2-70b-chat-hf                      | 70B   | Chat         | 4    | srt-llama-2-70b-chat-hf                      | ✅ Passed      | keyang | Chat completions, TP=4                       |
| meta                  | Meta-Llama-3-8B-Instruct                 | 8B    | Instruct     | 1    | srt-llama-3-8b-instruct                      | ✅ Passed      | keyang | Chat completions                             |
| meta                  | Llama-3-70B-Instruct                     | 70B   | Instruct     | 4    | srt-llama-3-70b-instruct                     | ✅ Passed      | keyang | Chat completions, TP=4                       |
| meta                  | Llama-3.1-8B-Instruct                    | 8B    | Instruct     | 1    | srt-llama-3-1-8b-instruct                    | ✅ Passed      | keyang | Chat completions                             |
| meta                  | Llama-3.1-70B-Instruct                   | 70B   | Instruct     | 4    | srt-llama-3-1-70b-instruct                   | ✅ Passed      | keyang | Chat completions, TP=4                       |
| meta                  | Llama-3.1-405B-Instruct-FP8              | 405B  | Instruct     | 8    | srt-llama-3-1-405b-instruct-fp8              | ❌ Failed      | keyang | NaN during inference (FP8 issue)             |
| meta                  | Llama-3.2-1B-Instruct                    | 1B    | Instruct     | 1    | srt-llama-3-2-1b-instruct                    | ✅ Passed      | keyang | Chat completions                             |
| meta                  | Llama-3.2-3B-Instruct                    | 3B    | Instruct     | 1    | srt-llama-3-2-3b-instruct                    | ✅ Passed      | keyang | Chat completions                             |
| meta                  | Llama-3.2-11B-Vision-Instruct            | 11B   | VLM          | 2    | srt-llama-3-2-11b-vision-instruct            | ❌ Failed      | keyang | Download timeout (gated)                     |
| meta                  | Llama-3.2-90B-Vision-Instruct            | 90B   | VLM          | -    | srt-llama-3-2-90b-vision-instruct            | 🔲 Not Tested | keyang |                                              |
| meta                  | Llama-3.2-90B-Vision-Instruct-FP8        | 90B   | VLM          | -    | srt-llama-3-2-90b-vision-instruct-fp8        | 🔲 Not Tested | keyang |                                              |
| meta                  | Llama-3.3-70B-instruct                   | 70B   | Instruct     | 4    | srt-llama-3-3-70b-instruct                   | ✅ Passed      | keyang | Chat completions, TP=4                       |
| meta                  | Llama-3.3-70B-Instruct-FP8-dynamic       | 70B   | Instruct     | -    | srt-llama-3-3-70b-instruct-fp8-dynamic       | 🔲 Not Tested | keyang |                                              |
| meta                  | Llama-4-Scout-17B-16E-Instruct           | 109B  | MoE          | 4    | srt-llama-4-scout-17b-16e-instruct           | ✅ Passed      | keyang | Chat completions, TP=4                       |
| meta                  | Llama-4-Maverick-17B-128E-Instruct       | 401B  | MoE          | 16   | srt-llama-4-maverick-17b-128e-instruct       | ❌ Failed      | keyang | CUDA OOM, needs 16 GPUs                      |
| meta                  | Llama-4-Maverick-17B-128E-Instruct-FP8   | 401B  | MoE          | 8    | srt-llama-4-maverick-17b-128e-instruct-fp8   | ✅ Passed      | keyang | Chat completions, TP=8, FP8                  |
| meta                  | Llama-Guard-3-8B                         | 8B    | Guard        | 1    | srt-llama-guard-3-8b                         | ✅ Passed      | keyang | Content moderation                           |
| microsoft             | phi-1_5                                  | 1.3B  | Base         | -    | -                                            | 🔲 Not Tested | keyang | No runtime                                   |
| microsoft             | phi-2                                    | 2.8B  | Base         | 1    | srt-phi-2                                    | ❌ Failed      | keyang | vllm module missing                          |
| microsoft             | Phi-3-mini-4k-instruct                   | 3.8B  | Instruct     | 1    | srt-phi-3-mini-4k-instruct                   | ❌ Failed      | beiwen | vllm._custom_ops missing                     |
| microsoft             | Phi-3-mini-128k-instruct                 | 3.8B  | Instruct     | -    | -                                            | 🔲 Not Tested | beiwen | No runtime                                   |
| microsoft             | Phi-3-small-8k-instruct                  | 7B    | Instruct     | -    | -                                            | 🔲 Not Tested | beiwen | No runtime                                   |
| microsoft             | Phi-3-medium-4k-instruct                 | 14B   | Instruct     | -    | -                                            | 🔲 Not Tested | beiwen | No runtime                                   |
| microsoft             | Phi-3-vision-128k-instruct               | 4.2B  | VLM          | -    | srt-phi-3-vision-128k-instruct               | 🔲 Not Tested | beiwen |                                              |
| microsoft             | Phi-3.5-mini-instruct                    | 3.8B  | Instruct     | 1    | srt-phi-3-5-mini-instruct                    | ✅ Passed      | beiwen | Triton backend required                      |
| microsoft             | Phi-3.5-MoE-instruct                     | 41.9B | MoE          | 4    | srt-phi-3-5-moe-instruct                     | ✅ Passed      | beiwen | TP=4                                         |
| microsoft             | phi-4                                    | 14B   | Instruct     | 1    | srt-phi-4                                    | ✅ Passed      | beiwen | Chat completions                             |
| microsoft             | Phi-4-mini-instruct                      | 3.8B  | Instruct     | 1    | srt-phi-4-mini-instruct                      | ✅ Passed      | beiwen | Chat completions                             |
| microsoft             | Phi-4-multimodal-instruct                | 5.6B  | VLM          | 1    | srt-phi-4-multimodal-instruct                | ✅ Passed      | beiwen | Chat completions                             |
| minimax               | MiniMax-M2                               | -     | -            | -    | srt-minimax-m2                               | 🔲 Not Tested | beiwen |                                              |
| mistralai             | Mistral-7B-v0.1                          | 7B    | Base         | 1    | -                                            | ❌ Failed      | beiwen | Download timeout, no runtime                 |
| mistralai             | Mistral-7B-Instruct-v0.2                 | 7B    | Instruct     | 1    | srt-mistral-7b-instruct-v0-2                 | ✅ Passed      | beiwen | Chat completions                             |
| mistralai             | Mistral-7B-Instruct-v0.3                 | 7B    | Instruct     | 1    | srt-mistral-7b-instruct-v0-3                 | ✅ Passed      | beiwen | Chat completions                             |
| mistralai             | Mistral-Nemo-Instruct-2407               | 12B   | Instruct     | 2    | srt-mistral-nemo-instruct-2407               | ✅ Passed      | beiwen | Chat completions                             |
| mistralai             | Mistral-Small-3.1-24B-Instruct-2503      | 24B   | Instruct     | 2    | srt-mistral-small-3-1-24b-instruct-2503      | ❌ Failed      | beiwen | Download timeout (48GB)                      |
| mistralai             | Mixtral-8x7B-v0.1                        | 47B   | MoE          | 4    | srt-mixtral-8x7b-v0-1                        | ✅ Passed      | beiwen | Chat completions, TP=4                       |
| mistralai             | Mixtral-8x7B-Instruct-v0.1               | 47B   | MoE          | 4    | srt-mixtral-8x7b-instruct-v0-1               | ✅ Passed      | beiwen | Chat completions, TP=4                       |
| mistralai             | Mixtral-8x22B-v0.1                       | 141B  | MoE          | 8    | srt-mixtral-8x22b-v0-1                       | ✅ Passed      | beiwen | Chat completions, TP=8                       |
| moonshotai            | Kimi-K2-Instruct                         | -     | Instruct     | -    | srt-kimi-k2-instruct                         | 🔲 Not Tested | beiwen |                                              |
| moonshotai            | Kimi-VL-A3B-Instruct                     | -     | VLM          | -    | srt-kimi-vl-a3b-instruct                     | 🔲 Not Tested | beiwen |                                              |
| mosaicml              | mpt-7b                                   | 7B    | Base         | 1    | srt-mpt-7b                                   | ❌ Failed      | beiwen | Download timeout                             |
| NousResearch          | hermes-2-pro-llama-3-8b                  | 8B    | Instruct     | 1    | srt-hermes-2-pro-llama-3-8b                  | ❌ Failed      | beiwen | Download timeout                             |
| nvidia                | Llama-3.1-Nemotron-Nano-8B-v1            | 8B    | Instruct     | 1    | srt-llama-3-1-nemotron-nano-8b-v1            | ✅ Passed      | beiwen | Chat completions                             |
| nvidia                | Llama-3.1-Nemotron-70B-Instruct-HF       | 70B   | Instruct     | 4    | srt-llama-3-1-nemotron-70b-instruct-hf       | ✅ Passed      | beiwen | Chat completions, TP=4                       |
| nvidia                | Llama-3_1-Nemotron-Ultra-253B-v1         | 253B  | Instruct     | 8    | srt-llama-3-1-nemotron-ultra-253b-v1         | ❌ Failed      | beiwen | Gated model download failed                  |
| nvidia                | Llama-3_3-Nemotron-Super-49B-v1          | 50B   | Instruct     | 4    | srt-llama-3-3-nemotron-super-49b-v1          | ✅ Passed      | beiwen | Chat completions, TP=4                       |
| nvidia                | NVIDIA-Nemotron-Nano-9B-v2               | 9B    | Base         | 1    | srt-nvidia-nemotron-nano-9b-v2               | ❌ Failed      | beiwen | KV cache memory failure                      |
| nvidia                | NVIDIA-Nemotron-3-Nano-30B-A3B-Base-BF16 | 30B   | Base         | -    | srt-nvidia-nemotron-3-nano-30b-a3b-base-bf16 | 🔲 Not Tested | beiwen |                                              |
| nvidia                | NVIDIA-Nemotron-3-Nano-30B-A3B-BF16      | 30B   | Instruct     | -    | srt-nvidia-nemotron-3-nano-30b-a3b-bf16      | 🔲 Not Tested | beiwen |                                              |
| nvidia                | NVIDIA-Nemotron-3-Nano-30B-A3B-FP8       | 30B   | Instruct     | -    | srt-nvidia-nemotron-3-nano-30b-a3b-fp8       | 🔲 Not Tested | beiwen |                                              |
| nvidia                | NVIDIA-Nemotron-Nano-12B-v2-VL-BF16      | 12B   | VLM          | -    | srt-nvidia-nemotron-nano-12b-v2-vl-bf16      | 🔲 Not Tested | beiwen |                                              |
| nvidia                | NVIDIA-Nemotron-Nano-12B-v2-VL-FP8       | 12B   | VLM          | -    | srt-nvidia-nemotron-nano-12b-v2-vl-fp8       | 🔲 Not Tested | beiwen |                                              |
| openai                | clip-vit-large-patch14-336               | -     | Vision       | -    | srt-clip-vit-large-patch14-336               | 🔲 Not Tested | beiwen |                                              |
| openai                | gpt-oss-20b                              | 20B   | Base         | -    | srt-gpt-oss-20b                              | 🔲 Not Tested | beiwen |                                              |
| openai                | gpt-oss-120b                             | 120B  | Base         | -    | srt-gpt-oss-120b                             | 🔲 Not Tested | beiwen |                                              |
| openbmb               | MiniCPM-2B-sft-bf16                      | 2B    | LLM          | 1    | srt-minicpm-2b-sft-bf16                      | ❌ Failed      | beiwen | Scheduler exception in warmup                |
| openbmb               | MiniCPM3-4B                              | 4B    | Chat         | 1    | srt-minicpm3-4b                              | ✅ Passed      | beiwen | Triton backend, CUDA graph disabled          |
| openbmb               | MiniCPM-V-2_6                            | 8B    | VLM          | 1    | srt-minicpm-v-2-6                            | ❌ Failed      | beiwen | HuggingFace license required                 |
| OpenGVLab             | InternVL2_5-8B                           | 8B    | VLM          | 1    | srt-internvl2-5-8b                           | ✅ Passed      | beiwen | Chat completions                             |
| OrionStarAI           | Orion-14B-Base                           | 14B   | Base         | 2    | srt-orion-14b-base                           | ✅ Passed      | beiwen | Completions only                             |
| Qwen                  | Qwen-7B-Chat                             | 7B    | Chat         | 1    | srt-qwen-7b-chat                             | ✅ Passed      | beiwen | Completions only (legacy)                    |
| Qwen                  | Qwen-VL                                  | -     | VLM          | -    | -                                            | 🔲 Not Tested | beiwen | No runtime                                   |
| Qwen                  | Qwen-VL-Chat                             | -     | VLM          | -    | -                                            | 🔲 Not Tested | beiwen | No runtime                                   |
| Qwen                  | Qwen-Image                               | -     | Vision       | -    | srt-qwen-image                               | 🔲 Not Tested | beiwen |                                              |
| Qwen                  | Qwen1.5-7B-Chat                          | 7B    | Chat         | 1    | srt-qwen1-5-7b-chat                          | ✅ Passed      | beiwen | Chat completions                             |
| Qwen                  | Qwen1.5-32B-Chat                         | 32B   | Chat         | 4    | srt-qwen1-5-32b-chat                         | ✅ Passed      | beiwen | Chat completions, TP=4                       |
| Qwen                  | Qwen1.5-72B-Chat                         | 72B   | Chat         | 8    | srt-qwen1-5-72b-chat                         | ✅ Passed      | beiwen | Chat completions, TP=8                       |
| Qwen                  | Qwen1.5-110B-Chat                        | 110B  | Chat         | 8    | srt-qwen1-5-110b-chat                        | ✅ Passed      | beiwen | Chat completions, TP=8                       |
| Qwen                  | Qwen2-7B-Instruct                        | 7B    | Instruct     | 1    | srt-qwen2-7b-instruct                        | ✅ Passed      | beiwen | Chat completions                             |
| Qwen                  | Qwen2-72B-Instruct                       | 72B   | Instruct     | 8    | srt-qwen2-72b-instruct                       | ✅ Passed      | beiwen | Chat completions, TP=8                       |
| Qwen                  | Qwen2-VL-2B-Instruct                     | 2B    | VLM          | -    | -                                            | 🔲 Not Tested | xinyue | No runtime                                   |
| Qwen                  | Qwen2-VL-7B-Instruct                     | 7B    | VLM          | 1    | srt-qwen2-vl-7b-instruct                     | ✅ Passed      | xinyue | Vision model                                 |
| Qwen                  | Qwen2-VL-72B-Instruct                    | 72B   | VLM          | -    | -                                            | 🔲 Not Tested | xinyue | No runtime                                   |
| Qwen                  | Qwen2.5-0.5B                             | 0.5B  | Base         | -    | -                                            | 🔲 Not Tested | xinyue | No runtime                                   |
| Qwen                  | Qwen2.5-1.5B                             | 1.5B  | Base         | 1    | srt-qwen2-5-1-5b                             | ✅ Passed      | xinyue | Chat completions                             |
| Qwen                  | Qwen2.5-3B                               | 3B    | Base         | -    | srt-qwen2-5-3b                               | 🔲 Not Tested | xinyue |                                              |
| Qwen                  | Qwen2.5-3B-Instruct                      | 3B    | Instruct     | 1    | srt-qwen2-5-3b-instruct                      | ✅ Passed      | xinyue | Chat completions                             |
| Qwen                  | Qwen2.5-7B                               | 7B    | Base         | 1    | srt-qwen2-5-7b                               | ✅ Passed      | xinyue | Chat completions                             |
| Qwen                  | Qwen2.5-14B                              | 14B   | Base         | -    | srt-qwen2-5-14b                              | 🔲 Not Tested | xinyue |                                              |
| Qwen                  | Qwen2.5-14B-Instruct                     | 14B   | Instruct     | 2    | srt-qwen2-5-14b-instruct                     | ✅ Passed      | xinyue | Chat completions, TP=2                       |
| Qwen                  | Qwen2.5-32B                              | 32B   | Base         | -    | -                                            | 🔲 Not Tested | xinyue | No runtime                                   |
| Qwen                  | Qwen2.5-32B-Instruct                     | 32B   | Instruct     | 4    | srt-qwen2-5-32b-instruct                     | ✅ Passed      | xinyue | Chat completions, TP=4                       |
| Qwen                  | Qwen2.5-72B                              | 72B   | Base         | -    | -                                            | 🔲 Not Tested | xinyue | No runtime                                   |
| Qwen                  | Qwen2.5-72B-Instruct                     | 72B   | Instruct     | 8    | srt-qwen2-5-72b-instruct                     | ✅ Passed      | xinyue | Chat completions, TP=8                       |
| Qwen                  | Qwen2.5-Coder-7B-Instruct                | 7B    | Code         | 1    | srt-qwen2-5-coder-7b-instruct                | ✅ Passed      | xinyue | Chat completions                             |
| Qwen                  | Qwen2.5-Coder-32B-Instruct               | 32B   | Code         | 4    | srt-qwen2-5-coder-32b-instruct               | ✅ Passed      | xinyue | Chat completions, TP=4                       |
| Qwen                  | Qwen2.5-Math-RM-72B                      | 72B   | Reward       | -    | -                                            | 🔲 Not Tested | xinyue | No runtime                                   |
| Qwen                  | Qwen2.5-VL-7B-Instruct                   | 7B    | VLM          | 1    | srt-qwen2-5-vl-7b-instruct                   | ✅ Passed      | xinyue | Vision model                                 |
| Qwen                  | Qwen3-0.6B                               | 0.6B  | Base         | 1    | srt-qwen3-0-6b                               | ⏭️ Skipped    | xinyue | Missing isvc                                 |
| Qwen                  | Qwen3-1.7B                               | 1.7B  | Base         | -    | -                                            | 🔲 Not Tested | xinyue | No runtime                                   |
| Qwen                  | Qwen3-4B                                 | 4B    | Base         | -    | srt-qwen3-4b                                 | 🔲 Not Tested | xinyue |                                              |
| Qwen                  | Qwen3-8B                                 | 8B    | Base         | 2    | srt-qwen3-8b                                 | ✅ Passed      | xinyue | Chat completions, reasoning                  |
| Qwen                  | Qwen3-14B                                | 14B   | Base         | -    | -                                            | 🔲 Not Tested | xinyue | No runtime                                   |
| Qwen                  | Qwen3-30B-A3B                            | 30B   | MoE          | -    | srt-qwen3-30b-a3b                            | 🔲 Not Tested | xinyue |                                              |
| Qwen                  | Qwen3-32B                                | 32B   | Base         | 4    | srt-qwen3-32b                                | ✅ Passed      | xinyue | Chat completions, TP=4                       |
| Qwen                  | Qwen3-Embedding-0.6B                     | 0.6B  | Embedding    | 1    | srt-qwen3-embedding-0-6b                     | ✅ Passed      | xinyue | Text generation                              |
| Qwen                  | Qwen3-Embedding-4B                       | 4B    | Embedding    | -    | srt-qwen3-embedding-4b                       | 🔲 Not Tested | xinyue |                                              |
| Qwen                  | Qwen3-Embedding-8B                       | 8B    | Embedding    | -    | -                                            | 🔲 Not Tested | xinyue | No runtime                                   |
| Qwen                  | Qwen3-Next-80B-A3B-Instruct              | 80B   | MoE          | -    | srt-qwen3-next-80b-a3b-instruct              | 🔲 Not Tested | xinyue |                                              |
| Qwen                  | Qwen3-VL-235B-A22B-Instruct              | 235B  | VLM          | -    | srt-qwen3-vl-235b-a22b-instruct              | 🔲 Not Tested | xinyue |                                              |
| rednote-hilab         | dots.ocr                                 | -     | Vision       | -    | srt-dots-ocr                                 | 🔲 Not Tested | xinyue |                                              |
| rednote-hilab         | dots.vlm1.inst                           | -     | VLM          | -    | srt-dots-vlm1-inst                           | 🔲 Not Tested | xinyue |                                              |
| Salesforce            | codegen-16B-multi                        | 16B   | Code         | 1    | -                                            | ❌ Failed      | xinyue | CodeGenForCausalLM not supported, no runtime |
| Salesforce            | xgen-7b-8k-inst                          | 7B    | Instruct     | 1    | srt-xgen-7b-8k-inst                          | ❌ Failed      | xinyue | PyTorch bin format (needs safetensors)       |
| Skywork               | Skywork-OR1-7B-Preview                   | 7B    | Reasoning    | 1    | srt-skywork-or1-7b-preview                   | ✅ Passed      | xinyue | Chat completions                             |
| Skywork               | Skywork-Reward-Gemma-2-27B-v0.2          | 27B   | Reward       | -    | -                                            | 🔲 Not Tested | xinyue | No runtime                                   |
| Skywork               | Skywork-Reward-Llama-3.1-8B-v0.2         | 8B    | Reward       | -    | -                                            | 🔲 Not Tested | xinyue | No runtime                                   |
| stabilityai           | stablelm-tuned-alpha-7b                  | 7B    | Chat         | 1    | srt-stablelm-tuned-alpha-7b                  | ❌ Failed      | xinyue | GPTNeoXForCausalLM not supported             |
| stabilityai           | stablelm-2-12b-chat                      | 12B   | Chat         | 2    | srt-stablelm-2-12b-chat                      | ❌ Failed      | xinyue | StableLmForCausalLM not supported            |
| THUDM                 | chatglm2-6b                              | 6B    | Chat         | 1    | srt-chatglm2-6b                              | ❌ Failed      | xinyue | ChatGLMTokenizer incompatible                |
| tiiuae                | falcon-7b-instruct                       | 7B    | Instruct     | 1    | srt-falcon-7b-instruct                       | ❌ Failed      | xinyue | FalconForCausalLM not supported              |
| tiiuae                | Falcon3-10B-Instruct                     | 10B   | Instruct     | 1    | srt-falcon3-10b-instruct                     | ✅ Passed      | xinyue | Chat completions                             |
| unsloth               | Llama-3.2-11B-Vision-Instruct            | 11B   | VLM          | 1    | srt-llama-3-2-11b-vision-instruct            | ✅ Passed      | xinyue | Chat completions                             |
| upstage               | SOLAR-10.7B-Instruct-v1.0                | 10.7B | Instruct     | 1    | srt-solar-10-7b-instruct-v1-0                | ✅ Passed      | xinyue | Completions only                             |
| xai-org               | grok-1                                   | 314B  | MoE          | -    | srt-grok-1                                   | ⏭️ Skipped    | xinyue | Config not found                             |
| xai-org               | grok-2                                   | 269B  | Chat         | 8    | srt-grok-2                                   | ✅ Passed      | xinyue | Chat completions, TP=8                       |
| XiaomiMiMo            | MiMo-7B-RL                               | 7B    | Chat         | 1    | srt-mimo-7b-rl                               | ✅ Passed      | xinyue | Reasoning model                              |
| XiaomiMiMo            | MiMo-VL-7B-RL                            | 7B    | VLM          | 1    | srt-mimo-vl-7b-rl                            | ❌ Failed      | xinyue | Download timeout                             |
| xverse                | XVERSE-MoE-A36B                          | 36B   | MoE          | -    | srt-xverse-moe-a36b                          | 🔲 Not Tested | xinyue |                                              |
| zai-org               | GLM-4.5V                                 | -     | VLM          | -    | srt-glm-4-5v                                 | 🔲 Not Tested | xinyue |                                              |
| ZhipuAI               | glm-4-9b-chat                            | 9.4B  | Chat         | 1    | srt-glm-4-9b-chat                            | ✅ Passed      | xinyue | Chat completions                             |

---

## Status Legend

| Symbol | Status     | Description                                    |
|--------|------------|------------------------------------------------|
| ✅      | Passed     | Model tested successfully, inference working   |
| ❌      | Failed     | Model tested but failed (see notes for reason) |
| ⏭️     | Skipped    | Model skipped due to config/access issues      |
| 🔲     | Not Tested | Model has YAML config but not yet tested       |

---

## Runtime Coverage

| Category | Count |
|----------|-------|
| Models with runtime | 163 |
| Models without runtime | 40 |

**Models missing runtimes:**
- bigcode/starcoder2-3b
- deepseek-ai/DeepSeek-V2, DeepSeek-V2.5, DeepSeek-R1, DeepSeek-R1-Zero
- google/gemma-2b, gemma-7b, gemma-2-2b, gemma-2-9b, gemma-2-27b, gemma-3-27b-it
- HuggingFaceTB/SmolLM-135M, SmolLM-360M
- ibm-granite/granite-3.0-2b-instruct, granite-3.0-8b-instruct, granite-3.1-2b-instruct
- jason9693/Qwen2.5-1.5B-apeach
- liuhaotian/llava-v1.5-7b, llava-v1.6-vicuna-7b, llava-v1.6-vicuna-13b
- lmms-lab/llava-next-8b
- microsoft/phi-1_5, Phi-3-mini-128k-instruct, Phi-3-small-8k-instruct, Phi-3-medium-4k-instruct
- mistralai/Mistral-7B-v0.1
- Qwen/Qwen-VL, Qwen-VL-Chat, Qwen2-VL-2B-Instruct, Qwen2-VL-72B-Instruct, Qwen2.5-0.5B, Qwen2.5-32B, Qwen2.5-72B, Qwen2.5-Math-RM-72B, Qwen3-1.7B, Qwen3-14B, Qwen3-Embedding-8B
- Salesforce/codegen-16B-multi
- Skywork/Skywork-Reward-Gemma-2-27B-v0.2, Skywork-Reward-Llama-3.1-8B-v0.2

---

## Model Architectures

### Supported Text Generation Architectures

| Architecture              | Models                                                                                                                | Count |
|---------------------------|-----------------------------------------------------------------------------------------------------------------------|-------|
| **LlamaForCausalLM**      | Llama-2, Llama-3, Llama-3.1, Llama-3.2, Llama-3.3, Llama-4, vicuna, Nemotron, DeepSeek-R1-Distill-Llama, hermes-2-pro | ~35   |
| **Qwen2ForCausalLM**      | Qwen2, Qwen2.5, Qwen3, DeepSeek-R1-Distill-Qwen                                                                       | ~30   |
| **QWenLMHeadModel**       | Qwen, Qwen1.5                                                                                                         | 5     |
| **MistralForCausalLM**    | Mistral-7B variants, Mistral-Nemo, Mistral-Small                                                                      | 5     |
| **MixtralForCausalLM**    | Mixtral-8x7B, Mixtral-8x22B                                                                                           | 3     |
| **Gemma2ForCausalLM**     | gemma-2, gemma-3                                                                                                      | 10    |
| **GemmaForCausalLM**      | gemma (original)                                                                                                      | 2     |
| **Phi3ForCausalLM**       | Phi-3, Phi-3.5, Phi-4                                                                                                 | 8     |
| **DeepseekV2ForCausalLM** | DeepSeek-V2, DeepSeek-V3, DeepSeek-R1                                                                                 | 8     |
| **DeepseekForCausalLM**   | deepseek-llm, deepseek-coder                                                                                          | 2     |
| **InternLM2ForCausalLM**  | internlm2-7b, internlm2-20b                                                                                           | 3     |
| **ChatGLMModel**          | glm-4, chatglm2                                                                                                       | 2     |
| **StarCoder2ForCausalLM** | starcoder2-3b, starcoder2-7b, starcoder2-15b                                                                          | 3     |
| **OLMoForCausalLM**       | OLMo-2                                                                                                                | 1     |
| **OlmoeForCausalLM**      | OLMoE-1B-7B                                                                                                           | 1     |
| **BaichuanForCausalLM**   | Baichuan2-7B, Baichuan2-13B                                                                                           | 2     |
| **CohereForCausalLM**     | c4ai-command-r                                                                                                        | 1     |
| **GraniteForCausalLM**    | granite-3.0, granite-3.1                                                                                              | 5     |
| **ExaoneForCausalLM**     | EXAONE-3.5                                                                                                            | 1     |
| **OrionForCausalLM**      | Orion-14B                                                                                                             | 1     |
| **MiniCPMForCausalLM**    | MiniCPM-2B, MiniCPM3-4B                                                                                               | 2     |
| **Falcon3ForCausalLM**    | Falcon3-10B                                                                                                           | 1     |
| **DbrxForCausalLM**       | dbrx-instruct                                                                                                         | 1     |
| **PersimmonForCausalLM**  | persimmon-8b                                                                                                          | 1     |
| **MptForCausalLM**        | mpt-7b                                                                                                                | 1     |
| **SOLAR**                 | SOLAR-10.7B                                                                                                           | 1     |
| **XGen**                  | xgen-7b                                                                                                               | 1     |
| **Grok**                  | grok-1, grok-2                                                                                                        | 2     |
| **MiMo**                  | MiMo-7B-RL                                                                                                            | 1     |

### Vision-Language Architectures

| Architecture                        | Models                                              | Count |
|-------------------------------------|-----------------------------------------------------|-------|
| **Qwen2VLForConditionalGeneration** | Qwen2-VL, Qwen2.5-VL, Qwen3-VL, gme-Qwen2-VL        | 6     |
| **InternVLChatModel**               | InternVL2.5-8B                                      | 1     |
| **LlavaForConditionalGeneration**   | llava-v1.5, llava-v1.6, llava-next, llava-onevision | 7     |
| **MllamaForConditionalGeneration**  | Llama-3.2-Vision (11B, 90B)                         | 4     |
| **DeepSeekVLV2**                    | deepseek-vl2                                        | 1     |
| **JanusForConditionalGeneration**   | Janus-Pro-7B                                        | 1     |
| **MiniCPMV**                        | MiniCPM-V-2_6                                       | 1     |
| **Phi3VForCausalLM**                | Phi-3-vision, Phi-4-multimodal                      | 2     |
| **NVILA**                           | NVILA-8B                                            | 1     |
| **NemotronVL**                      | Nemotron-Nano-12B-VL                                | 2     |
| **MiMoVL**                          | MiMo-VL-7B                                          | 1     |
| **CLIPVisionModel**                 | clip-vit-large                                      | 1     |

### Embedding/Reranker Architectures

| Architecture                       | Models                             | Count |
|------------------------------------|------------------------------------|-------|
| **XLMRobertaModel**                | bge-large-en, bge-m3, bge-reranker | 3     |
| **MistralModel**                   | e5-mistral-7b                      | 1     |
| **Qwen2ForSequenceClassification** | gte-Qwen2-7B                       | 1     |
| **Qwen3ForCausalLM**               | Qwen3-Embedding                    | 3     |

### Unsupported Architectures (12 models)

| Architecture            | Models                                | Reason                               |
|-------------------------|---------------------------------------|--------------------------------------|
| **BloomForCausalLM**    | bloomz-7b1                            | Not implemented in SGLang            |
| **GPTNeoXForCausalLM**  | dolly-v2-12b, stablelm-tuned-alpha-7b | Not implemented in SGLang            |
| **GPTJForCausalLM**     | gpt-j-6b                              | Not implemented in SGLang            |
| **FalconForCausalLM**   | falcon-7b-instruct                    | Not implemented (older architecture) |
| **StableLmForCausalLM** | stablelm-2-12b-chat                   | Not implemented in SGLang            |
| **CodeGenForCausalLM**  | codegen-16B-multi                     | Not implemented in SGLang            |

---

## Failure Categories

| Category                   | Count | Examples                                               |
|----------------------------|-------|--------------------------------------------------------|
| Architecture not supported | 12    | Bloom, Falcon, GPT-J, GPT-NeoX, StableLM, CodeGen, MPT |
| Download timeout           | 8     | Large models, gated models, system issues              |
| CUDA OOM                   | 3     | DeepSeek-V3, Llama-4-Maverick (non-FP8)                |
| Module missing             | 3     | phi-2, phi-3-mini (vllm module)                        |
| HuggingFace access         | 2     | Gated models requiring license                         |
| Inference issues           | 2     | FP8 NaN, warmup failures                               |
| Other                      | 10    | Various configuration/compatibility issues             |

---

## Owner Distribution

| Owner  | Models | Vendors                                              |
|--------|--------|------------------------------------------------------|
| simo   | 51     | adept → google (A-G vendors, deepseek-ai)            |
| keyang | 51     | HuggingFaceTB → microsoft (phi-2), meta Llama family |
| beiwen | 51     | microsoft (Phi-3+) → Qwen2-72B, mistralai, nvidia    |
| xinyue | 50     | Qwen2-VL+ → ZhipuAI, Qwen3, various smaller vendors  |

## Notes

- **GPUs**: Number of H100 80GB GPUs required for inference
- **TP**: Tensor Parallelism degree (when >1 GPU)
- **MoE**: Mixture of Experts architecture
- **VLM**: Vision-Language Model
- **Runtime**: SGLang runtime name (prefix `srt-`)
- **Owner**: Team member responsible for testing this model
