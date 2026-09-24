# OME-Agent

OME-Agent is a robust, multi-functional tool designed to manage various tasks related to model inference and training in OME. Built in Golang, this command-line tool consolidates essential model management operations, such as replication, encryption, and decryption, into a single, easy-to-use “Swiss Army Knife” solution for OME operators.

## Features

OME-Agent offers the following capabilities:

1. **Model Replication from HuggingFace**
    - **Nested File Handling**: Downloads all files within a model, including subdirectory content.
    - **Multithreaded Downloads**: Accelerates large file downloads, especially those stored in Git LFS.
    - **Integrity Verification**: Confirms successful downloads by validating SHA256 checksums.
    - **Resume Interrupted Downloads**: Automatically resumes incomplete downloads.
    - **Smart Skipping**: Skips files that are already downloaded, saving time and bandwidth.
    - **HuggingFace Token Authentication**: Supports HuggingFace Access Tokens for restricted models and datasets.
    - **Branch-Specific Updates**: Tracks and updates files when switching branches.

2. **Object Storage Replication Between OCI Buckets**
    - **Cross-Bucket Replication**: Copies models between OCI buckets to support data redundancy and multi-region deployments.
    - **Region/Tenancy Support**: Allows model replication across OCI regions and tenancies.
    - **Configurable Concurrency**: Optimizes upload/download speeds through customizable concurrent connections.

3. **Model Weight Encryption and Decryption**
    - **OCI Vault Integration**: Uses OCI Vault and Key Management Service (KMS) for secure decryption of model weights.
    - **Advanced Encryption Standards**: Protects sensitive model data with encryption for regulated environments.

## Getting Started

### Prerequisites

- **Go** version 1.23.0 or later.
- **Cloud CLI** and **SDK** for interacting with cloud storage services (optional if cloud services are not required).
- **HuggingFace Access Token** if downloading restricted models (optional if downloading public models).
- **OCI Vault and KMS** setup for model weight decryption (optional if not decrypting model weights).
- **Environment Variables**:
    - `GOPATH`: If not already set, select a directory and add it to the environment: `export GOPATH=...`.
    - `$GOPATH/bin` in `PATH`: Ensures tooling installed via `go get` functions correctly.
    - `GONOPROXY` and `GOPRIVATE`: (Optional) Set to retrieve dependencies from any internal repository.

### Installation

Clone the repository and install the OME-Agent CLI.

```bash
mkdir -p ${GOPATH}/src/sigs.k8s.io/ome
cd ${GOPATH}/src/sigs.k8s.io/ome
git clone https://github.com/ome-projects/ome.git
cd ome
make ome-agent
```

### Configuration
OME-Agent supports configuration through both environment variables and configuration files.

Sample configuration yaml file:
```yaml
model_store_directory: "<local path to store model weight>"
local_path: "<local path to store model weight>"
model_name: "meta-llama/Meta-Llama-3-8B"
hf_token: "<insert your own token>"
num_connections: 20
skip_sha: false
max_retries: 5
retry_internal_in_seconds: 10

auth_type: &default_auth_type "UserPrincipal"
profile: "DEFAULT"

download_size_limit_gb: 650
enable_size_limit_check: true
hf_download_timeout: "72h"
hf_download_stale_progress_timeout: "30m"

source:
  bucket_name: "model-store"
  prefix: "meta/llama-3-2-1b/"
  region: "us-chicago-1"
  namespace: "idqj093njucb"

target:
  bucket_name: "test-bucket"
  prefix: "meta/llama-3-2-1b/"
  region: "eu-frankfurt-1"
  namespace: "idqj093njucb"

compartment_id: "ocid1.compartment.oc1..aaaaaaaathgntpo75bdehisnl6wkxfc4slkd6rpheafbt5a6ekm2ri4bmeva"
vault_id: "ocid1.vault.oc1.us-chicago-1.ijsr6afaaagp2.abxxeljtatzowkwlt2iu42ndlmnmd4d4nausgoubu7uq7iro553xdj5b6weq"
key_name: "command_r"
secret_name: "command_r-dek"
```

Supported environment variables:
All environment variables ***must*** start the prefix `OME_AGENT_` to be recognized by the OME-Agent.

| YAML Key                                      | Environment Variable                                    | Default                   | Required                                                                             |
|-----------------------------------------------|---------------------------------------------------------|---------------------------|--------------------------------------------------------------------------------------|
| `auth_type`                                   | `OME_AGENT_AUTH_TYPE`                                   |                           | yes                                                                                  |
| `profile`                                     | `OME_AGENT_PROFILE`                                     | DEFAULT                   | no                                                                                   |
| `local_path`                                  | `OME_AGENT_LOCAL_PATH`                                  |                           | yes                                                                                  |
| `model_store_directory`                       | `OME_AGENT_MODEL_STORE_DIRECTORY`                       | /opt/ml/model             | no                                                                                   |
| `skip_sha`                                    | `OME_AGENT_SKIP_SHA`                                    | false                     | no                                                                                   |
| `max_retry`                                   | `OME_AGENT_MAX_RETRY`                                   | 5                         | no                                                                                   |
| `retry_internal_in_seconds`                   | `OME_AGENT_RETRY_INTERVAL_IN_SECONDS`                   | 10                        | no                                                                                   |
| `model_name`                                  | `OME_AGENT_MODEL_NAME`                                  |                           | yes                                                                                  |
| `hf_token`                                    | `OME_AGENT_HF_TOKEN`                                    |                           | no                                                                                   |
| `num_connections`                             | `OME_AGENT_NUM_CONNECTIONS`                             | 10                        | no                                                                                   |
| `download_size_limit_gb`                      | `OME_AGENT_DOWNLOAD_SIZE_LIMIT_GB`                      | 650                       | no                                                                                   |
| `enable_size_limit_check`                     | `OME_AGENT_ENABLE_SIZE_LIMIT_CHECK`                     | true                      | no                                                                                   |
| `hf_download_timeout`                         | `OME_AGENT_HF_DOWNLOAD_TIMEOUT`                         | 72h                       | no                                                                                   |
| `hf_download_stale_progress_timeout`          | `OME_AGENT_HF_DOWNLOAD_STALE_PROGRESS_TIMEOUT`          | 30m                       | no                                                                                   |
| `target.checksum.concurrency`                 | `OME_AGENT_TARGET_CHECKSUM_CONCURRENCY`                 | 8                         | no                                                                                   |
| `source.bucket_name`                          | `OME_AGENT_SOURCE_BUCKET_NAME`                          |                           | yes                                                                                  |
| `source.prefix`                               | `OME_AGENT_SOURCE_PREFIX`                               |                           | no                                                                                   |
| `source.region`                               | `OME_AGENT_SOURCE_REGION`                               |                           | yes                                                                                  |
| `source.namespace`                            | `OME_AGENT_SOURCE_NAMESPACE`                            |                           | yes                                                                                  |
| `target.bucket_name`                          | `OME_AGENT_TARGET_BUCKET_NAME`                          |                           | yes                                                                                  |
| `target.prefix`                               | `OME_AGENT_TARGET_PREFIX`                               |                           | no                                                                                   |
| `target.region`                               | `OME_AGENT_TARGET_REGION`                               |                           | yes                                                                                  |
| `target.namespace`                            | `OME_AGENT_TARGET_NAMESPACE`                            |                           | yes                                                                                  |
| `compartment_id`                              | `OME_AGENT_COMPARTMENT_ID`                              |                           | yes                                                                                  |
| `vault_id`                                    | `OME_AGENT_VAULT_ID`                                    |                           | yes                                                                                  |
| `key_name`                                    | `OME_AGENT_KEY_NAME`                                    |                           | yes                                                                                  |
| `secret_name`                                 | `OME_AGENT_SECRET_NAME`                                 | <key_name>-dek            | no                                                                                   |
| `model_type`                                  | `OME_AGENT_MODEL_TYPE`                                  |                           | yes                                                                                  |
| `model_framework`                             | `OME_AGENT_MODEL_FRAMEWORK`                             | tensorrtllm               | yes                                                                                  |
| `tensorrtllm_version`                         | `OME_AGENT_TENSORRTLLM_VERSION`                         | v0.11.0                   | yes                                                                                  |
| `node_shape_alias`                            | `OME_AGENT_NODE_SHAPE_ALIAS`                            |                           | no                                                                                   |
| `num_of_gpu`                                  | `OME_AGENT_NUM_OF_GPU`                                  | 1                         | yes                                                                                  |
| `disable_model_decryption`                    | `OME_AGENT_DISABLE_MODEL_DECRYPTION`                    | false                     | no                                                                                   |
| `model_directory`                             | `OME_AGENT_MODEL_DIRECTORY`                             |                           | yes                                                                                  |
| `input_object_store.enable_obo_token`         | `OME_AGENT_INPUT_OBJECT_STORE_ENABLE_OBO_TOKEN`         | true                      | no                                                                                   |
| `input_object_store.obo_token`                | `OME_AGENT_INPUT_OBJECT_STORE_OBO_TOKEN`                |                           | yes when `input_object_store.enable_obo_token` == `true`                             |
| `model.bucket_name`                           | `OME_AGENT_MODEL_BUCKET_NAME`                           | fine-tuned-model-weights  | no                                                                                   |
| `model.namespace`                             | `OME_AGENT_MODEL_NAMESPACE`                             |                           | yes                                                                                  |
| `model.object_name`                           | `OME_AGENT_MODEL_OBJECT_NAME`                           | equals to `training_name` | no                                                                                   |
|

### Reusing upload locks across replication retries

For OCI targets, `target_artifact_reuse_allowed` controls artifact reuse and
upload locking. It defaults to `false`. When enabled, the agent reuses completed
artifacts or manages an Object Storage upload lock when replication is needed.
Consumers do not need to implement lock operations themselves.

| `target_artifact_reuse_allowed` | `artifact_upload_lock_owner_id` | Upload lock behavior |
| --- | --- | --- |
| `false` (default) | Unset or provided | No artifact upload lock. |
| `true` | Unset | The agent manages a lock, but retries cannot reuse a leftover lock by ownership. |
| `true` | Provided | The agent manages a lock, and retries with the same owner ID can reuse it. |

Omitting the owner ID does not disable locking when artifact reuse is enabled.
To allow lock reuse across retries, the consumer can set
`artifact_upload_lock_owner_id` or its environment variable:

```bash
export OME_AGENT_TARGET_ARTIFACT_REUSE_ALLOWED=true
export OME_AGENT_ARTIFACT_UPLOAD_LOCK_OWNER_ID=replication-operation-123
```

Use the same ID for retries of one replication operation and a new, unique ID
for each independent operation. The agent stores `{"ownerID":"..."}` in the lock.
If an attempt is interrupted and leaves the lock behind, a retry with the same
ID can continue uploading with the existing lock and ETag, without deleting or
recreating it. Attempts with different IDs wait for the artifact to complete or
the lock to be released or expire.

The consumer schedules retries and must ensure the previous uploader has stopped
before starting another with the same ID. The agent only compares IDs; it does
not check whether another process is still running.

All consumers targeting the same artifact should use the same
`artifact_upload_lock_timeout` (default 120 hours), longer than the maximum
duration of any replication operation, including retries. Each agent uses its
own configured timeout to decide whether an existing lock is stale. A consumer
with a shorter timeout could delete another consumer's lock while that upload
is still running. Reusing a lock does not reset its age.

On a normal return, including an error, the agent attempts to release its lock
using the ETag. If the process exits before cleanup runs, the lock remains
available for a retry with the same ID. Locks without an owner ID retain the
existing wait/timeout behavior.

### Usage
OME-Agent uses subcommands to run specific tasks. Use the following commands:=
```bash
./ome-agent hf-download --config <path-to-config.yaml> --debug
```
```bash
./ome-agent replica --config <path-to-config.yaml> --debug
```
```bash
./ome-agent enigma --config <path-to-config.yaml> --debug
```


## Development Guide

### Make Commands

OME-Agent comes with several Makefile commands to simplify development, building, and deployment tasks. Below is a summary of the main commands available:

```bash
# Builds the OME-Agent CLI.
make ome-agent
# Builds the Docker image for OME-Agent, tagging it with the specified REGISTRY and TAG variables.
make ome-agent-image
# Pushes the OME-Agent Docker image to the specified REGISTRY.
make push-ome-agent-image
# Runs the OME-Agent CLI with the specified subcommand (e.g., hf-download, replica, enigma, ).
make run-ome-agent-enigma
make run-ome-agent-hf-download
make run-ome-agent-os-replica
```

### Code Structure
OME-Agent follows a modular and scalable design, which makes it easy to extend with new features. The core structure relies on:
1.	[Cobra](https://cobra.dev/): For command-line interface (CLI) management, organizing tasks into distinct subcommands (hf-download, enigma, and replica).
2.	[Fx](https://uber.github.io/fx/): A dependency injection framework that manages the lifecycle of each component, injecting necessary dependencies and ensuring clean startup/shutdown of services.
3.	[Viper](https://pkg.go.dev/github.com/spf13/viper): For configuration management, allowing settings to be loaded from a configuration file, environment variables, or command-line flags.

The main directory layout might look something like this:
```
ome/
├── cmd/                            # Contains subbinaries
│   └── ome-agent/                  # Contains Cobra subcommands
│       ├── main.go                 # Main entry point for the CLI
│       ├── hf_download_agent.go    # Subcommand for HuggingFace model downloads
│       ├── enigma_agent.go         # Subcommand for model encryption/decryption
│       ├── model_metadata_agent.go # Subcommand for model metadata extraction
│       ├── replica_agent.go        # Subcommand for object storage replication
│       └── serving_agent.go        # Subcommand for serving sidecar agent
│       └── fine-tuned-adapter.go   # Subcommand for fine-tuned adapter
├── internal/                       # Contains the core business logic for each feature
│   ├── enigma/                     # Logic for encryption/decryption
│   ├── fine-tuned-adapter/         # Logic for fine-tuned adapter
│   ├── model-metadata/             # Logic for model metadata extraction
│   ├── replica/                    # Logic for replication across OCI buckets
│   └── serving-agent/              # Logic for serving sidecar agent
├── pkg/                            # Shared libraries and utility functions
│   ├── configutils/                # Utility functions for handling configuration files
│   ├── constants/                  # Common constants used across the project
│   ├── logging/                    # Logging module for consistent logging across commands
│   ├── secrets/                    # Secret management (e.g., KMS integration for decryption)
│   └── hf_download/                # Logic for HuggingFace downloading (independent enough to be its own package)
```

### Key Components Explained

#### Main Entry (main.go)
   The main.go file initializes the root Cobra command (ome-agent) and executes it. All subcommands are registered here using rootCmd.AddCommand. This file acts as the command dispatcher, directing commands like hf-download, enigma, and replica to their respective handlers.
### Command Files (hf_download.go, enigma.go, replica.go)
   Each subcommand file defines:
   - Command Metadata: Such as Use, Short, and Long descriptions.
   - Run Function: This is where the logic for each command begins. When a user invokes a command, the Run function creates an Fx application, passing along necessary dependencies and modules.
   - Flags: Each command can define its own flags (e.g., --config, --debug) to customize execution.

For example, in hf_download.go, the Run function invokes runHFDownload, which sets up and starts the HuggingFace Download Agent.

#### Config Provider (configProvider function)

The configProvider function is responsible for loading configurations into a Viper instance, which is then injected into the Fx app. It does the following:

1. Sets up default values and environment variable mappings.
2. Loads configurations from a file (specified by the --config flag).
3. Allows configurations to be overridden by environment variables.

Using Viper enables flexible configuration management, as it can support different deployment environments with minimal change.

#### Dependency Injection with Fx

Fx handles the orchestration of dependencies, lifecycle management, and dependency injection for OME-Agent. Each subcommand creates an Fx application with an fx.New() call that includes:

- Configuration: configProvider injects Viper with configurations for each agent.
- Modules: These represent the core functionalities and external dependencies, like:
  - Environment Management (env.Module): Manages environment variables and system-level configurations.
  - File System Abstraction (afero.Module): Provides a file abstraction layer for operations like file download, allowing for easier testing and extensibility.
  - Logging (logging.Module): Sets up logging using Zap, enabling structured logging across the application.
  - Secrets (keymanagement and secretretrieval): Manages interactions with OCI’s Vault and Key Management Service.

#### Main Application Module

Each subcommand has its own main application module, implemented as an Fx Option. For example:

- HuggingFace Download Agent (hf_download.Module): Handles the logic for downloading models from HuggingFace, validating checksums, resuming interrupted downloads, and managing access tokens.
- Replica Agent (replica.Module): Manages replication of model weights across OCI buckets, handling cross-region and cross-tenancy replication.
- Enigma Agent (enigma.Module): Manages encryption and decryption of model weights using OCI Vault and KMS.
- Training Agent (training_agent.Module): Manages the training sidecar initialization, which includes its config and client setup required to manage the training lifecycle.
Each module includes the main agent logic and is registered as a dependency in the fx.Options list for that subcommand.

#### Lifecycle Hooks

Each agent uses an Fx Lifecycle Hook to manage the OnStart and OnStop events. This ensures:

- OnStart: The agent starts and performs its primary function. For example, the HuggingFace Download Agent begins downloading the model files.
- OnStop: The agent shuts down gracefully when the CLI terminates.

#### Example code for managing lifecycle hooks:
```go
fx.Invoke(func(lc fx.Lifecycle, agent *hf_download.HFDownloadAgent, logger *zap.Logger, shutdown fx.Shutdowner) {
    lc.Append(fx.Hook{
        OnStart: func(context.Context) error {
            go func() {
                if err := agent.Start(); err != nil {
                    logger.Error("Error starting agent", zap.Error(err))
                }
                shutdown.Shutdown()
            }()
            return nil
        },
        OnStop: func(ctx context.Context) error {
            return nil
        },
    })
})
```
This hook architecture allows each agent to handle startup tasks asynchronously and exit cleanly, providing flexibility and stability during operation.


#### Adding New Commands
New functionality can be added by creating a new command using Cobra and integrating it with Fx as shown below:

 1.	Define a new command.
 2.	Set up dependencies and configuration.
 3.	Add the command to the main command tree.

Example:
```go
var cmdNewFeature = &cobra.Command{
    Use:   "new-feature",
    Short: "Description of new feature",
    Run:   runNewFeature,
}

func init() {
    rootCmd.AddCommand(cmdNewFeature)
}
```
