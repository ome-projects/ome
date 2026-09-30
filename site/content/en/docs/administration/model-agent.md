---
title: "Model Agent Administration"
date: 2023-03-14
weight: 5
description: >
  Complete guide to Model Agent architecture, configuration, and operational management.
---

The Model Agent is the core component responsible for downloading, managing, and distributing models across your OME cluster. This guide provides comprehensive information for cluster administrators who need to configure, monitor, and troubleshoot the Model Agent in production environments.

## Architecture Overview

### DaemonSet Deployment

The Model Agent is deployed as a Kubernetes DaemonSet, ensuring it runs on every node in your cluster. This distributed architecture provides several benefits:

- **Parallel Downloads**: Models are downloaded simultaneously across all selected nodes
- **High Availability**: No single point of failure for model distribution
- **Local Storage**: Models are stored locally on each node for optimal performance
- **Node-Specific Configuration**: Each agent can be configured for the specific hardware and storage on its node

## Model Agent Lifecycle

When you create a BaseModel resource, here's the detailed workflow:

### 1. Resource Discovery

- **Kubernetes Informers**: Each Model Agent uses Kubernetes informers to watch for BaseModel and ClusterBaseModel resources
- **Change Detection**: The agent detects new models, updates to existing models, and model deletions in real-time
- **Event Processing**: Changes are queued and processed asynchronously to prevent blocking

### 2. Node Selection Evaluation

- **Label Matching**: The agent evaluates nodeSelector labels against the current node's labels
- **Affinity Evaluation**: Complex nodeAffinity rules are processed using Kubernetes' standard affinity logic
- **Eligibility Decision**: The agent determines if the current node should host this model

### 3. Task Creation and Queuing

The agent creates different types of tasks based on the operation:

- **Download Task**: For new models that need to be downloaded
- **DownloadOverride Task**: For existing models that need to be updated
- **Delete Task**: For models that should be removed from the node

### 4. Download Execution

The download process varies by storage backend but follows this general pattern:

#### OCI Object Storage Downloads

1. **Authentication**: Establish connection using configured auth method (Instance Principal, User Principal, etc.)
2. **Object Listing**: List all objects under the specified prefix
3. **Bulk Download**: Download files concurrently with configurable parallelism
4. **Verification**: Verify file integrity using MD5 checksums
5. **Atomic Placement**: Move verified files to final destination

#### Hugging Face Downloads

1. **Repository Analysis**: Query the Hugging Face API for repository information
2. **File Filtering**: Determine which files are needed based on model format
3. **LFS Handling**: Handle Git LFS files seamlessly
4. **Progressive Download**: Download files with progress tracking
5. **Cache Management**: Manage local cache for efficiency

##### Authentication

The Model Agent supports flexible authentication for Hugging Face models:

1. **Secret-based Authentication**: Use Kubernetes secrets to store tokens
2. **Parameter-based Authentication**: Include tokens directly in model parameters
3. **Custom Secret Key Names**: Configure the secret key name (defaults to "token")

Example with custom secret key:
```yaml
spec:
  storage:
    storageUri: "hf://meta-llama/Llama-2-7b-hf"
    key: "hf-credentials"
    parameters:
      secretKey: "access-token"  # Custom key name in the secret
```

This allows you to store Hugging Face tokens in secrets with any key name, not just "token".

### 5. Model Parsing and Analysis

After successful download, the agent performs comprehensive model analysis:

#### Configuration Parsing

- **config.json Analysis**: Parse the model configuration file to extract metadata
- **Architecture Detection**: Identify the model architecture using specialized parsers
- **Capability Inference**: Determine model capabilities based on architecture and configuration

#### SafeTensors Analysis

For models using SafeTensors format:

- **Metadata Extraction**: Read tensor metadata from SafeTensors headers
- **Parameter Counting**: Calculate exact parameter counts from tensor shapes
- **Memory Estimation**: Estimate memory requirements based on data types

### 6. Status Updates and Node Labeling

- **ConfigMap Updates**: Update per-node ConfigMaps with model status
- **Node Labeling**: Apply labels to nodes indicating model availability
- **Metric Emission**: Update Prometheus metrics for monitoring

## Configuration Reference

### Command Line Arguments

The Model Agent supports extensive configuration through command-line arguments:

#### Download Configuration

| Argument                  | Default | Description                                           |
|---------------------------|---------|-------------------------------------------------------|
| `--download-retry`        | 3       | Number of retry attempts for failed downloads         |
| `--concurrency`           | 4       | Number of concurrent file downloads per model         |
| `--multipart-concurrency` | 4       | Number of concurrent chunks for large file downloads  |
| `--num-download-worker`   | 5       | Number of parallel download workers across all models |
| `--hf-max-workers`        | 4       | Maximum concurrent workers for Hugging Face downloads |
| `--hf-max-retries`        | 10      | Maximum retry attempts for Hugging Face API calls     |
| `--hf-retry-interval`     | 15s     | Base retry interval for Hugging Face API errors       |

#### Storage Configuration

| Argument            | Default                | Description                                        |
|---------------------|------------------------|----------------------------------------------------|
| `--models-root-dir` | `/mnt/models`          | Root directory for storing models on nodes         |
| `--temp-dir`        | `/tmp/model-downloads` | Temporary directory for downloads                  |
| `--cleanup-temp`    | true                   | Whether to clean up temporary files after download |

#### Node and Cluster Configuration

| Argument             | Default      | Description                                             |
|----------------------|--------------|---------------------------------------------------------|
| `--node-name`        | `$NODE_NAME` | Name of the current node (usually from environment)     |
| `--namespace`        | `ome`        | Kubernetes namespace for ConfigMaps and status tracking |
| `--node-label-retry` | 5            | Number of retries for updating node labels              |

#### Logging and Monitoring

| Argument         | Default | Description                              |
|------------------|---------|------------------------------------------|
| `--log-level`    | `info`  | Log verbosity (debug, info, warn, error) |
| `--log-format`   | `text`  | Log format (text, json)                  |
| `--port`         | 8080    | HTTP port for health checks and metrics  |
| `--metrics-port` | 8080    | Port for Prometheus metrics endpoint     |

#### Advanced Configuration

| Argument                      | Default | Description                                  |
|-------------------------------|---------|----------------------------------------------|
| `--config-map-sync-interval`  | `30s`   | Interval for syncing ConfigMaps              |
| `--model-watch-resync-period` | `10m`   | Resync period for model watchers             |
| `--max-concurrent-reconciles` | 10      | Maximum concurrent reconciliation operations |

### Environment Variables

The Model Agent also supports configuration through environment variables:

| Variable | Description |
|----------|-------------|
| `NODE_NAME` | Name of the current Kubernetes node |
| `POD_NAMESPACE` | Namespace where the Model Agent pod is running |
| `OCI_CONFIG_FILE` | Path to OCI configuration file |
| `HUGGINGFACE_TOKEN` | Default Hugging Face access token |
| `INSTANCE_TYPE_MAP` | JSON mapping of cloud instance types to GPU short names (e.g., `{"BM.GPU.H100.8": "H100"}`) |

## Advanced Download Features

### TensorRT-LLM Support and Shape Filtering

For TensorRT-LLM models, the Model Agent provides intelligent shape filtering:

#### GPU Shape Detection

1. **Hardware Detection**: The agent detects the GPU configuration on the current node
2. **Shape Identification**: Maps GPU hardware to TensorRT-LLM shape identifiers (e.g., `GPU.A100.4`, `GPU.H100.8`)
3. **File Filtering**: Only downloads model files that match the detected GPU shape

The instance type to GPU short name mapping is configurable via the `model-agent-config-map` ConfigMap, which is automatically deployed with the Model Agent. This allows you to add support for new cloud instance types without code changes.
To add or modify mappings, you can edit this ConfigMap directly. For example, to add a mapping for a new instance type new-gpu-instance, you can use `kubectl edit configmap model-agent-config-map -n <namespace>` and add the new entry to the instance-type-map data."

#### Shape Filtering Logic

```go
// Simplified shape filtering logic
func (d *TensorRTLLMDownloader) filterByShape(files []string, nodeShape string) []string {
    var filtered []string
    for _, file := range files {
        if strings.Contains(file, nodeShape) || isShapeAgnostic(file) {
            filtered = append(filtered, file)
        }
    }
    return filtered
}
```

This optimization can save significant storage space and download time for large TensorRT-LLM models that may contain multiple GPU shape variants.

### Concurrent Download Optimization

#### Bulk Download Strategy

For OCI Object Storage, the agent implements sophisticated bulk download:

1. **Object Listing**: List all objects under the model prefix
2. **Size-Based Chunking**: Split large files into chunks for parallel download
3. **Connection Pooling**: Reuse HTTP connections for efficiency
4. **Rate Limiting**: Respect storage backend rate limits

#### Multipart Download Logic

Large files (>200MB) are automatically split into chunks:

```go
type MultipartDownload struct {
    URL        string
    ChunkSize  int64
    TotalSize  int64
    Chunks     []ChunkInfo
}

type ChunkInfo struct {
    Start  int64
    End    int64
    Status DownloadStatus
}
```

### Resume Capability

The Model Agent supports resuming interrupted downloads:

1. **Progress Tracking**: Track download progress for each file
2. **Partial File Detection**: Detect partially downloaded files on restart
3. **Range Requests**: Use HTTP range requests to resume from last position
4. **Integrity Verification**: Verify resumed downloads maintain file integrity

## Verification and Integrity

### Comprehensive File Verification

Every downloaded file undergoes rigorous verification:

#### Size Verification

```go
func verifyFileSize(localPath string, expectedSize int64) error {
    stat, err := os.Stat(localPath)
    if err != nil {
        return err
    }
    if stat.Size() != expectedSize {
        return fmt.Errorf("size mismatch: expected %d, got %d", expectedSize, stat.Size())
    }
    return nil
}
```

#### Checksum Verification

- **MD5 Checksums**: Computed and verified against object storage metadata
- **SHA256 Support**: For storage backends that provide SHA256 checksums
- **Custom Checksums**: Support for vendor-specific checksum methods

#### Atomic Operations

Files are downloaded to temporary locations and only moved to final destinations after successful verification:

1. **Temporary Download**: Download to `.tmp` extension
2. **Verification**: Verify size and checksum
3. **Atomic Move**: Rename to final filename (atomic operation on most filesystems)
4. **Cleanup**: Remove temporary files on failure

## Thread Safety and Concurrency

### ConfigMap Coordination

The Model Agent uses sophisticated locking for thread-safe ConfigMap operations:

```go
type ConfigMapManager struct {
    mutex     sync.RWMutex
    configMap *v1.ConfigMap
    client    kubernetes.Interface
}

func (cm *ConfigMapManager) UpdateModelStatus(modelKey string, status ModelStatus) error {
    cm.mutex.Lock()
    defer cm.mutex.Unlock()

    // Update ConfigMap data
    // Handle conflicts and retries
}
```

### Model Update Handling

When an existing model is updated:

1. **Change Detection**: Deep comparison of model specifications
2. **Status Transition**: Set model status to "Updating"
3. **Graceful Replacement**: Download new version alongside existing
4. **Verification**: Verify new download before removing old version
5. **Atomic Switch**: Atomically replace old model with new version

### Download Cancellation

The Model Agent supports graceful cancellation of ongoing downloads:

1. **Active Download Tracking**: Maintains a registry of all active downloads
2. **Immediate Cancellation**: When a model is deleted, any ongoing download is cancelled immediately
3. **Context Propagation**: Uses Go contexts to propagate cancellation throughout the download pipeline
4. **Cleanup**: Ensures partial downloads are cleaned up after cancellation

This prevents the issue where deleting a model resource would wait for the entire download to complete before deletion.

**Note**: For OCI Object Storage downloads, standard downloads stop between files (the file currently being transferred runs to completion), while multipart downloads of large files are interrupted mid-transfer, cleaning up per-part temporary files and the `.temp` assembly file. In-flight OCI SDK requests retain their retry policy and complete before cancellation takes effect, so cancellation may be delayed while an active request finishes. Hugging Face downloads support immediate cancellation.

### Worker Pool Management

The agent uses worker pools for concurrent operations:

```go
type WorkerPool struct {
    workers    int
    taskQueue  chan Task
    resultChan chan Result
    ctx        context.Context
    cancel     context.CancelFunc
}
```

## Monitoring and Observability

### Health Check Endpoints

The Model Agent exposes several HTTP endpoints for monitoring:

#### Basic Health Check (`/healthz`)

```go
func (h *HealthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    // Check if models root directory is accessible
    if _, err := os.Stat(h.modelsRootDir); err != nil {
        http.Error(w, "Models directory not accessible", http.StatusServiceUnavailable)
        return
    }

    w.WriteHeader(http.StatusOK)
    w.Write([]byte("OK"))
}
```

#### Readiness Check (`/readyz`)

Checks if the agent is ready to process models:
- Kubernetes API connectivity
- Storage backend accessibility
- Required directories exist

#### Liveness Check (`/livez`)

Verifies the agent is functioning correctly:
- Worker pool status
- Recent operation success
- Memory usage within limits

### Comprehensive Metrics

The Model Agent provides detailed Prometheus metrics:

#### Download Metrics

```prometheus
# Total successful downloads
model_agent_downloads_success_total{model_type="llama", namespace="default", name="llama-70b"} 1

# Total failed downloads
model_agent_downloads_failed_total{model_type="llama", namespace="default", name="llama-70b"} 0

# Download duration
model_agent_download_duration_seconds{model_type="llama", namespace="default", name="llama-70b"} 1234.56

# Download size in bytes
model_agent_download_bytes_total{model_type="llama", namespace="default", name="llama-70b"} 140737488355328
```

#### Verification Metrics

```prometheus
# Verification results
model_agent_verifications_total{model_type="llama", namespace="default", name="llama-70b", result="success"} 1

# Verification duration
model_agent_verification_duration_seconds 12.34

# MD5 checksum failures
model_agent_md5_checksum_failed_total{model_type="llama", namespace="default", name="llama-70b"} 0
```

#### Runtime Metrics

```prometheus
# Current goroutines
go_goroutines_current 45

# Memory allocation
go_memory_alloc_bytes 67108864

# GC pause time
go_gc_pause_duration_seconds_custom 0.001234
```

#### Agent-Specific Metrics

```prometheus
# Active download workers
model_agent_active_workers 3

# Queue depth
model_agent_task_queue_depth 2

# ConfigMap update operations
model_agent_configmap_operations_total{operation="update", result="success"} 15
```

### Rate Limiting Protection

The Model Agent includes sophisticated rate limiting protection for Hugging Face API:

#### Automatic Backoff
- **Exponential Backoff**: Automatically increases wait time between retries
- **Jitter**: Adds randomness to prevent thundering herd
- **Retry-After**: Respects server-provided retry delays
- **Max Retries**: Configurable retry limit (default: 10)

#### Staggered Start
When multiple agents start simultaneously (e.g., after cluster restart), they automatically stagger their initialization:
- Each node gets a deterministic delay based on its name (0-30 seconds)
- Prevents all agents from hitting the API at once
- Reduces initial rate limiting issues

#### Best Practices for Large Clusters
1. **Limit concurrent downloads**: Use fewer download workers for large clusters
2. **Increase retry intervals**: Set longer base retry intervals
3. **Monitor rate limits**: Watch for 429 errors in logs
4. **Use regional endpoints**: Consider using region-specific Hugging Face endpoints

Example configuration for large clusters:
```yaml
args:
- --hf-max-workers=2
- --hf-max-retries=15
- --hf-retry-interval=30s
- --num-download-worker=3
```

## Troubleshooting Guide

### Common Issues and Solutions

#### Downloads Fail with Permission Errors

**Symptoms:**
- HTTP 403 Forbidden errors
- Authentication failures in logs

**Diagnosis:**
```bash
# Check OCI authentication
kubectl exec -it <model-agent-pod> -- oci iam user get --user-id <user-ocid>

# Check Hugging Face token
kubectl get secret <hf-token-secret> -o yaml
```

**Solutions:**
- Verify OCI Instance Principal permissions
- Check Hugging Face token validity
- Ensure secrets are properly mounted

#### Model Parsing Failures

**Symptoms:**
- Models download but status shows "Failed"
- Parsing errors in agent logs

**Diagnosis:**
```bash
# Check model directory contents
kubectl exec -it <model-agent-pod> -- ls -la /mnt/models/<model-name>/

# Verify config.json exists and is valid
kubectl exec -it <model-agent-pod> -- cat /mnt/models/<model-name>/config.json | jq .
```

**Solutions:**
- Verify model directory structure
- Check if config.json is valid JSON
- Consider disabling auto-parsing with annotation

#### Storage Space Issues

**Symptoms:**
- Downloads fail with "no space left on device"
- Node storage metrics show high usage

**Diagnosis:**
```bash
# Check node storage usage
kubectl exec -it <model-agent-pod> -- df -h

# Check model directory sizes
kubectl exec -it <model-agent-pod> -- du -sh /mnt/models/*
```

**Solutions:**
- Increase node storage capacity
- Implement model cleanup policies
- Use node affinity to target nodes with sufficient storage

#### Performance Issues

**Symptoms:**
- Slow download speeds
- High memory usage
- Agent pod restarts

**Diagnosis:**
```bash
# Check resource usage
kubectl top pod <model-agent-pod>

# Check agent configuration
kubectl describe pod <model-agent-pod>

# Review agent metrics
curl http://<model-agent-pod>:8080/metrics | grep model_agent
```

**Solutions:**
- Adjust concurrency settings
- Increase resource limits
- Optimize storage backend configuration

### Debug Mode

Enable debug logging for detailed troubleshooting:

```yaml
spec:
  containers:
  - name: model-agent
    args:
    - --log-level=debug
    - --log-format=json
```

### Log Analysis

Key log patterns to monitor:

```bash
# Download progress
grep "downloading file" /var/log/model-agent.log

# Verification results
grep "verification" /var/log/model-agent.log

# Configuration updates
grep "configmap update" /var/log/model-agent.log

# Error patterns
grep "ERROR\|Failed\|Error" /var/log/model-agent.log
```

## Production Best Practices

### Resource Planning

#### Memory Requirements

- **Base Memory**: 256Mi minimum for agent operations
- **Download Buffers**: Additional 1-2Gi for concurrent downloads
- **Model Parsing**: 512Mi-1Gi for parsing large model configurations

#### Storage Planning

- **Local Storage**: Use fast local storage (NVMe SSDs) for model paths
- **Capacity Planning**: Plan for 2-3x model size for download + verification
- **Cleanup Policies**: Implement automated cleanup for old model versions

#### Network Bandwidth

- **Download Bandwidth**: Ensure sufficient bandwidth for multiple concurrent model downloads
- **Egress Costs**: Consider egress costs for cloud storage downloads
- **Regional Placement**: Place agents in the same region as storage when possible

### Security Configuration

#### RBAC Requirements

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: model-agent
rules:
- apiGroups: [""]
  resources: ["configmaps"]
  verbs: ["get", "list", "watch", "create", "update", "patch"]
- apiGroups: [""]
  resources: ["nodes"]
  verbs: ["get", "list", "watch", "patch"]
- apiGroups: ["ome.io"]
  resources: ["basemodels", "clusterbasemodels"]
  verbs: ["get", "list", "watch"]
```

#### Secret Management

- Use least-privilege service accounts
- Rotate credentials regularly
- Implement secret scanning and monitoring

### Monitoring Setup

#### Alerting Rules

```yaml
groups:
- name: model-agent
  rules:
  - alert: ModelDownloadFailure
    expr: increase(model_agent_downloads_failed_total[5m]) > 0
    for: 2m
    annotations:
      summary: "Model download failure detected"

  - alert: ModelAgentDown
    expr: up{job="model-agent"} == 0
    for: 1m
    annotations:
      summary: "Model Agent is down"
```

#### Dashboard Metrics

Key metrics to dashboard:
- Download success/failure rates
- Download duration and throughput
- Storage usage per node
- Agent resource utilization
- ConfigMap update frequency

This comprehensive guide provides the operational knowledge needed to effectively manage the Model Agent in production OME deployments.
