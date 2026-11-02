# Runtime Selector Package

The `runtimeselector` package provides a comprehensive, modular solution for runtime selection and validation in the OME operator. It determines the best serving runtime for machine learning models based on model characteristics, runtime capabilities, and scoring algorithms.

## Overview

The package provides:

- **Automatic runtime selection** based on model requirements
- **Accelerator-aware runtime selection** with GPU compatibility checking
- **Runtime validation** for user-specified runtimes
- **Detailed compatibility analysis** with clear error reporting
- **Efficient caching** using controller-runtime's cache mechanism
- **Flexible scoring system** with runtime-defined weights

## Architecture

### Component Structure

```
runtimeselector/
├── types.go        # Core interfaces and data structures
├── selector.go     # Main selector implementation
├── fetcher.go      # Runtime resource fetching with caching
├── matcher.go      # Compatibility evaluation logic
├── scorer.go       # Scoring and ranking algorithms
└── errors.go       # Custom error types
```

### Key Interfaces

#### Selector
The main interface for runtime selection operations:

```go
type Selector interface {
    // Auto-select best runtime for a model
    SelectRuntime(ctx context.Context, model *v1beta1.BaseModelSpec, isvc *v1beta1.InferenceService) (*RuntimeSelection, error)

    // Get all compatible runtimes sorted by priority
    GetCompatibleRuntimes(ctx context.Context, model *v1beta1.BaseModelSpec, isvc *v1beta1.InferenceService, namespace string) ([]RuntimeMatch, error)

    // Validate a specific runtime choice
    ValidateRuntime(ctx context.Context, runtimeName string, model *v1beta1.BaseModelSpec, isvc *v1beta1.InferenceService) error

    // Get runtime spec by name
    GetRuntime(ctx context.Context, name string, namespace string) (*v1beta1.ServingRuntimeSpec, bool, error)

    // Get the best supported model format for a runtime-model pair
    GetSupportedModelFormat(ctx context.Context, runtime *v1beta1.ServingRuntimeSpec, model *v1beta1.BaseModelSpec) *v1beta1.SupportedModelFormat
}
```

#### RuntimeFetcher
Abstracts runtime resource fetching:

```go
type RuntimeFetcher interface {
    // Fetch all runtimes in a namespace
    FetchRuntimes(ctx context.Context, namespace string) (*RuntimeCollection, error)

    // Get specific runtime by name
    GetRuntime(ctx context.Context, name string, namespace string) (*v1beta1.ServingRuntimeSpec, bool, error)
}
```

#### RuntimeMatcher
Handles compatibility checking:

```go
type RuntimeMatcher interface {
    // Check basic compatibility
    IsCompatible(runtime *v1beta1.ServingRuntimeSpec, model *v1beta1.BaseModelSpec, isvc *v1beta1.InferenceService, runtimeName string) (bool, error)

    // Get detailed compatibility report
    GetCompatibilityDetails(runtime *v1beta1.ServingRuntimeSpec, model *v1beta1.BaseModelSpec, isvc *v1beta1.InferenceService, runtimeName string) (*CompatibilityReport, error)
}
```

#### RuntimeScorer
Calculates runtime scores:

```go
type RuntimeScorer interface {
    // Calculate score for runtime-model pair
    CalculateScore(runtime *v1beta1.ServingRuntimeSpec, model *v1beta1.BaseModelSpec) (int64, error)

    // Compare two runtime matches
    CompareRuntimes(a, b RuntimeMatch, model *v1beta1.BaseModelSpec) int
}
```

## Algorithms

### Runtime Selection Algorithm

1. **Fetch Runtimes**
   - Retrieve all ServingRuntimes in the namespace
   - Retrieve all ClusterServingRuntimes
   - Sort by creation timestamp (newest first) and name

2. **Filter Compatible Runtimes**
   - Skip disabled runtimes
   - Check accelerator class compatibility (if required)
   - Check model format compatibility
   - Verify model size is within supported range
   - Ensure auto-select is enabled
   - Calculate compatibility score

3. **Score and Sort**
   - Only formats marked `autoSelect: true` count, each at its own priority;
     the format a pod renders with is the one that produced the score
   - Calculate weighted scores based on:
     - Model format match (weight × priority)
     - Model framework match (weight × priority)
   - Sort by score (highest first)
   - For equal scores, prefer:
     - Namespace-scoped over cluster-scoped
     - Closer model size range match

4. **Return Best Match**
   - Select the highest-scoring runtime
   - Include detailed match information

### Compatibility Checking

The matcher evaluates compatibility across multiple dimensions:

1. **Accelerator Class Requirements**
   - Checks if runtime's required accelerator classes match InferenceService requirements
   - Validates accelerator class from multiple sources:
     - InferenceService annotations (`ome.io/accelerator-class`)
     - InferenceService.Spec.AcceleratorSelector.AcceleratorClass
     - Component-specific AcceleratorOverride (Engine/Decoder)
   - If runtime requires accelerator but ISVC doesn't specify one, compatibility fails

2. **Model Format and Framework**
   - Name must match exactly
   - Version comparison using semantic versioning
   - Special handling for unofficial versions (forces equality)

3. **Diffusion Pipelines**
   - Pipeline class and components must match when specified (e.g., scheduler, transformer, VAE)

4. **Model Architecture**
   - Must match if both specify architecture

5. **Quantization**
   - Must match if both specify quantization

6. **Model Size**
   - Must be within runtime's min/max range

### Scoring Formula

```
Score = Σ(weight × priority) for each matching attribute
```

Where:
- **weight**: Importance of the attribute (defined in the runtime's ModelFormat/ModelFramework)
- **priority**: Runtime-specific multiplier for the supported model format entry

Note: A runtime can support multiple model formats, each with its own weights and priorities. The scorer evaluates all supported formats and uses the highest scoring match.

## Usage Examples

### Basic Integration

```go
// In InferenceService controller
type InferenceServiceReconciler struct {
    client.Client
    RuntimeSelector runtimeselector.Selector
    // ... other fields
}

func (r *InferenceServiceReconciler) SetupWithManager(mgr ctrl.Manager) error {
    // Initialize runtime selector
    r.RuntimeSelector = runtimeselector.New(mgr.GetClient())

    // Add watches to populate cache
    ctrlBuilder.
        Watches(&v1beta1.ServingRuntime{},
            handler.EnqueueRequestsFromMapFunc(func(context.Context, client.Object) []reconcile.Request {
                return nil // Just populate cache
            })).
        Watches(&v1beta1.ClusterServingRuntime{},
            handler.EnqueueRequestsFromMapFunc(func(context.Context, client.Object) []reconcile.Request {
                return nil // Just populate cache
            }))

    return ctrlBuilder.Complete(r)
}
```

### Runtime Selection in Reconciler

```go
func (r *InferenceServiceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
    // ... fetch InferenceService and BaseModel ...

    var rt *v1beta1.ServingRuntimeSpec
    var rtName string

    if isvc.Spec.Runtime != nil && isvc.Spec.Runtime.Name != "" {
        // Validate specified runtime
        rtName = isvc.Spec.Runtime.Name
        if err := r.RuntimeSelector.ValidateRuntime(ctx, rtName, baseModel, isvc); err != nil {
            return reconcile.Result{}, fmt.Errorf("runtime validation failed: %w", err)
        }

        // Get runtime spec
        rtSpec, _, err := r.RuntimeSelector.GetRuntime(ctx, rtName, isvc.Namespace)
        if err != nil {
            return reconcile.Result{}, err
        }
        rt = rtSpec
    } else {
        // Auto-select runtime (considers accelerator requirements)
        selection, err := r.RuntimeSelector.SelectRuntime(ctx, baseModel, isvc)
        if err != nil {
            return reconcile.Result{}, fmt.Errorf("runtime selection failed: %w", err)
        }
        rt = selection.Spec
        rtName = selection.Name
    }

    // Get the best supported model format for this runtime-model pair
    supportedFormat := r.RuntimeSelector.GetSupportedModelFormat(ctx, rt, baseModel)
    if supportedFormat != nil {
        log.Info("Using supported format", "priority", supportedFormat.Priority)
    }

    // ... continue with runtime spec ...
}
```

### Webhook Validation

```go
type InferenceServiceValidator struct {
    Client          client.Client
    RuntimeSelector runtimeselector.Selector
}

func (v *InferenceServiceValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
    isvc := obj.(*v1beta1.InferenceService)

    // Fetch the BaseModel
    // ... code to get baseModel ...

    // Check if runtime can be selected/validated (with accelerator awareness)
    if isvc.Spec.Runtime != nil && isvc.Spec.Runtime.Name != "" {
        if err := v.RuntimeSelector.ValidateRuntime(ctx, isvc.Spec.Runtime.Name, baseModel, isvc); err != nil {
            return nil, fmt.Errorf("invalid runtime selection: %w", err)
        }
    } else {
        // Verify auto-selection is possible (considers accelerator requirements)
        if _, err := v.RuntimeSelector.SelectRuntime(ctx, baseModel, isvc); err != nil {
            return nil, fmt.Errorf("no compatible runtime available: %w", err)
        }
    }

    return nil, nil
}
```

## Error Handling

The package provides rich error types with detailed information:

### NoRuntimeFoundError
```go
if runtimeselector.IsNoRuntimeFoundError(err) {
    e := err.(*runtimeselector.NoRuntimeFoundError)
    log.Info("No runtime found",
        "model", e.ModelName,
        "format", e.ModelFormat,
        "namespace", e.Namespace,
        "totalRuntimes", e.TotalRuntimes,
        "excludedCount", len(e.ExcludedRuntimes))

    // Log why each runtime was excluded
    for name, reason := range e.ExcludedRuntimes {
        log.Info("Runtime excluded", "runtime", name, "reason", reason)
    }
}
```

### RuntimeCompatibilityError
```go
if runtimeselector.IsRuntimeCompatibilityError(err) {
    e := err.(*runtimeselector.RuntimeCompatibilityError)
    log.Error(err, "Runtime incompatible",
        "runtime", e.RuntimeName,
        "model", e.ModelName,
        "format", e.ModelFormat,
        "reason", e.Reason)
}
```

## Testing

The package includes comprehensive tests covering:

- Runtime selection with multiple compatible runtimes
- Scoring algorithm verification
- Version comparison (semantic and unofficial)
- Model size range validation
- Namespace vs cluster runtime prioritization
- Error scenarios and edge cases

Run tests:
```bash
cd pkg/runtimeselector
go test -v
```

## Performance Considerations

1. **Caching**: Uses controller-runtime's cache to avoid repeated API calls
2. **Watches**: Runtime resources are watched to keep cache fresh
3. **Efficient Sorting**: Runtimes are pre-sorted by creation time and name
4. **Early Termination**: Compatibility checks fail fast on first mismatch

## Configurable Scoring

The scoring system supports runtime-defined weights for fine-tuning selection priorities:

```yaml
apiVersion: ome.io/v1beta1
kind: ServingRuntime
metadata:
  name: high-priority-pytorch-runtime
spec:
  supportedModelFormats:
  - modelFormat:
      name: pytorch
      weight: 20  # Higher weight = higher priority
    modelFramework:
      name: transformers
      weight: 15
    priority: 2     # Multiplier for all weights
    autoSelect: true
```

The final score is calculated as: `(modelFormat.weight × priority) + (modelFramework.weight × priority)`

## Accelerator-Aware Runtime Selection

The runtime selector integrates with the accelerator class system to ensure runtimes are compatible with the requested GPU types.

### How It Works

1. **Runtime Requirements**: ServingRuntimes can specify accelerator requirements:
   ```yaml
   apiVersion: ome.io/v1beta1
   kind: ServingRuntime
   metadata:
     name: sglang-gpu-optimized
   spec:
     supportedModelFormats:
       - name: safetensors
         autoSelect: true
     acceleratorRequirements:
       acceleratorClasses:
         - nvidia-h100-80gb
         - nvidia-a100-80gb
   ```

2. **InferenceService Selection**: InferenceServices can specify accelerator preferences:
   ```yaml
   apiVersion: ome.io/v1beta1
   kind: InferenceService
   metadata:
     name: llama-deployment
   spec:
     model:
       name: llama-7b
     runtime:
       name: sglang-gpu-optimized
     acceleratorSelector:
       acceleratorClass: nvidia-h100-80gb
     # Or component-specific
     engine:
       acceleratorOverride:
         acceleratorClass: nvidia-a100-80gb
   ```

3. **Compatibility Checking**: The matcher validates that:
   - If the InferenceService requires accelerator classes, the runtime's supported list must contain all of them
   - An InferenceService with no accelerator requirement is compatible with any runtime, including ones that declare accelerator requirements
   - Multiple sources are checked in priority order:
     - Component-specific `AcceleratorOverride.AcceleratorClass` (highest)
     - InferenceService-level `AcceleratorSelector.AcceleratorClass`
     - Annotation `ome.io/accelerator-class` (lowest)

### Integration with AcceleratorClassSelector

The runtime selector works alongside the `acceleratorclassselector` package:

```go
// 1. Select compatible runtime
runtime, err := runtimeSelector.SelectRuntime(ctx, model, isvc)
if err != nil {
    return err
}

// 2. Select accelerator class for each component
engineAcc, _, err := acceleratorSelector.GetAcceleratorClass(
    ctx, isvc, runtime.Spec, v1beta1.EngineComponent)
if err != nil {
    return err
}

// 3. Get the best supported format
supportedFormat := runtimeSelector.GetSupportedModelFormat(ctx, runtime.Spec, model)

// 4. Use runtime, accelerator, and format to configure deployment
```

### Example: Multi-GPU Runtime

```yaml
apiVersion: ome.io/v1beta1
kind: ServingRuntime
metadata:
  name: sglang-multi-gpu
spec:
  supportedModelFormats:
    - modelFormat:
        name: safetensors
      autoSelect: true
      priority: 1

  # Runtime supports multiple GPU types
  acceleratorRequirements:
    acceleratorClasses:
      - nvidia-h100-80gb    # Preferred for H100 features
      - nvidia-a100-80gb    # Fallback to A100
      - nvidia-a100-40gb    # Budget option
```

When this runtime is selected, the compatibility checker ensures the InferenceService's accelerator requirements can be satisfied by one of the supported accelerator classes.

## Future Enhancements

1. **Enhanced Accelerator Matching**: Advanced GPU capability-based matching
   - Automatic runtime scoring based on GPU performance characteristics
   - Cost-aware runtime selection considering GPU pricing
   - Dynamic runtime selection based on real-time GPU availability

2. **Runtime Affinity/Anti-affinity**: Support for preferred/excluded runtime lists at the InferenceService level

3. **Enhanced Version Matching**: Support for more complex version constraints and ranges (e.g., `>=2.0.0,<3.0.0`)

4. **Runtime Capabilities API**: Expose runtime capabilities for advanced selection scenarios
   - Query supported features (FP8, speculative decoding, etc.)
   - Feature-based runtime filtering

5. **Multi-Runtime Deployments**: Support for selecting different runtimes for Engine vs Decoder components

## Related Components

- **[acceleratorclassselector](../acceleratorclassselector/README.md)**: Selects accelerator classes (GPUs) for inference workloads

## Troubleshooting

### No Compatible Runtime Found

If runtime selection fails with `NoRuntimeFoundError`:

1. **Check runtime definitions**: Verify runtimes exist in the namespace or at cluster scope
   ```bash
   kubectl get servingruntimes -n <namespace>
   kubectl get clusterservingruntimes
   ```

2. **Review exclusion reasons**: The error includes detailed reasons why each runtime was excluded
   ```go
   if runtimeselector.IsNoRuntimeFoundError(err) {
       e := err.(*runtimeselector.NoRuntimeFoundError)
       for name, reason := range e.ExcludedRuntimes {
           log.Info("Runtime excluded", "name", name, "reason", reason)
       }
   }
   ```

3. **Common exclusion reasons**:
   - Runtime is disabled (`disabled: true`)
   - Model format not supported
   - Model size outside supported range
   - AutoSelect is disabled
   - Accelerator class mismatch

### Accelerator Compatibility Issues

If runtime validation fails due to accelerator requirements:

1. **Verify accelerator class exists**:
   ```bash
   kubectl get acceleratorclasses
   ```

2. **Check runtime's accelerator requirements**:
   ```bash
   kubectl get servingruntime <name> -o jsonpath='{.spec.acceleratorRequirements}'
   ```

3. **Ensure InferenceService specifies compatible accelerator**:
   ```yaml
   spec:
     acceleratorSelector:
       acceleratorClass: nvidia-h100-80gb  # Must be in runtime's supported list
   ```

4. **Review component-specific overrides**: Check if Engine or Decoder has conflicting accelerator settings
   ```yaml
   spec:
     engine:
       acceleratorOverride:
         acceleratorClass: nvidia-a100-80gb  # Must match runtime requirements
   ```

### Runtime Validation Fails

If `ValidateRuntime` returns an error:

1. **Check model format compatibility**: Ensure the runtime supports your model's format
   ```yaml
   # Runtime must have:
   spec:
     supportedModelFormats:
       - modelFormat:
           name: safetensors  # Must match model format
   ```

2. **Verify model size is within range**:
   ```yaml
   # If runtime specifies:
   spec:
     modelSizeRange:
       min: "7B"
       max: "70B"
   # Model must be within this range
   ```

3. **Check architecture and quantization**: If specified, they must match exactly
   ```yaml
   # Model architecture must match if both specify it
   model:
     modelArchitecture: "llama"
   # Runtime format must also specify:
   supportedModelFormats:
     - modelArchitecture: "llama"
   ```

### GetSupportedModelFormat Returns Nil

If `GetSupportedModelFormat` returns nil:

1. **Ensure runtime has supported formats with autoSelect enabled**:
   ```yaml
   spec:
     supportedModelFormats:
       - modelFormat:
           name: safetensors
         autoSelect: true  # Must be true
   ```

2. **Check format/framework matching**: Model and runtime formats must be compatible

3. **Verify weights are defined**: Formats with zero or missing weights won't score

### Performance Issues

If runtime selection is slow:

1. **Enable caching**: Ensure watches are configured for runtime resources
2. **Check for excessive runtimes**: Large numbers of runtimes can slow selection
3. **Review logging**: Disable detailed logging in production
   ```go
   config := &runtimeselector.Config{
       EnableDetailedLogging: false,  // Disable in production
   }
   ```

### Debugging Tips

Enable detailed logging to see selection logic:

```go
config := &runtimeselector.Config{
    Client:                mgr.GetClient(),
    EnableDetailedLogging: true,
    DefaultPriority:       1,
}
selector := runtimeselector.NewWithConfig(config)
```

Check compatibility details programmatically:

```go
runtimes, err := selector.GetCompatibleRuntimes(ctx, model, isvc, namespace)
if err != nil {
    return err
}

for _, runtime := range runtimes {
    log.Info("Compatible runtime found",
        "name", runtime.Name,
        "score", runtime.Score,
        "isCluster", runtime.IsCluster,
        "formatMatch", runtime.MatchDetails.FormatMatch,
        "frameworkMatch", runtime.MatchDetails.FrameworkMatch,
        "priority", runtime.MatchDetails.Priority)
}
```
