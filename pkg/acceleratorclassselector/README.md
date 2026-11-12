# Accelerator Class Selector Package

The `acceleratorclassselector` package provides a comprehensive solution for selecting and managing accelerator classes (GPUs) in the OME operator. It determines the appropriate accelerator class for inference workloads based on runtime requirements, InferenceService specifications, and selection policies.

## Overview

The package provides:

- **Automatic accelerator class selection** based on runtime and component requirements
- **Hierarchical selection logic** with clear precedence rules
- **Policy-based selection** supporting multiple strategies (FirstAvailable, BestFit, Cheapest, MostCapable)
- **Component-aware selection** with different accelerators for Engine and Decoder
- **Efficient caching** using controller-runtime's cache mechanism
- **Integration with AcceleratorClass CRD** for cluster-wide GPU management

## Architecture

### Component Structure

```
acceleratorclassselector/
├── types.go        # Core interfaces and data structures
├── selector.go     # Main selector implementation
├── fetcher.go      # AcceleratorClass resource fetching with caching
└── errors.go       # Custom error types
```

### Key Interfaces

#### Selector
The main interface for accelerator class selection operations:

```go
type Selector interface {
    // Get accelerator class for a specific component
    GetAcceleratorClass(ctx context.Context, isvc *v1beta1.InferenceService,
                        runtime *v1beta1.ServingRuntimeSpec,
                        component v1beta1.ComponentType) (*v1beta1.AcceleratorClass, string, error)
}
```

#### AcceleratorFetcher
Abstracts accelerator class resource fetching:

```go
type AcceleratorFetcher interface {
    // Get specific accelerator class by name
    GetAcceleratorClass(ctx context.Context, name string) (*v1beta1.AcceleratorClass, bool, error)
}
```

## Selection Algorithm

### Accelerator Class Selection Flow

The selector follows a prioritized selection logic:

1. **Check Runtime Requirements**
   - If runtime doesn't have `AcceleratorRequirements`, return nil (no accelerator needed)
   - If runtime specifies `AcceleratorClasses`, proceed with selection

2. **Selection by Name (Highest Priority)**
   - Check component-specific `AcceleratorOverride.AcceleratorClass`
     - For Engine: `isvc.Spec.Engine.AcceleratorOverride.AcceleratorClass`
     - For Decoder: `isvc.Spec.Decoder.AcceleratorOverride.AcceleratorClass`
   - Check InferenceService-level `AcceleratorSelector.AcceleratorClass`
     - `isvc.Spec.AcceleratorSelector.AcceleratorClass`

3. **Selection by Policy (Fallback)**
   - If no explicit name is specified, use policy-based selection
   - Determine effective policy from:
     - Component-specific override policy
     - InferenceService-level policy
     - If neither is set, no accelerator class is selected
   - Filter and score the candidates with the component-specific override's constraints when it states a block, else the InferenceService-level constraints
   - Apply policy logic:
     - **FirstAvailable**: Return the first eligible accelerator from runtime requirements
     - **BestFit**: Select based on best resource match (memory + compute scoring)
     - **Cheapest**: Select based on cost optimization
     - **MostCapable**: Select based on maximum capabilities

4. **Fetch and Return**
   - Fetch the selected accelerator class spec from the cluster
   - Return spec and name, or error if not found

### Selection Priority Hierarchy

```
Highest Priority
    ↓
Component.AcceleratorOverride.AcceleratorClass (Engine/Decoder)
    ↓
InferenceService.AcceleratorSelector.AcceleratorClass
    ↓
Policy-based selection:
  - Component.AcceleratorOverride.Policy
  - InferenceService.AcceleratorSelector.Policy
  - No policy set → no accelerator class selected
    ↓
Lowest Priority
```

Constraints follow the same order: the policy filters and scores the candidates with the constraints block of the component's `AcceleratorOverride` when it states one, else the InferenceService's `AcceleratorSelector` block; the block is taken whole, not merged with the service's. An empty override block (`constraints: {}`) counts as stated and clears the service's constraints for this component.

## Usage Examples

### Basic Integration

```go
// In InferenceService controller
type InferenceServiceReconciler struct {
    client.Client
    AcceleratorSelector acceleratorclassselector.Selector
    // ... other fields
}

func (r *InferenceServiceReconciler) SetupWithManager(mgr ctrl.Manager) error {
    // Initialize accelerator selector
    r.AcceleratorSelector = acceleratorclassselector.New(mgr.GetClient())

    // Add watches to populate cache
    ctrlBuilder.
        Watches(&v1beta1.AcceleratorClass{},
            handler.EnqueueRequestsFromMapFunc(func(context.Context, client.Object) []reconcile.Request {
                return nil // Just populate cache
            }))

    return ctrlBuilder.Complete(r)
}
```

### Accelerator Selection in Reconciler

```go
func (r *InferenceServiceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
    // ... fetch InferenceService and ServingRuntime ...

    // Get accelerator class for engine component
    engineAcceleratorSpec, engineAcceleratorName, err := r.AcceleratorSelector.GetAcceleratorClass(
        ctx, isvc, runtime, v1beta1.EngineComponent)
    if err != nil {
        return reconcile.Result{}, fmt.Errorf("failed to get engine accelerator: %w", err)
    }

    // Get accelerator class for decoder component (may be different)
    decoderAcceleratorSpec, decoderAcceleratorName, err := r.AcceleratorSelector.GetAcceleratorClass(
        ctx, isvc, runtime, v1beta1.DecoderComponent)
    if err != nil {
        return reconcile.Result{}, fmt.Errorf("failed to get decoder accelerator: %w", err)
    }

    // Use accelerator specs for pod configuration
    if engineAcceleratorSpec != nil {
        // Apply nodeSelector, resources, etc.
        log.Info("Using accelerator for engine", "name", engineAcceleratorName)
    }

    // ... continue with pod creation ...
}
```

### Component-Specific Selection

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: multi-gpu-deployment
spec:
  model:
    name: llama-70b
  runtime:
    name: sglang-universal

  # Different accelerators for different components
  engine:
    acceleratorOverride:
      acceleratorClass: nvidia-h100-80gb  # High-end GPU for compute-intensive engine

  decoder:
    acceleratorOverride:
      acceleratorClass: nvidia-a100-40gb  # Mid-range GPU for decoder
```

### Policy-Based Selection

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: cost-optimized-deployment
spec:
  model:
    name: llama-13b
  runtime:
    name: sglang-universal

  # Use policy for automatic selection
  acceleratorSelector:
    policy: FirstAvailable  # or: BestFit, Cheapest, MostCapable
```

### InferenceService-Level Default

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: standard-deployment
spec:
  model:
    name: llama-7b
  runtime:
    name: sglang-universal

  # Apply to all components (engine, decoder)
  acceleratorSelector:
    acceleratorClass: nvidia-a100-40gb
```

## Configuration

The package supports configuration through the `Config` struct:

```go
type Config struct {
    // Client is the Kubernetes client (uses controller-runtime cache)
    Client client.Client
}
```

### Creating a Selector with Custom Configuration

```go
config := &acceleratorclassselector.Config{
    Client: mgr.GetClient(),
}

selector := acceleratorclassselector.NewWithConfig(config)
```

## Error Handling

The package reports a missing class with one error type callers can test for:

### AcceleratorNotFoundError

```go
var e *acceleratorclassselector.AcceleratorNotFoundError
if errors.As(err, &e) {
    log.Error(err, "Accelerator class not found",
        "acceleratorClass", e.AcceleratorClassName)

    // Handle missing accelerator class
    // Perhaps fall back to default or fail the deployment
}
```

## Integration with Runtime Selector

The accelerator class selector works in conjunction with the runtime selector:

```go
// 1. Select runtime based on model
runtime, err := runtimeSelector.SelectRuntime(ctx, model, isvc)
if err != nil {
    return err
}

// 2. Check if runtime requires accelerators
if runtime.Spec.AcceleratorRequirements == nil {
    // No accelerator needed, proceed with CPU-only deployment
    return nil
}

// 3. Select accelerator for each component
engineAcc, engineName, err := acceleratorSelector.GetAcceleratorClass(
    ctx, isvc, runtime.Spec, v1beta1.EngineComponent)
if err != nil {
    return err
}

// 4. Apply accelerator configuration to pod spec
podSpec := buildPodSpec(runtime, engineAcc)
```

## Selection Policies

### FirstAvailable
- Returns the first eligible accelerator class from runtime's `AcceleratorRequirements.AcceleratorClasses`
- Fast and predictable
- Best for simple deployments

### BestFit
- Selects accelerator that best matches model requirements
- Considers memory, compute capability, and features
- Optimizes resource utilization

### Cheapest
- Selects lowest-cost accelerator that meets requirements
- Uses `AcceleratorClass.Spec.Cost` information
- Optimizes for cost efficiency

### MostCapable
- Selects accelerator with maximum capabilities
- Optimizes for performance
- Best for latency-sensitive workloads

## Component Types

The selector handles different component types:

- **EngineComponent**: Main inference engine (GPU-intensive)
- **DecoderComponent**: Decoder for speculative decoding (may use different GPU)
- **RouterComponent**: Traffic router (typically CPU-only, no accelerator)

Note: Router components are explicitly excluded from accelerator selection as they don't require GPU resources.

## Testing

The package includes comprehensive tests covering:

- Accelerator class fetching from cluster
- Selection by explicit name
- Selection by policy
- Component-specific overrides
- InferenceService-level defaults
- Error scenarios (missing accelerator classes)

Run tests:
```bash
cd pkg/acceleratorclassselector
go test -v
```

## Performance Considerations

1. **Caching**: Uses controller-runtime's cache to avoid repeated API calls
2. **Watches**: AcceleratorClass resources are watched to keep cache fresh
3. **Early Return**: Returns nil quickly when runtime doesn't require accelerators
4. **Efficient Lookup**: Direct lookup by name when explicit accelerator class is specified

## Future Enhancements

1. **Policy Implementation**: Complete implementation of BestFit, Cheapest, and MostCapable policies
2. **Cost Integration**: Implementation of AccleratorClass resource choose with GPU cost and customer cost plan
3. **Cost Tracking**: Integration with cost management systems
4. **Capability Matching**: Advanced matching based on required GPU features
5. **Availability Awareness**: Consider accelerator availability in selection

## Related Components

- **[runtimeselector](../runtimeselector/README.md)**: Selects serving runtime based on model requirements
- **AcceleratorClass CRD**: Defines cluster-wide accelerator resources and capabilities

## Example AcceleratorClass

```yaml
apiVersion: ome.io/v1beta1
kind: AcceleratorClass
metadata:
  name: nvidia-h100-80gb
spec:
  vendor: nvidia
  family: hopper
  model: h100

  discovery:
    nodeSelector:
      nvidia.com/gpu.product: "NVIDIA-H100-80GB-HBM3"

  capabilities:
    memoryGB: 80Gi
    computeCapability: "9.0"
    features:
      - tensor-cores
      - transformer-engine
      - fp8

  resources:
    - name: nvidia.com/gpu
      quantity: "1"

  cost:
    perHour: "5.12"
    tier: "high"
```

## Troubleshooting

### Accelerator Not Found

If selection fails with `AcceleratorNotFoundError`:
1. Verify AcceleratorClass exists: `kubectl get acceleratorclasses`
2. Check name spelling matches exactly
3. Ensure runtime's `AcceleratorRequirements.AcceleratorClasses` includes the desired accelerator

### No Accelerator Selected

If no accelerator is selected but one is expected:
1. Check if runtime has `AcceleratorRequirements` specified
2. Verify InferenceService or component has accelerator configuration
3. Review selection policy and ensure it matches your requirements

### Wrong Accelerator Selected

If unexpected accelerator is selected:
1. Review selection priority hierarchy
2. Check for component-specific overrides
3. Verify policy configuration
4. Enable detailed logging for debugging
