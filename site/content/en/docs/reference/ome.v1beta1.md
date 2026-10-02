---
title: OME API
content_type: tool-reference
package: ome.io/v1beta1
auto_generated: true
description: Generated API reference documentation for ome.io/v1beta1.
weight: 1
---
<p>Package v1beta1 contains API Schema definitions for the serving v1beta1 API group</p>


## Resource Types


- [BaseModel](#ome-io-v1beta1-BaseModel)
- [BenchmarkJob](#ome-io-v1beta1-BenchmarkJob)
- [ClusterBaseModel](#ome-io-v1beta1-ClusterBaseModel)
- [ClusterServingRuntime](#ome-io-v1beta1-ClusterServingRuntime)
- [FineTunedWeight](#ome-io-v1beta1-FineTunedWeight)
- [InferenceService](#ome-io-v1beta1-InferenceService)
- [ServingRuntime](#ome-io-v1beta1-ServingRuntime)


## `BaseModel`     {#ome-io-v1beta1-BaseModel}


**Appears in:**



<p>BaseModel is the Schema for the basemodels API</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody><tr><td><code>apiVersion</code><br/>string</td><td><code>ome.io/v1beta1</code></td></tr>
<tr><td><code>kind</code><br/>string</td><td><code>BaseModel</code></td></tr>


<tr><td><code>spec</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-BaseModelSpec"><code>BaseModelSpec</code></a>
</td>
<td>
   <span class="text-muted">No description provided.</span></td>
</tr>
<tr><td><code>status</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-ModelStatusSpec"><code>ModelStatusSpec</code></a>
</td>
<td>
   <span class="text-muted">No description provided.</span></td>
</tr>
</tbody>
</table>

## `BenchmarkJob`     {#ome-io-v1beta1-BenchmarkJob}


**Appears in:**



<p>BenchmarkJob is the schema for the BenchmarkJobs API</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody><tr><td><code>apiVersion</code><br/>string</td><td><code>ome.io/v1beta1</code></td></tr>
<tr><td><code>kind</code><br/>string</td><td><code>BenchmarkJob</code></td></tr>


<tr><td><code>spec</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-BenchmarkJobSpec"><code>BenchmarkJobSpec</code></a>
</td>
<td>
   <span class="text-muted">No description provided.</span></td>
</tr>
<tr><td><code>status</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-BenchmarkJobStatus"><code>BenchmarkJobStatus</code></a>
</td>
<td>
   <span class="text-muted">No description provided.</span></td>
</tr>
</tbody>
</table>

## `ClusterBaseModel`     {#ome-io-v1beta1-ClusterBaseModel}


**Appears in:**



<p>ClusterBaseModel is the Schema for the basemodels API</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody><tr><td><code>apiVersion</code><br/>string</td><td><code>ome.io/v1beta1</code></td></tr>
<tr><td><code>kind</code><br/>string</td><td><code>ClusterBaseModel</code></td></tr>


<tr><td><code>spec</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-BaseModelSpec"><code>BaseModelSpec</code></a>
</td>
<td>
   <span class="text-muted">No description provided.</span></td>
</tr>
<tr><td><code>status</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-ModelStatusSpec"><code>ModelStatusSpec</code></a>
</td>
<td>
   <span class="text-muted">No description provided.</span></td>
</tr>
</tbody>
</table>

## `ClusterServingRuntime`     {#ome-io-v1beta1-ClusterServingRuntime}


**Appears in:**



<p>ClusterServingRuntime is the Schema for the servingruntimes API</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody><tr><td><code>apiVersion</code><br/>string</td><td><code>ome.io/v1beta1</code></td></tr>
<tr><td><code>kind</code><br/>string</td><td><code>ClusterServingRuntime</code></td></tr>


<tr><td><code>spec</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-ServingRuntimeSpec"><code>ServingRuntimeSpec</code></a>
</td>
<td>
   <span class="text-muted">No description provided.</span></td>
</tr>
<tr><td><code>status</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-ServingRuntimeStatus"><code>ServingRuntimeStatus</code></a>
</td>
<td>
   <span class="text-muted">No description provided.</span></td>
</tr>
</tbody>
</table>

## `FineTunedWeight`     {#ome-io-v1beta1-FineTunedWeight}


**Appears in:**



<p>FineTunedWeight is the Schema for the finetunedweights API</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody><tr><td><code>apiVersion</code><br/>string</td><td><code>ome.io/v1beta1</code></td></tr>
<tr><td><code>kind</code><br/>string</td><td><code>FineTunedWeight</code></td></tr>


<tr><td><code>spec</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-FineTunedWeightSpec"><code>FineTunedWeightSpec</code></a>
</td>
<td>
   <span class="text-muted">No description provided.</span></td>
</tr>
<tr><td><code>status</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-ModelStatusSpec"><code>ModelStatusSpec</code></a>
</td>
<td>
   <span class="text-muted">No description provided.</span></td>
</tr>
</tbody>
</table>

## `InferenceService`     {#ome-io-v1beta1-InferenceService}


**Appears in:**



<p>InferenceService is the Schema for the InferenceServices API</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody><tr><td><code>apiVersion</code><br/>string</td><td><code>ome.io/v1beta1</code></td></tr>
<tr><td><code>kind</code><br/>string</td><td><code>InferenceService</code></td></tr>


<tr><td><code>spec</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-InferenceServiceSpec"><code>InferenceServiceSpec</code></a>
</td>
<td>
   <span class="text-muted">No description provided.</span></td>
</tr>
<tr><td><code>status</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-InferenceServiceStatus"><code>InferenceServiceStatus</code></a>
</td>
<td>
   <span class="text-muted">No description provided.</span></td>
</tr>
</tbody>
</table>

## `ServingRuntime`     {#ome-io-v1beta1-ServingRuntime}


**Appears in:**



<p>ServingRuntime is the Schema for the servingruntimes API</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody><tr><td><code>apiVersion</code><br/>string</td><td><code>ome.io/v1beta1</code></td></tr>
<tr><td><code>kind</code><br/>string</td><td><code>ServingRuntime</code></td></tr>


<tr><td><code>spec</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-ServingRuntimeSpec"><code>ServingRuntimeSpec</code></a>
</td>
<td>
   <span class="text-muted">No description provided.</span></td>
</tr>
<tr><td><code>status</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-ServingRuntimeStatus"><code>ServingRuntimeStatus</code></a>
</td>
<td>
   <span class="text-muted">No description provided.</span></td>
</tr>
</tbody>
</table>

## `AcceleratorCapabilities`     {#ome-io-v1beta1-AcceleratorCapabilities}


**Appears in:**

- [AcceleratorClassSpec](#ome-io-v1beta1-AcceleratorClassSpec)



<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>memoryGB</code><br/>
<a href="https://pkg.go.dev/k8s.io/apimachinery/pkg/api/resource#Quantity"><code>k8s.io/apimachinery/pkg/api/resource.Quantity</code></a>
</td>
<td>
   <p>Memory capacity in GB</p>
</td>
</tr>
<tr><td><code>computeCapability</code><br/>
<code>string</code>
</td>
<td>
   <p>Compute capability (NVIDIA) or equivalent</p>
</td>
</tr>
<tr><td><code>levelZeroVersion</code><br/>
<code>string</code>
</td>
<td>
   <p>Level Zero version (for Intel accelerators)</p>
</td>
</tr>
<tr><td><code>clockSpeedMHz</code><br/>
<code>int32</code>
</td>
<td>
   <p>Clock speeds</p>
</td>
</tr>
<tr><td><code>memoryBandwidthGBps</code><br/>
<a href="https://pkg.go.dev/k8s.io/apimachinery/pkg/api/resource#Quantity"><code>k8s.io/apimachinery/pkg/api/resource.Quantity</code></a>
</td>
<td>
   <p>Memory bandwidth</p>
</td>
</tr>
<tr><td><code>features</code><br/>
<code>[]string</code>
</td>
<td>
   <p>Features supported by this accelerator</p>
</td>
</tr>
<tr><td><code>performance</code><br/>
<a href="#ome-io-v1beta1-AcceleratorPerformance"><code>AcceleratorPerformance</code></a>
</td>
<td>
   <p>Performance metrics</p>
</td>
</tr>
</tbody>
</table>

## `AcceleratorClass`     {#ome-io-v1beta1-AcceleratorClass}


**Appears in:**




<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>spec</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-AcceleratorClassSpec"><code>AcceleratorClassSpec</code></a>
</td>
<td>
   <span class="text-muted">No description provided.</span></td>
</tr>
<tr><td><code>status</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-AcceleratorClassStatus"><code>AcceleratorClassStatus</code></a>
</td>
<td>
   <span class="text-muted">No description provided.</span></td>
</tr>
</tbody>
</table>

## `AcceleratorClassSpec`     {#ome-io-v1beta1-AcceleratorClassSpec}


**Appears in:**

- [AcceleratorClass](#ome-io-v1beta1-AcceleratorClass)



<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>vendor</code><br/>
<code>string</code>
</td>
<td>
   <p>Vendor of the accelerator (nvidia, amd, intel, etc.)</p>
</td>
</tr>
<tr><td><code>family</code><br/>
<code>string</code>
</td>
<td>
   <p>Family of the accelerator (ampere, hopper, cdna2, etc.)</p>
</td>
</tr>
<tr><td><code>model</code><br/>
<code>string</code>
</td>
<td>
   <p>Model name (a100, h100, mi250x, etc.)</p>
</td>
</tr>
<tr><td><code>discovery</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-AcceleratorDiscovery"><code>AcceleratorDiscovery</code></a>
</td>
<td>
   <p>Discovery patterns to identify nodes with this accelerator</p>
</td>
</tr>
<tr><td><code>capabilities</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-AcceleratorCapabilities"><code>AcceleratorCapabilities</code></a>
</td>
<td>
   <p>Capabilities of this accelerator class</p>
</td>
</tr>
<tr><td><code>resources</code><br/>
<a href="#ome-io-v1beta1-AcceleratorResource"><code>[]AcceleratorResource</code></a>
</td>
<td>
   <p>Resources exposed by this accelerator</p>
</td>
</tr>
<tr><td><code>integration</code><br/>
<a href="#ome-io-v1beta1-AcceleratorIntegration"><code>AcceleratorIntegration</code></a>
</td>
<td>
   <p>Integration with external systems</p>
</td>
</tr>
<tr><td><code>cost</code><br/>
<a href="#ome-io-v1beta1-AcceleratorCost"><code>AcceleratorCost</code></a>
</td>
<td>
   <p>Cost information for optimization decisions</p>
</td>
</tr>
</tbody>
</table>

## `AcceleratorClassStatus`     {#ome-io-v1beta1-AcceleratorClassStatus}


**Appears in:**

- [AcceleratorClass](#ome-io-v1beta1-AcceleratorClass)



<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>nodes</code><br/>
<code>[]string</code>
</td>
<td>
   <p>Nodes that have this accelerator</p>
</td>
</tr>
<tr><td><code>totalAccelerators</code><br/>
<code>int32</code>
</td>
<td>
   <p>Total number of accelerators in the cluster</p>
</td>
</tr>
<tr><td><code>availableAccelerators</code><br/>
<code>int32</code>
</td>
<td>
   <p>Available accelerators (not allocated)</p>
</td>
</tr>
<tr><td><code>lastUpdated</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#time-v1-meta"><code>k8s.io/apimachinery/pkg/apis/meta/v1.Time</code></a>
</td>
<td>
   <p>Last update time</p>
</td>
</tr>
<tr><td><code>conditions</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#condition-v1-meta"><code>[]k8s.io/apimachinery/pkg/apis/meta/v1.Condition</code></a>
</td>
<td>
   <p>Conditions represent the latest available observations</p>
</td>
</tr>
<tr><td><code>availableNodes</code><br/>
<code>int32</code>
</td>
<td>
   <p>AvailableNodes is the number of nodes that have this accelerator available</p>
</td>
</tr>
</tbody>
</table>

## `AcceleratorConstraints`     {#ome-io-v1beta1-AcceleratorConstraints}


**Appears in:**

- [AcceleratorSelector](#ome-io-v1beta1-AcceleratorSelector)


<p>AcceleratorConstraints defines requirements for accelerator selection</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>minMemory</code><br/>
<code>int64</code>
</td>
<td>
   <p>MinMemory in GB</p>
</td>
</tr>
<tr><td><code>maxMemory</code><br/>
<code>int64</code>
</td>
<td>
   <p>MaxMemory in GB (useful for cost control)</p>
</td>
</tr>
<tr><td><code>minComputePerformanceTFLOPS</code><br/>
<code>int64</code>
</td>
<td>
   <p>MinComputePerformanceTFLOPS in TFLOPS</p>
</td>
</tr>
<tr><td><code>minArchitectureVersion</code><br/>
<code>string</code>
</td>
<td>
   <p>MinArchitectureVersion Compute capability (NVIDIA) or equivalent</p>
</td>
</tr>
<tr><td><code>requiredFeatures</code><br/>
<code>[]string</code>
</td>
<td>
   <p>RequiredFeatures that must be present</p>
</td>
</tr>
<tr><td><code>excludedClasses</code><br/>
<code>[]string</code>
</td>
<td>
   <p>ExcludedClasses lists AcceleratorClasses to avoid</p>
</td>
</tr>
<tr><td><code>architectureFamilies</code><br/>
<code>[]string</code>
</td>
<td>
   <p>ArchitectureFamilies limits selection to specific families
Examples: [&quot;nvidia-hopper&quot;, &quot;nvidia-ampere&quot;]</p>
</td>
</tr>
<tr><td><code>preferredPrecisions</code><br/>
<code>[]string</code>
</td>
<td>
   <p>PreferredPrecisions lists numeric precisions in order of preference
Examples: [&quot;fp8&quot;, &quot;fp16&quot;, &quot;fp32&quot;]</p>
</td>
</tr>
</tbody>
</table>

## `AcceleratorCost`     {#ome-io-v1beta1-AcceleratorCost}


**Appears in:**

- [AcceleratorClassSpec](#ome-io-v1beta1-AcceleratorClassSpec)



<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>perHour</code><br/>
<a href="https://pkg.go.dev/k8s.io/apimachinery/pkg/api/resource#Quantity"><code>k8s.io/apimachinery/pkg/api/resource.Quantity</code></a>
</td>
<td>
   <p>Cost per hour in dollars</p>
</td>
</tr>
<tr><td><code>perMillionTokens</code><br/>
<a href="https://pkg.go.dev/k8s.io/apimachinery/pkg/api/resource#Quantity"><code>k8s.io/apimachinery/pkg/api/resource.Quantity</code></a>
</td>
<td>
   <p>Cost per million tokens (for usage-based pricing)</p>
</td>
</tr>
<tr><td><code>spotPerHour</code><br/>
<a href="https://pkg.go.dev/k8s.io/apimachinery/pkg/api/resource#Quantity"><code>k8s.io/apimachinery/pkg/api/resource.Quantity</code></a>
</td>
<td>
   <p>Spot instance pricing if available</p>
</td>
</tr>
<tr><td><code>tier</code><br/>
<code>string</code>
</td>
<td>
   <p>Cost tier for simplified selection (low, medium, high)</p>
</td>
</tr>
</tbody>
</table>

## `AcceleratorDiscovery`     {#ome-io-v1beta1-AcceleratorDiscovery}


**Appears in:**

- [AcceleratorClassSpec](#ome-io-v1beta1-AcceleratorClassSpec)



<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>nodeSelector</code><br/>
<code>map[string]string</code>
</td>
<td>
   <p>NodeSelector to identify nodes with this accelerator</p>
</td>
</tr>
<tr><td><code>affinity</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#affinity-v1-core"><code>k8s.io/api/core/v1.Affinity</code></a>
</td>
<td>
   <p>Affinity for more complex node selection</p>
</td>
</tr>
<tr><td><code>pciVendorID</code><br/>
<code>string</code>
</td>
<td>
   <p>PCIVendorID for device discovery (e.g., &quot;10de&quot; for NVIDIA)</p>
</td>
</tr>
<tr><td><code>deviceIDs</code><br/>
<code>[]string</code>
</td>
<td>
   <p>DeviceIDs list of PCI device IDs</p>
</td>
</tr>
</tbody>
</table>

## `AcceleratorIntegration`     {#ome-io-v1beta1-AcceleratorIntegration}


**Appears in:**

- [AcceleratorClassSpec](#ome-io-v1beta1-AcceleratorClassSpec)



<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>kueueResourceFlavor</code><br/>
<code>string</code>
</td>
<td>
   <p>KueueResourceFlavor name to sync with</p>
</td>
</tr>
<tr><td><code>volcanoGPUType</code><br/>
<code>string</code>
</td>
<td>
   <p>VolcanoGPUType for Volcano integration</p>
</td>
</tr>
</tbody>
</table>

## `AcceleratorLatency`     {#ome-io-v1beta1-AcceleratorLatency}


**Appears in:**

- [AcceleratorPerformance](#ome-io-v1beta1-AcceleratorPerformance)



<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>averageMillis</code><br/>
<code>int64</code>
</td>
<td>
   <p>Average latency in milliseconds</p>
</td>
</tr>
<tr><td><code>maximumMillis</code><br/>
<code>int64</code>
</td>
<td>
   <p>Maximum latency in milliseconds</p>
</td>
</tr>
</tbody>
</table>

## `AcceleratorPerformance`     {#ome-io-v1beta1-AcceleratorPerformance}


**Appears in:**

- [AcceleratorCapabilities](#ome-io-v1beta1-AcceleratorCapabilities)



<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>fp32Tflops</code><br/>
<code>int64</code>
</td>
<td>
   <p>FP32 performance in TFLOPS</p>
</td>
</tr>
<tr><td><code>fp16Tflops</code><br/>
<code>int64</code>
</td>
<td>
   <p>FP16 performance in TFLOPS</p>
</td>
</tr>
<tr><td><code>int8Tops</code><br/>
<code>int64</code>
</td>
<td>
   <p>INT8 performance in TOPS</p>
</td>
</tr>
<tr><td><code>int4Tops</code><br/>
<code>int64</code>
</td>
<td>
   <p>INT4 performance in TOPS</p>
</td>
</tr>
<tr><td><code>latency</code><br/>
<a href="#ome-io-v1beta1-AcceleratorLatency"><code>AcceleratorLatency</code></a>
</td>
<td>
   <p>Latency metrics</p>
</td>
</tr>
</tbody>
</table>

## `AcceleratorRequirements`     {#ome-io-v1beta1-AcceleratorRequirements}


**Appears in:**

- [ServingRuntimeSpec](#ome-io-v1beta1-ServingRuntimeSpec)


<p>AcceleratorRequirements specifies the accelerator requirements for this runtime</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>acceleratorClasses</code><br/>
<code>[]string</code>
</td>
<td>
   <p>AcceleratorClasses lists the names of AcceleratorClasses this runtime supports</p>
</td>
</tr>
<tr><td><code>minMemory</code><br/>
<code>int64</code>
</td>
<td>
   <p>MinMemory specifies minimum GPU memory required in GB</p>
</td>
</tr>
<tr><td><code>minComputePerformanceTFLOPS</code><br/>
<code>int64</code>
</td>
<td>
   <p>MinComputePerformanceTFLOPS specifies minimum compute capability in TFLOPS</p>
</td>
</tr>
<tr><td><code>minArchitectureVersion</code><br/>
<code>string</code>
</td>
<td>
   <p>MinArchitectureVersion Compute capability (NVIDIA) or equivalent</p>
</td>
</tr>
<tr><td><code>requiredFeatures</code><br/>
<code>[]string</code>
</td>
<td>
   <p>RequiredFeatures lists hardware features that must be present
Examples: [&quot;tensor-cores&quot;, &quot;fp8&quot;, &quot;nvlink&quot;]</p>
</td>
</tr>
<tr><td><code>preferredPrecisions</code><br/>
<code>[]string</code>
</td>
<td>
   <p>PreferredPrecisions lists numeric precisions in order of preference
Examples: [&quot;fp8&quot;, &quot;fp16&quot;, &quot;fp32&quot;]</p>
</td>
</tr>
</tbody>
</table>

## `AcceleratorResource`     {#ome-io-v1beta1-AcceleratorResource}


**Appears in:**

- [AcceleratorClassSpec](#ome-io-v1beta1-AcceleratorClassSpec)



<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>name</code> <B>[Required]</B><br/>
<code>string</code>
</td>
<td>
   <p>Name of the resource (e.g., nvidia.com/gpu)</p>
</td>
</tr>
<tr><td><code>quantity</code> <B>[Required]</B><br/>
<a href="https://pkg.go.dev/k8s.io/apimachinery/pkg/api/resource#Quantity"><code>k8s.io/apimachinery/pkg/api/resource.Quantity</code></a>
</td>
<td>
   <p>Quantity per accelerator</p>
</td>
</tr>
<tr><td><code>divisible</code><br/>
<code>bool</code>
</td>
<td>
   <p>Divisible indicates if the resource can be subdivided</p>
</td>
</tr>
</tbody>
</table>

## `AcceleratorSelection`     {#ome-io-v1beta1-AcceleratorSelection}


**Appears in:**

- [ComponentStatusSpec](#ome-io-v1beta1-ComponentStatusSpec)


<p>AcceleratorSelection shows what accelerator was selected and why</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>acceleratorClass</code> <B>[Required]</B><br/>
<code>string</code>
</td>
<td>
   <p>AcceleratorClass that was selected</p>
</td>
</tr>
<tr><td><code>reason</code><br/>
<code>string</code>
</td>
<td>
   <p>Reason explains why this accelerator was selected</p>
</td>
</tr>
<tr><td><code>nodeSelector</code><br/>
<code>map[string]string</code>
</td>
<td>
   <p>NodeSelector that was applied to pods</p>
</td>
</tr>
<tr><td><code>resourceRequests</code><br/>
<code>map[string]string</code>
</td>
<td>
   <p>ResourceRequests that were applied to pods</p>
</td>
</tr>
</tbody>
</table>

## `AcceleratorSelectionPolicy`     {#ome-io-v1beta1-AcceleratorSelectionPolicy}

(Alias of `string`)

**Appears in:**

- [AcceleratorSelector](#ome-io-v1beta1-AcceleratorSelector)


<p>AcceleratorSelectionPolicy defines how to select among matching accelerators</p>




## `AcceleratorSelector`     {#ome-io-v1beta1-AcceleratorSelector}


**Appears in:**

- [DecoderSpec](#ome-io-v1beta1-DecoderSpec)

- [EngineSpec](#ome-io-v1beta1-EngineSpec)

- [InferenceServiceSpec](#ome-io-v1beta1-InferenceServiceSpec)


<p>AcceleratorSelector defines how to select accelerators for the InferenceService</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>acceleratorClass</code><br/>
<code>string</code>
</td>
<td>
   <p>AcceleratorClass explicitly selects a specific AcceleratorClass
Takes precedence over other selectors</p>
</td>
</tr>
<tr><td><code>constraints</code><br/>
<a href="#ome-io-v1beta1-AcceleratorConstraints"><code>AcceleratorConstraints</code></a>
</td>
<td>
   <p>Constraints defines requirements that accelerators must meet</p>
</td>
</tr>
<tr><td><code>policy</code><br/>
<a href="#ome-io-v1beta1-AcceleratorSelectionPolicy"><code>AcceleratorSelectionPolicy</code></a>
</td>
<td>
   <p>Policy defines the selection policy when multiple accelerators match</p>
</td>
</tr>
</tbody>
</table>

## `BaseModelSpec`     {#ome-io-v1beta1-BaseModelSpec}


**Appears in:**

- [BaseModel](#ome-io-v1beta1-BaseModel)

- [ClusterBaseModel](#ome-io-v1beta1-ClusterBaseModel)


<p>BaseModelSpec defines the desired state of BaseModel</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>modelFormat</code><br/>
<a href="#ome-io-v1beta1-ModelFormat"><code>ModelFormat</code></a>
</td>
<td>
   <span class="text-muted">No description provided.</span></td>
</tr>
<tr><td><code>modelType</code><br/>
<code>string</code>
</td>
<td>
   <p>ModelType defines the architecture family of the model (e.g., &quot;bert&quot;, &quot;gpt2&quot;, &quot;llama&quot;).
This value typically corresponds to the &quot;model_type&quot; field in a Hugging Face model's config.json.
It is used to identify the transformer architecture and inform runtime selection and tokenizer behavior.</p>
</td>
</tr>
<tr><td><code>modelFramework</code><br/>
<a href="#ome-io-v1beta1-ModelFrameworkSpec"><code>ModelFrameworkSpec</code></a>
</td>
<td>
   <p>ModelFramework specifies the underlying framework used by the model,
such as &quot;ONNX&quot;, &quot;TensorFlow&quot;, &quot;PyTorch&quot;, &quot;Transformer&quot;, or &quot;TensorRTLLM&quot;.
This value helps determine the appropriate runtime for model serving.</p>
</td>
</tr>
<tr><td><code>modelArchitecture</code><br/>
<code>string</code>
</td>
<td>
   <p>ModelArchitecture specifies the concrete model implementation or head,
such as &quot;LlamaForCausalLM&quot;, &quot;GemmaForCausalLM&quot;, or &quot;MixtralForCausalLM&quot;.
This is often derived from the &quot;architectures&quot; field in Hugging Face config.json.</p>
</td>
</tr>
<tr><td><code>quantization</code><br/>
<a href="#ome-io-v1beta1-ModelQuantization"><code>ModelQuantization</code></a>
</td>
<td>
   <p>Quantization defines the quantization scheme applied to the model weights,
such as &quot;fp8&quot;, &quot;fbgemm_fp8&quot;, or &quot;int4&quot;. This influences runtime compatibility and performance.</p>
</td>
</tr>
<tr><td><code>modelParameterSize</code><br/>
<code>string</code>
</td>
<td>
   <p>ModelParameterSize indicates the total number of parameters in the model,
expressed in human-readable form such as &quot;7B&quot;, &quot;13B&quot;, or &quot;175B&quot;.
This can be used for scheduling or runtime selection.</p>
</td>
</tr>
<tr><td><code>modelCapabilities</code><br/>
<code>[]string</code>
</td>
<td>
   <p>ModelCapabilities of the model, e.g., &quot;TEXT_GENERATION&quot;, &quot;TEXT_SUMMARIZATION&quot;, &quot;TEXT_EMBEDDINGS&quot;</p>
</td>
</tr>
<tr><td><code>apiCapabilities</code><br/>
<code>[]string</code>
</td>
<td>
   <p>API capabilities supported by the model, e.g., &quot;OPENAI_V1_CHAT_COMPLETIONS&quot;</p>
</td>
</tr>
<tr><td><code>modelConfiguration</code><br/>
<a href="https://pkg.go.dev/k8s.io/apimachinery/pkg/runtime#RawExtension"><code>k8s.io/apimachinery/pkg/runtime.RawExtension</code></a>
</td>
<td>
   <p>Configuration of the model, stored as generic JSON for flexibility.</p>
</td>
</tr>
<tr><td><code>storage</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-StorageSpec"><code>StorageSpec</code></a>
</td>
<td>
   <p>Storage configuration for the model</p>
</td>
</tr>
<tr><td><code>ModelExtensionSpec</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-ModelExtensionSpec"><code>ModelExtensionSpec</code></a>
</td>
<td>(Members of <code>ModelExtensionSpec</code> are embedded into this type.)
   <p>ModelExtension is the common extension of the model</p>
</td>
</tr>
<tr><td><code>servingMode</code> <B>[Required]</B><br/>
<code>[]string</code>
</td>
<td>
   <span class="text-muted">No description provided.</span></td>
</tr>
<tr><td><code>maxTokens</code><br/>
<code>int32</code>
</td>
<td>
   <p>MaxTokens is the maximum number of tokens that can be processed by the model</p>
</td>
</tr>
<tr><td><code>diffusionPipeline</code><br/>
<a href="#ome-io-v1beta1-DiffusionPipelineSpec"><code>DiffusionPipelineSpec</code></a>
</td>
<td>
   <p>DiffusionPipeline captures pipeline-specific metadata for diffusion models (from model_index.json).</p>
</td>
</tr>
<tr><td><code>additionalMetadata</code><br/>
<code>map[string]string</code>
</td>
<td>
   <p>Additional metadata for the model</p>
</td>
</tr>
</tbody>
</table>

## `BenchmarkJobSpec`     {#ome-io-v1beta1-BenchmarkJobSpec}


**Appears in:**

- [BenchmarkJob](#ome-io-v1beta1-BenchmarkJob)


<p>BenchmarkJobSpec defines the specification for a benchmark job.
All fields within this specification collectively represent the desired
state and configuration of a BenchmarkJob.</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>huggingFaceSecretReference</code><br/>
<a href="#ome-io-v1beta1-HuggingFaceSecretReference"><code>HuggingFaceSecretReference</code></a>
</td>
<td>
   <p>HuggingFaceSecretReference is a reference to a Kubernetes Secret containing the Hugging Face API key.
The referenced Secret must reside in the same namespace as the BenchmarkJob.
This field replaces the raw HuggingFaceAPIKey field for improved security.</p>
</td>
</tr>
<tr><td><code>endpoint</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-EndpointSpec"><code>EndpointSpec</code></a>
</td>
<td>
   <p>Endpoint is the reference to the inference service to benchmark.</p>
</td>
</tr>
<tr><td><code>serviceMetadata</code><br/>
<a href="#ome-io-v1beta1-ServiceMetadata"><code>ServiceMetadata</code></a>
</td>
<td>
   <p>ServiceMetadata records metadata about the backend model server or service being benchmarked.
This includes details such as server engine, version, and GPU configuration for filtering experiments.</p>
</td>
</tr>
<tr><td><code>task</code> <B>[Required]</B><br/>
<code>string</code>
</td>
<td>
   <p>Task specifies the task to benchmark, pattern: <!-- raw HTML omitted -->-to-<!-- raw HTML omitted --> (e.g., &quot;text-to-text&quot;, &quot;image-to-text&quot;).</p>
</td>
</tr>
<tr><td><code>trafficScenarios</code><br/>
<code>[]string</code>
</td>
<td>
   <p>TrafficScenarios contains a list of traffic scenarios to simulate during the benchmark.
If not provided, defaults will be assigned via genai-bench.</p>
</td>
</tr>
<tr><td><code>numConcurrency</code><br/>
<code>[]int</code>
</td>
<td>
   <p>NumConcurrency defines a list of concurrency levels to test during the benchmark.
If not provided, defaults will be assigned via genai-bench.</p>
</td>
</tr>
<tr><td><code>maxTimePerIteration</code> <B>[Required]</B><br/>
<code>int</code>
</td>
<td>
   <p>MaxTimePerIteration specifies the maximum time (in minutes) for a single iteration.
Each iteration runs for a specific combination of TrafficScenarios and NumConcurrency.</p>
</td>
</tr>
<tr><td><code>maxRequestsPerIteration</code> <B>[Required]</B><br/>
<code>int</code>
</td>
<td>
   <p>MaxRequestsPerIteration specifies the maximum number of requests for a single iteration.
Each iteration runs for a specific combination of TrafficScenarios and NumConcurrency.</p>
</td>
</tr>
<tr><td><code>additionalRequestParams</code><br/>
<code>map[string]string</code>
</td>
<td>
   <p>AdditionalRequestParams contains additional request parameters as a map.</p>
</td>
</tr>
<tr><td><code>dataset</code><br/>
<a href="#ome-io-v1beta1-StorageSpec"><code>StorageSpec</code></a>
</td>
<td>
   <p>Dataset is the dataset used for benchmarking.
It is optional and only required for tasks other than &quot;text-to-<!-- raw HTML omitted -->&quot;.</p>
</td>
</tr>
<tr><td><code>outputLocation</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-StorageSpec"><code>StorageSpec</code></a>
</td>
<td>
   <p>OutputLocation specifies where the benchmark results will be stored (e.g., object storage).</p>
</td>
</tr>
<tr><td><code>resultFolderName</code><br/>
<code>string</code>
</td>
<td>
   <p>ResultFolderName specifies the name of the folder that stores the benchmark result. A default name will be assigned if not specified.</p>
</td>
</tr>
<tr><td><code>podOverride</code><br/>
<a href="#ome-io-v1beta1-PodOverride"><code>PodOverride</code></a>
</td>
<td>
   <p>Pod defines the pod configuration for the benchmark job. This is optional, if not provided, default values will be used.</p>
</td>
</tr>
</tbody>
</table>

## `BenchmarkJobStatus`     {#ome-io-v1beta1-BenchmarkJobStatus}


**Appears in:**

- [BenchmarkJob](#ome-io-v1beta1-BenchmarkJob)


<p>BenchmarkJobStatus reflects the state and results of the benchmark job. It
will be set and updated by the controller.</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>state</code> <B>[Required]</B><br/>
<code>string</code>
</td>
<td>
   <p>State represents the current state of the benchmark job: &quot;Pending&quot;, &quot;Running&quot;, &quot;Completed&quot;, &quot;Failed&quot;.</p>
</td>
</tr>
<tr><td><code>startTime</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#time-v1-meta"><code>k8s.io/apimachinery/pkg/apis/meta/v1.Time</code></a>
</td>
<td>
   <p>StartTime is the timestamp for when the benchmark job started.</p>
</td>
</tr>
<tr><td><code>completionTime</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#time-v1-meta"><code>k8s.io/apimachinery/pkg/apis/meta/v1.Time</code></a>
</td>
<td>
   <p>CompletionTime is the timestamp for when the benchmark job completed, either successfully or unsuccessfully.</p>
</td>
</tr>
<tr><td><code>lastReconcileTime</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#time-v1-meta"><code>k8s.io/apimachinery/pkg/apis/meta/v1.Time</code></a>
</td>
<td>
   <p>LastReconcileTime is the timestamp for the last time the job was reconciled by the controller.</p>
</td>
</tr>
<tr><td><code>failureMessage</code><br/>
<code>string</code>
</td>
<td>
   <p>FailureMessage contains any error messages if the benchmark job failed.</p>
</td>
</tr>
<tr><td><code>details</code><br/>
<code>string</code>
</td>
<td>
   <p>Details provide additional information or metadata about the benchmark job.</p>
</td>
</tr>
</tbody>
</table>

## `ComponentExtensionSpec`     {#ome-io-v1beta1-ComponentExtensionSpec}


**Appears in:**

- [DecoderSpec](#ome-io-v1beta1-DecoderSpec)

- [EngineSpec](#ome-io-v1beta1-EngineSpec)

- [RouterSpec](#ome-io-v1beta1-RouterSpec)


<p>ComponentExtensionSpec defines the deployment configuration for a given InferenceService component</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>minReplicas</code><br/>
<code>int</code>
</td>
<td>
   <p>Minimum number of replicas, defaults to 1.</p>
</td>
</tr>
<tr><td><code>maxReplicas</code><br/>
<code>int</code>
</td>
<td>
   <p>Maximum number of replicas for autoscaling.</p>
</td>
</tr>
<tr><td><code>scaleTarget</code><br/>
<code>int</code>
</td>
<td>
   <p>ScaleTarget specifies the integer target value of the metric type the Autoscaler watches for.</p>
</td>
</tr>
<tr><td><code>scaleMetric</code><br/>
<a href="#ome-io-v1beta1-ScaleMetric"><code>ScaleMetric</code></a>
</td>
<td>
   <p>ScaleMetric defines the scaling metric type watched by autoscaler.
Possible values are cpu, memory for HPA; KEDA additionally supports custom metrics.</p>
</td>
</tr>
<tr><td><code>timeoutSeconds</code><br/>
<code>int64</code>
</td>
<td>
   <p>TimeoutSeconds specifies the number of seconds to wait before timing out a request to the component.</p>
</td>
</tr>
<tr><td><code>labels</code><br/>
<code>map[string]string</code>
</td>
<td>
   <p>Labels that will be added to the component pod.
More info: http://kubernetes.io/docs/user-guide/labels</p>
</td>
</tr>
<tr><td><code>annotations</code><br/>
<code>map[string]string</code>
</td>
<td>
   <p>Annotations that will be added to the component pod.
More info: http://kubernetes.io/docs/user-guide/annotations</p>
</td>
</tr>
<tr><td><code>minAvailable</code><br/>
<code>k8s.io/apimachinery/pkg/util/intstr.IntOrString</code>
</td>
<td>
   <p>MinAvailable specifies how many component pods must still be available after the eviction</p>
</td>
</tr>
<tr><td><code>maxUnavailable</code><br/>
<code>k8s.io/apimachinery/pkg/util/intstr.IntOrString</code>
</td>
<td>
   <p>MaxUnavailable specifies how many component pods can be unavailable</p>
</td>
</tr>
<tr><td><code>deploymentStrategy</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#deploymentstrategy-v1-apps"><code>k8s.io/api/apps/v1.DeploymentStrategy</code></a>
</td>
<td>
   <p>The deployment strategy to use to replace existing pods with new ones. Only applicable for raw deployment mode.</p>
</td>
</tr>
<tr><td><code>kedaConfig</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-KedaConfig"><code>KedaConfig</code></a>
</td>
<td>
   <span class="text-muted">No description provided.</span></td>
</tr>
</tbody>
</table>

## `ComponentStatusSpec`     {#ome-io-v1beta1-ComponentStatusSpec}


**Appears in:**

- [InferenceServiceStatus](#ome-io-v1beta1-InferenceServiceStatus)


<p>ComponentStatusSpec describes the state of the component</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>latestReadyRevision</code><br/>
<code>string</code>
</td>
<td>
   <p>Latest revision name that is in ready state</p>
</td>
</tr>
<tr><td><code>latestCreatedRevision</code><br/>
<code>string</code>
</td>
<td>
   <p>Latest revision name that is created</p>
</td>
</tr>
<tr><td><code>url</code><br/>
<a href="https://pkg.go.dev/knative.dev/pkg/apis#URL"><code>knative.dev/pkg/apis.URL</code></a>
</td>
<td>
   <p>URL holds the primary url that will distribute traffic over the provided traffic targets.
This will be one the REST or gRPC endpoints that are available.
It generally has the form http[s]://{route-name}.{route-namespace}.{cluster-level-suffix}</p>
</td>
</tr>
<tr><td><code>restURL</code><br/>
<a href="https://pkg.go.dev/knative.dev/pkg/apis#URL"><code>knative.dev/pkg/apis.URL</code></a>
</td>
<td>
   <p>REST endpoint of the component if available.</p>
</td>
</tr>
<tr><td><code>address</code><br/>
<a href="https://pkg.go.dev/knative.dev/pkg/apis/duck/v1#Addressable"><code>knative.dev/pkg/apis/duck/v1.Addressable</code></a>
</td>
<td>
   <p>Addressable endpoint for the InferenceService</p>
</td>
</tr>
<tr><td><code>selectedAccelerator</code><br/>
<a href="#ome-io-v1beta1-AcceleratorSelection"><code>AcceleratorSelection</code></a>
</td>
<td>
   <p>SelectedAccelerator shows which AcceleratorClass was selected</p>
</td>
</tr>
</tbody>
</table>

## `DecoderSpec`     {#ome-io-v1beta1-DecoderSpec}


**Appears in:**

- [InferenceServiceSpec](#ome-io-v1beta1-InferenceServiceSpec)

- [ServingRuntimeSpec](#ome-io-v1beta1-ServingRuntimeSpec)


<p>DecoderSpec defines the configuration for the Decoder component (token generation in PD-disaggregated deployment)
Used specifically for prefill-decode disaggregated deployments to handle the token generation phase.
Similar to EngineSpec in structure, it allows for detailed pod and container configuration,
but is specifically used for the decode phase when separating prefill and decode processes.</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>PodSpec</code><br/>
<a href="#ome-io-v1beta1-PodSpec"><code>PodSpec</code></a>
</td>
<td>(Members of <code>PodSpec</code> are embedded into this type.)
   <p>This spec provides a full PodSpec for the decoder component
Allows complete customization of the Kubernetes Pod configuration including
containers, volumes, security contexts, affinity rules, and other pod settings.</p>
</td>
</tr>
<tr><td><code>ComponentExtensionSpec</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-ComponentExtensionSpec"><code>ComponentExtensionSpec</code></a>
</td>
<td>(Members of <code>ComponentExtensionSpec</code> are embedded into this type.)
   <p>ComponentExtensionSpec defines deployment configuration like min/max replicas, scaling metrics, etc.
Controls scaling behavior and resource allocation for the decoder component.</p>
</td>
</tr>
<tr><td><code>runner</code><br/>
<a href="#ome-io-v1beta1-RunnerSpec"><code>RunnerSpec</code></a>
</td>
<td>
   <p>Runner container override for customizing the main container
This is essentially a container spec that can override the default container
Defines the main decoder container configuration, including image,
resource requests/limits, environment variables, and command.</p>
</td>
</tr>
<tr><td><code>leader</code><br/>
<a href="#ome-io-v1beta1-LeaderSpec"><code>LeaderSpec</code></a>
</td>
<td>
   <p>Leader node configuration (only used for MultiNode deployment)
Defines the pod and container spec for the leader node that coordinates
distributed token generation in multi-node deployments.</p>
</td>
</tr>
<tr><td><code>worker</code><br/>
<a href="#ome-io-v1beta1-WorkerSpec"><code>WorkerSpec</code></a>
</td>
<td>
   <p>Worker nodes configuration (only used for MultiNode deployment)
Defines the pod and container spec for worker nodes that perform
distributed token generation tasks as directed by the leader.</p>
</td>
</tr>
<tr><td><code>acceleratorOverride</code><br/>
<a href="#ome-io-v1beta1-AcceleratorSelector"><code>AcceleratorSelector</code></a>
</td>
<td>
   <p>AcceleratorOverride allows overriding the global accelerator selection for this component</p>
</td>
</tr>
</tbody>
</table>

## `DiffusionComponentSpec`     {#ome-io-v1beta1-DiffusionComponentSpec}


**Appears in:**

- [DiffusionPipelineSpec](#ome-io-v1beta1-DiffusionPipelineSpec)


<p>DiffusionComponentSpec captures an individual component used by a diffusion pipeline.
The fields map directly to entries in a diffusers model_index.json file.</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>library</code><br/>
<code>string</code>
</td>
<td>
   <p>Library providing the component implementation, e.g., &quot;diffusers&quot; or &quot;transformers&quot;.</p>
</td>
</tr>
<tr><td><code>type</code><br/>
<code>string</code>
</td>
<td>
   <p>Type is the fully qualified class name for the component, e.g., &quot;FlowMatchEulerDiscreteScheduler&quot;.</p>
</td>
</tr>
</tbody>
</table>

## `DiffusionPipelineSpec`     {#ome-io-v1beta1-DiffusionPipelineSpec}


**Appears in:**

- [BaseModelSpec](#ome-io-v1beta1-BaseModelSpec)

- [SupportedModelFormat](#ome-io-v1beta1-SupportedModelFormat)


<p>DiffusionPipelineSpec describes a diffusers pipeline so that runtimes can validate compatibility.
When set, these fields should mirror the content of the model's model_index.json file.</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>className</code><br/>
<code>string</code>
</td>
<td>
   <p>ClassName is the pipeline implementation, e.g., &quot;StableDiffusionXLPipeline&quot; or &quot;QwenImagePipeline&quot;.</p>
</td>
</tr>
<tr><td><code>scheduler</code><br/>
<a href="#ome-io-v1beta1-DiffusionComponentSpec"><code>DiffusionComponentSpec</code></a>
</td>
<td>
   <p>Scheduler component used by the pipeline.</p>
</td>
</tr>
<tr><td><code>textEncoder</code><br/>
<a href="#ome-io-v1beta1-DiffusionComponentSpec"><code>DiffusionComponentSpec</code></a>
</td>
<td>
   <p>TextEncoder component used by the pipeline.</p>
</td>
</tr>
<tr><td><code>tokenizer</code><br/>
<a href="#ome-io-v1beta1-DiffusionComponentSpec"><code>DiffusionComponentSpec</code></a>
</td>
<td>
   <p>Tokenizer component used by the pipeline.</p>
</td>
</tr>
<tr><td><code>transformer</code><br/>
<a href="#ome-io-v1beta1-DiffusionComponentSpec"><code>DiffusionComponentSpec</code></a>
</td>
<td>
   <p>Transformer (UNet/DiT) component used by the pipeline.</p>
</td>
</tr>
<tr><td><code>vae</code><br/>
<a href="#ome-io-v1beta1-DiffusionComponentSpec"><code>DiffusionComponentSpec</code></a>
</td>
<td>
   <p>VAE component used by the pipeline.</p>
</td>
</tr>
<tr><td><code>additionalComponents</code><br/>
<a href="#ome-io-v1beta1-DiffusionComponentSpec"><code>map[string]DiffusionComponentSpec</code></a>
</td>
<td>
   <p>AdditionalComponents captures any other pipeline parts keyed by their model_index.json entry.</p>
</td>
</tr>
</tbody>
</table>

## `DownloadPolicy`     {#ome-io-v1beta1-DownloadPolicy}

(Alias of `string`)

**Appears in:**

- [StorageSpec](#ome-io-v1beta1-StorageSpec)





## `Endpoint`     {#ome-io-v1beta1-Endpoint}


**Appears in:**

- [EndpointSpec](#ome-io-v1beta1-EndpointSpec)


<p>Endpoint defines a direct URL-based inference service with additional API configuration.</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>url</code> <B>[Required]</B><br/>
<code>string</code>
</td>
<td>
   <p>URL represents the endpoint URL for the inference service.</p>
</td>
</tr>
<tr><td><code>apiFormat</code> <B>[Required]</B><br/>
<code>string</code>
</td>
<td>
   <p>APIFormat specifies the type of API, such as &quot;openai&quot; or &quot;oci-cohere&quot;.</p>
</td>
</tr>
<tr><td><code>modelName</code> <B>[Required]</B><br/>
<code>string</code>
</td>
<td>
   <p>ModelName specifies the name of the model being served at the endpoint.
Useful for endpoints that require model-specific configuration. For instance,
for openai API, this is a required field in the payload</p>
</td>
</tr>
</tbody>
</table>

## `EndpointSpec`     {#ome-io-v1beta1-EndpointSpec}


**Appears in:**

- [BenchmarkJobSpec](#ome-io-v1beta1-BenchmarkJobSpec)


<p>EndpointSpec defines a reference to an inference service.
It supports either a Kubernetes-style reference (InferenceService) or an Endpoint struct for a direct URL.
Cross-namespace references are supported for InferenceService but require appropriate RBAC permissions to access resources in the target namespace.</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>inferenceService</code><br/>
<a href="#ome-io-v1beta1-InferenceServiceReference"><code>InferenceServiceReference</code></a>
</td>
<td>
   <p>InferenceService holds a Kubernetes reference to an internal inference service.</p>
</td>
</tr>
<tr><td><code>endpoint</code><br/>
<a href="#ome-io-v1beta1-Endpoint"><code>Endpoint</code></a>
</td>
<td>
   <p>Endpoint holds the details of a direct endpoint for an external inference service, including URL and API details.</p>
</td>
</tr>
</tbody>
</table>

## `EngineSpec`     {#ome-io-v1beta1-EngineSpec}


**Appears in:**

- [InferenceServiceSpec](#ome-io-v1beta1-InferenceServiceSpec)

- [ServingRuntimeSpec](#ome-io-v1beta1-ServingRuntimeSpec)


<p>EngineSpec defines the configuration for the Engine component (can be used for both single-node and multi-node deployments)
Provides a comprehensive specification for deploying model serving containers and pods.
It allows for complete Kubernetes pod configuration including main containers,
init containers, sidecars, volumes, and other pod-level settings.
For distributed deployments, it supports leader-worker architecture configuration.</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>PodSpec</code><br/>
<a href="#ome-io-v1beta1-PodSpec"><code>PodSpec</code></a>
</td>
<td>(Members of <code>PodSpec</code> are embedded into this type.)
   <p>This spec provides a full PodSpec for the engine component
Allows complete customization of the Kubernetes Pod configuration including
containers, volumes, security contexts, affinity rules, and other pod settings.</p>
</td>
</tr>
<tr><td><code>ComponentExtensionSpec</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-ComponentExtensionSpec"><code>ComponentExtensionSpec</code></a>
</td>
<td>(Members of <code>ComponentExtensionSpec</code> are embedded into this type.)
   <p>ComponentExtensionSpec defines deployment configuration like min/max replicas, scaling metrics, etc.
Controls scaling behavior and resource allocation for the engine component.</p>
</td>
</tr>
<tr><td><code>runner</code><br/>
<a href="#ome-io-v1beta1-RunnerSpec"><code>RunnerSpec</code></a>
</td>
<td>
   <p>Runner container override for customizing the engine container
This is essentially a container spec that can override the default container
Defines the main model runner container configuration, including image,
resource requests/limits, environment variables, and command.</p>
</td>
</tr>
<tr><td><code>leader</code><br/>
<a href="#ome-io-v1beta1-LeaderSpec"><code>LeaderSpec</code></a>
</td>
<td>
   <p>Leader node configuration (only used for MultiNode deployment)
Defines the pod and container spec for the leader node that coordinates
distributed inference in multi-node deployments.</p>
</td>
</tr>
<tr><td><code>worker</code><br/>
<a href="#ome-io-v1beta1-WorkerSpec"><code>WorkerSpec</code></a>
</td>
<td>
   <p>Worker nodes configuration (only used for MultiNode deployment)
Defines the pod and container spec for worker nodes that perform
distributed processing tasks as directed by the leader.</p>
</td>
</tr>
<tr><td><code>acceleratorOverride</code><br/>
<a href="#ome-io-v1beta1-AcceleratorSelector"><code>AcceleratorSelector</code></a>
</td>
<td>
   <p>AcceleratorOverride allows overriding the global accelerator selection for this component</p>
</td>
</tr>
</tbody>
</table>

## `FailureInfo`     {#ome-io-v1beta1-FailureInfo}


**Appears in:**

- [ModelStatus](#ome-io-v1beta1-ModelStatus)



<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>location</code><br/>
<code>string</code>
</td>
<td>
   <p>Name of component to which the failure relates (usually Pod name)</p>
</td>
</tr>
<tr><td><code>reason</code><br/>
<a href="#ome-io-v1beta1-FailureReason"><code>FailureReason</code></a>
</td>
<td>
   <p>High level class of failure</p>
</td>
</tr>
<tr><td><code>message</code><br/>
<code>string</code>
</td>
<td>
   <p>Detailed error message</p>
</td>
</tr>
<tr><td><code>modelRevisionName</code><br/>
<code>string</code>
</td>
<td>
   <p>Internal Revision/ID of model, tied to specific Spec contents</p>
</td>
</tr>
<tr><td><code>time</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#time-v1-meta"><code>k8s.io/apimachinery/pkg/apis/meta/v1.Time</code></a>
</td>
<td>
   <p>Time failure occurred or was discovered</p>
</td>
</tr>
<tr><td><code>exitCode</code><br/>
<code>int32</code>
</td>
<td>
   <p>Exit status from the last termination of the container</p>
</td>
</tr>
</tbody>
</table>

## `FailureReason`     {#ome-io-v1beta1-FailureReason}

(Alias of `string`)

**Appears in:**

- [FailureInfo](#ome-io-v1beta1-FailureInfo)


<p>FailureReason enum</p>




## `FineTunedWeightSpec`     {#ome-io-v1beta1-FineTunedWeightSpec}


**Appears in:**

- [FineTunedWeight](#ome-io-v1beta1-FineTunedWeight)


<p>FineTunedWeightSpec defines the desired state of FineTunedWeight</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>baseModelRef</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-ObjectReference"><code>ObjectReference</code></a>
</td>
<td>
   <p>Reference to the base model that this weight is fine-tuned from</p>
</td>
</tr>
<tr><td><code>modelType</code> <B>[Required]</B><br/>
<code>string</code>
</td>
<td>
   <p>ModelType of the fine-tuned weight, e.g., &quot;Distillation&quot;, &quot;Adapter&quot;, &quot;Tfew&quot;</p>
</td>
</tr>
<tr><td><code>hyperParameters</code> <B>[Required]</B><br/>
<a href="https://pkg.go.dev/k8s.io/apimachinery/pkg/runtime#RawExtension"><code>k8s.io/apimachinery/pkg/runtime.RawExtension</code></a>
</td>
<td>
   <p>HyperParameters used for fine-tuning, stored as generic JSON for flexibility</p>
</td>
</tr>
<tr><td><code>ModelExtensionSpec</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-ModelExtensionSpec"><code>ModelExtensionSpec</code></a>
</td>
<td>(Members of <code>ModelExtensionSpec</code> are embedded into this type.)
   <p>ModelExtension is the common extension of the model</p>
</td>
</tr>
<tr><td><code>configuration</code><br/>
<a href="https://pkg.go.dev/k8s.io/apimachinery/pkg/runtime#RawExtension"><code>k8s.io/apimachinery/pkg/runtime.RawExtension</code></a>
</td>
<td>
   <p>Configuration of the fine-tuned weight, stored as generic JSON for flexibility</p>
</td>
</tr>
<tr><td><code>storage</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-StorageSpec"><code>StorageSpec</code></a>
</td>
<td>
   <p>Storage configuration for the fine-tuned weight</p>
</td>
</tr>
<tr><td><code>trainingJobRef</code><br/>
<a href="#ome-io-v1beta1-ObjectReference"><code>ObjectReference</code></a>
</td>
<td>
   <p>TrainingJobID is the ID of the training job that produced this weight</p>
</td>
</tr>
</tbody>
</table>

## `HuggingFaceSecretReference`     {#ome-io-v1beta1-HuggingFaceSecretReference}


**Appears in:**

- [BenchmarkJobSpec](#ome-io-v1beta1-BenchmarkJobSpec)


<p>HuggingFaceSecretReference defines a reference to a Kubernetes Secret containing the Hugging Face API key.
This secret must reside in the same namespace as the BenchmarkJob.
Cross-namespace references are not allowed for security and simplicity.</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>name</code> <B>[Required]</B><br/>
<code>string</code>
</td>
<td>
   <p>Name of the secret containing the Hugging Face API key.
The secret must reside in the same namespace as the BenchmarkJob.</p>
</td>
</tr>
</tbody>
</table>

## `InferenceServiceReference`     {#ome-io-v1beta1-InferenceServiceReference}


**Appears in:**

- [EndpointSpec](#ome-io-v1beta1-EndpointSpec)


<p>InferenceServiceReference defines the reference to a Kubernetes inference service.</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>name</code> <B>[Required]</B><br/>
<code>string</code>
</td>
<td>
   <p>Name specifies the name of the inference service to benchmark.</p>
</td>
</tr>
<tr><td><code>namespace</code> <B>[Required]</B><br/>
<code>string</code>
</td>
<td>
   <p>Namespace specifies the Kubernetes namespace where the inference service is deployed.
Cross-namespace references are allowed but require appropriate RBAC permissions.</p>
</td>
</tr>
</tbody>
</table>

## `InferenceServiceSpec`     {#ome-io-v1beta1-InferenceServiceSpec}


**Appears in:**

- [InferenceService](#ome-io-v1beta1-InferenceService)


<p>InferenceServiceSpec is the top level type for this resource</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>engine</code><br/>
<a href="#ome-io-v1beta1-EngineSpec"><code>EngineSpec</code></a>
</td>
<td>
   <p>Engine defines the serving engine spec
This provides detailed container and pod specifications for model serving.
It allows defining the model runner (container spec), as well as complete pod specifications
including init containers, sidecar containers, and other pod-level configurations.
Engine can also be configured for multi-node deployments using leader and worker specifications.</p>
</td>
</tr>
<tr><td><code>decoder</code><br/>
<a href="#ome-io-v1beta1-DecoderSpec"><code>DecoderSpec</code></a>
</td>
<td>
   <p>Decoder defines the decoder spec
This is specifically used for PD (Prefill-Decode) disaggregated serving deployments.
Similar to Engine in structure, it allows for container and pod specifications,
but is only utilized when implementing the disaggregated serving pattern
to separate the prefill and decode phases of inference.</p>
</td>
</tr>
<tr><td><code>model</code><br/>
<a href="#ome-io-v1beta1-ModelRef"><code>ModelRef</code></a>
</td>
<td>
   <p>Model defines the model to be used for inference, referencing either a BaseModel or a custom model.
This allows models to be managed independently of the serving configuration.</p>
</td>
</tr>
<tr><td><code>runtime</code><br/>
<a href="#ome-io-v1beta1-ServingRuntimeRef"><code>ServingRuntimeRef</code></a>
</td>
<td>
   <p>Runtime defines the serving runtime environment that will be used to execute the model.
It is an inference service spec template that determines how the service should be deployed.
Runtime is optional - if not defined, the operator will automatically select the best runtime
based on the model's size, architecture, format, quantization, and framework.</p>
</td>
</tr>
<tr><td><code>router</code><br/>
<a href="#ome-io-v1beta1-RouterSpec"><code>RouterSpec</code></a>
</td>
<td>
   <p>Router defines the router spec</p>
</td>
</tr>
<tr><td><code>kedaConfig</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-KedaConfig"><code>KedaConfig</code></a>
</td>
<td>
   <p>KedaConfig defines the autoscaling configuration for KEDA
Provides settings for event-driven autoscaling using KEDA (Kubernetes Event-driven Autoscaling),
allowing the service to scale based on custom metrics or event sources.</p>
</td>
</tr>
<tr><td><code>acceleratorSelector</code><br/>
<a href="#ome-io-v1beta1-AcceleratorSelector"><code>AcceleratorSelector</code></a>
</td>
<td>
   <p>AcceleratorSelector specifies accelerator selection preferences</p>
</td>
</tr>
</tbody>
</table>

## `InferenceServiceStatus`     {#ome-io-v1beta1-InferenceServiceStatus}


**Appears in:**

- [InferenceService](#ome-io-v1beta1-InferenceService)


<p>InferenceServiceStatus defines the observed state of InferenceService</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>Status</code> <B>[Required]</B><br/>
<a href="https://pkg.go.dev/knative.dev/pkg/apis/duck/v1#Status"><code>knative.dev/pkg/apis/duck/v1.Status</code></a>
</td>
<td>(Members of <code>Status</code> are embedded into this type.)
   <p>Conditions for the InferenceService <!-- raw HTML omitted --></p>
<ul>
<li>EngineReady: engine readiness condition; <!-- raw HTML omitted --></li>
<li>DecoderReady: decoder readiness condition; <!-- raw HTML omitted --></li>
<li>RouterReady: router readiness condition; <!-- raw HTML omitted --></li>
<li>IngressReady: ingress readiness condition; <!-- raw HTML omitted --></li>
<li>Ready: aggregated condition; <!-- raw HTML omitted --></li>
</ul>
</td>
</tr>
<tr><td><code>address</code><br/>
<a href="https://pkg.go.dev/knative.dev/pkg/apis/duck/v1#Addressable"><code>knative.dev/pkg/apis/duck/v1.Addressable</code></a>
</td>
<td>
   <p>Addressable endpoint for the InferenceService</p>
</td>
</tr>
<tr><td><code>url</code><br/>
<a href="https://pkg.go.dev/knative.dev/pkg/apis#URL"><code>knative.dev/pkg/apis.URL</code></a>
</td>
<td>
   <p>URL holds the url that will distribute traffic over the provided traffic targets.
It generally has the form http[s]://{route-name}.{route-namespace}.{cluster-level-suffix}</p>
</td>
</tr>
<tr><td><code>components</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-ComponentStatusSpec"><code>map[ComponentType]ComponentStatusSpec</code></a>
</td>
<td>
   <p>Statuses for the components of the InferenceService</p>
</td>
</tr>
<tr><td><code>modelStatus</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-ModelStatus"><code>ModelStatus</code></a>
</td>
<td>
   <p>Model related statuses</p>
</td>
</tr>
<tr><td><code>pinnedRevisionName</code><br/>
<code>string</code>
</td>
<td>
   <p>PinnedRevisionName is the ControllerRevision (OME ns) driving
pod specs. Empty when autoSync is true.</p>
</td>
</tr>
<tr><td><code>lastRuntimeSyncToken</code><br/>
<code>string</code>
</td>
<td>
   <p>LastRuntimeSyncToken is the ome.io/runtime-sync annotation
value at last pin advance; the controller advances the pin
when the live annotation differs from this.</p>
</td>
</tr>
</tbody>
</table>

## `KedaConfig`     {#ome-io-v1beta1-KedaConfig}


**Appears in:**

- [ComponentExtensionSpec](#ome-io-v1beta1-ComponentExtensionSpec)

- [InferenceServiceSpec](#ome-io-v1beta1-InferenceServiceSpec)


<p>KedaConfig stores the configuration settings for KEDA autoscaling within the InferenceService.
It includes fields like the Prometheus server address, custom query, scaling threshold, and operator.</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>enableKeda</code> <B>[Required]</B><br/>
<code>bool</code>
</td>
<td>
   <p>EnableKeda determines whether KEDA autoscaling is enabled for the InferenceService.</p>
<ul>
<li>true: KEDA will manage the autoscaling based on the provided configuration.</li>
<li>false: KEDA will not be used, and autoscaling will rely on other mechanisms (e.g., HPA).</li>
</ul>
</td>
</tr>
<tr><td><code>promServerAddress</code> <B>[Required]</B><br/>
<code>string</code>
</td>
<td>
   <p>PromServerAddress specifies the address of the Prometheus server that KEDA will query
to retrieve metrics for autoscaling decisions. This should be a fully qualified URL,
including the protocol and port number.</p>
<p>Example:
http://prometheus-operated.monitoring.svc.cluster.local:9090</p>
</td>
</tr>
<tr><td><code>customPromQuery</code> <B>[Required]</B><br/>
<code>string</code>
</td>
<td>
   <p>CustomPromQuery defines a custom Prometheus query that KEDA will execute to evaluate
the desired metric for scaling. This query should return a single numerical value that
represents the metric to be monitored.</p>
<p>Example:
avg_over_time(http_requests_total{service=&quot;llama&quot;}[5m])</p>
</td>
</tr>
<tr><td><code>scalingThreshold</code> <B>[Required]</B><br/>
<code>string</code>
</td>
<td>
   <p>ScalingThreshold sets the numerical threshold against which the result of the Prometheus
query will be compared. Depending on the ScalingOperator, this threshold determines
when to scale the number of replicas up or down.</p>
<p>Example:
&quot;10&quot; - The Autoscaler will compare the metric value to 10.</p>
</td>
</tr>
<tr><td><code>scalingOperator</code> <B>[Required]</B><br/>
<code>string</code>
</td>
<td>
   <p>ScalingOperator specifies the comparison operator used by KEDA to decide whether to scale
the Deployment. Common operators include:</p>
<ul>
<li>&quot;GreaterThanOrEqual&quot;: Scale up when the metric is &gt;= ScalingThreshold.</li>
<li>&quot;LessThanOrEqual&quot;: Scale down when the metric is &lt;= ScalingThreshold.</li>
</ul>
<p>This operator defines the condition under which scaling actions are triggered based on
the evaluated metric.</p>
<p>Example:
&quot;GreaterThanOrEqual&quot;</p>
</td>
</tr>
<tr><td><code>authenticationRef</code><br/>
<a href="#ome-io-v1beta1-ScalerAuthenticationRef"><code>ScalerAuthenticationRef</code></a>
</td>
<td>
   <p>AuthenticationRef references a TriggerAuthentication or ClusterTriggerAuthentication
resource that contains the authentication configuration for the Prometheus server.
This is required when the Prometheus server requires authentication (e.g., Grafana Cloud).</p>
<p>Example:
authenticationRef:
name: grafana-cloud-auth
kind: TriggerAuthentication</p>
</td>
</tr>
<tr><td><code>authModes</code><br/>
<code>string</code>
</td>
<td>
   <p>AuthModes specifies the authentication mode(s) for the Prometheus scaler.
Common values include: &quot;basic&quot;, &quot;tls&quot;, &quot;bearer&quot;, &quot;custom&quot;.
Multiple modes can be specified comma-separated (e.g., &quot;tls,basic&quot;).</p>
<p>Example:
&quot;basic&quot; - Use basic authentication with username/password from TriggerAuthentication</p>
</td>
</tr>
</tbody>
</table>

## `LeaderSpec`     {#ome-io-v1beta1-LeaderSpec}


**Appears in:**

- [DecoderSpec](#ome-io-v1beta1-DecoderSpec)

- [EngineSpec](#ome-io-v1beta1-EngineSpec)


<p>LeaderSpec defines the configuration for a leader node in a multi-node component
The leader node coordinates the activities of worker nodes in distributed inference or
token generation setups, handling task distribution and result aggregation.</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>PodSpec</code><br/>
<a href="#ome-io-v1beta1-PodSpec"><code>PodSpec</code></a>
</td>
<td>(Members of <code>PodSpec</code> are embedded into this type.)
   <p>Pod specification for the leader node
This overrides the main PodSpec when specified
Allows customization of the Kubernetes Pod configuration specifically for the leader node.</p>
</td>
</tr>
<tr><td><code>runner</code><br/>
<a href="#ome-io-v1beta1-RunnerSpec"><code>RunnerSpec</code></a>
</td>
<td>
   <p>Runner container override for customizing the main container
This is essentially a container spec that can override the default container
Provides fine-grained control over the container that executes the leader node's coordination logic.</p>
</td>
</tr>
</tbody>
</table>

## `LifeCycleState`     {#ome-io-v1beta1-LifeCycleState}

(Alias of `string`)

**Appears in:**

- [ModelStatusSpec](#ome-io-v1beta1-ModelStatusSpec)


<p>LifeCycleState enum</p>




## `ModelCopies`     {#ome-io-v1beta1-ModelCopies}


**Appears in:**

- [ModelStatus](#ome-io-v1beta1-ModelStatus)



<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>failedCopies</code> <B>[Required]</B><br/>
<code>int</code>
</td>
<td>
   <p>How many copies of this model failed to load recently</p>
</td>
</tr>
<tr><td><code>totalCopies</code><br/>
<code>int</code>
</td>
<td>
   <p>Total number copies of this model that are currently loaded</p>
</td>
</tr>
</tbody>
</table>

## `ModelExtensionSpec`     {#ome-io-v1beta1-ModelExtensionSpec}


**Appears in:**

- [BaseModelSpec](#ome-io-v1beta1-BaseModelSpec)

- [FineTunedWeightSpec](#ome-io-v1beta1-FineTunedWeightSpec)



<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>displayName</code><br/>
<code>string</code>
</td>
<td>
   <p>DisplayName is the user-friendly name of the model</p>
</td>
</tr>
<tr><td><code>version</code><br/>
<code>string</code>
</td>
<td>
   <span class="text-muted">No description provided.</span></td>
</tr>
<tr><td><code>disabled</code><br/>
<code>bool</code>
</td>
<td>
   <p>Whether the model is enabled or not</p>
</td>
</tr>
<tr><td><code>vendor</code><br/>
<code>string</code>
</td>
<td>
   <p>Vendor of the model, e.g., &quot;NVIDIA&quot;, &quot;Meta&quot;, &quot;HuggingFace&quot;</p>
</td>
</tr>
<tr><td><code>compartmentID</code><br/>
<code>string</code>
</td>
<td>
   <p>CompartmentID is the compartment ID of the model</p>
</td>
</tr>
</tbody>
</table>

## `ModelFormat`     {#ome-io-v1beta1-ModelFormat}


**Appears in:**

- [BaseModelSpec](#ome-io-v1beta1-BaseModelSpec)

- [SupportedModelFormat](#ome-io-v1beta1-SupportedModelFormat)



<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>name</code> <B>[Required]</B><br/>
<code>string</code>
</td>
<td>
   <p>Name of the format in which the model is stored, e.g., &quot;ONNX&quot;, &quot;TensorFlow SavedModel&quot;, &quot;PyTorch&quot;, &quot;SafeTensors&quot;</p>
</td>
</tr>
<tr><td><code>version</code><br/>
<code>string</code>
</td>
<td>
   <p>Version of the model format.
Used in validating that a runtime supports a model.
It Can be &quot;major&quot;, &quot;major.minor&quot; or &quot;major.minor.patch&quot;.</p>
</td>
</tr>
<tr><td><code>operator</code><br/>
<a href="#ome-io-v1beta1-RuntimeSelectorOperator"><code>RuntimeSelectorOperator</code></a>
</td>
<td>
   <p>Operator for the selector with supported values: &quot;Equal&quot;, &quot;GreaterThan&quot;
This is used to select the serving runtime based on the modelFormat version</p>
</td>
</tr>
<tr><td><code>weight</code><br/>
<code>int64</code>
</td>
<td>
   <p>Weight of the model format in the runtime selector, used to prioritize modelFormat</p>
</td>
</tr>
</tbody>
</table>

## `ModelFrameworkSpec`     {#ome-io-v1beta1-ModelFrameworkSpec}


**Appears in:**

- [BaseModelSpec](#ome-io-v1beta1-BaseModelSpec)

- [SupportedModelFormat](#ome-io-v1beta1-SupportedModelFormat)



<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>name</code> <B>[Required]</B><br/>
<code>string</code>
</td>
<td>
   <p>Name of the library in which the model is stored, e.g., &quot;ONNXRuntime&quot;, &quot;TensorFlow&quot;, &quot;PyTorch&quot;, &quot;Transformer&quot;, &quot;TensorRTLLM&quot;</p>
</td>
</tr>
<tr><td><code>version</code><br/>
<code>string</code>
</td>
<td>
   <p>Version of the library.
Used in validating that a runtime supports a model.
It Can be &quot;major&quot;, &quot;major.minor&quot; or &quot;major.minor.patch&quot;.</p>
</td>
</tr>
<tr><td><code>operator</code><br/>
<a href="#ome-io-v1beta1-RuntimeSelectorOperator"><code>RuntimeSelectorOperator</code></a>
</td>
<td>
   <p>Operator for the selector with supported values: &quot;Equal&quot;, &quot;GreaterThan&quot;
This is used to select the serving runtime based on the modelFramework version</p>
</td>
</tr>
<tr><td><code>weight</code><br/>
<code>int64</code>
</td>
<td>
   <p>Weight of the framework in the runtime selector, used to prioritize modelFramework</p>
</td>
</tr>
</tbody>
</table>

## `ModelQuantization`     {#ome-io-v1beta1-ModelQuantization}

(Alias of `string`)

**Appears in:**

- [BaseModelSpec](#ome-io-v1beta1-BaseModelSpec)

- [SupportedModelFormat](#ome-io-v1beta1-SupportedModelFormat)





## `ModelRef`     {#ome-io-v1beta1-ModelRef}


**Appears in:**

- [InferenceServiceSpec](#ome-io-v1beta1-InferenceServiceSpec)



<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>name</code> <B>[Required]</B><br/>
<code>string</code>
</td>
<td>
   <p>Name of the model being referenced
Identifies the specific model to be used for inference.</p>
</td>
</tr>
<tr><td><code>kind</code> <B>[Required]</B><br/>
<code>string</code>
</td>
<td>
   <p>Kind of the model being referenced
Defaults to ClusterBaseModel
Specifies the Kubernetes resource kind of the referenced model.</p>
</td>
</tr>
<tr><td><code>apiGroup</code> <B>[Required]</B><br/>
<code>string</code>
</td>
<td>
   <p>APIGroup of the resource being referenced
Defaults to <code>ome.io</code>
Specifies the Kubernetes API group of the referenced model.</p>
</td>
</tr>
<tr><td><code>fineTunedWeights</code><br/>
<code>[]string</code>
</td>
<td>
   <p>Optional FineTunedWeights references
References to fine-tuned weights that should be applied to the base model.</p>
</td>
</tr>
</tbody>
</table>

## `ModelRevisionStates`     {#ome-io-v1beta1-ModelRevisionStates}


**Appears in:**

- [ModelStatus](#ome-io-v1beta1-ModelStatus)



<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>activeModelState</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-ModelState"><code>ModelState</code></a>
</td>
<td>
   <p>High level state string: Pending, Standby, Loading, Loaded, FailedToLoad</p>
</td>
</tr>
<tr><td><code>targetModelState</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-ModelState"><code>ModelState</code></a>
</td>
<td>
   <span class="text-muted">No description provided.</span></td>
</tr>
</tbody>
</table>

## `ModelSizeRangeSpec`     {#ome-io-v1beta1-ModelSizeRangeSpec}


**Appears in:**

- [ServingRuntimeSpec](#ome-io-v1beta1-ServingRuntimeSpec)


<p>ModelSizeRangeSpec defines the range of model sizes supported by this runtime</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>min</code><br/>
<code>string</code>
</td>
<td>
   <p>Minimum size of the model in bytes</p>
</td>
</tr>
<tr><td><code>max</code><br/>
<code>string</code>
</td>
<td>
   <p>Maximum size of the model in bytes</p>
</td>
</tr>
</tbody>
</table>

## `ModelState`     {#ome-io-v1beta1-ModelState}

(Alias of `string`)

**Appears in:**

- [ModelRevisionStates](#ome-io-v1beta1-ModelRevisionStates)


<p>ModelState enum</p>




## `ModelStatus`     {#ome-io-v1beta1-ModelStatus}


**Appears in:**

- [InferenceServiceStatus](#ome-io-v1beta1-InferenceServiceStatus)



<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>transitionStatus</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-TransitionStatus"><code>TransitionStatus</code></a>
</td>
<td>
   <p>Whether the available model server endpoints reflect the current Spec or is in transition</p>
</td>
</tr>
<tr><td><code>modelRevisionStates</code><br/>
<a href="#ome-io-v1beta1-ModelRevisionStates"><code>ModelRevisionStates</code></a>
</td>
<td>
   <p>State information of the model.</p>
</td>
</tr>
<tr><td><code>lastFailureInfo</code><br/>
<a href="#ome-io-v1beta1-FailureInfo"><code>FailureInfo</code></a>
</td>
<td>
   <p>Details of last failure, when load of target model is failed or blocked.</p>
</td>
</tr>
<tr><td><code>modelCopies</code><br/>
<a href="#ome-io-v1beta1-ModelCopies"><code>ModelCopies</code></a>
</td>
<td>
   <p>Model copy information of the model.</p>
</td>
</tr>
</tbody>
</table>

## `ModelStatusSpec`     {#ome-io-v1beta1-ModelStatusSpec}


**Appears in:**

- [BaseModel](#ome-io-v1beta1-BaseModel)

- [ClusterBaseModel](#ome-io-v1beta1-ClusterBaseModel)

- [FineTunedWeight](#ome-io-v1beta1-FineTunedWeight)


<p>ModelStatusSpec defines the observed state of Model weight</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>lifecycle</code> <B>[Required]</B><br/>
<code>string</code>
</td>
<td>
   <p>LifeCycle is an enum of Deprecated, Experiment, Public, Internal</p>
</td>
</tr>
<tr><td><code>state</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-LifeCycleState"><code>LifeCycleState</code></a>
</td>
<td>
   <p>Status of the model weight</p>
</td>
</tr>
<tr><td><code>observedGeneration</code><br/>
<code>int64</code>
</td>
<td>
   <p>ObservedGeneration is the .metadata.generation the controller last
reconciled against. If this lags behind .metadata.generation, the
controller has not yet observed the latest spec edit.</p>
</td>
</tr>
<tr><td><code>lastReconcileTime</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#time-v1-meta"><code>k8s.io/apimachinery/pkg/apis/meta/v1.Time</code></a>
</td>
<td>
   <p>LastReconcileTime is the wall-clock timestamp of the controller's most
recent reconcile pass for this object. Liveness signal — if it falls
behind by more than the reconcile period, the controller is wedged or
not scheduled. Distinct from Condition.lastTransitionTime, which only
advances on actual status flips.</p>
</td>
</tr>
<tr><td><code>nodesReady</code> <B>[Required]</B><br/>
<code>[]string</code>
</td>
<td>
   <span class="text-muted">No description provided.</span></td>
</tr>
<tr><td><code>nodesFailed</code> <B>[Required]</B><br/>
<code>[]string</code>
</td>
<td>
   <span class="text-muted">No description provided.</span></td>
</tr>
<tr><td><code>conditions</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#condition-v1-meta"><code>[]k8s.io/apimachinery/pkg/apis/meta/v1.Condition</code></a>
</td>
<td>
   <p>Conditions describe model readiness and source/metadata state. The PVC
path reports SourceReachable and Ready; PerNode models continue to
report node-level state through NodesReady and NodesFailed.</p>
</td>
</tr>
</tbody>
</table>

## `ObjectReference`     {#ome-io-v1beta1-ObjectReference}


**Appears in:**

- [FineTunedWeightSpec](#ome-io-v1beta1-FineTunedWeightSpec)


<p>ObjectReference contains enough information to let you inspect or modify the referred object.</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>name</code> <B>[Required]</B><br/>
<code>string</code>
</td>
<td>
   <p>Name of the referenced object</p>
</td>
</tr>
<tr><td><code>namespace</code> <B>[Required]</B><br/>
<code>string</code>
</td>
<td>
   <p>Namespace of the referenced object</p>
</td>
</tr>
</tbody>
</table>

## `PodOverride`     {#ome-io-v1beta1-PodOverride}


**Appears in:**

- [BenchmarkJobSpec](#ome-io-v1beta1-BenchmarkJobSpec)



<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>image</code><br/>
<code>string</code>
</td>
<td>
   <p>Image specifies the container image to use for the benchmark job.</p>
</td>
</tr>
<tr><td><code>env</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#envvar-v1-core"><code>[]k8s.io/api/core/v1.EnvVar</code></a>
</td>
<td>
   <p>List of environment variables to set in the container.</p>
</td>
</tr>
<tr><td><code>envFrom</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#envfromsource-v1-core"><code>[]k8s.io/api/core/v1.EnvFromSource</code></a>
</td>
<td>
   <p>List of sources to populate environment variables in the container.</p>
</td>
</tr>
<tr><td><code>volumeMounts</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#volumemount-v1-core"><code>[]k8s.io/api/core/v1.VolumeMount</code></a>
</td>
<td>
   <p>Pod volumes to mount into the container's filesystem.</p>
</td>
</tr>
<tr><td><code>resources</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#resourcerequirements-v1-core"><code>k8s.io/api/core/v1.ResourceRequirements</code></a>
</td>
<td>
   <p>Compute Resources required by this container.
Cannot be updated.
More info: https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/</p>
</td>
</tr>
<tr><td><code>tolerations</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#toleration-v1-core"><code>[]k8s.io/api/core/v1.Toleration</code></a>
</td>
<td>
   <p>If specified, the pod's tolerations.</p>
</td>
</tr>
<tr><td><code>nodeSelector</code><br/>
<code>map[string]string</code>
</td>
<td>
   <p>NodeSelector is a selector which must be true for the pod to fit on a node.
Selector which must match a node's labels for the pod to be scheduled on that node.
More info: https://kubernetes.io/docs/concepts/configuration/assign-pod-node/</p>
</td>
</tr>
<tr><td><code>affinity</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#affinity-v1-core"><code>k8s.io/api/core/v1.Affinity</code></a>
</td>
<td>
   <p>If specified, the pod's scheduling constraints</p>
</td>
</tr>
<tr><td><code>volumes</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#volume-v1-core"><code>[]k8s.io/api/core/v1.Volume</code></a>
</td>
<td>
   <p>List of volumes that can be mounted by containers belonging to the pod.
More info: https://kubernetes.io/docs/concepts/storage/volumes</p>
</td>
</tr>
</tbody>
</table>

## `PodSpec`     {#ome-io-v1beta1-PodSpec}


**Appears in:**

- [DecoderSpec](#ome-io-v1beta1-DecoderSpec)

- [EngineSpec](#ome-io-v1beta1-EngineSpec)

- [LeaderSpec](#ome-io-v1beta1-LeaderSpec)

- [RouterSpec](#ome-io-v1beta1-RouterSpec)

- [WorkerSpec](#ome-io-v1beta1-WorkerSpec)


<p>PodSpec is a description of a pod.</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>volumes</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#volume-v1-core"><code>[]k8s.io/api/core/v1.Volume</code></a>
</td>
<td>
   <p>List of volumes that can be mounted by containers belonging to the pod.
More info: https://kubernetes.io/docs/concepts/storage/volumes</p>
</td>
</tr>
<tr><td><code>initContainers</code> <B>[Required]</B><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#container-v1-core"><code>[]k8s.io/api/core/v1.Container</code></a>
</td>
<td>
   <p>List of initialization containers belonging to the pod.
Init containers are executed in order prior to containers being started. If any
init container fails, the pod is considered to have failed and is handled according
to its restartPolicy. The name for an init container or normal container must be
unique among all containers.
Init containers may not have Lifecycle actions, Readiness probes, Liveness probes, or Startup probes.
The resourceRequirements of an init container are taken into account during scheduling
by finding the highest request/limit for each resource type, and then using the max of
of that value or the sum of the normal containers. Limits are applied to init containers
in a similar fashion.
Init containers cannot currently be added or removed.
Cannot be updated.
More info: https://kubernetes.io/docs/concepts/workloads/pods/init-containers/</p>
</td>
</tr>
<tr><td><code>containers</code> <B>[Required]</B><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#container-v1-core"><code>[]k8s.io/api/core/v1.Container</code></a>
</td>
<td>
   <p>List of containers belonging to the pod.
Containers cannot currently be added or removed.
There must be at least one container in a Pod.
Cannot be updated.</p>
</td>
</tr>
<tr><td><code>ephemeralContainers</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#ephemeralcontainer-v1-core"><code>[]k8s.io/api/core/v1.EphemeralContainer</code></a>
</td>
<td>
   <p>List of ephemeral containers run in this pod. Ephemeral containers may be run in an existing
pod to perform user-initiated actions such as debugging. This list cannot be specified when
creating a pod, and it cannot be modified by updating the pod spec. In order to add an
ephemeral container to an existing pod, use the pod's ephemeralcontainers subresource.</p>
</td>
</tr>
<tr><td><code>restartPolicy</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#restartpolicy-v1-core"><code>k8s.io/api/core/v1.RestartPolicy</code></a>
</td>
<td>
   <p>Restart policy for all containers within the pod.
One of Always, OnFailure, Never. In some contexts, only a subset of those values may be permitted.
Default to Always.
More info: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#restart-policy</p>
</td>
</tr>
<tr><td><code>terminationGracePeriodSeconds</code><br/>
<code>int64</code>
</td>
<td>
   <p>Optional duration in seconds the pod needs to terminate gracefully. May be decreased in delete request.
Value must be non-negative integer. The value zero indicates stop immediately via
the kill signal (no opportunity to shut down).
If this value is nil, the default grace period will be used instead.
The grace period is the duration in seconds after the processes running in the pod are sent
a termination signal and the time when the processes are forcibly halted with a kill signal.
Set this value longer than the expected cleanup time for your process.
Defaults to 30 seconds.</p>
</td>
</tr>
<tr><td><code>activeDeadlineSeconds</code><br/>
<code>int64</code>
</td>
<td>
   <p>Optional duration in seconds the pod may be active on the node relative to
StartTime before the system will actively try to mark it failed and kill associated containers.
Value must be a positive integer.</p>
</td>
</tr>
<tr><td><code>dnsPolicy</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#dnspolicy-v1-core"><code>k8s.io/api/core/v1.DNSPolicy</code></a>
</td>
<td>
   <p>Set DNS policy for the pod.
Defaults to &quot;ClusterFirst&quot;.
Valid values are 'ClusterFirstWithHostNet', 'ClusterFirst', 'Default' or 'None'.
DNS parameters given in DNSConfig will be merged with the policy selected with DNSPolicy.
To have DNS options set along with hostNetwork, you have to specify DNS policy
explicitly to 'ClusterFirstWithHostNet'.</p>
</td>
</tr>
<tr><td><code>nodeSelector</code><br/>
<code>map[string]string</code>
</td>
<td>
   <p>NodeSelector is a selector which must be true for the pod to fit on a node.
Selector which must match a node's labels for the pod to be scheduled on that node.
More info: https://kubernetes.io/docs/concepts/configuration/assign-pod-node/</p>
</td>
</tr>
<tr><td><code>serviceAccountName</code><br/>
<code>string</code>
</td>
<td>
   <p>ServiceAccountName is the name of the ServiceAccount to use to run this pod.
More info: https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/</p>
</td>
</tr>
<tr><td><code>serviceAccount</code><br/>
<code>string</code>
</td>
<td>
   <p>DeprecatedServiceAccount is a deprecated alias for ServiceAccountName.
Deprecated: Use serviceAccountName instead.</p>
</td>
</tr>
<tr><td><code>automountServiceAccountToken</code><br/>
<code>bool</code>
</td>
<td>
   <p>AutomountServiceAccountToken indicates whether a service account token should be automatically mounted.</p>
</td>
</tr>
<tr><td><code>nodeName</code><br/>
<code>string</code>
</td>
<td>
   <p>NodeName indicates in which node this pod is scheduled.
If empty, this pod is a candidate for scheduling by the scheduler defined in schedulerName.
Once this field is set, the kubelet for this node becomes responsible for the lifecycle of this pod.
This field should not be used to express a desire for the pod to be scheduled on a specific node.
https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#nodename</p>
</td>
</tr>
<tr><td><code>hostNetwork</code><br/>
<code>bool</code>
</td>
<td>
   <p>Host networking requested for this pod. Use the host's network namespace.
If this option is set, the ports that will be used must be specified.
Default to false.</p>
</td>
</tr>
<tr><td><code>hostPID</code><br/>
<code>bool</code>
</td>
<td>
   <p>Use the host's pid namespace.
Optional: Default to false.</p>
</td>
</tr>
<tr><td><code>hostIPC</code><br/>
<code>bool</code>
</td>
<td>
   <p>Use the host's ipc namespace.
Optional: Default to false.</p>
</td>
</tr>
<tr><td><code>shareProcessNamespace</code><br/>
<code>bool</code>
</td>
<td>
   <p>Share a single process namespace between all of the containers in a pod.
When this is set containers will be able to view and signal processes from other containers
in the same pod, and the first process in each container will not be assigned PID 1.
HostPID and ShareProcessNamespace cannot both be set.
Optional: Default to false.</p>
</td>
</tr>
<tr><td><code>securityContext</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#podsecuritycontext-v1-core"><code>k8s.io/api/core/v1.PodSecurityContext</code></a>
</td>
<td>
   <p>SecurityContext holds pod-level security attributes and common container settings.
Optional: Defaults to empty.  See type description for default values of each field.</p>
</td>
</tr>
<tr><td><code>imagePullSecrets</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#localobjectreference-v1-core"><code>[]k8s.io/api/core/v1.LocalObjectReference</code></a>
</td>
<td>
   <p>ImagePullSecrets is an optional list of references to secrets in the same namespace to use for pulling any of the images used by this PodSpec.
If specified, these secrets will be passed to individual puller implementations for them to use.
More info: https://kubernetes.io/docs/concepts/containers/images#specifying-imagepullsecrets-on-a-pod</p>
</td>
</tr>
<tr><td><code>hostname</code><br/>
<code>string</code>
</td>
<td>
   <p>Specifies the hostname of the Pod
If not specified, the pod's hostname will be set to a system-defined value.</p>
</td>
</tr>
<tr><td><code>subdomain</code><br/>
<code>string</code>
</td>
<td>
   <p>If specified, the fully qualified Pod hostname will be &quot;<!-- raw HTML omitted -->.<!-- raw HTML omitted -->.<!-- raw HTML omitted -->.svc.<!-- raw HTML omitted -->&quot;.
If not specified, the pod will not have a domainname at all.</p>
</td>
</tr>
<tr><td><code>affinity</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#affinity-v1-core"><code>k8s.io/api/core/v1.Affinity</code></a>
</td>
<td>
   <p>If specified, the pod's scheduling constraints</p>
</td>
</tr>
<tr><td><code>schedulerName</code><br/>
<code>string</code>
</td>
<td>
   <p>If specified, the pod will be dispatched by specified scheduler.
If not specified, the pod will be dispatched by default scheduler.</p>
</td>
</tr>
<tr><td><code>tolerations</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#toleration-v1-core"><code>[]k8s.io/api/core/v1.Toleration</code></a>
</td>
<td>
   <p>If specified, the pod's tolerations.</p>
</td>
</tr>
<tr><td><code>hostAliases</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#hostalias-v1-core"><code>[]k8s.io/api/core/v1.HostAlias</code></a>
</td>
<td>
   <p>HostAliases is an optional list of hosts and IPs that will be injected into the pod's hosts
file if specified.</p>
</td>
</tr>
<tr><td><code>priorityClassName</code><br/>
<code>string</code>
</td>
<td>
   <p>If specified, indicates the pod's priority. &quot;system-node-critical&quot; and
&quot;system-cluster-critical&quot; are two special keywords which indicate the
highest priorities with the former being the highest priority. Any other
name must be defined by creating a PriorityClass object with that name.
If not specified, the pod priority will be default or zero if there is no
default.</p>
</td>
</tr>
<tr><td><code>priority</code><br/>
<code>int32</code>
</td>
<td>
   <p>The priority value. Various system components use this field to find the
priority of the pod. When Priority Admission Controller is enabled, it
prevents users from setting this field. The admission controller populates
this field from PriorityClassName.
The higher the value, the higher the priority.</p>
</td>
</tr>
<tr><td><code>dnsConfig</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#poddnsconfig-v1-core"><code>k8s.io/api/core/v1.PodDNSConfig</code></a>
</td>
<td>
   <p>Specifies the DNS parameters of a pod.
Parameters specified here will be merged to the generated DNS
configuration based on DNSPolicy.</p>
</td>
</tr>
<tr><td><code>readinessGates</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#podreadinessgate-v1-core"><code>[]k8s.io/api/core/v1.PodReadinessGate</code></a>
</td>
<td>
   <p>If specified, all readiness gates will be evaluated for pod readiness.
A pod is ready when all its containers are ready AND
all conditions specified in the readiness gates have status equal to &quot;True&quot;
More info: https://git.k8s.io/enhancements/keps/sig-network/580-pod-readiness-gates</p>
</td>
</tr>
<tr><td><code>runtimeClassName</code><br/>
<code>string</code>
</td>
<td>
   <p>RuntimeClassName refers to a RuntimeClass object in the node.k8s.io group, which should be used
to run this pod.  If no RuntimeClass resource matches the named class, the pod will not be run.
If unset or empty, the &quot;legacy&quot; RuntimeClass will be used, which is an implicit class with an
empty definition that uses the default runtime handler.
More info: https://git.k8s.io/enhancements/keps/sig-node/585-runtime-class</p>
</td>
</tr>
<tr><td><code>enableServiceLinks</code><br/>
<code>bool</code>
</td>
<td>
   <p>EnableServiceLinks indicates whether information about services should be injected into pod's
environment variables, matching the syntax of Docker links.
Optional: Defaults to true.</p>
</td>
</tr>
<tr><td><code>preemptionPolicy</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#preemptionpolicy-v1-core"><code>k8s.io/api/core/v1.PreemptionPolicy</code></a>
</td>
<td>
   <p>PreemptionPolicy is the Policy for preempting pods with lower priority.
One of Never, PreemptLowerPriority.
Defaults to PreemptLowerPriority if unset.</p>
</td>
</tr>
<tr><td><code>overhead</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#resourcelist-v1-core"><code>k8s.io/api/core/v1.ResourceList</code></a>
</td>
<td>
   <p>Overhead represents the resource overhead associated with running a pod for a given RuntimeClass.
This field will be autopopulated at admission time by the RuntimeClass admission controller. If
the RuntimeClass admission controller is enabled, overhead must not be set in Pod create requests.
The RuntimeClass admission controller will reject Pod create requests which have the overhead already
set. If RuntimeClass is configured and selected in the PodSpec, Overhead will be set to the value
defined in the corresponding RuntimeClass, otherwise it will remain unset and treated as zero.
More info: https://git.k8s.io/enhancements/keps/sig-node/688-pod-overhead/README.md</p>
</td>
</tr>
<tr><td><code>topologySpreadConstraints</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#topologyspreadconstraint-v1-core"><code>[]k8s.io/api/core/v1.TopologySpreadConstraint</code></a>
</td>
<td>
   <p>TopologySpreadConstraints describes how a group of pods ought to spread across topology
domains. Scheduler will schedule pods in a way which abides by the constraints.
All topologySpreadConstraints are ANDed.</p>
</td>
</tr>
<tr><td><code>setHostnameAsFQDN</code><br/>
<code>bool</code>
</td>
<td>
   <p>If true the pod's hostname will be configured as the pod's FQDN, rather than the leaf name (the default).
In Linux containers, this means setting the FQDN in the hostname field of the kernel (the nodename field of struct utsname).
In Windows containers, this means setting the registry value of hostname for the registry key HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters to FQDN.
If a pod does not have FQDN, this has no effect.
Default to false.</p>
</td>
</tr>
<tr><td><code>os</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#podos-v1-core"><code>k8s.io/api/core/v1.PodOS</code></a>
</td>
<td>
   <p>Specifies the OS of the containers in the pod.
Some pod and container fields are restricted if this is set.</p>
<p>If the OS field is set to linux, the following fields must be unset:
-securityContext.windowsOptions</p>
<p>If the OS field is set to windows, following fields must be unset:</p>
<ul>
<li>spec.hostPID</li>
<li>spec.hostIPC</li>
<li>spec.hostUsers</li>
<li>spec.securityContext.appArmorProfile</li>
<li>spec.securityContext.seLinuxOptions</li>
<li>spec.securityContext.seccompProfile</li>
<li>spec.securityContext.fsGroup</li>
<li>spec.securityContext.fsGroupChangePolicy</li>
<li>spec.securityContext.sysctls</li>
<li>spec.shareProcessNamespace</li>
<li>spec.securityContext.runAsUser</li>
<li>spec.securityContext.runAsGroup</li>
<li>spec.securityContext.supplementalGroups</li>
<li>spec.securityContext.supplementalGroupsPolicy</li>
<li>spec.containers[*].securityContext.appArmorProfile</li>
<li>spec.containers[*].securityContext.seLinuxOptions</li>
<li>spec.containers[*].securityContext.seccompProfile</li>
<li>spec.containers[*].securityContext.capabilities</li>
<li>spec.containers[*].securityContext.readOnlyRootFilesystem</li>
<li>spec.containers[*].securityContext.privileged</li>
<li>spec.containers[*].securityContext.allowPrivilegeEscalation</li>
<li>spec.containers[*].securityContext.procMount</li>
<li>spec.containers[*].securityContext.runAsUser</li>
<li>spec.containers[*].securityContext.runAsGroup</li>
</ul>
</td>
</tr>
<tr><td><code>hostUsers</code><br/>
<code>bool</code>
</td>
<td>
   <p>Use the host's user namespace.
Optional: Default to true.
If set to true or not present, the pod will be run in the host user namespace, useful
for when the pod needs a feature only available to the host user namespace, such as
loading a kernel module with CAP_SYS_MODULE.
When set to false, a new userns is created for the pod. Setting false is useful for
mitigating container breakout vulnerabilities even allowing users to run their
containers as root without actually having root privileges on the host.
This field is alpha-level and is only honored by servers that enable the UserNamespacesSupport feature.</p>
</td>
</tr>
<tr><td><code>schedulingGates</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#podschedulinggate-v1-core"><code>[]k8s.io/api/core/v1.PodSchedulingGate</code></a>
</td>
<td>
   <p>SchedulingGates is an opaque list of values that if specified will block scheduling the pod.
If schedulingGates is not empty, the pod will stay in the SchedulingGated state and the
scheduler will not attempt to schedule the pod.</p>
<p>SchedulingGates can only be set at pod creation time, and be removed only afterwards.</p>
</td>
</tr>
<tr><td><code>resourceClaims</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#podresourceclaim-v1-core"><code>[]k8s.io/api/core/v1.PodResourceClaim</code></a>
</td>
<td>
   <p>ResourceClaims defines which ResourceClaims must be allocated
and reserved before the Pod is allowed to start. The resources
will be made available to those containers which consume them
by name.</p>
<p>This is an alpha field and requires enabling the
DynamicResourceAllocation feature gate.</p>
<p>This field is immutable.</p>
</td>
</tr>
</tbody>
</table>

## `RouterSpec`     {#ome-io-v1beta1-RouterSpec}


**Appears in:**

- [InferenceServiceSpec](#ome-io-v1beta1-InferenceServiceSpec)

- [ServingRuntimeSpec](#ome-io-v1beta1-ServingRuntimeSpec)


<p>RouterSpec defines the configuration for the Router component, which handles request routing</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>PodSpec</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-PodSpec"><code>PodSpec</code></a>
</td>
<td>(Members of <code>PodSpec</code> are embedded into this type.)
   <p>PodSpec defines the container configuration for the router</p>
</td>
</tr>
<tr><td><code>ComponentExtensionSpec</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-ComponentExtensionSpec"><code>ComponentExtensionSpec</code></a>
</td>
<td>(Members of <code>ComponentExtensionSpec</code> are embedded into this type.)
   <p>ComponentExtensionSpec defines deployment configuration like min/max replicas, scaling metrics, etc.</p>
</td>
</tr>
<tr><td><code>runner</code><br/>
<a href="#ome-io-v1beta1-RunnerSpec"><code>RunnerSpec</code></a>
</td>
<td>
   <p>This is essentially a container spec that can override the default container</p>
</td>
</tr>
<tr><td><code>config</code><br/>
<code>map[string]string</code>
</td>
<td>
   <p>Additional configuration parameters for the runner
This can include framework-specific settings</p>
</td>
</tr>
</tbody>
</table>

## `RunnerSpec`     {#ome-io-v1beta1-RunnerSpec}


**Appears in:**

- [DecoderSpec](#ome-io-v1beta1-DecoderSpec)

- [EngineSpec](#ome-io-v1beta1-EngineSpec)

- [LeaderSpec](#ome-io-v1beta1-LeaderSpec)

- [RouterSpec](#ome-io-v1beta1-RouterSpec)

- [WorkerSpec](#ome-io-v1beta1-WorkerSpec)


<p>RunnerSpec defines container configuration plus additional config settings
The Runner is the primary container that executes the model serving or token generation logic.</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>Container</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#container-v1-core"><code>k8s.io/api/core/v1.Container</code></a>
</td>
<td>(Members of <code>Container</code> are embedded into this type.)
   <p>Container spec for the runner
Provides complete Kubernetes container configuration for the primary execution container.</p>
</td>
</tr>
</tbody>
</table>

## `RuntimeSelectorOperator`     {#ome-io-v1beta1-RuntimeSelectorOperator}

(Alias of `string`)

**Appears in:**

- [ModelFormat](#ome-io-v1beta1-ModelFormat)

- [ModelFrameworkSpec](#ome-io-v1beta1-ModelFrameworkSpec)





## `ScaleMetric`     {#ome-io-v1beta1-ScaleMetric}

(Alias of `string`)

**Appears in:**

- [ComponentExtensionSpec](#ome-io-v1beta1-ComponentExtensionSpec)


<p>ScaleMetric enum</p>




## `ScalerAuthenticationRef`     {#ome-io-v1beta1-ScalerAuthenticationRef}


**Appears in:**

- [KedaConfig](#ome-io-v1beta1-KedaConfig)


<p>ScalerAuthenticationRef points to a KEDA TriggerAuthentication or ClusterTriggerAuthentication resource
that contains the credentials for authenticating with the scaler's target (e.g., Prometheus server).</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>name</code> <B>[Required]</B><br/>
<code>string</code>
</td>
<td>
   <p>Name of the TriggerAuthentication or ClusterTriggerAuthentication resource.</p>
</td>
</tr>
<tr><td><code>kind</code><br/>
<code>string</code>
</td>
<td>
   <p>Kind of the authentication resource being referenced.
Valid values are &quot;TriggerAuthentication&quot; (namespace-scoped) or &quot;ClusterTriggerAuthentication&quot; (cluster-scoped).</p>
</td>
</tr>
</tbody>
</table>

## `ServiceMetadata`     {#ome-io-v1beta1-ServiceMetadata}


**Appears in:**

- [BenchmarkJobSpec](#ome-io-v1beta1-BenchmarkJobSpec)


<p>ServiceMetadata contains metadata fields for recording the backend model server's configuration and version details.
This information helps track experiment context, enabling users to filter and query experiments based on server properties.</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>engine</code> <B>[Required]</B><br/>
<code>string</code>
</td>
<td>
   <p>Engine specifies the backend model server engine.
Supported values: &quot;vLLM&quot;, &quot;SGLang&quot;, &quot;TGI&quot;.</p>
</td>
</tr>
<tr><td><code>version</code> <B>[Required]</B><br/>
<code>string</code>
</td>
<td>
   <p>Version specifies the version of the model server (e.g., &quot;0.5.3&quot;).</p>
</td>
</tr>
<tr><td><code>gpuType</code> <B>[Required]</B><br/>
<code>string</code>
</td>
<td>
   <p>GpuType specifies the type of GPU used by the model server.
Supported values: &quot;H100&quot;, &quot;A100&quot;, &quot;MI300&quot;, &quot;A10&quot;.</p>
</td>
</tr>
<tr><td><code>gpuCount</code> <B>[Required]</B><br/>
<code>int</code>
</td>
<td>
   <p>GpuCount indicates the number of GPU cards available on the model server.</p>
</td>
</tr>
</tbody>
</table>

## `ServingRuntimePodSpec`     {#ome-io-v1beta1-ServingRuntimePodSpec}


**Appears in:**

- [ServingRuntimeSpec](#ome-io-v1beta1-ServingRuntimeSpec)

- [WorkerPodSpec](#ome-io-v1beta1-WorkerPodSpec)



<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>containers</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#container-v1-core"><code>[]k8s.io/api/core/v1.Container</code></a>
</td>
<td>
   <p>List of containers belonging to the pod.
Containers cannot currently be added or removed.
Cannot be updated.</p>
</td>
</tr>
<tr><td><code>volumes</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#volume-v1-core"><code>[]k8s.io/api/core/v1.Volume</code></a>
</td>
<td>
   <p>List of volumes that can be mounted by containers belonging to the pod.
More info: https://kubernetes.io/docs/concepts/storage/volumes</p>
</td>
</tr>
<tr><td><code>nodeSelector</code><br/>
<code>map[string]string</code>
</td>
<td>
   <p>NodeSelector is a selector which must be true for the pod to fit on a node.
Selector which must match a node's labels for the pod to be scheduled on that node.
More info: https://kubernetes.io/docs/concepts/configuration/assign-pod-node/</p>
</td>
</tr>
<tr><td><code>affinity</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#affinity-v1-core"><code>k8s.io/api/core/v1.Affinity</code></a>
</td>
<td>
   <p>If specified, the pod's scheduling constraints</p>
</td>
</tr>
<tr><td><code>tolerations</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#toleration-v1-core"><code>[]k8s.io/api/core/v1.Toleration</code></a>
</td>
<td>
   <p>If specified, the pod's tolerations.</p>
</td>
</tr>
<tr><td><code>labels</code><br/>
<code>map[string]string</code>
</td>
<td>
   <p>Labels that will be add to the pod.
More info: http://kubernetes.io/docs/user-guide/labels</p>
</td>
</tr>
<tr><td><code>annotations</code><br/>
<code>map[string]string</code>
</td>
<td>
   <p>Annotations that will be add to the pod.
More info: http://kubernetes.io/docs/user-guide/annotations</p>
</td>
</tr>
<tr><td><code>imagePullSecrets</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#localobjectreference-v1-core"><code>[]k8s.io/api/core/v1.LocalObjectReference</code></a>
</td>
<td>
   <p>ImagePullSecrets is an optional list of references to secrets in the same namespace to use for pulling any of the images used by this PodSpec.
If specified, these secrets will be passed to individual puller implementations for them to use.
More info: https://kubernetes.io/docs/concepts/containers/images#specifying-imagepullsecrets-on-a-pod</p>
</td>
</tr>
<tr><td><code>schedulerName</code><br/>
<code>string</code>
</td>
<td>
   <p>If specified, the pod will be dispatched by specified scheduler.
If not specified, the pod will be dispatched by default scheduler.</p>
</td>
</tr>
<tr><td><code>hostIPC</code><br/>
<code>bool</code>
</td>
<td>
   <p>Use the host's ipc namespace.
Optional: Default to false.</p>
</td>
</tr>
<tr><td><code>dnsPolicy</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#dnspolicy-v1-core"><code>k8s.io/api/core/v1.DNSPolicy</code></a>
</td>
<td>
   <p>Set DNS policy for the pod.
Defaults to &quot;ClusterFirst&quot;.
Valid values are 'ClusterFirstWithHostNet', 'ClusterFirst', 'Default' or 'None'.
DNS parameters given in DNSConfig will be merged with the policy selected with DNSPolicy.
To have DNS options set along with hostNetwork, you have to specify DNS policy
explicitly to 'ClusterFirstWithHostNet'.</p>
</td>
</tr>
<tr><td><code>hostNetwork</code><br/>
<code>bool</code>
</td>
<td>
   <p>Host networking requested for this pod. Use the host's network namespace.
If this option is set, the ports that will be used must be specified.
Default to false.</p>
</td>
</tr>
</tbody>
</table>

## `ServingRuntimeRef`     {#ome-io-v1beta1-ServingRuntimeRef}


**Appears in:**

- [InferenceServiceSpec](#ome-io-v1beta1-InferenceServiceSpec)



<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>name</code> <B>[Required]</B><br/>
<code>string</code>
</td>
<td>
   <p>Name of the runtime being referenced
Identifies the specific runtime environment to be used for model execution.</p>
</td>
</tr>
<tr><td><code>kind</code> <B>[Required]</B><br/>
<code>string</code>
</td>
<td>
   <p>Kind of the runtime being referenced
Defaults to ClusterServingRuntime
Specifies the Kubernetes resource kind of the referenced runtime.
ClusterServingRuntime is a cluster-wide runtime, while ServingRuntime is namespace-scoped.</p>
</td>
</tr>
<tr><td><code>apiGroup</code> <B>[Required]</B><br/>
<code>string</code>
</td>
<td>
   <p>APIGroup of the resource being referenced
Defaults to <code>ome.io</code>
Specifies the Kubernetes API group of the referenced runtime.</p>
</td>
</tr>
<tr><td><code>autoSync</code><br/>
<code>bool</code>
</td>
<td>
   <p>AutoSync (default true) re-renders pod specs from the live
runtime every reconcile. When false, the ISVC pins to a
ControllerRevision snapshot; bump ome.io/runtime-sync or set
spec.runtime.revision to roll forward.</p>
</td>
</tr>
<tr><td><code>revision</code><br/>
<code>string</code>
</td>
<td>
   <p>Revision pins to a named ControllerRevision in the OME
namespace. Enables rollback; ignored when AutoSync is true.</p>
</td>
</tr>
</tbody>
</table>

## `ServingRuntimeSpec`     {#ome-io-v1beta1-ServingRuntimeSpec}


**Appears in:**

- [ClusterServingRuntime](#ome-io-v1beta1-ClusterServingRuntime)

- [ServingRuntime](#ome-io-v1beta1-ServingRuntime)



<p>ServingRuntimeSpec defines the desired state of ServingRuntime. This spec is currently provisional
and are subject to change as details regarding single-model serving and multi-model serving
are hammered out.</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>supportedModelFormats</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-SupportedModelFormat"><code>[]SupportedModelFormat</code></a>
</td>
<td>
   <p>Model formats and version supported by this runtime</p>
</td>
</tr>
<tr><td><code>modelSizeRange</code><br/>
<a href="#ome-io-v1beta1-ModelSizeRangeSpec"><code>ModelSizeRangeSpec</code></a>
</td>
<td>
   <p>ModelSizeRange is the range of model sizes supported by this runtime</p>
</td>
</tr>
<tr><td><code>disabled</code><br/>
<code>bool</code>
</td>
<td>
   <p>Set to true to disable use of this runtime</p>
</td>
</tr>
<tr><td><code>routerConfig</code><br/>
<a href="#ome-io-v1beta1-RouterSpec"><code>RouterSpec</code></a>
</td>
<td>
   <p>Router configuration for this runtime</p>
</td>
</tr>
<tr><td><code>engineConfig</code><br/>
<a href="#ome-io-v1beta1-EngineSpec"><code>EngineSpec</code></a>
</td>
<td>
   <p>Engine configuration for this runtime</p>
</td>
</tr>
<tr><td><code>decoderConfig</code><br/>
<a href="#ome-io-v1beta1-DecoderSpec"><code>DecoderSpec</code></a>
</td>
<td>
   <p>Decoder configuration for this runtime</p>
</td>
</tr>
<tr><td><code>protocolVersions</code><br/>
<a href="https://pkg.go.dev/sigs.k8s.io/ome/pkg/constants#InferenceServiceProtocol"><code>[]sigs.k8s.io/ome/pkg/constants.InferenceServiceProtocol</code></a>
</td>
<td>
   <p>Supported protocol versions (i.e. openAI or cohere or openInference-v1 or openInference-v2)</p>
</td>
</tr>
<tr><td><code>ServingRuntimePodSpec</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-ServingRuntimePodSpec"><code>ServingRuntimePodSpec</code></a>
</td>
<td>(Members of <code>ServingRuntimePodSpec</code> are embedded into this type.)
   <p>PodSpec for the serving runtime</p>
</td>
</tr>
<tr><td><code>workers</code><br/>
<a href="#ome-io-v1beta1-WorkerPodSpec"><code>WorkerPodSpec</code></a>
</td>
<td>
   <p>WorkerPodSpec for the serving runtime, this is used for multi-node serving</p>
</td>
</tr>
<tr><td><code>acceleratorRequirements</code><br/>
<a href="#ome-io-v1beta1-AcceleratorRequirements"><code>AcceleratorRequirements</code></a>
</td>
<td>
   <p>AcceleratorRequirements specifies the accelerator requirements for this runtime</p>
</td>
</tr>
</tbody>
</table>

## `ServingRuntimeStatus`     {#ome-io-v1beta1-ServingRuntimeStatus}


**Appears in:**

- [ClusterServingRuntime](#ome-io-v1beta1-ClusterServingRuntime)

- [ServingRuntime](#ome-io-v1beta1-ServingRuntime)


<p>ServingRuntimeStatus defines the observed state of ServingRuntime</p>




## `StorageSpec`     {#ome-io-v1beta1-StorageSpec}


**Appears in:**

- [BaseModelSpec](#ome-io-v1beta1-BaseModelSpec)

- [BenchmarkJobSpec](#ome-io-v1beta1-BenchmarkJobSpec)

- [FineTunedWeightSpec](#ome-io-v1beta1-FineTunedWeightSpec)



<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>path</code><br/>
<code>string</code>
</td>
<td>
   <p>Path is the absolute path where the model will be downloaded and stored on the node.</p>
</td>
</tr>
<tr><td><code>schemaPath</code><br/>
<code>string</code>
</td>
<td>
   <p>SchemaPath is the path to the model schema or configuration file within the storage system.
This can be used to validate the model or customize how it's loaded.</p>
</td>
</tr>
<tr><td><code>parameters</code><br/>
<code>map[string]string</code>
</td>
<td>
   <p>Parameters contain key-value pairs to override default storage credentials or configuration.
These values are typically used to configure access to object storage or mount options.</p>
</td>
</tr>
<tr><td><code>key</code><br/>
<code>string</code>
</td>
<td>
   <p>StorageKey is the name of the key in a Kubernetes Secret used to authenticate access to the model storage.
This key will be used to fetch credentials during model download or access.</p>
</td>
</tr>
<tr><td><code>storageUri</code> <B>[Required]</B><br/>
<code>string</code>
</td>
<td>
   <p>StorageUri specifies the source URI of the model in a supported storage backend.
Supported formats:</p>
<ul>
<li>OCI Object Storage:   oci://n/{namespace}/b/{bucket}/o/{object_path}</li>
<li>CNCF ModelPack:       modelpack://{registry}/{repository}[:{tag}]</li>
<li>Persistent Volume:    pvc://{pvc-name}/{sub-path}</li>
<li>Vendor-specific:      vendor://{vendor-name}/{resource-type}/{resource-path}
This field is required.</li>
</ul>
</td>
</tr>
<tr><td><code>nodeSelector</code><br/>
<code>map[string]string</code>
</td>
<td>
   <p>NodeSelector defines a set of key-value label pairs that must be present on a node
for the model to be scheduled and downloaded onto that node.</p>
</td>
</tr>
<tr><td><code>nodeAffinity</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#nodeaffinity-v1-core"><code>k8s.io/api/core/v1.NodeAffinity</code></a>
</td>
<td>
   <p>NodeAffinity describes the node affinity rules that further constrain which nodes
are eligible to download and store this model, based on advanced scheduling policies.</p>
</td>
</tr>
<tr><td><code>downloadPolicy</code><br/>
<a href="#ome-io-v1beta1-DownloadPolicy"><code>DownloadPolicy</code></a>
</td>
<td>
   <p>DownloadPolicy describes the policy of downloading model artifacts
Supported policies:</p>
<ul>
<li>AlwaysDownload: always download a copy of model artifact in destination path</li>
<li>ReuseIfExists: if the identical model artifact has been downloaded in the node, such artifact will be reused</li>
</ul>
</td>
</tr>
</tbody>
</table>

## `SupportedModelFormat`     {#ome-io-v1beta1-SupportedModelFormat}


**Appears in:**

- [ServingRuntimeSpec](#ome-io-v1beta1-ServingRuntimeSpec)



<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>name</code><br/>
<code>string</code>
</td>
<td>
   <p>TODO this field is being used as model format name, and this is not correct, we should deprecate this and use Name from ModelFormat
Name of the model</p>
</td>
</tr>
<tr><td><code>modelFormat</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-ModelFormat"><code>ModelFormat</code></a>
</td>
<td>
   <p>ModelFormat of the model, e.g., &quot;PyTorch&quot;, &quot;TensorFlow&quot;, &quot;ONNX&quot;, &quot;SafeTensors&quot;</p>
</td>
</tr>
<tr><td><code>modelType</code><br/>
<code>string</code>
</td>
<td>
   <p>DEPRECATED: This field is deprecated and will be removed in future releases.</p>
</td>
</tr>
<tr><td><code>version</code><br/>
<code>string</code>
</td>
<td>
   <p>Version of the model format.
Used in validating that a runtime supports a model.
It Can be &quot;major&quot;, &quot;major.minor&quot; or &quot;major.minor.patch&quot;.</p>
</td>
</tr>
<tr><td><code>modelFramework</code> <B>[Required]</B><br/>
<a href="#ome-io-v1beta1-ModelFrameworkSpec"><code>ModelFrameworkSpec</code></a>
</td>
<td>
   <p>ModelFramework of the model, e.g., &quot;PyTorch&quot;, &quot;TensorFlow&quot;, &quot;ONNX&quot;, &quot;Transformers&quot;</p>
</td>
</tr>
<tr><td><code>modelArchitecture</code><br/>
<code>string</code>
</td>
<td>
   <p>ModelArchitecture of the model, e.g., &quot;LlamaForCausalLM&quot;, &quot;GemmaForCausalLM&quot;, &quot;MixtralForCausalLM&quot;</p>
</td>
</tr>
<tr><td><code>quantization</code><br/>
<a href="#ome-io-v1beta1-ModelQuantization"><code>ModelQuantization</code></a>
</td>
<td>
   <p>Quantization of the model, e.g., &quot;fp8&quot;, &quot;fbgemm_fp8&quot;, &quot;int4&quot;</p>
</td>
</tr>
<tr><td><code>diffusionPipeline</code><br/>
<a href="#ome-io-v1beta1-DiffusionPipelineSpec"><code>DiffusionPipelineSpec</code></a>
</td>
<td>
   <p>DiffusionPipeline supported by this runtime (used for diffusion models).</p>
</td>
</tr>
<tr><td><code>autoSelect</code><br/>
<code>bool</code>
</td>
<td>
   <p>Set to true to allow the ServingRuntime to be used for automatic model placement if
this model format is specified with no explicit runtime.</p>
</td>
</tr>
<tr><td><code>priority</code><br/>
<code>int32</code>
</td>
<td>
   <p>Priority of this serving runtime for auto selection.
This is used to select the serving runtime if more than one serving runtime supports the same model format.
The value should be greater than zero.  The higher the value, the higher the priority.
Priority is not considered if AutoSelect is either false or not specified.
Priority can be overridden by specifying the runtime in the InferenceService.</p>
</td>
</tr>
<tr><td><code>acceleratorConfig</code><br/>
<a href="#ome-io-v1beta1-*sigs-k8s-io-ome-pkg-apis-ome-v1beta1-AcceleratorModelConfig"><code>map[string]AcceleratorModelConfig</code></a>
</td>
<td>
   <p>AcceleratorConfig provides accelerator-specific overrides for this model format</p>
</td>
</tr>
</tbody>
</table>

## `TensorParallelismConfig`     {#ome-io-v1beta1-TensorParallelismConfig}


**Appears in:**



<p>TensorParallelismConfig specifies tensor parallelism settings</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>tensorParallelSize</code><br/>
<code>int64</code>
</td>
<td>
   <p>tensorParallelSize specifies the size of the tensor parallelism</p>
</td>
</tr>
<tr><td><code>pipelineParallelSize</code><br/>
<code>int64</code>
</td>
<td>
   <p>pipelineParallelSize specifies the size of the pipeline parallelism</p>
</td>
</tr>
<tr><td><code>dataParallelSize</code><br/>
<code>int64</code>
</td>
<td>
   <p>dataParallelSize specifies the size of the data parallelism</p>
</td>
</tr>
</tbody>
</table>

## `TransitionStatus`     {#ome-io-v1beta1-TransitionStatus}

(Alias of `string`)

**Appears in:**

- [ModelStatus](#ome-io-v1beta1-ModelStatus)


<p>TransitionStatus enum</p>




## `WorkerPodSpec`     {#ome-io-v1beta1-WorkerPodSpec}


**Appears in:**

- [ServingRuntimeSpec](#ome-io-v1beta1-ServingRuntimeSpec)



<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>size</code><br/>
<code>int</code>
</td>
<td>
   <p>Size of the worker, this is the number of pods in the worker.</p>
</td>
</tr>
<tr><td><code>ServingRuntimePodSpec</code><br/>
<a href="#ome-io-v1beta1-ServingRuntimePodSpec"><code>ServingRuntimePodSpec</code></a>
</td>
<td>(Members of <code>ServingRuntimePodSpec</code> are embedded into this type.)
   <p>PodSpec for the worker</p>
</td>
</tr>
</tbody>
</table>

## `WorkerSpec`     {#ome-io-v1beta1-WorkerSpec}


**Appears in:**

- [DecoderSpec](#ome-io-v1beta1-DecoderSpec)

- [EngineSpec](#ome-io-v1beta1-EngineSpec)


<p>WorkerSpec defines the configuration for worker nodes in a multi-node component
Worker nodes perform the distributed processing tasks assigned by the leader node,
enabling horizontal scaling for compute-intensive workloads.</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>

<tr><td><code>PodSpec</code><br/>
<a href="#ome-io-v1beta1-PodSpec"><code>PodSpec</code></a>
</td>
<td>(Members of <code>PodSpec</code> are embedded into this type.)
   <p>PodSpec for the worker
Allows customization of the Kubernetes Pod configuration specifically for worker nodes.</p>
</td>
</tr>
<tr><td><code>size</code><br/>
<code>int</code>
</td>
<td>
   <p>Size of the worker, this is the number of pods in the worker.
Controls how many worker pod instances will be deployed for horizontal scaling.</p>
</td>
</tr>
<tr><td><code>runner</code><br/>
<a href="#ome-io-v1beta1-RunnerSpec"><code>RunnerSpec</code></a>
</td>
<td>
   <p>Runner container override for customizing the main container
This is essentially a container spec that can override the default container
Provides fine-grained control over the container that executes the worker node's processing logic.</p>
</td>
</tr>
</tbody>
</table>
