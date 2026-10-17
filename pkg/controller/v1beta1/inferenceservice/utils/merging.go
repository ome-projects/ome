package utils

import (
	"bytes"
	"encoding/json"
	"html/template"
	"regexp"
	"strconv"
	"strings"

	goerrors "github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/strategicpatch"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/runtimeinheritance"
)

// MergeRuntimeContainers Merge the predictor Container struct with the runtime Container struct, allowing users
func MergeRuntimeContainers(runtimeContainer *v1.Container, predictorContainer *v1.Container) (*v1.Container, error) {
	// Save runtime container name, as the name can be overridden as empty string during the Unmarshal below
	// since the Name field does not have the 'omitempty' struct tag.
	runtimeContainerName := runtimeContainer.Name

	// Use JSON Marshal/Unmarshal to merge Container structs using strategic merge patch
	runtimeContainerJson, err := json.Marshal(runtimeContainer)
	if err != nil {
		return nil, err
	}

	overrides, err := json.Marshal(predictorContainer)
	if err != nil {
		return nil, err
	}

	overrides, err = runtimeinheritance.NullStaleEnvSources(overrides)
	if err != nil {
		return nil, err
	}

	mergedContainer := v1.Container{}
	jsonResult, err := strategicpatch.StrategicMergePatch(runtimeContainerJson, overrides, mergedContainer)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(jsonResult, &mergedContainer); err != nil {
		return nil, err
	}

	// Container name precedence walk: predictor (post-merge) wins, then
	// fall back to the runtime's pre-merge name, then to the canonical
	// "ome-container" default. The final default prevents the apiserver
	// "spec.containers[0].name: Required value" rejection when neither
	// the ServingRuntime's runner nor the ISVC's runner sets a container
	// name (an empty merged Name deadlocks OMENative rollouts at Pod
	// create time).
	if mergedContainer.Name == "" {
		mergedContainer.Name = runtimeContainerName
	}
	if mergedContainer.Name == "" {
		mergedContainer.Name = constants.MainContainerName
	}

	// Strategic merge patch will replace args but more useful behaviour here is to concatenate
	mergedContainer.Args = append(append([]string{}, runtimeContainer.Args...), predictorContainer.Args...)

	return &mergedContainer, nil
}

// mergeSpec merges two Kubernetes-style specs using strategic merge patch.
// `runtimeInit` is the base (typically from a runtime default), and `override` comes from the user-defined resource.
// Fields from `override` will overwrite corresponding fields in `runtimeInit`.
func mergeSpec[T any](runtimeInit T, override T) (*T, error) {
	baseJSON, err := json.Marshal(runtimeInit)
	if err != nil {
		return nil, err
	}

	overrideJSON, err := json.Marshal(override)
	if err != nil {
		return nil, err
	}

	overrideJSON, err = runtimeinheritance.NullStaleEnvSources(overrideJSON)
	if err != nil {
		return nil, err
	}

	var merged T
	jsonResult, err := strategicpatch.StrategicMergePatch(baseJSON, overrideJSON, merged)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(jsonResult, &merged); err != nil {
		return nil, err
	}

	return &merged, nil
}

// MergeRouterSpec merges a runtime-provided RouterSpec with a user-provided RouterSpec from InferenceService.
// The user-provided (isvcRouter) fields take precedence over the runtime defaults.
func MergeRouterSpec(isvcRouter, runtimeRouter *v1beta1.RouterSpec) (*v1beta1.RouterSpec, error) {
	switch {
	case isvcRouter == nil:
		// if router is not specified in isvc, return nil
		return nil, nil
	case runtimeRouter == nil:
		// if router is not specified in runtime, return a copy of isvcRouter
		return isvcRouter.DeepCopy(), nil
	}

	merged, err := mergeSpec(v1beta1.RouterSpec{
		ComponentExtensionSpec: runtimeRouter.ComponentExtensionSpec,
		PodSpec:                runtimeRouter.PodSpec,
		Runner:                 runtimeRouter.Runner,
		Config:                 runtimeRouter.Config,
	}, *isvcRouter)
	if err != nil {
		return nil, err
	}
	restoreRunnerName(merged.Runner, runtimeRouter.Runner)
	return merged, nil
}

// restoreRunnerName fixes the strategic-merge-patch blind spot where
// the user-provided RunnerSpec sets `name: ""` (the common case when
// only an image override is supplied — kubectl-defaulted zero value for
// fields the patch didn't mention). v1.Container.Name has no
// `omitempty` struct tag, so the empty string round-trips through
// JSON and overwrites the runtime's name during merge, producing a
// pod whose container has no name. apiserver then rejects pod creates
// with `spec.containers[0].name: Required value`, deadlocking
// OMENative rollouts. Same shape as the post-merge fixup
// MergeRuntimeContainers applies at the v1.Container level.
//
// Precedence walk: (1) preserve the merged name when set, (2) fall back
// to the runtime RunnerSpec's name, (3) fall back to the canonical
// "ome-container" default when neither side names the container — same
// final default MergeRuntimeContainers applies, kept in sync so the two
// merge paths produce identical Pod-name behavior. The merged pointer
// may be nil — guard accordingly. The runtime pointer is allowed to be
// nil too; in that case we still apply the canonical default if merged
// is non-nil with an empty name.
func restoreRunnerName(merged, runtime *v1beta1.RunnerSpec) {
	if merged == nil {
		return
	}
	if merged.Name == "" && runtime != nil {
		merged.Name = runtime.Name
	}
	if merged.Name == "" {
		merged.Name = constants.MainContainerName
	}
}

// foldComponentPodSpecIntoLeaderWorker folds the component-level PodSpec
// (the inline EngineSpec/DecoderSpec PodSpec) into the Leader and Worker
// pod templates of a multi-pod component, reusing the same strategic-merge
// path (mergeSpec) as every other merge in this file.
//
// Why this exists: for a multi-node component the renderer
// (engine.go/decoder.go reconcilePodSpec/reconcileWorkerPodSpec) sources its
// base PodSpec from Leader.PodSpec / Worker.PodSpec — NOT from the
// component-level PodSpec. Without this fold, component-level pod fields
// (volumes, nodeSelector, tolerations, affinity, imagePullSecrets, ...)
// declared on engineConfig/decoderConfig are silently dropped from the
// leader/worker pods while their container volumeMounts still apply, so a
// runtime declaring `engineConfig.volumes:[dshm]` + a leader runner mounting
// `dshm` renders a Pod that mounts a volume that doesn't exist → the
// apiserver rejects it ("volumeMounts[0].name: Not found: dshm").
//
// The component PodSpec is the base and the leader/worker PodSpec is the
// override, so leader/worker entries win — with the standard strategic-merge
// semantics carried by the PodSpec field tags: name-keyed lists (volumes,
// imagePullSecrets) merge by name; atomic fields (nodeSelector, tolerations)
// are replaced wholesale when the child sets them; structs (affinity,
// securityContext) deep-merge. Single-pod components render straight from the
// component-level PodSpec, which is left intact, so they're unaffected.
func foldComponentPodSpecIntoLeaderWorker(component *v1beta1.PodSpec, leader *v1beta1.LeaderSpec, worker *v1beta1.WorkerSpec) error {
	if component == nil {
		return nil
	}
	if leader != nil {
		folded, err := mergeSpec(*component, leader.PodSpec)
		if err != nil {
			return err
		}
		leader.PodSpec = *folded
	}
	if worker != nil {
		folded, err := mergeSpec(*component, worker.PodSpec)
		if err != nil {
			return err
		}
		worker.PodSpec = *folded
	}
	return nil
}

// MergeEngineSpec merges a runtime-provided EngineSpec with a user-provided EngineSpec from InferenceService.
// The user-provided (isvcEngine) fields take precedence over the runtime defaults.
func MergeEngineSpec(runtimeEngine, isvcEngine *v1beta1.EngineSpec) (*v1beta1.EngineSpec, error) {
	switch {
	case isvcEngine == nil:
		// if engine is not specified in isvc, return nil
		return nil, nil
	case runtimeEngine == nil:
		// if engine is not specified in runtime, return a copy of isvcEngine
		merged := isvcEngine.DeepCopy()
		if err := foldComponentPodSpecIntoLeaderWorker(&merged.PodSpec, merged.Leader, merged.Worker); err != nil {
			return nil, err
		}
		return merged, nil
	}

	merged, err := mergeSpec(v1beta1.EngineSpec{
		ComponentExtensionSpec: runtimeEngine.ComponentExtensionSpec,
		PodSpec:                runtimeEngine.PodSpec,
		Runner:                 runtimeEngine.Runner,
		Leader:                 runtimeEngine.Leader,
		Worker:                 runtimeEngine.Worker,
		TopologyKey:            runtimeEngine.TopologyKey,
		TopologySpread:         runtimeEngine.TopologySpread,
		TopologySpreadKey:      runtimeEngine.TopologySpreadKey,
	}, *isvcEngine)
	if err != nil {
		return nil, err
	}
	restoreRunnerName(merged.Runner, runtimeEngine.Runner)
	if err := foldComponentPodSpecIntoLeaderWorker(&merged.PodSpec, merged.Leader, merged.Worker); err != nil {
		return nil, err
	}
	return merged, nil
}

// MergeDecoderSpec merges a runtime-provided DecoderSpec with a user-provided DecoderSpec from InferenceService.
// The user-provided (isvcDecoder) fields take precedence over the runtime defaults.
func MergeDecoderSpec(runtimeDecoder, isvcDecoder *v1beta1.DecoderSpec) (*v1beta1.DecoderSpec, error) {
	switch {
	case isvcDecoder == nil:
		// if decoder is not specified in isvc, return nil
		return nil, nil
	case runtimeDecoder == nil:
		// if decoder is not specified in runtime, return a copy of isvcDecoder
		merged := isvcDecoder.DeepCopy()
		if err := foldComponentPodSpecIntoLeaderWorker(&merged.PodSpec, merged.Leader, merged.Worker); err != nil {
			return nil, err
		}
		return merged, nil
	}

	merged, err := mergeSpec(v1beta1.DecoderSpec{
		ComponentExtensionSpec: runtimeDecoder.ComponentExtensionSpec,
		PodSpec:                runtimeDecoder.PodSpec,
		Runner:                 runtimeDecoder.Runner,
		Leader:                 runtimeDecoder.Leader,
		Worker:                 runtimeDecoder.Worker,
		TopologyKey:            runtimeDecoder.TopologyKey,
		TopologySpread:         runtimeDecoder.TopologySpread,
		TopologySpreadKey:      runtimeDecoder.TopologySpreadKey,
	}, *isvcDecoder)
	if err != nil {
		return nil, err
	}
	restoreRunnerName(merged.Runner, runtimeDecoder.Runner)
	if err := foldComponentPodSpecIntoLeaderWorker(&merged.PodSpec, merged.Leader, merged.Worker); err != nil {
		return nil, err
	}
	return merged, nil
}

// ConvertPodSpec converts v1beta1.PodSpec to v1.PodSpec
// This handles the conversion between the custom v1beta1.PodSpec type and the core v1.PodSpec type
func ConvertPodSpec(spec *v1beta1.PodSpec) (*v1.PodSpec, error) {
	if spec == nil {
		return nil, goerrors.New("cannot convert nil PodSpec")
	}

	// Use JSON marshaling to convert between the types
	data, err := json.Marshal(spec)
	if err != nil {
		return nil, goerrors.Wrap(err, "failed to marshal v1beta1.PodSpec")
	}

	var podSpec v1.PodSpec
	if err := json.Unmarshal(data, &podSpec); err != nil {
		return nil, goerrors.Wrap(err, "failed to unmarshal to v1.PodSpec")
	}

	return &podSpec, nil
}

// ReplacePlaceholders Replace placeholders in runtime container by values from inferenceservice metadata
func ReplacePlaceholders(container *v1.Container, meta metav1.ObjectMeta) error {
	data, _ := json.Marshal(container)
	tmpl, err := template.New("container-tmpl").Parse(string(data))
	if err != nil {
		return err
	}
	buf := &bytes.Buffer{}
	err = tmpl.Execute(buf, meta)
	if err != nil {
		return err
	}
	return json.Unmarshal(buf.Bytes(), container)
}

// MergeNodeSelector merges node selectors from runtime, accelerator class, and inference service (isvc).
// Only add mergedNodeSelector to engine and decoder component.
func MergeNodeSelector(runtime *v1beta1.ServingRuntimeSpec, acceleratorClass *v1beta1.AcceleratorClassSpec, isvc *v1beta1.InferenceService, component v1beta1.ComponentType) map[string]string {
	// Start with runtime node selector
	mergedNodeSelector := map[string]string{}
	if runtime != nil && runtime.ServingRuntimePodSpec.NodeSelector != nil {
		for k, v := range runtime.ServingRuntimePodSpec.NodeSelector {
			mergedNodeSelector[k] = v
		}
	}

	// Merge in accelerator class node selector, overriding any conflicts
	if acceleratorClass != nil && acceleratorClass.Discovery.NodeSelector != nil {
		for k, v := range acceleratorClass.Discovery.NodeSelector {
			mergedNodeSelector[k] = v
		}
	}

	// Finally merge in isvc node selector, overriding any conflicts
	switch component {
	case v1beta1.EngineComponent:
		if isvc.Spec.Engine != nil && isvc.Spec.Engine.PodSpec.NodeSelector != nil {
			for k, v := range isvc.Spec.Engine.PodSpec.NodeSelector {
				mergedNodeSelector[k] = v
			}
		}
		return mergedNodeSelector

	case v1beta1.DecoderComponent:
		if isvc.Spec.Decoder != nil && isvc.Spec.Decoder.PodSpec.NodeSelector != nil {
			for k, v := range isvc.Spec.Decoder.PodSpec.NodeSelector {
				mergedNodeSelector[k] = v
			}
		}
		return mergedNodeSelector
	}

	return mergedNodeSelector
}

// MergeSchedulerName back-fills the runtime's top-level schedulerName
// into every pod-spec level of the merged component specs (component,
// leader, worker) so whichever template the renderer selects carries
// it. Fill-only — a name set at any of those levels (by the
// InferenceService or the runtime's component config) always wins, and
// with nothing set anywhere the field stays empty so the cluster's
// default scheduler applies.
func MergeSchedulerName(runtime *v1beta1.ServingRuntimeSpec, engine *v1beta1.EngineSpec, decoder *v1beta1.DecoderSpec, router *v1beta1.RouterSpec) {
	if runtime == nil || runtime.ServingRuntimePodSpec.SchedulerName == "" {
		return
	}
	name := runtime.ServingRuntimePodSpec.SchedulerName

	fill := func(spec *v1beta1.PodSpec) {
		if spec.SchedulerName == "" {
			spec.SchedulerName = name
		}
	}
	if engine != nil {
		fill(&engine.PodSpec)
		if engine.Leader != nil {
			fill(&engine.Leader.PodSpec)
		}
		if engine.Worker != nil {
			fill(&engine.Worker.PodSpec)
		}
	}
	if decoder != nil {
		fill(&decoder.PodSpec)
		if decoder.Leader != nil {
			fill(&decoder.Leader.PodSpec)
		}
		if decoder.Worker != nil {
			fill(&decoder.Worker.PodSpec)
		}
	}
	if router != nil {
		fill(&router.PodSpec)
	}
}

// MergeResource fills the container's resource requests/limits from
// the matching runtime container when the operator hasn't set them on
// the InferenceService spec (fill-only), then applies the accelerator
// class's declared resources, which take precedence over the runtime's.
func MergeResource(container *v1.Container, acceleratorClass *v1beta1.AcceleratorClassSpec, runtime *v1beta1.ServingRuntimeSpec) {
	if container == nil {
		return
	}

	// Merge resource requests.
	if container.Resources.Requests == nil {
		container.Resources.Requests = v1.ResourceList{}
	}
	// Merge resource requests from runtime with the same container name if it does not already exist.
	if runtime != nil && runtime.ServingRuntimePodSpec.Containers != nil {
		for _, rtContainer := range runtime.ServingRuntimePodSpec.Containers {
			if rtContainer.Name == container.Name {
				for resourceName, quantity := range rtContainer.Resources.Requests {
					if _, exists := container.Resources.Requests[resourceName]; !exists {
						container.Resources.Requests[resourceName] = quantity.DeepCopy()
					}
				}
				break
			}
		}
	}
	// Merge resource requests from accelerator class.
	// AcceleratorClass takes precedence and overrides runtime resources.
	if acceleratorClass != nil && acceleratorClass.Resources != nil {
		for _, resource := range acceleratorClass.Resources {
			resourceName := v1.ResourceName(resource.Name)
			quantity := resource.Quantity
			container.Resources.Requests[resourceName] = quantity.DeepCopy()
		}
	}

	// Merge resource limits
	if container.Resources.Limits == nil {
		container.Resources.Limits = v1.ResourceList{}
	}
	// Merge resource limits from runtime with the same container name if it does not already exist.
	if runtime != nil && runtime.ServingRuntimePodSpec.Containers != nil {
		for _, rtContainer := range runtime.ServingRuntimePodSpec.Containers {
			if rtContainer.Name == container.Name {
				for resourceName, quantity := range rtContainer.Resources.Limits {
					if _, exists := container.Resources.Limits[resourceName]; !exists {
						container.Resources.Limits[resourceName] = quantity.DeepCopy()
					}
				}
				break
			}
		}
	}
	// AcceleratorClass takes precedence and overrides runtime resource limits.
	if acceleratorClass != nil && acceleratorClass.Resources != nil {
		for _, resource := range acceleratorClass.Resources {
			resourceName := v1.ResourceName(resource.Name)
			quantity := resource.Quantity
			container.Resources.Limits[resourceName] = quantity.DeepCopy()
		}
	}
}

// extractArgKey extracts the key from an argument string for deduplication and override matching.
// For flags (starting with -), it extracts just the flag name.
// For non-flags (like "python3 -m server"), it returns the entire string as the key.
// This allows proper deduplication of both flags and command lines.
// Examples:
//   - "--tp-size=4" -> "--tp-size"
//   - "--tp-size 4" -> "--tp-size"
//   - "--enable-metrics" -> "--enable-metrics"
//   - "python3 -m server" -> "python3 -m server" (entire string is the key)
func extractArgKey(arg string) string {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return ""
	}

	// If it doesn't start with -, treat the entire string as the key
	// This handles commands like "python3 -m server"
	if !strings.HasPrefix(arg, "-") {
		return arg
	}

	// For flags, extract just the flag name
	// Handle --key=value format
	if idx := strings.Index(arg, "="); idx != -1 {
		return arg[:idx]
	}

	// Handle --key value format (space-separated)
	// Extract just the key part before any space
	parts := strings.Fields(arg)
	if len(parts) > 0 {
		return parts[0]
	}

	return arg
}

// isMultilineFormat checks if the args are in multi-line string format
// (contains newlines or backslash continuations)
func isMultilineFormat(args []string) bool {
	return len(args) > 0 &&
		(strings.Contains(args[0], "\n") || strings.Contains(args[0], "\\"))
}

// normalizeArgs converts args to a flat list of individual arguments.
// Handles both multi-line format and list-of-strings format.
func normalizeArgs(args []string) []string {
	var result []string
	for _, arg := range args {
		// Check if this is a multi-line string
		if isMultilineFormat([]string{arg}) {
			lines := strings.Split(arg, "\n")
			for _, line := range lines {
				trimmed := strings.TrimSpace(line)
				// Remove trailing backslash
				trimmed = strings.TrimRight(trimmed, "\\")
				trimmed = strings.TrimSpace(trimmed)
				if trimmed != "" {
					result = append(result, trimmed)
				}
			}
		} else {
			trimmed := strings.TrimSpace(arg)
			if trimmed != "" {
				result = append(result, trimmed)
			}
		}
	}
	return result
}

// toMultilineFormat converts a list of args back to multi-line format with backslash continuations.
func toMultilineFormat(args []string) []string {
	if len(args) == 0 {
		return args
	}

	var builder strings.Builder
	for i, arg := range args {
		if i > 0 {
			builder.WriteString("\n")
		}
		builder.WriteString(arg)
		if i < len(args)-1 {
			builder.WriteString(" \\")
		}
	}
	return []string{builder.String()}
}

// argGroup represents a parsed argument which may be a single element or a key-value pair
type argGroup struct {
	key    string   // The flag key (e.g., "--tp-size")
	values []string // The original elements (e.g., ["--tp-size=4"] or ["--tp-size", "4"])
}

// parseArgsIntoGroups parses a flat list of args into groups, detecting key-value pairs.
// Handles: --key=value (single element), --key value (two elements), --flag (boolean)
func parseArgsIntoGroups(args []string) []argGroup {
	var groups []argGroup
	i := 0
	for i < len(args) {
		arg := args[i]
		key := extractArgKey(arg)

		if key == "" {
			// Non-flag argument, treat as its own group
			groups = append(groups, argGroup{key: arg, values: []string{arg}})
			i++
			continue
		}

		// Check if this is --key=value format (value embedded)
		if strings.Contains(arg, "=") {
			groups = append(groups, argGroup{key: key, values: []string{arg}})
			i++
			continue
		}

		// Check if next element is a value (not a flag)
		// A value is something that doesn't start with "-"
		if i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
			// --key value format (two separate elements)
			groups = append(groups, argGroup{key: key, values: []string{arg, args[i+1]}})
			i += 2
			continue
		}

		// Boolean flag or flag at end without value
		groups = append(groups, argGroup{key: key, values: []string{arg}})
		i++
	}
	return groups
}

// mergeArgsWithKeyOverride performs key-based merging of normalized args.
// Override args with the same key replace existing args; new args are appended.
// Handles both --key=value and --key value formats.
// Returns the merged args in normalized (list) format.
func mergeArgsWithKeyOverride(baseArgs []string, overrideArgs []string) []string {
	// Parse both inputs into groups
	baseGroups := parseArgsIntoGroups(baseArgs)
	overrideGroups := parseArgsIntoGroups(overrideArgs)

	// Build ordered map: key -> argGroup
	argMap := make(map[string]argGroup)
	orderedKeys := make([]string, 0, len(baseGroups)+len(overrideGroups))

	// Process base args
	for _, group := range baseGroups {
		if _, exists := argMap[group.key]; !exists {
			orderedKeys = append(orderedKeys, group.key)
		}
		argMap[group.key] = group
	}

	// Apply overrides: replace existing or append new
	for _, group := range overrideGroups {
		if _, exists := argMap[group.key]; exists {
			// Replace existing value
			argMap[group.key] = group
		} else {
			// Add new key
			orderedKeys = append(orderedKeys, group.key)
			argMap[group.key] = group
		}
	}

	// Rebuild args in original order
	var result []string
	for _, key := range orderedKeys {
		if group, exists := argMap[key]; exists {
			result = append(result, group.values...)
		}
	}

	return result
}

// MergeArgs merges container args with override args using key-based deduplication.
// It handles both multi-line string format and list-of-strings format.
// Override args with the same key replace existing args; new args are appended.
// The output format matches the input format (multi-line or list).
func MergeArgs(containerArgs []string, overrideArgs []string) []string {
	if len(overrideArgs) == 0 {
		return containerArgs
	}
	if len(containerArgs) == 0 {
		return overrideArgs
	}

	// Detect input format
	inputIsMultiline := isMultilineFormat(containerArgs)

	// Normalize both inputs to flat list format
	baseArgs := normalizeArgs(containerArgs)
	overrides := normalizeArgs(overrideArgs)

	// Perform key-based merge
	merged := mergeArgsWithKeyOverride(baseArgs, overrides)

	// Convert back to original format
	if inputIsMultiline {
		return toMultilineFormat(merged)
	}
	return merged
}

// OverrideArgParam overrides a specific parameter with key in args.
// If the key exists (e.g., "--tp-size=4"), it replaces the value with the new one.
// Returns the updated args and a boolean indicating whether the key was found and replaced.
// The function handles most of the common formats:
// List of separate strings, Multi-line string, and Key-Value pairs.
func OverrideArgParam(containerArgs []string, key string, value int64) ([]string, bool) {
	if len(containerArgs) == 0 {
		return containerArgs, false
	}

	updated := false
	if isMultilineFormat(containerArgs) {
		arg := containerArgs[0]
		// Build regex pattern to match the key with its value
		// Matches: --tp-size=4 or --tp-size 4
		// Escapes special regex characters in the key
		escapedKey := regexp.QuoteMeta(key)
		pattern := regexp.MustCompile(escapedKey + `(?:=|\s+)\d+`)

		// Check if the key exists in the string
		if !pattern.MatchString(arg) {
			return containerArgs, updated
		}

		// Replace the existing value
		replacement := key + "=" + strconv.FormatInt(value, 10)
		arg = pattern.ReplaceAllString(arg, replacement)
		// Update the containerArgs with the modified value
		containerArgs[0] = arg
		updated = true
	} else {
		containerArgs, updated = overrideKeyValueInSlice(containerArgs, key, value)
	}
	return containerArgs, updated
}

func OverrideCommandParam(containerCommand []string, key string, value int64) ([]string, bool) {
	if len(containerCommand) == 0 {
		return containerCommand, false
	}

	return overrideKeyValueInSlice(containerCommand, key, value)
}

// overrideKeyValueInSlice finds a key in a slice of args and replaces its value.
// Handles both "--key value" (separate elements) and "--key=value" (combined) formats.
// Returns the modified slice and whether the key was found.
func overrideKeyValueInSlice(args []string, key string, value int64) ([]string, bool) {
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == key && i+1 < len(args) {
			args[i+1] = strconv.FormatInt(value, 10)
			return args, true
		} else if strings.HasPrefix(arg, key+"=") {
			args[i] = key + "=" + strconv.FormatInt(value, 10)
			return args, true
		}
	}
	return args, false
}
