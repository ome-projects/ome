package capacity

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation"
	resourcehelper "k8s.io/component-helpers/resource"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// PodSet describes identical rendered pods in one component replica, such as
// its primary pod or workers. Count is explicit; it has no replica default.
type PodSet struct {
	Name  string
	Count int64
	Spec  *corev1.PodSpec
}

// ReplicaUnit contains the declared Engine/Decoder components in the ratio of
// their minimums. Units holds each component's replica count within one unit;
// an absent entry means one replica and zero leaves the component out.
// InputFingerprint identifies the resolved runtime and rendering dependencies.
// The caller supplies complete pod templates, including admission resources
// such as RuntimeClass overhead. Router has a separate per-home replica policy.
type ReplicaUnit struct {
	InputFingerprint string
	Engine           []PodSet
	Decoder          []PodSet
	Units            map[v1beta1.ComponentType]int64
}

// componentPodSets is one declared component's pod sets within a replica unit.
type componentPodSets struct {
	Component v1beta1.ComponentType
	Pods      []PodSet
}

// components lists the engine and decoder pod sets in declared order; the
// first component with pod sets is the placement unit.
func (u ReplicaUnit) components() []componentPodSets {
	return []componentPodSets{{v1beta1.EngineComponent, u.Engine}, {v1beta1.DecoderComponent, u.Decoder}}
}

// PodDemand retains the pod's scheduling constraints for flavor attribution.
// Requests contains the total for Count pods, not a per-pod quantity.
type PodDemand struct {
	Component v1beta1.ComponentType
	PodSet
	Requests corev1.ResourceList
}

// UnitDemand is measured accelerator demand before resource/flavor attribution.
// Its fingerprint includes all supplied pod shapes and rendering dependencies.
// PrimaryUnits is the number of primary-component replicas the measured unit
// contains; hardware divided by this demand counts whole units of that many.
type UnitDemand struct {
	Fingerprint  string
	Pods         []PodDemand
	PrimaryUnits int64
}

// MeasureUnit accounts regular containers, ordered restartable init containers,
// initialization peaks, and overhead using Kubernetes scheduler arithmetic.
// It does not establish flavor compatibility or member application.
func MeasureUnit(unit ReplicaUnit, acceleratorResources []string) (UnitDemand, error) {
	if unit.InputFingerprint == "" || len(unit.Engine)+len(unit.Decoder) == 0 {
		return UnitDemand{}, fmt.Errorf("resolved rendering inputs and component pod sets are required")
	}
	resources, wanted, err := configuredAccelerators(acceleratorResources)
	if err != nil {
		return UnitDemand{}, err
	}
	var out UnitDemand
	for _, component := range unit.components() {
		replicas, err := unitReplicas(unit.Units, component.Component)
		if err != nil {
			return UnitDemand{}, err
		}
		if len(component.Pods) == 0 {
			continue
		}
		// The first declared component is the placement unit and must be present.
		if out.PrimaryUnits == 0 {
			if replicas == 0 {
				return UnitDemand{}, fmt.Errorf("%s replica unit multiplicity must be positive", component.Component)
			}
			out.PrimaryUnits = replicas
		}
		if replicas == 0 {
			continue
		}
		pods, err := measureComponent(component, replicas, wanted)
		if err != nil {
			return UnitDemand{}, err
		}
		out.Pods = append(out.Pods, pods...)
	}
	if !slices.ContainsFunc(out.Pods, func(pod PodDemand) bool { return len(pod.Requests) > 0 }) {
		return UnitDemand{}, fmt.Errorf("replica unit has no positive accelerator demand")
	}
	out.Fingerprint, err = fingerprint(struct {
		Inputs    string
		Resources []string
		Pods      []PodDemand
	}{unit.InputFingerprint, resources, out.Pods})
	if err != nil {
		return UnitDemand{}, fmt.Errorf("fingerprint replica demand: %w", err)
	}
	return out, nil
}

// configuredAccelerators returns the configured accelerator resource names,
// sorted and unique, with the lookup set that measuring consults. Every name
// must be an extended resource.
func configuredAccelerators(names []string) ([]string, map[corev1.ResourceName]bool, error) {
	resources := slices.Clone(names)
	slices.Sort(resources)
	resources = slices.Compact(resources)
	if len(resources) == 0 {
		return nil, nil, fmt.Errorf("accelerator resources must be configured")
	}
	wanted := make(map[corev1.ResourceName]bool, len(resources))
	for _, name := range resources {
		if !isExtendedResource(corev1.ResourceName(name)) {
			return nil, nil, fmt.Errorf("configured accelerator resource %q is not an extended resource", name)
		}
		wanted[corev1.ResourceName(name)] = true
	}
	return resources, wanted, nil
}

// unitReplicas reads a component's replica count within the measured unit.
// An absent entry is one replica; an explicit zero excludes the component.
func unitReplicas(units map[v1beta1.ComponentType]int64, component v1beta1.ComponentType) (int64, error) {
	replicas, declared := units[component]
	if !declared {
		return 1, nil
	}
	if replicas < 0 {
		return 0, fmt.Errorf("%s replica unit multiplicity must be nonnegative", component)
	}
	return replicas, nil
}

// measureComponent measures one component's pod sets in name order, each
// scaled by the component's replica count within the unit. Names are unique.
func measureComponent(component componentPodSets, replicas int64, wanted map[corev1.ResourceName]bool) ([]PodDemand, error) {
	sets := slices.Clone(component.Pods)
	slices.SortFunc(sets, func(a, b PodSet) int { return cmp.Compare(a.Name, b.Name) })
	var out []PodDemand
	for i, set := range sets {
		if set.Name == "" || set.Count <= 0 || set.Spec == nil {
			return nil, fmt.Errorf("%s pod set requires a name, positive count, and rendered spec", component.Component)
		}
		if i > 0 && sets[i-1].Name == set.Name {
			return nil, fmt.Errorf("duplicate %s pod set %q", component.Component, set.Name)
		}
		pod, err := measurePodSet(component.Component, set, replicas, wanted)
		if err != nil {
			return nil, err
		}
		out = append(out, pod)
	}
	return out, nil
}

// measurePodSet scales one pod set to the component's replicas and measures
// the accelerator requests of all its pods on a copy of the rendered spec.
func measurePodSet(component v1beta1.ComponentType, set PodSet, replicas int64, wanted map[corev1.ResourceName]bool) (PodDemand, error) {
	if set.Count > math.MaxInt64/replicas {
		return PodDemand{}, fmt.Errorf("%s pod set %q count exceeds int64 within the replica unit", component, set.Name)
	}
	set.Count *= replicas
	set.Spec = set.Spec.DeepCopy()
	requests, err := measurePod(set.Spec, wanted)
	if err != nil {
		return PodDemand{}, fmt.Errorf("%s pod set %q: %w", component, set.Name, err)
	}
	for name, quantity := range requests {
		count, exact := wholeUnits(quantity)
		if !exact || count > math.MaxInt64/set.Count {
			return PodDemand{}, fmt.Errorf("%s pod set %q demand for %s exceeds whole int64 units", component, set.Name, name)
		}
		requests[name] = *resource.NewQuantity(count*set.Count, resource.DecimalSI)
	}
	return PodDemand{Component: component, PodSet: set, Requests: requests}, nil
}

// measurePod returns one pod's effective request for each configured
// accelerator. It fills omitted accelerator requests in place, so callers
// pass a copy of the rendered spec.
func measurePod(spec *corev1.PodSpec, wanted map[corev1.ResourceName]bool) (corev1.ResourceList, error) {
	if len(spec.Containers) == 0 || len(spec.EphemeralContainers) != 0 {
		return nil, fmt.Errorf("rendered pod requires regular containers and no ephemeral containers")
	}
	if len(spec.ResourceClaims) != 0 {
		return nil, fmt.Errorf("dynamic resource claims have no allocatable resource mapping")
	}
	if err := validatePodLevelResources(spec.Resources); err != nil {
		return nil, err
	}
	for _, containers := range [][]corev1.Container{spec.Containers, spec.InitContainers} {
		for i := range containers {
			if err := validateContainerAccelerators(&containers[i], wanted); err != nil {
				return nil, err
			}
			if err := defaultAcceleratorRequests(&containers[i], wanted); err != nil {
				return nil, err
			}
		}
	}
	if err := validateAccelerators(spec.Overhead, wanted); err != nil {
		return nil, fmt.Errorf("pod overhead: %w", err)
	}
	return acceleratorRequests(spec, wanted)
}

// validatePodLevelResources accepts only the pod-level resources the
// scheduler's arithmetic supports, and no pod-level dynamic claims.
func validatePodLevelResources(resources *corev1.ResourceRequirements) error {
	if resources == nil {
		return nil
	}
	if len(resources.Claims) != 0 {
		return fmt.Errorf("pod-level dynamic resource claims have no allocatable resource mapping")
	}
	for _, values := range []corev1.ResourceList{resources.Requests, resources.Limits} {
		for name := range values {
			if !resourcehelper.IsSupportedPodLevelResource(name) {
				return fmt.Errorf("unsupported pod-level resource %q", name)
			}
		}
	}
	return nil
}

// validateContainerAccelerators rejects dynamic claims and any accelerator
// value that is not a whole configured quantity, in requests then limits.
func validateContainerAccelerators(container *corev1.Container, wanted map[corev1.ResourceName]bool) error {
	if len(container.Resources.Claims) != 0 {
		return fmt.Errorf("container %q has dynamic resource claims", container.Name)
	}
	for _, values := range []corev1.ResourceList{container.Resources.Requests, container.Resources.Limits} {
		if err := validateAccelerators(values, wanted); err != nil {
			return fmt.Errorf("container %q: %w", container.Name, err)
		}
	}
	return nil
}

// defaultAcceleratorRequests requires every accelerator request to equal its
// limit and copies an omitted request from the limit, as Kubernetes does for
// extended resources before the scheduler observes the pod.
func defaultAcceleratorRequests(container *corev1.Container, wanted map[corev1.ResourceName]bool) error {
	for name := range wanted {
		request, requested := container.Resources.Requests[name]
		limit, limited := container.Resources.Limits[name]
		if requested && (!limited || request.Cmp(limit) != 0) {
			return fmt.Errorf("container %q requires equal request and limit for %s", container.Name, name)
		}
		if limited && !requested {
			if container.Resources.Requests == nil {
				container.Resources.Requests = corev1.ResourceList{}
			}
			container.Resources.Requests[name] = limit.DeepCopy()
		}
	}
	return nil
}

// acceleratorRequests is the pod's request for each configured accelerator by
// the scheduler's arithmetic, without zero entries; nil when there is none.
func acceleratorRequests(spec *corev1.PodSpec, wanted map[corev1.ResourceName]bool) (corev1.ResourceList, error) {
	requests := resourcehelper.PodRequests(&corev1.Pod{Spec: *spec}, resourcehelper.PodResourcesOptions{})
	var out corev1.ResourceList
	for name, quantity := range requests {
		if !wanted[name] || quantity.IsZero() {
			continue
		}
		if _, exact := wholeUnits(quantity); !exact {
			return nil, fmt.Errorf("pod demand for %s exceeds whole int64 units", name)
		}
		if out == nil {
			out = corev1.ResourceList{}
		}
		out[name] = quantity.DeepCopy()
	}
	return out, nil
}

func validateAccelerators(values corev1.ResourceList, wanted map[corev1.ResourceName]bool) error {
	for name, quantity := range values {
		if !wanted[name] {
			if !quantity.IsZero() && isExtendedResource(name) {
				return fmt.Errorf("extended resource %q is absent from accelerator configuration", name)
			}
			continue
		}
		if _, exact := wholeUnits(quantity); !exact {
			return fmt.Errorf("accelerator %s must be nonnegative whole units within int64", name)
		}
	}
	return nil
}

func isExtendedResource(name corev1.ResourceName) bool {
	value := string(name)
	return strings.Contains(value, "/") &&
		!strings.Contains(value, corev1.ResourceDefaultNamespacePrefix) &&
		!strings.HasPrefix(value, corev1.DefaultResourceRequestsPrefix) &&
		len(validation.IsQualifiedName(corev1.DefaultResourceRequestsPrefix+value)) == 0
}

// fingerprint is the hex SHA-256 of a value's JSON encoding.
func fingerprint(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}
