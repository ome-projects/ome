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
	resources := slices.Clone(acceleratorResources)
	slices.Sort(resources)
	resources = slices.Compact(resources)
	if len(resources) == 0 {
		return UnitDemand{}, fmt.Errorf("accelerator resources must be configured")
	}
	wanted := make(map[corev1.ResourceName]bool, len(resources))
	for _, name := range resources {
		if !isExtendedResource(corev1.ResourceName(name)) {
			return UnitDemand{}, fmt.Errorf("configured accelerator resource %q is not an extended resource", name)
		}
		wanted[corev1.ResourceName(name)] = true
	}
	var out UnitDemand
	hasDemand := false
	for _, component := range []struct {
		name v1beta1.ComponentType
		pods []PodSet
	}{{v1beta1.EngineComponent, unit.Engine}, {v1beta1.DecoderComponent, unit.Decoder}} {
		replicas, err := unitReplicas(unit.Units, component.name)
		if err != nil {
			return UnitDemand{}, err
		}
		if len(component.pods) == 0 {
			continue
		}
		// The first declared component is the placement unit and must be present.
		if out.PrimaryUnits == 0 {
			if replicas == 0 {
				return UnitDemand{}, fmt.Errorf("%s replica unit multiplicity must be positive", component.name)
			}
			out.PrimaryUnits = replicas
		}
		if replicas == 0 {
			continue
		}
		sets := slices.Clone(component.pods)
		slices.SortFunc(sets, func(a, b PodSet) int { return cmp.Compare(a.Name, b.Name) })
		for i, set := range sets {
			if set.Name == "" || set.Count <= 0 || set.Spec == nil {
				return UnitDemand{}, fmt.Errorf("%s pod set requires a name, positive count, and rendered spec", component.name)
			}
			if i > 0 && sets[i-1].Name == set.Name {
				return UnitDemand{}, fmt.Errorf("duplicate %s pod set %q", component.name, set.Name)
			}
			if set.Count > math.MaxInt64/replicas {
				return UnitDemand{}, fmt.Errorf("%s pod set %q count exceeds int64 within the replica unit", component.name, set.Name)
			}
			set.Count *= replicas
			set.Spec = set.Spec.DeepCopy()
			requests, err := measurePod(set.Spec, wanted)
			if err != nil {
				return UnitDemand{}, fmt.Errorf("%s pod set %q: %w", component.name, set.Name, err)
			}
			for name, quantity := range requests {
				count, exact := wholeUnits(quantity)
				if !exact || count > math.MaxInt64/set.Count {
					return UnitDemand{}, fmt.Errorf("%s pod set %q demand for %s exceeds whole int64 units", component.name, set.Name, name)
				}
				requests[name] = *resource.NewQuantity(count*set.Count, resource.DecimalSI)
				hasDemand = true
			}
			out.Pods = append(out.Pods, PodDemand{Component: component.name, PodSet: set, Requests: requests})
		}
	}
	if !hasDemand {
		return UnitDemand{}, fmt.Errorf("replica unit has no positive accelerator demand")
	}
	encoded, err := json.Marshal(struct {
		Inputs    string
		Resources []string
		Pods      []PodDemand
	}{unit.InputFingerprint, resources, out.Pods})
	if err != nil {
		return UnitDemand{}, fmt.Errorf("fingerprint replica demand: %w", err)
	}
	sum := sha256.Sum256(encoded)
	out.Fingerprint = hex.EncodeToString(sum[:])
	return out, nil
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

func measurePod(spec *corev1.PodSpec, wanted map[corev1.ResourceName]bool) (corev1.ResourceList, error) {
	if len(spec.Containers) == 0 || len(spec.EphemeralContainers) != 0 {
		return nil, fmt.Errorf("rendered pod requires regular containers and no ephemeral containers")
	}
	if len(spec.ResourceClaims) != 0 {
		return nil, fmt.Errorf("dynamic resource claims have no allocatable resource mapping")
	}
	if spec.Resources != nil {
		if len(spec.Resources.Claims) != 0 {
			return nil, fmt.Errorf("pod-level dynamic resource claims have no allocatable resource mapping")
		}
		for _, resources := range []corev1.ResourceList{spec.Resources.Requests, spec.Resources.Limits} {
			for name := range resources {
				if !resourcehelper.IsSupportedPodLevelResource(name) {
					return nil, fmt.Errorf("unsupported pod-level resource %q", name)
				}
			}
		}
	}
	for _, containers := range [][]corev1.Container{spec.Containers, spec.InitContainers} {
		for i := range containers {
			container := &containers[i]
			if len(container.Resources.Claims) != 0 {
				return nil, fmt.Errorf("container %q has dynamic resource claims", container.Name)
			}
			for _, values := range []corev1.ResourceList{container.Resources.Requests, container.Resources.Limits} {
				if err := validateAccelerators(values, wanted); err != nil {
					return nil, fmt.Errorf("container %q: %w", container.Name, err)
				}
			}
			for name := range wanted {
				request, requested := container.Resources.Requests[name]
				limit, limited := container.Resources.Limits[name]
				if requested && (!limited || request.Cmp(limit) != 0) {
					return nil, fmt.Errorf("container %q requires equal request and limit for %s", container.Name, name)
				}
				// Kubernetes defaults an omitted extended-resource request to
				// its limit before the scheduler observes the pod.
				if limited && !requested {
					if container.Resources.Requests == nil {
						container.Resources.Requests = corev1.ResourceList{}
					}
					container.Resources.Requests[name] = limit.DeepCopy()
				}
			}
		}
	}
	if err := validateAccelerators(spec.Overhead, wanted); err != nil {
		return nil, fmt.Errorf("pod overhead: %w", err)
	}
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
