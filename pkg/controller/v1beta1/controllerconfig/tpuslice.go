package controllerconfig

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	kjson "sigs.k8s.io/json"

	"sigs.k8s.io/ome/pkg/tpuslice"
)

// TPUSliceProvisioningConfigName is the inferenceservice-config ConfigMap key
// holding the TPU slice provisioning block.
const TPUSliceProvisioningConfigName = "tpuSliceProvisioning"

// +kubebuilder:object:generate=false
// TPUSliceProvisioningConfig describes how the controller provisions TPU
// slices on node pools that grant them on demand. The block is optional and
// read once at manager startup: absent, the controller provisions no slices.
// Present, every field is required and the binary carries no default for any
// of them, so a malformed block prevents the manager from starting.
type TPUSliceProvisioningConfig struct {
	// ChipResource is the extended resource pods request TPU chips under.
	ChipResource v1.ResourceName `json:"chipResource"`

	// NodeLabels name the node labels that place a pod on a slice.
	NodeLabels TPUSliceNodeLabels `json:"nodeLabels"`

	// ProvisionOnly is the node label marking a node pool whose slices are
	// provisioned on demand. A pool without it holds static slices, which the
	// controller never provisions or releases.
	ProvisionOnly TPUSliceLabel `json:"provisionOnly"`

	// Accelerators maps an accelerator label value to how its slices are
	// provisioned. Only listed accelerators are provisioned.
	Accelerators map[string]TPUSliceAccelerator `json:"accelerators"`

	// Slice is the metadata and readiness contract of a slice object.
	Slice TPUSliceObject `json:"slice"`
}

// +kubebuilder:object:generate=false
// TPUSliceNodeLabels are node label keys. They must be distinct from each
// other and from the ProvisionOnly key.
type TPUSliceNodeLabels struct {
	// Accelerator carries a node's accelerator type. Pods select it, and its
	// value keys Accelerators.
	Accelerator string `json:"accelerator"`

	// Topology carries the topology of the slice a node belongs to. The value
	// pods select is the slice shape they need.
	Topology string `json:"topology"`

	// Slice carries the name of the slice a node belongs to. Selecting it
	// confines a pod to that slice's nodes.
	Slice string `json:"slice"`
}

// +kubebuilder:object:generate=false
// TPUSliceLabel is a node label key and value.
type TPUSliceLabel struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// +kubebuilder:object:generate=false
// TPUSliceAccelerator is how slices of one accelerator type are provisioned.
type TPUSliceAccelerator struct {
	// SliceType is the slice object's type for this accelerator.
	SliceType string `json:"sliceType"`

	// ChipsPerHost is how many chips one host of this accelerator has.
	ChipsPerHost int64 `json:"chipsPerHost"`

	// Topologies are the slice shapes that may be provisioned, each in
	// canonical form and a whole number of hosts.
	Topologies []string `json:"topologies"`
}

// Allows reports whether t is a provisionable topology. Configured topologies
// are canonical, so comparing canonical strings compares shapes.
func (a TPUSliceAccelerator) Allows(t tpuslice.Topology) bool {
	return !t.IsZero() && slices.Contains(a.Topologies, t.String())
}

// +kubebuilder:object:generate=false
// TPUSliceObject is the metadata and readiness contract of a slice object.
type TPUSliceObject struct {
	// OwnerKindLabel and OwnerNameLabel are label keys recording the kind and
	// name of the workload a slice serves. They trace a slice to its workload;
	// they do not decide which slices the controller may release.
	OwnerKindLabel string `json:"ownerKindLabel"`
	OwnerNameLabel string `json:"ownerNameLabel"`

	// Annotations are set on every slice the controller creates, such as the
	// annotation that hands a slice to its scheduler. Required; {} for none.
	Annotations map[string]string `json:"annotations"`

	// PodAnnotations, when set, are annotation keys copied from a workload's
	// pod template onto each slice the controller creates for it, such as the
	// annotation that sets a slice's scheduling priority. A key the template
	// does not set is left off the slice, and a slice keeps the values it was
	// created with. A key may not also be in Annotations.
	PodAnnotations []string `json:"podAnnotations,omitempty"`

	// ReadyStates are the ready-condition reasons of a slice whose chips can
	// be bound.
	ReadyStates []string `json:"readyStates"`

	// ReadyTimeout, when set, is how long a slice that has partitions may stay
	// out of a ready state before it is released and provisioned again, while
	// no pod holds it. Unset never times a slice out.
	ReadyTimeout string `json:"readyTimeout,omitempty"`
}

// ReadyTimeoutDuration is ReadyTimeout as a duration, zero when unset.
// Validate rejects a value that does not parse.
func (o TPUSliceObject) ReadyTimeoutDuration() time.Duration {
	d, _ := time.ParseDuration(o.ReadyTimeout)
	return d
}

// ShapeKeys are the node label keys a workload's slice shape is read from.
func (c *TPUSliceProvisioningConfig) ShapeKeys() tpuslice.Keys {
	return tpuslice.Keys{Accelerator: c.NodeLabels.Accelerator, Topology: c.NodeLabels.Topology}
}

// NewTPUSliceProvisioningConfig loads and validates the block once at startup.
// It returns nil, with a nil error, when the block is absent.
func NewTPUSliceProvisioningConfig(clientset kubernetes.Interface) (*TPUSliceProvisioningConfig, error) {
	configMap, err := getInferenceServiceConfigMap(clientset)
	if err != nil {
		return nil, err
	}
	return ParseTPUSliceProvisioningConfig(configMap)
}

// ParseTPUSliceProvisioningConfig decodes the block strictly: it must be a
// single JSON object with no unknown, duplicate, or trailing content, and
// every field must be present and usable. It returns nil, with a nil error,
// when the block is absent or blank.
func ParseTPUSliceProvisioningConfig(configMap *v1.ConfigMap) (*TPUSliceProvisioningConfig, error) {
	raw := configMap.Data[TPUSliceProvisioningConfigName]
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	cfg, err := decodeTPUSliceProvisioningConfig(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid %s config: %w", TPUSliceProvisioningConfigName, err)
	}
	return cfg, nil
}

func decodeTPUSliceProvisioningConfig(raw string) (*TPUSliceProvisioningConfig, error) {
	if !strings.HasPrefix(strings.TrimSpace(raw), "{") {
		return nil, errors.New("expected a JSON object")
	}
	cfg := &TPUSliceProvisioningConfig{}
	strictErrs, err := kjson.UnmarshalStrict([]byte(raw), cfg)
	if err != nil {
		return nil, fmt.Errorf("malformed JSON object: %w", err)
	}
	if len(strictErrs) > 0 {
		return nil, errors.Join(strictErrs...)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Validate reports every field that is missing or unusable.
func (c *TPUSliceProvisioningConfig) Validate() error {
	var errs []error
	errs = append(errs, qualifiedName("chipResource", string(c.ChipResource))...)

	nodeKeys := []struct{ path, key string }{
		{"nodeLabels.accelerator", c.NodeLabels.Accelerator},
		{"nodeLabels.topology", c.NodeLabels.Topology},
		{"nodeLabels.slice", c.NodeLabels.Slice},
		{"provisionOnly.key", c.ProvisionOnly.Key},
	}
	seen := map[string]string{}
	for _, k := range nodeKeys {
		errs = append(errs, qualifiedName(k.path, k.key)...)
		if first, dup := seen[k.key]; dup {
			errs = append(errs, fmt.Errorf("%s: %q is already used by %s", k.path, k.key, first))
		} else if k.key != "" {
			seen[k.key] = k.path
		}
	}
	errs = append(errs, labelValue("provisionOnly.value", c.ProvisionOnly.Value)...)

	if len(c.Accelerators) == 0 {
		errs = append(errs, errors.New("accelerators: at least one accelerator is required"))
	}
	for _, name := range slices.Sorted(maps.Keys(c.Accelerators)) {
		errs = append(errs, c.Accelerators[name].validate("accelerators["+name+"]", name)...)
	}

	errs = append(errs, qualifiedName("slice.ownerKindLabel", c.Slice.OwnerKindLabel)...)
	errs = append(errs, qualifiedName("slice.ownerNameLabel", c.Slice.OwnerNameLabel)...)
	if c.Slice.OwnerKindLabel != "" && c.Slice.OwnerKindLabel == c.Slice.OwnerNameLabel {
		errs = append(errs, fmt.Errorf("slice.ownerNameLabel: %q is already used by slice.ownerKindLabel", c.Slice.OwnerNameLabel))
	}
	if c.Slice.Annotations == nil {
		errs = append(errs, errors.New("slice.annotations: required; use {} for none"))
	}
	for _, key := range slices.Sorted(maps.Keys(c.Slice.Annotations)) {
		errs = append(errs, qualifiedName("slice.annotations", key)...)
	}
	podKeys := map[string]struct{}{}
	for i, key := range c.Slice.PodAnnotations {
		path := fmt.Sprintf("slice.podAnnotations[%d]", i)
		errs = append(errs, qualifiedName(path, key)...)
		if _, dup := podKeys[key]; dup {
			errs = append(errs, fmt.Errorf("%s: %q is listed twice", path, key))
		}
		podKeys[key] = struct{}{}
		if _, set := c.Slice.Annotations[key]; set {
			errs = append(errs, fmt.Errorf("%s: %q is already set by slice.annotations", path, key))
		}
	}
	if len(c.Slice.ReadyStates) == 0 {
		errs = append(errs, errors.New("slice.readyStates: at least one state is required"))
	}
	states := map[string]struct{}{}
	for i, s := range c.Slice.ReadyStates {
		path := fmt.Sprintf("slice.readyStates[%d]", i)
		if s == "" {
			errs = append(errs, fmt.Errorf("%s: must not be empty", path))
			continue
		}
		if _, dup := states[s]; dup {
			errs = append(errs, fmt.Errorf("%s: %q is listed twice", path, s))
		}
		states[s] = struct{}{}
	}
	if c.Slice.ReadyTimeout != "" {
		if d, err := time.ParseDuration(c.Slice.ReadyTimeout); err != nil || d <= 0 {
			errs = append(errs, fmt.Errorf("slice.readyTimeout: %q must be a positive duration", c.Slice.ReadyTimeout))
		}
	}
	return errors.Join(errs...)
}

func (a TPUSliceAccelerator) validate(path, name string) []error {
	var errs []error
	errs = append(errs, labelValue(path, name)...)
	if a.SliceType == "" {
		errs = append(errs, fmt.Errorf("%s.sliceType: required", path))
	}
	if a.ChipsPerHost <= 0 {
		errs = append(errs, fmt.Errorf("%s.chipsPerHost: must be positive, got %d", path, a.ChipsPerHost))
	}
	if len(a.Topologies) == 0 {
		errs = append(errs, fmt.Errorf("%s.topologies: at least one topology is required", path))
	}
	listed := map[string]struct{}{}
	for i, s := range a.Topologies {
		entry := fmt.Sprintf("%s.topologies[%d]", path, i)
		t, err := tpuslice.ParseTopology(s)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", entry, err))
			continue
		}
		if _, dup := listed[s]; dup {
			errs = append(errs, fmt.Errorf("%s: %q is listed twice", entry, s))
		}
		listed[s] = struct{}{}
		if a.ChipsPerHost > 0 {
			if _, err := t.Hosts(a.ChipsPerHost); err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", entry, err))
			}
		}
	}
	return errs
}

func qualifiedName(path, key string) []error {
	if key == "" {
		return []error{fmt.Errorf("%s: required", path)}
	}
	if problems := validation.IsQualifiedName(key); len(problems) > 0 {
		return []error{fmt.Errorf("%s: %q is not a qualified name: %s", path, key, strings.Join(problems, "; "))}
	}
	return nil
}

func labelValue(path, value string) []error {
	if value == "" {
		return []error{fmt.Errorf("%s: required", path)}
	}
	if problems := validation.IsValidLabelValue(value); len(problems) > 0 {
		return []error{fmt.Errorf("%s: %q is not a label value: %s", path, value, strings.Join(problems, "; "))}
	}
	return nil
}
