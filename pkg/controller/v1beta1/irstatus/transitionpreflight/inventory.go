// Package transitionpreflight is the read-only operator check that runs
// before and after an InferenceReplica status representation transition. It
// reads every cluster of an operator-supplied inventory directly and
// uncached, counts stored representations, verifies the manager image, the
// omenativeStatus configuration, and the served schema, and reports go or
// no-go with the reason for every failure. It never writes.
package transitionpreflight

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"

	"sigs.k8s.io/yaml"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

// Inventory is the operator's attestation of one managed fleet: every
// cluster to check, the manager image the fleet must run, where the manager
// and its configuration live, and the omenativeStatus block every manager
// must have loaded. Every field is explicit; the tool supplies no defaults.
type Inventory struct {
	// ManagerImage is the exact image reference the manager container must
	// run in every cluster.
	ManagerImage string `json:"managerImage"`
	// OMENativeStatus is the block every cluster's inferenceservice-config
	// must carry, compared field by field.
	OMENativeStatus ExpectedStatusConfig `json:"omenativeStatus"`
	// Manager locates the manager Deployment and configuration in each
	// cluster.
	Manager ManagerLocation `json:"manager"`
	// PageSize is the InferenceReplica LIST page size.
	PageSize int64 `json:"pageSize"`
	// Clusters are the managed clusters, each read through its own
	// kubeconfig context.
	Clusters []Cluster `json:"clusters"`
}

// ExpectedStatusConfig mirrors the omenativeStatus configuration block.
type ExpectedStatusConfig struct {
	InstanceStatusEncoding irstatus.Encoding `json:"instanceStatusEncoding"`
	MaxDecodedInstances    *uint64           `json:"maxDecodedInstances,omitempty"`
}

// ObservedStatusConfig is the block a cluster's manager parsed, in the shape
// the inventory and the reports use.
func ObservedStatusConfig(parsed controllerconfig.OMENativeStatusConfig) ExpectedStatusConfig {
	return ExpectedStatusConfig{InstanceStatusEncoding: parsed.InstanceStatusEncoding, MaxDecodedInstances: parsed.MaxDecodedInstances}
}

// Bound is the ColumnarV2 row bound to decode with; zero when none is set.
func (c ExpectedStatusConfig) Bound() uint64 {
	if c.MaxDecodedInstances == nil {
		return 0
	}
	return *c.MaxDecodedInstances
}

// BoundString is the bound as the reports print it: the number, or whenUnset
// when none is set.
func (c ExpectedStatusConfig) BoundString(whenUnset string) string {
	if c.MaxDecodedInstances == nil {
		return whenUnset
	}
	return strconv.FormatUint(*c.MaxDecodedInstances, 10)
}

// Matches reports whether observed runs the same encoding with the same
// bound; a set bound and an unset one never match.
func (c ExpectedStatusConfig) Matches(observed ExpectedStatusConfig) bool {
	return c.InstanceStatusEncoding == observed.InstanceStatusEncoding &&
		c.Bound() == observed.Bound() &&
		(c.MaxDecodedInstances == nil) == (observed.MaxDecodedInstances == nil)
}

// String renders the block the way the report prints it.
func (c ExpectedStatusConfig) String() string {
	if c.MaxDecodedInstances == nil {
		return fmt.Sprintf("instanceStatusEncoding=%s maxDecodedInstances=<unset>", c.InstanceStatusEncoding)
	}
	return fmt.Sprintf("instanceStatusEncoding=%s maxDecodedInstances=%d", c.InstanceStatusEncoding, *c.MaxDecodedInstances)
}

// ManagerLocation names the manager Deployment, its manager container, and
// the inferenceservice-config ConfigMap, all in one namespace.
type ManagerLocation struct {
	Namespace  string `json:"namespace"`
	Deployment string `json:"deployment"`
	Container  string `json:"container"`
	ConfigMap  string `json:"configMap"`
}

// Cluster is one managed cluster reached through an explicit kubeconfig
// context; the ambient kubeconfig and its current context are never used.
type Cluster struct {
	Name       string `json:"name"`
	Kubeconfig string `json:"kubeconfig"`
	Context    string `json:"context"`
}

// LoadInventory reads and validates an inventory file and returns it with
// the hex SHA-256 digest of the file bytes, which the report carries so the
// evidence names the exact attestation it was produced from.
func LoadInventory(path string) (*Inventory, string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, "", fmt.Errorf("read inventory: %w", err)
	}
	inventory, err := ParseInventory(data)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(data)
	return inventory, hex.EncodeToString(sum[:]), nil
}

// ParseInventory decodes YAML strictly (unknown fields are errors) and
// validates the result.
func ParseInventory(data []byte) (*Inventory, error) {
	inventory := &Inventory{}
	if err := yaml.UnmarshalStrict(data, inventory); err != nil {
		return nil, fmt.Errorf("parse inventory: %w", err)
	}
	if err := inventory.Validate(); err != nil {
		return nil, fmt.Errorf("invalid inventory: %w", err)
	}
	return inventory, nil
}

// Validate applies the same rules to the expected configuration that the
// manager applies to its loaded configuration, and requires every locator.
// The problems are named one per line in one order on every run: the image,
// the status block, the locators, the page size, then each cluster in turn.
func (inv *Inventory) Validate() error {
	var problems []error
	if inv.ManagerImage == "" {
		problems = append(problems, errors.New("managerImage is required"))
	}
	problems = append(problems, inv.OMENativeStatus.problems()...)
	problems = append(problems, inv.Manager.missingLocators()...)
	if inv.PageSize <= 0 {
		problems = append(problems, errors.New("pageSize must be a positive integer"))
	}
	problems = append(problems, clusterProblems(inv.Clusters)...)
	return errors.Join(problems...)
}

// problems names what keeps the block from being one the manager loads: an
// encoding it does not serve, a bound of zero, or ColumnarV2 without a bound.
func (c ExpectedStatusConfig) problems() []error {
	var problems []error
	switch c.InstanceStatusEncoding {
	case irstatus.EncodingDenseV1, irstatus.EncodingColumnarV2:
	case "":
		problems = append(problems, fmt.Errorf("omenativeStatus.instanceStatusEncoding is required (%s or %s)", irstatus.EncodingDenseV1, irstatus.EncodingColumnarV2))
	default:
		problems = append(problems, fmt.Errorf("omenativeStatus.instanceStatusEncoding %q is not supported; use %s or %s", c.InstanceStatusEncoding, irstatus.EncodingDenseV1, irstatus.EncodingColumnarV2))
	}
	if c.MaxDecodedInstances != nil && *c.MaxDecodedInstances == 0 {
		problems = append(problems, errors.New("omenativeStatus.maxDecodedInstances must be a positive integer"))
	}
	if c.InstanceStatusEncoding == irstatus.EncodingColumnarV2 && c.MaxDecodedInstances == nil {
		problems = append(problems, fmt.Errorf("omenativeStatus.maxDecodedInstances is required when instanceStatusEncoding is %s", irstatus.EncodingColumnarV2))
	}
	return problems
}

// missingLocators names the locators left blank. A list, not a map: they are
// named in this order on every run, so the same inventory always produces
// the same message.
func (m ManagerLocation) missingLocators() []error {
	var problems []error
	for _, locator := range []struct{ field, value string }{
		{"manager.namespace", m.Namespace},
		{"manager.deployment", m.Deployment},
		{"manager.container", m.Container},
		{"manager.configMap", m.ConfigMap},
	} {
		if locator.value == "" {
			problems = append(problems, fmt.Errorf("%s is required", locator.field))
		}
	}
	return problems
}

// clusterProblems requires at least one cluster and names what each cluster
// lacks, in list order.
func clusterProblems(clusters []Cluster) []error {
	var problems []error
	if len(clusters) == 0 {
		problems = append(problems, errors.New("clusters must list at least one cluster"))
	}
	listed := map[string]struct{}{}
	for i, cluster := range clusters {
		problems = append(problems, cluster.problems(i, listed)...)
		listed[cluster.Name] = struct{}{}
	}
	return problems
}

// problems names what the cluster at index i lacks: a name no earlier
// cluster carries, a kubeconfig and a context. listed holds the names of the
// clusters before it.
func (c Cluster) problems(i int, listed map[string]struct{}) []error {
	var problems []error
	if c.Name == "" {
		problems = append(problems, fmt.Errorf("clusters[%d].name is required", i))
	} else if _, dup := listed[c.Name]; dup {
		problems = append(problems, fmt.Errorf("clusters[%d].name %q is listed twice", i, c.Name))
	}
	if c.Kubeconfig == "" {
		problems = append(problems, fmt.Errorf("clusters[%d].kubeconfig is required", i))
	}
	if c.Context == "" {
		problems = append(problems, fmt.Errorf("clusters[%d].context is required", i))
	}
	return problems
}
