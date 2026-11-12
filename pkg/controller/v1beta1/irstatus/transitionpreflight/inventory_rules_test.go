package transitionpreflight

import (
	"strings"
	"testing"

	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

// completeInventory is an inventory every rule accepts: ColumnarV2 with a
// bound, every locator, a page size and two clusters. edit breaks one rule.
func completeInventory(edit func(*Inventory)) *Inventory {
	inventory := &Inventory{
		ManagerImage:    testImage,
		OMENativeStatus: ExpectedStatusConfig{InstanceStatusEncoding: irstatus.EncodingColumnarV2, MaxDecodedInstances: ptr.To[uint64](5000)},
		Manager:         ManagerLocation{Namespace: "ome", Deployment: "ome-controller-manager", Container: "manager", ConfigMap: "inferenceservice-config"},
		PageSize:        500,
		Clusters: []Cluster{
			{Name: "east", Kubeconfig: "/etc/ome/east.kubeconfig", Context: "east"},
			{Name: "west", Kubeconfig: "/etc/ome/west.kubeconfig", Context: "west"},
		},
	}
	if edit != nil {
		edit(inventory)
	}
	return inventory
}

// TestInventoryValidateRefusalsNameTheFieldAndTheRule holds the whole text
// of the refusal each rule gives when it is the one rule an inventory breaks.
func TestInventoryValidateRefusalsNameTheFieldAndTheRule(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*Inventory)
		want string
	}{
		{name: "no manager image", edit: func(i *Inventory) { i.ManagerImage = "" }, want: "managerImage is required"},
		{name: "no encoding", edit: func(i *Inventory) { i.OMENativeStatus.InstanceStatusEncoding = "" },
			want: "omenativeStatus.instanceStatusEncoding is required (DenseV1 or ColumnarV2)"},
		{name: "an encoding the manager does not serve", edit: func(i *Inventory) { i.OMENativeStatus.InstanceStatusEncoding = "Compressed" },
			want: `omenativeStatus.instanceStatusEncoding "Compressed" is not supported; use DenseV1 or ColumnarV2`},
		{name: "a bound of zero", edit: func(i *Inventory) { i.OMENativeStatus.MaxDecodedInstances = ptr.To[uint64](0) },
			want: "omenativeStatus.maxDecodedInstances must be a positive integer"},
		{name: "ColumnarV2 without a bound", edit: func(i *Inventory) { i.OMENativeStatus.MaxDecodedInstances = nil },
			want: "omenativeStatus.maxDecodedInstances is required when instanceStatusEncoding is ColumnarV2"},
		{name: "no manager namespace", edit: func(i *Inventory) { i.Manager.Namespace = "" }, want: "manager.namespace is required"},
		{name: "no manager deployment", edit: func(i *Inventory) { i.Manager.Deployment = "" }, want: "manager.deployment is required"},
		{name: "no manager container", edit: func(i *Inventory) { i.Manager.Container = "" }, want: "manager.container is required"},
		{name: "no configuration map", edit: func(i *Inventory) { i.Manager.ConfigMap = "" }, want: "manager.configMap is required"},
		{name: "a page size of zero", edit: func(i *Inventory) { i.PageSize = 0 }, want: "pageSize must be a positive integer"},
		{name: "a negative page size", edit: func(i *Inventory) { i.PageSize = -1 }, want: "pageSize must be a positive integer"},
		{name: "no clusters", edit: func(i *Inventory) { i.Clusters = nil }, want: "clusters must list at least one cluster"},
		{name: "a nameless second cluster", edit: func(i *Inventory) { i.Clusters[1].Name = "" }, want: "clusters[1].name is required"},
		{name: "a name listed twice", edit: func(i *Inventory) { i.Clusters[1].Name = "east" }, want: `clusters[1].name "east" is listed twice`},
		{name: "a cluster without a kubeconfig", edit: func(i *Inventory) { i.Clusters[1].Kubeconfig = "" }, want: "clusters[1].kubeconfig is required"},
		{name: "a cluster without a context", edit: func(i *Inventory) { i.Clusters[0].Context = "" }, want: "clusters[0].context is required"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := completeInventory(tt.edit).Validate()
			if err == nil || err.Error() != tt.want {
				t.Fatalf("Validate() = %v\nwant %s", err, tt.want)
			}
		})
	}
}

// TestInventoryValidateNamesEveryProblemInOneOrder holds that an inventory
// with several faults names them all, one per line, in one order on every
// run: the image, the status block, the locators, the page size, then each
// cluster in turn.
func TestInventoryValidateNamesEveryProblemInOneOrder(t *testing.T) {
	for _, tt := range []struct {
		name      string
		inventory *Inventory
		want      []string
	}{
		{name: "an empty inventory", inventory: &Inventory{}, want: []string{
			"managerImage is required",
			"omenativeStatus.instanceStatusEncoding is required (DenseV1 or ColumnarV2)",
			"manager.namespace is required",
			"manager.deployment is required",
			"manager.container is required",
			"manager.configMap is required",
			"pageSize must be a positive integer",
			"clusters must list at least one cluster",
		}},
		{name: "no encoding and a bound of zero", inventory: completeInventory(func(i *Inventory) {
			i.OMENativeStatus = ExpectedStatusConfig{MaxDecodedInstances: ptr.To[uint64](0)}
		}), want: []string{
			"omenativeStatus.instanceStatusEncoding is required (DenseV1 or ColumnarV2)",
			"omenativeStatus.maxDecodedInstances must be a positive integer",
		}},
		{name: "an unsupported encoding without a bound", inventory: completeInventory(func(i *Inventory) {
			i.OMENativeStatus = ExpectedStatusConfig{InstanceStatusEncoding: "Compressed"}
		}), want: []string{
			`omenativeStatus.instanceStatusEncoding "Compressed" is not supported; use DenseV1 or ColumnarV2`,
		}},
		{name: "every cluster rule at once", inventory: completeInventory(func(i *Inventory) {
			i.Clusters = []Cluster{{}, {Name: "east", Kubeconfig: "/etc/ome/east.kubeconfig", Context: "east"}, {Name: "east"}}
		}), want: []string{
			"clusters[0].name is required",
			"clusters[0].kubeconfig is required",
			"clusters[0].context is required",
			`clusters[2].name "east" is listed twice`,
			"clusters[2].kubeconfig is required",
			"clusters[2].context is required",
		}},
		{name: "two nameless clusters are each required, not listed twice", inventory: completeInventory(func(i *Inventory) {
			i.Clusters[0].Name, i.Clusters[1].Name = "", ""
		}), want: []string{
			"clusters[0].name is required",
			"clusters[1].name is required",
		}},
		{name: "a third copy of a name is listed twice again", inventory: completeInventory(func(i *Inventory) {
			i.Clusters = append(i.Clusters, i.Clusters[0])
			i.Clusters[1].Name = "east"
		}), want: []string{
			`clusters[1].name "east" is listed twice`,
			`clusters[2].name "east" is listed twice`,
		}},
		{name: "the page size before the clusters", inventory: completeInventory(func(i *Inventory) {
			i.PageSize, i.Clusters = 0, nil
		}), want: []string{
			"pageSize must be a positive integer",
			"clusters must list at least one cluster",
		}},
		{name: "the locators before the page size", inventory: completeInventory(func(i *Inventory) {
			i.Manager.ConfigMap, i.PageSize = "", 0
		}), want: []string{
			"manager.configMap is required",
			"pageSize must be a positive integer",
		}},
		{name: "the status block before the locators", inventory: completeInventory(func(i *Inventory) {
			i.OMENativeStatus.MaxDecodedInstances, i.Manager.Namespace = nil, ""
		}), want: []string{
			"omenativeStatus.maxDecodedInstances is required when instanceStatusEncoding is ColumnarV2",
			"manager.namespace is required",
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.inventory.Validate()
			if err == nil {
				t.Fatal("Validate() = nil, want every problem named")
			}
			if got, want := err.Error(), strings.Join(tt.want, "\n"); got != want {
				t.Fatalf("Validate() =\n%s\nwant\n%s", got, want)
			}
		})
	}
}

// TestInventoryValidateAcceptsACompleteInventory holds the inventories that
// pass: each encoding with its bound, DenseV1 without one, one cluster, the
// smallest page size.
func TestInventoryValidateAcceptsACompleteInventory(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*Inventory)
	}{
		{name: "ColumnarV2 with a bound"},
		{name: "DenseV1 with a bound", edit: func(i *Inventory) { i.OMENativeStatus.InstanceStatusEncoding = irstatus.EncodingDenseV1 }},
		{name: "DenseV1 without a bound", edit: func(i *Inventory) {
			i.OMENativeStatus = ExpectedStatusConfig{InstanceStatusEncoding: irstatus.EncodingDenseV1}
		}},
		{name: "one cluster", edit: func(i *Inventory) { i.Clusters = i.Clusters[:1] }},
		{name: "a page size of one", edit: func(i *Inventory) { i.PageSize = 1 }},
		{name: "a bound of one", edit: func(i *Inventory) { i.OMENativeStatus.MaxDecodedInstances = ptr.To[uint64](1) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := completeInventory(tt.edit).Validate(); err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
		})
	}
}
