package acceleratorquota

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/quota/tree"
)

const tpuFlavor = "tpu7x"

var derive = CapacityOptions{
	Resources:         []string{"google.com/tpu"},
	HysteresisPercent: 10,
}

func qty(s string) resource.Quantity { return resource.MustParse(s) }

type nodeOpt func(*corev1.Node)

func withLabels(kv map[string]string) nodeOpt {
	return func(n *corev1.Node) { n.Labels = kv }
}

func withAllocatable(kv map[string]string) nodeOpt {
	return func(n *corev1.Node) {
		n.Status.Allocatable = corev1.ResourceList{}
		for k, v := range kv {
			n.Status.Allocatable[corev1.ResourceName(k)] = qty(v)
		}
	}
}

func unschedulable(n *corev1.Node) { n.Spec.Unschedulable = true }

func workerNode(name string, opts ...nodeOpt) *corev1.Node {
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
		},
	}
	withLabels(map[string]string{"accelerator": tpuFlavor})(n)
	for _, o := range opts {
		o(n)
	}
	return n
}

func resourceFlavor(name string, nodeLabels map[string]string) *kueuev1beta2.ResourceFlavor {
	return &kueuev1beta2.ResourceFlavor{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       kueuev1beta2.ResourceFlavorSpec{NodeLabels: nodeLabels},
	}
}

// observed is the comparable shape of what lands on the root, without the
// timestamp — which changes on every sample and would make every row a moving
// target.
type observed struct {
	Resource    string
	Flavor      string
	Allocatable string
	HighWater   string
}

func capacityOf(t *testing.T, c client.Client) []observed {
	t.Helper()
	var root v1beta1.AcceleratorQuota
	if err := c.Get(context.Background(), client.ObjectKey{Name: rootName}, &root); err != nil {
		t.Fatalf("get root: %v", err)
	}
	out := make([]observed, 0, len(root.Status.Capacity))
	for _, c := range root.Status.Capacity {
		out = append(out, observed{
			Resource: c.ResourceName, Flavor: c.ResourceFlavor,
			Allocatable: c.Allocatable.String(), HighWater: c.HighWaterMark.String(),
		})
	}
	return out
}

// capacityScheme adds the two kinds this half of the controller reads on top of
// the OME types: Nodes are the measurement, ResourceFlavors are what it is
// attributed to.
func capacityScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := testScheme(t)
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("corev1 scheme: %v", err)
	}
	if err := kueuev1beta2.AddToScheme(s); err != nil {
		t.Fatalf("kueue scheme: %v", err)
	}
	return s
}

func TestReconcileCapacity(t *testing.T) {
	flavors := []client.Object{resourceFlavor(tpuFlavor, map[string]string{"accelerator": tpuFlavor})}

	tests := []struct {
		name string
		// quotas defaults to a lone root when nil.
		quotas  []client.Object
		nodes   []client.Object
		flavors []client.Object
		opts    *CapacityOptions // nil uses derive
		want    []observed
	}{
		{
			name:    "sums the fleet onto the root",
			nodes:   []client.Object{workerNode("n1", withAllocatable(map[string]string{"google.com/tpu": "4"})), workerNode("n2", withAllocatable(map[string]string{"google.com/tpu": "4"}))},
			flavors: flavors,
			want:    []observed{{Resource: "google.com/tpu", Flavor: tpuFlavor, Allocatable: "8", HighWater: "8"}},
		},
		{
			// Parked hardware is out of allocatable but still in the rack, so
			// the mark counts it. Half a pool cordoned is far past any sane
			// band; a mark tracking allocatable would collapse here, which is
			// the maintenance window it exists to survive.
			name: "a cordoned node leaves allocatable behind but not the mark",
			nodes: []client.Object{
				workerNode("n1", withAllocatable(map[string]string{"google.com/tpu": "4"})),
				workerNode("n2", withAllocatable(map[string]string{"google.com/tpu": "4"}), unschedulable),
			},
			flavors: flavors,
			want:    []observed{{Resource: "google.com/tpu", Flavor: tpuFlavor, Allocatable: "4", HighWater: "8"}},
		},
		{
			// Absent config disables rather than guessing which resources are
			// accelerators, so the root stays untouched.
			name:    "derivation off records nothing",
			nodes:   []client.Object{workerNode("n1", withAllocatable(map[string]string{"google.com/tpu": "4"}))},
			flavors: flavors,
			opts:    &CapacityOptions{},
			want:    []observed{},
		},
		{
			// Quota without Kueue materializes nothing, so a cluster with no
			// flavors is not an error — but nor is it silently rounded to zero:
			// capacity.Sum reports it unattributed and nothing is recorded.
			name:  "no ResourceFlavors records nothing",
			nodes: []client.Object{workerNode("n1", withAllocatable(map[string]string{"google.com/tpu": "4"}))},
			want:  []observed{},
		},
		{
			name:    "a cluster with no accelerator nodes records zero",
			nodes:   []client.Object{workerNode("cpu-only", withAllocatable(map[string]string{"cpu": "64"}))},
			flavors: flavors,
			want:    []observed{{Resource: "google.com/tpu", Flavor: tpuFlavor, Allocatable: "0", HighWater: "0"}},
		},
		{
			name: "capacity is keyed by (resource, flavor)",
			nodes: []client.Object{
				workerNode("t", withAllocatable(map[string]string{"google.com/tpu": "4"})),
				workerNode("g", withLabels(map[string]string{"accelerator": "gb300"}),
					withAllocatable(map[string]string{"nvidia.com/gpu": "8"})),
			},
			flavors: append([]client.Object{}, flavors[0],
				resourceFlavor("gb300", map[string]string{"accelerator": "gb300"})),
			opts: &CapacityOptions{Resources: []string{"google.com/tpu", "nvidia.com/gpu"}, HysteresisPercent: 10},
			want: []observed{
				{Resource: "google.com/tpu", Flavor: "gb300", Allocatable: "0", HighWater: "0"},
				{Resource: "google.com/tpu", Flavor: tpuFlavor, Allocatable: "4", HighWater: "4"},
				{Resource: "nvidia.com/gpu", Flavor: "gb300", Allocatable: "8", HighWater: "8"},
				{Resource: "nvidia.com/gpu", Flavor: tpuFlavor, Allocatable: "0", HighWater: "0"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			quotas := tc.quotas
			if quotas == nil {
				quotas = []client.Object{cohort(rootName, "")}
			}
			objs := append(append(append([]client.Object{}, quotas...), tc.nodes...), tc.flavors...)

			s := capacityScheme(t)
			c := fake.NewClientBuilder().
				WithScheme(s).
				WithObjects(objs...).
				WithStatusSubresource(&v1beta1.AcceleratorQuota{}).
				Build()
			r := &Reconciler{
				Client: c, Scheme: s, Log: logf.Log.WithName("test"), APIReader: c,
				Options:  tree.Options{RootName: rootName, MaxDepth: 5},
				Capacity: derive,
			}
			if tc.opts != nil {
				r.Capacity = *tc.opts
			}

			if _, err := r.Reconcile(context.Background(), ctrl.Request{}); err != nil {
				t.Fatalf("Reconcile() = %v", err)
			}
			if diff := cmp.Diff(tc.want, capacityOf(t, c)); diff != "" {
				t.Errorf("root capacity mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// Capacity is a property of the cluster rather than of anything an admin wrote,
// so it has to be reportable before a tree exists — otherwise a management plane
// sizing a Proportional split has nothing to read from a member with no budget
// yet, and the bootstrap is circular.
func TestReconcileCreatesTheRoot(t *testing.T) {
	tests := []struct {
		name     string
		existing []client.Object
		opts     *CapacityOptions // nil uses derive
		wantRoot bool
	}{
		{
			name:     "an empty cluster gets a root",
			wantRoot: true,
		},
		{
			// Deleting it is not a way to switch derivation off; that is what
			// the config is for.
			name:     "a deleted root comes back",
			existing: []client.Object{workerNode("n1", withAllocatable(map[string]string{"google.com/tpu": "4"}))},
			wantRoot: true,
		},
		{
			// With derivation off there is nothing to record, so creating a node
			// the admin did not author would be this controller deciding the
			// tree should exist.
			name:     "derivation off creates nothing",
			existing: []client.Object{workerNode("n1", withAllocatable(map[string]string{"google.com/tpu": "4"}))},
			opts:     &CapacityOptions{},
			wantRoot: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := capacityScheme(t)
			c := fake.NewClientBuilder().
				WithScheme(s).
				WithObjects(tc.existing...).
				WithStatusSubresource(&v1beta1.AcceleratorQuota{}).
				Build()
			r := &Reconciler{
				Client: c, Scheme: s, Log: logf.Log.WithName("test"), APIReader: c,
				Options:  tree.Options{RootName: rootName, MaxDepth: 5},
				Capacity: derive,
			}
			if tc.opts != nil {
				r.Capacity = *tc.opts
			}

			if _, err := r.Reconcile(context.Background(), ctrl.Request{}); err != nil {
				t.Fatalf("Reconcile() = %v", err)
			}

			var root v1beta1.AcceleratorQuota
			err := c.Get(context.Background(), client.ObjectKey{Name: rootName}, &root)
			switch {
			case tc.wantRoot && err != nil:
				t.Fatalf("the reserved root was not created: %v", err)
			case !tc.wantRoot && err == nil:
				t.Fatalf("a root was created with derivation off: %+v", root.Spec)
			case !tc.wantRoot:
				return
			}
			if root.Spec.Role != v1beta1.AcceleratorQuotaRoleCohort {
				t.Errorf("role = %q, want Cohort", root.Spec.Role)
			}
			// Bare: the budget is the admin's fleet total to write, and
			// inventing one would be this controller authorizing work.
			if len(root.Spec.Budgets) != 0 {
				t.Errorf("the created root carries budgets: %+v", root.Spec.Budgets)
			}
			if root.Spec.ParentRef != nil {
				t.Errorf("the created root has a parentRef: %+v", root.Spec.ParentRef)
			}
		})
	}
}

// The reserved-root create is gated by this component's own fail-closed webhook,
// so at startup the apiserver refuses it until the replica is a ready endpoint
// of its own webhook Service. That is a wait, not a failure — surfacing it would
// answer a self-clearing condition with a stack trace per attempt — while
// anything else the apiserver refuses still has to reach an operator.
func TestReconcileWaitsOutItsOwnWebhook(t *testing.T) {
	tests := []struct {
		name             string
		createErr        error
		wantErr          bool
		wantRequeueAfter time.Duration
	}{
		{
			name: "an unreachable webhook is waited out quietly",
			createErr: apierrors.NewInternalError(errors.New(
				`failed calling webhook "acceleratorquota.ome-quota-manager.validator": ` +
					`failed to call webhook: Post "https://svc.ns.svc:443/validate": ` +
					`dial tcp 10.0.0.1:443: connect: connection refused`)),
			wantRequeueAfter: rootBootstrapRetry,
		},
		{
			name: "no endpoints yet is the same wait",
			createErr: apierrors.NewInternalError(errors.New(
				`failed calling webhook "acceleratorquota.ome-quota-manager.validator": ` +
					`failed to call webhook: Post "https://svc.ns.svc:443/validate": ` +
					`no endpoints available for service "svc"`)),
			wantRequeueAfter: rootBootstrapRetry,
		},
		{
			name:      "a denied create surfaces",
			createErr: apierrors.NewForbidden(quotaGR, rootName, errors.New("no create grant")),
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := capacityScheme(t)
			c := fake.NewClientBuilder().
				WithScheme(s).
				WithStatusSubresource(&v1beta1.AcceleratorQuota{}).
				WithInterceptorFuncs(interceptor.Funcs{
					Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
						return tc.createErr
					},
				}).
				Build()
			r := &Reconciler{
				Client: c, Scheme: s, Log: logf.Log.WithName("test"), APIReader: c,
				Options:  tree.Options{RootName: rootName, MaxDepth: 5},
				Capacity: derive,
			}

			got, err := r.Reconcile(context.Background(), ctrl.Request{})
			if tc.wantErr {
				if err == nil {
					t.Fatalf("Reconcile() = %+v, nil; want the refusal to surface", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("Reconcile() = %v, want the wait to be absorbed", err)
			}
			if got.RequeueAfter != tc.wantRequeueAfter {
				t.Errorf("RequeueAfter = %v, want %v", got.RequeueAfter, tc.wantRequeueAfter)
			}
		})
	}
}

// The mark is what budget checks read, so it must survive the dips that routine
// operations cause and follow only the ones that mean hardware went away.
func TestReconcileCapacityTracksTheHighWaterMark(t *testing.T) {
	flavors := []client.Object{resourceFlavor(tpuFlavor, map[string]string{"accelerator": tpuFlavor})}

	tests := []struct {
		name string
		// second is the whole node set on the second pass; the first pass is
		// always two nodes of 8, so the mark starts at 16.
		second []client.Object
		want   []observed
	}{
		{
			// One chip lost from sixteen is 6.25%, inside a 10% band: the sort
			// of dip a device plugin restart produces.
			name: "a dip inside the band holds the mark",
			second: []client.Object{
				workerNode("n1", withAllocatable(map[string]string{"google.com/tpu": "8"})),
				workerNode("n2", withAllocatable(map[string]string{"google.com/tpu": "7"})),
			},
			want: []observed{{Resource: "google.com/tpu", Flavor: tpuFlavor, Allocatable: "15", HighWater: "16"}},
		},
		{
			// However much of the fleet is cordoned, the hardware has not gone
			// anywhere, so the mark does not move.
			name: "cordoning half the fleet does not move the mark",
			second: []client.Object{
				workerNode("n1", withAllocatable(map[string]string{"google.com/tpu": "8"})),
				workerNode("n2", withAllocatable(map[string]string{"google.com/tpu": "8"}), unschedulable),
			},
			want: []observed{{Resource: "google.com/tpu", Flavor: tpuFlavor, Allocatable: "8", HighWater: "16"}},
		},
		{
			// Hardware physically leaving is the case the band judges, and half
			// the fleet is past it.
			name: "a node leaving the fleet moves the mark",
			second: []client.Object{
				workerNode("n1", withAllocatable(map[string]string{"google.com/tpu": "8"})),
			},
			want: []observed{{Resource: "google.com/tpu", Flavor: tpuFlavor, Allocatable: "8", HighWater: "8"}},
		},
		{
			// A device plugin restarting drops the resource from allocatable
			// without the node going anywhere. Nothing reports it as parked
			// either, so this is the one transient the mark cannot tell from a
			// decommission — recorded so the limit is known rather than found.
			name: "a device plugin dropping the resource is read as hardware leaving",
			second: []client.Object{
				workerNode("n1", withAllocatable(map[string]string{"google.com/tpu": "8"})),
				workerNode("n2", withAllocatable(map[string]string{"cpu": "64"})),
			},
			want: []observed{{Resource: "google.com/tpu", Flavor: tpuFlavor, Allocatable: "8", HighWater: "8"}},
		},
		{
			name: "growth moves the mark immediately",
			second: []client.Object{
				workerNode("n1", withAllocatable(map[string]string{"google.com/tpu": "16"})),
				workerNode("n2", withAllocatable(map[string]string{"google.com/tpu": "16"})),
			},
			want: []observed{{Resource: "google.com/tpu", Flavor: tpuFlavor, Allocatable: "32", HighWater: "32"}},
		},
		{
			// Hardware that stops reporting is exactly what the mark is for:
			// the entry stays, so the fleet reads as having lost capacity
			// rather than never having had it.
			name:   "a pool that disappears keeps its mark",
			second: nil,
			want:   []observed{{Resource: "google.com/tpu", Flavor: tpuFlavor, Allocatable: "0", HighWater: "16"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := capacityScheme(t)
			first := []client.Object{
				cohort(rootName, ""),
				workerNode("n1", withAllocatable(map[string]string{"google.com/tpu": "8"})),
				workerNode("n2", withAllocatable(map[string]string{"google.com/tpu": "8"})),
			}
			c := fake.NewClientBuilder().
				WithScheme(s).
				WithObjects(append(first, flavors...)...).
				WithStatusSubresource(&v1beta1.AcceleratorQuota{}).
				Build()
			r := &Reconciler{
				Client: c, Scheme: s, Log: logf.Log.WithName("test"), APIReader: c,
				Options:  tree.Options{RootName: rootName, MaxDepth: 5},
				Capacity: derive,
			}
			ctx := context.Background()
			if _, err := r.Reconcile(ctx, ctrl.Request{}); err != nil {
				t.Fatalf("first Reconcile() = %v", err)
			}

			for _, n := range []string{"n1", "n2"} {
				if err := c.Delete(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: n}}); err != nil {
					t.Fatalf("delete %s: %v", n, err)
				}
			}
			for _, n := range tc.second {
				if err := c.Create(ctx, n); err != nil {
					t.Fatalf("create: %v", err)
				}
			}

			if _, err := r.Reconcile(ctx, ctrl.Request{}); err != nil {
				t.Fatalf("second Reconcile() = %v", err)
			}
			if diff := cmp.Diff(tc.want, capacityOf(t, c)); diff != "" {
				t.Errorf("root capacity mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// The predicate is load-bearing rather than an optimisation: node status is
// among the highest-volume writes in a cluster, so waking on all of it would
// rebuild the tree continuously on a large fleet.
func TestNodeCapacityChanged(t *testing.T) {
	base := func() *corev1.Node {
		return workerNode("n", withAllocatable(map[string]string{"google.com/tpu": "4", "cpu": "64"}))
	}

	tests := []struct {
		name   string
		mutate func(*corev1.Node)
		want   bool
	}{
		{name: "an identical node is ignored", mutate: func(*corev1.Node) {}},
		{
			name:   "a heartbeat that moves nothing is ignored",
			mutate: func(n *corev1.Node) { n.Status.Conditions[0].LastHeartbeatTime = metav1.Now() },
		},
		{
			name:   "an unrelated resource changing is ignored",
			mutate: func(n *corev1.Node) { n.Status.Allocatable["cpu"] = qty("32") },
		},
		{
			name:   "cordoning wakes it",
			mutate: func(n *corev1.Node) { n.Spec.Unschedulable = true },
			want:   true,
		},
		{
			name:   "going NotReady wakes it",
			mutate: func(n *corev1.Node) { n.Status.Conditions[0].Status = corev1.ConditionFalse },
			want:   true,
		},
		{
			name:   "an accelerator count changing wakes it",
			mutate: func(n *corev1.Node) { n.Status.Allocatable["google.com/tpu"] = qty("2") },
			want:   true,
		},
		{
			name:   "an accelerator disappearing wakes it",
			mutate: func(n *corev1.Node) { delete(n.Status.Allocatable, "google.com/tpu") },
			want:   true,
		},
		{
			name:   "an accelerator appearing wakes it",
			mutate: func(n *corev1.Node) { n.Status.Allocatable["nvidia.com/gpu"] = qty("8") },
			want:   true,
		},
		{
			// Labels decide which flavor a node belongs to, so a relabel moves
			// capacity between pools without changing any quantity.
			name:   "a relabel wakes it",
			mutate: func(n *corev1.Node) { n.Labels["accelerator"] = "gb300" },
			want:   true,
		},
		{
			name:   "dropping a label wakes it",
			mutate: func(n *corev1.Node) { delete(n.Labels, "accelerator") },
			want:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			old, updated := base(), base()
			tc.mutate(updated)
			got := nodeCapacityChanged(old, updated, []string{"google.com/tpu", "nvidia.com/gpu"})
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("nodeCapacityChanged (-want +got):\n%s", diff)
			}
		})
	}
}
