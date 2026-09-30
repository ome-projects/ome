package sliceprovision

import (
	"context"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/tpuslice"
)

const (
	keyAccelerator = "example.com/accelerator"
	keyTopology    = "example.com/topology"
	keySlice       = "example.com/slice"
	keyProvision   = "example.com/provisioning"
	valueProvision = "on-demand"
	chipResource   = "example.com/chip"
)

func testConfig() *controllerconfig.TPUSliceProvisioningConfig {
	return &controllerconfig.TPUSliceProvisioningConfig{
		ChipResource: chipResource,
		NodeLabels: controllerconfig.TPUSliceNodeLabels{
			Accelerator: keyAccelerator,
			Topology:    keyTopology,
			Slice:       keySlice,
		},
		ProvisionOnly: controllerconfig.TPUSliceLabel{Key: keyProvision, Value: valueProvision},
		Accelerators: map[string]controllerconfig.TPUSliceAccelerator{
			"tpu-a": {SliceType: "type-a", ChipsPerHost: 4, Topologies: []string{"2x2x1", "2x2x2"}},
		},
		Slice: controllerconfig.TPUSliceObject{
			OwnerKindLabel: "example.com/owner-kind",
			OwnerNameLabel: "example.com/owner-name",
			Annotations:    map[string]string{"example.com/managed-by": "scheduler"},
			ReadyStates:    []string{"ACTIVE", "ACTIVE_DEGRADED"},
		},
	}
}

func TestTestConfigIsValid(t *testing.T) {
	if err := testConfig().Validate(); err != nil {
		t.Fatalf("test config is invalid: %v", err)
	}
	if err := CheckConfig(testConfig()); err != nil {
		t.Fatalf("CheckConfig: %v", err)
	}
}

func node(name string, labels map[string]string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}}
}

func provisionOnlyNode(name, accelerator string) *corev1.Node {
	return node(name, map[string]string{keyAccelerator: accelerator, keyProvision: valueProvision})
}

func podSpec(accelerator, topology string, chips int64) *corev1.PodSpec {
	spec := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main"}}}
	if topology != "" {
		spec.NodeSelector = map[string]string{keyTopology: topology}
		if accelerator != "" {
			spec.NodeSelector[keyAccelerator] = accelerator
		}
	}
	if chips > 0 {
		spec.Containers[0].Resources.Limits = corev1.ResourceList{chipResource: *resource.NewQuantity(chips, resource.DecimalSI)}
	}
	return spec
}

func withSelector(spec *corev1.PodSpec, key, value string) *corev1.PodSpec {
	spec.NodeSelector[key] = value
	return spec
}

func repeat(spec *corev1.PodSpec, n int) []*corev1.PodSpec {
	out := make([]*corev1.PodSpec, n)
	for i := range out {
		out[i] = spec.DeepCopy()
	}
	return out
}

func mustTopology(t *testing.T, s string) tpuslice.Topology {
	t.Helper()
	topo, err := tpuslice.ParseTopology(s)
	if err != nil {
		t.Fatalf("ParseTopology(%q): %v", s, err)
	}
	return topo
}

func TestResolveNeeded(t *testing.T) {
	tests := []struct {
		name     string
		pods     []*corev1.PodSpec
		topology string
	}{
		{name: "single pod fills a single-host slice", pods: repeat(podSpec("tpu-a", "2x2x1", 4), 1), topology: "2x2x1"},
		{name: "a gang fills a two-host slice", pods: repeat(podSpec("tpu-a", "2x2x2", 4), 2), topology: "2x2x2"},
		{name: "pods sharing a host fill it", pods: repeat(podSpec("tpu-a", "2x2x1", 1), 4), topology: "2x2x1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodes := fake.NewClientBuilder().WithObjects(provisionOnlyNode("n1", "tpu-a")).Build()
			d, needed, err := Resolve(context.Background(), nodes, testConfig(), tt.pods)
			if err != nil || !needed {
				t.Fatalf("Resolve = needed %v, err %v; want needed", needed, err)
			}
			want := Demand{Shape: tpuslice.Shape{Accelerator: "tpu-a", Topology: mustTopology(t, tt.topology)}, SliceType: "type-a"}
			if d != want {
				t.Fatalf("Resolve demand = %+v, want %+v", d, want)
			}
		})
	}
}

func TestResolveNotNeeded(t *testing.T) {
	staticNode := node("static", map[string]string{keyAccelerator: "tpu-a", keyTopology: "2x2x1"})
	otherPool := provisionOnlyNode("other", "tpu-b")
	wrongValue := node("wrong-value", map[string]string{keyAccelerator: "tpu-a", keyProvision: "static"})
	tests := []struct {
		name  string
		cfg   *controllerconfig.TPUSliceProvisioningConfig
		nodes []client.Object
		pods  []*corev1.PodSpec
	}{
		{name: "no configuration", cfg: nil, nodes: []client.Object{provisionOnlyNode("n1", "tpu-a")}, pods: repeat(podSpec("tpu-a", "2x2x1", 4), 1)},
		{name: "pods select no topology", cfg: testConfig(), nodes: []client.Object{provisionOnlyNode("n1", "tpu-a")}, pods: repeat(podSpec("tpu-a", "", 4), 1)},
		{name: "no pods", cfg: testConfig(), nodes: []client.Object{provisionOnlyNode("n1", "tpu-a")}, pods: nil},
		{name: "only a static pool carries the accelerator", cfg: testConfig(), nodes: []client.Object{staticNode, otherPool, wrongValue}, pods: repeat(podSpec("tpu-a", "2x2x1", 4), 1)},
		// A static pool is left alone even when its pods could not fill a
		// provisioned slice.
		{name: "static pool with unprovisionable topology", cfg: testConfig(), nodes: []client.Object{staticNode}, pods: repeat(podSpec("tpu-a", "4x4x4", 4), 1)},
		{name: "static pool with an unconfigured accelerator", cfg: testConfig(), nodes: []client.Object{otherPool}, pods: repeat(podSpec("tpu-c", "2x2x1", 4), 1)},
		{name: "static pool pod selects a slice", cfg: testConfig(), nodes: []client.Object{staticNode}, pods: repeat(withSelector(podSpec("tpu-a", "2x2x1", 4), keySlice, "s1"), 1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodes := fake.NewClientBuilder().WithObjects(tt.nodes...).Build()
			d, needed, err := Resolve(context.Background(), nodes, tt.cfg, tt.pods)
			if err != nil || needed || d != (Demand{}) {
				t.Fatalf("Resolve = %+v, needed %v, err %v; want not needed", d, needed, err)
			}
		})
	}
}

func TestResolveRejects(t *testing.T) {
	tests := []struct {
		name    string
		pods    []*corev1.PodSpec
		wantErr string
	}{
		{name: "unconfigured accelerator", pods: repeat(podSpec("tpu-b", "2x2x1", 4), 1), wantErr: `accelerator "tpu-b"`},
		{name: "unprovisionable topology", pods: repeat(podSpec("tpu-a", "2x4x1", 4), 2), wantErr: "not provisionable"},
		{name: "too few chips", pods: repeat(podSpec("tpu-a", "2x2x1", 2), 1), wantErr: "do not fill topology"},
		{name: "too many pods", pods: repeat(podSpec("tpu-a", "2x2x1", 4), 2), wantErr: "do not fill topology"},
		{name: "pod requests no chips", pods: repeat(podSpec("tpu-a", "2x2x1", 0), 1), wantErr: "requests no chips"},
		{name: "pod spans hosts", pods: repeat(podSpec("tpu-a", "2x2x2", 8), 1), wantErr: "cannot span hosts"},
		{name: "pods select different topologies", pods: []*corev1.PodSpec{podSpec("tpu-a", "2x2x2", 4), podSpec("tpu-a", "2x2x1", 4)}, wantErr: "share one slice"},
		{name: "pods select different accelerators", pods: []*corev1.PodSpec{podSpec("tpu-a", "2x2x1", 4), podSpec("tpu-b", "2x2x1", 4)}, wantErr: "share one slice"},
		{name: "only some pods select a topology", pods: []*corev1.PodSpec{podSpec("tpu-a", "2x2x2", 4), podSpec("", "", 4)}, wantErr: "1 of 2 pods"},
		{name: "topology without accelerator", pods: repeat(podSpec("", "2x2x1", 4), 1), wantErr: "no accelerator"},
		{name: "non-canonical topology", pods: repeat(podSpec("tpu-a", "2x2x01", 4), 1), wantErr: "leading zeros"},
		{name: "pod selects a slice", pods: []*corev1.PodSpec{podSpec("tpu-a", "2x2x2", 4), withSelector(podSpec("tpu-a", "2x2x2", 4), keySlice, "s1")}, wantErr: `pod 1 selects slice "s1"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodes := fake.NewClientBuilder().WithObjects(provisionOnlyNode("n1", "tpu-a"), provisionOnlyNode("n2", "tpu-b")).Build()
			_, needed, err := Resolve(context.Background(), nodes, testConfig(), tt.pods)
			if !errors.Is(err, ErrInvalidDemand) {
				t.Fatalf("Resolve error = %v, want ErrInvalidDemand", err)
			}
			if needed {
				t.Fatalf("Resolve needed = true alongside error %v", err)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Resolve error = %q, want it to mention %q", err, tt.wantErr)
			}
		})
	}
}

func TestResolveNilPodSpec(t *testing.T) {
	nodes := fake.NewClientBuilder().WithObjects(provisionOnlyNode("n1", "tpu-a")).Build()
	_, needed, err := Resolve(context.Background(), nodes, testConfig(), []*corev1.PodSpec{podSpec("tpu-a", "2x2x1", 4), nil})
	if err == nil || needed {
		t.Fatalf("Resolve = needed %v, err %v; want an error for a nil spec", needed, err)
	}
}

func TestResolveNodeListError(t *testing.T) {
	boom := errors.New("boom")
	nodes := fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
		List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error { return boom },
	}).Build()
	_, needed, err := Resolve(context.Background(), nodes, testConfig(), repeat(podSpec("tpu-a", "2x2x1", 4), 1))
	if !errors.Is(err, boom) || errors.Is(err, ErrInvalidDemand) || needed {
		t.Fatalf("Resolve = needed %v, err %v; want the transient list error", needed, err)
	}
}
