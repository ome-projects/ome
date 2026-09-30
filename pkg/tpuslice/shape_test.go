package tpuslice

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func mustTopology(t *testing.T, s string) Topology {
	t.Helper()
	topo, err := ParseTopology(s)
	if err != nil {
		t.Fatalf("ParseTopology(%q): %v", s, err)
	}
	return topo
}

func TestParseTopology(t *testing.T) {
	tests := []struct {
		in       string
		wantDims []int64
		wantErr  string
	}{
		{in: "2x2x1", wantDims: []int64{2, 2, 1}},
		{in: "4x4x4", wantDims: []int64{4, 4, 4}},
		{in: "2x4", wantDims: []int64{2, 4}},
		{in: "16x16", wantDims: []int64{16, 16}},
		{in: "", wantErr: "want 2 or 3 dimensions"},
		{in: "4", wantErr: "want 2 or 3 dimensions"},
		{in: "2x2x2x2", wantErr: "want 2 or 3 dimensions"},
		{in: "2x0x1", wantErr: "not a positive integer"},
		{in: "02x2x1", wantErr: "not a positive integer"},
		{in: "+2x2x1", wantErr: "not a positive integer"},
		{in: "-2x2x1", wantErr: "not a positive integer"},
		{in: "2 x2x1", wantErr: "not a positive integer"},
		{in: "2X2X1", wantErr: "want 2 or 3 dimensions"},
		{in: "2x2x", wantErr: "not a positive integer"},
		{in: "99999999999x99999999999x99999999999", wantErr: "overflows"},
		{in: "99999999999999999999x1", wantErr: "overflows"},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := ParseTopology(tt.in)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("ParseTopology(%q) error = %v, want containing %q", tt.in, err, tt.wantErr)
				}
				if !got.IsZero() {
					t.Fatalf("ParseTopology(%q) = %v on error, want zero", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseTopology(%q): %v", tt.in, err)
			}
			if diff := cmp.Diff(tt.wantDims, got.Dims()); diff != "" {
				t.Fatalf("Dims() mismatch (-want +got):\n%s", diff)
			}
			if got.String() != tt.in {
				t.Fatalf("String() = %q, want round trip of %q", got.String(), tt.in)
			}
		})
	}
}

func TestTopologyComparable(t *testing.T) {
	a, b := mustTopology(t, "2x2x1"), mustTopology(t, "2x2x1")
	if a != b {
		t.Fatal("equal topologies compare unequal")
	}
	seen := map[Topology]bool{a: true}
	if !seen[b] {
		t.Fatal("equal topologies key different map entries")
	}
	if mustTopology(t, "2x2x1") == mustTopology(t, "2x2x2") {
		t.Fatal("different topologies compare equal")
	}
	if mustTopology(t, "2x2") == mustTopology(t, "2x2x1") {
		t.Fatal("2D and 3D topologies compare equal")
	}
}

func TestZeroTopology(t *testing.T) {
	var zero Topology
	if !zero.IsZero() || zero.Chips() != 0 || zero.String() != "" || len(zero.Dims()) != 0 {
		t.Fatalf("zero Topology = {chips %d, %q, %v}, want empty", zero.Chips(), zero.String(), zero.Dims())
	}
	if _, err := zero.Hosts(4); err == nil {
		t.Fatal("Hosts on the zero topology returned no error")
	}
}

func TestChipsAndHosts(t *testing.T) {
	// One 4x4x4 cube of 4-chip hosts and every carve it offers.
	tests := []struct {
		topology  string
		chips     int64
		hosts     int64
		perHost   int64
		wantError string
	}{
		{topology: "2x2x1", chips: 4, hosts: 1, perHost: 4},
		{topology: "2x2x2", chips: 8, hosts: 2, perHost: 4},
		{topology: "2x2x4", chips: 16, hosts: 4, perHost: 4},
		{topology: "2x4x4", chips: 32, hosts: 8, perHost: 4},
		{topology: "4x4x4", chips: 64, hosts: 16, perHost: 4},
		{topology: "8x8x8", chips: 512, hosts: 128, perHost: 4},
		{topology: "2x4", chips: 8, hosts: 1, perHost: 8},
		{topology: "1x1", chips: 1, hosts: 1, perHost: 1},
		{topology: "1x1", chips: 1, perHost: 4, wantError: "not a whole number"},
		{topology: "2x2x1", chips: 4, perHost: 8, wantError: "not a whole number"},
		{topology: "2x2x1", chips: 4, perHost: 0, wantError: "must be positive"},
		{topology: "2x2x1", chips: 4, perHost: -4, wantError: "must be positive"},
	}
	for _, tt := range tests {
		t.Run(tt.topology, func(t *testing.T) {
			topo := mustTopology(t, tt.topology)
			if got := topo.Chips(); got != tt.chips {
				t.Fatalf("Chips() = %d, want %d", got, tt.chips)
			}
			hosts, err := topo.Hosts(tt.perHost)
			if tt.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantError) {
					t.Fatalf("Hosts(%d) error = %v, want containing %q", tt.perHost, err, tt.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("Hosts(%d): %v", tt.perHost, err)
			}
			if hosts != tt.hosts {
				t.Fatalf("Hosts(%d) = %d, want %d", tt.perHost, hosts, tt.hosts)
			}
		})
	}
}

func TestTile(t *testing.T) {
	tests := []struct {
		name     string
		topology string
		perHost  int64
		podChips []int64
		wantErr  string
	}{
		{name: "one host one pod", topology: "2x2x1", perHost: 4, podChips: []int64{4}},
		{name: "one host four single-chip pods", topology: "2x2x1", perHost: 4, podChips: []int64{1, 1, 1, 1}},
		{name: "one host two pods", topology: "2x2x1", perHost: 4, podChips: []int64{2, 2}},
		{name: "leader and worker on two hosts", topology: "2x2x2", perHost: 4, podChips: []int64{4, 4}},
		{name: "full cube", topology: "4x4x4", perHost: 4, podChips: repeat(16, 4)},

		{name: "single-host pod on a two-host slice", topology: "2x2x2", perHost: 4, podChips: []int64{4}, wantErr: "do not fill topology 2x2x2"},
		{name: "two pods on a one-host slice", topology: "2x2x1", perHost: 4, podChips: []int64{4, 4}, wantErr: "do not fill topology 2x2x1"},
		{name: "partial host", topology: "2x2x1", perHost: 4, podChips: []int64{2}, wantErr: "do not fill"},
		{name: "pod spans hosts", topology: "2x2x2", perHost: 4, podChips: []int64{8}, wantErr: "cannot span hosts"},
		{name: "uneven host share", topology: "2x2x4", perHost: 4, podChips: []int64{3, 3, 3, 3, 3}, wantErr: "do not divide"},
		{name: "mixed chip counts", topology: "2x2x2", perHost: 4, podChips: []int64{4, 2, 2}, wantErr: "same count"},
		{name: "pod without chips", topology: "2x2x2", perHost: 4, podChips: []int64{4, 0}, wantErr: "requests no chips"},
		{name: "no pods", topology: "2x2x1", perHost: 4, wantErr: "no pod requests"},
		{name: "not whole hosts", topology: "1x1", perHost: 4, podChips: []int64{1}, wantErr: "not a whole number"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Tile(mustTopology(t, tt.topology), tt.perHost, tt.podChips)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Tile: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Tile error = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestTileRejectsZeroTopology(t *testing.T) {
	if err := Tile(Topology{}, 4, []int64{4}); err == nil {
		t.Fatal("Tile on the zero topology returned no error")
	}
}

func repeat(n int, v int64) []int64 {
	out := make([]int64, n)
	for i := range out {
		out[i] = v
	}
	return out
}

func TestFromNodeSelector(t *testing.T) {
	keys := Keys{Accelerator: "example.com/accelerator", Topology: "example.com/topology"}
	tests := []struct {
		name      string
		selector  map[string]string
		keys      Keys
		want      Shape
		wantFound bool
		wantErr   string
	}{
		{
			name:      "accelerator and topology",
			selector:  map[string]string{keys.Accelerator: "tpu-a", keys.Topology: "2x2x1", "pool": "p"},
			keys:      keys,
			want:      Shape{Accelerator: "tpu-a", Topology: mustTopology(t, "2x2x1")},
			wantFound: true,
		},
		{name: "no topology", selector: map[string]string{keys.Accelerator: "tpu-a"}, keys: keys},
		{name: "nil selector", keys: keys},
		{
			name:     "topology without accelerator",
			selector: map[string]string{keys.Topology: "2x2x1"},
			keys:     keys,
			wantErr:  "no accelerator",
		},
		{
			name:     "malformed topology",
			selector: map[string]string{keys.Accelerator: "tpu-a", keys.Topology: "2x2x"},
			keys:     keys,
			wantErr:  "example.com/topology",
		},
		{name: "unset keys", selector: map[string]string{"a": "b"}, wantErr: "must both be set"},
		{name: "unset topology key", selector: map[string]string{"a": "b"}, keys: Keys{Accelerator: "a"}, wantErr: "must both be set"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, found, err := FromNodeSelector(tt.selector, tt.keys)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("FromNodeSelector: %v", err)
			}
			if found != tt.wantFound || got != tt.want {
				t.Fatalf("FromNodeSelector = (%+v, %v), want (%+v, %v)", got, found, tt.want, tt.wantFound)
			}
		})
	}
}

func TestContainerChips(t *testing.T) {
	const tpu = corev1.ResourceName("example.com/tpu")
	container := func(limits, requests corev1.ResourceList) corev1.Container {
		return corev1.Container{Resources: corev1.ResourceRequirements{Limits: limits, Requests: requests}}
	}
	chips := func(n string) corev1.ResourceList { return corev1.ResourceList{tpu: resource.MustParse(n)} }
	tests := []struct {
		name       string
		containers []corev1.Container
		want       int64
	}{
		{name: "none"},
		{name: "limit", containers: []corev1.Container{container(chips("4"), nil)}, want: 4},
		{name: "request only", containers: []corev1.Container{container(nil, chips("2"))}, want: 2},
		{name: "limit wins over request", containers: []corev1.Container{container(chips("4"), chips("2"))}, want: 4},
		{name: "summed across containers", containers: []corev1.Container{container(chips("2"), nil), container(nil, chips("2"))}, want: 4},
		{
			name:       "other resources ignored",
			containers: []corev1.Container{container(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("8")}, nil)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ContainerChips(tt.containers, tpu); got != tt.want {
				t.Fatalf("ContainerChips = %d, want %d", got, tt.want)
			}
		})
	}
}
