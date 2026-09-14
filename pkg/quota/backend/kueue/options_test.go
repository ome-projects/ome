package kueue

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Options is deploy config, so every case here is a misconfiguration an operator
// can actually reach through the chart. Validate runs at startup precisely so
// these fail a launch rather than becoming a queue that admits nothing and
// reports the reason in no object.

func cover() map[corev1.ResourceName]resource.Quantity {
	return map[corev1.ResourceName]resource.Quantity{
		corev1.ResourceCPU:    resource.MustParse("1k"),
		corev1.ResourceMemory: resource.MustParse("1Ti"),
	}
}

// Materializing needs all three: a manager to own the fields, a cover so Kueue
// can assign a pod that asks for cpu, and somewhere to put the queues. Any one
// missing is off, not partly on.
func TestOptionsEnabled(t *testing.T) {
	tests := []struct {
		name string
		opts Options
		want bool
	}{
		{"fully configured", Options{
			FieldManager: "ome-quota", CoverResources: cover(),
			EnrolledNamespaces: []string{"ns-a"},
		}, true},
		{"nothing set", Options{}, false},
		{"no field manager", Options{
			CoverResources: cover(), EnrolledNamespaces: []string{"ns-a"},
		}, false},
		{"no cover", Options{
			FieldManager: "ome-quota", EnrolledNamespaces: []string{"ns-a"},
		}, false},
		{
			// A ClusterQueue selecting no namespace admits nothing and holds its
			// LocalQueues nowhere. Writing that is worse than writing nothing.
			name: "no enrolled namespace",
			opts: Options{FieldManager: "ome-quota", CoverResources: cover()},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.opts.Enabled(); got != tc.want {
				t.Errorf("Enabled() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestOptionsValidate(t *testing.T) {
	tests := []struct {
		name    string
		opts    Options
		wantErr string // "" means the configuration is acceptable
	}{
		{
			// Nothing set is how an operator says materialization is off, which
			// has to be distinguishable from getting it wrong.
			name: "wholly unset is not an error",
			opts: Options{},
		},
		{
			name: "fully configured",
			opts: Options{
				FieldManager: "ome-quota", CoverResources: cover(),
				EnrolledNamespaces: []string{"ns-a", "ns-b"},
			},
		},
		{
			name:    "cover without a field manager",
			opts:    Options{CoverResources: cover(), EnrolledNamespaces: []string{"ns-a"}},
			wantErr: "field manager is required",
		},
		{
			name:    "field manager without a cover",
			opts:    Options{FieldManager: "ome-quota", EnrolledNamespaces: []string{"ns-a"}},
			wantErr: "at least one cover resource",
		},
		{
			// Half-configured is the dangerous one: it reads as materialization
			// being on while every rendered queue would select nothing.
			name:    "field manager and cover without an enrolled namespace",
			opts:    Options{FieldManager: "ome-quota", CoverResources: cover()},
			wantErr: "at least one enrolled namespace",
		},
		{
			name: "a cover quantity of zero is not a ceiling",
			opts: Options{
				FieldManager:       "ome-quota",
				CoverResources:     map[corev1.ResourceName]resource.Quantity{corev1.ResourceCPU: resource.MustParse("0")},
				EnrolledNamespaces: []string{"ns-a"},
			},
			wantErr: "must be positive",
		},
		{
			// A namespace name is a DNS-1123 label. Rendering a LocalQueue into
			// one the apiserver will not accept fails every reconcile with a
			// message nobody reads.
			name: "an unusable namespace name",
			opts: Options{
				FieldManager: "ome-quota", CoverResources: cover(),
				EnrolledNamespaces: []string{"Not A Namespace"},
			},
			wantErr: "not a valid namespace name",
		},
		{
			name: "an empty namespace name",
			opts: Options{
				FieldManager: "ome-quota", CoverResources: cover(),
				EnrolledNamespaces: []string{""},
			},
			wantErr: "not a valid namespace name",
		},
		{
			// The chart deduplicates, so a duplicate reaching the binary means
			// something bypassed it. Rendering the same LocalQueue twice per
			// leaf is not harmful, but silently accepting it hides the bypass.
			name: "a namespace listed twice",
			opts: Options{
				FieldManager: "ome-quota", CoverResources: cover(),
				EnrolledNamespaces: []string{"ns-a", "ns-a"},
			},
			wantErr: "listed more than once",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.opts.Validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate() = nil, want an error containing %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("Validate() = %q, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}
