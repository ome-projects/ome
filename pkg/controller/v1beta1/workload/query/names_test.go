package query

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/validation"

	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestPerRevisionServiceName_Bounded covers the DNS1035 63-char guard on
// per-revision Service names. Unbounded, the headless variant of a long
// isvc name reaches 66 chars, which the apiserver rejects — hot-looping
// OMENative coordination.
func TestPerRevisionServiceName_Bounded(t *testing.T) {
	const max = validation.DNS1035LabelMaxLength // 63

	// A representative long isvc: routing name fits (57), headless overflows (66).
	longIsvc := "example-tenant-t5-shadow-model-0b7d9k"
	routing := PerRevisionServiceName(longIsvc, workload.ComponentEngine, "91b28c47")
	headless := PerRevisionHeadlessServiceName(longIsvc, workload.ComponentEngine, "91b28c47")

	if len(routing) > max {
		t.Errorf("routing name %q is %d chars, want <= %d", routing, len(routing), max)
	}
	if len(headless) > max {
		t.Errorf("headless name %q is %d chars, want <= %d", headless, len(headless), max)
	}
	// Routing fits as-is (57) so it must be left unchanged — renaming it
	// would orphan the live Service already on the cluster.
	if want := "example-tenant-t5-shadow-model-0b7d9k-engine-rev-91b28c47"; routing != want {
		t.Errorf("routing name = %q, want unchanged %q", routing, want)
	}
	// Routing and headless must stay distinct Services.
	if routing == headless {
		t.Errorf("routing and headless names collide: %q", routing)
	}
	// The shared truncation helper keeps the name's tail, so the bounded
	// headless name still ends in the `-headless` marker.
	if !strings.HasSuffix(headless, "-headless") {
		t.Errorf("bounded headless name %q lost the -headless suffix", headless)
	}
}

// TestBoundedServiceName covers the helper's contract directly.
func TestBoundedServiceName(t *testing.T) {
	const max = validation.DNS1035LabelMaxLength

	short := "short-engine-rev-deadbeef"
	if got := boundedServiceName(short); got != short {
		t.Errorf("short name changed: got %q, want %q", got, short)
	}

	// Exactly at the limit passes through unchanged.
	atLimit := strings.Repeat("a", max)
	if got := boundedServiceName(atLimit); got != atLimit {
		t.Errorf("at-limit name changed: got %d chars, want %d unchanged", len(got), max)
	}

	// Over the limit is truncated to <= max and stays deterministic.
	long := strings.Repeat("a", 80) + "-engine-rev-deadbeef-headless"
	b1 := boundedServiceName(long)
	b2 := boundedServiceName(long)
	if len(b1) > max {
		t.Errorf("bounded name %d chars, want <= %d", len(b1), max)
	}
	if b1 != b2 {
		t.Errorf("boundedServiceName not deterministic: %q != %q", b1, b2)
	}
	// Distinct inputs -> distinct outputs (hash suffix disambiguates).
	other := boundedServiceName(strings.Repeat("b", 80) + "-engine-rev-deadbeef-headless")
	if b1 == other {
		t.Errorf("distinct long inputs produced identical bounded names: %q", b1)
	}
	// No trailing dash left after truncation.
	if strings.HasSuffix(b1, "-") {
		t.Errorf("bounded name %q ends in a dash", b1)
	}
}

// TestHeadlessServiceName_Bounded confirms the per-Component headless
// name is guarded for the same overflow class.
func TestHeadlessServiceName_Bounded(t *testing.T) {
	const max = validation.DNS1035LabelMaxLength
	name := HeadlessServiceName(strings.Repeat("x", 60), workload.ComponentDecoder)
	if len(name) > max {
		t.Errorf("headless name %d chars, want <= %d", len(name), max)
	}
}

// TestPodGroupName_Bounded covers the label-value 63-char guard on the
// PodGroup name: the same string is stamped on every member pod as the
// LabelPodGroup value, so an overflowing name makes the apiserver reject
// the pod writes and the reconciler hot-loops.
func TestPodGroupName_Bounded(t *testing.T) {
	const max = validation.LabelValueMaxLength // 63

	// Short names pass through unchanged and keep the pivotable
	// <isvc>-<component>-<idx> shape.
	if got, want := PodGroupName("llama", workload.ComponentEngine, 3), "llama-engine-3"; got != want {
		t.Errorf("short PodGroup name = %q, want %q", got, want)
	}

	longIsvc := strings.Repeat("x", 70)
	b1 := PodGroupName(longIsvc, workload.ComponentEngine, 12)
	b2 := PodGroupName(longIsvc, workload.ComponentEngine, 12)
	if len(b1) > max {
		t.Errorf("bounded PodGroup name %d chars, want <= %d", len(b1), max)
	}
	if b1 != b2 {
		t.Errorf("PodGroupName not deterministic: %q != %q", b1, b2)
	}
	if errs := validation.IsValidLabelValue(b1); len(errs) > 0 {
		t.Errorf("bounded PodGroup name %q is not a valid label value: %v", b1, errs)
	}
	// Distinct instances of the same long owner stay distinct (the kept
	// tail includes the index).
	if other := PodGroupName(longIsvc, workload.ComponentEngine, 13); other == b1 {
		t.Errorf("distinct instance indices produced identical bounded names: %q", b1)
	}
}

func TestPodName(t *testing.T) {
	cases := []struct {
		name      string
		isvc      string
		component workload.ComponentType
		idx       int32
		runner    string
		ordinal   int32
		want      string
	}{
		{
			name:      "single-pod default",
			isvc:      "llama",
			component: workload.ComponentEngine,
			idx:       0,
			runner:    "default",
			ordinal:   0,
			want:      "llama-engine-0-default-0",
		},
		{
			name:      "multi-pod leader",
			isvc:      "llama-70b",
			component: workload.ComponentEngine,
			idx:       0,
			runner:    "leader",
			ordinal:   0,
			want:      "llama-70b-engine-0-leader-0",
		},
		{
			name:      "multi-pod worker ordinal 2",
			isvc:      "llama-70b",
			component: workload.ComponentEngine,
			idx:       1,
			runner:    "worker",
			ordinal:   2,
			want:      "llama-70b-engine-1-worker-2",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := PodName(tc.isvc, tc.component, tc.idx, tc.runner, tc.ordinal); got != tc.want {
				t.Errorf("PodName: got %q want %q", got, tc.want)
			}
		})
	}
}

// TestPodGroupName_MatchesInstanceSubdomain pins that the PodGroup name
// is the same shape as the per-Instance DNS subdomain — `<isvc>-<comp>-<idx>`.
// Operators that find a stuck PodGroup can directly run
// `kubectl get pods -l <pod-group-label>=<name>` to find its members,
// and operators that find an unhealthy pod can derive the owning
// PodGroup from the pod's name prefix without consulting status.
func TestPodGroupName_MatchesInstanceSubdomain(t *testing.T) {
	cases := []struct {
		name      string
		isvc      string
		component workload.ComponentType
		idx       int32
		want      string
	}{
		{"engine 0", "llama", workload.ComponentEngine, 0, "llama-engine-0"},
		{"decoder 1", "llama-70b", workload.ComponentDecoder, 1, "llama-70b-decoder-1"},
		{"engine high-index", "llama-70b-instruct", workload.ComponentEngine, 12, "llama-70b-instruct-engine-12"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := PodGroupName(tc.isvc, tc.component, tc.idx)
			if got != tc.want {
				t.Errorf("PodGroupName: got %q want %q", got, tc.want)
			}
			// Equivalence pin: same shape as InstanceSubdomain so the
			// operator's mental model is one prefix, not two.
			if sub := InstanceSubdomain(tc.isvc, tc.component, tc.idx); sub != got {
				t.Errorf("PodGroupName diverges from InstanceSubdomain: pg=%q sub=%q", got, sub)
			}
		})
	}
}

func TestHeadlessServiceName(t *testing.T) {
	got := HeadlessServiceName("llama-70b", workload.ComponentEngine)
	if got != "llama-70b-engine-headless" {
		t.Errorf("HeadlessServiceName: got %q want llama-70b-engine-headless", got)
	}
}

func TestStableServiceName(t *testing.T) {
	got := StableServiceName("llama-70b", workload.ComponentDecoder)
	if got != "llama-70b-decoder" {
		t.Errorf("StableServiceName: got %q want llama-70b-decoder", got)
	}
}
