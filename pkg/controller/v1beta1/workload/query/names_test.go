package query

import (
	"fmt"
	"math"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"

	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestPodHostname covers the pod hostname that carries the recreation
// count: the plain <pod-name>-i<count> form within the DNS label limit, the
// bounded {hash}-{suffix} form beyond it, and distinct hostnames per
// rebuild in both forms, which keeps a rebuilt Instance's peer DNS off the
// pods of the previous build.
func TestPodHostname(t *testing.T) {
	const max = validation.DNS1123LabelMaxLength // 63

	short1 := PodHostname("example-engine-0-leader-0", 1)
	if want := "example-engine-0-leader-0-i1"; short1 != want {
		t.Errorf("short name: got %q want %q", short1, want)
	}
	if short2 := PodHostname("example-engine-0-leader-0", 2); short2 == short1 {
		t.Errorf("short names of two rebuilds collide at %q", short1)
	}

	longPod := strings.Repeat("a", 49) + "-engine-0-leader-0" // 67 chars, over the limit
	long1 := PodHostname(longPod, 1)
	if len(long1) != max {
		t.Errorf("long name: got %d chars %q, want exactly %d", len(long1), long1, max)
	}
	if errs := validation.IsDNS1123Label(long1); len(errs) > 0 {
		t.Errorf("long name %q is not a DNS label: %v", long1, errs)
	}
	if long2 := PodHostname(longPod, 2); long2 == long1 {
		t.Errorf("bounded names of two rebuilds collide at %q", long1)
	}
}

// TestReadPodSetHostnames covers the reading of what a pod set's live pods
// answer to, through the hostname the reading gives a pod made beside them
// under a row at recreation count 7. A live leader or worker that answers
// to its pod name gives the pod name. One whose hostname ends in a count
// gives that count, whatever count its own label names, in the plain and
// in the bounded form. A terminal pod, a single-pod Instance's pod and a
// hostname PodHostname did not write say nothing, and with nothing read
// the hostname carries the row's count. Where live pods disagree, the
// first by name decides, in either order.
func TestReadPodSetHostnames(t *testing.T) {
	const (
		leader   = "example-engine-0-leader-0"
		worker   = "example-engine-0-worker-0"
		made     = "example-engine-0-worker-1" // the pod made beside the others
		rowCount = 7
	)
	pod := func(name, hostname, countLabel string, phase corev1.PodPhase) *corev1.Pod {
		p := &corev1.Pod{}
		p.Name = name
		p.Spec.Hostname = hostname
		p.Status.Phase = phase
		p.Labels = map[string]string{LabelRunner: workload.RunnerLeader}
		if countLabel != "" {
			p.Labels[LabelInstanceIncarnation] = countLabel
		}
		return p
	}
	singlePod := pod(leader, leader, "2", corev1.PodRunning)
	singlePod.Labels[LabelRunner] = workload.RunnerDefault
	longName := strings.Repeat("a", 49) + "-engine-0-leader-0" // 67 chars, so its hostname is bounded
	atRowCount := PodHostname(made, rowCount)
	for _, tc := range []struct {
		name string
		pods []*corev1.Pod
		want string
	}{
		{name: "no pod", want: atRowCount},
		{name: "a nil entry", pods: []*corev1.Pod{nil}, want: atRowCount},
		{name: "a running pod answers to its pod name", pods: []*corev1.Pod{pod(leader, leader, "7", corev1.PodRunning)}, want: made},
		{name: "a pending pod answers to its pod name", pods: []*corev1.Pod{pod(leader, leader, "7", corev1.PodPending)}, want: made},
		{name: "a pod of an earlier count answers to its pod name", pods: []*corev1.Pod{pod(leader, leader, "6", corev1.PodRunning)}, want: made},
		{name: "the hostname carries the row's count", pods: []*corev1.Pod{pod(leader, PodHostname(leader, 7), "7", corev1.PodRunning)}, want: atRowCount},
		{name: "the hostname carries an earlier count, as the label does", pods: []*corev1.Pod{pod(leader, PodHostname(leader, 6), "6", corev1.PodRunning)}, want: PodHostname(made, 6)},
		{name: "the hostname carries an earlier count than the label", pods: []*corev1.Pod{pod(leader, PodHostname(leader, 6), "7", corev1.PodRunning)}, want: PodHostname(made, 6)},
		{name: "the hostname carries a count and the label names none", pods: []*corev1.Pod{pod(leader, PodHostname(leader, 3), "", corev1.PodRunning)}, want: PodHostname(made, 3)},
		{name: "the hostname is bounded", pods: []*corev1.Pod{pod(longName, PodHostname(longName, 4), "4", corev1.PodRunning)}, want: PodHostname(made, 4)},
		{name: "the hostname carries the largest count", pods: []*corev1.Pod{pod(leader, PodHostname(leader, math.MaxInt64), "1", corev1.PodRunning)}, want: PodHostname(made, math.MaxInt64)},
		{name: "the pod failed", pods: []*corev1.Pod{pod(leader, leader, "7", corev1.PodFailed)}, want: atRowCount},
		{name: "the pod succeeded", pods: []*corev1.Pod{pod(leader, PodHostname(leader, 6), "6", corev1.PodSucceeded)}, want: atRowCount},
		{name: "the pod is a single-pod Instance's", pods: []*corev1.Pod{singlePod}, want: atRowCount},
		{name: "the pod has no hostname", pods: []*corev1.Pod{pod(leader, "", "6", corev1.PodRunning)}, want: atRowCount},
		{name: "the hostname is another pod's", pods: []*corev1.Pod{pod(leader, PodHostname(worker, 6), "6", corev1.PodRunning)}, want: atRowCount},
		{name: "the hostname spells its count another way", pods: []*corev1.Pod{pod(leader, leader+"-i06", "6", corev1.PodRunning)}, want: atRowCount},
		{name: "live pods disagree, the leader listed first", pods: []*corev1.Pod{
			pod(leader, PodHostname(leader, 6), "6", corev1.PodRunning), pod(worker, worker, "7", corev1.PodRunning),
		}, want: PodHostname(made, 6)},
		{name: "live pods disagree, the leader listed last", pods: []*corev1.Pod{
			pod(worker, worker, "7", corev1.PodRunning), pod(leader, PodHostname(leader, 6), "6", corev1.PodRunning),
		}, want: PodHostname(made, 6)},
		{name: "the first pod by name says nothing", pods: []*corev1.Pod{
			pod(leader, "", "6", corev1.PodRunning), pod(worker, PodHostname(worker, 6), "6", corev1.PodRunning),
		}, want: PodHostname(made, 6)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ReadPodSetHostnames(tc.pods).Hostname(made, rowCount); got != tc.want {
				t.Errorf("a pod made beside these gets hostname %q, want %q", got, tc.want)
			}
		})
	}
}

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

// TestKueuePodGroupName checks the Kueue pod-group name: one group per build of
// an Instance, a valid label value of at most 63 characters, and distinct
// names for distinct Instances and builds of the same long owner.
func TestKueuePodGroupName(t *testing.T) {
	const max = validation.LabelValueMaxLength

	if got, want := KueuePodGroupName("example-model", workload.ComponentEngine, 3, 2), "example-model-engine-3-2"; got != want {
		t.Errorf("short Kueue pod-group name = %q, want %q", got, want)
	}
	if KueuePodGroupName("example-model", workload.ComponentEngine, 3, 2) == KueuePodGroupName("example-model", workload.ComponentEngine, 3, 3) {
		t.Error("a rebuilt Instance kept the Kueue pod-group name of its previous build")
	}
	if KueuePodGroupName("example-model", workload.ComponentEngine, 3, 1) == PodGroupName("example-model", workload.ComponentEngine, 3) {
		t.Error("the Kueue pod-group name must differ from the scheduler PodGroup name")
	}

	longIsvc := strings.Repeat("x", 70)
	bounded := KueuePodGroupName(longIsvc, workload.ComponentDecoder, 12, 4)
	if len(bounded) > max {
		t.Errorf("bounded Kueue pod-group name has %d chars, want <= %d", len(bounded), max)
	}
	if errs := validation.IsValidLabelValue(bounded); len(errs) > 0 {
		t.Errorf("bounded Kueue pod-group name %q is not a valid label value: %v", bounded, errs)
	}
	// Kueue names the group's Workload after the group.
	if errs := validation.IsDNS1123Subdomain(bounded); len(errs) > 0 {
		t.Errorf("bounded Kueue pod-group name %q is not a valid Workload name: %v", bounded, errs)
	}
	if bounded != KueuePodGroupName(longIsvc, workload.ComponentDecoder, 12, 4) {
		t.Error("KueuePodGroupName is not deterministic")
	}
	for _, other := range []string{
		KueuePodGroupName(longIsvc, workload.ComponentDecoder, 13, 4),
		KueuePodGroupName(longIsvc, workload.ComponentDecoder, 12, 5),
	} {
		if other == bounded {
			t.Errorf("distinct Instances or builds produced the same bounded name %q", bounded)
		}
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

// TestPodName_IsTheFormattedNameForEveryInput holds the pod name to its
// form, `<owner>-<component>-<index>-<runner>-<ordinal>` with every number
// in decimal, over owner names up to the label limit, every component,
// indices up to the largest shape and past it, every runner and the
// ordinals a gang of eight and a surge use. The Create pass reads its pods
// by this name on every pass, so a builder that drops the formatting
// library must yield these bytes exactly.
func TestPodName_IsTheFormattedNameForEveryInput(t *testing.T) {
	owners := []string{"a", "llama-70b", "example-model-decoder-prefill", strings.Repeat("n", validation.DNS1035LabelMaxLength)}
	components := []workload.ComponentType{workload.ComponentEngine, workload.ComponentDecoder, workload.ComponentRouter}
	indices := []int32{0, 1, 9, 10, 99, 100, 255, 256, 999, 1000, 1724, 3999, 4000, 65535, 65536, 1 << 30, 1<<31 - 1, -1}
	runners := []string{workload.RunnerDefault, workload.RunnerLeader, workload.RunnerWorker, ""}
	ordinals := []int32{0, 1, 7, 8, 255, 256, 1<<31 - 1, -1}
	checked := 0
	for _, owner := range owners {
		for _, component := range components {
			for _, index := range indices {
				for _, runner := range runners {
					for _, ordinal := range ordinals {
						want := fmt.Sprintf("%s-%s-%d-%s-%d", owner, component, index, runner, ordinal)
						if got := PodName(owner, component, index, runner, ordinal); got != want {
							t.Fatalf("PodName(%q, %q, %d, %q, %d) = %q, want %q", owner, component, index, runner, ordinal, got, want)
						}
						checked++
					}
				}
			}
		}
	}
	if checked != len(owners)*len(components)*len(indices)*len(runners)*len(ordinals) {
		t.Fatalf("checked %d names, want every combination", checked)
	}
}

// TestPodGroupIndex_IsTheInverseOfPodGroupNameWithinTheLabelLimit: over
// services of every length up to the label limit, the Components and
// indexes from 0 to the int32 limit, PodGroupIndex reads the index back off
// PodGroupName whenever the plain name fits the limit, and nothing off a
// name the limit hashed.
func TestPodGroupIndex_IsTheInverseOfPodGroupNameWithinTheLabelLimit(t *testing.T) {
	components := []workload.ComponentType{workload.ComponentEngine, workload.ComponentDecoder, workload.ComponentRouter, ""}
	indexes := []int32{0, 1, 9, 10, 99, 100, 255, 256, 999, 1725, 3999, 65535, 1<<31 - 1}
	checked := 0
	for length := 0; length <= validation.LabelValueMaxLength+5; length++ {
		isvc := strings.Repeat("s", length)
		for _, component := range components {
			for _, index := range indexes {
				name := PodGroupName(isvc, component, index)
				plain := fmt.Sprintf("%s-%s-%d", isvc, component, index)
				got, ok := PodGroupIndex(name, isvc, component)
				if fits := len(plain) <= validation.LabelValueMaxLength; fits != ok || (ok && got != index) {
					t.Fatalf("PodGroupIndex(%q, %q, %q) = %d, %t; the plain name fits the limit: %t, index %d", name, isvc, component, got, ok, fits, index)
				}
				if plain != name {
					if got, ok := PodGroupIndex(plain, isvc, component); ok {
						t.Fatalf("PodGroupIndex(%q, %q, %q) = %d past the label limit; want none, the name is %q", plain, isvc, component, got, name)
					}
				}
				checked++
			}
		}
	}
	if checked < 2000 {
		t.Fatalf("checked %d names, want the whole table", checked)
	}
}

// TestPodGroupIndex_ReadsNothingOffAnyOtherName: a name changed in any way
// from the plain form reads as no index: another service or Component, an
// index with a sign, a leading zero, a space or a letter, an index past
// the int32 limit, the name cut short, and the name with more after it.
func TestPodGroupIndex_ReadsNothingOffAnyOtherName(t *testing.T) {
	const isvc, component = "fleet", workload.ComponentEngine
	for _, name := range []string{
		"", "fleet", "fleet-engine", "fleet-engine-", "fleet-engine-01", "fleet-engine-+1", "fleet-engine--1",
		"fleet-engine-1 ", "fleet-engine- 1", "fleet-engine-1a", "fleet-engine-a1", "fleet-engine-1-0",
		"fleet-engine-2147483648", "fleet-engine-99999999999", "fleet-decoder-1", "other-engine-1", "fleet_engine-1",
		"fleet-engine_1", "xfleet-engine-1", "fleetx-engine-1", "fleet-enginex-1", "fleet-engine-1" + strings.Repeat("0", 60),
	} {
		if index, ok := PodGroupIndex(name, isvc, component); ok {
			t.Errorf("PodGroupIndex(%q) read index %d; want none", name, index)
		}
	}
	for _, name := range []string{"fleet-engine-0", "fleet-engine-1", "fleet-engine-10", "fleet-engine-2147483647"} {
		index, ok := PodGroupIndex(name, isvc, component)
		if !ok || PodGroupName(isvc, component, index) != name {
			t.Errorf("PodGroupIndex(%q) = %d, %t; want the index PodGroupName writes as %q", name, index, ok, name)
		}
	}
}

// TestPodGroupIndex_AllocatesNothing holds the reading to zero allocations
// on a planned gang's name and on a name it refuses.
func TestPodGroupIndex_AllocatesNothing(t *testing.T) {
	for _, name := range []string{"fleet-engine-1725", "fleet-engine-01", "other-engine-1"} {
		if n := testing.AllocsPerRun(100, func() { _, _ = PodGroupIndex(name, "fleet", workload.ComponentEngine) }); n != 0 {
			t.Errorf("PodGroupIndex(%q) allocates %v times per reading; want none", name, n)
		}
	}
}
