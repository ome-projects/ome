package query

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/ome/pkg/constants"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

func TestLiveListPodsForInstance_FiltersByIndex(t *testing.T) {
	pod0 := newDiscoverPod("p-0", 0, 1, false)
	pod1 := newDiscoverPod("p-1", 1, 1, false)
	c := newDiscoverTestClient(t, pod0, pod1)

	got, err := LiveListPodsForInstance(context.Background(), c, "ns", "isvc", workload.ComponentEngine, 0)
	if err != nil {
		t.Fatalf("LiveListPodsForInstance: %v", err)
	}
	if len(got) != 1 || got[0].Name != "p-0" {
		t.Errorf("expected only p-0; got %v", podNames(got))
	}
}

// TestLiveListPodsForComponent_AllIndicesAndUnlabeled pins the teardown
// completion read: every component pod counts regardless of its
// instance-index label — including a pod whose index label is missing
// entirely (the statusless-orphan shape) — while pods of another
// component stay invisible.
func TestLiveListPodsForComponent_AllIndicesAndUnlabeled(t *testing.T) {
	pod0 := newDiscoverPod("p-0", 0, 1, false)
	pod1 := newDiscoverPod("p-1", 1, 1, false)
	orphan := newDiscoverPod("p-orphan", 9, 1, false)
	delete(orphan.Labels, LabelInstanceIdx)
	foreign := newDiscoverPod("p-foreign", 0, 1, false)
	foreign.Labels[constants.OMEComponentLabel] = string(workload.ComponentDecoder)
	c := newDiscoverTestClient(t, pod0, pod1, orphan, foreign)

	got, err := LiveListPodsForComponent(context.Background(), c, "ns", "isvc", workload.ComponentEngine)
	if err != nil {
		t.Fatalf("LiveListPodsForComponent: %v", err)
	}
	want := map[string]bool{"p-0": true, "p-1": true, "p-orphan": true}
	if len(got) != len(want) {
		t.Fatalf("expected %d pods, got %v", len(want), podNames(got))
	}
	for _, p := range got {
		if !want[p.Name] {
			t.Errorf("unexpected pod %s in component list", p.Name)
		}
	}
}

func TestLiveOldPodsClearedForRecreate_NoPods(t *testing.T) {
	c := newDiscoverTestClient(t)
	clear, err := LiveOldPodsClearedForRecreate(context.Background(), c, "ns", "isvc", workload.ComponentEngine, 0, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !clear {
		t.Errorf("expected clear=true with no pods, got false")
	}
}

func TestLiveOldPodsClearedForRecreate_OnlyNewIncarnation(t *testing.T) {
	// Phase B reconcile after the controller previously created the
	// new-incarnation pods but crashed before Ready. Old pods are
	// long gone, new pods exist. Should be clear.
	c := newDiscoverTestClient(t,
		newDiscoverPod("p-new-0", 0, 2, false),
		newDiscoverPod("p-new-1", 0, 2, false),
	)
	clear, err := LiveOldPodsClearedForRecreate(context.Background(), c, "ns", "isvc", workload.ComponentEngine, 0, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !clear {
		t.Errorf("expected clear=true when only new-incarnation pods present, got false")
	}
}

func TestLiveOldPodsClearedForRecreate_OldStillRunning(t *testing.T) {
	// Phase B reconcile fired but Phase A deletes haven't propagated
	// — old pod still present, no DeletionTimestamp. Must NOT proceed.
	c := newDiscoverTestClient(t,
		newDiscoverPod("p-old", 0, 1, false),
	)
	clear, err := LiveOldPodsClearedForRecreate(context.Background(), c, "ns", "isvc", workload.ComponentEngine, 0, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if clear {
		t.Errorf("expected clear=false when old pod still running, got true")
	}
}

func TestLiveOldPodsClearedForRecreate_OldTerminatingTolerated(t *testing.T) {
	// Old pod has DeletionTimestamp (foreground propagation in
	// flight) — Phase B can proceed because the stable name will
	// land on a different pod once GC completes.
	c := newDiscoverTestClient(t,
		newDiscoverPod("p-old", 0, 1, true),
	)
	clear, err := LiveOldPodsClearedForRecreate(context.Background(), c, "ns", "isvc", workload.ComponentEngine, 0, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !clear {
		t.Errorf("expected clear=true for terminating old pod, got false")
	}
}

func TestLiveOldPodsClearedForRecreate_MissingLabelDoesNotBlockPhaseB(t *testing.T) {
	// Unknown (label-missing) pods are not OLD —
	// LiveOldPodsClearedForRecreate only gates on OLD. In production an
	// orphan short-circuits Restart via FoundOrphan before this helper
	// runs; this test pins the helper's narrower contract: "old pods,
	// are they all terminating?".
	pod := newDiscoverPod("p-orphan", 0, 0, false)
	delete(pod.Labels, LabelInstanceIncarnation)
	c := newDiscoverTestClient(t, pod)

	clear, err := LiveOldPodsClearedForRecreate(context.Background(), c, "ns", "isvc", workload.ComponentEngine, 0, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !clear {
		t.Errorf("expected clear=true: unknown bucket pods don't count as old, so the old set is empty")
	}
}

func TestAllTerminating(t *testing.T) {
	if !AllTerminating(nil) {
		t.Errorf("nil slice should be trivially terminating")
	}
	if !AllTerminating([]*corev1.Pod{}) {
		t.Errorf("empty slice should be trivially terminating")
	}
	terminating := newDiscoverPod("t", 0, 1, true)
	if !AllTerminating([]*corev1.Pod{terminating}) {
		t.Errorf("pod with DeletionTimestamp should be terminating")
	}
	running := newDiscoverPod("r", 0, 1, false)
	if AllTerminating([]*corev1.Pod{terminating, running}) {
		t.Errorf("mixed slice should NOT be all-terminating")
	}
}

// TestListOMENativePodsByName_IndexAndFallbackAgree pins the core
// invariant of the field-index optimization: the MatchingFields fast path
// (index registered) and the label-selector fallback (no index) return
// the identical set, and the index excludes other (isvc, component)
// tuples. Without the index the existing tests above already cover the
// fallback; this adds the indexed path and the cross-tuple exclusion.
func TestListOMENativePodsByName_IndexAndFallbackAgree(t *testing.T) {
	objs := []client.Object{
		newDiscoverPodFor("eng-0", "isvc", workload.ComponentEngine),
		newDiscoverPodFor("eng-1", "isvc", workload.ComponentEngine),
		// Different component — must be excluded.
		newDiscoverPodFor("dec-0", "isvc", workload.ComponentDecoder),
		// Different isvc — must be excluded.
		newDiscoverPodFor("other-eng-0", "other", workload.ComponentEngine),
	}

	indexed := newIndexedDiscoverTestClient(t, objs...)
	fallback := newDiscoverTestClient(t, objs...)

	// useIndex=true on both: the indexed client takes the MatchingFields
	// fast path; the index-less client falls back to the label selector.
	// Both must return the identical set.
	gotIndexed, err := ListOMENativePodsByName(context.Background(), indexed, "ns", "isvc", workload.ComponentEngine, true)
	if err != nil {
		t.Fatalf("indexed list: %v", err)
	}
	gotFallback, err := ListOMENativePodsByName(context.Background(), fallback, "ns", "isvc", workload.ComponentEngine, true)
	if err != nil {
		t.Fatalf("fallback list: %v", err)
	}

	want := map[string]bool{"eng-0": true, "eng-1": true}
	assertPodSet(t, "indexed", gotIndexed, want)
	assertPodSet(t, "fallback", gotFallback, want)
}

// TestListOMENativePodsByName_LiveReaderSkipsProbe pins the live-reader
// contract: with useIndex=false (the live-reader / index-less path), the
// helper goes STRAIGHT to the label selector — exactly ONE PodList List,
// and ZERO MatchingFields probes. A MatchingFields probe against the live
// API reader always fails and forces a second fallback List. Asserting one
// List + zero probes proves the live path never attempts the doomed probe
// while the returned set is still correct.
func TestListOMENativePodsByName_LiveReaderSkipsProbe(t *testing.T) {
	objs := []client.Object{
		newDiscoverPodFor("eng-0", "isvc", workload.ComponentEngine),
		newDiscoverPodFor("eng-1", "isvc", workload.ComponentEngine),
		newDiscoverPodFor("dec-0", "isvc", workload.ComponentDecoder),
	}
	// Index-less client (the real APIReader has no Pod field index either).
	live := newDiscoverTestClient(t, objs...)
	counter := &probeCountingReader{Reader: live}

	got, err := ListOMENativePodsByName(context.Background(), counter, "ns", "isvc", workload.ComponentEngine, false)
	if err != nil {
		t.Fatalf("live list: %v", err)
	}
	assertPodSet(t, "live", got, map[string]bool{"eng-0": true, "eng-1": true})

	if counter.matchingFieldsHits != 0 {
		t.Errorf("live reader path must NOT issue a MatchingFields probe, got %d", counter.matchingFieldsHits)
	}
	if counter.podListCalls != 1 {
		t.Errorf("live reader path must issue exactly 1 PodList List, got %d", counter.podListCalls)
	}
}

// TestListOMENativePodsByName_CachedReaderUsesIndex is the companion to the
// live-path test: with useIndex=true against an index-backed client, the
// helper takes the MatchingFields fast path in a SINGLE List (the index
// resolves, no fallback). Together the two tests prove cached callers keep
// the index fast path while live callers skip the probe.
func TestListOMENativePodsByName_CachedReaderUsesIndex(t *testing.T) {
	objs := []client.Object{
		newDiscoverPodFor("eng-0", "isvc", workload.ComponentEngine),
		newDiscoverPodFor("eng-1", "isvc", workload.ComponentEngine),
		newDiscoverPodFor("dec-0", "isvc", workload.ComponentDecoder),
	}
	indexed := newIndexedDiscoverTestClient(t, objs...)
	counter := &probeCountingReader{Reader: indexed}

	got, err := ListOMENativePodsByName(context.Background(), counter, "ns", "isvc", workload.ComponentEngine, true)
	if err != nil {
		t.Fatalf("cached list: %v", err)
	}
	assertPodSet(t, "cached", got, map[string]bool{"eng-0": true, "eng-1": true})

	if counter.matchingFieldsHits != 1 {
		t.Errorf("cached reader path must take the MatchingFields fast path exactly once, got %d", counter.matchingFieldsHits)
	}
	if counter.podListCalls != 1 {
		t.Errorf("cached reader path must resolve via the index in 1 List (no fallback), got %d", counter.podListCalls)
	}
}

var promoteNow = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// promotePod builds a live pod carrying the requested conditions; a zero
// readyAt leaves the PodReady condition off entirely.
func promotePod(name string, containersReady bool, readyAt time.Time) *corev1.Pod {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	if containersReady {
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
			Type: corev1.ContainersReady, Status: corev1.ConditionTrue,
		})
	}
	if !readyAt.IsZero() {
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
			Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(readyAt),
		})
	}
	return pod
}

// TestPodSetPromotable covers the one bar every Ready stamp shares:
// ContainersReady alone is below it, PodReady past the minReadySeconds
// window clears it, and a set still inside the window reports the
// remainder so the caller wakes on it instead of polling.
func TestPodSetPromotable(t *testing.T) {
	cases := []struct {
		name            string
		pods            []*corev1.Pod
		minReadySeconds int32
		extra           []func(*corev1.Pod) bool
		want            bool
		wantWait        time.Duration
	}{
		{
			// Nothing running proves nothing: an Instance whose pods are all
			// gone must never be stamped Ready by a vacuously clear bar.
			name: "empty set is never promotable",
			want: false,
		},
		{
			name:            "empty set is never promotable under a window either",
			pods:            []*corev1.Pod{},
			minReadySeconds: 20,
			want:            false,
		},
		{
			name: "ContainersReady without PodReady is below the bar",
			pods: []*corev1.Pod{promotePod("a", true, time.Time{})},
			want: false,
		},
		{
			name: "PodReady with no window clears the bar",
			pods: []*corev1.Pod{promotePod("a", true, promoteNow.Add(-time.Second))},
			want: true,
		},
		{
			name:            "inside the window reports the remainder",
			pods:            []*corev1.Pod{promotePod("a", true, promoteNow.Add(-5*time.Second))},
			minReadySeconds: 20,
			want:            false,
			wantWait:        15 * time.Second,
		},
		{
			name: "the remainder is the last pod to clear the window",
			pods: []*corev1.Pod{
				promotePod("a", true, promoteNow.Add(-15*time.Second)),
				promotePod("b", true, promoteNow.Add(-5*time.Second)),
			},
			minReadySeconds: 20,
			want:            false,
			wantWait:        15 * time.Second,
		},
		{
			name:            "elapsed window clears the bar at the exact boundary",
			pods:            []*corev1.Pod{promotePod("a", true, promoteNow.Add(-20*time.Second))},
			minReadySeconds: 20,
			want:            true,
		},
		{
			name: "one pod short of PodReady holds the whole set with no remainder",
			pods: []*corev1.Pod{
				promotePod("a", true, promoteNow.Add(-time.Second)),
				promotePod("b", true, time.Time{}),
			},
			minReadySeconds: 20,
			want:            false,
		},
		{
			name: "a terminal pod is never promotable",
			pods: []*corev1.Pod{func() *corev1.Pod {
				p := promotePod("a", true, promoteNow.Add(-time.Second))
				p.Status.Phase = corev1.PodSucceeded
				return p
			}()},
			want: false,
		},
		{
			name:  "an unsatisfied extra predicate holds a PodReady set",
			pods:  []*corev1.Pod{promotePod("a", true, promoteNow.Add(-time.Second))},
			extra: []func(*corev1.Pod) bool{func(*corev1.Pod) bool { return false }},
			want:  false,
		},
		{
			name:  "a satisfied extra predicate leaves the bar to readiness",
			pods:  []*corev1.Pod{promotePod("a", true, promoteNow.Add(-time.Second))},
			extra: []func(*corev1.Pod) bool{func(*corev1.Pod) bool { return true }},
			want:  true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, wait := PodSetPromotable(tc.pods, tc.minReadySeconds, promoteNow, tc.extra...)
			if got != tc.want {
				t.Fatalf("promotable: got %t, want %t", got, tc.want)
			}
			if wait != tc.wantWait {
				t.Fatalf("wait: got %s, want %s", wait, tc.wantWait)
			}
		})
	}
}

// podInPhase builds a pod in the given phase. ready stamps a
// ContainersReady=True condition — on a terminal pod that models a stale
// condition the kubelet never cleared.
func podInPhase(name string, phase corev1.PodPhase, ready bool) *corev1.Pod {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "prod"}}
	pod.Status.Phase = phase
	if ready {
		pod.Status.Conditions = []corev1.PodCondition{{
			Type: corev1.ContainersReady, Status: corev1.ConditionTrue,
		}}
	}
	return pod
}

func TestIsTerminalPod(t *testing.T) {
	cases := []struct {
		name string
		pod  *corev1.Pod
		want bool
	}{
		{"nil", nil, false},
		{"unset phase", podInPhase("p", "", false), false},
		{"pending", podInPhase("p", corev1.PodPending, false), false},
		{"running", podInPhase("p", corev1.PodRunning, true), false},
		{"unknown", podInPhase("p", corev1.PodUnknown, false), false},
		{"failed", podInPhase("p", corev1.PodFailed, false), true},
		{"succeeded", podInPhase("p", corev1.PodSucceeded, false), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsTerminalPod(tc.pod); got != tc.want {
				t.Fatalf("IsTerminalPod = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestExcludeTerminalPods(t *testing.T) {
	live := podInPhase("live", corev1.PodRunning, true)
	pending := podInPhase("pending", corev1.PodPending, false)
	got := ExcludeTerminalPods([]*corev1.Pod{
		podInPhase("failed", corev1.PodFailed, false),
		live,
		nil,
		podInPhase("succeeded", corev1.PodSucceeded, false),
		pending,
	})
	if len(got) != 2 || got[0] != live || got[1] != pending {
		t.Fatalf("ExcludeTerminalPods kept %d pods, want exactly [live pending]", len(got))
	}
	if out := ExcludeTerminalPods(nil); out == nil || len(out) != 0 {
		t.Fatalf("ExcludeTerminalPods(nil) = %v, want an empty non-nil slice", out)
	}
}

// A terminal pod is never runtime-ready, even when it still carries a
// ContainersReady=True condition from before it died.
func TestAllPodsRuntimeReady_TerminalPodNeverReady(t *testing.T) {
	ready := podInPhase("ready", corev1.PodRunning, true)
	cases := []struct {
		name string
		pods []*corev1.Pod
		want bool
	}{
		{"empty", nil, false},
		{"all ready", []*corev1.Pod{ready, podInPhase("ready-2", corev1.PodRunning, true)}, true},
		{"one not ready", []*corev1.Pod{ready, podInPhase("booting", corev1.PodRunning, false)}, false},
		{"failed with stale ready condition", []*corev1.Pod{ready, podInPhase("dead", corev1.PodFailed, true)}, false},
		{"succeeded with stale ready condition", []*corev1.Pod{podInPhase("done", corev1.PodSucceeded, true)}, false},
		{"only terminal", []*corev1.Pod{podInPhase("dead", corev1.PodFailed, false)}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := AllPodsRuntimeReady(tc.pods); got != tc.want {
				t.Fatalf("AllPodsRuntimeReady = %v, want %v", got, tc.want)
			}
		})
	}
}
