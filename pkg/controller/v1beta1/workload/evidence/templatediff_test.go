package evidence_test

// The template diff, as the in-place roll reads it: which container
// images a live pod still owes the target revision (in its spec and at
// runtime), and which annotation keys the roll owns and may change.

import (
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
)

// TestPodImagesMatch_InjectedSidecarIgnored pins the container-subset
// compare: live containers absent from the target spec (istio/linkerd
// style injections) are not OMENative-owned and must not block the
// match — counting them would fail podImagesMatch and podRuntimeImagesMatch
// forever, livelocking the in-place update with the pod held drained.
// A TARGET container missing from the pod still fails both.
func TestPodImagesMatch_InjectedSidecarIgnored(t *testing.T) {
	target := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "llama:v2"}}}
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{Containers: []corev1.Container{
			{Name: "main", Image: "llama:v2"},
			{Name: "istio-proxy", Image: "istio/proxyv2:1.20"},
		}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			{Name: "main", Image: "llama:v2"},
			{Name: "istio-proxy", Image: "istio/proxyv2:1.20"},
		}},
	}
	if !evidence.PodImagesMatch(pod, target) {
		t.Errorf("PodImagesMatch: injected sidecar must be ignored")
	}
	if !evidence.PodRuntimeImagesMatch(pod, target) {
		t.Errorf("PodRuntimeImagesMatch: injected sidecar status must be ignored")
	}
	stale := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "llama:v3"}}}
	if evidence.PodImagesMatch(pod, stale) {
		t.Errorf("PodImagesMatch: diverged target image must still mismatch")
	}
	foreign := &corev1.PodSpec{Containers: []corev1.Container{{Name: "other", Image: "x:v1"}}}
	if evidence.PodImagesMatch(pod, foreign) {
		t.Errorf("PodImagesMatch: target container missing from pod must mismatch")
	}
	if evidence.PodRuntimeImagesMatch(pod, foreign) {
		t.Errorf("PodRuntimeImagesMatch: target container without a status must mismatch")
	}
}

// TestPodImagesMatch pins the spec-image equality check.
func TestPodImagesMatch(t *testing.T) {
	pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{
		{Name: "main", Image: "llama:v2"},
		{Name: "sidecar", Image: "metrics:v1"},
	}}}
	matching := &corev1.PodSpec{Containers: []corev1.Container{
		{Name: "main", Image: "llama:v2"},
		{Name: "sidecar", Image: "metrics:v1"},
	}}
	mismatching := &corev1.PodSpec{Containers: []corev1.Container{
		{Name: "main", Image: "llama:v3"},
		{Name: "sidecar", Image: "metrics:v1"},
	}}
	if !evidence.PodImagesMatch(pod, matching) {
		t.Errorf("matching images should report true")
	}
	if evidence.PodImagesMatch(pod, mismatching) {
		t.Errorf("differing images should report false")
	}
}

// TestCanonicalImage_NormalizesDockerHubReferences covers the qualified
// forms container runtimes report for short Docker Hub references.
func TestCanonicalImage_NormalizesDockerHubReferences(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want string
	}{
		{"implicit library tag", "fake-serving:v1", "fake-serving:v1"},
		{"runtime-stamped library tag", "docker.io/library/fake-serving:v1", "fake-serving:v1"},
		{"explicit registry round-trip", "ghcr.io/foo/bar:v1", "ghcr.io/foo/bar:v1"},
		{"runtime-stamped namespaced tag", "docker.io/myuser/myimg:v1", "myuser/myimg:v1"},
		{"empty string", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := evidence.CanonicalImage(tc.in); got != tc.want {
				t.Errorf("evidence.CanonicalImage(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestPodRuntimeImagesMatch_RegistryNormalization: `fake-serving:v1`
// (spec) and `docker.io/library/fake-serving:v1` (runtime) MUST
// compare equal so in-place updates against KIND or any cluster whose
// runtime fully-qualifies implicit Docker Hub references converge.
func TestPodRuntimeImagesMatch_RegistryNormalization(t *testing.T) {
	target := &corev1.PodSpec{Containers: []corev1.Container{
		{Name: "main", Image: "fake-serving:v2"},
	}}
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "fake-serving:v2"}}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			{Name: "main", Image: "docker.io/library/fake-serving:v2"},
		}},
	}
	if !evidence.PodRuntimeImagesMatch(pod, target) {
		t.Errorf("expected match between spec %q and runtime %q after canonicalImage normalization",
			target.Containers[0].Image, pod.Status.ContainerStatuses[0].Image)
	}
}

// TestAnnotationsDiff_Cases exercises the helper directly to pin each
// branch of the (add | update | delete | leave-alone) decision matrix.
// These cases compose into the per-pod patches but are easier to reason
// about in isolation than via the full Update fixture.
func TestAnnotationsDiff_Cases(t *testing.T) {
	cases := []struct {
		name                string
		pod, previous, want map[string]string
		expect              map[string]any
	}{
		{
			name:     "no diff: nothing to patch",
			pod:      map[string]string{"a": "1"},
			previous: map[string]string{"a": "1"},
			want:     map[string]string{"a": "1"},
			expect:   map[string]any{},
		},
		{
			name:     "add new key from target",
			pod:      map[string]string{"a": "1"},
			previous: map[string]string{"a": "1"},
			want:     map[string]string{"a": "1", "b": "2"},
			expect:   map[string]any{"b": "2"},
		},
		{
			name:     "update existing target key with new value",
			pod:      map[string]string{"a": "1"},
			previous: map[string]string{"a": "1"},
			want:     map[string]string{"a": "2"},
			expect:   map[string]any{"a": "2"},
		},
		{
			name:     "delete key the user removed (previous owned it)",
			pod:      map[string]string{"a": "1", "b": "2"},
			previous: map[string]string{"a": "1", "b": "2"},
			want:     map[string]string{"a": "1"},
			expect:   map[string]any{"b": nil},
		},
		{
			name:     "leave foreign key alone (in pod, not in previous or target)",
			pod:      map[string]string{"a": "1", "linkerd.io/inject": "enabled"},
			previous: map[string]string{"a": "1"},
			want:     map[string]string{"a": "1"},
			expect:   map[string]any{},
		},
		{
			name:     "delete only if pod actually has the key (no spurious null patch)",
			pod:      map[string]string{"a": "1"},
			previous: map[string]string{"a": "1", "b": "2"},
			want:     map[string]string{"a": "1"},
			expect:   map[string]any{},
		},
		{
			name:     "nil-as-empty across all inputs",
			pod:      nil,
			previous: nil,
			want:     nil,
			expect:   map[string]any{},
		},
		{
			name:     "first revision: previous nil, target adds key, pod empty",
			pod:      nil,
			previous: nil,
			want:     map[string]string{"a": "1"},
			expect:   map[string]any{"a": "1"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := evidence.AnnotationsDiff(tc.pod, tc.previous, tc.want)
			if len(got) != len(tc.expect) {
				t.Fatalf("diff length: got %d (%+v) want %d (%+v)", len(got), got, len(tc.expect), tc.expect)
			}
			for k, want := range tc.expect {
				gv, ok := got[k]
				if !ok {
					t.Errorf("missing key %q in diff", k)
					continue
				}
				if want == nil && gv != nil {
					t.Errorf("key %q: got %+v want nil", k, gv)
				}
				if want != nil && gv != want {
					t.Errorf("key %q: got %+v want %+v", k, gv, want)
				}
			}
		})
	}
}

func TestPodRuntimeImageChangesMatch(t *testing.T) {
	spec := func(main, helper string) *corev1.PodSpec {
		containers := []corev1.Container{{Name: "main", Image: main}}
		if helper != "" {
			containers = append(containers, corev1.Container{Name: "helper", Image: helper})
		}
		return &corev1.PodSpec{Containers: containers}
	}
	pod := func(main, helper string) *corev1.Pod {
		statuses := []corev1.ContainerStatus{{
			Name: "main", Image: main, ContainerID: "containerd://main", RestartCount: 3,
		}}
		if helper != "" {
			statuses = append(statuses, corev1.ContainerStatus{Name: "helper", Image: helper})
		}
		return &corev1.Pod{Status: corev1.PodStatus{ContainerStatuses: statuses}}
	}

	tests := []struct {
		name    string
		pod     *corev1.Pod
		running *corev1.PodSpec
		target  *corev1.PodSpec
		want    bool
	}{
		{
			name:    "metadata-only ignores runtime alias",
			pod:     pod("mirror.example.com/app:v1", ""),
			running: spec("example.com/app:v1", ""),
			target:  spec("example.com/app:v1", ""),
			want:    true,
		},
		{
			name:    "changed image matches while unchanged alias is ignored",
			pod:     pod("example.com/app:v2", "mirror.example.com/helper:v1"),
			running: spec("example.com/app:v1", "example.com/helper:v1"),
			target:  spec("example.com/app:v2", "example.com/helper:v1"),
			want:    true,
		},
		{
			name:    "changed image remains stale",
			pod:     pod("example.com/app:v1", "mirror.example.com/helper:v1"),
			running: spec("example.com/app:v1", "example.com/helper:v1"),
			target:  spec("example.com/app:v2", "example.com/helper:v1"),
		},
		{
			name:    "one of two changed images remains stale",
			pod:     pod("example.com/app:v2", "example.com/helper:v1"),
			running: spec("example.com/app:v1", "example.com/helper:v1"),
			target:  spec("example.com/app:v2", "example.com/helper:v2"),
		},
		{
			name:   "missing running revision checks every target image",
			pod:    pod("example.com/app:v2", "example.com/helper:v2"),
			target: spec("example.com/app:v2", "example.com/helper:v2"),
			want:   true,
		},
		{
			name:   "missing running revision rejects stale runtime",
			pod:    pod("example.com/app:v1", ""),
			target: spec("example.com/app:v2", ""),
		},
		{
			name:    "retarget does not accept prior target runtime",
			pod:     pod("example.com/app:v2", ""),
			running: spec("example.com/app:v1", ""),
			target:  spec("example.com/app:v3", ""),
		},
		{name: "nil pod", running: spec("example.com/app:v1", ""), target: spec("example.com/app:v1", "")},
		{name: "nil target", pod: pod("example.com/app:v1", ""), running: spec("example.com/app:v1", "")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := evidence.PodRuntimeImageChangesMatch(test.pod, test.running, test.target); got != test.want {
				t.Fatalf("evidence.PodRuntimeImageChangesMatch() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestPodRuntimeImageChangesMatchDoesNotUseRestartAsTargetProof(t *testing.T) {
	running := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "example.com/app:v1"}}}
	target := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "example.com/app:v2"}}}

	for _, observedGeneration := range []int64{0, 6, 7} {
		t.Run(fmt.Sprintf("observed-generation-%d", observedGeneration), func(t *testing.T) {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Generation: 7},
				Spec:       *target.DeepCopy(),
				Status: corev1.PodStatus{
					ObservedGeneration: observedGeneration,
					ContainerStatuses: []corev1.ContainerStatus{{
						Name: "main", Image: "example.com/app:v1",
						ContainerID: "containerd://restarted-old-image", RestartCount: 4,
					}},
				},
			}
			if evidence.PodRuntimeImageChangesMatch(pod, running, target) {
				t.Fatal("old runtime image was accepted after an unrelated restart")
			}
		})
	}
}

func TestAnnotationsDiffReservesInPlaceImageTransition(t *testing.T) {
	podAnnotations := map[string]string{
		constants.InferenceServiceInPlaceImageTransitionAnnotationKey: "controller-state",
		"example.com/release": "one",
	}
	previous := map[string]string{
		constants.InferenceServiceInPlaceImageTransitionAnnotationKey: "old-user-value",
		"example.com/release": "one",
	}
	target := map[string]string{
		constants.InferenceServiceInPlaceImageTransitionAnnotationKey: "forged-user-value",
		"example.com/release": "two",
	}
	diff := evidence.AnnotationsDiff(podAnnotations, previous, target)
	if _, found := diff[constants.InferenceServiceInPlaceImageTransitionAnnotationKey]; found {
		t.Fatalf("reserved annotation entered PodMeta diff: %#v", diff)
	}
	if diff["example.com/release"] != "two" {
		t.Fatalf("ordinary annotation diff was lost: %#v", diff)
	}
}
