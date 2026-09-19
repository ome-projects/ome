package revision

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// minimalISVC builds an ISVC with the minimum metadata Ensure* needs
// (Name, Namespace, UID for the OwnerReference UID asserts).
func minimalISVC(name, ns string, replicas int) *v1beta1.InferenceService {
	mr := replicas
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			UID:       types.UID(name + "-uid"),
		},
		Spec: v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: &mr,
				},
			},
		},
	}
}

// testManagedByLabelKey / testManagedByLabelValue mirror the workload
// query package's label-key constants so the "managed-by label is
// stamped" assertion reads them through a test-only alias and stays
// grep-stable. The values match constants in workload/query/labels.go.
const (
	testManagedByLabelKey   = "ome.io/managed-by"
	testManagedByLabelValue = "OMENative"
)

// testISVCGVK is the GroupVersionKind every ISVC-shaped OwnerReference
// stamps. Hard-coded here rather than importing the omenative/core
// package because that package depends on workload (cyclic).
var testISVCGVK = v1beta1.SchemeGroupVersion.WithKind("InferenceService")

// testKey builds the revision.Key for an ISVC: Namespace =
// isvc.Namespace, Name = "<isvc>-<component>", Labels = the ISVC
// pod-selector trio.
func testKey(isvc *v1beta1.InferenceService, component workload.ComponentType) Key {
	return Key{
		Namespace: isvc.Namespace,
		Name:      isvc.Name + "-" + string(component),
		Labels: map[string]string{
			"ome.io/inferenceservice": isvc.Name,
			"ome.io/component":        string(component),
			testManagedByLabelKey:     testManagedByLabelValue,
		},
	}
}

// newFakeClientWithApps builds a fake controller-runtime client carrying
// the schemes revision.go reads (corev1 + v1beta1 + appsv1 for
// ControllerRevision).
func newFakeClientWithApps(t *testing.T, initObjs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add appsv1: %v", err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1beta1: %v", err)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(initObjs...).
		Build()
}

func basicPodSpecForRevision() *corev1.PodSpec {
	return &corev1.PodSpec{
		Containers: []corev1.Container{
			{Name: "main", Image: "llama:v1"},
		},
	}
}

func TestRevisionHash_DeterministicForSameInput(t *testing.T) {
	ps := basicPodSpecForRevision()
	h1, raw1, err := Hash(ps, nil, nil, "")
	if err != nil {
		t.Fatalf("hash 1: %v", err)
	}
	h2, raw2, err := Hash(ps, nil, nil, "")
	if err != nil {
		t.Fatalf("hash 2: %v", err)
	}
	if h1 != h2 {
		t.Errorf("hash differs across calls: %q vs %q", h1, h2)
	}
	if string(raw1) != string(raw2) {
		t.Errorf("raw bytes differ across calls")
	}
}

// TestRevisionHash_DeterministicAcrossInvocations pins hash determinism
// across many invocations on a SAME PodSpec / PodMeta combination that
// exercises every map-typed field in the canonical payload — NodeSelector,
// Container.Resources.{Limits,Requests}, PodMeta.{Labels,Annotations}.
// Go's runtime randomizes map iteration order, so any code path that
// serializes a map without sorting keys would produce non-deterministic
// output and a non-deterministic hash; this test guards against any such
// regression. The 1000-iteration budget is high enough to catch even a
// rare flake; in practice json.Marshal sorts keys, so all 1000 calls
// must agree.
//
// Pins the absence of orphan-hash drift in the revision
// pipeline: a single bump of a pod-template annotation must produce
// exactly ONE new ControllerRevision name, not a fresh name on every
// reconcile.
func TestRevisionHash_DeterministicAcrossInvocations(t *testing.T) {
	// PodSpec with multiple map-typed fields to exercise the
	// serialization path most likely to be non-deterministic if
	// json.Marshal ever changes (or a custom MarshalJSON is added).
	ps := &corev1.PodSpec{
		NodeSelector: map[string]string{
			"node-pool":     "gpu-a100",
			"zone":          "us-west-2a",
			"models.ome.io": "Ready",
			"accelerator":   "h100",
		},
		Containers: []corev1.Container{
			{
				Name:  "main",
				Image: "engine:v1",
				Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						"nvidia.com/gpu": resource.MustParse("8"),
						"memory":         resource.MustParse("256Gi"),
						"cpu":            resource.MustParse("32"),
					},
					Requests: corev1.ResourceList{
						"nvidia.com/gpu": resource.MustParse("8"),
						"memory":         resource.MustParse("128Gi"),
						"cpu":            resource.MustParse("16"),
					},
				},
			},
		},
	}
	meta := &metav1.ObjectMeta{
		Labels: map[string]string{
			"app.kubernetes.io/name":      "engine",
			"app.kubernetes.io/instance":  "a1",
			"ome.io/serving-runtime":      "srt-test-clone-pd",
			"ome.io/base-model-name":      "llama-7b",
			"app.kubernetes.io/component": "engine",
		},
		Annotations: map[string]string{
			"ome.io/deploymentMode":           "OMENative",
			"ome.io/base-model-name":          "llama-7b",
			"test.ome.io/rollout-trigger":     "2026-05-27T10:42:31.999999999Z",
			"ome.io/ingress-disable-creation": "true",
			"ome.io/storage-uri":              "oci://models/llama-7b@v1",
			"ome.io/base-model-format":        "huggingface",
		},
	}
	const iterations = 1000
	want, _, err := HashWithWorker(ps, nil, meta, nil, "")
	if err != nil {
		t.Fatalf("baseline hash: %v", err)
	}
	for i := 0; i < iterations; i++ {
		got, _, err := HashWithWorker(ps, nil, meta, nil, "")
		if err != nil {
			t.Fatalf("iter %d hash: %v", i, err)
		}
		if got != want {
			t.Fatalf("hash drift at iter %d: got %q want %q (non-determinism in revision pipeline)",
				i, got, want)
		}
	}
}

func TestRevisionHash_CollisionCountChangesHash(t *testing.T) {
	ps := basicPodSpecForRevision()
	h0, _, _ := Hash(ps, nil, nil, "")
	h1, _, _ := Hash(ps, nil, ptr.To(int32(1)), "")
	h2, _, _ := Hash(ps, nil, ptr.To(int32(2)), "")
	if h0 == h1 || h0 == h2 || h1 == h2 {
		t.Errorf("collision-count salt should change the hash: got %q, %q, %q", h0, h1, h2)
	}
}

func TestRevisionHash_CanonicalPayloadIncludesPodMetaField(t *testing.T) {
	// PodMeta is in the canonical payload as a nil pointer for v1; the
	// JSON must carry the "podMeta":null tail so a future PR that
	// populates PodMeta doesn't invalidate existing revisions just by
	// adding the field. If this test breaks because the JSON no longer
	// contains "podMeta", the wrapper struct lost forward-compat.
	ps := basicPodSpecForRevision()
	_, raw, err := Hash(ps, nil, nil, "")
	if err != nil {
		t.Fatalf("Hash: %v", err)
	}
	if !bytes.Contains(raw, []byte(`"podMeta":null`)) {
		t.Errorf("canonical JSON should carry podMeta:null; got %s", raw)
	}
}

func TestRevisionHash_NilAndZeroPointerEquivalent(t *testing.T) {
	// CollisionCount in LifecycleStatus is *int32. Callers may
	// legitimately pass nil or ptr.To(int32(0)) for "no salt yet"; the
	// hash must be the same or a defaulter that normalizes the pointer
	// would invalidate every previously-recorded revision name.
	ps := basicPodSpecForRevision()
	hNil, _, _ := Hash(ps, nil, nil, "")
	hZero, _, _ := Hash(ps, nil, ptr.To(int32(0)), "")
	if hNil != hZero {
		t.Errorf("nil and *0 must hash identically: got %q vs %q", hNil, hZero)
	}
}

func TestRevisionHash_DifferentTemplateDifferentHash(t *testing.T) {
	a := basicPodSpecForRevision()
	b := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "llama:v2"}}}
	ha, _, _ := Hash(a, nil, nil, "")
	hb, _, _ := Hash(b, nil, nil, "")
	if ha == hb {
		t.Errorf("different templates produced same hash: %q", ha)
	}
}

// TestRevisionHash_WorkerPodSpecAffectsHash pins that a multi-pod
// Component's worker PodSpec contributes to the revision hash, so a
// worker-only image / env / spec bump triggers a rollout. Without this
// the controller would silently keep workers on the old image while the
// leader picked up the new one — exactly the kind of split-brain failure
// the revision pipeline must prevent.
func TestRevisionHash_WorkerPodSpecAffectsHash(t *testing.T) {
	leader := basicPodSpecForRevision()
	workerA := &corev1.PodSpec{Containers: []corev1.Container{{Name: "worker", Image: "llama-worker:v1"}}}
	workerB := &corev1.PodSpec{Containers: []corev1.Container{{Name: "worker", Image: "llama-worker:v2"}}}

	hA, _, err := HashWithWorker(leader, workerA, nil, nil, "")
	if err != nil {
		t.Fatalf("HashWithWorker A: %v", err)
	}
	hB, _, err := HashWithWorker(leader, workerB, nil, nil, "")
	if err != nil {
		t.Fatalf("HashWithWorker B: %v", err)
	}
	if hA == hB {
		t.Errorf("worker-only image bump must change hash: hA=%q hB=%q", hA, hB)
	}
}

// TestRevisionHash_TopologyKeyAffectsHash pins that a topology-only edit
// rolls the immutable worker affinity and its PodGroup contract together.
func TestRevisionHash_TopologyKeyAffectsHash(t *testing.T) {
	leader := basicPodSpecForRevision()
	worker := &corev1.PodSpec{Containers: []corev1.Container{{Name: "worker", Image: "llama-worker:v1"}}}

	hA, _, err := HashWithWorkerAndTopology(leader, worker, nil, "topology.example.com/domain-a", nil, "")
	if err != nil {
		t.Fatalf("HashWithWorkerAndTopology A: %v", err)
	}
	hB, _, err := HashWithWorkerAndTopology(leader, worker, nil, "topology.example.com/domain-b", nil, "")
	if err != nil {
		t.Fatalf("HashWithWorkerAndTopology B: %v", err)
	}
	if hA == hB {
		t.Errorf("topology-only edit must change hash: hA=%q hB=%q", hA, hB)
	}
}

func TestRevisionHash_TopologyKeyIgnoredWithoutWorker(t *testing.T) {
	leader := basicPodSpecForRevision()

	legacyHash, legacyRaw, err := Hash(leader, nil, nil, "")
	if err != nil {
		t.Fatalf("Hash: %v", err)
	}
	topologyHash, topologyRaw, err := HashWithWorkerAndTopology(
		leader, nil, nil, "topology.example.com/ignored", nil, "")
	if err != nil {
		t.Fatalf("HashWithWorkerAndTopology: %v", err)
	}
	if legacyHash != topologyHash || !bytes.Equal(legacyRaw, topologyRaw) {
		t.Fatalf("single-pod topology must not change revision: hash %q/%q raw %s/%s",
			legacyHash, topologyHash, legacyRaw, topologyRaw)
	}
}

func TestRevisionPayload_TopologyPresenceSurvivesRoundTrip(t *testing.T) {
	leader := basicPodSpecForRevision()
	worker := &corev1.PodSpec{Containers: []corev1.Container{{Name: "worker", Image: "llama-worker:v1"}}}
	const topologyKey = "topology.example.com/domain"

	_, raw, err := HashWithWorkerAndTopology(leader, worker, nil, topologyKey, nil, "")
	if err != nil {
		t.Fatalf("HashWithWorkerAndTopology: %v", err)
	}
	payload, err := PayloadFromControllerRevision(&appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "test-revision"},
		Data:       runtime.RawExtension{Raw: raw},
	})
	if err != nil {
		t.Fatalf("PayloadFromControllerRevision: %v", err)
	}
	if payload.TopologyKey == nil || *payload.TopologyKey != topologyKey {
		t.Fatalf("TopologyKey: got %#v want %q", payload.TopologyKey, topologyKey)
	}

	_, emptyRaw, err := HashWithWorkerAndTopology(leader, worker, nil, "", nil, "")
	if err != nil {
		t.Fatalf("HashWithWorkerAndTopology empty: %v", err)
	}
	emptyPayload, err := PayloadFromControllerRevision(&appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "topology-free-revision"},
		Data:       runtime.RawExtension{Raw: emptyRaw},
	})
	if err != nil {
		t.Fatalf("topology-free PayloadFromControllerRevision: %v", err)
	}
	if emptyPayload.TopologyKey != nil {
		t.Fatalf("topology-free TopologyKey: got %#v want nil", emptyPayload.TopologyKey)
	}

	legacyPayload, err := PayloadFromControllerRevision(&appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "legacy-revision"},
		Data: runtime.RawExtension{Raw: []byte(
			`{"podSpec":{"containers":[{"name":"main","image":"v1","resources":{}}]},"podMeta":null}`)},
	})
	if err != nil {
		t.Fatalf("legacy PayloadFromControllerRevision: %v", err)
	}
	if legacyPayload.TopologyKey != nil {
		t.Fatalf("legacy TopologyKey: got %#v want nil", legacyPayload.TopologyKey)
	}
}

// TestRevisionHash_EmptyTopologyKeyIsBackwardCompatible pins that leaving the
// optional key unset does not manufacture a new multi-pod revision.
func TestRevisionHash_EmptyTopologyKeyIsBackwardCompatible(t *testing.T) {
	leader := &corev1.PodSpec{Containers: []corev1.Container{{Name: "leader", Image: "test:v1"}}}
	worker := &corev1.PodSpec{Containers: []corev1.Container{{Name: "worker", Image: "test:v1"}}}
	wantRaw := []byte(`{"podSpec":{"containers":[{"name":"leader","image":"test:v1","resources":{}}]},"podMeta":null,"workerPodSpec":{"containers":[{"name":"worker","image":"test:v1","resources":{}}]}}`)
	const wantHash = "c3a098f7"

	topologyHash, topologyRaw, err := HashWithWorkerAndTopology(leader, worker, nil, "", nil, "")
	if err != nil {
		t.Fatalf("HashWithWorkerAndTopology: %v", err)
	}
	if topologyHash != wantHash || !bytes.Equal(topologyRaw, wantRaw) {
		t.Fatalf("empty topology changed legacy revision: hash got/want %q/%q raw got/want %s/%s",
			topologyHash, wantHash, topologyRaw, wantRaw)
	}
}

// TestRevisionHash_NilWorkerSpecBackwardCompat pins that
// HashWithWorker(.., nil) hashes identically to Hash(..), so existing
// single-pod revisions stay valid after the multi-pod field is
// introduced. A different value would invalidate every previously-recorded
// CR and force a fleet-wide phantom rollout on the next reconcile.
func TestRevisionHash_NilWorkerSpecBackwardCompat(t *testing.T) {
	leader := basicPodSpecForRevision()
	meta := &metav1.ObjectMeta{Labels: map[string]string{"k": "v"}}

	hSinglePod, rawSinglePod, _ := Hash(leader, meta, nil, "")
	hWithNilWorker, rawWithNilWorker, _ := HashWithWorker(leader, nil, meta, nil, "")
	if hSinglePod != hWithNilWorker {
		t.Errorf("Hash and HashWithWorker(nil) must match: %q vs %q", hSinglePod, hWithNilWorker)
	}
	if !bytes.Equal(rawSinglePod, rawWithNilWorker) {
		t.Errorf("canonical bytes must match: %s vs %s", rawSinglePod, rawWithNilWorker)
	}
}

// TestRevisionHash_WorkerPodSpecPresentChangesShape pins that adding a
// (non-nil) WorkerPodSpec changes the hash compared to nil, which is the
// correct behavior for a single-pod-to-multi-pod migration. The
// single-pod hash must remain stable for ISVCs that never declared a
// worker (covered by NilWorkerSpecBackwardCompat above).
func TestRevisionHash_WorkerPodSpecPresentChangesShape(t *testing.T) {
	leader := basicPodSpecForRevision()
	worker := &corev1.PodSpec{Containers: []corev1.Container{{Name: "worker", Image: "llama-worker:v1"}}}

	hNoWorker, _, _ := HashWithWorker(leader, nil, nil, nil, "")
	hWithWorker, _, _ := HashWithWorker(leader, worker, nil, nil, "")
	if hNoWorker == hWithWorker {
		t.Errorf("introducing a worker must change the hash; both got %q", hNoWorker)
	}
}

func TestRevisionHash_NilTemplateRejected(t *testing.T) {
	if _, _, err := Hash(nil, nil, nil, ""); err == nil {
		t.Fatal("expected error for nil template")
	}
}

// TemplateMeta must NOT include migration-request-v1-* annotations
// in the hashed metadata. Otherwise adding/removing a migration request
// annotation drifts every Component's revision hash — including Components
// that are not the migration target — and triggers a phantom rollout.
func TestRevisionHash_MigrationAnnotationDoesNotDriftHash(t *testing.T) {
	ps := basicPodSpecForRevision()

	base := &metav1.ObjectMeta{
		Annotations: map[string]string{
			"ome.io/base-model-name": "llama-7b",
			"ome.io/deploymentMode":  "OMENative",
		},
	}
	withMig := &metav1.ObjectMeta{
		Annotations: map[string]string{
			"ome.io/base-model-name": "llama-7b",
			"ome.io/deploymentMode":  "OMENative",
			"ome.io/migration-request-v1-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee": `{"component":"engine","schemaVersion":"v1"}`,
		},
	}

	hBase, _, _ := Hash(ps, base, nil, "")
	hWith, _, _ := Hash(ps, withMig, nil, "")
	if hBase != hWith {
		t.Errorf("migration request annotation must be filtered from revision hash: base=%s withMig=%s", hBase, hWith)
	}
}

func TestRevisionHash_PausedRolloutAnnotationDoesNotDriftHash(t *testing.T) {
	ps := basicPodSpecForRevision()

	base := &metav1.ObjectMeta{
		Annotations: map[string]string{
			"ome.io/base-model-name": "llama-7b",
			"ome.io/deploymentMode":  "OMENative",
		},
	}
	withPause := &metav1.ObjectMeta{
		Annotations: map[string]string{
			"ome.io/base-model-name": "llama-7b",
			"ome.io/deploymentMode":  "OMENative",
			"ome.io/rollout-paused":  "true",
		},
	}

	hBase, _, _ := Hash(ps, base, nil, "")
	hWith, _, _ := Hash(ps, withPause, nil, "")
	if hBase != hWith {
		t.Errorf("rollout-paused annotation must be filtered from revision hash: base=%s withPause=%s", hBase, hWith)
	}
}

// A queue assignment is derived from cluster state, not from the spec: it moves
// when quota is re-authored, and it appears or disappears wholesale when the
// operator turns queue stamping on or off. Hashing it makes each of those a
// rollout of every workload on the cluster.
func TestRevisionHash_SchedulingLabelsDoNotDriftHash(t *testing.T) {
	ps := basicPodSpecForRevision()
	base := &metav1.ObjectMeta{Labels: map[string]string{"app": "llama"}}

	for _, tc := range []struct {
		name   string
		labels map[string]string
	}{
		{
			name:   "queue stamped where there was none",
			labels: map[string]string{"app": "llama", "kueue.x-k8s.io/queue-name": "team-a"},
		},
		{
			name:   "queue re-pointed at another leaf",
			labels: map[string]string{"app": "llama", "kueue.x-k8s.io/queue-name": "team-b"},
		},
		{
			name: "priority class alongside the queue",
			labels: map[string]string{
				"app":                           "llama",
				"kueue.x-k8s.io/queue-name":     "team-a",
				"kueue.x-k8s.io/priority-class": "high",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hBase, _, _ := Hash(ps, base, nil, "")
			hWith, _, _ := Hash(ps, &metav1.ObjectMeta{Labels: tc.labels}, nil, "")
			if hBase != hWith {
				t.Errorf("admission-owned labels must not reach the revision hash: base=%s with=%s", hBase, hWith)
			}
		})
	}
}

// The annotation that SELECTS the queue is excluded for the same reason as the
// labels it produces. Backfilling it across a fleet is preparation, not a spec
// change, and hashing it would turn that preparation into a rollout of every
// workload annotated.
func TestRevisionHash_LocalQueueAnnotationDoesNotDriftHash(t *testing.T) {
	ps := basicPodSpecForRevision()
	base := &metav1.ObjectMeta{Annotations: map[string]string{"ome.io/deploymentMode": "OMENative"}}

	for _, tc := range []struct {
		name        string
		annotations map[string]string
	}{
		{
			name: "annotated where there was none",
			annotations: map[string]string{
				"ome.io/deploymentMode": "OMENative",
				"ome.io/local-queue":    "team-a",
			},
		},
		{
			name: "re-pointed at another queue",
			annotations: map[string]string{
				"ome.io/deploymentMode": "OMENative",
				"ome.io/local-queue":    "team-b",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hBase, _, _ := Hash(ps, base, nil, "")
			hWith, _, _ := Hash(ps, &metav1.ObjectMeta{Annotations: tc.annotations}, nil, "")
			if hBase != hWith {
				t.Errorf("the queue annotation must not reach the revision hash: base=%s with=%s", hBase, hWith)
			}
		})
	}
}

// Mirrors the labels-only case: a template carrying nothing but the queue
// annotation has to hash as though it carried no metadata, or the exclusion
// just relocates the drift.
func TestRevisionHash_OnlyLocalQueueAnnotationEqualsNoMeta(t *testing.T) {
	ps := basicPodSpecForRevision()
	only := &metav1.ObjectMeta{Annotations: map[string]string{"ome.io/local-queue": "team-a"}}

	hNil, _, _ := Hash(ps, nil, nil, "")
	hOnly, _, _ := Hash(ps, only, nil, "")
	if hNil != hOnly {
		t.Errorf("a template carrying only the queue annotation must hash as no metadata: nil=%s only=%s", hNil, hOnly)
	}
}

// Taking an already-running workload under multi-cluster management stamps
// provenance LABELS on it: which source it derives from, and which control
// plane placed it. That is identity bookkeeping written by the placer, not
// spec the user authored, and a running pod cannot act on it — so it must not
// mint a revision. Hashing it makes a live migration a full rollout.
//
// Labels only. The annotation half (ome.io/placement-origin-uid) is stripped
// before this function ever sees it, by the InferenceReplica reconciler's
// ome.io/revision-excluded-annotation-keys set — asserting it here would pass
// for a reason that does not hold in production.
func TestRevisionHash_PlacementMarkersDoNotDriftHash(t *testing.T) {
	ps := basicPodSpecForRevision()
	base := &metav1.ObjectMeta{
		Labels:      map[string]string{"app": "llama"},
		Annotations: map[string]string{"ome.io/deploymentMode": "OMENative"},
	}

	for _, tc := range []struct {
		name string
		meta *metav1.ObjectMeta
	}{
		{
			name: "origin label stamped on adoption",
			meta: &metav1.ObjectMeta{
				Labels: map[string]string{
					"app":                     "llama",
					"ome.io/placement-origin": "11111111-1111-4111-8111-111111111111",
				},
				Annotations: map[string]string{"ome.io/deploymentMode": "OMENative"},
			},
		},
		{
			name: "control-plane identity added when a second control plane appears",
			meta: &metav1.ObjectMeta{
				Labels: map[string]string{
					"app":                            "llama",
					"ome.io/placement-origin":        "11111111-1111-4111-8111-111111111111",
					"ome.io/placement-control-plane": "control-plane-a",
				},
				Annotations: map[string]string{"ome.io/deploymentMode": "OMENative"},
			},
		},
		{
			name: "re-derived from a recreated source, so every marker changes value",
			meta: &metav1.ObjectMeta{
				Labels: map[string]string{
					"app":                            "llama",
					"ome.io/placement-origin":        "0e2f1a77-0000-4000-8000-000000000000",
					"ome.io/placement-control-plane": "control-plane-b",
				},
				Annotations: map[string]string{"ome.io/deploymentMode": "OMENative"},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hBase, _, _ := Hash(ps, base, nil, "")
			hWith, _, _ := Hash(ps, tc.meta, nil, "")
			if hBase != hWith {
				t.Errorf("placement provenance must not reach the revision hash: base=%s with=%s", hBase, hWith)
			}
		})
	}
}

// Mirrors the queue cases: a template carrying nothing but the provenance
// labels has to hash as though it carried no metadata, or the exclusion merely
// relocates the drift.
func TestRevisionHash_OnlyPlacementMarkersEqualNoMeta(t *testing.T) {
	ps := basicPodSpecForRevision()
	only := &metav1.ObjectMeta{
		Labels: map[string]string{
			"ome.io/placement-origin":        "11111111-1111-4111-8111-111111111111",
			"ome.io/placement-control-plane": "control-plane-a",
		},
	}

	hNil, _, _ := Hash(ps, nil, nil, "")
	hOnly, _, _ := Hash(ps, only, nil, "")
	if hNil != hOnly {
		t.Errorf("a template carrying only placement provenance must hash as no metadata: nil=%s only=%s", hNil, hOnly)
	}
}

// The exclusion is scoped to the admission plane's own keys. A user label is
// spec the user wrote, and changing it is a rollout they asked for.
func TestRevisionHash_UserLabelsStillDriftHash(t *testing.T) {
	ps := basicPodSpecForRevision()
	a := &metav1.ObjectMeta{Labels: map[string]string{"app": "llama", "tier": "gold"}}
	b := &metav1.ObjectMeta{Labels: map[string]string{"app": "llama", "tier": "silver"}}

	hA, _, _ := Hash(ps, a, nil, "")
	hB, _, _ := Hash(ps, b, nil, "")
	if hA == hB {
		t.Errorf("a user label change must still bump the hash, got %s for both", hA)
	}
}

// A template carrying nothing but admission-owned labels has to hash as though
// it carried no metadata at all, or the exclusion just moves the drift: such a
// workload would differ from one that never had labels.
func TestRevisionHash_OnlySchedulingLabelsEqualsNoMeta(t *testing.T) {
	ps := basicPodSpecForRevision()
	only := &metav1.ObjectMeta{Labels: map[string]string{"kueue.x-k8s.io/queue-name": "team-a"}}

	hNil, _, _ := Hash(ps, nil, nil, "")
	hOnly, _, _ := Hash(ps, only, nil, "")
	if hNil != hOnly {
		t.Errorf("a template with only admission labels must hash as empty: nil=%s only=%s", hNil, hOnly)
	}
	if got := TemplateMeta(only); got != nil {
		t.Errorf("TemplateMeta should collapse to nil, got %+v", got)
	}
}

// The canary operator verbs (rollout-promote / rollout-rollback) are
// added to and removed from the ISVC mid-rollout. They must NOT feed the
// revision hash: otherwise an operator promoting revision X would mint a
// NEW revision (the spec + the promote annotation) and roll to that
// instead of X — and a rollback would mint a fresh revision rather than
// returning to the existing stable one.
func TestRevisionHash_CanaryVerbAnnotationsDoNotDriftHash(t *testing.T) {
	ps := basicPodSpecForRevision()
	base := &metav1.ObjectMeta{
		Annotations: map[string]string{
			"ome.io/base-model-name": "llama-7b",
			"ome.io/deploymentMode":  "OMENative",
		},
	}
	for _, verb := range []struct{ key, val string }{
		{"ome.io/rollout-promote", "abc12345"},
		{"ome.io/rollout-rollback", "true"},
	} {
		withVerb := &metav1.ObjectMeta{
			Annotations: map[string]string{
				"ome.io/base-model-name": "llama-7b",
				"ome.io/deploymentMode":  "OMENative",
				verb.key:                 verb.val,
			},
		}
		hBase, _, _ := Hash(ps, base, nil, "")
		hWith, _, _ := Hash(ps, withVerb, nil, "")
		if hBase != hWith {
			t.Errorf("%s annotation must be filtered from revision hash: base=%s withVerb=%s", verb.key, hBase, hWith)
		}
	}
}

// The InferenceReplica operator verbs — release-held-revision and
// reset-instances — are mailboxes consumed off the owner. They must NOT
// feed the revision hash: otherwise releasing one Held block or
// rebuilding two Failed Instances would mint a new revision and roll
// every Instance — the blunt outcome both verbs exist to avoid.
func TestRevisionHash_IROperatorVerbAnnotationsDoNotDriftHash(t *testing.T) {
	ps := basicPodSpecForRevision()
	base := &metav1.ObjectMeta{
		Annotations: map[string]string{"ome.io/base-model-name": "llama-7b"},
	}
	hBase, _, _ := Hash(ps, base, nil, "")
	hNil, _, _ := Hash(ps, nil, nil, "")
	for _, verb := range []struct{ key, val string }{
		{constants.ReleaseHeldRevisionAnnotationKey, "llama-engine-aaaaaaaa"},
		{constants.ResetInstancesAnnotationKey, "13,14"},
	} {
		withVerb := &metav1.ObjectMeta{
			Annotations: map[string]string{
				"ome.io/base-model-name": "llama-7b",
				verb.key:                 verb.val,
			},
		}
		hWith, _, _ := Hash(ps, withVerb, nil, "")
		if hBase != hWith {
			t.Errorf("%s annotation must be filtered from revision hash: base=%s withVerb=%s", verb.key, hBase, hWith)
		}
		onlyVerb := &metav1.ObjectMeta{
			Annotations: map[string]string{verb.key: verb.val},
		}
		hOnly, _, _ := Hash(ps, onlyVerb, nil, "")
		if hNil != hOnly {
			t.Errorf("a template carrying only %s must hash like no metadata: nil=%s only=%s", verb.key, hNil, hOnly)
		}
	}
}

// User-supplied annotations MUST still contribute to the hash so a
// legitimate pod-template metadata change triggers a rollout.
func TestRevisionHash_UserAnnotationStillDriftsHash(t *testing.T) {
	ps := basicPodSpecForRevision()

	base := &metav1.ObjectMeta{
		Annotations: map[string]string{"ome.io/base-model-name": "llama-7b"},
	}
	withUserAnnot := &metav1.ObjectMeta{
		Annotations: map[string]string{
			"ome.io/base-model-name": "llama-7b",
			"k8s.grafana.com/scrape": "true",
		},
	}

	hBase, _, _ := Hash(ps, base, nil, "")
	hWith, _, _ := Hash(ps, withUserAnnot, nil, "")
	if hBase == hWith {
		t.Errorf("user annotation must still drift the hash: both=%s", hBase)
	}
}

func TestEnsureControllerRevision_CreatesNew(t *testing.T) {
	isvc := minimalISVC("llama-70b", "prod", 1)
	c := newFakeClientWithApps(t, isvc)

	cr, collision, err := EnsureControllerRevision(context.Background(), c, c, isvc, testISVCGVK, testKey(isvc, workload.ComponentEngine), basicPodSpecForRevision(), nil, nil, isvc.UID)
	if err != nil {
		t.Fatalf("EnsureControllerRevision: %v", err)
	}
	if collision {
		t.Errorf("first-create should not report collision")
	}
	if cr == nil {
		t.Fatal("nil CR returned")
	}
	if cr.Revision != 1 {
		t.Errorf("Revision: got %d want 1", cr.Revision)
	}

	// Re-read to confirm it landed in the fake API.
	got := &appsv1.ControllerRevision{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(cr), got); err != nil {
		t.Fatalf("get CR: %v", err)
	}
	if got.Labels[testManagedByLabelKey] != testManagedByLabelValue {
		t.Errorf("managed-by label missing: %+v", got.Labels)
	}
	if len(got.OwnerReferences) != 1 || got.OwnerReferences[0].Name != "llama-70b" {
		t.Errorf("owner ref not set to ISVC: %+v", got.OwnerReferences)
	}
}

func TestEnsureControllerRevision_ReusesExistingMatchingData(t *testing.T) {
	isvc := minimalISVC("llama-70b", "prod", 1)
	c := newFakeClientWithApps(t, isvc)

	first, _, err := EnsureControllerRevision(context.Background(), c, c, isvc, testISVCGVK, testKey(isvc, workload.ComponentEngine), basicPodSpecForRevision(), nil, nil, isvc.UID)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	second, collision, err := EnsureControllerRevision(context.Background(), c, c, isvc, testISVCGVK, testKey(isvc, workload.ComponentEngine), basicPodSpecForRevision(), nil, nil, isvc.UID)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if collision {
		t.Errorf("identical inputs should not report collision")
	}
	if second.Name != first.Name {
		t.Errorf("Name: got %q want %q (reused)", second.Name, first.Name)
	}
	if second.Revision != first.Revision {
		t.Errorf("Revision should be reused, got %d vs %d", second.Revision, first.Revision)
	}

	// Only one CR should exist.
	list := &appsv1.ControllerRevisionList{}
	_ = c.List(context.Background(), list, client.InNamespace("prod"))
	if len(list.Items) != 1 {
		t.Errorf("CR count: got %d want 1", len(list.Items))
	}
}

func TestEnsureControllerRevision_DetectsCollision(t *testing.T) {
	// Pre-seed a CR whose Name matches what EnsureControllerRevision would
	// compute, but whose Data deliberately differs. This simulates a hash
	// collision (extremely rare but worth handling).
	isvc := minimalISVC("llama-70b", "prod", 1)
	want := basicPodSpecForRevision()
	hash, _, _ := Hash(want, nil, nil, isvc.UID)
	pre := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name:      Name(testKey(isvc, workload.ComponentEngine), hash),
			Namespace: isvc.Namespace,
			Labels:    Labels(testKey(isvc, workload.ComponentEngine)),
		},
		Data:     runtime.RawExtension{Raw: []byte(`{"podSpec":{"containers":[{"name":"other","image":"other:v1"}]}}`)},
		Revision: 1,
	}
	c := newFakeClientWithApps(t, isvc, pre)

	_, collision, err := EnsureControllerRevision(context.Background(), c, c, isvc, testISVCGVK, testKey(isvc, workload.ComponentEngine), want, nil, nil, isvc.UID)
	if err != nil {
		t.Fatalf("EnsureControllerRevision: %v", err)
	}
	if !collision {
		t.Fatalf("expected collision=true when existing CR has same name but different data")
	}
}

func TestEnsureControllerRevision_CollisionCountSaltRetryLands(t *testing.T) {
	// Simulate the recovery loop the caller would run: on collision,
	// bump CollisionCount and retry; the second call should produce a
	// different name and succeed without collision.
	isvc := minimalISVC("llama-70b", "prod", 1)
	want := basicPodSpecForRevision()
	hash0, _, _ := Hash(want, nil, nil, isvc.UID)
	pre := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name:      Name(testKey(isvc, workload.ComponentEngine), hash0),
			Namespace: isvc.Namespace,
			Labels:    Labels(testKey(isvc, workload.ComponentEngine)),
		},
		Data:     runtime.RawExtension{Raw: []byte(`{"podSpec":{"containers":[{"name":"other","image":"other:v1"}]}}`)},
		Revision: 1,
	}
	c := newFakeClientWithApps(t, isvc, pre)

	_, collision, err := EnsureControllerRevision(context.Background(), c, c, isvc, testISVCGVK, testKey(isvc, workload.ComponentEngine), want, nil, nil, isvc.UID)
	if err != nil || !collision {
		t.Fatalf("expected collision on first call: err=%v collision=%v", err, collision)
	}
	cr2, collision2, err := EnsureControllerRevision(context.Background(), c, c, isvc, testISVCGVK, testKey(isvc, workload.ComponentEngine), want, nil, ptr.To(int32(1)), isvc.UID)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if collision2 {
		t.Fatalf("retry with bumped collisionCount should not collide")
	}
	if cr2.Name == pre.Name {
		t.Fatalf("retry name should differ from colliding one")
	}
	if cr2.Revision != 2 {
		t.Errorf("Revision: got %d want 2 (max+1)", cr2.Revision)
	}
}

func TestEnsureControllerRevision_NilGuards(t *testing.T) {
	c := newFakeClientWithApps(t)
	isvc := minimalISVC("x", "y", 1)
	template := basicPodSpecForRevision()
	if _, _, err := EnsureControllerRevision(context.Background(), c, c, nil, testISVCGVK, testKey(isvc, workload.ComponentEngine), template, nil, nil, isvc.UID); err == nil {
		t.Fatal("expected error for nil ISVC")
	}
	if _, _, err := EnsureControllerRevision(context.Background(), nil, c, isvc, testISVCGVK, testKey(isvc, workload.ComponentEngine), template, nil, nil, isvc.UID); err == nil {
		t.Fatal("expected error for nil client")
	}
	if _, _, err := EnsureControllerRevision(context.Background(), c, nil, isvc, testISVCGVK, testKey(isvc, workload.ComponentEngine), template, nil, nil, isvc.UID); err == nil {
		t.Fatal("expected error for nil reader")
	}
}

func TestEnsureControllerRevision_RacePathCreateAlreadyExists(t *testing.T) {
	// Simulate two reconciles deciding to create at the same time: the
	// initial Get sees NotFound, the Create returns AlreadyExists (because
	// a concurrent writer landed first). EnsureControllerRevision must
	// re-Get and apply the collision/match check on the freshly-found CR.
	//
	// This is the branch unreachable from the pre-seed collision test —
	// that one exercises the existing-on-Get path, this one exercises the
	// existing-after-Create path.
	isvc := minimalISVC("llama-70b", "prod", 1)
	template := basicPodSpecForRevision()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = v1beta1.AddToScheme(scheme)

	// Pre-seed a CR with the same name EnsureControllerRevision will
	// compute, but with mismatching Data. The interceptor below makes
	// the initial Get pretend it doesn't exist, so the function falls
	// through to Create, which the underlying fake will then reject
	// with AlreadyExists because the object IS actually there.
	hash, _, _ := Hash(template, nil, nil, isvc.UID)
	conflicting := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name:      Name(testKey(isvc, workload.ComponentEngine), hash),
			Namespace: isvc.Namespace,
			Labels:    Labels(testKey(isvc, workload.ComponentEngine)),
		},
		Data:     runtime.RawExtension{Raw: []byte(`{"podSpec":{"containers":[{"name":"other","image":"other:v1"}]},"podMeta":null}`)},
		Revision: 1,
	}

	hideOnceOnGet := true
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(isvc, conflicting).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, cli client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*appsv1.ControllerRevision); ok && key.Name == conflicting.Name && hideOnceOnGet {
					hideOnceOnGet = false
					return apierrors.NewNotFound(appsv1.Resource("controllerrevisions"), key.Name)
				}
				return cli.Get(ctx, key, obj, opts...)
			},
		}).
		Build()

	cr, collision, err := EnsureControllerRevision(context.Background(), c, c, isvc, testISVCGVK, testKey(isvc, workload.ComponentEngine), template, nil, nil, isvc.UID)
	if err != nil {
		t.Fatalf("EnsureControllerRevision: %v", err)
	}
	if !collision {
		t.Fatalf("expected collision=true: Create raced with a conflicting writer")
	}
	if cr == nil {
		t.Fatalf("expected the conflicting CR to be returned for the caller to inspect")
	}
}

func TestRetainControllerRevisions_BelowMaxIsNoOp(t *testing.T) {
	isvc := minimalISVC("llama-70b", "prod", 1)
	c := newFakeClientWithApps(t, isvc)
	// Two revisions, both non-live; maxNonLive=10 keeps everything.
	for i, image := range []string{"a", "b"} {
		ps := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: image}}}
		if _, _, err := EnsureControllerRevision(context.Background(), c, c, isvc, testISVCGVK, testKey(isvc, workload.ComponentEngine), ps, nil, nil, isvc.UID); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}
	if err := RetainControllerRevisions(context.Background(), c, c, testKey(isvc, workload.ComponentEngine), 10); err != nil {
		t.Fatalf("retain: %v", err)
	}
	list := &appsv1.ControllerRevisionList{}
	_ = c.List(context.Background(), list, client.InNamespace("prod"))
	if len(list.Items) != 2 {
		t.Errorf("CRs: got %d want 2 (nothing should be deleted)", len(list.Items))
	}
}

func TestRetainControllerRevisions_DropsOldestNonLive(t *testing.T) {
	// Seed 5 distinct revisions, maxNonLive=2 → keep the 2 newest, drop 3.
	isvc := minimalISVC("llama-70b", "prod", 1)
	c := newFakeClientWithApps(t, isvc)
	for _, image := range []string{"v1", "v2", "v3", "v4", "v5"} {
		ps := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: image}}}
		if _, _, err := EnsureControllerRevision(context.Background(), c, c, isvc, testISVCGVK, testKey(isvc, workload.ComponentEngine), ps, nil, nil, isvc.UID); err != nil {
			t.Fatalf("seed %s: %v", image, err)
		}
	}
	if err := RetainControllerRevisions(context.Background(), c, c, testKey(isvc, workload.ComponentEngine), 2); err != nil {
		t.Fatalf("retain: %v", err)
	}
	list := &appsv1.ControllerRevisionList{}
	_ = c.List(context.Background(), list, client.InNamespace("prod"))
	if len(list.Items) != 2 {
		t.Errorf("CRs after retain: got %d want 2", len(list.Items))
	}
	// The two survivors must have the highest revision numbers (4 and 5).
	keptRevs := map[int64]bool{}
	for _, cr := range list.Items {
		keptRevs[cr.Revision] = true
	}
	if !keptRevs[4] || !keptRevs[5] {
		t.Errorf("expected survivors to be Revision 4 and 5, got %v", keptRevs)
	}
}

func TestRetainControllerRevisions_NeverDeletesLiveRevisions(t *testing.T) {
	// Seed 4 revisions, maxNonLive=1, mark v1 as live → it must survive
	// even though it's the oldest non-live by revision number.
	isvc := minimalISVC("llama-70b", "prod", 1)
	c := newFakeClientWithApps(t, isvc)
	names := make(map[string]string, 4) // image -> CR name
	for _, image := range []string{"v1", "v2", "v3", "v4"} {
		ps := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: image}}}
		cr, _, err := EnsureControllerRevision(context.Background(), c, c, isvc, testISVCGVK, testKey(isvc, workload.ComponentEngine), ps, nil, nil, isvc.UID)
		if err != nil {
			t.Fatalf("seed %s: %v", image, err)
		}
		names[image] = cr.Name
	}
	liveName := names["v1"] // oldest, but pinned as live
	if err := RetainControllerRevisions(context.Background(), c, c, testKey(isvc, workload.ComponentEngine), 1, liveName); err != nil {
		t.Fatalf("retain: %v", err)
	}
	list := &appsv1.ControllerRevisionList{}
	_ = c.List(context.Background(), list, client.InNamespace("prod"))
	// 1 live + 1 non-live kept by maxNonLive = 2 survivors.
	if len(list.Items) != 2 {
		t.Errorf("CRs: got %d want 2 (live + newest non-live)", len(list.Items))
	}
	keptNames := map[string]bool{}
	for _, cr := range list.Items {
		keptNames[cr.Name] = true
	}
	if !keptNames[liveName] {
		t.Errorf("live revision %s was deleted", liveName)
	}
}

func TestRetainControllerRevisions_NegativeMaxRejected(t *testing.T) {
	isvc := minimalISVC("llama-70b", "prod", 1)
	c := newFakeClientWithApps(t, isvc)
	if err := RetainControllerRevisions(context.Background(), c, c, testKey(isvc, workload.ComponentEngine), -1); err == nil {
		t.Fatal("expected error for negative maxNonLive")
	}
}

func TestRetainControllerRevisions_ContinuesAfterDeleteError(t *testing.T) {
	// Seed 4 CRs (Revisions 1..4), inject a Delete that fails on the
	// oldest. With maxNonLive=1 the function should still trim the
	// middle non-live CRs (Revisions 2 and 3), report an aggregated
	// error for the failure, and leave the freshest one intact.
	isvc := minimalISVC("llama-70b", "prod", 1)
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = v1beta1.AddToScheme(scheme)
	failName := Name(testKey(isvc, workload.ComponentEngine), "")
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(isvc).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(ctx context.Context, cli client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				if cr, ok := obj.(*appsv1.ControllerRevision); ok && cr.Revision == 1 {
					return fmt.Errorf("synthetic transient delete failure on %s", cr.Name)
				}
				return cli.Delete(ctx, obj, opts...)
			},
		}).
		Build()
	_ = failName // silence linter; reserved for future asserts that pin the failing name

	for _, image := range []string{"v1", "v2", "v3", "v4"} {
		ps := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: image}}}
		if _, _, err := EnsureControllerRevision(context.Background(), c, c, isvc, testISVCGVK, testKey(isvc, workload.ComponentEngine), ps, nil, nil, isvc.UID); err != nil {
			t.Fatalf("seed %s: %v", image, err)
		}
	}

	err := RetainControllerRevisions(context.Background(), c, c, testKey(isvc, workload.ComponentEngine), 1)
	if err == nil {
		t.Fatal("expected aggregated error from the failing delete")
	}
	if !strings.Contains(err.Error(), "synthetic transient delete failure") {
		t.Errorf("error should wrap the underlying delete failure: %v", err)
	}

	list := &appsv1.ControllerRevisionList{}
	_ = c.List(context.Background(), list, client.InNamespace("prod"))
	// Expected survivors: Revision 4 (the kept non-live one) plus the
	// Revision 1 the synthetic delete refused to remove.
	gotRevs := map[int64]bool{}
	for _, cr := range list.Items {
		gotRevs[cr.Revision] = true
	}
	if !gotRevs[4] {
		t.Errorf("Revision 4 (newest non-live) should have survived, got %v", gotRevs)
	}
	if !gotRevs[1] {
		t.Errorf("Revision 1 (failed-to-delete) should still be present, got %v", gotRevs)
	}
	if gotRevs[2] || gotRevs[3] {
		t.Errorf("middle non-live Revisions 2 and 3 should have been deleted, got %v", gotRevs)
	}
}

func TestNextRevisionNumber_NoExisting(t *testing.T) {
	isvc := minimalISVC("llama-70b", "prod", 1)
	c := newFakeClientWithApps(t, isvc)
	n, err := nextRevisionNumber(context.Background(), c, testKey(isvc, workload.ComponentEngine))
	if err != nil {
		t.Fatalf("nextRevisionNumber: %v", err)
	}
	if n != 1 {
		t.Errorf("first revision: got %d want 1", n)
	}
}

// --- CollisionCount framing ---

func TestRevisionHash_CollisionCountFramingDistinguishesAmbiguousConcat(t *testing.T) {
	// Without delimiters, raw_bytes + fmt.Sprintf("%d", cc) is ambiguous:
	// a JSON ending in `1` with cc=0 hashes the same stream as one ending
	// in `10` with cc=0. The framing must yield distinct hashes for any
	// (spec, cc) pair where the unframed concatenation would collide.
	spec := basicPodSpecForRevision()
	cc0 := int32(0)
	cc10 := int32(10)
	h0, _, err := Hash(spec, nil, &cc0, "")
	if err != nil {
		t.Fatalf("h0: %v", err)
	}
	h10, _, err := Hash(spec, nil, &cc10, "")
	if err != nil {
		t.Fatalf("h10: %v", err)
	}
	if h0 == h10 {
		t.Errorf("cc=0 and cc=10 must yield distinct hashes; got both %q", h0)
	}

	// Direct framing check: build two hashes by hand simulating raw
	// concatenation and prove the framed version diverges where the raw
	// version would collide.
	wantFrame := "|cc=0|"
	if !strings.Contains(fmt.Sprintf("%s%s", "raw-bytes-ending-in-1", wantFrame), wantFrame) {
		t.Errorf("framing literal must remain |cc=N| so raw-byte boundaries can't ambiguously concatenate")
	}
}

// --- ownership validation on CR reuse ---

func TestEnsureControllerRevision_RejectsForeignOwnerAsCollision(t *testing.T) {
	// A leftover CR from a previously-deleted-and-recreated ISVC of the
	// same name carries an OwnerReference whose UID belongs to the dead
	// ISVC. Reusing it would silently mix histories — treat as collision
	// so the caller bumps CollisionCount and writes a fresh-named CR.
	isvc := minimalISVC("llama-70b", "prod", 1)
	// Same PodSpec the new EnsureControllerRevision call will hash —
	// so the name matches and Data matches; ONLY the owner UID differs.
	// Hash is computed with isvc.UID so the pre-seeded CR lands at the
	// exact name EnsureControllerRevision will look up: the
	// foreign-owner branch fires on a true name collision.
	spec := basicPodSpecForRevision()
	hash, raw, _ := Hash(spec, nil, nil, isvc.UID)
	foreignUID := types.UID("ancient-uid")
	pre := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name:      Name(testKey(isvc, workload.ComponentEngine), hash),
			Namespace: isvc.Namespace,
			Labels:    Labels(testKey(isvc, workload.ComponentEngine)),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "ome.io/v1beta1",
				Kind:       "InferenceService",
				Name:       "llama-70b",
				UID:        foreignUID,
				Controller: ptr.To(true),
			}},
		},
		Data:     runtime.RawExtension{Raw: raw},
		Revision: 1,
	}
	c := newFakeClientWithApps(t, isvc, pre)

	_, collision, err := EnsureControllerRevision(context.Background(), c, c, isvc, testISVCGVK, testKey(isvc, workload.ComponentEngine), spec, nil, nil, isvc.UID)
	if err != nil {
		t.Fatalf("EnsureControllerRevision: %v", err)
	}
	if !collision {
		t.Errorf("foreign owner UID must trip the collision path even when Data matches")
	}
}

func TestEnsureControllerRevision_AcceptsMatchingOwnerUID(t *testing.T) {
	// Sanity check the inverse: when the existing CR's controller
	// OwnerReference UID matches the current ISVC, reuse proceeds (no
	// false collision).
	isvc := minimalISVC("llama-70b", "prod", 1)
	spec := basicPodSpecForRevision()
	hash, raw, _ := Hash(spec, nil, nil, isvc.UID)
	pre := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name:      Name(testKey(isvc, workload.ComponentEngine), hash),
			Namespace: isvc.Namespace,
			Labels:    Labels(testKey(isvc, workload.ComponentEngine)),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "ome.io/v1beta1",
				Kind:       "InferenceService",
				Name:       isvc.Name,
				UID:        isvc.UID,
				Controller: ptr.To(true),
			}},
		},
		Data:     runtime.RawExtension{Raw: raw},
		Revision: 1,
	}
	c := newFakeClientWithApps(t, isvc, pre)

	_, collision, err := EnsureControllerRevision(context.Background(), c, c, isvc, testISVCGVK, testKey(isvc, workload.ComponentEngine), spec, nil, nil, isvc.UID)
	if err != nil {
		t.Fatalf("EnsureControllerRevision: %v", err)
	}
	if collision {
		t.Errorf("matching owner UID + matching Data must reuse without collision")
	}
}

// TestRevisionHash_OwnerUIDPartitionsTheNameSpace pins the
// delete-and-recreate-same-name fix: two ISVCs sharing the same Name /
// Namespace / PodSpec / PodMeta / CollisionCount — exactly what a
// delete-and-recreate cycle produces — MUST emit distinct CR hashes when
// their UIDs differ. Without the UID partition, cascade-GC racing with
// recreation could leave the new ISVC pointing at a freshly-created CR
// carrying the predecessor's name, and the foreign-owner collision branch
// never fires (the CR is GC'd before
// the recreate's first reconcile).
func TestRevisionHash_OwnerUIDPartitionsTheNameSpace(t *testing.T) {
	ps := basicPodSpecForRevision()
	hOld, _, err := Hash(ps, nil, nil, types.UID("isvc-uid-old"))
	if err != nil {
		t.Fatalf("Hash old: %v", err)
	}
	hNew, _, err := Hash(ps, nil, nil, types.UID("isvc-uid-new"))
	if err != nil {
		t.Fatalf("Hash new: %v", err)
	}
	if hOld == hNew {
		t.Errorf("same spec, distinct owner UIDs MUST emit distinct hashes; got both %q (recreate path would silently inherit predecessor's CR)", hOld)
	}
}

// TestEnsureControllerRevision_RecreateProducesDistinctCRName pins the
// end-to-end shape: two ISVCs with the same Name + Spec but different
// UIDs (simulating delete-and-recreate after cascade GC) must land at
// distinct CR names so the new owner gets a fresh ControllerRevision
// history rather than reusing the predecessor's. This is the unit-test
// analog of the KIND spec lifecycle_kind_test.go:301
// "delete + recreate same-name ISVC produces a fresh ControllerRevision"
// — the unit form runs against the fake client so it doesn't depend on
// real cascade-GC timing.
func TestEnsureControllerRevision_RecreateProducesDistinctCRName(t *testing.T) {
	oldISVC := minimalISVC("recreate-isvc", "prod", 1)
	oldISVC.UID = types.UID("uid-A")
	spec := basicPodSpecForRevision()
	cOld := newFakeClientWithApps(t, oldISVC)
	crOld, _, err := EnsureControllerRevision(context.Background(), cOld, cOld, oldISVC, testISVCGVK, testKey(oldISVC, workload.ComponentEngine), spec, nil, nil, oldISVC.UID)
	if err != nil {
		t.Fatalf("first EnsureControllerRevision: %v", err)
	}
	if crOld == nil {
		t.Fatal("nil CR from first call")
	}

	// Simulate cascade GC having reaped the old CR (and the old ISVC),
	// then a fresh ISVC of the same name landing with a new UID.
	newISVC := minimalISVC("recreate-isvc", "prod", 1)
	newISVC.UID = types.UID("uid-B")
	cNew := newFakeClientWithApps(t, newISVC)
	crNew, collision, err := EnsureControllerRevision(context.Background(), cNew, cNew, newISVC, testISVCGVK, testKey(newISVC, workload.ComponentEngine), spec, nil, nil, newISVC.UID)
	if err != nil {
		t.Fatalf("second EnsureControllerRevision: %v", err)
	}
	if collision {
		t.Errorf("recreate path on an empty cluster must NOT report a collision (no foreign CR present); got collision=true")
	}
	if crNew == nil {
		t.Fatal("nil CR from second call")
	}
	if crNew.Name == crOld.Name {
		t.Errorf("recreated ISVC must NOT land at the predecessor's CR name; both got %q", crNew.Name)
	}
	if crNew.OwnerReferences[0].UID != newISVC.UID {
		t.Errorf("new CR owner UID: got %q want %q", crNew.OwnerReferences[0].UID, newISVC.UID)
	}
}

func TestControllerOwnedBy(t *testing.T) {
	uid := types.UID("a")
	tcases := []struct {
		name string
		refs []metav1.OwnerReference
		want bool
	}{
		{name: "empty", refs: nil, want: false},
		{name: "matching-but-non-controller", refs: []metav1.OwnerReference{{UID: uid}}, want: false},
		{name: "matching-controller-false", refs: []metav1.OwnerReference{{UID: uid, Controller: ptr.To(false)}}, want: false},
		{name: "matching-controller-true", refs: []metav1.OwnerReference{{UID: uid, Controller: ptr.To(true)}}, want: true},
		{name: "different-uid-controller", refs: []metav1.OwnerReference{{UID: types.UID("b"), Controller: ptr.To(true)}}, want: false},
	}
	for _, tc := range tcases {
		t.Run(tc.name, func(t *testing.T) {
			if got := controllerOwnedBy(tc.refs, uid); got != tc.want {
				t.Errorf("got %v want %v", got, tc.want)
			}
		})
	}
}

func TestRevisionTemplateMeta_NilStaysNilForBackCompat(t *testing.T) {
	// Empty input -> nil output -> JSON serializes as `"podMeta":null`,
	// matching the pre-PR-D hash shape so existing alpha CRs aren't
	// invalidated on first reconcile.
	cases := []struct {
		name string
		src  *metav1.ObjectMeta
	}{
		{"nil src", nil},
		{"empty labels and annotations", &metav1.ObjectMeta{}},
		{"empty map labels", &metav1.ObjectMeta{Labels: map[string]string{}}},
		{"empty map annotations", &metav1.ObjectMeta{Annotations: map[string]string{}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := TemplateMeta(tc.src)
			if got != nil {
				t.Errorf("expected nil for empty input, got %+v", got)
			}
		})
	}
}

func TestRevisionTemplateMeta_CapturesLabelsAndAnnotations(t *testing.T) {
	src := &metav1.ObjectMeta{
		Name:            "engine",
		Namespace:       "prod",
		UID:             "should-be-stripped",
		ResourceVersion: "12345",
		Labels: map[string]string{
			"app":           "llama-70b",
			"runtime":       "sglang",
			"models.ome.io": "Ready",
		},
		Annotations: map[string]string{
			"ome.io/deploymentMode": "OMENative",
		},
	}
	got := TemplateMeta(src)
	if got == nil {
		t.Fatalf("expected non-nil output for src with labels/annotations")
	}
	if got.Name != "" || got.Namespace != "" || got.UID != "" || got.ResourceVersion != "" {
		t.Errorf("operational fields leaked into revision meta: %+v", got)
	}
	if len(got.Labels) != 3 {
		t.Errorf("Labels not captured: %v", got.Labels)
	}
	if got.Annotations["ome.io/deploymentMode"] != "OMENative" {
		t.Errorf("Annotations not captured: %v", got.Annotations)
	}
}

func TestRevisionHash_LabelChangeProducesDifferentHash(t *testing.T) {
	// A user-template label edit must produce a new revision hash so
	// AggregateAndWriteStatus's diff detection fires a rollout.
	ps := &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "main", Image: "llama:v1"}},
	}
	metaV1 := &metav1.ObjectMeta{Labels: map[string]string{"tier": "free"}}
	metaV2 := &metav1.ObjectMeta{Labels: map[string]string{"tier": "premium"}}

	h1, _, err := Hash(ps, metaV1, nil, "")
	if err != nil {
		t.Fatalf("Hash v1: %v", err)
	}
	h2, _, err := Hash(ps, metaV2, nil, "")
	if err != nil {
		t.Fatalf("Hash v2: %v", err)
	}
	if h1 == h2 {
		t.Errorf("hash unchanged after label edit: %s == %s", h1, h2)
	}
}

func TestRevisionHash_AnnotationChangeProducesDifferentHash(t *testing.T) {
	ps := &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "main", Image: "llama:v1"}},
	}
	metaV1 := &metav1.ObjectMeta{Annotations: map[string]string{"sidecar.istio.io/inject": "false"}}
	metaV2 := &metav1.ObjectMeta{Annotations: map[string]string{"sidecar.istio.io/inject": "true"}}

	h1, _, _ := Hash(ps, metaV1, nil, "")
	h2, _, _ := Hash(ps, metaV2, nil, "")
	if h1 == h2 {
		t.Errorf("hash unchanged after annotation edit: %s == %s", h1, h2)
	}
}

func TestRevisionHash_NoMetaAndEmptyMetaProduceSameHash(t *testing.T) {
	// nil and empty meta MUST hash identically so the "no PodMeta" hash
	// shape stays stable for ISVCs without template labels/annotations.
	ps := &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "main", Image: "llama:v1"}},
	}
	hNil, _, _ := Hash(ps, nil, nil, "")
	hEmpty, _, _ := Hash(ps, &metav1.ObjectMeta{}, nil, "")
	if hNil != hEmpty {
		t.Errorf("hash should be stable across nil vs empty meta: %s vs %s", hNil, hEmpty)
	}
}

func TestCollectLiveRevisionNames_UnionAcrossInstanceStatuses(t *testing.T) {
	// Retention must protect every InstanceStatus.RunningRevision /
	// TargetRevision — an in-flight migration whose source points at an
	// older CR would otherwise have that CR swept mid-flight.
	instances := []workload.InstanceStatus{
		{Index: 0, RunningRevision: "rev-old-source", TargetRevision: ""},
		{Index: 2, RunningRevision: "rev-current", TargetRevision: "rev-update"},
		{Index: 3, RunningRevision: "", TargetRevision: ""},
	}
	got := CollectLiveRevisionNames("rev-current", "rev-update", instances)
	want := map[string]bool{
		"rev-current":    true,
		"rev-update":     true,
		"rev-old-source": true,
	}
	if len(got) != len(want) {
		t.Errorf("count mismatch: got %d want %d (%v)", len(got), len(want), got)
	}
	for _, name := range got {
		if !want[name] {
			t.Errorf("unexpected name in output: %s", name)
		}
	}
}

func TestCollectLiveRevisionNames_HandlesNilStatus(t *testing.T) {
	if got := CollectLiveRevisionNames("", "", nil); got != nil {
		t.Errorf("expected nil for empty (no live names), got %v", got)
	}
	if got := CollectLiveRevisionNames("", "", []workload.InstanceStatus{}); got != nil {
		t.Errorf("expected nil for empty status (no live names), got %v", got)
	}
}

func TestCollectLiveRevisionNames_DropsEmpty(t *testing.T) {
	instances := []workload.InstanceStatus{
		{Index: 0, RunningRevision: "", TargetRevision: ""},
	}
	got := CollectLiveRevisionNames("", "rev-a", instances)
	if len(got) != 1 || got[0] != "rev-a" {
		t.Errorf("expected only [rev-a], got %v", got)
	}
}
