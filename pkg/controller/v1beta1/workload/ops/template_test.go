package ops

import (
	"context"
	"encoding/json"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

func templateTestInput() workload.ReconcileInput {
	return workload.ReconcileInput{
		Key: workload.Key{Namespace: "prod", OwnerName: "llama-70b", Component: workload.ComponentEngine},
		DesiredSpec: workload.WorkloadDesiredSpec{
			PodSpec:       &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "registry.example.com/runtime:v2"}}},
			WorkerPodSpec: &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "registry.example.com/runtime-worker:v2"}}},
			PodTemplateObjectMeta: &metav1.ObjectMeta{
				Labels:      map[string]string{"tier": "v2", "release": "v2", "kueue.x-k8s.io/queue-name": "team-a"},
				Annotations: map[string]string{"owner.example.com/team": "team-a", "note": "v2", "example.com/build": "v2"},
			},
			PairingProtocol: "proto-v2",
		},
	}
}

// templateTestTarget is the roll target minted from templateTestInput's
// template: its recorded metadata is the hashed subset of the current
// template's, without the queue label and the inherited annotation.
func templateTestTarget(t *testing.T) *appsv1.ControllerRevision {
	t.Helper()
	return storedPayloadRevision(t, "llama-70b-engine-0000v2", revision.DataPayload{
		PodSpec: &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "registry.example.com/runtime:v2"}}},
		PodMeta: &metav1.ObjectMeta{
			Labels:      map[string]string{"tier": "v2", "release": "v2"},
			Annotations: map[string]string{"note": "v2", "example.com/build": "v2"},
		},
	})
}

func storedPayloadRevision(t *testing.T, name string, payload revision.DataPayload) *appsv1.ControllerRevision {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "prod"},
		Data:       runtime.RawExtension{Raw: raw},
	}
}

// The desired template is the roll target's and is paired with whatever
// revision the caller stamps for it, including no revision at all.
func TestDesiredTemplate_PairsTheCurrentTemplateWithTheGivenRevision(t *testing.T) {
	input := templateTestInput()
	plan := workload.ComponentPlan{PairingProtocol: input.DesiredSpec.PairingProtocol}
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-0000v2"}}

	got := desiredTemplate(input, plan, query.RevisionOf(target))
	if got.revision.Name() != target.Name || got.podSpec != input.DesiredSpec.PodSpec ||
		got.workerPodSpec != input.DesiredSpec.WorkerPodSpec || got.meta != input.DesiredSpec.PodTemplateObjectMeta ||
		got.pairingProtocol != "proto-v2" {
		t.Fatalf("desiredTemplate = %+v, want the current template paired with %s", got, target.Name)
	}
	if unstamped := desiredTemplate(input, plan, query.RevisionID{}); !unstamped.revision.IsZero() || unstamped.podSpec != input.DesiredSpec.PodSpec {
		t.Fatalf("an unstamped desired template must carry no revision: %+v", unstamped)
	}
}

// A stored revision renders exactly what its ControllerRevision records:
// its leader and worker templates, its pairing protocol, and its labels
// and annotations. A key the roll target hashes is a revision's: it takes
// the stored revision's value, and a key the stored revision does not
// record is absent even though the current template carries it — a newer
// revision added it. The queue label and the inherited annotation, which
// no revision hashes, keep following the current template.
func TestStoredTemplate_RendersTheRevisionItNames(t *testing.T) {
	input := templateTestInput()
	stored := storedPayloadRevision(t, "llama-70b-engine-0000v1", revision.DataPayload{
		PodSpec:         &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "registry.example.com/runtime:v1"}}},
		WorkerPodSpec:   &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "registry.example.com/runtime-worker:v1"}}},
		PodMeta:         &metav1.ObjectMeta{Labels: map[string]string{"tier": "v1", "cohort": "stable"}, Annotations: map[string]string{"note": "v1"}},
		PairingProtocol: ptr.To("proto-v1"),
	})
	c := legacyNewFakeClient(t, stored)

	got, found, err := storedTemplate(context.Background(), c, input, templateTestTarget(t), stored.Name)
	if err != nil || !found {
		t.Fatalf("storedTemplate: found=%v err=%v", found, err)
	}
	if got.revision.Name() != stored.Name || got.revision.Hash() != "0000v1" {
		t.Fatalf("revision = %+v, want %s", got.revision, stored.Name)
	}
	if got.podSpec.Containers[0].Image != "registry.example.com/runtime:v1" ||
		got.workerPodSpec.Containers[0].Image != "registry.example.com/runtime-worker:v1" {
		t.Fatalf("templates = %s / %s, want the stored revision's", got.podSpec.Containers[0].Image, got.workerPodSpec.Containers[0].Image)
	}
	if got.pairingProtocol != "proto-v1" {
		t.Fatalf("pairing protocol = %q, want the stored revision's", got.pairingProtocol)
	}
	wantLabels := map[string]string{"tier": "v1", "cohort": "stable", "kueue.x-k8s.io/queue-name": "team-a"}
	wantAnnotations := map[string]string{"note": "v1", "owner.example.com/team": "team-a"}
	if !mapsEqual(got.meta.Labels, wantLabels) || !mapsEqual(got.meta.Annotations, wantAnnotations) {
		t.Fatalf("meta = %+v, want labels %v and annotations %v", got.meta, wantLabels, wantAnnotations)
	}
	if input.DesiredSpec.PodTemplateObjectMeta.Labels["tier"] != "v2" || len(input.DesiredSpec.PodTemplateObjectMeta.Labels) != 3 {
		t.Fatalf("the current template's metadata was mutated by the overlay: %+v", input.DesiredSpec.PodTemplateObjectMeta)
	}
}

// A stored revision minted before the template carried any metadata
// renders none of the keys the roll target hashes, and still carries the
// keys no revision hashes. Rendering the target's annotation under the
// stored revision's label would route and drain one revision while
// running another's template.
func TestStoredTemplate_RevisionWithoutMetadataRendersNoneOfTheTargets(t *testing.T) {
	input := templateTestInput()
	stored := storedPayloadRevision(t, "llama-70b-engine-0000v1", revision.DataPayload{
		PodSpec: &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "registry.example.com/runtime:v1"}}},
	})
	c := legacyNewFakeClient(t, stored)

	got, found, err := storedTemplate(context.Background(), c, input, templateTestTarget(t), stored.Name)
	if err != nil || !found {
		t.Fatalf("storedTemplate: found=%v err=%v", found, err)
	}
	wantLabels := map[string]string{"kueue.x-k8s.io/queue-name": "team-a"}
	wantAnnotations := map[string]string{"owner.example.com/team": "team-a"}
	if !mapsEqual(got.meta.Labels, wantLabels) || !mapsEqual(got.meta.Annotations, wantAnnotations) {
		t.Fatalf("meta = %+v, want only the unhashed keys: labels %v and annotations %v", got.meta, wantLabels, wantAnnotations)
	}
}

// Without a roll target in hand the renderer cannot tell a key a newer
// revision added from a key no revision hashes, so every key of the
// current template the stored revision does not record follows the
// current template. A target that records no metadata means the current
// template hashes none, with the same result.
func TestStoredTemplate_WithoutATargetEveryUnrecordedKeyFollowsTheTemplate(t *testing.T) {
	input := templateTestInput()
	stored := storedPayloadRevision(t, "llama-70b-engine-0000v1", revision.DataPayload{
		PodSpec: &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "registry.example.com/runtime:v1"}}},
		PodMeta: &metav1.ObjectMeta{Labels: map[string]string{"tier": "v1"}, Annotations: map[string]string{"note": "v1"}},
	})
	c := legacyNewFakeClient(t, stored)
	targets := map[string]*appsv1.ControllerRevision{
		"no target":               nil,
		"target without metadata": storedPayloadRevision(t, "llama-70b-engine-0000bare", revision.DataPayload{PodSpec: input.DesiredSpec.PodSpec}),
	}
	wantLabels := map[string]string{"tier": "v1", "release": "v2", "kueue.x-k8s.io/queue-name": "team-a"}
	wantAnnotations := map[string]string{"note": "v1", "example.com/build": "v2", "owner.example.com/team": "team-a"}
	for name, target := range targets {
		got, found, err := storedTemplate(context.Background(), c, input, target, stored.Name)
		if err != nil || !found {
			t.Fatalf("%s: storedTemplate: found=%v err=%v", name, found, err)
		}
		if !mapsEqual(got.meta.Labels, wantLabels) || !mapsEqual(got.meta.Annotations, wantAnnotations) {
			t.Fatalf("%s: meta = %+v, want labels %v and annotations %v", name, got.meta, wantLabels, wantAnnotations)
		}
	}
}

// A pin that is the roll target reads back the current template without a
// read; any other pin reads its stored revision.
func TestPinnedTemplate_ReadsOnlyWhenThePinIsNotTheTarget(t *testing.T) {
	input := templateTestInput()
	plan := workload.ComponentPlan{PairingProtocol: input.DesiredSpec.PairingProtocol}
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-0000v2"}}
	stored := storedPayloadRevision(t, "llama-70b-engine-0000v1", revision.DataPayload{
		PodSpec: &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "registry.example.com/runtime:v1"}}},
	})
	c := legacyNewFakeClient(t, stored)

	onTarget, found, err := pinnedTemplate(context.Background(), c, input, plan, target, target.Name)
	if err != nil || !found || onTarget.podSpec != input.DesiredSpec.PodSpec || onTarget.revision.Name() != target.Name {
		t.Fatalf("pin on the target: found=%v err=%v template=%+v", found, err, onTarget)
	}
	pinned, found, err := pinnedTemplate(context.Background(), c, input, plan, target, stored.Name)
	if err != nil || !found || pinned.podSpec.Containers[0].Image != "registry.example.com/runtime:v1" || pinned.revision.Name() != stored.Name {
		t.Fatalf("pin off the target: found=%v err=%v template=%+v", found, err, pinned)
	}
	if pinned.pairingProtocol != "" {
		t.Fatalf("a stored revision minted without a protocol renders none, got %q", pinned.pairingProtocol)
	}
}

// A revision with no ControllerRevision left is reported as not found,
// never substituted by the current template; one whose payload records no
// pod template is an error.
func TestStoredTemplate_GoneOrEmptyRevisionIsNotTheCurrentTemplate(t *testing.T) {
	input := templateTestInput()
	empty := storedPayloadRevision(t, "llama-70b-engine-0000empty", revision.DataPayload{})
	c := legacyNewFakeClient(t, empty)

	if got, found, err := storedTemplate(context.Background(), c, input, nil, "llama-70b-engine-0000gone"); err != nil || found || got.podSpec != nil {
		t.Fatalf("gone revision: found=%v err=%v template=%+v, want not found and no template", found, err, got)
	}
	if _, found, err := storedTemplate(context.Background(), c, input, nil, empty.Name); err == nil || found {
		t.Fatalf("a revision recording no pod template must be an error, got found=%v err=%v", found, err)
	}
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
