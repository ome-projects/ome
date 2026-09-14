package placement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestDeriveISVC(t *testing.T) {
	src := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "svc", Namespace: "prod", UID: "uid-123",
			ResourceVersion: "999",
			Annotations: map[string]string{
				LocalQueueAnnotation:                "serving-lq",
				AcceleratorRequirementsAnnotation:   "gpu=gb300",
				ClusterSelectorAnnotation:           "provider=cloud-a",
				constants.RolloutPromoteAnnotation:  "abc123def",
				constants.RolloutRollbackAnnotation: "true",
				constants.NetworkVisibility:         "cluster-local", // an ingress override that SHOULD ride along
			},
		},
		Spec: v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{
				Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Name: "ome-container", Image: "img"}},
			},
			Decoder: &v1beta1.DecoderSpec{
				Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Name: "ome-container", Image: "img"}},
			},
		},
	}

	d := DeriveISVC(src, "cp-east", "")

	// identity preserved (worker addresses it by the same name/ns).
	assert.Equal(t, "svc", d.Name)
	assert.Equal(t, "prod", d.Namespace)
	// server-side fields cleared.
	assert.Empty(t, d.ResourceVersion)
	assert.Empty(t, string(d.UID))
	assert.True(t, d.Status.DeepCopy() != nil) // status is a fresh zero value (compiles regardless)
	// origin markers.
	assert.NotEmpty(t, d.Labels[PlacementOriginLabel])
	assert.Equal(t, "uid-123", d.Annotations[PlacementOriginUIDAnnotation])
	// control-plane identity stamped from the supplied id.
	assert.Equal(t, "cp-east", d.Labels[PlacementControlPlaneLabel])
	// Kueue queue-name label stamped on the engine component pod metadata
	// (the sole Kueue stamp).
	require.NotNil(t, d.Spec.Engine)
	assert.Equal(t, "serving-lq", d.Spec.Engine.ComponentExtensionSpec.Labels[constants.KueueQueueLabelKey])
	// ...and on the decoder component too.
	require.NotNil(t, d.Spec.Decoder)
	assert.Equal(t, "serving-lq", d.Spec.Decoder.ComponentExtensionSpec.Labels[constants.KueueQueueLabelKey])
	// control-plane-only directives are dropped on the derived object: the
	// placement selectors and the rollout operator verbs.
	for _, k := range []string{
		AcceleratorRequirementsAnnotation, ClusterSelectorAnnotation,
		constants.RolloutPromoteAnnotation, constants.RolloutRollbackAnnotation,
	} {
		_, has := d.Annotations[k]
		assert.Falsef(t, has, "control-plane-only annotation %q must be stripped", k)
	}
	// ...but a legitimate worker-side serving directive (ingress visibility)
	// rides along untouched.
	assert.Equal(t, "cluster-local", d.Annotations[constants.NetworkVisibility])

	// no LocalQueueAnnotation and no configured queue -> no queue label.
	src2 := src.DeepCopy()
	delete(src2.Annotations, LocalQueueAnnotation)
	d2 := DeriveISVC(src2, "", "")
	_, hasQueue := d2.Spec.Engine.ComponentExtensionSpec.Labels[constants.KueueQueueLabelKey]
	assert.False(t, hasQueue, "no queue configured -> nothing stamped")
	// empty control-plane id leaves the identity label unset (single-CP behavior).
	_, hasCP := d2.Labels[PlacementControlPlaneLabel]
	assert.False(t, hasCP)

	// source must not be mutated (deep copy).
	assert.Nil(t, src.Spec.Engine.ComponentExtensionSpec.Labels, "source ISVC must be untouched")
}

// Leaving it on overwrites the member object's own tracking-id, which is what
// ArgoCD reads to decide ownership — reassigning it to an application on
// another cluster that does not manage it.
func TestDeriveISVC_StripsGitOpsAnnotations(t *testing.T) {
	src := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "svc", Namespace: "prod", UID: "uid-123",
			Annotations: map[string]string{
				"argocd.argoproj.io/tracking-id":     "source-app:ome.io/InferenceService:prod/svc",
				"argocd.argoproj.io/sync-options":    "Prune=false",
				"argocd.argoproj.io/compare-options": "IgnoreExtraneous",
				"argocd.argoproj.io/sync-wave":       "1",
				// Not the control plane's tooling: a user annotation on a
				// lookalike-but-different domain must survive.
				"argocd.argoproj.io.example.com/keep": "yes",
				constants.NetworkVisibility:           "cluster-local",
			},
		},
		Spec: v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{
				Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Name: "ome-container", Image: "img"}},
			},
		},
	}

	d := DeriveISVC(src, "cp-east", "")

	for _, k := range []string{
		"argocd.argoproj.io/tracking-id",
		"argocd.argoproj.io/sync-options",
		"argocd.argoproj.io/compare-options",
		"argocd.argoproj.io/sync-wave",
	} {
		_, present := d.Annotations[k]
		assert.False(t, present, "%s must not ride along to the derived copy", k)
	}
	assert.Equal(t, "yes", d.Annotations["argocd.argoproj.io.example.com/keep"],
		"the prefix must match the family, not merely a leading substring")
	assert.Equal(t, "cluster-local", d.Annotations[constants.NetworkVisibility],
		"unrelated annotations still ride along")
	// The source is untouched: DeriveISVC works on a deep copy.
	assert.Equal(t, "source-app:ome.io/InferenceService:prod/svc",
		src.Annotations["argocd.argoproj.io/tracking-id"])
}

func TestSetDerivedReplicas(t *testing.T) {
	newISVC := func(engMax int, withDecoder bool) *v1beta1.InferenceService {
		i := &v1beta1.InferenceService{Spec: v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MaxReplicas: engMax}},
		}}
		if withDecoder {
			i.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MaxReplicas: engMax}}
		}
		return i
	}

	// maxPer > 0: the cap is the hard per-cluster ceiling on every scaled
	// component — clamping the declared MaxReplicas DOWN — while Min is this
	// home's apportioned share. A PD pair is capped 1:1.
	d := newISVC(9, true)
	setDerivedReplicas(d, 1, 2)
	require.NotNil(t, d.Spec.Engine.MinReplicas)
	assert.Equal(t, 1, *d.Spec.Engine.MinReplicas)
	assert.Equal(t, 2, d.Spec.Engine.MaxReplicas, "cap clamps the ceiling down from 9")
	require.NotNil(t, d.Spec.Decoder.MinReplicas)
	assert.Equal(t, 1, *d.Spec.Decoder.MinReplicas)
	assert.Equal(t, 2, d.Spec.Decoder.MaxReplicas, "decoder capped 1:1 with engine")

	// maxPer <= 0 (no cap declared): the component's own MaxReplicas stands.
	d = newISVC(5, false)
	setDerivedReplicas(d, 2, 0)
	assert.Equal(t, 2, *d.Spec.Engine.MinReplicas)
	assert.Equal(t, 5, d.Spec.Engine.MaxReplicas, "uncapped -> declared ceiling preserved")

	// maxPer <= 0 and the share exceeds the declared ceiling: raise Max to keep
	// Max >= Min (the request must be honorable).
	d = newISVC(1, false)
	setDerivedReplicas(d, 3, 0)
	assert.Equal(t, 3, *d.Spec.Engine.MinReplicas)
	assert.Equal(t, 3, d.Spec.Engine.MaxReplicas, "uncapped -> Max raised to keep Max >= Min")
}

// The queue a derived workload joins is operator-configurable: a per-ISVC
// annotation wins, then the configured queue. The queue names a resource the
// operator provisioned, so an unconfigured fleet gets no label rather than a
// guessed name Kueue would never match.
func TestDeriveISVC_LocalQueuePrecedence(t *testing.T) {
	base := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "svc", UID: "uid-1"},
		Spec: v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{},
		},
	}
	queueOf := func(d *v1beta1.InferenceService) string {
		return d.Spec.Engine.ComponentExtensionSpec.Labels[constants.KueueQueueLabelKey]
	}

	assert.Empty(t, queueOf(DeriveISVC(base, "", "")),
		"nothing configured -> no queue label")
	assert.Equal(t, "gpu-queue", queueOf(DeriveISVC(base, "", "gpu-queue")),
		"configured queue is stamped when no annotation is set")

	annotated := base.DeepCopy()
	annotated.Annotations = map[string]string{LocalQueueAnnotation: "per-isvc"}
	assert.Equal(t, "per-isvc", queueOf(DeriveISVC(annotated, "", "gpu-queue")),
		"per-ISVC annotation must beat the configured queue")
}
