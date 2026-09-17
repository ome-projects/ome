package inferencereplica

import (
	"testing"

	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
)

func TestParseExcludedAnnotationKeys(t *testing.T) {
	g := gomega.NewWithT(t)

	g.Expect(parseExcludedAnnotationKeys(&v1beta1.InferenceReplica{})).To(gomega.BeNil(),
		"no annotation -> nil")

	ir := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
		constants.RevisionExcludedAnnotationKeysAnnotationKey: "a,b,,c",
	}}}
	got := parseExcludedAnnotationKeys(ir)
	g.Expect(got).To(gomega.HaveLen(3), "empty split entries are dropped")
	g.Expect(got).To(gomega.HaveKey("a"))
	g.Expect(got).To(gomega.HaveKey("b"))
	g.Expect(got).To(gomega.HaveKey("c"))
}

func TestStripExcludedAnnotations(t *testing.T) {
	g := gomega.NewWithT(t)

	// nil excluded -> same pointer, untouched.
	meta := &metav1.ObjectMeta{Annotations: map[string]string{"a": "1"}}
	g.Expect(stripExcludedAnnotations(meta, nil)).To(gomega.BeIdenticalTo(meta))

	// nil meta -> nil.
	g.Expect(stripExcludedAnnotations(nil, map[string]struct{}{"x": {}})).To(gomega.BeNil())

	excluded := map[string]struct{}{"drop": {}}
	in := &metav1.ObjectMeta{
		Labels:      map[string]string{"l": "1"},
		Annotations: map[string]string{"keep": "1", "drop": "2"},
	}
	out := stripExcludedAnnotations(in, excluded)
	g.Expect(out).NotTo(gomega.BeIdenticalTo(in), "must return a copy, not mutate in place")
	g.Expect(out.Annotations).To(gomega.HaveKey("keep"))
	g.Expect(out.Annotations).NotTo(gomega.HaveKey("drop"))
	g.Expect(in.Annotations).To(gomega.HaveKey("drop"),
		"input must be untouched — pod rendering still uses the full annotation set")
	g.Expect(out.Labels).To(gomega.HaveKeyWithValue("l", "1"), "labels must be preserved")

	// nothing to drop -> same pointer (no needless copy).
	g.Expect(stripExcludedAnnotations(in, map[string]struct{}{"absent": {}})).
		To(gomega.BeIdenticalTo(in))
}

// TestRevisionHashStableUnderExcludedAnnotations verifies inherited ISVC annotations do not
// affect revision identity while component annotations remain hash inputs.
func TestRevisionHashStableUnderExcludedAnnotations(t *testing.T) {
	g := gomega.NewWithT(t)
	podSpec := &corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "img:1"}}}

	base := &metav1.ObjectMeta{
		Labels:      map[string]string{"app": "x"},
		Annotations: map[string]string{"ome.io/declared": "1"},
	}
	baseHash, _, err := revision.HashWithWorker(podSpec, nil, base, nil, "uid")
	g.Expect(err).NotTo(gomega.HaveOccurred())

	// Projected metadata retains the inherited annotation for pod rendering.
	withAmbient := &metav1.ObjectMeta{
		Labels: map[string]string{"app": "x"},
		Annotations: map[string]string{
			"ome.io/declared":                     "1",
			"editor.example.com/resource-version": "v1beta1",
		},
	}
	excluded := parseExcludedAnnotationKeys(&v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
			constants.RevisionExcludedAnnotationKeysAnnotationKey: "editor.example.com/resource-version",
		}},
	})

	strippedHash, _, err := revision.HashWithWorker(
		podSpec, nil, stripExcludedAnnotations(withAmbient, excluded), nil, "uid")
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(strippedHash).To(gomega.Equal(baseHash),
		"an excluded inherited annotation must not change the revision hash")

	// The unfiltered metadata remains a distinct revision input.
	unstrippedHash, _, err := revision.HashWithWorker(podSpec, nil, withAmbient, nil, "uid")
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(unstrippedHash).NotTo(gomega.Equal(baseHash),
		"the inherited annotation must change the hash before filtering")

	// A component annotation remains part of the revision identity.
	declaredChanged := &metav1.ObjectMeta{
		Labels:      map[string]string{"app": "x"},
		Annotations: map[string]string{"ome.io/declared": "2"},
	}
	declHash, _, err := revision.HashWithWorker(
		podSpec, nil, stripExcludedAnnotations(declaredChanged, excluded), nil, "uid")
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(declHash).NotTo(gomega.Equal(baseHash),
		"a deliberate declared-annotation change must still produce a new revision")
}

// TestRevisionHashUnchangedByIROperatorVerbAnnotations verifies the
// InferenceReplica operator verbs (release-held-revision, reset-instances)
// never mint a revision: stamped on the IR object they are not hash
// inputs at all, and even inherited onto the pod-template metadata they
// are filtered as lifecycle annotations. Each hash is computed on a fresh
// Reconciler so the memoization cache cannot mask a drift.
func TestRevisionHashUnchangedByIROperatorVerbAnnotations(t *testing.T) {
	podSpec := &corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "img:1"}}}
	templateMeta := &metav1.ObjectMeta{Annotations: map[string]string{"ome.io/declared": "1"}}
	input := workload.ReconcileInput{DesiredSpec: workload.WorkloadDesiredSpec{
		PodSpec:               podSpec,
		PodTemplateObjectMeta: templateMeta,
	}}
	ir := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{
		Name: "engine", Namespace: "default", UID: "ir-uid", Generation: 1,
	}}
	verbs := map[string]string{
		constants.ReleaseHeldRevisionAnnotationKey: "llama-engine-aaaaaaaa",
		constants.ResetInstancesAnnotationKey:      "13,14",
	}
	for key, val := range verbs {
		t.Run(key, func(t *testing.T) {
			g := gomega.NewWithT(t)

			baseHash, _, err := (&Reconciler{}).revisionHash(ir, input, nil, "scope-uid")
			g.Expect(err).NotTo(gomega.HaveOccurred())

			annotated := ir.DeepCopy()
			annotated.Annotations = map[string]string{key: val}
			objectHash, _, err := (&Reconciler{}).revisionHash(annotated, input, nil, "scope-uid")
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(objectHash).To(gomega.Equal(baseHash),
				"%s on the IR object must not change the revision", key)

			inherited := input
			inherited.DesiredSpec.PodTemplateObjectMeta = &metav1.ObjectMeta{Annotations: map[string]string{
				"ome.io/declared": "1",
				key:               val,
			}}
			templateHash, _, err := (&Reconciler{}).revisionHash(ir, inherited, nil, "scope-uid")
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(templateHash).To(gomega.Equal(baseHash),
				"%s inherited onto the pod template must be filtered from the revision", key)
		})
	}
}

func TestRevisionHashCacheInvalidatesWhenExcludedAnnotationsChange(t *testing.T) {
	g := gomega.NewWithT(t)
	const inheritedKey = "editor.example.com/resource-version"

	podSpec := &corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "img:1"}}}
	meta := &metav1.ObjectMeta{Annotations: map[string]string{inheritedKey: "v1beta1"}}
	input := workload.ReconcileInput{DesiredSpec: workload.WorkloadDesiredSpec{
		PodSpec:               podSpec,
		PodTemplateObjectMeta: meta,
	}}
	ir := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{
		Name:       "engine",
		Namespace:  "default",
		UID:        "ir-uid",
		Generation: 1,
	}}
	r := &Reconciler{}

	initialHash, _, err := r.revisionHash(ir, input, nil, "scope-uid")
	g.Expect(err).NotTo(gomega.HaveOccurred())

	ir.Annotations = map[string]string{
		constants.RevisionExcludedAnnotationKeysAnnotationKey: inheritedKey,
	}
	updatedHash, _, err := r.revisionHash(ir, input, nil, "scope-uid")
	g.Expect(err).NotTo(gomega.HaveOccurred())

	expectedMeta := stripExcludedAnnotations(meta, map[string]struct{}{inheritedKey: {}})
	expectedHash, _, err := revision.HashWithWorkerAndTopology(
		podSpec, nil, expectedMeta, "", nil, "scope-uid")
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(initialHash).NotTo(gomega.Equal(expectedHash))
	g.Expect(updatedHash).To(gomega.Equal(expectedHash))
}
