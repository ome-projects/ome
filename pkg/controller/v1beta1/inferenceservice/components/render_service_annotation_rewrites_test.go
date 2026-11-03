package components

import (
	"context"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
)

// TestServiceAnnotationValueRewritesTheReplicaWithoutANewRevision shows what
// a writer that patches a service annotation does to a projected replica:
// the production render copies every service annotation outside the
// disallowed list into the Component's pod template, so each new value
// changes the projected replica's spec and the projector writes it (a new
// generation on a live apiserver), while the revision hash leaves the key out
// and no pod changes. The template hash the projector writes beside the spec
// follows the revision, not the generation: it stands through the value
// change and moves with a declared component annotation, which the revision
// covers.
func TestServiceAnnotationValueRewritesTheReplicaWithoutANewRevision(t *testing.T) {
	ctx := context.Background()
	const key = "editor.example.com/write-counter"
	tc := goldenCases()[0]
	tc.service.Annotations[key] = "1"
	c := goldenClient(t, tc)

	first := renderThroughProjector(t, ctx, c, tc)
	stored := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: goldenNamespace, Name: irprojector.RoleReplicaName(tc.service, tc.component)}, stored); err != nil {
		t.Fatalf("read the projected replica: %v", err)
	}
	if got := stored.Spec.Runners[0].Template.Annotations[key]; got != "1" {
		t.Fatalf("the service annotation must reach the runner template, got %q", got)
	}
	firstVersion := stored.ResourceVersion
	firstHash := stored.Annotations[constants.InferenceReplicaTemplateHashAnnotationKey]
	excluded := false
	for _, k := range first.ExcludedAnnotationKeys {
		excluded = excluded || k == key
	}
	if !excluded {
		t.Fatalf("the key must be listed among the annotations the revision hash leaves out, got %v", first.ExcludedAnnotationKeys)
	}

	tc.service.Annotations[key] = "2"
	second := renderThroughProjector(t, ctx, c, tc)
	if err := c.Get(ctx, client.ObjectKeyFromObject(stored), stored); err != nil {
		t.Fatalf("read the replica after the second value: %v", err)
	}
	if got := stored.Spec.Runners[0].Template.Annotations[key]; got != "2" {
		t.Fatalf("the second value must reach the runner template, got %q", got)
	}
	if stored.ResourceVersion == firstVersion {
		t.Fatalf("the second value must be written onto the replica (the projector found the spec changed)")
	}
	if first.RevisionHash != second.RevisionHash {
		t.Fatalf("the value must not mint a revision: hash %s before, %s after", first.RevisionHash, second.RevisionHash)
	}
	templateHash := stored.Annotations[constants.InferenceReplicaTemplateHashAnnotationKey]
	if templateHash == "" || templateHash != firstHash {
		t.Fatalf("the template hash must stand through the value change: %q before, %q after", firstHash, templateHash)
	}
	if want := v1beta1convert.TemplateHash(stored, stored.Spec.Runners); templateHash != want {
		t.Fatalf("the annotation must be the hash of the stored templates: %q, want %q", templateHash, want)
	}

	tc.service.Spec.Engine.Annotations = map[string]string{"example.com/declared": "1"}
	third := renderThroughProjector(t, ctx, c, tc)
	if err := c.Get(ctx, client.ObjectKeyFromObject(stored), stored); err != nil {
		t.Fatalf("read the replica after the declared annotation: %v", err)
	}
	if third.RevisionHash == first.RevisionHash {
		t.Fatalf("a declared component annotation mints a revision; the hash stayed %s", third.RevisionHash)
	}
	if got := stored.Annotations[constants.InferenceReplicaTemplateHashAnnotationKey]; got == firstHash {
		t.Fatalf("the template hash must move with the revision; it stayed %q", got)
	}
}
