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

// TestServiceAnnotationValueLeavesTheReplicaUnwritten shows what a writer
// that patches a service annotation does to a projected replica: the
// production render keeps a service annotation the Component does not
// declare off the pod template, so a new value changes nothing in the
// projected spec and the projector writes nothing (no new generation on a
// live apiserver); the revision hash and the template hash stand. The
// projector still lists the key among the inherited keys the revision hash
// leaves out. A declared component annotation, which the revision covers,
// mints a revision and moves the template hash.
func TestServiceAnnotationValueLeavesTheReplicaUnwritten(t *testing.T) {
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
	if got, ok := stored.Spec.Runners[0].Template.Annotations[key]; ok {
		t.Fatalf("the service annotation must stay off the runner template, got %q", got)
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
	if got, ok := stored.Spec.Runners[0].Template.Annotations[key]; ok {
		t.Fatalf("the second value must stay off the runner template, got %q", got)
	}
	if stored.ResourceVersion != firstVersion {
		t.Fatalf("the second value must not be written onto the replica: resourceVersion %s before, %s after", firstVersion, stored.ResourceVersion)
	}
	if first.RevisionHash != second.RevisionHash {
		t.Fatalf("the value must not mint a revision: hash %s before, %s after", first.RevisionHash, second.RevisionHash)
	}
	templateHash := stored.Annotations[constants.InferenceReplicaTemplateHashAnnotationKey]
	if templateHash == "" || templateHash != firstHash {
		t.Fatalf("the template hash must stand through the value change: %q before, %q after", firstHash, templateHash)
	}
	if want := v1beta1convert.TemplateHash(stored, stored.Spec.Runners); templateHash != want {
		t.Fatalf("the recorded template hash must be the hash of the stored templates: %q, want %q", templateHash, want)
	}

	tc.service.Spec.Engine.Annotations = map[string]string{"example.com/declared": "1"}
	third := renderThroughProjector(t, ctx, c, tc)
	if err := c.Get(ctx, client.ObjectKeyFromObject(stored), stored); err != nil {
		t.Fatalf("read the replica after the declared annotation: %v", err)
	}
	if got := stored.Spec.Runners[0].Template.Annotations["example.com/declared"]; got != "1" {
		t.Fatalf("a declared component annotation must reach the runner template, got %q", got)
	}
	if third.RevisionHash == first.RevisionHash {
		t.Fatalf("a declared component annotation mints a revision; the hash stayed %s", third.RevisionHash)
	}
	if got := stored.Annotations[constants.InferenceReplicaTemplateHashAnnotationKey]; got == firstHash {
		t.Fatalf("the template hash must move with the revision; it stayed %q", got)
	}
}
