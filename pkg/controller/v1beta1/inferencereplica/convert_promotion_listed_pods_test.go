package inferencereplica

import (
	"context"
	"testing"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus/irstatustest"
)

// listedPodsPicture is a Component with one Instance under repair, whose
// pod carries the target or another revision, and what the two promotions
// that list the pods themselves must leave behind.
type listedPodsPicture struct {
	name       string
	podOnOther bool
	writes     int
	current    string
}

// listedPodsPictures: a live pod of another revision under an Instance
// under repair clears the current revision; a pod on the target keeps it.
func listedPodsPictures(target string) []listedPodsPicture {
	return []listedPodsPicture{
		{"a live pod of another revision under an Instance under repair clears the current revision", true, 1, ""},
		{"every pod on the target keeps the current revision", false, 0, target},
	}
}

// TestPromotionTheEngineAsksForReadsThePodsItLists: the promotion the
// engine asks for lists the Component's pods itself and decides from them
// as the end of the pass does from the pods it is handed.
func TestPromotionTheEngineAsksForReadsThePodsItLists(t *testing.T) {
	const target, other = irstatustest.Revision, irstatustest.OtherRevision
	for _, pic := range listedPodsPictures(target) {
		t.Run(pic.name, func(t *testing.T) {
			shape := publicationShape{name: "readySince/8", instances: 8}
			ir := publicationReplica(shape)
			rowRepairing(3)(&ir.Status)
			pods := publicationPods(ir, shape.instances)
			if pic.podOnOther {
				labelRevision(pods[3], other)
			}
			f := buildPublicationFixture(t, pic.name, ir, pods)
			r := f.pass.reconciler
			before := f.writes.Count()
			if err := buildPromoteCurrentRevision(r.statusWriter(), r.liveReader(), r.Client, f.pass.ir)(context.Background(), target); err != nil {
				t.Fatalf("promote the current revision: %v", err)
			}
			stored := f.storedStatus(t)
			if writes := f.writes.Count() - before; writes != pic.writes || stored.CurrentRevision != pic.current {
				t.Errorf("the promotion wrote %d times and left current=%q, want %d and %q", writes, stored.CurrentRevision, pic.writes, pic.current)
			}
		})
	}
}

// TestRecordUpdateRevisionReadsThePodsItLists: the record of a new update
// revision that the current revision already names lists the Component's
// pods itself and decides the current revision from them: cleared while
// a live pod of another revision runs under an Instance under repair, kept
// when every pod carries it. The update revision moves either way.
func TestRecordUpdateRevisionReadsThePodsItLists(t *testing.T) {
	const target, other = irstatustest.Revision, irstatustest.OtherRevision
	for _, pic := range listedPodsPictures(target) {
		t.Run(pic.name, func(t *testing.T) {
			shape := publicationShape{name: "readySince/8", instances: 8}
			ir := publicationReplica(shape)
			ir.Status.UpdateRevision = other
			rowRepairing(3)(&ir.Status)
			pods := publicationPods(ir, shape.instances)
			if pic.podOnOther {
				labelRevision(pods[3], other)
			}
			f := buildPublicationFixture(t, pic.name, ir, pods)
			r := f.pass.reconciler
			before := f.writes.Count()
			record := buildRecordUpdateRevision(r.statusWriter(), r.liveReader(), r.Client, f.pass.ir, f.pass.plan, everyNodePresent{})
			if err := record(context.Background(), target); err != nil {
				t.Fatalf("record the update revision: %v", err)
			}
			stored := f.storedStatus(t)
			if writes := f.writes.Count() - before; writes != 1 || stored.UpdateRevision != target || stored.CurrentRevision != pic.current {
				t.Errorf("the record wrote %d times and left current=%q update=%q, want 1 write, current=%q and update=%q", writes, stored.CurrentRevision, stored.UpdateRevision, pic.current, target)
			}
		})
	}
}
