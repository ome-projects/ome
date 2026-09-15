package ops

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// An Accepted v1 record retains instance/node intent, not the requesting
// caller's source Pod UID. Keep that limitation distinct from the existing
// authoritative FromNode guard: changing only the UID does not reject a move.
func TestMigrate_SourcePodRecreatedAfterAcceptance(t *testing.T) {
	for _, test := range []struct {
		name string
		node string
	}{
		{name: "same node migrates successor", node: "node-a"},
		{name: "different node rejects stale request", node: "node-b"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			f := newSinglePodMigFixture(t)
			pods := f.listPods(t)
			if len(pods) != 1 {
				t.Fatalf("fixture pods = %d, want one source", len(pods))
			}
			// Seed an explicit UID before acceptance; the shared fixture's
			// fake client does not allocate Kubernetes UIDs automatically.
			source := recreateCharacterizationSource(t, f, pods[0], "source-at-acceptance", "node-a")
			const uuid = "source-recreated-after-acceptance"
			f.records = []workload.MigrationRecord{mkMigRecord(uuid, 0, "node-a")}
			recreated := recreateCharacterizationSource(t, f, source, "source-at-execution", test.node)

			done, accepted, err := f.passResult(t, uuid)
			if err != nil || !accepted {
				t.Fatalf("execute accepted request: done=%v accepted=%v err=%v", done, accepted, err)
			}
			current := &corev1.Pod{}
			if err := f.c.Get(ctx, client.ObjectKeyFromObject(source), current); err != nil {
				t.Fatalf("source must remain before surge readiness: %v", err)
			}
			if current.UID != recreated.UID || current.UID == source.UID || current.Spec.NodeName != test.node || current.DeletionTimestamp != nil {
				t.Fatalf("unexpected successor source after first pass: UID=%q node=%q deletion=%v", current.UID, current.Spec.NodeName, current.DeletionTimestamp)
			}
			sourceStatus := findInstanceStatusOnIRForFixture(t, f, 0)
			if test.node != "node-a" {
				if !done {
					t.Fatal("changed source node must terminally reject the stale request")
				}
				assertRecordFailed(t, f, uuid, "does not match observed source node")
				if f.record(t, uuid).SurgeInstance != nil || len(f.listPods(t)) != 1 || sourceStatus == nil || sourceStatus.Phase != v1beta1.OMENativeInstanceReady || sourceStatus.Operation != nil {
					t.Fatalf("node mismatch must not allocate a surge or take source ownership: record=%+v source=%+v", f.record(t, uuid), sourceStatus)
				}
				return
			}

			rec := f.record(t, uuid)
			if done || rec.Phase != workload.MigrationPhaseSurgePending || rec.SurgeInstance == nil || *rec.SurgeInstance != 1 {
				t.Fatalf("same-node successor must start migration: done=%v record=%+v", done, rec)
			}
			if sourceStatus == nil || sourceStatus.Phase != v1beta1.OMENativeInstanceMigrating || sourceStatus.Operation == nil || sourceStatus.Operation.RequestUUID != uuid {
				t.Fatalf("source instance must be owned by the original request: %+v", sourceStatus)
			}
			surge := &corev1.Pod{}
			surgeKey := types.NamespacedName{Namespace: f.isvc.Namespace, Name: query.PodName(f.isvc.Name, f.component, 1, "default", 0)}
			if err := f.c.Get(ctx, surgeKey, surge); err != nil {
				t.Fatalf("migration must create the surge Pod: %v", err)
			}

			// Once readiness converges, the same original request drains
			// the recreated Pod, not the now-absent requesting-time UID.
			f.react(t)
			f.drive(t, uuid, 10)
			if rec = f.record(t, uuid); rec.Phase != workload.MigrationPhaseCompleted || rec.CompletedAt == nil {
				t.Fatalf("migration must complete: %+v", rec)
			}
			if err := f.c.Get(ctx, client.ObjectKeyFromObject(recreated), current); !apierrors.IsNotFound(err) {
				t.Fatalf("successor source must be drained; get returned %v", err)
			}
			ledger, err := audit.LoadLedgerForOwner(ctx, f.c, f.isvc)
			if err != nil {
				t.Fatalf("load terminal ledger: %v", err)
			}
			if len(ledger.Entries) != 1 || ledger.Entries[0].RequestUUID != uuid || ledger.Entries[0].Phase != audit.PhaseCompleted {
				t.Fatalf("terminal ledger = %+v, want original request Completed", ledger.Entries)
			}
		})
	}
}

func recreateCharacterizationSource(t *testing.T, f *migFixture, source *corev1.Pod, uid types.UID, node string) *corev1.Pod {
	t.Helper()
	ctx := context.Background()
	if err := f.c.Delete(ctx, source); err != nil {
		t.Fatalf("delete source UID %q: %v", source.UID, err)
	}
	recreated := source.DeepCopy()
	recreated.ResourceVersion = ""
	recreated.UID = uid
	recreated.Spec.NodeName = node
	if err := f.c.Create(ctx, recreated); err != nil {
		t.Fatalf("create source UID %q: %v", uid, err)
	}
	fresh := &corev1.Pod{}
	if err := f.c.Get(ctx, client.ObjectKeyFromObject(recreated), fresh); err != nil {
		t.Fatalf("read recreated source: %v", err)
	}
	if fresh.UID != uid || fresh.UID == source.UID {
		t.Fatalf("recreated source UID = %q, want %q different from %q", fresh.UID, uid, source.UID)
	}
	return fresh
}
