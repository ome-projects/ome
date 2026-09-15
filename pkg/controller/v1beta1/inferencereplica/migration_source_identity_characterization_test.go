package inferencereplica

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
)

// This characterizes the v1 mailbox contract, not a Pod identity guarantee:
// the request names an instance and source node, but carries no source Pod UID.
func TestConsumeMigrationRequests_SourcePodRecreatedBeforeConsumption(t *testing.T) {
	ctx := context.Background()
	const uuid = "source-recreated-before-consumption"
	key := audit.MigrationRequestAnnotationPrefix + uuid
	ir := baselineIR("llama-engine", "default", 1)
	source := podForIR(ir, 0, "default", 0, true, true)
	source.UID = "source-at-request"
	source.Spec.NodeName = "node-a"
	parent := migrationParent(map[string]string{key: validMigrationRequestJSON("engine", 0)}, false)
	r, c, _, ir, parent := newConsumeFixture(t, ir, parent, source)

	// The original request is already in the mailbox when the source is
	// deleted and recreated with the same name, instance labels, and node.
	if err := c.Delete(ctx, source); err != nil {
		t.Fatalf("delete original source: %v", err)
	}
	recreated := source.DeepCopy()
	recreated.ResourceVersion = ""
	recreated.UID = "source-at-consumption"
	if err := c.Create(ctx, recreated); err != nil {
		t.Fatalf("create successor source: %v", err)
	}

	requeue, err := r.consumeMigrationRequests(ctx, r.Log, ir, parent, workload.MigrationModeAuto, migrationTestTimeout)
	if err != nil || requeue {
		t.Fatalf("consume: requeue=%v err=%v", requeue, err)
	}
	fresh := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(ir), fresh); err != nil {
		t.Fatalf("get persisted IR: %v", err)
	}
	if len(fresh.Status.Migrations) != 1 {
		t.Fatalf("migrations = %+v, want one accepted request", fresh.Status.Migrations)
	}
	entry := fresh.Status.Migrations[0]
	if entry.RequestUUID != uuid || entry.Phase != v1beta1.MigrationPhaseAccepted || entry.SourceInstance != 0 || entry.FromNode != "node-a" || entry.SurgeInstance != nil {
		t.Fatalf("accepted record = %+v, want original instance/node intent without an allocated surge", entry)
	}
	ledger := loadTestLedger(t, c, parent)
	if len(ledger.Entries) != 1 || ledger.Entries[0].RequestUUID != uuid || ledger.Entries[0].Phase != audit.PhaseStarted {
		t.Fatalf("ledger = %+v, want one Started row for the consumed request", ledger.Entries)
	}
	if keys := migrationAnnotationsOf(t, c, parent); len(keys) != 0 {
		t.Fatalf("request annotation was not consumed: %v", keys)
	}
	current := &corev1.Pod{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(source), current); err != nil {
		t.Fatalf("get successor source: %v", err)
	}
	if current.UID != recreated.UID || current.UID == source.UID || current.Spec.NodeName != "node-a" || current.DeletionTimestamp != nil {
		t.Fatalf("acceptance must leave the new-UID source intact: UID=%q node=%q deletion=%v", current.UID, current.Spec.NodeName, current.DeletionTimestamp)
	}
	pods := &corev1.PodList{}
	if err := c.List(ctx, pods, client.InNamespace(ir.Namespace)); err != nil {
		t.Fatalf("list pods after acceptance: %v", err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("acceptance created pods: got %d, want only the successor source", len(pods.Items))
	}
}
