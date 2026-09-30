package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	alfredstatus "sigs.k8s.io/ome/pkg/alfred/irstatus"
	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/scheduling"
	"sigs.k8s.io/ome/pkg/alfred/scheduling/input"
	v1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	codec "sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

// These regressions catch diagnostic status or a released placement envelope
// permanently stranding an unchanged, prepared whole-instance request.
func TestDispatcherSemanticRetry(t *testing.T) {
	for _, gang := range []bool{false, true} {
		for _, compact := range []bool{false, true} {
			for _, change := range []string{"announcement", "failure-diagnostic", "condition-diagnostic", "pause-release"} {
				t.Run(fmt.Sprintf("gang=%t/compact=%t/%s", gang, compact, change), func(t *testing.T) {
					d, cl, _, candidate, cfg, arbiter := dispatchFixture(t, gang)
					updateRetryRow(t, cl.Client, compact, func(row *v1beta1.OMENativeInstanceStatus) {
						row.Conditions = []metav1.Condition{{Type: "AllPodsReady", Status: metav1.ConditionTrue,
							Reason: "Ready", Message: "ready", ObservedGeneration: 1, LastTransitionTime: metav1.NewTime(testNow)}}
					})
					if change == "pause-release" {
						setRetryPlacement(t, cl.Client, 1, false)
					}
					observed, err := d.freshObservation(context.Background(), cfg)
					if err != nil {
						t.Fatal(err)
					}
					cl.failBeforeApply = true
					d.Execute(context.Background(), observed, []policy.Candidate{candidate}, cfg, arbiter)
					before := onlyRetryEntry(t, cl.Client)
					if before.Phase != dispatchPrepared || before.LastAttempt == nil || cl.patches != 1 {
						t.Fatalf("expected a durable failed publication: %+v patches=%d", before, cl.patches)
					}
					cl.failBeforeApply = false
					if change == "pause-release" {
						setRetryPlacement(t, cl.Client, 2, true)
						d.Execute(context.Background(), observed, nil, cfg, &Arbiter{Ledger: NewLedger()})
						paused := onlyRetryEntry(t, cl.Client)
						if cl.patches != 1 || paused.UUID != before.UUID || paused.Payload != before.Payload || paused.Phase != dispatchPrepared {
							t.Fatalf("pause must retain the intent without publication: %+v patches=%d", paused, cl.patches)
						}
						setRetryPlacement(t, cl.Client, 3, false)
					} else {
						updateRetryRow(t, cl.Client, compact, func(row *v1beta1.OMENativeInstanceStatus) {
							switch change {
							case "announcement":
								row.Announced = []string{"PodGroupRebuilt@1"}
							case "failure-diagnostic":
								row.LastFailure = &v1beta1.InstanceTermination{PodName: "previous-pod", Reason: "OOMKilled"}
							case "condition-diagnostic":
								row.Conditions[0].Reason, row.Conditions[0].Message = "ReadyAgain", "same condition state"
								row.Conditions[0].ObservedGeneration++
								row.Conditions[0].LastTransitionTime = metav1.NewTime(testNow.Add(time.Second))
							}
						})
					}
					_, decisions := d.Execute(context.Background(), observed, nil, cfg, &Arbiter{Ledger: NewLedger()})
					got := decisionFor(t, decisions, "prod/a")
					if got.DispatchStatus != dispatchSubmitted || got.RequestUUID != before.UUID {
						t.Fatalf("unchanged source retry did not submit original UUID: %+v", got)
					}
					after := onlyRetryEntry(t, cl.Client)
					var owner v1beta1.InferenceService
					if err := cl.Client.Get(context.Background(), candidate.Workload, &owner); err != nil {
						t.Fatal(err)
					}
					if after.UUID != before.UUID || after.Payload != before.Payload || after.SourceFingerprint != before.SourceFingerprint ||
						owner.Annotations[migrationRequestPrefix+before.UUID] != before.Payload || cl.patches != 2 {
						t.Fatalf("retry changed intent or publication: %+v patches=%d", after, cl.patches)
					}
				})
			}
		}
	}
}

func TestDispatchSemanticFingerprintRetainsSafetyMeaning(t *testing.T) {
	type source struct {
		owner *v1beta1.InferenceService
		ir    *v1beta1.InferenceReplica
		pods  []corev1.Pod
	}
	for _, tc := range []struct {
		name   string
		mutate func(source)
	}{
		{"owner-uid", func(s source) { s.owner.UID = "other" }},
		{"owner-name", func(s source) { s.owner.Name = "other" }},
		{"owner-spec", func(s source) { s.owner.Spec.DeploymentMode = ptr.To(constants.RawDeployment) }},
		{"ir-uid", func(s source) { s.ir.UID = "other" }},
		{"ir-namespace", func(s source) { s.ir.Namespace = "other" }},
		{"ir-owner", func(s source) { s.ir.OwnerReferences[0].UID = "other" }},
		{"template", func(s source) { s.ir.Spec.Runners[0].Template.Spec.Containers[0].Image = "different" }},
		{"runner-size", func(s source) { s.ir.Spec.Runners[0].Size++ }},
		{"topology", func(s source) { s.ir.Spec.TopologyKey = ptr.To("rack") }},
		{"replicas", func(s source) { s.ir.Spec.Replicas = ptr.To(int32(2)) }},
		{"placement-limit", func(s source) { s.ir.Spec.PlacementReplicaLimit = ptr.To(int32(2)) }},
		{"current-revision", func(s source) { s.ir.Status.CurrentRevision = "other" }},
		{"update-revision", func(s source) { s.ir.Status.UpdateRevision = "other" }},
		{"incarnation", func(s source) { s.ir.Status.InstanceStatuses[0].Incarnation++ }},
		{"active-ordinal", func(s source) { s.ir.Status.InstanceStatuses[0].ActiveOrdinal++ }},
		{"phase", func(s source) { s.ir.Status.InstanceStatuses[0].Phase = "Draining" }},
		{"running-revision", func(s source) { s.ir.Status.InstanceStatuses[0].RunningRevision = "other" }},
		{"target-revision", func(s source) { s.ir.Status.InstanceStatuses[0].TargetRevision = "other" }},
		{"pod-count", func(s source) { s.ir.Status.InstanceStatuses[0].PodCount++ }},
		{"serving-count", func(s source) { s.ir.Status.InstanceStatuses[0].ServingPodCount-- }},
		{"available-count", func(s source) { s.ir.Status.InstanceStatuses[0].AvailablePodCount-- }},
		{"admission", func(s source) { s.ir.Status.InstanceStatuses[0].Admitted = false }},
		{"ready-since", func(s source) { s.ir.Status.InstanceStatuses[0].ReadySince = ptr.To(metav1.NewTime(testNow)) }},
		{"operation", func(s source) {
			s.ir.Status.InstanceStatuses[0].Operation = &v1beta1.InstanceOperation{ID: "operation"}
		}},
		{"unknown-condition-state", func(s source) { s.ir.Status.InstanceStatuses[0].Conditions[0].Status = metav1.ConditionFalse }},
		{"unknown-condition-type", func(s source) { s.ir.Status.InstanceStatuses[0].Conditions[0].Type = "DifferentCondition" }},
		{"member-uid", func(s source) { s.pods[0].UID = "other" }},
		{"member-node", func(s source) { s.pods[0].Spec.NodeName = "other" }},
		{"member-spec", func(s source) { s.pods[0].Spec.Containers[0].Image = "other" }},
		{"member-label", func(s source) { s.pods[0].Labels["ome.io/instance-incarnation"] = "2" }},
		{"member-annotation", func(s source) { s.pods[0].Annotations = map[string]string{"placement": "changed"} }},
		{"plan-identity", func(s source) {
			p := s.ir.Spec.PlacementExecution
			p.PlanID = "other"
			raw, _ := protocol.Encode(p)
			s.owner.Annotations[constants.PlacementExecution] = raw
		}},
		{"source-identity", func(s source) {
			p := s.ir.Spec.PlacementExecution
			p.SourceUID = "other"
			raw, _ := protocol.Encode(p)
			s.owner.Annotations[constants.PlacementOriginUID], s.owner.Annotations[constants.PlacementExecution] = "other", raw
		}},
		{"cluster-identity", func(s source) {
			p := s.ir.Spec.PlacementExecution
			p.ClusterUID = "other"
			raw, _ := protocol.Encode(p)
			s.owner.Annotations[constants.PlacementExecution] = raw
		}},
		{"unknown-authority-field", func(s source) {
			raw := s.owner.Annotations[constants.PlacementExecution]
			s.owner.Annotations[constants.PlacementExecution] = strings.TrimSuffix(raw, "}") + `,"futureAuthority":"changed"}`
		}},
		{"unknown-template-authority-field", func(s source) {
			annotations := s.ir.Spec.Runners[0].Template.Annotations
			annotations[constants.PlacementExecution] = strings.TrimSuffix(annotations[constants.PlacementExecution], "}") + `,"futureAuthority":"changed"}`
		}},
		{"unrelated-template-authority", func(s source) {
			p := s.ir.Spec.PlacementExecution.DeepCopy()
			p.PlanID, p.Revision = "other-plan", 2
			raw, _ := protocol.Encode(p)
			s.ir.Spec.Runners[0].Template.Annotations[constants.PlacementExecution] = raw
		}},
		{"malformed-template-authority", func(s source) {
			s.ir.Spec.Runners[0].Template.Annotations[constants.PlacementExecution] = "malformed"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, cl, _, candidate, _, _ := dispatchFixture(t, true)
			setRetryPlacement(t, cl.Client, 1, false)
			owner, ir, pods := retryFingerprintSource(t, cl.Client, candidate)
			ir.Status.InstanceStatuses[0].Conditions = []metav1.Condition{{Type: "FutureCondition", Status: metav1.ConditionTrue}}
			before, err := dispatchSourceFingerprint(owner, ir, pods, candidate.Instance, dispatchFingerprintSemantic)
			if err != nil {
				t.Fatal(err)
			}
			tc.mutate(source{owner, ir, pods})
			after, err := dispatchSourceFingerprint(owner, ir, pods, candidate.Instance, dispatchFingerprintSemantic)
			if tc.name == "unknown-authority-field" {
				// The transport rejects unknown authority fields, so the
				// fingerprint fails closed instead of authorizing a retry.
				if err == nil {
					t.Fatal("unknown authority field was accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if before == after {
				t.Fatal("material source change retained retry authorization")
			}
		})
	}
}

func TestDispatcherRejectsGenerationChangeDuringSimulation(t *testing.T) {
	d, cl, _, candidate, cfg, arbiter := dispatchFixture(t, false)
	setRetryPlacement(t, cl.Client, 1, false)
	observed, err := d.freshObservation(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	d.Simulator = simulationFunc(func(_ context.Context, request scheduling.Request) (scheduling.Result, error) {
		setRetryPlacement(t, cl.Client, 2, true)
		setRetryPlacement(t, cl.Client, 3, false)
		return feasiblePrediction(request), nil
	})
	_, decisions := d.Execute(context.Background(), observed, []policy.Candidate{candidate}, cfg, arbiter)
	got := decisionFor(t, decisions, "prod/a")
	_, journal, err := loadDispatchJournal(context.Background(), cl.Client, "ome")
	if err != nil || cl.patches != 0 || len(journal.Entries) != 0 || got.DispatchReason != "SourceChanged" {
		t.Fatalf("same-attempt generation fence was lost: %+v patches=%d journal=%+v err=%v", got, cl.patches, journal, err)
	}
}

func TestDispatchFingerprintNormalizesWithoutMutatingInputs(t *testing.T) {
	_, cl, _, candidate, _, _ := dispatchFixture(t, true)
	setRetryPlacement(t, cl.Client, 1, false)
	owner, ir, pods := retryFingerprintSource(t, cl.Client, candidate)
	ir.Status.InstanceStatuses[0].Conditions = []metav1.Condition{
		{Type: "Z", Status: metav1.ConditionFalse}, {Type: "A", Status: metav1.ConditionTrue},
	}
	ownerBefore, irBefore := owner.DeepCopy(), ir.DeepCopy()
	podsBefore := (&corev1.PodList{Items: pods}).DeepCopy().Items
	before, err := dispatchSourceFingerprint(owner, ir, pods, candidate.Instance, dispatchFingerprintSemantic)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ownerBefore, owner) || !reflect.DeepEqual(irBefore, ir) || !reflect.DeepEqual(podsBefore, pods) {
		t.Fatal("fingerprint mutated its observation inputs")
	}
	owner.Generation++
	owner.ResourceVersion, ir.ResourceVersion = "changed", "changed"
	ir.Generation++
	ir.Status.ObservedGeneration++
	ir.Spec.PlacementExecution.Revision++
	raw, err := protocol.Encode(ir.Spec.PlacementExecution)
	if err != nil {
		t.Fatal(err)
	}
	owner.Annotations[constants.PlacementExecution] = raw
	row := &ir.Status.InstanceStatuses[0]
	row.ReadyPodCount, row.ScheduledPodCount, row.NodesOccupied = 2, 2, []string{"source", "source-worker"}
	row.Conditions[0], row.Conditions[1] = row.Conditions[1], row.Conditions[0]
	pods[0], pods[1] = pods[1], pods[0]
	pods[0].ResourceVersion, pods[0].Status.Message = "new-version", "diagnostic"
	after, err := dispatchSourceFingerprint(owner, ir, pods, candidate.Instance, dispatchFingerprintSemantic)
	if err != nil || before != after {
		t.Fatalf("equivalent observations changed retry identity: %s %s %v", before, after, err)
	}
}

func retryFingerprintSource(t *testing.T, reader client.Reader, candidate policy.Candidate) (*v1beta1.InferenceService, *v1beta1.InferenceReplica, []corev1.Pod) {
	t.Helper()
	var owner v1beta1.InferenceService
	var ir v1beta1.InferenceReplica
	if err := reader.Get(context.Background(), candidate.Workload, &owner); err != nil {
		t.Fatal(err)
	}
	if err := reader.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "a-engine"}, &ir); err != nil {
		t.Fatal(err)
	}
	var pods corev1.PodList
	if err := reader.List(context.Background(), &pods, client.InNamespace("prod")); err != nil {
		t.Fatal(err)
	}
	return &owner, &ir, pods.Items
}

func TestDispatcherFingerprintVersionCompatibility(t *testing.T) {
	for _, mode := range []string{"legacy-unchanged", "legacy-changed", "unknown-version", "malformed-version", "prefixed-submitted"} {
		t.Run(mode, func(t *testing.T) {
			d, cl, observed, candidate, cfg, arbiter := dispatchFixture(t, false)
			cl.failBeforeApply = true
			d.Execute(context.Background(), observed, []policy.Candidate{candidate}, cfg, arbiter)
			before := onlyRetryEntry(t, cl.Client)
			if !strings.HasPrefix(before.SourceFingerprint, "semantic-v1:") {
				t.Fatal("new intent lacks a versioned semantic fence")
			}
			cm, journal, err := loadDispatchJournal(context.Background(), cl.Client, "ome")
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "legacy-unchanged", "legacy-changed":
				captured, err := input.Capture(context.Background(), cl.Client, d.now)
				if err != nil {
					t.Fatal(err)
				}
				request, err := input.BuildExecutionRequest(captured, input.Source{Namespace: "prod", InferenceService: "a", Component: candidate.Component, FromNode: "source"}, cfg.Scheduling, before.UUID, candidate.HintTargetNodes, d.now(), predictionMaxAge)
				if err != nil {
					t.Fatal(err)
				}
				owner, ir, _ := retryFingerprintSource(t, cl.Client, candidate)
				journal.Entries[0].SourceFingerprint, err = dispatchSourceFingerprint(owner, ir, request.SourcePods, candidate.Instance, dispatchFingerprintLegacy)
				if err != nil {
					t.Fatal(err)
				}
			case "unknown-version":
				journal.Entries[0].SourceFingerprint = "semantic-v99:" + strings.Repeat("a", 64)
			case "malformed-version":
				journal.Entries[0].SourceFingerprint = "semantic-v1:not-a-digest"
			case "prefixed-submitted":
				// Even a future fingerprint must not block reconciliation of an
				// already published request. No resubmission is needed or safe.
				journal.Entries[0].SourceFingerprint = "semantic-v99:" + strings.Repeat("a", 64)
				var owner v1beta1.InferenceService
				if err := cl.Client.Get(context.Background(), candidate.Workload, &owner); err != nil {
					t.Fatal(err)
				}
				owner.Annotations = map[string]string{migrationRequestPrefix + before.UUID: before.Payload}
				if err := cl.Client.Update(context.Background(), &owner); err != nil {
					t.Fatal(err)
				}
			}
			if err := saveDispatchJournal(context.Background(), cl.Client, cm, journal); err != nil {
				t.Fatal(err)
			}
			fingerprint := journal.Entries[0].SourceFingerprint
			if mode == "legacy-changed" {
				updateRetryRow(t, cl.Client, true, func(row *v1beta1.OMENativeInstanceStatus) { row.Announced = []string{"Event@1"} })
			}
			cl.failBeforeApply = false
			d.Execute(context.Background(), observed, nil, cfg, &Arbiter{Ledger: NewLedger()})
			after := onlyRetryEntry(t, cl.Client)
			if before.UUID != after.UUID || before.Payload != after.Payload || fingerprint != after.SourceFingerprint {
				t.Fatalf("existing intent was rewritten: %+v", after)
			}
			wantPhase, wantPatches := dispatchPrepared, 1
			if mode == "legacy-unchanged" {
				wantPhase, wantPatches = dispatchSubmitted, 2
			} else if mode == "prefixed-submitted" {
				wantPhase = dispatchSubmitted
			}
			if after.Phase != wantPhase || cl.patches != wantPatches {
				t.Fatalf("compatibility: phase=%s patches=%d; want %s/%d", after.Phase, cl.patches, wantPhase, wantPatches)
			}
		})
	}
}

func TestDispatchLegacyFingerprintWireCompatibility(t *testing.T) {
	// Frozen pre-upgrade JSON input, independently hashed with SHA-256. Do not
	// regenerate this expectation using either fingerprint implementation.
	const raw = `{"OwnerUID":"owner","OwnerGeneration":2,"IRUID":"replica","IRGeneration":3,"Revision":"rev","Row":{"index":7,"incarnation":2,"phase":"Ready","runningRevision":"rev","podCount":1,"servingPodCount":1,"availablePodCount":1,"admitted":true},"Pods":[]}`
	var fixture struct {
		OwnerUID, IRUID, Revision     string
		OwnerGeneration, IRGeneration int64
		Row                           v1beta1.OMENativeInstanceStatus
		Pods                          []corev1.Pod
	}
	if err := json.Unmarshal([]byte(raw), &fixture); err != nil {
		t.Fatal(err)
	}
	owner := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{UID: types.UID(fixture.OwnerUID), Generation: fixture.OwnerGeneration}}
	ir := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{UID: types.UID(fixture.IRUID), Generation: fixture.IRGeneration},
		Status: v1beta1.InferenceReplicaStatus{CurrentRevision: fixture.Revision, InstanceStatuses: []v1beta1.OMENativeInstanceStatus{fixture.Row}}}
	got, err := dispatchSourceFingerprint(owner, ir, fixture.Pods, 7, dispatchFingerprintLegacy)
	if err != nil || got != "96fd6049dffd03975384c7e9636677211954507d778a8dab5ead1d3ad2ed6a44" {
		t.Fatalf("legacy fingerprint changed: %s %v", got, err)
	}
}

func onlyRetryEntry(t *testing.T, reader client.Reader) dispatchEntry {
	t.Helper()
	_, journal, err := loadDispatchJournal(context.Background(), reader, "ome")
	if err != nil || len(journal.Entries) != 1 {
		t.Fatalf("expected exactly one durable intent: %+v, %v", journal, err)
	}
	return journal.Entries[0]
}

func updateRetryRow(t *testing.T, cl client.Client, compact bool, mutate func(*v1beta1.OMENativeInstanceStatus)) {
	t.Helper()
	var ir v1beta1.InferenceReplica
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "a-engine"}, &ir); err != nil {
		t.Fatal(err)
	}
	rows, err := alfredstatus.Rows(&ir.Status)
	if err != nil || len(rows) != 1 {
		t.Fatalf("source rows: %+v, %v", rows, err)
	}
	mutate(&rows[0])
	ir.Status.InstanceStatuses, ir.Status.InstanceStatusColumns, ir.Status.InstanceStatusEncoding = rows, nil, nil
	if compact {
		columns, err := codec.EncodeColumns(rows, alfredstatus.MaxRows)
		if err != nil {
			t.Fatal(err)
		}
		marker := v1beta1.InstanceStatusEncodingColumnarV2
		ir.Status.InstanceStatuses, ir.Status.InstanceStatusColumns, ir.Status.InstanceStatusEncoding = nil, columns, &marker
	}
	if err := cl.Update(context.Background(), &ir); err != nil {
		t.Fatal(err)
	}
}

func setRetryPlacement(t *testing.T, cl client.Client, revision int64, paused bool) {
	t.Helper()
	placement := &v1beta1.PlacementExecutionPolicy{PlanID: "plan", Revision: revision, SourceUID: "source", ClusterUID: "cluster", PauseSurge: paused}
	raw, err := protocol.Encode(placement)
	if err != nil {
		t.Fatal(err)
	}
	var owner v1beta1.InferenceService
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "a"}, &owner); err != nil {
		t.Fatal(err)
	}
	if owner.Annotations == nil {
		owner.Annotations = map[string]string{}
	}
	owner.Annotations[constants.PlacementOriginUID], owner.Annotations[constants.PlacementExecution] = "source", raw
	if err := cl.Update(context.Background(), &owner); err != nil {
		t.Fatal(err)
	}
	var ir v1beta1.InferenceReplica
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "a-engine"}, &ir); err != nil {
		t.Fatal(err)
	}
	ir.Spec.PlacementExecution = placement
	// Public ISVC metadata is also projected into every runner template.
	for i := range ir.Spec.Runners {
		if ir.Spec.Runners[i].Template.Annotations == nil {
			ir.Spec.Runners[i].Template.Annotations = map[string]string{}
		}
		ir.Spec.Runners[i].Template.Annotations[constants.PlacementExecution] = raw
	}
	// The fake client does not advance generation or run the real projection.
	// Model a fully observed spec update; live coverage uses the real controller.
	ir.Generation++
	ir.Status.ObservedGeneration = ir.Generation
	if err := cl.Update(context.Background(), &ir); err != nil {
		t.Fatal(err)
	}
}
