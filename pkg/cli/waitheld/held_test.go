package waitheld

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/yaml"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func heldFixture() (Target, Evidence) {
	controller := true
	parent := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "chat", Namespace: "demo", UID: types.UID("parent-uid"), ResourceVersion: "17", Generation: 7,
	}}
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name: "chat-engine", Namespace: "demo", UID: types.UID("ir-uid"), ResourceVersion: "31", Generation: 2,
			Labels:          map[string]string{constants.InferenceServiceLabel: "chat", constants.OMEComponentLabel: "engine"},
			Annotations:     map[string]string{constants.InferenceReplicaParentGenerationAnnotationKey: "7", constants.InferenceReplicaControllerWriteAnnotationKey: "true"},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "ome.io/v1beta1", Kind: "InferenceService", Name: "chat", UID: parent.UID, Controller: &controller}},
		},
		Spec: v1beta1.InferenceReplicaSpec{ParentRef: v1beta1.ParentReference{Name: "chat"}, Component: v1beta1.EngineComponent},
		Status: v1beta1.InferenceReplicaStatus{ObservedGeneration: 2, RetryBlocks: []v1beta1.RetryBlock{{
			TargetRevision: "chat-engine-aaaaaaaa", State: v1beta1.RetryBlockHeld, AttemptsStarted: 1,
		}}},
	}
	return Target{Namespace: "demo", ParentName: "chat", Component: "engine", IRName: "chat-engine", Revision: "chat-engine-aaaaaaaa", IRUID: "ir-uid"},
		Evidence{Parent: parent, Replica: ir, Complete: true, Sources: 1}
}

func TestHeldBlockRemainsUnmet(t *testing.T) {
	target, evidence := heldFixture()
	got := Evaluate(target, evidence)
	if got.Matched || got.Reason != ReasonHeld || got.TargetState != TargetHeld {
		t.Fatalf("Held target must be unmet: %+v", got)
	}
}

func TestCurrentAbsentBlockMatchesEvenAtZeroReplicas(t *testing.T) {
	target, evidence := heldFixture()
	evidence.Replica.Status.RetryBlocks = nil
	got := Evaluate(target, evidence)
	if !got.Matched || got.Reason != ReasonUnheld || got.TargetState != TargetAbsent || got.Attribution != AttributionUnverifiable {
		t.Fatalf("current exact empty block set must be state-only unheld: %+v", got)
	}
}

func TestNonHeldTargetStateMatchesOnlyAfterMailboxGone(t *testing.T) {
	for _, state := range []v1beta1.RetryBlockState{v1beta1.RetryBlockBackoff, v1beta1.RetryBlockRetryInProgress} {
		t.Run(string(state), func(t *testing.T) {
			target, evidence := heldFixture()
			evidence.Replica.Status.RetryBlocks[0].State = state
			if state == v1beta1.RetryBlockBackoff {
				when := metav1.NewTime(time.Date(2026, 9, 18, 0, 0, 0, 0, time.UTC))
				evidence.Replica.Status.RetryBlocks[0].NextRetryAt = &when
			}
			got := Evaluate(target, evidence)
			if !got.Matched || got.Reason != ReasonUnheld {
				t.Fatalf("non-Held target with absent mailbox should match: %+v", got)
			}
			evidence.Replica.Annotations[constants.ReleaseHeldRevisionAnnotationKey] = target.Revision
			got = Evaluate(target, evidence)
			if got.Matched || got.Reason != ReasonMailboxPending {
				t.Fatalf("mailbox still present cannot match: %+v", got)
			}
		})
	}
}

func TestUnchargedFailureWaveDoesNotInvalidateUnheldState(t *testing.T) {
	for _, state := range []v1beta1.RetryBlockState{v1beta1.RetryBlockBackoff, v1beta1.RetryBlockRetryInProgress} {
		for _, unrelated := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/unrelated=%t", state, unrelated), func(t *testing.T) {
				target, evidence := heldFixture()
				block := &evidence.Replica.Status.RetryBlocks[0]
				block.State = state
				block.AttemptsStarted = 0 // Environment-caused waves do not charge an attempt.
				if state == v1beta1.RetryBlockBackoff {
					when := metav1.NewTime(time.Date(2026, 9, 18, 0, 0, 0, 0, time.UTC))
					block.NextRetryAt = &when
				}
				if unrelated {
					block.TargetRevision = "chat-engine-bbbbbbbb"
				}
				got := Evaluate(target, evidence)
				if !got.Matched || got.Reason != ReasonUnheld {
					t.Fatalf("uncharged non-Held block must allow exact unheld state: %+v", got)
				}
			})
		}
	}
}

func TestMailboxOtherOrEmptyIsNeverConsumedEvidence(t *testing.T) {
	for _, value := range []string{"", "chat-engine-bbbbbbbb"} {
		target, evidence := heldFixture()
		evidence.Replica.Status.RetryBlocks = nil
		evidence.Replica.Annotations[constants.ReleaseHeldRevisionAnnotationKey] = value
		got := Evaluate(target, evidence)
		if got.Matched || got.Reason != ReasonMailboxSuperseded {
			t.Fatalf("other/empty mailbox must remain unmet: %+v", got)
		}
	}
}

func TestUnrelatedHeldBlockDoesNotPreventExactRevisionMatch(t *testing.T) {
	target, evidence := heldFixture()
	evidence.Replica.Status.RetryBlocks[0].TargetRevision = "chat-engine-bbbbbbbb"
	got := Evaluate(target, evidence)
	if !got.Matched || got.TargetState != TargetAbsent {
		t.Fatalf("other target must not be confused with the requested revision: %+v", got)
	}
}

func TestMalformedBlocksNeverBecomeAbsenceEvidence(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*v1beta1.InferenceReplica)
	}{
		{"duplicate", func(ir *v1beta1.InferenceReplica) {
			ir.Status.RetryBlocks = append(ir.Status.RetryBlocks, ir.Status.RetryBlocks[0])
		}},
		{"invalid revision", func(ir *v1beta1.InferenceReplica) { ir.Status.RetryBlocks[0].TargetRevision = "PRIVATE\nREVISION" }},
		{"invalid state", func(ir *v1beta1.InferenceReplica) { ir.Status.RetryBlocks[0].State = "PrivateState" }},
		{"zero attempts", func(ir *v1beta1.InferenceReplica) { ir.Status.RetryBlocks[0].AttemptsStarted = 0 }},
		{"negative attempts", func(ir *v1beta1.InferenceReplica) {
			ir.Status.RetryBlocks[0].State = v1beta1.RetryBlockRetryInProgress
			ir.Status.RetryBlocks[0].AttemptsStarted = -1
		}},
		{"Held with retry time", func(ir *v1beta1.InferenceReplica) { now := metav1.Now(); ir.Status.RetryBlocks[0].NextRetryAt = &now }},
		{"too many", func(ir *v1beta1.InferenceReplica) { ir.Status.RetryBlocks = make([]v1beta1.RetryBlock, 65) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target, evidence := heldFixture()
			// A malformed unrelated block also poisons any apparent absence.
			evidence.Replica.Status.RetryBlocks[0].TargetRevision = "chat-engine-bbbbbbbb"
			tc.edit(evidence.Replica)
			got := Evaluate(target, evidence)
			if got.Matched || got.Reason != ReasonInvalidRetryBlocks {
				t.Fatalf("malformed retry blocks must remain unmet: %+v", got)
			}
		})
	}
}

func TestExactUIDCurrentnessAndCompleteMembershipRequired(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*Target, *Evidence)
		want Reason
	}{
		{"missing IR", func(_ *Target, e *Evidence) { e.Replica, e.Sources = nil, 0 }, ReasonReplicaMissing},
		{"replacement UID", func(_ *Target, e *Evidence) { e.Replica.UID = "replacement" }, ReasonReplicaReplaced},
		{"deleting IR", func(_ *Target, e *Evidence) { now := metav1.Now(); e.Replica.DeletionTimestamp = &now }, ReasonDeleting},
		{"stale IR generation", func(_ *Target, e *Evidence) { e.Replica.Status.ObservedGeneration-- }, ReasonSourceStale},
		{"stale parent generation", func(_ *Target, e *Evidence) {
			e.Replica.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] = "6"
		}, ReasonSourceStale},
		{"controller marker absent", func(_ *Target, e *Evidence) {
			delete(e.Replica.Annotations, constants.InferenceReplicaControllerWriteAnnotationKey)
		}, ReasonSourceInvalid},
		{"changed source", func(_ *Target, e *Evidence) { e.Complete = false }, ReasonSourceIncomplete},
		{"replica without read", func(_ *Target, e *Evidence) { e.Sources = 0 }, ReasonSourceIncomplete},
		{"multiple source count", func(_ *Target, e *Evidence) { e.Sources = 2 }, ReasonSourceIncomplete},
		{"parent ref", func(_ *Target, e *Evidence) { e.Replica.Spec.ParentRef.Name = "other" }, ReasonSourceInvalid},
		{"owner UID", func(_ *Target, e *Evidence) { e.Replica.OwnerReferences[0].UID = "other" }, ReasonSourceInvalid},
		{"spec component", func(_ *Target, e *Evidence) { e.Replica.Spec.Component = v1beta1.DecoderComponent }, ReasonSourceInvalid},
		{"different exact name", func(_ *Target, e *Evidence) { e.Replica.Name = "other-engine" }, ReasonSourceInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target, evidence := heldFixture()
			evidence.Replica.Status.RetryBlocks = nil
			tc.edit(&target, &evidence)
			got := Evaluate(target, evidence)
			if got.Matched || got.Reason != tc.want {
				t.Fatalf("unsafe source must not match: %+v", got)
			}
		})
	}
}

func TestActionCompatibleReplicaIdentityWithoutOptionalLabelsMatches(t *testing.T) {
	for _, longParent := range []bool{false, true} {
		t.Run(fmt.Sprintf("longParent=%t", longParent), func(t *testing.T) {
			target, evidence := heldFixture()
			if longParent {
				target.ParentName = strings.Repeat("a", 64)
				target.Revision = target.ParentName + "-engine-aaaaaaaa"
				evidence.Parent.Name = target.ParentName
				evidence.Replica.Spec.ParentRef.Name = target.ParentName
				evidence.Replica.OwnerReferences[0].Name = target.ParentName
				delete(evidence.Replica.Labels, constants.InferenceServiceLabel)
			}
			delete(evidence.Replica.Labels, constants.OMEComponentLabel)
			evidence.Replica.Status.RetryBlocks = nil
			got := Evaluate(target, evidence)
			if !got.Matched || got.Reason != ReasonUnheld {
				t.Fatalf("action-compatible owner/spec identity must match without optional labels: %+v", got)
			}
		})
	}
}

func TestActionCompatibleOpaqueUIDAndResourceVersionsMatch(t *testing.T) {
	target, evidence := heldFixture()
	target.IRUID = strings.Repeat("u", 256)
	evidence.Parent.UID = "arn:aws:parent/abc+v1"
	evidence.Parent.ResourceVersion = "rv:parent/17+1"
	evidence.Replica.UID = types.UID(target.IRUID)
	evidence.Replica.ResourceVersion = "rv:ir/31+1"
	evidence.Replica.OwnerReferences[0].UID = evidence.Parent.UID
	evidence.Replica.Status.RetryBlocks = nil
	got := Evaluate(target, evidence)
	if !got.Matched || got.Reason != ReasonUnheld {
		t.Fatalf("action-compatible opaque UID/RV must remain observable: %+v", got)
	}
}

func TestValidatedActionTargetRejectsUnsafeUIDBeforeAcquisition(t *testing.T) {
	target, _ := heldFixture()
	if !ValidTarget(target) {
		t.Fatal("ordinary exact ActionResult target must validate")
	}
	for _, uid := range []string{"", strings.Repeat("u", 257), "user:password@host", "uid\nPRIVATE"} {
		target.IRUID = uid
		if ValidTarget(target) {
			t.Fatalf("unsafe UID must be rejected before API access: %q", uid)
		}
	}
	target.IRUID = "ghp_0123456789abcdefghijklmnopqrst"
	if !ValidTarget(target) {
		t.Fatal("credential-shaped but action-valid UID must remain operable and redacted at output")
	}
}

func TestOpaqueIdentityGuardMatchesActionBoundsAndUserinfoRule(t *testing.T) {
	for _, tc := range []struct {
		value string
		valid bool
	}{
		{value: "arn:aws:parent/abc+v1", valid: true},
		{value: "user@host", valid: true},
		{value: strings.Repeat("u", 256), valid: true},
		{value: "ghp_0123456789abcdefghijklmnopqrst", valid: true},
		{value: "", valid: false},
		{value: strings.Repeat("u", 257), valid: false},
		{value: "user:password@host", valid: false},
		{value: "https://user:password@host", valid: false},
		{value: "sk-proj-0123456789abcdefghijklmnopqrst", valid: false},
		{value: "uid\nPRIVATE", valid: false},
	} {
		if got := ValidIdentity(tc.value); got != tc.valid {
			t.Errorf("identity guard for %q = %t, want %t", tc.value, got, tc.valid)
		}
	}
}

func TestHeldRevisionRejectsFutureAndInconsistentBlockTimes(t *testing.T) {
	now := time.Now().UTC()
	first := metav1.NewTime(now.Add(-2 * time.Minute))
	last := metav1.NewTime(now.Add(-time.Minute))
	future := metav1.NewTime(now.Add(time.Hour))
	earlier := metav1.NewTime(now.Add(-3 * time.Minute))
	zero := metav1.Time{}
	for _, tc := range []struct {
		name string
		edit func(*v1beta1.RetryBlock)
	}{
		{name: "future first failure", edit: func(b *v1beta1.RetryBlock) { b.FirstFailureAt = &future }},
		{name: "future last failure", edit: func(b *v1beta1.RetryBlock) { b.LastFailureAt = &future }},
		{name: "zero timestamp", edit: func(b *v1beta1.RetryBlock) { b.FirstFailureAt = &zero }},
		{name: "last before first", edit: func(b *v1beta1.RetryBlock) { b.FirstFailureAt, b.LastFailureAt = &first, &earlier }},
		{name: "retry before first", edit: func(b *v1beta1.RetryBlock) {
			b.State, b.FirstFailureAt, b.LastFailureAt, b.NextRetryAt = v1beta1.RetryBlockBackoff, &first, &last, &earlier
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target, evidence := heldFixture()
			tc.edit(&evidence.Replica.Status.RetryBlocks[0])
			got := Evaluate(target, evidence)
			if got.Matched || got.Reason != ReasonInvalidRetryBlocks || got.Validity != ValidityInvalid {
				t.Fatalf("invalid block times must not be promoted to unheld evidence: %+v", got)
			}
		})
	}
}

func TestPlacementSignalsAndBadTargetNeverMatch(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*Target, *Evidence)
		want Reason
	}{
		{"parent finalizer", func(_ *Target, e *Evidence) { e.Parent.Finalizers = []string{"ome.io/placement"} }, ReasonUnsupportedPlacement},
		{"parent origin label", func(_ *Target, e *Evidence) { e.Parent.Labels = map[string]string{constants.PlacementOrigin: ""} }, ReasonUnsupportedPlacement},
		{"IR origin annotation", func(_ *Target, e *Evidence) { e.Replica.Annotations[constants.PlacementOriginUID] = "" }, ReasonUnsupportedPlacement},
		{"IR placement finalizer", func(_ *Target, e *Evidence) { e.Replica.Finalizers = []string{"ome.io/placement"} }, ReasonUnsupportedPlacement},
		{"wrong revision hash grammar", func(t *Target, _ *Evidence) { t.Revision = "chat-engine-AAAAAAAa" }, ReasonInvalidTarget},
		{"bare hash", func(t *Target, _ *Evidence) { t.Revision = "aaaaaaaa" }, ReasonInvalidTarget},
		{"missing expected UID", func(t *Target, _ *Evidence) { t.IRUID = "" }, ReasonInvalidTarget},
		{"missing exact name", func(t *Target, _ *Evidence) { t.IRName = "" }, ReasonInvalidTarget},
		{"malformed exact name", func(t *Target, _ *Evidence) { t.IRName = "other_engine" }, ReasonInvalidTarget},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target, evidence := heldFixture()
			evidence.Replica.Status.RetryBlocks = nil
			tc.edit(&target, &evidence)
			got := Evaluate(target, evidence)
			if got.Matched || got.Reason != tc.want {
				t.Fatalf("unsafe target or placement must remain unmet: %+v", got)
			}
		})
	}
}

func TestCompactedStatusMustBeValidBeforeEmptyBlockMatch(t *testing.T) {
	for _, tc := range []struct {
		name      string
		edit      func(*v1beta1.InferenceReplica)
		wantMatch bool
	}{
		{"valid ColumnarV2", func(ir *v1beta1.InferenceReplica) {
			marker := v1beta1.InstanceStatusEncodingColumnarV2
			ir.Status.InstanceStatusEncoding = &marker
			ir.Status.InstanceStatusColumns = &v1beta1.InstanceStatusColumns{Members: "0", Phases: []v1beta1.InstanceStatusPhaseGroup{{Value: v1beta1.OMENativeInstanceReady, Indexes: "0"}}}
		}, true},
		{"mixed dense and columns", func(ir *v1beta1.InferenceReplica) {
			marker := v1beta1.InstanceStatusEncodingColumnarV2
			ir.Status.InstanceStatusEncoding = &marker
			ir.Status.InstanceStatusColumns = &v1beta1.InstanceStatusColumns{Members: "0", Phases: []v1beta1.InstanceStatusPhaseGroup{{Value: v1beta1.OMENativeInstanceReady, Indexes: "0"}}}
			ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 0}}
		}, false},
		{"malformed column membership", func(ir *v1beta1.InferenceReplica) {
			marker := v1beta1.InstanceStatusEncodingColumnarV2
			ir.Status.InstanceStatusEncoding = &marker
			ir.Status.InstanceStatusColumns = &v1beta1.InstanceStatusColumns{Members: "0-1", Phases: []v1beta1.InstanceStatusPhaseGroup{{Value: v1beta1.OMENativeInstanceReady, Indexes: "0"}}}
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target, evidence := heldFixture()
			evidence.Replica.Status.RetryBlocks = nil
			tc.edit(evidence.Replica)
			got := Evaluate(target, evidence)
			if got.Matched != tc.wantMatch || !tc.wantMatch && got.Reason != ReasonInvalidStatus {
				t.Fatalf("compacted representation validity: %+v", got)
			}
		})
	}
}

func TestObservationNeverSerializesRawRetryReasonOrMailbox(t *testing.T) {
	target, evidence := heldFixture()
	evidence.Replica.Status.RetryBlocks[0].Reason = "PRIVATE_RETRY_REASON"
	evidence.Replica.Annotations[constants.ReleaseHeldRevisionAnnotationKey] = "PRIVATE_MAILBOX_VALUE"
	encoded, err := json.Marshal(Evaluate(target, evidence))
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"PRIVATE_RETRY_REASON", "PRIVATE_MAILBOX_VALUE"} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("observation leaked private data: %s", encoded)
		}
	}
}

func TestCredentialShapedValidIdentifiersAreRedactedFromObservation(t *testing.T) {
	for _, scenario := range []string{"sk parent", "ghp UID"} {
		t.Run(scenario, func(t *testing.T) {
			target, evidence := heldFixture()
			evidence.Replica.Status.RetryBlocks = nil
			secret := ""
			switch scenario {
			case "sk parent":
				secret = "sk-proj-0123456789abcdefghijklmnopqrst"
				target.ParentName = secret
				target.IRName = secret + "-engine"
				target.Revision = secret + "-engine-aaaaaaaa"
				evidence.Parent.Name = secret
				evidence.Replica.Name = target.IRName
				evidence.Replica.Spec.ParentRef.Name = secret
				evidence.Replica.Labels[constants.InferenceServiceLabel] = secret
				evidence.Replica.OwnerReferences[0].Name = secret
			case "ghp UID":
				secret = "ghp_0123456789abcdefghijklmnopqrst"
				target.IRUID = secret
				evidence.Replica.UID = types.UID(secret)
			}
			got := Evaluate(target, evidence)
			if !got.Matched {
				t.Fatalf("valid exact evidence should remain usable: %+v", got)
			}
			jsonBytes, err := json.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}
			yamlBytes, err := yaml.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}
			for _, rendered := range []string{string(jsonBytes), string(yamlBytes), fmt.Sprintf("%+v", got)} {
				if strings.Contains(rendered, secret) {
					t.Fatalf("credential-shaped identity leaked: %s", rendered)
				}
			}
		})
	}
}
