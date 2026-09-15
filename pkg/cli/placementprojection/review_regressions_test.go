package placementprojection

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/report"
	v "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func TestMalformedUnsupportedConditionInspectionVisiblePreservesIndependentReady(t *testing.T) {
	s := fixture(t)
	s.WorkloadClusters[0].Status.Conditions = append(s.WorkloadClusters[0].Status.Conditions, metav1.Condition{Type: "UnsupportedEvidence", Status: metav1.ConditionTrue, Reason: "Reported"})
	r, _ := ProjectExplain(s, fixtureClock)
	if r.Content.Clusters[0].ReportedReady.Source.Freshness != "Current" || r.Content.Clusters[0].ConditionPreview.State != "MalformedPayload" || len(r.Content.Issues) == 0 {
		t.Fatalf("full inspection or independent Ready lost: %+v", r.Content)
	}
}

func TestHumanUnavailableReasonsWindowsAndIssuesVisible(t *testing.T) {
	s := fixture(t)
	s.Fleet = v.PlacementAcquisition{State: "Unavailable", Reason: "Forbidden"}
	r, _ := ProjectExplain(s, fixtureClock)
	for _, table := range []report.Table{r.Content.Table(), r.Content.WideTable()} {
		var out bytes.Buffer
		table.Write(&out)
		if !strings.Contains(out.String(), "Forbidden") || !strings.Contains(out.String(), "Returned") || !strings.Contains(out.String(), "Pages") {
			t.Fatalf("fleet diagnostics missing: %s", &out)
		}
	}
	if r.Sources[1].Evidence != v.EvidenceUnavailable || r.Sources[1].UnavailableReason != v.UnavailableForbidden {
		t.Fatalf("unavailable source labelled observed: %+v", r.Sources)
	}
	s = fixture(t)
	s.TrafficMap.Spec.Entries[0].Weight = -1
	ep, _ := ProjectEndpoint(s, fixtureClock)
	for _, table := range []report.Table{ep.Content.Table(), ep.Content.WideTable()} {
		var out bytes.Buffer
		table.Write(&out)
		if !strings.Contains(out.String(), "MalformedPayload") || !strings.Contains(out.String(), "Entry inspection") {
			t.Fatalf("entry rejection invisible: %s", &out)
		}
	}
}

func TestCredentialShapedNamesRejectWithoutTaskDiskFalsePositive(t *testing.T) {
	for _, name := range []string{"sk-proj-0123456789abcdefghijklmnopqrstuvwxyz", "failure-sk-proj-0123456789abcdefghijklmnopqrstuvwxyz"} {
		s := fixture(t)
		s.WorkloadClusters[0].Name = name
		r, _ := ProjectExplain(s, fixtureClock)
		if len(r.Content.Clusters) != 0 || r.Content.Fleet.State != "Unavailable" {
			t.Fatal("credential-shaped WLC name admitted")
		}
		s = fixture(t)
		s.InferenceService.Status.Placement.Candidates[0].Autoscaling.Policies[0].Name = name
		p, _ := ProjectStatus(s, fixtureClock)
		if p.Content.Placement.Homes[0].Provenance.State != "MalformedPayload" {
			t.Fatal("credential-shaped policy name admitted")
		}
		raw, _ := json.Marshal(p)
		if strings.Contains(string(raw), name) {
			t.Fatal("credential-shaped policy name leaked")
		}
	}
	for _, name := range []string{"task-0123456789abcdefghijklmnopqrstuvwxyz", "disk-0123456789abcdefghijklmnopqrstuvwxyz"} {
		s := fixture(t)
		s.WorkloadClusters[0].Name = name
		r, _ := ProjectExplain(s, fixtureClock)
		if len(r.Content.Clusters) != 1 {
			t.Fatal("ordinary task/disk name rejected")
		}
	}
}

func TestLateNestedPolicyConflictIsInspectedBeforeOuterHomeCap(t *testing.T) {
	s := fixture(t)
	base := s.InferenceService.Status.Placement.Candidates[0]
	s.InferenceService.Status.Placement.Candidates = nil
	for i := 0; i < 70; i++ {
		h := *base.DeepCopy()
		h.Cluster = fmt.Sprintf("home-%03d", i)
		s.InferenceService.Status.Placement.Candidates = append(s.InferenceService.Status.Placement.Candidates, h)
	}
	late := &s.InferenceService.Status.Placement.Candidates[69]
	late.Autoscaling.Policies = append(late.Autoscaling.Policies, ome.CandidatePolicyDigest{Name: "demo-policy", PortableDigest: "pv1:999999999999"})
	r, _ := ProjectStatus(s, fixtureClock)
	found := false
	for _, issue := range r.Content.Issues {
		if issue.Group == "CandidateProvenance" && issue.Code == "MalformedPayload" {
			found = true
		}
	}
	if !found {
		t.Fatalf("late nested conflict discarded before inspection: homes=%+v issues=%+v", r.Content.Placement.HomePreview, r.Content.Issues)
	}
}

func TestCredentialShapedDigestUnavailableInEveryProvenanceField(t *testing.T) {
	for _, mutate := range []func(*ome.CandidatePlacement){
		func(h *ome.CandidatePlacement) { h.Autoscaling.Policies[0].PortableDigest = "AKIAIOSFODNN7EXAMPLE" },
		func(h *ome.CandidatePlacement) {
			h.Autoscaling.Components[ome.EngineComponent] = ome.CandidateComponentAutoscaling{ResolvedDigest: "eyJhbGciOiJIUzI1NiJ9.payload.signature", Ready: true}
		},
		func(h *ome.CandidatePlacement) { h.Rollout.ActiveGroups[0].PortableDigest = "BearerAbCdEf123456" },
		func(h *ome.CandidatePlacement) {
			h.Rollout.LastRun = &ome.CandidateRolloutLastRun{Outcome: ome.RolloutRunCompleted, Digest: "AKIAIOSFODNN7EXAMPLE"}
		},
	} {
		s := fixture(t)
		mutate(&s.InferenceService.Status.Placement.Candidates[0])
		r, _ := ProjectStatus(s, fixtureClock)
		if r.Content.Placement.Homes[0].Provenance.State != "MalformedPayload" {
			t.Fatal("credential-shaped non-producer digest admitted")
		}
		data, _ := json.Marshal(r)
		for _, raw := range []string{"AKIAIOSFODNN7EXAMPLE", "eyJhbGciOiJIUzI1NiJ9", "BearerAbCdEf123456"} {
			if strings.Contains(string(data), raw) {
				t.Fatalf("raw digest leaked: %s", data)
			}
		}
	}
}

func TestProducerShapedLiteralDigestsRetainReportedProvenance(t *testing.T) {
	s := fixture(t)
	h := &s.InferenceService.Status.Placement.Candidates[0]
	h.Autoscaling.Policies[0].PortableDigest = "pv1:111111111111"
	h.Autoscaling.Components[ome.EngineComponent] = ome.CandidateComponentAutoscaling{ResolvedDigest: "rv1:222222222222", Ready: true}
	h.Rollout.ActiveGroups[0].PortableDigest = "rp1:333333333333"
	h.Rollout.ActiveGroups[1].PortableDigest = "rp1:444444444444"
	h.Rollout.LastRun = &ome.CandidateRolloutLastRun{Outcome: ome.RolloutRunCompleted, Digest: "rp1:555555555555"}
	r, _ := ProjectStatus(s, fixtureClock)
	if r.Content.Placement.Homes[0].Provenance.State != "Reported" || r.Content.Placement.Homes[0].Provenance.Policies[0].PortableDigest != "pv1:111111111111" || r.Content.Placement.Homes[0].Provenance.Source != (v.PlacementEvidence{Evidence: v.EvidenceReported, Freshness: "Unverifiable", Reason: "NoObservationGeneration"}) {
		t.Fatalf("literal producer provenance rejected: %+v", r.Content.Placement.Homes[0].Provenance)
	}
}

func TestEmptyDigestsAreNotRecordedAndNestedBudgetPreservesAddresses(t *testing.T) {
	s := fixture(t)
	h := &s.InferenceService.Status.Placement.Candidates[0]
	h.Autoscaling.Policies[0].PortableDigest = ""
	h.Autoscaling.Components[ome.EngineComponent] = ome.CandidateComponentAutoscaling{}
	h.Rollout.ActiveGroups[0].PortableDigest = ""
	h.Rollout.LastRun = &ome.CandidateRolloutLastRun{Outcome: ome.RolloutRunCompleted}
	r, _ := ProjectStatus(s, fixtureClock)
	p := r.Content.Placement.Homes[0].Provenance
	if p.Policies[0].DigestState != "NotRecorded" || p.Components[0].DigestState != "NotRecorded" || p.ActiveGroups[0].DigestState != "NotRecorded" || p.LastDigestState != "NotRecorded" {
		t.Fatalf("empty digests falsely recorded: %+v", p)
	}
	h.Autoscaling.Policies = make([]ome.CandidatePolicyDigest, 65)
	r, _ = ProjectStatus(s, fixtureClock)
	if len(r.Content.Placement.Homes) != 2 || r.Content.Placement.Homes[0].Address.State != "Present" || r.Content.Placement.Homes[0].Provenance.State != "BudgetExceeded" || len(r.Content.Issues) != 1 || r.Content.Issues[0].Count != 1 {
		t.Fatalf("nested cap erased independent address/group facts: %+v", r.Content)
	}
}

func TestProvenanceInspectionNeverValidatesAnUninspectedOuterGroup(t *testing.T) {
	s := fixture(t)
	s.InferenceService.Status.Placement = nil
	r, _ := ProjectStatus(s, fixtureClock)
	if r.Content.Placement.ProvenancePreview.State != "NotRecorded" {
		t.Fatalf("absent provenance lacks a typed state: %+v", r.Content.Placement.ProvenancePreview)
	}
	for _, mutate := range []func(*ome.InferenceService){
		func(p *ome.InferenceService) { p.Status.Placement.Candidates[1].Cluster = "INVALID" },
		func(p *ome.InferenceService) {
			p.Status.Placement.Candidates[1].Cluster = p.Status.Placement.Candidates[0].Cluster
		},
		func(p *ome.InferenceService) { p.Status.Placement.Candidates = make([]ome.CandidatePlacement, 257) },
	} {
		s = fixture(t)
		mutate(s.InferenceService)
		r, _ = ProjectStatus(s, fixtureClock)
		if r.Content.Placement.ProvenancePreview.State != "Unavailable" || r.Content.Placement.ProvenancePreview.Kept != 0 || len(r.Content.Placement.Homes) != 0 {
			t.Fatalf("rejected outer group claimed inspected provenance: %+v", r.Content.Placement)
		}
	}
}

func TestAllOpenIdentityReferenceFieldsRejectCredentialShapes(t *testing.T) {
	const marker = "sk-proj-0123456789abcdefghijklmnopqrstuvwxyz"
	s := fixture(t)
	s.InferenceService.Namespace = marker
	if _, err := ProjectStatus(s, fixtureClock); err == nil {
		t.Error("credential-shaped primary namespace admitted")
	}
	for _, mutate := range []func(*ome.TrafficMapGatewayRef){
		func(r *ome.TrafficMapGatewayRef) { r.Group = marker },
		func(r *ome.TrafficMapGatewayRef) { r.Kind = marker },
		func(r *ome.TrafficMapGatewayRef) { r.Namespace = marker },
		func(r *ome.TrafficMapGatewayRef) { r.Name = marker },
	} {
		s = fixture(t)
		s.TrafficMap.Status.GatewayRef = &ome.TrafficMapGatewayRef{Group: "gateway.networking.k8s.io", Kind: "Gateway", Namespace: "cli-demo", Name: "demo-gateway"}
		mutate(s.TrafficMap.Status.GatewayRef)
		r, err := ProjectEndpoint(s, fixtureClock)
		if err != nil || r.Content.Routing.Gateway != nil || len(r.Content.Issues) != 1 || r.Content.Issues[0].Count != 1 {
			t.Errorf("unsafe reference admitted: %+v err=%v", r.Content, err)
		}
		raw, _ := json.Marshal(r)
		if strings.Contains(string(raw), marker) {
			t.Error("credential-shaped reference leaked")
		}
	}
	s = fixture(t)
	s.TrafficMap.Status.GatewayRef = &ome.TrafficMapGatewayRef{Kind: "Gateway", Name: "task-normal"}
	r, err := ProjectEndpoint(s, fixtureClock)
	if err != nil || r.Content.Routing.Gateway == nil || r.Content.Routing.Gateway.Group != "" || r.Content.Routing.Gateway.Kind != "Gateway" || r.Content.Routing.Gateway.Namespace != "cli-demo" {
		t.Fatalf("valid empty group or mixed-case kind rejected: %+v err=%v", r.Content.Routing.Gateway, err)
	}
	s.TrafficMap = nil
	r, _ = ProjectEndpoint(s, fixtureClock)
	if r.Content.ConditionPreview.State != "NotRecorded" {
		t.Fatalf("absent endpoint conditions lack typed state: %+v", r.Content.ConditionPreview)
	}
}

func TestEndpointMalformedConditionInspectionHasCountedVisibleIssue(t *testing.T) {
	s := fixture(t)
	s.TrafficMap.Status.Conditions = append(s.TrafficMap.Status.Conditions, metav1.Condition{Type: "UnsupportedEvidence", Status: metav1.ConditionTrue})
	r, err := ProjectEndpoint(s, fixtureClock)
	if err != nil || r.Content.ConditionPreview.State != "MalformedPayload" || len(r.Content.Issues) != 1 || r.Content.Issues[0] != (v.PlacementIssue{Group: "TrafficMapConditions", Code: "MalformedPayload", Count: 1}) || r.Content.Routing.Routable.Source.Freshness != "Current" || len(r.Content.Entries) != 2 {
		t.Fatalf("full inspection issue/independent facts lost: %+v err=%v", r.Content, err)
	}
	for _, table := range []report.Table{r.Content.Table(), r.Content.WideTable()} {
		var out bytes.Buffer
		table.Write(&out)
		if !strings.Contains(out.String(), "TrafficMapConditions: MalformedPayload (1)") {
			t.Fatalf("counted condition issue hidden: %s", &out)
		}
	}
}
