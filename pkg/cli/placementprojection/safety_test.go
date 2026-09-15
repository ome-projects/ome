package placementprojection

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	knapis "knative.dev/pkg/apis"
	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	c "sigs.k8s.io/ome/pkg/cli/placementcollection"
	"sigs.k8s.io/ome/pkg/cli/report"
	v "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func TestURLCompleteLocalValidationAndOriginOnly(t *testing.T) {
	for _, tc := range []struct {
		raw    knapis.URL
		origin string
	}{
		{knapis.URL{Scheme: "https", Host: "[2001:db8::1]:8443", Path: "/private-path", RawQuery: "private-token=x", Fragment: "private-fragment"}, "https://[2001:db8::1]:8443"},
		{knapis.URL{Scheme: "http", Host: "127.0.0.1:8080", Path: "/"}, "http://127.0.0.1:8080"},
		{knapis.URL{Scheme: "https", Host: "DEMO.invalid", Path: "/space here", RawPath: "/space%20here"}, "https://demo.invalid"},
		{knapis.URL{Scheme: "ftp", Host: "demo.invalid"}, ""},
		{knapis.URL{Scheme: "https", Opaque: "demo.invalid"}, ""},
		{knapis.URL{Host: "demo.invalid"}, ""},
		{knapis.URL{Scheme: "https", Host: ""}, ""},
		{knapis.URL{Scheme: "https", Host: "bad host.invalid"}, ""},
		{knapis.URL{Scheme: "https", Host: "bad_host.invalid"}, ""},
		{knapis.URL{Scheme: "https", Host: "demo.invalid:"}, ""},
		{knapis.URL{Scheme: "https", Host: "demo.invalid:70000"}, ""},
		{knapis.URL{Scheme: "https", Host: "demo.invalid:abc"}, ""},
		{knapis.URL{Scheme: "https", Host: "demo.invalid", Path: "/bad\n"}, ""},
		{knapis.URL{Scheme: "https", Host: "demo.invalid", Path: "/bad\\path"}, ""},
		{knapis.URL{Scheme: "https", Host: "demo.invalid", Path: strings.Repeat("a", 4097)}, ""},
		{knapis.URL{Scheme: "https", Host: "demo.invalid", Path: "/a", RawPath: "/b"}, ""},
		{knapis.URL{Scheme: "https", Host: "demo.invalid", Fragment: "a", RawFragment: "b"}, ""},
		{knapis.URL{Scheme: "https", Host: "demo.invalid", RawQuery: "private-token=%zz"}, ""},
		{knapis.URL{Scheme: "https", Host: "sk-proj-0123456789abcdefghijklmnopqrstuvwxyz.invalid"}, ""},
	} {
		s := fixture(t)
		s.InferenceService.Status.Placement.Endpoint = &tc.raw
		r, err := ProjectEndpoint(s, fixtureClock)
		if err != nil {
			t.Fatal(err)
		}
		if r.Content.Status.Placement.Address.EndpointOrigin != tc.origin {
			t.Fatalf("address=%+v want%q", r.Content.Status.Placement.Address, tc.origin)
		}
		for _, format := range []report.Format{report.FormatTable, report.FormatJSON, report.FormatYAML} {
			var out bytes.Buffer
			if err := report.Write(&out, format, r); err != nil {
				t.Fatal(err)
			}
			for _, secret := range []string{"private-", "fixture-isvc-uid", "fixture-map-uid", "private.invalid"} {
				if strings.Contains(out.String(), secret) {
					t.Fatalf("privacy %q: %s", secret, &out)
				}
			}
		}
	}
}

func TestReportedMembershipUsesCompleteHomesNotDisplayPrefix(t *testing.T) {
	s := fixture(t)
	s.InferenceService.Status.Placement.Candidates = nil
	for i := 0; i < 70; i++ {
		s.InferenceService.Status.Placement.Candidates = append(s.InferenceService.Status.Placement.Candidates, ome.CandidatePlacement{Cluster: fmt.Sprintf("home-%03d", i), Phase: ome.CandidatePhaseAdmitted})
	}
	s.WorkloadClusters[0].Name = "home-069"
	r, _ := ProjectExplain(s, fixtureClock)
	if !r.Content.Status.Placement.HomePreview.Truncated || r.Content.Clusters[0].ReportedHome != "True" {
		t.Fatalf("lost hidden reported home=%+v", r.Content)
	}
}

func TestBoundedFullConditionShapeValidation(t *testing.T) {
	for _, mutate := range []func(*metav1.Condition){
		func(x *metav1.Condition) { x.Reason = "" }, func(x *metav1.Condition) { x.Reason = "bad reason" }, func(x *metav1.Condition) { x.Reason = "bad-reason" }, func(x *metav1.Condition) { x.LastTransitionTime = metav1.Time{} }, func(x *metav1.Condition) { x.Status = "Maybe" }, func(x *metav1.Condition) { x.LastTransitionTime = metav1.NewTime(fixtureClock.Now().Add(time.Hour)) }, func(x *metav1.Condition) { x.Message = strings.Repeat("x", 4097) },
	} {
		s := fixture(t)
		mutate(&s.TrafficMap.Status.Conditions[0])
		r, _ := ProjectEndpoint(s, fixtureClock)
		if r.Content.Routing.Routable.Source.Freshness != "Invalid" || len(r.Content.Entries) != 2 {
			t.Fatalf("malformed condition=%+v", r.Content)
		}
	}
}

func TestFleetMalformedLabelsDuplicatesAndPartialUncertainty(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		mutate                    func(*c.Result)
		wantState, wantCompatible string
	}{
		{"mismatch", func(s *c.Result) { s.WorkloadClusters[0].Labels["provider"] = "other" }, "Observed", "False"},
		{"bad-label-value", func(s *c.Result) { s.WorkloadClusters[0].Labels["provider"] = "bad value" }, "Observed", "Unknown"},
		{"too-many-labels", func(s *c.Result) {
			for i := 0; i < 260; i++ {
				s.WorkloadClusters[0].Labels[fmt.Sprintf("key%d", i)] = "x"
			}
		}, "Observed", "Unknown"},
		{"bad-identity", func(s *c.Result) {
			s.WorkloadClusters[0].Name = "private-sk-proj-0123456789abcdefghijklmnopqrstuvwxyz"
		}, "Unavailable", ""},
		{"duplicate-conflict", func(s *c.Result) {
			copyValue := *s.WorkloadClusters[0].DeepCopy()
			copyValue.Generation++
			s.WorkloadClusters = append(s.WorkloadClusters, copyValue)
		}, "Unavailable", ""},
		{"over-budget", func(s *c.Result) { s.WorkloadClusters = make([]ome.WorkloadCluster, 65) }, "Unavailable", ""},
		{"partial", func(s *c.Result) { s.Fleet.State = "Partial"; s.Fleet.Complete = false }, "Partial", "True"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := fixture(t)
			tc.mutate(&s)
			r, _ := ProjectExplain(s, fixtureClock)
			if string(r.Content.Fleet.State) != tc.wantState {
				t.Fatalf("fleet=%+v", r.Content.Fleet)
			}
			if tc.wantCompatible != "" && (len(r.Content.Clusters) != 1 || string(r.Content.Clusters[0].ComputedSelectorCompatible) != tc.wantCompatible) {
				t.Fatalf("clusters=%+v", r.Content.Clusters)
			}
		})
	}
	s := fixture(t)
	s.WorkloadClusters = append(s.WorkloadClusters, *s.WorkloadClusters[0].DeepCopy())
	r, _ := ProjectExplain(s, fixtureClock)
	if len(r.Content.Clusters) != 1 {
		t.Fatal("exact duplicate not deduplicated")
	}
}

func TestLateRouteHiddenConflictAndValidDisplayCaps(t *testing.T) {
	s := fixture(t)
	for i := 0; i < 70; i++ {
		e := s.TrafficMap.Spec.Entries[0]
		e.Cluster = fmt.Sprintf("route-%03d", i)
		s.TrafficMap.Spec.Entries = append(s.TrafficMap.Spec.Entries, e)
	}
	r, _ := ProjectEndpoint(s, fixtureClock)
	if len(r.Content.Entries) != 64 || !r.Content.Routing.EntryPreview.Truncated {
		t.Fatalf("route preview=%+v", r.Content.Routing.EntryPreview)
	}
	conflict := s.TrafficMap.Spec.Entries[0]
	conflict.Endpoint = &knapis.URL{Scheme: "https", Host: "demo-a.invalid", RawQuery: "private-token=x"}
	s.TrafficMap.Spec.Entries = append(s.TrafficMap.Spec.Entries, conflict)
	r, _ = ProjectEndpoint(s, fixtureClock)
	if len(r.Content.Entries) != 0 || r.Content.Routing.EntryPreview.State != "ConflictingDuplicates" {
		t.Fatalf("hidden late conflict=%+v", r.Content.Routing)
	}
	s = fixture(t)
	s.TrafficMap.Spec.Entries = make([]ome.TrafficMapEntry, 257)
	r, _ = ProjectEndpoint(s, fixtureClock)
	if r.Content.Routing.EntryPreview.State != "BudgetExceeded" {
		t.Fatal("overbudget validated prefix")
	}
	s = fixture(t)
	s.TrafficMap.Spec.Entries = append(s.TrafficMap.Spec.Entries, s.TrafficMap.Spec.Entries[0])
	r, _ = ProjectEndpoint(s, fixtureClock)
	if len(r.Content.Entries) != 2 {
		t.Fatal("exact duplicate not deduplicated")
	}
}

func TestIndependentMalformedProbeAndProvenanceGroups(t *testing.T) {
	s := fixture(t)
	s.TrafficMap.Spec.Entries[0].Probe.LastProbeTime = &metav1.Time{Time: fixtureClock.Now().Add(time.Hour)}
	r, _ := ProjectEndpoint(s, fixtureClock)
	if len(r.Content.Entries) != 2 || r.Content.Entries[0].Probe.Result != "Invalid" {
		t.Fatalf("malformed probe erased independent route=%+v", r.Content)
	}
	s = fixture(t)
	s.InferenceService.Status.Placement.Candidates[0].Autoscaling.Policies = append(s.InferenceService.Status.Placement.Candidates[0].Autoscaling.Policies, ome.CandidatePolicyDigest{Name: "demo-policy", PortableDigest: "different"})
	status, _ := ProjectStatus(s, fixtureClock)
	if len(status.Content.Placement.Homes) != 2 || status.Content.Placement.Homes[0].Provenance.State != "MalformedPayload" {
		t.Fatalf("bad provenance=%+v", status.Content.Placement)
	}
}

func TestAbsentAndUnknownEvidenceNeverSuccessfulDefaults(t *testing.T) {
	for _, project := range []func(c.Result, v.Clock) error{
		func(s c.Result, clock v.Clock) error { _, err := ProjectStatus(s, clock); return err }, func(s c.Result, clock v.Clock) error { _, err := ProjectExplain(s, clock); return err }, func(s c.Result, clock v.Clock) error { _, err := ProjectEndpoint(s, clock); return err },
	} {
		if err := project(c.Result{}, nil); err == nil {
			t.Fatal("accepted unbound primary")
		}
	}
	s := fixture(t)
	s.InferenceService.Status.Placement.Phase = "Future"
	s.InferenceService.Status.Placement.Cluster = "private-sk-proj-0123456789abcdefghijklmnopqrstuvwxyz"
	s.InferenceService.Status.Placement.Candidates[0].Phase = "Future"
	s.InferenceService.Spec.Placement.Split.Replicas = nil
	s.InferenceService.Spec.Placement.Split.Spread = false
	r, _ := ProjectStatus(s, nil)
	if r.Content.Placement.Phase != "Unknown" || r.Content.Placement.Homes[0].Phase != "Unknown" || r.Content.Placement.ReportedCluster != "" {
		t.Fatal("unknown success or name leak")
	}
	s.TrafficMap = nil
	s.TrafficMapAcquisition = v.PlacementAcquisition{}
	ep, _ := ProjectEndpoint(s, nil)
	if ep.Content.Routing.Acquisition.State != "Unavailable" || ep.Content.Routing.Acknowledgement != "NoAcknowledgement" {
		t.Fatal("absent source invented")
	}
	s.WorkloadClusters[0].Spec.ClusterSource = ome.ClusterConnectionSource{KubeConfig: &ome.KubeConfigSource{}}
	ex, _ := ProjectExplain(s, nil)
	if ex.Content.Clusters[0].ConnectionSource != "KubeConfigReferenceNotResolved" {
		t.Fatal("source resolved")
	}
	s.WorkloadClusters[0].Spec.ClusterSource = ome.ClusterConnectionSource{}
	ex, _ = ProjectExplain(s, nil)
	if ex.Content.Clusters[0].ConnectionSource != "Unknown" {
		t.Fatal("unknown source defaulted")
	}
}

func TestRoutingCapacityGatewayAndAckCompatibilityOnly(t *testing.T) {
	s := fixture(t)
	value := int32(4)
	factor := resource.MustParse("2")
	s.TrafficMap.Spec.Entries[0].Capacity = &ome.TrafficMapCapacity{Allocated: 4, Ready: 2, Source: ome.CapacitySourceEndpoint, Factor: &factor, Reported: &value}
	s.TrafficMap.Status.GatewayRef = &ome.TrafficMapGatewayRef{Group: "gateway.networking.k8s.io", Kind: "HTTPRoute", Name: "demo-route"}
	s.TrafficMap.Status.Programmed = true
	s.TrafficMap.Status.ObservedTrafficMapGeneration = 4
	r, _ := ProjectEndpoint(s, fixtureClock)
	if r.Content.Routing.Gateway == nil || r.Content.Routing.Gateway.State != "ReportedReferenceNotResolved" || r.Content.Routing.Acknowledgement != "ReportedTrue" || r.Content.Entries[0].Capacity.Source != "Endpoint" || *r.Content.Entries[0].Capacity.Reported.Value != 4 {
		t.Fatalf("compatibility=%+v", r.Content)
	}
	s.TrafficMap.Status.GatewayRef.Name = "private-sk-proj-0123456789abcdefghijklmnopqrstuvwxyz"
	s.TrafficMap.Status.Programmed = false
	s.TrafficMap.Status.ObservedTrafficMapGeneration = 4
	r, _ = ProjectEndpoint(s, fixtureClock)
	if r.Content.Routing.Gateway != nil || r.Content.Routing.Acknowledgement != "Unknown" {
		t.Fatal("invalid ref or absent bool presence inferred")
	}
	s.TrafficMap.Spec.Entries[0].Capacity.Source = "Future"
	r, _ = ProjectEndpoint(s, fixtureClock)
	if len(r.Content.Entries) != 0 {
		t.Fatal("unknown capacity source treated as known")
	}
	data, _ := json.Marshal(r)
	if strings.Contains(string(data), "private-") {
		t.Fatal("private reference leaked")
	}
}
