package v1beta1

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

const testTrafficMapPublisherObservedOptionsDigest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestTrafficMapPublisherStatusJSONRoundTrip(t *testing.T) {
	status := TrafficMapStatus{
		Published: true,
		Publisher: &TrafficMapPublisherStatus{
			PublisherName:         "example-publisher",
			ClaimedTargets:        []string{"example-system/model-service"},
			ObservedOptionsDigest: testTrafficMapPublisherObservedOptionsDigest,
		},
	}

	raw, err := json.Marshal(status)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	var roundTripped TrafficMapStatus
	if err := json.Unmarshal(raw, &roundTripped); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if !reflect.DeepEqual(roundTripped.Publisher, status.Publisher) {
		t.Fatalf("publisher status after round trip = %#v, want %#v", roundTripped.Publisher, status.Publisher)
	}
	if !roundTripped.Published {
		t.Fatal("published status after round trip = false, want true")
	}
}

func TestTrafficMapPublisherStatusDeepCopyIsIndependent(t *testing.T) {
	original := &TrafficMap{
		Status: TrafficMapStatus{
			Publisher: &TrafficMapPublisherStatus{
				PublisherName:         "example-publisher",
				ClaimedTargets:        []string{"example-system/model-service"},
				ObservedOptionsDigest: testTrafficMapPublisherObservedOptionsDigest,
			},
		},
	}

	cloned := original.DeepCopy()
	cloned.Status.Publisher.ClaimedTargets[0] = "example-system/other-service"

	if got := original.Status.Publisher.ClaimedTargets[0]; got != "example-system/model-service" {
		t.Errorf("original claimed target = %q after clone mutation", got)
	}
}

func TestTrafficMapPublisherStatusOmitsDigestBeforeFirstSuccess(t *testing.T) {
	raw, err := json.Marshal(TrafficMapPublisherStatus{
		PublisherName:  "example-publisher",
		ClaimedTargets: []string{"example-system/model-service"},
	})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if strings.Contains(string(raw), `"observedOptionsDigest"`) {
		t.Fatalf("pre-mutation publisher status unexpectedly contains observedOptionsDigest: %s", raw)
	}
}

func TestTrafficMapStatusOmitsEmptyPublisher(t *testing.T) {
	raw, err := json.Marshal(TrafficMapStatus{})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if strings.Contains(string(raw), `"publisher"`) {
		t.Fatalf("empty status unexpectedly contains publisher: %s", raw)
	}
}

func TestTrafficMapEntry_ObservedProvenanceIsOmittedWhenUnused(t *testing.T) {
	// An entry generated without the optional observed inputs must serialize
	// exactly as it did before they existed: no probe object, no capacity
	// source, no reported count. A consumer that has not opted in should not be
	// able to tell the fields were added.
	raw, err := json.Marshal(TrafficMapEntry{Cluster: "cluster-a", Weight: 70, Healthy: true})
	if err != nil {
		t.Fatalf("Marshal entry: %v", err)
	}
	if got, want := string(raw), `{"cluster":"cluster-a","weight":70,"healthy":true}`; got != want {
		t.Fatalf("entry without observed inputs:\n got %s\nwant %s", got, want)
	}
}

func TestTrafficMapCapacity_ReportedDistinguishesZeroFromAbsent(t *testing.T) {
	// Reported is a pointer because "the home reported it can serve nothing"
	// and "the home was never asked" must not collapse to the same value. The
	// first is a real signal that drives Allocated to 0; the second means the
	// control-plane plan stands. A plain int32 would render both as 0.
	zero := int32(0)
	withZero, err := json.Marshal(TrafficMapCapacity{Allocated: 7, Reported: &zero})
	if err != nil {
		t.Fatalf("Marshal reported-zero: %v", err)
	}
	if got, want := string(withZero), `{"allocated":7,"reported":0}`; got != want {
		t.Fatalf("reported zero:\n got %s\nwant %s", got, want)
	}

	absent, err := json.Marshal(TrafficMapCapacity{Allocated: 7})
	if err != nil {
		t.Fatalf("Marshal reported-absent: %v", err)
	}
	if got, want := string(absent), `{"allocated":7}`; got != want {
		t.Fatalf("reported absent:\n got %s\nwant %s", got, want)
	}
}

func TestTrafficMapCapacity_FallbackReasonRoundTrips(t *testing.T) {
	in := TrafficMapCapacity{
		Allocated:      7,
		FallbackReason: "capacity endpoint returned HTTP 503",
	}
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal capacity fallback: %v", err)
	}
	var out TrafficMapCapacity
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("Unmarshal capacity fallback: %v", err)
	}
	if out != in {
		t.Fatalf("capacity fallback round trip:\n got %+v\nwant %+v", out, in)
	}
}

func TestTrafficMapProbe_RoundTrips(t *testing.T) {
	// An inconclusive attempt does not reopen a previously gated home, so the
	// latest result and current gate must survive independently.
	in := TrafficMapProbe{
		PolicyDigest:        "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Result:              ProbeResultUnknown,
		Gated:               true,
		ConsecutiveFailures: 3,
		Message:             "HTTP 429 (not in accept or gate list)",
	}
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal probe: %v", err)
	}
	var out TrafficMapProbe
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("Unmarshal probe: %v", err)
	}
	if out != in {
		t.Fatalf("probe round trip:\n got %+v\nwant %+v", out, in)
	}
}

func TestTrafficMapEntry_DrainRefsRoundTrip(t *testing.T) {
	in := TrafficMapEntry{
		Cluster:   "cluster-a",
		Weight:    0,
		Healthy:   true,
		DrainRefs: []string{"drain-a", "maintenance-a"},
	}
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out TrafficMapEntry
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(in.DrainRefs, out.DrainRefs) {
		t.Fatalf("DrainRefs = %v, want %v", out.DrainRefs, in.DrainRefs)
	}
}
