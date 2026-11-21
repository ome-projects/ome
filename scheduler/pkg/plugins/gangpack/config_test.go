package gangpack

import (
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
)

func TestDecodeArgs_EveryFieldSet_ReadsEachAndRequiresGCInterval(t *testing.T) {
	configured, err := decodeArgs(&runtime.Unknown{Raw: []byte(`{
		"topologyKey":"topology.ome.io/domain",
		"podGroupTopologyKeyAnnotation":"workloads.example/topology-key",
		"unsupportedPlacementGroupLabel":"workloads.example/placement-group",
		"defaultPermitTimeoutSeconds":90,
		"podGroupSyncTimeoutSeconds":15,
		"gcIntervalSeconds":45
	}`)})
	if err != nil {
		t.Fatalf("decodeArgs: %v", err)
	}
	if configured.TopologyKey != "topology.ome.io/domain" {
		t.Fatalf("TopologyKey = %q", configured.TopologyKey)
	}
	if configured.PodGroupTopologyKeyAnnotation != "workloads.example/topology-key" ||
		configured.UnsupportedPlacementGroupLabel != "workloads.example/placement-group" {
		t.Fatalf("metadata keys = %q/%q", configured.PodGroupTopologyKeyAnnotation, configured.UnsupportedPlacementGroupLabel)
	}
	if got := configured.defaultPermitTimeout(); got != 90*time.Second {
		t.Fatalf("defaultPermitTimeout = %v, want 90s", got)
	}
	if got := configured.podGroupSyncTimeout(); got != 15*time.Second {
		t.Fatalf("podGroupSyncTimeout = %v, want 15s", got)
	}
	if got := configured.gcInterval(); got != 45*time.Second {
		t.Fatalf("gcInterval = %v, want 45s", got)
	}

	if _, err := decodeArgs(nil); err == nil {
		t.Fatal("decodeArgs(nil) succeeded, want required gcIntervalSeconds error")
	}
}

func TestDecodeArgs_InvalidOrMissingValues_ReturnsError(t *testing.T) {
	for _, raw := range []string{
		`{"topologyKey":"not a label key/"}`,
		`{"podGroupTopologyKeyAnnotation":"not a key/"}`,
		`{"unsupportedPlacementGroupLabel":"not a key/"}`,
		`{"defaultPermitTimeoutSeconds":0}`,
		`{"defaultPermitTimeoutSeconds":-1}`,
		`{"podGroupSyncTimeoutSeconds":0}`,
		`{"gcIntervalSeconds":0}`,
		`{"gcIntervlSeconds":45}`,
		`{}`,
	} {
		if _, err := decodeArgs(&runtime.Unknown{Raw: []byte(raw)}); err == nil {
			t.Errorf("decodeArgs(%s) succeeded, want error", raw)
		}
	}
}

// TestDecodeArgs_ASecondsValueThatCannotBeADurationIsRefused: 9,223,372,037
// seconds is one more than a time.Duration can hold; the largest value that
// fits, 9,223,372,036, is accepted.
func TestDecodeArgs_ASecondsValueThatCannotBeADurationIsRefused(t *testing.T) {
	if _, err := decodeArgs(&runtime.Unknown{Raw: []byte(`{"gcIntervalSeconds":9223372037}`)}); err == nil || !strings.Contains(err.Error(), "gcIntervalSeconds is too large") {
		t.Fatalf("one second past the largest duration: err = %v, want \"gcIntervalSeconds is too large\"", err)
	}
	if _, err := decodeArgs(&runtime.Unknown{Raw: []byte(`{"gcIntervalSeconds":9223372036}`)}); err != nil {
		t.Fatalf("the largest duration: err = %v, want accepted", err)
	}
}

// TestDecodeArgs_ARefusalNamesTheArgumentItRefuses: each of the three duration
// settings is refused under its own name.
func TestDecodeArgs_ARefusalNamesTheArgumentItRefuses(t *testing.T) {
	for raw, want := range map[string]string{
		`{"defaultPermitTimeoutSeconds":0,"gcIntervalSeconds":5}`: "defaultPermitTimeoutSeconds must be positive",
		`{"podGroupSyncTimeoutSeconds":-1,"gcIntervalSeconds":5}`: "podGroupSyncTimeoutSeconds must be positive",
		`{"defaultPermitTimeoutSeconds":5}`:                       "gcIntervalSeconds must be configured",
	} {
		_, err := decodeArgs(&runtime.Unknown{Raw: []byte(raw)})
		if err == nil || err.Error() != want {
			t.Errorf("decodeArgs(%s) err = %v, want %q", raw, err, want)
		}
	}
}

// TestDecodeArgs_PreemptionIsOffUnlessSetAndNeedsACap: without a preemption
// block the plugin never preempts; a block without a positive cap is refused.
func TestDecodeArgs_PreemptionIsOffUnlessSetAndNeedsACap(t *testing.T) {
	off, err := decodeArgs(&runtime.Unknown{Raw: []byte(`{"gcIntervalSeconds":5}`)})
	if err != nil || off.maxPreemptionsInFlight() != 0 {
		t.Fatalf("no preemption block: cap = %d, err = %v, want 0 and nil", off.maxPreemptionsInFlight(), err)
	}
	on, err := decodeArgs(&runtime.Unknown{Raw: []byte(`{"gcIntervalSeconds":5,"preemption":{"maxInFlight":2}}`)})
	if err != nil || on.maxPreemptionsInFlight() != 2 {
		t.Fatalf("maxInFlight 2: cap = %d, err = %v, want 2 and nil", on.maxPreemptionsInFlight(), err)
	}
	for raw, want := range map[string]string{
		`{"gcIntervalSeconds":5,"preemption":{}}`:                "preemption.maxInFlight must be configured when preemption is set",
		`{"gcIntervalSeconds":5,"preemption":{"maxInFlight":0}}`: "preemption.maxInFlight must be at least 1",
	} {
		if _, err := decodeArgs(&runtime.Unknown{Raw: []byte(raw)}); err == nil || err.Error() != want {
			t.Errorf("decodeArgs(%s) err = %v, want %q", raw, err, want)
		}
	}
}
