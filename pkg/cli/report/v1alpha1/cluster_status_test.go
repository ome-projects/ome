package v1alpha1

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
)

// Caller-owned enum values are not trusted as public diagnostic text.
func TestClusterCanonicalHostileCallerAndWidth(t *testing.T) {
	value := ClusterStatusReport{Observation: "PRIVATE-ENUM\x1b[2J", UnavailableReason: "PRIVATE-ERROR", Clusters: []ClusterStatusRow{{Name: "bad\nPRIVATE-NAME", DeclaredProfile: "PRIVATE-PROFILE", SourceKind: "PRIVATE-SOURCE", SourceState: "PRIVATE-SOURCE-STATE", ProfileResolution: "PRIVATE-RESOLUTION", ReportedReady: "PRIVATE-READY", ConditionState: "PRIVATE-CONDITION", Freshness: "PRIVATE-FRESH", ConnectionState: "PRIVATE-CONNECTION", Evidence: "PRIVATE-EVIDENCE", Conditions: []ClusterCondition{{Type: "PRIVATE-TYPE", Status: "PRIVATE-STATUS", Freshness: "PRIVATE-FRESH", Reason: "PRIVATE-REASON", TransitionState: "PRIVATE-TRANSITION"}}}}}
	for _, format := range []report.Format{report.FormatTable, report.FormatJSON, report.FormatYAML} {
		var out bytes.Buffer
		if err := report.Write(&out, format, value); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(out.String(), "PRIVATE-") {
			t.Fatalf("untrusted caller fields escaped safe schema: %s", &out)
		}
	}
	var out bytes.Buffer
	if err := value.WideTable().Write(&out); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "PRIVATE-") {
		t.Fatalf("untrusted wide report: %s", &out)
	}
	long := strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 61)
	value = ClusterStatusReport{CollectedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Observation: ClusterComplete, Clusters: []ClusterStatusRow{{Name: long, DeclaredProfile: long, SourceKind: ClusterProfile, ReportedReady: ClusterReadyTrue, Freshness: StatusFreshnessCurrent, ConditionState: ClusterConditionReported}}}
	for _, table := range []report.Table{value.Table(), value.WideTable()} {
		out.Reset()
		if err := table.Write(&out); err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(out.String(), "\n") {
			if printers.CellDisplayWidth(line) > 80 {
				t.Fatalf("%d column line: %q", printers.CellDisplayWidth(line), line)
			}
		}
	}
	if !strings.Contains(out.String(), strings.Repeat("d", 61)[48:]) {
		t.Fatalf("wide details lost name suffix: %s", &out)
	}
}

func TestClusterCanonicalShuffleAndOwnership(t *testing.T) {
	stamp := time.Date(2020, 1, 2, 3, 4, 5, 0, time.FixedZone("offset", 3600))
	value := ClusterStatusReport{CollectedAt: stamp, Observation: ClusterComplete, Clusters: []ClusterStatusRow{{Name: "z", SourceKind: ClusterProfile, Conditions: []ClusterCondition{{Type: "Ready", Status: ClusterReadyFalse, TransitionTime: &stamp}, {Type: "Ready", Status: ClusterReadyTrue, ObservedGeneration: 1, TransitionTime: &stamp}}}, {Name: "a", Generation: 2}, {Name: "a", Generation: 1}}}
	before, _ := json.Marshal(value)
	want, _ := json.Marshal(value.Canonical())
	for seed := int64(0); seed < 32; seed++ {
		copy := value.Canonical()
		rng := rand.New(rand.NewSource(seed))
		rng.Shuffle(len(copy.Clusters), func(i, j int) { copy.Clusters[i], copy.Clusters[j] = copy.Clusters[j], copy.Clusters[i] })
		for i := range copy.Clusters {
			rng.Shuffle(len(copy.Clusters[i].Conditions), func(a, b int) {
				copy.Clusters[i].Conditions[a], copy.Clusters[i].Conditions[b] = copy.Clusters[i].Conditions[b], copy.Clusters[i].Conditions[a]
			})
		}
		got, _ := json.Marshal(copy.Canonical())
		if !bytes.Equal(got, want) {
			t.Fatalf("shuffle %d changed canonical wire", seed)
		}
	}
	copy := value.Canonical()
	*copy.Clusters[2].Conditions[0].TransitionTime = time.Date(2010, 1, 1, 0, 0, 0, 0, time.UTC)
	copy.Clusters[2].Conditions[0].Status = ClusterReadyUnknown
	after, _ := json.Marshal(value)
	if !bytes.Equal(before, after) {
		t.Fatal("canonical mutated source")
	}
	if !reflect.DeepEqual(value.Canonical(), value.Canonical().Canonical()) {
		t.Fatal("canonical is not idempotent")
	}
}

func TestClusterCanonicalTotalOrderEvenWithInvalidTime(t *testing.T) {
	invalid := time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
	value := ClusterStatusReport{Clusters: []ClusterStatusRow{{Name: "same", Conditions: []ClusterCondition{{Type: "Ready", Status: ClusterReadyTrue, TransitionTime: &invalid}, {Type: "Ready", Status: ClusterReadyFalse, TransitionTime: &invalid}}}, {Name: "same", Generation: 1}}}
	got := value.Canonical()
	if got.Clusters[0].Generation != 0 || got.Clusters[0].Conditions[0].Status != ClusterReadyFalse {
		t.Fatalf("serialization errors destroyed total order: %+v", got.Clusters)
	}
}

type clusterFailWriter struct{ short bool }

func (w clusterFailWriter) Write(p []byte) (int, error) {
	if w.short {
		return 0, nil
	}
	return 0, errors.New("writer failed")
}

func TestClusterWritersAndSerializationErrors(t *testing.T) {
	value := ClusterStatusReport{Observation: ClusterEmpty}
	for _, format := range []report.Format{report.FormatTable, report.FormatJSON, report.FormatYAML} {
		if err := report.Write(clusterFailWriter{}, format, value); err == nil {
			t.Fatal("writer error discarded")
		}
		if err := report.Write(clusterFailWriter{short: true}, format, value); !errors.Is(err, io.ErrShortWrite) {
			t.Fatalf("short writer: %v", err)
		}
	}
	if err := value.WideTable().Write(clusterFailWriter{}); err == nil {
		t.Fatal("wide writer error discarded")
	}
	value.CollectedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, format := range []report.Format{report.FormatJSON, report.FormatYAML} {
		if err := report.Write(io.Discard, format, value); err == nil {
			t.Fatal("invalid timestamp serialized")
		}
	}
}

// A healthy retained prefix must never hide a bounded/failed observation.
// Context rows use a non-DNS prefix, so they cannot impersonate a cluster.
func TestClusterCompactObservationAndTruncationEvidence(t *testing.T) {
	for _, reason := range []UnavailableReason{"", UnavailableForbidden} {
		value := ClusterStatusReport{Observation: ClusterPartial, UnavailableReason: reason, ObservedPages: 2, ReturnedSources: 1, AdmittedSources: 1, SourceLimit: 64, PageLimit: 2, PageSize: 32, SourcesTruncated: true, Clusters: []ClusterStatusRow{{Name: "healthy-prefix", SourceKind: ClusterKubeConfigSecret, SourceState: ClusterConditionReported, ReportedReady: ClusterReadyTrue, Freshness: StatusFreshnessCurrent, ConditionState: ClusterConditionReported}}}
		before, _ := json.Marshal(value)
		var out bytes.Buffer
		if err := value.Table().Write(&out); err != nil {
			t.Fatal(err)
		}
		for _, want := range []string{"@ observation", "Partial", "@ sources", "returned=1", "kept=1", "max=64", "cut=true", "@ pages", "read=2", "max=2", "size=32", "healthy-prefix", "True", "Current", "Reported"} {
			if !strings.Contains(out.String(), want) {
				t.Fatalf("missing literal %q from bounded compact report:\n%s", want, &out)
			}
		}
		if reason != "" && !strings.Contains(out.String(), "Forbidden") {
			t.Fatalf("later-page failure hidden: %s", &out)
		}
		if strings.Index(out.String(), "@ observation") > strings.Index(out.String(), "healthy-prefix") {
			t.Fatal("context must precede object rows")
		}
		after, _ := json.Marshal(value)
		if !bytes.Equal(before, after) {
			t.Fatal("table changed machine evidence")
		}
		for _, line := range strings.Split(out.String(), "\n") {
			if printers.CellDisplayWidth(line) > 80 {
				t.Fatalf("compact width %d: %q", printers.CellDisplayWidth(line), line)
			}
		}
	}
	for _, reason := range []UnavailableReason{UnavailableForbidden, UnavailableNotFound, UnavailableMalformedPayload} {
		value := ClusterStatusReport{Observation: ClusterUnavailable, UnavailableReason: reason}
		var out bytes.Buffer
		if err := value.Table().Write(&out); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(out.String(), "@ availability") || !strings.Contains(out.String(), string(reason)) {
			t.Fatalf("empty availability cause hidden: %s", &out)
		}
	}
	value := ClusterStatusReport{Observation: ClusterComplete, Clusters: []ClusterStatusRow{{Name: "gpu", SourceKind: ClusterInvalidSource, SourceState: ClusterConditionTruncated, ConditionState: ClusterConditionReported, ConditionsTruncated: true}}}
	var out bytes.Buffer
	if err := value.Table().Write(&out); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Invalid*", "Reported*", "@ * = truncated"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("source/condition detail cap hidden: %s", &out)
		}
	}
}
