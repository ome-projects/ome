package scheduling

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
)

func TestSimulatorV1WorkerFixtureIsRootCompatible(t *testing.T) {
	payload, err := os.ReadFile("testdata/simulator-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Request Request `json:"request"`
		Result  Result  `json:"result"`
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.Request.SchemaVersion != SimulationSchemaV1 {
		t.Fatalf("fixture schema = %q, want %q", fixture.Request.SchemaVersion, SimulationSchemaV1)
	}
	if len(fixture.Request.ReplacementPods) != 2 || fixture.Request.ReplacementPods[0].UID == "" || fixture.Request.ReplacementPods[1].UID != "" {
		t.Fatalf("fixture does not cover explicit and empty replacement UIDs: %+v", fixture.Request.ReplacementPods)
	}
	if err := ValidateResult(fixture.Request, fixture.Result); err != nil {
		t.Fatalf("root rejected worker fixture: %v", err)
	}
}
