package worker

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"sigs.k8s.io/ome/alfred-simulator/protocol"
)

func TestSnapshotTransportIsClosedAndCancellable(t *testing.T) {
	p := testProfile(t, omeConfig)
	r := gangRequest(t, p)
	snapshot, err := protocol.Validate(r)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transport := &snapshotTransport{ctx: ctx, groups: snapshot.PodGroups}
	for _, target := range []struct{ method, url string }{{"POST", "https://production.invalid/apis/scheduling.x-k8s.io/v1alpha1/podgroups"}, {"GET", "https://production.invalid/api/v1/pods"}} {
		request, _ := http.NewRequestWithContext(ctx, target.method, target.url, nil)
		if response, err := transport.RoundTrip(request); err == nil || response != nil {
			t.Fatal("unexpected snapshot transport request accepted")
		}
	}
	request, _ := http.NewRequestWithContext(ctx, "GET", "https://snapshot.invalid/apis/scheduling.x-k8s.io/v1alpha1/podgroups?watch=true&sendInitialEvents=true", nil)
	response, err := transport.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	decoder := json.NewDecoder(response.Body)
	var event struct {
		Type   string         `json:"type"`
		Object map[string]any `json:"object"`
	}
	if err := decoder.Decode(&event); err != nil || event.Type != "ADDED" {
		t.Fatalf("initial PodGroup event missing: %+v %v", event, err)
	}
	if err := decoder.Decode(&event); err != nil || event.Type != "BOOKMARK" {
		t.Fatalf("initial bookmark missing: %+v %v", event, err)
	}
	metadata := event.Object["metadata"].(map[string]any)
	if metadata["resourceVersion"] != "1" {
		t.Fatal("inconsistent snapshot resourceVersion")
	}
	cancel()
	if data, _ := io.ReadAll(response.Body); len(data) != 0 {
		t.Fatal("cancelled stream produced additional events")
	}
}
