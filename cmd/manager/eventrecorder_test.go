package main

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
)

// deliveredEvents stands in for the API server: it counts, per involved
// object, every event the broadcaster lets through its spam filter.
type deliveredEvents struct {
	mu     sync.Mutex
	counts map[string]int
}

func (d *deliveredEvents) accept(event *corev1.Event) (*corev1.Event, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.counts[event.InvolvedObject.Name]++
	return event, nil
}

func (d *deliveredEvents) Create(event *corev1.Event) (*corev1.Event, error) { return d.accept(event) }
func (d *deliveredEvents) Update(event *corev1.Event) (*corev1.Event, error) { return d.accept(event) }
func (d *deliveredEvents) Patch(event *corev1.Event, _ []byte) (*corev1.Event, error) {
	return d.accept(event)
}

func (d *deliveredEvents) count(name string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.counts[name]
}

// eventHarness drives a broadcaster built the way the manager builds its
// own, with a counting sink in place of the API server.
type eventHarness struct {
	recorder record.EventRecorder
	sink     *deliveredEvents
	sequence int
}

func startEventHarness(t *testing.T, settings controllerconfig.EventRecorderSettings) *eventHarness {
	t.Helper()
	broadcaster := newEventBroadcaster(settings)
	t.Cleanup(broadcaster.Shutdown)
	sink := &deliveredEvents{counts: map[string]int{}}
	broadcaster.StartRecordingToSink(sink)
	return &eventHarness{
		recorder: broadcaster.NewRecorder(runtime.NewScheme(), corev1.EventSource{Component: "v1beta1Controllers"}),
		sink:     sink,
	}
}

// emitStateChanges records count distinct Normal events about one object,
// the shape of the per-Instance events a rollout stamps on its component.
func (h *eventHarness) emitStateChanges(object string, count int) {
	subject := &corev1.ObjectReference{Kind: "InferenceReplica", Namespace: "team-a", Name: object, UID: types.UID("uid-" + object)}
	for i := 0; i < count; i++ {
		h.sequence++
		h.recorder.Eventf(subject, corev1.EventTypeNormal, fmt.Sprintf("StateChange%d", h.sequence), "state change %d", h.sequence)
	}
}

// settle emits a marker about a fresh object and waits for it. The
// broadcaster delivers in emission order, so once the marker arrives every
// earlier event has been delivered or dropped.
func (h *eventHarness) settle(t *testing.T) {
	t.Helper()
	h.sequence++
	marker := fmt.Sprintf("marker-%d", h.sequence)
	h.emitStateChanges(marker, 1)
	require.Eventually(t, func() bool { return h.sink.count(marker) == 1 }, 10*time.Second, 5*time.Millisecond)
}

// TestNewEventBroadcasterUnconfiguredKeepsClientGoBudget is the loss the
// configuration exists to fix: with nothing configured, one object receives
// client-go's default of 25 events and every later one is dropped silently.
func TestNewEventBroadcasterUnconfiguredKeepsClientGoBudget(t *testing.T) {
	harness := startEventHarness(t, controllerconfig.EventRecorderSettings{})
	harness.emitStateChanges("fleet-engine", 40)
	harness.settle(t)
	assert.Equal(t, 25, harness.sink.count("fleet-engine"))
}

// TestNewEventBroadcasterConfiguredBurstIsTheBound: a configured burst of 64
// lets one object emit 64 events back to back without a drop, and the 65th
// is the first one dropped — the bound is the configured one.
func TestNewEventBroadcasterConfiguredBurstIsTheBound(t *testing.T) {
	settings := controllerconfig.EventRecorderSettings{BurstSize: 64}

	withinBurst := startEventHarness(t, settings)
	withinBurst.emitStateChanges("fleet-engine", 64)
	withinBurst.settle(t)
	assert.Equal(t, 64, withinBurst.sink.count("fleet-engine"))

	pastBurst := startEventHarness(t, settings)
	pastBurst.emitStateChanges("fleet-engine", 65)
	pastBurst.settle(t)
	assert.Equal(t, 64, pastBurst.sink.count("fleet-engine"))
}

// TestNewEventBroadcasterRefillAdmitsEventsPastTheBurst: with a configured
// refill, an object that spent its burst keeps emitting without drops as
// long as it stays under the refill rate — here burst plus a few more inside
// one second. The gaps are several refill intervals wide so scheduling
// delay cannot starve the bucket.
func TestNewEventBroadcasterRefillAdmitsEventsPastTheBurst(t *testing.T) {
	harness := startEventHarness(t, controllerconfig.EventRecorderSettings{
		BurstSize:      8,
		RefillInterval: 20 * time.Millisecond,
	})
	harness.emitStateChanges("fleet-engine", 8)
	for i := 0; i < 3; i++ {
		time.Sleep(100 * time.Millisecond)
		harness.emitStateChanges("fleet-engine", 1)
	}
	harness.settle(t)
	assert.Equal(t, 11, harness.sink.count("fleet-engine"))
}
