package integration

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	restclient "k8s.io/client-go/rest"
	"k8s.io/component-base/metrics/legacyregistry"
)

// discardedPodLogLine is the scheduler's log line for a pod its queue popped
// while an older copy of the same pod was still in flight. The newer copy is
// dropped, and once the older one is done the pod is in no queue at all. The
// scheduler exports no metric for it, so the suite counts the log line.
const discardedPodLogLine = "The same pod is tracked in multiple places in the scheduler"

// logCounter forwards scheduler log output and counts the lines that carry a
// pattern, so a test can assert on a log line it has no metric for.
type logCounter struct {
	out     io.Writer
	pattern []byte
	mu      sync.Mutex
	count   int64
}

func (c *logCounter) Write(p []byte) (int, error) {
	if n := bytes.Count(p, c.pattern); n > 0 {
		c.mu.Lock()
		c.count += int64(n)
		c.mu.Unlock()
	}
	return c.out.Write(p)
}

func (c *logCounter) total() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.count
}

// statusPatchCounter counts the pod status patches sent through a rest config
// it wrapped. Built from the wrapped config, the scheduler's own client counts.
type statusPatchCounter struct{ patches atomic.Int64 }

func (c *statusPatchCounter) wrap(cfg *restclient.Config) *restclient.Config {
	out := restclient.CopyConfig(cfg)
	out.Wrap(func(rt http.RoundTripper) http.RoundTripper {
		return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodPatch && strings.Contains(req.URL.Path, "/pods/") && strings.HasSuffix(req.URL.Path, "/status") {
				c.patches.Add(1)
			}
			return rt.RoundTrip(req)
		})
	})
	return out
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

// counterTotal sums the samples of a counter metric whose labels carry every
// pair in match, read from the registry the scheduler and the plugin share.
func counterTotal(t *testing.T, name string, match map[string]string) float64 {
	t.Helper()
	families, err := legacyregistry.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	total := 0.0
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := make(map[string]string, len(metric.GetLabel()))
			for _, pair := range metric.GetLabel() {
				labels[pair.GetName()] = pair.GetValue()
			}
			matched := true
			for key, value := range match {
				if labels[key] != value {
					matched = false
					break
				}
			}
			if matched {
				total += metric.GetCounter().GetValue()
			}
		}
	}
	return total
}

// loopSample is one reading of the work a scheduler does on the pods it holds:
// failed attempts, pods popped from the backoff queue early, gate waits, the
// plugin's wake-ups, gang unwinds, pod status patches, and discarded pods.
type loopSample struct {
	attempts, popsFromBackoff, gateWaits, permitWakes, vetoWakes, unwinds float64
	patches, discards                                                     int64
}

func sampleLoop(t *testing.T, patches *statusPatchCounter) loopSample {
	t.Helper()
	return loopSample{
		attempts:        counterTotal(t, "scheduler_schedule_attempts_total", map[string]string{"result": "unschedulable"}),
		popsFromBackoff: counterTotal(t, "scheduler_queue_incoming_pods_total", map[string]string{"event": "PopFromBackoffQ"}),
		gateWaits:       counterTotal(t, "ome_scheduler_gang_gate_total", map[string]string{"result": "wait"}),
		permitWakes:     counterTotal(t, "ome_scheduler_gang_activation_total", map[string]string{"trigger": "permit"}),
		vetoWakes:       counterTotal(t, "ome_scheduler_gang_activation_total", map[string]string{"trigger": "domain_failed"}),
		unwinds:         counterTotal(t, "ome_scheduler_gang_unwind_total", nil),
		patches:         patches.patches.Load(),
		discards:        discardedPods.total(),
	}
}

// minus returns the work done between an earlier sample and this one.
func (s loopSample) minus(earlier loopSample) loopSample {
	return loopSample{
		attempts:        s.attempts - earlier.attempts,
		popsFromBackoff: s.popsFromBackoff - earlier.popsFromBackoff,
		gateWaits:       s.gateWaits - earlier.gateWaits,
		permitWakes:     s.permitWakes - earlier.permitWakes,
		vetoWakes:       s.vetoWakes - earlier.vetoWakes,
		unwinds:         s.unwinds - earlier.unwinds,
		patches:         s.patches - earlier.patches,
		discards:        s.discards - earlier.discards,
	}
}

func (s loopSample) String() string {
	return fmt.Sprintf("attempts=%.0f popsFromBackoff=%.0f gateWaits=%.0f permitWakes=%.0f vetoWakes=%.0f unwinds=%.0f statusPatches=%d discardedPods=%d",
		s.attempts, s.popsFromBackoff, s.gateWaits, s.permitWakes, s.vetoWakes, s.unwinds, s.patches, s.discards)
}
