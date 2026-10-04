package canary

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// sourceFront is a TCP relay standing in for a Service without a selector:
// the gate is pinned to the front's address and the test re-aims what sits
// behind it. As through a cluster's service proxy, a connection already
// established keeps flowing to wherever it was opened; only a new connection
// sees the current target, and with nothing behind the front a new
// connection is refused.
type sourceFront struct {
	t    *testing.T
	addr string

	mu     sync.Mutex
	ln     net.Listener
	target string
}

func newSourceFront(t *testing.T, target string) *sourceFront {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &sourceFront{t: t, addr: ln.Addr().String(), ln: ln, target: target}
	go f.serve(ln)
	t.Cleanup(func() { f.point("") })
	return f
}

// url is the address the gate is pinned to; it never changes.
func (f *sourceFront) url() string { return "http://" + f.addr }

// point aims the front at target (host:port). An empty target takes the
// listener down, so a new connection is refused. Established relays are
// left alone either way.
func (f *sourceFront) point(target string) {
	f.t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	f.target = target
	if target == "" {
		if f.ln != nil {
			_ = f.ln.Close()
			f.ln = nil
		}
		return
	}
	if f.ln == nil {
		ln, err := net.Listen("tcp", f.addr)
		if err != nil {
			f.t.Fatalf("listen again on %s: %v", f.addr, err)
		}
		f.ln = ln
		go f.serve(ln)
	}
}

func (f *sourceFront) serve(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		f.mu.Lock()
		target := f.target
		f.mu.Unlock()
		upstream, err := net.Dial("tcp", target)
		if err != nil {
			_ = conn.Close()
			continue
		}
		relay := func(dst, src net.Conn) {
			_, _ = io.Copy(dst, src)
			_ = dst.Close()
			_ = src.Close()
		}
		go relay(upstream, conn)
		go relay(conn, upstream)
	}
}

func hostPort(serverURL string) string { return strings.TrimPrefix(serverURL, "http://") }

// TestAnalysisLifecycle_SourceUnusableMidStep drives the gate through the
// production sampling stack against a source pinned behind a front, and
// breaks what sits behind the front while the step samples: the source
// answers nothing, or answers something that is not a metric. Every sample
// reaches the source as it is at that moment. The gate records an
// unevaluated sample naming the reason and holds the step; it counts no
// breach however many such samples it reads, with a failure limit of one;
// and once the source answers again it reads health and the step proceeds.
func TestAnalysisLifecycle_SourceUnusableMidStep(t *testing.T) {
	garbage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// A serving pod answering on the pinned port: a 200 that is not a
		// metrics API response.
		w.Header().Set("Content-Type", "text/html")
		_, _ = io.WriteString(w, "<html><body>model server</body></html>")
	}))
	t.Cleanup(garbage.Close)

	tests := []struct {
		name string
		// breakSource re-aims the front once the step has read health.
		breakSource func(front *sourceFront)
		// wantReason is carried by the unevaluated sample's message.
		wantReason string
	}{
		{
			name:        "an unreachable source records an unevaluated sample with its reason and holds the step",
			breakSource: func(front *sourceFront) { front.point("") },
			wantReason:  "connection refused",
		},
		{
			name:        "a source returning garbage records an unevaluated sample with a parse reason and holds the step",
			breakSource: func(front *sourceFront) { front.point(hostPort(garbage.URL)) },
			wantReason:  "bad_response",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const (
				interval = 30 * time.Second
				bake     = 5 * time.Minute
			)
			ctx := context.Background()
			store := newPromStub(t)
			store.respondVector("0.01")
			front := newSourceFront(t, hostPort(store.srv.URL))

			events := make(chan event.GenericEvent, 8)
			s := NewPrometheusSampler(events, 2, time.Hour)
			in, cs := lifecycleInputs(t, s)
			// The sampler stamps samples from the clock the gate paces by; the
			// phases below move that clock across the interval.
			s.now = func() time.Time { return in.Now }
			in.Prometheus = &v1beta1.AnalysisPrometheus{ServerAddress: front.url()}
			in.QueryTimeout = 5 * time.Second
			a := lifecycleAnalysis(interval, 1)
			step := v1beta1.RolloutGroupStep{
				Capacity: intstr.FromString("50%"),
				Traffic:  50,
				Pause:    &v1beta1.RolloutPause{Duration: &metav1.Duration{Duration: bake}},
			}

			// sample runs one interval's pair of passes: the kick, then the
			// consume once the query has landed.
			sample := func(t *testing.T, label string) stepDecision {
				t.Helper()
				if got := evaluateAnalysisStep(ctx, in, a, cs, step); got != decHold {
					t.Fatalf("%s: the pass that kicks the query decided %v, want hold", label, got)
				}
				waitEvent(t, events)
				return evaluateAnalysisStep(ctx, in, a, cs, step)
			}

			// The store answers behind the front: the step reads health and bakes.
			if got := sample(t, "healthy source"); got != decHold {
				t.Fatalf("healthy sample inside the bake = %v, want hold", got)
			}
			if mr := cs.MetricResults[0]; !mr.Passed || mr.Value != "0.01" {
				t.Fatalf("healthy sample surfaced %+v, want the store's passing value", mr)
			}
			readAt := cs.LastConclusiveEvaluationTime.Time

			tt.breakSource(front)
			for i := 1; i <= 2; i++ {
				label := fmt.Sprintf("unusable source, sample %d", i)
				in.Now = in.Now.Add(interval)
				if got := sample(t, label); got != decHold {
					t.Fatalf("%s: decision = %v, want hold (an unusable source is not a breach, even at FailureLimit 1)", label, got)
				}
				if len(cs.MetricResults) != 1 {
					t.Fatalf("%s: MetricResults = %+v, want the one metric", label, cs.MetricResults)
				}
				mr := cs.MetricResults[0]
				if mr.Passed || mr.Value != "" || mr.Message == "" {
					t.Fatalf("%s: MetricResults[0] = %+v, want an unevaluated sample (no value, not passed) carrying the reason", label, mr)
				}
				if !strings.Contains(mr.Message, tt.wantReason) {
					t.Fatalf("%s: message = %q, want it to name the reason (%q)", label, mr.Message, tt.wantReason)
				}
				if mr.Time == nil || !mr.Time.Time.Equal(in.Now.Truncate(time.Second)) {
					t.Fatalf("%s: sample stamped %v, want its own time %v", label, mr.Time, in.Now.Truncate(time.Second))
				}
				if cs.AnalysisFailedChecks != 0 {
					t.Fatalf("%s: AnalysisFailedChecks = %d, want 0", label, cs.AnalysisFailedChecks)
				}
				if !cs.LastConclusiveEvaluationTime.Time.Equal(readAt) {
					t.Fatalf("%s: LastConclusiveEvaluationTime moved to %v; an unevaluated sample is not conclusive", label, cs.LastConclusiveEvaluationTime)
				}
			}

			// The store answers behind the front again, and the bake has elapsed:
			// the next sample reads health and the step proceeds.
			front.point(hostPort(store.srv.URL))
			in.Now = in.Now.Add(bake)
			if got := sample(t, "source back"); got != decAdvance {
				t.Fatalf("source back, bake elapsed: decision = %v, want advance", got)
			}
			if mr := cs.MetricResults[0]; !mr.Passed || mr.Value != "0.01" || mr.Message != "" {
				t.Fatalf("source back: MetricResults[0] = %+v, want the store's passing value again", mr)
			}
			if !cs.LastConclusiveEvaluationTime.Time.After(readAt) {
				t.Fatalf("source back: LastConclusiveEvaluationTime = %v, want the new conclusive sample's time", cs.LastConclusiveEvaluationTime)
			}
			if cs.AnalysisFailedChecks != 0 {
				t.Fatalf("source back: AnalysisFailedChecks = %d, want 0", cs.AnalysisFailedChecks)
			}
		})
	}
}
