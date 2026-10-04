package analysis

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// fakeProm is an httptest-backed stub of the Prometheus instant-query API. It
// keys canned responses by the PromQL string it receives on the wire, so a test
// asserts the full client path: what NewQuerier actually sends (path, form
// encoding, headers) and how the response JSON decodes back into values.
type fakeProm struct {
	t   *testing.T
	srv *httptest.Server

	mu        sync.Mutex
	responses map[string]promResponse
	requests  []promRequest
}

type promResponse struct {
	status int
	body   string
}

type promRequest struct {
	path    string
	query   string
	headers http.Header
}

func newFakeProm(t *testing.T) *fakeProm {
	t.Helper()
	f := &fakeProm{t: t, responses: map[string]promResponse{}}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		q := r.Form.Get("query")
		f.mu.Lock()
		f.requests = append(f.requests, promRequest{path: r.URL.Path, query: q, headers: r.Header.Clone()})
		resp, ok := f.responses[q]
		f.mu.Unlock()
		if !ok {
			http.Error(w, `{"status":"error","errorType":"bad_data","error":"unexpected query"}`, http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.status)
		_, _ = w.Write([]byte(resp.body))
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeProm) respond(query string, status int, body string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.responses[query] = promResponse{status: status, body: body}
}

func (f *fakeProm) recorded() []promRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.requests)
}

// vectorBody builds a success response with one series per value. Prometheus
// encodes sample values as JSON strings, so NaN/Inf arrive as "NaN"/"+Inf" —
// exactly the wire shapes the decoder must handle.
func vectorBody(values ...string) string {
	series := make([]string, 0, len(values))
	for i, v := range values {
		series = append(series, `{"metric":{"pod":"pod-`+string(rune('a'+i))+`"},"value":[1700000000,"`+v+`"]}`)
	}
	return `{"status":"success","data":{"resultType":"vector","result":[` + strings.Join(series, ",") + `]}}`
}

func mustQuerier(t *testing.T, addr, token string, headers map[string]string) Querier {
	t.Helper()
	q, err := NewQuerier(addr, token, headers)
	if err != nil {
		t.Fatalf("NewQuerier: %v", err)
	}
	return q
}

// TestQuerierHTTP_ResultShapes pins the decode path for every Prometheus result
// shape the wire can produce: vectors (including NaN/Inf series), scalars,
// strings, empties, and the unsupported matrix type.
func TestQuerierHTTP_ResultShapes(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		want      []float64
		wantErr   error  // errors.Is target; nil means no specific target
		errSubstr string // non-empty means an error containing this is expected
	}{
		{
			name: "single-series vector",
			body: vectorBody("0.01"),
			want: []float64{0.01},
		},
		{
			name: "multi-series vector returns every series",
			body: vectorBody("0.01", "0.2", "0.05"),
			want: []float64{0.01, 0.2, 0.05},
		},
		{
			name: "NaN and Inf series are skipped",
			body: vectorBody("NaN", "2", "+Inf"),
			want: []float64{2},
		},
		{
			name:    "all-NaN vector is no data",
			body:    vectorBody("NaN", "NaN"),
			wantErr: ErrNoData,
		},
		{
			name:    "empty vector is no data",
			body:    `{"status":"success","data":{"resultType":"vector","result":[]}}`,
			wantErr: ErrNoData,
		},
		{
			name: "scalar result",
			body: `{"status":"success","data":{"resultType":"scalar","result":[1700000000,"1.5"]}}`,
			want: []float64{1.5},
		},
		{
			// The client library decodes only scalar/vector/matrix, so a string
			// resultType is a decode error on the wire — it must surface as an
			// error (→ Inconclusive), never as a value.
			name:      "string result is a decode error",
			body:      `{"status":"success","data":{"resultType":"string","result":[1700000000,"1.25"]}}`,
			errSubstr: "unexpected value type",
		},
		{
			name:      "matrix result is unsupported",
			body:      `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1700000000,"1"]]}]}}`,
			errSubstr: "unsupported prometheus result type",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prom := newFakeProm(t)
			prom.respond("up", http.StatusOK, tt.body)
			q := mustQuerier(t, prom.srv.URL, "", nil)

			got, err := q.Query(context.Background(), "up")
			switch {
			case tt.wantErr != nil:
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
			case tt.errSubstr != "":
				if err == nil || !strings.Contains(err.Error(), tt.errSubstr) {
					t.Fatalf("err = %v, want one containing %q", err, tt.errSubstr)
				}
			default:
				if err != nil {
					t.Fatalf("Query: %v", err)
				}
				if !slices.Equal(got, tt.want) {
					t.Fatalf("values = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

// TestQuerierHTTP_ServerErrors pins that HTTP-level and Prometheus-level errors
// come back as transport errors — never as ErrNoData, whose "no data" reading
// would mask a broken source as a quiet canary.
func TestQuerierHTTP_ServerErrors(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
	}{
		{
			name:   "prometheus error envelope",
			status: http.StatusUnprocessableEntity,
			body:   `{"status":"error","errorType":"execution","error":"query evaluation failed"}`,
		},
		{
			name:   "plain 500",
			status: http.StatusInternalServerError,
			body:   "internal error",
		},
		{
			name:   "503 from a front-end",
			status: http.StatusServiceUnavailable,
			body:   "upstream unavailable",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prom := newFakeProm(t)
			prom.respond("up", tt.status, tt.body)
			q := mustQuerier(t, prom.srv.URL, "", nil)

			_, err := q.Query(context.Background(), "up")
			if err == nil {
				t.Fatal("expected an error")
			}
			if errors.Is(err, ErrNoData) {
				t.Fatalf("server error must not read as ErrNoData: %v", err)
			}
		})
	}
}

// TestQuerierHTTP_Unreachable pins that a connection failure surfaces as an
// error, not ErrNoData.
func TestQuerierHTTP_Unreachable(t *testing.T) {
	prom := newFakeProm(t)
	addr := prom.srv.URL
	prom.srv.Close()

	q := mustQuerier(t, addr, "", nil)
	_, err := q.Query(context.Background(), "up")
	if err == nil {
		t.Fatal("expected an error against a closed server")
	}
	if errors.Is(err, ErrNoData) {
		t.Fatalf("connection failure must not read as ErrNoData: %v", err)
	}
}

// TestQuerierHTTP_AuthOnTheWire pins that NewQuerier wires the bearer token and
// custom headers into the requests the client actually sends — the round-tripper
// unit alone cannot prove the client was built with it.
func TestQuerierHTTP_AuthOnTheWire(t *testing.T) {
	prom := newFakeProm(t)
	prom.respond("up", http.StatusOK, vectorBody("1"))

	q := mustQuerier(t, prom.srv.URL, "test-token", map[string]string{"X-Scope-OrgID": "tenant-a"})
	if _, err := q.Query(context.Background(), "up"); err != nil {
		t.Fatalf("Query: %v", err)
	}

	reqs := prom.recorded()
	if len(reqs) == 0 {
		t.Fatal("no request reached the server")
	}
	for _, r := range reqs {
		if got := r.headers.Get("Authorization"); got != "Bearer test-token" {
			t.Errorf("Authorization = %q, want %q", got, "Bearer test-token")
		}
		if got := r.headers.Get("X-Scope-OrgID"); got != "tenant-a" {
			t.Errorf("X-Scope-OrgID = %q, want %q", got, "tenant-a")
		}
		if r.query != "up" {
			t.Errorf("query on the wire = %q, want %q", r.query, "up")
		}
		if r.path != "/api/v1/query" {
			t.Errorf("path = %q, want /api/v1/query", r.path)
		}
	}
}

// TestQuerierHTTP_NoAuthByDefault pins that an unauthenticated querier (the
// bundled-Prometheus path) sends no Authorization header at all.
func TestQuerierHTTP_NoAuthByDefault(t *testing.T) {
	prom := newFakeProm(t)
	prom.respond("up", http.StatusOK, vectorBody("1"))

	q := mustQuerier(t, prom.srv.URL, "", nil)
	if _, err := q.Query(context.Background(), "up"); err != nil {
		t.Fatalf("Query: %v", err)
	}
	for _, r := range prom.recorded() {
		if got := r.headers.Get("Authorization"); got != "" {
			t.Errorf("unexpected Authorization header %q on unauthenticated querier", got)
		}
	}
}

// TestQuerierHTTP_ContextTimeout pins that a hung Prometheus is bounded by the
// caller's context: Query returns a deadline error instead of blocking.
func TestQuerierHTTP_ContextTimeout(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Hang until the client gives up; release unblocks teardown so a hung
		// handler can never wedge the server's Close.
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer srv.Close()
	defer close(release)

	q := mustQuerier(t, srv.URL, "", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := q.Query(ctx, "up")
	if err == nil {
		t.Fatal("expected a deadline error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("Query blocked %v past its 100ms deadline", elapsed)
	}
}

// TestEvaluateHTTP_EndToEnd runs Evaluate over a real querier and the fake
// server: templates render into the query that reaches the wire, multi-series
// vectors reduce to the operator's worst series, and a broken second source
// surfaces per-metric while the breach still decides the outcome.
func TestEvaluateHTTP_EndToEnd(t *testing.T) {
	prom := newFakeProm(t)
	tc := TemplateContext{
		Namespace:     "canary-a",
		CanaryService: "canary-a-svc-engine-rev2",
	}

	t.Run("pass surfaces the worst series value", func(t *testing.T) {
		const rendered = `err_rate{service="canary-a-svc-engine-rev2"}`
		prom.respond(rendered, http.StatusOK, vectorBody("0.01", "0.04"))

		q := mustQuerier(t, prom.srv.URL, "", nil)
		a := &v1beta1.RolloutAnalysis{Metrics: []v1beta1.AnalysisMetric{{
			Name:      "err",
			Query:     `err_rate{service="{{.CanaryService}}"}`,
			Operator:  v1beta1.ComparisonLTE,
			Threshold: "0.05",
		}}}
		res := Evaluate(context.Background(), q, a, tc)
		if res.Outcome != Pass {
			t.Fatalf("Outcome = %v, want Pass (metrics=%+v)", res.Outcome, res.Metrics)
		}
		mr := res.Metrics[0]
		if !mr.Passed || mr.Value != "0.04" {
			t.Fatalf("metric = %+v, want passed with worst-series value 0.04", mr)
		}
	})

	t.Run("breach beats a broken metric and both surface", func(t *testing.T) {
		prom.respond(`breach_q`, http.StatusOK, vectorBody("0.5"))
		prom.respond(`broken_q`, http.StatusInternalServerError, "boom")

		q := mustQuerier(t, prom.srv.URL, "", nil)
		a := &v1beta1.RolloutAnalysis{Metrics: []v1beta1.AnalysisMetric{
			{Name: "err", Query: "breach_q", Operator: v1beta1.ComparisonLTE, Threshold: "0.05"},
			{Name: "lat", Query: "broken_q", Operator: v1beta1.ComparisonLTE, Threshold: "100"},
		}}
		res := Evaluate(context.Background(), q, a, tc)
		if res.Outcome != Fail {
			t.Fatalf("Outcome = %v, want Fail (a known breach beats a broken source)", res.Outcome)
		}
		if res.Metrics[0].Passed || res.Metrics[0].Value != "0.5" {
			t.Fatalf("breach metric = %+v, want failed with value 0.5", res.Metrics[0])
		}
		if res.Metrics[1].Message == "" {
			t.Fatalf("broken metric must carry the query error, got %+v", res.Metrics[1])
		}
	})
}

// TestQuerierHTTP_EachQueryDialsAnew pins that no connection is carried from
// one query to the next: every query dials the server address afresh and so
// reaches whatever that address resolves to now, not a peer connected before
// the address was re-aimed. One Querier per query, as the sampler builds them.
func TestQuerierHTTP_EachQueryDialsAnew(t *testing.T) {
	var mu sync.Mutex
	conns := 0
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, vectorBody("1"))
	}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			mu.Lock()
			conns++
			mu.Unlock()
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)

	for i := 1; i <= 3; i++ {
		q := mustQuerier(t, srv.URL, "", nil)
		if _, err := q.Query(context.Background(), "up"); err != nil {
			t.Fatalf("query %d: %v", i, err)
		}
		mu.Lock()
		got := conns
		mu.Unlock()
		if got != i {
			t.Fatalf("after query %d the server has seen %d connection(s), want one per query", i, got)
		}
	}
}
