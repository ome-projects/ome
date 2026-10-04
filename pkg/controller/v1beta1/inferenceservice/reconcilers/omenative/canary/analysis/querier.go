package analysis

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"

	promapi "github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"k8s.io/utils/clock"
)

// ErrNoData is returned when a query produces no usable value — an empty result
// vector, a NaN (a 0/0 error-rate ratio before any traffic), or an infinity (an
// x/0 ratio). The reconciler maps it to INCONCLUSIVE ("can't tell"), never to a
// metric breach: "no requests yet" must not be read as "the canary is failing,"
// and a divide-by-zero +Inf must not be read as "infinitely bad."
var ErrNoData = errors.New("prometheus query returned no usable data")

// Querier runs an instant PromQL query and returns every usable value it
// produced (one per series for a vector). Selecting which value to compare is
// operator-dependent, so it belongs to Evaluate, not here. Querier is the only
// component in this package that performs network I/O; everything layered above
// it (Evaluate) is pure and tested with a fake Querier.
type Querier interface {
	Query(ctx context.Context, promQL string) ([]float64, error)
}

// NewQuerier builds a Querier against serverAddress. A non-empty bearerToken is
// sent as "Authorization: Bearer <token>"; headers are sent verbatim on every
// request (e.g. "X-Scope-OrgID" for a multi-tenant Cortex/Thanos/Mimir
// front-end). With neither, requests are unauthenticated (the bundled
// Prometheus). TLS/mTLS is not yet supported.
//
// Every query dials serverAddress afresh. A connection kept from an earlier
// query would still reach the peer the address led to when it was opened, so
// a source that has since moved or gone away would keep reading as healthy.
func NewQuerier(serverAddress, bearerToken string, headers map[string]string) (Querier, error) {
	cfg := promapi.Config{Address: serverAddress, RoundTripper: dialPerQueryTransport()}
	if bearerToken != "" || len(headers) > 0 {
		cfg.RoundTripper = &headerRoundTripper{token: bearerToken, headers: headers, base: cfg.RoundTripper}
	}
	client, err := promapi.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("build prometheus client for %q: %w", serverAddress, err)
	}
	return &promQuerier{api: promv1.NewAPI(client), clock: clock.RealClock{}}, nil
}

type promQuerier struct {
	api   promv1.API
	clock clock.Clock
}

// dialPerQueryTransport is the default HTTP transport with connection reuse
// off: a query never rides a connection an earlier query opened.
func dialPerQueryTransport() *http.Transport {
	t, ok := http.DefaultTransport.(*http.Transport)
	if ok {
		t = t.Clone()
	} else {
		t = &http.Transport{Proxy: http.ProxyFromEnvironment}
	}
	t.DisableKeepAlives = true
	return t
}

// Query runs an instant query at the current time and extracts the usable
// values. A transport error is returned verbatim (the caller maps it to
// INCONCLUSIVE); query warnings are ignored.
func (p *promQuerier) Query(ctx context.Context, promQL string) ([]float64, error) {
	val, _, err := p.api.Query(ctx, promQL, p.clock.Now())
	if err != nil {
		return nil, err
	}
	return usableValues(val)
}

// usableValues extracts every comparable value from a Prometheus result: a
// scalar yields one value; a vector yields one value per series (NaN/Inf series
// are skipped — see unusable); a string result is parsed as a float. A result
// with no usable value (empty, or all NaN/Inf) is ErrNoData. Any other type is
// an error. All series are kept so the evaluator can pick the worst one for the
// metric's comparison operator.
func usableValues(v model.Value) ([]float64, error) {
	switch t := v.(type) {
	case *model.Scalar:
		f := float64(t.Value)
		if unusable(f) {
			return nil, ErrNoData
		}
		return []float64{f}, nil
	case model.Vector:
		vals := make([]float64, 0, len(t))
		for _, s := range t {
			f := float64(s.Value)
			if unusable(f) {
				continue
			}
			vals = append(vals, f)
		}
		if len(vals) == 0 {
			return nil, ErrNoData
		}
		return vals, nil
	case *model.String:
		f, err := strconv.ParseFloat(t.Value, 64)
		if err != nil {
			return nil, fmt.Errorf("non-numeric string result %q", t.Value)
		}
		return []float64{f}, nil
	default:
		return nil, fmt.Errorf("unsupported prometheus result type %T (expected scalar or vector)", v)
	}
}

// headerRoundTripper injects a static bearer token and/or custom headers. It
// clones the request so the shared transport never sees a caller request it
// mutated.
type headerRoundTripper struct {
	token   string
	headers map[string]string
	base    http.RoundTripper
}

func (h *headerRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	r := req.Clone(req.Context())
	if h.token != "" {
		r.Header.Set("Authorization", "Bearer "+h.token)
	}
	for k, v := range h.headers {
		r.Header.Set(k, v)
	}
	return h.base.RoundTrip(r)
}

// unusable reports whether a metric value cannot be compared to a threshold: a
// NaN (a 0/0 ratio) or an infinity (an x/0 ratio). Both mean "no usable signal,"
// not "infinitely bad" — treating +Inf as a breach would burn the failure budget
// on a divide-by-zero artifact — so both map to ErrNoData -> Inconclusive.
func unusable(f float64) bool {
	return math.IsNaN(f) || math.IsInf(f, 0)
}
