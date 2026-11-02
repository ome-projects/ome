// Package fakeclient builds the controller-runtime fake client the unit tests
// share: one scheme with the Kubernetes built-in types and the ome.io group,
// the objects a test starts from, the kinds whose status is a subresource,
// the field indexes the production code queries, and the behaviours a test
// asks of a client that misbehaves: refuse a call of one verb or of every
// write, count the calls of one verb or of every write, edit what a read
// answers, answer a read from a stale copy, run raw hooks.
//
// The behaviours sit in a fixed order outside the store, so options may be
// listed in any order: every call is counted first, then a refusing rule may
// end it, then the store (behind any raw hooks and read edits) answers, then
// a stale copy may replace the answer.
//
// Wrap layers the same behaviours over a client that exists already, so one
// store can be read through two views that misbehave differently, as a
// reconciler's cache and its direct API reader do.
//
// RecordedEvents reads what a FakeRecorder holds, so every test checks the
// events a pass emitted the same way.
package fakeclient

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// Option configures the client New builds.
type Option interface {
	apply(*config)
}

type optionFunc func(*config)

func (f optionFunc) apply(c *config) { f(c) }

type index struct {
	obj     client.Object
	field   string
	extract client.IndexerFunc
}

// config is what the options collect before New builds the client.
type config struct {
	scheme    *runtime.Scheme
	types     []func(*runtime.Scheme) error
	objects   []client.Object
	status    []client.Object
	indexes   []index
	raw       []interceptor.Funcs
	observers map[Verb][]*Calls
	failures  map[Verb][]*Failure
	stale     []client.Object
}

// New builds a fake client from the options. Without WithScheme it builds on
// Scheme(t); without a behaviour option it is the plain fake client.
func New(t testing.TB, opts ...Option) client.WithWatch {
	t.Helper()
	cfg := collect(opts)
	cfg.validate(t)
	s := cfg.scheme
	if s == nil {
		s = Scheme(t)
	}
	addTypes(t, s, cfg.types...)
	b := fake.NewClientBuilder().WithScheme(s).WithObjects(cfg.objects...)
	if len(cfg.status) > 0 {
		b = b.WithStatusSubresource(cfg.status...)
	}
	for _, ix := range cfg.indexes {
		b = b.WithIndex(ix.obj, ix.field, ix.extract)
	}
	return cfg.layer(b.Build())
}

// Wrap layers behaviours and raw hooks over a client that exists already,
// so a second view of its store can misbehave on its own: a reconciler's
// cache that lags or refuses beside the direct API reader that answers live.
// Writes through the view reach base's store. An option that shapes the
// store (WithScheme, WithTypes, WithObjects, WithStatusSubresource,
// WithIndex) fails the test, because the store is base's.
func Wrap(t testing.TB, base client.WithWatch, opts ...Option) client.WithWatch {
	t.Helper()
	cfg := collect(opts)
	cfg.validate(t)
	if name := cfg.storeOption(); name != "" {
		t.Fatalf("fakeclient: Wrap(%s): the store is the base client's; build it with New", name)
	}
	return cfg.layer(base)
}

func collect(opts []Option) *config {
	cfg := &config{observers: map[Verb][]*Calls{}, failures: map[Verb][]*Failure{}}
	for _, o := range opts {
		o.apply(cfg)
	}
	return cfg
}

// storeOption names the first option that shapes the store, or "".
func (c *config) storeOption() string {
	switch {
	case c.scheme != nil:
		return "WithScheme"
	case len(c.types) > 0:
		return "WithTypes"
	case len(c.objects) > 0:
		return "WithObjects"
	case len(c.status) > 0:
		return "WithStatusSubresource"
	case len(c.indexes) > 0:
		return "WithIndex"
	}
	return ""
}

// layer wraps base in the raw hooks and then in the behaviours, which run
// outside the hooks.
func (c *config) layer(base client.WithWatch) client.WithWatch {
	for _, f := range c.raw {
		base = interceptor.NewClient(base, f)
	}
	if len(c.observers)+len(c.failures)+len(c.stale) == 0 {
		return base
	}
	return interceptor.NewClient(base, c.hooks())
}

// validate fails the test on a verb no hook serves or a call number that
// can never come, so a misspelt behaviour never passes silently.
func (c *config) validate(t testing.TB) {
	t.Helper()
	for verb := range c.observers {
		if !knownVerbs[verb] {
			t.Fatalf("fakeclient: CountingVerb(%q): unknown verb", verb)
		}
	}
	for verb, fs := range c.failures {
		if !knownVerbs[verb] {
			t.Fatalf("fakeclient: FailingVerb(%q): unknown verb", verb)
		}
		for _, f := range fs {
			if f.nth < EveryCall {
				t.Fatalf("fakeclient: FailingVerb(%q, %d): the call number is EveryCall or 1-based", verb, f.nth)
			}
		}
	}
}

// WithScheme hands New the scheme to build on, for a test that shares it
// with a decoder or a reconciler. WithTypes still adds to it.
func WithScheme(s *runtime.Scheme) Option {
	return optionFunc(func(c *config) { c.scheme = s })
}

// WithTypes registers extra API groups on the scheme, each by its
// AddToScheme function.
func WithTypes(add ...func(*runtime.Scheme) error) Option {
	return optionFunc(func(c *config) { c.types = append(c.types, add...) })
}

// WithObjects seeds the store.
func WithObjects(objs ...client.Object) Option {
	return optionFunc(func(c *config) { c.objects = append(c.objects, objs...) })
}

// WithStatusSubresource names the kinds whose status is a subresource: Update
// then leaves status alone and Status().Update leaves the rest alone, as the
// apiserver does.
func WithStatusSubresource(objs ...client.Object) Option {
	return optionFunc(func(c *config) { c.status = append(c.status, objs...) })
}

// WithIndex registers a field index so that List with a matching field
// selector works, as it does against the manager's cache.
func WithIndex(obj client.Object, field string, extract client.IndexerFunc) Option {
	return optionFunc(func(c *config) { c.indexes = append(c.indexes, index{obj, field, extract}) })
}

// WithInterceptors wraps the store in raw hooks for a behaviour no option
// expresses. The hooks run inside the counting, refusing and stale layers, so
// a counter still sees a call a hook answers itself.
func WithInterceptors(funcs interceptor.Funcs) Option {
	return optionFunc(func(c *config) { c.raw = append(c.raw, funcs) })
}
