package v1beta1testing

import (
	"context"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Global Eventually defaults.
//
// These set the baseline timeout / polling interval Gomega uses when an
// Eventually/Consistently call omits explicit .WithTimeout/.WithPolling.
// They are exported vars (not consts) so a suite or CI lane can override
// them before calling SetDefaultEventually() — e.g. a slow KIND lane can
// raise the timeout without editing per-assertion tiers. Per-assertion
// tiers remain available for assertions that need a deliberately different
// bound; the global default
// just removes the need to annotate every routine create→reconcile→status
// poll.
//
// Chosen to match the common-case "resource created → reconciled → status
// updated" window without masking real hangs. Override before
// SetDefaultEventually() if a lane genuinely needs different bounds.
var (
	// DefaultEventuallyTimeout is the global Eventually/Consistently
	// timeout applied by SetDefaultEventually.
	DefaultEventuallyTimeout = 30 * time.Second

	// DefaultEventuallyPollingInterval is the global polling cadence
	// applied by SetDefaultEventually. 200ms keeps `go test` feedback
	// snappy while bounding apiserver QPS during long polls.
	DefaultEventuallyPollingInterval = 200 * time.Millisecond
)

// SetDefaultEventually installs DefaultEventuallyTimeout and
// DefaultEventuallyPollingInterval as Gomega's global Eventually
// defaults. Call once from a suite's BeforeSuite (or TestMain) so bare
// Eventually(...).Should(...) calls inherit a sane timeout/poll without
// per-assertion annotation. Suites that need a different bound for a
// specific assertion still pass .WithTimeout/.WithPolling (or the
// a suite's own timeout tiers) explicitly.
func SetDefaultEventually() {
	gomega.SetDefaultEventuallyTimeout(DefaultEventuallyTimeout)
	gomega.SetDefaultEventuallyPollingInterval(DefaultEventuallyPollingInterval)
}

// objPtr constrains a type to a pointer that implements client.Object,
// letting the generic helpers allocate a fresh T for read-backs.
type objPtr[T any] interface {
	client.Object
	*T
}

// CreateAndWait creates obj and then blocks (via Gomega Eventually,
// using the global default timeout/poll) until a fresh Get of the same
// key succeeds — i.e. the object is observable through the supplied
// client's cache. Useful right after creating a resource a controller
// will act on, so the test doesn't race the informer.
//
// The create itself is asserted with Expect; a create failure fails the
// calling spec immediately.
func CreateAndWait[PtrT objPtr[T], T any](ctx context.Context, c client.Client, obj PtrT) {
	ginkgo.GinkgoHelper()
	gomega.Expect(c.Create(ctx, obj)).To(gomega.Succeed())
	key := client.ObjectKeyFromObject(obj)
	fetched := PtrT(new(T))
	gomega.Eventually(func() error {
		return c.Get(ctx, key, fetched)
	}).Should(gomega.Succeed(), "created object never became observable")
}

// CreateAndWaitReady creates obj and then blocks until ready returns
// true for a freshly-fetched copy. The ready predicate receives the
// latest object read from the client each poll, so callers express
// readiness in terms of the live status (condition true, State==Ready,
// IsReady(), etc.) without writing the poll loop. Uses the global
// Eventually default timeout/poll; pass a custom timeout via
// CreateAndWaitReadyWithTimeout when a type legitimately needs longer.
//
// On success the last-observed (ready) object is copied back into obj,
// so the caller can inspect the live status after the call returns.
//
// readyDescription is woven into the failure message so a timeout points
// at what was being waited on.
func CreateAndWaitReady[PtrT objPtr[T], T any](ctx context.Context, c client.Client,
	obj PtrT, readyDescription string, ready func(PtrT) bool) {
	ginkgo.GinkgoHelper()
	createAndWaitReady(ctx, c, obj, readyDescription, ready, 0, 0)
}

// CreateAndWaitReadyWithTimeout is CreateAndWaitReady with an explicit
// timeout and polling interval (0 for either falls back to the global
// default). Use for the rare type whose readiness window differs from
// the global baseline.
func CreateAndWaitReadyWithTimeout[PtrT objPtr[T], T any](ctx context.Context, c client.Client,
	obj PtrT, readyDescription string, ready func(PtrT) bool, timeout, poll time.Duration) {
	ginkgo.GinkgoHelper()
	createAndWaitReady(ctx, c, obj, readyDescription, ready, timeout, poll)
}

func createAndWaitReady[PtrT objPtr[T], T any](ctx context.Context, c client.Client,
	obj PtrT, readyDescription string, ready func(PtrT) bool, timeout, poll time.Duration) {
	ginkgo.GinkgoHelper()
	gomega.Expect(c.Create(ctx, obj)).To(gomega.Succeed())

	key := client.ObjectKeyFromObject(obj)
	fetched := PtrT(new(T))
	assertion := gomega.Eventually(func(g gomega.Gomega) {
		g.Expect(c.Get(ctx, key, fetched)).To(gomega.Succeed())
		g.Expect(ready(fetched)).To(gomega.BeTrue())
	})
	if timeout > 0 {
		assertion = assertion.WithTimeout(timeout)
	}
	if poll > 0 {
		assertion = assertion.WithPolling(poll)
	}
	assertion.Should(gomega.Succeed(), "object never became ready: %s", readyDescription)
	// Surface the last-observed ready object to the caller.
	*obj = *fetched
}
