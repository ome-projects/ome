package types

import (
	"time"

	testclock "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// fixedClock pins the seam every Now() reads through, so a test asserting
// a moment does not race the wall clock.
func fixedClock(at time.Time) *testclock.FakeClock { return testclock.NewFakeClock(at) }

// namedClient is a distinguishable client: the Reader() precedence test
// only needs to tell the cached one from the uncached one apart.
type namedClient struct {
	client.Client
	name string
}

func newNamedClient(name string) *namedClient {
	return &namedClient{Client: fake.NewClientBuilder().Build(), name: name}
}
