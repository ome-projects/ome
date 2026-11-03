package types

import (
	"sync"
	"time"

	"k8s.io/utils/clock"
)

// expectationsTTL is how long an Instance's expectation stays in the
// cache before falling back to a fresh observation. Two minutes is
// generous; even slow watch propagation should converge well before.
const expectationsTTL = 2 * time.Minute

// Expectations is a per-Instance counter cache. Before issuing a batch
// of pod creates or deletes, the controller calls ExpectCreates / ExpectDeletes
// so subsequent reconciles can tell when the watch event chain has caught
// up. Subsequent reconciles call Satisfied to check whether they can
// safely issue another batch (or proceed past the create step).
//
// Concurrency: all methods are safe for concurrent use; an in-process
// singleton (DefaultExpectations) is provided for the dispatch path.
type Expectations struct {
	mu      sync.Mutex
	entries map[expectationKey]*expectationEntry
	clock   clock.Clock
}

// expectationKey is the per-Instance cache key. OwnerName mirrors
// types.Key.OwnerName — the workload-side owner identifier shared by
// every adapter (ISVC.Name, IR.Name, future owners) rather than the
// CRD-specific "ISVC" name.
type expectationKey struct {
	Namespace string
	OwnerName string
	Component ComponentType
	Instance  int32
}

type expectationEntry struct {
	Adds     int
	Deletes  int
	Deadline time.Time
}

// NewExpectations builds an empty cache.
func NewExpectations() *Expectations {
	return &Expectations{
		entries: make(map[expectationKey]*expectationEntry),
		clock:   clock.RealClock{},
	}
}

// NewExpectationsWithClock is NewExpectations with an injected clock,
// for TTL-boundary tests.
func NewExpectationsWithClock(c clock.Clock) *Expectations {
	e := NewExpectations()
	if c != nil {
		e.clock = c
	}
	return e
}

// DefaultExpectations is the in-process singleton used by the workload
// dispatcher. Tests construct their own via NewExpectations.
var DefaultExpectations = NewExpectations()

// ExpectCreates records that n pod creates are in flight for the given
// (OwnerName, Component, Instance). Satisfied returns false until all
// of them are observed via ObservedCreate, or the deadline elapses.
func (e *Expectations) ExpectCreates(namespace, ownerName string, component ComponentType, instance int32, n int) {
	e.expect(expectationKey{namespace, ownerName, component, instance}, n, 0)
}

// ExpectDeletes records that n pod deletes are in flight.
func (e *Expectations) ExpectDeletes(namespace, ownerName string, component ComponentType, instance int32, n int) {
	e.expect(expectationKey{namespace, ownerName, component, instance}, 0, n)
}

// expect adds in-flight creates and deletes to the key's entry and renews
// its deadline. A call that adds nothing leaves the cache untouched.
func (e *Expectations) expect(k expectationKey, adds, deletes int) {
	if adds <= 0 && deletes <= 0 {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	ent, ok := e.entries[k]
	if !ok {
		ent = &expectationEntry{}
		e.entries[k] = ent
	}
	ent.Adds += adds
	ent.Deletes += deletes
	ent.Deadline = e.clock.Now().Add(expectationsTTL)
}

// ObservedCreate decrements the create counter, called when a pod
// belonging to (OwnerName, Component, Instance) appears in the watch
// cache.
func (e *Expectations) ObservedCreate(namespace, ownerName string, component ComponentType, instance int32) {
	e.observed(namespace, ownerName, component, instance, true)
}

// ObservedDelete decrements the delete counter, called when a pod
// belonging to (OwnerName, Component, Instance) disappears.
func (e *Expectations) ObservedDelete(namespace, ownerName string, component ComponentType, instance int32) {
	e.observed(namespace, ownerName, component, instance, false)
}

func (e *Expectations) observed(namespace, ownerName string, component ComponentType, instance int32, isAdd bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	k := expectationKey{namespace, ownerName, component, instance}
	ent, ok := e.entries[k]
	if !ok {
		return
	}
	if isAdd && ent.Adds > 0 {
		ent.Adds--
	}
	if !isAdd && ent.Deletes > 0 {
		ent.Deletes--
	}
	if ent.Adds == 0 && ent.Deletes == 0 {
		delete(e.entries, k)
	}
}

// Satisfied returns true when no outstanding creates or deletes are
// expected for the Instance — either the watch cache has caught up,
// the entry expired, or no expectations were ever recorded.
func (e *Expectations) Satisfied(namespace, ownerName string, component ComponentType, instance int32) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	k := expectationKey{namespace, ownerName, component, instance}
	ent, ok := e.entries[k]
	if !ok {
		return true
	}
	if e.clock.Now().After(ent.Deadline) {
		delete(e.entries, k)
		return true
	}
	return ent.Adds <= 0 && ent.Deletes <= 0
}

// Forget clears the entry, e.g., when the Instance is deleted entirely.
func (e *Expectations) Forget(namespace, ownerName string, component ComponentType, instance int32) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.entries, expectationKey{namespace, ownerName, component, instance})
}
