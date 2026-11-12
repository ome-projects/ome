package crdskew

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
)

const (
	// ReasonLagging is the event reason and condition reason while a CRD of
	// the manager's API group lacks a field this release knows.
	ReasonLagging = "CRDSchemaLagging"
	// ReasonCurrent is the event reason and condition reason once a CRD
	// that lagged carries this release's fields again.
	ReasonCurrent = "CRDSchemaCurrent"
)

// Finding is the comparison of one installed CRD with this release's schema.
type Finding struct {
	// CRD is the CustomResourceDefinition's name: the plural, a dot, the group.
	CRD string
	// MissingFields are the fields this release knows that the installed
	// schema lacks, as sorted dotted paths; empty when the CRD is current.
	MissingFields []string
}

// Lagging reports whether the installed CRD lacks a field this release knows.
func (f Finding) Lagging() bool { return len(f.MissingFields) > 0 }

// Sentence says in plain words what the installed CRD lacks and what that
// costs. It is the text of the log line, the event and the condition.
func (f Finding) Sentence() string {
	if !f.Lagging() {
		return fmt.Sprintf("CRD %s carries every field this release of the manager knows.", f.CRD)
	}
	return fmt.Sprintf("CRD %s lacks fields this release of the manager knows: %s. The apiserver drops them from every write until the CRD is upgraded to this release.",
		f.CRD, strings.Join(f.MissingFields, ", "))
}

// Report is the manager's current picture of the installed CRDs, shared by
// the start-up comparison, the CRD watch and the service status writer. The
// zero value and a nil Report read as nothing lagging.
type Report struct {
	mu       sync.RWMutex
	findings map[string]Finding
}

// Set stores the finding for its CRD and reports whether the stored picture
// of that CRD changed: a first finding that lags, or a different set of
// missing fields from the one held.
func (r *Report) Set(f Finding) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	held, ok := r.findings[f.CRD]
	if r.findings == nil {
		r.findings = map[string]Finding{}
	}
	r.findings[f.CRD] = f
	if !ok {
		return f.Lagging()
	}
	return !slices.Equal(held.MissingFields, f.MissingFields)
}

// Forget drops the finding for a CRD that no longer exists.
func (r *Report) Forget(crd string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.findings, crd)
}

// Lagging returns the findings of the CRDs that lack a field, by CRD name.
func (r *Report) Lagging() []Finding {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	var lagging []Finding
	for _, f := range r.findings {
		if f.Lagging() {
			lagging = append(lagging, f)
		}
	}
	sort.Slice(lagging, func(i, j int) bool { return lagging[i].CRD < lagging[j].CRD })
	return lagging
}

// Message is the sentences of every lagging CRD, in CRD order; empty when
// nothing lags.
func (r *Report) Message() string {
	var sentences []string
	for _, f := range r.Lagging() {
		sentences = append(sentences, f.Sentence())
	}
	return strings.Join(sentences, " ")
}
