package tree

import (
	"fmt"
	"sort"
	"strings"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// Violation is one broken invariant, attributed to the node that broke it.
//
// Attribution is not always the node the caller was writing. Two children can
// each be individually valid and jointly bust their parent; that violation
// belongs to the parent. A webhook therefore cannot ask "did my object break
// anything?" by filtering on name — it compares the before and after sets. See
// Diff.
type Violation struct {
	// Node is the CR name the violation is attributed to. Empty only for a
	// violation about the input as a whole rather than any one node.
	Node string
	// Reason is an AcceleratorQuotaReason* constant, so a controller can map a
	// violation onto a Degraded condition without re-deriving why.
	Reason string
	// Subject identifies *what* within the node broke: a budget key, a
	// conflicting namespace, an unresolved parent name. It is what makes two
	// violations comparable across builds — unlike Message, which embeds running
	// totals that change as the tree is edited. Empty when the violation
	// concerns the node as a whole.
	Subject string
	// Message explains the violation in terms an operator can act on.
	Message string
}

// Key is the identity of a violation across builds: same node, same invariant,
// same subject. Diffing on Key rather than on the whole struct is what lets a
// webhook admit a write that *shrinks* an existing overrun — the numbers in
// Message change, the Key does not.
func (v Violation) Key() [3]string { return [3]string{v.Node, v.Reason, v.Subject} }

// String renders one violation. Violation is deliberately not an error: the
// slice is the error type, so errors.As on the pair is unambiguous.
func (v Violation) String() string {
	if v.Node == "" {
		return v.Message
	}
	return fmt.Sprintf("%s: %s", v.Node, v.Message)
}

// reasonRank orders reasons by which one an operator must act on first, so a
// node broken several ways reports the cause rather than a symptom. A node with
// no position in the tree cannot meaningfully be judged on its budget.
var reasonRank = map[string]int{
	v1beta1.AcceleratorQuotaReasonParentMissing:       0,
	v1beta1.AcceleratorQuotaReasonParentCycle:         1,
	v1beta1.AcceleratorQuotaReasonUnreachable:         2,
	v1beta1.AcceleratorQuotaReasonDuplicateNode:       3,
	v1beta1.AcceleratorQuotaReasonNodeKindInvalid:     4,
	v1beta1.AcceleratorQuotaReasonDepthExceeded:       5,
	v1beta1.AcceleratorQuotaReasonContainmentViolated: 6,
	v1beta1.AcceleratorQuotaReasonShareUnresolved:     7,
}

func rankOf(reason string) int {
	if r, ok := reasonRank[reason]; ok {
		return r
	}
	return len(reasonRank)
}

// Violations is the full set found in one Build, ordered by node name and then
// by which reason to act on first. The order is stable so a rejected apply
// produces the same message every time it is retried.
type Violations []Violation

// Error renders every violation on one line.
func (vs Violations) Error() string {
	switch len(vs) {
	case 0:
		return ""
	case 1:
		return vs[0].String()
	}
	parts := make([]string, 0, len(vs))
	for _, v := range vs {
		parts = append(parts, v.String())
	}
	return fmt.Sprintf("%d invariants violated: %s", len(vs), strings.Join(parts, "; "))
}

// OrNil returns vs as an error, or a nil error when empty. It exists because a
// non-nil error interface holding an empty slice is the classic Go trap.
func (vs Violations) OrNil() error {
	if len(vs) == 0 {
		return nil
	}
	return vs
}

// For returns the violations attributed to one node. Note this answers "what is
// wrong with this node", not "what did writing this node break" — for the
// latter, see Diff.
func (vs Violations) For(node string) Violations {
	var out Violations
	for _, v := range vs {
		if v.Node == node {
			out = append(out, v)
		}
	}
	return out
}

// Primary returns the violation to report as a node's condition reason: the
// highest-precedence one. Zero value when vs is empty.
func (vs Violations) Primary() Violation {
	var best Violation
	first := true
	for _, v := range vs {
		if first || rankOf(v.Reason) < rankOf(best.Reason) {
			best, first = v, false
		}
	}
	return best
}

// Diff returns the violations present in vs but absent from before, compared by
// Key. It is how an admission webhook decides whether the write under review
// made things worse:
//
//	_, before, err := tree.Build(live, opts)
//	_, after, err  := tree.Build(tree.Splice(live, incoming), opts)
//	if newly := after.Diff(before); len(newly) > 0 { deny(newly.Error()) }
//
// Comparing by Key rather than by value is the load-bearing part. A tree can
// already be violating when the write arrives — concurrent admissions can reach
// that state, which is why the controller re-checks at all — and an admin must
// be able to repair it. A write that shrinks an existing overrun keeps the same
// Key and is admitted; a write that adds an overrun on a different budget has a
// different Subject and is caught.
func (vs Violations) Diff(before Violations) Violations {
	seen := make(map[[3]string]struct{}, len(before))
	for _, v := range before {
		seen[v.Key()] = struct{}{}
	}
	var out Violations
	for _, v := range vs {
		if _, had := seen[v.Key()]; had {
			continue
		}
		out = append(out, v)
	}
	return out
}

// Nodes lists the distinct nodes carrying at least one violation, sorted. These
// are the nodes that *cause* a freeze; the frozen set also includes their
// descendants, which Tree.Frozen computes.
func (vs Violations) Nodes() []string {
	seen := map[string]struct{}{}
	for _, v := range vs {
		if v.Node == "" {
			continue
		}
		seen[v.Node] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func (vs Violations) sorted() Violations {
	sort.SliceStable(vs, func(i, j int) bool {
		if vs[i].Node != vs[j].Node {
			return vs[i].Node < vs[j].Node
		}
		if ri, rj := rankOf(vs[i].Reason), rankOf(vs[j].Reason); ri != rj {
			return ri < rj
		}
		return vs[i].Subject < vs[j].Subject
	})
	return vs
}
