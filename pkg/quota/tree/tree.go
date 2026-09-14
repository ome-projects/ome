package tree

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// maxRenderedCyclePath bounds how many node names a cycle message spells out.
// A large cycle would otherwise produce a message per member, each carrying
// every name, which overruns both an admission response and the apiserver's
// limit on a condition message.
const maxRenderedCyclePath = 8

// ErrRootNameUnset is returned when Options.RootName is empty. It is a caller
// configuration failure, not a property of the nodes, so it is an error rather
// than a Violation: a controller must requeue and say so, not quietly conclude
// that a fleet with no root has nothing to reconcile.
var ErrRootNameUnset = errors.New("quota tree: root name is not configured")

// Options carries the config-driven knobs. This package invents no defaults —
// an unset knob disables the check it governs rather than assuming a value.
type Options struct {
	// RootName is the reserved name of the single parent-less node. Required;
	// Build returns ErrRootNameUnset without it, because there is no way to tell
	// the fleet root from a node whose parent was deleted.
	//
	// In OME this is always v1beta1.AcceleratorQuotaRootName — the name is a
	// wire-contract identity, not a deployment tunable. It stays a parameter so
	// this package invents nothing and stays testable on an arbitrary tree.
	RootName string

	// MaxDepth is the greatest permitted distance from the root, counted in
	// edges: the root is 0, a top-tier grouping is 1. Zero disables the check.
	// Note the units when wiring this to config — a knob named for a count of
	// levels is one greater than a distance.
	MaxDepth int
}

// Node is one position in the assembled tree.
type Node struct {
	// Quota is the CR this node was built from; never nil. The tree holds a
	// pointer into the slice passed to Build rather than a copy, so a caller can
	// write status through it — and must not share that slice across goroutines.
	Quota *v1beta1.AcceleratorQuota
	// Parent is nil for the root and for any node whose parentRef did not
	// resolve. It may participate in a cycle, so never walk it with a bare loop;
	// use Ancestors.
	Parent *Node
	// Children are sorted by name for deterministic traversal.
	Children []*Node
	// Depth is the distance from the root; 0 for the root, -1 when the node is
	// not reachable from it.
	Depth int
}

// Name returns the node's CR name.
func (n *Node) Name() string { return n.Quota.Name }

// Role returns the node's declared role.
func (n *Node) Role() v1beta1.AcceleratorQuotaRole { return n.Quota.Spec.Role }

// IsLeaf reports whether this node carries the budget and is a tenant. It
// reads the declared role, never the presence of children: a grouping created
// before its first child is still a grouping.
func (n *Node) IsLeaf() bool {
	return n.Quota.Spec.Role == v1beta1.AcceleratorQuotaRoleClusterQueue
}

// Reachable reports whether the node has a position in the tree. An unreachable
// node must not materialize.
func (n *Node) Reachable() bool { return n.Depth >= 0 }

// Ancestors returns the node's ancestors root-first, excluding n itself, and is
// the only safe way to walk upward: Build keeps cyclic nodes in the tree with
// their parent edges intact, so a bare `for p := n.Parent; p != nil` loop never
// terminates. On a cycle the walk stops at the first repeat.
//
// Root-first is the order a materializer needs — a Cohort must exist before the
// object that names it as parent.
func (n *Node) Ancestors() []*Node {
	var up []*Node
	seen := map[string]struct{}{n.Name(): {}}
	for cur := n.Parent; cur != nil; cur = cur.Parent {
		if _, repeat := seen[cur.Name()]; repeat {
			break
		}
		seen[cur.Name()] = struct{}{}
		up = append(up, cur)
	}
	for i, j := 0, len(up)-1; i < j; i, j = i+1, j-1 {
		up[i], up[j] = up[j], up[i]
	}
	return up
}

// Tree is the assembled set. It always contains every input node, including
// ones that violate an invariant, so a caller can report on what is broken
// instead of losing sight of it.
type Tree struct {
	// Root is the reserved root node, or nil when no node claimed that name.
	Root  *Node
	nodes map[string]*Node
}

// Node looks a node up by CR name.
func (t *Tree) Node(name string) (*Node, bool) {
	n, ok := t.nodes[name]
	return n, ok
}

// Len is the number of nodes.
func (t *Tree) Len() int { return len(t.nodes) }

// Nodes returns every node, sorted by name.
func (t *Tree) Nodes() []*Node {
	out := make([]*Node, 0, len(t.nodes))
	for _, n := range t.nodes {
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name() < out[j].Name() })
	return out
}

// Leaves returns every node whose declared role is a leaf, sorted by name.
// These are the nodes that materialize a ClusterQueue.
func (t *Tree) Leaves() []*Node {
	var out []*Node
	for _, n := range t.Nodes() {
		if n.IsLeaf() {
			out = append(out, n)
		}
	}
	return out
}

// Subtree returns the node and every descendant in pre-order — parents before
// children, siblings by name. That is both the set a caller freezes and the
// order a materializer creates in, since a Cohort must exist before the object
// naming it. Nil for an unknown name.
func (t *Tree) Subtree(name string) []*Node {
	start, ok := t.nodes[name]
	if !ok {
		return nil
	}
	var out []*Node
	seen := map[string]struct{}{}
	var walk func(*Node)
	walk = func(n *Node) {
		if _, dup := seen[n.Name()]; dup {
			return // a cycle among unreachable nodes must not hang the walk
		}
		seen[n.Name()] = struct{}{}
		out = append(out, n)
		for _, c := range n.Children {
			walk(c)
		}
	}
	walk(start)
	return out
}

// Frozen maps every node name to the violation that freezes it — its own, or
// the nearest ancestor's. A node absent from the map is clear.
//
// This is the join a controller needs and must not hand-roll: freezing exactly
// the violating nodes would leave their descendants materializing under a
// parent that was never created, which Kueue papers over by silently rehoming
// the child onto an implicit default cohort.
func (t *Tree) Frozen(vs Violations) map[string]Violation {
	frozen := make(map[string]Violation)
	// Own violations win over an inherited one, so a node reports its own cause.
	for _, name := range vs.Nodes() {
		frozen[name] = vs.For(name).Primary()
	}
	for _, name := range vs.Nodes() {
		cause := frozen[name]
		for _, d := range t.Subtree(name) {
			if _, own := frozen[d.Name()]; own {
				continue
			}
			frozen[d.Name()] = cause
		}
	}
	return frozen
}

// Build assembles nodes into a tree and reports every invariant they violate.
//
// It returns a tree even when violations exist: a broken node must not cost a
// caller its view of the nodes that are still sound, or one bad edit would
// freeze the fleet. The error return is reserved for a caller-configuration
// failure, where there is no meaningful tree to return at all.
func Build(quotas []v1beta1.AcceleratorQuota, opts Options) (*Tree, Violations, error) {
	if opts.RootName == "" {
		return nil, nil, ErrRootNameUnset
	}

	var vs Violations
	t := &Tree{nodes: make(map[string]*Node, len(quotas))}
	for i := range quotas {
		q := &quotas[i]
		if _, dup := t.nodes[q.Name]; dup {
			vs = append(vs, Violation{
				Node:    q.Name,
				Reason:  v1beta1.AcceleratorQuotaReasonDuplicateNode,
				Message: "node name appears more than once in the assembled set",
			})
			continue
		}
		t.nodes[q.Name] = &Node{Quota: q, Depth: -1}
	}

	vs = append(vs, linkParents(t)...)
	vs = append(vs, resolveRoot(t, opts.RootName)...)
	vs = append(vs, checkUnreachable(t, vs)...)
	vs = append(vs, checkDepth(t, opts.MaxDepth)...)
	vs = append(vs, checkNodeKind(t)...)
	vs = append(vs, checkContainment(t)...)

	return t, vs.sorted(), nil
}

// linkParents wires parent/child edges and reports dangling parents and cycles.
func linkParents(t *Tree) Violations {
	var vs Violations
	for _, n := range t.Nodes() {
		ref := n.Quota.Spec.ParentRef
		if ref == nil {
			continue
		}
		if ref.Name == n.Name() {
			vs = append(vs, Violation{
				Node:    n.Name(),
				Reason:  v1beta1.AcceleratorQuotaReasonParentCycle,
				Subject: n.Name(),
				Message: "parentRef points at the node itself",
			})
			continue
		}
		parent, ok := t.nodes[ref.Name]
		if !ok {
			vs = append(vs, Violation{
				Node:    n.Name(),
				Reason:  v1beta1.AcceleratorQuotaReasonParentMissing,
				Subject: ref.Name,
				Message: fmt.Sprintf("parentRef %q does not resolve to an existing AcceleratorQuota", ref.Name),
			})
			continue
		}
		n.Parent = parent
		parent.Children = append(parent.Children, n)
	}
	for _, n := range t.Nodes() {
		sort.Slice(n.Children, func(i, j int) bool { return n.Children[i].Name() < n.Children[j].Name() })
	}
	return append(vs, reportCycles(t)...)
}

// reportCycles names each cycle once and attributes it to every member, so a
// controller can freeze each of them, without repeating the full path in every
// message. A node in a cycle has no assignable position, and neither does
// anything beneath it — the whole affected set is disabled rather than cut at
// an arbitrary edge, which is the rule Kueue applies to its own cohort graph.
func reportCycles(t *Tree) Violations {
	var vs Violations
	// A cycle is identified by its alphabetically-smallest member, so the
	// Subject is stable no matter which node the walk started from.
	rendered := map[string]bool{}
	for _, n := range t.Nodes() {
		members := cycleMembers(n)
		if members == nil {
			continue
		}
		id := members[0]
		for _, m := range members[1:] {
			if m < id {
				id = m
			}
		}
		msg := fmt.Sprintf("node is inside the parentRef cycle through %q", id)
		if !rendered[id] {
			rendered[id] = true
			msg = fmt.Sprintf("parentRef forms a cycle: %s", renderPath(members))
		}
		vs = append(vs, Violation{
			Node:    n.Name(),
			Reason:  v1beta1.AcceleratorQuotaReasonParentCycle,
			Subject: id,
			Message: msg,
		})
	}
	return vs
}

// cycleMembers walks parents from n and returns the cycle it lands in, or nil.
// Walking up rather than down means a node hanging off a cycle reports it too:
// it is just as unplaceable as a member.
func cycleMembers(n *Node) []string {
	seen := map[string]int{}
	var path []string
	for cur := n; cur != nil; cur = cur.Parent {
		if at, ok := seen[cur.Name()]; ok {
			return append(path[at:], cur.Name())
		}
		seen[cur.Name()] = len(path)
		path = append(path, cur.Name())
	}
	return nil
}

func renderPath(names []string) string {
	if len(names) > maxRenderedCyclePath {
		head := names[:maxRenderedCyclePath]
		return fmt.Sprintf("%s -> ... (%d more)", strings.Join(head, " -> "), len(names)-maxRenderedCyclePath)
	}
	return strings.Join(names, " -> ")
}

// resolveRoot identifies the reserved root and reports every other parent-less
// node. A second parent-less node is not a second tree we tolerate: the fleet
// budget is the root's number, and a detached subtree has no ceiling at all.
func resolveRoot(t *Tree, rootName string) Violations {
	var vs Violations
	for _, n := range t.Nodes() {
		if n.Quota.Spec.ParentRef != nil {
			continue
		}
		if n.Name() == rootName {
			t.Root = n
			continue
		}
		vs = append(vs, Violation{
			Node:    n.Name(),
			Reason:  v1beta1.AcceleratorQuotaReasonParentMissing,
			Subject: rootName,
			Message: fmt.Sprintf("only the reserved root %q may omit parentRef; every other node must name its parent", rootName),
		})
	}
	if t.Root != nil {
		assignDepth(t.Root)
	}
	return vs
}

// assignDepth walks down from the root, the only traversal guaranteed to
// terminate: nodes in or under a cycle are never reached, so they keep the
// Depth -1 they were built with.
func assignDepth(root *Node) {
	root.Depth = 0
	queue := []*Node{root}
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		for _, c := range n.Children {
			if c.Depth >= 0 {
				continue
			}
			c.Depth = n.Depth + 1
			queue = append(queue, c)
		}
	}
}

// checkUnreachable reports nodes that are structurally fine but have no
// position, because an ancestor is missing or looping. Without this the freeze
// set would stop at the broken ancestor and every node below it would
// materialize into a tree that does not exist.
func checkUnreachable(t *Tree, found Violations) Violations {
	faulted := map[string]struct{}{}
	for _, n := range found.Nodes() {
		faulted[n] = struct{}{}
	}
	var vs Violations
	for _, n := range t.Nodes() {
		if n.Reachable() {
			continue
		}
		if _, own := faulted[n.Name()]; own {
			continue // already reports the cause itself
		}
		cause := n.Name()
		for _, a := range n.Ancestors() {
			if _, bad := faulted[a.Name()]; bad {
				cause = a.Name()
				break
			}
		}
		vs = append(vs, Violation{
			Node:    n.Name(),
			Reason:  v1beta1.AcceleratorQuotaReasonUnreachable,
			Subject: cause,
			Message: fmt.Sprintf("node has no position in the tree because ancestor %q is unresolved", cause),
		})
	}
	return vs
}

func checkDepth(t *Tree, maxDepth int) Violations {
	if maxDepth <= 0 {
		return nil
	}
	var vs Violations
	for _, n := range t.Nodes() {
		if n.Depth > maxDepth {
			vs = append(vs, Violation{
				Node:   n.Name(),
				Reason: v1beta1.AcceleratorQuotaReasonDepthExceeded,
				Message: fmt.Sprintf("node is %d levels below the root, exceeding the configured maximum of %d",
					n.Depth, maxDepth),
			})
		}
	}
	return vs
}

// checkNodeKind validates structure against the declared role. The CRD's CEL
// rules cover the same-object half, but they are stripped from the minimal CRD
// variant and cannot see whether another node names this one as its parent, so
// the authority is here.
func checkNodeKind(t *Tree) Violations {
	var vs Violations
	for _, n := range t.Nodes() {
		spec := &n.Quota.Spec
		switch n.Role() {
		case v1beta1.AcceleratorQuotaRoleClusterQueue:
			// The violation belongs to the child that attached itself, not to
			// the leaf it attached to: a third party must not be able to freeze
			// someone else's live tenant by creating a node under it.
			for _, c := range n.Children {
				vs = append(vs, Violation{
					Node:    c.Name(),
					Reason:  v1beta1.AcceleratorQuotaReasonNodeKindInvalid,
					Subject: n.Name(),
					Message: fmt.Sprintf("parent %q is a ClusterQueue node, which takes no children; "+
						"attach to a Cohort, or add a ClusterQueue leaf beside it instead", n.Name()),
				})
			}
			if len(spec.Budgets) == 0 {
				vs = append(vs, Violation{
					Node:    n.Name(),
					Reason:  v1beta1.AcceleratorQuotaReasonNodeKindInvalid,
					Message: "a ClusterQueue node must carry at least one budget",
				})
			}
		case v1beta1.AcceleratorQuotaRoleCohort:
			var set []string
			if spec.PriorityTier != "" {
				set = append(set, "priorityTier")
			}
			if spec.Distribution != nil {
				set = append(set, "distribution")
			}
			if len(set) > 0 {
				vs = append(vs, Violation{
					Node:    n.Name(),
					Reason:  v1beta1.AcceleratorQuotaReasonNodeKindInvalid,
					Subject: strings.Join(set, ","),
					Message: fmt.Sprintf("a Cohort node must not set %s; no workload binds to a grouping",
						strings.Join(set, ", ")),
				})
			}
			for _, b := range spec.Budgets {
				if b.Policy == "" && b.BorrowingLimit == nil && b.LendingLimit == nil && len(b.PerCluster) == 0 {
					continue
				}
				key := budgetKey(b.ResourceName, b.ResourceFlavor)
				vs = append(vs, Violation{
					Node:    n.Name(),
					Reason:  v1beta1.AcceleratorQuotaReasonNodeKindInvalid,
					Subject: key,
					Message: fmt.Sprintf("budget %s carries leaf-only fields; a Cohort budget is an authoring "+
						"guardrail that never materializes, so it holds only resourceName, resourceFlavor, and nominal",
						key),
				})
			}
		default:
			vs = append(vs, Violation{
				Node:    n.Name(),
				Reason:  v1beta1.AcceleratorQuotaReasonNodeKindInvalid,
				Message: fmt.Sprintf("unknown role %q", n.Role()),
			})
		}
	}
	return vs
}

// checkContainment enforces parent nominal >= sum of children's nominal, per
// (resource, flavor).
//
// This is an authoring guardrail only. Kueue's cohort quota is additive rather
// than a ceiling, so nothing below the budget enforces it at runtime — which is
// exactly why it has to be enforced here, where an admin can still be told.
//
// A node with no budget entry for a pair is unconstrained on that pair, not
// implicitly zero. That is what lets a projected internal node (which carries
// no budget, its fleet number being meaningless on one cluster) and a grouping
// created before it is sized both pass.
func checkContainment(t *Tree) Violations {
	var vs Violations
	for _, n := range t.Nodes() {
		// A leaf may not have children at all; charging it for ones it was
		// illegally given would blame it twice for someone else's write.
		if n.IsLeaf() || len(n.Children) == 0 || len(n.Quota.Spec.Budgets) == 0 {
			continue
		}
		sums := map[string]*resource.Quantity{}
		for _, c := range n.Children {
			for _, b := range c.Quota.Spec.Budgets {
				k := budgetKey(b.ResourceName, b.ResourceFlavor)
				if cur, ok := sums[k]; ok {
					cur.Add(b.Nominal)
					continue
				}
				q := b.Nominal.DeepCopy()
				sums[k] = &q
			}
		}
		for _, b := range n.Quota.Spec.Budgets {
			k := budgetKey(b.ResourceName, b.ResourceFlavor)
			sum, ok := sums[k]
			if !ok {
				continue
			}
			if b.Nominal.Cmp(*sum) < 0 {
				vs = append(vs, Violation{
					Node:    n.Name(),
					Reason:  v1beta1.AcceleratorQuotaReasonContainmentViolated,
					Subject: k,
					Message: fmt.Sprintf("budget %s is %s but its children total %s",
						k, b.Nominal.String(), sum.String()),
				})
			}
		}
	}
	return vs
}

func budgetKey(resourceName, flavor string) string {
	return resourceName + " on " + flavor
}

// Splice returns existing with incoming replacing the entry of the same name,
// or appended when there is none. It is how a webhook asks "what would the tree
// look like if I admitted this?" without mutating what it read.
func Splice(existing []v1beta1.AcceleratorQuota, incoming v1beta1.AcceleratorQuota) []v1beta1.AcceleratorQuota {
	out := make([]v1beta1.AcceleratorQuota, 0, len(existing)+1)
	replaced := false
	for _, q := range existing {
		if q.Name == incoming.Name {
			out = append(out, incoming)
			replaced = true
			continue
		}
		out = append(out, q)
	}
	if !replaced {
		out = append(out, incoming)
	}
	return out
}

// Without returns existing with the named entry removed. It is how a webhook
// asks "what would the tree look like if I admitted this delete?" — the only
// way to catch a delete that would orphan a subtree.
func Without(existing []v1beta1.AcceleratorQuota, name string) []v1beta1.AcceleratorQuota {
	out := make([]v1beta1.AcceleratorQuota, 0, len(existing))
	for _, q := range existing {
		if q.Name == name {
			continue
		}
		out = append(out, q)
	}
	return out
}
