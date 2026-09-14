// Package tree assembles a set of AcceleratorQuota CRs into the quota tree they
// describe and reports every invariant they violate.
//
// The tree is the graph of spec.parentRef edges, so no single CR can be checked
// in isolation: whether a node's parent resolves, whether it sits inside a
// cycle, how deep it is, and whether its budget fits inside its parent's are
// all properties of the whole set. This package is that whole-set view, and it
// has two callers with different needs:
//
//   - The validating webhook splices the object under review into the live set
//     and rejects the write if the result is not a well-formed tree. It wants
//     one readable message.
//   - The controller rebuilds the tree every reconcile as the authority, because
//     admission cannot serialize concurrent writes. It wants to know which node
//     broke and why, so it can freeze that subtree and set a condition reason.
//
// Build therefore returns a best-effort tree *and* a list of violations rather
// than failing outright: a broken node must not cost the controller its view of
// the nodes that are still sound, or one bad edit would freeze the fleet.
//
// Pure: no Kubernetes client, no cluster state, no I/O. Everything the checks
// need is either in the CRs or in Options, which carries the config-driven knobs
// so this package invents no defaults of its own.
package tree
