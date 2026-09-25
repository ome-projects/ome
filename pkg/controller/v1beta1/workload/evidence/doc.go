// Package evidence classifies what the cluster is observed to be doing
// and decides nothing with it.
//
// One classifier per fact, each a pure read over the reconcile's input,
// the persisted row and the live pods: a pod wedged in a terminal kubelet
// waiting reason past its grace, a pod in readiness or gate limbo, the
// node under a Terminating pod, the class of an apiserver rejection, a
// crash-loop wedge on an otherwise idle row, whether a template diff can
// be rolled in place.
//
// Nothing here writes, and nothing here selects a branch. A classifier
// names a fact and the anchor time that fact began; the escalation, the
// deadline disposition and the operations are what turn a named fact into
// a transition. Keeping the two apart is what lets one fact be read at
// several points in a pass and mean the same thing at each of them.
package evidence
