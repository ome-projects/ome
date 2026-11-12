package types

import "context"

// NodePresence answers whether the cluster still holds a Node by name. The
// engine asks it once per node its pods are bound to on every pass, so an
// implementation answers from the informer cache and may confirm a Node
// the cache lacks on the live reader; it never lists Nodes per pod. A pod
// bound to a Node that does not exist runs nowhere: the engine reads it as
// gone and removes the object.
type NodePresence interface {
	NodeExists(ctx context.Context, name string) (bool, error)
}
