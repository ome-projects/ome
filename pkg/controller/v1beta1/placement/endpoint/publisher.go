// Package endpoint programs one concrete traffic backend for a multi-cluster
// InferenceService.
//
// The control-plane fan-out controller (package placement) already records the
// winner and the winner's externally-addressable URL in
// status.placement.{cluster,endpoint}. That is status-only: an external LB has
// to consume it. This package closes that gap by actually programming a backend.
// The Gateway API HTTPRoute backend is the default implementation. The
// EndpointPublisher interface lets another backend reuse the same watch and
// lifecycle reconciliation.
package endpoint

import (
	"context"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// Target is the resolved publication intent for one InferenceService: the global
// host to program and the one-or-more serving homes it must resolve to. Built by
// the reconciler from status.placement and the Config; the EndpointPublisher
// backend translates it into its own resource(s). Single mode yields exactly one
// Home; All/Split yield one per serving cluster.
type Target struct {
	// Service is the logical service represented by the target.
	Service string

	// GlobalHost is the externally-addressable hostname used by publishers that
	// create a global endpoint. It is empty for backends that consume only Homes.
	GlobalHost string

	// Homes are the serving clusters the global host resolves to, one per admitted
	// home. In Single there is exactly one; in All/Split there is one per cluster
	// currently serving. Never empty when the reconciler decides to publish.
	Homes []Home
}

// Home is one serving cluster the global host load-balances across.
type Home struct {
	// Cluster is the WorkloadCluster serving this home (labels/logging).
	Cluster string
	// Endpoint is the complete client-facing URL reported by this home. Backends
	// that need more than a Kubernetes Service hostname can consume it directly.
	Endpoint string
	// BackendHost is that cluster's ingress hostname the global host resolves to —
	// the host of the home's status endpoint. A bare hostname (no scheme/port);
	// the backend supplies the port from Config.
	BackendHost string
	// Weight is the home's relative traffic share: the TrafficMap's
	// capacity-aware, health-gated weight when the routing controller has
	// published one, else the reactive ready-replica count (its ready replicas in
	// Split, zero in Single/All or for a Split home with no ready replicas yet).
	// The publisher equal-weights all homes when every weight is zero, so an
	// unweighted placement routes evenly rather than dropping to no-traffic.
	Weight int32
}

// EndpointPublisher programs a backend from the resolved serving homes and
// tears it down when the service leaves the placed state. Implementations MUST be
// idempotent: Publish for an unchanged Target is a no-op, and Unpublish for an
// already-clean service is a no-op.
type EndpointPublisher interface {
	// Publish ensures the backend routes target.GlobalHost to
	// target.Homes for isvc, creating or repointing as needed.
	Publish(ctx context.Context, isvc *v1beta1.InferenceService, target Target) error

	// Unpublish removes any backend resources this publisher created for isvc.
	// Safe to call when nothing was ever published.
	Unpublish(ctx context.Context, isvc *v1beta1.InferenceService) error

	// Name identifies the backend for logging/metrics (e.g. "GatewayAPI").
	Name() string
}
