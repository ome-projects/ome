// Package endpoint programs ONE concrete global-traffic backend so the global
// host of a multi-cluster InferenceService resolves to whichever workload
// cluster currently wins the placement race.
//
// The control-plane fan-out controller (package placement) already records the
// winner and the winner's externally-addressable URL in
// status.placement.{cluster,endpoint}. That is status-only: an external LB has
// to consume it. This package closes that gap by actually programming a backend
// — the Gateway API HTTPRoute backend (GatewayAPIPublisher) is the first
// concrete implementation; the EndpointPublisher interface lets DNS/GSLB
// backends be added later without touching the reconciler.
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
	// GlobalHost is the externally-addressable hostname the publisher programs
	// (e.g. "my-svc.global.example"). Rendered from Config + the ISVC, never a
	// baked-in literal.
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

// EndpointPublisher programs the global-traffic backend(s) so the global host of
// a multi-cluster InferenceService points at every serving home (one in Single;
// several, load-balanced, in All/Split), adjusts the set as homes come and go,
// and tears it all down when the service is no longer placed. Implementations
// are backend-specific (Gateway API HTTPRoute today; DNS/GSLB later) and MUST be
// idempotent: Publish for an unchanged Target is a no-op, and Unpublish for an
// already-clean service is a no-op.
type EndpointPublisher interface {
	// Publish ensures the backend routes target.GlobalHost to
	// target.BackendHost for isvc, creating or repointing as needed.
	Publish(ctx context.Context, isvc *v1beta1.InferenceService, target Target) error

	// Unpublish removes any backend resources this publisher created for isvc.
	// Safe to call when nothing was ever published.
	Unpublish(ctx context.Context, isvc *v1beta1.InferenceService) error

	// Name identifies the backend for logging/metrics (e.g. "GatewayAPI").
	Name() string
}
