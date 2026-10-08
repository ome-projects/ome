// Package replay drives one InferenceService reconcile pass per tick over a
// scripted timeline, in the controller's order: the rollout run layer, the
// canary bind, the boundary flush, the replica projection, the canary
// dispatch, the coordination pass, the aggregate and the final flush with
// its annotation consumption. The engines are the production packages; the
// driver supplies the fake apiserver with a lagging informer view and a
// live reader, the InferenceReplica status the scenario publishes, the
// pods, the policies, the revisions and the clock, and records what every
// stage wrote as a normalized trace.
//
// A scenario is a YAML timeline. Parse decodes one against the event
// vocabulary, and the tests in this package exercise the format end to end.
package replay
