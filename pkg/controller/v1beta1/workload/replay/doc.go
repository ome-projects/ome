// Package replay drives workload.Reconcile over a scripted timeline and
// records everything the engine writes as a normalized text trace.
//
// The driver owns no expectations of its own: it states what the engine
// observed (pods, EndpointSlice membership, apiserver rejections, spec
// edits, clock advances) and records what the engine did about it. The
// kubelet, the scheduler and the endpoint controller are not simulated —
// a scenario asserts their outcomes as events, so the trace answers
// "given this observation, what did the engine write" and nothing else.
//
// Determinism is the package's contract. Every source of variation the
// engine can see is pinned: the clock is injected, the expectations cache
// is per-run, pod ordering comes from the fake client's sorted list, and
// identifiers that legitimately vary between builds (operation ids, UIDs,
// revision hashes, wall-clock instants) are normalized to stable tokens
// when the trace is rendered. Anything else that differs between two runs
// of the same scenario is a defect in the engine or the driver.
package replay
