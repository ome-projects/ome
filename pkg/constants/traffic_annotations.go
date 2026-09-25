// Traffic-management annotation keys.
//
// All annotations live under the OME group prefix (ome.io/). Operators
// set them on the InferenceService metadata; the traffic translator
// reads them and threads the values into the emitted backend policy
// resource.
package constants

// Backend-policy annotation keys.
//
// Long-tail load-balancing knobs that did not graduate to the typed
// core (TrafficSpec) in alpha. Each key is independently validated by
// the admission webhook; unknown keys with the `ome.io/` prefix that
// look like one of these (Levenshtein 2) are rejected with a
// Did-you-mean suggestion.
var (
	// Circuit breaker.
	CircuitBreakerMaxConnectionsAnnotation            = OMEAPIGroupName + "/circuit-breaker-max-connections"
	CircuitBreakerMaxParallelRequestsAnnotation       = OMEAPIGroupName + "/circuit-breaker-max-parallel-requests"
	CircuitBreakerMaxPendingRequestsAnnotation        = OMEAPIGroupName + "/circuit-breaker-max-pending-requests"
	CircuitBreakerMaxParallelRetriesAnnotation        = OMEAPIGroupName + "/circuit-breaker-max-parallel-retries"
	CircuitBreakerPerEndpointMaxConnectionsAnnotation = OMEAPIGroupName + "/circuit-breaker-per-endpoint-max-connections"

	// Retries.
	RetryAttemptsAnnotation      = OMEAPIGroupName + "/retry-attempts"
	RetryOnAnnotation            = OMEAPIGroupName + "/retry-on"
	RetryPerTryTimeoutAnnotation = OMEAPIGroupName + "/retry-per-try-timeout"

	// Connection / sub-second timeouts. Request-level timeout stays on
	// ComponentExtensionSpec.TimeoutSeconds; these cover the rest.
	TimeoutIdleAnnotation                  = OMEAPIGroupName + "/timeout-idle"
	TimeoutMaxConnectionDurationAnnotation = OMEAPIGroupName + "/timeout-max-connection-duration"
	TimeoutTCPConnectAnnotation            = OMEAPIGroupName + "/timeout-tcp-connect"
)

// Operational annotation keys.
var (
	// TrafficDrainAnnotation carries durable manual route-arm overrides on a
	// control-plane InferenceService. Its value is a strict JSON object keyed by
	// independent override IDs; each entry names one workload cluster and an
	// operator reason. The placement routing controller holds every matching arm
	// at weight zero until the corresponding entry is removed. The annotation is
	// control-plane-only and is stripped from derived workload ISVCs.
	TrafficDrainAnnotation = OMEAPIGroupName + "/traffic-drain"

	// ManagedByConflictAckedAnnotation, when set to "true" on the
	// InferenceService, acknowledges a hand-authored backend policy
	// with the conflicting name OME would use. Suppresses the GA
	// admission rejection while OME continues to defer to the
	// manually-authored resource.
	ManagedByConflictAckedAnnotation = OMEAPIGroupName + "/managed-by-conflict-acked"

	// RolloutReadyTimeoutAnnotation overrides the timeout for new-revision
	// pods to reach Ready before the rollout is marked Failed. Duration
	// string (e.g. "15m"). Wins over the plan's readyTimeout and the
	// operator-configured default (rollout.defaultReadyTimeout); when none of
	// the three is set the gate never escalates on capacity wait alone.
	RolloutReadyTimeoutAnnotation = OMEAPIGroupName + "/rollout-ready-timeout"

	// RevisionHistoryLimitAnnotation caps the number of non-live
	// ControllerRevisions retained per Component before garbage
	// collection. Must be a positive integer (rejected at admission
	// otherwise). Live revisions — current, target, and every
	// per-Instance running/target revision — are never deleted
	// regardless of the limit. Overrides the operator-level
	// lifecycle.revisionHistoryLimit value from the
	// inferenceservice-config ConfigMap; when neither is set, no
	// revisions are pruned.
	RevisionHistoryLimitAnnotation = OMEAPIGroupName + "/revision-history-limit"

	// RolloutPromoteAnnotation is the operator verb that advances a paused
	// canary step under Manual promotion (canary with neither auto nor
	// analysis set). Value: the canary revision hash (copied from
	// status.canary.canaryRevisionHash) — the hash guards against promoting
	// a stale revision, and admission rejects any other value. The
	// controller clears the annotation after consuming it. No-op when
	// spec.rollout.groups[].canary is unset.
	RolloutPromoteAnnotation = OMEAPIGroupName + "/rollout-promote"

	// RolloutRollbackAnnotation is the operator verb for instant
	// rollback. Value: "true" ("false" is identical to absence). Traffic
	// shifts to 100% old, new revision drains after scaleDownDelaySeconds.
	// Controller clears the annotation after consuming it.
	RolloutRollbackAnnotation = OMEAPIGroupName + "/rollout-rollback"

	// PausedRolloutAnnotation, set on the InferenceService, holds every
	// coordination group at its current phase (and the canary step
	// machine) and suspends fleet changes until the operator clears it.
	// Two accepted values:
	//   "true"   — pause: no Update / Create / Migration work starts or
	//              advances, but each component's RestartPolicy keeps
	//              repairing existing Instances at their current
	//              revision.
	//   "freeze" — full stop: repair is suspended too; only kubelet
	//              container restarts and deliberate scale-down remain.
	// Status stays truthful in either depth: an Instance that lost every
	// pod stops reporting Ready even though its recovery stays parked.
	// Any other value is treated as not paused.
	PausedRolloutAnnotation = OMEAPIGroupName + "/rollout-paused"

	// PausedRolloutFreezeValue is the PausedRolloutAnnotation value that
	// additionally suspends Instance repair.
	PausedRolloutFreezeValue = "freeze"

	// RolloutRepinAnnotation is the one-shot operator verb that replaces an
	// active run's pinned plan with a fresh render of the current effective
	// source, preserving run identity and progress. Value: the expected
	// render digest (CAS — the repin is rejected when the current render no
	// longer matches, guarding against a racing edit); the literal "now"
	// skips the digest check. A repin may only hold or tighten: the step
	// index clamps into the new ladder, and a clamped step that would raise
	// exposure is not applied — the run holds at the currently-programmed
	// capacity/traffic pending an explicit promote. The controller clears
	// the annotation after consuming it.
	RolloutRepinAnnotation = OMEAPIGroupName + "/rollout-repin"

	// RolloutPlanSourceAnnotation carries per-group policy provenance on a
	// DERIVED InferenceService whose rollout groups were inflated from
	// RolloutPolicy refs at derive time. Members copy it into the pinned
	// run's group provenance at run open, so member status reports the same
	// identity a locally-resolved ref would. Value: semicolon-separated
	// "<groupIndex>=<policyName>@<portableDigest>" entries. System-authored
	// (derived objects only) — never set by users.
	RolloutPlanSourceAnnotation = OMEAPIGroupName + "/rollout-plan-source"
)

// RolloutPauseState interprets PausedRolloutAnnotation on the given
// annotations map. paused reports whether any pause is in effect;
// freeze reports the full-stop variant. Unknown values are not paused —
// every consumer must share this parse so a pause registers uniformly
// across the coordination, canary, projection, and workload layers.
func RolloutPauseState(annotations map[string]string) (paused, freeze bool) {
	switch annotations[PausedRolloutAnnotation] {
	case "true":
		return true, false
	case PausedRolloutFreezeValue:
		return true, true
	default:
		return false, false
	}
}

// Pass-through annotation prefixes.
//
// Operators can reach implementation-specific fields OME does not
// type-model by setting annotations under these prefixes. The
// translator stitches the value verbatim into the named path on the
// emitted resource. OME performs no schema validation; the gateway
// controller catches errors and OME surfaces GatewayRejected.
//
// Each prefix is per-translator. Using a prefix whose translator is
// not active surfaces UnsupportedPassthrough at admission.
//
// These are var rather than const because OMEAPIGroupName itself is
// declared as var in this package; same pattern as the existing
// annotation block in constants.go.
var (
	// PassthroughEnvoyGatewayPrefix maps to
	// gateway.envoyproxy.io/v1alpha1.BackendTrafficPolicy.spec.<path>
	// when the Envoy Gateway translator is active.
	PassthroughEnvoyGatewayPrefix = OMEAPIGroupName + "/btp."

	// PassthroughIstioPrefix maps to
	// networking.istio.io/.../DestinationRule.spec.<path>
	// when the Istio translator is active.
	PassthroughIstioPrefix = OMEAPIGroupName + "/dr."
)
