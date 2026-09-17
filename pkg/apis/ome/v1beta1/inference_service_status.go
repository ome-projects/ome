package v1beta1

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
)

// InferenceServiceStatus defines the observed state of InferenceService
type InferenceServiceStatus struct {
	// Conditions for the InferenceService <br/>
	// - EngineReady: engine readiness condition; <br/>
	// - DecoderReady: decoder readiness condition; <br/>
	// - RouterReady: router readiness condition; <br/>
	// - IngressReady: ingress resource readiness; <br/>
	// - Ready: aggregated condition; <br/>
	duckv1.Status `json:",inline"`
	// Addressable endpoint for the InferenceService
	// +optional
	Address *duckv1.Addressable `json:"address,omitempty"`
	// URL holds the url that will distribute traffic over the provided traffic targets.
	// It generally has the form http[s]://{route-name}.{route-namespace}.{cluster-level-suffix}
	// +optional
	URL *apis.URL `json:"url,omitempty"`
	// Addresses lists every reachable endpoint for this service — one per gateway
	// its route attaches to, plus the cluster-local address. Each entry's Name
	// carries the operator-declared class (see IngressGatewaySpec.Class):
	// "internal", "external", "cluster-local", or any config-defined value.
	// Derived from the same builder that produces the HTTPRoutes, so it never
	// drifts from the actual routing. status.url is the primary gateway entry and
	// status.address is the "cluster-local" entry — both projections of this list.
	// +optional
	// +listType=atomic
	Addresses []duckv1.Addressable `json:"addresses,omitempty"`
	// Statuses for the components of the InferenceService
	Components map[ComponentType]ComponentStatusSpec `json:"components,omitempty"`
	// Model related statuses
	ModelStatus ModelStatus `json:"modelStatus,omitempty"`
	// MigrationHistory is a rolling window of OMENative migration requests
	// processed against this InferenceService. Bounded to the most recent
	// entries; older entries are dropped (and optionally archived to a
	// controller-owned ConfigMap).
	// +optional
	// +listType=atomic
	MigrationHistory []MigrationHistoryEntry `json:"migrationHistory,omitempty"`

	// MountedOverlays lists overlays the controller attached to the
	// pod spec. Skipped overlays (disabled, NotReady, NotFound) surface
	// via the OverlaysReady condition.
	// +optional
	// +listType=map
	// +listMapKey=name
	MountedOverlays []MountedOverlay `json:"mountedOverlays,omitempty"`

	// Traffic reflects the resolved backend traffic policy
	// and the emitted policy resource. Populated only when traffic
	// intent is declared via spec.traffic or any ome.io/* traffic
	// annotation; otherwise nil so older clients see nothing.
	// +optional
	Traffic *TrafficStatus `json:"traffic,omitempty"`

	// Canary tracks an in-progress spec.rollout.canary rollout (the step
	// state machine). Absent when no canary is running.
	// +optional
	Canary *CanaryStatus `json:"canary,omitempty"`

	// Placement reports multi-cluster placement of this ISVC, set by
	// the control-plane fan-out controller. Empty on workload clusters and on
	// single-cluster deployments.
	// +optional
	Placement *PlacementStatus `json:"placement,omitempty"`

	// PinnedRevisionName is the ControllerRevision (OME ns) driving
	// pod specs. Empty when autoSync is true.
	// +optional
	PinnedRevisionName string `json:"pinnedRevisionName,omitempty"`

	// LastRuntimeSyncToken is the ome.io/runtime-sync annotation
	// value at last pin advance; the controller advances the pin
	// when the live annotation differs from this.
	// +optional
	LastRuntimeSyncToken string `json:"lastRuntimeSyncToken,omitempty"`

	// Rollout is the run-model surface: the pinned active-run plan the
	// executors consume, the previous run's bounded record, and the
	// always-current per-group source resolution (including shadowed-policy
	// previews). See rollout_run_types.go.
	// +optional
	Rollout *RolloutStatus `json:"rollout,omitempty"`

	// RolloutCoordination reports per-group cross-Component
	// coordination state for the InferenceService. Populated only
	// when spec.rollout declares blueGreen/rollingUpdate groups
	// (canary groups report under status.canary instead).
	// +optional
	RolloutCoordination *RolloutCoordinationStatus `json:"rolloutCoordination,omitempty"`
}

// MountedOverlay records one overlay the controller wired into the
// current pod spec.
type MountedOverlay struct {
	Name   string `json:"name"`
	EnvVar string `json:"envVar"`
	// MountPath is empty for Sharded overlays (fetched at runtime).
	// +optional
	MountPath    string `json:"mountPath,omitempty"`
	Distribution string `json:"distribution"`
}

// ComponentStatusSpec describes the state of the component
type ComponentStatusSpec struct {
	// URL holds the primary url for this component.
	// It generally has the form http[s]://{name}.{namespace}.{cluster-level-suffix}
	// +optional
	URL *apis.URL `json:"url,omitempty"`
	// REST endpoint of the component if available.
	// +optional
	RestURL *apis.URL `json:"restURL,omitempty"`
	// Addressable endpoint for the InferenceService
	// +optional
	Address *duckv1.Addressable `json:"address,omitempty"`
	// SelectedAccelerator shows which AcceleratorClass was selected
	// +optional
	SelectedAccelerator *AcceleratorSelection `json:"selectedAccelerator,omitempty"`

	// Lifecycle reports OMENative-managed lifecycle state for this Component
	// when the Component resolves to deploymentMode OMENative
	// (spec.deploymentMode or the per-Component ome.io/deploymentMode
	// annotation).
	// Counterpart of the LifecycleSpec sub-block on ComponentExtensionSpec.
	// Nil otherwise.
	// +optional
	Lifecycle *LifecycleStatus `json:"lifecycle,omitempty"`

	// RolloutPhase reflects the current rollout state for this
	// Component. One of Stable, Canarying,
	// BlueGreenStandby, Pending, Paused, Promoting, RollingBack,
	// RolledBack, Failed. Empty when no rollout is in flight on this
	// Component (also empty for Components on deployment modes without
	// the rollout contract — e.g. RawDeployment).
	// +optional
	// +kubebuilder:validation:Enum=Stable;Canarying;BlueGreenStandby;Pending;Paused;Promoting;RollingBack;RolledBack;Failed
	RolloutPhase RolloutPhase `json:"rolloutPhase,omitempty"`

	// LatestReadyRevision is the per-revision Service name
	// (`<isvc>-<component>-rev-<hash>`) fronting the most recent
	// revision whose pods reached Ready. Equal to
	// LatestRolledoutRevision once the rollout completes; set ahead of
	// it during in-flight rollouts.
	// +optional
	LatestReadyRevision string `json:"latestReadyRevision,omitempty"`

	// LatestRolledoutRevision is the per-revision Service name
	// (`<isvc>-<component>-rev-<hash>`) of the most recent revision
	// that fully owns this Component's traffic (i.e., the rollout has
	// completed). Drives the consumer-side HTTPRoute backendRef.
	// +optional
	LatestRolledoutRevision string `json:"latestRolledoutRevision,omitempty"`

	// PreviousRolledoutRevision is the per-revision Service name
	// (`<isvc>-<component>-rev-<hash>`) of the revision that fully
	// owned this Component's traffic immediately before
	// LatestRolledoutRevision; recorded when LatestRolledoutRevision
	// advances. Retained for diagnosis and traffic reference during
	// partial rollbacks.
	// +optional
	PreviousRolledoutRevision string `json:"previousRolledoutRevision,omitempty"`

	// Traffic reports per-revision traffic weights for this
	// Component. Each entry corresponds to one revision currently
	// receiving traffic. The HTTPRoute builder reads this to
	// emit weighted backendRefs.
	// +optional
	// +listType=map
	// +listMapKey=revisionName
	Traffic []ComponentTrafficTarget `json:"traffic,omitempty"`

	// Canary tracks the canary step machine for the unit this Component
	// belongs to — the router alone, or engine+decoder together. It is
	// written on the unit's entrypoint Component (the router, or the engine)
	// and is absent on a secondary, so a reader never sees two copies of one
	// run. Absent when the unit has no canary running.
	//
	// Units advance independently, which is why the state cannot live in the
	// single InferenceServiceStatus.Canary: two runs would overwrite each
	// other's step counter and revision pair. That field is retained as an
	// alias for the entrypoint unit's run so existing readers keep working.
	// +optional
	Canary *CanaryStatus `json:"canary,omitempty"`

	// Autoscaler reports the per-Component autoscaler state — resolved
	// Class / ManagedBy / SpecSource and (when ManagedBy == "ome") live
	// CurrentReplicas / DesiredReplicas / LastScaleTime / Conditions
	// mirrored from the underlying HPA or ScaledObject. Populated by
	// the ISVC status writer for RawDeployment- and OMENative-managed
	// Components. See ComponentAutoscalerStatus for the full field semantics.
	// +optional
	Autoscaler *ComponentAutoscalerStatus `json:"autoscaler,omitempty"`

	// ScaleTargetRef is the canonical scale target for this Component.
	// For OMENative-managed Components this points at the
	// InferenceReplica's /scale subresource; for RawDeployment-managed
	// Components it points at the underlying Deployment. Published so
	// external scalers (when Autoscaler.ManagedBy == "external") know
	// the GroupKind they should target. Populated whenever the
	// Component has a defined scale target — independent of whether an
	// OME-managed HPA / SO is active.
	// +optional
	ScaleTargetRef *ScaleTargetRef `json:"scaleTargetRef,omitempty"`
}

// ComponentTrafficTarget describes the percentage of traffic routed to
// one revision of one Component. RevisionName is the per-revision
// Service name produced by OMENative's coordination layer (e.g.
// `llama-engine-rev-abc123`), which the HTTPRoute builder
// consumes directly as a backend reference.
type ComponentTrafficTarget struct {
	// RevisionName is the per-revision Service name for this target.
	RevisionName string `json:"revisionName"`

	// Percent is the percentage of traffic the consumer should route
	// to RevisionName. Sum across all entries for one Component is
	// 100.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	Percent int32 `json:"percent"`

	// Tag is an optional short identifier for the target (e.g.
	// "latest", "prev"). Cosmetic; not used by the consumer.
	// +optional
	Tag string `json:"tag,omitempty"`

	// LatestRevision is true when this entry corresponds to the
	// LatestRolledoutRevision for the Component.
	// +optional
	LatestRevision bool `json:"latestRevision,omitempty"`

	// PairingProtocol is the engine↔decoder wire-compatibility token the
	// target's revision was minted under (spec.rollout.pairingProtocol at
	// mint time). Consumers MUST only pair engine and decoder targets whose
	// PairingProtocol values are equal; an empty value pairs with anything.
	// +optional
	PairingProtocol string `json:"pairingProtocol,omitempty"`
}

// AcceleratorSelection shows what accelerator was selected and why
type AcceleratorSelection struct {
	// AcceleratorClass that was selected
	AcceleratorClass string `json:"acceleratorClass"`

	// Reason explains why this accelerator was selected
	// +optional
	Reason string `json:"reason,omitempty"`

	// NodeSelector that was applied to pods
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// ResourceRequests that were applied to pods
	// +optional
	ResourceRequests map[string]string `json:"resourceRequests,omitempty"`
}

// ComponentType contains the different types of components of the service
type ComponentType string

// ComponentType Enum
const (
	RouterComponent  ComponentType = "router"
	EngineComponent  ComponentType = "engine"
	DecoderComponent ComponentType = "decoder"
)

// ConditionType represents a Service condition value
const (
	// EngineReady is set when engine pods are ready.
	EngineReady apis.ConditionType = "EngineReady"
	// DecoderReady is set when decoder pods are ready.
	DecoderReady apis.ConditionType = "DecoderReady"
	// RouterReady is set when router has reported readiness.
	RouterReady apis.ConditionType = "RouterReady"
	// IngressReady is set when Ingress is created
	IngressReady apis.ConditionType = "IngressReady"
	// OverlaysReady is True iff every declared overlay was attached.
	// False with a reason when one or more were skipped; informational —
	// the primary still drives the deployment.
	OverlaysReady apis.ConditionType = "OverlaysReady"
	// RuntimeReady is False with reason=RuntimeNotFound when the ISVC's
	// spec.runtime (or the auto-selected runtime) cannot be resolved.
	// Advisory only — NOT a dependent of the aggregate Ready condition
	// (conditionSet), so a currently-serving ISVC whose spec is edited to
	// point at a missing runtime stays Ready=True on its running pods; only
	// the new spec is withheld until the runtime exists.
	RuntimeReady apis.ConditionType = "RuntimeReady"
)

type ModelStatus struct {
	// Whether the available predictor endpoints reflect the current Spec or is in transition.
	// Empty when the InferenceService is "lean" (no spec.model is declared) and
	// no model-loading lifecycle applies; the controller leaves this field
	// unset in that case. Populated as "InProgress" → "UpToDate" (or
	// "BlockedByFailedLoad"/"InvalidSpec") by the controller once a model
	// is referenced.
	// +optional
	TransitionStatus TransitionStatus `json:"transitionStatus,omitempty"`

	// State information of the predictor's model.
	// +optional
	ModelRevisionStates *ModelRevisionStates `json:"modelRevisionStates,omitempty"`

	// Details of last failure, when load of target model is failed or blocked.
	// +optional
	LastFailureInfo *FailureInfo `json:"lastFailureInfo,omitempty"`

	// Model copy information of the predictor's model.
	// +optional
	ModelCopies *ModelCopies `json:"modelCopies,omitempty"`
}

type ModelRevisionStates struct {
	// High level state string: Pending, Standby, Loading, Loaded, FailedToLoad
	// +kubebuilder:default=Pending
	ActiveModelState ModelState `json:"activeModelState"`
	// +kubebuilder:default=""
	TargetModelState ModelState `json:"targetModelState,omitempty"`
}

type ModelCopies struct {
	// How many copies of this predictor's models failed to load recently
	// +kubebuilder:default=0
	FailedCopies int `json:"failedCopies"`
	// Total number copies of this predictor's models that are currently loaded
	// +optional
	TotalCopies int `json:"totalCopies,omitempty"`
}

// TransitionStatus enum
// +kubebuilder:validation:Enum="";UpToDate;InProgress;BlockedByFailedLoad;InvalidSpec
type TransitionStatus string

// TransitionStatus Enum values
const (
	// Predictor is up-to-date (reflects current spec)
	UpToDate TransitionStatus = "UpToDate"
	// Waiting for target model to reach state of active model
	InProgress TransitionStatus = "InProgress"
	// Target model failed to load
	BlockedByFailedLoad TransitionStatus = "BlockedByFailedLoad"
	// Target predictor spec failed validation
	InvalidSpec TransitionStatus = "InvalidSpec"
)

// ModelState enum
// +kubebuilder:validation:Enum="";Pending;Standby;Loading;Loaded;FailedToLoad
type ModelState string

// ModelState Enum values
const (
	// Model is not yet registered
	Pending ModelState = "Pending"
	// Model is available but not loaded (will load when used)
	Standby ModelState = "Standby"
	// Model is loading
	Loading ModelState = "Loading"
	// At least one copy of the model is loaded
	Loaded ModelState = "Loaded"
	// All copies of the model failed to load
	FailedToLoad ModelState = "FailedToLoad"
)

// FailureReason enum
// +kubebuilder:validation:Enum=BaseModelNotReady;BaseModelNotFound;ModelLoadFailed;RuntimeUnhealthy;RuntimeDisabled;NoSupportingRuntime;RuntimeNotRecognized
type FailureReason string

// FailureReason enum values
const (
	// ModelLoadFailed The model failed to load within a ServingRuntime container
	ModelLoadFailed FailureReason = "ModelLoadFailed"
	// RuntimeUnhealthy Corresponding ServingRuntime containers failed to start or are unhealthy
	RuntimeUnhealthy FailureReason = "RuntimeUnhealthy"
	// RuntimeDisabled The ServingRuntime is disabled
	RuntimeDisabled FailureReason = "RuntimeDisabled"
	// NoSupportingRuntime There are no ServingRuntime which support the specified model type
	NoSupportingRuntime FailureReason = "NoSupportingRuntime"
	// RuntimeNotRecognized There is no ServingRuntime defined with the specified runtime name
	RuntimeNotRecognized FailureReason = "RuntimeNotRecognized"
	// BaseModelNotFound base model is not found either from the cluster level or from the specified namespace
	BaseModelNotFound FailureReason = "BaseModelNotFound"
	// BaseModelNotReady base model is not ready
	BaseModelNotReady FailureReason = "BaseModelNotReady"
	// FineTunedWeightsNotFound not found
	FineTunedWeightsNotFound FailureReason = "FineTunedWeightsNotFound"
	// BaseModelDisabled base model is disabled
	BaseModelDisabled FailureReason = "BaseModelDisabled"
	// FineTunedWeightsDisabled the fine-tuned weights are disabled
	FineTunedWeightsDisabled FailureReason = "FineTunedWeightsDisabled"
	// BaseModelDeprecated base model is deprecated
	BaseModelDeprecated FailureReason = "BaseModelDeprecated"
	// FineTunedWeightsDeprecated the fine-tuned weights are deprecated
	FineTunedWeightsDeprecated FailureReason = "FineTunedWeightsDeprecated"
	// FineTuneWeightLoadFailed fine-tuned weights load failed
	FineTuneWeightLoadFailed FailureReason = "FineTuneWeightLoadFailed"

	// RouterInvalidSpec router has invalid spec
	InvalidRouterSpec FailureReason = "InvalidRouterSpec"
)

type FailureInfo struct {
	// Name of component to which the failure relates (usually Pod name)
	//+optional
	Location string `json:"location,omitempty"`
	// High level class of failure
	//+optional
	Reason FailureReason `json:"reason,omitempty"`
	// Detailed error message
	//+optional
	Message string `json:"message,omitempty"`
	// Internal Revision/ID of model, tied to specific Spec contents
	//+optional
	ModelRevisionName string `json:"modelRevisionName,omitempty"`
	// Time failure occurred or was discovered
	//+optional
	Time *metav1.Time `json:"time,omitempty"`
	// Exit status from the last termination of the container
	//+optional
	ExitCode int32 `json:"exitCode,omitempty"`
}

// InferenceService component conditions
// The overall Ready condition is managed by the conditionSet which only requires IngressReady
// Component-specific ready conditions (EngineReady, DecoderReady) are managed separately
var conditionSet = apis.NewLivingConditionSet(
	IngressReady,
	EngineReady,
)

var _ apis.ConditionsAccessor = (*InferenceServiceStatus)(nil)

// IsReady returns the overall readiness for the inference service.
func (ss *InferenceServiceStatus) IsReady() bool {
	return conditionSet.Manage(ss).IsHappy()
}

// GetCondition returns the condition by name.
func (ss *InferenceServiceStatus) GetCondition(t apis.ConditionType) *apis.Condition {
	return conditionSet.Manage(ss).GetCondition(t)
}

// IsConditionReady returns the readiness for a given condition
func (ss *InferenceServiceStatus) IsConditionReady(t apis.ConditionType) bool {
	condition := conditionSet.Manage(ss).GetCondition(t)
	return condition != nil && condition.Status == v1.ConditionTrue
}

// IsConditionUnknown returns if a given condition is Unknown
func (ss *InferenceServiceStatus) IsConditionUnknown(t apis.ConditionType) bool {
	condition := conditionSet.Manage(ss).GetCondition(t)
	return condition == nil || condition.Status == v1.ConditionUnknown
}

// SetCondition sets a condition on the status using the conditionSet
func (ss *InferenceServiceStatus) SetCondition(conditionType apis.ConditionType, condition *apis.Condition) {
	switch {
	case condition == nil:
	case condition.Status == v1.ConditionUnknown:
		conditionSet.Manage(ss).MarkUnknown(conditionType, condition.Reason, condition.Message)
	case condition.Status == v1.ConditionTrue:
		// If reason or message are provided, we need to set them directly since MarkTrue doesn't support them
		if condition.Reason != "" || condition.Message != "" {
			// Get the condition manager to access the underlying conditions
			manager := conditionSet.Manage(ss)
			// First mark it true to set the basic state
			manager.MarkTrue(conditionType)
			// Then directly set the reason and message by finding and updating the condition
			if ss.Status.Conditions != nil {
				for i := range ss.Status.Conditions {
					if ss.Status.Conditions[i].Type == conditionType {
						ss.Status.Conditions[i].Reason = condition.Reason
						ss.Status.Conditions[i].Message = condition.Message
						break
					}
				}
			}
		} else {
			conditionSet.Manage(ss).MarkTrue(conditionType)
		}
	case condition.Status == v1.ConditionFalse:
		conditionSet.Manage(ss).MarkFalse(conditionType, condition.Reason, condition.Message)
	}
}

// PlacementPhase is the coarse state of an InferenceService's multi-cluster placement.
// +kubebuilder:validation:Enum=Pending;Racing;Placed;Failed
type PlacementPhase string

const (
	// PlacementPhasePending: no candidate cluster matched, or none connected yet.
	PlacementPhasePending PlacementPhase = "Pending"
	// PlacementPhaseRacing: fanned out to candidates; no cluster has admitted yet.
	PlacementPhaseRacing PlacementPhase = "Racing"
	// PlacementPhasePlaced: a candidate was admitted and won the race.
	PlacementPhasePlaced PlacementPhase = "Placed"
	// PlacementPhaseFailed: the winning placement failed terminally.
	PlacementPhaseFailed PlacementPhase = "Failed"
)

// CandidatePlacementPhase is the per-cluster state of a fan-out candidate.
// +kubebuilder:validation:Enum=Placed;Admitted
type CandidatePlacementPhase string

const (
	// CandidatePhasePlaced: derived ISVC created on this candidate; racing.
	CandidatePhasePlaced CandidatePlacementPhase = "Placed"
	// CandidatePhaseAdmitted: this candidate's Kueue admitted the pods (won).
	CandidatePhaseAdmitted CandidatePlacementPhase = "Admitted"
)

// PlacementStatus is the multi-cluster placement status: which workload
// cluster an InferenceService is placed on and a coarse phase.
type PlacementStatus struct {
	// Cluster is the WorkloadCluster the ISVC is currently placed on. Empty
	// while pending (no candidate yet, or transport not connected).
	// +optional
	Cluster string `json:"cluster,omitempty"`

	// Phase is a coarse placement state.
	// +optional
	Phase PlacementPhase `json:"phase,omitempty"`

	// Endpoint is the externally-addressable URL of the winning cluster's
	// placement, mirrored from the derived InferenceService's status.url once it
	// is admitted AND addressable. Empty while pending/racing/failed or before
	// the winner reports a URL. An external global LB/DNS consumes this to route
	// traffic to the winning cluster.
	// +optional
	Endpoint *apis.URL `json:"endpoint,omitempty"`

	// Candidates are the clusters this ISVC has been fanned out to during the
	// placement race. Populated by the control plane.
	// +optional
	// +listType=map
	// +listMapKey=cluster
	Candidates []CandidatePlacement `json:"candidates,omitempty"`
}

// CandidatePlacement is the per-cluster state of a fan-out candidate in the
// placement race.
type CandidatePlacement struct {
	// Cluster is the WorkloadCluster name.
	Cluster string `json:"cluster"`
	// Phase is the candidate's state.
	// +optional
	Phase CandidatePlacementPhase `json:"phase,omitempty"`

	// Endpoint is this candidate's own externally-addressable URL, mirrored from
	// its derived InferenceService's status.url once admitted AND addressable.
	// In Single mode only the winner carries one and the top-level
	// PlacementStatus.Endpoint is authoritative; in All/Split mode every serving
	// home carries its own, and this list is the source of truth an external
	// LB/DNS consumes to route across homes. Empty until the home is addressable.
	// +optional
	Endpoint *apis.URL `json:"endpoint,omitempty"`

	// AdmittedReplicas is how many replicas this home's Kueue has admitted (only
	// meaningful in Split, where a home serves a fraction of the desired count).
	// It drives global accounting — the control plane sums it across homes to
	// decide whether the desired count is met. Zero/unset outside Split.
	// +optional
	AdmittedReplicas int32 `json:"admittedReplicas,omitempty"`

	// ReadyReplicas is how many of this home's replicas are serving traffic. In
	// Split it is the weight an external LB uses to split traffic across homes
	// (traffic follows where the replicas actually landed). Zero/unset outside
	// Split.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// Autoscaling mirrors this home's AutoscalerPolicy state so digest skew
	// between homes is visible from the source InferenceService. Set only when
	// the source references at least one policy.
	// +optional
	Autoscaling *CandidateAutoscalingStatus `json:"autoscaling,omitempty"`

	// Rollout mirrors this home's rollout-run provenance (run identity plus
	// per-group plan digests, never the plan bodies) so plan drift between
	// homes is visible from the source InferenceService. Set only when the
	// source's rollout groups reference a RolloutPolicy.
	// +optional
	Rollout *CandidateRolloutStatus `json:"rollout,omitempty"`
}

// CandidateAutoscalingStatus is the per-home AutoscalerPolicy digest state
// the watch-funnel lifts off a derived InferenceService.
type CandidateAutoscalingStatus struct {
	// Policies reports the portable digest of each policy as observed on this
	// home. Every entry must match the control plane's copy; a mismatch is
	// distribution skew.
	// +optional
	// +listType=map
	// +listMapKey=name
	Policies []CandidatePolicyDigest `json:"policies,omitempty"`

	// Components maps each policy-consuming component to its per-home
	// resolved digest ("did my policy edit land here?").
	// +optional
	Components map[ComponentType]CandidateComponentAutoscaling `json:"components,omitempty"`

	// Ready is true when every policy-consuming component on this home
	// reports specSource=policy with a resolved digest.
	// +optional
	Ready bool `json:"ready,omitempty"`
}

// CandidatePolicyDigest pairs a policy name with the portable digest this
// home observed for it.
type CandidatePolicyDigest struct {
	Name string `json:"name"`
	// +optional
	PortableDigest string `json:"portableDigest,omitempty"`
}

// CandidateComponentAutoscaling is one component's rendered-autoscaler state
// on one home.
type CandidateComponentAutoscaling struct {
	// +optional
	ResolvedDigest string `json:"resolvedDigest,omitempty"`
	// +optional
	Ready bool `json:"ready,omitempty"`
}

// CandidateRolloutStatus is the bounded per-home rollout-run state the watch
// funnel lifts off a derived InferenceService: run identity and per-group
// provenance digests, never the pinned plan bodies (fleet-drift dashboards
// compare digests; plan content lives in the policy's git history).
type CandidateRolloutStatus struct {
	// ActiveRunID is the home's open rollout run, empty when none is open.
	// +optional
	ActiveRunID string `json:"activeRunID,omitempty"`

	// ActiveGroups is the active run's per-group provenance in pinned-plan
	// order: which source each group pinned and under which digest.
	// +optional
	// +listType=atomic
	ActiveGroups []CandidateRolloutGroup `json:"activeGroups,omitempty"`

	// LastRun is the bounded record of the home's most recently closed run.
	// +optional
	LastRun *CandidateRolloutLastRun `json:"lastRun,omitempty"`
}

// CandidateRolloutGroup is one pinned group's provenance as observed on a home.
type CandidateRolloutGroup struct {
	// Source is where the group's progression came from (Inline or Policy).
	Source RolloutPlanSource `json:"source"`

	// PolicyName names the RolloutPolicy when Source is Policy.
	// +optional
	PolicyName string `json:"policyName,omitempty"`

	// PortableDigest is the pinned progression body's portable digest.
	// +optional
	PortableDigest string `json:"portableDigest,omitempty"`
}

// CandidateRolloutLastRun is the bounded outcome of a home's last closed run.
type CandidateRolloutLastRun struct {
	// Outcome is how the run ended.
	Outcome RolloutRunOutcome `json:"outcome"`

	// Digest folds the closed run's per-group portable digests into one
	// order-sensitive comparable value.
	// +optional
	Digest string `json:"digest,omitempty"`
}
