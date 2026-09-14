package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/scheduling"
	"sigs.k8s.io/ome/pkg/alfred/scheduling/input"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
	v1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

type dispatchEvidence struct {
	candidate     policy.Candidate
	owner         *v1beta1.InferenceService
	ir            *v1beta1.InferenceReplica
	fingerprint   string
	targets       []string
	historyWindow time.Duration
}

func (d *Dispatcher) freshObservation(ctx context.Context, cfg *config.Config) (*snapshot.ClusterSnapshot, error) {
	return snapshot.Build(ctx, d.Reader, snapshot.Options{Now: d.now, DefaultMovable: cfg.DefaultMovable,
		TriggerConditions: cfg.Policies.NodeHealth.TriggerConditions, PreemptibleLabels: cfg.SpotPolicy.PreemptibleLabels,
		NodeSuspicionWindow: cfg.NodeSuspicionWindow(), MaintenanceTriggers: cfg.Policies.NodeHealth.Maintenance.Triggers,
		OMENativeExecutor: snapshot.OMENativeExecutorState{Available: true, WireVersion: "v1", Reason: "OperatorConfigured"}})
}

// Fresh policy replay is an additional execution authorization. Merely having
// a feasible scheduling diagnostic never changes an advisory into an action.
func (d *Dispatcher) freshCandidate(s *snapshot.ClusterSnapshot, c policy.Candidate, cfg *config.Config) (policy.Candidate, bool) {
	for _, p := range d.Policies {
		for _, fresh := range p.Evaluate(s, cfg) {
			if fresh.Executable && fresh.Mode == constants.OMENative && sameDispatchSource(fresh, c) && sameDispatchCause(fresh, c) {
				return fresh, true
			}
		}
	}
	return policy.Candidate{}, false
}

func (d *Dispatcher) preflight(ctx context.Context, observed *snapshot.ClusterSnapshot, c policy.Candidate, cfg *config.Config, arbiter *Arbiter, j *dispatchJournal, existing *dispatchEntry) (dispatchEvidence, string) {
	var empty dispatchEvidence
	fresh, err := d.freshObservation(ctx, cfg)
	if err != nil {
		return empty, "ObservationUnavailable"
	}
	current, ok := d.freshCandidate(fresh, c, cfg)
	if !ok {
		return empty, "PolicyNoLongerEligible"
	}
	decisions := arbiter.Admit(fresh, []policy.Candidate{current}, cfg, d.now())
	if len(decisions) != 1 || !decisions[0].Admitted {
		if len(decisions) == 1 {
			return empty, decisions[0].Reason
		}
		return empty, "NotAdmitted"
	}
	captured, err := input.Capture(ctx, d.Reader, d.now)
	if err != nil {
		return empty, "SnapshotUnavailable"
	}
	if reason := dispatchBudget(captured, j, current, cfg, d.now(), existing); reason != "" {
		return empty, reason
	}
	requestID := "preflight"
	if existing != nil {
		requestID = existing.UUID
	}
	hints := current.HintTargetNodes
	if existing != nil {
		req, err := requestForEntry(*existing)
		if err != nil {
			return empty, "JournalPayloadInvalid"
		}
		hints = req.HintTargetNodes
	}
	source := input.Source{Namespace: current.Workload.Namespace, InferenceService: current.Workload.Name, Component: current.Component, Instance: current.Instance, FromNode: current.FromNode}
	request, err := input.BuildExecutionRequest(captured, source, cfg.Scheduling, requestID, hints, d.now(), predictionMaxAge)
	if err != nil {
		return empty, "SourceUnsupported"
	}
	if !predictionOwnersMatch(fresh, captured, current) || !predictionMembersMatch(fresh, current, request.SourcePods) {
		return empty, "SourceChanged"
	}
	if observed != nil && (!predictionOwnersMatch(observed, captured, current) || !predictionMembersMatch(observed, current, request.SourcePods)) {
		return empty, "SourceChanged"
	}
	owner := fresh.Workloads[current.Workload].ISVC
	ir := fresh.Workloads[current.Workload].Components[current.Component].IR
	fingerprint := dispatchSourceFingerprint(owner, ir, request.SourcePods, current.Instance)
	if existing != nil && (fingerprint != existing.SourceFingerprint || owner.UID != existing.WorkloadUID || ir.UID != existing.IRUID || ir.Name != existing.IRName) {
		return empty, "SourceChanged"
	}
	result, err := d.Simulator.Evaluate(ctx, request)
	if err != nil || scheduling.ValidateResult(request, result) != nil || result.Decision != scheduling.DecisionFeasible {
		return empty, "SimulationNotFeasible"
	}
	if !observationFresh(fresh, d.now()) || captured.Validate(d.now(), predictionMaxAge) != nil {
		return empty, "SimulationStale"
	}
	// Re-read after simulation: source identity and scheduling objects must still
	// agree. Lists are not a transaction; the final owner CAS fences only the
	// submission, not future consumer acceptance (v1 has no incarnation fence).
	final, err := d.freshObservation(ctx, cfg)
	if err != nil {
		return empty, "ObservationUnavailable"
	}
	finalCandidate, ok := d.freshCandidate(final, current, cfg)
	if !ok {
		return empty, "PolicyNoLongerEligible"
	}
	finalCapture, err := input.Capture(ctx, d.Reader, d.now)
	if err != nil {
		return empty, "SnapshotUnavailable"
	}
	finalRequest, err := input.BuildExecutionRequest(finalCapture, source, cfg.Scheduling, requestID, hints, d.now(), predictionMaxAge)
	if err != nil {
		return empty, "SourceChanged"
	}
	if !predictionOwnersMatch(final, finalCapture, current) || !predictionMembersMatch(final, current, finalRequest.SourcePods) {
		return empty, "SourceChanged"
	}
	finalOwner := final.Workloads[current.Workload].ISVC
	finalIR := final.Workloads[current.Workload].Components[current.Component].IR
	if dispatchSourceFingerprint(finalOwner, finalIR, finalRequest.SourcePods, current.Instance) != fingerprint {
		return empty, "SourceChanged"
	}
	if !reflect.DeepEqual(captured.Objects, finalCapture.Objects) {
		return empty, "SchedulingStateChanged"
	}
	if reason := dispatchBudget(finalCapture, j, finalCandidate, cfg, d.now(), existing); reason != "" {
		return empty, reason
	}
	decisions = arbiter.Admit(final, []policy.Candidate{finalCandidate}, cfg, d.now())
	if len(decisions) != 1 || !decisions[0].Admitted {
		return empty, "SafetyStateChanged"
	}
	allowed := map[string]bool{}
	for _, node := range finalCandidate.PlacementTargetNodes {
		allowed[node] = true
	}
	if len(allowed) == 0 {
		for _, node := range finalCandidate.HintTargetNodes {
			allowed[node] = true
		}
	}
	targets := map[string]bool{}
	for _, placement := range result.Placements {
		node := final.Nodes[placement.NodeName]
		if !allowed[placement.NodeName] || node.UnavailableAsTarget() || !capturedNodeReady(finalCapture, placement.NodeName) {
			return empty, "PredictedTargetUnsafe"
		}
		if journalNodeCooling(j, placement.NodeName, cfg.PerNodeCooldown(), d.now(), existing) {
			return empty, "TargetCooldown"
		}
		targets[placement.NodeName] = true
	}
	if err := d.executionEnabled(ctx, cfg); err != nil {
		return empty, err.Error()
	}
	names := make([]string, 0, len(targets))
	for name := range targets {
		names = append(names, name)
	}
	sort.Strings(names)
	historyWindow, valid := dispatchHistoryWindow(finalCapture, cfg)
	if !valid {
		return empty, "InvalidCooldown"
	}
	current.HintTargetNodes = append([]string(nil), hints...)
	return dispatchEvidence{candidate: current, owner: finalOwner, ir: finalIR, fingerprint: fingerprint, targets: names,
		historyWindow: historyWindow}, ""
}

// Retain scheduling identity/spec and semantic incarnation, but not resource
// versions or changing readiness timestamps, in the durable retry fence.
func dispatchSourceFingerprint(owner *v1beta1.InferenceService, ir *v1beta1.InferenceReplica, pods []corev1.Pod, instance int32) string {
	normalized := make([]corev1.Pod, len(pods))
	for i := range pods {
		normalized[i] = *pods[i].DeepCopy()
		normalized[i].ResourceVersion = ""
		normalized[i].ManagedFields = nil
		normalized[i].Status = corev1.PodStatus{}
	}
	sort.Slice(normalized, func(i, j int) bool { return normalized[i].Name < normalized[j].Name })
	var row *v1beta1.OMENativeInstanceStatus
	for i := range ir.Status.InstanceStatuses {
		if ir.Status.InstanceStatuses[i].Index == instance {
			row = &ir.Status.InstanceStatuses[i]
			break
		}
	}
	raw, _ := json.Marshal(struct {
		OwnerUID        string
		OwnerGeneration int64
		IRUID           string
		IRGeneration    int64
		Revision        string
		Row             *v1beta1.OMENativeInstanceStatus
		Pods            []corev1.Pod
	}{string(owner.UID), owner.Generation, string(ir.UID), ir.Generation, ir.Status.CurrentRevision, row, normalized})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func capturedNodeReady(s *input.Snapshot, name string) bool {
	for _, object := range s.Objects {
		var node corev1.Node
		if json.Unmarshal(object.Raw, &node) != nil || node.Kind != "Node" || node.APIVersion != "v1" || node.Name != name {
			continue
		}
		for _, condition := range node.Status.Conditions {
			if condition.Type == corev1.NodeReady {
				return condition.Status == corev1.ConditionTrue
			}
		}
	}
	return false
}
