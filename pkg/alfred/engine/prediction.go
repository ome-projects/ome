package engine

import (
	"context"
	"fmt"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/scheduling"
	"sigs.k8s.io/ome/pkg/alfred/scheduling/input"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
	"sigs.k8s.io/ome/pkg/constants"
)

const (
	predictionCandidates    = 8
	predictionTimeout       = 30 * time.Second
	predictionMaxAge        = 30 * time.Second
	predictionMaxPlacements = 128
)

// PredictionStage annotates policy advice using a lossless API observation and
// an exact-profile simulator. Reader must be manager.GetAPIReader(), not the
// transformed observer cache. A prediction is neither a reservation nor a
// promise of the replacement objects a workload controller will create.
type PredictionStage struct {
	Reader    client.Reader
	Simulator scheduling.Simulator
	Now       func() time.Time
}

// Annotate runs serially in the leader's decision pass. It makes at most one
// capture and bounds the number and duration of evaluations. Input candidates
// and snapshots remain immutable; no successful result enables execution.
func (p *PredictionStage) Annotate(ctx context.Context, observed *snapshot.ClusterSnapshot, cfg *config.Config, candidates []policy.Candidate) []policy.Candidate {
	ctx, cancel := context.WithTimeout(ctx, predictionTimeout)
	defer cancel()
	out := make([]policy.Candidate, len(candidates))
	var captured *input.Snapshot
	var captureErr error
	captureAttempted := false
	attempts := 0
	for i, candidate := range candidates {
		c := gateSchedulingCandidate(observed, cfg, candidate)
		out[i] = c
		if c.Mode != constants.OMENative || c.Instance < 0 || c.Scheduling == nil || c.Scheduling.Reason != schedulingSimulationUnavailable {
			continue
		}
		if p.Simulator == nil || p.Reader == nil {
			continue
		}
		if candidate.Executable {
			out[i].AdvisoryReason = "SimulationRecommendOnly"
		}
		d := out[i].Scheduling // gate allocated a private diagnostic
		if ctx.Err() != nil || attempts >= predictionCandidates {
			d.Reason = "SimulationBudgetExceeded"
			continue
		}
		if !observationFresh(observed, p.now()) {
			d.Reason = "ObservationStale"
			continue
		}
		attempts++
		if !captureAttempted {
			captureAttempted = true
			captured, captureErr = input.Capture(ctx, p.Reader, p.now)
		}
		if ctx.Err() != nil {
			d.Reason = "SimulationBudgetExceeded"
			continue
		}
		if captureErr != nil {
			d.Reason = "CaptureFailed"
			continue
		}
		if captured.Validate(p.now(), predictionMaxAge) != nil {
			d.Reason = "SnapshotStale"
			continue
		}
		// Reject changed owner/revision evidence before building potentially
		// expensive replacement inputs, then fence complete source members.
		if !predictionOwnersMatch(observed, captured, c) {
			d.Reason = "SourceChanged"
			continue
		}
		r, err := input.BuildRequest(captured, input.Source{Namespace: c.Workload.Namespace, InferenceService: c.Workload.Name,
			Component: c.Component, Instance: c.Instance, FromNode: c.FromNode}, cfg.Scheduling,
			fmt.Sprintf("recommendation-%s-%d", captured.ID, i), p.now(), predictionMaxAge)
		if err != nil {
			d.Status = string(scheduling.DecisionUnsupported)
			d.Reason = "InputUnsupported"
			continue
		}
		if !predictionMembersMatch(observed, c, r.SourcePods) {
			d.Reason = "SourceChanged"
			continue
		}
		if len(r.ReplacementPods) > predictionMaxPlacements {
			d.Status = string(scheduling.DecisionUnsupported)
			d.Reason = "RecommendationTooLarge"
			continue
		}
		result, err := p.Simulator.Evaluate(ctx, r)
		if ctx.Err() != nil {
			d.Reason = "SimulationBudgetExceeded"
			continue
		}
		if err != nil {
			d.Reason = "WorkerFailed"
			continue
		}
		if captured.Validate(p.now(), predictionMaxAge) != nil || !observationFresh(observed, p.now()) {
			d.Reason = "SnapshotStale"
			continue
		}
		if scheduling.ValidateResponse(r, result) != nil {
			d.Reason = "InvalidResponse"
			continue
		}
		d.Status, d.Reason = string(result.Decision), string(result.Reason)
		d.SnapshotID = result.SnapshotID
		d.SnapshotTime = result.SnapshotTime.DeepCopy()
		d.Placements = append([]scheduling.Placement(nil), result.Placements...)
	}
	return out
}

func observationFresh(s *snapshot.ClusterSnapshot, now time.Time) bool {
	return s != nil && !s.Timestamp.IsZero() && !s.Timestamp.After(now) && now.Sub(s.Timestamp) <= predictionMaxAge
}

func (p *PredictionStage) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}
