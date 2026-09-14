package engine

import (
	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/scheduling"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
	"sigs.k8s.io/ome/pkg/constants"
)

const schedulingSimulationUnavailable = "SimulationUnavailable"

// gateSchedulingCandidate runs before arbitration, independently of policy
// eligibility. A selected profile never authorizes a GPU-heuristic move:
// authoritative replacement rendering and a simulation worker are not wired.
// Candidate is copied by value, and its diagnostic is newly allocated so
// policies can retain their output without sharing mutable engine state.
func gateSchedulingCandidate(snap *snapshot.ClusterSnapshot, cfg *config.Config, c policy.Candidate) policy.Candidate {
	if c.Mode != constants.OMENative || (c.Instance < 0 && !c.Executable) {
		return c
	}
	diagnostic := schedulingDiagnostic(snap, cfg, c)
	c.Scheduling = &diagnostic
	if c.Executable {
		c.Executable = false
		c.AdvisoryReason = diagnostic.Reason
	}
	return c
}

func schedulingDiagnostic(snap *snapshot.ClusterSnapshot, cfg *config.Config, c policy.Candidate) policy.SchedulingDiagnostics {
	invalid := policy.SchedulingDiagnostics{
		Status: string(scheduling.SelectionUnavailable),
		Reason: policy.AdvisoryOMENativeObservationInvalid,
	}
	w := snap.Workloads[c.Workload]
	if w == nil {
		return invalid
	}
	component := w.Components[c.Component]
	if component == nil || component.DeploymentMode != constants.OMENative ||
		component.IR == nil || !component.StatusFresh || !component.ObservationValid ||
		component.IR.Status.ObservedGeneration != component.IR.Generation {
		return invalid
	}
	var instance *snapshot.Instance
	for _, observed := range component.Instances {
		if observed != nil && observed.Index == c.Instance {
			instance = observed
			break
		}
	}
	if instance == nil || !instance.ObservationValid {
		return invalid
	}

	// These current IR templates only identify the prospective scheduler.
	// They are not replacement Pods and are never submitted for simulation.
	templates := make([]corev1.PodTemplateSpec, 0, len(component.IR.Spec.Runners))
	var pods int64
	for _, runner := range component.IR.Spec.Runners {
		if runner.Size <= 0 {
			return invalid
		}
		pods += int64(runner.Size)
		templates = append(templates, runner.Template)
	}
	requireGang := pods > 1 || instance.ObservedPods > 1 || len(instance.Pods) > 1
	selected := scheduling.Select(cfg.Scheduling, templates, requireGang)
	diagnostic := policy.SchedulingDiagnostics{
		SchedulerName:    selected.SchedulerName,
		Backend:          selected.Profile.Backend,
		SchedulerVersion: selected.Profile.SchedulerVersion,
		ConfigurationID:  selected.Profile.ConfigurationID,
		Status:           string(selected.Status),
		Reason:           string(selected.Reason),
	}
	if selected.Status == scheduling.SelectionReady {
		diagnostic.Status = string(scheduling.SelectionUnavailable)
		diagnostic.Reason = schedulingSimulationUnavailable
	}
	return diagnostic
}
