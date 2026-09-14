package engine

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/scheduling"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
	"sigs.k8s.io/ome/pkg/constants"
)

// DispatchOptions is immutable startup opt-in, independent of hot policy mode.
// APIVersion asserts operator-verified compatibility, not executor liveness.
type DispatchOptions struct {
	APIVersion             string
	AcknowledgementTimeout time.Duration
	FailureBackoff         time.Duration
}

// Dispatcher submits only public migration-v1 requests. One serial leader owns
// Execute; the persisted write-ahead journal carries uncertainty across leaders.
type Dispatcher struct {
	Reader    client.Reader
	Client    client.Client
	Simulator scheduling.Simulator
	Guard     func(context.Context) error
	Namespace string
	Options   DispatchOptions
	Now       func() time.Time
	Policies  []policy.Policy
	Store     *config.Store
}

func (d *Dispatcher) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

// Execute preserves advisory candidates, reconciles retained UUIDs, then may
// submit at most one freshly authorized complete-instance request. Admission is
// not submission, and acknowledgement timeouts never imply cancellation.
func (d *Dispatcher) Execute(ctx context.Context, observed *snapshot.ClusterSnapshot, candidates []policy.Candidate, cfg *config.Config, arbiter *Arbiter) ([]policy.Candidate, []Decision) {
	out := append([]policy.Candidate(nil), candidates...)
	decisions := arbiter.Admit(observed, candidates, cfg, d.now())
	for i := range decisions {
		if decisions[i].Admitted {
			decisions[i].DispatchStatus = "withheld"
			decisions[i].DispatchReason = "ExecutionDisabled"
		}
	}
	if d.Reader == nil || d.Client == nil || d.Namespace == "" {
		return out, decisions
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cm, j, err := loadDispatchJournal(ctx, d.Reader, d.Namespace)
	if err != nil {
		return out, withholdDecisions(decisions, "JournalUnavailable")
	}
	before := *j
	before.Entries = make([]dispatchEntry, len(j.Entries))
	copy(before.Entries, j.Entries)
	reports := d.reconcileDispatches(ctx, j)
	if !reflect.DeepEqual(before, *j) {
		if err := saveDispatchJournal(ctx, d.Client, cm, j); err != nil {
			return out, withholdDecisions(decisions, "JournalUnavailable")
		}
		// A successful CAS also refreshes resourceVersion for another write below.
		cm, j, err = loadDispatchJournal(ctx, d.Reader, d.Namespace)
		if err != nil {
			return out, withholdDecisions(decisions, "JournalUnavailable")
		}
		for _, e := range reports {
			if e.terminal() && arbiter.Ledger != nil {
				arbiter.Ledger.RecordOutcome(e.Phase == dispatchCompleted, d.now())
			}
		}
	}
	// Pending/transition rows remain visible even after policy stops emitting.
	for _, e := range reports {
		out, decisions = reportDispatchEntry(out, decisions, e)
	}
	pending := -1
	for i, e := range j.Entries {
		if !e.terminal() {
			if pending >= 0 {
				return out, withholdDecisions(decisions, "MultipleUnresolvedRequests")
			}
			pending = i
		}
	}
	if pending >= 0 {
		e := j.Entries[pending]
		out, decisions = reportDispatchEntry(out, decisions, e)
		// An applied annotation, acknowledged or stalled request only waits. A
		// prepared uncertain attempt may retry the exact UUID after full preflight.
		if e.Phase != dispatchPrepared {
			return out, withholdDecisions(decisions, "UnresolvedRequest")
		}
		if err := d.executionEnabled(ctx, cfg); err != nil {
			return out, withholdDecisions(decisions, err.Error())
		}
		req, err := requestForEntry(e)
		if err != nil {
			return out, withholdDecisions(decisions, "JournalPayloadInvalid")
		}
		c := entryCandidate(e)
		c.HintTargetNodes = req.HintTargetNodes
		evidence, reason := d.preflight(ctx, nil, c, cfg, arbiter, j, &e)
		if reason != "" {
			return out, withholdDecisions(decisions, reason)
		}
		e = d.attemptDispatch(ctx, cm, j, pending, evidence, cfg)
		return reportDispatchEntry(out, decisions, e)
	}
	if err := d.executionEnabled(ctx, cfg); err != nil {
		return out, withholdDecisions(decisions, err.Error())
	}
	if !observationFresh(observed, d.now()) {
		return out, withholdDecisions(decisions, "ObservationStale")
	}
	if j.BackoffUntil != nil && d.now().Before(*j.BackoffUntil) {
		return out, withholdDecisions(decisions, "FailureBackoff")
	}
	for i := range decisions {
		candidate := decisions[i].Candidate
		if !decisions[i].Admitted || !candidate.Executable || candidate.Mode != constants.OMENative {
			continue
		}
		evidence, reason := d.preflight(ctx, observed, candidate, cfg, arbiter, j, nil)
		if reason != "" {
			decisions[i].DispatchReason = reason
			continue
		}
		e, err := newDispatchEntry(evidence.candidate, evidence.owner.UID, evidence.ir.Name, evidence.ir.UID, evidence.fingerprint, d.now())
		if err != nil {
			decisions[i].DispatchReason = "InvalidRequest"
			continue
		}
		e.Targets = evidence.targets
		pruneDispatchHistory(j, evidence.historyWindow, d.now())
		j.Entries = append(j.Entries, e)
		if err := saveDispatchJournal(ctx, d.Client, cm, j); err != nil {
			decisions[i].DispatchReason = "JournalUnavailable"
			return out, decisions
		}
		cm, j, err = loadDispatchJournal(ctx, d.Reader, d.Namespace)
		if err != nil {
			decisions[i].DispatchReason = "JournalUnavailable"
			return out, decisions
		}
		// A bounded intent is durable before the first potentially applied write.
		e = d.attemptDispatch(ctx, cm, j, len(j.Entries)-1, evidence, cfg)
		out, decisions = reportDispatchEntry(out, decisions, e)
		return out, withholdDecisions(decisions, "SerialDispatchLimit")
	}
	return out, decisions
}

func (d *Dispatcher) executionEnabled(ctx context.Context, cfg *config.Config) error {
	if d.Options.APIVersion != "v1" || cfg.Mode != config.ModeExecute || cfg.OMENativeMigrationEnabled == nil || !*cfg.OMENativeMigrationEnabled {
		return fmt.Errorf("ExecutionDisabled")
	}
	if d.Options.AcknowledgementTimeout <= 0 || d.Options.AcknowledgementTimeout > time.Hour || d.Options.FailureBackoff <= 0 || d.Options.FailureBackoff > time.Hour {
		return fmt.Errorf("InvalidDispatchOptions")
	}
	if !dispatchConfigCooldownsValid(cfg) {
		return fmt.Errorf("InvalidCooldown")
	}
	if d.Store != nil && d.Store.Get() != cfg {
		return fmt.Errorf("ConfigurationChanged")
	}
	if d.Simulator == nil || len(d.Policies) == 0 {
		return fmt.Errorf("SimulationUnavailable")
	}
	if ctx.Err() != nil {
		return fmt.Errorf("DispatchDeadline")
	}
	if d.Guard == nil {
		return fmt.Errorf("GuardUnavailable")
	}
	if err := d.Guard(ctx); err != nil {
		return fmt.Errorf("GuardUnavailable")
	}
	return nil
}

func withholdDecisions(ds []Decision, reason string) []Decision {
	for i := range ds {
		if ds[i].Admitted && (ds[i].DispatchStatus == "" || ds[i].DispatchStatus == "withheld") {
			ds[i].DispatchStatus = "withheld"
			ds[i].DispatchReason = reason
		}
	}
	return ds
}

func entryCandidate(e dispatchEntry) policy.Candidate {
	c := policy.Candidate{Policy: "migration-dispatch", Workload: e.Workload, Component: e.Component, Instance: e.Instance, Mode: constants.OMENative, FromNode: e.FromNode, Executable: true, SurgeShaped: true}
	// The immutable public request already retains the original cause. Recover
	// it without changing the journal schema, so a restarted leader cannot
	// retry maintenance under another policy's health cooldown exemption.
	if req, err := requestForEntry(e); err == nil {
		c.Reason = req.Reason
		c.HintTargetNodes = append([]string(nil), req.HintTargetNodes...)
		switch req.Reason {
		case policy.ReasonNodeUnhealthy, policy.ReasonNodeMaintenance:
			c.Policy = "nodehealth"
		case policy.ReasonFragmentation:
			c.Policy = "defragmentation"
		}
	}
	return c
}

func reportDispatchEntry(cs []policy.Candidate, ds []Decision, e dispatchEntry) ([]policy.Candidate, []Decision) {
	c := entryCandidate(e)
	status := e.Phase
	if status == dispatchPrepared {
		status = "withheld"
	}
	decision := Decision{Candidate: c, Admitted: true, DispatchStatus: status, RequestUUID: e.UUID, DispatchReason: e.Reason}
	for i := range ds {
		if sameDispatchSource(ds[i].Candidate, c) && sameDispatchCause(ds[i].Candidate, c) {
			decision.Candidate = ds[i].Candidate
			ds[i] = decision
			return cs, ds
		}
	}
	for _, existing := range cs {
		if sameDispatchSource(existing, c) && sameDispatchCause(existing, c) {
			decision.Candidate = existing
			return cs, append(ds, decision)
		}
	}
	return append(cs, c), append(ds, decision)
}

func sameDispatchSource(a, b policy.Candidate) bool {
	return a.Workload == b.Workload && a.Component == b.Component && a.Instance == b.Instance && a.FromNode == b.FromNode
}

func sameDispatchCause(a, b policy.Candidate) bool {
	return a.Policy == b.Policy && a.Reason == b.Reason
}
