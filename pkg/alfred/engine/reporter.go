package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/metrics"
	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// Candidate outcomes as recorded in the alfred-recommendations ConfigMap.
const (
	OutcomeAdvisory = "advisory"
	// OutcomeAdmitted is reserved for a future phase in which an actual
	// Dispatcher confirms submission. This phase never records it.
	OutcomeAdmitted = "admitted"
	// OutcomeWithheld is an Arbiter-admitted candidate that was not sent to a
	// Dispatcher. This phase has no Dispatcher, regardless of configured mode.
	OutcomeWithheld = "withheld"
	OutcomeRejected = "rejected"

	withholdReasonRecommendOnly         = "RecommendOnly"
	withholdReasonDispatcherUnavailable = "DispatcherUnavailable"
	withholdNoteRecommendOnly           = "recommend-only: not dispatched"
	withholdNoteDispatcherUnavailable   = "dispatcher unavailable: not dispatched"
)

// recommendationsKey is the ConfigMap key holding the latest cycle record;
// node remediation records (Policy #2) live beside it under node.<name> keys.
const recommendationsKey = "last-cycle.json"

// Reporter is the engine's single observability emitter: every Event, every
// decision metric, and every alfred-recommendations ConfigMap entry comes
// from here, so emission is tested and deduplicated once, not per policy.
type Reporter struct {
	// Client updates the recommendations ConfigMap. Update-only: the chart
	// pre-creates it, and a missing ConfigMap disables the record with a
	// log line rather than a create (the write contract is narrow).
	Client client.Client
	// DirectReader reads the ConfigMap uncached for the read-modify-write
	// cycle (the manager's API reader): retrying a conflict against the
	// informer cache would just re-read the same stale object. Nil falls
	// back to Client.
	DirectReader client.Reader
	Recorder     record.EventRecorder
	Metrics      *metrics.Metrics
	Log          logr.Logger
	// Namespace is Alfred's own namespace — the recommendations ConfigMap
	// and the engine-level transition events live there.
	Namespace string
	// ConfigMapName is the alfred-config ConfigMap's configured name (the
	// --config-name flag): the anchor object for engine-level events.
	ConfigMapName string

	// Transition dedup: the OMENativeUnavailable event fires on the way
	// into degraded mode, never per pass.
	omenativeSeeded   bool
	omenativeDegraded bool

	// cmMissingLogged dedups the missing-ConfigMap log.
	cmMissingLogged bool

	// nodeSignals dedups node remediation signals across decision loops (a
	// flapping condition must not spam fresh signals): node name → the
	// condition fingerprint last signaled. Policy #2 drives this.
	nodeSignals map[string]string
}

// cycleRecord is the JSON document written to the recommendations ConfigMap.
type cycleRecord struct {
	Timestamp       time.Time            `json:"timestamp"`
	Mode            string               `json:"mode"`
	Recommendations []recommendationView `json:"recommendations"`
}

type recommendationView struct {
	Workload       string   `json:"workload"`
	Component      string   `json:"component"`
	Instance       int32    `json:"instance"`
	Policy         string   `json:"policy"`
	Reason         string   `json:"reason"`
	Outcome        string   `json:"outcome"`
	WithholdReason string   `json:"withholdReason,omitempty"`
	AdvisoryReason string   `json:"advisoryReason,omitempty"`
	RejectReason   string   `json:"rejectReason,omitempty"`
	FromNode       string   `json:"fromNode,omitempty"`
	Target         string   `json:"target,omitempty"`
	HintTargets    []string `json:"hintTargets,omitempty"`
	Score          float64  `json:"score"`
	Emergency      bool     `json:"emergency,omitempty"`
	CooldownOver   bool     `json:"cooldownOverridden,omitempty"`
}

// ReportCycle publishes one decision pass: produced/accepted/rejected
// counters, Events on the target InferenceServices, and (when enabled) the
// recommendations ConfigMap record. decisions covers the executable
// candidates; advisories appear only in candidates.
func (r *Reporter) ReportCycle(ctx context.Context, candidates []policy.Candidate, decisions []Decision, cfg *config.Config, now time.Time) {
	record := cycleRecord{Timestamp: now, Mode: cfg.Mode}

	decided := map[string]Decision{}
	for _, d := range decisions {
		decided[candidateKey(d.Candidate)] = d
	}

	for _, c := range candidates {
		executable := "false"
		if c.Executable {
			executable = "true"
		}
		r.Metrics.RecommendationsProduced.WithLabelValues(
			c.Policy, c.Workload.String(), string(c.Component), c.Reason, executable).Inc()

		view := recommendationView{
			Workload:       c.Workload.String(),
			Component:      string(c.Component),
			Instance:       c.Instance,
			Policy:         c.Policy,
			Reason:         c.Reason,
			AdvisoryReason: c.AdvisoryReason,
			FromNode:       c.FromNode,
			HintTargets:    c.HintTargetNodes,
			Score:          c.Score,
			Emergency:      c.Emergency,
		}

		if !c.Executable {
			view.Outcome = OutcomeAdvisory
			r.reportAdvisory(c)
			record.Recommendations = append(record.Recommendations, view)
			continue
		}

		d, ok := decided[candidateKey(c)]
		if !ok {
			// An executable candidate without a decision (the arbiter
			// never saw it) is a wiring bug worth surfacing loudly.
			r.Log.Error(nil, "executable candidate has no arbitration decision",
				"workload", c.Workload.String(), "component", c.Component)
			continue
		}
		if d.Admitted {
			view.Outcome = OutcomeWithheld
			view.WithholdReason, _ = withholdDetails(cfg.Mode)
			view.Target = d.Target
			view.CooldownOver = d.CooldownOverridden
			r.reportWithheld(c, d, cfg.Mode)
		} else {
			view.Outcome = OutcomeRejected
			view.RejectReason = d.Reason
			r.Metrics.RecommendationsRejected.WithLabelValues(
				c.Policy, c.Workload.String(), string(c.Component), d.Reason).Inc()
			r.event(c, corev1.EventTypeNormal, "RecommendationRejected",
				"%s recommendation for %s/%s rejected: %s",
				c.Policy, c.Workload.String(), c.Component, d.Reason)
		}
		record.Recommendations = append(record.Recommendations, view)
	}

	if *cfg.RecommendationsConfigMapEnabled {
		r.writeRecord(ctx, cfg.RecommendationsConfigMapName, record)
	}
}

func (r *Reporter) reportAdvisory(c policy.Candidate) {
	r.event(c, corev1.EventTypeNormal, producedEventReason(c),
		"%s advisory for %s/%s: %s (from %s, footprint %d GPUs)",
		c.Policy, c.Workload.String(), c.Component, c.AdvisoryReason, c.FromNode, c.FootprintGPUs)
	if c.AdvisoryReason == policy.AdvisoryLWSMigrationUnsupported {
		r.Metrics.LWSRecommendations.WithLabelValues(c.Workload.String(), "MigrateToOMENative").Inc()
	}
}

func (r *Reporter) reportWithheld(c policy.Candidate, d Decision, mode string) {
	r.Metrics.RecommendationsAccepted.WithLabelValues(
		c.Policy, c.Workload.String(), string(c.Component)).Inc()

	_, note := withholdDetails(mode)
	target := d.Target
	if target == "" {
		target = "scheduler's choice"
	}
	r.event(c, corev1.EventTypeNormal, "RecommendationWithheld",
		"%s recommends migrating %s/%s instance %d off %s (target %s, score %.3f) — %s",
		c.Policy, c.Workload.String(), c.Component, c.Instance, c.FromNode, target, c.Score, note)

	if d.CooldownOverridden {
		r.Metrics.CooldownOverrides.WithLabelValues(c.Policy).Inc()
		r.event(c, corev1.EventTypeNormal, "CooldownOverriddenForEvacuation",
			"health evacuation of %s/%s admitted under the cooldown floor while the standard window was still running",
			c.Workload.String(), c.Component)
	}
}

func withholdDetails(mode string) (reason, note string) {
	if mode == config.ModeRecommendOnly {
		return withholdReasonRecommendOnly, withholdNoteRecommendOnly
	}
	return withholdReasonDispatcherUnavailable, withholdNoteDispatcherUnavailable
}

// ReportOMENativeState emits the degraded-mode transition event: once on the
// way in, never per pass (the alfred_omenative_unavailable gauge — published
// by the observation loop every pass — is the per-pass signal; the event
// stream must not become noise).
func (r *Reporter) ReportOMENativeState(available bool) {
	degraded := !available
	enteringDegraded := degraded && (!r.omenativeSeeded || !r.omenativeDegraded)
	if enteringDegraded {
		r.Recorder.Eventf(r.namespaceRef(), corev1.EventTypeWarning, "OMENativeUnavailable",
			"no OMENative executor is available; multi-pod candidates degrade to advisory")
	}
	if r.omenativeSeeded && r.omenativeDegraded && available {
		r.Log.Info("OMENative executor available again; degraded mode cleared")
	}
	r.omenativeSeeded, r.omenativeDegraded = true, degraded
}

// NodeSignal emits a node remediation signal, deduplicated across decision
// loops: a Warning Event on the Node (what remediation controllers watch), a
// node.<name> entry in the recommendations ConfigMap (the durable record
// Alfred owns), and the signal counter. A repeat with the same condition
// fingerprint is suppressed until ClearNodeSignal. Policy #2 drives this.
func (r *Reporter) NodeSignal(ctx context.Context, node, condition string, workloads []string, cfg *config.Config, now time.Time) {
	if r.nodeSignals == nil {
		r.nodeSignals = map[string]string{}
	}
	if r.nodeSignals[node] == condition {
		return
	}
	r.nodeSignals[node] = condition

	r.Metrics.NodeHealthSignals.WithLabelValues(node, condition).Inc()
	r.Recorder.Eventf(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: node}},
		corev1.EventTypeWarning, "GpuUnhealthyEvacuating",
		"node %s is unhealthy (%s); Alfred is evacuating %d workload(s) and signaling for repair",
		node, condition, len(workloads))

	if !*cfg.RecommendationsConfigMapEnabled {
		return
	}
	entry, err := json.Marshal(map[string]interface{}{
		"condition": condition,
		"workloads": workloads,
		"signaled":  now,
	})
	if err != nil {
		return
	}
	writeErr := r.updateConfigMap(ctx, cfg.RecommendationsConfigMapName, func(cm *corev1.ConfigMap) {
		cm.Data["node."+node] = string(entry)
	})
	if writeErr != nil && !apierrors.IsNotFound(writeErr) {
		// The durable record is missing while the fingerprint says
		// "signaled" — drop it so the next pass retries the write. A
		// NotFound keeps the dedup: the record surface is absent by
		// operator choice, and the event and metric already fired.
		delete(r.nodeSignals, node)
	}
}

// ClearNodeSignal forgets a node's dedup record so a fresh incident signals
// again after the condition genuinely cleared.
func (r *Reporter) ClearNodeSignal(node string) {
	delete(r.nodeSignals, node)
}

func (r *Reporter) writeRecord(ctx context.Context, name string, rec cycleRecord) {
	raw, err := json.Marshal(rec)
	if err != nil {
		r.Log.Error(err, "marshal recommendations record")
		return
	}
	// A failed cycle record needs no unwinding: the next pass rewrites it.
	_ = r.updateConfigMap(ctx, name, func(cm *corev1.ConfigMap) {
		cm.Data[recommendationsKey] = string(raw)
	})
}

// updateConfigMap applies a mutation to the recommendations ConfigMap,
// update-only: other keys (node records) are preserved, and a missing
// ConfigMap logs once instead of being created. The read-modify-write cycle
// retries on conflict with an uncached read — a lost node-signal write would
// otherwise be deduped away and never rewritten. The error is returned
// (already logged) so callers holding dedup state can undo it on failure.
func (r *Reporter) updateConfigMap(ctx context.Context, name string, mutate func(*corev1.ConfigMap)) error {
	key := types.NamespacedName{Namespace: r.Namespace, Name: name}
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var cm corev1.ConfigMap
		if err := r.reader().Get(ctx, key, &cm); err != nil {
			return err
		}
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		mutate(&cm)
		return r.Client.Update(ctx, &cm)
	})
	switch {
	case err == nil:
		r.cmMissingLogged = false
	case apierrors.IsNotFound(err):
		if !r.cmMissingLogged {
			r.cmMissingLogged = true
			r.Log.Info("recommendations ConfigMap missing; record disabled (the chart pre-creates it)",
				"configmap", key.String())
		}
	default:
		r.Log.Error(err, "update recommendations ConfigMap", "configmap", key.String())
	}
	return err
}

func (r *Reporter) reader() client.Reader {
	if r.DirectReader != nil {
		return r.DirectReader
	}
	return r.Client
}

// event emits a candidate-scoped Event on its InferenceService. A minimal
// object reference is enough for the recorder, and candidates without a
// workload (node-scoped signals) skip the ISVC event.
func (r *Reporter) event(c policy.Candidate, eventType, reason, format string, args ...interface{}) {
	if c.Workload.Name == "" {
		return
	}
	ref := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Namespace: c.Workload.Namespace,
		Name:      c.Workload.Name,
	}}
	r.Recorder.Eventf(ref, eventType, reason, format, args...)
}

func (r *Reporter) namespaceRef() *corev1.ConfigMap {
	name := r.ConfigMapName
	if name == "" {
		name = "alfred-config"
	}
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Namespace: r.Namespace,
		Name:      name,
	}}
}

func producedEventReason(c policy.Candidate) string {
	if c.AdvisoryReason == policy.AdvisoryRawDeploymentMigrationUnsupported {
		return policy.AdvisoryRawDeploymentMigrationUnsupported
	}
	switch c.Reason {
	case policy.ReasonNodeUnhealthy:
		return "EvacuationRecommendationProduced"
	case policy.ReasonRemediationSignal:
		return "RemediationSignalProduced"
	default:
		return "FragmentationRecommendationProduced"
	}
}

// candidateKey identifies one candidate within a cycle. The policy is part
// of the key: node-health evacuation and defragmentation can both target the
// same instance in one pass, and their decisions must not overwrite each
// other in the report.
func candidateKey(c policy.Candidate) string {
	return fmt.Sprintf("%s|%s|%s|%d", c.Policy, c.Workload.String(), c.Component, c.Instance)
}
