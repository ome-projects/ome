package replay

import (
	"context"
	"fmt"
	"time"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/analysis"
	"sigs.k8s.io/ome/pkg/rollout"
)

// sampleScript is the metric gate's stand-in for the Prometheus sampler:
// the answers a scenario staged with analysis.sample, one per member, each
// dated by the clock when it was staged and bound to the step the unit's
// record was on. The gate reads a member's answer when it is newer than
// the last sample it consumed, as the real sampler serves a result produced
// after the caller's last read; a member with no answer, with one the gate
// consumed already, or with one staged for another step has no fresh
// sample, which is the real sampler's miss. The driver wires the script
// only when the scenario stages analysis.sample somewhere; a scenario that
// never does runs with no sampler at all, as every one did before.
type sampleScript struct {
	samples map[string]scriptedSample
}

// scriptedSample is one member's staged answer.
type scriptedSample struct {
	outcome analysis.Outcome
	step    int32
	at      time.Time
}

func newSampleScript() *sampleScript {
	return &sampleScript{samples: map[string]scriptedSample{}}
}

// Get answers one member's read of the step's checks from the script.
func (s *sampleScript) Get(req canary.SampleRequest, since time.Time) (analysis.Result, time.Time, bool) {
	e, ok := s.samples[req.Key.Component]
	if !ok || e.step != req.Key.Step || !e.at.After(since) {
		return analysis.Result{}, time.Time{}, false
	}
	res := analysis.Result{Outcome: e.outcome}
	if req.Analysis != nil {
		for _, m := range req.Analysis.Metrics {
			mr := analysis.MetricResult{Name: m.Name, Threshold: m.Threshold, Operator: m.Operator, Passed: e.outcome == analysis.Pass}
			if e.outcome == analysis.Inconclusive {
				mr.Message = "no data"
			}
			res.Metrics = append(res.Metrics, mr)
		}
	}
	return res, e.at, true
}

// sampleOutcomes are the answers a scenario may stage, in the gate's words.
var sampleOutcomes = map[string]analysis.Outcome{
	"pass":         analysis.Pass,
	"fail":         analysis.Fail,
	"inconclusive": analysis.Inconclusive,
}

// applyAnalysisSample stages one member's answer for the metric gate's
// next read: the outcome the scenario names, dated now and bound to the
// step the member's unit is on, read off the primary's live record.
func applyAnalysisSample(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	outcome, ok := sampleOutcomes[ev.Args.Outcome]
	if !ok {
		return "", fmt.Errorf("replay: %s needs an outcome of pass, fail or inconclusive, not %q", ev.Key(), ev.Args.Outcome)
	}
	if d.samples == nil {
		return "", fmt.Errorf("replay: %s: no sample script is wired", ev.Key())
	}
	live, err := d.liveService(ctx)
	if err != nil {
		return "", err
	}
	step, err := d.unitStep(live, m.component)
	if err != nil {
		return "", fmt.Errorf("replay: %s: %w", ev.Key(), err)
	}
	d.samples.samples[string(m.component)] = scriptedSample{outcome: outcome, step: step, at: d.clock.Now()}
	return fmt.Sprintf("component=%s outcome=%s step=%d", m.component, ev.Args.Outcome, step), nil
}

// unitStep is the step the canary record of the unit a Component belongs
// to is on: the record lives on the primary of the group that lists the
// Component. A Component outside every canary group, or one whose unit
// carries no record yet, has no step for a sample to bind to.
func (d *driver) unitStep(isvc *v1beta1.InferenceService, c v1beta1.ComponentType) (int32, error) {
	for i := range d.service.Groups {
		g := &d.service.Groups[i]
		for _, member := range g.Components {
			if member != c {
				continue
			}
			cs := rollout.CanaryStatusFor(&isvc.Status, rollout.PrimaryOf(g))
			if cs == nil {
				return 0, fmt.Errorf("the unit of %s carries no canary record for a sample to answer", c)
			}
			return cs.CurrentStep, nil
		}
	}
	return 0, fmt.Errorf("%s is in no rollout group, so no metric gate reads it", c)
}
