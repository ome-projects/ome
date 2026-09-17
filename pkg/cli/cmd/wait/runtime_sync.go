package wait

import (
	"context"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/exitcode"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/waitengine"
	"sigs.k8s.io/ome/pkg/cli/waitruntime"
	"sigs.k8s.io/ome/pkg/cli/waitsource"
)

func (o *options) runRuntimeSync(ctx context.Context, f factory.Factory, name, namespace string) error {
	config, err := f.RESTConfig()
	if err != nil {
		return errConfig
	}
	source, err := waitsource.NewInferenceService(config, namespace, name)
	if err != nil {
		return errConfig
	}
	observed := reportv1alpha1.WaitRuntimeSyncObservation{
		RequestID: o.requestID, TokenState: waitruntime.TokenUnavailable,
		DriftState: waitruntime.DriftUnavailable, PinState: waitruntime.PinUnavailable,
		PlacementState: waitruntime.PlacementDirect, Validity: "Unavailable",
		GenerationFreshness: "Unverifiable",
	}
	predicate := func(v *ome.InferenceService) (waitengine.Decision, error) {
		decision, current, evaluateErr := waitruntime.Evaluate(v, o.requestID)
		observed = reportv1alpha1.WaitRuntimeSyncObservation(current)
		return decision, evaluateErr
	}
	result, err := waitengine.Run(ctx, source, predicate, waitengine.Options{Timeout: o.timeout, Clock: o.clock, PollOnly: true})
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return &waitengine.Error{Reason: waitengine.ReasonCanceled}
	}
	if result.Outcome == waitengine.OutcomeReplaced || result.Outcome == waitengine.OutcomeDeleted || result.Outcome == waitengine.OutcomeNotFound {
		observed = reportv1alpha1.WaitRuntimeSyncObservation{
			RequestID: o.requestID, TokenState: waitruntime.TokenUnavailable,
			DriftState: waitruntime.DriftUnavailable, PinState: waitruntime.PinUnavailable,
			PlacementState: waitruntime.PlacementDirect, Validity: "Unavailable",
			GenerationFreshness: "Unverifiable",
		}
		result.Reason = waitruntime.ReasonNotRecorded
	}
	content := reportv1alpha1.WaitContent{
		Requested: reportv1alpha1.WaitRequestedRuntimeSyncAcknowledged,
		Outcome:   result.Outcome, Reason: result.Reason, RuntimeSync: &observed,
		ElapsedMilliseconds: result.Elapsed.Milliseconds(),
		Counts: reportv1alpha1.WaitCounts{Gets: result.Counts.Gets, Watches: result.Counts.Watches,
			Polls: result.Counts.Polls, Events: result.Counts.Events, Observations: result.Counts.Observations},
		Method: result.Method, Fallback: result.Fallback,
	}
	value := reportv1alpha1.NewWaitReport(reportv1alpha1.Metadata{Namespace: namespace, Name: name}, content, reportv1alpha1.ClockFunc(o.clock.Now))
	if o.wide {
		err = value.WideTable().Write(o.streams.Out)
	} else {
		err = report.Write(o.streams.Out, o.format, value)
	}
	if err != nil {
		return errOutput
	}
	if ctx.Err() != nil {
		return &waitengine.Error{Reason: waitengine.ReasonCanceled}
	}
	if result.Outcome != waitengine.OutcomeMatched {
		return &exitcode.UnmetAssertionError{Err: errUnmet}
	}
	return nil
}
