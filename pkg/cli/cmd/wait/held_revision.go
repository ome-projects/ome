package wait

import (
	"context"

	"sigs.k8s.io/ome/pkg/cli/exitcode"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/transport"
	"sigs.k8s.io/ome/pkg/cli/waitengine"
	"sigs.k8s.io/ome/pkg/cli/waitheld"
	"sigs.k8s.io/ome/pkg/client/clientset/versioned"
)

func (o *options) runHeldRevision(ctx context.Context, f factory.Factory, name, namespace string) error {
	var client versioned.Interface
	var err error
	if scoped, ok := f.(factory.ActionReadClientsResolver); ok {
		client, err = scoped.OMEClientForAction(ctx)
	} else {
		client, err = f.OMEClient()
	}
	if err != nil || client == nil {
		return errConfig
	}
	target := waitheld.Target{Namespace: namespace, ParentName: name, Component: o.component,
		IRName: o.irName, Revision: o.revision, IRUID: o.irUID}
	config, err := f.RESTConfig()
	if err != nil || config == nil {
		return errConfig
	}
	replicaReader, err := transport.NewBounded(config, 1<<20)
	if err != nil {
		return errConfig
	}
	source, err := waitheld.NewSource(client.OmeV1beta1(), replicaReader, target)
	if err != nil {
		return errHeldTarget
	}
	unavailable := reportv1alpha1.WaitHeldRevisionObservation{
		Reason: waitheld.ReasonSourceIncomplete, Validity: waitheld.ValidityUnavailable,
		TargetState: waitheld.TargetUnknown, MailboxState: waitheld.MailboxUnknown,
		Attribution: waitheld.AttributionUnverifiable, Component: o.component,
		Revision: o.revision, InferenceReplica: o.irName, InferenceReplicaUID: o.irUID,
	}
	observed := unavailable
	predicate := func(current waitheld.Observation) (waitengine.Decision, error) {
		observed = reportv1alpha1.WaitHeldRevisionObservation(current)
		return waitengine.Decision{Matched: current.Matched, Reason: waitengine.Reason(current.Reason)}, nil
	}
	result, err := waitengine.Run(ctx, source, predicate, waitengine.Options{Timeout: o.timeout, Clock: o.clock, PollOnly: true})
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return &waitengine.Error{Reason: waitengine.ReasonCanceled}
	}
	if result.Outcome == waitengine.OutcomeReplaced || result.Outcome == waitengine.OutcomeDeleted || result.Outcome == waitengine.OutcomeNotFound {
		observed = unavailable
		result.Reason = waitengine.Reason(waitheld.ReasonSourceIncomplete)
	}
	content := reportv1alpha1.WaitContent{
		Requested: reportv1alpha1.WaitRequestedHeldRevisionUnheld,
		Outcome:   result.Outcome, Reason: result.Reason, HeldRevision: &observed,
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
	if value.Content.Outcome != waitengine.OutcomeMatched {
		return &exitcode.UnmetAssertionError{Err: errUnmet}
	}
	return nil
}
