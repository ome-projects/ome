package wait

import (
	"context"

	"sigs.k8s.io/ome/pkg/cli/exitcode"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/waitengine"
	"sigs.k8s.io/ome/pkg/cli/waitir"
	"sigs.k8s.io/ome/pkg/client/clientset/versioned"
)

func (o *options) runReadyReplicas(ctx context.Context, f factory.Factory, name, namespace string) error {
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
	source := waitir.NewSource(client.OmeV1beta1(), namespace, name, o.clock.Now)
	evaluator := &waitir.Evaluator{}
	component := reportv1alpha1.RuntimeComponentType(o.component)
	observed := reportv1alpha1.WaitReadyReplicasObservation{Component: component, Requested: o.replicas, Validity: "Unavailable"}
	predicate := func(v reportv1alpha1.InstanceListReport) (waitengine.Decision, error) {
		decision, current := evaluator.Evaluate(v, component, o.replicas)
		observed = reportv1alpha1.WaitReadyReplicasObservation{
			Component: current.Component, Requested: current.Requested,
			Observed: current.ReadyReplicas, Validity: current.Validity,
		}
		return decision, nil
	}
	result, err := waitengine.Run(ctx, source, predicate, waitengine.Options{Timeout: o.timeout, Clock: o.clock, PollOnly: true})
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return &waitengine.Error{Reason: waitengine.ReasonCanceled}
	}
	if result.Outcome == waitengine.OutcomeReplaced || result.Outcome == waitengine.OutcomeDeleted || result.Outcome == waitengine.OutcomeNotFound {
		observed = reportv1alpha1.WaitReadyReplicasObservation{Component: component, Requested: o.replicas, Validity: "Unavailable"}
		result.Reason = waitengine.ReasonReplicaReadyNotRecorded
	}
	content := reportv1alpha1.WaitContent{
		Requested: reportv1alpha1.WaitRequestedReadyReplicas, Outcome: result.Outcome, Reason: result.Reason,
		ReadyReplicas: &observed, ElapsedMilliseconds: result.Elapsed.Milliseconds(),
		Counts: reportv1alpha1.WaitCounts{Gets: result.Counts.Gets, Watches: result.Counts.Watches, Polls: result.Counts.Polls, Events: result.Counts.Events, Observations: result.Counts.Observations},
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
