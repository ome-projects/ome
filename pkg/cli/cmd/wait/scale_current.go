package wait

import (
	"context"

	"k8s.io/apimachinery/pkg/types"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/exitcode"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/transport"
	"sigs.k8s.io/ome/pkg/cli/waitengine"
	"sigs.k8s.io/ome/pkg/cli/waitscale"
	"sigs.k8s.io/ome/pkg/client/clientset/versioned"
)

func (o *options) runScaleCurrent(ctx context.Context, f factory.Factory, name, namespace string) error {
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
	target := waitscale.Target{Component: ome.ComponentType(o.component), Replicas: o.replicas,
		IRName: o.irName, IRUID: types.UID(o.irUID)}
	config, err := f.RESTConfig()
	if err != nil || config == nil {
		return errConfig
	}
	replicaReader, err := transport.NewBounded(config, 1<<20)
	if err != nil {
		return errConfig
	}
	source := waitscale.NewSource(client.OmeV1beta1(), replicaReader, namespace, name, target)
	evaluator := waitscale.NewEvaluator(target)
	unavailable := reportv1alpha1.WaitScaleObservation{
		Component: target.Component, Requested: target.Replicas,
		Validity: "Unavailable", Freshness: "Unavailable",
	}
	observed := unavailable
	predicate := func(evidence waitscale.Evidence) (waitengine.Decision, error) {
		decision, current := evaluator.Evaluate(evidence)
		observed = reportv1alpha1.WaitScaleObservation(current)
		return decision, nil
	}
	result, err := waitengine.Run(ctx, source, predicate, waitengine.Options{
		Timeout: o.timeout, Clock: o.clock, PollOnly: true,
	})
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return &waitengine.Error{Reason: waitengine.ReasonCanceled}
	}
	if result.Outcome == waitengine.OutcomeReplaced || result.Outcome == waitengine.OutcomeDeleted || result.Outcome == waitengine.OutcomeNotFound {
		observed = unavailable
		result.Reason = waitscale.ReasonNotRecorded
	}
	content := reportv1alpha1.WaitContent{
		Requested: reportv1alpha1.WaitRequestedScaleCurrent,
		Outcome:   result.Outcome, Reason: result.Reason, Scale: &observed,
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
