// Package wait exposes explicit, bounded observations of reported conditions.
package wait

import (
	"context"
	"errors"
	"time"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/utils/clock"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/exitcode"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/waitengine"
	"sigs.k8s.io/ome/pkg/cli/waitpredicate"
	"sigs.k8s.io/ome/pkg/cli/waitsource"
)

var (
	errFlags     = errors.New("InvalidWaitFlags")
	errPredicate = errors.New("InvalidWaitPredicate: require --for=condition=Ready[=True|False|Unknown]")
	errName      = errors.New("InvalidInferenceServiceName")
	errNamespace = errors.New("InvalidNamespace")
	errTimeout   = errors.New("InvalidWaitTimeout: require positive duration no greater than 24h")
	errFormat    = errors.New("InvalidOutputFormat: supported table, wide, json, yaml")
	errConfig    = errors.New("WaitConfigurationUnavailable")
	errOutput    = errors.New("WaitOutputFailed")
	errUnmet     = errors.New("RequestedConditionUnmet")
)

type options struct {
	streams          genericiooptions.IOStreams
	forValue, output string
	timeout          time.Duration
	requested        corev1.ConditionStatus
	format           report.Format
	wide             bool
	clock            clock.Clock
}

func NewCmd(f factory.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	return newCmd(f, streams, clock.RealClock{})
}
func newCmd(f factory.Factory, streams genericiooptions.IOStreams, clock clock.Clock) *cobra.Command {
	o := options{streams: streams, clock: clock}
	cmd := &cobra.Command{
		Use:   "wait INFERENCESERVICE --for=condition=Ready[=True|False|Unknown]",
		Short: "Wait for an explicit reported InferenceService Ready condition",
		Long: `Wait for the requested reported Ready condition on one bound service.
--for is required; omitted condition status means True. Missing Ready is
NotRecorded, not the explicit Unknown status. The timeout defaults to 60s
and must be positive and no greater than 24h.

Exit 0 means the requested condition was observed on the same object;
exit 2 means timeout, absence, deletion, or replacement left it unmet.
API/configuration/output failures and parent cancellation return exit 1.
Generation freshness is Unverifiable: reported Ready is not proof of
current-spec or rollout convergence. No controller algorithms are reproduced.

The command uses only a named GET and an exact-name WATCH, with bounded
named-GET polling fallback. Cancellation/deadlines are cooperative;
external credential plugins or custom transports may ignore cancellation.
One final typed report is emitted; no raw conditions or API objects are printed.`,
		Example: `  kubectl ome wait chat --for=condition=Ready -n prod
  kubectl ome wait chat --for=condition=Ready=False --timeout=2m -o json
  kubectl ome wait chat --for=condition=Ready=Unknown -o wide`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.validate(args[0]); err != nil {
				return err
			}
			return o.run(cmd.Context(), f, args[0])
		},
	}
	cmd.SetFlagErrorFunc(func(*cobra.Command, error) error { return errFlags })
	cmd.Flags().StringVar(&o.forValue, "for", "", "Required predicate: condition=Ready[=True|False|Unknown]")
	cmd.Flags().DurationVar(&o.timeout, "timeout", 60*time.Second, "Positive wait timeout, at most 24h")
	cmd.Flags().StringVarP(&o.output, "output", "o", "table", "Output format: table, wide, json or yaml")
	return cmd
}
func (o *options) validate(name string) error {
	if len(utilvalidation.IsDNS1123Subdomain(name)) > 0 {
		return errName
	}
	switch o.forValue {
	case "condition=Ready", "condition=Ready=True":
		o.requested = corev1.ConditionTrue
	case "condition=Ready=False":
		o.requested = corev1.ConditionFalse
	case "condition=Ready=Unknown":
		o.requested = corev1.ConditionUnknown
	default:
		return errPredicate
	}
	if o.timeout <= 0 || o.timeout > 24*time.Hour {
		return errTimeout
	}
	if o.output == "wide" {
		o.format = report.FormatTable
		o.wide = true
		return nil
	}
	format, err := report.ParseFormat(o.output)
	if err != nil {
		return errFormat
	}
	o.format = format
	return nil
}
func (o *options) run(ctx context.Context, f factory.Factory, name string) error {
	if ctx.Err() != nil {
		return &waitengine.Error{Reason: waitengine.ReasonCanceled}
	}
	namespace, _, err := f.Namespace()
	if err != nil {
		return errConfig
	}
	if len(utilvalidation.IsDNS1123Label(namespace)) > 0 {
		return errNamespace
	}
	config, err := f.RESTConfig()
	if err != nil {
		return errConfig
	}
	source, err := waitsource.NewInferenceService(config, namespace, name)
	if err != nil {
		return errConfig
	}
	observation := waitpredicate.Observation{Status: "NotRecorded", Validity: "Unavailable", GenerationFreshness: "Unverifiable", Inspection: waitpredicate.Inspection{State: "NotInspected", Warnings: []waitpredicate.Warning{}}}
	predicate := func(v *ome.InferenceService) (waitengine.Decision, error) {
		decision, observed, evaluateErr := waitpredicate.EvaluateReady(v, o.requested, o.clock.Now())
		observation = observed
		return decision, evaluateErr
	}
	result, err := waitengine.Run(ctx, source, predicate, waitengine.Options{Timeout: o.timeout, Clock: o.clock})
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return &waitengine.Error{Reason: waitengine.ReasonCanceled}
	}
	content := reportv1alpha1.WaitContent{Requested: reportv1alpha1.WaitRequested("Ready=" + string(o.requested)), Outcome: result.Outcome, Reason: result.Reason, Observed: observation,
		ElapsedMilliseconds: result.Elapsed.Milliseconds(), Counts: reportv1alpha1.WaitCounts{Gets: result.Counts.Gets, Watches: result.Counts.Watches, Polls: result.Counts.Polls, Events: result.Counts.Events, Observations: result.Counts.Observations}, Method: result.Method, Fallback: result.Fallback}
	reportValue := reportv1alpha1.NewWaitReport(reportv1alpha1.Metadata{Namespace: namespace, Name: name}, content, reportv1alpha1.ClockFunc(o.clock.Now))
	if o.wide {
		err = reportValue.WideTable().Write(o.streams.Out)
	} else {
		err = report.Write(o.streams.Out, o.format, reportValue)
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
