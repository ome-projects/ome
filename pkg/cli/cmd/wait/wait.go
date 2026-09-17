// Package wait exposes explicit, bounded observations of reported conditions.
package wait

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
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
	"sigs.k8s.io/ome/pkg/cli/waitheld"
	"sigs.k8s.io/ome/pkg/cli/waitmigration"
	"sigs.k8s.io/ome/pkg/cli/waitpredicate"
	"sigs.k8s.io/ome/pkg/cli/waitrollout"
	"sigs.k8s.io/ome/pkg/cli/waitsource"
	"sigs.k8s.io/ome/pkg/client/clientset/versioned"
)

var (
	errFlags                = errors.New("InvalidWaitFlags")
	errPredicate            = errors.New("InvalidWaitPredicate: require condition=Ready[=True|False|Unknown], rollout=stable|failed|rolled-back, migration=terminal, replicas=ready|current, runtime-sync=acknowledged, or held-revision=unheld")
	errRequestID            = errors.New("InvalidMigrationRequestID: require canonical UUID only with migration=terminal")
	errRuntimeSyncRequestID = errors.New("InvalidRuntimeSyncRequestID: require canonical v4 UUID with runtime-sync=acknowledged")
	errCountFlags           = errors.New("InvalidReadyReplicaFlags: require --component=engine|decoder|router and --replicas=N only with replicas=ready; N must be nonnegative")
	errScaleFlags           = errors.New("InvalidCurrentReplicaFlags: require --component=engine|decoder|router and positive --replicas=N; --ir-name and --ir-uid must be paired")
	errHeldTarget           = errors.New("InvalidHeldRevisionTarget: require --component, full --revision, --ir-name and --ir-uid only with held-revision=unheld")
	errName                 = errors.New("InvalidInferenceServiceName")
	errNamespace            = errors.New("InvalidNamespace")
	errTimeout              = errors.New("InvalidWaitTimeout: require positive duration no greater than 24h")
	errFormat               = errors.New("InvalidOutputFormat: supported table, wide, json, yaml")
	errConfig               = errors.New("WaitConfigurationUnavailable")
	errOutput               = errors.New("WaitOutputFailed")
	errUnmet                = errors.New("RequestedConditionUnmet")
)

type options struct {
	streams                 genericiooptions.IOStreams
	forValue, output        string
	timeout                 time.Duration
	requested               corev1.ConditionStatus
	requestedRollout        reportv1alpha1.WaitRequested
	requestedMigration      bool
	requestedReadyReplicas  bool
	requestedScaleCurrent   bool
	requestedRuntimeSync    bool
	requestedHeldRevision   bool
	requestID               string
	component               string
	revision, irName, irUID string
	replicas                int32
	componentSet            bool
	replicasSet             bool
	revisionSet             bool
	irNameSet               bool
	irUIDSet                bool
	format                  report.Format
	wide                    bool
	clock                   clock.Clock
}

func NewCmd(f factory.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	return newCmd(f, streams, clock.RealClock{})
}
func newCmd(f factory.Factory, streams genericiooptions.IOStreams, clock clock.Clock) *cobra.Command {
	o := options{streams: streams, clock: clock}
	cmd := &cobra.Command{
		Use:   "wait INFERENCESERVICE --for=PREDICATE",
		Short: "Wait for reported service, replica, or runtime-sync state",
		Long: `Wait for a reported condition, rollout, migration, exact IR count, or
runtime-sync acknowledgment on one bound service.
--for is required; omitted condition status means True. Missing Ready is
NotRecorded, not the explicit Unknown status. The timeout defaults to 60s
and must be positive and no greater than 24h.

Use condition=Ready[=True|False|Unknown], rollout=stable|failed|rolled-back,
or migration=terminal with a canonical --request-id UUID. Migration matches
only that exact request in a complete, current, owned InferenceReplica status
snapshot. Completed, Failed, and Relocated are all terminal outcomes; a match
does not imply success. Missing, invalid, partial, or stale evidence does not
match. The migration path polls bounded parent/IR reads every 5s because a
parent watch need not observe IR-only status updates.
Use replicas=ready with --component=engine|decoder|router and --replicas=N
for an exact, nonnegative status.readyReplicas count on one current owned IR.
A verified zero needs a positive matching status.observedGeneration; an
unobserved omitted zero does not match. Ready count is not serving,
availability, /scale convergence, or attribution to a preceding action.
Like migration, this path polls bounded parent/IR reads every 5s.
Reads admit at most 32 related IRs and 2,048 status rows; incomplete
snapshots never satisfy an exact count.
Use replicas=current with --component=engine|decoder|router and positive
--replicas=N for exact current InferenceReplica spec.replicas and reported
logical status.replicas. DenseV1 and ColumnarV2 instance status are decoded
and checked; Ready is reported separately, not required for this predicate.
Optional paired --ir-name and --ir-uid bind the wait to the original target
from a scale ActionResult; otherwise the current parent scaleTargetRef selects
one IR. This is status-oriented, not durable scale intent, readiness, or proof
that a preceding action caused the state. It polls bounded named parent and
exact IR GETs every 5s, without sibling LISTs or parent WATCHes.
Use runtime-sync=acknowledged with the v4 --request-id from runtime sync.
It matches only when the same current parent reports the exact annotation
token acknowledged in status, an eligible managed pin, and no reported
RuntimeDrifted condition. This is state-oriented: token acknowledgment is
not live-runtime convergence, serving readiness, or action attribution.
This path polls bounded named parent GETs every 5s; it does not read
runtime objects, IRs, Secrets, or raw controller messages.
Use held-revision=unheld with --component and the full scoped --revision,
formed as ISVC-COMPONENT-REVISIONHASH from the release-held ActionResult's
revisionHash (or copied from instance retry-blocks). Copy --ir-name and
--ir-uid from that ActionResult's target.name and target.uid. Only the
original, current, owned IR can match
when that exact revision is no longer Held and the mailbox is absent.
This is state-oriented, not proof the release request caused the state:
natural pruning or a later re-Held can race. The first poll may match.
It polls named parent and exact IR GETs every 5s, with no sibling LIST or WATCH.
Rollout assertions use qualified canonical aggregate ReportedState:
stable means Succeeded, not NotConfigured or Staged; failed means Failed;
rolled-back means RolledBack. Missing or invalid evidence never matches.
They describe same-object reported state, not attribution to a preceding action.

Exit 0 means the requested condition was observed on the same object;
exit 2 means timeout, absence, deletion, or replacement left it unmet.
API/configuration/output failures and parent cancellation return exit 1.
Generation freshness is Unverifiable: reported Ready is not proof of
current-spec or rollout convergence. No controller algorithms are reproduced.

Ready and rollout use a named GET and exact-name WATCH with bounded named-GET
polling fallback. Migration and IR count use bounded named parent/IR reads;
they do not claim parent-watch observation of IR-only changes.
Cancellation/deadlines are cooperative;
external credential plugins or custom transports may ignore cancellation.
One final typed report is emitted; no raw conditions or API objects are printed.`,
		Example: `  kubectl ome wait chat --for=condition=Ready -n prod
  kubectl ome wait chat --for=condition=Ready=False --timeout=2m -o json
  kubectl ome wait chat --for=condition=Ready=Unknown -o wide
  kubectl ome wait chat --for=rollout=stable --timeout=2m -o json
  kubectl ome wait chat --for=rollout=failed -o wide
  kubectl ome wait chat --for=migration=terminal --request-id=12345678-1234-4234-8234-123456789abc -n prod
  kubectl ome wait chat --for=replicas=ready --component=engine --replicas=2 -n prod
  kubectl ome wait chat --for=replicas=current --component=engine --replicas=2 -n prod
  kubectl ome wait chat --for=runtime-sync=acknowledged --request-id=123e4567-e89b-42d3-a456-426614174000 -n prod
  kubectl ome wait chat --for=held-revision=unheld --component=engine --revision=chat-engine-aaaaaaaa --ir-name=chat-engine --ir-uid=original-uid -n prod`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			o.componentSet = cmd.Flags().Changed("component")
			o.replicasSet = cmd.Flags().Changed("replicas")
			o.revisionSet = cmd.Flags().Changed("revision")
			o.irNameSet = cmd.Flags().Changed("ir-name")
			o.irUIDSet = cmd.Flags().Changed("ir-uid")
			if err := o.validate(args[0]); err != nil {
				return err
			}
			return o.run(cmd.Context(), f, args[0])
		},
	}
	cmd.SetFlagErrorFunc(func(*cobra.Command, error) error { return errFlags })
	cmd.Flags().StringVar(&o.forValue, "for", "", "Required: condition=Ready[=True|False|Unknown], rollout=stable|failed|rolled-back, migration=terminal, replicas=ready|current, runtime-sync=acknowledged, or held-revision=unheld")
	cmd.Flags().StringVar(&o.requestID, "request-id", "", "Canonical UUID for migration=terminal; canonical v4 UUID for runtime-sync=acknowledged")
	cmd.Flags().StringVar(&o.component, "component", "", "IR component for replica or held-revision waits: engine, decoder, router")
	cmd.Flags().Int32Var(&o.replicas, "replicas", 0, "Exact count: nonnegative for replicas=ready; positive for replicas=current")
	cmd.Flags().StringVar(&o.revision, "revision", "", "Full ISVC-COMPONENT-REVISIONHASH, required only for held-revision=unheld")
	cmd.Flags().StringVar(&o.irName, "ir-name", "", "Exact ActionResult target.name; required for held-revision, optional paired with --ir-uid for replicas=current")
	cmd.Flags().StringVar(&o.irUID, "ir-uid", "", "Exact ActionResult target.uid; required for held-revision, optional paired with --ir-name for replicas=current")
	cmd.Flags().DurationVar(&o.timeout, "timeout", 60*time.Second, "Positive wait timeout, at most 24h")
	cmd.Flags().StringVarP(&o.output, "output", "o", "table", "Output format: table, wide, json or yaml")
	return cmd
}
func (o *options) validate(name string) error {
	if len(utilvalidation.IsDNS1123Subdomain(name)) > 0 {
		return errName
	}
	o.requested = ""
	o.requestedRollout = ""
	o.requestedMigration = false
	o.requestedReadyReplicas = false
	o.requestedScaleCurrent = false
	o.requestedRuntimeSync = false
	o.requestedHeldRevision = false
	o.wide = false
	switch o.forValue {
	case "condition=Ready", "condition=Ready=True":
		o.requested = corev1.ConditionTrue
	case "condition=Ready=False":
		o.requested = corev1.ConditionFalse
	case "condition=Ready=Unknown":
		o.requested = corev1.ConditionUnknown
	case "rollout=stable":
		o.requestedRollout = reportv1alpha1.WaitRequestedRolloutStable
	case "rollout=failed":
		o.requestedRollout = reportv1alpha1.WaitRequestedRolloutFailed
	case "rollout=rolled-back":
		o.requestedRollout = reportv1alpha1.WaitRequestedRolloutRolledBack
	case "migration=terminal":
		o.requestedMigration = true
	case "replicas=ready":
		o.requestedReadyReplicas = true
	case "replicas=current":
		o.requestedScaleCurrent = true
	case "runtime-sync=acknowledged":
		o.requestedRuntimeSync = true
	case "held-revision=unheld":
		o.requestedHeldRevision = true
	default:
		return errPredicate
	}
	if o.requestedMigration {
		parsed, err := uuid.Parse(o.requestID)
		if err != nil || parsed.String() != o.requestID || parsed.Variant() != uuid.RFC4122 || parsed.Version() < 1 || parsed.Version() > 8 {
			return errRequestID
		}
	} else if o.requestedRuntimeSync {
		parsed, err := uuid.Parse(o.requestID)
		if err != nil || parsed.String() != o.requestID || parsed.Variant() != uuid.RFC4122 || parsed.Version() != 4 {
			return errRuntimeSyncRequestID
		}
	} else if o.requestID != "" {
		return errRequestID
	}
	if o.requestedReadyReplicas {
		if !o.componentSet || !o.replicasSet || o.replicas < 0 ||
			(o.component != "engine" && o.component != "decoder" && o.component != "router") {
			return errCountFlags
		}
	} else if o.requestedScaleCurrent {
		if !o.componentSet || !o.replicasSet || o.replicas < 1 ||
			(o.component != "engine" && o.component != "decoder" && o.component != "router") ||
			o.irNameSet != o.irUIDSet ||
			(o.irNameSet && (len(utilvalidation.IsDNS1123Subdomain(o.irName)) > 0 || !waitheld.ValidIdentity(o.irUID))) {
			return errScaleFlags
		}
	} else if o.requestedHeldRevision {
		if !o.componentSet || o.replicasSet || !o.revisionSet || !o.irNameSet || !o.irUIDSet ||
			!waitheld.ValidTarget(waitheld.Target{Namespace: "default", ParentName: name, Component: o.component,
				IRName: o.irName, Revision: o.revision, IRUID: o.irUID}) {
			return errHeldTarget
		}
	} else if o.componentSet || o.replicasSet {
		return errCountFlags
	}
	if !o.requestedHeldRevision && o.revisionSet ||
		!o.requestedHeldRevision && !o.requestedScaleCurrent && (o.irNameSet || o.irUIDSet) {
		return errHeldTarget
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
	if o.requestedMigration {
		return o.runMigration(ctx, f, name, namespace)
	}
	if o.requestedReadyReplicas {
		return o.runReadyReplicas(ctx, f, name, namespace)
	}
	if o.requestedScaleCurrent {
		return o.runScaleCurrent(ctx, f, name, namespace)
	}
	if o.requestedRuntimeSync {
		return o.runRuntimeSync(ctx, f, name, namespace)
	}
	if o.requestedHeldRevision {
		return o.runHeldRevision(ctx, f, name, namespace)
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
	rolloutObservation := reportv1alpha1.WaitRolloutObservation{Validity: "Unavailable", Inspection: reportv1alpha1.WaitRolloutInspection{State: "NotInspected"}}
	predicate := func(v *ome.InferenceService) (waitengine.Decision, error) {
		if o.requestedRollout.IsRollout() {
			decision, observed, evaluateErr := waitrollout.Evaluate(v, o.requestedRollout, o.clock.Now())
			rolloutObservation = observed
			return decision, evaluateErr
		}
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
	if o.requestedRollout.IsRollout() {
		content.Requested = o.requestedRollout
		content.Rollout = &rolloutObservation
	}
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

func (o *options) runMigration(ctx context.Context, f factory.Factory, name, namespace string) error {
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
	source := waitmigration.NewSource(client.OmeV1beta1(), namespace, name, o.clock.Now)
	evaluator := &waitmigration.Evaluator{}
	observed := reportv1alpha1.WaitMigrationObservation{
		RequestID: o.requestID, Phase: reportv1alpha1.MigrationPhaseUnknown,
		Outcome: reportv1alpha1.MigrationOutcomeUnknown, Validity: "Unavailable",
	}
	predicate := func(v reportv1alpha1.MigrationStatusReport) (waitengine.Decision, error) {
		decision, observation := evaluator.Evaluate(v, o.requestID)
		observed = observation
		return decision, nil
	}
	result, err := waitengine.Run(ctx, source, predicate, waitengine.Options{Timeout: o.timeout, Clock: o.clock, PollOnly: true})
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return &waitengine.Error{Reason: waitengine.ReasonCanceled}
	}
	// Replacement, deletion, and absence end the parent binding before another
	// predicate evaluation. The previous IR record is no longer live evidence.
	if result.Outcome == waitengine.OutcomeReplaced || result.Outcome == waitengine.OutcomeDeleted || result.Outcome == waitengine.OutcomeNotFound {
		observed = reportv1alpha1.WaitMigrationObservation{
			RequestID: o.requestID, Phase: reportv1alpha1.MigrationPhaseUnknown,
			Outcome: reportv1alpha1.MigrationOutcomeUnknown, Validity: "Unavailable",
		}
		result.Reason = waitengine.ReasonMigrationNotRecorded
	}
	content := reportv1alpha1.WaitContent{
		Requested: reportv1alpha1.WaitRequestedMigrationTerminal, Outcome: result.Outcome, Reason: result.Reason,
		Migration: &observed, ElapsedMilliseconds: result.Elapsed.Milliseconds(),
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
