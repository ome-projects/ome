// Package scale implements the finite alpha guarded transient /scale action.
package scale

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/rest"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/exitcode"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/mutate"
	"sigs.k8s.io/ome/pkg/cli/namespace"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/transport"
)

var ErrArguments = errors.New("invalid scale arguments; require one service, component engine|decoder|router, positive base-10 --replicas, supported output/dry-run and valid namespace/context/timeout")
var ErrOutcomeUnknown = errors.New("scale request outcome unknown; inspect kubectl ome autoscale status before preparing another request")

type options struct {
	streams   genericiooptions.IOStreams
	ome       *namespace.Options
	component string
	replicas  string
	output    string
	dryRun    string
	override  bool
	yes       bool
	clock     reportv1alpha1.Clock
}

// NewCmd constructs the isolated scale command; the root owns registration.
func NewCmd(f factory.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	return newCmd(f, streams, reportv1alpha1.SystemClock{})
}

func newCmd(f factory.Factory, streams genericiooptions.IOStreams, clock reportv1alpha1.Clock) *cobra.Command {
	o := &options{streams: streams, ome: namespace.NewOptions(), clock: clock}
	cmd := &cobra.Command{Use: "scale INFERENCESERVICE", Short: "Guard a transient positive OMENative replica request (alpha)",
		Long: `Sends one UID/resourceVersion-guarded JSON Patch to the selected InferenceReplica /scale.
This is transient API acceptance, not durable parent replica intent or controller convergence.
Every ownership class, including None, requires --override-autoscaler --yes.
OMENative does not preserve zero replicas. Scale-down may drain/delete Instances;
pause does not freeze teardown. Policies, scalers and other components are never changed.
The action has a 45-second deadline; each request is capped at ten seconds.
Pinned plans may require up to two additional exact status-selected sibling IR reads.
Every used IR is revalidated; these reads are best-effort, not a multi-object transaction.
Credential plugins/custom transports may not honor cancellation.`,
		SilenceUsage: true, SilenceErrors: true,
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) != 1 {
				return ErrArguments
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			replicas, format, wide, err := o.validate(cmd, args[0])
			if err != nil {
				return err
			}
			return o.run(cmd.Context(), f, args[0], replicas, format, wide)
		}}
	cmd.SetFlagErrorFunc(func(*cobra.Command, error) error { return ErrArguments })
	cmd.Flags().StringVar(&o.component, "component", "", "Required selected component: engine, decoder or router")
	cmd.Flags().StringVar(&o.replicas, "replicas", "", "Required positive base-10 logical Instance count (1..2147483647)")
	cmd.Flags().StringVarP(&o.output, "output", "o", "table", "Output: table, wide, json or yaml")
	cmd.Flags().StringVar(&o.dryRun, "dry-run", "none", "Dry run: none, client or server (all retain safety reads)")
	cmd.Flags().BoolVar(&o.override, "override-autoscaler", false, "Authorize a transient reconciliation-overwritten request; requires --yes")
	cmd.Flags().BoolVar(&o.yes, "yes", false, "Confirm this exact transient request without a terminal prompt")
	o.ome.AddOMEFlags(cmd.Flags())
	return cmd
}

func (o *options) validate(cmd *cobra.Command, name string) (int32, report.Format, bool, error) {
	invalid := func() (int32, report.Format, bool, error) { return 0, "", false, ErrArguments }
	if name == "" || len(validation.IsDNS1123Subdomain(name)) != 0 ||
		(o.component != "engine" && o.component != "decoder" && o.component != "router") || !cmd.Flags().Changed("replicas") || o.replicas == "" {
		return invalid()
	}
	for _, char := range o.replicas {
		if char < '0' || char > '9' {
			return invalid()
		}
	}
	replicas, err := strconv.ParseInt(o.replicas, 10, 32)
	if err != nil || replicas < 1 || (o.override && !o.yes) {
		return invalid()
	}
	if o.dryRun != "none" && o.dryRun != "client" && o.dryRun != "server" {
		return invalid()
	}
	if _, err := o.ome.Resolve("default"); err != nil {
		return invalid()
	}
	for _, name := range []string{"namespace", "context", "request-timeout"} {
		flag := cmd.Flag(name)
		if flag == nil || !flag.Changed {
			continue
		}
		value := flag.Value.String()
		switch name {
		case "namespace":
			if value == "" || len(validation.IsDNS1123Label(value)) != 0 {
				return invalid()
			}
		case "context":
			if !mutate.SafeScalar(value) {
				return invalid()
			}
		case "request-timeout":
			duration, err := time.ParseDuration(value)
			if err != nil || duration < 0 {
				return invalid()
			}
		}
	}
	wide := o.output == "wide"
	format := report.FormatTable
	if !wide {
		format, err = report.ParseFormat(o.output)
		if err != nil {
			return invalid()
		}
	}
	return int32(replicas), format, wide, nil
}

func actionConfig(config *rest.Config) (*rest.Config, error) {
	if config == nil {
		return nil, ErrArguments
	}
	local := *config
	if local.ExecProvider != nil {
		local.ExecProvider = local.ExecProvider.DeepCopy()
	}
	owned := rest.CopyConfig(&local)
	if owned.Timeout <= 0 || owned.Timeout > 10*time.Second {
		owned.Timeout = 10 * time.Second
	}
	return owned, nil
}

func (o *options) run(ctx context.Context, f factory.Factory, name string, replicas int32, format report.Format, wide bool) error {
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	evidence, err := collect(ctx, f, o.ome, name, v1beta1.ComponentType(o.component), o.clock)
	if err != nil {
		return err
	}
	plan, err := mutate.PrepareScale(evidence.parent, evidence.state, evidence.scale, v1beta1.ComponentType(o.component), replicas, o.override, o.yes, o.clock)
	if err != nil {
		return err
	}
	if err := plan.WritePreview(o.streams.ErrOut, evidence.context, evidence.omeNamespace, reportv1alpha1.DryRunMode(o.dryRun)); err != nil {
		return err
	}
	if err := mutate.Confirm(ctx, o.streams.In, o.streams.ErrOut, o.yes); err != nil {
		return err
	}
	if err := evidence.source.Revalidate(ctx, evidence.reader); err != nil {
		return err
	}
	if err := evidence.scale.Revalidate(ctx, evidence.ome, o.clock); err != nil {
		return err
	}
	result := reportv1alpha1.NewActionResult("scale", plan.Target(), reportv1alpha1.DryRunMode(o.dryRun), o.clock)
	details := plan.Details()
	result.Scale = &details
	result.FollowUp = fmt.Sprintf("kubectl ome autoscale status %s -n %s --context=%s", name, evidence.parent.Namespace, evidence.context)
	result.Message = "Validated locally; no scale patch sent."
	if o.dryRun != "client" {
		requestContext, requestCancel := context.WithTimeout(ctx, 10*time.Second)
		response, err := evidence.transport.PatchInferenceReplicaScale(requestContext, transport.Resource{Namespace: result.Target.Namespace, Resource: "inferencereplicas", Name: result.Target.Name}, plan.Patch(), transport.JSONPatchOptions{DryRun: o.dryRun == "server"})
		requestCancel()
		if err != nil {
			return patchError(err)
		}
		if err := plan.ValidateResponse(response); err != nil {
			return ErrOutcomeUnknown
		}
		result.Accepted = true
		result.Applied = o.dryRun == "none"
		result.Message = "API accepted transient scale request; convergence not observed."
		if o.dryRun == "server" {
			result.Message = "API dry-run accepted; no changes persisted."
		}
	}
	if wide {
		err = result.Canonical().WideTable().Write(o.streams.Out)
	} else {
		err = report.Write(o.streams.Out, format, result)
	}
	if err != nil {
		return errors.New("write scale result failed; API acceptance may already have occurred; inspect current status")
	}
	return nil
}

func patchError(err error) error {
	cas := apierrors.IsConflict(err)
	var status apierrors.APIStatus
	if errors.As(err, &status) {
		value := status.Status()
		cas = cas || value.Code == 422 && value.Reason == metav1.StatusReasonInvalid && value.Status == metav1.StatusFailure &&
			value.Message == "the server rejected our request due to an error in our request" && value.Details != nil &&
			value.Details.Name == "" && value.Details.Group == "" && value.Details.Kind == "" && value.Details.UID == "" && value.Details.RetryAfterSeconds == 0 && len(value.Details.Causes) == 0
	}
	if cas {
		return &exitcode.PreconditionError{Err: errors.New("guarded scale precondition rejected; inspect current status and prepare a new request")}
	}
	return ErrOutcomeUnknown
}
