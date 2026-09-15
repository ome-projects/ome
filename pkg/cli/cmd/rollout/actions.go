package rollout

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/cli/effective"
	"sigs.k8s.io/ome/pkg/cli/exitcode"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/mutate"
	"sigs.k8s.io/ome/pkg/cli/namespace"
	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/transport"
)

type actionOptions struct {
	streams          genericiooptions.IOStreams
	clock            reportv1alpha1.Clock
	namespaces       *namespace.Options
	action           string
	output           string
	dryRun           string
	yes              bool
	discard          bool
	overrideAnalysis bool
}

func newActionCmd(f factory.Factory, streams genericiooptions.IOStreams, clock reportv1alpha1.Clock, action string) *cobra.Command {
	o := &actionOptions{streams: streams, clock: clock, namespaces: namespace.NewOptions(), action: action}
	cmd := &cobra.Command{Use: action + " INFERENCESERVICE", Short: "Alpha: guarded service-wide rollout " + action, Long: `Alpha guarded OMENative lifecycle action. Requires current controller evidence,
exact target identity, no placement ownership and confirmation or --yes.

Pause sets true: Update/Create/Migration hold while RestartPolicy repair continues.
It does not request freeze. Existing freeze is never silently downgraded.
Resume clears either recognized depth. Timed canary gates continue aging;
scale-down and deletion may proceed. Acceptance is not controller convergence.

Client dry-run validates and confirms but sends no patch; server dry-run sends
the identical guarded JSON Patch with dryRun=All. Preview and prompt use stderr.
API requests and confirmation use a 45-second action context. Each request is
capped at 10 seconds, preserving shorter --request-timeout settings. External
credential plugins/custom transports may not honor cancellation.
Safety inspection caps
32 related IRs / two pages, 2048 instances and 256 migrations per IR; incomplete,
stale or malformed safety evidence refuses instead of accepting a prefix.`, Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		format, _, err := parseRolloutOutput(o.output)
		if err != nil {
			return errors.New("output must be table, wide, json or yaml")
		}
		mode := reportv1alpha1.DryRunMode(o.dryRun)
		if mode != reportv1alpha1.DryRunNone && mode != reportv1alpha1.DryRunClient && mode != reportv1alpha1.DryRunServer {
			return errors.New("dry-run must be none, client or server")
		}
		if o.discard && !o.yes {
			return mutate.ErrStrongConfirmation
		}
		if o.overrideAnalysis && !o.yes {
			return mutate.ErrAnalysisOverrideConfirmation
		}
		return o.run(cmd.Context(), f, args[0], format, mode)
	}}
	if action == "promote" || action == "rollback" {
		cmd.Short = "Alpha: guarded canary rollout " + action
		cmd.Long = `Alpha guarded canary mailbox action on the current pinned run and step.
Promote advances an active indefinite manual gate. Analysis is never bypassed
by ordinary promote: --override-analysis --yes is required for an active
analysis gate and bypasses its health checks, warm-up and bake for this step.
Rollback aborts every canary member to its own reported stable revision and
holds the rejected target; it is not retry, redeploy or a rollout-spec edit.
Globally paused, placement-owned, pending or unbound targets are refused.
Acceptance is not controller convergence; no automatic replay or wait.

Client dry-run validates, previews and confirms without a patch. Server dry-run
sends the identical UID/resourceVersion-guarded JSON Patch with dryRun=All.
Preview and prompt use stderr; stdout is one ActionResult. The action context
is 45 seconds and each request is capped at 10 seconds, preserving shorter
--request-timeout settings. External credential plugins/custom transports may
not honor cancellation. Inspection caps 32 related IRs / two pages, 2048
instances and 256 migrations per IR; incomplete or malformed evidence refuses.`
	}
	cmd.SetFlagErrorFunc(func(*cobra.Command, error) error {
		return errors.New("invalid rollout action flags; use --help")
	})
	cmd.Flags().BoolVar(&o.yes, "yes", false, "Confirm the exact preview without an interactive prompt")
	cmd.Flags().StringVar(&o.dryRun, "dry-run", "none", "Dry-run mode: none, client or server")
	cmd.Flags().StringVarP(&o.output, "output", "o", "table", "Output: table, wide (bounded), json or yaml")
	o.namespaces.AddOMEFlags(cmd.Flags())
	if action == "resume" {
		cmd.Flags().BoolVar(&o.discard, "discard-pending-actions", false, "Atomically discard pending promote/rollback mailboxes; requires --yes")
	}
	if action == "promote" {
		cmd.Flags().BoolVar(&o.overrideAnalysis, "override-analysis", false, "Bypass only this active analysis gate, including warm-up and bake; requires --yes")
	}
	return cmd
}

func (o *actionOptions) run(parent context.Context, f factory.Factory, name string, format report.Format, dryRun reportv1alpha1.DryRunMode) error {
	if !mutate.SafeScalar(name) || len(validation.IsDNS1123Subdomain(name)) != 0 {
		return ErrInvalidInferenceServiceName
	}
	ctx, cancel := context.WithTimeout(parent, 45*time.Second)
	defer cancel()
	workloadNS, _, err := f.Namespace()
	if err != nil {
		return errors.New("resolve workload namespace failed")
	}
	resolved, err := o.namespaces.Resolve(workloadNS)
	if err != nil {
		return errors.New("action namespaces are invalid")
	}
	contextResolver, ok := f.(factory.ContextResolver)
	if !ok {
		return errors.New("selected kubeconfig context is unavailable for action preview")
	}
	contextName, err := contextResolver.ContextName()
	if err != nil || !mutate.SafeScalar(contextName) {
		return errors.New("selected kubeconfig context is unavailable or unsafe")
	}
	client, err := f.OMEClient()
	if err != nil {
		return mutate.SafeAPIError(err)
	}
	readCtx, stopRead := context.WithTimeout(ctx, 10*time.Second)
	v, err := client.OmeV1beta1().InferenceServices(resolved.WorkloadNamespace).Get(readCtx, name, metav1.GetOptions{})
	readErr := readCtx.Err()
	stopRead()
	if err != nil {
		return mutate.SafeAPIError(err)
	}
	if readErr != nil {
		return readErr
	}
	if err = mutate.ValidateTarget(v); err != nil {
		return err
	}
	if v.Name != name || v.Namespace != resolved.WorkloadNamespace {
		return errors.New("action target response does not match exact request")
	}
	kube, err := f.KubeClient()
	if err != nil {
		return mutate.SafeAPIError(err)
	}
	var runtimeClient ctrlclient.Client
	if bounded, ok := f.(factory.ActionRuntimeResolver); ok {
		runtimeClient, err = bounded.RuntimeClientForAction(ctx)
	} else {
		runtimeClient, err = f.RuntimeClient()
	}
	if err != nil {
		return mutate.SafeAPIError(err)
	}
	resolver, err := effective.NewRuntimePinResolver(kube.AppsV1(), effective.NewRuntimeResolver(runtimeClient), resolved.OMENamespace, paging.Limits{PageSize: 16, MaxItems: 32, MaxPages: 2, RequestTimeout: 10 * time.Second})
	if err != nil {
		return errors.New("construct action runtime resolver failed")
	}
	state, err := resolver.Resolve(ctx, v, effective.RuntimeResolveOptions{})
	if err != nil {
		return mutate.SafeAPIError(err)
	}
	components, err := mutate.RequireNativeRuntime(v, state)
	if err != nil {
		return err
	}
	var work mutate.ReplicaEvidence
	if o.action == "pause" || o.action == "promote" || o.action == "rollback" {
		work, err = mutate.CollectReplicaEvidence(ctx, client.OmeV1beta1(), v, components, o.clock)
		if err != nil {
			return err
		}
	}
	var plan mutate.RolloutPlan
	if o.action == "promote" || o.action == "rollback" {
		plan, err = mutate.PrepareCanaryRollout(v, state, work, o.action, o.overrideAnalysis, o.yes, o.clock)
	} else {
		plan, err = mutate.PrepareRollout(v, state, work, o.action, o.discard, o.yes, o.clock)
	}
	if err != nil {
		return err
	}
	config, err := f.RESTConfig()
	if err != nil {
		return mutate.SafeAPIError(err)
	}
	if config == nil {
		return errors.New("action REST configuration is unavailable")
	}
	localConfig := *config
	config = &localConfig
	if config.Timeout <= 0 || config.Timeout > 10*time.Second {
		config.Timeout = 10 * time.Second
	}
	patchClient, err := transport.NewBounded(config, 1024*1024)
	if err != nil {
		return errors.New("construct guarded action transport failed")
	}
	if err = plan.WritePreview(o.streams.ErrOut, contextName, resolved.OMENamespace, dryRun); err != nil {
		return err
	}
	if err = mutate.Confirm(ctx, o.streams.In, o.streams.ErrOut, o.yes); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	result := reportv1alpha1.NewActionResult("rollout "+o.action, reportv1alpha1.ActionTarget{Kind: "InferenceService", Namespace: v.Namespace, Name: v.Name, UID: string(v.UID), ResourceVersion: v.ResourceVersion}, dryRun, o.clock)
	result.FollowUp = "kubectl ome rollout status " + name + " -n " + resolved.WorkloadNamespace + " --context=" + contextName
	result.RevisionHash = plan.RevisionHash()
	result.Message = "Validated locally; no patch sent."
	if dryRun != reportv1alpha1.DryRunClient {
		body, e := patchClient.JSONPatch(ctx, transport.Resource{Namespace: v.Namespace, Resource: "inferenceservices", Name: v.Name}, plan.Patch(), transport.JSONPatchOptions{DryRun: dryRun == reportv1alpha1.DryRunServer})
		if e != nil {
			if errors.Is(e, transport.ErrResponseIdentity) {
				return errors.New("API response is not bound to the request; outcome unknown, check rollout status")
			}
			if errors.Is(e, transport.ErrResponseTooLarge) {
				return errors.New("API response exceeds safety bounds; request outcome unknown, check rollout status")
			}
			return guardedPatchError(e)
		}
		if len(body) > 1024*1024 {
			return errors.New("API response exceeds safety bounds; request outcome unknown, check rollout status")
		}
		var response struct {
			APIVersion string `json:"apiVersion"`
			Kind       string `json:"kind"`
			Metadata   struct {
				Name            string `json:"name"`
				Namespace       string `json:"namespace"`
				UID             string `json:"uid"`
				ResourceVersion string `json:"resourceVersion"`
			} `json:"metadata"`
		}
		if e = json.Unmarshal(body, &response); e != nil || response.Kind != "InferenceService" || response.APIVersion != "ome.io/v1beta1" || response.Metadata.Name != v.Name || response.Metadata.Namespace != v.Namespace || response.Metadata.UID != string(v.UID) || !mutate.SafeScalar(response.Metadata.ResourceVersion) {
			return errors.New("API response is not bound to the request; outcome unknown, check rollout status")
		}
		result.Accepted = true
		result.Applied = dryRun == reportv1alpha1.DryRunNone
		result.Message = "API accepted annotation request; convergence not observed."
		if dryRun == reportv1alpha1.DryRunServer {
			result.Message = "API dry-run accepted; no changes persisted."
		}
	}
	if o.overrideAnalysis {
		result.Message = "Analysis override: " + result.Message
	}
	if err := report.Write(o.streams.Out, format, result); err != nil {
		return errors.New("write action result failed; check rollout status")
	}
	return nil
}

func guardedPatchError(err error) error {
	conflict := apierrors.IsConflict(err)
	var status apierrors.APIStatus
	if errors.As(err, &status) && status.Status().Code == 422 {
		s := status.Status()
		d := s.Details
		// The API server hides JSON Patch application causes behind this exact
		// generic status. It proves a guarded rejection, not which test failed.
		// Admission Invalid statuses carry resource/causes and must stay ordinary
		// API errors, even if their arbitrary messages quote JSON Patch text.
		conflict = s.Status == metav1.StatusFailure && s.Reason == metav1.StatusReasonInvalid && s.Message == "the server rejected our request due to an error in our request" && d != nil && d.Name == "" && d.Group == "" && d.Kind == "" && d.UID == "" && d.RetryAfterSeconds == 0 && len(d.Causes) == 0
	}
	if conflict {
		return &exitcode.PreconditionError{Err: errors.New("guarded annotation patch rejected; refresh rollout status and retry explicitly")}
	}
	return mutate.SafeAPIError(err)
}
