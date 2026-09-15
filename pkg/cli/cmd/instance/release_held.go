package instance

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/kubernetes"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/effective"
	"sigs.k8s.io/ome/pkg/cli/exitcode"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/mutate"
	"sigs.k8s.io/ome/pkg/cli/namespace"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/transport"
	"sigs.k8s.io/ome/pkg/client/clientset/versioned"
)

type releaseHeldOptions struct {
	streams                             genericiooptions.IOStreams
	clock                               reportv1alpha1.Clock
	namespaces                          *namespace.Options
	component, revision, output, dryRun string
	yes                                 bool
}

func newReleaseHeldCmd(f factory.Factory, streams genericiooptions.IOStreams, clock reportv1alpha1.Clock) *cobra.Command {
	o := &releaseHeldOptions{streams: streams, clock: clock, namespaces: namespace.NewOptions()}
	cmd := &cobra.Command{Use: "release-held INFERENCESERVICE", Short: "Alpha: request release of one exact Held revision", Long: `Alpha guarded OMENative retry-authority mailbox request on one owned InferenceReplica.
Requires a complete bounded source collection, current native IR status, exact
parent-generation stamp, existing controller-write=true and an absent mailbox.
Revision is a full scoped target name or unambiguous eight-lowercase-hex hash.
--yes bypasses only confirmation, never eligibility or finite revalidation.

Client dry-run reads, previews, confirms and refreshes without PATCH. Server
dry-run sends the same UID/resourceVersion JSON Patch with dryRun=All. Table
and wide both use the bounded ActionResult table. Preview/prompt use stderr.

API acceptance is not controller release, retry, garbage collection or restored
availability. Release does not resume a paused workload. The actor has no durable
request-correlated receipt; historical retry records may also be retention-pruned.
The actor checks Held on its initial snapshot; fresh removal does not recheck it.
Mailbox consumption is not compare-and-delete of its handled value or original UID.
Later mailbox/block absence on that original UID is consistent with release, not
proof this request caused it; natural pruning and a later re-Held can race.
IR CAS is not a transaction across parent, runtime and related membership, so a
post-refresh multi-object race remains. No implicit wait or automatic replay.

The action context is capped at 45 seconds and each API call at 10 seconds,
preserving shorter REST request timeout. Source payload budgets are post-decode
typed admission before resolver copies, not hard initial wire allocation bounds.
External credential plugins/custom transports may not honor cancellation.`, Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		format := report.Format(o.output)
		if o.output != "table" && o.output != "wide" && o.output != "json" && o.output != "yaml" {
			return errors.New("output must be table, wide, json or yaml")
		}
		if o.output == "wide" {
			format = report.FormatTable
		}
		mode := reportv1alpha1.DryRunMode(o.dryRun)
		if mode != reportv1alpha1.DryRunNone && mode != reportv1alpha1.DryRunClient && mode != reportv1alpha1.DryRunServer {
			return errors.New("dry-run must be none, client or server")
		}
		if o.component != "engine" && o.component != "decoder" && o.component != "router" {
			return errors.New("component must be engine, decoder or router")
		}
		if !mutate.SafeScalar(args[0]) || len(validation.IsDNS1123Subdomain(args[0])) != 0 || !mutate.SafeScalar(o.revision) || len(validation.IsDNS1123Subdomain(o.revision)) != 0 {
			return errors.New("InferenceService and revision identities are required and must be safe")
		}
		if !mutate.SafeScalar(o.namespaces.OMENamespace) || len(validation.IsDNS1123Label(o.namespaces.OMENamespace)) != 0 {
			return errors.New("action OME namespace is invalid")
		}
		return o.run(cmd.Context(), f, args[0], format, mode)
	}}
	cmd.SetFlagErrorFunc(func(*cobra.Command, error) error { return errors.New("invalid held-release action flags; use --help") })
	cmd.Flags().StringVar(&o.component, "component", "", "Component: engine, decoder or router (required)")
	cmd.Flags().StringVar(&o.revision, "revision", "", "Exact full target revision or eight-lowercase-hex hash (required)")
	cmd.Flags().BoolVar(&o.yes, "yes", false, "Confirm the exact preview without an interactive prompt")
	cmd.Flags().StringVar(&o.dryRun, "dry-run", "none", "Dry-run mode: none, client or server")
	cmd.Flags().StringVarP(&o.output, "output", "o", "table", "Output: table, wide (bounded), json or yaml")
	o.namespaces.AddOMEFlags(cmd.Flags())
	return cmd
}

func (o *releaseHeldOptions) run(parent context.Context, f factory.Factory, name string, format report.Format, dryRun reportv1alpha1.DryRunMode) error {
	ctx, cancel := context.WithTimeout(parent, 45*time.Second)
	defer cancel()
	workloadNS, _, err := f.Namespace()
	if err != nil {
		return errors.New("resolve workload namespace failed")
	}
	resolved, err := o.namespaces.Resolve(workloadNS)
	if err != nil {
		return errors.New("held-release action namespaces are invalid")
	}
	contextResolver, ok := f.(factory.ContextResolver)
	if !ok {
		return errors.New("selected context is unavailable for action preview")
	}
	contextName, err := contextResolver.ContextName()
	if err != nil || !mutate.SafeScalar(contextName) {
		return errors.New("selected context is unavailable or unsafe")
	}
	config, err := f.RESTConfig()
	if err != nil || config == nil {
		return errors.New("held-release REST configuration is unavailable")
	}
	// Only Timeout is changed here. Leave auth fields caller-owned; the shared
	// bounded mutation constructor makes its first private provider copy.
	localConfig := *config
	config = &localConfig
	if config.Timeout <= 0 || config.Timeout > 10*time.Second {
		config.Timeout = 10 * time.Second
	}
	var client versioned.Interface
	if owned, ok := f.(factory.ActionReadClientsResolver); ok {
		client, err = owned.OMEClientForAction(ctx)
	} else {
		client, err = f.OMEClient()
	}
	if err != nil {
		return mutate.SafeAPIError(err)
	}
	if client == nil {
		return errors.New("held-release OME client is unavailable")
	}
	var kube kubernetes.Interface
	if owned, ok := f.(factory.ActionReadClientsResolver); ok {
		kube, err = owned.KubeClientForAction(ctx)
	} else {
		kube, err = f.KubeClient()
	}
	if err != nil {
		return mutate.SafeAPIError(err)
	}
	if kube == nil {
		return errors.New("held-release Kubernetes client is unavailable")
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
	// A fresh resolver/binding per pass prevents cached first-pass sources from
	// being reused as current authority after confirmation.
	read := func() (*v1beta1.InferenceService, *effective.RuntimeState, mutate.HeldReleaseEvidence, *mutate.HeldRuntimeBinding, error) {
		call, stop := context.WithTimeout(ctx, config.Timeout)
		v, e := client.OmeV1beta1().InferenceServices(resolved.WorkloadNamespace).Get(call, name, metav1.GetOptions{})
		callErr := call.Err()
		stop()
		if callErr != nil {
			return nil, nil, mutate.HeldReleaseEvidence{}, nil, callErr
		}
		if e != nil {
			return nil, nil, mutate.HeldReleaseEvidence{}, nil, mutate.SafeAPIError(e)
		}
		if e = mutate.ValidateTarget(v); e != nil {
			return nil, nil, mutate.HeldReleaseEvidence{}, nil, e
		}
		if v.Name != name || v.Namespace != resolved.WorkloadNamespace {
			return nil, nil, mutate.HeldReleaseEvidence{}, nil, mutate.ErrUnsafeTarget
		}
		resolver, binding, e := mutate.NewHeldReleaseRuntimeResolver(kube.AppsV1(), runtimeClient, resolved.OMENamespace, config.Timeout)
		if e != nil {
			return nil, nil, mutate.HeldReleaseEvidence{}, nil, e
		}
		state, e := resolver.Resolve(ctx, v, effective.RuntimeResolveOptions{})
		if ctx.Err() != nil {
			return nil, nil, mutate.HeldReleaseEvidence{}, nil, ctx.Err()
		}
		if e != nil {
			return nil, nil, mutate.HeldReleaseEvidence{}, nil, mutate.SafeAPIError(e)
		}
		if _, e = mutate.RequireNativeRuntime(v, state); e != nil {
			return nil, nil, mutate.HeldReleaseEvidence{}, nil, e
		}
		evidence, e := mutate.CollectHeldReleaseEvidence(ctx, client.OmeV1beta1(), v, o.component, o.clock, config.Timeout)
		return v, state, evidence, binding, e
	}
	v, state, evidence, binding, err := read()
	if err != nil {
		return err
	}
	plan, err := mutate.PrepareHeldRelease(v, state, evidence, o.component, o.revision, o.clock)
	if err != nil {
		return err
	}
	patchClient, err := transport.NewBounded(config, 1024*1024)
	if err != nil {
		return errors.New("construct held-release transport failed")
	}
	if err = plan.WritePreview(o.streams.ErrOut, contextName, resolved.OMENamespace, dryRun); err != nil {
		return err
	}
	if err = mutate.Confirm(ctx, o.streams.In, o.streams.ErrOut, o.yes); err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	fresh, freshState, freshEvidence, freshBinding, err := read()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil || !plan.MatchesRefresh(fresh, freshState, freshEvidence) || !binding.Matches(freshBinding) {
		return &exitcode.PreconditionError{Err: errors.New("held-release preview became stale; inspect instance retry-blocks and retry explicitly")}
	}
	result := reportv1alpha1.NewActionResult("instance release-held", plan.Target(), dryRun, o.clock)
	result.RevisionHash = plan.RevisionHash()
	result.FollowUp = "kubectl ome instance retry-blocks " + name + " --component=" + o.component + " -n " + resolved.WorkloadNamespace + " --context=" + contextName
	result.Message = "Validated locally; no patch sent."
	if dryRun != reportv1alpha1.DryRunClient {
		body, e := patchClient.JSONPatch(ctx, transport.Resource{Namespace: plan.Target().Namespace, Resource: "inferencereplicas", Name: plan.Target().Name}, plan.Patch(), transport.JSONPatchOptions{DryRun: dryRun == reportv1alpha1.DryRunServer})
		if e != nil {
			classified := mutate.GuardedPatchError(e)
			if exitcode.FromError(classified) == exitcode.MutationConflict {
				return &exitcode.PreconditionError{Err: errors.New("guarded held-release rejected; inspect instance retry-blocks")}
			}
			return errors.New("held-release request outcome unknown; inspect instance retry-blocks before another explicit request")
		}
		if ctx.Err() != nil || !heldReleaseResponseMatches(body, plan.Target()) {
			return errors.New("held-release API response is unbound or unavailable; outcome unknown, inspect instance retry-blocks")
		}
		result.Accepted = true
		result.Applied = dryRun == reportv1alpha1.DryRunNone
		result.Message = "API accepted release annotation request; controller release/convergence not observed."
		if dryRun == reportv1alpha1.DryRunServer {
			result.Message = "API dry-run accepted; no changes persisted."
		}
	}
	if err = report.Write(o.streams.Out, format, result); err != nil {
		return errors.New("write held-release result failed; request outcome may be unknown, inspect instance retry-blocks")
	}
	return nil
}

func heldReleaseResponseMatches(body []byte, target reportv1alpha1.ActionTarget) bool {
	if len(body) > 1024*1024 {
		return false
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
	return json.Unmarshal(body, &response) == nil && response.APIVersion == "ome.io/v1beta1" && response.Kind == "InferenceReplica" && response.Metadata.Name == target.Name && response.Metadata.Namespace == target.Namespace && response.Metadata.UID == target.UID && mutate.SafeScalar(response.Metadata.ResourceVersion)
}
