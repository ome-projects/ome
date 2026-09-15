package migration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/transport"
	"sigs.k8s.io/ome/pkg/client/clientset/versioned"
)

type startOptions struct {
	streams                             genericiooptions.IOStreams
	namespaces                          *namespace.Options
	component, instance, output, dryRun string
	request                             mutate.MigrationOptions
	yes                                 bool
}

func newStartCmd(f factory.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	o := &startOptions{streams: streams, namespaces: namespace.NewOptions()}
	cmd := &cobra.Command{Use: "start INFERENCESERVICE", Short: "Alpha: request one guarded OMENative migration", Long: `Request one OMENative migration by annotation, never by spec, status or scale.
Requires an explicit component and canonical instance index, current native
runtime/IR evidence and a complete owned current-revision Pod set. --from-node
is required for a gang spanning nodes. Node hints are soft preferences.

A new UUID is generated once. --request-id is lookup-only: an identical retained
v1 mailbox is reported without replay; lossy history or an unseen/pruned UUID
refuses. Acceptance is not scheduling, delivery or controller convergence.
Preview and confirmation use stderr; only ActionResult uses stdout. Client
dry-run still confirms and rechecks source evidence, but sends no patch. Server
dry-run sends the same UID/resourceVersion guarded patch with dryRun=All.
The action has a 45-second context and each request a 10-second cap. Credential
plugins/custom transports may not honor cancellation. Safety collection caps
32 IRs, 2048 instances/256 migrations per IR and 128 source Pods; GET inspection
bounds are post-decode, not HTTP response body limits.`, Args: func(_ *cobra.Command, args []string) error {
		if len(args) != 1 {
			return errors.New("exactly one inference service is required")
		}
		return nil
	}, RunE: func(cmd *cobra.Command, args []string) error {
		if o.component == "" {
			return errors.New("component is required")
		}
		index, err := mutate.ParseMigrationIndex(o.instance)
		if err != nil {
			return err
		}
		o.request.Component, o.request.Instance = v1beta1.ComponentType(o.component), index
		if err = mutate.ValidateMigrationOptions(o.request); err != nil {
			return err
		}
		if !mutate.SafeScalar(args[0]) || len(validation.IsDNS1123Subdomain(args[0])) != 0 {
			return errors.New("invalid inference service name")
		}
		if _, err = o.namespaces.Resolve("validation"); err != nil {
			return errors.New("action namespaces are invalid")
		}
		format := report.FormatTable
		if o.output != "wide" {
			format, err = report.ParseFormat(o.output)
		}
		if err != nil {
			return errors.New("output must be table, wide, json or yaml")
		}
		mode := reportv1alpha1.DryRunMode(o.dryRun)
		if mode != reportv1alpha1.DryRunNone && mode != reportv1alpha1.DryRunClient && mode != reportv1alpha1.DryRunServer {
			return errors.New("dry-run must be none, client or server")
		}
		return o.run(cmd.Context(), f, args[0], format, mode)
	}}
	cmd.SetFlagErrorFunc(func(*cobra.Command, error) error { return errors.New("invalid migration action flags; use --help") })
	cmd.Flags().StringVar(&o.component, "component", "", "Component: engine, decoder or router (required)")
	cmd.Flags().StringVar(&o.instance, "instance", "", "Canonical instance index (required)")
	cmd.Flags().StringVar(&o.request.FromNode, "from-node", "", "Source node hosting a current Pod; required for a multi-node gang")
	cmd.Flags().StringSliceVar(&o.request.HintNodes, "hint-node", nil, "Ordered soft target-node preferences (maximum eight)")
	cmd.Flags().StringVar(&o.request.Reason, "reason", "", "Bounded non-secret advisory reason")
	cmd.Flags().StringVar(&o.request.RequestedBy, "requested-by", "kubectl-ome", "Bounded advisory tool/operator label, not authenticated identity")
	cmd.Flags().StringVar(&o.request.RequestID, "request-id", "", "Lookup-only canonical UUID; never regenerated or replayed")
	cmd.Flags().BoolVar(&o.yes, "yes", false, "Confirm the exact preview without an interactive prompt")
	cmd.Flags().StringVar(&o.dryRun, "dry-run", "none", "Dry-run: none, client or server")
	cmd.Flags().StringVarP(&o.output, "output", "o", "table", "Output: table, wide (bounded), json or yaml")
	o.namespaces.AddOMEFlags(cmd.Flags())
	return cmd
}

func (o *startOptions) run(parent context.Context, f factory.Factory, name string, format report.Format, mode reportv1alpha1.DryRunMode) error {
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
		return errors.New("selected context is unavailable for action preview")
	}
	contextName, err := contextResolver.ContextName()
	if err != nil || !mutate.SafeScalar(contextName) {
		return errors.New("selected context is unavailable or unsafe")
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
		return errors.New("migration action OME client is unavailable")
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
		return errors.New("migration action Kubernetes client is unavailable")
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
	evidence, err := mutate.CollectMigrationEvidence(ctx, client.OmeV1beta1(), kube, v, components, o.request, nil)
	if err != nil {
		return err
	}
	plan, err := mutate.PrepareMigration(v, state, evidence, o.request, nil, nil)
	if err != nil {
		return err
	}
	if err = plan.WritePreview(o.streams.ErrOut, contextName, resolved.OMENamespace, mode); err != nil {
		return err
	}
	result := reportv1alpha1.NewActionResult("migration start", reportv1alpha1.ActionTarget{Kind: "InferenceService", Namespace: v.Namespace, Name: v.Name, UID: string(v.UID), ResourceVersion: v.ResourceVersion}, mode, nil)
	result.RequestID = plan.RequestID()
	result.FollowUp = "kubectl ome migration status " + name + " --component=" + string(o.request.Component) + " -n " + resolved.WorkloadNamespace + " --context=" + contextName
	if plan.Existing() {
		result.Message = "Identical retained mailbox observed; no replay sent; acceptance and convergence not observed."
	} else {
		if err = mutate.Confirm(ctx, o.streams.In, o.streams.ErrOut, o.yes); err != nil {
			return err
		}
		if err = mutate.RecheckMigration(ctx, client.OmeV1beta1(), kube, v, plan, nil); err != nil {
			return err
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		result.Message = "Validated locally; no patch sent."
		if mode != reportv1alpha1.DryRunClient {
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
			body, err := patchClient.JSONPatch(ctx, transport.Resource{Namespace: v.Namespace, Resource: "inferenceservices", Name: v.Name}, plan.Patch(), transport.JSONPatchOptions{DryRun: mode == reportv1alpha1.DryRunServer})
			if err != nil {
				if errors.Is(err, transport.ErrResponseTooLarge) {
					return errors.New("API response exceeds bounds; outcome unknown; inspect migration status using the preview UUID")
				}
				if errors.Is(err, transport.ErrResponseIdentity) {
					return errors.New("API response is not bound to request; outcome unknown; inspect migration status using the preview UUID")
				}
				classified := mutate.GuardedPatchError(err)
				if exitcode.FromError(classified) == exitcode.MutationConflict {
					return &exitcode.PreconditionError{Err: errors.New("guarded migration rejected; inspect migration status using preview UUID")}
				}
				return fmt.Errorf("migration request failed or outcome unknown; inspect migration status using the preview UUID: %w", classified)
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
			if len(body) > 1024*1024 || json.Unmarshal(body, &response) != nil || response.APIVersion != "ome.io/v1beta1" || response.Kind != "InferenceService" || response.Metadata.Name != v.Name || response.Metadata.Namespace != v.Namespace || response.Metadata.UID != string(v.UID) || !mutate.SafeScalar(response.Metadata.ResourceVersion) {
				return errors.New("API response is not bound to request; outcome unknown; inspect migration status using the preview UUID")
			}
			result.Accepted, result.Applied = true, mode == reportv1alpha1.DryRunNone
			result.Message = "API accepted annotation request; delivery and convergence not observed."
			if mode == reportv1alpha1.DryRunServer {
				result.Message = "API dry-run accepted; no changes persisted."
			}
		}
	}
	if err = report.Write(o.streams.Out, format, result); err != nil {
		return errors.New("write action result failed; inspect migration status using the preview UUID")
	}
	return nil
}
