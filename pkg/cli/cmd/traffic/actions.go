package traffic

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/mutate"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/transport"
	"sigs.k8s.io/ome/pkg/client/clientset/versioned"
)

const trafficActionResponseLimit = 1024 * 1024

type actionOptions struct {
	streams         genericiooptions.IOStreams
	clock           reportv1alpha1.Clock
	action          string
	overrideID      string
	workloadCluster string
	reason          string
	dryRun, output  string
	yes             bool
}

func newActionCmd(f factory.Factory, streams genericiooptions.IOStreams, clock reportv1alpha1.Clock, action string) *cobra.Command {
	o := &actionOptions{streams: streams, clock: clock, action: action}
	cmd := &cobra.Command{
		Use:   action + " INFERENCESERVICE",
		Short: "Alpha: " + action + " one cross-cluster traffic arm",
		Long: `Alpha guarded traffic action on one control-plane InferenceService.
Drain adds one independently removable ome.io/traffic-drain override. Undrain
removes exactly one existing ID. Existing annotation JSON is parsed strictly;
unknown fields, duplicates, unsafe values, and bounded-state overflow refuse.

Drain requires --workload-cluster to identify the WorkloadCluster to drain.
The global --cluster flag selects the Kubernetes API cluster from kubeconfig;
it is not an alias for --workload-cluster.

The request tests the exact InferenceService UID and resourceVersion. It
preserves unrelated annotations and every other override ID. Confirmation or
--yes is required. Client dry-run reads, previews, and confirms without PATCH;
server dry-run sends the identical guarded JSON Patch with dryRun=All.

Preview and prompt use stderr; stdout is one ActionResult. API acceptance is
not TrafficMap convergence or data-plane realization; follow up with traffic
status. The action uses a 45-second context and each request is capped at
10 seconds, preserving a shorter --request-timeout. Responses are bounded and
redirects are refused; credential plugins/custom transports may not honor
cancellation.`,
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errors.New("exactly one inference service is required")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			format, wide, err := parseActionOutput(o.output)
			if err != nil {
				return err
			}
			mode := reportv1alpha1.DryRunMode(o.dryRun)
			if mode != reportv1alpha1.DryRunNone && mode != reportv1alpha1.DryRunClient && mode != reportv1alpha1.DryRunServer {
				return errors.New("dry-run must be none, client or server")
			}
			if !mutate.SafeScalar(args[0]) || len(validation.IsDNS1123Subdomain(args[0])) != 0 {
				return errors.New("invalid inference service name")
			}
			if action == "drain" && o.workloadCluster == "" {
				return errors.New("traffic drain requires --workload-cluster; --cluster selects the Kubernetes API cluster")
			}
			request := mutate.TrafficDrainRequest{Action: action, ID: o.overrideID, Cluster: o.workloadCluster, Reason: o.reason}
			if err := mutate.ValidateTrafficDrainRequest(request); err != nil {
				return err
			}
			return o.run(cmd.Context(), f, args[0], request, format, wide, mode)
		},
	}
	cmd.SetFlagErrorFunc(func(*cobra.Command, error) error {
		return errors.New("invalid traffic action flags; use --help")
	})
	cmd.Flags().StringVar(&o.overrideID, "id", "", "DNS-1123 override ID (required)")
	if action == "drain" {
		cmd.Flags().StringVar(&o.workloadCluster, "workload-cluster", "", "WorkloadCluster DNS name to drain (required)")
		cmd.Flags().StringVar(&o.reason, "reason", "", "Bounded non-secret operator reason (required)")
	}
	cmd.Flags().BoolVar(&o.yes, "yes", false, "Confirm the exact preview without an interactive prompt")
	cmd.Flags().StringVar(&o.dryRun, "dry-run", "none", "Dry-run mode: none, client or server")
	cmd.Flags().StringVarP(&o.output, "output", "o", "table", "Output: table, wide (bounded), json or yaml")
	return cmd
}

func (o *actionOptions) run(parent context.Context, f factory.Factory, name string, request mutate.TrafficDrainRequest, format report.Format, wide bool, mode reportv1alpha1.DryRunMode) error {
	ctx, cancel := context.WithTimeout(parent, 45*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	namespace, _, err := f.Namespace()
	if err != nil {
		return errors.New("resolve workload namespace failed")
	}
	if !mutate.SafeScalar(namespace) || len(validation.IsDNS1123Label(namespace)) != 0 {
		return errors.New("resolved workload namespace is invalid")
	}
	contextResolver, ok := f.(factory.ContextResolver)
	if !ok {
		return errors.New("selected kubeconfig context is unavailable for action preview")
	}
	contextName, err := contextResolver.ContextName()
	if err != nil || !mutate.SafeScalar(contextName) {
		return errors.New("selected kubeconfig context is unavailable or unsafe")
	}

	getOME := f.OMEClient
	if owned, ok := f.(factory.ActionReadClientsResolver); ok {
		getOME = func() (versioned.Interface, error) { return owned.OMEClientForAction(ctx) }
	}
	client, err := getOME()
	if err != nil {
		return mutate.SafeAPIError(err)
	}
	if client == nil {
		return errors.New("traffic action OME client is unavailable")
	}
	readCtx, stopRead := context.WithTimeout(ctx, 10*time.Second)
	service, err := client.OmeV1beta1().InferenceServices(namespace).Get(readCtx, name, metav1.GetOptions{})
	readErr := readCtx.Err()
	stopRead()
	if err != nil {
		return mutate.SafeAPIError(err)
	}
	if readErr != nil {
		return readErr
	}
	if service == nil || service.Name != name || service.Namespace != namespace {
		return errors.New("action target response does not match exact request")
	}
	plan, err := mutate.PrepareTrafficDrain(service, request)
	if err != nil {
		return err
	}
	if err = plan.WritePreview(o.streams.ErrOut, contextName, mode); err != nil {
		return err
	}
	if err = mutate.Confirm(ctx, o.streams.In, o.streams.ErrOut, o.yes); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}

	result := reportv1alpha1.NewActionResult("traffic "+o.action, plan.Target(), mode, o.clock)
	details := plan.Details()
	result.Traffic = &details
	result.FollowUp = "kubectl ome traffic status " + name + " -n " + namespace + " --context=" + contextName
	result.Message = "Validated locally; no patch sent. TrafficMap convergence was not observed."
	if mode != reportv1alpha1.DryRunClient {
		config, configErr := f.RESTConfig()
		if configErr != nil {
			return mutate.SafeAPIError(configErr)
		}
		if config == nil {
			return errors.New("traffic action REST configuration is unavailable")
		}
		localConfig := *config
		config = &localConfig
		if config.Timeout <= 0 || config.Timeout > 10*time.Second {
			config.Timeout = 10 * time.Second
		}
		patchClient, clientErr := transport.NewBounded(config, trafficActionResponseLimit)
		if clientErr != nil {
			return errors.New("construct guarded traffic action transport failed")
		}
		body, patchErr := patchClient.JSONPatch(ctx, transport.Resource{
			Namespace: service.Namespace, Resource: "inferenceservices", Name: service.Name,
		}, plan.Patch(), transport.JSONPatchOptions{DryRun: mode == reportv1alpha1.DryRunServer})
		if patchErr != nil {
			if errors.Is(patchErr, transport.ErrResponseIdentity) || errors.Is(patchErr, transport.ErrResponseTooLarge) {
				return errors.New("API response is not safely bound to the request; outcome unknown, check traffic status")
			}
			return mutate.GuardedAnnotationPatchError(patchErr, "traffic status")
		}
		if err := validateTrafficActionResponse(body, plan.Target()); err != nil {
			return err
		}
		result.Accepted = true
		result.Applied = mode == reportv1alpha1.DryRunNone
		result.Message = "API accepted traffic annotation request; not TrafficMap convergence."
		if mode == reportv1alpha1.DryRunServer {
			result.Message = "API dry-run accepted; no changes persisted."
		}
	}
	if wide {
		if err := result.Canonical().WideTable().Write(o.streams.Out); err != nil {
			return errors.New("write traffic action result failed; check traffic status")
		}
		return nil
	}
	if err := report.Write(o.streams.Out, format, result); err != nil {
		return errors.New("write traffic action result failed; check traffic status")
	}
	return nil
}

func parseActionOutput(value string) (report.Format, bool, error) {
	if value == "wide" {
		return report.FormatTable, true, nil
	}
	format, err := report.ParseFormat(value)
	if err != nil {
		return "", false, errors.New("output must be table, wide, json or yaml")
	}
	return format, false, nil
}

func validateTrafficActionResponse(body []byte, target reportv1alpha1.ActionTarget) error {
	if len(body) == 0 || len(body) > trafficActionResponseLimit {
		return errors.New("API response exceeds safety bounds; outcome unknown, check traffic status")
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
	if err := json.Unmarshal(body, &response); err != nil ||
		response.APIVersion != "ome.io/v1beta1" || response.Kind != "InferenceService" ||
		response.Metadata.Name != target.Name || response.Metadata.Namespace != target.Namespace ||
		response.Metadata.UID != target.UID || !mutate.SafeScalar(response.Metadata.ResourceVersion) {
		return errors.New("API response is not bound to the request; outcome unknown, check traffic status")
	}
	return nil
}
