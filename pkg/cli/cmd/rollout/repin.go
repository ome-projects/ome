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

	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/mutate"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/transport"
	"sigs.k8s.io/ome/pkg/constants"
)

const rolloutRepinResponseLimit = 1024 * 1024

type repinOptions struct {
	streams genericiooptions.IOStreams
	clock   reportv1alpha1.Clock
	output  string
	dryRun  string
	yes     bool
}

func newRepinCmd(f factory.Factory, streams genericiooptions.IOStreams, clock reportv1alpha1.Clock) *cobra.Command {
	o := &repinOptions{streams: streams, clock: clock}
	cmd := &cobra.Command{
		Use:   "repin INFERENCESERVICE",
		Short: "Alpha: safely repin a drifted active rollout plan",
		Long: `Alpha guarded repin for one active InferenceService rollout run.
The command issues one bounded uncached GET and zero related-resource reads.
It requires a non-empty valid active run, current ready/drift evidence, the
same live and pinned topology, and at most one canary group; empty plans and
topology changes fail closed.

The annotation value is the exact current combined progression-render digest;
this command never sends the unsafe literal "now". The request uses one PATCH
guarded by the exact UID and resourceVersion, with no automatic retry. The
controller performs its own live render and digest CAS, which is not a full-plan
or topology CAS. The CLI separately refuses topology changes. The controller
preserves run identity and progress, and may clamp a canary into a pre-step
hold. API acceptance is not controller convergence.

Client dry-run performs the read, proof, preview and confirmation without a
PATCH. Server dry-run sends the identical guarded PATCH with dryRun=All.
Preview and prompt use stderr; stdout is one typed ActionResult. The command
uses a 45-second context and caps each API request at 10 seconds. Responses
are bounded, redirects are refused, and an ambiguous mutation outcome is
never retried.`,
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errors.New("exactly one inference service is required")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			format, wide, err := parseRepinOutput(o.output)
			if err != nil {
				return errors.New("invalid rollout repin flags; use --help")
			}
			mode := reportv1alpha1.DryRunMode(o.dryRun)
			if mode != reportv1alpha1.DryRunNone && mode != reportv1alpha1.DryRunClient && mode != reportv1alpha1.DryRunServer {
				return errors.New("invalid rollout repin flags; use --help")
			}
			if !mutate.SafeScalar(args[0]) || len(validation.IsDNS1123Subdomain(args[0])) != 0 {
				return ErrInvalidInferenceServiceName
			}
			return o.run(cmd.Context(), f, args[0], format, wide, mode)
		},
	}
	cmd.SetFlagErrorFunc(func(*cobra.Command, error) error {
		return errors.New("invalid rollout repin flags; use --help")
	})
	cmd.Flags().BoolVar(&o.yes, "yes", false, "Confirm the exact preview without an interactive prompt")
	cmd.Flags().StringVar(&o.dryRun, "dry-run", "none", "Dry-run mode: none, client or server")
	cmd.Flags().StringVarP(&o.output, "output", "o", "table", "Output: table, wide (bounded), json or yaml")
	return cmd
}

func parseRepinOutput(value string) (report.Format, bool, error) {
	format, wide, err := parseRolloutOutput(value)
	if err != nil {
		return "", false, errors.New("output must be table, wide, json or yaml")
	}
	return format, wide, nil
}

func (o *repinOptions) run(parent context.Context, f factory.Factory, name string, format report.Format, wide bool, mode reportv1alpha1.DryRunMode) error {
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
	config, err := f.RESTConfig()
	if err != nil {
		return mutate.SafeAPIError(err)
	}
	if config == nil {
		return errors.New("rollout repin REST configuration is unavailable")
	}
	localConfig := *config
	config = &localConfig
	if config.Timeout <= 0 || config.Timeout > 10*time.Second {
		config.Timeout = 10 * time.Second
	}
	client, err := transport.NewBounded(config, rolloutRepinResponseLimit)
	if err != nil {
		return errors.New("construct guarded rollout repin transport failed")
	}

	readCtx, stopRead := context.WithTimeout(ctx, 10*time.Second)
	service, err := client.GetInferenceService(readCtx, namespace, name, metav1.GetOptions{})
	readErr := readCtx.Err()
	stopRead()
	if err != nil {
		switch {
		case errors.Is(err, transport.ErrResponseTooLarge):
			return errors.New("API read response exceeds safety bounds; no request submitted")
		case errors.Is(err, transport.ErrResponseIdentity):
			return errors.New("API read response is not safely bound to the request; no request submitted")
		default:
			return mutate.SafeAPIError(err)
		}
	}
	if readErr != nil {
		return readErr
	}
	if service == nil || service.Name != name || service.Namespace != namespace {
		return errors.New("action target response does not match exact request")
	}
	plan, err := mutate.PrepareRolloutRepin(service, o.clock)
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

	result := reportv1alpha1.NewActionResult("rollout repin", plan.Target(), mode, o.clock)
	details := plan.Details()
	result.Rollout = &details
	result.FollowUp = "kubectl ome rollout explain " + name + " -n " + namespace + " --context=" + contextName
	result.Message = "Validated locally; no patch sent. Controller repin was not observed."
	if mode != reportv1alpha1.DryRunClient {
		body, patchErr := client.JSONPatch(ctx, transport.Resource{
			Namespace: service.Namespace, Resource: "inferenceservices", Name: service.Name,
		}, plan.Patch(), transport.JSONPatchOptions{DryRun: mode == reportv1alpha1.DryRunServer})
		if patchErr != nil {
			return rolloutRepinPatchError(patchErr)
		}
		if err := validateRolloutRepinResponse(body, plan.Target(), service.Generation, details.RequestedPlanDigest); err != nil {
			return err
		}
		result.Accepted = true
		result.Applied = mode == reportv1alpha1.DryRunNone
		result.Message = "API accepted repin annotation; controller consumption and plan replacement were not observed."
		if mode == reportv1alpha1.DryRunServer {
			result.Message = "API dry-run accepted; no changes persisted."
		}
	}
	if wide {
		if err := result.Canonical().WideTable().Write(o.streams.Out); err != nil {
			return rolloutRepinWriteError(mode)
		}
		return nil
	}
	if err := report.Write(o.streams.Out, format, result); err != nil {
		return rolloutRepinWriteError(mode)
	}
	return nil
}

func rolloutRepinPatchError(err error) error {
	if errors.Is(err, transport.ErrResponseTooLarge) {
		return errors.New("API response exceeds safety bounds; outcome unknown, do not replay, check rollout explain")
	}
	if errors.Is(err, transport.ErrResponseIdentity) {
		return errors.New("API response is not safely bound to the request; outcome unknown, do not replay, check rollout explain")
	}
	var status apierrors.APIStatus
	if errors.As(err, &status) {
		code := status.Status().Code
		if code < 400 || code >= 500 {
			return errors.New("rollout repin request outcome unknown; do not replay, check rollout explain")
		}
		return mutate.GuardedAnnotationPatchError(err, "rollout explain")
	}
	return errors.New("rollout repin request outcome unknown; do not replay, check rollout explain")
}

func rolloutRepinWriteError(mode reportv1alpha1.DryRunMode) error {
	if mode == reportv1alpha1.DryRunClient {
		return errors.New("write rollout repin result failed; no patch sent")
	}
	return errors.New("write rollout repin result failed after request outcome became unknown; do not replay, check rollout explain")
}

func validateRolloutRepinResponse(body []byte, target reportv1alpha1.ActionTarget, generation int64, digest string) error {
	if len(body) == 0 || len(body) > rolloutRepinResponseLimit {
		return errors.New("API response exceeds safety bounds; outcome unknown, do not replay, check rollout explain")
	}
	var response struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
		Metadata   struct {
			Name            string            `json:"name"`
			Namespace       string            `json:"namespace"`
			UID             string            `json:"uid"`
			ResourceVersion string            `json:"resourceVersion"`
			Generation      int64             `json:"generation"`
			Annotations     map[string]string `json:"annotations"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(body, &response); err != nil ||
		response.APIVersion != "ome.io/v1beta1" || response.Kind != "InferenceService" ||
		response.Metadata.Name != target.Name || response.Metadata.Namespace != target.Namespace ||
		response.Metadata.UID != target.UID || !mutate.SafeScalar(response.Metadata.ResourceVersion) ||
		response.Metadata.Generation != generation || response.Metadata.Annotations[constants.RolloutRepinAnnotation] != digest {
		return errors.New("API response is not bound to the exact rollout repin request; outcome unknown, do not replay, check rollout explain")
	}
	return nil
}
