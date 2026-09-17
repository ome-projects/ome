package runtime

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"time"

	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/kubernetes"
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

func newSyncCmd(f factory.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	var output, dryRun string
	var yes bool
	namespaces := namespace.NewOptions()
	c := &cobra.Command{Use: "sync INFERENCESERVICE", Short: "Alpha: request guarded managed runtime pin advancement", Args: func(_ *cobra.Command, args []string) error {
		if len(args) != 1 {
			return errors.New("runtime sync requires exactly one InferenceService name")
		}
		return nil
	}, RunE: func(c *cobra.Command, args []string) error {
		if output != "table" && output != "wide" && output != "json" && output != "yaml" {
			return errors.New("output must be table, wide, json or yaml")
		}
		if dryRun != "none" && dryRun != "client" && dryRun != "server" {
			return errors.New("dry-run must be none, client or server")
		}
		if !mutate.SafeScalar(args[0]) || len(validation.IsDNS1123Subdomain(args[0])) != 0 {
			return errors.New("InferenceService name is invalid")
		}
		if _, err := namespaces.Resolve("default"); err != nil {
			return errors.New("runtime sync namespaces are invalid")
		}
		return runRuntimeSync(c.Context(), f, streams, namespaces, args[0], output, reportv1alpha1.DryRunMode(dryRun), yes)
	}}
	c.SetFlagErrorFunc(func(*cobra.Command, error) error { return errors.New("invalid runtime sync flags; use --help") })
	c.Flags().BoolVar(&yes, "yes", false, "Confirm the exact preview without an interactive prompt")
	c.Flags().StringVar(&dryRun, "dry-run", "none", "Dry-run: none, client or server")
	c.Flags().StringVarP(&output, "output", "o", "table", "Output: table, wide, json or yaml")
	namespaces.AddOMEFlags(c.Flags())
	c.Long = `Alpha: request advancement of an eligible autoSync=false managed runtime pin.
Requires bound source, pin, drift, bounded revision history and no rollback,
held, pending or active work. Portable services do not require fictitious IRs.
Preview and confirmation use stderr; stdout is one typed ActionResult.
The token requests the controller's latest live runtime when consumed:
previewed content is not locked. Acceptance is not convergence or readiness.
Client dry-run sends no PATCH; server dry-run uses dryRun=All. No replay/wait.
Each action has 45 seconds; requests have at most 10 seconds, preserving
shorter REST timeouts. Credential plugins/custom transports may ignore cancel.
Typed GET evidence is structurally capped after decoding, before retention,
copy and hashing; GET decoding itself is not byte-limited. History and IRs
have 16 items/page, 32 total and two pages. PATCH responses are byte-limited.`
	return c
}

func runRuntimeSync(parent context.Context, f factory.Factory, streams genericiooptions.IOStreams, namespaces *namespace.Options, name, output string, mode reportv1alpha1.DryRunMode, yes bool) error {
	ctx, cancel := context.WithTimeout(parent, 45*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	workload, _, err := f.Namespace()
	if err != nil {
		return errors.New("resolve workload namespace failed")
	}
	resolved, err := namespaces.Resolve(workload)
	if err != nil {
		return errors.New("runtime sync namespaces are invalid")
	}
	contexts, ok := f.(factory.ContextResolver)
	if !ok {
		return errors.New("runtime sync context is unavailable")
	}
	contextName, err := contexts.ContextName()
	if err != nil || !mutate.SafeScalar(contextName) {
		return errors.New("runtime sync context is unavailable or unsafe")
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
		return errors.New("runtime sync OME client is unavailable")
	}
	getParent := func() (*v1beta1.InferenceService, error) {
		request, stop := context.WithTimeout(ctx, 10*time.Second)
		defer stop()
		v, e := client.OmeV1beta1().InferenceServices(resolved.WorkloadNamespace).Get(request, name, metav1.GetOptions{})
		if e != nil {
			return nil, mutate.SafeAPIError(e)
		}
		if e = request.Err(); e != nil {
			return nil, e
		}
		if e = mutate.ValidateTarget(v); e != nil {
			return nil, e
		}
		if v.Name != name || v.Namespace != resolved.WorkloadNamespace {
			return nil, mutate.ErrUnsafeTarget
		}
		return v, nil
	}
	v, err := getParent()
	if err != nil {
		return err
	}
	getKube := f.KubeClient
	if owned, ok := f.(factory.ActionReadClientsResolver); ok {
		getKube = func() (kubernetes.Interface, error) { return owned.KubeClientForAction(ctx) }
	}
	kube, err := getKube()
	if err != nil {
		return mutate.SafeAPIError(err)
	}
	if kube == nil {
		return errors.New("runtime sync revision client is unavailable")
	}
	bounded, ok := f.(factory.ActionRuntimeResolver)
	if !ok {
		return errors.New("runtime sync requires an action-bound runtime client")
	}
	runtimeClient, err := bounded.RuntimeClientForAction(ctx)
	if err != nil {
		return mutate.SafeAPIError(err)
	}
	resolver, err := effective.NewRuntimeSyncResolver(kube.AppsV1(), runtimeClient, resolved.OMENamespace, paging.Limits{PageSize: 16, MaxItems: 32, MaxPages: 2, RequestTimeout: 10 * time.Second})
	if err != nil {
		return err
	}
	evidence, err := resolver.Resolve(ctx, v)
	if err != nil {
		if errors.Is(err, effective.ErrRuntimeSyncEvidence) {
			return effective.ErrRuntimeSyncEvidence
		}
		return mutate.SafeAPIError(err)
	}
	work, err := mutate.CollectRuntimeSyncReplicaEvidence(ctx, client.OmeV1beta1(), v, evidence.NativeComponents(), nil)
	if err != nil {
		return err
	}
	uuid, err := mutate.NewRuntimeSyncRequestID(evidence, rand.Reader)
	if err != nil {
		return err
	}
	plan, err := mutate.PrepareRuntimeSync(v, evidence, work, uuid, nil)
	if err != nil {
		return err
	}
	config, err := f.RESTConfig()
	if err != nil {
		return mutate.SafeAPIError(err)
	}
	if config == nil {
		return errors.New("runtime sync REST configuration is unavailable")
	}
	local := *config // preserve nested provider ownership until NewBounded copies it safely.
	if local.Timeout <= 0 || local.Timeout > 10*time.Second {
		local.Timeout = 10 * time.Second
	}
	patchClient, err := transport.NewBounded(&local, 1048576)
	if err != nil {
		return errors.New("construct runtime sync action transport failed")
	}
	if err = plan.WritePreview(streams.ErrOut, contextName, resolved.OMENamespace, mode); err != nil {
		return err
	}
	if err = mutate.Confirm(ctx, streams.In, streams.ErrOut, yes); err != nil {
		return err
	}
	stale := func() error {
		return &exitcode.PreconditionError{Err: errors.New("runtime sync safety snapshot changed; refresh runtime effective and retry explicitly")}
	}
	// Exactly one fresh parent/source/IR recheck; never rebuild/repreview the plan.
	current, err := getParent()
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return stale()
	}
	fresh, err := resolver.Resolve(ctx, current)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return stale()
	}
	freshWork, err := mutate.CollectRuntimeSyncReplicaEvidence(ctx, client.OmeV1beta1(), current, fresh.NativeComponents(), nil)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return stale()
	}
	if !evidence.SameSnapshot(fresh) || !work.SameSnapshot(freshWork) {
		return stale()
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	result := plan.Result(mode, nil)
	result.FollowUp = "kubectl ome runtime effective " + name + " -n " + resolved.WorkloadNamespace + " --context=" + contextName + " --ome-namespace=" + resolved.OMENamespace
	if mode != reportv1alpha1.DryRunClient {
		body, e := patchClient.JSONPatch(ctx, transport.Resource{Namespace: v.Namespace, Resource: "inferenceservices", Name: v.Name}, plan.Patch(), transport.JSONPatchOptions{DryRun: mode == reportv1alpha1.DryRunServer})
		if e != nil {
			if errors.Is(e, transport.ErrResponseIdentity) || errors.Is(e, transport.ErrResponseTooLarge) {
				return errors.New("runtime sync response is unbound or oversized; outcome unknown, check runtime effective; do not replay")
			}
			var status apierrors.APIStatus
			if !errors.As(e, &status) || status.Status().Code >= 500 {
				return &syncUnknownOutcomeError{cause: mutate.SafeAPIError(e)}
			}
			return mutate.GuardedAnnotationPatchError(e, "runtime effective")
		}
		var response struct {
			APIVersion string `json:"apiVersion"`
			Kind       string `json:"kind"`
			Metadata   struct {
				Name            string            `json:"name"`
				Namespace       string            `json:"namespace"`
				UID             string            `json:"uid"`
				ResourceVersion string            `json:"resourceVersion"`
				Annotations     map[string]string `json:"annotations"`
			} `json:"metadata"`
		}
		if len(body) > 1048576 || json.Unmarshal(body, &response) != nil || response.APIVersion != "ome.io/v1beta1" || response.Kind != "InferenceService" || response.Metadata.Name != v.Name || response.Metadata.Namespace != v.Namespace || response.Metadata.UID != string(v.UID) || !mutate.SafeScalar(response.Metadata.ResourceVersion) || !plan.ResponseHasToken(response.Metadata.Annotations) {
			return errors.New("runtime sync response is unbound; outcome unknown, check runtime effective; do not replay")
		}
		result.Accepted = true
		result.Applied = mode == reportv1alpha1.DryRunNone
		result.Message = "API accepted annotation request; consumption and convergence not observed."
		if mode == reportv1alpha1.DryRunServer {
			result.Message = "API dry-run accepted; no changes persisted."
		}
	}
	format := report.Format(output)
	if output == "wide" {
		format = report.FormatTable
	}
	if err = report.Write(streams.Out, format, result); err != nil {
		return errors.New("write runtime sync result failed; request outcome may be unknown, check runtime effective; do not replay")
	}
	return nil
}

type syncUnknownOutcomeError struct{ cause error }

func (e *syncUnknownOutcomeError) Error() string {
	return "runtime sync request outcome unknown; check runtime effective; do not replay"
}
func (e *syncUnknownOutcomeError) Unwrap() error { return e.cause }
