package admin

import (
	"context"
	"errors"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/tools/clientcmd"

	"sigs.k8s.io/ome/pkg/cli/doctorcollection"
	"sigs.k8s.io/ome/pkg/cli/doctorprojection"
	"sigs.k8s.io/ome/pkg/cli/exitcode"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/namespace"
	"sigs.k8s.io/ome/pkg/cli/report"
	r "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/version"
)

type doctorDependencies struct {
	clock   r.Clock
	collect func(context.Context, doctorcollection.Clients, doctorcollection.Selection, time.Duration) (doctorcollection.Snapshot, error)
}

func newDoctorCmd(f factory.Factory, streams genericiooptions.IOStreams, ns *namespace.Options, deps doctorDependencies) *cobra.Command {
	var output, isvc string
	cmd := &cobra.Command{
		Use: "doctor", Short: "Inspect bounded current-context OME diagnostic evidence",
		Long: `Read seven fixed API group-version discovery documents and the exact
ome-controller-manager Deployment in --ome-namespace. With --isvc NAME, also
GET only that named InferenceService in the effective workload namespace:
at most nine sequential GET API calls. No LIST, access review, Secrets,
ConfigMap content, remote cluster, remediation or controller health probes.

Advertised APIs do not prove read authorization. A named read describes only
that exact request. Without --isvc, the workload read is NotRequested and
feature evidence is NotSelected. Optional missing or unreadable sources are
typed unavailable diagnostics, not Healthy. Feature generation freshness and
running operator compatibility remain Unverifiable. A stable selected-manager
image tag is only a declared candidate; its comparison is computed, not
running-version or compatibility proof. Use the corresponding rollout,
autoscale, traffic, instance or migration diagnostic for additional evidence.

Each request is capped at 10 seconds and the command at 30 seconds, preserving
shorter request-timeout and caller deadlines. External credential plugins or
custom transports may not honor cancellation. Scan limits apply to decoded
documents (256 discovery resources, 32 manager containers, 2048 image bytes),
not network allocation. No arbitrary object, metadata or error text is shown.
Compact tables fit 80 columns; wide expands only the same safe typed facts.

Exit 0: diagnostic report produced, including optional unavailable evidence.
Exit 1: invalid local selection/configuration, selected ISVC unreadable,
cancellation or render failure. Exit 2: report produced with proven required
API non-discoverability at a queried version. Forbidden discovery does not
prove an API missing and does not by itself cause exit 2.`,
		Example: "  kubectl ome admin doctor --isvc chat -n team-a --ome-namespace ome",
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) != 0 {
				return errors.New("doctor accepts no positional arguments")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			if output != "table" && output != "wide" && output != "json" && output != "yaml" {
				return errors.New("doctor output must be table, wide, json or yaml")
			}
			if cmd.Flags().Changed("isvc") && (isvc == "" || len(validation.IsDNS1123Subdomain(isvc)) != 0) {
				return errors.New("doctor InferenceService name is invalid")
			}
			// Resolve local options against a placeholder before touching kubeconfig.
			if ns == nil {
				return errors.New("doctor namespaces are invalid")
			}
			if _, err := ns.Resolve("default"); err != nil {
				return errors.New("doctor namespaces are invalid")
			}
			if flag := cmd.Flags().Lookup("namespace"); flag != nil && flag.Changed && len(validation.IsDNS1123Label(flag.Value.String())) != 0 {
				return errors.New("doctor workload namespace is invalid")
			}
			if flag := cmd.Flags().Lookup("request-timeout"); flag != nil && flag.Changed {
				timeout, err := clientcmd.ParseTimeout(flag.Value.String())
				if err != nil || timeout < 0 {
					return errors.New("doctor request timeout is invalid")
				}
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), doctorcollection.CollectionTimeout)
			defer cancel()
			if ctx.Err() != nil {
				return errors.New("doctor canceled or timed out")
			}
			if f == nil {
				return errors.New("doctor configuration is unavailable")
			}
			workload, _, err := f.Namespace()
			if err != nil {
				return errors.New("doctor workload namespace is unavailable")
			}
			resolved, err := ns.Resolve(workload)
			if err != nil {
				return errors.New("doctor namespaces are invalid")
			}
			resolver, ok := f.(factory.ContextResolver)
			if !ok {
				return errors.New("doctor selected context is unavailable")
			}
			contextName, err := resolver.ContextName()
			if err != nil || contextName == "" {
				return errors.New("doctor selected context is unavailable")
			}
			if ctx.Err() != nil {
				return errors.New("doctor canceled or timed out")
			}
			config, err := f.RESTConfig()
			if err != nil || config == nil {
				return errors.New("doctor REST configuration is unavailable")
			}
			timeout := doctorcollection.RequestTimeout
			if config.Timeout > 0 && config.Timeout < timeout {
				timeout = config.Timeout
			}
			clients, err := doctorcollection.NewClients(config, isvc != "")
			if err != nil {
				return errors.New("doctor API clients are unavailable")
			}
			if ctx.Err() != nil {
				return errors.New("doctor canceled or timed out")
			}
			selected := doctorcollection.Selection{ContextName: contextName, WorkloadNamespace: resolved.WorkloadNamespace, OMENamespace: resolved.OMENamespace, ISVCName: isvc}
			snapshot, err := deps.collect(ctx, clients, selected, timeout)
			if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return errors.New("doctor canceled or timed out")
			}
			if err != nil {
				return errors.New("doctor collection failed")
			}
			value := doctorprojection.Project(snapshot, version.GitVersion, deps.clock)
			if output == "wide" {
				err = value.Content.WideTable().Write(streams.Out)
			} else {
				err = report.Write(streams.Out, report.Format(output), value)
			}
			if err != nil {
				return errors.New("write doctor report failed")
			}
			if value.Content.HasViolations() {
				return &exitcode.UnmetAssertionError{Err: errors.New("doctor found required API violations")}
			}
			return nil
		},
	}
	cmd.SetFlagErrorFunc(func(*cobra.Command, error) error { return errors.New("invalid doctor flags; use --help") })
	cmd.Flags().StringVarP(&output, "output", "o", "table", "Output: table, wide, json or yaml")
	cmd.Flags().StringVar(&isvc, "isvc", "", "Inspect only this exact named InferenceService in the workload namespace")
	return cmd
}
