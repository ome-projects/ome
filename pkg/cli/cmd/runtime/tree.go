package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"sigs.k8s.io/ome/pkg/cli/apierror"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/inferenceservicecollection"
	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/runtimecollection"
	"sigs.k8s.io/ome/pkg/cli/runtimegraph"
	"sigs.k8s.io/ome/pkg/cli/runtimetreeprojection"
	"sigs.k8s.io/ome/pkg/cli/runtimeusage"
)

type treeCommandDependencies struct {
	clock  reportv1alpha1.Clock
	limits paging.Limits
}

type treeOptions struct {
	genericiooptions.IOStreams
	dependencies treeCommandDependencies
	name         string
	kind         string
	output       string
	format       report.Format
	wide         bool
}

func newTreeCmd(f factory.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	return newTreeCmdWithDependencies(f, streams, treeCommandDependencies{
		clock: reportv1alpha1.SystemClock{},
		limits: paging.Limits{
			PageSize: paging.ChunkSize, MaxItems: 1000, MaxPages: 2,
			RequestTimeout: 10 * time.Second,
		},
	})
}

func newTreeCmdWithDependencies(
	f factory.Factory,
	streams genericiooptions.IOStreams,
	dependencies treeCommandDependencies,
) *cobra.Command {
	o := &treeOptions{IOStreams: streams, dependencies: dependencies}
	cmd := &cobra.Command{
		Use:   "tree RUNTIME",
		Short: "Show runtime inheritance and InferenceService users",
		Long: `Shows the selected runtime's controller-accurate parent paths,
every descendant runtime that inherits from it, and InferenceServices that
explicitly reference each visible runtime head.

Namespaced and cluster resolution contexts remain separate. Runtime
inheritance is rendered only from complete runtime collections. An incomplete
InferenceService collection remains visible as partial dependency evidence.
Runtime specs, InferenceService specs and status, labels, annotations, and
resource versions are never printed. Namespaced targets list ServingRuntimes
and InferenceServices only in the selected namespace; cluster targets expand
those reads across namespaces.

The default table bounds every line to 80 display columns and marks each
clipped identity component with a stable fingerprint.
Use -o wide for the complete unabridged tree.`,
		Example: `  # Auto-detect when the name resolves to exactly one runtime
  kubectl ome runtime tree vllm-runtime

  # Select a cluster-scoped runtime explicitly
  kubectl ome runtime tree shared --kind ClusterServingRuntime

  # Select a namespaced runtime explicitly and emit the automation contract
  kubectl ome runtime tree shared --kind ServingRuntime -n team-a -o json`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			o.name = args[0]
			if err := o.validate(); err != nil {
				return err
			}
			return o.run(cmd.Context(), f)
		},
	}
	cmd.Flags().StringVar(&o.kind, "kind", "", "Runtime kind: ServingRuntime or ClusterServingRuntime (auto-detected when omitted)")
	cmd.Flags().StringVarP(&o.output, "output", "o", "table", "Output format: table, wide, json or yaml")
	return cmd
}

func (o *treeOptions) validate() error {
	format, wide, err := parseRuntimeTreeOutput(o.output)
	if err != nil {
		return err
	}
	o.format = format
	o.wide = wide
	if problems := validation.IsDNS1123Subdomain(o.name); len(problems) > 0 {
		return fmt.Errorf("runtime name %q is invalid: %s", o.name, strings.Join(problems, "; "))
	}
	switch runtimegraph.Kind(o.kind) {
	case "", runtimegraph.KindServingRuntime, runtimegraph.KindClusterServingRuntime:
		return nil
	default:
		return fmt.Errorf(
			"unsupported runtime kind %q (supported: ServingRuntime, ClusterServingRuntime)",
			o.kind,
		)
	}
}

func parseRuntimeTreeOutput(value string) (report.Format, bool, error) {
	if value == "wide" {
		return report.FormatTable, true, nil
	}
	format, err := report.ParseFormat(value)
	if err != nil {
		return "", false, fmt.Errorf(
			"unsupported output format %q (supported: table, wide, json, yaml)", value,
		)
	}
	return format, false, nil
}

func (o *treeOptions) run(ctx context.Context, f factory.Factory) error {
	target := runtimegraph.Target{Kind: runtimegraph.Kind(o.kind), Name: o.name}
	if target.Kind != runtimegraph.KindClusterServingRuntime {
		namespace, _, err := f.Namespace()
		if err != nil {
			return fmt.Errorf("resolve runtime namespace: %w", err)
		}
		if problems := validation.IsDNS1123Label(namespace); len(problems) > 0 {
			return fmt.Errorf(
				"runtime namespace %q is invalid: %s", namespace, strings.Join(problems, "; "),
			)
		}
		target.Namespace = namespace
	}

	client, err := f.OMEClient()
	if err != nil {
		return fmt.Errorf("construct OME client: %w", err)
	}
	var runtimes runtimecollection.Result
	var runtimeCollectionErr error
	if target.Kind == runtimegraph.KindClusterServingRuntime {
		runtimes, runtimeCollectionErr = runtimecollection.Collect(
			ctx, client.OmeV1beta1(), o.dependencies.limits,
		)
	} else {
		runtimes, runtimeCollectionErr = runtimecollection.CollectInNamespace(
			ctx, client.OmeV1beta1(), target.Namespace, o.dependencies.limits,
		)
	}
	clusterRuntimesUnavailable := runtimecollection.HasCollectionFailure(
		runtimeCollectionErr, runtimecollection.CollectionClusterServingRuntime,
	)
	namespacedRuntimesUnavailable := runtimecollection.HasCollectionFailure(
		runtimeCollectionErr, runtimecollection.CollectionServingRuntime,
	)
	if runtimeCollectionErr != nil &&
		(ctx.Err() != nil || !clusterRuntimesUnavailable && !namespacedRuntimesUnavailable) {
		return fmt.Errorf("collect runtimes: %w", apierror.Friendly(runtimeCollectionErr))
	}
	if err := requireCompleteRuntimeEvidence(
		runtimes.Completeness,
		clusterRuntimesUnavailable,
		namespacedRuntimesUnavailable,
	); err != nil {
		return err
	}

	graph, err := runtimegraph.Build(runtimes.Snapshot)
	if err != nil {
		return fmt.Errorf("build runtime inheritance graph: %w", err)
	}
	projection, err := graph.Project(target)
	if err != nil {
		if errors.Is(err, runtimegraph.ErrTargetAmbiguous) {
			return fmt.Errorf(
				"runtime %q is ambiguous; pass --kind ServingRuntime or --kind ClusterServingRuntime: %w",
				o.name,
				err,
			)
		}
		return fmt.Errorf("project runtime inheritance tree: %w", err)
	}

	if target.Kind == "" && projection.Target.Kind == runtimegraph.KindClusterServingRuntime {
		expanded, expandErr := runtimecollection.CollectServingRuntimes(
			ctx, client.OmeV1beta1(), metav1.NamespaceAll, o.dependencies.limits,
		)
		if expandErr != nil && ctx.Err() != nil {
			return fmt.Errorf("collect cross-namespace ServingRuntimes: %w", apierror.Friendly(expandErr))
		}
		runtimes.Snapshot.ServingRuntimes = expanded.ServingRuntimes
		runtimes.Completeness.ServingRuntimes = expanded.Completeness
		namespacedRuntimesUnavailable = runtimecollection.HasCollectionFailure(
			expandErr, runtimecollection.CollectionServingRuntime,
		)
		if expandErr != nil && !namespacedRuntimesUnavailable {
			return fmt.Errorf("collect cross-namespace ServingRuntimes: %w", apierror.Friendly(expandErr))
		}
		if err := requireCompleteRuntimeEvidence(
			runtimes.Completeness,
			clusterRuntimesUnavailable,
			namespacedRuntimesUnavailable,
		); err != nil {
			return err
		}

		graph, err = runtimegraph.Build(runtimes.Snapshot)
		if err != nil {
			return fmt.Errorf("build expanded runtime inheritance graph: %w", err)
		}
		projection, err = graph.Project(runtimegraph.Target{
			Kind: projection.Target.Kind, Name: projection.Target.Name,
		})
		if err != nil {
			return fmt.Errorf("project expanded runtime inheritance tree: %w", err)
		}
	}

	var services inferenceservicecollection.Result
	var serviceCollectionErr error
	if projection.Target.Kind == runtimegraph.KindServingRuntime {
		services, serviceCollectionErr = inferenceservicecollection.CollectInNamespace(
			ctx, client.OmeV1beta1(), projection.Target.Namespace, o.dependencies.limits,
		)
	} else {
		services, serviceCollectionErr = inferenceservicecollection.Collect(
			ctx, client.OmeV1beta1(), o.dependencies.limits,
		)
	}
	if serviceCollectionErr != nil && ctx.Err() != nil {
		return fmt.Errorf("collect InferenceServices: %w", apierror.Friendly(serviceCollectionErr))
	}

	usage := runtimeusage.Build(services.InferenceServices, runtimes.Snapshot)
	dependents, err := projectTreeDependents(projection, usage)
	if err != nil {
		return fmt.Errorf("project runtime users: %w", err)
	}
	namespacedCollectionScope := reportv1alpha1.RuntimeTreeCollectionScopeAllNamespaces
	collectionNamespace := ""
	if projection.Target.Kind == runtimegraph.KindServingRuntime {
		namespacedCollectionScope = reportv1alpha1.RuntimeTreeCollectionScopeNamespace
		collectionNamespace = projection.Target.Namespace
	}

	projected, err := runtimetreeprojection.Project(runtimetreeprojection.Input{
		Projection: projection,
		Snapshot: runtimetreeprojection.SnapshotObservation{Collections: []runtimetreeprojection.CollectionObservation{
			runtimeTreeCollectionObservation(
				reportv1alpha1.RuntimeTreeCollectionClusterServingRuntime,
				reportv1alpha1.RuntimeTreeCollectionScopeCluster,
				"",
				runtimes.Completeness.ClusterServingRuntimes,
				clusterRuntimesUnavailable,
			),
			runtimeTreeCollectionObservation(
				reportv1alpha1.RuntimeTreeCollectionServingRuntime,
				namespacedCollectionScope,
				collectionNamespace,
				runtimes.Completeness.ServingRuntimes,
				namespacedRuntimesUnavailable,
			),
			{
				Kind:      reportv1alpha1.RuntimeTreeCollectionInferenceService,
				Scope:     namespacedCollectionScope,
				Namespace: collectionNamespace,
				Status: collectionStatusWithAvailability(
					services.Completeness.Truncated, serviceCollectionErr != nil,
				),
				ObservedPages: services.Completeness.ObservedPages,
				ObservedItems: services.Completeness.ObservedItems,
			},
		}},
		Dependents: dependents,
	}, o.dependencies.clock)
	if err != nil {
		return fmt.Errorf("build runtime tree report: %w", err)
	}
	if o.wide {
		if err := reportv1alpha1.RuntimeTreeWideTable(projected).Write(o.Out); err != nil {
			return fmt.Errorf("write runtime tree report: write report table: %w", err)
		}
		return nil
	}
	if err := report.Write(o.Out, o.format, projected); err != nil {
		return fmt.Errorf("write runtime tree report: %w", err)
	}
	return nil
}

func requireCompleteRuntimeEvidence(
	completeness runtimecollection.Completeness,
	clusterUnavailable bool,
	servingRuntimeUnavailable bool,
) error {
	collections := []struct {
		kind         runtimecollection.CollectionKind
		completeness runtimecollection.KindCompleteness
		unavailable  bool
	}{
		{
			kind:         runtimecollection.CollectionClusterServingRuntime,
			completeness: completeness.ClusterServingRuntimes,
			unavailable:  clusterUnavailable,
		},
		{
			kind:         runtimecollection.CollectionServingRuntime,
			completeness: completeness.ServingRuntimes,
			unavailable:  servingRuntimeUnavailable,
		},
	}
	for _, collection := range collections {
		status := ""
		switch {
		case collection.unavailable:
			status = "unavailable"
		case collection.completeness.Truncated:
			status = "truncated"
		default:
			continue
		}
		return fmt.Errorf(
			"runtime tree requires complete runtime evidence: %s collection is %s "+
				"(pages=%d items=%d); restore list access or narrow the runtime scope, then retry",
			collection.kind,
			status,
			collection.completeness.ObservedPages,
			collection.completeness.ObservedItems,
		)
	}
	return nil
}

func runtimeTreeCollectionObservation(
	kind reportv1alpha1.RuntimeTreeCollectionKind,
	scope reportv1alpha1.RuntimeTreeCollectionScope,
	namespace string,
	completeness runtimecollection.KindCompleteness,
	unavailable bool,
) runtimetreeprojection.CollectionObservation {
	return runtimetreeprojection.CollectionObservation{
		Kind: kind, Scope: scope, Namespace: namespace,
		Status:        collectionStatusWithAvailability(completeness.Truncated, unavailable),
		ObservedPages: completeness.ObservedPages,
		ObservedItems: completeness.ObservedItems,
	}
}

func collectionStatus(truncated bool) reportv1alpha1.RuntimeTreeCollectionStatus {
	if truncated {
		return reportv1alpha1.RuntimeTreeCollectionStatusTruncated
	}
	return reportv1alpha1.RuntimeTreeCollectionStatusComplete
}

func collectionStatusWithAvailability(
	truncated bool,
	unavailable bool,
) reportv1alpha1.RuntimeTreeCollectionStatus {
	if unavailable {
		return reportv1alpha1.RuntimeTreeCollectionStatusUnavailable
	}
	return collectionStatus(truncated)
}

func projectTreeDependents(
	projection runtimegraph.Projection,
	usage *runtimeusage.Index,
) ([]runtimetreeprojection.DependentLeaf, error) {
	result := []runtimetreeprojection.DependentLeaf{}
	for _, context := range projection.Contexts {
		for _, path := range context.Paths {
			users, err := usage.ForRuntime(path.Subject)
			if err != nil {
				return nil, err
			}
			for _, service := range users.InferenceServices {
				result = append(result, runtimetreeprojection.DependentLeaf{
					Runtime:   path.Subject,
					Kind:      reportv1alpha1.RuntimeTreeDependentInferenceService,
					Namespace: service.Namespace,
					Name:      service.Name,
				})
			}
		}
	}
	return result, nil
}
