package migration

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/migrationcollection"
	"sigs.k8s.io/ome/pkg/cli/migrationprojection"
	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	omeclient "sigs.k8s.io/ome/pkg/client/clientset/versioned/typed/ome/v1beta1"
)

const (
	maxMigrationSources        = 64
	maxMigrationRecords        = 200
	maxScannedMigrationRecords = 800
	maxMigrationHints          = 8
	maxScannedMigrationHints   = 64
)

var (
	ErrInvalidInferenceServiceName = errors.New("inference service name is invalid")
	ErrInvalidNamespace            = errors.New("namespace is invalid")
	ErrInvalidComponent            = errors.New("component must be engine, decoder, or router")
)

type collectionClient = omeclient.OmeV1beta1Interface
type collectionLimits = paging.Limits

type statusCollector func(
	context.Context,
	collectionClient,
	string,
	string,
	collectionLimits,
) (migrationcollection.Result, error)

type statusProjector func(
	migrationcollection.Result,
	string,
	migrationprojection.Limits,
	reportv1alpha1.Clock,
) (reportv1alpha1.MigrationStatusReport, error)

type statusDependencies struct {
	clock            reportv1alpha1.Clock
	collectionLimits paging.Limits
	projectionLimits migrationprojection.Limits
	collect          statusCollector
	project          statusProjector
}

func defaultStatusDependencies() statusDependencies {
	return statusDependencies{
		clock: reportv1alpha1.SystemClock{},
		collectionLimits: paging.Limits{
			PageSize:       32,
			MaxItems:       maxMigrationSources,
			MaxPages:       2,
			RequestTimeout: 10 * time.Second,
		},
		projectionLimits: migrationprojection.Limits{
			MaxRecords: maxMigrationRecords, MaxScannedRecords: maxScannedMigrationRecords,
			MaxNodeHints: maxMigrationHints, MaxScannedNodeHints: maxScannedMigrationHints,
		},
		collect: migrationcollection.Collect,
		project: migrationprojection.Project,
	}
}

type statusOptions struct {
	genericiooptions.IOStreams
	output    string
	component string
	format    report.Format
	deps      statusDependencies
}

func newStatusCmd(f factory.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	return newStatusCmdWithDependencies(f, streams, defaultStatusDependencies())
}

func newStatusCmdWithDependencies(
	f factory.Factory,
	streams genericiooptions.IOStreams,
	deps statusDependencies,
) *cobra.Command {
	o := &statusOptions{IOStreams: streams, deps: deps}
	cmd := &cobra.Command{
		Use:   "status INFERENCESERVICE",
		Short: "Show live InferenceReplica migration status",
		Long: `Show the bounded, authoritative migration records reported by the
InferenceReplicas owned by one InferenceService. The command reads exactly that
InferenceService and an identity-validated InferenceReplica collection. It uses
the relationship label when representable; longer names use a bounded namespace
scan with exact parent and controller-owner checks. It never reads migration
audit ConfigMaps, pods, or Events.

Controller blocker and terminal-outcome messages are sanitized and capped at
256 display columns in machine output. Table details are clipped further so
the table's maximum natural width remains 80 columns.

The current API does not report a request timestamp or capacity/rate limits;
machine output marks that evidence unavailable instead of inferring it.`,
		Example: `  kubectl ome migration status chat -n prod
  kubectl ome migration status chat --component engine -o json`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.validate(args[0]); err != nil {
				return err
			}
			return o.run(cmd.Context(), f, args[0])
		},
	}
	cmd.Flags().StringVar(&o.component, "component", "", "Filter by component: engine, decoder, or router")
	cmd.Flags().StringVarP(&o.output, "output", "o", "table", "Output format: table, json or yaml")
	return cmd
}

func (o *statusOptions) validate(name string) error {
	format, err := report.ParseFormat(o.output)
	if err != nil {
		return err
	}
	o.format = format
	if len(utilvalidation.IsDNS1123Subdomain(name)) > 0 {
		return ErrInvalidInferenceServiceName
	}
	switch o.component {
	case "", "engine", "decoder", "router":
		return nil
	default:
		return ErrInvalidComponent
	}
}

func (o *statusOptions) run(ctx context.Context, f factory.Factory, name string) error {
	namespace, _, err := f.Namespace()
	if err != nil {
		return fmt.Errorf("resolve namespace: %w", err)
	}
	if len(utilvalidation.IsDNS1123Label(namespace)) > 0 {
		return ErrInvalidNamespace
	}
	client, err := f.OMEClient()
	if err != nil {
		return fmt.Errorf("create OME client: %w", err)
	}
	snapshot, err := o.deps.collect(
		ctx, client.OmeV1beta1(), namespace, name, o.deps.collectionLimits,
	)
	if err != nil {
		return fmt.Errorf("collect migration status: %w", err)
	}
	projected, err := o.deps.project(
		snapshot, o.component, o.deps.projectionLimits, o.deps.clock,
	)
	if err != nil {
		return fmt.Errorf("project migration status: %w", err)
	}
	if err := report.Write(o.Out, o.format, projected); err != nil {
		return fmt.Errorf("write migration status: %w", err)
	}
	return nil
}
