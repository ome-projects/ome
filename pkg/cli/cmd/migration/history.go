package migration

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/kubernetes"

	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/migrationhistorycollection"
	"sigs.k8s.io/ome/pkg/cli/migrationhistoryprojection"
	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	omeclient "sigs.k8s.io/ome/pkg/client/clientset/versioned/typed/ome/v1beta1"
)

const (
	maxMigrationHistorySources        = 64
	maxMigrationHistoryRecords        = 200
	maxScannedMigrationHistoryRecords = 800
	maxMigrationHistoryNodeHints      = 8
	maxScannedMigrationHistoryHints   = 64
	maxMigrationHistoryEvents         = 16
	maxScannedMigrationHistoryEvents  = 128
	maxMigrationAuditBytes            = 1 << 20
)

type historyCollector func(
	context.Context,
	omeclient.OmeV1beta1Interface,
	kubernetes.Interface,
	string,
	string,
	paging.Limits,
) (migrationhistorycollection.Result, error)

type historyProjector func(
	migrationhistorycollection.Result,
	string,
	migrationhistoryprojection.Limits,
	reportv1alpha1.Clock,
) (reportv1alpha1.MigrationHistoryReport, error)

type historyDependencies struct {
	clock            reportv1alpha1.Clock
	collectionLimits paging.Limits
	projectionLimits migrationhistoryprojection.Limits
	collect          historyCollector
	project          historyProjector
}

func defaultHistoryDependencies() historyDependencies {
	return historyDependencies{
		clock: reportv1alpha1.SystemClock{},
		collectionLimits: paging.Limits{
			PageSize: 32, MaxItems: maxMigrationHistorySources,
			MaxPages: 2, RequestTimeout: 10 * time.Second,
		},
		projectionLimits: migrationhistoryprojection.Limits{
			MaxRecords:                 maxMigrationHistoryRecords,
			MaxScannedRecordsPerSource: maxScannedMigrationHistoryRecords,
			MaxNodeHints:               maxMigrationHistoryNodeHints,
			MaxScannedNodeHints:        maxScannedMigrationHistoryHints,
			MaxEvents:                  maxMigrationHistoryEvents,
			MaxScannedEvents:           maxScannedMigrationHistoryEvents,
			MaxAuditBytes:              maxMigrationAuditBytes,
		},
		collect: migrationhistorycollection.Collect,
		project: migrationhistoryprojection.Project,
	}
}

type historyOptions struct {
	genericiooptions.IOStreams
	output    string
	component string
	format    report.Format
	wide      bool
	deps      historyDependencies
}

func newHistoryCmd(f factory.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	return newHistoryCmdWithDependencies(f, streams, defaultHistoryDependencies())
}

func newHistoryCmdWithDependencies(
	f factory.Factory,
	streams genericiooptions.IOStreams,
	deps historyDependencies,
) *cobra.Command {
	o := &historyOptions{IOStreams: streams, deps: deps}
	cmd := &cobra.Command{
		Use:   "history INFERENCESERVICE",
		Short: "Show bounded migration evidence history",
		Long: `Show three separately labeled migration evidence windows for one service.
InferenceReplica status is authoritative work state. InferenceService history
and the optional audit ConfigMap are bounded historical evidence; neither can
replace or reactivate authoritative work.

All reads use the InferenceService's workload namespace. The audit ConfigMap is
owned beside that service, so this command does not read the OME control-plane
namespace and does not need --ome-namespace.

Raw ConfigMap data, annotations, caller identity, reasons, event messages, UIDs,
resource versions, and unrelated fields are never printed. Operational outcome
summaries are sanitized and bounded. The compact table stays within 80 columns;
use -o wide for complete safe identities, timestamps, provenance, availability,
freshness, bounded-window metadata, and issue codes.`,
		Example: `  kubectl ome migration history chat -n prod
  kubectl ome migration history chat --component engine -o wide
  kubectl ome migration history chat -o json`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.validate(args[0]); err != nil {
				return err
			}
			return o.run(cmd.Context(), f, args[0])
		},
	}
	cmd.Flags().StringVar(&o.component, "component", "", "Filter by component: engine, decoder, or router")
	cmd.Flags().StringVarP(&o.output, "output", "o", "table", "Output format: table, wide, json or yaml")
	return cmd
}

func (o *historyOptions) validate(name string) error {
	format, wide, err := parseMigrationHistoryOutput(o.output)
	if err != nil {
		return err
	}
	o.format, o.wide = format, wide
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

func parseMigrationHistoryOutput(value string) (report.Format, bool, error) {
	if value == "wide" {
		return report.FormatTable, true, nil
	}
	format, err := report.ParseFormat(value)
	if err != nil {
		return "", false, fmt.Errorf("unsupported output format %q (supported: table, wide, json, yaml)", value)
	}
	return format, false, nil
}

func (o *historyOptions) run(ctx context.Context, f factory.Factory, name string) error {
	namespace, _, err := f.Namespace()
	if err != nil {
		return fmt.Errorf("resolve namespace: %w", err)
	}
	if len(utilvalidation.IsDNS1123Label(namespace)) > 0 {
		return ErrInvalidNamespace
	}
	omeClient, err := f.OMEClient()
	if err != nil {
		return fmt.Errorf("create OME client: %w", err)
	}
	kubeClient, err := f.KubeClient()
	if err != nil {
		return fmt.Errorf("create Kubernetes client: %w", err)
	}
	snapshot, err := o.deps.collect(
		ctx, omeClient.OmeV1beta1(), kubeClient, namespace, name, o.deps.collectionLimits,
	)
	if err != nil {
		return fmt.Errorf("collect migration history: %w", err)
	}
	reportValue, err := o.deps.project(snapshot, o.component, o.deps.projectionLimits, o.deps.clock)
	if err != nil {
		return fmt.Errorf("project migration history: %w", err)
	}
	if o.wide {
		if err := reportValue.WideTable().Write(o.Out); err != nil {
			return fmt.Errorf("write migration history: %w", err)
		}
		return nil
	}
	if err := report.Write(o.Out, o.format, reportValue); err != nil {
		return fmt.Errorf("write migration history: %w", err)
	}
	return nil
}
