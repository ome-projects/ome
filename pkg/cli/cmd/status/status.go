package status

import (
	"context"
	"errors"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/namespace"
	publicreport "sigs.k8s.io/ome/pkg/cli/report"
	r "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func NewCmd(f factory.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	return newCmd(f, streams, r.SystemClock{})
}
func newCmd(f factory.Factory, streams genericiooptions.IOStreams, clock r.Clock) *cobra.Command {
	output := "table"
	namespaceOptions := namespace.NewOptions()
	cmd := &cobra.Command{
		Use:   "status INFERENCESERVICE",
		Short: "Show the full readiness story of an InferenceService",
		Long: `Show one read-only, versioned StatusReport.

Ready is reported condition evidence, not a convergence assertion. Absent is
NotRecorded; invalid records are Invalid. Global generations are advisory and
always Unverifiable. Pod counts are observed labelled Pods, not desired replicas
or logical instances. Optional unreadable sources differ from observed empty.

Observation windows are post-decode: 1000 Pods/two pages and 100 Warning Events
across at most 16 object targets. Wide expands safe facts; phase counters
R/P/F/S/U mean Running/Pending/Failed/Succeeded/Unknown, and deleting counts
terminating Pods. JSON/YAML retain bounded values. Rollout summary uses canonical
rollout evidence; use rollout status for detail. Autoscaling uses only
controller-reported parent status, not live HPA, KEDA, or InferenceReplica
reads; use autoscale status for detail. Traffic uses that same parent snapshot.
Runtime active summarizes an exact named runtime and its pin without candidate
lists or history. If a model is referenced, this path makes at most two exact
model GETs to preserve controller validation. Auto-selection is not probed.
Accelerator class checks use at most two exact GETs for current Engine/Decoder
selections. Optional read failures remain typed Unavailable/Partial; use the
dedicated commands for detail.`,
		SilenceErrors: true, SilenceUsage: true,
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errors.New("status requires exactly one InferenceService name")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			wide, err := parseOutput(output)
			if err != nil {
				return err
			}
			if len(validation.IsDNS1123Subdomain(args[0])) != 0 {
				return errors.New("status requires a valid InferenceService name")
			}
			ns, _, err := f.Namespace()
			if err != nil {
				return errors.New("status configuration is unavailable")
			}
			if len(validation.IsDNS1123Label(ns)) != 0 {
				return errors.New("status requires a valid namespace")
			}
			resolved, err := namespaceOptions.Resolve(ns)
			if err != nil {
				return errors.New("status requires a valid OME namespace")
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
			defer cancel()
			snapshot, err := gatherTypedWithOMENamespace(ctx, f, ns, args[0], resolved.OMENamespace)
			if err != nil {
				return err
			}
			document, err := projectStatus(snapshot, clock)
			if err != nil {
				return errStatusSource
			}
			if err = ctx.Err(); err != nil {
				return errStatusCancelled
			}
			if wide {
				err = document.WideTable().Write(streams.Out)
			} else {
				err = publicreport.Write(streams.Out, publicreport.Format(output), document)
			}
			if err != nil {
				return errors.New("status output could not be written")
			}
			return nil
		},
	}
	cmd.SetFlagErrorFunc(func(*cobra.Command, error) error { return errors.New("status flags are invalid") })
	cmd.SetIn(streams.In)
	cmd.SetOut(streams.Out)
	cmd.SetErr(streams.ErrOut)
	cmd.Flags().StringVarP(&output, "output", "o", "table", "Output format: table, wide, json, or yaml")
	namespaceOptions.AddOMEFlags(cmd.Flags())
	return cmd
}

func parseOutput(value string) (bool, error) {
	switch value {
	case "table", "json", "yaml":
		return false, nil
	case "wide":
		return true, nil
	default:
		return false, errors.New("status output must be table, wide, json, or yaml")
	}
}
