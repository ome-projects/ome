package autoscale

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/apierror"
	"sigs.k8s.io/ome/pkg/cli/autoscaleprojection"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

var (
	// ErrReturnedInferenceServiceNameMismatch rejects a response that is not
	// bound to the requested resource name.
	ErrReturnedInferenceServiceNameMismatch = errors.New("returned inference service name does not match request")
	// ErrReturnedInferenceServiceNamespaceMismatch rejects a response that is
	// not bound to the resolved request namespace.
	ErrReturnedInferenceServiceNamespaceMismatch = errors.New("returned inference service namespace does not match request")
	// ErrReturnedInferenceServiceUIDMissing rejects a response that cannot be
	// durably bound to the requested Kubernetes object.
	ErrReturnedInferenceServiceUIDMissing = errors.New("returned inference service has no UID")
)

type statusProjector func(
	*omev1beta1.InferenceService,
	reportv1alpha1.Clock,
) (reportv1alpha1.AutoscaleStatusReport, error)

type statusDependencies struct {
	clock   reportv1alpha1.Clock
	project statusProjector
}

type statusOptions struct {
	genericiooptions.IOStreams
	output     string
	liveScale  bool
	liveScaler bool
	deps       statusDependencies
}

func newStatusCmd(
	f factory.Factory,
	streams genericiooptions.IOStreams,
	deps statusDependencies,
) *cobra.Command {
	o := &statusOptions{IOStreams: streams, deps: deps}
	cmd := &cobra.Command{
		Use:   "status INFERENCESERVICE",
		Short: "Show controller-reported autoscaling status",
		Long: `Show autoscaling evidence reported on the InferenceService parent.
By default, this performs no HPA, KEDA, Deployment, or InferenceReplica reads.
--live-scale adds exact parent-selected InferenceReplica and /scale reads only.
--live-scaler adds exact HPA or KEDA ScaledObject reads for OME-managed scalers;
IR-backed targets first require a verified exact InferenceReplica read.
KEDA's generated HPA and reconciliation freshness are not observed by this flag.
SCALER-GEN=Matched means only HPA observedGeneration equals HPA generation.
STATE remains parent-reported; SCALER-EVIDENCE is separate live evidence.
Count equality is not proof of ongoing freshness or scaler health.
The compact table abbreviates InferenceReplica as IR and formats LAST-SCALE
as UTC MonDD HH:MMZ. ISSUES uses compact aliases:
  UnknownComp=UnknownComponentStatus
  NoAutoscaler=AutoscalerNotReported
  NoTarget=ScaleTargetNotReported
  BadClass=ClassInvalid
  BadManager=ManagedByInvalid
  OwnerMismatch=OwnershipMismatch
  BadSpecSource=SpecSourceInvalid
  UnexpectedEv=UnexpectedScalerEvidence
  ReplicaAmbig=ReplicaEvidenceAmbiguous
  BadReplica=ReplicaEvidenceInvalid
  BadTarget=ScaleTargetInvalid
  BadCondition=ConditionInvalid
  CondConflict=ConditionConflict
Unknown or future issue codes use X# followed by a stable 10-digit hex digest.
Use -o wide for exact issue codes, complete identities, and timestamps.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return o.run(cmd.Context(), f, args[0])
		},
	}
	cmd.Flags().StringVarP(&o.output, "output", "o", "table", "Output format: table, wide, json or yaml")
	cmd.Flags().BoolVar(&o.liveScale, "live-scale", false, "Compare parent counts with exact selected InferenceReplica and /scale reads")
	cmd.Flags().BoolVar(&o.liveScaler, "live-scaler", false, "Inspect exact selected HPA or KEDA ScaledObject (additional read permission)")
	return cmd
}

func (o *statusOptions) run(ctx context.Context, f factory.Factory, name string) error {
	format, wide, err := parseAutoscaleStatusOutput(o.output)
	if err != nil {
		return err
	}
	if problems := utilvalidation.IsDNS1123Subdomain(name); len(problems) > 0 {
		return fmt.Errorf("invalid InferenceService name %q: %s", name, strings.Join(problems, "; "))
	}

	namespace, _, err := f.Namespace()
	if err != nil {
		return fmt.Errorf("resolve namespace: %w", err)
	}
	if namespace == "" {
		return fmt.Errorf("resolved namespace must not be empty")
	}
	if problems := utilvalidation.IsDNS1123Label(namespace); len(problems) > 0 {
		return fmt.Errorf("invalid resolved namespace: %s", strings.Join(problems, "; "))
	}
	client, err := f.OMEClient()
	if err != nil {
		return fmt.Errorf("create OME client: %w", err)
	}
	isvc, err := client.OmeV1beta1().InferenceServices(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get InferenceService %q: %w", namespace+"/"+name, apierror.Friendly(err))
	}
	if isvc == nil {
		return autoscaleprojection.ErrInferenceServiceRequired
	}
	if isvc.Name != name {
		return ErrReturnedInferenceServiceNameMismatch
	}
	if isvc.Namespace != namespace {
		return ErrReturnedInferenceServiceNamespaceMismatch
	}
	if isvc.UID == "" {
		return ErrReturnedInferenceServiceUIDMissing
	}

	reportValue, err := o.deps.project(isvc, o.deps.clock)
	if err != nil {
		return fmt.Errorf("project autoscale status for InferenceService %q: %w", namespace+"/"+name, err)
	}
	if o.liveScale {
		reportValue, err = autoscaleprojection.EnrichLiveScale(ctx, isvc, reportValue, &statusScaleReader{factory: f}, o.deps.clock)
		if err != nil {
			return fmt.Errorf("inspect live scale for InferenceService %q: %w", namespace+"/"+name, err)
		}
	}
	if o.liveScaler {
		reportValue, err = autoscaleprojection.EnrichLiveScaler(ctx, isvc, reportValue, &statusScalerReader{factory: f}, o.deps.clock)
		if err != nil {
			return fmt.Errorf("inspect live scaler for InferenceService %q: %w", namespace+"/"+name, err)
		}
	}
	if wide {
		if err := reportValue.WideTable().Write(o.Out); err != nil {
			return fmt.Errorf("write autoscale status: %w", err)
		}
		return nil
	}
	if err := report.Write(o.Out, format, reportValue); err != nil {
		return fmt.Errorf("write autoscale status: %w", err)
	}
	return nil
}

func parseAutoscaleStatusOutput(value string) (report.Format, bool, error) {
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
