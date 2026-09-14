package autoscale

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

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/apierror"
	"sigs.k8s.io/ome/pkg/cli/autoscaleprojection"
	"sigs.k8s.io/ome/pkg/cli/effective"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/namespace"
	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

var (
	// ErrReturnedInferenceServiceResourceVersionMissing rejects a snapshot that
	// cannot be bound to the runtime evidence collected from it.
	ErrReturnedInferenceServiceResourceVersionMissing = errors.New("returned inference service has no resourceVersion")
	// ErrRuntimeEvidenceSnapshotMismatch rejects evidence resolved from a
	// different InferenceService snapshot.
	ErrRuntimeEvidenceSnapshotMismatch = errors.New("runtime evidence does not match the inference service snapshot")
	// ErrReturnedRuntimeRevisionIdentityMismatch rejects a successful exact
	// revision GET whose returned metadata does not match the requested key.
	ErrReturnedRuntimeRevisionIdentityMismatch = errors.New("runtime revision response does not match the requested identity")
	// ErrReturnedRuntimeRevisionIdentityUnbindable rejects an active revision
	// response without the UID and resourceVersion needed to bind its snapshot.
	ErrReturnedRuntimeRevisionIdentityUnbindable = errors.New("runtime revision response could not be safely bound")
)

type explainProjector func(
	*omev1beta1.InferenceService,
	*effective.RuntimeState,
	reportv1alpha1.Clock,
) (reportv1alpha1.AutoscaleExplainReport, error)

type explainVirtualProjector func(
	*omev1beta1.InferenceService,
	reportv1alpha1.Clock,
) (reportv1alpha1.AutoscaleExplainReport, error)

type explainCollector func(
	context.Context,
	factory.Factory,
	*namespace.Options,
	string,
	paging.Limits,
) (*explainEvidence, error)

type explainDependencies struct {
	clock          reportv1alpha1.Clock
	limits         paging.Limits
	collect        explainCollector
	project        explainProjector
	projectVirtual explainVirtualProjector
}

type explainEvidence struct {
	inferenceService *omev1beta1.InferenceService
	state            *effective.RuntimeState
	virtual          bool
}

type explainOptions struct {
	genericiooptions.IOStreams
	namespaceOptions *namespace.Options
	dependencies     explainDependencies
	name             string
	output           string
	format           report.Format
}

func newExplainCmd(f factory.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	return newExplainCmdWithDependencies(f, streams, explainDependencies{
		clock: reportv1alpha1.SystemClock{},
		limits: paging.Limits{
			PageSize:       paging.ChunkSize,
			MaxItems:       1000,
			MaxPages:       2,
			RequestTimeout: 10 * time.Second,
		},
		collect:        collectAutoscaleExplainEvidence,
		project:        autoscaleprojection.ProjectExplain,
		projectVirtual: autoscaleprojection.ProjectVirtualExplain,
	})
}

func newExplainCmdWithDependencies(
	f factory.Factory,
	streams genericiooptions.IOStreams,
	dependencies explainDependencies,
) *cobra.Command {
	o := &explainOptions{
		IOStreams: streams, namespaceOptions: namespace.NewOptions(), dependencies: dependencies,
	}
	cmd := &cobra.Command{
		Use:   "explain INFERENCESERVICE",
		Short: "Explain effective and reported autoscaling",
		Long: `Explains declared autoscaling from the controller-selected active runtime configuration
and compares it with evidence reported on the InferenceService parent.

The command does not query child autoscaler or workload objects. A matching
report describes one bound API snapshot; it does not prove rollout convergence
or wall-clock freshness. Raw runtime specifications, autoscaler payloads,
resource versions, status messages, and synchronization tokens are never printed.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			o.name = args[0]
			if err := o.validate(); err != nil {
				return err
			}
			return o.run(cmd.Context(), f)
		},
	}
	cmd.Flags().StringVarP(&o.output, "output", "o", "table", "Output format: table, json or yaml")
	o.namespaceOptions.AddOMEFlags(cmd.Flags())
	return cmd
}

func (o *explainOptions) validate() error {
	format, err := report.ParseFormat(o.output)
	if err != nil {
		return err
	}
	o.format = format
	if problems := validation.IsDNS1123Subdomain(o.name); len(problems) > 0 {
		return fmt.Errorf("InferenceService name %q is invalid: %s", o.name, strings.Join(problems, "; "))
	}
	return nil
}

func (o *explainOptions) run(ctx context.Context, f factory.Factory) error {
	evidence, err := o.dependencies.collect(ctx, f, o.namespaceOptions, o.name, o.dependencies.limits)
	if err != nil {
		return err
	}
	if evidence == nil || evidence.inferenceService == nil {
		return autoscaleprojection.ErrInferenceServiceRequired
	}
	var reportValue reportv1alpha1.AutoscaleExplainReport
	if evidence.virtual {
		if o.dependencies.projectVirtual == nil {
			return errors.New("virtual autoscale explain projector is not configured")
		}
		reportValue, err = o.dependencies.projectVirtual(evidence.inferenceService, o.dependencies.clock)
	} else {
		if o.dependencies.project == nil {
			return errors.New("autoscale explain projector is not configured")
		}
		reportValue, err = o.dependencies.project(evidence.inferenceService, evidence.state, o.dependencies.clock)
	}
	if err != nil {
		return fmt.Errorf("project autoscale explain report: %w", err)
	}
	if err := report.Write(o.Out, o.format, reportValue); err != nil {
		return fmt.Errorf("write autoscale explain report: %w", err)
	}
	return nil
}

func collectAutoscaleExplainEvidence(
	ctx context.Context,
	f factory.Factory,
	namespaceOptions *namespace.Options,
	name string,
	limits paging.Limits,
) (*explainEvidence, error) {
	workloadNamespace, _, err := f.Namespace()
	if err != nil {
		return nil, fmt.Errorf("resolve workload namespace: %w", err)
	}
	resolved, err := namespaceOptions.Resolve(workloadNamespace)
	if err != nil {
		return nil, err
	}

	// One deadline covers the authoritative primary GET and every runtime/pin
	// read. Nested per-request deadlines may shorten individual reads, but can
	// never extend this acquisition window.
	acquisitionContext, cancel := context.WithTimeout(ctx, limits.RequestTimeout)
	defer cancel()
	if err := acquisitionContext.Err(); err != nil {
		return nil, err
	}

	omeClient, err := f.OMEClient()
	if err != nil {
		return nil, fmt.Errorf("construct OME client: %w", err)
	}
	isvc, err := omeClient.OmeV1beta1().InferenceServices(resolved.WorkloadNamespace).
		Get(acquisitionContext, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get InferenceService %q: %w", resolved.WorkloadNamespace+"/"+name, apierror.Friendly(err))
	}
	if err := acquisitionContext.Err(); err != nil {
		return nil, err
	}
	if err := bindExplainInferenceService(isvc, resolved.WorkloadNamespace, name); err != nil {
		return nil, err
	}
	if effective.IsServiceVirtualDeployment(isvc) {
		return &explainEvidence{inferenceService: isvc, virtual: true}, nil
	}

	kubeClient, err := f.KubeClient()
	if err != nil {
		return nil, fmt.Errorf("construct Kubernetes client: %w", err)
	}
	runtimeClient, err := f.RuntimeClient()
	if err != nil {
		return nil, fmt.Errorf("construct runtime client: %w", err)
	}
	liveResolver, err := effective.NewBoundedRuntimeResolver(runtimeClient, limits)
	if err != nil {
		return nil, fmt.Errorf("construct bounded runtime resolver: %w", err)
	}
	pinResolver, err := effective.NewRuntimePinResolver(
		kubeClient.AppsV1(), liveResolver, resolved.OMENamespace, limits,
	)
	if err != nil {
		return nil, fmt.Errorf("construct runtime pin resolver: %w", err)
	}
	state, err := pinResolver.Resolve(acquisitionContext, isvc, effective.RuntimeResolveOptions{IncludeHistory: false})
	if err != nil {
		return nil, fmt.Errorf("collect active runtime configuration: %w", err)
	}
	if err := validateExplainRuntimeRevisionIdentities(state); err != nil {
		return nil, fmt.Errorf("collect active runtime configuration: %w", err)
	}
	for _, issue := range state.SourceIssues() {
		var truncated *effective.RuntimeSelectionTruncated
		if errors.As(issue, &truncated) {
			return nil, fmt.Errorf("collect active runtime configuration: %w", truncated)
		}
		for _, safetyError := range []error{
			effective.ErrRuntimeObjectIdentityMismatch,
			effective.ErrRuntimeSnapshotChanged,
			effective.ErrRuntimeSnapshotUnbindable,
		} {
			if errors.Is(issue, safetyError) {
				return nil, fmt.Errorf("collect active runtime configuration: %w", safetyError)
			}
		}
	}
	if err := acquisitionContext.Err(); err != nil {
		return nil, err
	}
	if !state.MatchesInferenceService(isvc) {
		return nil, ErrRuntimeEvidenceSnapshotMismatch
	}
	active, err := state.RequireActive()
	if err != nil {
		return nil, fmt.Errorf("require active runtime configuration: %w", err)
	}
	if err := validateExplainCausalIdentities(state, active); err != nil {
		return nil, fmt.Errorf("bind active runtime configuration: %w", err)
	}
	return &explainEvidence{inferenceService: isvc, state: state}, nil
}

func validateExplainCausalIdentities(
	state *effective.RuntimeState,
	active *effective.ActiveConfiguration,
) error {
	if state == nil || active == nil {
		return ErrRuntimeEvidenceSnapshotMismatch
	}
	switch active.Origin {
	case effective.ConfigurationOriginLiveRuntime:
		live := state.LiveConfiguration()
		if live == nil || live.Runtime.Name != active.RuntimeName ||
			live.Runtime.Kind != active.RuntimeKind || live.Runtime.Namespace != active.RuntimeNamespace ||
			!live.Runtime.IdentityObserved {
			return effective.ErrRuntimeSnapshotUnbindable
		}
		if live.Runtime.SelectionSource == effective.RuntimeSelected &&
			(live.Model == nil || live.Model.UID == "" || live.Model.ResourceVersion == "") {
			return effective.ErrRuntimeSnapshotUnbindable
		}
		return nil
	case effective.ConfigurationOriginControllerRevision:
		for _, observation := range state.RevisionObservations() {
			if observation.ExpectedName() != active.RevisionName ||
				!hasRuntimeRevisionRole(observation.Roles(), effective.RuntimeRevisionRoleActive) {
				continue
			}
			if !observation.ObjectReturned() || observation.UID == "" || observation.ResourceVersion == "" {
				return ErrReturnedRuntimeRevisionIdentityUnbindable
			}
			return nil
		}
		return ErrReturnedRuntimeRevisionIdentityUnbindable
	default:
		return ErrRuntimeEvidenceSnapshotMismatch
	}
}

func hasRuntimeRevisionRole(roles []effective.RuntimeRevisionRole, want effective.RuntimeRevisionRole) bool {
	for _, role := range roles {
		if role == want {
			return true
		}
	}
	return false
}

func validateExplainRuntimeRevisionIdentities(state *effective.RuntimeState) error {
	if state == nil {
		return ErrRuntimeEvidenceSnapshotMismatch
	}
	for _, observation := range state.RevisionObservations() {
		if !observation.ObjectReturned() || observation.ExpectedName() == "" {
			continue
		}
		if observation.ReturnedName() != observation.ExpectedName() ||
			observation.ReturnedNamespace() != observation.ExpectedNamespace() {
			return ErrReturnedRuntimeRevisionIdentityMismatch
		}
	}
	return nil
}

func bindExplainInferenceService(isvc *omev1beta1.InferenceService, wantNamespace, wantName string) error {
	switch {
	case isvc == nil:
		return autoscaleprojection.ErrInferenceServiceRequired
	case isvc.Name != wantName:
		return ErrReturnedInferenceServiceNameMismatch
	case isvc.Namespace != wantNamespace:
		return ErrReturnedInferenceServiceNamespaceMismatch
	case isvc.UID == "":
		return ErrReturnedInferenceServiceUIDMissing
	case isvc.ResourceVersion == "":
		return ErrReturnedInferenceServiceResourceVersionMissing
	default:
		return nil
	}
}
