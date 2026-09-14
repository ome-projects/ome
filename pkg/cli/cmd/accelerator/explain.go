package accelerator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/acceleratorprojection"
	"sigs.k8s.io/ome/pkg/cli/effective"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/namespace"
	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

var (
	errReturnedInferenceServiceIdentity = errors.New("InferenceService response does not match the requested identity")
	errReturnedInferenceServiceUnbound  = errors.New("InferenceService response could not be safely bound")
	errRuntimeEvidenceSnapshotMismatch  = errors.New("runtime evidence does not match the InferenceService snapshot")
	errRuntimeRevisionIdentityMismatch  = errors.New("runtime revision response does not match the requested identity")
	errRuntimeRevisionIdentityUnbound   = errors.New("runtime revision response could not be safely bound")
)

type primaryReadClassification string

const (
	primaryReadNotFound       primaryReadClassification = "not found"
	primaryReadForbidden      primaryReadClassification = "forbidden"
	primaryReadUnsupportedAPI primaryReadClassification = "unsupported API"
	primaryReadTimedOut       primaryReadClassification = "timed out"
	primaryReadUnreadable     primaryReadClassification = "unreadable"
)

type primaryInferenceServiceReadError struct {
	target         string
	classification primaryReadClassification
	cause          error
}

func (e *primaryInferenceServiceReadError) Error() string {
	return fmt.Sprintf("read InferenceService %q: %s", e.target, e.classification)
}

func (e *primaryInferenceServiceReadError) GoString() string { return e.Error() }

func (e *primaryInferenceServiceReadError) Unwrap() error { return e.cause }

type explainEvidence struct {
	inferenceService *omev1beta1.InferenceService
	base             effective.AcceleratorBaseResolution
	classes          map[string]acceleratorprojection.AcceleratorClassEvidence
}

type explainCollector func(
	context.Context,
	factory.Factory,
	*namespace.Options,
	string,
	paging.Limits,
) (*explainEvidence, error)

type explainProjector func(
	*omev1beta1.InferenceService,
	effective.AcceleratorBaseResolution,
	map[string]acceleratorprojection.AcceleratorClassEvidence,
	reportv1alpha1.Clock,
) (reportv1alpha1.AcceleratorExplainReport, error)

type explainDependencies struct {
	clock   reportv1alpha1.Clock
	limits  paging.Limits
	collect explainCollector
	project explainProjector
}

type explainOptions struct {
	genericiooptions.IOStreams
	namespaceOptions *namespace.Options
	dependencies     explainDependencies
	name             string
	output           string
	format           report.Format
	wide             bool
}

func newExplainCmd(f factory.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	return newExplainCmdWithDependencies(f, streams, explainDependencies{
		clock: reportv1alpha1.SystemClock{},
		limits: paging.Limits{
			PageSize: paging.ChunkSize, MaxItems: 1000, MaxPages: 2,
			RequestTimeout: 10 * time.Second,
		},
		collect: collectAcceleratorEvidence,
		project: acceleratorprojection.Project,
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
		Short: "Explain accelerator intent and reported selection",
		Long: `Shows declared accelerator policy, current controller-reported selection,
verified AcceleratorClass identity, and pin-aware base versus applied resource
requests for each serving component.

The command never reruns accelerator selection or treats a locally computed
candidate as controller success. Stale, missing, or malformed status remains
unknown. The free-form selection reason is represented by a digest so a status
message cannot expose credentials. Use -o wide for the complete human view.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			o.name = args[0]
			if err := o.validate(); err != nil {
				return err
			}
			return o.run(cmd.Context(), f)
		},
	}
	cmd.Flags().StringVarP(&o.output, "output", "o", "table", "Output format: table, wide, json or yaml")
	o.namespaceOptions.AddOMEFlags(cmd.Flags())
	return cmd
}

func (o *explainOptions) validate() error {
	if o.output == "wide" {
		o.format = report.FormatTable
		o.wide = true
	} else {
		format, err := report.ParseFormat(o.output)
		if err != nil {
			return fmt.Errorf(
				"unsupported output format %q (supported: table, wide, json, yaml)", o.output,
			)
		}
		o.format = format
	}
	if problems := validation.IsDNS1123Subdomain(o.name); len(problems) > 0 {
		return fmt.Errorf("InferenceService name %q is invalid: %s", o.name, strings.Join(problems, "; "))
	}
	return nil
}

func (o *explainOptions) run(ctx context.Context, f factory.Factory) error {
	evidence, err := o.dependencies.collect(
		ctx, f, o.namespaceOptions, o.name, o.dependencies.limits,
	)
	if err != nil {
		return err
	}
	if evidence == nil || evidence.inferenceService == nil {
		return acceleratorprojection.ErrInvalidEvidence
	}
	reportValue, err := o.dependencies.project(
		evidence.inferenceService, evidence.base, evidence.classes, o.dependencies.clock,
	)
	if err != nil {
		return fmt.Errorf("project accelerator explain report: %w", err)
	}
	if o.wide {
		if err := reportValue.WideTable().Write(o.Out); err != nil {
			return fmt.Errorf("write accelerator explain report: %w", err)
		}
		return nil
	}
	if err := report.Write(o.Out, o.format, reportValue); err != nil {
		return fmt.Errorf("write accelerator explain report: %w", err)
	}
	return nil
}

func collectAcceleratorEvidence(
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
	if limits.PageSize <= 0 || limits.MaxItems <= 0 || limits.MaxPages <= 0 || limits.RequestTimeout <= 0 {
		return nil, errors.New("accelerator evidence limits are invalid")
	}
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
		return nil, safePrimaryReadError(resolved.WorkloadNamespace, name, err)
	}
	if err := acquisitionContext.Err(); err != nil {
		return nil, err
	}
	if err := bindInferenceService(isvc, resolved.WorkloadNamespace, name); err != nil {
		return nil, err
	}
	if effective.IsServiceVirtualDeployment(isvc) {
		base, baseErr := effective.ResolveVirtualAcceleratorBase(isvc)
		if baseErr != nil {
			return nil, fmt.Errorf("resolve virtual accelerator evidence: %w", baseErr)
		}
		return &explainEvidence{
			inferenceService: isvc, base: base,
			classes: map[string]acceleratorprojection.AcceleratorClassEvidence{},
		}, nil
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
	state, err := pinResolver.Resolve(
		acquisitionContext, isvc, effective.RuntimeResolveOptions{IncludeHistory: false},
	)
	if err != nil {
		return nil, fmt.Errorf("collect active runtime configuration: %w", err)
	}
	if err := validateRuntimeEvidence(state, isvc); err != nil {
		return nil, err
	}
	base, err := effective.ResolveAcceleratorBase(isvc, state)
	if err != nil {
		return nil, fmt.Errorf("resolve accelerator base requests: %w", err)
	}
	if base.ActiveState == effective.AcceleratorActiveUnavailable {
		return &explainEvidence{
			inferenceService: isvc, base: base,
			classes: map[string]acceleratorprojection.AcceleratorClassEvidence{},
		}, nil
	}

	classes := make(map[string]acceleratorprojection.AcceleratorClassEvidence, 2)
	for _, className := range acceleratorprojection.ReportedAcceleratorClassNames(isvc) {
		class, getErr := omeClient.OmeV1beta1().AcceleratorClasses().Get(
			acquisitionContext, className, metav1.GetOptions{},
		)
		if err := acquisitionContext.Err(); err != nil {
			return nil, err
		}
		if getErr != nil {
			classes[className] = unavailableClassEvidence(className, getErr)
			continue
		}
		observed, observeErr := acceleratorprojection.ObserveAcceleratorClass(class)
		if observeErr != nil || class.Name != className {
			invalid, invalidErr := acceleratorprojection.UnavailableAcceleratorClass(
				className, reportv1alpha1.AcceleratorClassInvalid,
			)
			if invalidErr != nil {
				return nil, acceleratorprojection.ErrInvalidClassEvidence
			}
			classes[className] = invalid
			continue
		}
		classes[className] = observed
	}
	return &explainEvidence{inferenceService: isvc, base: base, classes: classes}, nil
}

func safePrimaryReadError(
	namespace, name string,
	cause error,
) error {
	classification := primaryReadUnreadable
	switch {
	case apierrors.IsForbidden(cause), apierrors.IsUnauthorized(cause):
		classification = primaryReadForbidden
	case unsupportedAPIRead(cause):
		classification = primaryReadUnsupportedAPI
	case apierrors.IsNotFound(cause):
		classification = primaryReadNotFound
	case apierrors.IsTimeout(cause), apierrors.IsServerTimeout(cause),
		errors.Is(cause, context.DeadlineExceeded):
		classification = primaryReadTimedOut
	}
	return &primaryInferenceServiceReadError{
		target: namespace + "/" + name, classification: classification, cause: cause,
	}
}

func unsupportedAPIRead(err error) bool {
	if !apierrors.IsNotFound(err) {
		return false
	}
	var status apierrors.APIStatus
	if !errors.As(err, &status) {
		return false
	}
	details := status.Status().Details
	return details == nil || details.Name == ""
}

func validateRuntimeEvidence(
	state *effective.RuntimeState,
	isvc *omev1beta1.InferenceService,
) error {
	if state == nil || !state.MatchesInferenceService(isvc) {
		return errRuntimeEvidenceSnapshotMismatch
	}
	for _, issue := range state.SourceIssues() {
		var truncated *effective.RuntimeSelectionTruncated
		if errors.As(issue, &truncated) {
			return fmt.Errorf("collect active runtime configuration: %w", truncated)
		}
		for _, safetyError := range []error{
			effective.ErrRuntimeObjectIdentityMismatch,
			effective.ErrRuntimeSnapshotChanged,
			effective.ErrRuntimeSnapshotUnbindable,
		} {
			if errors.Is(issue, safetyError) {
				return fmt.Errorf("collect active runtime configuration: %w", safetyError)
			}
		}
	}
	for _, observation := range state.RevisionObservations() {
		if !observation.ObjectReturned() || observation.ExpectedName() == "" {
			continue
		}
		if observation.ReturnedName() != observation.ExpectedName() ||
			observation.ReturnedNamespace() != observation.ExpectedNamespace() {
			return errRuntimeRevisionIdentityMismatch
		}
	}
	active, err := state.RequireActive()
	if err != nil {
		if errors.Is(err, effective.ErrActiveRuntimeUnavailable) {
			return nil
		}
		return fmt.Errorf("require active runtime configuration: %w", err)
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
	case effective.ConfigurationOriginControllerRevision:
		for _, observation := range state.RevisionObservations() {
			if observation.ExpectedName() != active.RevisionName ||
				!hasRevisionRole(observation.Roles(), effective.RuntimeRevisionRoleActive) {
				continue
			}
			if !observation.ObjectReturned() || observation.UID == "" || observation.ResourceVersion == "" {
				return errRuntimeRevisionIdentityUnbound
			}
			return nil
		}
		return errRuntimeRevisionIdentityUnbound
	default:
		return errRuntimeEvidenceSnapshotMismatch
	}
	return nil
}

func hasRevisionRole(roles []effective.RuntimeRevisionRole, want effective.RuntimeRevisionRole) bool {
	for _, role := range roles {
		if role == want {
			return true
		}
	}
	return false
}

func bindInferenceService(
	isvc *omev1beta1.InferenceService,
	wantNamespace, wantName string,
) error {
	if isvc == nil || isvc.Name != wantName || isvc.Namespace != wantNamespace {
		return errReturnedInferenceServiceIdentity
	}
	if isvc.UID == "" || isvc.ResourceVersion == "" || isvc.Generation <= 0 {
		return errReturnedInferenceServiceUnbound
	}
	return nil
}

func unavailableClassEvidence(
	name string,
	err error,
) acceleratorprojection.AcceleratorClassEvidence {
	state := reportv1alpha1.AcceleratorClassUnreadable
	switch {
	case apierrors.IsForbidden(err), apierrors.IsUnauthorized(err):
		state = reportv1alpha1.AcceleratorClassForbidden
	case unsupportedAPIRead(err):
		state = reportv1alpha1.AcceleratorClassUnsupportedAPI
	case apierrors.IsNotFound(err):
		state = reportv1alpha1.AcceleratorClassNotFound
	}
	evidence, evidenceErr := acceleratorprojection.UnavailableAcceleratorClass(name, state)
	if evidenceErr != nil {
		return acceleratorprojection.AcceleratorClassEvidence{}
	}
	return evidence
}
