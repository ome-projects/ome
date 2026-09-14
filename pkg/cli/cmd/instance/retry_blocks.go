package instance

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
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/instancecollection"
	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/retryblockprojection"
)

var (
	ErrRetryBlocksComponentRequired = errors.New("--component is required")
	ErrRetryBlocksComponentInvalid  = errors.New("component must be engine, decoder, or router")
)

type retryBlocksProjector func(
	retryblockprojection.Input,
	reportv1alpha1.Clock,
) (reportv1alpha1.InstanceRetryBlocksReport, error)

type retryBlocksDependencies struct {
	clock          reportv1alpha1.Clock
	limits         paging.Limits
	maxRetryBlocks int
	project        retryBlocksProjector
}

type retryBlocksOptions struct {
	streams   genericiooptions.IOStreams
	output    string
	component string
	deps      retryBlocksDependencies
}

func newRetryBlocksCmd(
	f factory.Factory,
	streams genericiooptions.IOStreams,
	deps retryBlocksDependencies,
) *cobra.Command {
	if deps.project == nil {
		deps.project = retryblockprojection.Project
	}
	o := &retryBlocksOptions{streams: streams, deps: deps}
	cmd := &cobra.Command{
		Use:   "retry-blocks INFERENCESERVICE --component COMPONENT",
		Short: "Show controller-reported retry authority",
		Long: `Show bounded RetryBlock evidence from the exact InferenceReplica for one component.

STATE is BACKOFF, HELD, or RUNNING (RetryInProgress).
ATT counts lifecycle attempts; it does not count kubelet container restarts.
NEXT uses compact UTC (YY-MM-DDThh:mmZ). BACKOFF requires it; RUNNING can
retain the deadline from its prior backoff.
REL is YES only when collection evidence is complete and the Held block is
current and valid; release-held may submit that exact target revision.
TARGET uses a display-only PREFIX#DIGEST alias when the full revision is long;
JSON and YAML retain the complete valid target revision.

Issue aliases:
  COLL_UNAV=CollectionUnavailable  COLL_TRUNC=CollectionTruncated
  ID_REJECT=IdentityRejected      DUP_COMP=DuplicateComponent
  NO_COMPONENT=ComponentNotProjected
  BLOCK_TRUNC=RetryBlocksTruncated
  PARENT_MISS=ParentGenerationMissing  PARENT_BAD=ParentGenerationInvalid
  PARENT_STALE=ParentGenerationStale   PARENT_AHEAD=ParentGenerationAhead
  STATUS_UNOBS=StatusUnobserved    OBSGEN_BAD=ObservedGenerationInvalid
  STATUS_STALE=StatusStale         TARGET_BAD=TargetRevisionInvalid
  STATE_BAD=StateInvalid           ATT_BAD=AttemptsInvalid
  TIME_BAD=TimestampsInvalid       DUP_TARGET=DuplicateTarget

This command is read-only. It does not patch or release a retry block.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return o.run(cmd.Context(), f, args[0])
		},
	}
	cmd.Flags().StringVar(&o.component, "component", "", "Component: engine, decoder or router (required)")
	cmd.Flags().StringVarP(&o.output, "output", "o", "table", "Output format: table, json or yaml")
	return cmd
}

func (o *retryBlocksOptions) run(ctx context.Context, f factory.Factory, name string) error {
	format, err := report.ParseFormat(o.output)
	if err != nil {
		return err
	}
	if problems := utilvalidation.IsDNS1123Subdomain(name); len(problems) > 0 {
		return ErrInvalidInferenceServiceName
	}
	component, err := retryBlocksComponent(o.component)
	if err != nil {
		return err
	}
	namespace, _, err := f.Namespace()
	if err != nil {
		return fmt.Errorf("resolve namespace: %w", err)
	}
	if namespace == "" || len(utilvalidation.IsDNS1123Label(namespace)) > 0 {
		return ErrInvalidNamespace
	}
	client, err := f.OMEClient()
	if err != nil {
		return fmt.Errorf("create OME client: %w", err)
	}
	getCtx, cancel := context.WithTimeout(ctx, o.deps.limits.RequestTimeout)
	isvc, err := client.OmeV1beta1().InferenceServices(namespace).Get(getCtx, name, metav1.GetOptions{})
	requestErr := getCtx.Err()
	cancel()
	if requestErr != nil {
		return requestErr
	}
	if err != nil {
		return fmt.Errorf("get InferenceService %q: %w", namespace+"/"+name, apierror.Friendly(err))
	}
	if isvc == nil {
		return ErrReturnedInferenceServiceNil
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
	if !validUID(string(isvc.UID)) {
		return ErrReturnedInferenceServiceUIDInvalid
	}

	collection, collectionErr := instancecollection.CollectRelated(
		ctx,
		client.OmeV1beta1().InferenceReplicas(namespace),
		isvc,
		instancecollection.Limits{
			Paging: o.deps.limits, MaxStatusRows: 1, MaxRetryBlocks: o.deps.maxRetryBlocks,
		},
	)
	if collectionErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if errors.Is(collectionErr, context.Canceled) {
			return collectionErr
		}
	}
	input := retryblockprojection.Input{
		InferenceService: isvc, Collection: collection, Component: component,
	}
	if collectionErr != nil {
		input.CollectionUnavailable = collectionUnavailableReason(collectionErr)
	}
	reportValue, err := o.deps.project(input, o.deps.clock)
	if err != nil {
		return fmt.Errorf("project retry blocks for InferenceService %q: %w", namespace+"/"+name, err)
	}
	if err := report.Write(o.streams.Out, format, reportValue); err != nil {
		return fmt.Errorf("write instance retry blocks: %w", err)
	}
	return nil
}

func retryBlocksComponent(value string) (omev1beta1.ComponentType, error) {
	if strings.TrimSpace(value) == "" {
		return "", ErrRetryBlocksComponentRequired
	}
	switch value {
	case string(omev1beta1.EngineComponent):
		return omev1beta1.EngineComponent, nil
	case string(omev1beta1.DecoderComponent):
		return omev1beta1.DecoderComponent, nil
	case string(omev1beta1.RouterComponent):
		return omev1beta1.RouterComponent, nil
	default:
		return "", ErrRetryBlocksComponentInvalid
	}
}
