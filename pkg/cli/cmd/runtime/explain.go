package runtime

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/apierror"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/namespace"
	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/runtimeselector"
)

type explainOptions struct {
	genericiooptions.IOStreams
	Model            string
	ISVC             string
	WithEffective    bool
	namespaceOptions *namespace.Options
	candidateLimits  paging.Limits
}

func newExplainCmd(f factory.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	o := &explainOptions{
		IOStreams: streams, namespaceOptions: namespace.NewOptions(),
		candidateLimits: defaultRuntimeExplainLimits(),
	}
	cmd := &cobra.Command{
		Use:   "explain (--model NAME | --isvc NAME)",
		Short: "Explain which serving runtimes match a model and why",
		Long: `Runs the operator's own runtime-selection engine (pkg/runtimeselector)
against the live cluster and prints every namespace-scoped and cluster-scoped
serving runtime it considered, whether each is compatible with the model, and
why -- including runtimes that were rejected.

Runtime candidates come from one all-or-nothing snapshot limited to 1,000
objects across 4 pages. Every snapshot API request has a 10-second timeout.
Selector ranking and any downstream selector reads use that immutable snapshot,
so the explanation cannot mix runtime revisions or issue one API read per
candidate.

With --isvc --with-effective, append independent live and active runtime
evidence. This does not change the selector verdict or imply rollout
convergence. Unavailable effective evidence is shown without hiding the
selector result.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := o.Validate(); err != nil {
				return err
			}
			return o.Run(cmd.Context(), f)
		},
	}
	cmd.Flags().StringVar(&o.Model, "model", "", "Explain runtime selection for this BaseModel/ClusterBaseModel")
	cmd.Flags().StringVar(&o.ISVC, "isvc", "", "Explain runtime selection for this InferenceService's model")
	cmd.Flags().BoolVar(&o.WithEffective, "with-effective", false, "Append bounded effective context for --isvc (auto scan: 1,000 items/2 pages)")
	o.namespaceOptions.AddOMEFlags(cmd.Flags())
	return cmd
}

func (o *explainOptions) Validate() error {
	if (o.Model == "") == (o.ISVC == "") {
		return fmt.Errorf("exactly one of --model or --isvc is required")
	}
	if o.WithEffective && o.ISVC == "" {
		return fmt.Errorf("--with-effective requires --isvc")
	}
	return nil
}

func (o *explainOptions) Run(ctx context.Context, f factory.Factory) error {
	ns, _, err := f.Namespace()
	if err != nil {
		return err
	}
	modelSpec, isvc, err := o.resolveTarget(ctx, f, ns)
	if err != nil {
		return err
	}

	ctrl, err := f.RuntimeClient()
	if err != nil {
		return err
	}
	snapshot, err := collectRuntimeCandidateSnapshot(
		ctx, ctrl, ns, o.candidateLimits,
	)
	if err != nil {
		return apierror.Friendly(err)
	}
	if err := snapshot.validate(); err != nil {
		return err
	}
	selector := runtimeselector.New(snapshot.client)
	// A standalone matcher/scorer pair, built from the exact same defaults
	// runtimeselector.New wires up internally, so explain.go can recompute
	// compatibility/auto-select/score details for a specific spec instead of
	// only getting a yes/no verdict out of the Selector interface.
	cfg := runtimeselector.NewConfig(snapshot.client)
	matcherImpl := runtimeselector.NewDefaultRuntimeMatcher(cfg)
	scorerImpl := runtimeselector.NewDefaultRuntimeScorer(cfg)

	// GetCompatibleRuntimes is the operator's actual auto-select ranking:
	// every entry it returns already passed compatibility, auto-select
	// eligibility and scoring, so these are unconditionally "Yes".
	matches, err := selector.GetCompatibleRuntimes(ctx, modelSpec, isvc, ns)
	if err != nil {
		return apierror.Friendly(err)
	}

	// GetCompatibleRuntimes silently drops everything that isn't a match.
	// The complete rejected set comes from the same immutable snapshot that
	// backed the selector, rather than a second live LIST.
	candidates := snapshot.candidates
	if o.ISVC != "" && isvc.Spec.Runtime != nil {
		if _, err := fmt.Fprintln(
			o.ErrOut,
			"Note: spec.runtime is explicit; automatic selection below is hypothetical.",
		); err != nil {
			return err
		}
	}
	if len(matches) == 0 && len(candidates) == 0 {
		message := "No serving runtimes found in the selected namespace or at cluster scope."
		if o.WithEffective {
			message = "No serving runtimes found for selector."
		}
		if _, err := fmt.Fprintln(o.ErrOut, message); err != nil {
			return err
		}
		if o.WithEffective {
			return o.writeEffectiveContext(ctx, f, ctrl, ns, isvc)
		}
		return nil
	}

	matched := make(map[runtimeKey]bool, len(matches))
	table := printers.Table{Headers: []string{"RUNTIME", "SCOPE", "COMPATIBLE", "PRIORITY", "WEIGHT", "REASON"}}
	for _, m := range matches {
		matched[runtimeKey{m.Name, m.IsCluster}] = true
		table.Rows = append(table.Rows, []string{
			m.Name, scopeLabel(m.IsCluster), "Yes",
			fmt.Sprintf("%d", m.MatchDetails.Priority),
			fmt.Sprintf("%d", m.MatchDetails.Weight),
			printers.OrDash(strings.Join(m.MatchDetails.Reasons, "; ")),
		})
	}
	for _, c := range candidates {
		if matched[runtimeKey(c)] {
			continue
		}
		spec := snapshot.rawSpec(c)
		if spec == nil {
			return errRuntimeSnapshotInconsistent
		}
		reason := evaluateReason(matcherImpl, scorerImpl, spec, modelSpec, isvc, c.name)
		table.Rows = append(table.Rows, []string{c.name, scopeLabel(c.isCluster), "No", "-", "-", reason})
	}

	if err := o.writeSelectorTable(table); err != nil {
		return err
	}
	if o.WithEffective {
		return o.writeEffectiveContext(ctx, f, ctrl, ns, isvc)
	}
	return nil
}

// resolveTarget loads the model spec (and, for --isvc, the service) using the
// operator's resolution order: namespaced BaseModel first, then
// ClusterBaseModel.
func (o *explainOptions) resolveTarget(ctx context.Context, f factory.Factory, ns string) (*v1beta1.BaseModelSpec, *v1beta1.InferenceService, error) {
	ome, err := f.OMEClient()
	if err != nil {
		return nil, nil, err
	}
	modelName := o.Model
	// Synthetic isvc gives the selector namespace context on the --model path.
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns}}
	if o.ISVC != "" {
		got, err := ome.OmeV1beta1().InferenceServices(ns).Get(ctx, o.ISVC, metav1.GetOptions{})
		if err != nil {
			return nil, nil, apierror.Friendly(err)
		}
		if got.Spec.Model == nil {
			return nil, nil, fmt.Errorf("InferenceService %q has no spec.model; pass --model instead", o.ISVC)
		}
		isvc = got
		modelName = got.Spec.Model.Name
	}
	if bm, err := ome.OmeV1beta1().BaseModels(ns).Get(ctx, modelName, metav1.GetOptions{}); err == nil {
		return &bm.Spec, isvc, nil
	} else if !kerrors.IsNotFound(err) {
		return nil, nil, apierror.Friendly(err)
	}
	cbm, err := ome.OmeV1beta1().ClusterBaseModels().Get(ctx, modelName, metav1.GetOptions{})
	if err != nil {
		return nil, nil, apierror.Friendly(err)
	}
	return &cbm.Spec, isvc, nil
}

// runtimeKey identifies a runtime by name and scope; namespace-scoped and
// cluster-scoped runtimes sharing a name are distinct candidates, matching
// how pkg/runtimeselector itself never deduplicates across scope.
type runtimeKey struct {
	name      string
	isCluster bool
}

type runtimeCandidate struct {
	name      string
	isCluster bool
}

// evaluateReason evaluates spec against model/isvc directly with the exported
// matcher/scorer and returns the REASON text for why it is not a match.
// Mirrors, in order, the checks that keep a runtime out of
// GetCompatibleRuntimes' matches: compatibility (disabled, accelerator
// class, deployment mode, format, size -- all folded into
// matcher.GetCompatibilityDetails), then evaluateRuntime's auto-select gates
// (pkg/runtimeselector/selector.go): some format has autoSelect=true, the
// specific matching format has autoSelect=true, and CalculateScore > 0.
func evaluateReason(
	matcher runtimeselector.RuntimeMatcher,
	scorer runtimeselector.RuntimeScorer,
	spec *v1beta1.ServingRuntimeSpec,
	model *v1beta1.BaseModelSpec,
	isvc *v1beta1.InferenceService,
	name string,
) string {
	report, err := matcher.GetCompatibilityDetails(spec, model, isvc, name)
	if err != nil {
		return err.Error()
	}
	if !report.IsCompatible {
		if len(report.IncompatibilityReasons) > 0 {
			return report.IncompatibilityReasons[0]
		}
		return "incompatible model format"
	}

	if !hasAutoSelectFormat(spec) {
		return "supports the model but has no supportedModelFormats[].autoSelect=true entry, so automatic selection skips it (pin it explicitly via spec.runtime.name instead)"
	}
	if !report.MatchDetails.AutoSelectEnabled {
		return fmt.Sprintf(
			"matching format %s is not autoSelect-enabled (a different supportedModelFormats entry on this runtime has autoSelect=true, but not the one that matches this model)",
			matchingFormatName(spec, model))
	}

	if score, scoreErr := scorer.CalculateScore(spec, model); scoreErr == nil && score <= 0 {
		if report.MatchDetails.Priority == 0 {
			return "auto-select score is 0 (format priority 0)"
		}
		return "auto-select score is 0"
	}

	return "compatible and auto-select eligible, but automatic selection did not choose it (unexpected)"
}

// hasAutoSelectFormat reports whether ANY supportedModelFormats entry has
// autoSelect=true, regardless of whether that entry is the one that matches
// this particular model. Faithfully replicates the unexported
// runtimeHasAutoSelectFormat in pkg/runtimeselector/matcher.go, which gates
// GetCompatibleRuntimes' evaluateRuntime (pkg/runtimeselector/selector.go)
// and isn't itself exported.
func hasAutoSelectFormat(spec *v1beta1.ServingRuntimeSpec) bool {
	for _, format := range spec.SupportedModelFormats {
		if format.AutoSelect != nil && *format.AutoSelect {
			return true
		}
	}
	return false
}

// matchingFormatName returns the ModelFormat.Name of the supportedModelFormats
// entry that determines compatibility for model, using the same primary key
// (ModelFormat.Name equality) that pkg/runtimeselector/matcher.go's
// unexported compareSupportedModelFormats checks first. Used only to make
// the REASON text specific -- the compatible/incompatible verdict itself
// always comes from matcher.GetCompatibilityDetails, never from this lookup.
func matchingFormatName(spec *v1beta1.ServingRuntimeSpec, model *v1beta1.BaseModelSpec) string {
	for _, format := range spec.SupportedModelFormats {
		if format.ModelFormat != nil && format.ModelFormat.Name == model.ModelFormat.Name {
			return format.ModelFormat.Name
		}
	}
	return model.ModelFormat.Name
}

func scopeLabel(isCluster bool) string {
	if isCluster {
		return "Cluster"
	}
	return "Namespaced"
}
