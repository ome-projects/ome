package runtime

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/apierror"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/namespace"
	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/runtimeinheritance"
	"sigs.k8s.io/ome/pkg/runtimeselector"
)

type explainOptions struct {
	genericiooptions.IOStreams
	Model            string
	ISVC             string
	WithEffective    bool
	namespaceOptions *namespace.Options
}

func newExplainCmd(f factory.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	o := &explainOptions{IOStreams: streams, namespaceOptions: namespace.NewOptions()}
	cmd := &cobra.Command{
		Use:   "explain (--model NAME | --isvc NAME)",
		Short: "Explain which serving runtimes match a model and why",
		Long: `Runs the operator's own runtime-selection engine (pkg/runtimeselector)
against the live cluster and prints every namespace-scoped and cluster-scoped
serving runtime it considered, whether each is compatible with the model, and
why -- including runtimes that were rejected.

With --isvc --with-effective, append independent live and active runtime
evidence. This does not change the selector verdict or imply rollout
convergence. Named runtime reads are bounded; an auto-selected runtime may
require an additional candidate scan limited to 1,000 items across 2 pages
with a 10-second request timeout. Unavailable effective evidence is shown
without hiding the selector result.`,
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
	if o.ISVC != "" && isvc.Spec.Runtime != nil {
		if o.WithEffective {
			fmt.Fprintln(o.ErrOut, "Note: spec.runtime is explicit; selector verdict is hypothetical.")
		} else {
			fmt.Fprintf(o.ErrOut, "Note: InferenceService %q pins spec.runtime=%q; the table below shows what automatic selection would choose, which may differ from what is currently deployed.\n", o.ISVC, isvc.Spec.Runtime.Name)
		}
	}

	ctrl, err := f.RuntimeClient()
	if err != nil {
		return err
	}
	selector := runtimeselector.New(ctrl)
	// A standalone matcher/scorer pair, built from the exact same defaults
	// runtimeselector.New wires up internally, so explain.go can recompute
	// compatibility/auto-select/score details for a specific spec instead of
	// only getting a yes/no verdict out of the Selector interface.
	cfg := runtimeselector.NewConfig(ctrl)
	matcherImpl := runtimeselector.NewDefaultRuntimeMatcher(cfg)
	scorerImpl := runtimeselector.NewDefaultRuntimeScorer(cfg)

	// GetCompatibleRuntimes is the operator's actual auto-select ranking:
	// every entry it returns already passed compatibility, auto-select
	// eligibility and scoring, so these are unconditionally "Yes".
	matches, err := selector.GetCompatibleRuntimes(ctx, modelSpec, isvc, ns)
	if err != nil {
		return apierror.Friendly(err)
	}

	// GetCompatibleRuntimes silently drops everything that isn't a match --
	// disabled runtimes, format/size mismatches, and runtimes with no
	// autoSelect-enabled format never appear in its result at all. The OEP
	// requires rejected runtimes to show up too, with a reason, so list every
	// runtime the selector could have considered and re-validate whichever
	// ones didn't make the cut.
	candidates, err := listCandidateRuntimes(ctx, ctrl, ns)
	if err != nil {
		return apierror.Friendly(err)
	}
	if len(matches) == 0 && len(candidates) == 0 {
		if o.WithEffective {
			fmt.Fprintln(o.ErrOut, "No serving runtimes found for selector.")
		} else {
			fmt.Fprintf(o.ErrOut, "No serving runtimes found in namespace %q or at cluster scope.\n", ns)
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
	// Cluster-scoped candidates whose name is shadowed by a namespace-scoped
	// runtime of the same name need special handling below (defect b).
	shadowed := shadowedClusterNames(candidates)
	for _, c := range candidates {
		if matched[runtimeKey(c)] {
			continue
		}
		reason := candidateReason(ctx, selector, ctrl, matcherImpl, scorerImpl, c, shadowed[c.name], ns, modelSpec, isvc)
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

// listCandidateRuntimes enumerates every runtime GetCompatibleRuntimes could
// have considered: namespace-scoped runtimes in ns, plus every cluster-scoped
// runtime. This mirrors pkg/runtimeselector's own fetch (namespace runtimes
// always precede cluster ones) so rejected runtimes can be re-validated for a
// reason instead of silently vanishing from the table.
func listCandidateRuntimes(ctx context.Context, c ctrlclient.Client, ns string) ([]runtimeCandidate, error) {
	var out []runtimeCandidate

	nsRuntimes := &v1beta1.ServingRuntimeList{}
	if err := c.List(ctx, nsRuntimes, ctrlclient.InNamespace(ns)); err != nil {
		return nil, err
	}
	for _, rt := range nsRuntimes.Items {
		out = append(out, runtimeCandidate{name: rt.Name})
	}

	clusterRuntimes := &v1beta1.ClusterServingRuntimeList{}
	if err := c.List(ctx, clusterRuntimes); err != nil {
		return nil, err
	}
	for _, rt := range clusterRuntimes.Items {
		out = append(out, runtimeCandidate{name: rt.Name, isCluster: true})
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].isCluster != out[j].isCluster {
			return !out[i].isCluster // namespace-scoped first, matching GetCompatibleRuntimes' ordering
		}
		return out[i].name < out[j].name
	})
	return out, nil
}

// rejectionReason extracts a human-readable, non-redundant reason from the
// errors ValidateRuntime returns. Unrecognized error types fall back to
// err.Error() so the table always has something to show.
func rejectionReason(err error) string {
	switch e := err.(type) {
	case *runtimeselector.RuntimeCompatibilityError:
		return e.Reason
	case *runtimeselector.RuntimeDisabledError:
		return "runtime is disabled"
	default:
		return err.Error()
	}
}

// shadowedClusterNames returns the set of names that identify both a
// cluster-scoped and a namespace-scoped candidate in candidates.
// pkg/runtimeinheritance documents name-based shadowing as an intentional
// pattern (a namespace-scoped runtime overrides a cluster-scoped one of the
// same name), but it also defeats ValidateRuntime's name-only,
// namespaced-first resolution (pkg/runtimeselector/fetcher.go GetRuntime)
// for the cluster-scoped side -- see candidateReason.
func shadowedClusterNames(candidates []runtimeCandidate) map[string]bool {
	nsNames := make(map[string]bool)
	for _, c := range candidates {
		if !c.isCluster {
			nsNames[c.name] = true
		}
	}
	shadowed := make(map[string]bool)
	for _, c := range candidates {
		if c.isCluster && nsNames[c.name] {
			shadowed[c.name] = true
		}
	}
	return shadowed
}

// candidateReason produces the REASON text for a candidate GetCompatibleRuntimes
// did not return. Two defects made this text unreliable before:
//
//   - (a) A hardcoded "no autoSelect entry" reason was shown whenever
//     ValidateRuntime returned nil, but ValidateRuntime only checks
//     compatibility. GetCompatibleRuntimes' evaluateRuntime
//     (pkg/runtimeselector/selector.go) additionally requires an
//     autoSelect-enabled *matching* format and a positive score
//     (pkg/runtimeselector/scorer.go CalculateScore), so a compatible
//     runtime can be excluded for reasons the hardcoded text never
//     considered: the matching format specifically isn't autoSelect-enabled
//     (even though a different format on the same runtime is), or its score
//     computes to 0 (e.g. an explicit Priority of 0).
//   - (b) ValidateRuntime resolves a runtime by name only, namespaced first
//     (pkg/runtimeselector/fetcher.go GetRuntime), so re-validating a
//     cluster-scoped candidate whose name collides with a namespace-scoped
//     runtime of the same name silently validated the WRONG object.
//
// shadowedByNamespaced (only meaningful when c.isCluster) signals case (b).
func candidateReason(
	ctx context.Context,
	sel runtimeselector.Selector,
	ctrl ctrlclient.Client,
	matcher runtimeselector.RuntimeMatcher,
	scorer runtimeselector.RuntimeScorer,
	c runtimeCandidate,
	shadowedByNamespaced bool,
	ns string,
	model *v1beta1.BaseModelSpec,
	isvc *v1beta1.InferenceService,
) string {
	if c.isCluster && shadowedByNamespaced {
		// fetcher.GetRuntime (used by both ValidateRuntime and
		// Selector.GetRuntime) always checks the namespace-scoped runtime of
		// this name first, so calling either here would silently grade the
		// NAMESPACED object and report its reason on this CLUSTER row.
		// ResolveClusterRuntime only ever Gets a ClusterServingRuntime, so
		// it cannot be shadowed the same way -- resolve (with inheritance,
		// same as ValidateRuntime would apply) and evaluate directly.
		spec, _, err := runtimeinheritance.ResolveClusterRuntime(ctx, ctrl, c.name)
		if err != nil {
			return fmt.Sprintf("shadowed by a namespaced runtime of the same name, so it could not be re-validated by name; resolving the cluster runtime directly also failed: %v", err)
		}
		return evaluateReason(matcher, scorer, spec, model, isvc, c.name)
	}

	if err := sel.ValidateRuntime(ctx, c.name, model, isvc); err != nil {
		return rejectionReason(err)
	}

	// ValidateRuntime agrees the runtime is compatible, yet it is missing
	// from GetCompatibleRuntimes -- defect (a): recompute the real
	// auto-select/score cause instead of assuming "no autoSelect entry".
	spec, _, err := sel.GetRuntime(ctx, c.name, ns, "")
	if err != nil {
		return "runtime is compatible but was not auto-selected, and the exact cause could not be recomputed: " + err.Error()
	}
	return evaluateReason(matcher, scorer, spec, model, isvc, c.name)
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
