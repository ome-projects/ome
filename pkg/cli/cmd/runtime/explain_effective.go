package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/effective"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/runtimeprojection"
)

const explainEffectiveValueWidth = 54

const explainEffectiveTerminalWidth = 80

type explainWidthWriter struct {
	io.Writer
	width int
}

func (w explainWidthWriter) TerminalWidth() (int, bool) { return w.width, true }

func (o *explainOptions) writeSelectorTable(table printers.Table) error {
	if !o.WithEffective {
		return table.Write(o.Out)
	}
	width := explainEffectiveTerminalWidth
	if terminalWidth, terminal := printers.TerminalWidth(o.Out); terminal && terminalWidth < width {
		width = terminalWidth
	}
	// The opt-in view reuses the terminal renderer, which wraps rather than
	// truncating the original selector reason and keeps every line bounded.
	return table.Write(explainWidthWriter{Writer: o.Out, width: width})
}

// writeEffectiveContext appends a separate, allowlisted observation. It does
// not change or reinterpret the selector verdict already printed above it.
// An unavailable observation is deliberately not a selector failure.
func (o *explainOptions) writeEffectiveContext(
	ctx context.Context,
	f factory.Factory,
	runtimeClient ctrlclient.Client,
	namespace string,
	isvc *v1beta1.InferenceService,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	projected, err := o.projectEffectiveContext(ctx, f, runtimeClient, namespace, isvc)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if _, writeErr := fmt.Fprint(o.Out, "\nEffective context (separate observation; selector verdict unchanged):\n"); writeErr != nil {
		return writeErr
	}
	if err != nil {
		// Never print the raw resolver error: it may include user-authored
		// runtime fields or API details. The selector result remains usable.
		return (report.Table{
			Headers: []string{"SCOPE", "FIELD", "VALUE"},
			Rows: [][]string{
				{"Service", "EVIDENCE", "Unavailable"},
				{"Service", "CAVEAT", "Selector verdict above is unchanged"},
			},
		}).Write(o.Out)
	}
	return explainEffectiveTable(projected.Content).Write(o.Out)
}

func (o *explainOptions) projectEffectiveContext(
	ctx context.Context,
	f factory.Factory,
	runtimeClient ctrlclient.Client,
	namespace string,
	isvc *v1beta1.InferenceService,
) (reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeEffectiveContent], error) {
	var empty reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeEffectiveContent]
	if isvc == nil || bindInferenceService(isvc, namespace, o.ISVC) != nil {
		return empty, errors.New("InferenceService identity is unavailable")
	}
	resolved, err := o.namespaceOptions.Resolve(namespace)
	if err != nil {
		return empty, err
	}
	kubeClient, err := f.KubeClient()
	if err != nil {
		return empty, err
	}
	if kubeClient == nil || runtimeClient == nil {
		return empty, errors.New("runtime evidence clients are unavailable")
	}
	limits := paging.Limits{
		PageSize: paging.ChunkSize, MaxItems: 1000, MaxPages: 2,
		RequestTimeout: 10 * time.Second,
	}
	liveResolver, err := effective.NewBoundedRuntimeResolver(runtimeClient, limits)
	if err != nil {
		return empty, err
	}
	resolver, err := effective.NewRuntimePinResolver(
		kubeClient.AppsV1(), liveResolver, resolved.OMENamespace, limits,
	)
	if err != nil {
		return empty, err
	}
	state, err := resolver.Resolve(ctx, isvc, effective.RuntimeResolveOptions{IncludeHistory: false})
	if err != nil {
		return empty, err
	}
	return runtimeprojection.ProjectEffective(isvc, state, reportv1alpha1.SystemClock{})
}

func explainEffectiveTable(content reportv1alpha1.RuntimeEffectiveContent) report.Table {
	table := content.Table()
	rows := [][]string{
		{"Service", "SELECTION", string(content.Selection.Source)},
		{"Service", "SOURCE", explainRuntimeReference(content.Selection.Runtime)},
		{"Source", "INHERITANCE", string(content.Inheritance.State)},
	}
	if content.Inheritance.UnavailableReason != "" {
		rows = append(rows, []string{"Source", "REASON", string(content.Inheritance.UnavailableReason)})
	}
	for i := range content.Inheritance.Sources {
		rows = append(rows, []string{
			"Source", "ROOT-FIRST", explainRuntimeReference(&content.Inheritance.Sources[i]),
		})
	}
	rows = append(rows, []string{"Service", "CAVEAT", "Independent snapshot; not rollout convergence"})
	table.Rows = append(rows, table.Rows...)
	return table
}

func explainRuntimeReference(reference *reportv1alpha1.RuntimeObjectReference) string {
	if reference == nil {
		return "-"
	}
	kind := "Unknown"
	switch reference.Kind {
	case reportv1alpha1.RuntimeKindServingRuntime:
		kind = "SR"
	case reportv1alpha1.RuntimeKindClusterServingRuntime:
		kind = "CSR"
	}
	value := kind + "/"
	if reference.Namespace != "" {
		value += reference.Namespace + "/"
	}
	value += reference.Name
	return printers.BoundedMiddleCell(value, explainEffectiveValueWidth)
}
