// Package get implements `kubectl ome get`: a table-driven view over every
// OME resource. Each resource is one registry entry; adding a resource means
// adding an entry, never a new command.
package get

import (
	"context"
	"fmt"
	"sort"
	"strings"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"knative.dev/pkg/apis"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/cli/printers"
)

type listFunc func(ctx context.Context, f factory.Factory, ns string, opts metav1.ListOptions) ([]runtime.Object, error)

type getFunc func(ctx context.Context, f factory.Factory, ns, name string) (runtime.Object, error)

type column struct {
	Name    string
	Wide    bool // shown only with -o wide
	Extract func(obj runtime.Object) string
}

type entry struct {
	Canonical  string
	Aliases    []string
	Namespaced bool
	Columns    []column
	TableRows  func(runtime.Object, bool) [][]string
	List       listFunc
	GetOne     getFunc
}

func resolve(resource string) (*entry, error) {
	needle := strings.ToLower(resource)
	for _, e := range registry {
		if e.Canonical == needle {
			return e, nil
		}
		for _, a := range e.Aliases {
			if a == needle {
				return e, nil
			}
		}
	}
	names := make([]string, 0, len(registry))
	for _, e := range registry {
		names = append(names, e.Canonical)
	}
	sort.Strings(names)
	return nil, fmt.Errorf("unknown resource %q (valid resources: %s)", resource, strings.Join(names, ", "))
}

// nameCol adapts a typed extractor (e.g. func(*v1beta1.InferenceService) string)
// to the runtime.Object-keyed column.Extract signature, so each entry's
// column table can be written against its concrete API type instead of
// runtime.Object.
func nameCol[T runtime.Object](extract func(T) string) func(runtime.Object) string {
	return func(o runtime.Object) string { return extract(o.(T)) }
}

// safeCol is nameCol's nil-safe sibling: a wrong-typed object returns "?"
// instead of panicking through a failed type assertion. The Task 2.3
// long-tail entries (acceleratorclasses, benchmarkjobs, finetunedweights,
// inferencereplicas, workloadclusters) use this instead of nameCol because
// TestLongTailColumnsWrongType requires every one of their columns to
// survive an object of the wrong resource kind.
func safeCol[T runtime.Object](extract func(T) string) func(runtime.Object) string {
	return func(o runtime.Object) string {
		t, ok := o.(T)
		if !ok {
			return "?"
		}
		return extract(t)
	}
}

// derefOrDash dereferences a *string for column rendering, substituting "-"
// for a nil pointer (mirrors baseModelColumns' local deref helper, promoted
// to package scope since finetunedweights needs it for two separate fields).
func derefOrDash(p *string) string {
	if p == nil {
		return "-"
	}
	return printers.OrDash(*p)
}

var isvcEntry = &entry{
	Canonical:  "inferenceservices",
	Aliases:    []string{"inferenceservice", "isvc", "isvcs"},
	Namespaced: true,
	Columns: []column{
		{Name: "NAME", Extract: nameCol(func(i *v1beta1.InferenceService) string { return i.Name })},
		{Name: "MODEL", Extract: nameCol(func(i *v1beta1.InferenceService) string {
			if i.Spec.Model == nil {
				return "-"
			}
			return i.Spec.Model.Name
		})},
		{Name: "RUNTIME", Extract: nameCol(func(i *v1beta1.InferenceService) string {
			if i.Spec.Runtime == nil {
				return "-" // auto-selected; status view shows the resolved one
			}
			return i.Spec.Runtime.Name
		})},
		{Name: "READY", Extract: nameCol(func(i *v1beta1.InferenceService) string {
			c := i.Status.GetCondition(apis.ConditionReady)
			if c == nil {
				return "Unknown"
			}
			return string(c.Status)
		})},
		{Name: "URL", Extract: nameCol(func(i *v1beta1.InferenceService) string {
			if i.Status.URL == nil {
				return "-"
			}
			return i.Status.URL.String()
		})},
		{Name: "AGE", Extract: nameCol(func(i *v1beta1.InferenceService) string {
			return printers.Age(i.CreationTimestamp)
		})},
	},
	List: func(ctx context.Context, f factory.Factory, ns string, opts metav1.ListOptions) ([]runtime.Object, error) {
		c, err := f.OMEClient()
		if err != nil {
			return nil, err
		}
		return paging.ListAllPaged(ctx, func(pageOpts metav1.ListOptions) ([]runtime.Object, string, error) {
			pageOpts.LabelSelector = opts.LabelSelector
			l, err := c.OmeV1beta1().InferenceServices(ns).List(ctx, pageOpts)
			if err != nil {
				return nil, "", err
			}
			items := make([]runtime.Object, 0, len(l.Items))
			for i := range l.Items {
				items = append(items, &l.Items[i])
			}
			return items, l.Continue, nil
		})
	},
	GetOne: func(ctx context.Context, f factory.Factory, ns, name string) (runtime.Object, error) {
		c, err := f.OMEClient()
		if err != nil {
			return nil, err
		}
		return c.OmeV1beta1().InferenceServices(ns).Get(ctx, name, metav1.GetOptions{})
	},
}

// baseModelColumns builds the shared column set for BaseModel/ClusterBaseModel
// objects. withScope adds a SCOPE column distinguishing namespaced vs
// cluster-scoped entries; used only by the merged `models` view since the
// single-kind `basemodels`/`clusterbasemodels` views have a fixed scope.
func baseModelColumns(withScope bool) []column {
	cols := []column{
		{Name: "NAME", Extract: func(o runtime.Object) string {
			switch m := o.(type) {
			case *v1beta1.BaseModel:
				return m.Name
			case *v1beta1.ClusterBaseModel:
				return m.Name
			}
			return "?"
		}},
	}
	if withScope {
		cols = append(cols, column{Name: "SCOPE", Extract: func(o runtime.Object) string {
			switch o.(type) {
			case *v1beta1.ClusterBaseModel:
				return "Cluster"
			case *v1beta1.BaseModel:
				return "Namespaced"
			}
			return "?"
		}})
	}
	spec := func(o runtime.Object) *v1beta1.BaseModelSpec {
		switch m := o.(type) {
		case *v1beta1.BaseModel:
			return &m.Spec
		case *v1beta1.ClusterBaseModel:
			return &m.Spec
		}
		return nil
	}
	status := func(o runtime.Object) *v1beta1.ModelStatusSpec {
		switch m := o.(type) {
		case *v1beta1.BaseModel:
			return &m.Status
		case *v1beta1.ClusterBaseModel:
			return &m.Status
		}
		return nil
	}
	deref := func(p *string) string {
		if p == nil {
			return "-"
		}
		return *p
	}
	cols = append(cols,
		column{Name: "ARCH", Extract: func(o runtime.Object) string {
			s := spec(o)
			if s == nil {
				return "?"
			}
			return deref(s.ModelArchitecture)
		}},
		column{Name: "PARAMS", Extract: func(o runtime.Object) string {
			s := spec(o)
			if s == nil {
				return "?"
			}
			return deref(s.ModelParameterSize)
		}},
		column{Name: "FORMAT", Extract: func(o runtime.Object) string {
			s := spec(o)
			if s == nil {
				return "?"
			}
			return printers.OrDash(s.ModelFormat.Name)
		}},
		column{Name: "STATE", Extract: func(o runtime.Object) string {
			s := status(o)
			if s == nil {
				return "?"
			}
			return printers.OrDash(string(s.State))
		}},
		column{Name: "AGE", Extract: func(o runtime.Object) string {
			acc, ok := o.(metav1.Object)
			if !ok {
				return "?"
			}
			return printers.Age(metav1.Time{Time: acc.GetCreationTimestamp().Time})
		}},
	)
	return cols
}

// modelsEntry is the merged view: BaseModels from the target namespace (all
// namespaces with -A) plus ClusterBaseModels always. A single-name lookup
// resolves BaseModel first, falling back to ClusterBaseModel on NotFound —
// the same precedence the operator itself applies when resolving an
// InferenceService's model reference (see pkg/modelparser).
var modelsEntry = &entry{
	Canonical:  "models",
	Aliases:    []string{"model"},
	Namespaced: true, // namespace scopes the BaseModel half of the view
	Columns:    baseModelColumns(true),
	List: func(ctx context.Context, f factory.Factory, ns string, opts metav1.ListOptions) ([]runtime.Object, error) {
		c, err := f.OMEClient()
		if err != nil {
			return nil, err
		}
		var out []runtime.Object
		bms, err := paging.ListAllPaged(ctx, func(pageOpts metav1.ListOptions) ([]runtime.Object, string, error) {
			pageOpts.LabelSelector = opts.LabelSelector
			l, err := c.OmeV1beta1().BaseModels(ns).List(ctx, pageOpts)
			if err != nil {
				return nil, "", err
			}
			items := make([]runtime.Object, 0, len(l.Items))
			for i := range l.Items {
				items = append(items, &l.Items[i])
			}
			return items, l.Continue, nil
		})
		if err != nil {
			return nil, err
		}
		out = append(out, bms...)
		cbms, err := paging.ListAllPaged(ctx, func(pageOpts metav1.ListOptions) ([]runtime.Object, string, error) {
			pageOpts.LabelSelector = opts.LabelSelector
			l, err := c.OmeV1beta1().ClusterBaseModels().List(ctx, pageOpts)
			if err != nil {
				return nil, "", err
			}
			items := make([]runtime.Object, 0, len(l.Items))
			for i := range l.Items {
				items = append(items, &l.Items[i])
			}
			return items, l.Continue, nil
		})
		if err != nil {
			return nil, err
		}
		out = append(out, cbms...)
		return out, nil
	},
	GetOne: func(ctx context.Context, f factory.Factory, ns, name string) (runtime.Object, error) {
		c, err := f.OMEClient()
		if err != nil {
			return nil, err
		}
		if bm, err := c.OmeV1beta1().BaseModels(ns).Get(ctx, name, metav1.GetOptions{}); err == nil {
			return bm, nil
		} else if !kerrors.IsNotFound(err) {
			return nil, err
		}
		return c.OmeV1beta1().ClusterBaseModels().Get(ctx, name, metav1.GetOptions{})
	},
}

var baseModelsEntry = &entry{
	Canonical:  "basemodels",
	Aliases:    []string{"basemodel", "bm"},
	Namespaced: true,
	Columns:    baseModelColumns(false),
	List: func(ctx context.Context, f factory.Factory, ns string, opts metav1.ListOptions) ([]runtime.Object, error) {
		c, err := f.OMEClient()
		if err != nil {
			return nil, err
		}
		return paging.ListAllPaged(ctx, func(pageOpts metav1.ListOptions) ([]runtime.Object, string, error) {
			pageOpts.LabelSelector = opts.LabelSelector
			l, err := c.OmeV1beta1().BaseModels(ns).List(ctx, pageOpts)
			if err != nil {
				return nil, "", err
			}
			items := make([]runtime.Object, 0, len(l.Items))
			for i := range l.Items {
				items = append(items, &l.Items[i])
			}
			return items, l.Continue, nil
		})
	},
	GetOne: func(ctx context.Context, f factory.Factory, ns, name string) (runtime.Object, error) {
		c, err := f.OMEClient()
		if err != nil {
			return nil, err
		}
		return c.OmeV1beta1().BaseModels(ns).Get(ctx, name, metav1.GetOptions{})
	},
}

var clusterBaseModelsEntry = &entry{
	Canonical:  "clusterbasemodels",
	Aliases:    []string{"clusterbasemodel", "cbm"},
	Namespaced: false,
	Columns:    baseModelColumns(false),
	List: func(ctx context.Context, f factory.Factory, ns string, opts metav1.ListOptions) ([]runtime.Object, error) {
		c, err := f.OMEClient()
		if err != nil {
			return nil, err
		}
		return paging.ListAllPaged(ctx, func(pageOpts metav1.ListOptions) ([]runtime.Object, string, error) {
			pageOpts.LabelSelector = opts.LabelSelector
			l, err := c.OmeV1beta1().ClusterBaseModels().List(ctx, pageOpts)
			if err != nil {
				return nil, "", err
			}
			items := make([]runtime.Object, 0, len(l.Items))
			for i := range l.Items {
				items = append(items, &l.Items[i])
			}
			return items, l.Continue, nil
		})
	},
	GetOne: func(ctx context.Context, f factory.Factory, ns, name string) (runtime.Object, error) {
		c, err := f.OMEClient()
		if err != nil {
			return nil, err
		}
		return c.OmeV1beta1().ClusterBaseModels().Get(ctx, name, metav1.GetOptions{})
	},
}

// runtimeColumns builds the shared column set for ServingRuntime/
// ClusterServingRuntime objects. withScope adds a SCOPE column; used only by
// the merged `runtimes` view, mirroring baseModelColumns.
func runtimeColumns(withScope bool) []column {
	spec := func(o runtime.Object) *v1beta1.ServingRuntimeSpec {
		switch r := o.(type) {
		case *v1beta1.ServingRuntime:
			return &r.Spec
		case *v1beta1.ClusterServingRuntime:
			return &r.Spec
		}
		return nil
	}
	cols := []column{
		{Name: "NAME", Extract: func(o runtime.Object) string {
			acc, ok := o.(metav1.Object)
			if !ok {
				return "?"
			}
			return acc.GetName()
		}},
	}
	if withScope {
		cols = append(cols, column{Name: "SCOPE", Extract: func(o runtime.Object) string {
			switch o.(type) {
			case *v1beta1.ClusterServingRuntime:
				return "Cluster"
			case *v1beta1.ServingRuntime:
				return "Namespaced"
			}
			return "?"
		}})
	}
	cols = append(cols,
		column{Name: "DISABLED", Extract: func(o runtime.Object) string {
			s := spec(o)
			if s == nil {
				return "?"
			}
			d := s.Disabled
			return fmt.Sprintf("%t", d != nil && *d)
		}},
		column{Name: "FORMATS", Extract: func(o runtime.Object) string {
			s := spec(o)
			if s == nil {
				return "?"
			}
			fmts := s.SupportedModelFormats
			names := make([]string, 0, len(fmts))
			for _, sf := range fmts {
				switch {
				case sf.Name != "":
					names = append(names, sf.Name)
				case sf.ModelFormat != nil && sf.ModelFormat.Name != "":
					names = append(names, sf.ModelFormat.Name)
				}
			}
			if len(names) > 3 {
				return strings.Join(names[:3], ",") + fmt.Sprintf(",+%d", len(names)-3)
			}
			return printers.OrDash(strings.Join(names, ","))
		}},
		column{Name: "AGE", Extract: func(o runtime.Object) string {
			acc, ok := o.(metav1.Object)
			if !ok {
				return "?"
			}
			return printers.Age(metav1.Time{Time: acc.GetCreationTimestamp().Time})
		}},
	)
	return cols
}

// runtimesEntry is the merged view: ServingRuntimes from the target
// namespace (all namespaces with -A) plus ClusterServingRuntimes always. A
// single-name lookup resolves ServingRuntime first, falling back to
// ClusterServingRuntime on NotFound — mirroring modelsEntry's precedence.
var runtimesEntry = &entry{
	Canonical:  "runtimes",
	Aliases:    []string{"runtime"},
	Namespaced: true, // namespace scopes the ServingRuntime half of the view
	Columns:    runtimeColumns(true),
	List: func(ctx context.Context, f factory.Factory, ns string, opts metav1.ListOptions) ([]runtime.Object, error) {
		c, err := f.OMEClient()
		if err != nil {
			return nil, err
		}
		var out []runtime.Object
		srts, err := paging.ListAllPaged(ctx, func(pageOpts metav1.ListOptions) ([]runtime.Object, string, error) {
			pageOpts.LabelSelector = opts.LabelSelector
			l, err := c.OmeV1beta1().ServingRuntimes(ns).List(ctx, pageOpts)
			if err != nil {
				return nil, "", err
			}
			items := make([]runtime.Object, 0, len(l.Items))
			for i := range l.Items {
				items = append(items, &l.Items[i])
			}
			return items, l.Continue, nil
		})
		if err != nil {
			return nil, err
		}
		out = append(out, srts...)
		csrts, err := paging.ListAllPaged(ctx, func(pageOpts metav1.ListOptions) ([]runtime.Object, string, error) {
			pageOpts.LabelSelector = opts.LabelSelector
			l, err := c.OmeV1beta1().ClusterServingRuntimes().List(ctx, pageOpts)
			if err != nil {
				return nil, "", err
			}
			items := make([]runtime.Object, 0, len(l.Items))
			for i := range l.Items {
				items = append(items, &l.Items[i])
			}
			return items, l.Continue, nil
		})
		if err != nil {
			return nil, err
		}
		out = append(out, csrts...)
		return out, nil
	},
	GetOne: func(ctx context.Context, f factory.Factory, ns, name string) (runtime.Object, error) {
		c, err := f.OMEClient()
		if err != nil {
			return nil, err
		}
		if srt, err := c.OmeV1beta1().ServingRuntimes(ns).Get(ctx, name, metav1.GetOptions{}); err == nil {
			return srt, nil
		} else if !kerrors.IsNotFound(err) {
			return nil, err
		}
		return c.OmeV1beta1().ClusterServingRuntimes().Get(ctx, name, metav1.GetOptions{})
	},
}

var servingRuntimesEntry = &entry{
	Canonical:  "servingruntimes",
	Aliases:    []string{"servingruntime", "srt"},
	Namespaced: true,
	Columns:    runtimeColumns(false),
	List: func(ctx context.Context, f factory.Factory, ns string, opts metav1.ListOptions) ([]runtime.Object, error) {
		c, err := f.OMEClient()
		if err != nil {
			return nil, err
		}
		return paging.ListAllPaged(ctx, func(pageOpts metav1.ListOptions) ([]runtime.Object, string, error) {
			pageOpts.LabelSelector = opts.LabelSelector
			l, err := c.OmeV1beta1().ServingRuntimes(ns).List(ctx, pageOpts)
			if err != nil {
				return nil, "", err
			}
			items := make([]runtime.Object, 0, len(l.Items))
			for i := range l.Items {
				items = append(items, &l.Items[i])
			}
			return items, l.Continue, nil
		})
	},
	GetOne: func(ctx context.Context, f factory.Factory, ns, name string) (runtime.Object, error) {
		c, err := f.OMEClient()
		if err != nil {
			return nil, err
		}
		return c.OmeV1beta1().ServingRuntimes(ns).Get(ctx, name, metav1.GetOptions{})
	},
}

var clusterServingRuntimesEntry = &entry{
	Canonical:  "clusterservingruntimes",
	Aliases:    []string{"clusterservingruntime", "csrt"},
	Namespaced: false,
	Columns:    runtimeColumns(false),
	List: func(ctx context.Context, f factory.Factory, ns string, opts metav1.ListOptions) ([]runtime.Object, error) {
		c, err := f.OMEClient()
		if err != nil {
			return nil, err
		}
		return paging.ListAllPaged(ctx, func(pageOpts metav1.ListOptions) ([]runtime.Object, string, error) {
			pageOpts.LabelSelector = opts.LabelSelector
			l, err := c.OmeV1beta1().ClusterServingRuntimes().List(ctx, pageOpts)
			if err != nil {
				return nil, "", err
			}
			items := make([]runtime.Object, 0, len(l.Items))
			for i := range l.Items {
				items = append(items, &l.Items[i])
			}
			return items, l.Continue, nil
		})
	},
	GetOne: func(ctx context.Context, f factory.Factory, ns, name string) (runtime.Object, error) {
		c, err := f.OMEClient()
		if err != nil {
			return nil, err
		}
		return c.OmeV1beta1().ClusterServingRuntimes().Get(ctx, name, metav1.GetOptions{})
	},
}

var acceleratorClassesEntry = &entry{
	Canonical:  "acceleratorclasses",
	Aliases:    []string{"acceleratorclass", "ac"},
	Namespaced: false,
	Columns: []column{
		{Name: "NAME", Extract: safeCol(func(a *v1beta1.AcceleratorClass) string { return a.Name })},
		{Name: "VENDOR", Extract: safeCol(func(a *v1beta1.AcceleratorClass) string { return printers.OrDash(a.Spec.Vendor) })},
		{Name: "FAMILY", Extract: safeCol(func(a *v1beta1.AcceleratorClass) string { return printers.OrDash(a.Spec.Family) })},
		{Name: "AGE", Extract: safeCol(func(a *v1beta1.AcceleratorClass) string { return printers.Age(a.CreationTimestamp) })},
	},
	List: func(ctx context.Context, f factory.Factory, ns string, opts metav1.ListOptions) ([]runtime.Object, error) {
		c, err := f.OMEClient()
		if err != nil {
			return nil, err
		}
		return paging.ListAllPaged(ctx, func(pageOpts metav1.ListOptions) ([]runtime.Object, string, error) {
			pageOpts.LabelSelector = opts.LabelSelector
			l, err := c.OmeV1beta1().AcceleratorClasses().List(ctx, pageOpts)
			if err != nil {
				return nil, "", err
			}
			items := make([]runtime.Object, 0, len(l.Items))
			for i := range l.Items {
				items = append(items, &l.Items[i])
			}
			return items, l.Continue, nil
		})
	},
	GetOne: func(ctx context.Context, f factory.Factory, ns, name string) (runtime.Object, error) {
		c, err := f.OMEClient()
		if err != nil {
			return nil, err
		}
		return c.OmeV1beta1().AcceleratorClasses().Get(ctx, name, metav1.GetOptions{})
	},
}

var benchmarkJobsEntry = &entry{
	Canonical:  "benchmarkjobs",
	Aliases:    []string{"benchmarkjob", "bj"},
	Namespaced: true,
	Columns: []column{
		{Name: "NAME", Extract: safeCol(func(b *v1beta1.BenchmarkJob) string { return b.Name })},
		{Name: "STATE", Extract: safeCol(func(b *v1beta1.BenchmarkJob) string { return printers.OrDash(b.Status.State) })},
		{Name: "AGE", Extract: safeCol(func(b *v1beta1.BenchmarkJob) string { return printers.Age(b.CreationTimestamp) })},
	},
	List: func(ctx context.Context, f factory.Factory, ns string, opts metav1.ListOptions) ([]runtime.Object, error) {
		c, err := f.OMEClient()
		if err != nil {
			return nil, err
		}
		return paging.ListAllPaged(ctx, func(pageOpts metav1.ListOptions) ([]runtime.Object, string, error) {
			pageOpts.LabelSelector = opts.LabelSelector
			l, err := c.OmeV1beta1().BenchmarkJobs(ns).List(ctx, pageOpts)
			if err != nil {
				return nil, "", err
			}
			items := make([]runtime.Object, 0, len(l.Items))
			for i := range l.Items {
				items = append(items, &l.Items[i])
			}
			return items, l.Continue, nil
		})
	},
	GetOne: func(ctx context.Context, f factory.Factory, ns, name string) (runtime.Object, error) {
		c, err := f.OMEClient()
		if err != nil {
			return nil, err
		}
		return c.OmeV1beta1().BenchmarkJobs(ns).Get(ctx, name, metav1.GetOptions{})
	},
}

// fineTunedWeightsEntry is cluster-scoped: FineTunedWeight carries
// +genclient:nonNamespaced and +kubebuilder:resource:scope="Cluster" (see
// pkg/apis/ome/v1beta1/model.go), so the generated FineTunedWeights() getter
// takes no namespace argument -- unlike BenchmarkJobs/InferenceReplicas.
var fineTunedWeightsEntry = &entry{
	Canonical:  "finetunedweights",
	Aliases:    []string{"finetunedweight", "ftw"},
	Namespaced: false,
	Columns: []column{
		{Name: "NAME", Extract: safeCol(func(w *v1beta1.FineTunedWeight) string { return w.Name })},
		{Name: "TYPE", Extract: safeCol(func(w *v1beta1.FineTunedWeight) string { return derefOrDash(w.Spec.ModelType) })},
		{Name: "BASEMODEL", Extract: safeCol(func(w *v1beta1.FineTunedWeight) string { return derefOrDash(w.Spec.BaseModelRef.Name) })},
		{Name: "AGE", Extract: safeCol(func(w *v1beta1.FineTunedWeight) string { return printers.Age(w.CreationTimestamp) })},
	},
	List: func(ctx context.Context, f factory.Factory, ns string, opts metav1.ListOptions) ([]runtime.Object, error) {
		c, err := f.OMEClient()
		if err != nil {
			return nil, err
		}
		return paging.ListAllPaged(ctx, func(pageOpts metav1.ListOptions) ([]runtime.Object, string, error) {
			pageOpts.LabelSelector = opts.LabelSelector
			l, err := c.OmeV1beta1().FineTunedWeights().List(ctx, pageOpts)
			if err != nil {
				return nil, "", err
			}
			items := make([]runtime.Object, 0, len(l.Items))
			for i := range l.Items {
				items = append(items, &l.Items[i])
			}
			return items, l.Continue, nil
		})
	},
	GetOne: func(ctx context.Context, f factory.Factory, ns, name string) (runtime.Object, error) {
		c, err := f.OMEClient()
		if err != nil {
			return nil, err
		}
		return c.OmeV1beta1().FineTunedWeights().Get(ctx, name, metav1.GetOptions{})
	},
}

var inferenceReplicasEntry = &entry{
	Canonical:  "inferencereplicas",
	Aliases:    []string{"inferencereplica", "ir"},
	Namespaced: true,
	Columns: []column{
		{Name: "NAME", Extract: safeCol(func(r *v1beta1.InferenceReplica) string { return r.Name })},
		{Name: "COMPONENT", Extract: safeCol(func(r *v1beta1.InferenceReplica) string { return printers.OrDash(string(r.Spec.Component)) })},
		{Name: "PARENT", Extract: safeCol(func(r *v1beta1.InferenceReplica) string { return printers.OrDash(r.Spec.ParentRef.Name) })},
		{Name: "DESIRED", Extract: safeCol(func(r *v1beta1.InferenceReplica) string { return int32OrDash(r.Spec.Replicas) })},
		{Name: "CURRENT", Extract: safeCol(func(r *v1beta1.InferenceReplica) string { return fmt.Sprintf("%d", r.Status.Replicas) })},
		{Name: "READY", Extract: safeCol(func(r *v1beta1.InferenceReplica) string { return fmt.Sprintf("%d", r.Status.ReadyReplicas) })},
		{Name: "AVAILABLE", Extract: safeCol(func(r *v1beta1.InferenceReplica) string { return fmt.Sprintf("%d", r.Status.AvailableReplicas) })},
		{Name: "LIFECYCLE", Extract: safeCol(func(r *v1beta1.InferenceReplica) string { return inferenceReplicaLifecycle(r).state })},
		{Name: "REASON", Extract: safeCol(func(r *v1beta1.InferenceReplica) string { return inferenceReplicaLifecycle(r).reason })},
		{Name: "SERVING", Wide: true, Extract: safeCol(func(r *v1beta1.InferenceReplica) string { return fmt.Sprintf("%d", r.Status.ServingReplicas) })},
		{Name: "UPDATED", Wide: true, Extract: safeCol(func(r *v1beta1.InferenceReplica) string { return fmt.Sprintf("%d", r.Status.UpdatedReplicas) })},
		{Name: "CURRENT-REVISION", Wide: true, Extract: safeCol(func(r *v1beta1.InferenceReplica) string { return printers.OrDash(r.Status.CurrentRevision) })},
		{Name: "UPDATE-REVISION", Wide: true, Extract: safeCol(func(r *v1beta1.InferenceReplica) string { return printers.OrDash(r.Status.UpdateRevision) })},
		{Name: "MIGRATIONS", Wide: true, Extract: safeCol(func(r *v1beta1.InferenceReplica) string { return fmt.Sprintf("%d", len(r.Status.Migrations)) })},
		{Name: "PAUSED", Wide: true, Extract: safeCol(func(r *v1beta1.InferenceReplica) string { return fmt.Sprintf("%t", r.Spec.Paused) })},
		{Name: "COORDINATION", Wide: true, Extract: safeCol(func(r *v1beta1.InferenceReplica) string { return printers.OrDash(r.Status.CoordinationGroupRef) })},
		{Name: "LIFECYCLE-FRESHNESS", Wide: true, Extract: safeCol(func(r *v1beta1.InferenceReplica) string { return inferenceReplicaLifecycle(r).freshness })},
		{Name: "AGE", Extract: safeCol(func(r *v1beta1.InferenceReplica) string { return printers.Age(r.CreationTimestamp) })},
	},
	List: func(ctx context.Context, f factory.Factory, ns string, opts metav1.ListOptions) ([]runtime.Object, error) {
		c, err := f.OMEClient()
		if err != nil {
			return nil, err
		}
		return paging.ListAllPaged(ctx, func(pageOpts metav1.ListOptions) ([]runtime.Object, string, error) {
			pageOpts.LabelSelector = opts.LabelSelector
			l, err := c.OmeV1beta1().InferenceReplicas(ns).List(ctx, pageOpts)
			if err != nil {
				return nil, "", err
			}
			items := make([]runtime.Object, 0, len(l.Items))
			for i := range l.Items {
				items = append(items, &l.Items[i])
			}
			return items, l.Continue, nil
		})
	},
	GetOne: func(ctx context.Context, f factory.Factory, ns, name string) (runtime.Object, error) {
		c, err := f.OMEClient()
		if err != nil {
			return nil, err
		}
		return c.OmeV1beta1().InferenceReplicas(ns).Get(ctx, name, metav1.GetOptions{})
	},
}

func int32OrDash(value *int32) string {
	if value == nil {
		return "-"
	}
	return fmt.Sprintf("%d", *value)
}

type inferenceReplicaLifecycleValue struct {
	state     string
	reason    string
	freshness string
}

func inferenceReplicaLifecycle(replica *v1beta1.InferenceReplica) inferenceReplicaLifecycleValue {
	// RolloutStalled is advisory and may coexist with Ready=True. Prefer an
	// active stall so inventory cannot make a serving-but-wedged rollout look
	// healthy. Otherwise Ready is the primary lifecycle condition.
	stalled := meta.FindStatusCondition(replica.Status.Conditions, "RolloutStalled")
	if stalled != nil && stalled.Status == metav1.ConditionTrue {
		return inferenceReplicaLifecycleFromCondition(stalled, replica.Generation)
	}
	ready := meta.FindStatusCondition(replica.Status.Conditions, "Ready")
	if ready != nil {
		return inferenceReplicaLifecycleFromCondition(ready, replica.Generation)
	}
	if stalled != nil {
		return inferenceReplicaLifecycleFromCondition(stalled, replica.Generation)
	}
	return inferenceReplicaLifecycleValue{state: "Unavailable", reason: "-", freshness: "Unavailable"}
}

func inferenceReplicaLifecycleFromCondition(condition *metav1.Condition, generation int64) inferenceReplicaLifecycleValue {
	return inferenceReplicaLifecycleValue{
		state:     fmt.Sprintf("%s=%s", condition.Type, condition.Status),
		reason:    printers.OrDash(condition.Reason),
		freshness: generationFreshness(generation, condition.ObservedGeneration),
	}
}

var workloadClustersEntry = &entry{
	Canonical:  "workloadclusters",
	Aliases:    []string{"workloadcluster", "wc"},
	Namespaced: false,
	Columns: []column{
		{Name: "NAME", Extract: safeCol(func(w *v1beta1.WorkloadCluster) string { return w.Name })},
		{Name: "CONNECTION", Extract: safeCol(func(w *v1beta1.WorkloadCluster) string { return workloadClusterConnection(w).kind })},
		{Name: "REFERENCE", Extract: safeCol(func(w *v1beta1.WorkloadCluster) string { return workloadClusterConnection(w).reference })},
		{Name: "KEY", Extract: safeCol(func(w *v1beta1.WorkloadCluster) string { return workloadClusterConnection(w).key })},
		{Name: "READY", Extract: safeCol(func(w *v1beta1.WorkloadCluster) string {
			return conditionStatus(w.Status.Conditions, v1beta1.WorkloadClusterReady)
		})},
		{Name: "GENERATION", Extract: safeCol(func(w *v1beta1.WorkloadCluster) string { return fmt.Sprintf("%d", w.Generation) })},
		{Name: "OBSERVED-GENERATION", Extract: safeCol(func(w *v1beta1.WorkloadCluster) string {
			condition := meta.FindStatusCondition(w.Status.Conditions, v1beta1.WorkloadClusterReady)
			if condition == nil || condition.ObservedGeneration == 0 {
				return "-"
			}
			return fmt.Sprintf("%d", condition.ObservedGeneration)
		})},
		{Name: "REASON", Wide: true, Extract: safeCol(func(w *v1beta1.WorkloadCluster) string {
			condition := meta.FindStatusCondition(w.Status.Conditions, v1beta1.WorkloadClusterReady)
			if condition == nil {
				return "-"
			}
			return printers.OrDash(condition.Reason)
		})},
		{Name: "AGE", Extract: safeCol(func(w *v1beta1.WorkloadCluster) string { return printers.Age(w.CreationTimestamp) })},
	},
	List: func(ctx context.Context, f factory.Factory, ns string, opts metav1.ListOptions) ([]runtime.Object, error) {
		c, err := f.OMEClient()
		if err != nil {
			return nil, err
		}
		return paging.ListAllPaged(ctx, func(pageOpts metav1.ListOptions) ([]runtime.Object, string, error) {
			pageOpts.LabelSelector = opts.LabelSelector
			l, err := c.OmeV1beta1().WorkloadClusters().List(ctx, pageOpts)
			if err != nil {
				return nil, "", err
			}
			items := make([]runtime.Object, 0, len(l.Items))
			for i := range l.Items {
				items = append(items, &l.Items[i])
			}
			return items, l.Continue, nil
		})
	},
	GetOne: func(ctx context.Context, f factory.Factory, ns, name string) (runtime.Object, error) {
		c, err := f.OMEClient()
		if err != nil {
			return nil, err
		}
		return c.OmeV1beta1().WorkloadClusters().Get(ctx, name, metav1.GetOptions{})
	},
}

type workloadClusterConnectionValue struct {
	kind      string
	reference string
	key       string
}

func workloadClusterConnection(cluster *v1beta1.WorkloadCluster) workloadClusterConnectionValue {
	source := cluster.Spec.ClusterSource
	switch {
	case source.KubeConfig != nil && source.ClusterProfileRef == nil:
		key := source.KubeConfig.Key
		if key == "" {
			key = "kubeconfig"
		}
		name := source.KubeConfig.SecretRef.Name
		if name == "" {
			return workloadClusterConnectionValue{kind: "KubeConfig", reference: "-", key: key}
		}
		if source.KubeConfig.SecretRef.Namespace != "" {
			name = source.KubeConfig.SecretRef.Namespace + "/" + name
		}
		return workloadClusterConnectionValue{kind: "KubeConfig", reference: printers.OrDash(name), key: key}
	case source.ClusterProfileRef != nil && source.KubeConfig == nil:
		return workloadClusterConnectionValue{kind: "ClusterProfile", reference: printers.OrDash(source.ClusterProfileRef.Name), key: "-"}
	default:
		return workloadClusterConnectionValue{kind: "Invalid", reference: "-", key: "-"}
	}
}

type acceleratorQuotaBudgetRow struct {
	resource        string
	flavor          string
	nominal         string
	admitted        string
	source          string
	statusFreshness string
	borrowed        string
	reserved        string
}

func acceleratorQuotaBudgetRows(quota *v1beta1.AcceleratorQuota) []acceleratorQuotaBudgetRow {
	statusFreshness := acceleratorQuotaStatusFreshness(quota)
	rows := make([]acceleratorQuotaBudgetRow, 0, len(quota.Status.Budgets)+len(quota.Spec.Budgets))
	if len(quota.Status.Budgets) > 0 {
		for _, budget := range quota.Status.Budgets {
			rows = append(rows, acceleratorQuotaBudgetRow{
				resource:        budget.ResourceName,
				flavor:          budget.ResourceFlavor,
				nominal:         budget.Nominal.String(),
				admitted:        budget.Admitted.String(),
				source:          "Reported",
				statusFreshness: statusFreshness,
				borrowed:        budget.Borrowed.String(),
				reserved:        budget.Reserved.String(),
			})
		}
	}
	// Current status is authoritative for the flattened budget view. When it
	// has not observed metadata.generation, retain the current declaration as
	// separate evidence instead of hiding it behind stale reported rows.
	if len(quota.Spec.Budgets) > 0 && (len(quota.Status.Budgets) == 0 || statusFreshness != "Current") {
		for _, budget := range quota.Spec.Budgets {
			rows = append(rows, acceleratorQuotaBudgetRow{
				resource:        budget.ResourceName,
				flavor:          budget.ResourceFlavor,
				nominal:         budget.Nominal.String(),
				admitted:        "-",
				source:          "Declared",
				statusFreshness: statusFreshness,
				borrowed:        "-",
				reserved:        "-",
			})
		}
	}
	if len(rows) == 0 {
		rows = append(rows, acceleratorQuotaBudgetRow{
			resource: "-", flavor: "-", nominal: "-", admitted: "-", source: "Unavailable",
			statusFreshness: statusFreshness, borrowed: "-", reserved: "-",
		})
	}
	return sortAcceleratorQuotaBudgetRows(rows)
}

func sortAcceleratorQuotaBudgetRows(rows []acceleratorQuotaBudgetRow) []acceleratorQuotaBudgetRow {
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].resource != rows[j].resource {
			return rows[i].resource < rows[j].resource
		}
		if rows[i].flavor != rows[j].flavor {
			return rows[i].flavor < rows[j].flavor
		}
		if rows[i].source != rows[j].source {
			return rows[i].source == "Declared"
		}
		return false
	})
	return rows
}

func acceleratorQuotaStatusFreshness(quota *v1beta1.AcceleratorQuota) string {
	if quota.Status.ObservedGeneration == 0 {
		// Zero generations occur only in synthetic objects. Preserve the useful
		// convention that populated synthetic status is current in unit tests.
		if quota.Generation == 0 && (len(quota.Status.Budgets) > 0 || len(quota.Status.Conditions) > 0) {
			return "Current"
		}
		return "Unobserved"
	}
	return generationFreshness(quota.Generation, quota.Status.ObservedGeneration)
}

func generationFreshness(generation, observedGeneration int64) string {
	if generation == observedGeneration {
		return "Current"
	}
	if observedGeneration == 0 {
		return "Unobserved"
	}
	return "Stale"
}

func acceleratorQuotaParent(quota *v1beta1.AcceleratorQuota) string {
	if quota.Spec.ParentRef == nil {
		return "-"
	}
	return printers.OrDash(quota.Spec.ParentRef.Name)
}

func conditionStatus(conditions []metav1.Condition, conditionType string) string {
	condition := meta.FindStatusCondition(conditions, conditionType)
	if condition == nil {
		return "Unknown"
	}
	return string(condition.Status)
}

func firstAcceleratorQuotaBudget(quota *v1beta1.AcceleratorQuota) acceleratorQuotaBudgetRow {
	return acceleratorQuotaBudgetRows(quota)[0]
}

var acceleratorQuotasEntry = &entry{
	Canonical:  "acceleratorquotas",
	Aliases:    []string{"acceleratorquota", "aq"},
	Namespaced: false,
	Columns: []column{
		{Name: "NAME", Extract: safeCol(func(q *v1beta1.AcceleratorQuota) string { return q.Name })},
		{Name: "ROLE", Extract: safeCol(func(q *v1beta1.AcceleratorQuota) string { return printers.OrDash(string(q.Spec.Role)) })},
		{Name: "PARENT", Extract: safeCol(acceleratorQuotaParent)},
		{Name: "RESOURCE", Extract: safeCol(func(q *v1beta1.AcceleratorQuota) string {
			return printers.OrDash(firstAcceleratorQuotaBudget(q).resource)
		})},
		{Name: "FLAVOR", Extract: safeCol(func(q *v1beta1.AcceleratorQuota) string {
			return printers.OrDash(firstAcceleratorQuotaBudget(q).flavor)
		})},
		{Name: "NOMINAL", Extract: safeCol(func(q *v1beta1.AcceleratorQuota) string {
			return printers.OrDash(firstAcceleratorQuotaBudget(q).nominal)
		})},
		{Name: "ADMITTED", Extract: safeCol(func(q *v1beta1.AcceleratorQuota) string {
			return printers.OrDash(firstAcceleratorQuotaBudget(q).admitted)
		})},
		{Name: "SOURCE", Extract: safeCol(func(q *v1beta1.AcceleratorQuota) string {
			return firstAcceleratorQuotaBudget(q).source
		})},
		{Name: "STATUS-FRESHNESS", Extract: safeCol(func(q *v1beta1.AcceleratorQuota) string {
			return firstAcceleratorQuotaBudget(q).statusFreshness
		})},
		{Name: "READY", Extract: safeCol(func(q *v1beta1.AcceleratorQuota) string {
			return conditionStatus(q.Status.Conditions, v1beta1.AcceleratorQuotaReady)
		})},
		{Name: "DEGRADED", Extract: safeCol(func(q *v1beta1.AcceleratorQuota) string {
			return conditionStatus(q.Status.Conditions, v1beta1.AcceleratorQuotaDegraded)
		})},
		{Name: "AGE", Extract: safeCol(func(q *v1beta1.AcceleratorQuota) string { return printers.Age(q.CreationTimestamp) })},
		{Name: "BORROWED", Wide: true, Extract: safeCol(func(q *v1beta1.AcceleratorQuota) string {
			return printers.OrDash(firstAcceleratorQuotaBudget(q).borrowed)
		})},
		{Name: "RESERVED", Wide: true, Extract: safeCol(func(q *v1beta1.AcceleratorQuota) string {
			return printers.OrDash(firstAcceleratorQuotaBudget(q).reserved)
		})},
	},
	TableRows: acceleratorQuotaTableRows,
	List: func(ctx context.Context, f factory.Factory, ns string, opts metav1.ListOptions) ([]runtime.Object, error) {
		client, err := f.OMEClient()
		if err != nil {
			return nil, err
		}
		return paging.ListAllPaged(ctx, func(pageOpts metav1.ListOptions) ([]runtime.Object, string, error) {
			pageOpts.LabelSelector = opts.LabelSelector
			list, err := client.OmeV1beta1().AcceleratorQuotas().List(ctx, pageOpts)
			if err != nil {
				return nil, "", err
			}
			items := make([]runtime.Object, 0, len(list.Items))
			for index := range list.Items {
				items = append(items, &list.Items[index])
			}
			return items, list.Continue, nil
		})
	},
	GetOne: func(ctx context.Context, f factory.Factory, ns, name string) (runtime.Object, error) {
		client, err := f.OMEClient()
		if err != nil {
			return nil, err
		}
		return client.OmeV1beta1().AcceleratorQuotas().Get(ctx, name, metav1.GetOptions{})
	},
}

var rolloutPoliciesEntry = &entry{
	Canonical:  "rolloutpolicies",
	Aliases:    []string{"rolloutpolicy", "rp"},
	Namespaced: true,
	Columns: []column{
		{Name: "NAME", Extract: safeCol(func(p *v1beta1.RolloutPolicy) string { return p.Name })},
		{Name: "PROGRESSION", Extract: safeCol(func(p *v1beta1.RolloutPolicy) string {
			progression, ok := p.Spec.Progression()
			if !ok {
				return "-"
			}
			return string(progression)
		})},
		{Name: "READY", Extract: safeCol(func(p *v1beta1.RolloutPolicy) string {
			return rolloutPolicyConditionStatus(p, v1beta1.RolloutPolicyReadyCondition)
		})},
		{Name: "DIGEST", Extract: safeCol(func(p *v1beta1.RolloutPolicy) string {
			if !rolloutPolicyStatusCurrent(p) {
				return "-"
			}
			return printers.OrDash(p.Status.PortableDigest)
		})},
		{Name: "REFS", Extract: safeCol(func(p *v1beta1.RolloutPolicy) string {
			if !rolloutPolicyStatusCurrent(p) {
				return "-"
			}
			return fmt.Sprintf("%d", p.Status.AttachedGroups)
		})},
		{Name: "REASON", Wide: true, Extract: safeCol(func(p *v1beta1.RolloutPolicy) string {
			if !rolloutPolicyStatusCurrent(p) {
				return "-"
			}
			condition := meta.FindStatusCondition(p.Status.Conditions, v1beta1.RolloutPolicyReadyCondition)
			if condition == nil {
				return "-"
			}
			return printers.OrDash(condition.Reason)
		})},
		{Name: "IN-USE", Wide: true, Extract: safeCol(func(p *v1beta1.RolloutPolicy) string {
			return rolloutPolicyConditionStatus(p, v1beta1.RolloutPolicyInUseCondition)
		})},
		{Name: "STATUS-FRESHNESS", Wide: true, Extract: safeCol(rolloutPolicyStatusFreshness)},
		{Name: "AGE", Extract: safeCol(func(p *v1beta1.RolloutPolicy) string { return printers.Age(p.CreationTimestamp) })},
	},
	List: func(ctx context.Context, f factory.Factory, ns string, opts metav1.ListOptions) ([]runtime.Object, error) {
		client, err := f.RuntimeClient()
		if err != nil {
			return nil, err
		}
		selector, err := labels.Parse(opts.LabelSelector)
		if err != nil {
			return nil, err
		}
		return paging.ListAllPaged(ctx, func(pageOpts metav1.ListOptions) ([]runtime.Object, string, error) {
			pageOpts.LabelSelector = opts.LabelSelector
			list := &v1beta1.RolloutPolicyList{}
			if err := client.List(ctx, list, &ctrlclient.ListOptions{
				Namespace:     ns,
				LabelSelector: selector,
				Limit:         pageOpts.Limit,
				Continue:      pageOpts.Continue,
				Raw:           &pageOpts,
			}); err != nil {
				return nil, "", err
			}
			items := make([]runtime.Object, 0, len(list.Items))
			for i := range list.Items {
				items = append(items, &list.Items[i])
			}
			return items, list.Continue, nil
		})
	},
	GetOne: func(ctx context.Context, f factory.Factory, ns, name string) (runtime.Object, error) {
		client, err := f.RuntimeClient()
		if err != nil {
			return nil, err
		}
		policy := &v1beta1.RolloutPolicy{}
		if err := client.Get(ctx, ctrlclient.ObjectKey{Namespace: ns, Name: name}, policy); err != nil {
			return nil, err
		}
		return policy, nil
	},
}

func rolloutPolicyStatusFreshness(policy *v1beta1.RolloutPolicy) string {
	if policy.Status.ObservedGeneration == 0 {
		return "Unobserved"
	}
	return generationFreshness(policy.Generation, policy.Status.ObservedGeneration)
}

func rolloutPolicyStatusCurrent(policy *v1beta1.RolloutPolicy) bool {
	return rolloutPolicyStatusFreshness(policy) == "Current"
}

func rolloutPolicyConditionStatus(policy *v1beta1.RolloutPolicy, conditionType string) string {
	if !rolloutPolicyStatusCurrent(policy) {
		return "Unknown"
	}
	return conditionStatus(policy.Status.Conditions, conditionType)
}

const (
	acceleratorQuotaColumns     = 12
	acceleratorQuotaWideColumns = 14
)

func acceleratorQuotaTableRows(obj runtime.Object, wide bool) [][]string {
	quota, ok := obj.(*v1beta1.AcceleratorQuota)
	if !ok {
		width := acceleratorQuotaColumns
		if wide {
			width = acceleratorQuotaWideColumns
		}
		row := make([]string, width)
		for index := range row {
			row[index] = "?"
		}
		return [][]string{row}
	}
	rows := make([][]string, 0, len(acceleratorQuotaBudgetRows(quota)))
	for _, budget := range acceleratorQuotaBudgetRows(quota) {
		row := []string{
			quota.Name,
			printers.OrDash(string(quota.Spec.Role)),
			acceleratorQuotaParent(quota),
			printers.OrDash(budget.resource),
			printers.OrDash(budget.flavor),
			printers.OrDash(budget.nominal),
			printers.OrDash(budget.admitted),
			budget.source,
			budget.statusFreshness,
			conditionStatus(quota.Status.Conditions, v1beta1.AcceleratorQuotaReady),
			conditionStatus(quota.Status.Conditions, v1beta1.AcceleratorQuotaDegraded),
			printers.Age(quota.CreationTimestamp),
		}
		if wide {
			row = append(row, printers.OrDash(budget.borrowed), printers.OrDash(budget.reserved))
		}
		rows = append(rows, row)
	}
	return rows
}

// registry lists every resource `kubectl ome get` knows how to render. Adding
// a resource means adding an entry here, never a new command.
var registry = []*entry{
	isvcEntry,
	modelsEntry,
	baseModelsEntry, clusterBaseModelsEntry,
	runtimesEntry, servingRuntimesEntry, clusterServingRuntimesEntry,
	acceleratorClassesEntry, acceleratorQuotasEntry, benchmarkJobsEntry,
	fineTunedWeightsEntry, inferenceReplicasEntry, workloadClustersEntry, rolloutPoliciesEntry,
	autoscalerPoliciesEntry,
}
