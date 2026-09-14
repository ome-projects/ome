package get

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"sigs.k8s.io/ome/pkg/cli/apierror"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/printers"
)

type Options struct {
	genericiooptions.IOStreams
	Resource      string
	Name          string
	Output        string // "", "wide", "json", "yaml"
	AllNamespaces bool
	Selector      string

	entry     *entry
	namespace string
}

func NewCmd(f factory.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	o := &Options{IOStreams: streams}
	cmd := &cobra.Command{
		Use:   "get RESOURCE [NAME]",
		Short: "List OME resources with model-centric columns",
		Long: `List OME resources. RESOURCE is one of the OME CRDs (e.g. isvc,
basemodels, servingruntimes) or a merged view: "models" (BaseModels +
ClusterBaseModels) or "runtimes" (ServingRuntimes + ClusterServingRuntimes).`,
		Args: cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.Complete(f, args); err != nil {
				return err
			}
			if err := o.Validate(); err != nil {
				return err
			}
			return o.Run(cmd.Context(), f)
		},
	}
	cmd.Flags().StringVarP(&o.Output, "output", "o", "", "Output format: wide, json or yaml")
	cmd.Flags().BoolVarP(&o.AllNamespaces, "all-namespaces", "A", false, "List across all namespaces")
	cmd.Flags().StringVarP(&o.Selector, "selector", "l", "", "Label selector to filter on")
	return cmd
}

func (o *Options) Complete(f factory.Factory, args []string) error {
	o.Resource = args[0]
	if len(args) == 2 {
		o.Name = args[1]
	}
	e, err := resolve(o.Resource)
	if err != nil {
		return err
	}
	o.entry = e
	ns, _, err := f.Namespace()
	if err != nil {
		return err
	}
	o.namespace = ns
	if o.AllNamespaces {
		if e.Namespaced {
			o.namespace = metav1.NamespaceAll
		} else {
			fmt.Fprintf(o.ErrOut, "warning: --all-namespaces is ignored for the cluster-scoped resource %q\n", e.Canonical)
		}
	}
	return nil
}

func (o *Options) Validate() error {
	switch o.Output {
	case "", "wide", "json", "yaml":
	default:
		return fmt.Errorf("unsupported output format %q (supported: wide, json, yaml)", o.Output)
	}
	if o.Name != "" && o.AllNamespaces {
		return fmt.Errorf("a resource name cannot be combined with --all-namespaces")
	}
	if o.Name != "" && o.Selector != "" {
		return fmt.Errorf("a resource name cannot be combined with --selector")
	}
	return nil
}

func (o *Options) Run(ctx context.Context, f factory.Factory) error {
	var objs []runtime.Object
	if o.Name != "" {
		obj, err := o.entry.GetOne(ctx, f, o.namespace, o.Name)
		if err != nil {
			return apierror.Friendly(err)
		}
		objs = []runtime.Object{obj}
	} else {
		var err error
		objs, err = o.entry.List(ctx, f, o.namespace, metav1.ListOptions{LabelSelector: o.Selector})
		if err != nil {
			return apierror.Friendly(err)
		}
	}
	if o.Output == "json" || o.Output == "yaml" {
		if o.Name != "" {
			return printers.PrintObj(objs[0], o.Output, o.Out)
		}
		list := &corev1.List{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "List"}, Items: []runtime.RawExtension{}}
		for _, obj := range objs {
			list.Items = append(list.Items, runtime.RawExtension{Object: obj})
		}
		return printers.PrintObj(list, o.Output, o.Out)
	}
	if len(objs) == 0 {
		if !o.entry.Namespaced || o.AllNamespaces {
			fmt.Fprintf(o.ErrOut, "No %s found.\n", o.entry.Canonical)
		} else {
			fmt.Fprintf(o.ErrOut, "No %s found in namespace %q.\n", o.entry.Canonical, o.namespace)
		}
		return nil
	}
	table := printers.Table{}
	includeNamespace := o.AllNamespaces && o.entry.Namespaced
	if includeNamespace {
		table.Headers = append(table.Headers, "NAMESPACE")
	}
	for _, c := range o.entry.Columns {
		if c.Wide && o.Output != "wide" {
			continue
		}
		table.Headers = append(table.Headers, c.Name)
	}
	for _, obj := range objs {
		if o.entry.TableRows != nil {
			rows := o.entry.TableRows(obj, o.Output == "wide")
			if includeNamespace {
				for i := range rows {
					rows[i] = append([]string{objectNamespace(obj)}, rows[i]...)
				}
			}
			table.Rows = append(table.Rows, rows...)
			continue
		}
		var row []string
		if includeNamespace {
			row = append(row, objectNamespace(obj))
		}
		for _, c := range o.entry.Columns {
			if c.Wide && o.Output != "wide" {
				continue
			}
			row = append(row, c.Extract(obj))
		}
		table.Rows = append(table.Rows, row)
	}
	return table.Write(o.Out)
}

func objectNamespace(obj runtime.Object) string {
	object, ok := obj.(metav1.Object)
	if !ok {
		return "?"
	}
	return printers.OrDash(object.GetNamespace())
}
