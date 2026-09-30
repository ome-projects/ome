package main

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// A checker validates objects against an API server.
type checker struct {
	client client.Client
	// served holds the names of the API groups the server serves. The
	// core group's name is "".
	served map[string]bool
}

func newChecker(cfg *rest.Config) (*checker, error) {
	c, err := client.New(cfg, client.Options{})
	if err != nil {
		return nil, fmt.Errorf("create client: %w", err)
	}
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("create discovery client: %w", err)
	}
	groups, err := dc.ServerGroups()
	if err != nil {
		return nil, fmt.Errorf("list API groups: %w", err)
	}
	served := make(map[string]bool)
	for _, g := range groups.Groups {
		served[g.Name] = true
	}
	return &checker{client: c, served: served}, nil
}

// A failure is an example that the YAML parser or API server rejected.
type failure struct {
	path    string
	line    int
	object  string // describe's result, or empty for invalid YAML
	message string
}

func (f failure) String() string {
	if f.object == "" {
		return fmt.Sprintf("%s:%d: %s", f.path, f.line, f.message)
	}
	return fmt.Sprintf("%s:%d: %s: %s", f.path, f.line, f.object, f.message)
}

// A report is the outcome of a check.
type report struct {
	checked  int // objects sent to the API server
	failures []failure
	// skipped counts the objects whose API group the server doesn't
	// serve, by "group/version Kind".
	skipped map[string]int
}

// fail adds a failure for doc. object is empty when doc isn't an object.
func (r *report) fail(doc document, object, message string) {
	r.failures = append(r.failures, failure{path: doc.path, line: doc.line, object: object, message: message})
}

// An example is an object from the documentation.
type example struct {
	doc document
	obj *unstructured.Unstructured
	gvk schema.GroupVersionKind
}

// parse returns the objects in docs, and a report with a failure for each
// document that isn't valid YAML, has an apiVersion that isn't a group and
// version, or is an object with neither metadata.name nor
// metadata.generateName. It doesn't need the API server.
func parse(docs []document) ([]example, *report) {
	rep := &report{skipped: make(map[string]int)}
	var examples []example
	for _, doc := range docs {
		obj, err := parseObject(doc.text)
		if err != nil {
			rep.fail(doc, "", "invalid YAML: "+yamlMessage(doc, err))
			continue
		}
		if obj == nil {
			continue
		}
		// obj.GroupVersionKind is empty when the apiVersion doesn't parse,
		// and the lookup would then report a kind with no name.
		gv, err := schema.ParseGroupVersion(obj.GetAPIVersion())
		if err != nil {
			rep.fail(doc, describe(obj), fmt.Sprintf("invalid apiVersion %q: %v", obj.GetAPIVersion(), err))
			continue
		}
		// Config files such as a kubeconfig have an apiVersion and kind but
		// no name. The API server needs one.
		if obj.GetName() == "" && obj.GetGenerateName() == "" {
			rep.fail(doc, describe(obj), "no metadata.name or metadata.generateName: add metadata.name, or mark the block check=skip")
			continue
		}
		examples = append(examples, example{doc: doc, obj: obj, gvk: gv.WithKind(obj.GetKind())})
	}
	return examples, rep
}

// check validates examples against the API server and adds the outcome to
// rep. It creates the namespaces the examples use, then dry-run creates
// each object with strict field validation. It returns an error only when
// the API server fails, not when an example does.
func (c *checker) check(ctx context.Context, examples []example, rep *report) error {
	var known []example
	namespaces := make(map[string]bool)
	for _, ex := range examples {
		gvk := ex.gvk
		mapping, err := c.client.RESTMapper().RESTMapping(gvk.GroupKind(), gvk.Version)
		switch {
		case meta.IsNoMatchError(err) && !c.served[gvk.Group]:
			rep.skipped[gvk.GroupVersion().String()+" "+gvk.Kind]++
			continue
		case meta.IsNoMatchError(err):
			rep.fail(ex.doc, describe(ex.obj), fmt.Sprintf("the API server has no kind %s in %s", gvk.Kind, gvk.GroupVersion()))
			continue
		case err != nil:
			return fmt.Errorf("map %s: %w", gvk, err)
		}
		if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
			if ex.obj.GetNamespace() == "" {
				ex.obj.SetNamespace(metav1.NamespaceDefault)
			}
			namespaces[ex.obj.GetNamespace()] = true
		}
		known = append(known, ex)
	}

	// Creating a namespace can fail for an example's reason, such as an
	// invalid name, so the error goes to the examples that use it.
	namespaceErrors := make(map[string]error)
	for _, name := range slices.Sorted(maps.Keys(namespaces)) {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
		if err := c.client.Create(ctx, ns); err != nil && !apierrors.IsAlreadyExists(err) {
			namespaceErrors[name] = fmt.Errorf("create namespace %s: %w", name, err)
		}
	}

	for _, ex := range known {
		rep.checked++
		err := namespaceErrors[ex.obj.GetNamespace()]
		if err == nil {
			err = c.client.Create(ctx, ex.obj, client.DryRunAll, client.FieldValidation("Strict"))
		}
		// A dry run stores nothing, so an object that already exists is a
		// namespace created above or by the server. It passed validation,
		// which runs first.
		if err != nil && !apierrors.IsAlreadyExists(err) {
			rep.fail(ex.doc, describe(ex.obj), err.Error())
		}
	}

	slices.SortStableFunc(rep.failures, func(a, b failure) int {
		return cmp.Or(strings.Compare(a.path, b.path), cmp.Compare(a.line, b.line))
	})
	return nil
}

// describe returns "Kind name" for obj, with its generateName when it has
// no name, or just "Kind" when it has neither.
func describe(obj *unstructured.Unstructured) string {
	name := obj.GetName()
	if name == "" {
		name = obj.GetGenerateName()
	}
	return strings.TrimSpace(obj.GetKind() + " " + name)
}

// print writes the failures, then a summary, to w.
func (r *report) print(w io.Writer) {
	for _, f := range r.failures {
		fmt.Fprintln(w, f)
	}
	fmt.Fprintf(w, "Checked %s: %s.\n", count(r.checked, "object"), count(len(r.failures), "problem"))
	if len(r.skipped) > 0 {
		fmt.Fprintln(w, "Skipped objects of kinds whose CRDs aren't installed:")
		for _, kind := range slices.Sorted(maps.Keys(r.skipped)) {
			fmt.Fprintf(w, "  %s: %d\n", kind, r.skipped[kind])
		}
	}
}

// count returns n and noun, pluralized.
func count(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return fmt.Sprintf("%d %ss", n, noun)
}
