package crdskew

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/go-logr/logr"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// +kubebuilder:rbac:groups=apiextensions.k8s.io,resources=customresourcedefinitions,verbs=get;list;watch

// CRDReader reads CustomResourceDefinitions; the apiextensions typed client
// satisfies it.
type CRDReader interface {
	Get(ctx context.Context, name string, opts metav1.GetOptions) (*apiextensionsv1.CustomResourceDefinition, error)
	List(ctx context.Context, opts metav1.ListOptions) (*apiextensionsv1.CustomResourceDefinitionList, error)
}

// Watcher tells Changed of the Report's picture once the manager runs and
// keeps the picture current as the CRDs of the manager's API group change. It runs in every
// replica, leader or not, so a replica that takes the lead already holds a
// current picture. On a cluster that does not let the manager read CRDs the
// picture stays as start-up read it; the Watcher says so once.
type Watcher struct {
	Shipped Shipped
	Report  *Report
	CRDs    CRDReader
	// Informers is the manager's cache, asked for a metadata-only informer
	// on CustomResourceDefinitions.
	Informers cache.Informers
	Log       logr.Logger
	// Changed is told of every lagging CRD when the Watcher starts and of
	// every CRD whose picture changes afterwards, once the Report holds it.
	Changed func(Finding)
}

// NeedLeaderElection is false: every replica keeps its own picture.
func (w *Watcher) NeedLeaderElection() bool { return false }

// Start tells Changed of the picture, then follows CRD changes until ctx ends.
func (w *Watcher) Start(ctx context.Context) error {
	for _, finding := range w.Report.Lagging() {
		w.tell(finding)
	}
	if _, err := w.CRDs.List(ctx, metav1.ListOptions{Limit: 1}); err != nil {
		w.Log.Error(err, "CRD changes are not followed; the picture of the installed CRDs stays as start-up read it")
		return nil
	}
	crdMeta := &metav1.PartialObjectMetadata{}
	crdMeta.SetGroupVersionKind(apiextensionsv1.SchemeGroupVersion.WithKind(reflect.TypeOf(apiextensionsv1.CustomResourceDefinition{}).Name()))
	informer, err := w.Informers.GetInformer(ctx, crdMeta, cache.BlockUntilSynced(false))
	if err != nil {
		w.Log.Error(err, "CRD changes are not followed; the picture of the installed CRDs stays as start-up read it")
		return nil
	}
	changed := newNameSet()
	note := func(obj any) {
		if name := crdName(obj); name != "" {
			changed.add(name)
		}
	}
	if _, err := informer.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
		AddFunc:    note,
		UpdateFunc: func(_, obj any) { note(obj) },
		DeleteFunc: note,
	}); err != nil {
		w.Log.Error(err, "CRD changes are not followed; the picture of the installed CRDs stays as start-up read it")
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-changed.signal:
			for _, name := range changed.drain() {
				w.read(ctx, name)
			}
		}
	}
}

// read compares one CRD again and tells Changed of a changed picture.
func (w *Watcher) read(ctx context.Context, name string) {
	crd, err := w.CRDs.Get(ctx, name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		w.Report.Forget(name)
		return
	case err != nil:
		w.Log.Error(err, "Could not read a changed CRD; its picture stays as last read", "crd", name)
		return
	}
	finding, ok, err := w.Shipped.CompareCRD(crd)
	if err != nil {
		w.Log.Error(err, "Could not compare a changed CRD; its picture stays as last read", "crd", name)
		return
	}
	if !ok {
		w.Report.Forget(name)
		return
	}
	if w.Report.Set(finding) {
		w.tell(finding)
	}
}

func (w *Watcher) tell(finding Finding) {
	if w.Changed != nil {
		w.Changed(finding)
	}
}

// crdName returns the name of a CRD of the manager's API group seen by the
// informer, or "" for any other object.
func crdName(obj any) string {
	if deleted, ok := obj.(toolscache.DeletedFinalStateUnknown); ok {
		obj = deleted.Obj
	}
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return ""
	}
	name := accessor.GetName()
	if !strings.HasSuffix(name, "."+v1beta1.SchemeGroupVersion.Group) {
		return ""
	}
	return name
}

// nameSet collects the names of changed CRDs between reads; one signal
// covers any number of names.
type nameSet struct {
	mu     sync.Mutex
	names  map[string]struct{}
	signal chan struct{}
}

func newNameSet() *nameSet {
	return &nameSet{names: map[string]struct{}{}, signal: make(chan struct{}, 1)}
}

func (s *nameSet) add(name string) {
	s.mu.Lock()
	s.names[name] = struct{}{}
	s.mu.Unlock()
	select {
	case s.signal <- struct{}{}:
	default:
	}
}

func (s *nameSet) drain() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := make([]string, 0, len(s.names))
	for name := range s.names {
		names = append(names, name)
	}
	s.names = map[string]struct{}{}
	sort.Strings(names)
	return names
}
