package fakeclient

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// MemoryReader answers Get and List from objects held in memory the way the
// manager's informer cache answers a controller: a Get is one deep copy of
// the held object, a List keeps the held objects of the list's kind that
// match the namespace and the label selector and deep copies each match,
// and a read that asks for the cache's own objects
// (client.UnsafeDisableDeepCopy) is answered without a copy. A field
// selector is refused, as the cache refuses a field it has no index for.
// Nothing is serialized, so a read costs what it costs the manager. Lists
// and Copies count what the reader served.
type MemoryReader struct {
	held []client.Object
	// Lists is the number of List calls answered.
	Lists int
	// Copies is the number of held objects handed out as deep copies.
	Copies int
}

// ReadFromMemory holds objs for a MemoryReader. The reader hands the held
// objects themselves to a read that asks for them uncopied, so a test that
// must prove them unchanged keeps its own copy.
func ReadFromMemory(objs ...client.Object) *MemoryReader {
	return &MemoryReader{held: append([]client.Object(nil), objs...)}
}

// Get answers the held object of obj's type under key: a deep copy, or the
// object itself when the read asks for it uncopied.
func (r *MemoryReader) Get(_ context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	options := client.GetOptions{}
	options.ApplyOptions(opts)
	for _, held := range r.held {
		if reflect.TypeOf(held) != reflect.TypeOf(obj) || client.ObjectKeyFromObject(held) != key {
			continue
		}
		if uncopied(options.UnsafeDisableDeepCopy) {
			reflect.ValueOf(obj).Elem().Set(reflect.ValueOf(held).Elem())
			return nil
		}
		copyInto(obj, held)
		r.Copies++
		return nil
	}
	return apierrors.NewNotFound(guessedResource(obj), key.Name)
}

// List answers the held objects of the list's kind that match the namespace
// and the label selector, in the order held: deep copies, or the objects
// themselves when the read asks for them uncopied.
func (r *MemoryReader) List(_ context.Context, list client.ObjectList, opts ...client.ListOption) error {
	options := client.ListOptions{}
	options.ApplyOptions(opts)
	if options.FieldSelector != nil && !options.FieldSelector.Empty() {
		return fmt.Errorf("fakeclient: MemoryReader has no index for the field selector %q", options.FieldSelector)
	}
	itemType, err := listItemType(list)
	if err != nil {
		return err
	}
	r.Lists++
	var items []runtime.Object
	for _, held := range r.held {
		if reflect.TypeOf(held).Elem() != itemType || !matches(held, &options) {
			continue
		}
		if uncopied(options.UnsafeDisableDeepCopy) {
			items = append(items, held)
			continue
		}
		items = append(items, held.DeepCopyObject())
		r.Copies++
	}
	return meta.SetList(list, items)
}

// uncopied reads the deep-copy switch of a read's options.
func uncopied(disableDeepCopy *bool) bool {
	return disableDeepCopy != nil && *disableDeepCopy
}

// matches is the namespace and label match of the informer cache's List.
func matches(obj client.Object, options *client.ListOptions) bool {
	if options.Namespace != "" && obj.GetNamespace() != options.Namespace {
		return false
	}
	return options.LabelSelector == nil || options.LabelSelector.Matches(labels.Set(obj.GetLabels()))
}

// listItemType is the type of one item of list.
func listItemType(list client.ObjectList) (reflect.Type, error) {
	itemsPtr, err := meta.GetItemsPtr(list)
	if err != nil {
		return nil, fmt.Errorf("fakeclient: MemoryReader: %w", err)
	}
	return reflect.TypeOf(itemsPtr).Elem().Elem(), nil
}

// guessedResource names obj's resource for a NotFound error from the type
// alone, since the reader has no scheme.
func guessedResource(obj client.Object) schema.GroupResource {
	return schema.GroupResource{Resource: strings.ToLower(reflect.TypeOf(obj).Elem().Name()) + "s"}
}
