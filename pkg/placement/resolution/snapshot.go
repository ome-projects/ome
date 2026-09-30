package resolution

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

type snapshot struct {
	identity string
	read     func(context.Context) (string, error)
}

// snapshotClient preserves lookup absences as well as identities: adding a
// namespaced runtime can change selection without editing any existing object.
type snapshotClient struct {
	client.Client
	ctx       context.Context
	snapshots map[string]snapshot
	readError error
}

func (c *snapshotClient) Get(_ context.Context, key client.ObjectKey, object client.Object, opts ...client.GetOption) (err error) {
	defer func() { c.rememberError(err) }()
	if err := c.ctx.Err(); err != nil {
		return err
	}
	err = c.Client.Get(c.ctx, key, object, opts...)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	identity := ""
	if err == nil {
		var identityErr error
		identity, identityErr = objectIdentity(object)
		if identityErr != nil {
			return identityErr
		}
	}
	template := object.DeepCopyObject().(client.Object)
	read := func(ctx context.Context) (string, error) {
		fresh := template.DeepCopyObject().(client.Object)
		if err := c.Client.Get(ctx, key, fresh, opts...); err != nil {
			if apierrors.IsNotFound(err) {
				return "", nil
			}
			return "", err
		}
		return objectIdentity(fresh)
	}
	if recordErr := c.record(fmt.Sprintf("%T/%s", object, key), snapshot{identity, read}); recordErr != nil {
		return recordErr
	}
	return err
}

func (c *snapshotClient) List(_ context.Context, list client.ObjectList, opts ...client.ListOption) (err error) {
	defer func() { c.rememberError(err) }()
	if err := c.ctx.Err(); err != nil {
		return err
	}
	if err := c.Client.List(c.ctx, list, opts...); err != nil {
		return err
	}
	identity, err := listIdentity(list)
	if err != nil {
		return err
	}
	template := list.DeepCopyObject().(client.ObjectList)
	read := func(ctx context.Context) (string, error) {
		fresh := template.DeepCopyObject().(client.ObjectList)
		if err := c.Client.List(ctx, fresh, opts...); err != nil {
			return "", err
		}
		return listIdentity(fresh)
	}
	// Each list call retains its own options and observation.
	return c.record(fmt.Sprintf("%T/%d", list, len(c.snapshots)), snapshot{identity, read})
}

func (c *snapshotClient) record(key string, value snapshot) error {
	if previous, exists := c.snapshots[key]; exists && previous.identity != value.identity {
		return fmt.Errorf("runtime inputs changed during resolution: %s", key)
	}
	c.snapshots[key] = value
	return nil
}

func (c *snapshotClient) check(ctx context.Context) error {
	if c.readError != nil {
		return c.readError
	}
	for key, expected := range c.snapshots {
		if err := ctx.Err(); err != nil {
			return err
		}
		got, err := expected.read(ctx)
		if err != nil {
			return err
		}
		if got != expected.identity {
			return fmt.Errorf("runtime input changed: %s", key)
		}
	}
	return ctx.Err()
}

// Some member helpers tolerate failed optional lookups. A capacity observation
// cannot certify inputs after such a read, even when rendering returns a spec.
func (c *snapshotClient) rememberError(err error) {
	if err != nil && !apierrors.IsNotFound(err) && c.readError == nil {
		c.readError = err
	}
}

func objectIdentity(object metav1.Object) (string, error) {
	if object.GetName() == "" || object.GetUID() == "" || object.GetResourceVersion() == "" || !object.GetDeletionTimestamp().IsZero() {
		return "", fmt.Errorf("runtime input %s/%s has no verified live identity", object.GetNamespace(), object.GetName())
	}
	if service, ok := object.(*v1beta1.InferenceService); ok {
		// Runtime pin state is the only standing service status used by resolution.
		// Serving observations must not invalidate otherwise unchanged inputs.
		input := service.DeepCopy()
		input.TypeMeta = metav1.TypeMeta{}
		input.ResourceVersion, input.ManagedFields = "", nil
		input.Status = v1beta1.InferenceServiceStatus{
			PinnedRevisionName:   service.Status.PinnedRevisionName,
			LastRuntimeSyncToken: service.Status.LastRuntimeSyncToken,
		}
		data, err := json.Marshal(input)
		return string(data), err
	}
	data, err := json.Marshal([]string{object.GetNamespace(), object.GetName(), string(object.GetUID()), object.GetResourceVersion()})
	return string(data), err
}

func listIdentity(list client.ObjectList) (string, error) {
	if list.GetContinue() != "" {
		return "", fmt.Errorf("runtime catalog observation is incomplete")
	}
	items, err := meta.ExtractList(list)
	if err != nil {
		return "", err
	}
	identities := make([]string, 0, len(items))
	for _, item := range items {
		accessor, err := meta.Accessor(item)
		if err != nil {
			return "", err
		}
		identity, err := objectIdentity(accessor)
		if err != nil {
			return "", err
		}
		identities = append(identities, identity)
	}
	slices.Sort(identities)
	data, err := json.Marshal(identities)
	return string(data), err
}
