package replay

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"
)

// laggedClient is the engine's cached client during a stale pass: every
// read is served from the objects as they stood at the end of the previous
// tick, while every write goes to the apiserver. The live reader the
// engine holds beside it is untouched, so the pass separates the two
// exactly as the engine does.
type laggedClient struct {
	client.Client
	snapshot client.Reader
}

func (c *laggedClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	return c.snapshot.Get(ctx, key, obj, opts...)
}

func (c *laggedClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	return c.snapshot.List(ctx, list, opts...)
}

// snapshotCluster copies every object the engine could read through its
// cached client into a reader frozen at this instant. Pods are read
// through the driver's own client, so the copies carry the deletion
// metadata the driver displays rather than what the fake store holds.
func (d *driver) snapshotCluster(ctx context.Context) (client.Reader, error) {
	var objects []client.Object
	for _, list := range []client.ObjectList{
		&corev1.PodList{}, &discoveryv1.EndpointSliceList{}, &corev1.ServiceList{}, &corev1.ConfigMapList{},
		&corev1.NodeList{}, &appsv1.ControllerRevisionList{}, &schedulingv1alpha1.PodGroupList{},
	} {
		if err := d.cli.List(ctx, list); err != nil {
			return nil, fmt.Errorf("replay: snapshot the cluster: %w", err)
		}
		items, err := listItems(list)
		if err != nil {
			return nil, err
		}
		objects = append(objects, items...)
	}
	scheme, err := buildScheme()
	if err != nil {
		return nil, err
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build(), nil
}

// listItems detaches a list's items as independent objects.
func listItems(list client.ObjectList) ([]client.Object, error) {
	var out []client.Object
	add := func(obj runtime.Object) error {
		copied, ok := obj.DeepCopyObject().(client.Object)
		if !ok {
			return fmt.Errorf("replay: snapshot: %T is not an object", obj)
		}
		out = append(out, copied)
		return nil
	}
	switch l := list.(type) {
	case *corev1.PodList:
		for i := range l.Items {
			if err := add(&l.Items[i]); err != nil {
				return nil, err
			}
		}
	case *discoveryv1.EndpointSliceList:
		for i := range l.Items {
			if err := add(&l.Items[i]); err != nil {
				return nil, err
			}
		}
	case *corev1.ServiceList:
		for i := range l.Items {
			if err := add(&l.Items[i]); err != nil {
				return nil, err
			}
		}
	case *corev1.ConfigMapList:
		for i := range l.Items {
			if err := add(&l.Items[i]); err != nil {
				return nil, err
			}
		}
	case *corev1.NodeList:
		for i := range l.Items {
			if err := add(&l.Items[i]); err != nil {
				return nil, err
			}
		}
	case *appsv1.ControllerRevisionList:
		for i := range l.Items {
			if err := add(&l.Items[i]); err != nil {
				return nil, err
			}
		}
	case *schedulingv1alpha1.PodGroupList:
		for i := range l.Items {
			if err := add(&l.Items[i]); err != nil {
				return nil, err
			}
		}
	default:
		return nil, fmt.Errorf("replay: snapshot: unhandled list %T", list)
	}
	return out, nil
}

// cachedClient is the client the engine reads its cache through this
// pass: the apiserver itself, or the previous tick's objects when the
// scenario staged a stale read.
func (d *driver) cachedClient() client.Client {
	if d.stalePass {
		return &laggedClient{Client: d.cli, snapshot: d.staleCache}
	}
	return d.cli
}

// refreshStaleCache takes the snapshot a later stale pass will read, at
// the end of a tick. Only a scenario that stages a stale read pays for it.
func (d *driver) refreshStaleCache(ctx context.Context, tick int) error {
	if !d.usesStaleReads {
		return nil
	}
	snapshot, err := d.snapshotCluster(ctx)
	if err != nil {
		return err
	}
	d.staleCache, d.staleCacheTick = snapshot, tick
	return nil
}

// applyStaleRead stages one pass whose cached reads serve the previous
// tick's objects while its live reads stay current: the informer lagging
// one event behind the apiserver.
func applyStaleRead(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	if ev.Variant != "" {
		return "", fmt.Errorf("replay: ctrl.staleRead takes no variant")
	}
	if d.staleCache == nil {
		return "", fmt.Errorf("replay: ctrl.staleRead: no cache snapshot to serve")
	}
	d.stalePass = true
	if d.staleCacheTick == 0 {
		return "cache=initial", nil
	}
	return fmt.Sprintf("cache=end-of-tick-%d", d.staleCacheTick), nil
}
