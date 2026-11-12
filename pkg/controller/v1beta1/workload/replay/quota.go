package replay

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// replayQuotaName is the ResourceQuota a scenario's quota stands as.
const replayQuotaName = "replay-quota"

// seedQuota writes the namespace's ResourceQuota the run enforces, with
// the pod count it admits as its enforced limit and nothing charged yet.
func (d *driver) seedQuota(ctx context.Context, spec *QuotaSpec) error {
	if spec == nil {
		return nil
	}
	d.quota = spec
	quota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: d.opts.Namespace, Name: replayQuotaName},
		Spec:       corev1.ResourceQuotaSpec{Hard: podCount(int(spec.Pods))},
		Status:     corev1.ResourceQuotaStatus{Hard: podCount(int(spec.Pods)), Used: podCount(0)},
	}
	return d.staging(func() error { return d.cli.Create(ctx, quota) })
}

func podCount(pods int) corev1.ResourceList {
	return corev1.ResourceList{corev1.ResourcePods: *resource.NewQuantity(int64(pods), resource.DecimalSI)}
}

// podsCharged counts the pods the quota charges: every pod of the
// namespace that has not ended, a terminating one included, as the
// apiserver counts them.
func (d *driver) podsCharged(ctx context.Context, c client.Reader) (int, error) {
	pods := &corev1.PodList{}
	if err := c.List(ctx, pods, client.InNamespace(d.opts.Namespace)); err != nil {
		return 0, fmt.Errorf("replay: count the pods the quota charges: %w", err)
	}
	charged := 0
	for i := range pods.Items {
		switch pods.Items[i].Status.Phase {
		case corev1.PodSucceeded, corev1.PodFailed:
		default:
			charged++
		}
	}
	return charged, nil
}

// publishQuotaUsage writes the pods charged into the quota's status before
// the engine reads it, as the apiserver's quota controller keeps a real
// one current. The read is traced, so a golden shows what the engine saw.
func (d *driver) publishQuotaUsage(ctx context.Context, c client.WithWatch) error {
	if d.quota == nil {
		return nil
	}
	charged, err := d.podsCharged(ctx, c)
	if err != nil {
		return err
	}
	quota := &corev1.ResourceQuota{}
	key := client.ObjectKey{Namespace: d.opts.Namespace, Name: replayQuotaName}
	if err := c.Get(ctx, key, quota); err != nil {
		return fmt.Errorf("replay: read the quota: %w", err)
	}
	quota.Status.Used = podCount(charged)
	if err := c.Update(ctx, quota); err != nil {
		return fmt.Errorf("replay: publish the quota's usage: %w", err)
	}
	if !d.quiet {
		d.trace.emit("quota pods used=%d hard=%d", charged, d.quota.Pods)
	}
	return nil
}

// quotaRefusal is admission's answer to a pod create under the run's
// quota: nil when the pod fits, when the run has no quota, or when the
// scenario itself stages the write.
func (d *driver) quotaRefusal(ctx context.Context, c client.WithWatch, obj client.Object) error {
	pod, isPod := obj.(*corev1.Pod)
	if d.quota == nil || !isPod || d.quiet {
		return nil
	}
	charged, err := d.podsCharged(ctx, c)
	if err != nil {
		return err
	}
	if charged+1 <= int(d.quota.Pods) {
		return nil
	}
	return apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, pod.Name, fmt.Errorf(
		"exceeded quota: %s, requested: pods=1, used: pods=%d, limited: pods=%d", replayQuotaName, charged, d.quota.Pods))
}
