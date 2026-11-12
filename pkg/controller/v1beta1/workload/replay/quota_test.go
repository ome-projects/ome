package replay

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func quotaTestDriver(t *testing.T, pods ...*corev1.Pod) (*driver, client.WithWatch) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add to scheme: %v", err)
	}
	objects := make([]client.Object, 0, len(pods))
	for _, pod := range pods {
		objects = append(objects, pod)
	}
	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
	return &driver{opts: Options{Namespace: "ns"}, cli: cli, trace: &Trace{}}, cli
}

func podInPhase(name string, phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name},
		Status:     corev1.PodStatus{Phase: phase},
	}
}

// TestQuotaChargesEveryPodThatHasNotEnded: the run's quota counts the pods
// the apiserver counts, a terminating one included and an ended one not,
// refuses the create that would pass the limit in the apiserver's words,
// and answers nothing for the scenario's own staged writes.
func TestQuotaChargesEveryPodThatHasNotEnded(t *testing.T) {
	d, cli := quotaTestDriver(t,
		podInPhase("running", corev1.PodRunning),
		podInPhase("pending", corev1.PodPending),
		podInPhase("ended", corev1.PodSucceeded),
	)
	charged, err := d.podsCharged(context.Background(), cli)
	if err != nil || charged != 2 {
		t.Fatalf("pods charged: got %d err=%v want 2", charged, err)
	}
	next := podInPhase("next", corev1.PodPending)
	if err := d.quotaRefusal(context.Background(), cli, next); err != nil {
		t.Fatalf("with no quota every create is admitted, got %v", err)
	}
	if err := d.seedQuota(context.Background(), &QuotaSpec{Pods: 2}); err != nil {
		t.Fatalf("seed quota: %v", err)
	}
	err = d.quotaRefusal(context.Background(), cli, next)
	if !apierrors.IsForbidden(err) {
		t.Fatalf("a create past the limit: got %v want the apiserver's forbidden answer", err)
	}
	want := "exceeded quota: replay-quota, requested: pods=1, used: pods=2, limited: pods=2"
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("refusal: got %q want it to carry %q", err.Error(), want)
	}
	d.quiet = true
	if err := d.quotaRefusal(context.Background(), cli, next); err != nil {
		t.Fatalf("a staged write is never refused, got %v", err)
	}
	d.quiet = false
	d.quota.Pods = 3
	if err := d.quotaRefusal(context.Background(), cli, next); err != nil {
		t.Fatalf("a create within the limit: got %v want admitted", err)
	}
}

// TestQuotaUsagePublishedBeforeTheEngineReads: the quota object the engine
// lists carries the pods charged at the moment of the read.
func TestQuotaUsagePublishedBeforeTheEngineReads(t *testing.T) {
	d, cli := quotaTestDriver(t, podInPhase("running", corev1.PodRunning))
	if err := d.seedQuota(context.Background(), &QuotaSpec{Pods: 5}); err != nil {
		t.Fatalf("seed quota: %v", err)
	}
	if err := d.publishQuotaUsage(context.Background(), cli); err != nil {
		t.Fatalf("publish usage: %v", err)
	}
	quota := &corev1.ResourceQuota{}
	if err := cli.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: replayQuotaName}, quota); err != nil {
		t.Fatalf("read quota: %v", err)
	}
	if used := quota.Status.Used[corev1.ResourcePods]; used.Value() != 1 {
		t.Fatalf("used pods: got %s want 1", used.String())
	}
	if hard := quota.Status.Hard[corev1.ResourcePods]; hard.Value() != 5 {
		t.Fatalf("hard pods: got %s want 5", hard.String())
	}
}
