package inferenceservice

import (
	"context"
	"testing"
	"time"

	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
)

var ttlTestNow = time.Date(2026, time.January, 2, 15, 4, 5, 0, time.UTC)

func TestTTLRemaining(t *testing.T) {
	created := metav1.NewTime(ttlTestNow.Add(-30 * time.Minute))
	tests := []struct {
		name          string
		ttl           *int64
		created       metav1.Time
		wantOK        bool
		wantRemaining time.Duration
	}{
		{name: "no TTL", ttl: nil, created: created, wantOK: false},
		{name: "no creation timestamp", ttl: ptr.To[int64](3600), wantOK: false},
		{name: "not yet expired", ttl: ptr.To[int64](3600), created: created, wantOK: true, wantRemaining: 30 * time.Minute},
		{name: "expires now", ttl: ptr.To[int64](1800), created: created, wantOK: true, wantRemaining: 0},
		{name: "expired", ttl: ptr.To[int64](60), created: created, wantOK: true, wantRemaining: -29 * time.Minute},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{CreationTimestamp: test.created},
				Spec:       v1beta1.InferenceServiceSpec{TTLSecondsAfterCreation: test.ttl},
			}
			remaining, ok := ttlRemaining(isvc, ttlTestNow)
			g.Expect(ok).To(gomega.Equal(test.wantOK))
			if test.wantOK {
				g.Expect(remaining).To(gomega.Equal(test.wantRemaining))
			}
		})
	}
}

func TestRequeueBy(t *testing.T) {
	after := 10 * time.Minute
	tests := []struct {
		name   string
		result ctrl.Result
		err    error
		want   ctrl.Result
	}{
		{name: "no requeue gets the deadline", result: ctrl.Result{}, want: ctrl.Result{RequeueAfter: after}},
		{name: "later requeue is pulled in", result: ctrl.Result{RequeueAfter: time.Hour}, want: ctrl.Result{RequeueAfter: after}},
		{name: "sooner requeue is kept", result: ctrl.Result{RequeueAfter: time.Minute}, want: ctrl.Result{RequeueAfter: time.Minute}},
		{name: "immediate requeue is kept", result: ctrl.Result{Requeue: true}, want: ctrl.Result{Requeue: true}},
		{name: "error result is kept", result: ctrl.Result{}, err: apierrors.NewBadRequest("x"), want: ctrl.Result{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			g.Expect(requeueBy(test.result, test.err, after)).To(gomega.Equal(test.want))
		})
	}
}

func TestReconcileTTLAfterCreation(t *testing.T) {
	tests := []struct {
		name        string
		age         time.Duration
		ttlSeconds  int64
		member      bool
		wantDeleted bool
		wantEvent   bool
	}{
		{name: "expired InferenceService is deleted", age: 2 * time.Hour, ttlSeconds: 3600, wantDeleted: true, wantEvent: true},
		{name: "live InferenceService is requeued by its deadline", age: 10 * time.Minute, ttlSeconds: 3600},
		{name: "placement member copy ignores the TTL", age: 2 * time.Hour, ttlSeconds: 3600, member: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			ctx := context.Background()
			scheme := runtime.NewScheme()
			g.Expect(v1beta1.AddToScheme(scheme)).To(gomega.Succeed())

			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "ttl",
					Namespace:         "test",
					UID:               types.UID("ttl-uid"),
					CreationTimestamp: metav1.NewTime(ttlTestNow.Add(-test.age)),
					Annotations: map[string]string{
						constants.DeploymentMode: string(constants.VirtualDeployment),
					},
				},
				Spec: v1beta1.InferenceServiceSpec{TTLSecondsAfterCreation: ptr.To(test.ttlSeconds)},
			}
			if test.member {
				isvc.Labels = map[string]string{constants.PlacementOrigin: "source-cluster"}
			}
			live := clientfake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(isvc).
				WithStatusSubresource(isvc).
				Build()
			recorder := record.NewFakeRecorder(10)
			reconciler := &InferenceServiceReconciler{
				Client:    live,
				APIReader: live,
				Clientset: kubernetesfake.NewSimpleClientset(&corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      constants.InferenceServiceConfigMapName,
						Namespace: constants.OMENamespace,
					},
					Data: map[string]string{
						controllerconfig.DeployConfigName: `{"defaultDeploymentMode":"RawDeployment"}`,
					},
				}),
				Log:      ctrl.Log.WithName("test"),
				Recorder: recorder,
				Clock:    clocktesting.NewFakePassiveClock(ttlTestNow),
			}
			key := client.ObjectKeyFromObject(isvc)

			result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: key})

			stored := &v1beta1.InferenceService{}
			getErr := live.Get(ctx, key, stored)
			if test.wantDeleted {
				g.Expect(err).NotTo(gomega.HaveOccurred())
				g.Expect(apierrors.IsNotFound(getErr)).To(gomega.BeTrue())
			} else {
				g.Expect(getErr).NotTo(gomega.HaveOccurred())
			}
			if test.wantEvent {
				g.Expect(recorder.Events).To(gomega.Receive(gomega.ContainSubstring(ttlExpiredReason)))
			}
			if !test.wantDeleted && !test.member {
				g.Expect(err).NotTo(gomega.HaveOccurred())
				remaining := time.Duration(test.ttlSeconds)*time.Second - test.age
				g.Expect(result.RequeueAfter).To(gomega.BeNumerically(">", 0))
				g.Expect(result.RequeueAfter).To(gomega.BeNumerically("<=", remaining))
			}
		})
	}
}
