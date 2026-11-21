package inferenceservice

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

var ttlTestNow = time.Date(2026, time.January, 2, 15, 4, 5, 0, time.UTC)

// longestTTLSeconds is the longest TTL whose nanoseconds fit a
// time.Duration. The tests work it out from the two types themselves, so
// they hold the controller to it without reading the controller's own limit.
const longestTTLSeconds = math.MaxInt64 / int64(time.Second)

func TestTTLRemaining(t *testing.T) {
	created := metav1.NewTime(ttlTestNow.Add(-30 * time.Minute))
	fresh := metav1.NewTime(ttlTestNow)
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
		{name: "the longest TTL a Duration holds, on a fresh service", ttl: ptr.To[int64](longestTTLSeconds), created: fresh,
			wantOK: true, wantRemaining: time.Duration(longestTTLSeconds) * time.Second},
		{name: "one second past the longest TTL never passes", ttl: ptr.To[int64](longestTTLSeconds + 1), created: fresh, wantOK: false},
		{name: "2 to the power 40 seconds never passes", ttl: ptr.To[int64](1 << 40), created: fresh, wantOK: false},
		{name: "the largest int64 never passes", ttl: ptr.To[int64](math.MaxInt64), created: fresh, wantOK: false},
		{name: "2 to the power 40 seconds never passes on a service half an hour old", ttl: ptr.To[int64](1 << 40), created: created, wantOK: false},
		{name: "a service created 400 years back has passed: Sub holds at the smallest Duration", ttl: ptr.To[int64](60),
			created: metav1.NewTime(ttlTestNow.AddDate(-400, 0, 0)), wantOK: true, wantRemaining: time.Duration(math.MinInt64)},
		{name: "a service created 400 years ahead has not passed: Sub holds at the largest Duration", ttl: ptr.To[int64](60),
			created: metav1.NewTime(ttlTestNow.AddDate(400, 0, 0)), wantOK: true, wantRemaining: time.Duration(math.MaxInt64)},
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

// requeueBy compares and copies and adds nothing, so the largest Duration
// goes through it as it came.
func TestRequeueByTheLargestDuration(t *testing.T) {
	g := gomega.NewWithT(t)
	largest := time.Duration(math.MaxInt64)
	g.Expect(requeueBy(ctrl.Result{}, nil, largest)).To(gomega.Equal(ctrl.Result{RequeueAfter: largest}))
	g.Expect(requeueBy(ctrl.Result{RequeueAfter: time.Minute}, nil, largest)).To(gomega.Equal(ctrl.Result{RequeueAfter: time.Minute}))
	g.Expect(requeueBy(ctrl.Result{RequeueAfter: largest}, nil, time.Minute)).To(gomega.Equal(ctrl.Result{RequeueAfter: time.Minute}))
}

// ttlPass is what one reconcile pass over a service returned and left behind.
type ttlPass struct {
	result ctrl.Result
	err    error
	// getErr is the error of reading the service once the pass has ended.
	getErr error
	// events are the events the pass recorded, in order.
	events []string
}

// reconcileTTLService runs one pass over a service of the given age, while
// the clock reads ttlTestNow. A nil ttl leaves the TTL unset, and member
// makes the service a placement member copy.
func reconcileTTLService(t *testing.T, age time.Duration, ttl *int64, member bool) ttlPass {
	t.Helper()
	ctx := context.Background()
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "ttl",
			Namespace:         "test",
			UID:               types.UID("ttl-uid"),
			CreationTimestamp: metav1.NewTime(ttlTestNow.Add(-age)),
			Annotations: map[string]string{
				constants.DeploymentMode: string(constants.VirtualDeployment),
			},
		},
		Spec: v1beta1.InferenceServiceSpec{TTLSecondsAfterCreation: ttl},
	}
	if member {
		isvc.Labels = map[string]string{constants.PlacementOrigin: "source-cluster"}
	}
	live := fakeclient.New(t, fakeclient.WithObjects(isvc), fakeclient.WithStatusSubresource(isvc))
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

	pass := ttlPass{}
	pass.result, pass.err = reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: key})
	pass.getErr = live.Get(ctx, key, &v1beta1.InferenceService{})
	for {
		select {
		case event := <-recorder.Events:
			pass.events = append(pass.events, event)
		default:
			return pass
		}
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
		{name: "fresh InferenceService with the longest TTL a Duration holds is requeued by its deadline", ttlSeconds: longestTTLSeconds},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			pass := reconcileTTLService(t, test.age, ptr.To(test.ttlSeconds), test.member)

			if test.wantDeleted {
				g.Expect(pass.err).NotTo(gomega.HaveOccurred())
				g.Expect(apierrors.IsNotFound(pass.getErr)).To(gomega.BeTrue())
			} else {
				g.Expect(pass.getErr).NotTo(gomega.HaveOccurred())
			}
			if test.wantEvent {
				g.Expect(pass.events).To(gomega.ContainElement(gomega.ContainSubstring(ttlExpiredReason)))
			} else {
				g.Expect(pass.events).NotTo(gomega.ContainElement(gomega.ContainSubstring(ttlExpiredReason)))
			}
			if !test.wantDeleted && !test.member {
				g.Expect(pass.err).NotTo(gomega.HaveOccurred())
				remaining := time.Duration(test.ttlSeconds)*time.Second - test.age
				g.Expect(pass.result.RequeueAfter).To(gomega.BeNumerically(">", 0))
				g.Expect(pass.result.RequeueAfter).To(gomega.BeNumerically("<=", remaining))
			}
		})
	}
}

// A TTL longer than a Duration holds never passes, so a fresh service that
// carries one is treated as a service with no TTL: it stays, no TTLExpired
// event is recorded, and the pass asks for the same next pass.
func TestReconcileKeepsAServiceWhoseTTLNoDurationHolds(t *testing.T) {
	noTTL := reconcileTTLService(t, 0, nil, false)
	gomega.NewWithT(t).Expect(noTTL.err).NotTo(gomega.HaveOccurred())
	tests := []struct {
		name       string
		ttlSeconds int64
	}{
		{name: "2 to the power 40 seconds", ttlSeconds: 1 << 40},
		{name: "the largest int64", ttlSeconds: math.MaxInt64},
		{name: "one second past the longest TTL a Duration holds", ttlSeconds: longestTTLSeconds + 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			pass := reconcileTTLService(t, 0, ptr.To(test.ttlSeconds), false)

			g.Expect(pass.err).NotTo(gomega.HaveOccurred())
			g.Expect(pass.getErr).NotTo(gomega.HaveOccurred())
			g.Expect(pass.events).NotTo(gomega.ContainElement(gomega.ContainSubstring(ttlExpiredReason)))
			g.Expect(pass.result).To(gomega.Equal(noTTL.result))
		})
	}
}
