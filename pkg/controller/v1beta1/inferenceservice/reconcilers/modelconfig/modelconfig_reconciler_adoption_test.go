package multimodelconfig

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// serviceNamingAModel is a service that names a cluster model, with that model.
func serviceNamingAModel(uid types.UID) (*v1beta1.InferenceService, *v1beta1.ClusterBaseModel) {
	model := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "my-cluster-base-model"},
		Spec:       v1beta1.BaseModelSpec{ModelFormat: v1beta1.ModelFormat{Name: "safetensors"}},
	}
	isvc := modelLessService(uid)
	isvc.Spec.Model = &v1beta1.ModelRef{Name: model.Name}
	return isvc, model
}

// handMadeModelConfig is a ConfigMap of the service's model ConfigMap name
// that nobody controls, with a key of its own.
func handMadeModelConfig(isvc *v1beta1.InferenceService) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: constants.ModelConfigName(isvc.Name), Namespace: isvc.Namespace, UID: "hand-made-uid"},
		Data:       map[string]string{"notes": "written by hand"},
	}
}

// controllerReference is the controller reference of the given owner.
func controllerReference(apiVersion, kind, name string, uid types.UID) metav1.OwnerReference {
	return metav1.OwnerReference{APIVersion: apiVersion, Kind: kind, Name: name, UID: uid, Controller: ptr.To(true)}
}

// passOutcome is what one pass of the reconciler left behind.
type passOutcome struct {
	client   client.WithWatch
	scheme   *runtime.Scheme
	updates  *fakeclient.Calls
	recorder *record.FakeRecorder
	err      error
}

// reconcileOnce runs one pass of the service over the given objects.
func reconcileOnce(t *testing.T, isvc *v1beta1.InferenceService, objects ...client.Object) passOutcome {
	t.Helper()
	logf.SetLogger(logr.Discard())
	s := fakeclient.Scheme(t)
	updates := fakeclient.CountingVerb(fakeclient.Update)
	c := fakeclient.New(t, fakeclient.WithScheme(s), fakeclient.WithObjects(objects...), fakeclient.WithObjects(isvc), updates)
	recorder := record.NewFakeRecorder(8)
	_, err := NewModelConfigReconciler(c, s, recorder).Reconcile(context.Background(), isvc)
	return passOutcome{client: c, scheme: s, updates: updates, recorder: recorder, err: err}
}

// A ConfigMap of the service's name that nobody controls becomes the
// service's: it gets the controller reference and the label a created one
// carries, the model entry is written, and its other keys stay.
func TestModelConfigReconciler_AnUnownedConfigMapIsAdopted(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc, model := serviceNamingAModel("isvc-uid")
	handMade := handMadeModelConfig(isvc)
	pass := reconcileOnce(t, isvc, model, handMade)
	g.Expect(pass.err).NotTo(gomega.HaveOccurred())

	adopted := &corev1.ConfigMap{}
	g.Expect(pass.client.Get(context.Background(), client.ObjectKeyFromObject(handMade), adopted)).To(gomega.Succeed())
	g.Expect(adopted.UID).To(gomega.Equal(handMade.UID), "the ConfigMap is taken over, not replaced")
	g.Expect(adopted.OwnerReferences).To(gomega.ConsistOf(metav1.OwnerReference{
		APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: "InferenceService", Name: isvc.Name, UID: isvc.UID,
		Controller: ptr.To(true), BlockOwnerDeletion: ptr.To(true),
	}), "the reference garbage collection follows when the service goes")
	g.Expect(adopted.Labels).To(gomega.HaveKeyWithValue(constants.InferenceServicePodLabelKey, isvc.Name))
	g.Expect(adopted.Data).To(gomega.HaveKeyWithValue("notes", "written by hand"))
	g.Expect(adopted.Data[constants.ModelConfigKey]).To(gomega.ContainSubstring(model.Name))
	g.Expect(fakeclient.RecordedEvents(pass.recorder)).To(gomega.BeEmpty())
}

// The ConfigMap the service already controls is kept: one pass brings its
// entry up to date under its one reference, and the next pass writes nothing.
func TestModelConfigReconciler_AConfigMapItControlsIsWrittenOnce(t *testing.T) {
	g := gomega.NewWithT(t)
	ctx := context.Background()
	isvc, model := serviceNamingAModel("isvc-uid")
	own := modelConfigControlledBy(isvc, isvc.UID)
	pass := reconcileOnce(t, isvc, model, own)
	g.Expect(pass.err).NotTo(gomega.HaveOccurred())
	g.Expect(pass.updates.Count()).To(gomega.Equal(1), "the entry of the old model is rewritten")

	kept := &corev1.ConfigMap{}
	g.Expect(pass.client.Get(ctx, client.ObjectKeyFromObject(own), kept)).To(gomega.Succeed())
	g.Expect(kept.OwnerReferences).To(gomega.HaveLen(1))
	g.Expect(metav1.IsControlledBy(kept, isvc)).To(gomega.BeTrue())
	g.Expect(kept.Data[constants.ModelConfigKey]).To(gomega.ContainSubstring(model.Name))

	_, err := NewModelConfigReconciler(pass.client, pass.scheme, pass.recorder).Reconcile(ctx, isvc)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(pass.updates.Count()).To(gomega.Equal(1), "a pass that finds nothing to change writes nothing")
	g.Expect(fakeclient.RecordedEvents(pass.recorder)).To(gomega.BeEmpty())
}

// A ConfigMap of the service's name under another controller stays as it
// is: nothing is written, the service's event names that controller, and
// the pass goes on without a model ConfigMap.
func TestModelConfigReconciler_AConfigMapAnotherControllerHoldsIsRefused(t *testing.T) {
	for name, owner := range map[string]metav1.OwnerReference{
		"another InferenceService":     controllerReference(v1beta1.SchemeGroupVersion.String(), "InferenceService", "other-service", "other-service-uid"),
		"a controller that is not OME": controllerReference("apps/v1", "Deployment", "other-app", "other-app-uid"),
	} {
		t.Run(name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			isvc, model := serviceNamingAModel("isvc-uid")
			held := handMadeModelConfig(isvc)
			held.OwnerReferences = []metav1.OwnerReference{owner}
			pass := reconcileOnce(t, isvc, model, held)
			g.Expect(pass.err).NotTo(gomega.HaveOccurred(), "the pass goes on without a model ConfigMap")
			g.Expect(pass.updates.Count()).To(gomega.BeZero())

			unchanged := &corev1.ConfigMap{}
			g.Expect(pass.client.Get(context.Background(), client.ObjectKeyFromObject(held), unchanged)).To(gomega.Succeed())
			g.Expect(unchanged.OwnerReferences).To(gomega.Equal([]metav1.OwnerReference{owner}))
			g.Expect(unchanged.Data).To(gomega.Equal(map[string]string{"notes": "written by hand"}))
			g.Expect(fakeclient.RecordedEvents(pass.recorder)).To(gomega.ConsistOf(
				"Warning ModelConfigNotAdopted OME did not take over ConfigMap " + held.Name + ": " + owner.Kind + " " + owner.Name +
					" controls it. The service runs without a model ConfigMap until that ConfigMap is deleted or its controller reference removed."))
		})
	}
}

// A ConfigMap controlled by an earlier service of this name, one deleted
// and created again before garbage collection took its ConfigMap, is taken
// over: the controller reference moves to the service that exists.
func TestModelConfigReconciler_AConfigMapOfAnEarlierServiceOfThisNameIsTakenOver(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc, model := serviceNamingAModel("isvc-uid")
	earlier := modelConfigControlledBy(isvc, "earlier-service-uid")
	pass := reconcileOnce(t, isvc, model, earlier)
	g.Expect(pass.err).NotTo(gomega.HaveOccurred())

	taken := &corev1.ConfigMap{}
	g.Expect(pass.client.Get(context.Background(), client.ObjectKeyFromObject(earlier), taken)).To(gomega.Succeed())
	g.Expect(taken.OwnerReferences).To(gomega.HaveLen(1))
	g.Expect(metav1.IsControlledBy(taken, isvc)).To(gomega.BeTrue())
	g.Expect(fakeclient.RecordedEvents(pass.recorder)).To(gomega.BeEmpty())
}
