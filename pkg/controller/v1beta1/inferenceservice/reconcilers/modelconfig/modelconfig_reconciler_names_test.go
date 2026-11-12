package multimodelconfig

import (
	"context"
	"testing"

	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// serviceNamed is a service of the given name that names a cluster model of
// the given name, with that model.
func serviceNamed(name, modelName string) (*v1beta1.InferenceService, *v1beta1.ClusterBaseModel) {
	isvc, model := serviceNamingAModel(types.UID(name + "-uid"))
	isvc.Name = name
	model.Name = modelName
	isvc.Spec.Model.Name = modelName
	return isvc, model
}

// modelConfigNamed is a model ConfigMap of the service under the given name:
// labelled with the service's name, controlled by the owner UID (by nobody
// when empty), holding one model entry.
func modelConfigNamed(isvc *v1beta1.InferenceService, name string, ownerUID types.UID) *corev1.ConfigMap {
	configMap := modelConfigControlledBy(isvc, ownerUID)
	configMap.Name = name
	configMap.UID = types.UID(name + "-uid")
	configMap.Labels = map[string]string{constants.InferenceServicePodLabelKey: isvc.Name}
	return configMap
}

// Two services whose names share a long tail each keep a model ConfigMap of
// their own, named after the whole service name and holding their own model.
func TestModelConfigReconciler_ServicesWhoseNamesShareATailKeepTheirOwnConfigMaps(t *testing.T) {
	g := gomega.NewWithT(t)
	ctx := context.Background()
	blue, blueModel := serviceNamed("blue-llama-70b-instruct-chat-v2", "blue-model")
	green, greenModel := serviceNamed("green-llama-70b-instruct-chat-v2", "green-model")
	s := fakeclient.Scheme(t)
	recorder := record.NewFakeRecorder(8)
	c := fakeclient.New(t, fakeclient.WithScheme(s), fakeclient.WithObjects(blue, blueModel, green, greenModel))
	reconciler := NewModelConfigReconciler(c, s, recorder)

	for _, isvc := range []*v1beta1.InferenceService{blue, green} {
		_, err := reconciler.Reconcile(ctx, isvc)
		g.Expect(err).NotTo(gomega.HaveOccurred(), isvc.Name)
	}
	for isvc, model := range map[*v1beta1.InferenceService]*v1beta1.ClusterBaseModel{blue: blueModel, green: greenModel} {
		configMap := &corev1.ConfigMap{}
		g.Expect(c.Get(ctx, types.NamespacedName{Name: constants.ModelConfigName(isvc.Name), Namespace: isvc.Namespace}, configMap)).To(gomega.Succeed())
		g.Expect(configMap.Name).To(gomega.HaveSuffix(isvc.Name))
		g.Expect(metav1.IsControlledBy(configMap, isvc)).To(gomega.BeTrue(), isvc.Name)
		g.Expect(configMap.Data[constants.ModelConfigKey]).To(gomega.ContainSubstring(model.Name))
	}
	g.Expect(fakeclient.RecordedEvents(recorder)).To(gomega.BeEmpty())
}

// A model ConfigMap this service controls under another name goes once the
// one under the current name is in place. A ConfigMap another owner
// controls, or one without the models key, is not this service's model
// ConfigMap and stays.
func TestModelConfigReconciler_AModelConfigMapItControlsUnderAnotherNameIsRemoved(t *testing.T) {
	g := gomega.NewWithT(t)
	ctx := context.Background()
	isvc, model := serviceNamingAModel("isvc-uid")
	former := modelConfigNamed(isvc, "modelconfig-chat-v2-prod", isvc.UID)
	another := modelConfigNamed(isvc, "modelconfig-chat-v2-other", "other-service-uid")
	unowned := modelConfigNamed(isvc, "modelconfig-chat-v2-unowned", "")
	notes := modelConfigNamed(isvc, "notes-of-the-service", isvc.UID)
	notes.Data = map[string]string{"notes": "written by hand"}
	s := fakeclient.Scheme(t)
	deletes := fakeclient.CountingVerb(fakeclient.Delete)
	c := fakeclient.New(t, fakeclient.WithScheme(s), fakeclient.WithObjects(isvc, model, former, another, unowned, notes), deletes)

	_, err := NewModelConfigReconciler(c, s, record.NewFakeRecorder(8)).Reconcile(ctx, isvc)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	current := &corev1.ConfigMap{}
	g.Expect(c.Get(ctx, types.NamespacedName{Name: constants.ModelConfigName(isvc.Name), Namespace: isvc.Namespace}, current)).To(gomega.Succeed())
	g.Expect(metav1.IsControlledBy(current, isvc)).To(gomega.BeTrue())
	g.Expect(apierrors.IsNotFound(c.Get(ctx, client.ObjectKeyFromObject(former), &corev1.ConfigMap{}))).To(gomega.BeTrue(), "the ConfigMap under the other name stays")
	for _, kept := range []*corev1.ConfigMap{another, unowned, notes} {
		g.Expect(c.Get(ctx, client.ObjectKeyFromObject(kept), &corev1.ConfigMap{})).To(gomega.Succeed(), kept.Name)
	}
	options := fakeclient.OptionsOf[client.DeleteOptions](deletes)
	g.Expect(options).To(gomega.HaveLen(1))
	g.Expect(options[0].Preconditions.UID).To(gomega.HaveValue(gomega.Equal(former.UID)), "the delete names the object that was read")
}

// A service without a model keeps no model ConfigMap under any name.
func TestModelConfigReconciler_AServiceWithoutAModelRemovesItsConfigMapUnderAnotherName(t *testing.T) {
	g := gomega.NewWithT(t)
	ctx := context.Background()
	isvc := modelLessService("isvc-uid")
	former := modelConfigNamed(isvc, "modelconfig-chat-v2-prod", isvc.UID)
	s := fakeclient.Scheme(t)
	c := fakeclient.New(t, fakeclient.WithScheme(s), fakeclient.WithObjects(isvc, former))

	_, err := NewModelConfigReconciler(c, s, record.NewFakeRecorder(8)).Reconcile(ctx, isvc)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(apierrors.IsNotFound(c.Get(ctx, client.ObjectKeyFromObject(former), &corev1.ConfigMap{}))).To(gomega.BeTrue(), "the ConfigMap under the other name stays")
}

// The ConfigMap under the current name comes first: while the model cannot
// be read, the one under another name stays.
func TestModelConfigReconciler_AnUnreadableModelLeavesTheConfigMapUnderAnotherName(t *testing.T) {
	g := gomega.NewWithT(t)
	ctx := context.Background()
	isvc, _ := serviceNamingAModel("isvc-uid")
	former := modelConfigNamed(isvc, "modelconfig-chat-v2-prod", isvc.UID)
	s := fakeclient.Scheme(t)
	deletes := fakeclient.CountingVerb(fakeclient.Delete)
	c := fakeclient.New(t, fakeclient.WithScheme(s), fakeclient.WithObjects(isvc, former), deletes)

	_, err := NewModelConfigReconciler(c, s, record.NewFakeRecorder(8)).Reconcile(ctx, isvc)
	g.Expect(err).To(gomega.HaveOccurred())
	g.Expect(deletes.Count()).To(gomega.BeZero())
	g.Expect(c.Get(ctx, client.ObjectKeyFromObject(former), &corev1.ConfigMap{})).To(gomega.Succeed())
}

// While the ConfigMap under the current name is another controller's, the
// service keeps no model ConfigMap at all: the one it controls under
// another name goes in the pass that records the refusal. A service has one
// model ConfigMap or none, never a copy under a name it does not write.
func TestModelConfigReconciler_ARefusedCurrentNameLeavesTheServiceWithNoConfigMap(t *testing.T) {
	g := gomega.NewWithT(t)
	ctx := context.Background()
	isvc, model := serviceNamingAModel("isvc-uid")
	holder := controllerReference("apps/v1", "Deployment", "other-app", "other-app-uid")
	held := handMadeModelConfig(isvc)
	held.OwnerReferences = []metav1.OwnerReference{holder}
	former := modelConfigNamed(isvc, "modelconfig-chat-v2-prod", isvc.UID)
	s := fakeclient.Scheme(t)
	deletes := fakeclient.CountingVerb(fakeclient.Delete)
	c := fakeclient.New(t, fakeclient.WithScheme(s), fakeclient.WithObjects(isvc, model, held, former), deletes)
	recorder := record.NewFakeRecorder(8)

	_, err := NewModelConfigReconciler(c, s, recorder).Reconcile(ctx, isvc)
	g.Expect(err).NotTo(gomega.HaveOccurred(), "the pass goes on without a model ConfigMap")
	unchanged := &corev1.ConfigMap{}
	g.Expect(c.Get(ctx, client.ObjectKeyFromObject(held), unchanged)).To(gomega.Succeed())
	g.Expect(unchanged.OwnerReferences).To(gomega.Equal([]metav1.OwnerReference{holder}), "the held ConfigMap stays the other controller's")
	g.Expect(apierrors.IsNotFound(c.Get(ctx, client.ObjectKeyFromObject(former), &corev1.ConfigMap{}))).To(gomega.BeTrue(),
		"the ConfigMap under the other name stays while the current name is refused")
	g.Expect(deletes.Count()).To(gomega.Equal(1))
	g.Expect(fakeclient.RecordedEvents(recorder)).To(gomega.HaveLen(1), "the refusal is recorded once")
	all := &corev1.ConfigMapList{}
	g.Expect(c.List(ctx, all, client.InNamespace(isvc.Namespace))).To(gomega.Succeed())
	for i := range all.Items {
		g.Expect(metav1.IsControlledBy(&all.Items[i], isvc)).To(gomega.BeFalse(), all.Items[i].Name)
	}
}
