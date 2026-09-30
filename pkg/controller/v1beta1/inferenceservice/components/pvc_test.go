package components

import (
	"testing"

	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/render"
)

func newPVCBaseComponent(uri string) *BaseComponentFields {
	return &BaseComponentFields{Piece: render.Piece{
		BaseModel: &v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{StorageUri: ptr.To(uri)},
		},
		BaseModelMeta: &metav1.ObjectMeta{Name: "llama-7b", Namespace: "models"},
		Log:           ctrl.Log.WithName("test"),
	}}
}

func TestUpdatePodSpecVolumes_PVC(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	b := newPVCBaseComponent("pvc://my-pvc/models/llama")

	podSpec := &v1.PodSpec{}
	UpdatePodSpecVolumes(b, &v1beta1.InferenceService{}, podSpec, &metav1.ObjectMeta{})

	g.Expect(podSpec.Volumes).To(gomega.HaveLen(1))
	vol := podSpec.Volumes[0]
	g.Expect(vol.Name).To(gomega.Equal("llama-7b"))
	g.Expect(vol.PersistentVolumeClaim).NotTo(gomega.BeNil())
	g.Expect(vol.PersistentVolumeClaim.ClaimName).To(gomega.Equal("my-pvc"))
	g.Expect(vol.PersistentVolumeClaim.ReadOnly).To(gomega.BeTrue())
	g.Expect(vol.HostPath).To(gomega.BeNil())
}

func TestUpdatePodSpecVolumes_PVC_ClusterScoped(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	b := newPVCBaseComponent("pvc://shared:my-pvc/models/llama")

	podSpec := &v1.PodSpec{}
	UpdatePodSpecVolumes(b, &v1beta1.InferenceService{}, podSpec, &metav1.ObjectMeta{})

	g.Expect(podSpec.Volumes).To(gomega.HaveLen(1))
	g.Expect(podSpec.Volumes[0].PersistentVolumeClaim.ClaimName).To(gomega.Equal("my-pvc"))
}

func TestUpdatePodSpecVolumes_HostPath_Unchanged(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	path := "/mnt/data/models/llama"
	b := &BaseComponentFields{Piece: render.Piece{
		BaseModel: &v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{
			StorageUri: ptr.To("oci://n/ns/b/bucket/o/path"),
			Path:       &path,
		}},
		BaseModelMeta: &metav1.ObjectMeta{Name: "llama-7b", Namespace: "models"},
		Log:           ctrl.Log.WithName("test"),
	}}

	podSpec := &v1.PodSpec{}
	UpdatePodSpecVolumes(b, &v1beta1.InferenceService{}, podSpec, &metav1.ObjectMeta{})

	g.Expect(podSpec.Volumes).To(gomega.HaveLen(1))
	g.Expect(podSpec.Volumes[0].HostPath).NotTo(gomega.BeNil())
	g.Expect(podSpec.Volumes[0].HostPath.Path).To(gomega.Equal(path))
	g.Expect(podSpec.Volumes[0].PersistentVolumeClaim).To(gomega.BeNil())
}

func TestUpdateVolumeMounts_PVC(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	b := newPVCBaseComponent("pvc://my-pvc/models/llama")

	container := &v1.Container{Name: "main"}
	UpdateVolumeMounts(b, &v1beta1.InferenceService{}, container, &metav1.ObjectMeta{})

	g.Expect(container.VolumeMounts).To(gomega.HaveLen(1))
	vm := container.VolumeMounts[0]
	g.Expect(vm.Name).To(gomega.Equal("llama-7b"))
	g.Expect(vm.MountPath).To(gomega.Equal(constants.ModelDefaultMountPath))
	g.Expect(vm.SubPath).To(gomega.Equal("models/llama"))
	g.Expect(vm.ReadOnly).To(gomega.BeTrue())
}

func TestUpdatePodSpecNodeSelector_PVC_Skipped(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	b := newPVCBaseComponent("pvc://my-pvc/models/llama")

	podSpec := &v1.PodSpec{}
	UpdatePodSpecNodeSelector(b, &v1beta1.InferenceService{}, podSpec, "")

	if podSpec.NodeSelector != nil {
		for k := range podSpec.NodeSelector {
			g.Expect(k).NotTo(gomega.HavePrefix("models.ome.io/"),
				"PVC-backed BaseModel must not get model node selector")
		}
	}
	if podSpec.Affinity != nil && podSpec.Affinity.NodeAffinity != nil {
		g.Expect(podSpec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution).
			To(gomega.BeEmpty(), "PVC-backed BaseModel must not get preferred node affinity")
	}
}

func TestUpdateEnvVariables_PVC_SetsModelPath(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	b := newPVCBaseComponent("pvc://my-pvc/models/llama")

	container := &v1.Container{Name: "main"}
	UpdateEnvVariables(b, &v1beta1.InferenceService{}, container, &metav1.ObjectMeta{})

	var got string
	for _, e := range container.Env {
		if e.Name == constants.ModelPathEnvVarKey {
			got = e.Value
		}
	}
	g.Expect(got).To(gomega.Equal(constants.ModelDefaultMountPath))
}
