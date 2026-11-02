package runtimerevision

import (
	"context"
	"testing"

	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

const omeNS = "ome"

func newClient(t *testing.T, initial ...client.Object) client.Client {
	t.Helper()
	return fakeclient.New(t, fakeclient.WithObjects(initial...))
}

func minimalSpec() *v1beta1.ServingRuntimeSpec {
	return &v1beta1.ServingRuntimeSpec{
		SupportedModelFormats: []v1beta1.SupportedModelFormat{{
			ModelFormat:    &v1beta1.ModelFormat{Name: "safetensors"},
			ModelFramework: &v1beta1.ModelFrameworkSpec{Name: "transformers"},
		}},
	}
}

func TestFindOrCreate_CreatesWhenAbsent(t *testing.T) {
	g := gomega.NewWithT(t)
	c := newClient(t)
	name, err := FindOrCreate(context.Background(), c, omeNS,
		KindClusterServingRuntime, "", "srt-llama-pd", minimalSpec())
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(name).To(gomega.HavePrefix("cr-srt-llama-pd-"))

	var list appsv1.ControllerRevisionList
	g.Expect(c.List(context.Background(), &list, client.InNamespace(omeNS))).To(gomega.Succeed())
	g.Expect(list.Items).To(gomega.HaveLen(1))

	got := &list.Items[0]
	g.Expect(got.Labels).To(gomega.HaveKeyWithValue(constants.RuntimeRevisionOfLabelKey, "srt-llama-pd"))
	g.Expect(got.Labels).To(gomega.HaveKeyWithValue(constants.RuntimeRevisionOfKindLabelKey, "ClusterServingRuntime"))
	g.Expect(got.Annotations).To(gomega.HaveKeyWithValue(constants.RuntimeRevisionCreatedByKey, "ome-controller"))
	g.Expect(got.Data.Raw).NotTo(gomega.BeEmpty())
}

func TestFindOrCreate_DedupsByHash(t *testing.T) {
	g := gomega.NewWithT(t)
	c := newClient(t)
	first, err := FindOrCreate(context.Background(), c, omeNS,
		KindClusterServingRuntime, "", "srt-llama-pd", minimalSpec())
	g.Expect(err).NotTo(gomega.HaveOccurred())
	second, err := FindOrCreate(context.Background(), c, omeNS,
		KindClusterServingRuntime, "", "srt-llama-pd", minimalSpec())
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(second).To(gomega.Equal(first), "same content must reuse existing revision")

	var list appsv1.ControllerRevisionList
	g.Expect(c.List(context.Background(), &list, client.InNamespace(omeNS))).To(gomega.Succeed())
	g.Expect(list.Items).To(gomega.HaveLen(1))
}

func TestFindOrCreate_DifferentContentCreatesNew(t *testing.T) {
	g := gomega.NewWithT(t)
	c := newClient(t)
	specA := minimalSpec()
	specB := minimalSpec()
	specB.Disabled = ptr.To(true)

	a, _ := FindOrCreate(context.Background(), c, omeNS, KindClusterServingRuntime, "", "srt", specA)
	b, _ := FindOrCreate(context.Background(), c, omeNS, KindClusterServingRuntime, "", "srt", specB)
	g.Expect(a).NotTo(gomega.Equal(b))

	var list appsv1.ControllerRevisionList
	g.Expect(c.List(context.Background(), &list, client.InNamespace(omeNS))).To(gomega.Succeed())
	g.Expect(list.Items).To(gomega.HaveLen(2))
}

func TestFetch_RoundTripsSpec(t *testing.T) {
	g := gomega.NewWithT(t)
	c := newClient(t)
	original := minimalSpec()
	name, err := FindOrCreate(context.Background(), c, omeNS,
		KindClusterServingRuntime, "", "srt-llama-pd", original)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	got, err := Fetch(context.Background(), c, omeNS, name)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(got.SupportedModelFormats).To(gomega.HaveLen(1))
	g.Expect(got.SupportedModelFormats[0].ModelFormat.Name).To(gomega.Equal("safetensors"))
}

func TestFetch_NotFoundSurfacesIsNotFound(t *testing.T) {
	g := gomega.NewWithT(t)
	c := newClient(t)
	_, err := Fetch(context.Background(), c, omeNS, "missing")
	g.Expect(err).To(gomega.HaveOccurred())
	g.Expect(apierrors.IsNotFound(err)).To(gomega.BeTrue())
}
