package inferenceservice

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestModelArtifactEventsIgnoreStatusAndUsage(t *testing.T) {
	old := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "models", UID: "uid", Annotations: map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "old"}}}
	p := modelArtifactChangePredicate()
	require.True(t, p.Create(event.CreateEvent{Object: old}))
	require.True(t, p.Delete(event.DeleteEvent{Object: old}))
	for _, change := range []string{"request", "uid", "usage", "status", "unchanged"} {
		t.Run(change, func(t *testing.T) {
			current := old.DeepCopy()
			switch change {
			case "request":
				current.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "new"
			case "uid":
				current.UID = "new-uid"
			case "usage":
				current.Annotations["ome.io/model-usage"] = "new-usage"
			case "status":
				current.Status.State = v1beta1.LifeCycleStateReady
			}
			require.Equal(t, change == "request" || change == "uid", p.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: current}))
		})
	}
}

func TestModelEventsMatchResolvedPrimaryScope(t *testing.T) {
	service := func(ns, name string, kind *string) *v1beta1.InferenceService {
		return &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}, Spec: v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{Name: "model", Kind: kind}}}
	}
	local := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "models"}}
	global := &v1beta1.ClusterBaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model"}}
	invalid := service("models", "unsupported-group", ptr.To("BaseModel"))
	invalid.Spec.Model.APIGroup = ptr.To("other.io")
	objects := []client.Object{local, global, service("models", "explicit-local", ptr.To("BaseModel")), service("other", "other-local", ptr.To("BaseModel")), service("models", "explicit-global", ptr.To("ClusterBaseModel")), service("other", "other-global", ptr.To("ClusterBaseModel")), service("models", "legacy-local", nil), service("other", "legacy-global", ptr.To("")), invalid, service("models", "unsupported-kind", ptr.To("Unknown"))}
	c := runtimeWatchClient(t, objects...)
	r := &InferenceServiceReconciler{Client: c, Log: logr.Discard()}
	require.Equal(t, []string{"models/explicit-local", "models/legacy-local"}, reqNames(r.isvcsReferencingModel(context.Background(), local)))
	require.Equal(t, []string{"models/explicit-global", "other/legacy-global", "other/other-global"}, reqNames(r.isvcsReferencingModel(context.Background(), global)))
	// A new namespaced Model takes over the legacy cluster fallback.
	appearing := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "other"}}
	require.Equal(t, []string{"other/legacy-global", "other/other-local"}, reqNames(r.isvcsReferencingModel(context.Background(), appearing)))
	// When the namespaced Model disappears, its legacy references must reconcile
	// again and become eligible for later cluster Model events.
	require.NoError(t, c.Delete(context.Background(), local))
	require.Equal(t, []string{"models/explicit-local", "models/legacy-local"}, reqNames(r.isvcsReferencingModel(context.Background(), local)))
	require.Contains(t, reqNames(r.isvcsReferencingModel(context.Background(), global)), "models/legacy-local")
}
