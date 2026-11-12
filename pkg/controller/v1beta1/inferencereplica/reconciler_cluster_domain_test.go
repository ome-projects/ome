package inferencereplica

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
)

// peerEndpointUnder renders an engine pod of a prefill/decode service through
// the engine deps a pass builds against the given operator ConfigMap data,
// and returns the decoder address the pod was given.
func peerEndpointUnder(t *testing.T, data map[string]string) string {
	t.Helper()
	r, _ := newReconciler(t)
	r.Clientset = kubefake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: constants.InferenceServiceConfigMapName, Namespace: constants.OMENamespace},
		Data:       data,
	})
	r.ConfigCache = controllerconfig.NewConfigCache(0)
	pass := &replicaPass{reconciler: r, ir: &v1beta1.InferenceReplica{}, parent: coordinatedISVC(), log: logr.Discard()}
	ok, err := pass.buildEngineDeps(context.Background())
	if !ok || err != nil {
		t.Fatalf("buildEngineDeps: ok=%v err=%v", ok, err)
	}
	pod := renderedPod(v1beta1.EngineComponent)
	pass.deps.RenderHook(pod, "runner-0", 0, "eng123")
	got, _ := envValue(pod.Spec.Containers[0], "OME_DECODER_ENDPOINT")
	return got
}

// The decoder address a prefill pod is given ends with the cluster domain the
// ingress block of inferenceservice-config names, so a cluster whose DNS
// domain is not cluster.local still pairs its prefill and decode pods.
func TestBuildEngineDeps_PeerAddressesFollowTheConfiguredClusterDomain(t *testing.T) {
	got := peerEndpointUnder(t, map[string]string{
		controllerconfig.IngressConfigKeyName: `{"ingressGateway":"istio-system/gateway","ingressService":"gateway.istio-system","clusterDomain":"example.org"}`,
	})
	if got != "llama-decoder.prod.svc.example.org" {
		t.Fatalf("OME_DECODER_ENDPOINT = %q, want llama-decoder.prod.svc.example.org", got)
	}
}

// Without an ingress block, or with one that names no cluster domain, the
// address is the namespaced short name; no domain suffix is written into the
// binary.
func TestBuildEngineDeps_NoClusterDomainRendersTheShortPeerName(t *testing.T) {
	for name, data := range map[string]map[string]string{
		"no ingress block":  {},
		"no cluster domain": {controllerconfig.IngressConfigKeyName: `{"ingressGateway":"istio-system/gateway","ingressService":"gateway.istio-system"}`},
	} {
		if got := peerEndpointUnder(t, data); got != "llama-decoder.prod" {
			t.Errorf("%s: OME_DECODER_ENDPOINT = %q, want llama-decoder.prod", name, got)
		}
	}
}
