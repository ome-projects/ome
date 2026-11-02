package testing

import (
	"path/filepath"

	"google.golang.org/protobuf/proto"
	istioclientv1beta1 "istio.io/client-go/pkg/apis/networking/v1beta1"
	netv1 "k8s.io/api/networking/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var log = logf.Log.WithName("TestingEnvSetup")

func SetupEnvTest() *envtest.Environment {
	t := &envtest.Environment{
		// Paths are relative to the calling test package, one entry per depth a caller sits at.
		CRDDirectoryPaths: []string{
			filepath.Join("..", "..", "..", "..", "..", "..", "config", "crd", "ome.io_trainedmodels.yaml"),
			filepath.Join("..", "..", "..", "..", "..", "..", "test", "crds"),
			filepath.Join("..", "..", "..", "..", "config", "crd", "ome.io_trainedmodels.yaml"),
			filepath.Join("..", "..", "..", "..", "test", "crds"),
		},
		UseExistingCluster: proto.Bool(false),
	}
	EtcdForBusyHosts(t)

	if err := netv1.SchemeBuilder.AddToScheme(scheme.Scheme); err != nil {
		log.Error(err, "Failed to add networking v1 scheme")
	}

	if err := istioclientv1beta1.SchemeBuilder.AddToScheme(scheme.Scheme); err != nil {
		log.Error(err, "Failed to add istio scheme")
	}
	return t
}

// EtcdForBusyHosts widens the raft timing of the envtest etcd. etcd fails a
// write with "etcdserver: request timed out" when a proposal is not committed
// within five seconds plus twice the election timeout, and on a shared host
// several test control planes starting at once starve one another of CPU for
// longer than the default one-second window. A single-member cluster still
// elects itself immediately because etcd advances the initial election tick.
func EtcdForBusyHosts(env *envtest.Environment) *envtest.Environment {
	if env.ControlPlane.Etcd == nil {
		env.ControlPlane.Etcd = &envtest.Etcd{}
	}
	args := env.ControlPlane.Etcd.Configure()
	args.Append("heartbeat-interval", "500")
	args.Append("election-timeout", "5000")
	return env
}
