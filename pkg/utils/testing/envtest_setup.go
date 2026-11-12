package testing

import (
	"fmt"
	"path/filepath"
	"strings"
	gotesting "testing"
	"time"

	"google.golang.org/protobuf/proto"
	istioclientv1beta1 "istio.io/client-go/pkg/apis/networking/v1beta1"
	netv1 "k8s.io/api/networking/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var log = logf.Log.WithName("TestingEnvSetup")

// Waits for an envtest control plane on a shared host. envtest gives the
// apiserver and etcd twenty seconds to turn healthy, a CRD ten seconds to
// appear in discovery and a shutdown twenty seconds. A host running
// several control planes at once has taken more than eighteen seconds for
// one CRD and a little longer than twenty for etcd to shut down. A control
// plane that never comes up still fails, two minutes later.
const (
	busyHostStartTimeout = 2 * time.Minute
	busyHostCRDWait      = 2 * time.Minute
	busyHostStopTimeout  = 90 * time.Second
)

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
	ForBusyHosts(t)

	if err := netv1.SchemeBuilder.AddToScheme(scheme.Scheme); err != nil {
		log.Error(err, "Failed to add networking v1 scheme")
	}

	if err := istioclientv1beta1.SchemeBuilder.AddToScheme(scheme.Scheme); err != nil {
		log.Error(err, "Failed to add istio scheme")
	}
	return t
}

// ForBusyHosts tunes an envtest control plane for a shared host. etcd
// fails a write with "etcdserver: request timed out" when a proposal is
// not committed within five seconds plus twice the election timeout, and
// several control planes starting at once starve one another of CPU for
// longer than the default one-second window, so the raft timing is
// widened; a single-member cluster still elects itself immediately
// because etcd advances the initial election tick. The start, CRD and
// stop waits the caller left unset are raised to the busy-host values.
func ForBusyHosts(env *envtest.Environment) *envtest.Environment {
	if env.ControlPlane.Etcd == nil {
		env.ControlPlane.Etcd = &envtest.Etcd{}
	}
	args := env.ControlPlane.Etcd.Configure()
	args.Append("heartbeat-interval", "500")
	args.Append("election-timeout", "5000")
	if env.ControlPlaneStartTimeout == 0 {
		env.ControlPlaneStartTimeout = busyHostStartTimeout
	}
	if env.ControlPlaneStopTimeout == 0 {
		env.ControlPlaneStopTimeout = busyHostStopTimeout
	}
	if env.CRDInstallOptions.MaxTime == 0 {
		env.CRDInstallOptions.MaxTime = busyHostCRDWait
	}
	return env
}

// CRDInstallForBusyHosts returns install options for crds on a running
// control plane, waiting the busy-host time for them to be served.
func CRDInstallForBusyHosts(crds ...*apiextensionsv1.CustomResourceDefinition) envtest.CRDInstallOptions {
	return envtest.CRDInstallOptions{CRDs: crds, MaxTime: busyHostCRDWait}
}

// StartOnBusyHost starts the control plane newEnv builds, tuned by
// ForBusyHosts, and returns it with its client config; Stop runs when t
// ends. envtest polls for the default namespace for five seconds once the
// apiserver is healthy and exposes no knob for that wait, while the
// apiserver creates the namespace on start and retries only once a
// minute, so a start that missed the window is stopped and a fresh
// control plane is built once in its place. Any other start error fails
// t, with the half-started control plane stopped.
func StartOnBusyHost(t gotesting.TB, newEnv func() *envtest.Environment) (*envtest.Environment, *rest.Config) {
	t.Helper()
	env, cfg, err := startOrRebuild(
		func() *envtest.Environment { return ForBusyHosts(newEnv()) },
		(*envtest.Environment).Start,
		(*envtest.Environment).Stop,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Error(err)
		}
	})
	return env, cfg
}

// defaultNamespaceWindowMissed reports the start error envtest returns
// when the default namespace was not visible within its five-second poll.
func defaultNamespaceWindowMissed(err error) bool {
	return err != nil && strings.Contains(err.Error(), "default namespace didn't register within deadline")
}

// startOrRebuild starts a fresh control plane, stops it when the start
// fails, and builds one more only when the first missed the
// default-namespace window.
func startOrRebuild(
	newEnv func() *envtest.Environment,
	start func(*envtest.Environment) (*rest.Config, error),
	stop func(*envtest.Environment) error,
) (*envtest.Environment, *rest.Config, error) {
	for attempt := 1; ; attempt++ {
		env := newEnv()
		cfg, err := start(env)
		if err == nil {
			return env, cfg, nil
		}
		if stopErr := stop(env); stopErr != nil {
			err = fmt.Errorf("%w; stopping the control plane: %v", err, stopErr)
		}
		if attempt > 1 || !defaultNamespaceWindowMissed(err) {
			return nil, nil, err
		}
	}
}
