// Package factory hands commands their API clients. Commands depend on the
// Factory interface only, so tests substitute Static and the real
// implementation stays in one place.
package factory

import (
	"sync"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/client/clientset/versioned"
)

type Factory interface {
	// RESTConfig returns the resolved client-go REST config.
	RESTConfig() (*rest.Config, error)
	// KubeClient returns a core Kubernetes clientset (pods, events, logs).
	KubeClient() (kubernetes.Interface, error)
	// OMEClient returns the generated OME typed clientset.
	OMEClient() (versioned.Interface, error)
	// RuntimeClient returns a lazily constructed controller-runtime client
	// (scheme: client-go core + ome.io/v1beta1) for commands that reuse
	// operator libraries such as pkg/runtimeselector.
	RuntimeClient() (ctrlclient.Client, error)
	// Namespace returns the effective namespace and whether the user set it
	// explicitly (flag or kubeconfig context override).
	Namespace() (string, bool, error)
}

func New(flags *genericclioptions.ConfigFlags) Factory {
	return &defaultFactory{flags: flags}
}

type defaultFactory struct {
	flags *genericclioptions.ConfigFlags

	mu      sync.Mutex
	rest    *rest.Config
	kube    kubernetes.Interface
	ome     versioned.Interface
	runtime ctrlclient.Client
}

func (f *defaultFactory) RESTConfig() (*rest.Config, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.rest != nil {
		return f.rest, nil
	}
	cfg, err := f.flags.ToRESTConfig()
	if err != nil {
		return nil, err
	}
	// CLI bursts many small reads (status fans out to pods/events, one
	// field-selector query per involved object); match kubectl's own
	// client-side rate limits (QPS 50 / Burst 300) so those fan-out bursts
	// don't stall on client-side throttling before a request even reaches
	// the API server.
	cfg.QPS = 50
	cfg.Burst = 300
	f.rest = cfg
	return cfg, nil
}

// protobufConfig returns a COPY of cfg negotiating protobuf the way kubectl
// itself does for core types (pods, events, deployments, ...): smaller,
// faster-to-decode responses than JSON. It never mutates cfg -- KubeClient
// is the only caller, and cfg is the same *rest.Config shared with
// OMEClient/RuntimeClient, which must keep negotiating JSON. The OME CRD
// clientset and the controller-runtime client intentionally do NOT get this
// treatment: CRDs do not serve protobuf (only Kubernetes' built-in types
// do), so a protobuf-negotiating client against a CRD would just add a
// failed content-type negotiation round trip to every request.
func protobufConfig(cfg *rest.Config) *rest.Config {
	cp := rest.CopyConfig(cfg)
	cp.AcceptContentTypes = "application/vnd.kubernetes.protobuf,application/json"
	cp.ContentType = "application/vnd.kubernetes.protobuf"
	return cp
}

func (f *defaultFactory) KubeClient() (kubernetes.Interface, error) {
	f.mu.Lock()
	if f.kube != nil {
		defer f.mu.Unlock()
		return f.kube, nil
	}
	f.mu.Unlock()
	cfg, err := f.RESTConfig()
	if err != nil {
		return nil, err
	}
	// Core-type traffic (pods/events/...) gets protobuf, kubectl-style; see
	// protobufConfig's doc comment for why this must be a copy.
	c, err := kubernetes.NewForConfig(protobufConfig(cfg))
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.kube = c
	return c, nil
}

func (f *defaultFactory) OMEClient() (versioned.Interface, error) {
	f.mu.Lock()
	if f.ome != nil {
		defer f.mu.Unlock()
		return f.ome, nil
	}
	f.mu.Unlock()
	// Deliberately uses the shared cfg as-is (default JSON content type),
	// unlike KubeClient(): CRDs do not serve protobuf, so the OME typed
	// clientset must keep negotiating JSON.
	cfg, err := f.RESTConfig()
	if err != nil {
		return nil, err
	}
	c, err := versioned.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ome = c
	return c, nil
}

func (f *defaultFactory) RuntimeClient() (ctrlclient.Client, error) {
	f.mu.Lock()
	if f.runtime != nil {
		defer f.mu.Unlock()
		return f.runtime, nil
	}
	f.mu.Unlock()
	// Same reasoning as OMEClient(): this reuses ome.io/v1beta1 CRD types,
	// so it must keep the shared cfg's default JSON content type too.
	cfg, err := f.RESTConfig()
	if err != nil {
		return nil, err
	}
	c, err := newRuntimeClient(cfg)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.runtime = c
	return c, nil
}

func newRuntimeClient(cfg *rest.Config) (ctrlclient.Client, error) {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1beta1.AddToScheme(scheme))
	return ctrlclient.New(cfg, ctrlclient.Options{Scheme: scheme})
}

func (f *defaultFactory) Namespace() (string, bool, error) {
	return f.flags.ToRawKubeConfigLoader().Namespace()
}
