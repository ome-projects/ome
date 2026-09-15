package factory

import (
	"errors"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/client/clientset/versioned"
)

// Static is a Factory for tests: fields are returned as-is. A nil client
// yields an error so tests fail loudly when a command grabs a client the
// test didn't provide.
type Static struct {
	Kube       kubernetes.Interface
	OME        versioned.Interface
	Runtime    ctrlclient.Client
	NS         string
	NSExplicit bool
	Context    string
}

func (s Static) RESTConfig() (*rest.Config, error) { return &rest.Config{}, nil }

func (s Static) KubeClient() (kubernetes.Interface, error) {
	if s.Kube == nil {
		return nil, errors.New("static factory: no kube client configured")
	}
	return s.Kube, nil
}

func (s Static) OMEClient() (versioned.Interface, error) {
	if s.OME == nil {
		return nil, errors.New("static factory: no OME client configured")
	}
	return s.OME, nil
}

func (s Static) RuntimeClient() (ctrlclient.Client, error) {
	if s.Runtime == nil {
		return nil, errors.New("static factory: no runtime client configured")
	}
	return s.Runtime, nil
}

func (s Static) Namespace() (string, bool, error) { return s.NS, s.NSExplicit, nil }

func (s Static) ContextName() (string, error) {
	if s.Context == "" {
		return "", errors.New("static factory: no context configured")
	}
	return s.Context, nil
}
