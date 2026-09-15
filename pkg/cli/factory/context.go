package factory

import "errors"

// ContextResolver is an optional action-only capability. Existing read commands
// and the Factory interface remain unchanged.
type ContextResolver interface{ ContextName() (string, error) }

func (f *defaultFactory) ContextName() (string, error) {
	if f == nil || f.flags == nil {
		return "", errors.New("selected kubeconfig context is unavailable")
	}
	if f.flags.Context != nil && *f.flags.Context != "" {
		return *f.flags.Context, nil
	}
	config, err := f.flags.ToRawKubeConfigLoader().RawConfig()
	if err != nil || config.CurrentContext == "" {
		return "", errors.New("selected kubeconfig context is unavailable")
	}
	return config.CurrentContext, nil
}
