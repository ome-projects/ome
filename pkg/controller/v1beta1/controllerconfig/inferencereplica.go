package controllerconfig

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	authenticationv1 "k8s.io/api/authentication/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

// InferenceReplicaConfigName is the inferenceservice-config ConfigMap key
// holding the InferenceReplica admission block.
const InferenceReplicaConfigName = "inferenceReplica"

// +kubebuilder:object:generate=false
// InferenceReplicaConfig is the operator configuration the InferenceReplica
// admission webhook reads. An absent block, or one without
// controllerIdentity, leaves the controller identity unconfigured.
type InferenceReplicaConfig struct {
	// ControllerIdentity names the Kubernetes identity the InferenceService
	// controller writes with. The webhook admits spec writes on a projected
	// replica only from this identity.
	ControllerIdentity *ControllerIdentityConfig `json:"controllerIdentity,omitempty"`
}

// Identity returns the controller identity, or nil when the config or its
// controllerIdentity is absent. Configured and Matches treat nil as
// unconfigured, so callers chain them without a nil check.
func (c *InferenceReplicaConfig) Identity() *ControllerIdentityConfig {
	if c == nil {
		return nil
	}
	return c.ControllerIdentity
}

// +kubebuilder:object:generate=false
// ControllerIdentityConfig matches an admission request's user against the
// controller: a request matches when its username is listed or any of its
// groups is listed.
type ControllerIdentityConfig struct {
	// Usernames are exact usernames, such as
	// system:serviceaccount:<namespace>:<serviceaccount>.
	Usernames []string `json:"usernames,omitempty"`
	// Groups are group names, any one of which identifies the controller.
	Groups []string `json:"groups,omitempty"`
}

// Configured reports whether at least one username or group is listed.
func (c *ControllerIdentityConfig) Configured() bool {
	return c != nil && (len(c.Usernames) > 0 || len(c.Groups) > 0)
}

// Matches reports whether user is the controller identity. An unconfigured
// identity matches nobody.
func (c *ControllerIdentityConfig) Matches(user authenticationv1.UserInfo) bool {
	if !c.Configured() {
		return false
	}
	if slices.Contains(c.Usernames, user.Username) {
		return true
	}
	for _, group := range user.Groups {
		if slices.Contains(c.Groups, group) {
			return true
		}
	}
	return false
}

// NewInferenceReplicaConfigCached loads the block through the shared
// ConfigCache (a nil cache reads the apiserver directly). An absent or blank
// key yields (nil, nil).
func NewInferenceReplicaConfigCached(cache *ConfigCache, clientset kubernetes.Interface) (*InferenceReplicaConfig, error) {
	configMap, err := cache.get(clientset)
	if err != nil {
		return nil, err
	}
	return parseInferenceReplicaConfig(configMap)
}

// The block's JSON keys, which the strict key walk matches exactly.
const (
	controllerIdentityKey = "controllerIdentity"
	usernamesKey          = "usernames"
	groupsKey             = "groups"
)

// Kubernetes authentication gives these identities to a whole class of
// callers (every anonymous request, every authenticated or unauthenticated
// caller, every service account), so none of them can single out the
// controller.
var (
	everyCallerUsernames = []string{"system:anonymous"}
	everyCallerGroups    = []string{"system:authenticated", "system:unauthenticated", "system:serviceaccounts"}
)

// parseInferenceReplicaConfig decodes the block strictly. null or a
// non-object, an unknown or duplicate key at either level, content after the
// object, a blank or whitespace-padded entry and an identity a whole class of
// callers holds are errors, so a malformed block never loads as an identity
// other than the one written.
func parseInferenceReplicaConfig(configMap *v1.ConfigMap) (*InferenceReplicaConfig, error) {
	raw, ok := configMap.Data[InferenceReplicaConfigName]
	if !ok || strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	cfg, err := decodeInferenceReplicaConfig([]byte(raw))
	if err != nil {
		return nil, fmt.Errorf("invalid %s config: %w", InferenceReplicaConfigName, err)
	}
	return cfg, nil
}

func decodeInferenceReplicaConfig(raw []byte) (*InferenceReplicaConfig, error) {
	fields, err := jsonObjectFields(raw, controllerIdentityKey)
	if err != nil {
		return nil, err
	}
	if identity, ok := fields[controllerIdentityKey]; ok {
		if _, err := jsonObjectFields(identity, usernamesKey, groupsKey); err != nil {
			return nil, fmt.Errorf("%s: %w", controllerIdentityKey, err)
		}
	}
	// The walk admitted only exact keys, so encoding/json's case-insensitive
	// key matching has nothing left to match loosely.
	cfg := &InferenceReplicaConfig{}
	if err := json.Unmarshal(raw, cfg); err != nil {
		return nil, err
	}
	if err := cfg.ControllerIdentity.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// validate rejects a blank or whitespace-padded entry, which marks a rendering
// mistake rather than a real identity, and an identity a whole class of
// callers holds.
func (c *ControllerIdentityConfig) validate() error {
	if c == nil {
		return nil
	}
	for i, username := range c.Usernames {
		if strings.TrimSpace(username) == "" {
			return fmt.Errorf("%s.%s[%d] is blank", controllerIdentityKey, usernamesKey, i)
		}
		if strings.TrimSpace(username) != username {
			return fmt.Errorf("%s.%s[%d] has surrounding whitespace", controllerIdentityKey, usernamesKey, i)
		}
		if slices.Contains(everyCallerUsernames, username) {
			return fmt.Errorf("username %q names every caller and cannot identify the controller", username)
		}
	}
	for i, group := range c.Groups {
		if strings.TrimSpace(group) == "" {
			return fmt.Errorf("%s.%s[%d] is blank", controllerIdentityKey, groupsKey, i)
		}
		if strings.TrimSpace(group) != group {
			return fmt.Errorf("%s.%s[%d] has surrounding whitespace", controllerIdentityKey, groupsKey, i)
		}
		if slices.Contains(everyCallerGroups, group) {
			return fmt.Errorf("group %q names every caller and cannot identify the controller", group)
		}
	}
	return nil
}

// jsonObjectFields reads data as exactly one JSON object and returns its raw
// field values by key. null, a non-object, a key outside allowed, a repeated
// key and content after the object are errors; encoding/json alone would
// match keys case-insensitively and keep the last of repeated keys.
func jsonObjectFields(data []byte, allowed ...string) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := expectDelim(decoder, '{'); err != nil {
		return nil, err
	}
	fields := make(map[string]json.RawMessage, len(allowed))
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("malformed JSON object: %w", err)
		}
		key, ok := token.(string)
		if !ok {
			return nil, fmt.Errorf("malformed JSON object: expected a field name, got %v", token)
		}
		if !slices.Contains(allowed, key) {
			return nil, fmt.Errorf("unknown field %q", key)
		}
		if _, repeated := fields[key]; repeated {
			return nil, fmt.Errorf("duplicate key %q", key)
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, fmt.Errorf("malformed JSON object: %w", err)
		}
		fields[key] = value
	}
	if err := expectDelim(decoder, '}'); err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("trailing data after the JSON object")
	}
	return fields, nil
}
