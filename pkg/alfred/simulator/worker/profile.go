// Package worker runs a pinned scheduler against a closed, private snapshot.
package worker

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"runtime/debug"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	metavalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apiserver/pkg/util/feature"
	clientfeatures "k8s.io/client-go/features"
	config "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/scheme"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/validation"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins"
	"sigs.k8s.io/ome/alfred-simulator/protocol"
	"sigs.k8s.io/ome/scheduler/pkg/plugins/gangpack"
)

const SchedulerVersion = "v1.35.4"

// Profile exposes the operator mapping; evaluation uses an immutable private copy.
type Profile struct {
	Identity       protocol.ProfileIdentity
	GangScheduling bool
	identity       protocol.ProfileIdentity
	gang           bool
	config         *config.KubeSchedulerConfiguration
	gates          map[string]bool
}

func featureState() map[string]bool {
	m := map[string]bool{}
	for name := range feature.DefaultMutableFeatureGate.GetAll() {
		m[string(name)] = feature.DefaultFeatureGate.Enabled(name)
	}
	_ = clientfeatures.AddFeaturesToExistingFeatureGates(clientFeatureState(m))
	return m
}

type clientFeatureState map[string]bool

func (m clientFeatureState) Add(specs map[clientfeatures.Feature]clientfeatures.FeatureSpec) error {
	for name := range specs {
		m["client-go/"+string(name)] = clientfeatures.FeatureGates().Enabled(name)
	}
	return nil
}

// LoadProfile strictly decodes and defaults the actual upstream scheduler configuration.
// No kubeconfig, extenders, or additional plugin factories are accepted.
func LoadProfile(backend string, configYAML []byte) (*Profile, error) {
	if strings.TrimSpace(backend) == "" {
		return nil, fmt.Errorf("backend is required")
	}
	build := buildProvenance()
	if err := validateSchedulerBuild(build); err != nil {
		return nil, err
	}
	obj, gvk, err := scheme.Codecs.UniversalDecoder().Decode(configYAML, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("scheduler configuration: %w", err)
	}
	c, ok := obj.(*config.KubeSchedulerConfiguration)
	if !ok || gvk.GroupVersion().String() != "kubescheduler.config.k8s.io/v1" {
		return nil, fmt.Errorf("expected kubescheduler.config.k8s.io/v1 KubeSchedulerConfiguration")
	}
	c.APIVersion = gvk.GroupVersion().String()
	if err := validation.ValidateKubeSchedulerConfiguration(c); err != nil {
		return nil, err
	}
	if len(c.Profiles) != 1 {
		return nil, fmt.Errorf("exactly one scheduler profile is required")
	}
	if len(c.Extenders) != 0 || c.ClientConnection.Kubeconfig != "" {
		return nil, fmt.Errorf("external clients and extenders are unsupported")
	}
	registry := plugins.NewInTreeRegistry()
	pr := &c.Profiles[0]
	sets := reflect.ValueOf(pr.Plugins).Elem()
	for i := 0; i < sets.NumField(); i++ {
		s := sets.Field(i).Interface().(config.PluginSet)
		for _, plugin := range s.Enabled {
			if _, ok := registry[plugin.Name]; !ok && plugin.Name != gangpack.Name {
				return nil, fmt.Errorf("unregistered plugin %q", plugin.Name)
			}
			if plugin.Name == "GangScheduling" {
				return nil, fmt.Errorf("upstream PodGroup scheduling is unsupported; use OMEGangPack")
			}
		}
	}
	gang := hookEnabled(pr.Plugins.MultiPoint, gangpack.Name)
	hooks := []config.PluginSet{pr.Plugins.PreFilter, pr.Plugins.Filter, pr.Plugins.PostFilter, pr.Plugins.Reserve, pr.Plugins.Permit, pr.Plugins.PostBind}
	anyGang := gang
	allGang := true
	for _, hook := range hooks {
		enabled := gang && !hookDisabled(hook, gangpack.Name) || hookEnabled(hook, gangpack.Name)
		anyGang = anyGang || enabled
		allGang = allGang && enabled
	}
	if anyGang && !allGang {
		return nil, fmt.Errorf("OMEGangPack requires PreFilter, Filter, PostFilter, Reserve, Permit and PostBind")
	}
	gang = allGang
	hasGangArgs := false
	for i := range pr.PluginConfig {
		pc := &pr.PluginConfig[i]
		if _, ok := registry[pc.Name]; !ok && pc.Name != gangpack.Name {
			return nil, fmt.Errorf("unregistered plugin config %q", pc.Name)
		}
		if pc.Name == gangpack.Name {
			hasGangArgs = true
			u, ok := pc.Args.(*runtime.Unknown)
			if !ok {
				return nil, fmt.Errorf("invalid OMEGangPack args")
			}
			var a gangpack.Args
			decoder := json.NewDecoder(bytes.NewReader(u.Raw))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&a); err != nil {
				return nil, err
			}
			if a.GCIntervalSeconds == nil || *a.GCIntervalSeconds <= 0 {
				return nil, fmt.Errorf("OMEGangPack gcIntervalSeconds must be positive")
			}
			for name, value := range map[string]*int64{"gcIntervalSeconds": a.GCIntervalSeconds, "podGroupSyncTimeoutSeconds": a.PodGroupSyncTimeoutSeconds, "defaultPermitTimeoutSeconds": a.DefaultPermitTimeoutSeconds} {
				if value != nil && (*value <= 0 || *value > math.MaxInt64/int64(time.Second)) {
					return nil, fmt.Errorf("OMEGangPack %s must be a positive representable duration", name)
				}
			}
			for name, value := range map[string]string{"topologyKey": a.TopologyKey, "podGroupTopologyKeyAnnotation": a.PodGroupTopologyKeyAnnotation, "unsupportedPlacementGroupLabel": a.UnsupportedPlacementGroupLabel} {
				if value != "" && len(metavalidation.IsQualifiedName(value)) != 0 {
					return nil, fmt.Errorf("OMEGangPack %s must be a qualified metadata key", name)
				}
			}
			if a.StandaloneDomainPacking == nil {
				v := true
				a.StandaloneDomainPacking = &v
			}
			u.Raw, err = json.Marshal(a)
			if err != nil {
				return nil, err
			}
		}
	}
	if gang && !hasGangArgs {
		return nil, fmt.Errorf("OMEGangPack configuration is required")
	}
	gates := featureState()
	canonical, err := json.Marshal(struct {
		Version        string
		Config         *config.KubeSchedulerConfiguration
		Gates          map[string]bool
		WorkerContract string
		Build          *debug.BuildInfo
	}{SchedulerVersion, c, gates, "alfred-snapshot-v1", build})
	if err != nil {
		return nil, err
	}
	identity := protocol.ProfileIdentity{Backend: backend, SchedulerName: pr.SchedulerName, SchedulerVersion: SchedulerVersion, ConfigurationID: fmt.Sprintf("sha256:%x", sha256.Sum256(canonical))}
	return &Profile{Identity: identity, GangScheduling: gang, identity: identity, gang: gang, config: c.DeepCopy(), gates: gates}, nil
}

// Local module replacements do not carry a source checksum. Backend must also
// identify the operator's immutable worker artifact when provenance is absent.
func buildProvenance() *debug.BuildInfo {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return nil
	}
	return info
}

func validateSchedulerBuild(info *debug.BuildInfo) error {
	if info == nil {
		return nil
	}
	for _, module := range info.Deps {
		if module.Path != "k8s.io/kubernetes" {
			continue
		}
		actual := module
		if module.Replace != nil {
			actual = module.Replace
		}
		if actual.Path != "k8s.io/kubernetes" || actual.Version != SchedulerVersion {
			return fmt.Errorf("compiled scheduler dependency must be k8s.io/kubernetes %s", SchedulerVersion)
		}
	}
	// Test binaries can omit dependency metadata. The checked-in module pin and
	// immutable backend artifact remain the version authority in that case.
	return nil
}

func hookEnabled(s config.PluginSet, name string) bool {
	for _, p := range s.Enabled {
		if p.Name == name {
			return true
		}
	}
	return false
}
func hookDisabled(s config.PluginSet, name string) bool {
	for _, p := range s.Disabled {
		if p.Name == name || p.Name == "*" {
			return true
		}
	}
	return false
}
