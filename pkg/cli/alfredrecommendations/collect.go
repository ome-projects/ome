// Package alfredrecommendations collects and projects Alfred's persisted,
// advisory recommendations without importing its policy or execution engines.
package alfredrecommendations

import (
	"context"
	"errors"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	"sigs.k8s.io/yaml"

	alfredconfig "sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/cli/namespace"
)

const (
	RecordKey      = "last-cycle.json"
	MaxConfigBytes = 64 << 10
	MaxRecordBytes = 256 << 10
	MaxScannedRows = 800
	MaxRows        = 200
	RequestTimeout = 10 * time.Second
)

// ConfigEvidence describes the selected ConfigMap, not Alfred's in-memory
// last-known-good configuration or whether the policy loop is running.
type ConfigEvidence struct {
	State      string
	Enabled    bool
	RecordName string
	Mode       string
	Interval   time.Duration
}

// Snapshot retains only the selected payload and safe collection evidence.
// It must be projected before rendering; raw payloads and API errors never
// enter the report schema.
type Snapshot struct {
	Namespace   string
	ConfigName  string
	ConfigKey   string
	Config      ConfigEvidence
	RecordState string
	Record      string
}

// ParseConfig uses Alfred's actual strict loader and defaults. Only the
// schema discriminator is read separately to distinguish unsupported versions.
func ParseConfig(raw string) ConfigEvidence {
	bad := ConfigEvidence{State: "Malformed"}
	if len(raw) > MaxConfigBytes {
		return ConfigEvidence{State: "Oversized"}
	}
	var schema struct {
		SchemaVersion *int `json:"schemaVersion"`
	}
	if err := yaml.Unmarshal([]byte(raw), &schema); err != nil {
		return bad
	}
	if schema.SchemaVersion == nil {
		return bad
	}
	if *schema.SchemaVersion != 1 {
		return ConfigEvidence{State: "UnsupportedSchema"}
	}
	cfg, err := alfredconfig.Load([]byte(raw))
	if err != nil {
		return bad
	}
	if len(validation.IsDNS1123Subdomain(cfg.RecommendationsConfigMapName)) != 0 {
		return bad
	}
	return ConfigEvidence{State: "Available", Enabled: *cfg.RecommendationsConfigMapEnabled,
		RecordName: cfg.RecommendationsConfigMapName, Mode: cfg.Mode, Interval: cfg.DecisionLoopInterval.Duration}
}

// Collect performs at most two named GETs in Alfred's namespace. Timeout is
// per request; parent cancellation terminates the command without raw errors.
func Collect(ctx context.Context, client coreclient.ConfigMapsGetter, ns namespace.Resolved, timeout time.Duration) (Snapshot, error) {
	result := Snapshot{Namespace: ns.AlfredNamespace, ConfigName: ns.AlfredConfigName, ConfigKey: ns.AlfredConfigKey, RecordState: "NotRead"}
	if timeout <= 0 || timeout > RequestTimeout {
		return result, errors.New("invalid recommendations request timeout")
	}
	if len(validation.IsDNS1123Label(ns.AlfredNamespace)) != 0 || len(validation.IsDNS1123Subdomain(ns.AlfredConfigName)) != 0 || len(validation.IsConfigMapKey(ns.AlfredConfigKey)) != 0 {
		return result, errors.New("invalid Alfred source selection")
	}
	if client == nil {
		return result, errors.New("Kubernetes client unavailable")
	}
	configMap, state := get(ctx, client, ns.AlfredNamespace, ns.AlfredConfigName, timeout)
	if ctx.Err() != nil {
		return result, ctx.Err()
	}
	result.Config.State = state
	if state != "Available" {
		return result, nil
	}
	raw, found := configMap.Data[ns.AlfredConfigKey]
	if !found {
		result.Config.State = "KeyAbsent"
		return result, nil
	}
	result.Config = ParseConfig(raw)
	if result.Config.State != "Available" {
		return result, nil
	}
	if !result.Config.Enabled {
		result.RecordState = "Disabled"
		return result, nil
	}
	cm, state := get(ctx, client, ns.AlfredNamespace, result.Config.RecordName, timeout)
	if ctx.Err() != nil {
		return result, ctx.Err()
	}
	result.RecordState = state
	if state != "Available" {
		return result, nil
	}
	raw, found = cm.Data[RecordKey]
	if !found {
		result.RecordState = "KeyAbsent"
		return result, nil
	}
	if len(raw) > MaxRecordBytes {
		result.RecordState = "Oversized"
		return result, nil
	}
	result.Record = raw
	return result, nil
}

func get(ctx context.Context, client coreclient.ConfigMapsGetter, ns, name string, timeout time.Duration) (*corev1.ConfigMap, string) {
	if ctx.Err() != nil {
		return nil, "Unreadable"
	}
	request, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cm, err := client.ConfigMaps(ns).Get(request, name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		return nil, "NotFound"
	case apierrors.IsForbidden(err):
		return nil, "Forbidden"
	case err != nil:
		return nil, "Unreadable"
	case cm == nil || cm.Namespace != ns || cm.Name != name:
		return nil, "IdentityMismatch"
	default:
		return cm, "Available"
	}
}
