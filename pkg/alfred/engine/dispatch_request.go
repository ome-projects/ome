package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/alfred/policy"
	v1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

const migrationRequestPrefix = "ome.io/migration-request-v1-"

// migrationRequest is the existing public v1 wire API. Keeping the wire
// boundary here avoids linking Alfred to the controller implementation.
type migrationRequest struct {
	SchemaVersion   string   `json:"schemaVersion"`
	Component       string   `json:"component"`
	Instance        int32    `json:"instance"`
	FromNode        string   `json:"from_node"`
	HintTargetNodes []string `json:"hint_target_nodes,omitempty"`
	Reason          string   `json:"reason,omitempty"`
	RequestedAt     string   `json:"requested_at,omitempty"`
	RequestedBy     string   `json:"requested_by,omitempty"`
}

func newDispatchEntry(c policy.Candidate, owner types.UID, irName string, irUID types.UID, fingerprint string, now time.Time) (dispatchEntry, error) {
	req := migrationRequest{SchemaVersion: "v1", Component: string(c.Component), Instance: c.Instance, FromNode: c.FromNode,
		HintTargetNodes: append([]string(nil), c.HintTargetNodes...), Reason: c.Reason, RequestedAt: now.UTC().Format(time.RFC3339Nano), RequestedBy: "alfred"}
	if err := req.validate(); err != nil {
		return dispatchEntry{}, err
	}
	raw, err := json.Marshal(req)
	if err != nil || len(raw) > 4096 {
		return dispatchEntry{}, fmt.Errorf("invalid bounded migration payload")
	}
	return dispatchEntry{UUID: uuid.NewString(), Workload: c.Workload, WorkloadUID: owner, IRName: irName, IRUID: irUID,
		Component: c.Component, Instance: c.Instance, FromNode: c.FromNode, Payload: string(raw), CreatedAt: now, Phase: dispatchPrepared, SourceFingerprint: fingerprint}, nil
}

func (r migrationRequest) validate() error {
	if r.SchemaVersion != "v1" || r.Instance < 0 || len(validation.IsDNS1123Subdomain(r.FromNode)) != 0 {
		return fmt.Errorf("invalid migration source")
	}
	switch v1beta1.ComponentType(r.Component) {
	case v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent:
	default:
		return fmt.Errorf("unsupported migration component")
	}
	if len(r.HintTargetNodes) > 8 {
		return fmt.Errorf("too many migration hints")
	}
	seen := map[string]bool{}
	for _, hint := range r.HintTargetNodes {
		if hint == r.FromNode || seen[hint] || len(validation.IsDNS1123Subdomain(hint)) != 0 {
			return fmt.Errorf("invalid migration hint")
		}
		seen[hint] = true
	}
	return nil
}

func requestForEntry(e dispatchEntry) (migrationRequest, error) {
	var r migrationRequest
	decoder := json.NewDecoder(strings.NewReader(e.Payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&r); err != nil {
		return r, err
	}
	if err := r.validate(); err != nil {
		return r, err
	}
	if r.Component != string(e.Component) || r.Instance != e.Instance || r.FromNode != e.FromNode || r.RequestedBy != "alfred" || r.RequestedAt != e.CreatedAt.UTC().Format(time.RFC3339Nano) {
		return r, fmt.Errorf("journal payload identity mismatch")
	}
	return r, nil
}

// submitDispatch never updates the object wholesale. Both UID and resource
// version are tested atomically by the API server before adding one key.
func submitDispatch(ctx context.Context, writer client.Client, owner *v1beta1.InferenceService, e dispatchEntry) error {
	if owner.UID == "" || owner.UID != e.WorkloadUID || owner.ResourceVersion == "" || client.ObjectKeyFromObject(owner) != e.Workload {
		return fmt.Errorf("source owner identity mismatch")
	}
	if _, err := requestForEntry(e); err != nil {
		return err
	}
	key := migrationRequestPrefix + e.UUID
	if existing, ok := owner.Annotations[key]; ok {
		if existing == e.Payload {
			return nil
		}
		return fmt.Errorf("migration UUID already has a different payload")
	}
	ops := []map[string]any{{"op": "test", "path": "/metadata/uid", "value": string(owner.UID)}, {"op": "test", "path": "/metadata/resourceVersion", "value": owner.ResourceVersion}}
	if owner.Annotations == nil {
		ops = append(ops, map[string]any{"op": "add", "path": "/metadata/annotations", "value": map[string]string{}})
	}
	path := strings.ReplaceAll(strings.ReplaceAll(key, "~", "~0"), "/", "~1")
	ops = append(ops, map[string]any{"op": "add", "path": "/metadata/annotations/" + path, "value": e.Payload})
	raw, err := json.Marshal(ops)
	if err != nil {
		return err
	}
	return writer.Patch(ctx, owner.DeepCopy(), client.RawPatch(types.JSONPatchType, raw))
}
