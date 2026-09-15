package transport

import (
	"bytes"
	"context"
	"encoding/json"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
)

// PatchInferenceReplicaScale sends exactly one guarded JSON Patch to /scale.
// It never falls back to PUT or to mutating the parent resource. Identity and
// count binding against the inspected target remain the action plan's job.
func (c *Client) PatchInferenceReplicaScale(ctx context.Context, target Resource, patch []byte, opts JSONPatchOptions) (*autoscalingv1.Scale, error) {
	raw, err := c.jsonPatch(ctx, target, "scale", patch, opts)
	if err != nil {
		return nil, err
	}
	return decodeScaleResponse(raw)
}

func decodeScaleResponse(raw []byte) (*autoscalingv1.Scale, error) {
	if !unambiguousResponseIdentity(raw) ||
		!identityObject(raw, []string{"apiVersion", "kind", "metadata", "spec", "status"}, true) {
		return nil, ErrResponseIdentity
	}
	var envelope map[string]json.RawMessage
	if json.Unmarshal(raw, &envelope) != nil ||
		!identityObject(envelope["spec"], []string{"replicas"}, false) || !explicitScaleCount(envelope["spec"]) {
		return nil, ErrResponseIdentity
	}
	if !identityObject(envelope["status"], []string{"replicas", "selector"}, false) || !explicitScaleCount(envelope["status"]) {
		return nil, ErrResponseIdentity
	}
	var scale autoscalingv1.Scale
	if json.Unmarshal(raw, &scale) != nil || scale.APIVersion != "autoscaling/v1" || scale.Kind != "Scale" {
		return nil, ErrResponseIdentity
	}
	return &scale, nil
}

func explicitScaleCount(raw []byte) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return false
	}
	value, found := fields["replicas"]
	if !found || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
		return false
	}
	var count int32
	return json.Unmarshal(value, &count) == nil && count >= 0
}
