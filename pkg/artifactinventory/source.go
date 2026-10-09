package artifactinventory

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// These read-only projections follow modelagent/model_data.go and
// modelagent/hf_artifact_data.go. Keeping only inventory fields avoids importing
// the model-agent's download dependencies. Unknown JSON fields are ignored.
type modelEntry struct {
	Name                      string           `json:"name"`
	Status                    string           `json:"status"`
	ModelUID                  types.UID        `json:"modelUID"`
	HfArtifactKey             string           `json:"hfArtifactKey"`
	HfArtifactPendingDeletion *json.RawMessage `json:"hfArtifactPendingDeletion"`
}

type sharedParent struct {
	Key       string            `json:"key"`
	Status    string            `json:"status"`
	LocalPath string            `json:"localPath"`
	Children  map[string]string `json:"children"`
}

type selectedModel struct {
	key   string
	entry modelEntry
	model *v1beta1.BaseModel
	issue *Issue
}

func (b *Builder) readConfigMap(ctx context.Context, nodeName string) (*corev1.ConfigMap, error) {
	cm := &corev1.ConfigMap{}
	key := client.ObjectKey{Namespace: b.namespace, Name: nodeName}
	if err := b.reader.Get(ctx, key, cm); err != nil {
		return nil, fmt.Errorf("read ConfigMap %s: %w", key, err)
	}
	if cm.UID == "" || cm.DeletionTimestamp != nil {
		return nil, fmt.Errorf("ConfigMap %s is deleting or has no UID", key)
	}
	return cm, nil
}

// readModels uses CM readiness rather than the asynchronously aggregated
// CR.status.nodesReady. Only the selected CRs are fetched; there is no List.
func (b *Builder) readModels(ctx context.Context, cm *corev1.ConfigMap) ([]selectedModel, error) {
	keys := make([]string, 0, len(cm.Data))
	for key := range cm.Data {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var selected []selectedModel
	for _, key := range keys {
		namespace, name, clusterScoped, ok := constants.ParseModelInfoFromConfigMapKey(key)
		if strings.HasPrefix(key, "artifact.huggingface.") || !ok || clusterScoped {
			continue
		}
		var entry modelEntry
		if err := json.Unmarshal([]byte(cm.Data[key]), &entry); err != nil {
			return nil, fmt.Errorf("decode ModelEntry %s: %w", key, err)
		}
		if entry.Status != "Ready" {
			continue
		}
		item := selectedModel{key: key, entry: entry}
		// Long CM keys can be truncated and hashed. Never GET a guessed name.
		if namespace == "" || name == "" || name != entry.Name || constants.GetModelConfigMapKey(namespace, name, false) != key {
			item.issue = &Issue{Code: "modelIdentityMismatch", Message: "Ready entry cannot be resolved to a BaseModel", ModelKey: key}
			selected = append(selected, item)
			continue
		}

		m := &v1beta1.BaseModel{}
		err := b.reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, m)
		if apierrors.IsNotFound(err) {
			item.issue = &Issue{Code: "modelNotFound", Message: "Ready entry has no current BaseModel", ModelKey: key}
			selected = append(selected, item)
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read BaseModel %s/%s: %w", namespace, name, err)
		}
		item.model = m
		if m.UID == "" || (entry.ModelUID != "" && entry.ModelUID != m.UID) {
			// Keep a diagnostic for the old association; a replacement CR is not
			// sufficient proof that the Ready entry belongs to it.
			item.issue = &Issue{Code: "modelUIDMismatch", Message: "BaseModel UID is missing or differs from the Ready entry", ModelKey: key}
		} else if m.Annotations["ome.io/artifact-residency"] == "Evicted" || strings.HasPrefix(storageURI(m.Spec.Storage), "vendor://") {
			continue
		}
		selected = append(selected, item)
	}
	return selected, nil
}
