package artifactinventory

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Builder reads fresh Kubernetes inputs for each node. Use an uncached Reader
// (for example manager.GetAPIReader()) to preserve the targeted GET behavior.
type Builder struct {
	reader    client.Reader
	namespace string
}

// NewBuilder uses the model-agent ConfigMap namespace, defaulting to ome.
// The caller configures request timeouts on the client and a deadline on ctx.
func NewBuilder(reader client.Reader, namespace string) *Builder {
	if namespace == "" {
		namespace = "ome"
	}
	return &Builder{reader: reader, namespace: namespace}
}

// Build returns artifact paths, model associations, and estimated sizes for
// Ready entries on nodeName. It never queries Node/Pod objects or writes data.
// A failure returns nil, not a partial inventory usable for automatic eviction.
func (b *Builder) Build(ctx context.Context, nodeName string) (*Inventory, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if b.reader == nil || nodeName == "" {
		return nil, fmt.Errorf("Kubernetes reader and node name are required")
	}

	// 1. Read one node CM and GET only the BaseModels selected by its Ready
	// entries. Parent records remain in this same CM snapshot for path lookup.
	cm, err := b.readConfigMap(ctx, nodeName)
	if err != nil {
		return nil, err
	}
	models, err := b.readModels(ctx, cm)
	if err != nil {
		return nil, err
	}

	// 2. Group by artifact path, not CR name or namespace. A bad association
	// blocks the whole group; it must not disappear and leave a seemingly safe
	// subset. If even the artifact path is unknown, fail the entire build.
	groups := make(map[string]*artifactGroup)
	for _, item := range models {
		artifactPath, issues, err := resolvePath(cm.Data, item)
		if err != nil {
			return nil, fmt.Errorf("resolve %s: %w", item.key, err)
		}
		group := groups[artifactPath]
		if group == nil {
			group = &artifactGroup{artifact: Artifact{Path: artifactPath, Models: []ModelRef{}, Issues: []Issue{}}}
			groups[artifactPath] = group
		}
		group.artifact.Issues = append(group.artifact.Issues, issues...)
		if item.model != nil {
			m := item.model
			entryPath := storagePath(m.Spec.Storage)
			if clean, err := cleanPath(entryPath); err == nil {
				entryPath = clean
			}
			group.artifact.Models = append(group.artifact.Models, ModelRef{
				Namespace: m.Namespace, Name: m.Name, UID: m.UID,
				ResourceVersion: m.ResourceVersion, TenancyID: m.Labels["tenancy-id"], Path: entryPath,
				// Copy the status list; absent lists are emitted as [] rather than null.
				NodesReady: append([]string{}, m.Status.NodesReady...),
			})
			group.models = append(group.models, item)
		}
	}

	// 3. Estimate each valid group's size once. Job reads are cached only for
	// this Build, including missing Jobs shared by several namespace copies.
	paths := make([]string, 0, len(groups))
	for p := range groups {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	jobs := make(map[string]*replicationJob)
	result := &Inventory{NodeName: nodeName, Artifacts: []Artifact{}}
	for _, p := range paths {
		group := groups[p]
		if len(group.artifact.Issues) == 0 {
			if err := b.estimateSize(ctx, group, jobs); err != nil {
				return nil, err
			}
		}
		group.artifact.EvictionEligible = len(group.artifact.Issues) == 0 && group.artifact.EstimatedArtifactBytes != nil
		result.Artifacts = append(result.Artifacts, group.artifact)
	}
	return result, nil
}

type artifactGroup struct {
	artifact Artifact
	models   []selectedModel
}

// resolvePath trusts the two supported layouts: an ordinary real directory, or
// a registered shared symlink. It never falls back from a broken parent to the
// child path, because that would split one physical artifact into false groups.
func resolvePath(data map[string]string, item selectedModel) (string, []Issue, error) {
	if item.entry.HfArtifactKey == "" {
		if item.issue != nil {
			return "", nil, fmt.Errorf("%s: artifact path is unknown", item.issue.Code)
		}
		if item.entry.HfArtifactPendingDeletion != nil {
			return "", nil, fmt.Errorf("pending shared deletion without a live parent association")
		}
		p, err := cleanPath(storagePath(item.model.Spec.Storage))
		if err != nil {
			return "", nil, err
		}
		return p, modelIssues(item), nil
	}

	parentKey := item.entry.HfArtifactKey
	raw, ok := data[parentKey]
	if !ok {
		return "", nil, fmt.Errorf("shared parent %s is missing", parentKey)
	}
	var parent sharedParent
	if err := json.Unmarshal([]byte(raw), &parent); err != nil {
		return "", nil, fmt.Errorf("decode shared parent %s: %w", parentKey, err)
	}
	p, err := cleanPath(parent.LocalPath)
	if err != nil {
		return "", nil, fmt.Errorf("shared parent %s: %w", parentKey, err)
	}
	issues := modelIssues(item)
	addIssue := func(code, message string) {
		issues = append(issues, Issue{Code: code, Message: message, ModelKey: item.key})
	}
	if parent.Key != parentKey {
		addIssue("parentKeyMismatch", "Shared parent key does not match hfArtifactKey")
	}
	if parent.Status != "Ready" {
		addIssue("parentNotReady", "Shared parent is not Ready")
	}
	if item.entry.HfArtifactPendingDeletion != nil {
		addIssue("pendingDeletion", "Shared cleanup is still pending")
	}
	if item.model != nil {
		entryPath, entryErr := cleanPath(storagePath(item.model.Spec.Storage))
		childPath, childErr := cleanPath(parent.Children[item.key])
		if entryErr != nil || childErr != nil || entryPath != childPath {
			addIssue("childPathMismatch", "BaseModel storage.path and parent.children must contain the same valid path")
		}
	}
	return p, issues, nil
}

func modelIssues(item selectedModel) []Issue {
	var issues []Issue
	if item.issue != nil {
		issues = append(issues, *item.issue)
	}
	if item.model != nil && item.model.DeletionTimestamp != nil {
		issues = append(issues, Issue{Code: "modelDeleting", Message: "BaseModel deletion is in progress", ModelKey: item.key})
	}
	return issues
}

// cleanPath is lexical only; deployment guarantees no undeclared symlink aliases
// or nested artifact roots. Do not turn a '..' path into an accepted directory.
func cleanPath(p string) (string, error) {
	if !path.IsAbs(p) {
		return "", fmt.Errorf("artifact path must be nonempty and absolute: %q", p)
	}
	for _, part := range strings.Split(p, "/") {
		if part == ".." {
			return "", fmt.Errorf("artifact path contains '..': %q", p)
		}
	}
	return path.Clean(p), nil
}
