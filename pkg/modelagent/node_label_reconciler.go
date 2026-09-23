package modelagent

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"strings"
	"time"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils"
)

// NodeLabelOp represents an operation on node labels
// This is used to pass model references to the NodeLabelReconciler
type NodeLabelOp struct {
	ModelStateOnNode ModelStateOnNode
	BaseModel        *v1beta1.BaseModel
	ClusterBaseModel *v1beta1.ClusterBaseModel
}

// NodeLabelReconciler handles updating node labels œwith model status information
// It provides a clean separation from ConfigMap operations
type NodeLabelReconciler struct {
	opRetry    int                  // Number of retries for operations
	kubeClient kubernetes.Interface // Kubernetes client for node operations
	nodeName   string               // The name of the node
	logger     *zap.SugaredLogger   // Logger for recording operations
}

// patchStringValue represents a JSON patch operation for node labels
type patchStringValue struct {
	Op    string `json:"op,omitempty"`
	Path  string `json:"path"`
	Value string `json:"value,omitempty"`
}

// ModelStateOnNode represents the model state in legacy format
// Maintained for backward compatibility with existing codepaths
type ModelStateOnNode string

// Model state constants (legacy format)
const (
	// Ready indicates the model is ready to use
	Ready ModelStateOnNode = "Ready"
	// Updating indicates the model is being downloaded or updated
	Updating ModelStateOnNode = "Updating"
	// Failed indicates the model failed to download or initialize
	Failed ModelStateOnNode = "Failed"
	// Deleted indicates the model was marked for deletion
	Deleted ModelStateOnNode = "Deleted"
	Evicted ModelStateOnNode = "Evicted"
)

// NewNodeLabelReconciler creates a new NodeLabelReconciler instance
func NewNodeLabelReconciler(nodeName string, kubeClient kubernetes.Interface, opRetry int, logger *zap.SugaredLogger) *NodeLabelReconciler {
	return &NodeLabelReconciler{
		opRetry:    opRetry,
		nodeName:   nodeName,
		kubeClient: kubeClient,
		logger:     logger,
	}
}

// ReconcileNodeLabels applies model state changes to node labels with retries
func (n *NodeLabelReconciler) ReconcileNodeLabels(op *NodeLabelOp) error {
	return utils.Retry(n.opRetry, 100*time.Millisecond, func() error {
		return n.applyNodeLabelOperation(op)
	})
}

// applyNodeLabelOperation applies model state changes to the node labels
func (n *NodeLabelReconciler) applyNodeLabelOperation(op *NodeLabelOp) error {
	if getModelResourceUID(op.BaseModel, op.ClusterBaseModel) != "" {
		return n.applyArtifactNodeLabelOperation(op)
	}
	modelInfo := getNodeLabelModelInfo(op)
	n.logger.Infof("Processing node label %s operation for %s in state: %s", op.ModelStateOnNode, modelInfo, op.ModelStateOnNode)

	// Get label key for this model
	labelKey, err := getModelLabelKey(op)
	if err != nil {
		n.logger.Errorf("Failed to get label key for %s: %v", modelInfo, err)
		return nil // Don't retry for invalid model references
	}

	// First get the node to check existing labels
	node, err := n.kubeClient.CoreV1().Nodes().Get(context.TODO(), n.nodeName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			// Node doesn't exist, log warning and return nil to avoid retries
			n.logger.Warnf("Node %s not found, skipping node labeling for %s: %v", n.nodeName, modelInfo, err)
			return nil // Return nil to avoid retries for non-existent nodes
		}
		// For other errors, log and return error for possible retry
		n.logger.Errorf("Error checking node %s existence for %s: %v", n.nodeName, modelInfo, err)
		return err
	}

	// Check current labels - make idempotent based on operation type
	currentValue, labelExists := node.Labels[labelKey]

	// Handle operation based on desired state and current state
	switch op.ModelStateOnNode {
	case Deleted:
		// For delete operations, if the label doesn't exist, the operation is already complete
		if !labelExists {
			n.logger.Infof("Label %s already removed from node %s for %s - operation is idempotent", labelKey, n.nodeName, modelInfo)
			return nil
		}
	case Ready, Updating, Failed, Evicted:
		// For add/update operations, if the label already has the desired value, skip
		if labelExists && currentValue == string(op.ModelStateOnNode) {
			n.logger.Infof("Label %s already set to %s on node %s for %s - operation is idempotent",
				labelKey, string(op.ModelStateOnNode), n.nodeName, modelInfo)
			return nil
		}
	}

	// Generate patch payload
	payloadBytes, err := getNodeLabelPatchPayloadBytes(op)
	if err != nil {
		n.logger.Errorf("Failed to get node label patch payload for %s: %v", modelInfo, err)
		return nil // Don't retry for payload generation issues
	}
	n.logger.Debugf("Generated node label patch payload for %s: %s", modelInfo, string(payloadBytes))

	// Skip empty patch operations
	if len(payloadBytes) <= 2 { // Just "[]" for empty patch
		n.logger.Infof("Empty patch payload for %s, skipping operation", modelInfo)
		return nil
	}

	// Apply the patch
	_, err = n.kubeClient.CoreV1().Nodes().Patch(
		context.TODO(),
		n.nodeName,
		types.JSONPatchType,
		payloadBytes,
		metav1.PatchOptions{},
	)
	if err != nil {
		// Check for specific error types and handle them gracefully
		if errors.IsNotFound(err) {
			// Node disappeared after our initial check
			n.logger.Warnf("Node %s not found during patch operation for %s, skipping", n.nodeName, modelInfo)
			return nil // Don't retry for non-existent nodes
		} else if errors.IsConflict(err) {
			// Conflict means the resource was modified - this is retryable
			n.logger.Warnf("Conflict during patch operation for node %s and model %s, will retry: %v", n.nodeName, modelInfo, err)
			return err // Return error to trigger retry
		} else if errors.IsInvalid(err) || errors.IsBadRequest(err) {
			// For delete operations that fail with "not found" patch path errors, consider it already done
			if op.ModelStateOnNode == Deleted && strings.Contains(err.Error(), "not found") {
				n.logger.Infof("Label %s already removed from node %s for %s - considering delete operation successful",
					labelKey, n.nodeName, modelInfo)
				return nil
			}

			// Other invalid request, could be malformed patch, log but don't retry
			n.logger.Warnf("Invalid patch request for node %s and model %s: %v", n.nodeName, modelInfo, err)
			return nil // Don't retry for bad requests
		}

		// For other errors, log and return for retry
		n.logger.Errorf("Failed to patch node %s for %s: %v", n.nodeName, modelInfo, err)
		return err
	}
	n.logger.Infof("Successfully patched node %s with %s state for %s", n.nodeName, op.ModelStateOnNode, modelInfo)

	return nil
}

// Eviction status changes must not overwrite another worker's node labels.
func (n *NodeLabelReconciler) applyArtifactNodeLabelOperation(op *NodeLabelOp) error {
	key, err := getModelLabelKey(op)
	if err != nil {
		return err
	}
	node, err := n.kubeClient.CoreV1().Nodes().Get(context.TODO(), n.nodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	labels := maps.Clone(node.Labels)
	if labels == nil {
		labels = make(map[string]string)
	}
	switch op.ModelStateOnNode {
	case Ready, Updating, Failed, Evicted:
		labels[key] = string(op.ModelStateOnNode)
	case Deleted:
		delete(labels, key)
	default:
		return nil
	}
	if maps.Equal(labels, node.Labels) {
		return nil
	}
	return patchNodeLabels(context.TODO(), n.kubeClient, node, labels)
}

func patchNodeLabels(ctx context.Context, client kubernetes.Interface, node *corev1.Node, labels map[string]string) error {
	patch := []map[string]interface{}{}
	if node.UID != "" {
		patch = append(patch, map[string]interface{}{"op": "test", "path": "/metadata/uid", "value": string(node.UID)})
	}
	if node.ResourceVersion != "" {
		patch = append(patch, map[string]interface{}{"op": "test", "path": "/metadata/resourceVersion", "value": node.ResourceVersion})
	}
	patch = append(patch, map[string]interface{}{"op": "add", "path": "/metadata/labels", "value": labels})
	payload, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	_, err = client.CoreV1().Nodes().Patch(ctx, node.Name, types.JSONPatchType, payload, metav1.PatchOptions{})
	return err
}

// getNodeLabelModelInfo returns a string identifying a model for logging
func getNodeLabelModelInfo(op *NodeLabelOp) string {
	if op.BaseModel != nil {
		return fmt.Sprintf("BaseModel %s/%s", op.BaseModel.Namespace, op.BaseModel.Name)
	} else if op.ClusterBaseModel != nil {
		return fmt.Sprintf("ClusterBaseModel %s", op.ClusterBaseModel.Name)
	}
	return "unknown model"
}

// getModelLabelKey gets the label key for a model
func getModelLabelKey(op *NodeLabelOp) (string, error) {
	var labelKey string

	// Use the deterministic labeling system
	if op.ClusterBaseModel != nil {
		labelKey = constants.GetClusterBaseModelLabel(op.ClusterBaseModel.Name)
	} else if op.BaseModel != nil {
		labelKey = constants.GetBaseModelLabel(op.BaseModel.Namespace, op.BaseModel.Name)
	}

	if len(labelKey) == 0 {
		if op.ClusterBaseModel == nil && op.BaseModel == nil {
			return "", fmt.Errorf("node labeler got empty op without any models")
		}
		return "", fmt.Errorf("could not generate label key for model")
	}

	return labelKey, nil
}

// getNodeLabelPatchPayloadBytes generates the JSON patch for node labels
func getNodeLabelPatchPayloadBytes(op *NodeLabelOp) ([]byte, error) {
	labelKey, err := getModelLabelKey(op)
	if err != nil {
		return []byte{}, err
	}

	var payload []patchStringValue
	switch op.ModelStateOnNode {
	case Ready:
		payload = []patchStringValue{{
			Op:    "add",
			Path:  fmt.Sprintf("/metadata/labels/%s", strings.ReplaceAll(labelKey, "/", "~1")),
			Value: string(Ready),
		}}
	case Updating:
		payload = []patchStringValue{{
			Op:    "add",
			Path:  fmt.Sprintf("/metadata/labels/%s", strings.ReplaceAll(labelKey, "/", "~1")),
			Value: string(Updating),
		}}
	case Failed:
		payload = []patchStringValue{{
			Op:    "add",
			Path:  fmt.Sprintf("/metadata/labels/%s", strings.ReplaceAll(labelKey, "/", "~1")),
			Value: string(Failed),
		}}
	case Evicted:
		payload = []patchStringValue{{
			Op:    "add",
			Path:  fmt.Sprintf("/metadata/labels/%s", strings.ReplaceAll(labelKey, "/", "~1")),
			Value: string(Evicted),
		}}
	case Deleted:
		payload = []patchStringValue{{
			Op:   "remove",
			Path: fmt.Sprintf("/metadata/labels/%s", strings.ReplaceAll(labelKey, "/", "~1")),
		}}
	default:
		break
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return payloadBytes, nil
}
