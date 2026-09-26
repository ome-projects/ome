package modelagent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"
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
	validateCurrent       func() error
	artifactRehydrationID string // Only a committed Ready report supplies this.
	nodeUID               types.UID
	requestLabelOnly      bool
	deleteLabelKeys       []string // Present keys in the node snapshot, rebuilt on retry.
	ModelStateOnNode      ModelStateOnNode
	BaseModel             *v1beta1.BaseModel
	ClusterBaseModel      *v1beta1.ClusterBaseModel
}

// NodeLabelReconciler handles updating node labels œwith model status information
// It provides a clean separation from ConfigMap operations
type NodeLabelReconciler struct {
	opRetry    int                  // Number of retries for operations
	kubeClient kubernetes.Interface // Kubernetes client for node operations
	nodeName   string               // The name of the node
	nodeUID    types.UID            // Bound once by NewGopher before starting workers.
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

// WithdrawReadiness verifies the resulting node state as part of each attempt.
// Eviction removes the normal label; restoration requires it to be non-Ready.
func (n *NodeLabelReconciler) WithdrawReadiness(ctx context.Context, op *NodeLabelOp) error {
	if op == nil || op.validateCurrent == nil {
		return fmt.Errorf("readiness withdrawal requires current Model validation")
	}
	if op.requestLabelOnly || op.ModelStateOnNode != Deleted && op.ModelStateOnNode != Updating {
		return fmt.Errorf("readiness withdrawal requires Deleted or Updating state")
	}
	// Keep the first observed node UID across retries without mutating the caller.
	attempt := *op
	var lastErr error
	// Keep the underlying error: the legacy Retry helper formats away its type.
	_ = utils.Retry(n.opRetry, 100*time.Millisecond, func() error {
		lastErr = n.applyNodeLabelOperationWithContext(ctx, &attempt, true)
		return lastErr
	})
	return lastErr
}

// applyNodeLabelOperation applies model state changes to the node labels
func (n *NodeLabelReconciler) applyNodeLabelOperation(op *NodeLabelOp) error {
	return n.applyNodeLabelOperationWithContext(context.TODO(), op, false)
}

func (n *NodeLabelReconciler) applyNodeLabelOperationWithContext(ctx context.Context, op *NodeLabelOp, strict bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !strict {
		copy := *op
		op = &copy
	}
	if n.nodeUID != "" {
		op.nodeUID = n.nodeUID
	}
	finish := func() error {
		if strict {
			return n.verifyReadinessWithdrawn(ctx, op)
		}
		return nil
	}
	modelInfo := getNodeLabelModelInfo(op)
	n.logger.Infof("Processing node label %s operation for %s in state: %s", op.ModelStateOnNode, modelInfo, op.ModelStateOnNode)

	// Get label key for this model
	labelKey, err := getModelLabelKey(op)
	if err != nil {
		n.logger.Errorf("Failed to get label key for %s: %v", modelInfo, err)
		if strict {
			return err
		}
		return nil // Don't retry for invalid model references
	}

	// First get the node to check existing labels
	node, err := n.kubeClient.CoreV1().Nodes().Get(ctx, n.nodeName, metav1.GetOptions{})
	if err != nil {
		if strict {
			return err
		}
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
	if op.validateCurrent != nil {
		if err := op.validateCurrent(); err != nil {
			return err
		}
	}
	if op.nodeUID != "" && node.UID != op.nodeUID {
		return fmt.Errorf("node identity changed before model label publication")
	}
	if strict && op.nodeUID == "" {
		op.nodeUID = node.UID
	}
	currentValue, labelExists := node.Labels[labelKey]

	// Handle operation based on desired state and current state
	switch op.ModelStateOnNode {
	case Deleted:
		op.deleteLabelKeys = make([]string, 0, 2)
		if labelExists && !op.requestLabelOnly {
			op.deleteLabelKeys = append(op.deleteLabelKeys, labelKey)
		}
		if uid := getModelResourceUID(op.BaseModel, op.ClusterBaseModel); uid != "" {
			requestKey := constants.GetModelArtifactRequestLabel(uid)
			if _, exists := node.Labels[requestKey]; exists {
				op.deleteLabelKeys = append(op.deleteLabelKeys, requestKey)
			}
		}
		if len(op.deleteLabelKeys) == 0 {
			n.logger.Infof("Label %s already removed from node %s for %s - operation is idempotent", labelKey, n.nodeName, modelInfo)
			return finish()
		}
	case Ready, Updating, Failed:
		// For add/update operations, if the label already has the desired value, skip
		pairedReady := op.ModelStateOnNode != Ready || op.artifactRehydrationID == "" ||
			node.Labels[constants.GetModelArtifactRequestLabel(getModelResourceUID(op.BaseModel, op.ClusterBaseModel))] == op.artifactRehydrationID
		if labelExists && currentValue == string(op.ModelStateOnNode) && pairedReady {
			n.logger.Infof("Label %s already set to %s on node %s for %s - operation is idempotent",
				labelKey, string(op.ModelStateOnNode), n.nodeName, modelInfo)
			return finish()
		}
	}

	// Generate patch payload
	resourceVersion := ""
	if strict || op.validateCurrent != nil || op.nodeUID != "" {
		// Bind the validated request to the node snapshot. A newer Ready
		// publication must conflict, then retry with fresh request validation.
		resourceVersion = node.ResourceVersion
	}
	payloadBytes, err := getNodeLabelPatchPayloadBytes(op, resourceVersion)
	if err != nil {
		n.logger.Errorf("Failed to get node label patch payload for %s: %v", modelInfo, err)
		if strict {
			return err
		}
		return nil // Don't retry for payload generation issues
	}
	n.logger.Debugf("Generated node label patch payload for %s: %s", modelInfo, string(payloadBytes))

	// Skip empty patch operations
	if len(payloadBytes) <= 2 { // Just "[]" for empty patch
		n.logger.Infof("Empty patch payload for %s, skipping operation", modelInfo)
		return finish()
	}

	// Apply the patch
	_, err = n.kubeClient.CoreV1().Nodes().Patch(
		ctx,
		n.nodeName,
		types.JSONPatchType,
		payloadBytes,
		metav1.PatchOptions{},
	)
	if err != nil {
		if strict {
			return err
		}
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
			if op.validateCurrent != nil || op.nodeUID != "" {
				return err // Includes a failed resourceVersion JSON test.
			}
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

	return finish()
}

func (n *NodeLabelReconciler) verifyReadinessWithdrawn(ctx context.Context, op *NodeLabelOp) error {
	node, err := n.kubeClient.CoreV1().Nodes().Get(ctx, n.nodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if err := op.validateCurrent(); err != nil {
		return err
	}
	if op.nodeUID != "" && node.UID != op.nodeUID {
		return fmt.Errorf("node identity changed during readiness withdrawal")
	}
	label, err := getModelLabelKey(op)
	if err != nil {
		return err
	}
	value, present := node.Labels[label]
	if op.ModelStateOnNode == Deleted && present {
		return fmt.Errorf("model readiness label has not been removed")
	}
	if op.ModelStateOnNode == Updating && value == string(Ready) {
		return fmt.Errorf("model readiness has not been withdrawn")
	}
	return ctx.Err()
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
func getNodeLabelPatchPayloadBytes(op *NodeLabelOp, resourceVersion string) ([]byte, error) {
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
	case Deleted:
		keys := op.deleteLabelKeys
		if keys == nil {
			keys = []string{labelKey}
		}
		for _, key := range keys {
			payload = append(payload, patchStringValue{Op: "remove", Path: "/metadata/labels/" + strings.ReplaceAll(key, "/", "~1")})
		}
	default:
		break
	}
	if len(payload) != 0 && resourceVersion != "" {
		payload = append([]patchStringValue{{Op: "test", Path: "/metadata/resourceVersion", Value: resourceVersion}}, payload...)
	}
	if op.ModelStateOnNode == Ready && op.artifactRehydrationID != "" {
		key := constants.GetModelArtifactRequestLabel(getModelResourceUID(op.BaseModel, op.ClusterBaseModel))
		payload = append(payload, patchStringValue{Op: "add", Path: "/metadata/labels/" + strings.ReplaceAll(key, "/", "~1"), Value: op.artifactRehydrationID})
	}
	if len(payload) != 0 && op.nodeUID != "" {
		payload = append([]patchStringValue{{Op: "test", Path: "/metadata/uid", Value: string(op.nodeUID)}}, payload...)
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return payloadBytes, nil
}
