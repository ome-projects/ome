// Package protocol defines the isolated scheduler worker's versioned wire
// contract and validates immutable snapshot inputs.
package protocol

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

const SchemaVersion = "v1"

type ProfileIdentity struct {
	SchedulerName    string `json:"schedulerName"`
	Backend          string `json:"backend"`
	SchedulerVersion string `json:"schedulerVersion"`
	ConfigurationID  string `json:"configurationID"`
}

type PodIdentity struct {
	Namespace string    `json:"namespace"`
	Name      string    `json:"name"`
	UID       types.UID `json:"uid,omitempty"`
}

type Request struct {
	SchemaVersion   string                 `json:"schemaVersion"`
	RequestID       string                 `json:"requestID"`
	Profile         ProfileIdentity        `json:"profile"`
	ReplacementPods []corev1.Pod           `json:"replacementPods"`
	SourcePods      []corev1.Pod           `json:"sourcePods"`
	ClusterObjects  []runtime.RawExtension `json:"clusterObjects"`
	SnapshotID      string                 `json:"snapshotID"`
	SnapshotTime    metav1.Time            `json:"snapshotTime"`
	RequireGang     bool                   `json:"requireGang,omitempty"`
	ExcludedNodes   []string               `json:"excludedNodes"`
}

type Decision string

const (
	DecisionFeasible    Decision = "Feasible"
	DecisionInfeasible  Decision = "Infeasible"
	DecisionUnsupported Decision = "Unsupported"
)

type Reason string

const (
	ReasonPlacementFound      Reason = "PlacementFound"
	ReasonNoFeasiblePlacement Reason = "NoFeasiblePlacement"
	ReasonUnsupported         Reason = "Unsupported"
)

type Placement struct {
	Pod      PodIdentity `json:"pod"`
	NodeName string      `json:"nodeName"`
}

type Result struct {
	SchemaVersion string          `json:"schemaVersion"`
	RequestID     string          `json:"requestID"`
	SnapshotID    string          `json:"snapshotID"`
	SnapshotTime  metav1.Time     `json:"snapshotTime"`
	Profile       ProfileIdentity `json:"profile"`
	Decision      Decision        `json:"decision"`
	Reason        Reason          `json:"reason"`
	Placements    []Placement     `json:"placements,omitempty"`
}

type Snapshot struct {
	Objects    []runtime.Object
	Nodes      map[string]*corev1.Node
	Pods       map[types.NamespacedName]*corev1.Pod
	Namespaces map[string]*corev1.Namespace
	PodGroups  map[types.NamespacedName]*unstructured.Unstructured
}

func ResultFor(request Request, decision Decision) Result {
	reason := ReasonUnsupported
	switch decision {
	case DecisionFeasible:
		reason = ReasonPlacementFound
	case DecisionInfeasible:
		reason = ReasonNoFeasiblePlacement
	case DecisionUnsupported:
	default:
		decision = DecisionUnsupported
	}
	return Result{
		SchemaVersion: request.SchemaVersion,
		RequestID:     request.RequestID,
		SnapshotID:    request.SnapshotID,
		SnapshotTime:  request.SnapshotTime,
		Profile:       request.Profile,
		Decision:      decision,
		Reason:        reason,
	}
}
