package utils

import (
	"strconv"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// ModelMatchesNodePlacement shares the agent's storage placement semantics.
// Health, taints, schedulability and preferred affinity do not affect membership.
func ModelMatchesNodePlacement(storage *v1beta1.StorageSpec, node *corev1.Node) bool {
	if storage == nil {
		return true
	}
	for key, value := range storage.NodeSelector {
		if actual, exists := node.Labels[key]; !exists || actual != value {
			return false
		}
	}
	if storage.NodeAffinity != nil && storage.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		terms := storage.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
		if len(terms) > 0 {
			for _, term := range terms {
				if NodeMatchesModelSelectorTerm(node, term) {
					return true
				}
			}
			return false
		}
	}
	return true
}

func NodeMatchesModelSelectorTerm(node *corev1.Node, term corev1.NodeSelectorTerm) bool {
	for _, expr := range term.MatchExpressions {
		if !NodeMatchesModelRequirement(node, expr) {
			return false
		}
	}
	for _, field := range term.MatchFields {
		if !NodeMatchesModelRequirement(node, field) {
			return false
		}
	}
	return true
}

// NodeMatchesModelRequirement preserves Scout's historical missing-key and
// string-comparison behavior, including metadata.name as a label fallback.
func NodeMatchesModelRequirement(node *corev1.Node, expr corev1.NodeSelectorRequirement) bool {
	value, exists := node.Labels[expr.Key]
	if !exists && expr.Key == "metadata.name" {
		value, exists = node.Name, true
	}
	if !exists {
		return expr.Operator == corev1.NodeSelectorOpDoesNotExist
	}
	switch expr.Operator {
	case corev1.NodeSelectorOpIn, corev1.NodeSelectorOpNotIn:
		for _, required := range expr.Values {
			if value == required {
				return expr.Operator == corev1.NodeSelectorOpIn
			}
		}
		return expr.Operator == corev1.NodeSelectorOpNotIn
	case corev1.NodeSelectorOpExists:
		return true
	case corev1.NodeSelectorOpDoesNotExist:
		return false
	case corev1.NodeSelectorOpGt, corev1.NodeSelectorOpLt:
		if len(expr.Values) == 0 {
			return false
		}
		nodeValue, nodeErr := strconv.Atoi(value)
		requiredValue, requiredErr := strconv.Atoi(expr.Values[0])
		if nodeErr == nil && requiredErr == nil {
			if expr.Operator == corev1.NodeSelectorOpGt {
				return nodeValue > requiredValue
			}
			return nodeValue < requiredValue
		}
		if expr.Operator == corev1.NodeSelectorOpGt {
			return value > expr.Values[0]
		}
		return value < expr.Values[0]
	}
	return false
}
