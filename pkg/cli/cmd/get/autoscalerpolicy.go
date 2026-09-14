package get

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/cli/printers"
)

var autoscalerPoliciesEntry = &entry{
	Canonical:  "autoscalerpolicies",
	Aliases:    []string{"autoscalerpolicy", "ap"},
	Namespaced: true,
	Columns: []column{
		{Name: "NAME", Extract: safeCol(func(policy *v1beta1.AutoscalerPolicy) string { return policy.Name })},
		{Name: "CLASS", Extract: safeCol(func(policy *v1beta1.AutoscalerPolicy) string {
			return printers.OrDash(string(policy.Spec.Class))
		})},
		{Name: "READY", Extract: safeCol(func(policy *v1beta1.AutoscalerPolicy) string {
			return autoscalerPolicyConditionStatus(policy, v1beta1.AutoscalerPolicyReadyCondition)
		})},
		{Name: "ATTACHED", Extract: safeCol(func(policy *v1beta1.AutoscalerPolicy) string {
			if !autoscalerPolicyStatusCurrent(policy) {
				return "-"
			}
			return fmt.Sprintf("%d", policy.Status.AttachedComponents)
		})},
		{Name: "AGE", Extract: safeCol(func(policy *v1beta1.AutoscalerPolicy) string {
			return printers.Age(policy.CreationTimestamp)
		})},
		{Name: "DIGEST", Wide: true, Extract: safeCol(func(policy *v1beta1.AutoscalerPolicy) string {
			if !autoscalerPolicyStatusCurrent(policy) {
				return "-"
			}
			return printers.OrDash(policy.Status.PortableDigest)
		})},
		{Name: "REASON", Wide: true, Extract: safeCol(func(policy *v1beta1.AutoscalerPolicy) string {
			condition := autoscalerPolicyCurrentCondition(policy, v1beta1.AutoscalerPolicyReadyCondition)
			if condition == nil {
				return "-"
			}
			return printers.OrDash(condition.Reason)
		})},
		{Name: "IN-USE", Wide: true, Extract: safeCol(func(policy *v1beta1.AutoscalerPolicy) string {
			return autoscalerPolicyConditionStatus(policy, v1beta1.AutoscalerPolicyInUseCondition)
		})},
		{Name: "STATUS-FRESHNESS", Wide: true, Extract: safeCol(autoscalerPolicyStatusFreshness)},
	},
	List: func(ctx context.Context, f factory.Factory, namespace string, options metav1.ListOptions) ([]runtime.Object, error) {
		client, err := f.OMEClient()
		if err != nil {
			return nil, err
		}
		policies := client.OmeV1beta1().AutoscalerPolicies(namespace)
		return paging.ListAllPaged(ctx, func(pageOptions metav1.ListOptions) ([]runtime.Object, string, error) {
			pageOptions.LabelSelector = options.LabelSelector
			list, err := policies.List(ctx, pageOptions)
			if err != nil {
				return nil, "", err
			}
			items := make([]runtime.Object, 0, len(list.Items))
			for index := range list.Items {
				items = append(items, &list.Items[index])
			}
			return items, list.Continue, nil
		})
	},
	GetOne: func(ctx context.Context, f factory.Factory, namespace, name string) (runtime.Object, error) {
		client, err := f.OMEClient()
		if err != nil {
			return nil, err
		}
		return client.OmeV1beta1().AutoscalerPolicies(namespace).Get(ctx, name, metav1.GetOptions{})
	},
}

func autoscalerPolicyStatusFreshness(policy *v1beta1.AutoscalerPolicy) string {
	if policy.Status.ObservedGeneration == 0 {
		return "Unobserved"
	}
	return generationFreshness(policy.Generation, policy.Status.ObservedGeneration)
}

func autoscalerPolicyStatusCurrent(policy *v1beta1.AutoscalerPolicy) bool {
	return autoscalerPolicyStatusFreshness(policy) == "Current"
}

func autoscalerPolicyCurrentCondition(policy *v1beta1.AutoscalerPolicy, conditionType string) *metav1.Condition {
	if !autoscalerPolicyStatusCurrent(policy) {
		return nil
	}
	condition := meta.FindStatusCondition(policy.Status.Conditions, conditionType)
	if condition == nil || condition.ObservedGeneration != policy.Generation {
		return nil
	}
	return condition
}

func autoscalerPolicyConditionStatus(policy *v1beta1.AutoscalerPolicy, conditionType string) string {
	condition := autoscalerPolicyCurrentCondition(policy, conditionType)
	if condition == nil {
		return "Unknown"
	}
	return string(condition.Status)
}
