package utils

import (
	"encoding/json"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
)

/*
GetDeploymentModeFromAnnotations extracts a valid deployment mode from annotations if present.
Returns the deployment mode and true if found and valid, otherwise returns empty string and false.
*/
func GetDeploymentModeFromAnnotations(annotations map[string]string) (constants.DeploymentModeType, bool) {
	if annotations == nil {
		return "", false
	}
	if mode, exists := annotations[constants.DeploymentMode]; exists {
		deploymentMode := constants.DeploymentModeType(mode)
		if deploymentMode.IsValid() {
			return deploymentMode, true
		}
	}
	return "", false
}

func IsOriginalModelVolumeMountNecessary(annotations map[string]string) bool {
	return annotations[constants.FTServingWithMergedWeightsAnnotationKey] != "true"
}

func IsEmptyModelDirVolumeRequired(annotations map[string]string) bool {
	return len(annotations[constants.FineTunedAdapterInjectionKey]) > 0
}

func IsCohereCommand1TFewFTServing(servingPodObjectMeta *metav1.ObjectMeta) bool {
	if servingPodObjectMeta.Annotations[constants.BaseModelVendorAnnotationKey] == string(constants.Cohere) &&
		servingPodObjectMeta.Annotations[constants.FineTunedWeightFTStrategyKey] == string(constants.TFewTrainingStrategy) &&
		servingPodObjectMeta.Annotations[constants.FTServingWithMergedWeightsAnnotationKey] != "true" {
		return true
	}
	return false
}

func SetPodLabelsFromAnnotations(metadata *metav1.ObjectMeta) {
	// Check if the VolcanoQueue annotation exists and set the label if it does.
	if volcanoQueue, ok := metadata.Annotations[constants.VolcanoQueue]; ok {
		metadata.Labels[constants.VolcanoQueueName] = volcanoQueue
		// If VolcanoQueue annotation does not exist, check and set to DedicatedAICluster name
	} else if dac, ok := metadata.Annotations[constants.DedicatedAICluster]; ok {
		if _, ok = metadata.Annotations[constants.KueueEnabledLabelKey]; ok {
			// Kueue case
			metadata.Labels[constants.KueueQueueLabelKey] = dac
			metadata.Labels[constants.KueueWorkloadPriorityClassLabelKey] = constants.DedicatedAiClusterPreemptionWorkloadPriorityClass
		} else {
			// Volcano case
			metadata.Labels[constants.VolcanoQueueName] = dac
		}
	}
}

func RemovePodAnnotations(metadata *metav1.ObjectMeta, annotationsToRemove []string) {
	for _, annotation := range annotationsToRemove {
		delete(metadata.Annotations, annotation)
	}
}

// ResolveIngressConfig creates an effective ingress configuration by merging
// global defaults from configMap with per-service annotation overrides
func ResolveIngressConfig(baseConfig *controllerconfig.IngressConfig, annotations map[string]string) *controllerconfig.IngressConfig {
	// Copy all fields of the base config so the effective config can never
	// silently drop a newly added one; the overrides below replace fields
	// rather than mutate the original's shared slices/pointers.
	resolvedCopy := *baseConfig
	resolved := &resolvedCopy

	// Override with annotation values if present
	if domainTemplate, exists := annotations[constants.IngressDomainTemplate]; exists && domainTemplate != "" {
		resolved.DomainTemplate = domainTemplate
	}

	if ingressDomain, exists := annotations[constants.IngressDomain]; exists && ingressDomain != "" {
		resolved.IngressDomain = ingressDomain
	}

	if urlScheme, exists := annotations[constants.IngressURLScheme]; exists && urlScheme != "" {
		resolved.UrlScheme = urlScheme
	}

	if pathTemplate, exists := annotations[constants.IngressPathTemplate]; exists {
		resolved.PathTemplate = pathTemplate
	}

	if additionalDomains, exists := annotations[constants.IngressAdditionalDomains]; exists && additionalDomains != "" {
		// Parse comma-separated list
		domains := strings.Split(additionalDomains, ",")
		for i := range domains {
			domains[i] = strings.TrimSpace(domains[i])
		}
		resolved.AdditionalIngressDomains = &domains
	}

	// Boolean overrides
	if disableCreation, exists := annotations[constants.IngressDisableCreation]; exists {
		resolved.DisableIngressCreation = disableCreation == "true"
	}

	// Gateway / host-scheme overrides — let a single ISVC target a different
	// gateway or host scheme than the cluster default (e.g. expose just this
	// ISVC on an external gateway, or use a per-ISVC subdomain).
	if gateway, exists := annotations[constants.IngressGatewayOverride]; exists && gateway != "" {
		resolved.OmeIngressGateway = gateway
	}

	if perISVC, exists := annotations[constants.IngressPerISVCSubdomain]; exists {
		resolved.PerISVCSubdomain = perISVC == "true"
	}

	// SharedHostPrefix overrides on bare presence so an explicit empty value can
	// mean "no prefix" (bare domain) for this ISVC.
	if prefix, exists := annotations[constants.IngressSharedHostPrefix]; exists {
		resolved.SharedHostPrefix = prefix
	}

	// AdditionalIngressGateways is a JSON array of {omeIngressGateway, ingressDomain}.
	// A malformed value is ignored (keeps the base) — the webhook validates format.
	if additionalGateways, exists := annotations[constants.IngressAdditionalGateways]; exists && additionalGateways != "" {
		var gateways []controllerconfig.IngressGatewaySpec
		if err := json.Unmarshal([]byte(additionalGateways), &gateways); err == nil {
			resolved.AdditionalIngressGateways = gateways
		}
	}

	return resolved
}
