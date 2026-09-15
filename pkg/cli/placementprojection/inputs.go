package placementprojection

import (
	"strings"
	"unicode"
	"unicode/utf8"

	"k8s.io/apimachinery/pkg/labels"
	validation "k8s.io/apimachinery/pkg/util/validation"
	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	v "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/safetext"
)

func projectInputs(parent *ome.InferenceService) (v.PlacementInputs, labels.Selector) {
	requirements := parent.Annotations["ome.io/accelerator-requirements"]
	selector := parent.Annotations["ome.io/cluster-selector"]
	in := v.PlacementInputs{Source: "LegacyAnnotations", Mode: "Single", ModeEvidence: "Defaulted", LegacyRequirementsPresent: requirements != "", LegacyClusterSelectorPresent: selector != ""}
	if p := parent.Spec.Placement; p != nil {
		in.Source = "Structured"
		requirements = p.Requirements
		selector = p.ClusterSelector
		if p.Mode != "" {
			in.Mode = modeValue(p.Mode)
			in.ModeEvidence = "Declared"
		}
		if p.Split != nil {
			in.Split = &v.PlacementSplit{Replicas: pointerCount(p.Split.Replicas), Spread: "UnknownOrDefaultedPacked", MaxReplicasPerCluster: count(p.Split.MaxReplicasPerCluster, true), MinReplicasPerCluster: count(p.Split.MinReplicasPerCluster, true)}
			if p.Split.Spread {
				in.Split.Spread = "DeclaredBalanced"
			}
		}
	}
	in.Requirements = v.PlacementSelector{Present: requirements != "", State: "Absent"}
	in.ClusterSelector = v.PlacementSelector{Present: selector != "", State: "Absent"}
	if requirements == "" && selector == "" {
		in.RequirementsState = "NoRequirements"
		return in, nil
	}
	combined := labels.NewSelector()
	in.RequirementsState = "Valid"
	for _, input := range []struct {
		raw    string
		target *v.PlacementSelector
	}{{requirements, &in.Requirements}, {selector, &in.ClusterSelector}} {
		if input.raw == "" {
			continue
		}
		if len(input.raw) > 4096 {
			input.target.State = "BudgetExceeded"
			in.RequirementsState = "BudgetExceeded"
			return in, nil
		}
		if !safeText(input.raw, 4096, true) {
			input.target.State = "InvalidSelector"
			in.RequirementsState = "InvalidSelector"
			return in, nil
		}
		parsed, err := labels.Parse(input.raw)
		if err != nil {
			input.target.State = "InvalidSelector"
			in.RequirementsState = "InvalidSelector"
			return in, nil
		}
		input.target.State = "Valid"
		reqs, _ := parsed.Requirements()
		combined = combined.Add(reqs...)
	}
	return in, combined
}

func validLabels(values map[string]string) bool {
	if len(values) > 256 {
		return false
	}
	total := 0
	for key, value := range values {
		total += len(key) + len(value)
		if total > 16384 || len(key) > 253 || len(validation.IsQualifiedName(key)) != 0 || len(validation.IsValidLabelValue(value)) != 0 {
			return false
		}
	}
	return true
}

func publicName(value string) bool {
	if len(value) > 253 || len(validation.IsDNS1123Subdomain(value)) != 0 {
		return false
	}
	if safetext.Sanitize(value, 253) != value {
		return false
	}
	return true
}

func publicNamespace(value string) bool {
	return publicName(value) && len(validation.IsDNS1123Label(value)) == 0
}

func safeText(value string, max int, space bool) bool {
	if len(value) > max || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) || (!space && unicode.IsSpace(r)) {
			return false
		}
	}
	return true
}

func safeDigest(value, prefix string) bool {
	if value == "" {
		return true
	}
	if len(value) != len(prefix)+12 || !strings.HasPrefix(value, prefix) {
		return false
	}
	for _, r := range value[len(prefix):] {
		if !(r >= 'a' && r <= 'f' || r >= '0' && r <= '9') {
			return false
		}
	}
	return true
}

func modeValue(mode ome.PlacementMode) v.PlacementValue {
	switch mode {
	case ome.PlacementModeSingle, ome.PlacementModeAll, ome.PlacementModeSplit:
		return v.PlacementValue(mode)
	default:
		return "Unknown"
	}
}
func placementPhase(phase ome.PlacementPhase) v.PlacementValue {
	switch phase {
	case ome.PlacementPhasePending, ome.PlacementPhaseRacing, ome.PlacementPhasePlaced, ome.PlacementPhaseFailed:
		return v.PlacementValue(phase)
	case "":
		return "NotRecorded"
	default:
		return "Unknown"
	}
}
func candidatePhase(phase ome.CandidatePlacementPhase) v.PlacementValue {
	switch phase {
	case ome.CandidatePhasePlaced, ome.CandidatePhaseAdmitted:
		return v.PlacementValue(phase)
	case "":
		return "NotRecorded"
	default:
		return "Unknown"
	}
}
func truth(value bool) v.PlacementValue {
	if value {
		return "True"
	}
	return "False"
}
func optionalTruth(value bool) v.PlacementValue {
	if value {
		return "ReportedTrue"
	}
	return "Unknown"
}
func reported() v.PlacementEvidence {
	return v.PlacementEvidence{Evidence: v.EvidenceReported, Freshness: "Unverifiable", Reason: "NoObservationGeneration"}
}
func count(value int32, applicable bool) v.PlacementCount {
	if !applicable {
		return v.PlacementCount{State: "NotApplicable"}
	}
	if value < 0 {
		return v.PlacementCount{State: "Invalid"}
	}
	if value == 0 {
		return v.PlacementCount{State: "Unknown"}
	}
	copyValue := value
	return v.PlacementCount{Value: &copyValue, State: "Reported"}
}
func pointerCount(value *int32) v.PlacementCount {
	if value == nil {
		return v.PlacementCount{State: "UnknownOrDefaulted"}
	}
	if *value < 0 {
		return v.PlacementCount{State: "Invalid"}
	}
	copyValue := *value
	return v.PlacementCount{Value: &copyValue, State: "Declared"}
}
func connectionSource(w *ome.WorkloadCluster) v.PlacementValue {
	if w.Spec.ClusterSource.KubeConfig != nil && w.Spec.ClusterSource.ClusterProfileRef == nil {
		return "KubeConfigReferenceNotResolved"
	}
	if w.Spec.ClusterSource.KubeConfig == nil && w.Spec.ClusterSource.ClusterProfileRef != nil {
		return "ClusterProfileReferenceNotResolved"
	}
	return "Unknown"
}
