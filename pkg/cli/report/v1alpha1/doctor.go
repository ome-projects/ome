package v1alpha1

import (
	"cmp"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
	"sigs.k8s.io/ome/pkg/cli/safetext"
)

type DoctorAPIID string
type DoctorAvailability string
type DoctorReason string
type DoctorReadID string
type DoctorImageState string
type DoctorVersionState string
type DoctorComparison string

const (
	DoctorAvailable       DoctorAvailability = "Available"
	DoctorNotDiscoverable DoctorAvailability = "NotDiscoverableAtVersion"
	DoctorUnavailable     DoctorAvailability = "Unavailable"
	DoctorNotRequested    DoctorAvailability = "NotRequested"
	DoctorNotSelected     DoctorAvailability = "NotSelected"
	DoctorPresent         DoctorAvailability = "Present"
	DoctorAbsent          DoctorAvailability = "AbsentOnSelectedObject"

	DoctorNotFound     DoctorReason = "NotFound"
	DoctorForbidden    DoctorReason = "Forbidden"
	DoctorUnauthorized DoctorReason = "Unauthorized"
	DoctorTimeout      DoctorReason = "Timeout"
	DoctorThrottled    DoctorReason = "Throttled"
	DoctorServerError  DoctorReason = "ServerError"
	DoctorUnreadable   DoctorReason = "Unreadable"
	DoctorMalformed    DoctorReason = "MalformedPayload"
	DoctorTruncated    DoctorReason = "Truncated"

	DoctorReadManager DoctorReadID = "ManagerDeployment"
	DoctorReadISVC    DoctorReadID = "SelectedInferenceService"

	DoctorSelectedStableTag  DoctorImageState = "SelectedStableTag"
	DoctorNoManagerContainer DoctorImageState = "NoManagerContainer"
	DoctorAmbiguousManager   DoctorImageState = "AmbiguousManagerContainer"
	DoctorUnversionedImage   DoctorImageState = "UnversionedImage"
	DoctorDigestImage        DoctorImageState = "DigestOnly"
	DoctorNonCanonicalTag    DoctorImageState = "NonCanonicalTag"
	DoctorMalformedImage     DoctorImageState = "Malformed"
	DoctorImageUnavailable   DoctorImageState = "Unavailable"

	DoctorUnknown           DoctorVersionState = "Unknown"
	DoctorUnverifiable      DoctorVersionState = "Unverifiable"
	DoctorKnownCandidate    DoctorVersionState = "CanonicalCandidate"
	DoctorWithinMinor       DoctorComparison   = "WithinOneMinor"
	DoctorOutsideMinor      DoctorComparison   = "OutsideOneMinor"
	DoctorMajorMismatch     DoctorComparison   = "MajorMismatch"
	DoctorComparisonUnknown DoctorComparison   = "Unknown"
)

// DoctorReport owns a privacy boundary in addition to the shared envelope.
// It describes observations, never a Kubernetes resource or authorization.
type DoctorReport struct{ Envelope[DoctorContent] }

type DoctorContext struct {
	Name              string `json:"name"`
	WorkloadNamespace string `json:"workloadNamespace"`
	OMENamespace      string `json:"omeNamespace"`
}

type DoctorSummary struct {
	State                 string `json:"state"`
	RequiredAPIViolations int    `json:"requiredAPIViolations"`
	UnavailableEvidence   int    `json:"unavailableEvidence"`
}

// DoctorAPI is discoverability at one fixed version, not read authorization.
type DoctorAPI struct {
	ID           DoctorAPIID        `json:"id"`
	GroupVersion string             `json:"groupVersion"`
	Resource     string             `json:"resource"`
	Kind         string             `json:"kind"`
	Scope        string             `json:"scope"`
	Required     bool               `json:"required"`
	Availability DoctorAvailability `json:"availability"`
	Reason       DoctorReason       `json:"reason,omitempty"`
	Evidence     EvidenceLevel      `json:"evidence"`
}

// DoctorRead describes only an exact attempted named GET or NotRequested.
type DoctorRead struct {
	ID           DoctorReadID       `json:"id"`
	Method       string             `json:"method"`
	GroupVersion string             `json:"groupVersion"`
	Resource     string             `json:"resource"`
	Namespace    string             `json:"namespace"`
	Name         string             `json:"name,omitempty"`
	Outcome      DoctorAvailability `json:"outcome"`
	Reason       DoctorReason       `json:"reason,omitempty"`
	Evidence     EvidenceLevel      `json:"evidence"`
}

type DoctorManagerEvidence struct {
	ImageState            DoctorImageState `json:"imageState"`
	ImageVersionCandidate string           `json:"imageVersionCandidate,omitempty"`
}

type DoctorVersion struct {
	ClientVersion         string             `json:"clientVersion,omitempty"`
	ClientVersionState    DoctorVersionState `json:"clientVersionState"`
	ImageState            DoctorImageState   `json:"imageState"`
	ImageVersionCandidate string             `json:"imageVersionCandidate,omitempty"`
	OperatorVersionState  DoctorVersionState `json:"operatorVersionState"`
	SkewState             DoctorVersionState `json:"skewState"`
	ImageTagComparison    DoctorComparison   `json:"imageTagComparison"`
	ComparisonEvidence    EvidenceLevel      `json:"comparisonEvidence"`
}

type DoctorFeature struct {
	ID           string             `json:"id"`
	Availability DoctorAvailability `json:"availability"`
	Freshness    DoctorVersionState `json:"generationFreshness"`
	Evidence     EvidenceLevel      `json:"evidence"`
}

type DoctorContent struct {
	Context  DoctorContext   `json:"context"`
	Summary  DoctorSummary   `json:"summary"`
	APIs     []DoctorAPI     `json:"apis"`
	Reads    []DoctorRead    `json:"reads"`
	Version  DoctorVersion   `json:"version"`
	Features []DoctorFeature `json:"features"`
}

var doctorStableVersion = regexp.MustCompile(`^v?(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

// CanonicalDoctorVersion accepts only bounded stable numeric version candidates.
// It does not establish a running operator version or API compatibility.
func CanonicalDoctorVersion(value string) string {
	if len(value) > 64 || !doctorStableVersion.MatchString(value) {
		return ""
	}
	parts := strings.Split(strings.TrimPrefix(value, "v"), ".")
	for _, part := range parts {
		if _, err := strconv.ParseUint(part, 10, 64); err != nil {
			return ""
		}
	}
	return "v" + strings.Join(parts, ".")
}

// DoctorAPICatalog is a copy of the closed alpha diagnostic partition.
func DoctorAPICatalog() []DoctorAPI {
	result := []DoctorAPI{}
	add := func(gv, resource, kind string, namespaced, required bool) {
		scope := "Cluster"
		if namespaced {
			scope = "Namespaced"
		}
		result = append(result, DoctorAPI{ID: DoctorAPIID(gv + "/" + resource), GroupVersion: gv, Resource: resource, Kind: kind, Scope: scope, Required: required})
	}
	add("v1", "pods", "Pod", true, true)
	add("v1", "events", "Event", true, true)
	add("v1", "configmaps", "ConfigMap", true, true)
	add("apps/v1", "deployments", "Deployment", true, true)
	add("apps/v1", "controllerrevisions", "ControllerRevision", true, false)
	add("ome.io/v1beta1", "inferenceservices", "InferenceService", true, true)
	add("ome.io/v1beta1", "basemodels", "BaseModel", true, true)
	add("ome.io/v1beta1", "servingruntimes", "ServingRuntime", true, true)
	add("ome.io/v1beta1", "clusterbasemodels", "ClusterBaseModel", false, true)
	add("ome.io/v1beta1", "clusterservingruntimes", "ClusterServingRuntime", false, true)
	add("ome.io/v1beta1", "inferencereplicas", "InferenceReplica", true, false)
	add("ome.io/v1beta1", "autoscalerpolicies", "AutoscalerPolicy", true, false)
	add("ome.io/v1beta1", "trafficmaps", "TrafficMap", true, false)
	add("ome.io/v1beta1", "benchmarkjobs", "BenchmarkJob", true, false)
	add("ome.io/v1beta1", "finetunedweights", "FineTunedWeight", false, false)
	add("ome.io/v1beta1", "acceleratorclasses", "AcceleratorClass", false, false)
	add("ome.io/v1beta1", "acceleratorquotas", "AcceleratorQuota", false, false)
	add("ome.io/v1beta1", "workloadclusters", "WorkloadCluster", false, false)
	add("autoscaling/v2", "horizontalpodautoscalers", "HorizontalPodAutoscaler", true, false)
	add("keda.sh/v1alpha1", "scaledobjects", "ScaledObject", true, false)
	add("gateway.networking.k8s.io/v1", "httproutes", "HTTPRoute", true, false)
	add("gateway.networking.k8s.io/v1", "gateways", "Gateway", true, false)
	add("gateway.networking.k8s.io/v1", "gatewayclasses", "GatewayClass", false, false)
	add("kueue.x-k8s.io/v1beta1", "workloads", "Workload", true, false)
	add("kueue.x-k8s.io/v1beta1", "localqueues", "LocalQueue", true, false)
	add("kueue.x-k8s.io/v1beta1", "clusterqueues", "ClusterQueue", false, false)
	return result
}

func doctorReason(value DoctorReason) DoctorReason {
	return clusterEnum(value, DoctorUnreadable, "", DoctorNotFound, DoctorForbidden, DoctorUnauthorized, DoctorTimeout, DoctorThrottled, DoctorServerError, DoctorMalformed, DoctorTruncated)
}

func doctorName(value string, namespace bool) string {
	// These public markers must survive the repeated canonicalization used by
	// envelope and human rendering. They are never valid collector selections.
	if value == "[REDACTED]" || value == "[OMITTED]" {
		return value
	}
	if namespace {
		if len(validation.IsDNS1123Label(value)) == 0 {
			return safetext.Sanitize(value, 63)
		}
	} else if len(validation.IsDNS1123Subdomain(value)) == 0 {
		return safetext.Sanitize(value, 253)
	}
	return ""
}

func doctorFeatureIDs() []string {
	return []string{"Components", "Lifecycle", "Autoscaling", "ScaleTarget", "Traffic", "Canary", "Rollout", "RolloutCoordination", "Placement", "MigrationHistory", "RuntimePin"}
}

// DoctorFeatureIDs returns the fixed feature-presence subjects, not capabilities.
func DoctorFeatureIDs() []string { return doctorFeatureIDs() }

func (c DoctorContent) Canonical() DoctorContent {
	if strings.Contains(c.Context.Name, "://") {
		c.Context.Name = "[OMITTED]"
	} else {
		c.Context.Name = safetext.Sanitize(c.Context.Name, 128)
	}
	c.Context.WorkloadNamespace = doctorName(c.Context.WorkloadNamespace, true)
	c.Context.OMENamespace = doctorName(c.Context.OMENamespace, true)
	catalog := DoctorAPICatalog()
	apis := []DoctorAPI{}
	for _, row := range c.APIs {
		index := slices.IndexFunc(catalog, func(a DoctorAPI) bool { return a.ID == row.ID })
		if index < 0 {
			continue
		}
		fixed := catalog[index]
		fixed.Availability = clusterEnum(row.Availability, DoctorUnavailable, DoctorAvailable, DoctorNotDiscoverable)
		fixed.Reason = doctorReason(row.Reason)
		fixed.Evidence = EvidenceUnavailable
		if fixed.Availability == DoctorAvailable {
			fixed.Evidence = EvidenceObserved
		}
		apis = append(apis, fixed)
	}
	c.APIs = apis
	slices.SortFunc(c.APIs, func(a, b DoctorAPI) int {
		return cmp.Or(cmp.Compare(a.ID, b.ID), cmp.Compare(a.Availability, b.Availability), cmp.Compare(a.Reason, b.Reason))
	})
	reads := []DoctorRead{}
	for _, row := range c.Reads {
		if row.ID != DoctorReadManager && row.ID != DoctorReadISVC {
			continue
		}
		row.Method = "GET"
		row.Namespace = doctorName(row.Namespace, true)
		row.Name = doctorName(row.Name, false)
		row.GroupVersion, row.Resource = "ome.io/v1beta1", "inferenceservices"
		if row.ID == DoctorReadManager {
			row.GroupVersion, row.Resource, row.Name = "apps/v1", "deployments", "ome-controller-manager"
		}
		row.Outcome = clusterEnum(row.Outcome, DoctorUnavailable, DoctorAvailable, DoctorNotRequested)
		row.Reason = doctorReason(row.Reason)
		row.Evidence = EvidenceUnavailable
		if row.Outcome == DoctorAvailable {
			row.Evidence = EvidenceObserved
		}
		reads = append(reads, row)
	}
	c.Reads = reads
	slices.SortFunc(c.Reads, func(a, b DoctorRead) int {
		return cmp.Or(cmp.Compare(a.ID, b.ID), cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Name, b.Name), cmp.Compare(a.Outcome, b.Outcome), cmp.Compare(a.Reason, b.Reason))
	})
	features := []DoctorFeature{}
	for _, row := range c.Features {
		if !slices.Contains(doctorFeatureIDs(), row.ID) {
			continue
		}
		row.Availability = clusterEnum(row.Availability, DoctorUnavailable, DoctorNotSelected, DoctorPresent, DoctorAbsent)
		row.Freshness = DoctorUnverifiable
		row.Evidence = EvidenceUnavailable
		if row.Availability == DoctorPresent || row.Availability == DoctorAbsent {
			row.Evidence = EvidenceReported
		}
		features = append(features, row)
	}
	c.Features = features
	slices.SortFunc(c.Features, func(a, b DoctorFeature) int {
		return cmp.Or(cmp.Compare(a.ID, b.ID), cmp.Compare(a.Availability, b.Availability))
	})
	c.Version.ClientVersion = CanonicalDoctorVersion(c.Version.ClientVersion)
	c.Version.ClientVersionState = DoctorUnknown
	if c.Version.ClientVersion != "" {
		c.Version.ClientVersionState = DoctorKnownCandidate
	}
	c.Version.ImageState = clusterEnum(c.Version.ImageState, DoctorImageUnavailable, DoctorSelectedStableTag, DoctorNoManagerContainer, DoctorAmbiguousManager, DoctorUnversionedImage, DoctorDigestImage, DoctorNonCanonicalTag, DoctorMalformedImage)
	c.Version.ImageVersionCandidate = CanonicalDoctorVersion(c.Version.ImageVersionCandidate)
	if c.Version.ImageState != DoctorSelectedStableTag {
		c.Version.ImageVersionCandidate = ""
	}
	c.Version.OperatorVersionState, c.Version.SkewState = DoctorUnverifiable, DoctorUnverifiable
	c.Version.ImageTagComparison = clusterEnum(c.Version.ImageTagComparison, DoctorComparisonUnknown, DoctorWithinMinor, DoctorOutsideMinor, DoctorMajorMismatch)
	if c.Version.ClientVersion == "" || c.Version.ImageVersionCandidate == "" {
		c.Version.ImageTagComparison = DoctorComparisonUnknown
	}
	c.Version.ComparisonEvidence = EvidenceComputed
	c.Summary = DoctorSummary{State: "Complete"}
	// No canonical running compatibility source exists. Complete API reads do
	// not turn this unavailable diagnostic fact into a fully verified result.
	if c.Version.SkewState == DoctorUnverifiable {
		c.Summary.UnavailableEvidence++
	}
	for _, row := range c.APIs {
		if row.Required && row.Availability == DoctorNotDiscoverable {
			c.Summary.RequiredAPIViolations++
		}
		if row.Availability != DoctorAvailable {
			c.Summary.UnavailableEvidence++
		}
	}
	for _, row := range c.Reads {
		if row.Outcome != DoctorAvailable {
			c.Summary.UnavailableEvidence++
		}
	}
	for _, row := range c.Features {
		if row.Availability != DoctorPresent {
			c.Summary.UnavailableEvidence++
		}
	}
	if c.Summary.UnavailableEvidence > 0 || len(c.APIs) == 0 {
		c.Summary.State = "Incomplete"
	}
	if c.Summary.RequiredAPIViolations > 0 {
		c.Summary.State = "Violations"
	}
	return c
}

func (c DoctorContent) HasViolations() bool {
	for _, a := range c.Canonical().APIs {
		if a.Required && a.Availability == DoctorNotDiscoverable {
			return true
		}
	}
	return false
}

func (r DoctorReport) Canonical() DoctorReport {
	r.Envelope = r.Envelope.Canonical()
	r.Kind = "DoctorReport"
	r.Metadata = Metadata{Name: "doctor", Namespace: r.Content.Context.WorkloadNamespace}
	sources := []SourceReference{}
	for _, source := range r.Sources {
		switch source.Kind {
		case "APIResourceList":
			if !slices.ContainsFunc(DoctorAPICatalog(), func(a DoctorAPI) bool { return a.GroupVersion == source.Name }) {
				continue
			}
			source.Namespace = ""
		case "Deployment":
			if source.Name != "ome-controller-manager" {
				continue
			}
			source.Namespace = doctorName(source.Namespace, true)
		case "InferenceService":
			source.Name, source.Namespace = doctorName(source.Name, false), doctorName(source.Namespace, true)
			if source.Name == "" || source.Namespace == "" {
				continue
			}
		default:
			continue
		}
		source.UID, source.ResourceVersion = "", ""
		if source.Generation < 0 {
			source.Generation = 0
		}
		source.Evidence = clusterEnum(source.Evidence, EvidenceUnavailable, EvidenceObserved)
		source.UnavailableReason = clusterEnum(source.UnavailableReason, UnavailableUnreadable, "", UnavailableNotFound, UnavailableForbidden, UnavailableUnsupportedAPI, UnavailableMalformedPayload, UnavailableUnreadable)
		sources = append(sources, source)
	}
	r.Sources = sources
	warnings := []Warning{}
	for _, w := range r.Warnings {
		switch w.Code {
		case WarningSourceUnavailable:
			w.Message = "Diagnostic evidence is unavailable"
		case WarningPartialData:
			w.Message = "Diagnostic evidence is incomplete"
		case WarningTruncated:
			w.Message = "Decoded diagnostic source exceeded its scan limit"
		default:
			continue
		}
		warnings = append(warnings, w)
	}
	r.Warnings = warnings
	r.Envelope = r.Envelope.Canonical()
	return r
}

func (r DoctorReport) Table() report.Table      { return r.Canonical().Content.Table() }
func (c DoctorContent) Table() report.Table     { return c.table(false) }
func (c DoctorContent) WideTable() report.Table { return c.table(true) }
func (c DoctorContent) table(wide bool) report.Table {
	c = c.Canonical()
	t := report.Table{Headers: []string{"SUBJECT", "EVIDENCE", "DETAIL"}, Rows: [][]string{}}
	add := func(a, b, d string) {
		if wide {
			// Keep full evidence states while bounding physical buffered output.
			// Three columns plus padding total at most 80 display cells.
			a = printers.BoundedCell(a, 23)
			b = printers.BoundedCell(b, 24)
			d = printers.BoundedCell(d, 27)
		} else {
			a = printers.BoundedCell(a, 24)
			b = printers.BoundedCell(b, 19)
			d = printers.BoundedCell(d, 29)
		}
		t.Rows = append(t.Rows, []string{a, b, d})
	}
	add("Doctor", c.Summary.State, "GET-only; not authorization")
	add("Context", c.Context.Name, "Current kubeconfig context")
	add("Workload namespace", c.Context.WorkloadNamespace, "Only selected named GET")
	add("OME namespace", c.Context.OMENamespace, "Named manager Deployment GET")
	for _, a := range c.APIs {
		requirement := "Optional"
		if a.Required {
			requirement = "Required"
		}
		detail := requirement + " " + string(a.Reason)
		if wide {
			detail += " " + a.GroupVersion + " " + a.Kind + " " + a.Scope
		}
		add(a.Resource, string(a.Availability), detail)
	}
	for _, r := range c.Reads {
		detail := r.Namespace + "/" + r.Name + " " + string(r.Reason)
		if wide {
			detail = r.Method + " " + r.GroupVersion + " " + r.Resource + " " + detail
		}
		add(string(r.ID), string(r.Outcome), detail)
	}
	add("CLI version", c.Version.ClientVersion, string(c.Version.ClientVersionState))
	add("Manager image candidate", c.Version.ImageVersionCandidate, string(c.Version.ImageState))
	add("Operator compatibility", string(c.Version.SkewState), "No canonical running version")
	add("Image-tag comparison", string(c.Version.ImageTagComparison), "Computed; not compatibility")
	for _, f := range c.Features {
		add(f.ID, string(f.Availability), "Generation unverifiable")
	}
	if wide {
		add("Full values", "", "Use -o json or -o yaml")
	}
	return t
}
