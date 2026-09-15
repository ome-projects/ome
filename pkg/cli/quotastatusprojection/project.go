// Package quotastatusprojection projects bounded, allowlisted quota status.
// It performs no auxiliary or remote reads and never asserts enforcement.
package quotastatusprojection

import (
	"encoding/json"
	"errors"
	"math"
	"regexp"
	"sort"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	api "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	v "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

const inspectLimit = 64
const displayLimit = 16
const objectDisplayLimit = 64

var ErrInvalidSnapshot = errors.New("AcceleratorQuota status snapshot is invalid or incomplete")
var credentialShape = regexp.MustCompile(`(?:^|[^A-Za-z0-9])sk-[A-Za-z0-9_-]{20,}`)
var conditionReasonShape = regexp.MustCompile(`^[A-Za-z]([A-Za-z0-9_,:]*[A-Za-z0-9_])?$`)

type Input struct {
	Quotas        []api.AcceleratorQuota
	Target        string
	Scope         string
	ObservedPages int
	ObservedItems int
	Truncated     bool
}

func Project(in Input, clock v.Clock) (v.Envelope[v.QuotaStatusContent], error) {
	if in.Truncated || in.ObservedPages < 1 || in.ObservedPages > 2 || in.ObservedItems != len(in.Quotas) || len(in.Quotas) > 1000 || (in.Scope != "Named" && in.Scope != "Cluster") || (in.Target != "" && !validName(in.Target, 63)) || in.Scope == "Named" && (in.Target == "" || len(in.Quotas) != 1) {
		return v.Envelope[v.QuotaStatusContent]{}, ErrInvalidSnapshot
	}
	if clock == nil {
		clock = v.SystemClock{}
	}
	now := clock.Now().UTC()
	c := v.QuotaStatusContent{Target: in.Target, Snapshot: v.QuotaTreeSnapshot{Scope: in.Scope, Completeness: "Complete", ObservedPages: in.ObservedPages, ObservedItems: len(in.Quotas), Evidence: v.EvidenceObserved}, ObjectsGroup: group(len(in.Quotas))}
	seen := map[string]bool{}
	for _, q := range in.Quotas {
		if !validName(q.Name, 63) || q.Namespace != "" || q.Generation < 0 || seen[q.Name] || (in.Scope == "Named" && (len(in.Quotas) != 1 || q.Name != in.Target && in.Target != "")) {
			return v.Envelope[v.QuotaStatusContent]{}, ErrInvalidSnapshot
		}
		seen[q.Name] = true
		o := v.QuotaStatusObject{Name: q.Name, Generation: q.Generation, ObservedGeneration: q.Status.ObservedGeneration, Freshness: freshness(q.Generation, q.Status.ObservedGeneration), ParentState: "Unavailable", SourceGeneration: q.Status.SourceGeneration, SourceGenerationState: "Unknown", Evidence: v.EvidenceReported}
		if q.Status.Parent != "" {
			o.ParentState = "Invalid"
			if validName(q.Status.Parent, 63) {
				o.ReportedParent = q.Status.Parent
				o.ParentState = "Reported"
			}
		}
		if q.Status.SourceGeneration > 0 {
			o.SourceGenerationState = "Reported/Unverifiable"
		} else if q.Status.SourceGeneration < 0 {
			o.SourceGenerationState = "Invalid"
			o.SourceGeneration = 0
		}
		o.Budgets, o.BudgetsGroup = budgets(q.Status.Budgets)
		o.Capacity, o.CapacityGroup = capacity(q.Status.Capacity, q.Name, now)
		o.Projections, o.ProjectionsGroup = projections(q.Status.Clusters, q.Generation, now)
		o.Materialization, o.MaterializationGroup = materialization(q.Status.Materialization, q.Generation, now)
		o.Conditions, o.ConditionsGroup = conditions(q.Status.Conditions, q.Generation, now)
		c.Objects = append(c.Objects, o)
	}
	c = c.Canonical()
	if len(c.Objects) > objectDisplayLimit {
		c.Objects = c.Objects[:objectDisplayLimit]
		c.ObjectsGroup.Truncated = true
	}
	c.ObjectsGroup.Displayed = len(c.Objects)
	name := "root"
	if in.Target != "" {
		name = in.Target
	}
	out := v.NewEnvelope(v.QuotaStatusReportKind, v.Metadata{Name: name}, c, v.ClockFunc(func() time.Time { return now }))
	// Only displayed identities are retained in sources; acquisition counts
	// still describe the complete independently validated snapshot.
	for _, q := range c.Objects {
		out.Sources = append(out.Sources, v.SourceReference{Kind: "AcceleratorQuota", Name: q.Name, Generation: q.Generation, Evidence: v.EvidenceReported, CollectedAt: now})
	}
	out.Warnings = []v.Warning{{Code: "ReportedOnly", Message: "Status is reported evidence, not independent proof of controller installation, admission, enforcement, or free capacity. Zero optional scalars have unknown wire presence; sourceGeneration is not comparable to local generation. HighWaterMark is historical. Sample timestamps alone do not establish a configured freshness SLA."}}
	return out.Canonical(), nil
}

func validName(s string, max int) bool {
	return len(s) <= max && len(validation.IsDNS1123Subdomain(s)) == 0 && !credentialShape.MatchString(s)
}
func validResource(s string) bool {
	return len(s) <= 253 && len(validation.IsQualifiedName(s)) == 0 && !credentialShape.MatchString(s)
}
func freshness(expected, observed int64) string {
	if expected < 0 || observed < 0 || expected > 0 && observed > expected {
		return "Inconsistent"
	}
	if expected == 0 || observed == 0 {
		return "Unknown"
	}
	if observed < expected {
		return "Stale"
	}
	return "Current"
}
func group(n int) v.QuotaStatusGroup {
	if n == 0 {
		return v.QuotaStatusGroup{State: "Unavailable", Reason: "NotReported", Evidence: v.EvidenceUnavailable}
	}
	return v.QuotaStatusGroup{State: "Available", Reason: "Reported", Evidence: v.EvidenceReported, Observed: n, Displayed: n}
}
func invalid(n int, reason string) v.QuotaStatusGroup {
	return v.QuotaStatusGroup{State: "Invalid", Reason: reason, Evidence: v.EvidenceUnavailable, Observed: n}
}
func trim[T any](values []T, g v.QuotaStatusGroup) ([]T, v.QuotaStatusGroup) {
	sort.Slice(values, func(i, j int) bool { return key(values[i]) < key(values[j]) })
	if len(values) > displayLimit {
		values = values[:displayLimit]
		g.Truncated = true
	}
	g.Displayed = len(values)
	return values, g
}
func key[T any](value T) string { data, _ := json.Marshal(value); return string(data) }
func validQuantity(q resource.Quantity) bool {
	s := q.String()
	value := q.AsApproximateFloat64()
	return len(s) <= 64 && q.Sign() >= 0 && !math.IsInf(value, 0) && !math.IsNaN(value)
}
func quantity(q resource.Quantity) string {
	if q.IsZero() {
		return "Unknown/Defaulted"
	}
	return q.String()
}
func usage(n, a, r, b resource.Quantity) (v.QuotaStatusUsage, bool) {
	if !validQuantity(n) || !validQuantity(a) || !validQuantity(r) || !validQuantity(b) {
		return v.QuotaStatusUsage{}, false
	}
	// A zero optional scalar has no provable wire presence. Only positive
	// supplied values establish these comparisons independently.
	if !a.IsZero() && !r.IsZero() && r.Cmp(a) < 0 || !a.IsZero() && !b.IsZero() && b.Cmp(a) > 0 {
		return v.QuotaStatusUsage{}, false
	}
	return v.QuotaStatusUsage{Nominal: quantity(n), Admitted: quantity(a), Reserved: quantity(r), Borrowed: quantity(b)}, true
}
func timeValue(t *metav1.Time, now time.Time) (string, string, bool) {
	if t == nil || t.IsZero() {
		return "", "Unknown", true
	}
	if t.Year() < 1 || t.Year() > 9999 || t.After(now) {
		return "", "Invalid", false
	}
	return t.UTC().Format(time.RFC3339Nano), "Reported/Unverifiable", true
}

func clusterUsage(in []api.AcceleratorClusterBudgetStatus) ([]v.QuotaStatusClusterUsage, v.QuotaStatusGroup) {
	g := group(len(in))
	if len(in) > inspectLimit {
		return nil, invalid(len(in), "Oversize")
	}
	seen := map[string]bool{}
	out := []v.QuotaStatusClusterUsage{}
	for _, s := range in {
		u, ok := usage(s.Nominal, s.Admitted, s.Reserved, s.Borrowed)
		if !ok || !validName(s.Cluster, 253) || seen[s.Cluster] {
			return nil, invalid(len(in), "MalformedPayload")
		}
		seen[s.Cluster] = true
		out = append(out, v.QuotaStatusClusterUsage{Cluster: s.Cluster, Usage: u})
	}
	return trim(out, g)
}
func budgets(in []api.AcceleratorBudgetStatus) ([]v.QuotaStatusBudget, v.QuotaStatusGroup) {
	g := group(len(in))
	if len(in) > inspectLimit {
		return nil, invalid(len(in), "Oversize")
	}
	seen := map[string]bool{}
	out := []v.QuotaStatusBudget{}
	for _, s := range in {
		id := key([2]string{s.ResourceName, s.ResourceFlavor})
		u, ok := usage(s.Nominal, s.Admitted, s.Reserved, s.Borrowed)
		if !ok || !validResource(s.ResourceName) || !validName(s.ResourceFlavor, 253) || seen[id] {
			return nil, invalid(len(in), "MalformedPayload")
		}
		seen[id] = true
		clusters, cg := clusterUsage(s.PerCluster)
		out = append(out, v.QuotaStatusBudget{ResourceName: s.ResourceName, ResourceFlavor: s.ResourceFlavor, Usage: u, PerClusterGroup: cg, PerCluster: clusters})
	}
	return trim(out, g)
}
func clusterCapacity(in []api.AcceleratorClusterCapacityStatus, now time.Time) ([]v.QuotaStatusClusterCapacity, v.QuotaStatusGroup) {
	g := group(len(in))
	if len(in) > inspectLimit {
		return nil, invalid(len(in), "Oversize")
	}
	seen := map[string]bool{}
	out := []v.QuotaStatusClusterCapacity{}
	for _, s := range in {
		stamp, state, ok := timeValue(s.ObservedAt, now)
		if !ok || !validName(s.Cluster, 253) || seen[s.Cluster] || !validQuantity(s.Allocatable) || !validQuantity(s.HighWaterMark) || !s.Allocatable.IsZero() && !s.HighWaterMark.IsZero() && s.HighWaterMark.Cmp(s.Allocatable) < 0 {
			return nil, invalid(len(in), "MalformedPayload")
		}
		seen[s.Cluster] = true
		out = append(out, v.QuotaStatusClusterCapacity{Cluster: s.Cluster, Allocatable: quantity(s.Allocatable), HighWaterMark: quantity(s.HighWaterMark), ObservedAt: stamp, SampleState: state})
	}
	return trim(out, g)
}
func capacity(in []api.AcceleratorCapacityStatus, name string, now time.Time) ([]v.QuotaStatusCapacity, v.QuotaStatusGroup) {
	g := group(len(in))
	if len(in) > inspectLimit {
		return nil, invalid(len(in), "Oversize")
	}
	if len(in) > 0 && name != api.AcceleratorQuotaRootName {
		return nil, invalid(len(in), "UnexpectedNonRootCapacity")
	}
	seen := map[string]bool{}
	out := []v.QuotaStatusCapacity{}
	for _, s := range in {
		id := key([2]string{s.ResourceName, s.ResourceFlavor})
		stamp, state, ok := timeValue(s.ObservedAt, now)
		if !ok || !validResource(s.ResourceName) || !validName(s.ResourceFlavor, 253) || seen[id] || !validQuantity(s.Allocatable) || !validQuantity(s.HighWaterMark) || !s.Allocatable.IsZero() && !s.HighWaterMark.IsZero() && s.HighWaterMark.Cmp(s.Allocatable) < 0 {
			return nil, invalid(len(in), "MalformedPayload")
		}
		seen[id] = true
		clusters, cg := clusterCapacity(s.PerCluster, now)
		out = append(out, v.QuotaStatusCapacity{ResourceName: s.ResourceName, ResourceFlavor: s.ResourceFlavor, Allocatable: quantity(s.Allocatable), HighWaterMark: quantity(s.HighWaterMark), ObservedAt: stamp, SampleState: state, PerClusterGroup: cg, PerCluster: clusters})
	}
	return trim(out, g)
}
func projections(in []api.AcceleratorQuotaClusterStatus, generation int64, now time.Time) ([]v.QuotaStatusProjection, v.QuotaStatusGroup) {
	g := group(len(in))
	if len(in) > inspectLimit {
		return nil, invalid(len(in), "Oversize")
	}
	seen := map[string]bool{}
	out := []v.QuotaStatusProjection{}
	for _, s := range in {
		stamp, _, ok := timeValue(s.AppliedTime, now)
		projection := freshness(generation, s.AppliedGeneration)
		materialized := freshness(s.AppliedGeneration, s.MaterializedGeneration)
		if !ok || !validName(s.Cluster, 253) || seen[s.Cluster] || projection == "Inconsistent" || materialized == "Inconsistent" || s.AppliedGeneration == 0 && s.MaterializedGeneration > 0 {
			return nil, invalid(len(in), "MalformedPayload")
		}
		seen[s.Cluster] = true
		out = append(out, v.QuotaStatusProjection{Cluster: s.Cluster, AppliedGeneration: s.AppliedGeneration, AppliedTime: stamp, ProjectionFreshness: projection, MaterializedGeneration: s.MaterializedGeneration, MaterializationFreshness: materialized})
	}
	return trim(out, g)
}
func materialization(s *api.AcceleratorQuotaMaterialization, generation int64, now time.Time) (*v.QuotaStatusMaterialization, v.QuotaStatusGroup) {
	if s == nil {
		return nil, group(0)
	}
	frozenAt, _, ok := timeValue(s.FrozenAt, now)
	lastApplied, _, lastOK := timeValue(s.LastAppliedTime, now)
	fresh := freshness(generation, s.LastAppliedGeneration)
	if !ok || !lastOK || fresh == "Inconsistent" || !s.Frozen && s.FrozenAt != nil && !s.FrozenAt.IsZero() || s.FrozenAt != nil && s.LastAppliedTime != nil && !s.FrozenAt.IsZero() && !s.LastAppliedTime.IsZero() && s.LastAppliedTime.After(s.FrozenAt.Time) {
		return nil, invalid(1, "MalformedPayload")
	}
	state := "Unknown/Defaulted"
	if s.Frozen {
		state = "Frozen"
	}
	return &v.QuotaStatusMaterialization{FreezeState: state, FrozenAt: frozenAt, Reason: reason(s.Reason), LastAppliedGeneration: s.LastAppliedGeneration, LastAppliedTime: lastApplied, Freshness: fresh}, group(1)
}
func conditions(in []metav1.Condition, generation int64, now time.Time) ([]v.QuotaStatusCondition, v.QuotaStatusGroup) {
	g := group(len(in))
	if len(in) > inspectLimit {
		return nil, invalid(len(in), "Oversize")
	}
	seen := map[string]bool{}
	out := []v.QuotaStatusCondition{}
	for _, s := range in {
		// Unknown condition types are not part of this schema, but input sizes
		// and duplicate/type/status contradictions are validated before omission.
		if len(s.Type) > 64 || len(validation.IsQualifiedName(s.Type)) > 0 || seen[s.Type] || s.LastTransitionTime.IsZero() || len(s.Reason) < 1 || len(s.Reason) > 1024 || !conditionReasonShape.MatchString(s.Reason) || len(s.Message) > 32768 || s.Status != metav1.ConditionTrue && s.Status != metav1.ConditionFalse && s.Status != metav1.ConditionUnknown {
			return nil, invalid(len(in), "MalformedPayload")
		}
		seen[s.Type] = true
		stamp, _, ok := timeValue(&s.LastTransitionTime, now)
		fresh := freshness(generation, s.ObservedGeneration)
		if !ok || fresh == "Inconsistent" {
			return nil, invalid(len(in), "MalformedPayload")
		}
		if s.Type != api.AcceleratorQuotaReady && s.Type != api.AcceleratorQuotaDegraded && s.Type != api.AcceleratorQuotaMaterialized {
			continue
		}
		out = append(out, v.QuotaStatusCondition{Type: s.Type, Status: string(s.Status), Reason: reason(s.Reason), ObservedGeneration: s.ObservedGeneration, Freshness: fresh, LastTransitionTime: stamp})
	}
	if len(out) == 0 && len(in) > 0 {
		g.State = "Unavailable"
		g.Reason = "NoSupportedConditions"
		g.Evidence = v.EvidenceUnavailable
	} else if len(out) < len(in) {
		g.Reason = "UnsupportedTypesOmitted"
	}
	// The controller writes Ready/Degraded as a mutually exclusive pair.
	// Different or absent observed generations cannot establish the same pass.
	var ready, degraded *v.QuotaStatusCondition
	for i := range out {
		if out[i].Type == api.AcceleratorQuotaReady {
			ready = &out[i]
		}
		if out[i].Type == api.AcceleratorQuotaDegraded {
			degraded = &out[i]
		}
	}
	if ready != nil && degraded != nil && ready.ObservedGeneration > 0 && ready.ObservedGeneration == degraded.ObservedGeneration && ready.Status == degraded.Status && ready.Status != string(metav1.ConditionUnknown) {
		return nil, invalid(len(in), "MalformedPayload")
	}
	return trim(out, g)
}
func reason(s string) string {
	switch s {
	case api.AcceleratorQuotaReasonAdmitted, api.AcceleratorQuotaReasonParentMissing, api.AcceleratorQuotaReasonContainmentViolated, api.AcceleratorQuotaReasonCapacityExceeded, api.AcceleratorQuotaReasonFlavorMissing, api.AcceleratorQuotaReasonShareUnresolved, api.AcceleratorQuotaReasonClusterUnreachable, api.AcceleratorQuotaReasonFrozen, api.AcceleratorQuotaReasonMaterializationFailed, api.AcceleratorQuotaReasonObjectConflict, api.AcceleratorQuotaReasonParentCycle, api.AcceleratorQuotaReasonUnreachable, api.AcceleratorQuotaReasonNodeKindInvalid, api.AcceleratorQuotaReasonDepthExceeded, api.AcceleratorQuotaReasonDuplicateNode:
		return s
	case "":
		return "Unknown"
	default:
		return "Other"
	}
}
