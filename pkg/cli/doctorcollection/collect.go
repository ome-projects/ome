// Package doctorcollection collects fixed current-context read evidence.
// It has no writes, credential reads, discovery fanout or remote executor.
package doctorcollection

import (
	"context"
	"errors"
	"net"
	"strings"
	"time"
	"unicode"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/rest"
	omev1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	r "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

const (
	RequestTimeout       = 10 * time.Second
	CollectionTimeout    = 30 * time.Second
	MaxManagerContainers = 32
	MaxImageBytes        = 2048
)

type Selection struct{ ContextName, WorkloadNamespace, OMENamespace, ISVCName string }

// Snapshot owns only safe facts. Raw API objects/errors never cross this seam.
type Snapshot struct {
	Selection Selection
	APIs      []r.DoctorAPI
	Reads     []r.DoctorRead
	Manager   r.DoctorManagerEvidence
	Features  []r.DoctorFeature
	Sources   []r.SourceReference
	Warnings  []r.Warning
}

// Collect makes at most eight fixed GET API calls, or nine with a named ISVC.
// Each request disables Retry-After retries and honors a smaller caller timeout.
// External exec credentials/custom transports remain outside this API-call bound.
func Collect(ctx context.Context, clients Clients, selected Selection, timeout time.Duration) (Snapshot, error) {
	s := Snapshot{Selection: selected, APIs: []r.DoctorAPI{}, Reads: []r.DoctorRead{}, Features: []r.DoctorFeature{}, Sources: []r.SourceReference{}, Warnings: []r.Warning{}, Manager: r.DoctorManagerEvidence{ImageState: r.DoctorImageUnavailable}}
	if err := ctx.Err(); err != nil {
		return s, err
	}
	if timeout <= 0 || timeout > RequestTimeout {
		return s, errors.New("invalid doctor request timeout")
	}
	if len(validation.IsDNS1123Label(selected.WorkloadNamespace)) != 0 || len(validation.IsDNS1123Label(selected.OMENamespace)) != 0 || selected.ISVCName != "" && len(validation.IsDNS1123Subdomain(selected.ISVCName)) != 0 {
		return s, errors.New("invalid doctor source selection")
	}
	if !clients.valid(selected.ISVCName != "") {
		return s, errors.New("doctor API clients unavailable")
	}
	safeContext := (r.DoctorContent{Context: r.DoctorContext{Name: selected.ContextName, WorkloadNamespace: selected.WorkloadNamespace, OMENamespace: selected.OMENamespace}}).Canonical().Context
	s.Selection.ContextName = safeContext.Name
	collection, cancel := context.WithTimeout(ctx, CollectionTimeout)
	defer cancel()
	for _, group := range discoveryGroups {
		if err := collection.Err(); err != nil {
			return s, err
		}
		document := &metav1.APIResourceList{}
		reason := read(collection, timeout, clients.discovery.Get().AbsPath(group.path).SetHeader("Accept", "application/json"), document)
		if err := collection.Err(); err != nil {
			return s, err
		}
		rows := discoveryRows(group.version, document, reason)
		s.APIs = append(s.APIs, rows...)
		source := r.SourceReference{Kind: "APIResourceList", Name: group.version, Evidence: r.EvidenceObserved}
		if reason != "" {
			source.Evidence = r.EvidenceUnavailable
			source.UnavailableReason = unavailable(reason)
		}
		// A malformed/oversized successful response is unavailable, too.
		if reason == "" && (document.GroupVersion != group.version || len(document.APIResources) > MaxDiscoveryResources) {
			source.Evidence = r.EvidenceUnavailable
			source.UnavailableReason = r.UnavailableMalformedPayload
		}
		s.Sources = append(s.Sources, source)
		for _, row := range rows {
			if row.Availability != r.DoctorAvailable {
				s.Warnings = append(s.Warnings, r.Warning{Code: r.WarningSourceUnavailable})
				break
			}
		}
		if len(document.APIResources) > MaxDiscoveryResources {
			s.Warnings = append(s.Warnings, r.Warning{Code: r.WarningTruncated})
		}
	}
	if err := collection.Err(); err != nil {
		return s, err
	}
	dep := &appsv1.Deployment{}
	reason := read(collection, timeout, clients.apps.Get().Namespace(selected.OMENamespace).Resource("deployments").Name("ome-controller-manager"), dep)
	if err := collection.Err(); err != nil {
		return s, err
	}
	if reason == "" && (dep.Name != "ome-controller-manager" || dep.Namespace != selected.OMENamespace) {
		reason = r.DoctorMalformed
	}
	managerRead := r.DoctorRead{ID: r.DoctorReadManager, Method: "GET", GroupVersion: "apps/v1", Resource: "deployments", Namespace: selected.OMENamespace, Name: "ome-controller-manager", Outcome: r.DoctorAvailable, Evidence: r.EvidenceObserved}
	var managerGeneration int64
	if reason != "" {
		managerRead.Outcome, managerRead.Reason, managerRead.Evidence = r.DoctorUnavailable, reason, r.EvidenceUnavailable
		s.Warnings = append(s.Warnings, r.Warning{Code: r.WarningSourceUnavailable})
	} else {
		managerGeneration = nonnegative(dep.Generation)
		s.Manager = managerEvidence(dep)
		if len(dep.Spec.Template.Spec.Containers) > MaxManagerContainers {
			s.Warnings = append(s.Warnings, r.Warning{Code: r.WarningTruncated})
		}
	}
	s.Reads = append(s.Reads, managerRead)
	s.Sources = append(s.Sources, r.SourceReference{Kind: "Deployment", Namespace: selected.OMENamespace, Name: "ome-controller-manager", Generation: managerGeneration, Evidence: managerRead.Evidence, UnavailableReason: unavailable(reason)})
	isvcRead := r.DoctorRead{ID: r.DoctorReadISVC, Method: "GET", GroupVersion: "ome.io/v1beta1", Resource: "inferenceservices", Namespace: selected.WorkloadNamespace, Name: selected.ISVCName, Outcome: r.DoctorNotRequested, Evidence: r.EvidenceUnavailable}
	if selected.ISVCName == "" {
		s.Reads = append(s.Reads, isvcRead)
		s.Features = featureEvidence(nil)
		return s, nil
	}
	if err := collection.Err(); err != nil {
		return s, err
	}
	isvc := &omev1.InferenceService{}
	reason = read(collection, timeout, clients.ome.Get().Namespace(selected.WorkloadNamespace).Resource("inferenceservices").Name(selected.ISVCName), isvc)
	if err := collection.Err(); err != nil {
		return s, err
	}
	if reason == "" && (isvc.Name != selected.ISVCName || isvc.Namespace != selected.WorkloadNamespace || isvc.UID == "") {
		reason = r.DoctorMalformed
	}
	if reason != "" {
		return s, errors.New("selected InferenceService unavailable")
	}
	isvcRead.Outcome, isvcRead.Evidence = r.DoctorAvailable, r.EvidenceObserved
	s.Reads = append(s.Reads, isvcRead)
	s.Sources = append(s.Sources, r.SourceReference{Kind: "InferenceService", Name: selected.ISVCName, Namespace: selected.WorkloadNamespace, Generation: nonnegative(isvc.Generation), Evidence: r.EvidenceObserved})
	s.Features = featureEvidence(isvc)
	return s, nil
}

func read(ctx context.Context, timeout time.Duration, request *rest.Request, target runtime.Object) r.DoctorReason {
	call, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	result := request.WarningHandlerWithContext(rest.NoWarnings{}).MaxRetries(0).Do(call)
	if call.Err() != nil {
		return r.DoctorTimeout
	}
	if err := result.Error(); err != nil {
		return classify(err)
	}
	if err := result.Into(target); err != nil {
		return r.DoctorMalformed
	}
	if call.Err() != nil {
		return r.DoctorTimeout
	}
	return ""
}

func classify(err error) r.DoctorReason {
	var network net.Error
	switch {
	case apierrors.IsNotFound(err):
		return r.DoctorNotFound
	case apierrors.IsForbidden(err):
		return r.DoctorForbidden
	case apierrors.IsUnauthorized(err):
		return r.DoctorUnauthorized
	case errors.Is(err, context.DeadlineExceeded), apierrors.IsTimeout(err), apierrors.IsServerTimeout(err):
		return r.DoctorTimeout
	case apierrors.IsTooManyRequests(err):
		return r.DoctorThrottled
	case apierrors.IsInternalError(err), apierrors.IsServiceUnavailable(err):
		return r.DoctorServerError
	case errors.As(err, &network) && network.Timeout():
		return r.DoctorTimeout
	default:
		return r.DoctorUnreadable
	}
}

func unavailable(reason r.DoctorReason) r.UnavailableReason {
	switch reason {
	case "":
		return ""
	case r.DoctorNotFound:
		return r.UnavailableNotFound
	case r.DoctorForbidden, r.DoctorUnauthorized:
		return r.UnavailableForbidden
	case r.DoctorMalformed, r.DoctorTruncated:
		return r.UnavailableMalformedPayload
	default:
		return r.UnavailableUnreadable
	}
}
func nonnegative(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}

func managerEvidence(dep *appsv1.Deployment) r.DoctorManagerEvidence {
	fact := r.DoctorManagerEvidence{ImageState: r.DoctorImageUnavailable}
	containers := dep.Spec.Template.Spec.Containers
	if len(containers) > MaxManagerContainers {
		return fact
	}
	count := 0
	image := ""
	for _, container := range containers {
		if container.Name == "manager" {
			count++
			image = container.Image
		}
	}
	if count == 0 {
		fact.ImageState = r.DoctorNoManagerContainer
		return fact
	}
	if count != 1 {
		fact.ImageState = r.DoctorAmbiguousManager
		return fact
	}
	if len(image) > MaxImageBytes {
		return fact
	}
	if image == "" || strings.ContainsFunc(image, unicode.IsSpace) || strings.Contains(image, "://") {
		fact.ImageState = r.DoctorMalformedImage
		return fact
	}
	if strings.Contains(image, "@") {
		fact.ImageState = r.DoctorDigestImage
		return fact
	}
	colon, slash := strings.LastIndex(image, ":"), strings.LastIndex(image, "/")
	if colon <= slash || colon < 0 {
		fact.ImageState = r.DoctorUnversionedImage
		return fact
	}
	if colon == 0 || colon == len(image)-1 {
		fact.ImageState = r.DoctorMalformedImage
		return fact
	}
	fact.ImageVersionCandidate = r.CanonicalDoctorVersion(image[colon+1:])
	fact.ImageState = r.DoctorNonCanonicalTag
	if fact.ImageVersionCandidate != "" {
		fact.ImageState = r.DoctorSelectedStableTag
	}
	return fact
}

func featureEvidence(isvc *omev1.InferenceService) []r.DoctorFeature {
	rows := []r.DoctorFeature{}
	presence := map[string]bool{}
	if isvc != nil {
		presence["Traffic"] = isvc.Status.Traffic != nil
		presence["Canary"] = isvc.Status.Canary != nil
		presence["Rollout"] = isvc.Status.Rollout != nil
		presence["RolloutCoordination"] = isvc.Status.RolloutCoordination != nil
		presence["Placement"] = isvc.Status.Placement != nil
		presence["MigrationHistory"] = len(isvc.Status.MigrationHistory) > 0
		presence["RuntimePin"] = isvc.Status.PinnedRevisionName != "" || isvc.Status.LastRuntimeSyncToken != ""
		for _, component := range []omev1.ComponentType{omev1.EngineComponent, omev1.DecoderComponent, omev1.RouterComponent} {
			if status, found := isvc.Status.Components[component]; found {
				presence["Components"] = true
				presence["Lifecycle"] = presence["Lifecycle"] || status.Lifecycle != nil
				presence["Autoscaling"] = presence["Autoscaling"] || status.Autoscaler != nil
				presence["ScaleTarget"] = presence["ScaleTarget"] || status.ScaleTargetRef != nil
			}
		}
	}
	for _, id := range r.DoctorFeatureIDs() {
		row := r.DoctorFeature{ID: id, Availability: r.DoctorNotSelected, Freshness: r.DoctorUnverifiable, Evidence: r.EvidenceUnavailable}
		if isvc != nil {
			row.Availability, row.Evidence = r.DoctorAbsent, r.EvidenceReported
			if presence[id] {
				row.Availability = r.DoctorPresent
			}
		}
		rows = append(rows, row)
	}
	return rows
}
