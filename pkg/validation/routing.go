package validation

import (
	"fmt"
	"strings"
	"time"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// ValidateRouting validates the per-InferenceService routing policy.
func ValidateRouting(spec *v1beta1.InferenceServiceSpec) error {
	if spec == nil {
		return nil
	}
	if err := validateRouterCapacityEndpoint(spec); err != nil {
		return err
	}
	if spec.Routing == nil {
		return nil
	}
	for cluster, factor := range spec.Routing.CapacityFactors {
		if factor.Sign() <= 0 {
			return fmt.Errorf("spec.routing.capacityFactors[%q] must be positive, got %q", cluster, factor.String())
		}
	}
	if err := validateRoutingProbe(spec.Routing.Probe); err != nil {
		return err
	}
	if err := validateRoutingCapacity(spec.Routing.Capacity); err != nil {
		return err
	}
	return validateRoutingPublisher(spec.Routing.Publisher)
}

func validateRoutingProbe(probe *v1beta1.RoutingProbeSpec) error {
	if probe == nil {
		return nil
	}
	if probe.Disabled {
		if routingProbeHasOperationalFields(probe) {
			return fmt.Errorf("spec.routing.probe.disabled must not be combined with probe configuration fields")
		}
		return nil
	}
	if probe.Path == "" {
		return fmt.Errorf("spec.routing.probe.path is required")
	}
	if !strings.HasPrefix(probe.Path, "/") {
		return fmt.Errorf("spec.routing.probe.path %q must begin with %q", probe.Path, "/")
	}
	if probe.Method == "" {
		return fmt.Errorf("spec.routing.probe.method is required")
	}
	if !isSafeRoutingHTTPMethod(probe.Method) {
		return fmt.Errorf("spec.routing.probe.method %q must be GET, HEAD, or POST", probe.Method)
	}
	if len(probe.AcceptStatuses) == 0 {
		return fmt.Errorf("spec.routing.probe.acceptStatuses is required")
	}
	if len(probe.GateStatuses) == 0 {
		return fmt.Errorf("spec.routing.probe.gateStatuses is required")
	}
	accepted := make(map[int32]struct{}, len(probe.AcceptStatuses))
	for _, status := range probe.AcceptStatuses {
		if err := validateHTTPStatus("spec.routing.probe.acceptStatuses", status); err != nil {
			return err
		}
		accepted[status] = struct{}{}
	}
	for _, status := range probe.GateStatuses {
		if err := validateHTTPStatus("spec.routing.probe.gateStatuses", status); err != nil {
			return err
		}
		if status == 401 || status == 403 || status == 429 {
			return fmt.Errorf("spec.routing.probe.gateStatuses must not contain %d", status)
		}
		if _, found := accepted[status]; found {
			return fmt.Errorf("spec.routing.probe status %d is in both acceptStatuses and gateStatuses", status)
		}
	}
	if probe.Period.Duration <= 0 {
		return fmt.Errorf("spec.routing.probe.period must be positive")
	}
	if probe.Timeout.Duration <= 0 {
		return fmt.Errorf("spec.routing.probe.timeout must be positive")
	}
	if probe.Timeout.Duration >= probe.Period.Duration {
		return fmt.Errorf("spec.routing.probe.timeout (%s) must be shorter than period (%s)",
			probe.Timeout.Duration, probe.Period.Duration)
	}
	if probe.FailureThreshold <= 0 {
		return fmt.Errorf("spec.routing.probe.failureThreshold must be positive")
	}
	if probe.SuccessThreshold <= 0 {
		return fmt.Errorf("spec.routing.probe.successThreshold must be positive")
	}
	switch probe.AllFailedPolicy {
	case v1beta1.RoutingAllFailedPolicyPreserveTraffic, v1beta1.RoutingAllFailedPolicyDrain:
	default:
		return fmt.Errorf("spec.routing.probe.allFailedPolicy %q must be %q or %q",
			probe.AllFailedPolicy,
			v1beta1.RoutingAllFailedPolicyPreserveTraffic,
			v1beta1.RoutingAllFailedPolicyDrain)
	}
	return nil
}

func routingProbeHasOperationalFields(probe *v1beta1.RoutingProbeSpec) bool {
	return probe.Path != "" ||
		probe.Method != "" ||
		len(probe.AcceptStatuses) != 0 ||
		len(probe.GateStatuses) != 0 ||
		probe.Period.Duration != 0 ||
		probe.Timeout.Duration != 0 ||
		probe.FailureThreshold != 0 ||
		probe.SuccessThreshold != 0 ||
		probe.AllFailedPolicy != ""
}

func validateRoutingCapacity(capacity *v1beta1.RoutingCapacitySpec) error {
	if capacity == nil {
		return nil
	}
	if capacity.Disabled {
		if routingCapacityHasOperationalFields(capacity) {
			return fmt.Errorf("spec.routing.capacity.disabled must not be combined with capacity configuration fields")
		}
		return nil
	}
	if capacity.Mode != "" && capacity.Mode != v1beta1.RoutingCapacityModeCeiling && capacity.Mode != v1beta1.RoutingCapacityModeReported {
		return fmt.Errorf("spec.routing.capacity.mode %q must be Ceiling or Reported", capacity.Mode)
	}
	if capacity.Path == "" {
		return fmt.Errorf("spec.routing.capacity.path is required")
	}
	if !strings.HasPrefix(capacity.Path, "/") {
		return fmt.Errorf("spec.routing.capacity.path %q must begin with %q", capacity.Path, "/")
	}
	if capacity.Method == "" {
		return fmt.Errorf("spec.routing.capacity.method is required")
	}
	if !isSafeRoutingHTTPMethod(capacity.Method) {
		return fmt.Errorf("spec.routing.capacity.method %q must be GET, HEAD, or POST", capacity.Method)
	}
	if capacity.Format == "" {
		return fmt.Errorf("spec.routing.capacity.format is required")
	}
	if capacity.Period.Duration <= 0 {
		return fmt.Errorf("spec.routing.capacity.period must be positive")
	}
	if capacity.Timeout.Duration <= 0 {
		return fmt.Errorf("spec.routing.capacity.timeout must be positive")
	}
	if capacity.Timeout.Duration >= capacity.Period.Duration {
		return fmt.Errorf("spec.routing.capacity.timeout (%s) must be shorter than period (%s)",
			capacity.Timeout.Duration, capacity.Period.Duration)
	}
	if capacity.Samples <= 0 {
		return fmt.Errorf("spec.routing.capacity.samples must be positive")
	}
	if capacity.Quorum <= 0 {
		return fmt.Errorf("spec.routing.capacity.quorum must be positive")
	}
	if capacity.Quorum > capacity.Samples {
		return fmt.Errorf("spec.routing.capacity.quorum (%d) must not exceed samples (%d)",
			capacity.Quorum, capacity.Samples)
	}
	if capacity.MaxAge.Duration <= 0 {
		return fmt.Errorf("spec.routing.capacity.maxAge must be positive")
	}
	if capacity.MaxAge.Duration < capacity.Period.Duration {
		return fmt.Errorf("spec.routing.capacity.maxAge (%s) must be at least period (%s)",
			capacity.MaxAge.Duration, capacity.Period.Duration)
	}
	if capacity.Quorum > 1 {
		intervals := time.Duration(capacity.Quorum)
		wholePeriods := capacity.MaxAge.Duration / capacity.Period.Duration
		if wholePeriods < intervals {
			return fmt.Errorf("spec.routing.capacity.maxAge (%s) must span at least quorum (%d) period intervals",
				capacity.MaxAge.Duration, capacity.Quorum)
		}
	}
	return nil
}

func routingCapacityHasOperationalFields(capacity *v1beta1.RoutingCapacitySpec) bool {
	return capacity.Mode != "" || capacity.Path != "" ||
		capacity.Method != "" ||
		capacity.Format != "" ||
		len(capacity.Options) != 0 ||
		capacity.Period.Duration != 0 ||
		capacity.Timeout.Duration != 0 ||
		capacity.Samples != 0 ||
		capacity.Quorum != 0 ||
		capacity.MaxAge.Duration != 0
}

func validateHTTPStatus(field string, status int32) error {
	if status < 100 || status > 599 {
		return fmt.Errorf("%s contains invalid HTTP status %d; statuses must be between 100 and 599", field, status)
	}
	return nil
}

func isSafeRoutingHTTPMethod(method string) bool {
	switch method {
	case "GET", "HEAD", "POST":
		return true
	default:
		return false
	}
}

// ValidateRoutingPublisherOptionAnnotations checks the
// ome.io/routing-publisher.<option> annotations under the limits that apply to
// spec.routing.publisher.options, counted together with it. An option set both
// in the spec and by an annotation is refused, because the publisher could not
// tell which one the operator meant. The admission webhook runs it, and so does
// the control plane's TrafficMap publisher before it reads the options, because
// the control plane runs no InferenceService admission webhook.
func ValidateRoutingPublisherOptionAnnotations(annotations map[string]string, spec *v1beta1.InferenceServiceSpec) error {
	var specOptions map[string]string
	if spec != nil && spec.Routing != nil && spec.Routing.Publisher != nil {
		specOptions = spec.Routing.Publisher.Options
	}
	count := len(specOptions)
	for key, value := range annotations {
		option, found := strings.CutPrefix(key, constants.RoutingPublisherOptionAnnotationPrefix)
		if !found {
			continue
		}
		count++
		if _, inSpec := specOptions[option]; inSpec {
			return fmt.Errorf("publisher option %q is set both in spec.routing.publisher.options and in annotation %q",
				option, key)
		}
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("annotation %q must not be empty or whitespace-only", key)
		}
		if len(value) > v1beta1.MaxRoutingPublisherOptionValueBytes {
			return fmt.Errorf("annotation %q exceeds the %d-byte value limit",
				key, v1beta1.MaxRoutingPublisherOptionValueBytes)
		}
	}
	if count > v1beta1.MaxRoutingPublisherOptions {
		return fmt.Errorf("spec.routing.publisher.options and the %s annotations must set at most %d options together, got %d",
			constants.RoutingPublisherOptionAnnotationPrefix, v1beta1.MaxRoutingPublisherOptions, count)
	}
	return nil
}

func validateRoutingPublisher(publisher *v1beta1.RoutingPublisherSpec) error {
	if publisher == nil {
		return nil
	}
	if len(publisher.Options) > v1beta1.MaxRoutingPublisherOptions {
		return fmt.Errorf("spec.routing.publisher.options must contain at most %d entries, got %d",
			v1beta1.MaxRoutingPublisherOptions, len(publisher.Options))
	}
	for key, value := range publisher.Options {
		if strings.TrimSpace(key) == "" {
			return fmt.Errorf("spec.routing.publisher.options contains an empty or whitespace-only key")
		}
		if len(key) > v1beta1.MaxRoutingPublisherOptionKeyBytes {
			return fmt.Errorf("spec.routing.publisher.options key %q exceeds the %d-byte limit",
				key, v1beta1.MaxRoutingPublisherOptionKeyBytes)
		}
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("spec.routing.publisher.options[%q] must not be empty or whitespace-only", key)
		}
		if len(value) > v1beta1.MaxRoutingPublisherOptionValueBytes {
			return fmt.Errorf("spec.routing.publisher.options[%q] exceeds the %d-byte value limit",
				key, v1beta1.MaxRoutingPublisherOptionValueBytes)
		}
	}
	return nil
}

// validateRouterCapacityEndpoint checks the router path that serving clusters
// expose for the capacity poll. The path must be one a Gateway API exact match
// accepts, so a bad value fails admission instead of every route update. When
// this InferenceService also sets the poll inline, both paths must agree, or
// the poll would reach the serving port instead.
func validateRouterCapacityEndpoint(spec *v1beta1.InferenceServiceSpec) error {
	if spec.Router == nil || spec.Router.CapacityEndpoint == nil {
		return nil
	}
	endpoint := spec.Router.CapacityEndpoint
	if err := validateExactRoutePath(endpoint.Path); err != nil {
		return fmt.Errorf("spec.router.capacityEndpoint.path %q %w", endpoint.Path, err)
	}
	if endpoint.Port < 1 || endpoint.Port > 65535 {
		return fmt.Errorf("spec.router.capacityEndpoint.port %d must be from 1 through 65535", endpoint.Port)
	}
	if spec.Routing == nil || spec.Routing.Capacity == nil {
		return nil
	}
	capacity := spec.Routing.Capacity
	if !capacity.Disabled && capacity.Path != "" && capacity.Path != endpoint.Path {
		return fmt.Errorf("spec.router.capacityEndpoint.path %q must equal spec.routing.capacity.path %q, which the control plane polls",
			endpoint.Path, capacity.Path)
	}
	return nil
}

// validateExactRoutePath applies the Gateway API rules for an exact path match,
// and rejects "/" because it would capture every request.
func validateExactRoutePath(path string) error {
	switch {
	case !strings.HasPrefix(path, "/"):
		return fmt.Errorf("must begin with %q", "/")
	case path == "/":
		return fmt.Errorf("must name a path below %q", "/")
	case len(path) > 1024:
		return fmt.Errorf("must be at most 1024 characters")
	case strings.ContainsAny(path, "?#"):
		return fmt.Errorf("must not contain a query or fragment")
	case strings.Contains(path, "//"), strings.Contains(path, "/./"), strings.Contains(path, "/../"),
		strings.Contains(strings.ToLower(path), "%2f"),
		strings.HasSuffix(path, "/."), strings.HasSuffix(path, "/.."):
		return fmt.Errorf("must not contain empty, %q or %q segments or an encoded %q", ".", "..", "/")
	}
	return nil
}
