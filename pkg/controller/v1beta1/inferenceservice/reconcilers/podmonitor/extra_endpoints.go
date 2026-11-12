package podmonitor

import (
	"strings"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"

	"sigs.k8s.io/ome/pkg/constants"
)

// ParseExtraEndpoints returns the additional PodMonitor scrape endpoints from
// the prometheus.ome.io/extra-endpoints annotation (comma-separated
// "portName:path"), each scraped every interval, to be appended after OME's
// own endpoint. Malformed entries are skipped with a warning so a bad
// annotation never fails reconcile.
func ParseExtraEndpoints(annotations map[string]string, interval monitoringv1.Duration) []monitoringv1.PodMetricsEndpoint {
	raw := strings.TrimSpace(annotations[constants.ExtraPodMetricsEndpointsAnnotationKey])
	if raw == "" {
		return nil
	}
	var out []monitoringv1.PodMetricsEndpoint
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		// Split on the FIRST ':' — port names contain no ':' and paths start
		// with '/', so "http:/engine_metrics" is unambiguous.
		name, path, ok := strings.Cut(entry, ":")
		name, path = strings.TrimSpace(name), strings.TrimSpace(path)
		if !ok || name == "" || !strings.HasPrefix(path, "/") {
			log.Info("ignoring malformed prometheus.ome.io/extra-endpoints entry (want \"portName:/path\")", "entry", entry)
			continue
		}
		portName := name // new var each iteration -> distinct pointer
		out = append(out, monitoringv1.PodMetricsEndpoint{Port: &portName, Path: path, Interval: interval})
	}
	return out
}
