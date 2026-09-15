package placementprojection

import (
	"net"
	"net/url"
	"strconv"
	"strings"

	validation "k8s.io/apimachinery/pkg/util/validation"
	knapis "knative.dev/pkg/apis"
	v "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/safetext"
)

func address(raw *knapis.URL) v.PlacementAddress {
	out := v.PlacementAddress{State: "NotRecorded", Source: reported()}
	if raw == nil {
		return out
	}
	out.State = "InvalidEndpoint"
	parts := []string{raw.Scheme, raw.Opaque, raw.Host, raw.Path, raw.RawPath, raw.RawQuery, raw.Fragment, raw.RawFragment}
	if raw.User != nil {
		parts = append(parts, raw.User.Username())
		if password, ok := raw.User.Password(); ok {
			parts = append(parts, password)
		}
	}
	total := 0
	for _, part := range parts {
		total += len(part)
		if total > 4096 || !safeText(part, 4096, true) || strings.Contains(part, "\\") {
			return out
		}
	}
	if raw.Opaque != "" || raw.OmitHost || (raw.Scheme != "http" && raw.Scheme != "https") || raw.Host == "" {
		return out
	}
	if raw.RawPath != "" {
		decoded, err := url.PathUnescape(raw.RawPath)
		if err != nil || decoded != raw.Path {
			return out
		}
	}
	if raw.RawFragment != "" {
		decoded, err := url.PathUnescape(raw.RawFragment)
		if err != nil || decoded != raw.Fragment {
			return out
		}
	}
	if _, err := url.QueryUnescape(raw.RawQuery); err != nil {
		return out
	}
	if raw.Path != "" && !strings.HasPrefix(raw.Path, "/") {
		return out
	}
	parsed, err := url.Parse(raw.String())
	if err != nil || parsed.Host == "" {
		return out
	}
	host := parsed.Hostname()
	if safetext.Sanitize(host, 253) != host {
		return out
	}
	if host == "" || strings.ContainsAny(host, "%/@?# ") {
		return out
	}
	ip := net.ParseIP(host)
	if ip == nil && (strings.Contains(host, ":") || len(validation.IsDNS1123Subdomain(strings.ToLower(strings.TrimSuffix(host, ".")))) != 0) {
		return out
	}
	port := parsed.Port()
	if strings.HasSuffix(parsed.Host, ":") {
		return out
	}
	if port != "" {
		for _, r := range port {
			if r < '0' || r > '9' {
				return out
			}
		}
		number, err := strconv.Atoi(port)
		if err != nil || number < 1 || number > 65535 {
			return out
		}
	}
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	} else {
		host = strings.ToLower(host)
	}
	if port != "" {
		host += ":" + port
	}
	out.State = "Present"
	out.EndpointOrigin = raw.Scheme + "://" + host
	out.PathPresent = raw.Path != "" || raw.RawPath != ""
	return out
}
