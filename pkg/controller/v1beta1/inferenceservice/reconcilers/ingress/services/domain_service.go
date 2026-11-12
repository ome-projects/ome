package services

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"text/template"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/ome/pkg/constants"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/interfaces"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
)

var log = logf.Log.WithName("DomainService")

// templateCache holds every domain template parsed so far, keyed by its text.
var templateCache = sync.Map{}

// DefaultDomainService renders the hostnames of an InferenceService from the
// operator's ingress configuration.
type DefaultDomainService struct{}

// NewDomainService creates a new domain service
func NewDomainService() interfaces.DomainService {
	return &DefaultDomainService{}
}

// DomainTemplateValues is what a domain template may read.
type DomainTemplateValues struct {
	Name          string
	Namespace     string
	IngressDomain string
	Annotations   map[string]string
	Labels        map[string]string
}

// getTemplate retrieves a cached template or creates and caches a new one
func getTemplate(templateStr string) (*template.Template, error) {
	if cached, ok := templateCache.Load(templateStr); ok {
		return cached.(*template.Template), nil
	}

	tpl, err := template.New("domain-template").Parse(templateStr)
	if err != nil {
		return nil, err
	}

	templateCache.Store(templateStr, tpl)
	return tpl, nil
}

// estimateBufferSize is the capacity a rendered domain is likely to need: the
// three values a template usually joins, plus room for the separators.
func estimateBufferSize(values *DomainTemplateValues) int {
	return len(values.Name) + len(values.Namespace) + len(values.IngressDomain) + 32
}

// GenerateDomainName renders the external hostname: the ingress configuration
// with the object's annotation overrides, over the name cut to a DNS label.
func (d *DefaultDomainService) GenerateDomainName(name string, obj interface{}, ingressConfig *controllerconfig.IngressConfig) (string, error) {
	objMeta, err := objectMetaOf(obj)
	if err != nil {
		return "", err
	}
	effectiveConfig := utils.ResolveIngressConfig(ingressConfig, objMeta.Annotations)
	return renderDomain(effectiveConfig.DomainTemplate, DomainTemplateValues{
		Name:          constants.TruncateNameWithMaxLength(name, validation.DNS1123LabelMaxLength),
		Namespace:     objMeta.Namespace,
		IngressDomain: effectiveConfig.IngressDomain,
		Annotations:   objMeta.Annotations,
		Labels:        objMeta.Labels,
	})
}

// objectMetaOf is the metadata a domain is rendered for: an InferenceService's
// or a bare ObjectMeta.
func objectMetaOf(obj interface{}) (metav1.ObjectMeta, error) {
	switch v := obj.(type) {
	case *v1beta1.InferenceService:
		return v.ObjectMeta, nil
	case metav1.ObjectMeta:
		return v, nil
	}
	return metav1.ObjectMeta{}, fmt.Errorf("unsupported object type for domain generation")
}

// renderDomain executes the template over values and refuses a result that is
// not a fully qualified domain name.
func renderDomain(templateStr string, values DomainTemplateValues) (string, error) {
	tpl, err := getTemplate(templateStr)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	buf.Grow(estimateBufferSize(&values))
	if err := tpl.Execute(&buf, values); err != nil {
		return "", fmt.Errorf("error rendering the domain template: %w", err)
	}

	urlErrs := validation.IsFullyQualifiedDomainName(field.NewPath("url"), buf.String())
	if urlErrs != nil {
		return "", fmt.Errorf("invalid domain name %q: %w", buf.String(), urlErrs.ToAggregate())
	}
	return buf.String(), nil
}

// GetAdditionalHosts is the service's hostname under each additional ingress
// domain: what precedes the first listed domain that ends serviceHost, joined to
// each additional domain. None when no listed domain ends serviceHost.
func (d *DefaultDomainService) GetAdditionalHosts(domainList *[]string, serviceHost string, config *controllerconfig.IngressConfig) *[]string {
	additionalHosts := &[]string{}
	subdomain := subdomainOf(serviceHost, domainList)
	if len(subdomain) == 0 || config.AdditionalIngressDomains == nil {
		return additionalHosts
	}
	*additionalHosts = hostsUnder(subdomain, *config.AdditionalIngressDomains)
	return additionalHosts
}

// subdomainOf is what precedes the first listed domain that ends serviceHost,
// "" when none does.
func subdomainOf(serviceHost string, domains *[]string) string {
	if domains == nil {
		return ""
	}
	for _, domain := range *domains {
		if subdomain, found := strings.CutSuffix(serviceHost, domain); found {
			return subdomain
		}
	}
	return ""
}

// hostsUnder is one host per distinct domain, in the domains' order. A host
// that is not a valid DNS subdomain is logged and left out.
func hostsUnder(subdomain string, domains []string) []string {
	hosts := []string{}
	rendered := make(map[string]bool, len(domains))
	for _, domain := range domains {
		if rendered[domain] {
			continue
		}
		host := subdomain + domain
		if err := validation.IsDNS1123Subdomain(host); len(err) > 0 {
			log.Error(fmt.Errorf("the domain name %s in the additionalIngressDomains is not valid", domain),
				"Failed to get the valid host name")
			continue
		}
		hosts = append(hosts, host)
		rendered[domain] = true
	}
	return hosts
}
