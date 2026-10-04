// Package render turns an AutoscalerPolicy template into a concrete
// per-Component autoscaler block. It is pure — no Kubernetes clients, no
// filesystem, no network — so the controller, the admission webhook, and CI
// tooling share one implementation and cannot disagree about what a policy
// renders to.
package render

import (
	"fmt"
	"strings"
	"sync"
	"text/template"
	"text/template/parse"

	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// allowedVariables is the closed, controller-derived variable set metadata
// templates may reference. Unknown variables fail validation and rendering.
var allowedVariables = map[string]bool{
	"Namespace":   true,
	"ISVCName":    true,
	"Component":   true,
	"MinReplicas": true,
	"MaxReplicas": true,
	"TargetName":  true,
}

// ForbiddenMetadataKeys are trigger metadata keys a policy may never carry:
// the cluster-local provider binding owns the endpoint and its
// authentication, so a policy author can only name a provider, never aim a
// scaler at an arbitrary address.
var ForbiddenMetadataKeys = []string{"serverAddress", "authModes"}

// IsForbiddenMetadataKey reports whether key is provider-owned
// (case-insensitive, matching KEDA's metadata handling).
func IsForbiddenMetadataKey(key string) bool {
	for _, banned := range ForbiddenMetadataKeys {
		if strings.EqualFold(key, banned) {
			return true
		}
	}
	return false
}

// parseMetadataTemplate parses one metadata value with missingkey=error — a
// typo'd variable must fail the render (fail closed) rather than silently
// interpolate "<no value>" into a query — and then enforces the structural
// allowlist: field access and literal text only. No functions, pipelines,
// variables, or control flow, so "no arbitrary executable templates" is a
// property of the parse tree, not a review hope.
func parseMetadataTemplate(name, text string) (*template.Template, error) {
	tmpl, err := template.New(name).Option("missingkey=error").Parse(text)
	if err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	if tmpl.Root == nil {
		return tmpl, nil
	}
	if err := checkNodeAllowlist(tmpl.Root); err != nil {
		return nil, err
	}
	return tmpl, nil
}

func checkNodeAllowlist(root *parse.ListNode) error {
	for _, n := range root.Nodes {
		switch node := n.(type) {
		case *parse.TextNode:
		case *parse.ActionNode:
			if err := checkActionNode(node); err != nil {
				return err
			}
		default:
			return fmt.Errorf("forbidden template construct %q: only {{ .Var }} field access and literal text are allowed", n.String())
		}
	}
	return nil
}

func checkActionNode(action *parse.ActionNode) error {
	pipe := action.Pipe
	if pipe == nil || len(pipe.Decl) != 0 || len(pipe.Cmds) != 1 {
		return fmt.Errorf("forbidden template construct %q: pipelines, declarations, and functions are not allowed", action.String())
	}
	cmd := pipe.Cmds[0]
	if len(cmd.Args) != 1 {
		return fmt.Errorf("forbidden template construct %q: function calls are not allowed", action.String())
	}
	field, ok := cmd.Args[0].(*parse.FieldNode)
	if !ok {
		return fmt.Errorf("forbidden template construct %q: only field access is allowed", action.String())
	}
	if len(field.Ident) != 1 || !allowedVariables[field.Ident[0]] {
		return fmt.Errorf("unknown template variable %q: allowed variables are .Namespace .ISVCName .Component .MinReplicas .MaxReplicas .TargetName", action.String())
	}
	return nil
}

// compiledPolicy is one policy generation's parsed metadata templates, keyed
// by trigger index + metadata key.
type compiledPolicy struct {
	generation int64
	templates  map[string]*template.Template
}

func templateKey(triggerIndex int, metadataKey string) string {
	return fmt.Sprintf("t%d/%s", triggerIndex, metadataKey)
}

// compileSpec parses every templated metadata value in the spec once.
func compileSpec(spec *v1beta1.AutoscalerPolicySpec) (*compiledPolicy, error) {
	compiled := &compiledPolicy{
		templates: map[string]*template.Template{},
	}
	if spec.Keda == nil {
		return compiled, nil
	}
	for i := range spec.Keda.Triggers {
		for key, value := range spec.Keda.Triggers[i].Metadata {
			tmpl, err := parseMetadataTemplate(templateKey(i, key), value)
			if err != nil {
				return nil, fmt.Errorf("trigger %d metadata %q: %w", i, key, err)
			}
			compiled.templates[templateKey(i, key)] = tmpl
		}
	}
	return compiled, nil
}

// Cache holds compiled templates keyed by policy UID, invalidated by
// generation. Entries are immutable once built; one entry exists per live
// policy object, so growth is bounded by the number of policies.
type Cache struct {
	mu      sync.Mutex
	entries map[types.UID]*compiledPolicy
}

// NewCache returns an empty template cache.
func NewCache() *Cache {
	return &Cache{entries: map[types.UID]*compiledPolicy{}}
}

// DefaultCache is the process-wide cache shared by dispatch and the status
// writer's independent re-resolution.
var DefaultCache = NewCache()

func (c *Cache) compiledFor(policy *v1beta1.AutoscalerPolicy) (*compiledPolicy, error) {
	if policy.UID == "" {
		// No stable identity (unit tests, webhook dry-runs): compile fresh.
		return compileSpec(&policy.Spec)
	}
	c.mu.Lock()
	entry, ok := c.entries[policy.UID]
	c.mu.Unlock()
	if ok && entry.generation == policy.Generation {
		return entry, nil
	}
	compiled, err := compileSpec(&policy.Spec)
	if err != nil {
		return nil, err
	}
	compiled.generation = policy.Generation
	c.mu.Lock()
	c.entries[policy.UID] = compiled
	c.mu.Unlock()
	return compiled, nil
}
