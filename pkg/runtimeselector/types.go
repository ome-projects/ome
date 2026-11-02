package runtimeselector

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// Runtime kind names accepted by GetRuntime's kind argument. They mirror
// the ServingRuntimeRef.Kind API values; an empty kind means the reference
// did not declare a scope.
const (
	KindServingRuntime        = "ServingRuntime"
	KindClusterServingRuntime = "ClusterServingRuntime"
)

// RefKind returns the runtime kind declared on an explicit runtime
// reference, or "" when the ref (or its Kind) is absent.
func RefKind(ref *v1beta1.ServingRuntimeRef) string {
	if ref == nil || ref.Kind == nil {
		return ""
	}
	return *ref.Kind
}

// Selector is the main interface for runtime selection.
type Selector interface {
	// SelectRuntime returns the highest-scoring runtime that supports the
	// model, or a *NoRuntimeFoundError if no compatible runtime exists.
	SelectRuntime(ctx context.Context, model *v1beta1.BaseModelSpec, isvc *v1beta1.InferenceService) (*RuntimeSelection, error)

	// GetCompatibleRuntimes returns all compatible runtimes sorted by score
	// (best first). Useful for debugging and listing available options.
	GetCompatibleRuntimes(ctx context.Context, model *v1beta1.BaseModelSpec, isvc *v1beta1.InferenceService, namespace string) ([]RuntimeMatch, error)

	// ValidateRuntime returns nil if the named runtime is compatible, or an
	// error explaining why it isn't. Used to validate user-pinned runtimes.
	ValidateRuntime(ctx context.Context, runtimeName string, model *v1beta1.BaseModelSpec, isvc *v1beta1.InferenceService) error

	// GetRuntime fetches a runtime by name. kind (a ServingRuntimeRef.Kind
	// value, or "" when undeclared) scopes the lookup — see
	// DefaultRuntimeFetcher.GetRuntime for the precedence rules. The bool
	// return reports whether the resolved runtime was cluster-scoped.
	GetRuntime(ctx context.Context, name string, namespace string, kind string) (*v1beta1.ServingRuntimeSpec, bool, error)

	// GetSupportedModelFormat picks the SupportedModelFormat a pod renders
	// with. For an auto-selected runtime (userSpecifiedRuntime false) it
	// reads the formats as SelectRuntime's ranking does, only those marked
	// autoSelect: true and each at its own priority, so the result is the
	// format that gave the runtime its score. For a named runtime every
	// format is considered at the default priority.
	GetSupportedModelFormat(ctx context.Context, runtime *v1beta1.ServingRuntimeSpec, model *v1beta1.BaseModelSpec, userSpecifiedRuntime bool) *v1beta1.SupportedModelFormat
}

// RuntimeSelection represents the selected runtime with metadata.
type RuntimeSelection struct {
	Name      string
	Spec      *v1beta1.ServingRuntimeSpec
	Score     int64
	IsCluster bool
}

// RuntimeMatch is a RuntimeSelection enriched with the per-dimension match
// details that drove its score, useful for debugging selection decisions.
type RuntimeMatch struct {
	RuntimeSelection
	MatchDetails MatchDetails
}

// MatchDetails records which dimensions of runtime-model compatibility
// contributed to (or excluded) a match. Fields default to true for
// dimensions that the matcher silently accepts when neither side specifies
// them (e.g. ArchitectureMatch is true when both model and runtime omit
// ModelArchitecture).
type MatchDetails struct {
	FormatMatch             bool
	FrameworkMatch          bool
	SizeMatch               bool
	ArchitectureMatch       bool
	DiffusionPipelineMatch  bool
	QuantizationMatch       bool
	ModelCacheProviderMatch bool
	Priority                int32
	Weight                  int64
	AutoSelectEnabled       bool
	Reasons                 []string
}

// RuntimeFetcher abstracts the fetching of runtime resources.
type RuntimeFetcher interface {
	FetchRuntimes(ctx context.Context, namespace string) (*RuntimeCollection, error)
	GetRuntime(ctx context.Context, name string, namespace string, kind string) (*v1beta1.ServingRuntimeSpec, bool, error)
}

// RuntimeCollection holds both namespace and cluster scoped runtimes.
type RuntimeCollection struct {
	NamespaceRuntimes []v1beta1.ServingRuntime
	ClusterRuntimes   []v1beta1.ClusterServingRuntime
}

// forEach invokes fn for every runtime in the collection, namespace-scoped
// first then cluster-scoped. isCluster reports the scope of the current item.
func (c *RuntimeCollection) forEach(fn func(name string, spec *v1beta1.ServingRuntimeSpec, isCluster bool)) {
	for i := range c.NamespaceRuntimes {
		rt := &c.NamespaceRuntimes[i]
		fn(rt.Name, &rt.Spec, false)
	}
	for i := range c.ClusterRuntimes {
		rt := &c.ClusterRuntimes[i]
		fn(rt.Name, &rt.Spec, true)
	}
}

// RuntimeMatcher answers whether a runtime serves a model for a service,
// and why not.
type RuntimeMatcher interface {
	// IsCompatible is GetCompatibilityDetails's answer as a bool. The error
	// is set only when the runtime will not serve the service at all: a
	// RuntimeDisabledError for a disabled runtime, a RuntimeCompatibilityError
	// for one that does not declare the service's accelerator class or
	// deployment mode. A model that no format or the size range fits gives
	// false and a nil error.
	IsCompatible(runtime *v1beta1.ServingRuntimeSpec, model *v1beta1.BaseModelSpec, isvc *v1beta1.InferenceService, runtimeName string) (bool, error)

	// GetCompatibilityDetails runs the compatibility checks in order and
	// reports the first one the runtime failed with its reason, or the
	// format match that made the runtime compatible. The reasons are the
	// texts NoRuntimeFoundError lists per excluded runtime.
	GetCompatibilityDetails(runtime *v1beta1.ServingRuntimeSpec, model *v1beta1.BaseModelSpec, isvc *v1beta1.InferenceService, runtimeName string) (*CompatibilityReport, error)
}

// CompatibilityReport is one runtime's answer for one model and service:
// compatible, with the details of the format match, or the first failed
// check with its reason.
type CompatibilityReport struct {
	IsCompatible           bool
	MatchDetails           MatchDetails
	IncompatibilityReasons []string
	Warnings               []string

	failedCheck compatibilityCheck
}

// compatibilityCheck names the check an incompatible report stopped at. The
// zero value means no check failed. The checks run in this order: the first
// three compare the runtime with the service, the last two with the model.
type compatibilityCheck int

const (
	runtimeDisabled compatibilityCheck = iota + 1
	acceleratorClassNotServed
	deploymentModeMismatch
	modelFormatNotSupported
	modelSizeOutOfRange
)

// RuntimeScorer calculates scores for runtime-model pairs.
type RuntimeScorer interface {
	// CalculateScore returns the best score among the formats marked
	// autoSelect: true, each at its own priority, or 0 when none fits the
	// model; higher means a better match.
	CalculateScore(runtime *v1beta1.ServingRuntimeSpec, model *v1beta1.BaseModelSpec) (int64, error)

	// CompareRuntimes returns positive if r1 is better, negative if r2 is
	// better, 0 if equal. Used as a sort comparator.
	CompareRuntimes(r1, r2 RuntimeMatch, model *v1beta1.BaseModelSpec) int

	CalculateFormatScore(model *v1beta1.BaseModelSpec, supportedFormat v1beta1.SupportedModelFormat, priority int64) int64
}

// Config holds configuration for the runtime selector.
type Config struct {
	Client client.Client

	// EnableDetailedLogging is retained for backward compatibility with
	// external callers; it is currently a no-op (verbose logs use
	// log.V() levels driven by the global log config instead).
	EnableDetailedLogging bool

	// DefaultPriority is the multiplier used when a SupportedModelFormat
	// doesn't set Priority explicitly.
	DefaultPriority int32

	// ModelFormatWeight / ModelFrameworkWeight are the fallback weights used
	// when the runtime author sets weight=0 on a supported format. Tuned so
	// format matches outweigh framework matches at equal priority.
	ModelFormatWeight    int64
	ModelFrameworkWeight int64

	// ModelCacheProvider is the cluster-configured cache provider name; only
	// runtimes whose formats list this provider are eligible for sharded
	// BaseModels (Distribution == DistributionSharded).
	ModelCacheProvider string
}

// NewConfig returns a Config with the default scoring constants:
// priority=1, format weight 10, framework weight 5.
func NewConfig(client client.Client) *Config {
	return &Config{
		Client:               client,
		DefaultPriority:      1,
		ModelFormatWeight:    10,
		ModelFrameworkWeight: 5,
	}
}
