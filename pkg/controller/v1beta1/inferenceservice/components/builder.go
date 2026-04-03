package components

import (
	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/constants"
	"github.com/sgl-project/ome/pkg/controller/v1beta1/controllerconfig"
	"github.com/sgl-project/ome/pkg/controller/v1beta1/inferenceservice/status"
)

// ComponentBuilder helps build components with common configuration
type ComponentBuilder struct {
	client                 client.Client
	clientset              kubernetes.Interface
	scheme                 *runtime.Scheme
	inferenceServiceConfig *controllerconfig.InferenceServicesConfig
	deploymentMode         constants.DeploymentModeType
	baseModel              *v1beta1.BaseModelSpec
	baseModelMeta          *metav1.ObjectMeta
	runtime                *v1beta1.ServingRuntimeSpec
	runtimeName            string
	supportedModelFormat   *v1beta1.SupportedModelFormat
	acceleratorClass       *v1beta1.AcceleratorClassSpec
	acceleratorClassName   string
	logger                 logr.Logger
}

// NewComponentBuilder creates a new component builder
func NewComponentBuilder(
	client client.Client,
	clientset kubernetes.Interface,
	scheme *runtime.Scheme,
	inferenceServiceConfig *controllerconfig.InferenceServicesConfig,
) *ComponentBuilder {
	return &ComponentBuilder{
		client:                 client,
		clientset:              clientset,
		scheme:                 scheme,
		inferenceServiceConfig: inferenceServiceConfig,
		logger:                 ctrl.Log.WithName("ComponentBuilder"),
	}
}

// WithDeploymentMode sets the deployment mode
func (b *ComponentBuilder) WithDeploymentMode(mode constants.DeploymentModeType) *ComponentBuilder {
	b.deploymentMode = mode
	return b
}

// WithBaseModel sets the base model
func (b *ComponentBuilder) WithBaseModel(spec *v1beta1.BaseModelSpec, meta *metav1.ObjectMeta) *ComponentBuilder {
	b.baseModel = spec
	b.baseModelMeta = meta
	return b
}

// WithRuntime sets the runtime
func (b *ComponentBuilder) WithRuntime(spec *v1beta1.ServingRuntimeSpec, name string) *ComponentBuilder {
	b.runtime = spec
	b.runtimeName = name
	return b
}

// WithLogger sets a custom logger
func (b *ComponentBuilder) WithLogger(logger logr.Logger) *ComponentBuilder {
	b.logger = logger
	return b
}

// WithAcceleratorClass sets the accelerator class
func (b *ComponentBuilder) WithAcceleratorClass(acceleratorClass *v1beta1.AcceleratorClassSpec, acceleratorClassName string) *ComponentBuilder {
	b.acceleratorClass = acceleratorClass
	b.acceleratorClassName = acceleratorClassName
	return b
}

// WithSupportedModelFormat sets the supported model format
func (b *ComponentBuilder) WithSupportedModelFormat(format *v1beta1.SupportedModelFormat) *ComponentBuilder {
	b.supportedModelFormat = format
	return b
}

// buildBaseFields creates the common base fields
func (b *ComponentBuilder) buildBaseFields() BaseComponentFields {
	statusManager := status.NewStatusReconciler(b.clientset).WithLogger(b.logger.WithName("InferenceServiceStatus"))
	return BaseComponentFields{
		Client:                 b.client,
		Clientset:              b.clientset,
		Scheme:                 b.scheme,
		InferenceServiceConfig: b.inferenceServiceConfig,
		DeploymentMode:         b.deploymentMode,
		BaseModel:              b.baseModel,
		BaseModelMeta:          b.baseModelMeta,
		Runtime:                b.runtime,
		RuntimeName:            b.runtimeName,
		StatusManager:          statusManager,
		Log:                    b.logger,
	}
}

// BuildEngine creates an Engine component
func (b *ComponentBuilder) BuildEngine(spec *v1beta1.EngineSpec) Component {
	// For now, using the existing Engine constructor
	return NewEngine(
		b.client,
		b.clientset,
		b.scheme,
		b.inferenceServiceConfig,
		b.deploymentMode,
		b.baseModel,
		b.baseModelMeta,
		spec,
		b.runtime,
		b.runtimeName,
		b.supportedModelFormat,
		b.acceleratorClass,
		b.acceleratorClassName,
	)
}

// BuildDecoder creates a Decoder component
func (b *ComponentBuilder) BuildDecoder(spec *v1beta1.DecoderSpec) Component {
	// For now, using the existing Decoder constructor
	return NewDecoder(
		b.client,
		b.clientset,
		b.scheme,
		b.inferenceServiceConfig,
		b.deploymentMode,
		b.baseModel,
		b.baseModelMeta,
		spec,
		b.runtime,
		b.runtimeName,
		b.supportedModelFormat,
		b.acceleratorClass,
		b.acceleratorClassName,
	)
}

// BuildRouter creates a Router component
func (b *ComponentBuilder) BuildRouter(spec *v1beta1.RouterSpec) Component {
	// For now, using the existing Router constructor (from router_v2.go)
	return NewRouter(
		b.client,
		b.clientset,
		b.scheme,
		b.inferenceServiceConfig,
		b.deploymentMode,
		b.baseModel,
		b.baseModelMeta,
		spec,
		b.runtime,
		b.runtimeName,
	)
}

// BuildCustomComponent creates a custom component with a strategy
func (b *ComponentBuilder) BuildCustomComponent(componentType v1beta1.ComponentType, strategy ComponentConfig) Component {
	baseFields := b.buildBaseFields()
	baseFields.Log = b.logger.WithName(string(componentType) + "Reconciler")

	// This would return a generic component that uses the strategy
	// For now, returning nil as we haven't implemented the generic component yet
	return nil
}

// ComponentBuilderFactory provides a factory for creating builders
type ComponentBuilderFactory struct {
	client                 client.Client
	clientset              kubernetes.Interface
	scheme                 *runtime.Scheme
	inferenceServiceConfig *controllerconfig.InferenceServicesConfig
}

// NewComponentBuilderFactory creates a new factory
func NewComponentBuilderFactory(
	client client.Client,
	clientset kubernetes.Interface,
	scheme *runtime.Scheme,
	inferenceServiceConfig *controllerconfig.InferenceServicesConfig,
) *ComponentBuilderFactory {
	return &ComponentBuilderFactory{
		client:                 client,
		clientset:              clientset,
		scheme:                 scheme,
		inferenceServiceConfig: inferenceServiceConfig,
	}
}

// NewBuilder creates a new component builder
func (f *ComponentBuilderFactory) NewBuilder() *ComponentBuilder {
	return NewComponentBuilder(f.client, f.clientset, f.scheme, f.inferenceServiceConfig)
}

// CreateEngineComponent is a convenience method to create an engine component
func (f *ComponentBuilderFactory) CreateEngineComponent(
	deploymentMode constants.DeploymentModeType,
	baseModel *v1beta1.BaseModelSpec,
	baseModelMeta *metav1.ObjectMeta,
	engineSpec *v1beta1.EngineSpec,
	runtime *v1beta1.ServingRuntimeSpec,
	runtimeName string,
	supportedModelFormat *v1beta1.SupportedModelFormat,
	acceleratorClass *v1beta1.AcceleratorClassSpec,
	acceleratorClassName string,
) Component {
	return f.NewBuilder().
		WithDeploymentMode(deploymentMode).
		WithBaseModel(baseModel, baseModelMeta).
		WithRuntime(runtime, runtimeName).
		WithAcceleratorClass(acceleratorClass, acceleratorClassName).
		WithSupportedModelFormat(supportedModelFormat).
		BuildEngine(engineSpec)
}

// CreateDecoderComponent is a convenience method to create a decoder component
func (f *ComponentBuilderFactory) CreateDecoderComponent(
	deploymentMode constants.DeploymentModeType,
	baseModel *v1beta1.BaseModelSpec,
	baseModelMeta *metav1.ObjectMeta,
	decoderSpec *v1beta1.DecoderSpec,
	runtime *v1beta1.ServingRuntimeSpec,
	runtimeName string,
	supportedModelFormat *v1beta1.SupportedModelFormat,
	acceleratorClass *v1beta1.AcceleratorClassSpec,
	acceleratorClassName string,
) Component {
	return f.NewBuilder().
		WithDeploymentMode(deploymentMode).
		WithBaseModel(baseModel, baseModelMeta).
		WithRuntime(runtime, runtimeName).
		WithAcceleratorClass(acceleratorClass, acceleratorClassName).
		WithSupportedModelFormat(supportedModelFormat).
		BuildDecoder(decoderSpec)
}

// CreateRouterComponent is a convenience method to create a router component
func (f *ComponentBuilderFactory) CreateRouterComponent(
	deploymentMode constants.DeploymentModeType,
	baseModel *v1beta1.BaseModelSpec,
	baseModelMeta *metav1.ObjectMeta,
	routerSpec *v1beta1.RouterSpec,
	runtime *v1beta1.ServingRuntimeSpec,
	runtimeName string,
) Component {
	return f.NewBuilder().
		WithDeploymentMode(deploymentMode).
		WithBaseModel(baseModel, baseModelMeta).
		WithRuntime(runtime, runtimeName).
		BuildRouter(routerSpec)
}
