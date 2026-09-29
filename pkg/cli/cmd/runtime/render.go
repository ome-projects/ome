package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/yaml"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/effective"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/namespace"
	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
)

const (
	renderAPIVersion = "cli.ome.io/v1alpha1"
	renderKind       = "RenderedInferenceService"

	renderViewLive   = "Live"
	renderViewActive = "Active"

	renderOriginCluster = "Cluster"
	renderOriginFile    = "File"

	deployDefaultsApplied       = "Applied"
	deployDefaultsNotApplicable = "NotApplicable"
)

// renderedInferenceService is the CLI-owned output of runtime render. Its
// apiVersion is not served by the API server, so the output cannot be applied
// by mistake. It carries no resource versions, UIDs, or status, so equal
// inputs render byte-identical output.
type renderedInferenceService struct {
	APIVersion     string             `json:"apiVersion"`
	Kind           string             `json:"kind"`
	Metadata       renderedMetadata   `json:"metadata"`
	View           string             `json:"view"`
	Sources        []renderedSource   `json:"sources"`
	DeployDefaults string             `json:"deployDefaults"`
	Components     renderedComponents `json:"components"`
}

type renderedMetadata struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

type renderedSource struct {
	Kind   string `json:"kind"`
	Name   string `json:"name"`
	Origin string `json:"origin"`
}

type renderedComponents struct {
	Engine  *renderedComponent `json:"engine,omitempty"`
	Decoder *renderedComponent `json:"decoder,omitempty"`
	Router  *renderedComponent `json:"router,omitempty"`
}

type renderedComponent struct {
	DeploymentMode       constants.DeploymentModeType            `json:"deploymentMode"`
	DeploymentModeSource effective.ComponentDeploymentModeSource `json:"deploymentModeSource"`
	Spec                 any                                     `json:"spec"`
}

type renderOptions struct {
	genericiooptions.IOStreams
	namespaceOptions *namespace.Options
	limits           paging.Limits
	readFile         func(string) ([]byte, error)
	name             string
	view             string
	output           string
	deployConfigPath string
}

func newRenderCmd(f factory.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	return newRenderCmdWithOptions(f, &renderOptions{
		IOStreams:        streams,
		namespaceOptions: namespace.NewOptions(),
		limits: paging.Limits{
			PageSize:       paging.ChunkSize,
			MaxItems:       1000,
			MaxPages:       2,
			RequestTimeout: 10 * time.Second,
		},
		readFile: readRenderFile,
	})
}

func newRenderCmdWithOptions(f factory.Factory, o *renderOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "render INFERENCESERVICE",
		Short: "Print the component specs the controller acts on",
		Long: `Prints the engine, decoder, and router specs the InferenceService
controller acts on: the service merged with its runtime, each component's
resolved deployment mode, and the deploy defaults from the
inferenceservice-config ConfigMap in --ome-namespace. --deploy-config reads
the ConfigMap from a manifest file instead, for example to preview a change
to the defaults.

--view live merges the runtime as it is now; --view active merges the
ControllerRevision the controller has pinned.

For a service-level VirtualDeployment the controller creates no workloads.
The components are still printed for inspection, with deployDefaults:
NotApplicable.

WARNING: the output is not redacted. It contains complete specs, including
literal environment values and any credential placed inline in the
InferenceService or runtime. Do not paste it into logs or tickets.

The output is a cli.ome.io/v1alpha1 object for reading and diffing. It
cannot be applied.`,
		Example: `  kubectl ome runtime render chat -n prod
  kubectl ome runtime render chat -n prod --view active -o json
  kubectl ome runtime render chat -n prod --deploy-config ./inferenceservice-config.yaml`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			o.name = args[0]
			if err := o.validate(); err != nil {
				return err
			}
			return o.run(cmd.Context(), f)
		},
	}
	cmd.Flags().StringVar(&o.view, "view", "live", "Runtime to merge: live or active")
	cmd.Flags().StringVarP(&o.output, "output", "o", "yaml", "Output format: yaml or json")
	cmd.Flags().StringVar(&o.deployConfigPath, "deploy-config", "",
		"Read deploy defaults from an inferenceservice-config ConfigMap manifest instead of the cluster")
	o.namespaceOptions.AddOMEFlags(cmd.Flags())
	return cmd
}

func (o *renderOptions) validate() error {
	if err := (&effectiveOptions{name: o.name}).validateName(); err != nil {
		return err
	}
	switch o.view {
	case "live", "active":
	default:
		return fmt.Errorf("unsupported view %q (supported: live, active)", o.view)
	}
	switch o.output {
	case "yaml", "json":
	default:
		return fmt.Errorf("unsupported output format %q (supported: yaml, json)", o.output)
	}
	return nil
}

func (o *renderOptions) run(ctx context.Context, f factory.Factory) error {
	loader, deployConfigSource := o.deployConfigLoader()
	evidence, err := collectRuntimeEvidence(
		ctx, f, o.namespaceOptions, o.name, o.limits,
		runtimeEvidenceOptions{LoadDeployConfig: loader},
	)
	if err != nil {
		return err
	}
	if deployConfigSource.Name == "" {
		deployConfigSource.Name = evidence.omeNamespace + "/" + constants.InferenceServiceConfigMapName
	}

	isvc := evidence.inferenceService
	sources := []renderedSource{{
		Kind: "InferenceService", Name: isvc.Namespace + "/" + isvc.Name, Origin: renderOriginCluster,
	}}
	var view string
	var components []effective.EffectiveComponent
	if o.view == "active" {
		view = renderViewActive
		active, err := evidence.state.RequireActive()
		if err != nil {
			return fmt.Errorf("render active view (pin state %s): %w", evidence.state.PinState, err)
		}
		sources = append(sources, renderedSource{
			Kind: active.RuntimeKind, Name: objectName(active.RuntimeNamespace, active.RuntimeName), Origin: renderOriginCluster,
		})
		if active.Origin == effective.ConfigurationOriginControllerRevision {
			sources = append(sources, renderedSource{
				Kind: "ControllerRevision", Name: evidence.omeNamespace + "/" + active.RevisionName, Origin: renderOriginCluster,
			})
		}
		components = active.Components()
	} else {
		view = renderViewLive
		live := evidence.state.LiveConfiguration()
		if live == nil {
			return fmt.Errorf("render live view: %w", liveRuntimeError(evidence.state))
		}
		if live.Model != nil {
			sources = append(sources, renderedSource{
				Kind: live.Model.Kind, Name: objectName(live.Model.Namespace, live.Model.Name), Origin: renderOriginCluster,
			})
		}
		sources = append(sources, renderedSource{
			Kind: live.Runtime.Kind, Name: objectName(live.Runtime.Namespace, live.Runtime.Name), Origin: renderOriginCluster,
		})
		components = live.Components
	}
	sources = append(sources, deployConfigSource)

	rendered := buildRendered(isvc, evidence.deployConfig, view, sources, components)
	return writeRendered(o.Out, o.output, rendered)
}

// deployConfigLoader returns the loader for --deploy-config, or for the live
// ConfigMap, and the source entry it will be reported as. The live entry's
// name is filled once the OME namespace is resolved.
func (o *renderOptions) deployConfigLoader() (deployConfigLoader, renderedSource) {
	if o.deployConfigPath != "" {
		path := o.deployConfigPath
		return func(context.Context, kubernetes.Interface, string) (*controllerconfig.DeployConfig, error) {
			return o.decodeDeployConfigFile(path)
		}, renderedSource{Kind: "ConfigMap", Name: path, Origin: renderOriginFile}
	}
	return func(ctx context.Context, kube kubernetes.Interface, omeNamespace string) (*controllerconfig.DeployConfig, error) {
		return effective.LoadDeployConfig(ctx, kube.CoreV1(), omeNamespace)
	}, renderedSource{Kind: "ConfigMap", Origin: renderOriginCluster}
}

func (o *renderOptions) decodeDeployConfigFile(path string) (*controllerconfig.DeployConfig, error) {
	data, err := o.readFile(path)
	if err != nil {
		return nil, fmt.Errorf("read --deploy-config: %w", err)
	}
	deployConfig, err := effective.DecodeDeployConfig(data)
	if err != nil {
		return nil, fmt.Errorf("--deploy-config %s: %w", path, err)
	}
	return deployConfig, nil
}

func liveRuntimeError(state *effective.RuntimeState) error {
	for _, issue := range state.SourceIssues() {
		switch issue.Code {
		case effective.RuntimeSourceIssueLiveNotFound, effective.RuntimeSourceIssueLiveDisabled, effective.RuntimeSourceIssueLiveUnavailable:
			if cause := errors.Unwrap(issue); cause != nil {
				return fmt.Errorf("%s: %w", issue.Error(), cause)
			}
			return issue
		}
	}
	return fmt.Errorf("live runtime is %s", strings.ToLower(string(state.LiveAvailability())))
}

func buildRendered(
	isvc *v1beta1.InferenceService,
	deployConfig *controllerconfig.DeployConfig,
	view string,
	sources []renderedSource,
	components []effective.EffectiveComponent,
) *renderedInferenceService {
	defaults := deployDefaultsApplied
	if _, virtual := effective.ServiceVirtualDeployment(isvc, deployConfig); virtual {
		defaults = deployDefaultsNotApplicable
	}
	rendered := &renderedInferenceService{
		APIVersion:     renderAPIVersion,
		Kind:           renderKind,
		Metadata:       renderedMetadata{Name: isvc.Name, Namespace: isvc.Namespace},
		View:           view,
		Sources:        sources,
		DeployDefaults: defaults,
	}
	for _, component := range components {
		entry := &renderedComponent{
			DeploymentMode:       component.DeploymentMode,
			DeploymentModeSource: component.DeploymentModeSource,
			Spec:                 component.RenderSpec(),
		}
		switch component.Type {
		case v1beta1.EngineComponent:
			rendered.Components.Engine = entry
		case v1beta1.DecoderComponent:
			rendered.Components.Decoder = entry
		case v1beta1.RouterComponent:
			rendered.Components.Router = entry
		}
	}
	return rendered
}

func writeRendered(w io.Writer, format string, rendered *renderedInferenceService) error {
	var data []byte
	var err error
	if format == "json" {
		data, err = json.MarshalIndent(rendered, "", "  ")
		data = append(data, '\n')
	} else {
		data, err = yaml.Marshal(rendered)
	}
	if err != nil {
		return fmt.Errorf("marshal rendered InferenceService: %w", err)
	}
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("write rendered InferenceService: %w", err)
	}
	return nil
}

func objectName(namespace, name string) string {
	if namespace == "" {
		return name
	}
	return namespace + "/" + name
}

// maxRenderFileBytes bounds each input file. It is far above the 1 MiB an
// API object may hold, so only a wrong path hits it.
const maxRenderFileBytes = 8 << 20

func readRenderFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxRenderFileBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxRenderFileBytes {
		return nil, fmt.Errorf("%s is larger than %d bytes", path, maxRenderFileBytes)
	}
	return data, nil
}
