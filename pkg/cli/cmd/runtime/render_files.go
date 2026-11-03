package runtime

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"

	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/tools/clientcmd"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/yaml"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/effective"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
)

// fileDecoder knows only the kinds file mode reads, so every other object
// decodes as not registered and is skipped. Strict decoding rejects unknown
// fields instead of dropping them.
var fileDecoder = func() k8sruntime.Decoder {
	scheme := k8sruntime.NewScheme()
	scheme.AddKnownTypes(v1beta1.SchemeGroupVersion,
		&v1beta1.InferenceService{}, &v1beta1.ServingRuntime{}, &v1beta1.ClusterServingRuntime{},
		&v1beta1.BaseModel{}, &v1beta1.ClusterBaseModel{})
	return serializer.NewCodecFactory(scheme, serializer.EnableStrict).UniversalDeserializer()
}()

var fileClusterScopedKinds = map[string]bool{"ClusterServingRuntime": true, "ClusterBaseModel": true, "WorkloadCluster": true}

type fileObject struct {
	object ctrlclient.Object
	kind   string
}

func (o fileObject) key() string {
	return o.kind + " " + objectName(o.object.GetNamespace(), o.object.GetName())
}

// runFiles renders from -f manifests and --deploy-config without any API
// request. Runtimes and models are served to the live resolver from memory,
// so selection and merging follow the same code as cluster mode. Without a
// name it renders every InferenceService in the files.
func (o *renderOptions) runFiles(ctx context.Context, f factory.Factory) error {
	workloadNamespace, err := fileWorkloadNamespace(f)
	if err != nil {
		return err
	}
	deployConfig, err := o.decodeDeployConfigFile(o.deployConfigPath)
	if err != nil {
		return err
	}
	inferenceServiceSchema, err := o.loadInferenceServiceStructuralSchema()
	if err != nil {
		return err
	}
	objects, err := o.readFileObjects(workloadNamespace, inferenceServiceSchema)
	if err != nil {
		return err
	}

	scheme := k8sruntime.NewScheme()
	utilruntime.Must(v1beta1.AddToScheme(scheme))
	builder := ctrlfake.NewClientBuilder().WithScheme(scheme)
	var services []*v1beta1.InferenceService
	for _, object := range objects {
		if isvc, ok := object.object.(*v1beta1.InferenceService); ok {
			services = append(services, isvc)
		} else {
			builder = builder.WithObjects(object.object)
		}
	}
	resolver := effective.NewRuntimeResolver(builder.Build())
	resolver.SetDeployConfig(deployConfig)

	if o.name != "" {
		i := slices.IndexFunc(services, func(isvc *v1beta1.InferenceService) bool {
			return isvc.Name == o.name && isvc.Namespace == workloadNamespace
		})
		if i < 0 {
			return fmt.Errorf("InferenceService %s/%s not found in -f files", workloadNamespace, o.name)
		}
		rendered, err := o.renderFileService(ctx, resolver, deployConfig, services[i])
		if err != nil {
			return err
		}
		return writeRendered(o.Out, o.output, rendered)
	}

	if len(services) == 0 {
		if o.placementProjectionInput {
			return writeRendered(o.Out, o.output, &renderedInferenceServiceList{
				APIVersion: renderAPIVersion, Kind: renderListKind, Items: []*renderedInferenceService{},
			})
		}
		return errors.New("no InferenceService in -f files")
	}
	sort.Slice(services, func(i, j int) bool {
		if services[i].Namespace != services[j].Namespace {
			return services[i].Namespace < services[j].Namespace
		}
		return services[i].Name < services[j].Name
	})
	list := &renderedInferenceServiceList{APIVersion: renderAPIVersion, Kind: renderListKind}
	var failures []error
	for _, isvc := range services {
		rendered, err := o.renderFileService(ctx, resolver, deployConfig, isvc)
		if err != nil {
			failures = append(failures, fmt.Errorf("InferenceService %s/%s: %w", isvc.Namespace, isvc.Name, err))
			continue
		}
		list.Items = append(list.Items, rendered)
	}
	// A partial list would make the failed services look unchanged in a
	// diff, so any failure writes nothing.
	if len(failures) > 0 {
		return fmt.Errorf("%d of %d InferenceServices failed to render:\n%w", len(failures), len(services), errors.Join(failures...))
	}
	return writeRendered(o.Out, o.output, list)
}

func (o *renderOptions) renderFileService(
	ctx context.Context,
	resolver *effective.RuntimeResolver,
	deployConfig *controllerconfig.DeployConfig,
	isvc *v1beta1.InferenceService,
) (*renderedInferenceService, error) {
	live, err := resolver.ResolveLive(ctx, isvc)
	if err != nil {
		return nil, fmt.Errorf("render live view: %w", err)
	}
	sources := []renderedSource{{
		Kind: "InferenceService", Name: isvc.Namespace + "/" + isvc.Name, Origin: renderOriginFile,
	}, {
		Kind: "CustomResourceDefinition", Name: o.inferenceServiceCRDPath, Origin: renderOriginFile,
	}}
	sources = append(sources, liveSources(live, renderOriginFile)...)
	sources = append(sources, renderedSource{Kind: "ConfigMap", Name: o.deployConfigPath, Origin: renderOriginFile})
	return buildRendered(isvc, deployConfig, renderViewLive, sources, live.Components), nil
}

// fileWorkloadNamespace returns the kubectl namespace. Without a kubeconfig
// it falls back to "default", as file mode needs no cluster.
func fileWorkloadNamespace(f factory.Factory) (string, error) {
	workloadNamespace, _, err := f.Namespace()
	switch {
	case clientcmd.IsEmptyConfig(err):
		return metav1.NamespaceDefault, nil
	case err != nil:
		return "", fmt.Errorf("resolve workload namespace: %w", err)
	case workloadNamespace == "":
		return metav1.NamespaceDefault, nil
	}
	return workloadNamespace, nil
}

// readFileObjects decodes every -f file. Namespaced objects without a
// namespace get workloadNamespace, as kubectl apply does. An object defined
// twice is ambiguous and fails.
func (o *renderOptions) readFileObjects(
	workloadNamespace string,
	inferenceServiceSchema *structuralschema.Structural,
) ([]fileObject, error) {
	o.placementProjectionInput = false
	var objects []fileObject
	seen := map[string]string{}
	for _, path := range o.filenames {
		data, err := o.readFile(path)
		if err != nil {
			return nil, fmt.Errorf("read -f %s: %w", path, err)
		}
		decoded, err := o.decodeFileObjects(path, data, workloadNamespace, inferenceServiceSchema)
		if err != nil {
			return nil, err
		}
		for _, object := range decoded {
			key := object.key()
			if previous, ok := seen[key]; ok {
				return nil, fmt.Errorf("ambiguous file input: %s is defined in %s and %s", key, previous, path)
			}
			seen[key] = path
			objects = append(objects, object)
		}
	}
	return objects, nil
}

func (o *renderOptions) decodeFileObjects(
	path string,
	data []byte,
	workloadNamespace string,
	inferenceServiceSchema *structuralschema.Structural,
) ([]fileObject, error) {
	reader := utilyaml.NewYAMLReader(bufio.NewReader(bytes.NewReader(data)))
	var objects []fileObject
	for {
		document, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return objects, nil
			}
			return nil, fmt.Errorf("-f %s: read manifest: %w", path, err)
		}
		// Strict conversion rejects duplicate mapping keys, which a plain
		// conversion would resolve silently to the last value.
		raw, err := yaml.YAMLToJSONStrict(document)
		if err != nil {
			return nil, fmt.Errorf("-f %s: decode manifest: %w", path, err)
		}
		if len(raw) == 0 || string(raw) == "null" {
			continue
		}
		documents := [][]byte{raw}
		decoder := fileDecoder
		if o.projectPlacement {
			decoder = placementFileDecoder
		} else {
			var projection bool
			documents, projection, err = expandPlacementProjection(raw)
			if err != nil {
				return nil, fmt.Errorf("-f %s: %w", path, err)
			}
			o.placementProjectionInput = o.placementProjectionInput || projection
		}
		for _, raw := range documents {
			raw, err = defaultFileInferenceService(raw, inferenceServiceSchema)
			if err != nil {
				return nil, fmt.Errorf("-f %s: apply InferenceService CRD defaults: %w", path, err)
			}
			if o.projectPlacement {
				if err := validatePlacementFields(raw); err != nil {
					return nil, fmt.Errorf("-f %s: validate spec.placement: %w", path, err)
				}
				if err := o.validateFileSpec(raw); err != nil {
					return nil, fmt.Errorf("-f %s: validate InferenceService schema: %w", path, err)
				}
			}
			decoded, gvk, err := decoder.Decode(raw, nil, nil)
			if k8sruntime.IsNotRegisteredError(err) {
				kinds := "InferenceServices, runtimes, and models"
				if o.projectPlacement {
					kinds = "InferenceServices and WorkloadClusters"
				}
				fmt.Fprintf(o.ErrOut, "ignoring %s %s from %s: -f reads only %s\n",
					gvk.GroupVersion(), gvk.Kind, path, kinds)
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("-f %s: %w", path, err)
			}
			object := decoded.(ctrlclient.Object)
			if object.GetName() == "" {
				return nil, fmt.Errorf("-f %s: %s has no metadata.name", path, gvk.Kind)
			}
			switch {
			case fileClusterScopedKinds[gvk.Kind]:
				object.SetNamespace("")
			case object.GetNamespace() == "":
				object.SetNamespace(workloadNamespace)
			}
			objects = append(objects, fileObject{object: object, kind: gvk.Kind})
		}
	}
}
