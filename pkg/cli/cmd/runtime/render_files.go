package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/tools/clientcmd"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/yaml"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/effective"
	"sigs.k8s.io/ome/pkg/cli/factory"
)

// fileObjectKinds are the ome.io/v1beta1 kinds file mode reads. The value
// reports whether the kind is namespaced.
var fileObjectKinds = map[string]bool{
	"InferenceService":      true,
	"ServingRuntime":        true,
	"ClusterServingRuntime": false,
	"BaseModel":             true,
	"ClusterBaseModel":      false,
}

type fileObject struct {
	object ctrlclient.Object
	kind   string
	path   string
}

func (o fileObject) key() string {
	return o.kind + " " + objectName(o.object.GetNamespace(), o.object.GetName())
}

// runFiles renders from -f manifests and --deploy-config without any API
// request. Runtimes and models are served to the live resolver from memory,
// so selection and merging follow the same code as cluster mode.
func (o *renderOptions) runFiles(ctx context.Context, f factory.Factory) error {
	workloadNamespace, err := fileWorkloadNamespace(f)
	if err != nil {
		return err
	}
	deployConfig, err := o.decodeDeployConfigFile(o.deployConfigPath)
	if err != nil {
		return err
	}
	objects, err := o.readFileObjects(workloadNamespace)
	if err != nil {
		return err
	}
	isvc, err := selectFileInferenceService(objects, o.name, workloadNamespace)
	if err != nil {
		return err
	}

	scheme := k8sruntime.NewScheme()
	utilruntime.Must(v1beta1.AddToScheme(scheme))
	builder := ctrlfake.NewClientBuilder().WithScheme(scheme)
	for _, object := range objects {
		if object.kind != "InferenceService" {
			builder = builder.WithObjects(object.object)
		}
	}
	resolver := effective.NewRuntimeResolver(builder.Build())
	resolver.SetDeployConfig(deployConfig)
	live, err := resolver.ResolveLive(ctx, isvc)
	if err != nil {
		return fmt.Errorf("render live view: %w", err)
	}

	sources := []renderedSource{{
		Kind: "InferenceService", Name: isvc.Namespace + "/" + isvc.Name, Origin: renderOriginFile,
	}}
	if live.Model != nil {
		sources = append(sources, renderedSource{
			Kind: live.Model.Kind, Name: objectName(live.Model.Namespace, live.Model.Name), Origin: renderOriginFile,
		})
	}
	sources = append(sources,
		renderedSource{
			Kind: live.Runtime.Kind, Name: objectName(live.Runtime.Namespace, live.Runtime.Name), Origin: renderOriginFile,
		},
		renderedSource{Kind: "ConfigMap", Name: o.deployConfigPath, Origin: renderOriginFile},
	)
	rendered := buildRendered(isvc, deployConfig, renderViewLive, sources, live.Components)
	return writeRendered(o.Out, o.output, rendered)
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
func (o *renderOptions) readFileObjects(workloadNamespace string) ([]fileObject, error) {
	var objects []fileObject
	seen := map[string]string{}
	for _, path := range o.filenames {
		data, err := o.readFile(path)
		if err != nil {
			return nil, fmt.Errorf("read -f %s: %w", path, err)
		}
		decoded, err := o.decodeFileObjects(path, data, workloadNamespace)
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

func (o *renderOptions) decodeFileObjects(path string, data []byte, workloadNamespace string) ([]fileObject, error) {
	decoder := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
	var objects []fileObject
	for {
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			if errors.Is(err, io.EOF) {
				return objects, nil
			}
			return nil, fmt.Errorf("-f %s: decode manifest: %w", path, err)
		}
		if len(raw) == 0 || string(raw) == "null" {
			continue
		}
		var header struct {
			metav1.TypeMeta `json:",inline"`
			Metadata        struct {
				Name string `json:"name"`
			} `json:"metadata"`
		}
		if err := json.Unmarshal(raw, &header); err != nil {
			return nil, fmt.Errorf("-f %s: decode manifest: %w", path, err)
		}
		namespaced, known := fileObjectKinds[header.Kind]
		if !known || header.APIVersion != v1beta1.SchemeGroupVersion.String() {
			fmt.Fprintf(o.ErrOut, "ignoring %s %s %q from %s: -f reads only InferenceServices, runtimes, and models\n",
				header.APIVersion, header.Kind, header.Metadata.Name, path)
			continue
		}
		object := newFileObject(header.Kind)
		if err := yaml.UnmarshalStrict(raw, object); err != nil {
			return nil, fmt.Errorf("-f %s: %s %q: %w", path, header.Kind, header.Metadata.Name, err)
		}
		if object.GetName() == "" {
			return nil, fmt.Errorf("-f %s: %s has no metadata.name", path, header.Kind)
		}
		switch {
		case !namespaced:
			object.SetNamespace("")
		case object.GetNamespace() == "":
			object.SetNamespace(workloadNamespace)
		}
		objects = append(objects, fileObject{object: object, kind: header.Kind, path: path})
	}
}

func newFileObject(kind string) ctrlclient.Object {
	switch kind {
	case "InferenceService":
		return &v1beta1.InferenceService{}
	case "ServingRuntime":
		return &v1beta1.ServingRuntime{}
	case "ClusterServingRuntime":
		return &v1beta1.ClusterServingRuntime{}
	case "BaseModel":
		return &v1beta1.BaseModel{}
	default:
		return &v1beta1.ClusterBaseModel{}
	}
}

func selectFileInferenceService(objects []fileObject, name, workloadNamespace string) (*v1beta1.InferenceService, error) {
	var elsewhere []string
	for _, object := range objects {
		isvc, ok := object.object.(*v1beta1.InferenceService)
		if !ok || isvc.Name != name {
			continue
		}
		if isvc.Namespace == workloadNamespace {
			return isvc, nil
		}
		elsewhere = append(elsewhere, isvc.Namespace)
	}
	if len(elsewhere) > 0 {
		sort.Strings(elsewhere)
		return nil, fmt.Errorf("InferenceService %s/%s not found in -f files; it is defined in namespace %s (use -n)",
			workloadNamespace, name, strings.Join(elsewhere, ", "))
	}
	return nil, fmt.Errorf("InferenceService %s/%s not found in -f files", workloadNamespace, name)
}
