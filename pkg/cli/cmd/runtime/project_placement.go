package runtime

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/placement"
)

const placementProjectionKind = "PlacementProjection"

type placementProjection struct {
	APIVersion string            `json:"apiVersion"`
	Kind       string            `json:"kind"`
	Member     string            `json:"member"`
	Items      []json.RawMessage `json:"items"`
}

var placementFileDecoder = func() k8sruntime.Decoder {
	scheme := k8sruntime.NewScheme()
	scheme.AddKnownTypes(v1beta1.SchemeGroupVersion, &v1beta1.InferenceService{}, &v1beta1.WorkloadCluster{})
	return serializer.NewCodecFactory(scheme, serializer.EnableStrict).UniversalDeserializer()
}()

type projectPlacementOptions struct {
	renderOptions
	member string
}

func newProjectPlacementCmd(f factory.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	return newProjectPlacementCmdWithOptions(f, &projectPlacementOptions{renderOptions: renderOptions{
		IOStreams: streams, readFile: readRenderFile,
	}})
}

func newProjectPlacementCmdWithOptions(f factory.Factory, o *projectPlacementOptions) *cobra.Command {
	o.projectPlacement = true
	cmd := &cobra.Command{
		Use:   "project-placement [INFERENCESERVICE]",
		Short: "Project static Split replica bounds for one member from files",
		Long: `Reads hub InferenceServices and the complete WorkloadCluster inventory
from -f files and uses the placement controller's planner to project final
desired engine, decoder, and router replica bounds for --member. Only explicit
ClusterAffinity placement with mode Split is supported. Status, admission,
and retained rollout replicas do not affect the projection.

--inference-service-crd supplies the hub's API defaults and validation.
Objects without a namespace get the -n namespace. Without INFERENCESERVICE,
every source in the files is projected, sorted by namespace and name. A source
with no allocation on the member is omitted; no allocations yield items: [].
Any invalid source or duplicate input fails the whole command without output.

The command makes no API request and does not resolve runtimes, models, or
WorkloadCluster credentials. It outputs a cli.ome.io/v1alpha1 PlacementProjection,
which runtime render accepts through -f. The output contains supplied manifest
fields and is for preview only; it is not a complete controller-derived member
manifest and cannot be applied.

Use the hub's OME revision for projection, then the member's revision, runtime
files, deploy config, and CRD for runtime render.`,
		Example: `  kubectl ome runtime project-placement --member member-a -f hub.yaml -f registrations.yaml --inference-service-crd hub-crd.yaml -o json > member.json
  kubectl ome runtime render -f member.json -f runtimes.yaml --deploy-config deploy.yaml --inference-service-crd member-crd.yaml -o json`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 1 {
				o.name = args[0]
			}
			if len(o.filenames) == 0 || o.inferenceServiceCRDPath == "" || o.member == "" {
				return errors.New("project-placement requires -f, --member, and --inference-service-crd")
			}
			if problems := validation.IsDNS1123Subdomain(o.member); len(problems) > 0 {
				return fmt.Errorf("member name %q is invalid: %s", o.member, strings.Join(problems, "; "))
			}
			if o.name != "" {
				if err := (&effectiveOptions{name: o.name}).validateName(); err != nil {
					return err
				}
			}
			if o.output != "yaml" && o.output != "json" {
				return fmt.Errorf("unsupported output format %q (supported: yaml, json)", o.output)
			}
			return o.runProjection(f)
		},
	}
	cmd.Flags().StringVar(&o.member, "member", "", "WorkloadCluster whose desired replica bounds to project")
	cmd.Flags().StringArrayVarP(&o.filenames, "filename", "f", nil, "Read hub InferenceServices and the complete WorkloadCluster inventory (repeatable)")
	cmd.Flags().StringVar(&o.inferenceServiceCRDPath, "inference-service-crd", "", "Read the hub's InferenceService CRD defaults and validation")
	cmd.Flags().StringVarP(&o.output, "output", "o", "yaml", "Output format: yaml or json")
	return cmd
}

func (o *projectPlacementOptions) runProjection(f factory.Factory) error {
	workloadNamespace, err := fileWorkloadNamespace(f)
	if err != nil {
		return err
	}
	schema, err := o.loadInferenceServiceStructuralSchema()
	if err != nil {
		return err
	}
	objects, err := o.readFileObjects(workloadNamespace, schema)
	if err != nil {
		return err
	}
	var services []*v1beta1.InferenceService
	var clusters []v1beta1.WorkloadCluster
	for _, object := range objects {
		switch resource := object.object.(type) {
		case *v1beta1.InferenceService:
			if o.name == "" || resource.Name == o.name && resource.Namespace == workloadNamespace {
				services = append(services, resource)
			}
		case *v1beta1.WorkloadCluster:
			clusters = append(clusters, *resource)
		}
	}
	if len(services) == 0 {
		return errors.New("no matching InferenceService in -f files")
	}
	sort.Slice(services, func(i, j int) bool {
		if services[i].Namespace != services[j].Namespace {
			return services[i].Namespace < services[j].Namespace
		}
		return services[i].Name < services[j].Name
	})
	projection := placementProjection{APIVersion: renderAPIVersion, Kind: placementProjectionKind, Member: o.member, Items: []json.RawMessage{}}
	var failures []error
	for _, service := range services {
		projected, err := placement.ProjectSplitReplicas(service, clusters, o.member)
		if err != nil {
			failures = append(failures, fmt.Errorf("InferenceService %s: %w", objectName(service.Namespace, service.Name), err))
			continue
		}
		if projected == nil {
			continue
		}
		item, err := json.Marshal(struct {
			metav1.TypeMeta `json:",inline"`
			Metadata        metav1.ObjectMeta            `json:"metadata"`
			Spec            v1beta1.InferenceServiceSpec `json:"spec"`
		}{
			TypeMeta: metav1.TypeMeta{APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: "InferenceService"},
			Metadata: metav1.ObjectMeta{Name: projected.Name, Namespace: projected.Namespace, Labels: projected.Labels, Annotations: projected.Annotations},
			Spec:     projected.Spec,
		})
		if err != nil {
			return fmt.Errorf("encode projected InferenceService: %w", err)
		}
		projection.Items = append(projection.Items, item)
	}
	if len(failures) > 0 {
		return errors.Join(failures...)
	}
	return writeRendered(o.Out, o.output, projection)
}

func expandPlacementProjection(raw []byte) ([][]byte, bool, error) {
	var typeMeta metav1.TypeMeta
	if err := json.Unmarshal(raw, &typeMeta); err != nil || typeMeta.APIVersion != renderAPIVersion || typeMeta.Kind != placementProjectionKind {
		return [][]byte{raw}, false, nil
	}
	var projection placementProjection
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&projection); err != nil {
		return nil, true, fmt.Errorf("decode PlacementProjection: %w", err)
	}
	if projection.Member == "" || projection.Items == nil {
		return nil, true, errors.New("PlacementProjection requires member and an items array")
	}
	items := make([][]byte, 0, len(projection.Items))
	for _, item := range projection.Items {
		var typeMeta metav1.TypeMeta
		if err := json.Unmarshal(item, &typeMeta); err != nil || typeMeta.APIVersion != v1beta1.SchemeGroupVersion.String() || typeMeta.Kind != "InferenceService" {
			return nil, true, errors.New("PlacementProjection items must be ome.io/v1beta1 InferenceServices")
		}
		items = append(items, item)
	}
	return items, true, nil
}

// PlacementSpec's custom JSON decoder preserves null affinity but ignores
// unknown fields. An alias checks the same API fields without changing that
// decoder's presence semantics.
func validatePlacementFields(raw []byte) error {
	var typeMeta metav1.TypeMeta
	if err := json.Unmarshal(raw, &typeMeta); err != nil {
		return err
	}
	if typeMeta.APIVersion != v1beta1.SchemeGroupVersion.String() || typeMeta.Kind != "InferenceService" {
		return nil
	}
	var source struct {
		Spec struct {
			Placement json.RawMessage `json:"placement"`
		} `json:"spec"`
	}
	if err := json.Unmarshal(raw, &source); err != nil {
		return err
	}
	if len(source.Spec.Placement) == 0 {
		return nil
	}
	type placementFields v1beta1.PlacementSpec
	var fields placementFields
	decoder := json.NewDecoder(bytes.NewReader(source.Spec.Placement))
	decoder.DisallowUnknownFields()
	return decoder.Decode(&fields)
}
