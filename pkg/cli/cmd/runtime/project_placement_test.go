package runtime

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

const placementSource = `apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: service
  namespace: team-z
  uid: stale-source
  resourceVersion: "17"
  labels: {example: preview}
spec:
  runtime: {name: vllm, kind: ServingRuntime}
  routing: {enabled: true}
  placement:
    policy: ClusterAffinity
    mode: Split
    clusterAffinity:
    - matchFields:
      - {key: metadata.name, operator: In, values: [member-a]}
      weights: {default: {min: 2, max: 2}}
    - matchFields:
      - {key: metadata.name, operator: In, values: [member-b]}
      weights: {default: {min: 1, max: 1}}
  engine: {minReplicas: 60, maxReplicas: 60}
  decoder: {minReplicas: 40, maxReplicas: 40}
  router: {minReplicas: 3, maxReplicas: 3}
`

const placementClusters = `apiVersion: ome.io/v1beta1
kind: WorkloadCluster
metadata: {name: member-b, namespace: ignored}
---
apiVersion: ome.io/v1beta1
kind: WorkloadCluster
metadata: {name: member-a}
`

var projectionArgs = []string{"--member", "member-a", "-f", "sources.yaml", "-f", "clusters.yaml", "--inference-service-crd", "isvc-crd.yaml", "-o", "json"}

func projectionFiles() map[string]string {
	normal := strings.Split(placementSource, "  decoder:")[0]
	normal = strings.NewReplacer("name: service", "name: normal", "  namespace: team-z\n", "", "Replicas: 60", "Replicas: 6").Replace(normal)
	return map[string]string{
		"sources.yaml":  placementSource + "---\n" + normal,
		"clusters.yaml": placementClusters,
		"isvc-crd.yaml": renderFileInferenceServiceCRD,
	}
}

func executeProjection(t *testing.T, f *acquisitionFactory, files map[string]string, args ...string) (string, error) {
	t.Helper()
	var out, stderr bytes.Buffer
	o := &projectPlacementOptions{renderOptions: renderOptions{
		IOStreams: genericiooptions.IOStreams{Out: &out, ErrOut: &stderr},
		readFile: func(path string) ([]byte, error) {
			data, ok := files[path]
			if !ok {
				return nil, os.ErrNotExist
			}
			return []byte(data), nil
		},
	}}
	cmd := newProjectPlacementCmdWithOptions(f, o)
	cmd.SilenceErrors, cmd.SilenceUsage = true, true
	cmd.SetArgs(args)
	err := cmd.Execute()
	assert.Zero(t, f.omeGet+f.kubeGet+f.runtimeGet, "projection must not construct API clients")
	return out.String(), err
}

func TestProjectPlacementFeedsRuntimeRender(t *testing.T) {
	f := renderFactory(t, false)
	files := projectionFiles()
	crd, err := os.ReadFile("../../../../config/crd/full/ome.io_inferenceservices.yaml")
	require.NoError(t, err)
	files["isvc-crd.yaml"] = string(crd)
	output, err := executeProjection(t, f, files, projectionArgs...)
	require.NoError(t, err)
	var projected placementProjection
	require.NoError(t, json.Unmarshal([]byte(output), &projected))
	assert.Equal(t, renderAPIVersion, projected.APIVersion)
	assert.Equal(t, placementProjectionKind, projected.Kind)
	assert.Equal(t, "member-a", projected.Member)
	require.Len(t, projected.Items, 2)
	var normal, split v1beta1.InferenceService
	require.NoError(t, json.Unmarshal(projected.Items[0], &normal))
	require.NoError(t, json.Unmarshal(projected.Items[1], &split))
	assert.Equal(t, "team-a/normal", objectName(normal.Namespace, normal.Name))
	assert.Equal(t, 4, *normal.Spec.Engine.MinReplicas)
	assert.Equal(t, "team-z/service", objectName(split.Namespace, split.Name))
	assert.Equal(t, 40, *split.Spec.Engine.MinReplicas)
	assert.Equal(t, 27, *split.Spec.Decoder.MinReplicas)
	assert.Equal(t, 2, *split.Spec.Router.MinReplicas)
	assert.Nil(t, split.Spec.Placement)
	assert.Nil(t, split.Spec.Routing)
	assert.Empty(t, split.UID)
	assert.Empty(t, split.ResourceVersion)
	assert.Equal(t, "preview", split.Labels["example"])
	assert.NotContains(t, output, `"status"`)

	files["sources.yaml"] = strings.ReplaceAll(files["sources.yaml"], "stale-source", "another-source")
	other, err := executeProjection(t, renderFactory(t, false), files, projectionArgs...)
	require.NoError(t, err)
	assert.Equal(t, output, other, "server identity must not affect output")
	runtime := strings.Split(renderFileRuntimes, "---")[0]
	files["member.json"], files["deploy.yaml"] = output, renderFileDeployConfig
	files["runtimes.yaml"] = runtime + "---\n" + strings.Replace(runtime, "  name: vllm\n", "  name: vllm\n  namespace: team-z\n", 1)
	rendered, _, err := executeRenderArgs(t, renderFactory(t, false), files,
		"-f", "member.json", "-f", "runtimes.yaml", "--deploy-config", "deploy.yaml", "--inference-service-crd", "isvc-crd.yaml", "-o", "json")
	require.NoError(t, err)
	var effective renderedInferenceServiceList
	require.NoError(t, json.Unmarshal([]byte(rendered), &effective))
	require.Len(t, effective.Items, 2)
	spec := effective.Items[1].Components.Engine.Spec.(map[string]any)
	assert.Equal(t, float64(40), spec["minReplicas"])
	assert.Equal(t, "runtime:v1", spec["runner"].(map[string]any)["image"])
}

func TestProjectPlacementEmptyAllocation(t *testing.T) {
	files := projectionFiles()
	files["sources.yaml"] = strings.NewReplacer("Replicas: 6", "Replicas: 1").Replace(strings.Split(files["sources.yaml"], "---\n")[1])
	args := append(append([]string{}, projectionArgs...), "normal", "--member", "member-b")
	output, err := executeProjection(t, renderFactory(t, false), files, args...)
	require.NoError(t, err)
	assert.Contains(t, output, `"items": []`)
	files["member.json"], files["deploy.yaml"] = output, renderFileDeployConfig
	rendered, _, err := executeRenderArgs(t, renderFactory(t, false), files,
		"-f", "member.json", "--deploy-config", "deploy.yaml", "--inference-service-crd", "isvc-crd.yaml", "-o", "json")
	require.NoError(t, err)
	assert.Contains(t, rendered, `"items": []`)
}

func TestProjectPlacementCurrentContract(t *testing.T) {
	ratios := strings.NewReplacer(
		"weights: {default: {min: 2, max: 2}}", "weights: {default: {min: 2, max: 1}}",
		"weights: {default: {min: 1, max: 1}}", "weights: {default: {min: 1, max: 3}}",
		"maxReplicas: 60", "maxReplicas: 180", "maxReplicas: 40", "maxReplicas: 120", "maxReplicas: 3}", "maxReplicas: 12}",
	).Replace(placementSource)
	components := strings.NewReplacer(
		"weights: {default: {min: 2, max: 2}}", "weights: {engine: {min: 2, max: 1}, decoder: {min: 1, max: 3}, router: {min: 1, max: 1}}",
		"weights: {default: {min: 1, max: 1}}", "weights: {engine: {min: 1, max: 3}, decoder: {min: 3, max: 1}, router: {min: 1, max: 1}}",
		"maxReplicas: 60", "maxReplicas: 120", "maxReplicas: 40", "maxReplicas: 80", "router: {minReplicas: 3, maxReplicas: 3}", "router: {minReplicas: 6, maxReplicas: 12}",
	).Replace(placementSource)
	for _, tc := range []struct {
		name, source string
		bounds       [3][2]int
	}{
		{"default ratios", ratios, [3][2]int{{40, 45}, {27, 30}, {2, 3}}},
		{"component ratios", components, [3][2]int{{40, 40}, {10, 60}, {3, 6}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			files := projectionFiles()
			files["sources.yaml"] = tc.source
			output, err := executeProjection(t, renderFactory(t, false), files, projectionArgs...)
			require.NoError(t, err)
			var projection placementProjection
			require.NoError(t, json.Unmarshal([]byte(output), &projection))
			require.Len(t, projection.Items, 1)
			var isvc v1beta1.InferenceService
			require.NoError(t, json.Unmarshal(projection.Items[0], &isvc))
			bounds := [3][2]int{{*isvc.Spec.Engine.MinReplicas, isvc.Spec.Engine.MaxReplicas}, {*isvc.Spec.Decoder.MinReplicas, isvc.Spec.Decoder.MaxReplicas}, {*isvc.Spec.Router.MinReplicas, isvc.Spec.Router.MaxReplicas}}
			assert.Equal(t, tc.bounds, bounds)
		})
	}
}

func TestProjectPlacementRejectsInvalidInputAtomically(t *testing.T) {
	for _, tc := range []struct{ name, file, replacement, want string }{
		{"router shortage", "sources.yaml", strings.Replace(placementSource, "minReplicas: 3", "minReplicas: 1", 1), "router minimum 1"},
		{"no registrations", "clusters.yaml", "", "has no WorkloadCluster registration"},
		{"duplicate registrations", "clusters.yaml", placementClusters + "---\n" + placementClusters, "ambiguous file input"},
		{"unknown registration field", "clusters.yaml", placementClusters + "spec: {typo: true}\n", "unknown field"},
		{"unsupported mode", "sources.yaml", strings.Replace(placementSource, "mode: Split", "mode: All", 1), "supports only explicit ClusterAffinity"},
		{"removed scalar weight", "sources.yaml", strings.Replace(placementSource, "weights: {default: {min: 2, max: 2}}", "weight: 2", 1), `unknown field "weight"`},
		{"removed replica cap", "sources.yaml", strings.Replace(placementSource, "mode: Split\n", "mode: Split\n    split: {maxReplicasPerCluster: 50}\n", 1), `unknown field "maxReplicasPerCluster"`},
		{"weights beside split", "sources.yaml", strings.Replace(placementSource, "mode: Split\n", "mode: Split\n    split: {maxPerCluster: {engine: 50, decoder: 30, router: 4}}\n", 1), "replace spec.placement.split"},
		{"zero component cap", "sources.yaml", strings.Replace(placementSource, "mode: Split\n", "mode: Split\n    split: {maxPerCluster: {engine: 0}}\n", 1), "greater than or equal to 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			files := projectionFiles()
			crd, err := os.ReadFile("../../../../config/crd/full/ome.io_inferenceservices.yaml")
			require.NoError(t, err)
			files["isvc-crd.yaml"] = string(crd)
			if tc.file == "sources.yaml" {
				tc.replacement += "---\n" + strings.Split(files["sources.yaml"], "---\n")[1]
			}
			files[tc.file] = tc.replacement
			output, err := executeProjection(t, renderFactory(t, false), files, projectionArgs...)
			require.ErrorContains(t, err, tc.want)
			assert.Empty(t, output)
		})
	}
}

func TestRuntimeRenderRejectsMalformedPlacementProjection(t *testing.T) {
	for _, input := range []string{
		`{"apiVersion":"cli.ome.io/v1alpha1","kind":"PlacementProjection","member":"member-a","items":null}`,
		`{"apiVersion":"cli.ome.io/v1alpha1","kind":"PlacementProjection","member":"member-a","items":[],"typo":true}`,
		`{"apiVersion":"cli.ome.io/v1alpha1","kind":"PlacementProjection","member":"member-a","items":[{"apiVersion":"v1","kind":"Secret"}]}`,
	} {
		files := renderFiles(map[string]string{"member.json": input})
		output, _, err := executeRenderArgs(t, renderFactory(t, false), files,
			"-f", "member.json", "--deploy-config", "deploy.yaml", "--inference-service-crd", "isvc-crd.yaml")
		require.Error(t, err)
		assert.Empty(t, output)
	}
}

func projectionRuntimeInputs(t *testing.T) (map[string]string, placementProjection) {
	t.Helper()
	files := projectionFiles()
	crd, err := os.ReadFile("../../../../config/crd/full/ome.io_inferenceservices.yaml")
	require.NoError(t, err)
	files["isvc-crd.yaml"] = string(crd)
	output, err := executeProjection(t, renderFactory(t, false), files, projectionArgs...)
	require.NoError(t, err)
	var projection placementProjection
	require.NoError(t, json.Unmarshal([]byte(output), &projection))
	runtime := strings.Split(renderFileRuntimes, "---")[0]
	files["runtimes.yaml"] = runtime + "---\n" + strings.Replace(runtime, "  name: vllm\n", "  name: vllm\n  namespace: team-z\n", 1)
	files["deploy.yaml"] = renderFileDeployConfig
	return files, projection
}

const memberReplicaLimitCRD = renderFileInferenceServiceCRD + `
              engine:
                type: object
                properties:
                  minReplicas:
                    type: integer
                    maximum: 39
`

func TestRuntimeRenderValidatesProjectionItems(t *testing.T) {
	for _, tc := range []struct{ name, want string }{
		{"removed placement field", `unknown field "weight"`},
		{"scalar placement weight", "cannot unmarshal number"},
		{"member schema limit", "less than or equal to 39"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			files, projection := projectionRuntimeInputs(t)
			if tc.name != "member schema limit" {
				var item map[string]any
				require.NoError(t, json.Unmarshal(projection.Items[1], &item))
				term := map[string]any{
					"matchFields": []any{map[string]any{"key": "metadata.name", "operator": "In", "values": []string{"member-a"}}},
				}
				if tc.name == "removed placement field" {
					term["weight"] = 2
				} else {
					term["weights"] = map[string]any{"engine": 1, "decoder": map[string]any{"min": 1, "max": 1}}
				}
				item["spec"].(map[string]any)["placement"] = map[string]any{
					"policy": "ClusterAffinity", "mode": "Split",
					"clusterAffinity": []any{term},
				}
				var err error
				projection.Items[1], err = json.Marshal(item)
				require.NoError(t, err)
			} else {
				files["isvc-crd.yaml"] = memberReplicaLimitCRD
			}
			data, err := json.Marshal(projection)
			require.NoError(t, err)
			files["member.json"] = string(data)
			out, _, err := executeRenderArgs(t, renderFactory(t, false), files,
				"-f", "member.json", "-f", "runtimes.yaml", "--deploy-config", "deploy.yaml", "--inference-service-crd", "isvc-crd.yaml", "-o", "json")
			require.ErrorContains(t, err, tc.want)
			assert.Empty(t, out)
		})
	}
}

func TestRuntimeRenderProjectionValidationIsPerDocument(t *testing.T) {
	files, projection := projectionRuntimeInputs(t)
	projection.Items = projection.Items[:1]
	data, err := json.Marshal(projection)
	require.NoError(t, err)
	ordinary := strings.Replace(placementSource, "weights: {default: {min: 2, max: 2}}", "weight: 2", 1)
	files["mixed.yaml"] = string(data) + "\n---\n" + ordinary
	files["isvc-crd.yaml"] = memberReplicaLimitCRD
	out, _, err := executeRenderArgs(t, renderFactory(t, false), files,
		"-f", "mixed.yaml", "-f", "runtimes.yaml", "--deploy-config", "deploy.yaml", "--inference-service-crd", "isvc-crd.yaml", "-o", "json")
	require.NoError(t, err)
	var rendered renderedInferenceServiceList
	require.NoError(t, json.Unmarshal([]byte(out), &rendered))
	require.Len(t, rendered.Items, 2)
}
