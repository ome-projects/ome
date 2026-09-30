package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

var (
	defineBlock = regexp.MustCompile(`(?s)\{\{- define "([^"]+)" -\}\}\n(.*?)\{\{- end \}\}\n`)
	includeLine = regexp.MustCompile(`(?m)^([ ]*)\{\{- include "([^"]+)" \. \| nindent (\d+) \}\}$`)
)

func resourcesSchema() map[string]interface{} {
	return map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"limits":   map[string]interface{}{"type": "object", "additionalProperties": map[string]interface{}{"type": "string"}},
			"requests": map[string]interface{}{"type": "object", "additionalProperties": map[string]interface{}{"type": "string"}},
		},
	}
}

func containerSchema() map[string]interface{} {
	return map[string]interface{}{
		"type":     "object",
		"required": []interface{}{"name"},
		"properties": map[string]interface{}{
			"name":      map[string]interface{}{"type": "string"},
			"image":     map[string]interface{}{"type": "string"},
			"resources": resourcesSchema(),
			"ports": map[string]interface{}{
				"type": "array",
				"items": map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"containerPort": map[string]interface{}{"type": "integer", "format": "int32", "maximum": json.Number("65535")},
						"protocol":      map[string]interface{}{"type": "string", "default": "TCP"},
					},
				},
			},
		},
	}
}

func podSchema() map[string]interface{} {
	return map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"containers":     map[string]interface{}{"type": "array", "items": containerSchema()},
			"initContainers": map[string]interface{}{"type": "array", "items": containerSchema()},
			"resources":      resourcesSchema(),
		},
	}
}

func crdDoc(name string, spec map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{
		"apiVersion": "apiextensions.k8s.io/v1",
		"kind":       "CustomResourceDefinition",
		"metadata":   map[string]interface{}{"name": name + "s.example.com"},
		"spec": map[string]interface{}{
			"group": "example.com",
			"names": map[string]interface{}{"kind": name, "plural": name + "s"},
			"scope": "Namespaced",
			"versions": []interface{}{
				map[string]interface{}{
					"name":    "v1",
					"served":  true,
					"storage": true,
					"schema": map[string]interface{}{
						"openAPIV3Schema": map[string]interface{}{
							"type": "object",
							"properties": map[string]interface{}{
								"spec": spec,
							},
						},
					},
				},
			},
		},
	}
}

func writeDoc(t *testing.T, dir, name string, doc map[string]interface{}) string {
	t.Helper()
	data, err := yaml.Marshal(doc)
	require.NoError(t, err)
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, data, 0o600))
	return path
}

func parseDoc(t *testing.T, data []byte) map[string]interface{} {
	t.Helper()
	doc := map[string]interface{}{}
	require.NoError(t, yaml.Unmarshal(data, &doc, useNumber))
	return doc
}

func parsePartials(data []byte) map[string]string {
	out := map[string]string{}
	for _, m := range defineBlock.FindAllSubmatch(data, -1) {
		out[string(m[1])] = string(m[2])
	}
	return out
}

// expandIncludes resolves include lines the way Helm's include + nindent do:
// the include line is replaced by the partial body, every body line indented
// by the line's own indentation plus the nindent width.
func expandIncludes(t *testing.T, doc string, partials map[string]string) string {
	t.Helper()
	for i := 0; i < 10000; i++ {
		m := includeLine.FindStringSubmatchIndex(doc)
		if m == nil {
			return doc
		}
		lead := doc[m[2]:m[3]]
		name := doc[m[4]:m[5]]
		n, err := strconv.Atoi(doc[m[6]:m[7]])
		require.NoError(t, err)
		body, ok := partials[name]
		require.True(t, ok, "missing partial %s", name)
		pad := strings.Repeat(" ", len(lead)+n)
		indented := pad + strings.ReplaceAll(strings.TrimSuffix(body, "\n"), "\n", "\n"+pad)
		doc = doc[:m[0]] + indented + doc[m[1]:]
	}
	t.Fatal("include expansion did not terminate")
	return ""
}

func fixtureInputs(t *testing.T) []string {
	t.Helper()
	dir := t.TempDir()
	a := crdDoc("Alpha", map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"engine":  podSchema(),
			"decoder": podSchema(),
			"name":    map[string]interface{}{"type": "string"},
		},
	})
	b := crdDoc("Beta", map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"template": podSchema(),
		},
	})
	return []string{writeDoc(t, dir, "a.yaml", a), writeDoc(t, dir, "b.yaml", b)}
}

var testOpts = helmChartOptions{MinBytes: 100, PartialsFile: "_schemas.tpl", Prefix: "test.schema"}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}

func TestBuildHelmChartRoundTrip(t *testing.T) {
	inputs := fixtureInputs(t)
	files, err := buildHelmChart(inputs, testOpts)
	require.NoError(t, err)
	partials := parsePartials(files["_schemas.tpl"])
	for _, in := range inputs {
		want := parseDoc(t, mustRead(t, in))
		tpl, ok := files[filepath.Base(in)]
		require.True(t, ok, "no template for %s", in)
		got := parseDoc(t, []byte(expandIncludes(t, string(tpl), partials)))
		assert.True(t, reflect.DeepEqual(want, got), "expanded %s differs from input", in)
	}
}

func TestBuildHelmChartSharesPartialsAcrossFiles(t *testing.T) {
	inputs := fixtureInputs(t)
	files, err := buildHelmChart(inputs, testOpts)
	require.NoError(t, err)
	partials := parsePartials(files["_schemas.tpl"])
	// The pod, container and resources schemas repeat across both files and
	// must be shared partials named by their content.
	for _, schema := range []map[string]interface{}{podSchema(), containerSchema(), resourcesSchema()} {
		assert.Contains(t, partials, "test.schema."+shortHash(canonical(schema)))
	}
	for name := range partials {
		assert.True(t, strings.HasPrefix(name, "test.schema."), name)
	}
	// A "properties" map is never a partial of its own: no define body is
	// only a "properties:" include wrapper.
	for name, body := range partials {
		assert.NotRegexp(t, `^properties:\n\{\{- include`, body, name)
	}
	assert.Equal(t, 2, len(includeLine.FindAll(files["a.yaml"], -1)), "engine and decoder become includes")
	assert.Equal(t, 1, len(includeLine.FindAll(files["b.yaml"], -1)), "template becomes an include")
	assert.NotContains(t, string(files["a.yaml"]), includeMarker)
	assert.NotContains(t, string(files["_schemas.tpl"]), includeMarker)
}

func TestBuildHelmChartDeterministic(t *testing.T) {
	inputs := fixtureInputs(t)
	first, err := buildHelmChart(inputs, testOpts)
	require.NoError(t, err)
	second, err := buildHelmChart(inputs, testOpts)
	require.NoError(t, err)
	require.Equal(t, len(first), len(second))
	for name, data := range first {
		assert.True(t, bytes.Equal(data, second[name]), "%s differs between runs", name)
	}
}

func TestBuildHelmChartNoRepeatsKeepsSchemaInline(t *testing.T) {
	inputs := fixtureInputs(t)
	opts := testOpts
	opts.MinBytes = 1 << 20
	files, err := buildHelmChart(inputs, opts)
	require.NoError(t, err)
	assert.Empty(t, parsePartials(files["_schemas.tpl"]))
	for _, in := range inputs {
		assert.Empty(t, includeLine.FindAll(files[filepath.Base(in)], -1))
		assert.True(t, reflect.DeepEqual(parseDoc(t, mustRead(t, in)), parseDoc(t, files[filepath.Base(in)])))
	}
}

func TestBuildHelmChartRejectsTemplateDelimiters(t *testing.T) {
	dir := t.TempDir()
	doc := crdDoc("Gamma", map[string]interface{}{
		"type":    "string",
		"pattern": "^{{.*}}$",
	})
	_, err := buildHelmChart([]string{writeDoc(t, dir, "g.yaml", doc)}, testOpts)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "template delimiter")
}

func TestBuildHelmChartRejectsNonCRD(t *testing.T) {
	dir := t.TempDir()
	doc := map[string]interface{}{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]interface{}{"name": "x"}}
	_, err := buildHelmChart([]string{writeDoc(t, dir, "cm.yaml", doc)}, testOpts)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CustomResourceDefinition")
}

func TestRunHelmChartWritesFiles(t *testing.T) {
	inputs := fixtureInputs(t)
	out := t.TempDir()
	args := append([]string{"--out", out, "--partials", "_schemas.tpl", "--min-bytes", "100", "--prefix", "test.schema"}, inputs...)
	require.NoError(t, runHelmChart(args))
	for _, name := range []string{"a.yaml", "b.yaml", "_schemas.tpl"} {
		_, err := os.Stat(filepath.Join(out, name))
		assert.NoError(t, err, name)
	}
	assert.Error(t, runHelmChart([]string{"--out", out}), "inputs are required")
	assert.Error(t, runHelmChart(inputs), "--out is required")
}
