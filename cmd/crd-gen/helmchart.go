package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"sigs.k8s.io/yaml"

	"sigs.k8s.io/ome/pkg/digest"
)

// partialHashLength is the length of the content hash in a schema partial's
// name. The checked-in templates carry names of this length, so it is part
// of the generated chart's contract.
const partialHashLength = 12

// helmChartOptions controls how repeated schema subtrees become Helm partials.
type helmChartOptions struct {
	// MinBytes is the smallest canonical JSON size an object node must have
	// to become a partial; smaller repeats stay inline.
	MinBytes int
	// PartialsFile is the name of the generated partials file.
	PartialsFile string
	// Prefix is the named-template prefix; a content hash is appended.
	Prefix string
}

// includeMarker tags a factored node inside the marshaled YAML; renderYAML
// turns every tagged "key: marker" line into a Helm include.
const includeMarker = "__CRDGEN_INCLUDE__"

var markerLine = regexp.MustCompile(`(?m)^([ ]*)([^ \n][^\n]*?):[ ]+"?` + includeMarker + `([^"\s]+)"?[ ]*$`)

// buildHelmChart factors the CRDs in inputs into chart templates: one file per
// CRD, named like the input, plus opts.PartialsFile holding every schema
// subtree that repeats. Expanding the includes yields the input schema.
func buildHelmChart(inputs []string, opts helmChartOptions) (map[string][]byte, error) {
	if err := checkChartOptions(inputs, opts); err != nil {
		return nil, err
	}
	sorted := append([]string(nil), inputs...)
	sort.Strings(sorted)
	docs, names, err := readChartInputs(sorted, opts.PartialsFile)
	if err != nil {
		return nil, err
	}
	repeats := newRepeatedNodes(opts.MinBytes)
	for _, doc := range docs {
		for _, holder := range schemaHolders(doc) {
			repeats.count(holder["openAPIV3Schema"], true)
		}
	}
	partials := repeats.partials(opts.Prefix)
	return renderChartFiles(docs, names, partials, repeats.nodes, opts.PartialsFile)
}

// checkChartOptions refuses a size that cannot select a partial, an unnamed
// partials file or prefix, and an empty input list.
func checkChartOptions(inputs []string, opts helmChartOptions) error {
	if opts.MinBytes <= 0 {
		return errors.New("min-bytes must be positive")
	}
	if opts.PartialsFile == "" || opts.Prefix == "" {
		return errors.New("partials file and prefix are required")
	}
	if len(inputs) == 0 {
		return errors.New("at least one CRD file is required")
	}
	return nil
}

// readChartInputs reads every CRD in order and names its output after the
// input's base name; a base name taken twice, or by the partials file, is an
// error.
func readChartInputs(inputs []string, partialsFile string) ([]map[string]interface{}, []string, error) {
	docs := make([]map[string]interface{}, 0, len(inputs))
	names := make([]string, 0, len(inputs))
	seen := map[string]bool{partialsFile: true}
	for _, in := range inputs {
		base := filepath.Base(in)
		if seen[base] {
			return nil, nil, fmt.Errorf("%s: output name %s is already taken", in, base)
		}
		seen[base] = true
		doc, err := readCRD(in)
		if err != nil {
			return nil, nil, err
		}
		docs = append(docs, doc)
		names = append(names, base)
	}
	return docs, names, nil
}

// renderChartFiles replaces every partial occurrence in the schemas with an
// include and renders one template per CRD and the partials file.
func renderChartFiles(docs []map[string]interface{}, names []string, partials map[string]string, nodes map[string]map[string]interface{}, partialsFile string) (map[string][]byte, error) {
	out := make(map[string][]byte, len(docs)+1)
	for i, doc := range docs {
		for _, holder := range schemaHolders(doc) {
			holder["openAPIV3Schema"] = rewriteNode(holder["openAPIV3Schema"], true, partials)
		}
		data, err := renderYAML(doc)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", names[i], err)
		}
		out[names[i]] = data
	}
	data, err := renderPartials(partials, nodes)
	if err != nil {
		return nil, err
	}
	out[partialsFile] = data
	return out, nil
}

// useNumber keeps numbers as json.Number so they round-trip byte for byte.
func useNumber(d *json.Decoder) *json.Decoder {
	d.UseNumber()
	return d
}

func readCRD(path string) (map[string]interface{}, error) {
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	doc := map[string]interface{}{}
	if err := yaml.Unmarshal(data, &doc, useNumber); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if doc["kind"] != "CustomResourceDefinition" {
		return nil, fmt.Errorf("%s: kind %v is not CustomResourceDefinition", path, doc["kind"])
	}
	if s := findTemplateDelimiter(doc); s != "" {
		return nil, fmt.Errorf("%s: %q contains a Go template delimiter", path, s)
	}
	return doc, nil
}

// schemaHolders returns every versions[].schema map that carries an
// openAPIV3Schema, so callers can read or replace the schema in place.
func schemaHolders(doc map[string]interface{}) []map[string]interface{} {
	spec, _ := doc["spec"].(map[string]interface{})
	versions, _ := spec["versions"].([]interface{})
	var out []map[string]interface{}
	for _, v := range versions {
		ver, _ := v.(map[string]interface{})
		holder, _ := ver["schema"].(map[string]interface{})
		if _, ok := holder["openAPIV3Schema"].(map[string]interface{}); ok {
			out = append(out, holder)
		}
	}
	return out
}

// findTemplateDelimiter returns the first key or string value that Helm
// would try to interpret, or "" when the document is safe to template.
func findTemplateDelimiter(node interface{}) string {
	switch n := node.(type) {
	case string:
		if hasTemplateDelimiter(n) {
			return n
		}
	case map[string]interface{}:
		return findTemplateDelimiterInMap(n)
	case []interface{}:
		for _, v := range n {
			if s := findTemplateDelimiter(v); s != "" {
				return s
			}
		}
	}
	return ""
}

// findTemplateDelimiterInMap reads the keys in sorted order, each key before
// its value, so the same document always names the same delimiter.
func findTemplateDelimiterInMap(n map[string]interface{}) string {
	keys := make([]string, 0, len(n))
	for k := range n {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if hasTemplateDelimiter(k) {
			return k
		}
		if s := findTemplateDelimiter(n[k]); s != "" {
			return s
		}
	}
	return ""
}

func hasTemplateDelimiter(s string) bool {
	return strings.Contains(s, "{{") || strings.Contains(s, "}}")
}

// canonical is the identity of a subtree: compact JSON with sorted keys.
func canonical(node map[string]interface{}) string {
	b, err := json.Marshal(node)
	if err != nil {
		panic(err) // decoded JSON always re-encodes
	}
	return string(b)
}

// childEligible says whether the value under key may become a partial. A
// schema's "properties" map repeats exactly when the schema object around it
// does, so factoring the object is enough; the entries inside stay eligible.
func childEligible(key string) bool {
	return key != "properties"
}

// repeatedNodes tallies every object node of the schemas that is large enough
// to become a partial, by its canonical form, and keeps one copy of each. A
// node is eligible only as the value of a mapping key, never as a list
// element, so every include lands on a "key:" line.
type repeatedNodes struct {
	minBytes int
	counts   map[string]int
	nodes    map[string]map[string]interface{}
}

func newRepeatedNodes(minBytes int) *repeatedNodes {
	return &repeatedNodes{minBytes: minBytes, counts: map[string]int{}, nodes: map[string]map[string]interface{}{}}
}

// count walks node and tallies its eligible object nodes.
func (r *repeatedNodes) count(node interface{}, eligible bool) {
	switch n := node.(type) {
	case map[string]interface{}:
		if eligible {
			r.countObject(n)
		}
		for k, v := range n {
			r.count(v, childEligible(k))
		}
	case []interface{}:
		for _, v := range n {
			r.count(v, false)
		}
	}
}

// countObject tallies one object node when its canonical form is at least
// minBytes, keeping the first copy met; equal canonical forms are equal
// nodes, so which copy is kept changes nothing.
func (r *repeatedNodes) countObject(n map[string]interface{}) {
	c := canonical(n)
	if len(c) < r.minBytes {
		return
	}
	r.counts[c]++
	if _, ok := r.nodes[c]; !ok {
		r.nodes[c] = n
	}
}

// partials names every node seen at least twice: the prefix and a short
// content hash.
func (r *repeatedNodes) partials(prefix string) map[string]string {
	partials := map[string]string{}
	for canon, n := range r.counts {
		if n >= 2 {
			partials[canon] = prefix + "." + digest.Short([]byte(canon), partialHashLength)
		}
	}
	return partials
}

// rewriteNode returns node with every partial occurrence replaced by an
// include marker; the input is not modified.
func rewriteNode(node interface{}, eligible bool, partials map[string]string) interface{} {
	switch n := node.(type) {
	case map[string]interface{}:
		if eligible {
			if name, ok := partials[canonical(n)]; ok {
				return includeMarker + name
			}
		}
		out := make(map[string]interface{}, len(n))
		for k, v := range n {
			out[k] = rewriteNode(v, childEligible(k), partials)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(n))
		for i, v := range n {
			out[i] = rewriteNode(v, false, partials)
		}
		return out
	default:
		return node
	}
}

// renderYAML marshals node and turns each marker line into a Helm include
// indented two deeper than its key, which is where the value would sit.
func renderYAML(node interface{}) ([]byte, error) {
	data, err := yaml.Marshal(node)
	if err != nil {
		return nil, err
	}
	out := markerLine.ReplaceAllFunc(data, func(line []byte) []byte {
		m := markerLine.FindSubmatch(line)
		return []byte(fmt.Sprintf("%s%s:\n{{- include %q . | nindent %d }}", m[1], m[2], string(m[3]), len(m[1])+2))
	})
	if bytes.Contains(out, []byte(includeMarker)) {
		return nil, errors.New("an include marker is not the value of a key")
	}
	return out, nil
}

// renderPartials emits one define block per partial, sorted by name. Bodies
// are themselves factored, so partials nest.
func renderPartials(partials map[string]string, nodes map[string]map[string]interface{}) ([]byte, error) {
	type entry struct{ name, canon string }
	entries := make([]entry, 0, len(partials))
	for canon, name := range partials {
		entries = append(entries, entry{name: name, canon: canon})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].name < entries[j].name })

	var buf bytes.Buffer
	buf.WriteString("{{/*\nSchema subtrees shared by the CRD templates. Generated by\n`crd-gen helmchart` from config/crd/full; the Go API types are the source.\n*/}}\n")
	for _, e := range entries {
		node := nodes[e.canon]
		body := make(map[string]interface{}, len(node))
		for k, v := range node {
			body[k] = rewriteNode(v, childEligible(k), partials)
		}
		text, err := renderYAML(body)
		if err != nil {
			return nil, fmt.Errorf("partial %s: %w", e.name, err)
		}
		fmt.Fprintf(&buf, "{{- define %q -}}\n%s{{- end }}\n", e.name, text)
	}
	return buf.Bytes(), nil
}

// runHelmChart parses the helmchart flags and writes the generated files.
func runHelmChart(args []string) error {
	fs := flag.NewFlagSet("helmchart", flag.ContinueOnError)
	out := fs.String("out", "", "directory that receives the chart templates")
	partialsFile := fs.String("partials", "_schemas.tpl", "file name of the shared schema partials")
	minBytes := fs.Int("min-bytes", 2048, "smallest canonical JSON size a repeated subtree must have to become a partial")
	prefix := fs.String("prefix", "ome-crd.schema", "named-template prefix")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *out == "" || fs.NArg() == 0 {
		return errors.New("usage: crd-gen helmchart --out DIR [--partials NAME] [--min-bytes N] [--prefix P] CRD.yaml...")
	}
	files, err := buildHelmChart(fs.Args(), helmChartOptions{MinBytes: *minBytes, PartialsFile: *partialsFile, Prefix: *prefix})
	if err != nil {
		return err
	}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(*out, name), data, 0o600); err != nil {
			return err
		}
	}
	return nil
}
