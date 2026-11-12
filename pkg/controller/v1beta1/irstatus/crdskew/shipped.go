package crdskew

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"sort"
)

// shippedSchemaJSON is the shape of the CRDs this release ships, written by
// `make manifests` from the generated CRD files after their post-processing,
// so the manager compares against exactly what the chart installs.
//
//go:embed zz_generated.shipped_schema.json
var shippedSchemaJSON []byte

// shippedFile is the generated file's form: every distinct node once, in a
// list, and each kind's root by index. A sub-schema that repeats, such as
// the pod template inside every Component, is stored once. A node's
// children always come before it in the list.
type shippedFile struct {
	Kinds map[string]int `json:"kinds"`
	Nodes []shippedNode  `json:"nodes"`
}

type shippedNode struct {
	Properties           map[string]int `json:"properties,omitempty"`
	Items                *int           `json:"items,omitempty"`
	AdditionalProperties *int           `json:"additionalProperties,omitempty"`
	PreserveUnknown      bool           `json:"x-kubernetes-preserve-unknown-fields,omitempty"`
}

// ShippedSchemas returns the schema this release ships, from the generated
// file beside this package.
func ShippedSchemas() (Shipped, error) {
	return DecodeShipped(shippedSchemaJSON)
}

// DecodeShipped reads a generated shipped-schema file.
func DecodeShipped(data []byte) (Shipped, error) {
	var file shippedFile
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("parse the shipped schema: %w", err)
	}
	shapes, err := decodeShippedNodes(file.Nodes)
	if err != nil {
		return nil, err
	}
	return shippedKinds(file.Kinds, shapes)
}

// decodeShippedNodes reads the node list into shapes in order, so each node
// finds its children among the shapes already read.
func decodeShippedNodes(nodes []shippedNode) ([]Shape, error) {
	shapes := make([]Shape, len(nodes))
	for i, node := range nodes {
		shape, err := shapeOfNode(node, shapes[:i])
		if err != nil {
			return nil, err
		}
		shapes[i] = shape
	}
	return shapes, nil
}

// shapeOfNode reads one node whose children are among earlier, the shapes
// of the nodes before it.
func shapeOfNode(node shippedNode, earlier []Shape) (Shape, error) {
	shape := Shape{PreserveUnknown: node.PreserveUnknown}
	if len(node.Properties) > 0 {
		shape.Properties = make(map[string]Shape, len(node.Properties))
		for name, index := range node.Properties {
			field, err := childShape(earlier, &index)
			if err != nil {
				return Shape{}, err
			}
			shape.Properties[name] = *field
		}
	}
	var err error
	if shape.Items, err = childShape(earlier, node.Items); err != nil {
		return Shape{}, err
	}
	if shape.AdditionalProperties, err = childShape(earlier, node.AdditionalProperties); err != nil {
		return Shape{}, err
	}
	return shape, nil
}

// childShape is the shape a node names by index among earlier, the shapes of
// the nodes before it; no index is no child, and an index outside earlier
// names a node that does not come before its parent.
func childShape(earlier []Shape, index *int) (*Shape, error) {
	if index == nil {
		return nil, nil
	}
	parent := len(earlier)
	if *index < 0 || *index >= parent {
		return nil, fmt.Errorf("shipped schema node %d names child %d, which does not come before it", parent, *index)
	}
	return &earlier[*index], nil
}

// shippedKinds maps every kind to its root shape; a root index outside the
// node list is refused.
func shippedKinds(kinds map[string]int, shapes []Shape) (Shipped, error) {
	shipped := make(Shipped, len(kinds))
	for kind, index := range kinds {
		if index < 0 || index >= len(shapes) {
			return nil, fmt.Errorf("shipped schema kind %s names node %d of %d", kind, index, len(shapes))
		}
		shipped[kind] = shapes[index]
	}
	return shipped, nil
}

// EncodeShipped writes the shipped-schema file for the kinds' shapes, every
// distinct sub-schema once.
func EncodeShipped(byKind map[string]Shape) ([]byte, error) {
	e := &shippedEncoder{indexes: map[string]int{}}
	file := shippedFile{Kinds: map[string]int{}, Nodes: []shippedNode{}}
	kinds := make([]string, 0, len(byKind))
	for kind := range byKind {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	for _, kind := range kinds {
		index, err := e.intern(byKind[kind])
		if err != nil {
			return nil, err
		}
		file.Kinds[kind] = index
	}
	file.Nodes = e.nodes
	data, err := json.MarshalIndent(file, "", " ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

type shippedEncoder struct {
	nodes   []shippedNode
	indexes map[string]int
}

// intern stores a shape once and returns its index; its children are stored
// first, so every child index is smaller than its parent's.
func (e *shippedEncoder) intern(s Shape) (int, error) {
	node := shippedNode{PreserveUnknown: s.PreserveUnknown}
	if len(s.Properties) > 0 {
		node.Properties = make(map[string]int, len(s.Properties))
		for _, name := range sortedKeys(s.Properties) {
			index, err := e.intern(s.Properties[name])
			if err != nil {
				return 0, err
			}
			node.Properties[name] = index
		}
	}
	for _, child := range []struct {
		shape *Shape
		index **int
	}{{s.Items, &node.Items}, {s.AdditionalProperties, &node.AdditionalProperties}} {
		if child.shape == nil {
			continue
		}
		index, err := e.intern(*child.shape)
		if err != nil {
			return 0, err
		}
		*child.index = &index
	}
	key, err := json.Marshal(node)
	if err != nil {
		return 0, err
	}
	if index, ok := e.indexes[string(key)]; ok {
		return index, nil
	}
	e.nodes = append(e.nodes, node)
	e.indexes[string(key)] = len(e.nodes) - 1
	return len(e.nodes) - 1, nil
}
