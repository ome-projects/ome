// Command ir-status reads one raw InferenceReplica from stdin and emits a
// separate logical view. It never rewrites the raw API evidence.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"sigs.k8s.io/ome/pkg/alfred/irstatus"
	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func main() {
	if err := run(os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(input io.Reader, output io.Writer) error {
	var ir ome.InferenceReplica
	decoder := json.NewDecoder(input)
	if err := decoder.Decode(&ir); err != nil {
		return fmt.Errorf("read raw InferenceReplica: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return fmt.Errorf("expected exactly one InferenceReplica JSON object")
	}
	rows, err := irstatus.Rows(&ir.Status)
	if err != nil {
		return fmt.Errorf("decode InferenceReplica status: %w", err)
	}
	rawEncoding := ""
	if ir.Status.InstanceStatusEncoding != nil {
		rawEncoding = string(*ir.Status.InstanceStatusEncoding)
	}
	return json.NewEncoder(output).Encode(struct {
		RawEncoding string                        `json:"rawEncoding"`
		Rows        []ome.OMENativeInstanceStatus `json:"rows"`
	}{rawEncoding, rows})
}
