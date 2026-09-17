package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestRunDecodesLogicalRowsAndRetainsRawEncoding(t *testing.T) {
	for _, tc := range []struct {
		name, input, encoding string
	}{
		{"legacy dense", `{"status":{"instanceStatuses":[{"index":0,"phase":"Ready"}]}}`, ""},
		{"columnar", `{"status":{"instanceStatusEncoding":"ColumnarV2","instanceStatusColumns":{"members":"0","phases":[{"value":"Ready","indexes":"0"}]}}}`, "ColumnarV2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var output bytes.Buffer
			if err := run(strings.NewReader(tc.input), &output); err != nil {
				t.Fatal(err)
			}
			var got struct {
				RawEncoding string `json:"rawEncoding"`
				Rows        []struct {
					Index int32  `json:"index"`
					Phase string `json:"phase"`
				} `json:"rows"`
			}
			if err := json.Unmarshal(output.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			if got.RawEncoding != tc.encoding || len(got.Rows) != 1 || got.Rows[0].Index != 0 || got.Rows[0].Phase != "Ready" {
				t.Fatalf("unexpected decoded view: %s", output.String())
			}
		})
	}
}

func TestRunFailsClosedWithoutPartialOutput(t *testing.T) {
	for _, input := range []string{
		`{"status":{"instanceStatusEncoding":"DenseV1"}}`,
		`{"status":{"instanceStatusEncoding":"ColumnarV2"}}`,
		`{"status":{"instanceStatusEncoding":"future"}}`,
		`{"status":{"instanceStatusEncoding":"ColumnarV2","instanceStatusColumns":{"members":"0-10000","phases":[{"value":"Ready","indexes":"0-10000"}]}}}`,
		`{"status":{"instanceStatusEncoding":"ColumnarV2","instanceStatuses":[{"index":0,"phase":"Ready"}],"instanceStatusColumns":{"members":"0","phases":[{"value":"Ready","indexes":"0"}]}}}`,
		`{"status":{"instanceStatusEncoding":"ColumnarV2","instanceStatusColumns":{"members":"0","phases":[{"value":"Ready","indexes":"1"}]}}}`,
		`{"status":`,
		`{"status":{}} {"status":{}}`,
	} {
		var output bytes.Buffer
		if err := run(strings.NewReader(input), &output); err == nil {
			t.Errorf("accepted invalid input: %s", input)
		}
		if output.Len() != 0 {
			t.Errorf("returned partial evidence for invalid input: %s", output.String())
		}
	}
}
