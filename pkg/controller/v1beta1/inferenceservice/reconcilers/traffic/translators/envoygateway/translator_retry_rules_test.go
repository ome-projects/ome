package envoygateway

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/traffic"
)

// applyRetry writes an InferenceService's retry intent onto its
// BackendTrafficPolicy: the attempt count under spec.retry.numRetries, the
// per-try timeout under spec.retry.perRetry.timeout as a duration string,
// and the retry-on list split into string triggers and numeric status codes
// under spec.retry.retryOn. A field the intent leaves unset is not written,
// an empty bucket is not written, and a document whose spec cannot hold the
// field names that field in its error. One row per rule: the intent, the
// document before the write, and the document after it.

type retryRow struct {
	name   string
	intent traffic.RetryIntent
	// before is what the document holds under spec before the write; nil
	// is a document without a spec.
	before any
	// want is the whole document after the write.
	want map[string]any
	// wantErr is the error's text; empty means the write succeeds.
	wantErr string
}

func retrySpec(retry map[string]any) map[string]any {
	return map[string]any{"spec": map[string]any{"retry": retry}}
}

// cannotHold is the error the unstructured library gives when spec is not a
// map, as applyRetry wraps it under the field's name.
func cannotHold(field string, path ...string) string {
	err := unstructured.SetNestedField(map[string]any{"spec": "text"}, int64(1), append([]string{"spec"}, path...)...)
	if err == nil {
		return "set " + field + ": a spec that is text must refuse a nested write"
	}
	return "set " + field + ": " + err.Error()
}

var (
	three       = int32(3)
	zero        = int32(0)
	fiveSeconds = 5 * time.Second
	halfSecond  = 1500 * time.Millisecond
)

var retryRows = []retryRow{
	{
		name: "an empty intent writes nothing",
		want: map[string]any{},
	},
	{
		name:   "the attempt count is written as numRetries",
		intent: traffic.RetryIntent{Attempts: &three},
		want:   retrySpec(map[string]any{"numRetries": int64(3)}),
	},
	{
		name:   "a zero attempt count is still written",
		intent: traffic.RetryIntent{Attempts: &zero},
		want:   retrySpec(map[string]any{"numRetries": int64(0)}),
	},
	{
		name:   "the per-try timeout is written as a duration string",
		intent: traffic.RetryIntent{PerTryTimeout: &fiveSeconds},
		want:   retrySpec(map[string]any{"perRetry": map[string]any{"timeout": "5s"}}),
	},
	{
		name:   "string tokens become triggers and no status code list is written",
		intent: traffic.RetryIntent{RetryOn: []string{"5xx", "reset"}},
		want:   retrySpec(map[string]any{"retryOn": map[string]any{"triggers": []any{"5xx", "reset"}}}),
	},
	{
		name:   "numeric tokens become status codes and no trigger list is written",
		intent: traffic.RetryIntent{RetryOn: []string{"503", "504"}},
		want:   retrySpec(map[string]any{"retryOn": map[string]any{"httpStatusCodes": []any{int64(503), int64(504)}}}),
	},
	{
		name:   "mixed tokens keep their order inside each list",
		intent: traffic.RetryIntent{RetryOn: []string{"5xx", "503", "reset", "504"}},
		want: retrySpec(map[string]any{"retryOn": map[string]any{
			"triggers":        []any{"5xx", "reset"},
			"httpStatusCodes": []any{int64(503), int64(504)},
		}}),
	},
	{
		name:   "empty tokens alone write no retry-on block",
		intent: traffic.RetryIntent{RetryOn: []string{""}},
		want:   map[string]any{},
	},
	{
		name:   "every field at once",
		intent: traffic.RetryIntent{Attempts: &three, PerTryTimeout: &halfSecond, RetryOn: []string{"gateway-error", "502"}},
		want: retrySpec(map[string]any{
			"numRetries": int64(3),
			"perRetry":   map[string]any{"timeout": "1.5s"},
			"retryOn": map[string]any{
				"triggers":        []any{"gateway-error"},
				"httpStatusCodes": []any{int64(502)},
			},
		}),
	},
	{
		name:   "the write keeps what the spec already holds",
		intent: traffic.RetryIntent{Attempts: &three},
		before: map[string]any{"targetRefs": []any{"route-a"}},
		want: map[string]any{"spec": map[string]any{
			"targetRefs": []any{"route-a"},
			"retry":      map[string]any{"numRetries": int64(3)},
		}},
	},
	{
		name:    "a spec that cannot hold the attempt count names it",
		intent:  traffic.RetryIntent{Attempts: &three},
		before:  "text",
		want:    map[string]any{"spec": "text"},
		wantErr: cannotHold("retry.numRetries", "retry", "numRetries"),
	},
	{
		name:    "a spec that cannot hold the per-try timeout names it",
		intent:  traffic.RetryIntent{PerTryTimeout: &fiveSeconds},
		before:  "text",
		want:    map[string]any{"spec": "text"},
		wantErr: cannotHold("retry.perRetry.timeout", "retry", "perRetry", "timeout"),
	},
	{
		name:    "a spec that cannot hold the triggers names them",
		intent:  traffic.RetryIntent{RetryOn: []string{"reset"}},
		before:  "text",
		want:    map[string]any{"spec": "text"},
		wantErr: cannotHold("retry.retryOn.triggers", "retry", "retryOn", "triggers"),
	},
	{
		name:    "a spec that cannot hold the status codes names them",
		intent:  traffic.RetryIntent{RetryOn: []string{"503"}},
		before:  "text",
		want:    map[string]any{"spec": "text"},
		wantErr: cannotHold("retry.retryOn.httpStatusCodes", "retry", "retryOn", "httpStatusCodes"),
	},
}

func TestApplyRetryRules(t *testing.T) {
	for _, row := range retryRows {
		t.Run(row.name, func(t *testing.T) {
			document := &unstructured.Unstructured{Object: map[string]any{}}
			if row.before != nil {
				document.Object["spec"] = row.before
			}
			intent := row.intent
			err := applyRetry(document, &intent)
			got := ""
			if err != nil {
				got = err.Error()
			}
			if got != row.wantErr {
				t.Fatalf("error = %q, want %q", got, row.wantErr)
			}
			if diff := cmp.Diff(row.want, document.Object); diff != "" {
				t.Fatalf("document after the write (-want +got):\n%s", diff)
			}
		})
	}
}
