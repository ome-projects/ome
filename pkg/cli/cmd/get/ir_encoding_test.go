package get

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
)

func TestInferenceReplicaEncodingColumnDoesNotGuessMalformedUnion(t *testing.T) {
	entry, err := resolve("ir")
	require.NoError(t, err)
	var encodingColumn *column
	for i := range entry.Columns {
		if entry.Columns[i].Name == "ENCODING" {
			encodingColumn = &entry.Columns[i]
			break
		}
	}
	require.NotNil(t, encodingColumn)
	assert.True(t, encodingColumn.Wide, "default inventory must not grow wider")

	columnar := v1beta1.InstanceStatusEncodingColumnarV2
	unknown := v1beta1.InstanceStatusEncoding("FutureV3")
	columns := &v1beta1.InstanceStatusColumns{Members: "0", Phases: []v1beta1.InstanceStatusPhaseGroup{{Value: v1beta1.OMENativeInstanceReady, Indexes: "0"}}}
	for _, tc := range []struct {
		name   string
		status v1beta1.InferenceReplicaStatus
		want   string
	}{
		{name: "unmarked dense", want: "DenseV1"},
		{name: "marked columnar", status: v1beta1.InferenceReplicaStatus{InstanceStatusEncoding: &columnar, InstanceStatusColumns: columns}, want: "ColumnarV2"},
		{name: "unmarked columns", status: v1beta1.InferenceReplicaStatus{InstanceStatusColumns: columns}, want: "Invalid"},
		{name: "marked without columns", status: v1beta1.InferenceReplicaStatus{InstanceStatusEncoding: &columnar}, want: "Invalid"},
		{name: "mixed representations", status: v1beta1.InferenceReplicaStatus{InstanceStatusEncoding: &columnar, InstanceStatusColumns: columns, InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{Index: 0}}}, want: "Invalid"},
		{name: "unknown marker", status: v1beta1.InferenceReplicaStatus{InstanceStatusEncoding: &unknown, InstanceStatusColumns: columns}, want: "Unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, encodingColumn.Extract(&v1beta1.InferenceReplica{Status: tc.status}))
		})
	}
}

func TestGetInferenceReplicaEncodingWideUsesFetchedStatus(t *testing.T) {
	columnar := v1beta1.InstanceStatusEncodingColumnarV2
	columns := &v1beta1.InstanceStatusColumns{Members: "0", Phases: []v1beta1.InstanceStatusPhaseGroup{{Value: v1beta1.OMENativeInstanceReady, Indexes: "0"}}}
	fake := omefake.NewSimpleClientset(
		&v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Name: "dense", Namespace: "demo"}},
		&v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Name: "columnar", Namespace: "demo"}, Status: v1beta1.InferenceReplicaStatus{InstanceStatusEncoding: &columnar, InstanceStatusColumns: columns}},
	)
	f := factory.Static{OME: fake, NS: "demo"}
	wide, err := execute(t, f, "ir", "-o", "wide")
	require.NoError(t, err)
	assert.Contains(t, wide, "ENCODING")
	assert.Contains(t, wide, "DenseV1")
	assert.Contains(t, wide, "ColumnarV2")
	require.Len(t, fake.Actions(), 1, "encoding extraction must not fetch more resources")
	assert.Equal(t, "list", fake.Actions()[0].GetVerb())

	def, err := execute(t, f, "ir")
	require.NoError(t, err)
	assert.NotContains(t, def, "ENCODING")
	assert.NotContains(t, def, "ColumnarV2")

	jsonOut, err := execute(t, f, "ir", "columnar", "-o", "json")
	require.NoError(t, err)
	assert.Contains(t, jsonOut, `"instanceStatusEncoding": "ColumnarV2"`)
	assert.Contains(t, jsonOut, `"instanceStatusColumns"`)
	assert.NotContains(t, jsonOut, `"encoding":`)
	yamlOut, err := execute(t, f, "ir", "columnar", "-o", "yaml")
	require.NoError(t, err)
	assert.Contains(t, yamlOut, "instanceStatusEncoding: ColumnarV2")
	assert.Contains(t, yamlOut, "instanceStatusColumns:")
	assert.NotContains(t, yamlOut, "encoding:")

	t.Logf("wide output (synthetic fixture):\n%s", wide)
}
