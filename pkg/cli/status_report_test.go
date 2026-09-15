package cli

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"knative.dev/pkg/apis"
	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	r "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/yaml"
)

func TestRootTypedStatusRemainsObservationalAcrossFormats(t *testing.T) {
	for _, format := range []string{"table", "wide", "json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			v := &ome.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "prod", UID: "private-uid", ResourceVersion: "private-rv"}}
			v.Status.Conditions = []apis.Condition{{Type: apis.ConditionReady, Status: corev1.ConditionFalse, Reason: "Waiting"}}
			f := factory.Static{OME: omefake.NewSimpleClientset(v), Kube: kubefake.NewSimpleClientset(), NS: "prod"}
			var out, stderr bytes.Buffer
			root := NewRootCmdWithFactory(f, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr})
			root.SetArgs([]string{"status", "chat", "--namespace", "prod", "-o", format})
			require.NoError(t, root.Execute())
			require.Empty(t, stderr.String())
			require.NotContains(t, out.String(), "private-")
			if format == "json" || format == "yaml" {
				var got r.StatusReport
				if format == "json" {
					require.NoError(t, json.Unmarshal(out.Bytes(), &got))
				} else {
					require.NoError(t, yaml.Unmarshal(out.Bytes(), &got))
				}
				require.Equal(t, "StatusReport", got.Kind)
				require.Equal(t, r.StatusReadyState("False"), got.Content.Ready.Status)
			} else {
				require.Contains(t, out.String(), "False / Valid")
			}
		})
	}
}
