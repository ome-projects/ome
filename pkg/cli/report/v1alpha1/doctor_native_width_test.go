package v1alpha1

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/cli/printers"
)

func TestDoctorFullCatalogWideFits80DisplayColumns(t *testing.T) {
	c := DoctorContent{
		Context: DoctorContext{Name: strings.Repeat("測", 60), WorkloadNamespace: strings.Repeat("a", 63), OMENamespace: strings.Repeat("b", 63)},
		APIs:    DoctorAPICatalog(),
		Reads:   []DoctorRead{{ID: DoctorReadManager, Namespace: strings.Repeat("b", 63), Outcome: DoctorAvailable}, {ID: DoctorReadISVC, Namespace: strings.Repeat("a", 63), Name: strings.Repeat("c", 100), Outcome: DoctorAvailable}},
	}
	for i := range c.APIs {
		c.APIs[i].Availability = DoctorNotDiscoverable
	}
	var out bytes.Buffer
	require.NoError(t, c.WideTable().Write(&out))
	for _, row := range strings.Split(out.String(), "\n") {
		require.LessOrEqual(t, printers.CellDisplayWidth(row), 80, "wide output must fit a terminal and non-TTY capture")
	}
	require.Contains(t, out.String(), "NotDiscoverableAtVersion", "wide retains the full evidence state")
	require.Contains(t, out.String(), "GET-only; not authorization", "the authority caveat must remain readable, not truncated")
	require.Contains(t, out.String(), "Computed; not compatibility", "an image tag is not proof of running compatibility")
	require.Contains(t, out.String(), "Use -o json or -o yaml", "shortened safe facts need an expansion hint")
}

func TestDoctorFineTunedWeightUsesActualClusterScope(t *testing.T) {
	for _, api := range DoctorAPICatalog() {
		if api.Resource == "finetunedweights" {
			require.Equal(t, "FineTunedWeight", api.Kind)
			require.Equal(t, "Cluster", api.Scope, "matches config/crd/full/ome.io_finetunedweights.yaml")
			require.False(t, api.Required)
			return
		}
	}
	t.Fatal("FineTunedWeight missing from the closed diagnostic catalog")
}
