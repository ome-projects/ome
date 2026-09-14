package v1alpha1_test

import (
	"reflect"
	"testing"

	"sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func TestMigrationHistoryReportSchemaHasNoUnstructuredOrSecretBearingFields(t *testing.T) {
	t.Parallel()

	assertTypedSchema(t, reflect.TypeOf(v1alpha1.MigrationHistoryReport{}), map[reflect.Type]bool{})
}
