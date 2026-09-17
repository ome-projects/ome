package runtime

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"k8s.io/cli-runtime/pkg/genericiooptions"

	"sigs.k8s.io/ome/pkg/cli/factory"
)

type syncNamespaceOrderFactory struct {
	factory.Static
	calls int
}

func (f *syncNamespaceOrderFactory) Namespace() (string, bool, error) {
	f.calls++
	return "", false, errors.New("PRIVATE_FACTORY_ACQUISITION")
}

func TestRuntimeSyncRejectsInvalidOMENamespaceBeforeAcquisition(t *testing.T) {
	for _, value := range []string{"", "Bad_PRIVATE", "private\nnamespace"} {
		t.Run(value, func(t *testing.T) {
			f := &syncNamespaceOrderFactory{}
			var out, stderr bytes.Buffer
			cmd := newSyncCmd(f, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr})
			cmd.SilenceErrors, cmd.SilenceUsage = true, true
			cmd.SetArgs([]string{"chat", "--yes", "--dry-run=client", "--ome-namespace=" + value})
			err := cmd.Execute()
			if err == nil || f.calls != 0 || out.Len() != 0 || stderr.Len() != 0 || strings.Contains(err.Error(), "PRIVATE") {
				t.Fatalf("invalid OME namespace acquired factory or emitted private output: acquisitions=%d err=%v", f.calls, err)
			}
			if err.Error() != "runtime sync namespaces are invalid" {
				t.Fatalf("unexpected diagnostic: %s", err)
			}
		})
	}
}
