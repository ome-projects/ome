package irprojector

import (
	"context"
	"strings"
	"testing"

	"github.com/go-logr/logr/funcr"
	"github.com/onsi/gomega"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// captureLogs returns a context carrying a V(1)-enabled logger and a getter
// for everything it has emitted so far.
func captureLogs(t *testing.T) (context.Context, func() string, func()) {
	t.Helper()
	var sb strings.Builder
	logger := funcr.New(func(prefix, args string) {
		sb.WriteString(prefix + " " + args + "\n")
	}, funcr.Options{Verbosity: 1})
	return log.IntoContext(context.Background(), logger),
		sb.String,
		sb.Reset
}

// TestEnsureInferenceReplica_ChangedProjectionLogsPatch pins the diagnostic
// contract of the spec write: a projector write bumps the IR's generation and
// so starves every downstream fresh-snapshot consumer if it repeats each pass.
// The write must therefore name the field that caused it.
func TestEnsureInferenceReplica_ChangedProjectionLogsPatch(t *testing.T) {
	g := gomega.NewWithT(t)
	ctx, logged, reset := captureLogs(t)

	isvc := baselineISVC("llama", "prod")
	p := minimalParams(t, isvc, fake.NewClientBuilder().WithScheme(testScheme(t)).Build())

	_, err := EnsureInferenceReplica(ctx, p)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	reset()

	changed := 5
	isvc.Spec.Engine.MinReplicas = &changed
	_, err = EnsureInferenceReplica(ctx, p)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	out := logged()
	g.Expect(out).To(gomega.ContainSubstring("InferenceReplica projection changed"),
		"a spec write must be logged so it can be attributed to a field")
	g.Expect(out).To(gomega.ContainSubstring(`replicas\":5`),
		"the logged merge patch must name the changed field path")
	g.Expect(out).To(gomega.ContainSubstring("generation"),
		"the logged line must carry the pre-write generation")
}

// TestEnsureInferenceReplica_UnchangedProjectionLogsNothing pins the other
// half: the no-op guard returns before the write, so a steady-state pass emits
// no write log. Logging it unconditionally would make the wedge the log exists
// to diagnose indistinguishable from normal operation.
func TestEnsureInferenceReplica_UnchangedProjectionLogsNothing(t *testing.T) {
	g := gomega.NewWithT(t)
	ctx, logged, reset := captureLogs(t)

	isvc := baselineISVC("llama", "prod")
	p := minimalParams(t, isvc, fake.NewClientBuilder().WithScheme(testScheme(t)).Build())

	_, err := EnsureInferenceReplica(ctx, p)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	reset()

	_, err = EnsureInferenceReplica(ctx, p)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	g.Expect(logged()).NotTo(gomega.ContainSubstring("InferenceReplica projection changed"),
		"an unchanged projection issues no write and must log none")
}

// TestBoundPatchForLog_TruncatesOversizedPatch pins the log-size guard: a
// patch larger than the cap is cut and explicitly marked, never silently
// shortened.
func TestBoundPatchForLog_TruncatesOversizedPatch(t *testing.T) {
	g := gomega.NewWithT(t)

	short := []byte(`{"spec":{"replicas":5}}`)
	g.Expect(boundPatchForLog(short)).To(gomega.Equal(string(short)))

	long := []byte(strings.Repeat("x", maxLoggedPatchBytes+64))
	got := boundPatchForLog(long)
	g.Expect(got).To(gomega.HaveSuffix("...(truncated)"))
	g.Expect(len(got)).To(gomega.BeNumerically("<", len(long)))
}
