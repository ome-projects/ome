// Package version implements `kubectl ome version`.
package version

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"sigs.k8s.io/ome/pkg/cli/apierror"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/safetext"
	"sigs.k8s.io/ome/pkg/version"
)

const maxVersionDisplayWidth = 256

type Options struct {
	genericiooptions.IOStreams
	OMENamespace string
}

func NewCmd(f factory.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	o := &Options{IOStreams: streams, OMENamespace: "ome"}
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Print kubectl-ome and OME operator versions",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return o.Run(cmd.Context(), f)
		},
	}
	cmd.Flags().StringVar(&o.OMENamespace, "ome-namespace", o.OMENamespace,
		"Namespace where the OME control plane is installed")
	return cmd
}

// Run reports the client version and a best-effort operator image identity.
// Lookup failures degrade to "unknown"; output failures remain errors.
func (o *Options) Run(ctx context.Context, f factory.Factory) error {
	client := fmt.Sprintf("%s (commit %s)",
		safetext.Sanitize(version.GitVersion, maxVersionDisplayWidth),
		safetext.Sanitize(version.GitCommit, maxVersionDisplayWidth))
	if _, terminal := printers.TerminalWidth(o.Out); terminal {
		if err := (printers.Table{
			Headers: []string{"COMPONENT", "VERSION"},
			Rows: [][]string{
				{"Client", client},
				{"Operator", safetext.Sanitize(o.operatorVersion(ctx, f), maxVersionDisplayWidth)},
			},
		}).Write(o.Out); err != nil {
			return errors.New("write version output failed")
		}
		return nil
	}
	if _, err := fmt.Fprintf(o.Out, "Client Version: %s\n", client); err != nil {
		return errors.New("write version output failed")
	}
	if _, err := fmt.Fprintf(o.Out, "Operator Version: %s\n", safetext.Sanitize(o.operatorVersion(ctx, f), maxVersionDisplayWidth)); err != nil {
		return errors.New("write version output failed")
	}
	return nil
}

func (o *Options) operatorVersion(ctx context.Context, f factory.Factory) string {
	kube, err := f.KubeClient()
	if err != nil {
		return fmt.Sprintf("unknown (%v)", apierror.SafeRead(err, apierror.ReadTarget{
			Operation: apierror.ReadOperationResolve,
			Resource:  "kubernetes-client",
		}))
	}
	dep, err := kube.AppsV1().Deployments(o.OMENamespace).Get(ctx, "ome-controller-manager", metav1.GetOptions{})
	if err != nil {
		return fmt.Sprintf("unknown (%v)", apierror.SafeRead(err, apierror.ReadTarget{
			Operation: apierror.ReadOperationGet,
			Resource:  "deployments.apps",
			Namespace: o.OMENamespace,
			Name:      "ome-controller-manager",
		}))
	}
	containers := dep.Spec.Template.Spec.Containers
	if len(containers) == 0 {
		return "unknown (manager Deployment has no containers)"
	}
	for _, container := range containers {
		if container.Name == "manager" {
			return imageVersion(container.Image)
		}
	}
	// Older Deployments may not use the manager container name. A sole
	// container is unambiguous; multiple containers must not select a sidecar.
	if len(containers) == 1 {
		return imageVersion(containers[0].Image)
	}
	return "unknown (manager container unavailable)"
}

func imageVersion(image string) string {
	repository, digest, hasDigest := strings.Cut(image, "@")
	tag := ""
	if colon := strings.LastIndex(repository, ":"); colon > strings.LastIndex(repository, "/") && colon < len(repository)-1 {
		tag = repository[colon+1:]
	}
	if hasDigest && digest != "" {
		if tag != "" {
			return tag + "@" + digest
		}
		return digest
	}
	if tag != "" {
		return tag
	}
	return "unknown (image has no tag or digest)"
}
