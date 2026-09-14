package logs

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	k8svalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/observation"
	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

const maxInstanceIndex int64 = 1<<31 - 1

var (
	revisionHashPattern = regexp.MustCompile(`^[0-9a-f]{8}$`)
	fullRevisionPattern = regexp.MustCompile(`^(.+)-(engine|decoder|router)-([0-9a-f]{8})$`)
)

type dependencies struct {
	podLimits paging.Limits
}

var defaultDependencies = dependencies{podLimits: paging.Limits{
	PageSize:       50,
	MaxItems:       200,
	MaxPages:       10,
	RequestTimeout: 10 * time.Second,
}}

type Options struct {
	genericiooptions.IOStreams
	Name      string
	Component string
	Instance  int64
	Revision  string
	Container string
	Follow    bool
	Tail      int64
	Since     time.Duration

	instanceSet  bool
	revisionHash string
}

func NewCmd(f factory.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	return newCmdWithDependencies(f, streams, defaultDependencies)
}

func newCmdWithDependencies(f factory.Factory, streams genericiooptions.IOStreams, deps dependencies) *cobra.Command {
	o := &Options{IOStreams: streams, Instance: -1, Tail: -1}
	cmd := &cobra.Command{
		Use:   "logs INFERENCESERVICE",
		Short: "Stream logs from the pods behind an InferenceService",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			o.Name = args[0]
			o.instanceSet = cmd.Flags().Changed("instance")
			if err := o.Validate(); err != nil {
				return err
			}
			return o.run(cmd.Context(), f, deps)
		},
	}
	cmd.Flags().StringVarP(&o.Component, "component", "c", "", "Only this component: engine, decoder or router")
	cmd.Flags().Int64Var(&o.Instance, "instance", o.Instance, "Only this OMENative instance index (requires --component or a full --revision)")
	cmd.Flags().StringVar(&o.Revision, "revision", "", "Only this OMENative revision hash or full ControllerRevision name")
	cmd.Flags().StringVar(&o.Container, "container", "", "Container name (default: the OME main container, falling back to the pod's first container)")
	cmd.Flags().BoolVarP(&o.Follow, "follow", "f", false, "Stream new log lines as they arrive")
	cmd.Flags().Int64Var(&o.Tail, "tail", o.Tail, "Lines of recent log to show per pod (-1 for all)")
	cmd.Flags().DurationVar(&o.Since, "since", 0, "Only logs newer than this duration (e.g. 10m)")
	return cmd
}

func (o *Options) Validate() error {
	if problems := k8svalidation.IsDNS1123Subdomain(o.Name); len(problems) > 0 {
		return fmt.Errorf("invalid InferenceService name %q: %s", o.Name, problems[0])
	}
	switch o.Component {
	case "", "engine", "decoder", "router":
	default:
		return fmt.Errorf("invalid component %q (valid: engine, decoder, router)", o.Component)
	}
	if o.instanceSet {
		if o.Instance < 0 || o.Instance > maxInstanceIndex {
			return fmt.Errorf("--instance must be between 0 and %d", maxInstanceIndex)
		}
	}
	if o.Revision == "" {
		if o.instanceSet && o.Component == "" {
			return fmt.Errorf("--instance requires --component")
		}
		return nil
	}
	if revisionHashPattern.MatchString(o.Revision) {
		if o.Component == "" {
			return fmt.Errorf("hash-only --revision requires --component")
		}
		o.revisionHash = o.Revision
		return nil
	}
	parts := fullRevisionPattern.FindStringSubmatch(o.Revision)
	if parts == nil {
		return fmt.Errorf("invalid revision %q (expected 8 lowercase hexadecimal characters or <inferenceservice>-<component>-<hash>)", o.Revision)
	}
	if parts[1] != o.Name {
		return fmt.Errorf("revision %q does not belong to InferenceService %q", o.Revision, o.Name)
	}
	if o.Component != "" && o.Component != parts[2] {
		return fmt.Errorf("revision component %q does not match --component %q", parts[2], o.Component)
	}
	o.Component = parts[2]
	o.revisionHash = parts[3]
	return nil
}

func (o *Options) Run(ctx context.Context, f factory.Factory) error {
	return o.run(ctx, f, defaultDependencies)
}

func (o *Options) run(ctx context.Context, f factory.Factory, deps dependencies) error {
	ns, _, err := f.Namespace()
	if err != nil {
		return err
	}
	kube, err := f.KubeClient()
	if err != nil {
		return err
	}
	selectorLabels := labels.Set{constants.InferenceServiceLabel: o.Name}
	if o.Component != "" {
		selectorLabels[constants.OMEComponentLabel] = o.Component
	}
	if o.instanceSet || o.Revision != "" {
		selectorLabels[query.LabelManagedBy] = query.ManagedByOMENative
	}
	if o.instanceSet {
		selectorLabels[query.LabelInstanceIdx] = strconv.FormatInt(o.Instance, 10)
	}
	if o.Revision != "" {
		selectorLabels[query.LabelRevisionHash] = o.revisionHash
	}
	selector := selectorLabels.AsSelector().String()
	collection, err := observation.CollectPods(ctx, kube.CoreV1(), ns, selector, deps.podLimits)
	if err != nil {
		return fmt.Errorf("discover pods: %w", err)
	}
	if collection.Truncated {
		return fmt.Errorf("pod discovery was truncated after %d requests; narrow the query with --component, --instance, or --revision", collection.Requests)
	}
	pods := make([]corev1.Pod, 0, len(collection.Items))
	for _, pod := range collection.Items {
		if pod.Namespace == ns {
			pods = append(pods, pod)
		}
	}
	if len(pods) == 0 {
		return fmt.Errorf("no pods found for InferenceService %q in namespace %q (selector %s)", o.Name, ns, selector)
	}
	opts := &corev1.PodLogOptions{Follow: o.Follow}
	if o.Tail >= 0 {
		opts.TailLines = &o.Tail
	}
	if o.Since > 0 {
		secs := int64(o.Since.Seconds())
		opts.SinceSeconds = &secs
	}
	var streams []namedStream
	for _, p := range pods {
		po := *opts
		po.Container = o.containerFor(&p)
		req := kube.CoreV1().Pods(ns).GetLogs(p.Name, &po)
		reader, err := req.Stream(ctx)
		if err != nil {
			// Don't leak the API-server log connections already opened for
			// earlier pods in this loop: multiplex() never gets to run its
			// deferred Close on them since we're bailing out before the call.
			for _, s := range streams {
				_ = s.Reader.Close()
			}
			return fmt.Errorf("streaming logs for pod %s: %w", p.Name, err)
		}
		prefix := ""
		if len(pods) > 1 {
			prefix = fmt.Sprintf("[%s/%s] ", p.Labels[constants.OMEComponentLabel], p.Name)
		}
		streams = append(streams, namedStream{Prefix: prefix, Reader: reader})
	}
	return multiplex(streams, o.Out)
}

// containerFor picks --container if set, else the OME main container when the
// pod has one, else the first container.
func (o *Options) containerFor(p *corev1.Pod) string {
	if o.Container != "" {
		return o.Container
	}
	for _, c := range p.Spec.Containers {
		if c.Name == constants.MainContainerName {
			return c.Name
		}
	}
	return "" // empty lets the API default to the only/first container
}
