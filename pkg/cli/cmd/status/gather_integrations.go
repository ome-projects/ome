package status

import (
	"context"
	"errors"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/acceleratorprojection"
	"sigs.k8s.io/ome/pkg/cli/effective"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/paging"
	r "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	omeclient "sigs.k8s.io/ome/pkg/client/clientset/versioned"
)

func gatherStatusIntegrations(
	ctx context.Context,
	f factory.Factory,
	omeClient omeclient.Interface,
	kubeClient kubernetes.Interface,
	snapshot *report,
	omeNamespace string,
) error {
	v := snapshot.ISVC
	if effective.IsServiceVirtualDeployment(v) || v.Spec.Runtime == nil || v.Spec.Runtime.Name == "" {
		return nil
	}
	markUnavailable := func(reason r.StatusSummaryReason) {
		snapshot.RuntimeReason = reason
		snapshot.AcceleratorReason = reason
	}
	var runtimeClient ctrlclient.Client
	var err error
	if action, ok := f.(factory.ActionRuntimeResolver); ok {
		runtimeClient, err = action.RuntimeClientForAction(ctx)
	} else {
		runtimeClient, err = f.RuntimeClient()
	}
	if ctx.Err() != nil {
		return errStatusCancelled
	}
	if err != nil || runtimeClient == nil {
		markUnavailable(r.StatusReasonReadFailed)
		return nil
	}
	limits := paging.Limits{PageSize: 32, MaxItems: 64, MaxPages: 2, RequestTimeout: 5 * time.Second}
	live, err := effective.NewBoundedRuntimeResolver(runtimeClient, limits)
	if err != nil {
		markUnavailable(r.StatusReasonProjectionInvalid)
		return nil
	}
	pin, err := effective.NewRuntimePinResolver(kubeClient.AppsV1(), live, omeNamespace, limits)
	if err != nil {
		markUnavailable(r.StatusReasonProjectionInvalid)
		return nil
	}
	state, err := pin.Resolve(ctx, v, effective.RuntimeResolveOptions{IncludeHistory: false})
	if ctx.Err() != nil {
		return errStatusCancelled
	}
	if err != nil || state == nil || !state.MatchesInferenceService(v) {
		markUnavailable(r.StatusReasonReadFailed)
		return nil
	}
	snapshot.RuntimeState = state
	base, err := effective.ResolveAcceleratorBase(v, state)
	if err != nil {
		snapshot.AcceleratorReason = r.StatusReasonProjectionInvalid
		return nil
	}
	snapshot.AcceleratorBase = &base
	if base.ActiveState != effective.AcceleratorActiveAvailable {
		return nil
	}
	classes, err := collectStatusAcceleratorClasses(ctx, omeClient, v)
	if err != nil {
		return err
	}
	snapshot.AcceleratorClasses = classes
	return nil
}

// collectStatusAcceleratorClasses performs no LIST. Only current, valid class
// references from Engine/Decoder status can trigger an exact cluster GET.
func collectStatusAcceleratorClasses(
	ctx context.Context,
	client omeclient.Interface,
	v *ome.InferenceService,
) (map[string]acceleratorprojection.AcceleratorClassEvidence, error) {
	classes := make(map[string]acceleratorprojection.AcceleratorClassEvidence, 2)
	for _, name := range acceleratorprojection.ReportedAcceleratorClassNames(v) {
		requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		class, getErr := client.OmeV1beta1().AcceleratorClasses().Get(requestCtx, name, metav1.GetOptions{})
		cancel()
		if ctx.Err() != nil {
			return nil, errStatusCancelled
		}
		if getErr != nil {
			classes[name], _ = acceleratorprojection.UnavailableAcceleratorClass(name, statusClassReadState(getErr))
			continue
		}
		observed, err := acceleratorprojection.ObserveAcceleratorClass(class)
		if err != nil || class.Name != name {
			classes[name], _ = acceleratorprojection.UnavailableAcceleratorClass(name, r.AcceleratorClassInvalid)
			continue
		}
		classes[name] = observed
	}
	return classes, nil
}

func statusClassReadState(err error) r.AcceleratorClassState {
	switch {
	case apierrors.IsForbidden(err), apierrors.IsUnauthorized(err):
		return r.AcceleratorClassForbidden
	case apierrors.IsNotFound(err):
		var status apierrors.APIStatus
		if errors.As(err, &status) {
			if details := status.Status().Details; details == nil || details.Name == "" {
				return r.AcceleratorClassUnsupportedAPI
			}
		}
		return r.AcceleratorClassNotFound
	default:
		return r.AcceleratorClassUnreadable
	}
}
