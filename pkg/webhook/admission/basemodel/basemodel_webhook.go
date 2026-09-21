package basemodel

import (
	"context"
	"net/http"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/validation"
)

var log = logf.Log.WithName("basemodel-validation-webhook")

// +kubebuilder:webhook:verbs=create;update,path=/validate-ome-io-v1beta1-basemodel,mutating=false,failurePolicy=fail,groups=ome.io,resources=basemodels,versions=v1beta1,name=basemodel.ome-webhook-server.validator
// +kubebuilder:object:generate=false

// BaseModelValidator denies BaseModels whose PVC URI violates the PVC
// URI shape rules. Pure schema check — no API server reads, hence no Client.
type BaseModelValidator struct {
	Decoder admission.Decoder
}

func (v *BaseModelValidator) Handle(_ context.Context, req admission.Request) admission.Response {
	bm := &v1beta1.BaseModel{}
	if err := v.Decoder.Decode(req, bm); err != nil {
		log.Error(err, "Failed to decode BaseModel", "name", bm.Name, "namespace", bm.Namespace)
		return admission.Errored(http.StatusBadRequest, err)
	}
	if err := validation.ValidatePVCStorage(&bm.Spec, false); err != nil {
		return admission.Denied(err.Error())
	}
	if err := validation.ValidateStageStorage(&bm.Spec); err != nil {
		return admission.Denied(err.Error())
	}
	return admission.Allowed("")
}

// +kubebuilder:webhook:verbs=create;update,path=/validate-ome-io-v1beta1-clusterbasemodel,mutating=false,failurePolicy=fail,groups=ome.io,resources=clusterbasemodels,versions=v1beta1,name=clusterbasemodel.ome-webhook-server.validator
// +kubebuilder:object:generate=false

// ClusterBaseModelValidator is the cluster-scoped sibling of
// BaseModelValidator; same shape rules, no API server reads.
type ClusterBaseModelValidator struct {
	Decoder admission.Decoder
}

func (v *ClusterBaseModelValidator) Handle(_ context.Context, req admission.Request) admission.Response {
	cbm := &v1beta1.ClusterBaseModel{}
	if err := v.Decoder.Decode(req, cbm); err != nil {
		log.Error(err, "Failed to decode ClusterBaseModel", "name", cbm.Name)
		return admission.Errored(http.StatusBadRequest, err)
	}
	if err := validation.ValidatePVCStorage(&cbm.Spec, true); err != nil {
		return admission.Denied(err.Error())
	}
	if err := validation.ValidateStageStorage(&cbm.Spec); err != nil {
		return admission.Denied(err.Error())
	}
	return admission.Allowed("")
}
