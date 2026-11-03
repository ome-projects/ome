package basemodel

import (
	"context"
	"net/http"

	"sigs.k8s.io/controller-runtime/pkg/client"
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
	return validate(v.Decoder, req, bm, &bm.Spec, false)
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
	return validate(v.Decoder, req, cbm, &cbm.Spec, true)
}

// validate decodes the request into model and refuses a PVC URI whose
// shape does not fit the model's scope (spec is the field inside model).
func validate(decoder admission.Decoder, req admission.Request, model client.Object, spec *v1beta1.BaseModelSpec, clusterScoped bool) admission.Response {
	if err := decoder.Decode(req, model); err != nil {
		log.Error(err, "Failed to decode "+req.Kind.Kind, "name", model.GetName(), "namespace", model.GetNamespace())
		return admission.Errored(http.StatusBadRequest, err)
	}
	if err := validation.ValidatePVCStorage(spec, clusterScoped); err != nil {
		return admission.Denied(err.Error())
	}
	return admission.Allowed("")
}
