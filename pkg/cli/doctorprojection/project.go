// Package doctorprojection derives typed reports only from safe read snapshots.
package doctorprojection

import (
	"strconv"
	"strings"

	"sigs.k8s.io/ome/pkg/cli/doctorcollection"
	r "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

// Project does not infer running operator compatibility from Deployment images.
func Project(snapshot doctorcollection.Snapshot, clientVersion string, clock r.Clock) r.DoctorReport {
	content := r.DoctorContent{
		Context: r.DoctorContext{Name: snapshot.Selection.ContextName, WorkloadNamespace: snapshot.Selection.WorkloadNamespace, OMENamespace: snapshot.Selection.OMENamespace},
		APIs:    snapshot.APIs, Reads: snapshot.Reads, Features: snapshot.Features,
		Version: r.DoctorVersion{ClientVersion: clientVersion, ImageState: snapshot.Manager.ImageState, ImageVersionCandidate: snapshot.Manager.ImageVersionCandidate, ImageTagComparison: compare(clientVersion, snapshot.Manager.ImageVersionCandidate)},
	}
	result := r.DoctorReport{Envelope: r.NewEnvelope("DoctorReport", r.Metadata{Name: "doctor", Namespace: snapshot.Selection.WorkloadNamespace}, content, clock)}
	result.Sources = snapshot.Sources
	result.Warnings = snapshot.Warnings
	return result.Canonical()
}

func compare(client, image string) r.DoctorComparison {
	client, image = r.CanonicalDoctorVersion(client), r.CanonicalDoctorVersion(image)
	if client == "" || image == "" {
		return r.DoctorComparisonUnknown
	}
	c := strings.Split(strings.TrimPrefix(client, "v"), ".")
	i := strings.Split(strings.TrimPrefix(image, "v"), ".")
	if c[0] != i[0] {
		return r.DoctorMajorMismatch
	}
	cm, _ := strconv.ParseUint(c[1], 10, 64)
	im, _ := strconv.ParseUint(i[1], 10, 64)
	if cm > im {
		cm, im = im, cm
	}
	if im-cm > 1 {
		return r.DoctorOutsideMinor
	}
	return r.DoctorWithinMinor
}
