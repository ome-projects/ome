// Package render turns a resolved model, runtime and role spec into the pod
// templates and runners an InferenceReplica runs. The InferenceService
// controller and the InferenceReplica controller both render through it, so a
// projected replica and a standalone replica built from the same inputs carry
// the same runners and the same revision hash.
package render
