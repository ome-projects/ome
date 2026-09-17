package mutate

import "sigs.k8s.io/ome/pkg/cli/actionbounds"

func boundedPrivatePayload(input any) bool { return actionbounds.PrivatePayload(input) }
