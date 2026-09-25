package evidence

// The readings the external tests pin under their own names. Exported
// from the test build only: the readings are consumed inside the package
// and have no caller outside it.
var (
	PodRunningNotReady    = podRunningNotReady
	PodGateNotFolded      = podGateNotFolded
	PodRuntimeImagesMatch = podRuntimeImagesMatch
)
