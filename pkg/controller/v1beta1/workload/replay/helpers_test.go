package replay

import (
	"time"
)

// minimalScenario is the smallest loadable timeline, for tests that need a
// constructed driver rather than a full replay.
const minimalScenario = `
scenario: minimal
arrows: [T-empty-create]
initial:
  spec:
    replicas: 1
    image: registry.example.com/runtime:v1
timeline:
  - tick: 1
`

var traceStart = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
