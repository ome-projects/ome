package routing

// RoutingControllerName names the routing (TrafficMap generator) controller —
// explicit so it doesn't collide with the placement or endpoint controllers,
// which also watch InferenceService.
const RoutingControllerName = "placement-routing"
