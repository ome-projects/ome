// Package reading reads an Instance's pods once per pass and answers every
// "is this pod serving" question from that one reading.
//
// A PodReading places one pod on six levels: Live, RuntimeReady, Serving,
// Routed, Available and Pairable. An InstanceReading holds the readings of
// one Instance's pods, counts them per level and gives the verdicts the
// passes act on: FullyServing, ServesNothing, OutOfRotation,
// ContainersUnreadyPastGrace, UnavailableOnHeldMember and PairableServing.
//
// The package is pure. It takes the pods, the pass's clock, the Component's
// windows, the rotation set of the routed Service and the peer Component's
// pairing picture through Context, and it holds no client, no clock and no
// logger. The compare subpackage sets the readers the passes use beside
// the reading so a test can count where they disagree.
package reading
