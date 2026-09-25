// Package holds owns InstanceOperation.Waiting: every external hold an
// Instance can report, the rules that order them, and the one point in
// the reconcile where they are recorded and released.
//
// Several authorities outside the workload can stall an operation
// without anything being wrong with it — admission refusing a create for
// lack of quota, the scheduler finding no placement, a PodGroup name
// still being collected, a node that stopped reporting a pod whose name
// the operation needs, a surge whose source left the rotation, an
// operator pause. Each records itself as a short token on Waiting, which
// is what parks the InstanceReadyTimeout clock and what the escalation
// pass reads to leave the row alone. The quota wait alone has a second
// carrier: the refusal is an apiserver answer the create site records
// on the operation, the token is written from that record on the next
// pass, and the deadline parks on either — so the clock stops on the
// pass admission said no and stays stopped while the token lingers.
//
// Two rules make that safe with six writers:
//
//   - RECORD EARLY. The pass runs once per observed row at the top of the
//     pass, before any verb pass: every token is written as soon as its
//     condition is observed, releases before records, so a row that
//     stops waiting on one authority and starts waiting on another does
//     it inside one pass. Left unheld for even one pass, the parking
//     step reads the empty token as admission and restarts a whole
//     InstanceReadyTimeout.
//   - ACT AT THE BOUNDARY. Recording a token decides nothing about the
//     step in flight. Whether the owner abandons its step now or
//     finishes it first is the ownership table's Interruptible answer
//     (types.Interruptible), never this package's.
//
// The verb passes and the escalation pass READ holds. Nothing outside
// this package writes one.
package holds
