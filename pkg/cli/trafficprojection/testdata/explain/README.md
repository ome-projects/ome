# Traffic explain output contracts

Each small `*.input.json` API snapshot has four exact expected outputs:
`compact`, `wide`, `json`, and `yaml`. Tests only read these files. There is no
automatic update mode and no comparison between two calls to the renderer.

The expectations were specified independently from a literal report model and
formatted separately from the production projector and renderers. The model
was checked against the public schema and the existing status projector's
contract. In particular, current Pending implies no unsupported fields, while
stale AcceptedByGateway leaves that classification Unknown.

The healthy snapshot contains only one declared algorithm, one policy, one
route, and one current readiness condition. Each variant changes a focused
part of that snapshot:

- `partial`: missing readiness with an existing policy reference.
- `unavailable`: no traffic status or realization evidence.
- `unsupported`: current unsupported-fields condition.
- `mismatch`: different declared and reported algorithms.
- `stale`: readiness from the previous generation.
- `stale-unsupported`: current readiness plus historical unsupported fields.
- `malformed`: invalid policy identity, omitted safely from output.
- `absent-translator`: pending readiness with no policy reference.
- `noop-translator`: explicit NoTranslatorAvailable rejection.
- `hostile-secret`: credentials in an endpoint, metadata, a condition message,
  and a declared header name; none may appear in any output format.
- `canary100`: promoting at 100%, with only a canary allocation; the stable
  revision remains visible in the separate wide canary identity rows.
- `canary-only`: the same rollout without declared backend-policy intent and
  with the documented nil TrafficStatus shape.
- `completed-canary`: the terminal step sentinel, no stable revision hash,
  and exactly the single stable allocation reported by the controller.

Support and realization sources are explicit in machine summaries. Historical
unsupported evidence makes computed support Stale while a current route remains
Reported/Current. A discarded hostile endpoint makes realization
Computed/Unverifiable; a malformed policy instead invalidates support and its
comparison, preserving the independent algorithm and route evidence.

When intentional public output changes require editing these contracts,
review all four formats and retain the independent evidence/freshness labels.
