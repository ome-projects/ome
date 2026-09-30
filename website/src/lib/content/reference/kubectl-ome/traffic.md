---
title: kubectl ome traffic
description: Show the traffic routes, weights and canary split the controller reports for an InferenceService, check declared behavior, and drain a cluster (alpha).
since: v1.3
---

`kubectl ome traffic` shows what the controller reports about how an [InferenceService](../../concepts/serving/inference-services.md) receives traffic, and drains workload clusters out of multi-cluster routing. The read commands report the [traffic policy](../../concepts/rollouts-and-traffic/traffic-policy.md) that OME wrote for your gateway and whether the gateway accepted it, the routes and endpoints in the status, and each canary's progress with the traffic weights that OME records for its revisions. `explain` compares that status with the traffic behavior you declared. The alpha actions add and remove drains, which ask multi-cluster routing to hold a workload cluster at weight 0 in the service's [TrafficMap](../../concepts/rollouts-and-traffic/traffic-map.md). Multi-cluster routing is alpha and still in development.

```text
kubectl ome traffic SUBCOMMAND INFERENCESERVICE [flags]
```

| Subcommand | What it does |
| --- | --- |
| [`status`](#status) | Shows the traffic status the controller reports for an InferenceService. |
| [`explain`](#explain) | Compares declared traffic behavior with the status the controller reports. |
| [`drain`](#drain-and-undrain) | Alpha. Asks multi-cluster routing to hold one workload cluster at weight 0. |
| [`undrain`](#drain-and-undrain) | Alpha. Removes one drain by its ID. |

Each subcommand takes the name of one InferenceService. The CLI looks for it in the namespace you give with `-n`, then in the namespace of your kubeconfig context, then in `default`.

`status` and `explain` read only the InferenceService. They don't read the policy that OME wrote, HTTPRoutes, Services, endpoints or pods. `drain` and `undrain` read the InferenceService and patch its annotations. A server dry run sends the patch too, so it needs the same permission. The [kubectl-ome overview](overview.md) lists the RBAC each command needs, and [Guarded actions](guarded-actions.md) the rule the actions add.

Each subcommand prints a table. For `status` and `explain`, `-o wide` prints more detail. For `drain` and `undrain`, it adds the target's UID and resourceVersion to the result. `-o json` and `-o yaml` print the whole report, with `apiVersion: cli.ome.io/v1alpha1` and the kind `TrafficStatusReport`, `TrafficExplainReport` or `ActionResult`. On a terminal narrower than the table, the CLI wraps long cells, or prints each row as a list of fields when even the headers don't fit. Output to a file or pipe keeps one line per row.

The SOURCE column of `status` and `explain` says how the CLI got each value, as `<evidence>/<freshness>`, or `-` for a value that has neither, such as the report's kind. The evidence level is one of:

| Evidence | Meaning |
| --- | --- |
| `Declared` | Read from the spec or annotations of the InferenceService. |
| `Reported` | Read from the status the controller wrote. |
| `Computed` | Worked out by the CLI from other values. |
| `Unavailable` | The CLI couldn't get the value. |

The freshness says whether the value is tied to the current generation of the InferenceService:

| Freshness | Meaning |
| --- | --- |
| `Current` | The controller wrote the value with a BackendPolicyReady condition whose `observedGeneration` equals the InferenceService's `metadata.generation`. |
| `Stale` | The condition's `observedGeneration` is lower than `metadata.generation`, so the controller hasn't caught up with the latest spec. |
| `Unverifiable` | The CLI can't tie the value to a generation. The algorithm, the policy, the routes and the translator are `Unverifiable` when BackendPolicyReady is missing or malformed. Endpoints, canary progress and revision weights carry no generation, so they're always `Unverifiable`. |
| `Unavailable` | The value is missing, such as every reported value when the status has no traffic section. |

## `status`

```text
kubectl ome traffic status INFERENCESERVICE [flags]
```

`status` shows the traffic status that the controller wrote on the InferenceService: the policy that OME wrote for your gateway and whether the gateway accepted it, the load-balancing algorithm, the HTTPRoutes and endpoints, each canary's progress, and the traffic weight that OME records for each revision. It reads the InferenceService once and nothing else, so the report shows what the controller recorded, not proof that requests reach your pods. It doesn't show the TrafficMap or drains: [`drain` and `undrain`](#drain-and-undrain) say where to read them. [`kubectl ome status`](status.md) shows the same STATE, translator, algorithm and policy readiness in its Traffic row.

### Flags {#status-flags}

| Flag | Default | Description |
| --- | --- | --- |
| `-o`, `--output` | `table` | Output format: table, wide, json or yaml |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

### Output fields {#status-output-fields}

The table has the columns FIELD, COMP, VALUE and SOURCE. COMP names the component a row is about, or shows `-` for the whole service. The first five rows sum up the service:

| Row | What it shows |
| --- | --- |
| STATE | What the traffic status adds up to, from the states in the next table. Its source is always `Computed/Unverifiable`. |
| TRANSLATOR | The translator that wrote the policy, worked out from the kind of the policy that `status.traffic.backendPolicyResource` names: `envoy-gateway` for an Envoy Gateway BackendTrafficPolicy, and `istio` for an Istio DestinationRule. `Unavailable` when the status names no policy, or when BackendPolicyReady says OME has no translator for your gateway. |
| ALGORITHM | The load-balancing algorithm the controller reports: `Default`, `RoundRobin`, `LeastRequest`, `Random` or `ConsistentHash`. `Unknown` when the status has no traffic section, or names an algorithm the CLI doesn't recognize. |
| POLICY-READY | The BackendPolicyReady condition, as `status/reason`. The controller sets `Unknown/Pending`, `True/AcceptedByGateway`, `False/GatewayRejected` or `False/NoTranslatorAvailable`, which [Read the traffic status](../../concepts/rollouts-and-traffic/traffic-policy.md#read-the-traffic-status) explains. `Unknown/NotReported` means the condition is missing. A malformed condition shows its status as `Unknown`, with its reason, or `Unknown/Unknown` when the CLI doesn't recognize the reason. |
| UNSUPPORTED | Whether the translator left out fields you declared. `Present` when the status has a `BackendPolicyUnsupportedFields` condition. `None` when it has none, and BackendPolicyReady is current and says the gateway accepted the policy, rejected it or hasn't decided yet. `Unknown` otherwise. |

STATE is the first of these that applies:

| State | When |
| --- | --- |
| `Invalid` | The status holds a value the CLI rejects, or contradicts itself: any issue marked invalid in the issue table below. |
| `Unavailable` | The status has no traffic section, or no BackendPolicyReady condition. |
| `Partial` | BackendPolicyReady isn't tied to the current generation. |
| `Degraded` | BackendPolicyReady is `False`: the gateway rejected the policy, or OME has no translator for your gateway. |
| `Pending` | BackendPolicyReady is `Unknown`: the gateway hasn't accepted or rejected the policy yet. |
| `Reported` | BackendPolicyReady is `True`: the gateway accepted the policy. |

`Pending` and `Reported` become `Partial` when part of the evidence is missing or cut: UNSUPPORTED is `Present`, a condition is stale, or the report cut a list.

A service that declares no traffic behavior has no `status.traffic`, as [Defaults when you declare nothing](../../concepts/rollouts-and-traffic/traffic-policy.md#defaults-when-you-declare-nothing) describes. For it, STATE is `Unavailable` and ISSUE lists `TrafficStatusMissing`, but the endpoints, canaries and revision weights from the rest of the status still appear.

The rows after the summary list what the status holds:

| Row | What it shows |
| --- | --- |
| ROUTES | How many HTTPRoutes the policy targets, from `status.traffic.targetedHTTPRoutes`. |
| ENDPOINTS | How many URLs the status lists for the service: those in `status.addresses`, or else `status.url` and `status.address.url`. The CLI drops a URL that isn't `http` or `https`, or that has user info, a query or a fragment. |
| CANARY | One row for each canary, with its primary component in COMP, as `step/total @ weight%`: the current step, counting from 1, and the traffic weight the controller recorded in the canary's `observedTrafficWeight`. |
| WEIGHT | One row for each revision in a component's `status.components.<component>.traffic`, as `role:hash=weight%`, where the hash is the end of the revision's name, `<name>-<component>-rev-<hash>`. |
| ISSUE | One row for each issue, with its code in VALUE and its component in COMP, or `-` for the whole service. |

!!! warning "The traffic weight doesn't route requests"
    A canary step sets a traffic weight for the new revision, which OME records in the InferenceService's status. In this release, OME doesn't apply the weight to routing. The Services that OME creates for a component select its ready instances of every revision, so the new revision gets requests in proportion to its share of those instances, which the step's `capacity` sets. [Canary progression](../rollouts/canary-progression.md#traffic-split) has the details.

The role in a WEIGHT row says which revision the weight is for. In the component a CANARY row names, `canary` is the new revision and `stable` the one it replaces, and once the canary has passed its last step, the new revision is `stable`. In other components, `stable` is the revision that finished rolling out, from `latestRolledoutRevision`, or the only revision when that's unset and the revision is marked latest. Any other revision is `other`.

`-o wide` prints the five summary rows, then every value in full in place of the counts and WEIGHT rows:

| Row | What it shows |
| --- | --- |
| POLICY | The policy that OME wrote, as `apiVersion/Kind/namespace/name`. |
| ROUTE | One row for each HTTPRoute. |
| ENDPOINT | One row for each URL. |
| CANARY | As in the default table. |
| TARGET | One row for each revision weight, as `role:revision=weight%`, with the revision's full name. |
| CONDITION | One row for each condition, as `Type=Status/Reason gen=<observedGeneration> at=<lastTransitionTime>`. |
| ISSUE | As in the default table. |

The report keeps at most 4 routes, 16 endpoints and 8 revision weights in each component, the first in alphabetical order, and lists an issue when it cuts one of these lists. An issue marked invalid makes STATE `Invalid`:

| Issue | Invalid | Cause |
| --- | --- | --- |
| `TrafficStatusMissing` | No | The status has no traffic section. |
| `PolicyConditionMissing` | No | The traffic section has no BackendPolicyReady condition. |
| `RoutesTruncated` | No | The status lists more than 4 HTTPRoutes. |
| `EndpointsTruncated` | No | The status lists more than 16 URLs. |
| `AllocationsTruncated` | No | A component lists more than 8 revision weights. |
| `AlgorithmInvalid` | Yes | The status names an algorithm the CLI doesn't recognize. |
| `PolicyReferenceInvalid` | Yes | The policy's name in the status isn't valid. |
| `PolicyKindUnsupported` | Yes | The status names a policy that is neither an Envoy Gateway BackendTrafficPolicy nor an Istio DestinationRule. |
| `ConditionInvalid` | Yes | A condition has an unknown status or reason, a status and reason that don't go together, an `observedGeneration` of 0 or newer than the InferenceService, or no `lastTransitionTime`. |
| `ConditionConflict` | Yes | The status has two conditions of the same type. |
| `RouteInvalid` | Yes | The status lists an HTTPRoute name that isn't valid. |
| `EndpointInvalid` | Yes | The status lists a URL the CLI dropped. |
| `CanaryInvalid` | Yes | The canary record doesn't add up: `status.canary` is set without a canary group in the spec, there are more than 3 canary groups, a group's plan is invalid or has more than 20 steps, a rolling-out component has no canary record, or the record contradicts itself or the spec, such as a recorded weight that isn't the current step's `traffic`, or revision weights that don't match it. |
| `AllocationInvalid` | Yes | A revision weight names a revision of another InferenceService or component, is outside 0 to 100, or a component's weights don't add up to 100, or its stable revision is missing or ambiguous. |
| `AllocationConflict` | Yes | A component lists the same revision twice. |
| `UnknownComponentStatus` | Yes | `status.components` has a key other than `engine`, `decoder` and `router`. |
| `StatusCombinationInvalid` | Yes | The status lists HTTPRoutes or reports the policy accepted without naming a policy, or names a policy and says OME has no translator. |

`-o json` and `-o yaml` print a `TrafficStatusReport`. Its `sources` lists the InferenceService the CLI read, with its UID and generation, and `content` holds every value with its `source`. `warnings` lists `PartialData` when the report has an invalid issue or partial evidence, `StaleEvidence` when a condition is stale, and `Truncated` when the report cut a list. It's empty when none applies. The tables don't show warnings.

### Examples {#status-examples}

`chat` in `prod` declares `RoundRobin` behind Envoy Gateway, and its engine is at the first of two canary steps. OME has written the policy, and the gateway hasn't accepted or rejected it yet:

```bash
kubectl ome traffic status chat -n prod
```

```output
FIELD          COMP     VALUE                 SOURCE
STATE          -        Pending               Computed/Unverifiable
TRANSLATOR     -        envoy-gateway         Computed/Current
ALGORITHM      -        RoundRobin            Reported/Current
POLICY-READY   -        Unknown/Pending       Reported/Current
UNSUPPORTED    -        None                  Reported/Current
ROUTES         -        2                     Reported/Current
ENDPOINTS      -        2                     Reported/Unverifiable
CANARY         engine   1/2 @ 20%             Reported/Unverifiable
WEIGHT         engine   stable:a1b2c3d4=80%   Reported/Unverifiable
WEIGHT         engine   canary:e5f6a7b8=20%   Reported/Unverifiable
```

Show every value behind those counts:

```bash
kubectl ome traffic status chat -n prod -o wide
```

```output
FIELD          COMP     VALUE                                                              SOURCE
STATE          -        Pending                                                            Computed/Unverifiable
TRANSLATOR     -        envoy-gateway                                                      Computed/Current
ALGORITHM      -        RoundRobin                                                         Reported/Current
POLICY-READY   -        Unknown/Pending                                                    Reported/Current
UNSUPPORTED    -        None                                                               Reported/Current
POLICY         -        gateway.envoyproxy.io/v1alpha1/BackendTrafficPolicy/prod/chat      Reported/Current
ROUTE          -        chat                                                               Reported/Current
ROUTE          -        chat-engine                                                        Reported/Current
ENDPOINT       -        http://chat-engine.prod.svc.cluster.local                          Reported/Unverifiable
ENDPOINT       -        https://chat.prod.example/                                         Reported/Unverifiable
CANARY         engine   1/2 @ 20%                                                          Reported/Unverifiable
TARGET         engine   stable:chat-engine-rev-a1b2c3d4=80%                                Reported/Unverifiable
TARGET         engine   canary:chat-engine-rev-e5f6a7b8=20%                                Reported/Unverifiable
CONDITION      -        BackendPolicyReady=Unknown/Pending gen=7 at=2026-09-14T16:59:00Z   Reported/Current
```

With `-o json`, the same report is a `TrafficStatusReport` whose `warnings` is empty, since nothing in it is invalid, stale or cut.

`status` fails before it reads anything with `unsupported output format "<value>" (supported: table, wide, json, yaml)` for another `-o` value, and with `invalid InferenceService name "<name>": ` and the reason for a name that isn't valid. When the read fails, it prints `get InferenceService "<namespace>/<name>": ` followed by the API server's error, or, when the cluster doesn't serve the ome.io API, by `OME does not appear to be installed on this cluster` and a link to OME's README.

## `explain`

```text
kubectl ome traffic explain INFERENCESERVICE [flags]
```

`explain` compares the traffic behavior the InferenceService declares, in `spec.traffic`, its traffic annotations and its canary rollout groups, with the status the controller reports. It says whether the gateway supports the declared behavior, whether the status reports it realized, and where the two disagree. Like `status`, it reads the InferenceService once and nothing else, so a match means the controller's status agrees with the spec, not that requests follow it. The report never prints annotation values, header names, condition messages, credentials, UIDs or resourceVersions.

### Flags {#explain-flags}

| Flag | Default | Description |
| --- | --- | --- |
| `-o`, `--output` | `table` | Output format: table, wide, json or yaml |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

### Output fields {#explain-output-fields}

The table has the columns LAYER, STATE, VALUE and SOURCE, with a row for each layer of the comparison:

| Row | What it shows |
| --- | --- |
| SUMMARY | The verdict, from the states in the next table. |
| INTENT | Whether the service declares traffic behavior, with the algorithm it declares, or `Default`. |
| SUPPORT | Whether the gateway supports the declared behavior, going by the controller's status. |
| TRANSLATE | The translator, as TRANSLATOR in `status`. Its STATE is always `Computed`. |
| REALIZE | Whether the status reports the behavior realized, with its routes, endpoints and revision weights counted as `r=<routes> e=<endpoints> w=<weights>`. |
| CANARY | With more than one canary, one row for each, with its component in STATE and `step/total @ weight%` in VALUE. |
| CHECK | One row for each check, with its result in STATE and what it compares in VALUE: `algorithm`, `policy` or `canary-weight`. |
| ISSUE | One row for each issue code. |

SUMMARY is the first of these that applies:

| State | When |
| --- | --- |
| `Invalid` | INTENT, SUPPORT, REALIZE or a check is `Invalid`. |
| `NoIntent` | The service declares no traffic behavior and no canary. |
| `Unsupported` | The translator left out fields you declared, or OME has no translator for your gateway. |
| `Mismatch` | A check found a mismatch, or the gateway rejected the policy. |
| `Pending` | The gateway hasn't accepted or rejected the policy yet. |
| `Unavailable` | The status has no traffic section, although the service declares traffic behavior. |
| `Partial` | Part of the evidence is missing, isn't tied to the current generation, or was cut, or a check can't be verified. |
| `Consistent` | The status agrees with the declared behavior everywhere the CLI can check. |

INTENT is one of:

| State | When |
| --- | --- |
| `Declared` | The service sets an algorithm, `consistentHash` or `endpointOverride` in `spec.traffic`, has a [traffic annotation](../api/traffic-annotations.md), or has a canary rollout group. |
| `Invalid` | The declared values break the rules the webhook checks, apart from those that depend on your gateway, or name an algorithm the CLI doesn't recognize. |
| `Absent` | The service declares none of these. |

SUPPORT is the first of these that applies:

| State | When |
| --- | --- |
| `NotApplicable` | The service declares no traffic behavior for the gateway, including a service that declares only a canary. |
| `Invalid` | INTENT is `Invalid`, or the status has one of the issues `AlgorithmInvalid`, `ConditionInvalid`, `ConditionConflict`, `PolicyReferenceInvalid`, `PolicyKindUnsupported` and `StatusCombinationInvalid`. |
| `Unavailable` | The status has no traffic section. |
| `Partial` | BackendPolicyReady is missing or isn't tied to the current generation, or the gateway accepted the policy but UNSUPPORTED isn't a current `None`. |
| `Honored` | The gateway accepted the policy, and the current status shows no fields left out. |
| `Pending` | The gateway hasn't accepted or rejected the policy yet. |
| `Rejected` | BackendPolicyReady is `False`: the gateway rejected the policy, or OME has no translator for your gateway. |

REALIZE is `Invalid` when the status has one of the issues `RouteInvalid`, `EndpointInvalid`, `CanaryInvalid`, `AllocationInvalid`, `AllocationConflict` and `UnknownComponentStatus`. Otherwise it's `Unavailable` when the status lists no routes, endpoints, revision weights or canaries, `Partial` when the report cut a list, and `Reported` when it lists some.

The checks compare the declared behavior with the status, not with requests:

| Check | What it compares |
| --- | --- |
| `algorithm` | The declared algorithm, or `Default`, with the algorithm the controller reports: `Match` or `Mismatch`. `Unverifiable` when the reported algorithm isn't tied to the current generation. |
| `policy` | Whether the controller reports a policy for the declared behavior. `Match` when the gateway accepted the policy and the status names it, even with fields left out. `Mismatch` when the gateway rejected it or OME has no translator. `Unverifiable` while the gateway hasn't decided, when the status has no traffic section, or when BackendPolicyReady isn't tied to the current generation. |
| `canary-weight` | Whether the status holds one consistent canary record. `Match` when it reports exactly one canary that passed the checks behind `CanaryInvalid`: while the canary is progressing, paused or promoting, its recorded weight must equal the current step's `traffic`, or the previous step's while the controller advances. `Unverifiable` when there's no canary record yet, or more than one canary, since there's no single weight to compare. |

`algorithm` and `policy` are `NotApplicable` when the service declares no traffic behavior for the gateway, and `canary-weight` when it has no canary rollout group. A check is `Invalid` when INTENT is `Invalid` or the evidence it needs is: `canary-weight` is `Invalid`, never `Mismatch`, when the status has `CanaryInvalid` or an allocation issue in a canary's component.

An ISSUE row lists each of these that applies, in this order:

| Issue | When |
| --- | --- |
| `DeclaredSpecInvalid` | `spec.traffic` breaks the rules INTENT checks. |
| `DeclaredAnnotationsInvalid` | A traffic annotation breaks the rules INTENT checks. |
| `ReportedEvidenceInvalid` | The `status` report's STATE is `Invalid`. |
| `ReportedEvidenceStale` | A condition was written for an older generation. |
| `UnsupportedDeclaredFields` | SUPPORT is `Partial`, and a current `BackendPolicyUnsupportedFields` condition says the translator left out fields. |
| `NoTranslatorAvailable` | BackendPolicyReady is `False` with the reason `NoTranslatorAvailable`. |
| `PolicyRejected` | BackendPolicyReady is `False` with another reason. |
| `IntentNotReported` | The service declares traffic behavior, and the status has no traffic section. |
| `RealizationUnavailable` | The service declares traffic behavior or a canary, and REALIZE is `Unavailable`. |
| `AlgorithmMismatch` | The `algorithm` check is `Mismatch`. |

`-o wide` prints a row for each value behind the verdict, with a FIELD column after LAYER:

| LAYER | FIELD | What it shows |
| --- | --- | --- |
| REPORT, SUBJECT | `api-version`, `kind`, `namespace`, `name`, `collected-at` | The report's kind, the InferenceService it's about, and when the CLI read it. |
| SOURCE | `InferenceService` | The InferenceService the CLI read, with its generation. |
| SUMMARY | `state`, `intent`, `support`, `realization` | The states of the default table. `realization` counts the routes, endpoints and revision weights as `routes=N endpoints=N targets=N`. |
| INTENT | `algorithm`, `consistent-hash`, `endpoint-override` | Each part of `spec.traffic`: `Declared`, `Invalid` or `Absent`. For session affinity and endpoint override, VALUE shows the type and how many inputs it names, such as `Header inputs=1`, and never the names. |
| EXTENSION | `CircuitBreaker`, `Retry`, `Timeout`, `EnvoyPassthrough`, `IstioPassthrough` | One row for each kind of traffic annotation the service sets, with how many it sets. |
| REPORTED | `algorithm`, `translator`, `policy-ready`, `unsupported`, `policy`, `route` | The values `status` reports, with a row for each HTTPRoute. |
| OBSERVED | `endpoint`, `canary`, `stable-revision`, `canary-revision`, `target` | Each endpoint; each canary as `<component> step=N/T traffic=P%`, with its revision hashes; and each revision weight as `<component>/<role>/<revision>=P%`. With more than one canary, the revision fields start with the component, as `engine/stable-revision`. These values come from the status too, despite the layer's name. |
| REPORTED | `condition` | Each condition, as CONDITION in `status -o wide`. |
| REPORTED-ISSUE | The component, or `-` | Each issue of the `status` report. |
| CHECK | `algorithm`, `policy`, `canary-weight` | The checks of the default table. |
| ISSUE | `-` | Each issue of the `explain` report. |
| WARNING | `-` | Each warning of the report. |

`-o json` and `-o yaml` print a `TrafficExplainReport`. Its `content` holds the summary, the declared intent, the whole `status` report content under `reported`, the checks under `comparisons`, and the issues. Its `sources` lists the InferenceService without its UID. `warnings` repeats the warnings of the `status` report, and adds `SourceUnavailable` when the service declares traffic behavior and the status has no traffic section, and `PartialData` when SUMMARY is `Partial` or `Unsupported`.

### Examples {#explain-examples}

`chat` in `prod` declares `RoundRobin` behind Envoy Gateway. The gateway has accepted the policy that OME wrote, and the status lists one HTTPRoute:

```bash
kubectl ome traffic explain chat -n prod
```

```output
LAYER       STATE           VALUE           SOURCE
SUMMARY     Consistent      -               Computed/Current
INTENT      Declared        RoundRobin      Declared/Current
SUPPORT     Honored         -               Computed/Current
TRANSLATE   Computed        envoy-gateway   Computed/Current
REALIZE     Reported        r=1 e=0 w=0     Reported/Current
CHECK       Match           algorithm       Computed/Current
CHECK       Match           policy          Computed/Current
CHECK       NotApplicable   canary-weight   Computed/Current
```

On a cluster where the controller found no backend policy CRD when it started, OME writes no policy for the same service. With a status that lists no routes, endpoints or revision weights, `explain` shows that the gateway can't support the declared behavior and nothing is realized:

```bash
kubectl ome traffic explain chat -n prod
```

```output
LAYER       STATE           VALUE                    SOURCE
SUMMARY     Unsupported     -                        Computed/Unavailable
INTENT      Declared        RoundRobin               Declared/Current
SUPPORT     Rejected        -                        Computed/Current
TRANSLATE   Computed        Unavailable              Computed/Current
REALIZE     Unavailable     r=0 e=0 w=0              Unavailable/Unavailable
CHECK       Match           algorithm                Computed/Current
CHECK       Mismatch        policy                   Computed/Current
CHECK       NotApplicable   canary-weight            Computed/Current
ISSUE       -               NoTranslatorAvailable    Computed/Unverifiable
ISSUE       -               RealizationUnavailable   Computed/Unverifiable
```

Show every value behind the first verdict:

```bash
kubectl ome traffic explain chat -n prod -o wide
```

```output
LAYER      FIELD               STATE           VALUE                                                                     SOURCE
REPORT     api-version         -               cli.ome.io/v1alpha1                                                       -
REPORT     kind                -               TrafficExplainReport                                                      -
SUBJECT    namespace           -               prod                                                                      -
SUBJECT    name                -               chat                                                                      -
REPORT     collected-at        -               2026-09-14T17:00:00Z                                                      -
SOURCE     InferenceService    Reported        prod/chat gen=7 at=2026-09-14T17:00:00Z                                   Reported/Current
SUMMARY    state               Consistent      -                                                                         Computed/Current
SUMMARY    intent              Declared        -                                                                         Declared/Current
SUMMARY    support             Honored         -                                                                         Computed/Current
SUMMARY    realization         Reported        routes=1 endpoints=0 targets=0                                            Reported/Current
INTENT     algorithm           Declared        RoundRobin                                                                Declared/Current
INTENT     consistent-hash     Absent          -                                                                         Declared/Current
INTENT     endpoint-override   Absent          -                                                                         Declared/Current
REPORTED   algorithm           Reported        RoundRobin                                                                Reported/Current
REPORTED   translator          Computed        envoy-gateway                                                             Computed/Current
REPORTED   policy-ready        True            AcceptedByGateway                                                         Reported/Current
REPORTED   unsupported         None            -                                                                         Reported/Current
REPORTED   policy              Reported        gateway.envoyproxy.io/v1alpha1/BackendTrafficPolicy/prod/chat             Reported/Current
REPORTED   route               Reported        chat                                                                      Reported/Current
REPORTED   condition           Reported        BackendPolicyReady=True/AcceptedByGateway gen=7 at=2026-09-14T16:59:00Z   Reported/Current
CHECK      algorithm           Match           -                                                                         Computed/Current
CHECK      policy              Match           -                                                                         Computed/Current
CHECK      canary-weight       NotApplicable   -                                                                         Computed/Current
```

`explain` fails before it reads anything for the same `-o` values and names as `status`. When the read fails, it prints `get InferenceService "<namespace>/<name>": ` and one of `not found`, `forbidden`, `unauthorized`, `request cancelled`, `request timed out`, `request throttled`, `service unavailable`, `server error` or `API request failed`, and never the API server's own text.

## `drain` and `undrain`

!!! note "Alpha"
    These commands are alpha. Their flags and behavior can change between releases.

```text
kubectl ome traffic drain INFERENCESERVICE --workload-cluster CLUSTER --id ID --reason REASON [flags]
kubectl ome traffic undrain INFERENCESERVICE --id ID [flags]
```

`drain` asks multi-cluster routing to hold one workload cluster at weight 0 in the InferenceService's TrafficMap, for example during planned maintenance. It adds one entry to the `ome.io/traffic-drain` annotation on the control-plane InferenceService, keyed by `--id`, with the WorkloadCluster from `--workload-cluster` and your `--reason`. `undrain` removes one entry by its ID. For the drain in the [examples](#drain-and-undrain-examples), the annotation is:

```yaml
metadata:
  annotations:
    ome.io/traffic-drain: '{"maintenance-a":{"cluster":"worker-a","reason":"planned maintenance"}}'
```

Each ID is added and removed on its own, so several drains can hold the same cluster, and the cluster gets its traffic back only when the last of them is removed. Adding or removing one ID keeps every other ID and every other annotation, and removing the last ID removes the annotation. The annotation belongs to the control-plane InferenceService: placement strips it from the copies that it makes on workload clusters, and both commands refuse a copy.

The routing controller reads the annotation each time it builds the TrafficMap. Each entry whose cluster matches a drain gets weight 0 and the drain's ID in `drainRefs`, and the other entries keep their weights, so the clusters that are left share the traffic in the same ratio as before. When drains hold every cluster that had a positive weight, the TrafficMap's [`Routable`](../../concepts/rollouts-and-traffic/traffic-map.md#routable) condition is `False` with the reason `TrafficDrain`. A drain changes only the TrafficMap: it doesn't mark the cluster unhealthy, and it doesn't scale, move or stop the replicas there. It's separate from `allFailedPolicy: Drain` in routing health probes. [Why a cluster's weight is zero](../../concepts/rollouts-and-traffic/traffic-map.md#why-a-clusters-weight-is-zero) lists the other reasons an entry has weight 0.

The CLI doesn't check that multi-cluster routing is on, or that the cluster you name serves the InferenceService. Routing is off by default. It runs only on a control-plane cluster installed with the Helm values `ome.multicluster.enabled=true`, `ome.multicluster.role=control-plane` and `ome.multicluster.config.routing.enabled=true`, and skips a service that sets `spec.routing.enabled: false`. A drain whose cluster has no entry in the TrafficMap holds nothing, and when none of the service's drains matches an entry, the TrafficMap's [`OverrideActive`](../../concepts/rollouts-and-traffic/traffic-map.md#overrideactive) condition is `False` with the reason `OverridesPending`. List the WorkloadClusters with `kubectl get workloadclusters`. [`kubectl ome cluster`](cluster.md) shows their status, and [`kubectl ome placement`](placement.md) shows where the service is placed. `--workload-cluster` names the WorkloadCluster to drain. The global `--cluster` flag selects a cluster from your kubeconfig, and isn't an alias for it.

Nothing in OME removes a drain: it stays until you undrain it. To clear every drain at once, without the CLI's checks, remove the annotation with kubectl:

```bash
kubectl annotate inferenceservice chat -n prod ome.io/traffic-drain-
```

kubectl reports that it annotated the InferenceService, and the routing controller rebuilds the TrafficMap without drains. If a hand edit leaves the annotation malformed, both commands refuse, and the routing controller logs `parse ome.io/traffic-drain:` and keeps the last TrafficMap it built, rather than act as if there were no drains. Fix the value by hand, or remove the annotation.

[Drain a workload cluster](../../guides/multi-cluster/drain-a-workload-cluster.md) walks through a drain and an undrain.

### Flags {#drain-and-undrain-flags}

| Flag | Default | Description |
| --- | --- | --- |
| `--dry-run` | `none` | Dry-run mode: none, client or server |
| `--id` | None | DNS-1123 override ID (required) |
| `-o`, `--output` | `table` | Output: table, wide (bounded), json or yaml |
| `--reason` | None | `drain` only. Bounded non-secret operator reason (required) |
| `--workload-cluster` | None | `drain` only. WorkloadCluster DNS name to drain (required) |
| `--yes` | `false` | Confirm the exact preview without an interactive prompt |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

### How the actions run {#how-the-actions-run}

`drain` and `undrain` change the InferenceService in the same guarded way:

1. The CLI checks the flags and the name, reads the InferenceService, and refuses if the change isn't safe. A refusal changes nothing and exits `1`.
2. It prints a preview of the exact change to stderr: the context, the namespace, the target with its UID and resourceVersion, the drain's ID, cluster and reason, the number of drains before and after, and the follow-up command. It prints the preview even with `--yes`.
3. It asks `Confirm this exact action? [y/N]` on stderr, and goes ahead only on `y` or `yes`, in any case. `--yes` confirms without the prompt, and without a terminal you must pass it.
4. It sends a JSON Patch that tests the UID and resourceVersion from the preview, then sets the `ome.io/traffic-drain` annotation to the new drains, or removes it when the last drain goes. If the InferenceService changed after the CLI read it, the API server rejects the patch and the CLI exits `3`.
5. It prints the result, an ActionResult, to stdout.

`--dry-run client` runs every check, prints the preview and asks for confirmation, then stops without sending the patch. `--dry-run server` sends the same patch with `dryRun=All`, so the API server runs its checks and stores nothing.

The command has 45 seconds in all, including the time the prompt waits for your answer, and each API request has at most 10 seconds, or less if you set a shorter `--request-timeout`. When the time runs out, the command fails with `context deadline exceeded`. A credential plugin or custom transport in your kubeconfig may not stop when the time runs out. Without `--yes`, the command fails with `action not confirmed; noninteractive input requires --yes` when you answer anything but `y` or `yes`, type more than 16 characters, press Ctrl-D, or run it without a terminal. Ctrl-C at the prompt fails with `context canceled`. A declined or interrupted prompt sends no patch.

The ActionResult table shows:

| Field | What it shows |
| --- | --- |
| `action` | `traffic drain` or `traffic undrain`. |
| `target` | The InferenceService, as `InferenceService/<namespace>/<name>`. |
| `uid`, `resource-version` | With `-o wide` only, the UID and resourceVersion the patch tests. |
| `dry-run` | `none`, `client` or `server`. |
| `accepted` | Whether the API server accepted the patch: `Yes` or `No`. Always `No` for a client dry run. |
| `applied` | Whether the change was stored. Always `No` for a dry run. |
| `override-id` | The drain's ID. |
| `cluster` | The WorkloadCluster the drain holds. For `undrain`, the cluster of the drain it removes. |
| `overrides` | The number of drains on the service, as `before -> after`. |
| `message` | `Validated locally; no patch sent. TrafficMap convergence was not observed.` for a client dry run, `API dry-run accepted; no changes persisted.` for a server dry run, and otherwise `API accepted traffic annotation request; not TrafficMap convergence.` |
| `follow-up` | `kubectl ome traffic status <name> -n <namespace> --context=<context>`. |
| `hint` | `Use -o json or -o yaml for full values.` |

The table cuts each value to 56 characters: a longer value keeps its first 53 characters and ends in `...`. So the client dry-run message prints as `Validated locally; no patch sent. TrafficMap converge...`, the accepted message as `API accepted traffic annotation request; not TrafficM...`, and the follow-up command prints cut once the name, namespace and context together pass 14 characters. `-o json` and `-o yaml` print every value in full, with the drain's ID, cluster and counts under `traffic`. The reason appears only in the preview and in the annotation.

`applied` means that the API server stored the annotation, and nothing more. A drain goes through three stages, and the command sees only the first:

1. The API server stores the annotation.
2. The routing controller rebuilds the TrafficMap with the drain applied.
3. The TrafficMap's publisher applies the new weights, and reports whether it has in the TrafficMap's [`Published`](../../concepts/rollouts-and-traffic/traffic-map.md#published) condition. [Publish a global endpoint](../../guides/multi-cluster/publish-a-global-endpoint.md) describes what it writes.

To check the second stage, read the TrafficMap: the drained cluster's entry has weight 0 and the drain's ID in `drainRefs`, and `OverrideActive` is `True` with the reason `OverridesApplied`. [Drain a workload cluster](../../guides/multi-cluster/drain-a-workload-cluster.md) shows the commands. The follow-up command, `traffic status`, shows neither drains nor the TrafficMap, and [`kubectl ome wait`](wait.md) has no condition for a drain. To list the drains on a service, read the annotation:

```bash
kubectl get inferenceservice chat -n prod -o jsonpath='{.metadata.annotations.ome\.io/traffic-drain}{"\n"}'
```

```output
{"maintenance-a":{"cluster":"worker-a","reason":"planned maintenance"}}
```

If the patch fails with anything other than a refusal or a conflict, read the annotation before you run the command again, since the API server may have stored the patch. The error messages say to check traffic status, but `traffic status` doesn't show drains. The CLI never retries on its own. The error calls the outcome unknown when the API server's response is too large or doesn't match the request, as in `API response is not bound to the request; outcome unknown, check traffic status`. A server error, a timeout or a network error prints `required Kubernetes API request failed; check access and connectivity` or `context deadline exceeded`, even though the patch may have gone through. When the patch succeeds but the result can't be written, the command fails with `write traffic action result failed; check traffic status`. Each of these errors exits `1`. Running a command again is safe: a drain that's already stored refuses with `override ID already exists`, and an undrain that already went through refuses with `override ID does not exist`.

[Guarded actions](guarded-actions.md) describes the contract every mutating kubectl-ome command follows.

### Refusals {#drain-and-undrain-refusals}

Before it sends any request, each command checks its arguments in this order, and refuses on the first that fails:

| Message | Cause | Command |
| --- | --- | --- |
| `invalid traffic action flags; use --help` | A flag is unknown, has no value or has a value of the wrong type, such as `--yes=maybe`. `undrain` has no `--workload-cluster` or `--reason` flag. | Both |
| `exactly one inference service is required` | No InferenceService name, or more than one. | Both |
| `output must be table, wide, json or yaml` | `-o` has another value. | Both |
| `dry-run must be none, client or server` | `--dry-run` has another value. | Both |
| `invalid inference service name` | The name isn't a DNS-1123 subdomain, or looks like a credential. | Both |
| `traffic drain requires --workload-cluster; --cluster selects the Kubernetes API cluster` | `--workload-cluster` is missing. | `drain` |
| `traffic override ID must be a DNS-1123 label` | `--id` is missing, isn't a DNS-1123 label, or looks like a credential. | Both |
| `traffic drain cluster must be a DNS-1123 subdomain` | `--workload-cluster` isn't a DNS-1123 subdomain, or looks like a credential. | `drain` |
| `traffic drain reason must be bounded, trimmed, printable, and non-secret` | `--reason` is missing or longer than 256 bytes, isn't valid UTF-8, has leading or trailing spaces, line breaks, control characters or invisible formatting characters, or looks like a credential, such as an `sk-` API key or a JSON Web Token. | `drain` |
| `resolve workload namespace failed` | The CLI couldn't work out the namespace from your kubeconfig. | Both |
| `resolved workload namespace is invalid` | The namespace isn't a DNS-1123 label, or looks like a credential. | Both |
| `selected kubeconfig context is unavailable or unsafe` | Neither `--context` nor the kubeconfig's current context names a context, or the name is unsafe to show. | Both |

The command then reads the InferenceService. When the read fails, it prints `required Kubernetes API request failed; check access and connectivity`, whether the InferenceService doesn't exist in the namespace, you lack `get` permission or the API server can't be reached, or `context deadline exceeded` when the read runs out of time. Check with `kubectl get inferenceservice chat -n prod`.

After the read, each command refuses in these cases, listed in the order the CLI first checks them:

| Message | Cause | Command |
| --- | --- | --- |
| `action target response does not match exact request` | The API server returned a different InferenceService from the one the CLI asked for. | Both |
| `traffic action refused: traffic-drain state exceeds safety bounds` | The InferenceService is too large to check: the object is larger than 1 MiB or nested too deeply, or it has more than 256 annotations or labels, more than 64 finalizers or status conditions, more than 3 components in its status, or more than 64 KiB of annotations, labels and finalizers together. Or the drains are: the annotation's value is larger than 32 KiB or holds more than 64 drains, or the change would take it past either limit, add a 257th annotation, or take the metadata past 64 KiB. Undrain the drains you no longer need. | Both |
| `action refused: placement sources and derived services cannot be mutated` | The InferenceService is a copy that placement made on a workload cluster: it has the label or annotation `ome.io/placement-origin`, `ome.io/placement-origin-uid` or `ome.io/placement-control-plane`. Switch to the control-plane cluster's context. The commands also refuse a service with the label `ome.io/accelerator-requirements` or `ome.io/cluster-selector`. | Both |
| `traffic action refused: target is not eligible for cross-cluster traffic routing` | The service's `spec.placement` sets neither `requirements` nor `clusterSelector`. A service with no `spec.placement` is eligible when it has the annotation `ome.io/accelerator-requirements` or `ome.io/cluster-selector`. | Both |
| `action refused: target identity is missing or unsafe` | The InferenceService's name, namespace, UID, resourceVersion or generation is missing, invalid or unsafe to show. | Both |
| `action refused: target is being deleted` | The InferenceService is being deleted. | Both |
| `action refused: safety inputs exceed inspection bounds` | The rollout state is larger than the CLI checks, such as more than 3 rollout groups in the active run, a canary with more than 20 steps or 10 analysis metrics, or more than 8 revision weights in a component's status. | Both |
| `traffic action refused: existing traffic-drain annotation is malformed or unsafe` | The annotation isn't valid, has unknown fields or duplicate IDs, or one of its drains breaks the rules for `--id`, `--workload-cluster` or `--reason`, usually after a hand edit. | Both |
| `traffic drain refused: override ID already exists` | The service already has a drain with this ID, even if its cluster and reason are the same. Choose another ID, or undrain it first to change its cluster or reason. | `drain` |
| `traffic undrain refused: override ID does not exist` | The service has no drain with this ID. | `undrain` |

### Examples {#drain-and-undrain-examples}

The examples run against the control-plane cluster, whose kubeconfig context `hub` is the current context. Try a drain of `worker-a` for `chat` without storing it. The command prints its preview to stderr, as in the next example with `Dry-run` set to `server`, and then the result:

```bash
kubectl ome traffic drain chat -n prod \
  --workload-cluster worker-a \
  --id maintenance-a \
  --reason "planned maintenance" \
  --dry-run=server --yes
```

```output
FIELD         VALUE
action        traffic drain
target        InferenceService/prod/chat
dry-run       server
accepted      Yes
applied       No
override-id   maintenance-a
cluster       worker-a
overrides     0 -> 1
message       API dry-run accepted; no changes persisted.
follow-up     kubectl ome traffic status chat -n prod --context=hub
hint          Use -o json or -o yaml for full values.
```

Drain `worker-a`, confirming at the prompt:

```bash
kubectl ome traffic drain chat -n prod --workload-cluster worker-a --id maintenance-a --reason "planned maintenance"
```

The command prints the preview and the prompt to stderr. Answer `y`:

```output
ALPHA guarded traffic action (not TrafficMap convergence)
FIELD             VALUE
Action            traffic drain
Context           hub
Workload NS       prod
Target            InferenceService/chat
UID               2f6c1d9e-4b7a-4c1e-9f3d-8a5b6c7d0e12
ResourceVersion   1284711
Dry-run           none
Override ID       maintenance-a
Cluster           worker-a
Reason            "planned maintenance"
Override count    0 -> 1
Annotation        ome.io/traffic-drain
Follow-up         kubectl ome traffic status chat -n prod --context=hub
API acceptance is not TrafficMap convergence or data-plane realization.
The exact InferenceService UID/resourceVersion is tested atomically.
Other annotations and traffic-drain override IDs are preserved.
Run the previewed traffic status command to observe controller evidence.
Confirm this exact action? [y/N] y
```

Then it prints the result:

```output
FIELD         VALUE
action        traffic drain
target        InferenceService/prod/chat
dry-run       none
accepted      Yes
applied       Yes
override-id   maintenance-a
cluster       worker-a
overrides     0 -> 1
message       API accepted traffic annotation request; not TrafficM...
follow-up     kubectl ome traffic status chat -n prod --context=hub
hint          Use -o json or -o yaml for full values.
```

Remove the drain without a prompt, as a script would, and print the result as JSON. After the preview, which shows `Override count` as `1 -> 0`, the command prints:

```bash
kubectl ome traffic undrain chat -n prod --id maintenance-a --yes -o json
```

```output
{
  "apiVersion": "cli.ome.io/v1alpha1",
  "kind": "ActionResult",
  "collectedAt": "2026-09-28T09:30:12.483920113Z",
  "action": "traffic undrain",
  "target": {
    "kind": "InferenceService",
    "namespace": "prod",
    "name": "chat",
    "uid": "2f6c1d9e-4b7a-4c1e-9f3d-8a5b6c7d0e12",
    "resourceVersion": "1284802"
  },
  "dryRun": "none",
  "accepted": true,
  "applied": true,
  "message": "API accepted traffic annotation request; not TrafficMap convergence.",
  "followUp": "kubectl ome traffic status chat -n prod --context=hub",
  "traffic": {
    "overrideID": "maintenance-a",
    "cluster": "worker-a",
    "overridesBefore": 1,
    "overridesAfter": 0
  }
}
```

## Exit codes

| Code | Meaning | Returned by |
| --- | --- | --- |
| `0` | Success | Every subcommand that finishes. |
| `1` | General error | Every subcommand, for an invalid name or flag, a failed API request, a refusal, an action you didn't confirm, Ctrl-C, or an action whose outcome is unknown. |
| `3` | Mutation conflict | `drain` and `undrain`, when the API server rejects the guarded patch because the InferenceService changed after the CLI read it. The error is `guarded annotation patch rejected; refresh traffic status and retry explicitly`. |

Errors print to stderr as `error: <message>`.

## Related guides

- [Drain a workload cluster](../../guides/multi-cluster/drain-a-workload-cluster.md)
- [Troubleshoot an InferenceService](../../guides/troubleshoot/troubleshoot-an-inferenceservice.md)
