---
title: kubectl ome accelerator
description: Show the accelerator class or policy that the engine and decoder of an InferenceService ask for, and the resource requests their pods start from.
since: v1.3
---

`kubectl ome accelerator` shows the [AcceleratorClass](../../concepts/runtimes/accelerator-classes.md) or selection policy that the engine and decoder of an [InferenceService](../../concepts/serving/inference-services.md) ask for, and the resource requests their pods start from. It can't show the class that OME picked, because OME doesn't record it in the status. This is a known bug. To see that class, check the node selector of the component's pods, as in [Step 4 of Select accelerators](../../guides/deploy-models/select-accelerators.md#step-4-check-the-selected-class). [Select accelerators](../../guides/deploy-models/select-accelerators.md) shows how to name a class or choose a policy.

```text
kubectl ome accelerator SUBCOMMAND INFERENCESERVICE [flags]
```

| Subcommand | What it does |
| --- | --- |
| [`explain`](#explain) | Shows each component's class or policy, and its base requests. |

## `explain`

```text
kubectl ome accelerator explain INFERENCESERVICE [flags]
```

`explain` prints a row for the engine when `spec.engine` is set, and one for the decoder when `spec.decoder` is set. The router gets no row, because OME never picks a class for it.

In `-o wide`, STATUS_FRESHNESS reflects the observed generation of a component's workload, rather than whether the status is current. This is a known bug.

For a component with a class or policy, SEL and its issue depend on how the InferenceService runs:

| When | SEL | Issue |
| --- | --- | --- |
| The InferenceService runs as a Deployment whose generation matches the InferenceService's. | `Missing` | `SelectionNotReported` |
| The Deployment's generation is behind, for example after a change to the InferenceService that left the Deployment as it was. | `Unavail` | `StatusStale` |
| The Deployment's generation is ahead, for example after an HPA or KEDA scaled it. | `Invalid` | `StatusInvalid` |
| Every component has run on [OMENative](../../concepts/omenative/overview.md) since the InferenceService was created. | `Unavail` | `StatusUnobserved` |
| The CLI couldn't resolve the runtime in force, or the InferenceService uses VirtualDeployment. | `Unavail` | `ActiveConfigurationUnavailable` |

A component with no class or policy shows `None`. The report shows a declared class or policy even when the runtime lists no classes in `acceleratorRequirements.acceleratorClasses`, and OME then applies no class. [Step 2 of Select accelerators](../../guides/deploy-models/select-accelerators.md#step-2-check-the-runtimes-candidates) shows how to check the list.

### Flags {#explain-flags}

| Flag | Default | Description |
| --- | --- | --- |
| `--ome-namespace` | `ome` | Namespace where the OME control plane is installed |
| `-o`, `--output` | `table` | Output format: table, wide, json or yaml |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

[Required RBAC](overview.md#required-rbac) lists the permissions `explain` needs.

### Output fields {#explain-output-fields}

`explain` takes the name of one InferenceService and prints a table. `-o wide` prints each field on its own row, with its evidence. `-o json` and `-o yaml` print the whole report, with `kind: AcceleratorExplainReport`.

The table has these columns:

| Column | What it shows |
| --- | --- |
| COMP | The component: `engine` or `decoder`. |
| POLICY | `Explicit` when the component names a valid class. Otherwise its policy: `BestFit`, `Cheapest`, `MostCapable`, `FirstAvail` for `FirstAvailable`, or `-`. |
| CLASS | The declared class when its name is valid, else `-`. Names over 16 characters are cut in the middle with `...`. |
| SEL | Whether the status reports a selection: `Missing`, `Unavail`, `Invalid` or `None`. The table under [`explain`](#explain) says when each appears. |
| REQUESTS | The base requests, such as `base:cpu=10,+2`. |
| ISS | The component's issues, plus any issues that belong to no row. `-o wide` lists them. |

REQUESTS shows the first base request by name and the number of others, cut to 18 characters. The base requests are those of the component's serving container, or of its leader in a multi-node component. The CLI works them out from the InferenceService and the runtime in force, or its pinned revision, which it reads from the OME namespace that `--ome-namespace` sets. When the InferenceService names no runtime, the CLI picks one as OME would. The base requests leave out the class: a class with `resources` changes what the pods request, unless the runner sets its own. See [How InferenceServices use a class](../../concepts/runtimes/accelerator-classes.md#how-services-choose-a-class). REQUESTS shows `base:Unknown` when the CLI couldn't work out the base requests, `base:Invalid` when they're malformed, and a bare `None`, `Unknown` or `Invalid` when the serving container requests nothing.

`-o wide` has the columns SCOPE, COMP, FIELD, VALUE and EVIDENCE. EVIDENCE says how the CLI got each value:

| Evidence | Meaning |
| --- | --- |
| `Declared/Service` | Set in `spec.acceleratorSelector`. |
| `Declared/Component` | Set in the component's `acceleratorOverride`. |
| `Declared` | Read from the InferenceService's spec. |
| `Observed` | Read from the API server by this command, as in the SOURCE rows. |
| `Computed` | Worked out by the CLI, as the base requests are. |
| `Unavailable` | The CLI couldn't get the value. |

As in OME, a class in an `acceleratorOverride` wins over one in `spec.acceleratorSelector`, and a class wins over a policy.

The first two rows describe the whole report:

| Row | What it shows |
| --- | --- |
| `SUMMARY STATE` | The state of the report, in the next table. |
| `SUMMARY STATUS_FRESHNESS` | How `status.observedGeneration` compares with `metadata.generation`: `Current` if equal, `Stale` if lower, `Unobserved` if 0, `Invalid` if higher. |

The report can be in these states:

| State | Meaning |
| --- | --- |
| `Partial` | There are issues, but nothing is malformed. This is the usual state for an InferenceService with a class or policy. |
| `NotConfigured` | No component has a class or policy, and there are no issues. |
| `Invalid` | A value is malformed, or STATUS_FRESHNESS is `Invalid` for a component with a class or policy. The issues say which. |

Each component then has these rows. A row with no value is left out.

| Row | What it shows |
| --- | --- |
| `INTENT MODE` | `Class` when the component names a class, `Policy` when it sets only a policy, and `NotConfigured` when it has neither. `Invalid` when the class name isn't a valid object name. The `ome.io/accelerator-class` annotation doesn't count, because it doesn't pick a class. |
| `INTENT POLICY` | The declared policy. |
| `INTENT CLASS` | The declared class. |
| `SELECTION STATE` | As in SEL: `NotReported`, `NotConfigured`, `Unavailable` or `Invalid`, for `Missing`, `None`, `Unavail` and `Invalid`. |
| `SELECTION REASON_STATE` | Whether the status gives a reason for the selection: `NotReported`, `Unavailable` or `Invalid`. |
| `CLASS STATE` | `NotRequested`, with the evidence `-`: the status reports no class for the CLI to read. |
| `REQUEST BASE_STATE` | `Available`, `Unavailable` or `Invalid`. |
| `REQUEST BASE` | Every base request, comma-separated, or `None`. It appears only when BASE_STATE is `Available`. |
| `REQUEST EFFECTIVE_STATE` | Whether the status reports requests: `NotConfigured` for a component with no class or policy, `Invalid` when STATUS_FRESHNESS is `Invalid`, and `Unavailable` otherwise. |

Each `ISSUE` row has an issue code in FIELD:

| Code | Meaning |
| --- | --- |
| `ActiveConfigurationUnavailable` | The CLI couldn't resolve the runtime configuration in force, for example because it can't read the runtime or the pinned revision, or because the InferenceService uses VirtualDeployment. |
| `ActiveRevisionInconsistent` | The metadata of the pinned runtime revision doesn't match its runtime or its content, so the CLI works out no base requests from it. |
| `BaseRequestsUnavailable` | The CLI couldn't work out the component's base requests. |
| `DeclaredClassInvalid` | The class that the spec names isn't a valid object name. |
| `RequestsInvalid` | The base requests are malformed, such as a value that isn't a non-negative quantity. |
| `SelectionNotReported` | The component has a class or policy, STATUS_FRESHNESS is `Current`, and the status reports no selection. |
| `StatusInvalid` | STATUS_FRESHNESS is `Invalid`, as in the table under [`explain`](#explain). |
| `StatusStale` | STATUS_FRESHNESS is `Stale`, as in the table under [`explain`](#explain). |
| `StatusUnobserved` | STATUS_FRESHNESS is `Unobserved`, as in the table under [`explain`](#explain). |

For a declared class that doesn't exist, look for an `AcceleratorClassError` event on the InferenceService, as in [The InferenceService has an AcceleratorClassError event](../../guides/deploy-models/select-accelerators.md#the-inferenceservice-has-an-acceleratorclasserror-event).

The warnings sum up the issues:

- `PartialData`: the report is `Partial` or `Invalid`.
- `StaleEvidence`: an issue is `StatusStale`.
- `SourceUnavailable`: an issue is `ActiveConfigurationUnavailable` or `BaseRequestsUnavailable`.

The SOURCE rows, and `sources` in JSON and YAML, list what the report rests on, with the NAME, GENERATION and COLLECTED_AT time of each. They're the InferenceService and the runtime in force: a ServingRuntime, a ClusterServingRuntime or, for a pinned revision, a ControllerRevision, whose GENERATION is `-`.

In `-o json` and `-o yaml`, optional fields with no value are left out, and lists are always present, even when empty.

??? note "When something else reports a selection"
    If a tool other than OME writes `status.components.<component>.selectedAccelerator`, the report adds what it finds there, with the evidence `Reported`. SEL can then show `Reported`, and so can the report's state when there are no issues. CLASS shows the reported class, and REQUESTS shows the reported requests with no `base:` prefix. `-o wide` adds the SELECTION CLASS, SELECTION REASON_DIGEST, CLASS NAME and REQUEST EFFECTIVE rows. The digest stands in for the reason, which could hold credentials.

    The CLI also reads the reported AcceleratorClass and lists it under SOURCE. CLASS STATE is then `Observed`, or says why the CLI couldn't use the class: `NotFound`, `Forbidden`, `UnsupportedAPI`, `Unreadable` or `Invalid`. These issues can appear too:

    | Code | Meaning |
    | --- | --- |
    | `ClassNotFound`, `ClassForbidden`, `ClassUnreadable`, `ClassUnsupportedAPI` | The CLI couldn't read the reported class. These add the warning `SourceUnavailable`. |
    | `ClassInvalid` | The reported class, or its name, is malformed. |
    | `ReportedClassMismatch` | The reported class differs from the one the spec names. |
    | `NodeSelectorInvalid`, `RequestsInvalid` | The reported `nodeSelector` or `resourceRequests` is malformed. |
    | `RequestsNotReported` | The selection has no `resourceRequests`. |
    | `SelectionUnexpected` | A component with no class or policy has a selection. |
    | `UnexpectedComponentEvidence` | A component other than the engine and decoder, such as the router, has a selection. |

### Examples {#explain-examples}

The examples use the InferenceService from [Select accelerators](../../guides/deploy-models/select-accelerators.md), which runs as a Deployment, names the runtime `srt-llama-3-3-70b-instruct` and pins the class `nvidia-h100`. Explain its accelerator selection:

```bash
kubectl ome accelerator explain llama-3-3-70b-instruct -n llama-demo
```

```output
COMP     POLICY     CLASS         SEL       REQUESTS         ISS
engine   Explicit   nvidia-h100   Missing   base:cpu=10,+2   1
```

The engine names `nvidia-h100`, so POLICY is `Explicit` and CLASS shows the declared class. SEL is `Missing`, and ISS counts the `SelectionNotReported` issue. REQUESTS shows `cpu=10` and two more base requests.

For the InferenceService in the guide's "Choose a policy" tab, which sets `policy: BestFit` and `minMemory: 80` in place of the class, POLICY shows the policy, and CLASS shows `-`:

```output
COMP     POLICY    CLASS   SEL       REQUESTS         ISS
engine   BestFit   -       Missing   base:cpu=10,+2   1
```

The report doesn't show the constraints.

Print every field of the InferenceService that pins `nvidia-h100`:

```bash
kubectl ome accelerator explain llama-3-3-70b-instruct -n llama-demo -o wide
```

```output
SCOPE       COMP                    FIELD                  VALUE                                  EVIDENCE
SUMMARY     -                       STATE                  Partial                                Computed
SUMMARY     -                       STATUS_FRESHNESS       Current                                Computed
INTENT      engine                  MODE                   Class                                  Declared
INTENT      engine                  CLASS                  nvidia-h100                            Declared/Service
SELECTION   engine                  STATE                  NotReported                            Unavailable
SELECTION   engine                  REASON_STATE           NotReported                            Unavailable
CLASS       engine                  STATE                  NotRequested                           -
REQUEST     engine                  BASE_STATE             Available                              Computed
REQUEST     engine                  BASE                   cpu=10,memory=160Gi,nvidia.com/gpu=4   Computed
REQUEST     engine                  EFFECTIVE_STATE        Unavailable                            Unavailable
ISSUE       engine                  SelectionNotReported   -                                      Computed
SOURCE      ClusterServingRuntime   NAME                   srt-llama-3-3-70b-instruct             Observed
SOURCE      ClusterServingRuntime   GENERATION             2                                      Observed
SOURCE      ClusterServingRuntime   COLLECTED_AT           2026-09-14T20:30:00Z                   Observed
SOURCE      InferenceService        NAME                   llama-demo/llama-3-3-70b-instruct      Observed
SOURCE      InferenceService        GENERATION             1                                      Observed
SOURCE      InferenceService        COLLECTED_AT           2026-09-14T20:30:00Z                   Observed
WARNING     -                       CODE                   PartialData                            Computed
```

The class comes from `spec.acceleratorSelector`, so its evidence is `Declared/Service`. The base requests are the runtime's 10 CPUs, 160 GiB of memory and 4 GPUs. `nvidia-h100` sets no `resources`, so the pods request the same.

After an edit that leaves the Deployment as it was, SEL can show `Unavail`, with the issue `StatusStale`:

```output
COMP     POLICY     CLASS         SEL       REQUESTS         ISS
engine   Explicit   nvidia-h100   Unavail   base:cpu=10,+2   1
```

`-o wide` shows STATUS_FRESHNESS `Stale` and the warnings `PartialData` and `StaleEvidence`. An InferenceService whose components have all run on OMENative since it was created shows the same row, with the issue `StatusUnobserved`.

Without `spec.acceleratorSelector`, the engine has no class or policy, and the report is `NotConfigured`, with no issues or warnings:

```output
COMP     POLICY   CLASS   SEL    REQUESTS         ISS
engine   -        -       None   base:cpu=10,+2   0
```

To use the report in a script, print its issues from the JSON:

```bash
kubectl ome accelerator explain llama-3-3-70b-instruct -n llama-demo -o json \
  | jq -r '.content.issues[] | "\(.component) \(.code)"'
```

```output
engine SelectionNotReported
```

## Exit codes

| Code | Meaning | Returned by |
| --- | --- | --- |
| `0` | Success | `explain`, whenever it prints a report, even a `Partial` or `Invalid` one. |
| `1` | General error | `explain`, when it can't build a report, such as for a bad name, flag or namespace, a failed read of the InferenceService, reads that take over 10 seconds in all, or Ctrl-C. |

Errors print to stderr as `error: <message>`. For a missing InferenceService, the message is `read InferenceService "llama-demo/llama-3-3-70b-instruct": not found`.

## Related guides

- [Select accelerators](../../guides/deploy-models/select-accelerators.md)
- [Troubleshoot an InferenceService](../../guides/troubleshoot/troubleshoot-an-inferenceservice.md)
