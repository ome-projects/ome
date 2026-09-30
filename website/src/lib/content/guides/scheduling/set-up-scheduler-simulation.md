---
title: Set up scheduler simulation for Alfred
description: Load your schedulers' configurations into Alfred, so that it predicts where the replacement pods of each move would land.
since: v1.3
---

Scheduler simulation lets [Alfred](../../concepts/scheduling/alfred.md) predict where the replacement pods of each move would land, by running your scheduler's configuration against a copy of the cluster. In recommend-only mode, the prediction shows in Alfred's recommendations. To [let Alfred migrate Instances](let-alfred-migrate-instances.md), you need it: Alfred sends a request only when the simulation places every replacement pod. Alfred is alpha.

Alfred runs each simulation in a worker: the `/alfred-simulator` binary in Alfred's image, loaded with the configuration of one scheduler. You list the workers in an immutable ConfigMap, and give Alfred the identities that the workers report. Alfred checks the workers when it starts, and exits when one fails to load or reports another identity.

<div class="prerequisites" markdown>

- Alfred installed with the `ome-alfred` chart in the `ome` namespace: see [Run Alfred in recommend-only mode](run-alfred.md). Each step adds to the `values.yaml` you installed it with. Keep every value in that file: `helm upgrade -f` resets any value that the file leaves out to the chart's default.
- The PodGroup CRD, `scheduling.x-k8s.io/v1alpha1`, which the simulation reads even when no pod uses a PodGroup. No OME chart installs it: see [Gang scheduling](../../concepts/serving/gang-scheduling.md#when-the-podgroup-crd-is-missing).
- For an Instance of more than one pod, the [OME scheduler](../../concepts/scheduling/ome-scheduler.md), which is alpha and runs only on Kubernetes 1.35. Alfred can simulate such an Instance only with the OME scheduler's configuration, so its pods must use the OME scheduler: see [Opt in OMENative components](../operate-ome/ome-scheduler.md#opt-in-omenative-components).
- Helm, kubectl and jq.
- Permission to upgrade the `ome-alfred` release, and to manage Pods and ConfigMaps in `ome`.

</div>

## Step 1: Pin Alfred's image

A worker's identity includes a hash of Alfred's build, so the profiles you print in Step 3 must come from the image that Alfred runs. Find the digest that your Alfred pods run:

```bash
kubectl get pods -n ome -l control-plane=ome-alfred \
  -o jsonpath='{range .items[*]}{.status.containerStatuses[0].imageID}{"\n"}{end}'
```

```output
registry.example.com/ome/alfred@sha256:<digest>
registry.example.com/ome/alfred@sha256:<digest>
registry.example.com/ome/alfred@sha256:<digest>
```

Each line is one pod's image. When the pods show different digests, the tag moved between their pulls: pick the build you want. Add its digest to the tag in your `values.yaml`, so that all Alfred pods run that build:

```yaml title="values.yaml"
global:
  hub: registry.example.com/ome
image:
  tag: v1.3.0@sha256:<digest>
```

The chart then runs `registry.example.com/ome/alfred:v1.3.0@sha256:<digest>`. You apply this change in [Step 5](#point-alfred-at-the-workers).

## Step 2: Write the scheduler configurations

Each worker simulates one scheduler, from a KubeSchedulerConfiguration file. Write one file for each scheduler that your OMENative pods name in `schedulerName`. For pods that name `default-scheduler`, or no scheduler:

```yaml title="default-scheduler.yaml" check=skip
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
profiles:
  - schedulerName: default-scheduler
```

For the OME scheduler, copy the configuration that it runs from its ConfigMap:

```bash
kubectl get configmap ome-scheduler-config -n ome -o jsonpath='{.data.config\.yaml}' > ome-scheduler.yaml
```

The command prints nothing. `ome-scheduler.yaml` now holds the OME scheduler's one profile, `ome-scheduler`, which runs the OMEGangPack plugin. Without the OME scheduler, leave out everything below that names `ome-scheduler`: its `--from-file` flags, its container in the Pod, its worker in `workers.json` and its profile in `values.yaml`.

The file above and the OME scheduler's configuration, as its chart renders it, meet the [rules that the simulator checks](../../reference/scheduling/alfred-configuration.md#simulation), which any other file must meet too. The simulator runs these configurations on the scheduler of Kubernetes v1.35.4, whatever version your cluster runs, so a prediction can differ from what your scheduler does.

## Step 3: Print each profile {#print-each-profile}

With `--print-profile`, the simulator prints a worker's profile, which holds the identity that Alfred checks. Put both files in a ConfigMap:

```bash
kubectl create configmap alfred-print-profile -n ome \
  --from-file=default-scheduler.yaml --from-file=ome-scheduler.yaml
```

```output
configmap/alfred-print-profile created
```

Then run each file through the simulator from the image you pinned. This Pod runs one container per file, with the same mount path and security settings as Alfred. `--backend` is a name that you choose for the worker, and becomes part of its identity:

```yaml title="alfred-print-profile.yaml"
apiVersion: v1
kind: Pod
metadata:
  name: alfred-print-profile
  namespace: ome
spec:
  restartPolicy: Never
  automountServiceAccountToken: false
  enableServiceLinks: false
  securityContext:
    runAsNonRoot: true
    seccompProfile:
      type: RuntimeDefault
  containers:
    - name: default-scheduler
      image: registry.example.com/ome/alfred:v1.3.0@sha256:<digest>
      command: ["/alfred-simulator"]
      args:
        - --backend=kube-v135
        - --scheduler-config=/etc/alfred-simulation/default-scheduler.yaml
        - --print-profile
      securityContext:
        allowPrivilegeEscalation: false
        readOnlyRootFilesystem: true
        capabilities:
          drop: ["ALL"]
      volumeMounts:
        - name: config
          mountPath: /etc/alfred-simulation
          readOnly: true
    - name: ome-scheduler
      image: registry.example.com/ome/alfred:v1.3.0@sha256:<digest>
      command: ["/alfred-simulator"]
      args:
        - --backend=ome-v135
        - --scheduler-config=/etc/alfred-simulation/ome-scheduler.yaml
        - --print-profile
      securityContext:
        allowPrivilegeEscalation: false
        readOnlyRootFilesystem: true
        capabilities:
          drop: ["ALL"]
      volumeMounts:
        - name: config
          mountPath: /etc/alfred-simulation
          readOnly: true
  volumes:
    - name: config
      configMap:
        name: alfred-print-profile
```

If your registry needs credentials, add the pull secrets from `global.imagePullSecrets` to the Pod, under `spec.imagePullSecrets`.

```bash
kubectl apply -f alfred-print-profile.yaml
```

```output
pod/alfred-print-profile created
```

Wait for both containers to finish:

```bash
kubectl wait pod/alfred-print-profile -n ome --for=jsonpath='{.status.phase}'=Succeeded --timeout=90s
```

```output
pod/alfred-print-profile condition met
```

Each container's log holds its worker's profile:

```bash
kubectl logs alfred-print-profile -n ome -c default-scheduler
```

```output
{"identity":{"schedulerName":"default-scheduler","backend":"kube-v135","schedulerVersion":"v1.35.4","configurationID":"sha256:<default-id>"},"gangScheduling":false}
```

```bash
kubectl logs alfred-print-profile -n ome -c ome-scheduler
```

```output
{"identity":{"schedulerName":"ome-scheduler","backend":"ome-v135","schedulerVersion":"v1.35.4","configurationID":"sha256:<ome-id>"},"gangScheduling":true}
```

Copy both profiles exactly. The `configurationID` changes when the configuration or Alfred's image changes, and `gangScheduling` is `true` when OMEGangPack runs.

When the simulator rejects a configuration, its container fails, the Pod ends `Failed` and the wait times out: that container's log says what's wrong. When the Pod stays `Pending`, `kubectl describe pod alfred-print-profile -n ome` shows why, for example an image pull error.

Once you've copied the profiles, remove the Pod and its ConfigMap:

```bash
kubectl delete pod,configmap alfred-print-profile -n ome
```

```output
pod "alfred-print-profile" deleted from ome namespace
configmap "alfred-print-profile" deleted from ome namespace
```

## Step 4: Create the simulation ConfigMap

List the workers in `workers.json`, with the profiles you printed and the paths where Alfred mounts the ConfigMap, under `/etc/alfred-simulation`:

```json title="workers.json"
{
  "workers": [
    {
      "binaryPath": "/alfred-simulator",
      "schedulerConfigPath": "/etc/alfred-simulation/default-scheduler.yaml",
      "identity": {
        "schedulerName": "default-scheduler",
        "backend": "kube-v135",
        "schedulerVersion": "v1.35.4",
        "configurationID": "sha256:<default-id>"
      },
      "gangScheduling": false
    },
    {
      "binaryPath": "/alfred-simulator",
      "schedulerConfigPath": "/etc/alfred-simulation/ome-scheduler.yaml",
      "identity": {
        "schedulerName": "ome-scheduler",
        "backend": "ome-v135",
        "schedulerVersion": "v1.35.4",
        "configurationID": "sha256:<ome-id>"
      },
      "gangScheduling": true
    }
  ]
}
```

Alfred reads this file strictly: an unknown field or a repeated identity stops it at startup.

Create the simulation ConfigMap from the three files, then make it immutable, so that it can't change under a running Alfred:

```bash
kubectl create configmap alfred-simulation-1 -n ome \
  --from-file=workers.json --from-file=default-scheduler.yaml --from-file=ome-scheduler.yaml
```

```output
configmap/alfred-simulation-1 created
```

```bash
kubectl patch configmap alfred-simulation-1 -n ome -p '{"immutable":true}'
```

```output
configmap/alfred-simulation-1 patched
```

When you later change a scheduler's configuration or Alfred's image, print the profiles again, create a ConfigMap with a new name, such as `alfred-simulation-2`, and point Alfred at it.

## Step 5: Point Alfred at the workers {#point-alfred-at-the-workers}

Add the ConfigMap and the profiles to your `values.yaml`, next to the image you pinned. `alfredConfig.scheduling.profiles` is keyed by the scheduler name, and each entry repeats that worker's identity:

```yaml title="values.yaml"
simulation:
  configMapName: alfred-simulation-1
alfredConfig:
  scheduling:
    profiles:
      default-scheduler:
        backend: kube-v135
        schedulerVersion: v1.35.4
        configurationID: sha256:<default-id>
      ome-scheduler:
        backend: ome-v135
        schedulerVersion: v1.35.4
        configurationID: sha256:<ome-id>
        gangScheduling: true
```

Upgrade the release:

```bash
helm upgrade ome-alfred oci://ghcr.io/moirai-internal/charts/ome-alfred \
  --version 1.3.0 --namespace ome -f values.yaml
```

Helm upgrades the release and prints its status. Wait for the new pods:

```bash
kubectl rollout status deployment/ome-alfred -n ome
```

```output
deployment "ome-alfred" successfully rolled out
```

kubectl can print `Waiting for deployment "ome-alfred" rollout to finish` lines first. Alfred checks the workers before its pods become ready, so a finished rollout means that the workers loaded. When the rollout doesn't finish, see [Alfred exits at startup](#alfred-exits-at-startup).

## Step 6: Check the predictions

After the next decision pass, 5 minutes at most by default, read the predictions from the recommendation record:

```bash
kubectl get configmap alfred-recommendations -n ome -o jsonpath='{.data.last-cycle\.json}' \
  | jq '.recommendations[] | select(.scheduling) | {workload, component, instance, advisoryReason, scheduling: (.scheduling | {schedulerName, status, reason})}'
```

```output
{
  "workload": "prod/chat",
  "component": "engine",
  "instance": 3,
  "advisoryReason": "OMENativeUnavailable",
  "scheduling": {
    "schedulerName": "default-scheduler",
    "status": "Feasible",
    "reason": "PlacementFound"
  }
}
```

The command prints nothing until a policy proposes to move an OMENative Instance. Each move stays advisory, with `OMENativeUnavailable`, until you turn on migration. Its `scheduling.status` is `Feasible`, with the reason `PlacementFound`, when the simulation placed every replacement pod, and `Infeasible` when it didn't. Otherwise, `scheduling.reason` says why there's no prediction:

| Reason | Meaning |
| --- | --- |
| `ProfileNotConfigured` | No profile has the scheduler name that the pods use. Add a worker and a profile for that scheduler. |
| `GangUnsupported` | The Instance has more than one pod, and its profile doesn't have `gangScheduling: true`. Make its pods use the OME scheduler. |
| `WorkerFailed` | The worker didn't answer, for example because the profile differs from its identity in `workers.json`. Print the profiles again. |
| `CaptureFailed` | Alfred couldn't copy the cluster, for example because the PodGroup CRD is missing: see [Before you begin](#before-you-begin). |
| `SimulationBudgetExceeded` | The move was past the 8 that Alfred simulates in a pass, or the pass ran out of its 30 seconds for simulation. |

[Scheduling results](../../reference/scheduling/alfred-metrics-and-events.md#scheduling-results) lists every reason.

## Troubleshooting

### Alfred exits at startup

A worker that fails to load, or reports another identity, makes Alfred exit, and the rollout doesn't finish. Read the logs of the Alfred pods:

```bash
kubectl logs -n ome -l control-plane=ome-alfred --tail=-1 --prefix
```

A pod that's restarting may print nothing yet: add `--previous` to read its last run. Look for the error `unable to configure recommendation simulation`, and the cause after it:

| Cause | What to do |
| --- | --- |
| `worker profile identity does not match registry entry` | The identity in `workers.json` isn't the one that the worker prints. Print the profiles again from the digest that Alfred runs, and create a new ConfigMap. |
| `worker gang scheduling claim does not match registry entry` | Fix the worker's `gangScheduling` in `workers.json`, in a new ConfigMap. |
| `scheduler worker exited unsuccessfully` | The worker couldn't load its configuration. Run it with `--print-profile`, as in [Step 3](#print-each-profile), to see why. |

The error `invalid startup configuration` means that a chart value is out of range, and the cause names its flag: see [Helm chart values](../../reference/scheduling/alfred-configuration.md#helm-chart-values).

## Clean up

If you turned on migration, turn it off first: see [Clean up](let-alfred-migrate-instances.md#clean-up) in Let Alfred migrate Instances. Then remove `simulation` and `alfredConfig.scheduling` from your `values.yaml`, upgrade the release as in [Step 5](#point-alfred-at-the-workers), and delete the ConfigMap:

```bash
kubectl delete configmap alfred-simulation-1 -n ome
```

```output
configmap "alfred-simulation-1" deleted from ome namespace
```

## Next steps

- [Let Alfred migrate Instances](let-alfred-migrate-instances.md): turn on execute mode, which needs the simulation.
- [Alfred configuration](../../reference/scheduling/alfred-configuration.md#scheduling-profiles): the scheduling profiles and the simulation values.
- [Alfred metrics and events](../../reference/scheduling/alfred-metrics-and-events.md#scheduling-results): every scheduling result.
- [OME scheduler](../../concepts/scheduling/ome-scheduler.md): the scheduler that places Instances of more than one pod.
