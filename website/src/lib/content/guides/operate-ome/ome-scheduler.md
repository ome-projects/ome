---
title: Use the OME scheduler
description: Install the alpha ome-scheduler as a second scheduler and opt workloads into topology-packed gang placement; it requires Kubernetes 1.35.
since: v1.3
---

The OME scheduler, `ome-scheduler`, keeps the pods of a multi-node model replica together. It places each gang of pods in one accelerator domain, such as an NVLink domain, and binds the gang only once all its pods have a node. It's kube-scheduler with one extra plugin, OMEGangPack, and it runs as a second scheduler next to your default one: only pods that set `schedulerName: ome-scheduler` use it. The OME scheduler is alpha, and it runs only on Kubernetes 1.35.

You install the scheduler, test it with a two-pod demo gang, and then move [OMENative](../../concepts/omenative/overview.md) components onto it. [The OME scheduler](../../concepts/scheduling/ome-scheduler.md) explains how it places gangs. On another Kubernetes version, use the scheduler-plugins Coscheduling plugin, which gang-schedules pods without domain packing, as [Gang scheduling](../../concepts/serving/gang-scheduling.md#bring-a-gang-aware-scheduler) describes.

<div class="prerequisites" markdown>

- A Kubernetes 1.35 cluster, and permission to create ClusterRoles, ClusterRoleBindings and a RoleBinding in `kube-system`.
- Helm.
- Two schedulable nodes with 4Gi of free memory each, for the scheduler's two replicas. On a one-node cluster, run one replica, as [Step 1](#step-1-install-the-chart) shows.
- Accelerator nodes with a topology label: a label whose value is the same on all the nodes of one accelerator domain, such as an NVLink domain or a rack. The examples use `nvidia.com/gpu.clique`. List your nodes' labels with `kubectl get nodes --show-labels`.
- For the demo, two GPU nodes with the same value of that label and a free GPU each.
- For [Step 4](#opt-in-omenative-components), OME and an OMENative InferenceService with multi-pod Instances, like the one in [Serve a multi-node model](../omenative/serve-a-multi-node-model.md).
- An ome-scheduler image in a registry that your nodes can pull from. OME's releases don't publish it, so build it with Docker from the OME repository at your release's tag, and push it:

```bash
make ome-scheduler-image REGISTRY=registry.example.com/ome TAG=v1.3.0 DOCKER_BUILD_CMD=docker
docker push registry.example.com/ome/ome-scheduler:v1.3.0
```

`make` builds a `linux/amd64` image and prints Docker's build log, and `docker push` prints the image's digest. For Arm nodes, add `ARCH=linux/arm64` to the `make` command.

- The scheduler-plugins PodGroup CRD, `scheduling.x-k8s.io/v1alpha1`. Install it yourself, at the version that the scheduler is built against, `v0.35.4-devel`:

```bash
kubectl apply -f https://raw.githubusercontent.com/kubernetes-sigs/scheduler-plugins/v0.35.4-devel/config/crd/bases/scheduling.x-k8s.io_podgroups.yaml
```

```output
customresourcedefinition.apiextensions.k8s.io/podgroups.scheduling.x-k8s.io created
```

</div>

## How gang packing works

A gang is the set of pods whose `scheduling.x-k8s.io/pod-group` label names the same PodGroup, and the PodGroup's `minMember` is the gang's size. An accelerator domain is the set of nodes that share one value of a topology label. The PodGroup's `ome.io/topology-key` annotation names the label, or the chart's [fallback](#set-a-fallback-topology-key) does. Gang members land only on nodes that carry the label.

The scheduler waits until `minMember` members of a gang exist, and picks a domain with room for all of them, one node per member. It holds that domain while the gang forms, and binds the members together once `minMember` of them have a node. A gang that fits in no domain waits until one has room, since the chart turns preemption off by default. [How it places a gang](../../concepts/scheduling/ome-scheduler.md#how-it-places-a-gang) explains the choice of domain, the hold and the timeout.

## Step 1: Install the chart

Point the chart at your image in a values file:

```yaml title="values.yaml"
global:
  hub: registry.example.com/ome
scheduler:
  image:
    tag: v1.3.0
```

The chart then pulls `registry.example.com/ome/ome-scheduler:v1.3.0`. Leave `scheduler.name` at its default, `ome-scheduler`, the name that pods set in `spec.schedulerName`.

The chart runs two replicas on separate nodes, with leader election: one replica schedules, and the other takes over if it stops. On a one-node cluster, add these values to `values.yaml` to run one replica without a PodDisruptionBudget:

```yaml title="values.yaml"
scheduler:
  replicaCount: 1
  podDisruptionBudget:
    enabled: false
```

On a small cluster, you can also lower the replicas' `scheduler.resources`.

Install the chart into the `ome` namespace:

```bash
helm install ome-scheduler oci://ghcr.io/moirai-internal/charts/ome-scheduler \
  --version 1.3.0 --namespace ome --create-namespace -f values.yaml
```

Helm installs the release and prints its status. Wait for the scheduler to start:

```bash
kubectl rollout status deployment/ome-scheduler -n ome
```

```output
deployment "ome-scheduler" successfully rolled out
```

kubectl can print progress lines first. If the rollout hangs, list the scheduler's pods with `kubectl get pods -n ome -l app.kubernetes.io/name=ome-scheduler`. A replica stays `Pending` when no second node can run it, and `ImagePullBackOff` means that the nodes failed to pull the image.

## Step 2: Run a demo gang

Run a gang of two plain pods, outside OME, to see the scheduler work on its own.

Create a namespace for the demo:

```bash
kubectl create namespace gang-demo
```

```output
namespace/gang-demo created
```

Create the PodGroup first. Pods created before it wait until it exists.

```yaml title="podgroup.yaml"
apiVersion: scheduling.x-k8s.io/v1alpha1
kind: PodGroup
metadata:
  name: gang-demo
  namespace: gang-demo
  annotations:
    ome.io/topology-key: nvidia.com/gpu.clique
spec:
  minMember: 2
  scheduleTimeoutSeconds: 300
```

- `ome.io/topology-key` names the label that defines the gang's domains. It's a label key: the scheduler picks the domain.
- `minMember` is the gang's size.
- `scheduleTimeoutSeconds` is how long, in seconds, members wait for the rest of the gang.

The scheduler ignores `minResources` and doesn't update the PodGroup's `status`, so watch the pods instead, as Step 3 does.

```bash
kubectl apply -f podgroup.yaml
```

```output
podgroup.scheduling.x-k8s.io/gang-demo created
```

!!! warning "Create a gang's pods together"
    The scheduler places none of a gang's pods until `minMember` of them exist. A controller that creates one pod at a time and waits for each to become Ready never completes the gang, so its first pod stays `Pending`. A StatefulSet does this with its default `OrderedReady` policy: give it `podManagementPolicy: Parallel`, or use a Deployment or a Job.

Then create the gang: a Deployment of two pods that set `schedulerName: ome-scheduler`, carry the PodGroup's label and request one GPU each. The scheduler plans a separate node per member but doesn't keep members apart, so the pod anti-affinity keeps the two pods on separate nodes. Add the tolerations and node selector that your GPU nodes need.

```yaml title="gang.yaml"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: gang-demo
  namespace: gang-demo
spec:
  replicas: 2
  selector:
    matchLabels:
      app: gang-demo
  template:
    metadata:
      labels:
        app: gang-demo
        scheduling.x-k8s.io/pod-group: gang-demo
    spec:
      schedulerName: ome-scheduler
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            - labelSelector:
                matchLabels:
                  app: gang-demo
              topologyKey: kubernetes.io/hostname
      containers:
        - name: pause
          image: registry.k8s.io/pause:3.10
          resources:
            limits:
              nvidia.com/gpu: 1
```

```bash
kubectl apply -f gang.yaml
```

```output
deployment.apps/gang-demo created
```

## Step 3: Check the placement

List the gang's pods and their nodes:

```bash
kubectl get pods -n gang-demo -l app=gang-demo \
  -o custom-columns=NAME:.metadata.name,STATUS:.status.phase,NODE:.spec.nodeName
```

kubectl lists both pods as `Running`, on separate nodes. While the gang forms, both are `Pending` with no node.

Check that the two nodes share a domain:

```bash
kubectl get nodes -L nvidia.com/gpu.clique
```

kubectl adds a column with each node's value of the label, and the gang's two nodes show the same value.

Read the pods' events:

```bash
kubectl describe pods -n gang-demo -l app=gang-demo
```

Both pods have a `Scheduled` event from `ome-scheduler`, with the message `Successfully assigned gang-demo/{pod} to {node}`. Pods still waiting for a place have `FailedScheduling` events that give the reason, as [Scheduling events](#scheduling-events) explains.

## Step 4: Opt in OMENative components {#opt-in-omenative-components}

OME creates a PodGroup for each multi-pod [Instance](../../concepts/omenative/instances.md) of an OMENative component, and labels the Instance's pods with it. [Gang scheduling](../../concepts/serving/gang-scheduling.md#one-podgroup-per-multi-pod-instance) describes these PodGroups.

OME looks for the PodGroup CRD only when its manager starts. If you installed the CRD while OME was running, restart the manager:

```bash
kubectl rollout restart deployment ome-controller-manager -n ome
```

```output
deployment.apps/ome-controller-manager restarted
```

Until the restart, OME creates no PodGroups but still labels the pods, so the OME scheduler keeps them `Pending` with `PodGroup {namespace}/{name} not resolvable yet`.

OME uses the scheduler that the runtime or the InferenceService names. Set `spec.schedulerName` on the runtime to cover the component pod specs that set no scheduler of their own. Or set `schedulerName` on an InferenceService component, which wins over the runtime's. For a component whose Instances have more than one pod, also set the component's `topologyKey`:

=== "ServingRuntime"

    ```yaml
    spec:
      schedulerName: ome-scheduler
      engineConfig:
        topologyKey: nvidia.com/gpu.clique
    ```

=== "InferenceService"

    ```yaml
    spec:
      engine:
        schedulerName: ome-scheduler
        topologyKey: nvidia.com/gpu.clique
    ```

A multi-node component without a `topologyKey` needs the chart's [fallback topology key](#set-a-fallback-topology-key). Without either, its pods stay `Pending` with `PodGroup {namespace}/{name} must declare ome.io/topology-key or the scheduler must configure topologyKey`. Components with single-pod Instances need only the `schedulerName`.

For a running InferenceService, the change starts a rollout, and the new Instances use the OME scheduler. An InferenceService with `autoSync: false` picks up a runtime change only when you [roll it forward](../../concepts/runtimes/runtime-revisions.md#roll-forward-to-the-latest-runtime). With the chart's defaults, `topologySpread: Preferred` has no effect under the OME scheduler, so use `Required` to spread Instances across domains: see [Spread under the OME scheduler](../omenative/spread-instances-across-fault-domains.md#preferred-under-the-ome-scheduler).

Once the rollout finishes, check where the pods run. For the InferenceService of [Serve a multi-node model](../omenative/serve-a-multi-node-model.md):

```bash
kubectl get pods -n qwen3-multi-node -l ome.io/inferenceservice=qwen3-multi-node \
  -o custom-columns=NAME:.metadata.name,NODE:.spec.nodeName
```

kubectl lists the pods and their nodes. The pods of an Instance run on nodes with the same `nvidia.com/gpu.clique` value, as `kubectl get nodes -L nvidia.com/gpu.clique` shows.

## Tune packing

These settings are chart values. Add them to `values.yaml`, then upgrade the release:

```bash
helm upgrade ome-scheduler oci://ghcr.io/moirai-internal/charts/ome-scheduler \
  --version 1.3.0 --namespace ome -f values.yaml
```

Helm upgrades the release. When the scheduler's configuration changes, the Deployment replaces its pods one at a time. [OME scheduler configuration](../../reference/scheduling/ome-scheduler-configuration.md#chart-values) lists all the values.

### Set a fallback topology key

`scheduler.plugin.topologyKey` is the domain label for gangs whose PodGroup has no `ome.io/topology-key` annotation. Multi-pod OMENative components without a `topologyKey` need it. It's empty by default, and the annotation wins over it:

```yaml title="values.yaml"
scheduler:
  plugin:
    topologyKey: nvidia.com/gpu.clique
```

### Pack by accelerator

The scheduler favors nodes that are already busy, and by default it weighs `cpu` and `memory`. Add your accelerator resource to `scheduler.nodeResourcesFit.resources`:

```yaml title="values.yaml"
scheduler:
  nodeResourcesFit:
    resources:
      - name: nvidia.com/gpu
        weight: 10
      - name: cpu
        weight: 1
      - name: memory
        weight: 1
```

### Pack standalone pods

Pods that use the OME scheduler without a gang, like single-node model replicas, are standalone pods. By default, `scheduler.plugin.standaloneDomainPacking` steers them toward partly used domains, which keeps whole domains free for gangs. It suits pods that take a whole node, and it assumes that all domains have the same number of nodes. It works only when `scheduler.plugin.topologyKey` is set.

By default, the scheduler scores a share of the nodes that fit a pod. Set `scheduler.percentageOfNodesToScore` to `100` so the packing compares all the domains:

```yaml title="values.yaml"
scheduler:
  plugin:
    topologyKey: nvidia.com/gpu.clique
  percentageOfNodesToScore: 100
```

### Set the permit timeout

A stuck gang holds its domain until its members' wait times out. The PodGroup's `scheduleTimeoutSeconds` sets that wait, and `scheduler.plugin.defaultPermitTimeoutSeconds`, 600 by default, applies when a PodGroup sets none. A shorter timeout frees a stuck gang's domain sooner. OME sets `scheduleTimeoutSeconds` on its own PodGroups, 600 seconds with the ome-resources chart's defaults: [Bound the gang schedule timeout](../../concepts/serving/gang-scheduling.md#bound-the-gang-schedule-timeout) shows how to change it.

## Metrics

The scheduler serves `/metrics` over HTTPS on port 10259, to callers whose token may read it. On a Prometheus Operator stack, create a ServiceMonitor, and list the ServiceAccount that your Prometheus runs as, since Prometheus scrapes with that account's token:

```yaml title="values.yaml"
scheduler:
  metrics:
    reader:
      serviceAccounts:
        - name: prometheus
          namespace: monitoring
    serviceMonitor:
      enabled: true
```

Upgrade the release as [Tune packing](#tune-packing) shows. If your Prometheus selects ServiceMonitors by label, add the labels in `scheduler.metrics.serviceMonitor.additionalLabels`. The scheduler's certificate is self-signed, so the ServiceMonitor skips verification unless you set a CA in `scheduler.metrics.serviceMonitor.tlsConfig`.

Next to kube-scheduler's standard metrics, OMEGangPack exports alpha gang metrics. For example, `ome_scheduler_gang_pin_total{result="no_fit"}` grows while gangs fit in no domain. [OME scheduler configuration](../../reference/scheduling/ome-scheduler-configuration.md#metrics) lists them all.

## Troubleshooting

### Pods stay Pending with no events

No running scheduler has the name in the pods' `schedulerName`. Check that the rollout in [Step 1](#step-1-install-the-chart) finished, and that the name matches the chart's `scheduler.name`.

### Pods stay Pending with a FailedScheduling event {#scheduling-events}

The event's message gives the reason. These are the common ones:

| Reason | What it means |
| --- | --- |
| `no domain has room for gang {namespace}/{name}` | No domain has room for the whole gang. The gang waits until one does. |
| `PodGroup {namespace}/{name} not resolvable yet` | The PodGroup that the pod's label names is missing from the pod's namespace. Create it or fix the label. For OMENative pods, see [Step 4](#opt-in-omenative-components). |
| `PodGroup {namespace}/{name} must declare ome.io/topology-key or the scheduler must configure topologyKey` | The gang has no domain label. Set the component's `topologyKey`, or the chart's [fallback](#set-a-fallback-topology-key). |
| `waiting for all PodGroup member templates for {namespace}/{name}` | Fewer than `minMember` member pods exist. [Create a gang's pods together](#step-2-run-a-demo-gang). |
| `pinned domain {domain} cannot fit all remaining gang members {namespace}/{name}` | Some members have nodes, and their domain has no room for the rest. The gang stays in that domain while any member runs there. |

[OME scheduler configuration](../../reference/scheduling/ome-scheduler-configuration.md#scheduling-events) lists all the reasons, and what `didn't satisfy plugin(s) [OMEGangPack]` means in a message.

### An InferenceService has a MaybeNoGangScheduler warning event {#an-inferenceservice-has-a-maybenogangscheduler-warning}

OME created PodGroups for a component whose pods use the default scheduler. The stock kube-scheduler ignores PodGroups, so it can place some of an Instance's pods and leave the rest `Pending`. Move the component to the OME scheduler, as [Step 4](#opt-in-omenative-components) shows. If your default scheduler runs the Coscheduling plugin, you can ignore the warning: see [Gang scheduling](../../concepts/serving/gang-scheduling.md#bring-a-gang-aware-scheduler).

### Read the scheduler's logs {#logs}

```bash
kubectl logs -n ome -l app.kubernetes.io/name=ome-scheduler --tail=-1
```

kubectl prints the logs of both replicas, and only the leader's log shows scheduling. To log OMEGangPack's domain decisions, set `scheduler.verbosity` to 4, up from its default of 2. The `gangpack.pinGang.result` lines name the domain that a gang pinned, and the `gangpack.pinGang.no_fit` lines show why no domain fits. The `gangpack.Filter.reject` lines show nodes outside a gang's domain.

## Clean up

Delete the demo namespace, with its Deployment and PodGroup:

```bash
kubectl delete namespace gang-demo
```

```output
namespace "gang-demo" deleted
```

Before you remove the scheduler, change or remove `schedulerName: ome-scheduler` on the runtimes and InferenceServices that set it. Running pods keep running, but new pods that name the scheduler stay `Pending` with no events.

Uninstall the chart:

```bash
helm uninstall ome-scheduler --namespace ome
```

```output
release "ome-scheduler" uninstalled
```

The PodGroup CRD stays. Leave it installed while OME runs multi-pod OMENative Instances, since OME creates a PodGroup for each.

## Next steps

- [The OME scheduler](../../concepts/scheduling/ome-scheduler.md): how the scheduler places gangs, and its limitations.
- [OME scheduler configuration](../../reference/scheduling/ome-scheduler-configuration.md): the chart values, the PodGroup fields that the scheduler reads, and its events and metrics.
- [Serve a multi-node model](../omenative/serve-a-multi-node-model.md): run a model whose Instances span nodes.
- [Let Alfred migrate Instances](../scheduling/let-alfred-migrate-instances.md): to migrate a multi-pod Instance, Alfred needs its pods on the OME scheduler.
- [Gang scheduling](../../concepts/serving/gang-scheduling.md): the PodGroups that OME creates, and gang scheduling without the OME scheduler.
