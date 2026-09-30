---
title: Move a manifest install to the Helm charts
description: "Move an OME install from make install or kubectl apply -k to the ome-crd and ome-resources Helm charts, and keep your InferenceServices, models and runtimes in place."
---

If you installed OME with `make install` or `kubectl apply -k config/default`, move it to the Helm charts that [Install OME](../../getting-started/install.md) uses, as the releases `ome-crd` and `ome`. Helm takes over the existing objects, so your InferenceServices, models and runtimes stay in place. Since v1.3, the charts also set defaults that a manifest install leaves out, such as the [replica defaults](set-replica-defaults.md) and the [PodDisruptionBudget defaults](../deploy-models/configure-pod-disruption-budgets.md).

<div class="prerequisites" markdown>

- OME installed with `make install` or `kubectl apply -k config/default`, which install it into the namespace `ome`.
- `kubectl`, with the rights that [Install OME](../../getting-started/install.md) lists.
- Helm 4, or Helm 3.17 or newer, for the `--take-ownership` flag.
- cert-manager, installed as in [Step 1 of Install OME](../../getting-started/install.md#step-1-install-cert-manager), even if your manifest install ran with `OME_ENABLE_SELF_SIGNED_CA=true`.

</div>

Install the charts at the version that your manifest install came from. The commands use `1.2.2`, for a checkout of v1.2.2. For a build of `main`, install the charts from the checkout as in [Install from source](../../getting-started/install.md#install-from-source), and add `--take-ownership` to both commands.

## Step 1: Prepare the InferenceService CRD

The manifest install's `inferenceservices.ome.io` CRD has a conversion webhook, and the annotation `cert-manager.io/inject-ca-from`, which has cert-manager write a CA bundle into it. The chart's CRD has neither, and Helm leaves both in place when it takes the CRD over. Remove the annotation, so that cert-manager stops writing the CA bundle, and then the conversion webhook:

```bash
kubectl annotate crd inferenceservices.ome.io cert-manager.io/inject-ca-from-
kubectl patch crd inferenceservices.ome.io --type=json -p '[{"op":"remove","path":"/spec/conversion"}]'
```

```output
customresourcedefinition.apiextensions.k8s.io/inferenceservices.ome.io annotated
customresourcedefinition.apiextensions.k8s.io/inferenceservices.ome.io patched
```

The CRD serves a single version, so removing the conversion webhook leaves your InferenceServices as they are.

## Step 2: Install the ome-crd chart

Install the `ome-crd` chart over the existing CRDs:

```bash
helm upgrade --install ome-crd oci://ghcr.io/moirai-internal/charts/ome-crd \
  --version 1.2.2 \
  --namespace ome \
  --take-ownership
```

Helm installs the release and reports its status as `deployed`.

## Step 3: Install the ome release

Install the `ome-resources` chart over the existing controller and model agent. The manifest install's model agent keeps model weights under `/raid/models`, so keep that path in your values file:

```yaml title="values.yaml"
modelAgent:
  enabled: true
  hostPath: /raid/models
```

=== "Helm 4"

    ```bash
    helm upgrade --install ome oci://ghcr.io/moirai-internal/charts/ome-resources \
      --version 1.2.2 \
      --namespace ome \
      --take-ownership \
      -f values.yaml \
      --server-side=false
    ```

=== "Helm 3"

    ```bash
    helm upgrade --install ome oci://ghcr.io/moirai-internal/charts/ome-resources \
      --version 1.2.2 \
      --namespace ome \
      --take-ownership \
      -f values.yaml
    ```

Helm installs the release and reports its status as `deployed`. Since v1.3, the command can fail with the [webhook error of a first install](../../getting-started/install.md#the-first-install-fails-with-a-webhook-error), with a message that starts with `Error: failed to create resource:`. Run it again as that section describes.

Pass the same values file to every later upgrade, as [Step 3 of Install OME](../../getting-started/install.md#step-3-install-ome) explains, so that the model agent keeps its path.

## Step 4: Delete the leftover objects

The charts leave out two objects of the manifest install, which are unused after the move. Delete them:

```bash
kubectl delete service ome-controller-manager-metrics-service -n ome
kubectl delete secret ome-webhook-server-secret -n ome
```

```output
service "ome-controller-manager-metrics-service" deleted from ome namespace
secret "ome-webhook-server-secret" deleted from ome namespace
```

The runtimes and models that `make install` applied from `config/runtimes` and `config/models` stay in place, outside both releases.

Then check the controller and the model agent as in [Step 4 of Install OME](../../getting-started/install.md#step-4-verify-the-installation).

## Troubleshooting

### Helm can't take over an existing object

Helm installs only over objects that it manages. Without `--take-ownership`, it stops at the first existing object and names it:

```text
Error: unable to continue with install: CustomResourceDefinition "acceleratorclasses.ome.io" in namespace "" exists and cannot be imported into the current release: invalid ownership metadata; label validation error: missing key "app.kubernetes.io/managed-by": must be set to "Helm"; annotation validation error: missing key "meta.helm.sh/release-name": must be set to "ome-crd"; annotation validation error: missing key "meta.helm.sh/release-namespace": must be set to "ome"
```

Run the command again with `--take-ownership`, on Helm 4, or Helm 3.17 or newer.

## Next steps

- [Configure the controller](configure-the-controller.md): tune the OME controller manager's command-line flags.
- [Set replica defaults](set-replica-defaults.md): choose the `minReplicas` and `maxReplicas` that components get by default.
- [Run the model agent](model-agent.md): configure the model agent that downloads models to each node.
