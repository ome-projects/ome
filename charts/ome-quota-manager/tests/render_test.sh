#!/usr/bin/env bash
set -euo pipefail

chart_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
helm_bin="${HELM_BIN:-helm}"

fail() {
  echo "ome-quota-manager chart test: $*" >&2
  exit 1
}

# Release name deliberately unlike the component name: object names come from
# quotaManager.name, and a release-derived name would slip through unnoticed if
# the two matched.
render() {
  "${helm_bin}" template rel "${chart_dir}" --namespace ome "$@"
}

# --- mode is required, and the guard must fire rather than render a bad flag ---

if render >/dev/null 2>&1; then
  fail "an unset mode rendered instead of failing"
fi
for mode in workload management; do
  render --set quotaManager.mode="${mode}" >/dev/null ||
    fail "mode ${mode} did not render"
done
if render --set quotaManager.mode=both >/dev/null 2>&1; then
  fail "an invalid mode rendered instead of failing"
fi

# --- object names track quotaManager.name, never the release ---

default="$(render --set quotaManager.mode=workload)"

grep -Fq 'name: ome-quota-manager' <<<"${default}" ||
  fail "objects are not named from quotaManager.name"
if grep -Eq '^\s+name: rel(-|$)' <<<"${default}"; then
  fail "an object was named from the release instead of quotaManager.name"
fi

renamed="$(render --set quotaManager.mode=workload --set quotaManager.name=aq-mgr)"
grep -Fq 'name: aq-mgr' <<<"${renamed}" ||
  fail "overriding quotaManager.name did not rename objects"
# The RBAC scopes itself by name, so a rename that misses one grant would leave
# the component unable to inject its own caBundle.
grep -Fq 'resourceNames: ["aq-mgr.ome.io"]' <<<"${renamed}" ||
  fail "the webhook-config grant did not follow the rename"
grep -Fq 'resourceNames: ["aq-mgr-webhook-cert"]' <<<"${renamed}" ||
  fail "the secret grant did not follow the rename"
grep -Fq -- '--webhook-config-name=aq-mgr.ome.io' <<<"${renamed}" ||
  fail "the binary flag did not follow the rename"

# --- internal cert management: the out-of-the-box default ---

grep -Fq -- '--internal-cert-management=true' <<<"${default}" ||
  fail "internal cert management is not the default"
grep -Fq -- '--cert-namespace=ome' <<<"${default}" ||
  fail "the cert namespace was not passed"
grep -Fq -- '--cert-secret-name=ome-quota-manager-webhook-cert' <<<"${default}" ||
  fail "the cert secret name was not passed"
grep -Fq -- '--webhook-config-name=ome-quota-manager.ome.io' <<<"${default}" ||
  fail "the webhook config name was not passed"

# The rotator only ever Updates the Secret, so the chart creating it empty is
# load-bearing: without it the component never becomes ready.
grep -Fq 'name: ome-quota-manager-webhook-cert' <<<"${default}" ||
  fail "the placeholder cert Secret was not rendered"
grep -Fq 'helm.sh/resource-policy: keep' <<<"${default}" ||
  fail "the cert Secret is not retained across an uninstall"

# The rotator writes the Secret and the kubelet projects it back down, so the
# mount is the delivery path, not just the cert-manager hand-off.
grep -Fq 'secretName: ome-quota-manager-webhook-cert' <<<"${default}" ||
  fail "the cert Secret is not mounted"

if grep -Fq 'cert-manager.io' <<<"${default}"; then
  fail "the default install still references cert-manager"
fi

# Self-injection is scoped to this component's own webhook config. A grant that
# lost its resourceNames could rewrite ome-manager's fail-closed pod mutator.
grep -Fq 'resourceNames: ["ome-quota-manager.ome.io"]' <<<"${default}" ||
  fail "webhook-config update is not scoped by name"
if grep -Fq 'resources: ["mutatingwebhookconfigurations"]' <<<"${default}"; then
  fail "the quota plane was granted access to mutating webhooks"
fi
grep -Fq 'resourceNames: ["ome-quota-manager-webhook-cert"]' <<<"${default}" ||
  fail "secret update is not scoped by name"

# --- capacity derivation carries its own RBAC, and only in workload mode ---

# The grants and the flags have to appear together: flags without the grants is
# a crash-looping RBAC error, grants without the flags is a cluster-wide Node
# read nothing uses.
grep -Fq 'resources: ["nodes"]' <<<"${default}" ||
  fail "workload mode did not grant the Node read capacity derivation needs"
grep -Fq 'resources: ["resourceflavors"]' <<<"${default}" ||
  fail "workload mode did not grant the ResourceFlavor read"
grep -Fq -- '--accelerator-resources=google.com/tpu,nvidia.com/gpu' <<<"${default}" ||
  fail "the accelerator suffixes were not passed"
grep -Fq -- '--capacity-hysteresis-percent=10' <<<"${default}" ||
  fail "the hysteresis band was not passed"

# A management-mode install holds the authored fleet tree and has no local
# silicon, so a cluster-wide Node read there would be unused privilege.
management="$(render --set quotaManager.mode=management)"
for unwanted in 'resources: ["nodes"]' 'resources: ["resourceflavors"]' '--accelerator-resources'; do
  if grep -Fq -- "${unwanted}" <<<"${management}"; then
    fail "management mode rendered ${unwanted}"
  fi
done

# Turning derivation off must drop the grants with it, or the privilege
# outlives the feature that justified it. Every spelling an operator might
# reach for, because Helm deletes a defaulted map on a null rather than merging
# it and an unguarded lookup then aborts the whole render.
for off in \
  'quotaManager.capacity.resources=null' \
  'quotaManager.capacity=null' \
  'quotaManager.capacity.resources={}' \
  ; do
  nocapacity="$(render --set quotaManager.mode=workload --set "${off}")" ||
    fail "--set ${off} failed to render at all"
  for unwanted in 'resources: ["nodes"]' 'resources: ["resourceflavors"]' '--accelerator-resources'; do
    if grep -Fq -- "${unwanted}" <<<"${nocapacity}"; then
      fail "--set ${off} still rendered ${unwanted}"
    fi
  done
done

# A blank entry is not a resource. The chart must agree with the binary, which
# trims and drops blanks — otherwise a list of blanks grants cluster-wide Node
# reads for a feature the binary treats as switched off.
blanks="$(render --set quotaManager.mode=workload --set 'quotaManager.capacity.resources[0]=')"
if grep -Fq -- 'resources: ["nodes"]' <<<"${blanks}"; then
  fail "a list of blank resources still granted the Node read"
fi

# A flag rendered with no value crash-loops the pod on parse.
allrendered="$(render --set quotaManager.mode=workload --set quotaManager.capacity.hysteresisPercent=null)"
if grep -Eq -- '--[a-z-]+=(")?$' <<<"${allrendered}"; then
  fail "a flag rendered with an empty value"
fi

# --- materialization carries its own Kueue write RBAC, workload mode only ---

# Materializing needs BOTH a cover and somewhere to put the queues. A leaf's
# LocalQueue lives in an enrolled namespace and its ClusterQueue selects exactly
# that set, so with none there is nothing to render: the queue would select no
# namespace, admit nothing, and say so in no object.
materializing="$(render --set quotaManager.mode=workload \
  --set 'quotaManager.materialize.enrolledNamespaces={ml-serving}')"

# Flags and grants have to appear together: flags without the grants is a
# crash-looping RBAC error, grants without the flags is cluster-wide write on
# Kueue that nothing uses.
grep -Fq -- '--cover-resources=cpu=16M,ephemeral-storage=16Pi,memory=16Pi' <<<"${materializing}" ||
  fail "the default cover resources were not passed"
grep -Fq -- '--enrolled-namespaces=ml-serving' <<<"${materializing}" ||
  fail "the enrolled namespaces were not passed"

# Sorted and deduplicated, so reordering an overlay cannot change the flag and a
# repeated entry cannot reach the binary, which rejects a duplicate at startup.
manyns="$(render --set quotaManager.mode=workload \
  --set 'quotaManager.materialize.enrolledNamespaces={ml-serving,ml-batch,ml-serving}')"
grep -Fq -- '--enrolled-namespaces=ml-batch,ml-serving' <<<"${manyns}" ||
  fail "the enrolled namespaces were not sorted and deduplicated"

# An operator overrides one key and keeps the rest: Helm merges the map rather
# than replacing it, and the whole point of a default is that omitting a key
# leaves it alone.
onekey="$(render --set quotaManager.mode=workload \
  --set 'quotaManager.materialize.enrolledNamespaces={ml-serving}' \
  --set quotaManager.materialize.coverResources.cpu=2k)"
grep -Fq -- '--cover-resources=cpu=2k,ephemeral-storage=16Pi,memory=16Pi' <<<"${onekey}" ||
  fail "overriding one cover resource did not leave the others at their defaults"

# A Cohort's subtree quota sums the cover across its children, and the Kueue this
# repo compiles against wraps rather than clamps on int64 overflow. Ei-scale
# defaults leave no room for that sum, and a wrapped ceiling is a wrong admission
# decision rather than a failure.
if grep -Eq -- '--cover-resources=[^ ]*[0-9]+(Ei|E)([,"]|$)' <<<"${materializing}"; then
  fail "the default cover resources reach Ei/E scale, leaving no room for a cohort subtree sum"
fi
grep -Fq -- '--field-manager=ome-quota-manager' <<<"${materializing}" ||
  fail "the field manager did not default to the component name"
grep -Fq 'resources: ["cohorts", "clusterqueues", "localqueues"]' <<<"${materializing}" ||
  fail "workload mode did not grant the Kueue write materialization needs"
# patch on the resource, not update on the finalizers subresource: a finalizer
# is metadata, so claiming a node is an ordinary patch. Granting the subresource
# instead leaves every reconcile failing on a forbidden patch, which is invisible
# to a render test that only checks a grant is present.
grep -Fq 'verbs: ["patch"]' <<<"${materializing}" ||
  fail "the finalizer patch grant is missing, so no node could be claimed or reaped"
if grep -Fq 'resources: ["acceleratorquotas/finalizers"]' <<<"${materializing}"; then
  fail "granted the finalizers subresource, which governs blockOwnerDeletion and is not what a finalizer needs"
fi

# No enrolled namespace is the default, and it must render no materialization at
# all rather than a queue that selects nothing.
for unwanted in 'resources: ["cohorts", "clusterqueues", "localqueues"]' '--cover-resources' \
  '--enrolled-namespaces' '--field-manager'; do
  if grep -Fq -- "${unwanted}" <<<"${default}"; then
    fail "no enrolled namespace still rendered ${unwanted}"
  fi
done

# A rename must carry the field manager with it, or two installs would claim
# each other's objects.
renamedmat="$(render --set quotaManager.mode=workload --set quotaManager.name=aq-mgr \
  --set 'quotaManager.materialize.enrolledNamespaces={ml-serving}')"
grep -Fq -- '--field-manager=aq-mgr' <<<"${renamedmat}" ||
  fail "the field manager did not follow the rename"

# A management-mode install writes no Kueue object anywhere, so the write grant
# there would be pure unused privilege.
for unwanted in 'resources: ["cohorts", "clusterqueues", "localqueues"]' '--cover-resources' '--field-manager'; do
  if grep -Fq -- "${unwanted}" <<<"${management}"; then
    fail "management mode rendered ${unwanted}"
  fi
done

# Turning materialization off must drop the write grant with it, in every
# spelling an operator might reach for.
for off in \
  'quotaManager.materialize.coverResources=null' \
  'quotaManager.materialize.enrolledNamespaces=null' \
  'quotaManager.materialize=null' \
  ; do
  nomat="$(render --set quotaManager.mode=workload \
    --set 'quotaManager.materialize.enrolledNamespaces={ml-serving}' --set "${off}")" ||
    fail "--set ${off} failed to render at all"
  for unwanted in 'resources: ["cohorts", "clusterqueues", "localqueues"]' '--cover-resources' \
    'verbs: ["patch"]'; do
    if grep -Fq -- "${unwanted}" <<<"${nomat}"; then
      fail "--set ${off} still rendered ${unwanted}"
    fi
  done
done

# A blank quantity is not a cover resource. The chart must agree with the
# binary, which rejects a valueless pair at startup.
blankqty="$(render --set quotaManager.mode=workload \
  --set 'quotaManager.materialize.enrolledNamespaces={ml-serving}' \
  --set quotaManager.materialize.coverResources.cpu= \
  --set quotaManager.materialize.coverResources.memory= \
  --set quotaManager.materialize.coverResources.ephemeral-storage=)"
if grep -Fq -- 'resources: ["cohorts", "clusterqueues", "localqueues"]' <<<"${blankqty}"; then
  fail "blank cover quantities still granted the Kueue write"
fi

# --- the CRD is opt-in, and retained when it is opted into ---

if grep -Fq 'kind: CustomResourceDefinition' <<<"${default}"; then
  fail "the CRD rendered by default, which collides with ome-crd"
fi

withcrd="$(render --set quotaManager.mode=workload --set quotaManager.crd.install=true)"
grep -Fq 'name: acceleratorquotas.ome.io' <<<"${withcrd}" ||
  fail "opting in did not render the CRD"
# Deleting the CRD cascades to every AcceleratorQuota, and each of those deletes
# is gated by this chart's own fail-closed webhook — which by uninstall time has
# no endpoints. Without the retention annotation the CRD wedges in Terminating.
"${helm_bin}" template rel "${chart_dir}" --namespace ome \
  --set quotaManager.mode=workload --set quotaManager.crd.install=true \
  --show-only templates/crd.yaml | grep -Fq 'helm.sh/resource-policy: keep' ||
  fail "the opted-in CRD is not retained on uninstall"

# --- cert-manager remains available as an opt-out ---

certmanager="$(render --set quotaManager.mode=workload \
  --set quotaManager.webhook.internalCertManagement=false)"

grep -Fq 'kind: Certificate' <<<"${certmanager}" ||
  fail "opting out did not render a cert-manager Certificate"
grep -Fq 'cert-manager.io/inject-ca-from: ome/ome-quota-manager-serving-cert' <<<"${certmanager}" ||
  fail "opting out did not annotate the webhook config for cainjector"
grep -Fq 'secretName: ome-quota-manager-webhook-cert' <<<"${certmanager}" ||
  fail "opting out did not mount the cert Secret"
if grep -Fq 'resources: ["validatingwebhookconfigurations"]' <<<"${certmanager}"; then
  fail "opting out left the self-injection RBAC in place"
fi

# --- disabling the webhook drops the whole cert story ---

nowebhook="$(render --set quotaManager.mode=workload \
  --set quotaManager.webhook.enabled=false)"

for unwanted in ValidatingWebhookConfiguration cert-manager.io ome-quota-manager-webhook-cert; do
  if grep -Fq "${unwanted}" <<<"${nowebhook}"; then
    fail "disabling the webhook still rendered ${unwanted}"
  fi
done

# --- reaching the fleet is a management-mode grant only ---

mgmt="$(render --set quotaManager.mode=management --show-only templates/rbac.yaml)"
for wanted in '"workloadclusters"' '"secrets"'; do
  grep -Fq "${wanted}" <<<"${mgmt}" ||
    fail "management mode cannot reach the fleet: ${wanted} not granted"
done
# The registry has one writer, in ome-manager, and its grace state is in memory.
# A second writer flaps the condition and loses silently.
if grep -Fq 'workloadclusters/status' <<<"${mgmt}"; then
  fail "management mode grants WorkloadCluster status, which belongs to the registry"
fi

work="$(render --set quotaManager.mode=workload --show-only templates/rbac.yaml)"
if grep -Fq '"workloadclusters"' <<<"${work}"; then
  fail "workload mode was granted the registry it never reads"
fi

# --- a projecting plane can claim the nodes it projects ---

# The projector puts a finalizer on every node it claims, which is an ordinary
# patch of the object. Without the grant each pass fails forbidden before
# anything is projected, and the fleet never leaves its authored state.
mgmtProject="$(render --set quotaManager.mode=management \
  --set quotaManager.projection.origin=hub-1 \
  --set quotaManager.projection.fieldManager=proj \
  --show-only templates/rbac.yaml)"

# One rule spans two lines, and a newline inside a grep pattern makes it two
# alternative patterns rather than one contiguous match — which would pass on
# the resource name alone, whatever the verbs beside it. Flatten instead.
flat() { tr '\n' ' ' | tr -s ' '; }
claim='resources: ["acceleratorquotas"] verbs: ["patch"]'

grep -Fq "${claim}" <<<"$(flat <<<"${mgmtProject}")" ||
  fail "a projecting management plane cannot claim a node with a finalizer"

# ...and one that is not projecting carries no write it cannot use.
if grep -Fq "${claim}" <<<"$(flat <<<"${mgmt}")"; then
  fail "management mode grants a spec write with no projector to use it"
fi

# --- projection is off until it is named, and never leaks into workload mode ---

projectOn="$(render --set quotaManager.mode=management \
  --set quotaManager.projection.origin=hub-1 \
  --set quotaManager.projection.fieldManager=proj \
  --set quotaManager.projection.defaultDistributionPolicy=Proportional \
  --show-only templates/deployment.yaml)"
# Counted, not merely found. A duplicated template block renders every flag
# twice, which Go's flag parsing tolerates -- last wins -- so nothing downstream
# would complain and a `grep -q` would pass while the args list carried each
# projection flag two times over.
for wanted in \
  '--projection-origin=hub-1' \
  '--projection-field-manager=proj' \
  '--default-distribution-policy=Proportional'; do
  seen="$(grep -Fc -- "${wanted}" <<<"${projectOn}" || true)"
  [ "${seen}" = 1 ] ||
    fail "management mode rendered ${wanted} ${seen} times, want exactly 1"
done

# An unmarked copy cannot be told from a node an admin authored on the member,
# so projection must stay off until an origin is chosen deliberately.
bare="$(render --set quotaManager.mode=management --show-only templates/deployment.yaml)"
if grep -Fq -- '--projection-origin' <<<"${bare}"; then
  fail "projection turned itself on without an origin"
fi

# A workload-mode manager holds no transport at all; the flags would be inert
# at best and misleading at worst.
leak="$(render --set quotaManager.mode=workload \
  --set quotaManager.projection.origin=hub-1 --show-only templates/deployment.yaml)"
if grep -Fq -- '--projection-origin' <<<"${leak}"; then
  fail "projection flags leaked into workload mode"
fi

# Exec is opt-in, and naming no command still permits none.
if grep -Fq -- '--allow-exec-credentials' <<<"${projectOn}"; then
  fail "exec credentials were enabled without being asked for"
fi

# --- the platform escape hatches an exec plugin needs ---
#
# An exec-credential binary runs inside this pod and authenticates with material
# the chart cannot know about, so extraEnv/extraVolumes/extraVolumeMounts have to
# reach the container. The case worth pinning is with the webhook OFF: the
# volumes and volumeMounts blocks used to exist only for the webhook cert, so a
# mount added while the webhook is disabled is the one that silently vanishes.
# The three markers share no substring: env renders outside the webhook guard,
# so a value that merely contained the mount path would satisfy the mount's
# assertion too and hide exactly the regression this is here to catch.
extras=(
  --set-json 'quotaManager.extraEnv=[{"name":"PLUGIN_CERT","value":"/var/run/plugin/combined.pem"}]'
  --set-json 'quotaManager.extraVolumes=[{"name":"creds","hostPath":{"path":"/etc/creds","type":"Directory"}}]'
  --set-json 'quotaManager.extraVolumeMounts=[{"name":"creds","mountPath":"/opt/creds","readOnly":true}]'
)
for webhook in true false; do
  out="$(render --set quotaManager.mode=management \
    --set quotaManager.webhook.enabled="${webhook}" "${extras[@]}" \
    --show-only templates/deployment.yaml)"
  for needle in 'name: PLUGIN_CERT' 'path: /etc/creds' 'mountPath: /opt/creds'; do
    if ! grep -Fq -- "${needle}" <<<"${out}"; then
      fail "extra '${needle}' dropped with webhook.enabled=${webhook}"
    fi
  done
done

# --- runAsNonRoot binds the container, not the pod ---
#
# A pod-level runAsNonRoot also binds every container the platform injects, and
# an injected initContainer carries no securityContext of its own. One that runs
# as root then fails with "container has runAsNonRoot and image will run as
# root" and takes the pod with it -- which is how an exec-credential install
# breaks. The workload's own guarantee is unchanged either way.
ctx="$(render --set quotaManager.mode=management --show-only templates/deployment.yaml)"
python3 - "$ctx" <<'PY' || fail "runAsNonRoot is not scoped to the container"
import sys, re
doc = sys.argv[1]
spec = doc.split('    spec:', 1)[1]
pod_ctx = spec.split('      containers:', 1)[0]
if 'runAsNonRoot' in pod_ctx:
    raise SystemExit("pod-level securityContext still sets runAsNonRoot")
ctr = spec.split('      containers:', 1)[1]
if 'runAsNonRoot: true' not in ctr:
    raise SystemExit("container securityContext lost runAsNonRoot")
PY

# And absent, they add nothing: an empty env/volumes block is invalid, not inert.
bare="$(render --set quotaManager.mode=management \
  --set quotaManager.webhook.enabled=false --show-only templates/deployment.yaml)"
for empty in 'env:' 'volumes:' 'volumeMounts:'; do
  if grep -Eq "^[[:space:]]*${empty}[[:space:]]*$" <<<"${bare}"; then
    fail "rendered an empty ${empty} block with no webhook and no extras"
  fi
done
# --- remote access is off unless asked for, grants narrowly, and mints no
# --- non-expiring credential unless explicitly told to ---

for unwanted in ome-quota-access quota-remote-access; do
  if grep -Fq "${unwanted}" <<<"${default}"; then
    fail "remote access rendered without being enabled (${unwanted})"
  fi
done

# Only this template, so the local manager's own grants cannot be mistaken for
# the remote ones. Comments are stripped: the file explains at length which
# grants it deliberately omits, and prose about a rule must not read as the rule.
remote_only() {
  render --set quotaManager.mode=workload \
    --set quotaManager.remoteAccess.enabled=true "$@" \
    --show-only templates/remote_access.yaml | grep -v '^[[:space:]]*#'
}

remote="$(remote_only)"

for wanted in \
  'name: "ome-quota-access"' \
  'resources: ["acceleratorquotas"]'; do
  grep -Fq "${wanted}" <<<"${remote}" ||
    fail "enabling remote access did not render ${wanted}"
done

# A token that never expires is the thing this chart should not hand out by
# default. It is available for simulators, and only when asked for twice.
if grep -Fq 'service-account-token' <<<"${remote}"; then
  fail "enabling remote access minted a non-expiring token without being asked"
fi
grep -Fq 'service-account-token' \
  <<<"$(remote_only --set quotaManager.remoteAccess.serviceAccount.staticToken=true)" ||
  fail "opting into staticToken did not mint the token Secret"

# The role must be bindable to an identity the platform already rotates, with
# no in-cluster ServiceAccount at all.
external="$(remote_only \
  --set quotaManager.remoteAccess.serviceAccount.create=false \
  --set 'quotaManager.remoteAccess.subjects[0].kind=User' \
  --set 'quotaManager.remoteAccess.subjects[0].name=projector@example.com')"
grep -Fq 'kind: "User"' <<<"${external}" ||
  fail "an external subject was not bound"
if grep -Fq 'kind: ServiceAccount' <<<"${external}"; then
  fail "serviceAccount.create=false still rendered a ServiceAccount"
fi

# A role bound to nobody is a silent no-op, so it must not render at all.
if render --set quotaManager.mode=workload \
  --set quotaManager.remoteAccess.enabled=true \
  --set quotaManager.remoteAccess.serviceAccount.create=false >/dev/null 2>&1; then
  fail "remote access bound to nobody rendered instead of failing"
fi

# The whole point of a separate credential is that it can only write budgets.
# A grant that let it create workloads, read the fleet's hardware, or touch
# Kueue would make it interchangeable with ome-multicluster-access, and a leak
# of either would then cost the same as a leak of both.
#
# Status on a member belongs to the workload-mode manager running there; a
# remote writer cannot see what it would be overwriting.
for forbidden in \
  'acceleratorquotas/status' \
  '"nodes"' \
  '"pods"' \
  '"inferenceservices"' \
  'kueue.x-k8s.io'; do
  if grep -Fq "${forbidden}" <<<"${remote}"; then
    fail "the remote role reaches beyond projecting quota: ${forbidden}"
  fi
done

# --- the metrics endpoint is served always; the monitor object is opt-in ---
#
# Scrape annotations are stamped unconditionally, matching ome-controller-manager:
# inert where no annotation-driven collector runs, required where one does. A
# ServiceMonitor is different -- an object no collector selects is
# indistinguishable from a broken scrape -- so it may not appear uninvited.

if grep -Fq 'kind: ServiceMonitor' <<<"${default}"; then
  fail "a ServiceMonitor rendered without being asked for"
fi
grep -Fq 'prometheus.io/scrape: "true"' <<<"${default}" ||
  fail "the scrape annotation is not stamped by default"
# The port must track metricsPort. A literal would be scraped at the wrong port
# the moment an operator moves the endpoint.
grep -Fq 'prometheus.io/port: "8080"' <<<"${default}" ||
  fail "the scrape annotation did not carry metricsPort"

# --- one Service fronts every port, and five references must agree on its name ---
#
# The Service name reaches the serving cert's CN and DNS names, the
# ValidatingWebhookConfiguration's clientConfig, the --webhook-service-name flag
# and the ServiceMonitor's target. A rename that missed one would fail closed:
# the apiserver dialling a name the cert does not cover rejects every
# AcceleratorQuota write.

if [ "$(grep -c '^kind: Service$' <<<"${default}")" -ne 1 ]; then
  fail "expected exactly one Service; the component fronts all ports with one"
fi
# Anchored: the cert Secret is ome-quota-manager-webhook-cert and keeps its name.
if grep -Eq 'name: ome-quota-manager-webhook$' <<<"${default}"; then
  fail "the webhook-only Service name came back"
fi
grep -Fq 'name: ome-quota-manager-service' <<<"${default}" ||
  fail "the Service is not named from the serviceName helper"
# Under internal cert management the DNS name is not in a manifest at all: the
# rotator derives it from this flag. cert-controller verifies the existing cert
# against that name and reissues on a mismatch, which is what makes renaming the
# Service safe on a live cluster.
grep -Fq -- '--webhook-service-name=ome-quota-manager-service' <<<"${default}" ||
  fail "the cert flag does not point at the Service"

# On the cert-manager path the SANs are explicit, and must cover the same name.
certmgr="$(render --set quotaManager.mode=workload \
  --set quotaManager.webhook.internalCertManagement=false)"
for san in \
  'commonName: ome-quota-manager-service.ome.svc' \
  '- ome-quota-manager-service.ome.svc' \
  '- ome-quota-manager-service.ome.svc.cluster.local'; do
  grep -Fq -- "${san}" <<<"${certmgr}" ||
    fail "the issued cert does not cover the Service DNS name: ${san}"
done

# The rename must follow quotaManager.name through every reference. The cert
# flags render only under internal management and the SANs only under
# cert-manager, so each path is checked on its own render.
svc_renamed="$(render --set quotaManager.mode=workload --set quotaManager.name=aq-mgr)"
for ref in \
  'name: aq-mgr-service' \
  '--webhook-service-name=aq-mgr-service'; do
  grep -Fq -- "${ref}" <<<"${svc_renamed}" ||
    fail "a Service-name reference did not follow the rename: ${ref}"
done
if grep -Eq 'name: aq-mgr-webhook$' <<<"${svc_renamed}"; then
  fail "the renamed chart still emits a webhook-only Service"
fi

certmgr_renamed="$(render --set quotaManager.mode=workload --set quotaManager.name=aq-mgr \
  --set quotaManager.webhook.internalCertManagement=false)"
grep -Fq 'commonName: aq-mgr-service.ome.svc' <<<"${certmgr_renamed}" ||
  fail "the issued cert's DNS name did not follow the rename"

# With the webhook off there is nothing to front unless metrics asked for it.
nowebhook="$(render --set quotaManager.mode=workload --set quotaManager.webhook.enabled=false)"
if grep -q '^kind: Service$' <<<"${nowebhook}"; then
  fail "a Service rendered with no port to serve"
fi

annotated="$(render --set quotaManager.mode=workload \
  --set quotaManager.podAnnotations.keep=me)"
grep -Fq 'keep: me' <<<"${annotated}" ||
  fail "the scrape annotations displaced operator-supplied podAnnotations"

moved="$(render --set quotaManager.mode=workload --set quotaManager.metricsPort=9090)"
grep -Fq 'prometheus.io/port: "9090"' <<<"${moved}" ||
  fail "the scrape annotation did not follow metricsPort"

monitored="$(render --set quotaManager.mode=workload \
  --set quotaManager.metrics.serviceMonitor.enabled=true \
  --set quotaManager.metrics.serviceMonitor.additionalLabels.release=kps)"
grep -Fq 'kind: ServiceMonitor' <<<"${monitored}" ||
  fail "the ServiceMonitor did not render"
grep -Fq 'release: kps' <<<"${monitored}" ||
  fail "additionalLabels did not reach the ServiceMonitor"
# Still one Service: the metrics port joins the existing one rather than
# spawning a second object whose labels would be indistinguishable from it.
if [ "$(grep -c '^kind: Service$' <<<"${monitored}")" -ne 1 ]; then
  fail "enabling the ServiceMonitor added a second Service"
fi
# targetPort is the container port NAME, so it survives a metricsPort change.
grep -Fq 'targetPort: metrics' <<<"${monitored}" ||
  fail "the Service did not target the named metrics container port"
# The monitor selects the metrics port by name, so the webhook port on the same
# Service is not scraped.
grep -Fq 'port: metrics' <<<"${monitored}" ||
  fail "the ServiceMonitor did not select the metrics port by name"

# --- exactly one collector, never two ---
#
# The annotation path and the ServiceMonitor are collectors, not preferences. A
# cluster that runs both scrapes the pod twice, every unqualified sum() over the
# result is doubled, and a leader gauge reads 2 on a healthy deployment. The
# monitor wins because it carries namespace, pod, service and endpoint.

if grep -Fq 'prometheus.io/scrape' <<<"${monitored}"; then
  fail "both scrape paths are on at once; the pod would be collected twice"
fi
# ...and with no operator annotations either, the block must not render empty.
if grep -Eq '^      annotations:$' <<<"${monitored}"; then
  fail "an empty annotations block rendered"
fi
# Operator annotations still render when the scrape ones are suppressed.
ident="$(render --set quotaManager.mode=workload \
  --set quotaManager.metrics.serviceMonitor.enabled=true \
  --set 'quotaManager.podAnnotations.identity\.example\.com/inject=true')"
grep -Fq 'identity.example.com/inject: true' <<<"${ident}" ||
  fail "suppressing the scrape annotations dropped operator podAnnotations"
if grep -Fq 'prometheus.io/scrape' <<<"${ident}"; then
  fail "the scrape annotation survived alongside a ServiceMonitor"
fi
# The escape hatch: an operator who genuinely wants both restates it explicitly.
both="$(render --set quotaManager.mode=workload \
  --set quotaManager.metrics.serviceMonitor.enabled=true \
  --set 'quotaManager.podAnnotations.prometheus\.io/scrape=true')"
grep -Fq 'prometheus.io/scrape' <<<"${both}" ||
  fail "an explicit podAnnotations override could not re-enable the annotation path"

# Metrics without a webhook: the Service exists for the metrics port alone.
metricsonly="$(render --set quotaManager.mode=workload \
  --set quotaManager.webhook.enabled=false \
  --set quotaManager.metrics.serviceMonitor.enabled=true)"
grep -Fq 'name: ome-quota-manager-service' <<<"${metricsonly}" ||
  fail "the Service did not render for metrics alone"
if grep -Fq 'targetPort: webhook' <<<"${metricsonly}"; then
  fail "the Service kept a webhook port with the webhook disabled"
fi
# No interval is set by default: the collector's own default applies, rather
# than the chart inventing a scrape rate on its behalf.
if grep -Eq '^\s+interval:' <<<"${monitored}"; then
  fail "the ServiceMonitor invented a scrape interval"
fi

echo "ome-quota-manager chart contracts OK"
