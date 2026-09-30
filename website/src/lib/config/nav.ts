import type { NavSection } from '$lib/docs/types';

/** Sidebar order for each section. Each section's index.md is its landing page and is not listed. */
export const nav: NavSection[] = [
	{
		id: 'getting-started',
		label: 'Getting Started',
		groups: [
			{
				label: null,
				pages: [
					'introduction.md',
					'install.md',
					'serve-your-first-model.md',
					'pre-configured-models.md',
					'private-registries.md'
				]
			}
		]
	},
	{
		id: 'guides',
		label: 'Guides',
		groups: [
			{
				label: 'Deploy models',
				pages: [
					'deploy-models/deploy-an-inferenceservice.md',
					'deploy-models/reference-a-runtime-explicitly.md',
					'deploy-models/troubleshoot-runtime-selection.md',
					'deploy-models/serve-models-from-pvc.md',
					'deploy-models/serve-models-from-local-storage.md',
					'deploy-models/select-accelerators.md',
					'deploy-models/configure-pod-disruption-budgets.md',
					'deploy-models/run-benchmarks.md'
				]
			},
			{
				label: 'OMENative',
				pages: [
					'omenative/serve-a-model-on-omenative.md',
					'omenative/serve-a-multi-node-model.md',
					'omenative/serve-a-prefill-decode-model.md',
					'omenative/move-from-leaderworkerset.md',
					'omenative/spread-instances-across-fault-domains.md',
					'omenative/set-instance-readiness-deadlines.md',
					'omenative/reset-failed-instances.md',
					'omenative/recover-stuck-deletions.md',
					'omenative/change-the-status-encoding.md'
				]
			},
			{
				label: 'Networking',
				pages: [
					'networking/configure-ingress.md',
					'networking/expose-without-ingress.md',
					'networking/gateway-host-schemes.md',
					'networking/multiple-gateways.md',
					'networking/namespace-gateways.md',
					'networking/configure-route-timeouts.md',
					'networking/set-service-app-protocols.md'
				]
			},
			{
				label: 'Roll out changes',
				pages: [
					'roll-out-changes/pause-and-resume-a-rollout.md',
					'roll-out-changes/promote-or-roll-back-a-canary.md',
					'roll-out-changes/release-a-held-revision.md',
					'roll-out-changes/repin-a-drifted-rollout-plan.md',
					'roll-out-changes/pace-rollouts-with-min-ready-seconds.md'
				]
			},
			{
				label: 'Scale and migrate',
				pages: [
					'scale-and-migrate/request-a-transient-scale.md',
					'scale-and-migrate/request-an-instance-migration.md',
					'scale-and-migrate/scale-to-zero-with-keda.md',
					'scale-and-migrate/bring-your-own-autoscaler.md'
				]
			},
			{
				label: 'Scheduling and capacity',
				pages: [
					'operate-ome/ome-scheduler.md',
					'scheduling/run-alfred.md',
					'scheduling/set-up-scheduler-simulation.md',
					'scheduling/let-alfred-migrate-instances.md',
					'operate-ome/accelerator-quota.md'
				]
			},
			{
				label: 'Multi-cluster',
				preview: true,
				pages: [
					'multi-cluster/publish-a-global-endpoint.md',
					'multi-cluster/routing-health-probes.md',
					'multi-cluster/drain-a-workload-cluster.md',
					'multi-cluster/weight-traffic-for-heterogeneous-clusters.md',
					'multi-cluster/tune-routing-capacity-polling.md'
				]
			},
			{
				label: 'Operate OME',
				pages: [
					'operate-ome/configure-the-controller.md',
					'operate-ome/set-replica-defaults.md',
					'operate-ome/model-agent.md',
					'operate-ome/configure-model-artifact-retention.md',
					'operate-ome/shared-hf-artifacts.md',
					'operate-ome/metrics.md',
					'operate-ome/alerting.md',
					'operate-ome/move-to-the-helm-charts.md'
				]
			},
			{
				label: 'Troubleshoot',
				pages: ['troubleshoot/troubleshoot-an-inferenceservice.md']
			}
		]
	},
	{
		id: 'concepts',
		label: 'Concepts',
		groups: [
			{
				label: 'Architecture',
				pages: ['architecture/how-ome-works.md', 'architecture/deployment-modes.md']
			},
			{
				label: 'OMENative',
				pages: [
					'omenative/overview.md',
					'omenative/instances.md',
					'architecture/omenative-update-strategies.md',
					'omenative/instance-restart-policy.md',
					'omenative/migration-and-transient-scale.md'
				]
			},
			{
				label: 'Scheduling and capacity',
				pages: [
					'serving/gang-scheduling.md',
					'scheduling/ome-scheduler.md',
					'scheduling/alfred.md',
					'scheduling/alfred-policies.md'
				]
			},
			{
				label: 'Models',
				pages: ['models/base-models.md', 'models/fine-tuned-weights.md']
			},
			{
				label: 'Runtimes',
				pages: [
					'runtimes/serving-runtimes.md',
					'runtimes/runtime-inheritance.md',
					'runtimes/runtime-revisions.md',
					'runtimes/accelerator-classes.md'
				]
			},
			{
				label: 'Serving',
				pages: [
					'serving/inference-services.md',
					'serving/component-autoscaling.md',
					'serving/autoscaler-policy.md',
					'serving/benchmarks.md'
				]
			},
			{
				label: 'Rollouts and traffic',
				pages: [
					'rollouts-and-traffic/rollout-groups.md',
					'rollouts-and-traffic/rollout-policy.md',
					'rollouts-and-traffic/ingress.md',
					'rollouts-and-traffic/traffic-policy.md',
					'rollouts-and-traffic/traffic-map.md'
				]
			}
		]
	},
	{
		id: 'reference',
		label: 'Reference',
		groups: [
			{
				label: 'API',
				pages: ['api/ome.v1beta1.md', 'api/labels-and-annotations.md', 'api/traffic-annotations.md']
			},
			{
				label: 'kubectl ome',
				pages: [
					'kubectl-ome/overview.md',
					'kubectl-ome/accelerator.md',
					'kubectl-ome/admin.md',
					'kubectl-ome/autoscale.md',
					'kubectl-ome/cluster.md',
					'kubectl-ome/get.md',
					'kubectl-ome/instance.md',
					'kubectl-ome/logs.md',
					'kubectl-ome/migration.md',
					'kubectl-ome/placement.md',
					'kubectl-ome/quota.md',
					'kubectl-ome/rollout.md',
					'kubectl-ome/runtime.md',
					'kubectl-ome/scale.md',
					'kubectl-ome/status.md',
					'kubectl-ome/traffic.md',
					'kubectl-ome/version.md',
					'kubectl-ome/wait.md',
					'kubectl-ome/guarded-actions.md'
				]
			},
			{
				label: 'Matching',
				pages: [
					'matching/runtime-selection-scoring.md',
					'matching/model-version-matching.md',
					'matching/model-size-range-matching.md',
					'matching/runtime-accelerator-class-matching.md',
					'matching/runtime-deployment-mode-matching.md',
					'matching/diffusion-pipeline-runtime-matching.md'
				]
			},
			{
				label: 'Rollouts',
				pages: ['rollouts/canary-progression.md', 'rollouts/canary-analysis.md']
			},
			{
				label: 'Scheduling and capacity',
				pages: [
					'scheduling/ome-scheduler-configuration.md',
					'scheduling/alfred-configuration.md',
					'scheduling/alfred-metrics-and-events.md'
				]
			},
			{
				label: 'Operate OME',
				pages: ['operate-ome/controller-manager-flags.md', 'operate-ome/ome-serving-values.md']
			},
			{
				label: 'Storage',
				pages: ['storage/benchmark-output-storage.md']
			}
		]
	},
	{
		id: 'contributing',
		label: 'Contributing',
		groups: [
			{
				label: null,
				pages: ['development-setup.md', 'pull-requests-and-oeps.md', 'writing-docs.md']
			}
		]
	}
];
