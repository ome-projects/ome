<script lang="ts">
	import { base } from '$app/paths';
	import { cubicOut } from 'svelte/easing';
	import { slide } from 'svelte/transition';
	import { scrollReveal } from '$lib/actions/scrollReveal';
	import PlusMark from '$lib/components/PlusMark.svelte';
	import SectionLabel from '$lib/components/SectionLabel.svelte';
	import { repoUrl, site } from '$lib/config/site';

	const panelMotion = { duration: 320, easing: cubicOut };

	// Organizations that run OME in production and have agreed to be named here.
	// ADOPTERS.md at the repo root is the list of record; keep the two in step.
	// While this list is empty, the block invites adopters to add themselves.
	const adopters: readonly { name: string; href?: string }[] = [];
	const adoptersUrl = `${repoUrl}/blob/${site.branch}/ADOPTERS.md`;

	// Each body is checked against the code that does what it says; recheck
	// them when OMENative, the model agent, runtime selection, accelerator
	// policies, deployment modes, rollouts, the OME scheduler, Alfred or
	// multi-cluster routing change. OMENative comes first, so it's the item
	// that starts open. The OME scheduler, Alfred and multi-cluster placement
	// and routing are alpha; take "(alpha)" out of their titles when they
	// graduate, and "alpha, still in development" out of the multi-cluster
	// body.
	type Feature = {
		title: string;
		body: string;
		// The page that explains the feature, linked under the body.
		link?: { href: string; label: string };
	};

	const features: readonly Feature[] = [
		{
			title: 'OMENative workload engine',
			body: 'OME creates, repairs, and migrates each replica as one Instance: one pod, or a leader and its workers. By default, new Instances serve before old ones drain. A failing revision is held, and prefill and decode can roll out together.',
			link: { href: `${base}/concepts/omenative/overview`, label: 'Read the OMENative overview' }
		},
		{
			title: 'Models as Kubernetes resources',
			body: 'BaseModel and ClusterBaseModel say where the weights live. The optional model agent downloads them to your nodes and reads the architecture, size, and capabilities. Or leave the model out, and your runtime loads the weights itself.'
		},
		{
			title: 'Automatic runtime selection',
			body: "OME compares the model's format, architecture, quantization, and size with each runtime that allows auto-selection, then picks the highest-scoring match. Name a runtime yourself and OME uses it; a format mismatch is only a warning."
		},
		{
			title: 'GPU-aware placement',
			body: 'AcceleratorClasses describe your GPUs by memory, compute, and cost. Name one or let the BestFit, Cheapest, or MostCapable policy choose, and OME sets node selectors, GPU requests, and runtime arguments to match.'
		},
		{
			title: 'Any serving topology',
			body: 'Single-node models default to Deployments and multi-node models to OMENative, which any component can opt in to; LeaderWorkerSet is deprecated. Add a decoder to split prefill and decode, and on OMENative roll them out together.',
			link: {
				href: `${base}/guides/omenative/move-from-leaderworkerset`,
				label: 'Move from LeaderWorkerSet to OMENative'
			}
		},
		{
			title: 'Progressive rollouts',
			body: 'Pin an InferenceService to a runtime revision and roll forward or back when you choose. On OMENative, related components can roll out as one group, and canaries can promote or roll back on metrics. kubectl ome rollout shows progress.'
		},
		{
			title: 'The OME scheduler (alpha)',
			body: 'An optional second scheduler, for Kubernetes 1.35 only: kube-scheduler with the OMEGangPack plugin. For pods that opt in with schedulerName, it packs each OMENative gang into one topology domain, all or nothing.',
			link: {
				href: `${base}/concepts/scheduling/ome-scheduler`,
				label: 'Read about the OME scheduler'
			}
		},
		{
			title: 'Alfred, GPU cluster caretaker (alpha)',
			body: 'Alfred recommends Instance moves when free GPUs fragment or a node turns unhealthy. It only recommends by default; turn on execute mode and it asks OMENative to carry them out.',
			link: { href: `${base}/concepts/scheduling/alfred`, label: 'Read about Alfred' }
		},
		{
			title: 'Multi-cluster placement and routing (alpha)',
			body: "A control-plane cluster places InferenceServices on workload clusters, and a TrafficMap weights a global endpoint's traffic across them by ready capacity, health probes, and drains. Both are alpha, still in development, and off by default.",
			link: {
				href: `${base}/concepts/rollouts-and-traffic/traffic-map`,
				label: 'Read about the traffic map'
			}
		}
	];

	// The first item starts open, as in the mockup, so the section shows what an
	// answer looks like before anyone clicks.
	let openIndex = $state<number | null>(0);

	function toggle(index: number) {
		openIndex = openIndex === index ? null : index;
	}
</script>

<section class="home-why" aria-labelledby="home-why-heading">
	<div
		class="home-why-intro"
		use:scrollReveal={{
			children: '.home-why-label, .home-why-copy, .home-why-adopters',
			y: 32,
			stagger: 0.12,
			duration: 0.85,
			start: 'top 88%'
		}}
	>
		<SectionLabel label="Why OME?" as="h2" id="home-why-heading" class="home-why-label" />
		<p class="home-why-copy">
			OME turns models and GPUs into production endpoints on Kubernetes. You describe what to serve.
			OME chooses the runtime, generates the workloads, and rolls out your changes.
		</p>
		<div class="home-why-adopters">
			{#if adopters.length > 0}
				<p class="home-why-adopters-label">Runs in production at</p>
				<ul class="home-why-adopters-list">
					{#each adopters as adopter (adopter.name)}
						<li>
							{#if adopter.href}
								<a class="home-why-adopter" href={adopter.href} rel="noopener">{adopter.name}</a>
							{:else}
								<span class="home-why-adopter">{adopter.name}</span>
							{/if}
						</li>
					{/each}
				</ul>
			{:else}
				<p class="home-why-adopters-label">Adopters</p>
				<p class="home-why-adopters-cta">
					Running OME in production? <a href={adoptersUrl}>Add your organization to ADOPTERS.md</a>.
				</p>
			{/if}
		</div>
	</div>

	<div
		class="home-why-accordion"
		use:scrollReveal={{
			children: '.home-why-item',
			y: 28,
			stagger: 0.1,
			duration: 0.75,
			start: 'top 85%'
		}}
	>
		{#each features as feature, index (feature.title)}
			<div class="home-why-item" class:home-why-item--open={openIndex === index}>
				<button
					type="button"
					class="home-why-trigger"
					aria-expanded={openIndex === index}
					onclick={() => toggle(index)}
				>
					<span class="home-why-trigger-title">{feature.title}</span>
					<span class="home-why-toggle" aria-hidden="true">
						<PlusMark class="home-why-plus" />
					</span>
				</button>
				{#if openIndex === index}
					<div class="home-why-panel" transition:slide={panelMotion}>
						<p>{feature.body}</p>
						{#if feature.link}
							<a class="home-why-link" href={feature.link.href}>{feature.link.label}</a>
						{/if}
					</div>
				{/if}
			</div>
		{/each}
	</div>
</section>
