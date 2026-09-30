<script lang="ts">
	import { base } from '$app/paths';
	import { scrollReveal } from '$lib/actions/scrollReveal';
	import ChoosePathArrow from '$lib/components/ChoosePathArrow.svelte';
	import SectionLabel from '$lib/components/SectionLabel.svelte';

	const paths = [
		{
			label: 'New to OME',
			description: 'Install OME and serve your first model.',
			href: `${base}/getting-started`
		},
		{
			label: 'Guides',
			description: 'Deploy models, set up networking, roll out changes, and run OME itself.',
			href: `${base}/guides`
		},
		{
			label: 'Concepts',
			description: 'How models, runtimes, and InferenceServices fit together.',
			href: `${base}/concepts`
		},
		{
			label: 'Reference',
			description: 'The OME API, kubectl ome commands, and matching rules.',
			href: `${base}/reference`
		}
	] as const;
</script>

<!-- SiteHeader finds this section by its home-choose-path class to switch to its dark style. -->
<section class="home-choose-path" aria-labelledby="home-choose-path-heading">
	<div class="home-choose-path-inner">
		<div
			class="home-choose-path-header"
			use:scrollReveal={{
				children: '.home-choose-path-label, .home-choose-path-intro',
				y: 36,
				stagger: 0.12,
				duration: 0.9,
				start: 'top 88%'
			}}
		>
			<SectionLabel
				label="Choose your path"
				as="h2"
				id="home-choose-path-heading"
				class="home-choose-path-label"
			/>
			<p class="home-choose-path-intro">
				Start with what you need today, whether that's a first model on a test cluster or a fleet
				you already run.
			</p>
		</div>

		<div
			class="home-choose-path-grid"
			use:scrollReveal={{
				children: '.home-choose-path-card',
				y: 48,
				stagger: 0.11,
				duration: 0.85,
				start: 'top 85%'
			}}
		>
			{#each paths as path (path.href)}
				<article class="home-choose-path-card">
					<p class="home-choose-path-card-copy">{path.description}</p>
					<a class="home-choose-path-cta" href={path.href}>
						<span class="home-choose-path-cta-label">{path.label}</span>
						<span class="home-choose-path-cta-icon" aria-hidden="true">
							<ChoosePathArrow />
						</span>
					</a>
				</article>
			{/each}
		</div>
	</div>
</section>
