<script lang="ts">
	import { scrollReveal } from '$lib/actions/scrollReveal';
	import HomeMetricValue from '$lib/components/HomeMetricValue.svelte';
	import SectionLabel from '$lib/components/SectionLabel.svelte';

	// Fixed in code so website/ doesn't depend on the rest of the repo. The recipe
	// count and families come from config/runtimes until the recipes repository
	// exists; recheck them against it then. The strategies are the rollout group
	// strategies in pkg/apis/ome/v1beta1 (rolling, blue-green, canary), and the
	// command count is the AddCommand calls in pkg/cli/root.go.
	const metrics = [
		{
			title: 'RUNTIME RECIPES',
			prefix: '',
			value: 200,
			suffix: '+',
			description: 'Pre-defined for DeepSeek, Kimi, MiniMax, GLM, Qwen and Gemma'
		},
		{
			title: 'ROLLOUT STRATEGIES',
			prefix: '',
			value: 3,
			suffix: '',
			description: 'Rolling, blue-green and canary, across prefill and decode'
		},
		{
			title: 'KUBECTL OME',
			prefix: '',
			value: 17,
			suffix: '',
			description: 'Commands to inspect, explain, and act'
		}
	] as const;
</script>

<section
	class="home-metrics"
	aria-label="OME at a glance"
	use:scrollReveal={{ children: '.home-metrics-card', y: 48, stagger: 0.12, duration: 0.85 }}
>
	<div class="home-metrics-grid">
		{#each metrics as metric, index (metric)}
			<article class="home-metrics-card">
				<div class="home-metrics-card-head">
					<SectionLabel label={metric.title} as="h2" class="home-metrics-card-title" />
				</div>
				<div class="home-metrics-card-body">
					<p class="home-metrics-card-value">
						<HomeMetricValue
							prefix={metric.prefix}
							value={metric.value}
							suffix={metric.suffix}
							duration={1500 + index * 120}
						/>
					</p>
					<p class="home-metrics-card-desc">{metric.description}</p>
				</div>
			</article>
		{/each}
	</div>
</section>
