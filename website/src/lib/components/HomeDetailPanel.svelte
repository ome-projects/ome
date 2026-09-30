<script lang="ts">
	import { scrollReveal } from '$lib/actions/scrollReveal';
	import HomeHowItWorks from '$lib/components/HomeHowItWorks.svelte';
	import HomeWhy from '$lib/components/HomeWhy.svelte';
</script>

<div class="home-detail-panel" use:scrollReveal={{ y: 56, duration: 1.1, start: 'top 90%' }}>
	<div class="home-detail-panel-inner">
		<HomeHowItWorks />
		<HomeWhy />
	</div>
</div>
