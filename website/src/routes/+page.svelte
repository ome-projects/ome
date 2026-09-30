<script lang="ts">
	import HomeChoosePath from '$lib/components/HomeChoosePath.svelte';
	import HomeDetailPanel from '$lib/components/HomeDetailPanel.svelte';
	import HomeHero from '$lib/components/HomeHero.svelte';
	import HomeMetrics from '$lib/components/HomeMetrics.svelte';
	import HomeWorksWith from '$lib/components/HomeWorksWith.svelte';

	let { data } = $props();
</script>

<HomeHero title={data.content['home.hero.title']} subtitle={data.content['home.hero.subtitle']} />
<HomeMetrics />
<HomeWorksWith />
<HomeDetailPanel />
<HomeChoosePath />
