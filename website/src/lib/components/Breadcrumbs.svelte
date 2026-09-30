<script lang="ts">
	import { base } from '$app/paths';
	import type { NavTree, PagePosition } from '$lib/docs/navigation';

	let { tree, position, route }: { tree: NavTree; position: PagePosition; route: string } =
		$props();

	const landingRoute = $derived(tree.landing?.route ?? tree.id);
</script>

<!-- The page title isn't repeated here: the h1 is right below. -->
<nav class="doc-breadcrumbs" aria-label="Breadcrumb">
	<ol>
		<li>
			{#if route === landingRoute}
				<span aria-current="page">{tree.label}</span>
			{:else}
				<a href="{base}/{landingRoute}">{tree.label}</a>
			{/if}
		</li>
		{#if position.group}
			<li>{position.group}</li>
		{/if}
	</ol>
</nav>
