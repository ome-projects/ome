<script lang="ts">
	import type { Snippet } from 'svelte';
	import DocsSidebar from '$lib/components/DocsSidebar.svelte';
	import DocsToc from '$lib/components/DocsToc.svelte';
	import type { NavTree } from '$lib/docs/navigation';
	import type { TocEntry } from '$lib/docs/types';

	let {
		tree,
		route,
		toc,
		children
	}: { tree: NavTree; route: string; toc: TocEntry[]; children: Snippet } = $props();

	// A single heading isn't worth a column.
	const showToc = $derived(toc.length >= 2);
</script>

<div class="docs-layout" class:docs-layout--with-toc={showToc}>
	<!-- Keyed so the open and closed groups reset when the section changes. -->
	{#key tree.id}
		<DocsSidebar {tree} {route} />
	{/key}
	<div class="docs-layout-content">
		{@render children()}
	</div>
	{#if showToc}
		<DocsToc {toc} />
	{/if}
</div>
