<script lang="ts">
	import { base } from '$app/paths';
	import type { PagePosition } from '$lib/docs/navigation';

	let { position }: { position: PagePosition } = $props();
</script>

{#if position.prev || position.next}
	<nav class="doc-pagenav" aria-label="Previous and next pages">
		{#if position.prev}
			<a class="doc-pagenav-link doc-pagenav-link--prev" href="{base}/{position.prev.route}">
				<span class="doc-pagenav-label">Previous</span>
				<span class="doc-pagenav-title">{position.prev.label}</span>
			</a>
		{/if}
		{#if position.next}
			<a class="doc-pagenav-link doc-pagenav-link--next" href="{base}/{position.next.route}">
				<span class="doc-pagenav-label">Next</span>
				<span class="doc-pagenav-title">{position.next.label}</span>
			</a>
		{/if}
	</nav>
{/if}
