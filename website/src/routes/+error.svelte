<script lang="ts">
	import { base } from '$app/paths';
	import { page } from '$app/stores';
	import { headerNavItems } from '$lib/config/header';

	// The last path segment as words, so /guides/serve-from-pvc searches for "serve from pvc".
	const guess = $derived.by(() => {
		const segment = $page.url.pathname.split('/').filter(Boolean).at(-1) ?? '';
		try {
			return decodeURIComponent(segment).replaceAll('-', ' ');
		} catch {
			return segment.replaceAll('-', ' ');
		}
	});
</script>

<div class="error-page">
	{#if $page.status === 404}
		<h1>Page not found</h1>
		<p>There's no page at this address. It may have moved when the docs were reorganized.</p>
		<form class="search-page-form" method="GET" action="{base}/search" role="search">
			<input
				value={guess}
				type="search"
				name="q"
				autocomplete="off"
				spellcheck="false"
				placeholder="Search the docs"
				aria-label="Search the docs"
			/>
		</form>
		<ul class="error-page-links">
			{#each headerNavItems as item (item.href)}
				<li><a href={item.href}>{item.label}</a></li>
			{/each}
		</ul>
	{:else}
		<h1>Something went wrong</h1>
		<p>{$page.error?.message}</p>
		<p><a href="{base}/">Go to the home page</a></p>
	{/if}
</div>
