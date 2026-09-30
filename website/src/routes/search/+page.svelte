<script lang="ts">
	import { onDestroy, onMount } from 'svelte';
	import { afterNavigate, goto } from '$app/navigation';
	import { base } from '$app/paths';
	import { page } from '$app/stores';
	import SearchResults from '$lib/components/SearchResults.svelte';
	import { loadSearch, type Search } from '$lib/docs/search-client';
	import { groupHits } from '$lib/ui/group-hits';

	let query = $state($page.url.searchParams.get('q') ?? '');
	let search = $state<Search | null>(null);
	let loading = $state(false);
	let failed = $state(false);
	let timer: ReturnType<typeof setTimeout> | undefined;

	const hits = $derived(search && query.trim() !== '' ? search(query, 50) : []);
	const groups = $derived(groupHits(hits));

	function load() {
		loading = true;
		failed = false;
		loadSearch(base).then(
			(loaded) => {
				search = loaded;
				loading = false;
			},
			() => {
				failed = true;
				loading = false;
			}
		);
	}

	onMount(load);
	onDestroy(() => clearTimeout(timer));

	// Other navigations to this page, such as the search dialog's form or the back button,
	// bring their own query. Typing navigates with goto, and keeps what's in the input.
	afterNavigate(({ type }) => {
		if (type !== 'goto') query = $page.url.searchParams.get('q') ?? '';
	});

	function oninput() {
		clearTimeout(timer);
		timer = setTimeout(() => {
			goto(`?q=${encodeURIComponent(query)}`, {
				replaceState: true,
				keepFocus: true,
				noScroll: true
			});
		}, 150);
	}
</script>

<div class="search-page">
	<h1>Search</h1>
	<form
		class="search-page-form"
		method="GET"
		action="{base}/search"
		role="search"
		data-sveltekit-keepfocus
		data-sveltekit-noscroll
		data-sveltekit-replacestate
	>
		<input
			bind:value={query}
			{oninput}
			type="search"
			name="q"
			autocomplete="off"
			spellcheck="false"
			placeholder="Search the docs"
			aria-label="Search the docs"
		/>
	</form>
	<noscript><p class="search-status">Search needs JavaScript.</p></noscript>
	<div role="status">
		{#if failed}
			<p class="search-status">
				Search couldn't load. Check your connection and try again.
				<button type="button" class="search-retry" onclick={load}>Retry</button>
			</p>
		{:else if loading}
			<p class="search-status">Loading the search index…</p>
		{:else if search && query.trim() !== '' && hits.length === 0}
			<p class="search-status">No pages match “{query.trim()}”.</p>
		{/if}
	</div>
	<section class="search-page-results" aria-label="Search results">
		<SearchResults {groups} {query} idPrefix="search-page-hit" listbox={false} />
	</section>
</div>
