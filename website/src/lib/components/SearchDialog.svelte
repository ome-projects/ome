<script lang="ts">
	import { tick, untrack } from 'svelte';
	import { goto } from '$app/navigation';
	import { base } from '$app/paths';
	import SearchResults from '$lib/components/SearchResults.svelte';
	import { loadSearch, type Search } from '$lib/docs/search-client';
	import { groupHits, rowOrder } from '$lib/ui/group-hits';
	import { isPlainClick, isSearchShortcut, moveSelection } from '$lib/ui/search-keys';

	const idPrefix = 'search-dialog-hit';

	let dialog = $state<HTMLDialogElement | null>(null);
	let input = $state<HTMLInputElement | null>(null);
	let body = $state<HTMLDivElement | null>(null);
	let query = $state('');
	let search = $state<Search | null>(null);
	let loading = $state(false);
	let failed = $state(false);
	let returnFocus: HTMLElement | null = null;
	let pressedBackdrop = false;

	const hits = $derived(search && query.trim() !== '' ? search(query, 12) : []);
	const groups = $derived(groupHits(hits));
	// Grouping reorders the hits; active indexes the rows as they're shown.
	const rows = $derived(rowOrder(groups));
	// Resets whenever the results change; the arrow keys move it in between.
	let active = $derived(rows.length > 0 ? 0 : -1);
	const allResults = $derived(
		query.trim() === '' ? `${base}/search` : `${base}/search?q=${encodeURIComponent(query)}`
	);

	// New results start at the top.
	$effect(() => {
		void hits;
		untrack(() => body?.scrollTo({ top: 0 }));
	});

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

	export function open(initialQuery = '') {
		if (!dialog) return;
		query = initialQuery;
		if (!dialog.open) {
			returnFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
			dialog.showModal();
		}
		if (!search && !loading) load();
		input?.focus();
		input?.select();
	}

	function close() {
		dialog?.close();
	}

	function onclose() {
		if (returnFocus?.isConnected) returnFocus.focus();
		returnFocus = null;
	}

	async function onkeydown(event: KeyboardEvent) {
		if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
			event.preventDefault();
			active = moveSelection(active, event.key === 'ArrowDown' ? 1 : -1, rows.length);
			await tick();
			document.getElementById(`${idPrefix}-${active}`)?.scrollIntoView({ block: 'nearest' });
		} else if (event.key === 'Enter' && !event.isComposing) {
			const hit = rows[active];
			// With no active hit, the form submits to the search page.
			if (!hit) return;
			event.preventDefault();
			close();
			await goto(`${base}/${hit.route}`);
		} else if (event.key === 'Escape' && !event.isComposing && query !== '') {
			// Chromium and WebKit spend the first Escape clearing a search field, so the
			// dialog's cancel event only comes on the second. Close on the first.
			event.preventDefault();
			close();
		}
	}

	function onWindowKeydown(event: KeyboardEvent) {
		if (!isSearchShortcut(event)) return;
		event.preventDefault();
		if (dialog?.open) {
			input?.focus();
			input?.select();
		} else {
			open();
		}
	}
</script>

<svelte:window onkeydown={onWindowKeydown} />

<!-- A click on the dialog element itself is a click on the backdrop; a press that starts in
	the box and ends outside it doesn't count. Escape closes it through the native cancel
	event, or through onkeydown when the search field has text. -->
<dialog
	class="search-dialog"
	aria-label="Search the docs"
	bind:this={dialog}
	{onclose}
	onpointerdown={(event) => (pressedBackdrop = event.target === dialog)}
	onclick={(event) => {
		if (pressedBackdrop && event.target === dialog) close();
	}}
>
	<form class="search-dialog-form" method="GET" action="{base}/search" onsubmit={close}>
		<input
			bind:this={input}
			bind:value={query}
			type="search"
			name="q"
			autocomplete="off"
			spellcheck="false"
			placeholder="Search the docs"
			aria-label="Search the docs"
			role="combobox"
			aria-autocomplete="list"
			aria-controls="search-dialog-results"
			aria-expanded={hits.length > 0}
			aria-activedescendant={active >= 0 ? `${idPrefix}-${active}` : undefined}
			{onkeydown}
		/>
	</form>
	<div class="search-dialog-body" bind:this={body}>
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
		<div id="search-dialog-results" role="listbox" aria-label="Search results">
			<SearchResults {groups} {query} {active} {idPrefix} onchoose={close} />
		</div>
	</div>
	<div class="search-dialog-footer">
		<span class="search-dialog-keys">
			<span><kbd>↑</kbd> <kbd>↓</kbd> to move</span>
			<span><kbd>↵</kbd> to open</span>
			<span><kbd>esc</kbd> to close</span>
		</span>
		<a
			class="search-dialog-all"
			href={allResults}
			onclick={(event) => {
				if (isPlainClick(event)) close();
			}}>See all results</a
		>
	</div>
</dialog>
