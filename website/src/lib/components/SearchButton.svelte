<script lang="ts">
	import { onMount } from 'svelte';
	import { base } from '$app/paths';
	import { isPlainClick, shortcutLabel } from '$lib/ui/search-keys';

	let {
		onopen,
		variant,
		onclick
	}: { onopen: () => void; variant: 'pill' | 'menu'; onclick?: () => void } = $props();

	// The server doesn't know the platform, so it renders the Apple label and the browser
	// corrects it once mounted.
	let shortcut = $state('⌘K');
	onMount(() => {
		shortcut = shortcutLabel(navigator.platform);
	});

	function handleClick(event: MouseEvent) {
		// Leave modified clicks to the browser, so they can open the search page in a new tab.
		if (!isPlainClick(event)) return;
		event.preventDefault();
		onclick?.();
		onopen();
	}
</script>

<a
	href="{base}/search"
	class="site-search-trigger"
	aria-keyshortcuts="Meta+K Control+K"
	onclick={handleClick}
>
	Search
	{#if variant === 'pill'}
		<kbd aria-hidden="true">{shortcut}</kbd>
	{/if}
</a>
