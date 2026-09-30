<script lang="ts">
	import { base } from '$app/paths';
	import { untrack } from 'svelte';
	import { slide } from 'svelte/transition';
	import type { NavTree } from '$lib/docs/navigation';
	import { closedGroups, sidebarPageLabel } from '$lib/ui/sidebar';

	let { tree, route }: { tree: NavTree; route: string } = $props();

	const landingRoute = $derived(tree.landing?.route ?? tree.id);
	const pageLabel = $derived(sidebarPageLabel(tree, route));

	// In the compact layout the page list folds behind a toggle, so the
	// article leads the first screen. The list stays open only on the page
	// where it was opened: following one of its links folds it again. The
	// wide layout hides the toggle and always shows the list.
	let openOn = $state<string | null>(null);
	const open = $derived(openOn === route);

	// The wide layout's value is the same on the server and the client, so
	// hydration matches. A new Set is assigned on every change, because
	// mutating a Set in $state doesn't trigger updates.
	let closed = $state(untrack(() => closedGroups(tree, route, false)));

	const without = (labels: Set<string>, label: string) =>
		new Set([...labels].filter((other) => other !== label));

	// In the compact layout the list opens with only the current page's group
	// open. This runs once, on mount.
	$effect(() => {
		if (!window.matchMedia('(max-width: 900px)').matches) return;
		closed = untrack(() => closedGroups(tree, route, true));
	});

	// Following a previous or next link into a closed group opens it.
	$effect(() => {
		const label = tree.groups.find((group) =>
			group.links.some((link) => link.route === route)
		)?.label;
		if (!label) return;
		const current = untrack(() => closed);
		if (current.has(label)) closed = without(current, label);
	});

	function toggle(label: string) {
		closed = closed.has(label) ? without(closed, label) : new Set([...closed, label]);
	}

	function groupSlide(node: Element) {
		const reduce = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
		return slide(node, { duration: reduce ? 0 : 180 });
	}
</script>

{#snippet links(group: NavTree['groups'][number], id?: string)}
	<ul {id} class="docs-sidebar-links" transition:groupSlide>
		{#each group.links as link (link.route)}
			<li>
				<a
					class="docs-sidebar-link"
					class:active={link.route === route}
					aria-current={link.route === route ? 'page' : undefined}
					href="{base}/{link.route}"
				>
					{link.label}
					<!-- A preview group already carries the pill. -->
					{#if link.status === 'preview' && !group.preview}
						<span class="docs-sidebar-pill">preview</span>
					{/if}
				</a>
			</li>
		{/each}
	</ul>
{/snippet}

<nav class="docs-sidebar" class:docs-sidebar--open={open} aria-label="{tree.label} pages">
	<button
		type="button"
		class="docs-sidebar-toggle"
		aria-expanded={open}
		aria-controls="docs-sidebar-sections"
		onclick={() => (openOn = open ? null : route)}
	>
		<span class="docs-sidebar-toggle-section">{tree.label}</span>
		{#if pageLabel}
			<span class="docs-sidebar-toggle-page">
				<span class="visually-hidden">, current page:</span>
				{pageLabel}
			</span>
		{/if}
	</button>
	<ul id="docs-sidebar-sections" class="docs-sidebar-sections">
		<li class="docs-sidebar-section">
			<a
				class="docs-sidebar-link docs-sidebar-link--index"
				class:active={landingRoute === route}
				aria-current={landingRoute === route ? 'page' : undefined}
				href="{base}/{landingRoute}"
			>
				{tree.label}
			</a>
		</li>
		{#each tree.groups as group, index (group.label ?? `group-${index}`)}
			<li class="docs-sidebar-section">
				{#if group.label === null}
					{@render links(group)}
				{:else}
					{@const label = group.label}
					{@const id = `docs-sidebar-group-${index}`}
					<button
						type="button"
						class="docs-sidebar-heading"
						aria-expanded={!closed.has(label)}
						aria-controls={id}
						onclick={() => toggle(label)}
					>
						<span class="docs-sidebar-heading-label">
							{label}
							{#if group.preview}
								<span class="docs-sidebar-pill">preview</span>
							{/if}
						</span>
					</button>
					{#if !closed.has(label)}
						{@render links(group, id)}
					{/if}
				{/if}
			</li>
		{/each}
	</ul>
</nav>
