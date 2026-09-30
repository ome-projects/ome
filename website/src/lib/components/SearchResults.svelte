<script lang="ts">
	import { base } from '$app/paths';
	import type { HitGroup } from '$lib/ui/group-hits';
	import { highlight } from '$lib/ui/highlight';
	import { isPlainClick } from '$lib/ui/search-keys';

	let {
		groups,
		query,
		active = -1,
		idPrefix,
		listbox = true,
		onchoose
	}: {
		groups: HitGroup[];
		query: string;
		active?: number;
		idPrefix: string;
		/** Rows are options of a listbox that the search field controls, so they take no focus. */
		listbox?: boolean;
		onchoose?: () => void;
	} = $props();

	// Each group's first row, so row ids and the active row count across groups.
	const offsets = $derived.by(() => {
		let next = 0;
		return groups.map((group) => {
			const start = next;
			next += group.hits.length;
			return start;
		});
	});

	const statusLabels = { preview: 'Preview', draft: 'Draft' } as const;
</script>

{#snippet marked(text: string)}
	{#each highlight(text, query) as segment, index (index)}
		{#if segment.match}<mark>{segment.text}</mark>{:else}{segment.text}{/if}
	{/each}
{/snippet}

{#each groups as group, g (group.label)}
	<div class="search-group" role="group" aria-labelledby="{idPrefix}-group-{g}">
		<p class="search-group-label" id="{idPrefix}-group-{g}">{group.label}</p>
		<ul role={listbox ? 'presentation' : undefined}>
			{#each group.hits as hit, h (hit.route)}
				{@const i = offsets[g] + h}
				{@const detail = [hit.group, hit.description].filter(Boolean).join(' · ')}
				<li
					role={listbox ? 'option' : undefined}
					id={listbox ? `${idPrefix}-${i}` : undefined}
					aria-selected={listbox ? i === active : undefined}
				>
					<a
						class="search-hit"
						class:active={i === active}
						href="{base}/{hit.route}"
						tabindex={listbox ? -1 : undefined}
						onclick={(event) => {
							if (isPlainClick(event)) onchoose?.();
						}}
					>
						<span class="search-hit-title">{@render marked(hit.title)}</span>
						{#if hit.status}
							<span class="search-hit-status search-hit-status--{hit.status}"
								>{statusLabels[hit.status]}</span
							>
						{/if}
						{#if detail}
							<span class="search-hit-detail">{@render marked(detail)}</span>
						{/if}
					</a>
				</li>
			{/each}
		</ul>
	</div>
{/each}
