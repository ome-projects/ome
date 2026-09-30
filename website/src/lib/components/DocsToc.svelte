<script lang="ts">
	import type { TocEntry } from '$lib/docs/types';
	import { activeHeading } from '$lib/ui/active-heading';

	let { toc }: { toc: TocEntry[] } = $props();

	// Pages with hundreds of headings, such as the generated API reference,
	// list only their h2s; the full list would be longer than the page.
	const entries = $derived(toc.length > 40 ? toc.filter((entry) => entry.depth === 2) : toc);

	let active = $state(-1);

	$effect(() => {
		const ids = entries.map((entry) => entry.id);
		let frame = 0;

		const update = () => {
			frame = 0;
			const root = document.documentElement;
			const header = parseFloat(getComputedStyle(root).getPropertyValue('--site-header-height'));
			const atBottom = window.innerHeight + window.scrollY >= root.scrollHeight - 2;
			const tops = ids.map(
				(id) => document.getElementById(id)?.getBoundingClientRect().top ?? Infinity
			);
			active = activeHeading(tops, header + 24, atBottom);
		};

		const schedule = () => {
			if (frame === 0) frame = requestAnimationFrame(update);
		};

		update();
		window.addEventListener('scroll', schedule, { passive: true });
		window.addEventListener('resize', schedule, { passive: true });
		return () => {
			cancelAnimationFrame(frame);
			window.removeEventListener('scroll', schedule);
			window.removeEventListener('resize', schedule);
		};
	});
</script>

<nav class="docs-toc" aria-label="On this page">
	<p class="docs-toc-heading">On this page</p>
	<ul class="docs-toc-list">
		{#each entries as entry, index (entry.id)}
			<li class="docs-toc-item" class:docs-toc-item--h3={entry.depth === 3}>
				<a
					class="docs-toc-link"
					class:active={index === active}
					aria-current={index === active ? 'location' : undefined}
					href="#{entry.id}"
				>
					{entry.text}
				</a>
			</li>
		{/each}
	</ul>
</nav>
