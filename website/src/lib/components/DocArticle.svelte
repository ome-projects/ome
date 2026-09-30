<script lang="ts">
	import Breadcrumbs from '$lib/components/Breadcrumbs.svelte';
	import PageNav from '$lib/components/PageNav.svelte';
	import type { DocPageData } from '$lib/docs/page-data';
	import { copyCode } from '$lib/ui/copy-code';

	let { data }: { data: DocPageData } = $props();
</script>

<article class="doc-article">
	<div class="doc-article-head">
		<Breadcrumbs tree={data.tree} position={data.position} route={data.page.route} />
		<div class="doc-toolbar">
			{#if data.editUrl}
				<a
					class="doc-toolbar-btn"
					href={data.editUrl}
					target="_blank"
					rel="noopener noreferrer"
					title="Edit this page on GitHub"
					aria-label="Edit this page on GitHub"
				>
					<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" aria-hidden="true">
						<path
							d="M10 20H6V4h7v5h5v3.1l2-2V8l-6-6H6c-1.1 0-2 .9-2 2v16c0 1.1.9 2 2 2h4zm10.2-7c.1 0 .3.1.4.2l1.3 1.3c.2.2.2.6 0 .8l-1 1-2.1-2.1 1-1c.1-.1.2-.2.4-.2m0 3.9L14.1 23H12v-2.1l6.1-6.1z"
						/>
					</svg>
				</a>
			{/if}
			<a
				class="doc-toolbar-btn"
				href={data.sourceUrl}
				target="_blank"
				rel="noopener noreferrer"
				title="View Markdown source on GitHub"
				aria-label="View Markdown source on GitHub"
			>
				<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" aria-hidden="true">
					<path
						d="M17 18c.56 0 1 .44 1 1s-.44 1-1 1-1-.44-1-1 .44-1 1-1m0-3c-2.73 0-5.06 1.66-6 4 .94 2.34 3.27 4 6 4s5.06-1.66 6-4c-.94-2.34-3.27-4-6-4m0 6.5a2.5 2.5 0 0 1-2.5-2.5 2.5 2.5 0 0 1 2.5-2.5 2.5 2.5 0 0 1 2.5 2.5 2.5 2.5 0 0 1-2.5 2.5M9.27 20H6V4h7v5h5v4.07c.7.08 1.36.25 2 .49V8l-6-6H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h4.5a8.2 8.2 0 0 1-1.23-2"
					/>
				</svg>
			</a>
		</div>
	</div>

	<h1>
		{data.page.title}
		{#if data.page.status === 'preview'}
			<span class="doc-status-badge">Preview</span>
		{/if}
		{#if data.since}
			<span class="doc-since doc-since--{data.since.state}">{data.since.label}</span>
		{/if}
	</h1>
	<p class="doc-lead">{data.description}</p>

	<!-- Every preview page is about multi-cluster routing; see the spec's Preview pages. -->
	{#if data.page.status === 'preview'}
		<aside class="doc-admonition doc-admonition--preview">
			<p class="doc-admonition-title">In development</p>
			<p>Multi-cluster routing is alpha. Fields and behavior can change between releases.</p>
		</aside>
	{/if}

	{#if data.page.status === 'draft'}
		<aside class="doc-draft">
			<p class="doc-draft-title">This page is being rewritten</p>
			{#if data.replaces.length > 0}
				<p>Until it's done, the current docs cover this topic:</p>
				<ul>
					{#each data.replaces as replaced (replaced.old)}
						<li><a href={replaced.url}>{replaced.url.replace(/^https:\/\//, '')}</a></li>
					{/each}
				</ul>
			{:else}
				<p>This is a new page, and it hasn't been written yet.</p>
			{/if}
		</aside>
	{:else}
		<div class="doc-body" use:copyCode>
			<!-- eslint-disable-next-line svelte/no-at-html-tags -- rendered at build time from this repo's own Markdown -->
			{@html data.html}
		</div>
	{/if}

	<PageNav position={data.position} />
</article>
