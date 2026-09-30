<script lang="ts">
	import DocArticle from '$lib/components/DocArticle.svelte';
	import DocsLayout from '$lib/components/DocsLayout.svelte';

	let { data } = $props();
</script>

<DocsLayout tree={data.tree} route={data.page.route} toc={data.page.toc}>
	<DocArticle {data} />
</DocsLayout>
