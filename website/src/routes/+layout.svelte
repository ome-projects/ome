<script lang="ts">
	import '../app.css';
	import '../styles/docs.css';
	import '../styles/home.css';
	import '../styles/search.css';
	import { onNavigate } from '$app/navigation';
	import { page } from '$app/stores';
	import SiteFooter from '$lib/components/SiteFooter.svelte';
	import SiteHeader from '$lib/components/SiteHeader.svelte';
	import { site } from '$lib/config/site';
	import { heroShaderActive, heroShaderEnergy } from '$lib/stores/hero-shader';
	import type { LayoutData } from './$types';

	onNavigate((navigation) => {
		if (typeof document === 'undefined') return;
		if (!document.startViewTransition) return;
		if (navigation.willUnload) return;
		if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) return;

		// The home hero is full-viewport with a negative margin; morphing main's bounds looks like the hero sliding up.
		if (navigation.from?.route.id === '/' || navigation.to?.route.id === '/') return;

		return new Promise<void>((resolve) => {
			document.startViewTransition(async () => {
				resolve();
				await navigation.complete;
			});
		});
	});

	$effect(() => {
		if ($page.route.id !== '/') {
			heroShaderActive.set(false);
			heroShaderEnergy.set(0);
		}
	});

	// Pages set `title` and `description` in their load data.
	const pageTitle = $derived.by(() => {
		if ($page.error) {
			return `${$page.status === 404 ? 'Page not found' : 'Something went wrong'} · ${site.name}`;
		}
		const title: string | undefined = $page.data.title;
		return title ? `${title} · ${site.name}` : `${site.name} · ${site.longName}`;
	});
	const description = $derived<string>($page.data.description ?? site.description);

	let { data, children }: { data: LayoutData; children: import('svelte').Snippet } = $props();

	const isHome = $derived($page.route.id === '/');
</script>

<svelte:head>
	<title>{pageTitle}</title>
	<meta name="description" content={description} />
</svelte:head>

<div
	class="site-shell"
	class:site-shell--home={isHome}
	style:--hero-shader-energy={isHome ? $heroShaderEnergy : 0}
>
	<SiteHeader github={data.github} />
	<main class="site-main" class:site-main--docs={!isHome}>
		{@render children()}
	</main>
	<SiteFooter />
</div>
