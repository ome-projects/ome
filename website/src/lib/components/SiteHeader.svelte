<script lang="ts">
	import { base } from '$app/paths';
	import { page } from '$app/stores';
	import { slide, fade } from 'svelte/transition';
	import { cubicOut } from 'svelte/easing';
	import { headerNavItems } from '$lib/config/header';
	import OrbitMark from '$lib/components/OrbitMark.svelte';
	import { site } from '$lib/config/site';
	import GitHubBadge from '$lib/components/GitHubBadge.svelte';
	import SearchButton from '$lib/components/SearchButton.svelte';
	import SearchDialog from '$lib/components/SearchDialog.svelte';
	import type { GitHubRepoStats } from '$lib/server/github';
	import { heroShaderEnergy } from '$lib/stores/hero-shader';
	import { focusLeaves, headerOnDark, navItemActive, overDark, stickyOffset } from '$lib/ui/header';

	let { github }: { github: GitHubRepoStats } = $props();

	// As the hero shader lightens the top of the hero, white header text fades into it,
	// and cobalt text doesn't read until the top is nearly white. So partway up, the
	// header takes its frosted backing and cobalt text together.
	const SHADER_BACKING_ENERGY = 0.4;

	let headerEl = $state<HTMLElement | null>(null);
	let brandEl = $state<HTMLElement | null>(null);
	let menuToggle = $state<HTMLButtonElement | null>(null);
	let searchDialog = $state<SearchDialog | null>(null);
	let scrolled = $state(false);
	let onDark = $state(false);
	let menuOpen = $state(false);
	let hidden = $state(false);
	let headerHeight = $state(0);

	const isHome = $derived($page.route.id === '/');
	const solid = $derived(!isHome || scrolled);
	// The open menu fills the header with cobalt on the hero, so it needs no backing.
	const backed = $derived(
		isHome && !solid && !menuOpen && $heroShaderEnergy >= SHADER_BACKING_ENERGY
	);

	// SvelteKit sets page.error while it shows +error.svelte, such as the 404 page.
	const errorPage = $derived($page.error !== null);

	function closeMenu() {
		menuOpen = false;
	}

	// The menu covers the page, so once focus moves past its last link (or before the
	// brand) the menu closes rather than leave focus on content hidden behind it.
	function closeMenuOnFocusLeave(event: FocusEvent) {
		if (menuOpen && headerEl && focusLeaves(headerEl, event.relatedTarget as Node | null)) {
			menuOpen = false;
		}
	}

	// The menu's Search link goes away with the menu, so the dialog returns focus to the
	// menu button instead.
	function openSearchFromMenu() {
		menuToggle?.focus();
		searchDialog?.open();
	}

	// Close the mobile menu whenever the route changes.
	$effect(() => {
		void $page.url.pathname;
		menuOpen = false;
	});

	// Close on Escape while the menu is open.
	$effect(() => {
		if (!menuOpen) return;
		const onKey = (event: KeyboardEvent) => {
			if (event.key === 'Escape') menuOpen = false;
		};
		window.addEventListener('keydown', onKey);
		return () => window.removeEventListener('keydown', onKey);
	});

	$effect(() => {
		let lastY = window.scrollY;
		let ticking = false;

		const update = () => {
			const y = window.scrollY;
			scrolled = y > 16;

			const delta = y - lastY;
			// Hide on downward scroll past the header, reveal on upward scroll.
			if (!menuOpen && Math.abs(delta) > 6) {
				if (delta > 0 && y > 96) hidden = true;
				else if (delta < 0) hidden = false;
			}
			lastY = y;
			ticking = false;
		};

		const onScroll = () => {
			if (!ticking) {
				ticking = true;
				requestAnimationFrame(update);
			}
		};

		update();
		window.addEventListener('scroll', onScroll, { passive: true });
		return () => window.removeEventListener('scroll', onScroll);
	});

	// Keep the header visible whenever the mobile menu is open.
	$effect(() => {
		if (menuOpen) hidden = false;
	});

	$effect(() => {
		if (!isHome || !brandEl) {
			onDark = false;
			return;
		}

		const darkSections = () =>
			[...document.querySelectorAll('.home-choose-path, .site-footer')] as HTMLElement[];

		const updateOnDark = () => {
			// The header is stuck to the top of the viewport, so the brand's offset within
			// it is the nav row's position on screen, even while the header is slid away.
			const navRowMiddle = brandEl!.offsetTop + brandEl!.offsetHeight / 2;
			onDark = overDark(
				navRowMiddle,
				darkSections().map((section) => section.getBoundingClientRect())
			);
		};

		updateOnDark();
		window.addEventListener('scroll', updateOnDark, { passive: true });
		window.addEventListener('resize', updateOnDark);

		return () => {
			window.removeEventListener('scroll', updateOnDark);
			window.removeEventListener('resize', updateOnDark);
			onDark = false;
		};
	});

	$effect(() => {
		if (!headerEl) return;

		const measure = () => {
			headerHeight = headerEl!.offsetHeight;
		};

		measure();
		const observer = new ResizeObserver(measure);
		observer.observe(headerEl);
		return () => observer.disconnect();
	});

	// The height keeps anchor jumps and keyboard focus below the header. The offset
	// places the sticky sidebar and table of contents, which move up while the header
	// is hidden.
	$effect(() => {
		if (headerHeight === 0) return;
		const root = document.documentElement.style;
		root.setProperty('--site-header-height', `${headerHeight}px`);
		root.setProperty('--site-header-offset', `${stickyOffset(headerHeight, hidden, menuOpen)}px`);
	});
</script>

<header
	class="site-header"
	class:site-header--solid={solid}
	class:site-header--backed={backed}
	class:site-header--on-dark={headerOnDark(onDark, menuOpen)}
	class:site-header--menu-open={menuOpen}
	class:site-header--hidden={hidden}
	bind:this={headerEl}
	onfocusout={closeMenuOnFocusLeave}
>
	<div class="site-header-inner">
		<a class="site-brand" href="{base}/" onclick={closeMenu} bind:this={brandEl}>
			<OrbitMark size={24} />
			<span class="site-brand-name">{site.name}</span>
		</a>

		<div class="site-header-right">
			<nav class="site-nav-pill" aria-label="Primary">
				<a
					class="site-nav-symbol"
					href={github.url}
					aria-label="Go to GitHub repository"
					target="_blank"
					rel="noopener noreferrer"
				>
					<OrbitMark size={18} />
				</a>
				<ul class="site-nav-links">
					{#each headerNavItems as item (item)}
						<li>
							<a
								href={item.href}
								class:active={navItemActive(item.href, $page.url.pathname, errorPage)}
							>
								{item.label}
							</a>
						</li>
					{/each}
					<li><SearchButton variant="pill" onopen={() => searchDialog?.open()} /></li>
				</ul>
			</nav>
			<GitHubBadge {github} />
		</div>

		<button
			bind:this={menuToggle}
			type="button"
			class="site-menu-toggle"
			aria-label={menuOpen ? 'Close menu' : 'Open menu'}
			aria-expanded={menuOpen}
			aria-controls="site-mobile-menu"
			onclick={() => (menuOpen = !menuOpen)}
		>
			<span class="site-menu-toggle-icon" class:is-open={menuOpen} aria-hidden="true">
				<span></span>
				<span></span>
				<span></span>
			</span>
		</button>
	</div>

	<SearchDialog bind:this={searchDialog} />

	{#if menuOpen}
		<button
			type="button"
			class="site-menu-backdrop"
			aria-label="Close menu"
			tabindex="-1"
			onclick={closeMenu}
			transition:fade={{ duration: 180 }}
		></button>

		<div
			id="site-mobile-menu"
			class="site-mobile-menu"
			transition:slide={{ duration: 220, easing: cubicOut }}
		>
			<nav class="site-mobile-nav" aria-label="Mobile">
				<ul class="site-mobile-links">
					{#each headerNavItems as item (item)}
						<li>
							<a
								href={item.href}
								class:active={navItemActive(item.href, $page.url.pathname, errorPage)}
								onclick={closeMenu}
							>
								{item.label}
							</a>
						</li>
					{/each}
					<li>
						<SearchButton variant="menu" onclick={closeMenu} onopen={openSearchFromMenu} />
					</li>
				</ul>
			</nav>

			<a
				class="site-mobile-repo"
				href={github.url}
				target="_blank"
				rel="noopener noreferrer"
				onclick={closeMenu}
			>
				<OrbitMark size={18} />
				<span class="site-mobile-repo-name">{github.fullName}</span>
				{#if github.stars !== null}
					<span class="site-mobile-repo-stats">&#9733; {github.stars}</span>
				{/if}
			</a>
		</div>
	{/if}
</header>
