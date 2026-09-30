<script lang="ts">
	import { base } from '$app/paths';
	import { scrollReveal } from '$lib/actions/scrollReveal';
	import HeroMark from '$lib/components/HeroMark.svelte';
	import HeroShaderBackground from '$lib/components/HeroShaderBackground.svelte';
	import { repoUrl } from '$lib/config/site';
	import { heroShaderActive, toggleHeroShader } from '$lib/stores/hero-shader';

	let {
		title,
		subtitle
	}: {
		title: string;
		subtitle: string;
	} = $props();
</script>

<section class="home-hero">
	<div class="home-hero-bg">
		<HeroShaderBackground active={$heroShaderActive} />
	</div>

	<div class="home-hero-mark">
		<button
			type="button"
			class="home-hero-mark-btn"
			class:is-active={$heroShaderActive}
			aria-label="Toggle ambient background effect"
			aria-pressed={$heroShaderActive}
			onclick={toggleHeroShader}
		>
			<HeroMark width={48} active={$heroShaderActive} />
		</button>
	</div>

	<div class="home-hero-inner">
		<div
			class="home-hero-content"
			use:scrollReveal={{
				mode: 'enter',
				children: '.home-hero-title, .home-hero-subtitle, .home-hero-btn',
				y: 32,
				duration: 1,
				delay: 0.15,
				stagger: 0.14
			}}
		>
			<h1 class="home-hero-title">{title}</h1>
			<p class="home-hero-subtitle">{subtitle}</p>
			<div class="home-hero-actions">
				<a class="home-hero-btn" href="{base}/getting-started">Get Started</a>
				<a
					class="home-hero-btn home-hero-btn--github"
					href={repoUrl}
					target="_blank"
					rel="noopener noreferrer"
				>
					View on GitHub
				</a>
			</div>
		</div>
	</div>
</section>
