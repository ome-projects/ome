<script lang="ts">
	import { base } from '$app/paths';
	import OrbitMark from '$lib/components/OrbitMark.svelte';
	import { repoUrl, site } from '$lib/config/site';

	const year = new Date().getFullYear();

	const links = [
		{ label: 'GitHub', href: repoUrl, external: true },
		{ label: 'Issues', href: `${repoUrl}/issues`, external: true },
		{ label: 'Contributing', href: `${base}/contributing`, external: false }
	] as const;
</script>

<footer class="site-footer">
	<div class="site-footer-inner">
		<div class="site-footer-brand-block">
			<a href="{base}/" class="site-footer-brand" aria-label="{site.name} home">
				<OrbitMark class="site-footer-mark" size={28} />
			</a>
			<p class="site-footer-name">{site.longName}</p>
		</div>

		<div class="site-footer-meta">
			<p class="site-footer-copy">© {year} OME Contributors</p>

			<ul class="site-footer-links">
				{#each links as link (link.label)}
					<li>
						<a
							class="site-footer-link"
							href={link.href}
							target={link.external ? '_blank' : undefined}
							rel={link.external ? 'noopener noreferrer' : undefined}
						>
							{link.label}
						</a>
					</li>
				{/each}
			</ul>
		</div>
	</div>
</footer>
