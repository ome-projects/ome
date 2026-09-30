<script lang="ts">
	import { base } from '$app/paths';
	import { scrollReveal } from '$lib/actions/scrollReveal';
	import SectionLabel from '$lib/components/SectionLabel.svelte';

	// `logo` names an SVG in static/works-with/. Set it only once the project's brand or
	// trademark terms allow the logo here; until then the pill shows the project's name.
	// As on the SMG site, the logo sits inside the pill and the name goes under it; a wide
	// logo is a wordmark, a mark is a square symbol.
	type Project = {
		name: string;
		caption: string;
		href: string;
		logo?: string;
		logoKind?: 'wide' | 'mark';
		// For a mark whose artwork leaves a lot of empty canvas, so it reads as small.
		logoSize?: 'lg';
	};

	// The first row is what serves the model; the second is the Kubernetes, model and
	// storage ecosystem OME plugs into, drawn quieter. Five items per row, so both rows
	// fill the same width. The vLLM, TokenSpeed, SGLang, TensorRT-LLM and SMG files come
	// from the SMG docs site, which uses them the same way; the rest are the projects'
	// own artwork.
	const rows: { eco: boolean; projects: Project[] }[] = [
		{
			eco: false,
			projects: [
				{
					name: 'vLLM',
					caption: 'vLLM',
					href: 'https://vllm.ai',
					logo: 'vllm.svg',
					logoKind: 'wide'
				},
				{
					name: 'TokenSpeed',
					caption: 'TokenSpeed',
					href: 'https://github.com/lightseekorg/tokenspeed',
					logo: 'tokenspeed.svg',
					logoKind: 'mark'
				},
				{
					name: 'SGLang',
					caption: 'SGLang',
					href: 'https://github.com/sgl-project/sglang',
					logo: 'sglang.svg',
					logoKind: 'mark'
				},
				{
					name: 'TensorRT-LLM',
					caption: 'TensorRT-LLM',
					href: 'https://developer.nvidia.com/tensorrt-llm',
					logo: 'tensorrt-llm.svg',
					logoKind: 'mark'
				},
				{
					name: 'SMG',
					caption: 'SMG',
					href: 'https://lightseek.org/smg',
					logo: 'smg.svg',
					logoKind: 'mark'
				}
			]
		},
		{
			eco: true,
			projects: [
				{
					name: 'Hugging Face',
					caption: 'Hugging Face',
					href: 'https://huggingface.co',
					logo: 'huggingface.svg',
					logoKind: 'mark'
				},
				{
					name: 'OCI Object Storage',
					caption: 'OCI Object Storage',
					href: 'https://www.oracle.com/cloud/storage/object-storage/',
					logo: 'oracle.svg',
					logoKind: 'wide'
				},
				{
					name: 'Kueue',
					caption: 'Kueue',
					href: 'https://kueue.sigs.k8s.io',
					logo: 'kueue.svg',
					logoKind: 'mark',
					logoSize: 'lg'
				},
				{
					name: 'KEDA',
					caption: 'KEDA',
					href: 'https://keda.sh',
					logo: 'keda.svg',
					logoKind: 'mark',
					logoSize: 'lg'
				},
				{
					name: 'Gateway API',
					caption: 'Gateway API',
					href: 'https://gateway-api.sigs.k8s.io',
					logo: 'kubernetes.svg',
					logoKind: 'mark',
					logoSize: 'lg'
				}
			]
		}
	];

	// Starts with the visible name, so voice control can use it, then the caption if it differs.
	const accessibleName = (project: Project) =>
		project.caption === project.name ? project.name : `${project.name}, ${project.caption}`;
</script>

<section
	class="home-works-with"
	aria-label="Projects OME works with"
	use:scrollReveal={{
		children: '.home-works-with-label, .home-works-with-item',
		y: 36,
		stagger: 0.08,
		duration: 0.8
	}}
>
	<SectionLabel label="Works with" as="h2" class="home-works-with-label" />

	<ul class="home-works-with-grid">
		{#each rows as row (row.eco)}
			{#each row.projects as project (project.name)}
				<li class="home-works-with-item" class:home-works-with-item--eco={row.eco}>
					<a
						class="home-works-item"
						href={project.href}
						target="_blank"
						rel="noopener noreferrer"
						aria-label="{accessibleName(project)} (opens in new tab)"
					>
						<span class="home-works-pill" class:home-works-pill--eco={row.eco}>
							{#if project.logo}
								<!-- The link's label names the project, so the image needs no alt text. -->
								<img
									class="home-works-logo"
									class:home-works-logo--wide={project.logoKind === 'wide'}
									class:home-works-logo--mark={project.logoKind === 'mark'}
									class:home-works-logo--lg={project.logoSize === 'lg'}
									src="{base}/works-with/{project.logo}"
									alt=""
									width="120"
									height="32"
									loading="lazy"
									decoding="async"
								/>
							{:else}
								{project.name}
							{/if}
						</span>
						<!-- Under a logo the name always shows; under a text pill only a caption that
							differs from the name does. -->
						{#if project.logo || project.caption !== project.name}
							<span class="home-works-caption">{project.caption}</span>
						{/if}
					</a>
				</li>
			{/each}
		{/each}
	</ul>
</section>
