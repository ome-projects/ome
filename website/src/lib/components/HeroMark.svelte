<script lang="ts">
	let {
		class: className = '',
		width = 72,
		active = false
	}: {
		class?: string;
		width?: number;
		active?: boolean;
	} = $props();
</script>

<svg
	class={className}
	{width}
	height={width}
	viewBox="0 0 40 40"
	fill="none"
	xmlns="http://www.w3.org/2000/svg"
	aria-hidden="true"
>
	<defs>
		<!-- The regions reach past the 40x40 box so each blur has room to fade out. -->
		<filter
			id="ome-glow-idle"
			filterUnits="userSpaceOnUse"
			x="-15"
			y="-15"
			width="70"
			height="70"
			color-interpolation-filters="sRGB"
		>
			<feGaussianBlur in="SourceAlpha" stdDeviation="5" result="blur" />
			<feFlood flood-color="#ffffff" flood-opacity="0.5" result="color" />
			<feComposite in="color" in2="blur" operator="in" result="glow" />
			<feMerge>
				<feMergeNode in="glow" />
				<feMergeNode in="SourceGraphic" />
			</feMerge>
		</filter>

		<filter
			id="ome-glow-hover"
			filterUnits="userSpaceOnUse"
			x="-18"
			y="-18"
			width="76"
			height="76"
			color-interpolation-filters="sRGB"
		>
			<feGaussianBlur in="SourceAlpha" stdDeviation="7" result="blur" />
			<feFlood flood-color="#ffffff" flood-opacity="0.72" result="color" />
			<feComposite in="color" in2="blur" operator="in" result="glow" />
			<feMerge>
				<feMergeNode in="glow" />
				<feMergeNode in="SourceGraphic" />
			</feMerge>
		</filter>

		<filter
			id="ome-glow-active"
			filterUnits="userSpaceOnUse"
			x="-22"
			y="-22"
			width="84"
			height="84"
			color-interpolation-filters="sRGB"
		>
			<feGaussianBlur in="SourceAlpha" stdDeviation="6" result="blur-white">
				<animate attributeName="stdDeviation" values="5;8;5" dur="2.4s" repeatCount="indefinite" />
			</feGaussianBlur>
			<feFlood flood-color="#ffffff" flood-opacity="0.85" result="color-white" />
			<feComposite in="color-white" in2="blur-white" operator="in" result="glow-white" />
			<feGaussianBlur in="SourceAlpha" stdDeviation="14" result="blur-cool" />
			<feFlood flood-color="#9fb4ff" flood-opacity="0.45" result="color-cool" />
			<feComposite in="color-cool" in2="blur-cool" operator="in" result="glow-cool" />
			<feMerge>
				<feMergeNode in="glow-cool" />
				<feMergeNode in="glow-white" />
				<feMergeNode in="SourceGraphic" />
			</feMerge>
		</filter>
	</defs>

	<g class="mark-body" class:is-active={active}>
		<circle cx="20" cy="20" r="8" fill="currentColor" />
		<circle
			cx="20"
			cy="20"
			r="14"
			fill="none"
			stroke="currentColor"
			stroke-width="2"
			opacity="0.8"
		/>
		<circle cx="31" cy="9.5" r="3" fill="currentColor" />
	</g>
</svg>

<style>
	svg {
		display: block;
		overflow: visible;
		color: #ffffff;
	}

	.mark-body {
		filter: url(#ome-glow-idle);
		transition: color 0.5s ease;
	}

	.mark-body.is-active {
		color: var(--ome-accent);
		filter: url(#ome-glow-active);
	}

	:global(.home-hero-mark-btn:hover) .mark-body:not(.is-active) {
		filter: url(#ome-glow-hover);
	}

	@media (prefers-reduced-motion: reduce) {
		.mark-body.is-active {
			filter: url(#ome-glow-hover);
		}
	}
</style>
