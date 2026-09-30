<script lang="ts">
	let {
		class: className = '',
		size = 24,
		title = null
	}: {
		class?: string;
		size?: number;
		/** Accessible name; without one the mark is decorative. */
		title?: string | null;
	} = $props();
</script>

<svg
	class={className}
	width={size}
	height={size}
	viewBox="0 0 40 40"
	xmlns="http://www.w3.org/2000/svg"
	role={title ? 'img' : undefined}
	aria-label={title ?? undefined}
	aria-hidden={title ? undefined : 'true'}
>
	<circle cx="20" cy="20" r="8" fill="currentColor" />
	<circle cx="20" cy="20" r="14" fill="none" stroke="currentColor" stroke-width="2" opacity="0.8" />
	<circle cx="31" cy="9.5" r="3" fill="currentColor" />
</svg>
