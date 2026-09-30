<script lang="ts">
	let {
		label,
		as = 'div',
		id,
		markSize = 6,
		class: className = ''
	}: {
		label: string;
		as?: keyof HTMLElementTagNameMap;
		id?: string;
		markSize?: number;
		class?: string;
	} = $props();
</script>

<svelte:element this={as} class="section-label {className}" {id}>
	<span
		class="section-label-mark"
		style:width="{markSize}px"
		style:height="{markSize}px"
		aria-hidden="true"
	></span>
	<span class="section-label-text">{label}</span>
</svelte:element>
