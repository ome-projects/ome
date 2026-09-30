<script lang="ts">
	import { cubicOut } from 'svelte/easing';

	let {
		prefix = '',
		value,
		suffix = '',
		duration = 1600
	}: {
		prefix?: string;
		value: number;
		suffix?: string;
		duration?: number;
	} = $props();

	let root: HTMLSpanElement | undefined = $state();
	let progress = $state(0);
	let reducedMotion = $state(false);
	let hasStarted = false;

	const label = $derived(`${prefix}${value}${suffix}`);
	const done = $derived(progress >= 1);

	const displayValue = $derived.by(() => {
		if (reducedMotion || done) return value;
		return value * cubicOut(progress);
	});

	const visibleDigits = $derived.by(() => {
		if (done || reducedMotion) return String(value).split('').map(Number);
		const current = Math.floor(displayValue);
		return String(current).split('').map(Number);
	});

	const digitOffsets = $derived.by(() => {
		const fraction = displayValue - Math.floor(displayValue);

		return visibleDigits.map((digit, index) => {
			if (done || reducedMotion) return -digit * 10;

			const isLast = index === visibleDigits.length - 1;
			const amount = digit + (isLast ? fraction : 0);
			return -amount * 10;
		});
	});

	$effect(() => {
		if (!root) return;

		reducedMotion =
			typeof window !== 'undefined' &&
			window.matchMedia('(prefers-reduced-motion: reduce)').matches;

		const observer = new IntersectionObserver(
			([entry]) => {
				if (!entry?.isIntersecting || hasStarted) return;
				hasStarted = true;
				observer.disconnect();

				if (reducedMotion) {
					progress = 1;
					return;
				}

				const start = performance.now();
				const tick = (now: number) => {
					progress = Math.min(1, (now - start) / duration);
					if (progress < 1) requestAnimationFrame(tick);
				};
				requestAnimationFrame(tick);
			},
			{ threshold: 0.35, rootMargin: '0px 0px -8% 0px' }
		);

		observer.observe(root);
		return () => observer.disconnect();
	});
</script>

<span class="home-metric-value" bind:this={root}>
	<!-- The digits roll up from zero, so screen readers get the final value instead. -->
	<span class="visually-hidden">{label}</span>

	{#if prefix}
		<span class="home-metric-value-affix home-metric-value-prefix" aria-hidden="true">{prefix}</span
		>
	{/if}

	<span class="home-metric-value-digits" aria-hidden="true">
		{#each visibleDigits as digit, index (visibleDigits.length + '-' + index + '-' + digit)}
			<span class="home-metric-value-digit">
				<span
					class="home-metric-value-digit-strip"
					class:home-metric-value-digit-strip--done={done || reducedMotion}
					style:transform="translateY({digitOffsets[index]}%)"
				>
					{#each Array.from({ length: 10 }, (_, n) => n) as num (num)}
						<span class="home-metric-value-digit-num">{num}</span>
					{/each}
				</span>
			</span>
		{/each}
	</span>

	{#if suffix}
		<span class="home-metric-value-affix home-metric-value-suffix" aria-hidden="true">{suffix}</span
		>
	{/if}
</span>
