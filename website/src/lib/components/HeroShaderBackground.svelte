<script lang="ts">
	import { onMount } from 'svelte';
	import { heroShaderEnergy } from '$lib/stores/hero-shader';

	let { active = false }: { active?: boolean } = $props();

	let canvas: HTMLCanvasElement | undefined = $state();

	const runtime = {
		active: false,
		raf: 0,
		startLoop: null as (() => void) | null
	};

	$effect(() => {
		runtime.active = active;
		if (active) {
			runtime.startLoop?.();
		}
	});

	const vertexSource = `
		attribute vec2 a_position;
		void main() {
			gl_Position = vec4(a_position, 0.0, 1.0);
		}
	`;

	const fragmentSource = `
		precision mediump float;

		uniform vec2 u_resolution;
		uniform float u_time;
		uniform float u_energy;

		vec3 figmaGradient(
			float y,
			float midStart,
			float midEnd,
			float whiteStart
		) {
			vec3 accent = vec3(0.141, 0.278, 0.722);
			vec3 mid = vec3(0.839, 0.855, 0.890);
			vec3 white = vec3(1.0);
			vec3 col = accent;
			col = mix(col, mid, smoothstep(midStart, midEnd, y));
			col = mix(col, white, smoothstep(whiteStart, 1.0, y));
			return col;
		}

		void main() {
			vec2 uv = gl_FragCoord.xy / u_resolution;
			float t = u_time;
			float e = u_energy;

			float y = uv.y * 1.12 - 0.06 * (1.0 - uv.y);
			float drift = sin(t * 0.11) * 0.04;
			y += drift * (0.25 + 0.75 * e);

			float bend = sin(uv.x * 1.2 + t * 0.1) * 0.009 * e;
			float swell = sin(t * 0.075) * 0.014 * e;
			float glide = sin(uv.y * 1.6 - t * 0.085) * 0.006 * e;
			float yAnim = y + bend + swell + glide;

			float midStart = 0.63 + sin(t * 0.09) * 0.016 * e;
			float midEnd = 0.85 + cos(t * 0.08) * 0.012 * e;
			float whiteStart = 0.85 + sin(t * 0.07 + 1.2) * 0.01 * e;

			vec3 still = figmaGradient(y, 0.63, 0.85, 0.85);
			vec3 living = figmaGradient(yAnim, midStart, midEnd, whiteStart);
			vec3 col = mix(still, living, e);

			gl_FragColor = vec4(col, e);
		}
	`;

	function compileShader(gl: WebGLRenderingContext, type: number, source: string) {
		const shader = gl.createShader(type);
		if (!shader) return null;
		gl.shaderSource(shader, source);
		gl.compileShader(shader);
		if (!gl.getShaderParameter(shader, gl.COMPILE_STATUS)) {
			gl.deleteShader(shader);
			return null;
		}
		return shader;
	}

	onMount(() => {
		if (!canvas) return;

		const prefersReducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
		if (prefersReducedMotion) return;

		const gl = canvas.getContext('webgl', {
			alpha: true,
			antialias: false,
			premultipliedAlpha: false
		});
		if (!gl) return;

		gl.enable(gl.BLEND);
		gl.blendFunc(gl.SRC_ALPHA, gl.ONE_MINUS_SRC_ALPHA);

		const vertexShader = compileShader(gl, gl.VERTEX_SHADER, vertexSource);
		const fragmentShader = compileShader(gl, gl.FRAGMENT_SHADER, fragmentSource);
		if (!vertexShader || !fragmentShader) return;

		const program = gl.createProgram();
		if (!program) return;

		gl.attachShader(program, vertexShader);
		gl.attachShader(program, fragmentShader);
		gl.linkProgram(program);
		if (!gl.getProgramParameter(program, gl.LINK_STATUS)) return;

		const buffer = gl.createBuffer();
		gl.bindBuffer(gl.ARRAY_BUFFER, buffer);
		gl.bufferData(gl.ARRAY_BUFFER, new Float32Array([-1, -1, 1, -1, -1, 1, 1, 1]), gl.STATIC_DRAW);

		const positionLocation = gl.getAttribLocation(program, 'a_position');
		const resolutionLocation = gl.getUniformLocation(program, 'u_resolution');
		const timeLocation = gl.getUniformLocation(program, 'u_time');
		const energyLocation = gl.getUniformLocation(program, 'u_energy');

		let currentEnergy = 0;
		let sessionStart = 0;

		const host = canvas.parentElement;
		if (!host) return;

		const resize = () => {
			const dpr = Math.min(window.devicePixelRatio || 1, 2);
			const width = host.clientWidth;
			const height = host.clientHeight;
			if (width < 1 || height < 1) return;
			canvas!.width = Math.floor(width * dpr);
			canvas!.height = Math.floor(height * dpr);
			canvas!.style.width = `${width}px`;
			canvas!.style.height = `${height}px`;
			gl.viewport(0, 0, canvas!.width, canvas!.height);
		};

		const render = (now: number) => {
			const targetEnergy = runtime.active ? 1 : 0;
			const easing = runtime.active ? 0.009 : 0.014;
			currentEnergy += (targetEnergy - currentEnergy) * easing;

			if (!runtime.active && currentEnergy < 0.002) {
				currentEnergy = 0;
				heroShaderEnergy.set(0);
				canvas!.style.opacity = '0';
				runtime.raf = 0;
				return;
			}

			heroShaderEnergy.set(currentEnergy);

			canvas!.style.opacity = '1';

			if (runtime.active && sessionStart === 0) {
				sessionStart = now;
			}
			if (!runtime.active && currentEnergy < 0.01) {
				sessionStart = 0;
			}

			const t = sessionStart > 0 ? (now - sessionStart) * 0.001 : 0;

			gl.clearColor(0, 0, 0, 0);
			gl.clear(gl.COLOR_BUFFER_BIT);
			gl.useProgram(program);
			gl.bindBuffer(gl.ARRAY_BUFFER, buffer);
			gl.enableVertexAttribArray(positionLocation);
			gl.vertexAttribPointer(positionLocation, 2, gl.FLOAT, false, 0, 0);
			gl.uniform2f(resolutionLocation, canvas!.width, canvas!.height);
			gl.uniform1f(timeLocation, t);
			gl.uniform1f(energyLocation, currentEnergy);
			gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);

			runtime.raf = requestAnimationFrame(render);
		};

		const startLoop = () => {
			if (runtime.raf) return;
			runtime.raf = requestAnimationFrame(render);
		};

		runtime.startLoop = startLoop;

		const resizeObserver = new ResizeObserver(resize);

		resize();
		resizeObserver.observe(host);
		window.addEventListener('resize', resize);

		return () => {
			if (runtime.raf) cancelAnimationFrame(runtime.raf);
			runtime.raf = 0;
			runtime.startLoop = null;
			resizeObserver.disconnect();
			window.removeEventListener('resize', resize);
			gl.deleteProgram(program);
			gl.deleteShader(vertexShader);
			gl.deleteShader(fragmentShader);
			gl.deleteBuffer(buffer);
		};
	});
</script>

<canvas bind:this={canvas} class="hero-shader-bg" aria-hidden="true"></canvas>

<style>
	.hero-shader-bg {
		display: block;
		width: 100%;
		height: 100%;
		pointer-events: none;
		opacity: 0;
	}
</style>
