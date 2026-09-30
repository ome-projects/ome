import { gsap } from 'gsap';
import { ScrollTrigger } from 'gsap/ScrollTrigger';

let registered = false;

function ensureGsap() {
	if (registered) return;
	gsap.registerPlugin(ScrollTrigger);
	registered = true;
}

export type ScrollRevealOptions = {
	/** 'scroll' = ScrollTrigger, 'enter' = play on mount */
	mode?: 'scroll' | 'enter';
	y?: number;
	duration?: number;
	delay?: number;
	stagger?: number;
	/** CSS selector for child elements to stagger; omit to animate the root node */
	children?: string;
	start?: string;
	once?: boolean;
};

const defaults: Required<ScrollRevealOptions> = {
	mode: 'scroll',
	y: 40,
	duration: 0.9,
	delay: 0,
	stagger: 0.1,
	children: '',
	start: 'top 86%',
	once: true
};

function resolveTargets(node: HTMLElement, children?: string) {
	if (children) {
		const targets = [...node.querySelectorAll<HTMLElement>(children)];
		return targets.length > 0 ? targets : [node];
	}
	return [node];
}

function animateIn(node: HTMLElement, options: ScrollRevealOptions): gsap.core.Tween {
	ensureGsap();

	const opts = { ...defaults, ...options };
	const targets = resolveTargets(node, opts.children || undefined);

	gsap.set(targets, { opacity: 0, y: opts.y, willChange: 'opacity, transform' });

	return gsap.to(targets, {
		opacity: 1,
		y: 0,
		duration: opts.duration,
		delay: opts.delay,
		stagger: opts.stagger,
		ease: 'power3.out',
		clearProps: 'willChange',
		scrollTrigger:
			opts.mode === 'scroll'
				? {
						trigger: node,
						start: opts.start,
						once: opts.once
					}
				: undefined,
		onComplete: () => {
			gsap.set(targets, { clearProps: 'opacity,transform' });
		}
	});
}

function cleanup(tween: gsap.core.Tween | null) {
	tween?.scrollTrigger?.kill();
	tween?.kill();
}

export function scrollReveal(node: HTMLElement, options: ScrollRevealOptions = {}) {
	if (typeof window === 'undefined') {
		return { update() {}, destroy() {} };
	}

	if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
		return { update() {}, destroy() {} };
	}

	let tween: gsap.core.Tween | null = null;

	function mount(opts: ScrollRevealOptions) {
		cleanup(tween);
		tween = animateIn(node, opts);
	}

	mount(options);

	return {
		update(opts: ScrollRevealOptions) {
			mount(opts);
		},
		destroy() {
			cleanup(tween);
			tween = null;
		}
	};
}
