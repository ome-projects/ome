<script lang="ts">
	import { scrollReveal } from '$lib/actions/scrollReveal';
	import SectionLabel from '$lib/components/SectionLabel.svelte';

	// Eight living stamps, one per thing OME does. Each scene is a CSS loop whose
	// motion is the behaviour the tile describes, so recheck them against the docs
	// when OMENative updates, rollout groups, gang scheduling, the OME scheduler or
	// Alfred change. The OME scheduler and Alfred are alpha; take "(alpha)" out of
	// their labels when they graduate. The canary tile plays a prefill-decode
	// canary of four engines and two decoders: each step moves the same share of
	// both, metrics pass and it promotes, then a second pass fails and rolls back.
</script>

<section class="home-how" aria-labelledby="home-how-heading">
	<header class="home-how-header" use:scrollReveal={{ y: 28, duration: 0.75, start: 'top 90%' }}>
		<SectionLabel label="How it works" as="h2" id="home-how-heading" class="home-how-label" />
	</header>

	<div
		class="home-stamps"
		use:scrollReveal={{
			children: '.home-stamp',
			y: 32,
			stagger: 0.08,
			duration: 0.8,
			start: 'top 88%'
		}}
	>
		<article class="home-stamp">
			<h3 class="home-stamp-label">OMENative</h3>
			<div class="home-stamp-scene home-stamp-scene--instance" aria-hidden="true">
				<svg class="home-stamp-inst home-stamp-inst--old" viewBox="0 0 100 56">
					<rect
						x="2"
						y="2"
						width="96"
						height="52"
						rx="12"
						fill="none"
						stroke="currentColor"
						stroke-width="2.5"
					/>
					<line x1="40" y1="28" x2="58" y2="18" stroke="currentColor" stroke-width="2" />
					<line x1="40" y1="28" x2="58" y2="38" stroke="currentColor" stroke-width="2" />
					<circle
						class="home-stamp-part home-stamp-breathe"
						cx="30"
						cy="28"
						r="11"
						fill="currentColor"
					/>
					<circle
						class="home-stamp-part home-stamp-breathe home-stamp-breathe--d1"
						cx="64"
						cy="17"
						r="5.5"
						fill="currentColor"
					/>
					<circle
						class="home-stamp-part home-stamp-breathe home-stamp-breathe--d2"
						cx="64"
						cy="39"
						r="5.5"
						fill="currentColor"
					/>
				</svg>
				<svg class="home-stamp-inst home-stamp-inst--new" viewBox="0 0 100 56">
					<rect
						class="home-stamp-ink-stroke"
						x="2"
						y="2"
						width="96"
						height="52"
						rx="12"
						fill="none"
						stroke-width="2.5"
					/>
					<line class="home-stamp-ink-stroke" x1="40" y1="28" x2="58" y2="18" stroke-width="2" />
					<line class="home-stamp-ink-stroke" x1="40" y1="28" x2="58" y2="38" stroke-width="2" />
					<circle class="home-stamp-part home-stamp-ink-fill" cx="30" cy="28" r="11" />
					<circle
						class="home-stamp-part home-stamp-ink-fill home-stamp-breathe--d1"
						cx="64"
						cy="17"
						r="5.5"
					/>
					<circle
						class="home-stamp-part home-stamp-ink-fill home-stamp-breathe--d2"
						cx="64"
						cy="39"
						r="5.5"
					/>
				</svg>
				<span class="home-stamp-cap">new Instance up, then the old one drains</span>
			</div>
			<p class="home-stamp-text">
				Each replica is an Instance, a leader and its workers as one unit. The new one serves before
				the old one drains.
			</p>
		</article>

		<article class="home-stamp">
			<h3 class="home-stamp-label">Canary rollouts</h3>
			<div class="home-stamp-scene home-stamp-scene--canary" aria-hidden="true">
				<div class="home-stamp-wrap">
					<div class="home-stamp-bird">
						<svg viewBox="0 0 128 84">
							<g class="home-stamp-part home-stamp-bob">
								<ellipse cx="62" cy="46" rx="22" ry="16" fill="currentColor" />
								<circle cx="82" cy="34" r="11" fill="currentColor" />
								<polygon points="92,32 104,36 92,40" class="home-stamp-beak" />
								<circle cx="85" cy="31" r="2" fill="#ffffff" />
								<path class="home-stamp-flap" d="M52 44 Q 62 30, 76 42 Q 62 48, 52 44 Z" />
								<line x1="56" y1="62" x2="56" y2="70" class="home-stamp-legs" />
								<line x1="68" y1="62" x2="68" y2="70" class="home-stamp-legs" />
							</g>
						</svg>
					</div>
					<span class="home-stamp-note home-stamp-rowlbl home-stamp-rowlbl--pf">prefill</span>
					<span class="home-stamp-note home-stamp-rowlbl home-stamp-rowlbl--dc">decode</span>
					<span class="home-stamp-sq home-stamp-pf home-stamp-i0 home-stamp-k1"></span>
					<span class="home-stamp-sq home-stamp-pf home-stamp-i1 home-stamp-k2"></span>
					<span class="home-stamp-sq home-stamp-pf home-stamp-i2 home-stamp-k3"></span>
					<span class="home-stamp-sq home-stamp-pf home-stamp-i3 home-stamp-k3"></span>
					<span class="home-stamp-sq home-stamp-dc home-stamp-i0 home-stamp-k1"></span>
					<span class="home-stamp-sq home-stamp-dc home-stamp-i1 home-stamp-k3"></span>
					<svg class="home-stamp-mark home-stamp-mark--check" viewBox="0 0 26 26">
						<circle cx="13" cy="13" r="12" fill="currentColor" />
						<path
							d="M7 13.5 L 11 17.5 L 19 9"
							fill="none"
							stroke="#ffffff"
							stroke-width="2.5"
							stroke-linecap="round"
							stroke-linejoin="round"
						/>
					</svg>
					<svg class="home-stamp-mark home-stamp-mark--cross" viewBox="0 0 26 26">
						<circle cx="13" cy="13" r="12" fill="currentColor" />
						<path
							d="M8.5 8.5 L 17.5 17.5 M17.5 8.5 L 8.5 17.5"
							fill="none"
							stroke="#ffffff"
							stroke-width="2.5"
							stroke-linecap="round"
						/>
					</svg>
					<span class="home-stamp-cap home-stamp-lbl home-stamp-lbl--25"
						>25% of prefill and 25% of decode</span
					>
					<span class="home-stamp-cap home-stamp-lbl home-stamp-lbl--50"
						>50%, both ready, metrics pass</span
					>
					<span class="home-stamp-cap home-stamp-lbl home-stamp-lbl--promoted">promoted</span>
					<span class="home-stamp-cap home-stamp-lbl home-stamp-lbl--rolledback"
						>metrics failed: rolled back</span
					>
				</div>
			</div>
			<p class="home-stamp-text">
				A share of prefill and decode moves together, step by step. Metrics promote the rest, or
				roll back.
			</p>
		</article>

		<article class="home-stamp">
			<h3 class="home-stamp-label">Prefill and decode</h3>
			<div class="home-stamp-scene home-stamp-scene--pd" aria-hidden="true">
				<div class="home-stamp-wrap">
					<div class="home-stamp-box home-stamp-pdbox--pf">prefill</div>
					<div class="home-stamp-box home-stamp-pdbox--dc">decode</div>
					<span class="home-stamp-dot home-stamp-tok"></span>
					<span class="home-stamp-dot home-stamp-tok home-stamp-tok--d1"></span>
					<span class="home-stamp-dot home-stamp-tok home-stamp-tok--d2"></span>
				</div>
				<span class="home-stamp-cap">two InferenceReplicas, one rollout group</span>
			</div>
			<p class="home-stamp-text">
				Prefill in the engine, decode in the decoder. A router pairs them, and they roll out as one
				group.
			</p>
		</article>

		<article class="home-stamp">
			<h3 class="home-stamp-label">Gang scheduling</h3>
			<div class="home-stamp-scene home-stamp-scene--gang" aria-hidden="true">
				<div class="home-stamp-outline home-stamp-gang-outline"></div>
				<span class="home-stamp-dot home-stamp-gang home-stamp-gang--1"></span>
				<span class="home-stamp-dot home-stamp-gang home-stamp-gang--2"></span>
				<span class="home-stamp-dot home-stamp-gang home-stamp-gang--3"></span>
				<span class="home-stamp-dot home-stamp-gang home-stamp-gang--4"></span>
				<span class="home-stamp-cap">together, or not at all</span>
			</div>
			<p class="home-stamp-text">
				A leader and its workers land together or not at all. With the PodGroup CRD, no half-started
				gangs.
			</p>
		</article>

		<article class="home-stamp">
			<h3 class="home-stamp-label">The OME scheduler (alpha)</h3>
			<div class="home-stamp-scene home-stamp-scene--scheduler" aria-hidden="true">
				<div class="home-stamp-wrap">
					<div class="home-stamp-outline home-stamp-domain home-stamp-domain--1"></div>
					<div class="home-stamp-outline home-stamp-domain home-stamp-domain--2"></div>
					<span class="home-stamp-note home-stamp-domlbl home-stamp-domlbl--1">domain 1</span>
					<span class="home-stamp-note home-stamp-domlbl home-stamp-domlbl--2">domain 2</span>
					<span class="home-stamp-dot home-stamp-pack home-stamp-pack--1" style="--dx: 0px"></span>
					<span class="home-stamp-dot home-stamp-pack home-stamp-pack--2" style="--dx: 0px"></span>
					<span class="home-stamp-dot home-stamp-pack home-stamp-pack--3" style="--dx: 112px"
					></span>
					<span class="home-stamp-dot home-stamp-pack home-stamp-pack--4" style="--dx: 112px"
					></span>
				</div>
				<span class="home-stamp-cap">one domain, all or nothing</span>
			</div>
			<p class="home-stamp-text">
				Packs a gang into one accelerator domain, all or nothing. For Kubernetes 1.35; pods opt in.
			</p>
		</article>

		<article class="home-stamp">
			<h3 class="home-stamp-label">Alfred (alpha)</h3>
			<div class="home-stamp-scene home-stamp-scene--alfred" aria-hidden="true">
				<svg class="home-stamp-bell" viewBox="0 0 128 84">
					<g class="home-stamp-ring">
						<path
							d="M64 10 C 48 10, 42 24, 42 40 L 42 54 L 34 62 L 94 62 L 86 54 L 86 40 C 86 24, 80 10, 64 10 Z"
							fill="currentColor"
						/>
						<circle cx="64" cy="8" r="4" fill="currentColor" />
						<circle cx="64" cy="70" r="6" fill="currentColor" />
					</g>
				</svg>
				<div class="home-stamp-gpugrid">
					<span class="home-stamp-sq home-stamp-cell home-stamp-cell--00 home-stamp-cell--on"
					></span>
					<span class="home-stamp-sq home-stamp-cell home-stamp-cell--01"></span>
					<span class="home-stamp-sq home-stamp-cell home-stamp-cell--02 home-stamp-cell--on"
					></span>
					<span class="home-stamp-sq home-stamp-cell home-stamp-cell--03"></span>
					<span class="home-stamp-sq home-stamp-cell home-stamp-cell--10"></span>
					<span class="home-stamp-sq home-stamp-cell home-stamp-cell--11"></span>
					<span class="home-stamp-sq home-stamp-cell home-stamp-cell--12"></span>
					<span class="home-stamp-sq home-stamp-cell home-stamp-cell--13"></span>
					<span class="home-stamp-sq home-stamp-cell home-stamp-mover"></span>
					<div class="home-stamp-outline home-stamp-freerow"></div>
				</div>
				<span class="home-stamp-cap">one move frees a whole row of GPUs</span>
			</div>
			<p class="home-stamp-text">
				Spots fragmented GPUs and unhealthy nodes, recommends a move, and in execute mode makes
				room.
			</p>
		</article>

		<article class="home-stamp">
			<h3 class="home-stamp-label">Runtime selection</h3>
			<div class="home-stamp-scene home-stamp-scene--runtime" aria-hidden="true">
				<svg class="home-stamp-rings" viewBox="0 0 128 84">
					<circle class="home-stamp-pulse" cx="50" cy="42" r="24" fill="none" stroke-width="2.5" />
					<circle
						class="home-stamp-pulse home-stamp-pulse--d1"
						cx="78"
						cy="42"
						r="24"
						fill="none"
						stroke-width="2.5"
					/>
					<path
						class="home-stamp-part home-stamp-tick"
						d="M58 42 L 63 48 L 72 36"
						fill="none"
						stroke-width="3"
						stroke-linecap="round"
						stroke-linejoin="round"
					/>
				</svg>
				<span class="home-stamp-cap">model meets runtime: the best score wins</span>
			</div>
			<p class="home-stamp-text">
				Scores every runtime that supports the model and picks the best. Name one, and it's yours.
			</p>
		</article>

		<article class="home-stamp">
			<h3 class="home-stamp-label">kubectl ome</h3>
			<div class="home-stamp-scene home-stamp-scene--cli" aria-hidden="true">
				<svg class="home-stamp-bubble" viewBox="0 0 128 84">
					<path
						d="M18 14 H 110 A 8 8 0 0 1 118 22 V 54 A 8 8 0 0 1 110 62 H 48 L 34 74 L 36 62 H 18 A 8 8 0 0 1 10 54 V 22 A 8 8 0 0 1 18 14 Z"
						fill="none"
						stroke="currentColor"
						stroke-width="2.5"
					/>
					<rect
						class="home-stamp-part home-stamp-type"
						x="24"
						y="26"
						width="64"
						height="6"
						rx="3"
					/>
					<rect
						class="home-stamp-part home-stamp-type home-stamp-type--d1 home-stamp-type--soft"
						x="24"
						y="38"
						width="80"
						height="6"
						rx="3"
					/>
					<rect
						class="home-stamp-part home-stamp-type home-stamp-type--d2 home-stamp-type--soft"
						x="24"
						y="50"
						width="48"
						height="6"
						rx="3"
					/>
				</svg>
				<span class="home-stamp-cap">status · rollout · instance · runtime explain</span>
			</div>
			<p class="home-stamp-text">
				Says what OME did and why: status, rollout, instance, runtime explain. Seventeen commands.
			</p>
		</article>
	</div>

	<!-- The spaces between the spans are where a narrow screen can break the line, and they
		keep the phrases apart for screen readers. -->
	<p class="home-how-tagline" use:scrollReveal={{ y: 24, duration: 0.7, start: 'top 92%' }}>
		<span class="home-how-tagline-item">Any model</span>
		<span class="home-how-tagline-sep" aria-hidden="true">•</span>
		<span class="home-how-tagline-item">The right runtime</span>
		<span class="home-how-tagline-sep" aria-hidden="true">•</span>
		<span class="home-how-tagline-item">Progressive rollouts</span>
	</p>
</section>
