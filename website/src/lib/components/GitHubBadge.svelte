<script lang="ts">
	import type { GitHubRepoStats } from '$lib/server/github';

	let { github }: { github: GitHubRepoStats } = $props();
</script>

<a
	class="github-badge"
	href={github.url}
	title="Go to repository"
	target="_blank"
	rel="noopener noreferrer"
>
	<span class="github-top">
		<svg class="github-icon" viewBox="0 0 512 512" aria-hidden="true">
			<path
				fill="currentColor"
				d="M173.9 397.4c0 2-2.3 3.6-5.2 3.6-3.3.3-5.6-1.3-5.6-3.6 0-2 2.3-3.6 5.2-3.6 3-.3 5.6 1.3 5.6 3.6m-31.1-4.5c-.7 2 1.3 4.3 4.3 4.9 2.6 1 5.6 0 6.2-2s-1.3-4.3-4.3-5.2c-2.6-.7-5.5.3-6.2 2.3m44.2-1.7c-2.9.7-4.9 2.6-4.6 4.9.3 2 2.9 3.3 5.9 2.6 2.9-.7 4.9-2.6 4.6-4.6-.3-1.9-3-3.2-5.9-2.9M252.8 8C114.1 8 8 113.3 8 252c0 110.9 69.8 205.8 169.5 239.2 12.8 2.3 17.3-5.6 17.3-12.1 0-6.2-.3-40.4-.3-61.4 0 0-70 15-84.7-29.8 0 0-11.4-29.1-27.8-36.6 0 0-22.9-15.7 1.6-15.4 0 0 24.9 2 38.6 25.8 21.9 38.6 58.6 27.5 72.9 20.9 2.3-16 8.8-27.1 16-33.7-55.9-6.2-112.3-14.3-112.3-110.5 0-27.5 7.6-41.3 23.6-58.9-2.6-6.5-11.1-33.3 2.6-67.9 20.9-6.5 69 27 69 27 20-5.6 41.5-8.5 62.8-8.5s42.8 2.9 62.8 8.5c0 0 48.1-33.6 69-27 13.7 34.7 5.2 61.4 2.6 67.9 16 17.7 25.8 31.5 25.8 58.9 0 96.5-58.9 104.2-114.8 110.5 9.2 7.9 17 22.9 17 46.4 0 33.7-.3 75.4-.3 83.6 0 6.5 4.6 14.4 17.3 12.1C436.2 457.8 504 362.9 504 252 504 113.3 391.5 8 252.8 8M105.2 352.9c-1.3 1-1 3.3.7 5.2 1.6 1.6 3.9 2.3 5.2 1 1.3-1 1-3.3-.7-5.2-1.6-1.6-3.9-2.3-5.2-1m-10.8-8.1c-.7 1.3.3 2.9 2.3 3.9 1.6 1 3.6.7 4.3-.7.7-1.3-.3-2.9-2.3-3.9-2-.6-3.6-.3-4.3.7m32.4 35.6c-1.6 1.3-1 4.3 1.3 6.2 2.3 2.3 5.2 2.6 6.5 1 1.3-1.3.7-4.3-1.3-6.2-2.2-2.3-5.2-2.6-6.5-1m-11.4-14.7c-1.6 1-1.6 3.6 0 5.9s4.3 3.3 5.6 2.3c1.6-1.3 1.6-3.9 0-6.2-1.4-2.3-4-3.3-5.6-2"
			/>
		</svg>
		<span class="github-repo">{github.fullName}</span>
	</span>
	{#if github.version !== null || github.stars !== null || github.forks !== null}
		<ul class="github-stats">
			{#if github.version}
				<li class="github-stat">{github.version}</li>
			{/if}
			{#if github.stars !== null}
				<li class="github-stat">
					<svg viewBox="0 0 16 16" aria-hidden="true">
						<path
							fill="currentColor"
							d="M8 .25a.75.75 0 0 1 .673.418l1.882 3.815 4.21.612a.75.75 0 0 1 .416 1.279l-3.046 2.97.719 4.192a.75.75 0 0 1-1.088.791L8 12.347l-3.766 1.98a.75.75 0 0 1-1.088-.79l.72-4.194L.819 6.856a.75.75 0 0 1 .416-1.28l4.21-.611L7.327.668A.75.75 0 0 1 8 .25Z"
						/>
					</svg>
					{github.stars}
				</li>
			{/if}
			{#if github.forks !== null}
				<li class="github-stat">
					<svg viewBox="0 0 16 16" aria-hidden="true">
						<path
							fill="currentColor"
							d="M5 3.25a.75.75 0 1 1 1.5 0 .75.75 0 0 1-1.5 0Zm3.75 0a.75.75 0 1 1 1.5 0 .75.75 0 0 1-1.5 0ZM3 7.25a.75.75 0 0 1 .75-.75h8.5a.75.75 0 0 1 0 1.5h-8.5a.75.75 0 0 1-.75-.75ZM3.75 10.5a.75.75 0 0 0 0 1.5h8.5a.75.75 0 0 0 0-1.5h-8.5Z"
						/>
					</svg>
					{github.forks}
				</li>
			{/if}
		</ul>
	{/if}
</a>

<style>
	.github-badge {
		display: inline-flex;
		flex-direction: column;
		align-items: flex-start;
		gap: 0.2rem;
		padding: 0.5rem 0.8rem;
		border-radius: 0.5rem;
		background: var(--nav-pill-bg, rgba(255, 255, 255, 0.14));
		border: 1px solid var(--nav-pill-border, rgba(255, 255, 255, 0.16));
		backdrop-filter: blur(12px);
		color: inherit;
		text-decoration: none;
		font-size: 0.72rem;
		font-weight: 500;
		line-height: 1.3;
	}

	.github-badge:hover {
		opacity: 0.9;
		text-decoration: none;
	}

	.github-top {
		display: flex;
		align-items: center;
		gap: 0.4rem;
	}

	.github-icon {
		width: 1rem;
		height: 1rem;
		flex-shrink: 0;
	}

	.github-repo {
		font-weight: 500;
		letter-spacing: 0.01em;
	}

	.github-stats {
		display: flex;
		align-items: center;
		gap: 0.65rem;
		margin: 0;
		padding: 0;
		list-style: none;
		opacity: 0.85;
	}

	.github-stat {
		display: inline-flex;
		align-items: center;
		gap: 0.2rem;
	}

	.github-stat svg {
		width: 0.75rem;
		height: 0.75rem;
		opacity: 0.9;
	}

	/* On the nav row (see .site-header-right), the badge is one line, as tall as the pill. */
	@media (min-width: 1200px) {
		.github-badge {
			flex-direction: row;
			align-items: center;
			gap: 0.65rem;
			height: 2.5rem;
			padding-block: 0;
			border-radius: 0.625rem;
			white-space: nowrap;
		}
	}
</style>
