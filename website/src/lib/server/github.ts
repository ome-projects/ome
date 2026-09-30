import { site } from '$lib/config/site';

export type GitHubRepoStats = {
	fullName: string;
	url: string;
	version: string | null;
	// null when GitHub could not be reached, so the UI hides the count instead of showing 0.
	stars: number | null;
	forks: number | null;
};

type CachedStats = { stats: GitHubRepoStats; fetchedAt: number };

const REPO = site.repo;
const UNKNOWN_STATS: GitHubRepoStats = {
	fullName: REPO,
	url: `https://github.com/${REPO}`,
	version: null,
	stars: null,
	forks: null
};

// Serve a successful fetch this long before asking GitHub again.
const FRESH_TTL_MS = 15 * 60 * 1000;
// Keep serving a stale result this long while refreshes keep failing.
const STALE_TTL_MS = 24 * 60 * 60 * 1000;
// Back off after a failed fetch so a busy edge location does not hammer GitHub.
const RETRY_DELAY_MS = 60 * 1000;
const FETCH_TIMEOUT_MS = 4000;
// Synthetic key for the per-location edge cache; the host is never requested.
const EDGE_CACHE_KEY = 'https://ome-site.invalid/github/repo-stats';

// Per-isolate state: isolates are short-lived and per location, so the edge cache is the real store.
let memoryCache: CachedStats | null = null;
let lastFailureAt = 0;
let inflight: Promise<CachedStats | null> | null = null;
let warnedMissingToken = false;

function githubHeaders(token?: string): HeadersInit {
	const headers: Record<string, string> = {
		Accept: 'application/vnd.github+json',
		'User-Agent': 'ome-site',
		'X-GitHub-Api-Version': '2022-11-28'
	};
	if (token) headers.Authorization = `Bearer ${token}`;
	return headers;
}

function describeError(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}

async function fetchJson<T>(url: string, token?: string): Promise<T | null> {
	try {
		const response = await fetch(url, {
			headers: githubHeaders(token),
			signal: AbortSignal.timeout(FETCH_TIMEOUT_MS)
		});
		if (!response.ok) {
			const remaining = response.headers.get('x-ratelimit-remaining');
			const reset = response.headers.get('x-ratelimit-reset');
			console.warn(
				`[github] ${url} -> ${response.status} (rate limit remaining=${remaining}, reset=${reset})`
			);
			return null;
		}
		return (await response.json()) as T;
	} catch (error) {
		console.warn(`[github] ${url} failed: ${describeError(error)}`);
		return null;
	}
}

async function fetchStats(token?: string): Promise<GitHubRepoStats | null> {
	const [repo, release] = await Promise.all([
		fetchJson<{
			full_name: string;
			html_url: string;
			stargazers_count: number;
			forks_count: number;
		}>(`https://api.github.com/repos/${REPO}`, token),
		fetchJson<{ tag_name: string }>(`https://api.github.com/repos/${REPO}/releases/latest`, token)
	]);

	if (!repo) return null;

	return {
		fullName: repo.full_name,
		url: repo.html_url,
		version: release?.tag_name ?? null,
		stars: repo.stargazers_count,
		forks: repo.forks_count
	};
}

async function readEdgeCache(cache: Cache | undefined): Promise<CachedStats | null> {
	if (!cache) return null;
	try {
		const response = await cache.match(EDGE_CACHE_KEY);
		if (!response) return null;
		const cached = (await response.json()) as Partial<CachedStats>;
		if (!cached.stats || typeof cached.fetchedAt !== 'number') return null;
		return cached as CachedStats;
	} catch (error) {
		console.warn(`[github] edge cache read failed: ${describeError(error)}`);
		return null;
	}
}

async function writeEdgeCache(cache: Cache | undefined, entry: CachedStats): Promise<void> {
	if (!cache) return;
	try {
		await cache.put(
			EDGE_CACHE_KEY,
			new Response(JSON.stringify(entry), {
				headers: {
					'Content-Type': 'application/json',
					'Cache-Control': `max-age=${Math.floor(STALE_TTL_MS / 1000)}`
				}
			})
		);
	} catch (error) {
		console.warn(`[github] edge cache write failed: ${describeError(error)}`);
	}
}

// One GitHub round trip per isolate at a time; concurrent callers share the result.
function refresh(cache: Cache | undefined, token?: string): Promise<CachedStats | null> {
	if (inflight) return inflight;
	inflight = (async () => {
		const stats = await fetchStats(token);
		if (!stats) {
			lastFailureAt = Date.now();
			return null;
		}
		const entry: CachedStats = { stats, fetchedAt: Date.now() };
		memoryCache = entry;
		await writeEdgeCache(cache, entry);
		return entry;
	})().finally(() => {
		inflight = null;
	});
	return inflight;
}

function isFresh(entry: CachedStats, now: number): boolean {
	return now - entry.fetchedAt < FRESH_TTL_MS;
}

function isUsable(entry: CachedStats | null, now: number): entry is CachedStats {
	return entry !== null && now - entry.fetchedAt < STALE_TTL_MS;
}

export async function getGitHubRepoStats(platform?: App.Platform): Promise<GitHubRepoStats> {
	const token = platform?.env?.GITHUB_TOKEN;
	if (!token && !warnedMissingToken) {
		warnedMissingToken = true;
		console.warn(
			'[github] GITHUB_TOKEN is not set; unauthenticated requests share a 60/hour limit per IP'
		);
	}

	// vite.config.ts imports svelte.config.js, which loads the adapter's
	// App.Platform ahead of src/app.d.ts, so `caches` gets workers-types'
	// Cache; the helpers only call match and put.
	const cache = platform?.caches?.default as Cache | undefined;
	const now = Date.now();

	let cached = isUsable(memoryCache, now) ? memoryCache : null;
	if (!cached || !isFresh(cached, now)) {
		const fromEdge = await readEdgeCache(cache);
		if (isUsable(fromEdge, now) && (!cached || fromEdge.fetchedAt > cached.fetchedAt)) {
			cached = fromEdge;
			memoryCache = fromEdge;
		}
	}

	if (cached && isFresh(cached, now)) return cached.stats;

	const canRetry = now - lastFailureAt >= RETRY_DELAY_MS;

	if (cached) {
		// Serve the stale value now and refresh off the request path when the runtime allows it.
		if (canRetry) {
			const pending = refresh(cache, token);
			if (platform?.ctx) {
				platform.ctx.waitUntil(pending);
			} else {
				const refreshed = await pending;
				if (refreshed) return refreshed.stats;
			}
		}
		return cached.stats;
	}

	// Nothing cached at this location yet: this request pays for the fetch.
	if (!canRetry) return UNKNOWN_STATS;
	const entry = await refresh(cache, token);
	return entry?.stats ?? UNKNOWN_STATS;
}
