import MiniSearch from 'minisearch';
import type { SearchEntry } from './types';

export type SearchHit = Pick<
	SearchEntry,
	'route' | 'title' | 'sectionLabel' | 'group' | 'description' | 'status'
>;

export type Search = (query: string, limit?: number) => SearchHit[];

// A generated page's scores are scaled by this. The API reference is many times
// longer than any other page, and each of its type names that starts with a query
// word, such as RolloutPolicy for "rollout", adds a prefix match. It outranked
// pages whose titles match.
const GENERATED_PAGE_BOOST = 0.4;

/** Indexes entries for ranked search: all terms first, any term if that finds nothing. */
export function createSearch(entries: readonly SearchEntry[]): Search {
	const generated = new Set(entries.filter((entry) => entry.generated).map((entry) => entry.id));
	const index = new MiniSearch<SearchEntry>({
		fields: ['title', 'headings', 'description', 'body'],
		storeFields: ['route', 'title', 'sectionLabel', 'group', 'description', 'status'],
		searchOptions: {
			boost: { title: 5, headings: 2, description: 2 },
			boostDocument: (id: string) => (generated.has(id) ? GENERATED_PAGE_BOOST : 1),
			prefix: true,
			fuzzy: 0.2
		}
	});
	index.addAll(entries);

	return (query, limit = 20) => {
		const text = query.trim();
		if (text === '') return [];
		let results = index.search(text, { combineWith: 'AND' });
		if (results.length === 0) results = index.search(text, { combineWith: 'OR' });
		return results.slice(0, limit).map((result) => ({
			route: result.route,
			title: result.title,
			sectionLabel: result.sectionLabel,
			group: result.group,
			description: result.description,
			status: result.status
		}));
	};
}

let loading: Promise<Search> | null = null;

/** Fetches and indexes /search.json once per page load. A failed fetch is retried on the next call. */
export function loadSearch(base: string, fetcher: typeof fetch = fetch): Promise<Search> {
	loading ??= fetcher(`${base}/search.json`)
		.then((response) => {
			if (!response.ok) throw new Error(`search index request failed: ${response.status}`);
			return response.json() as Promise<SearchEntry[]>;
		})
		.then(createSearch)
		.catch((error: unknown) => {
			loading = null;
			throw error;
		});
	return loading;
}
