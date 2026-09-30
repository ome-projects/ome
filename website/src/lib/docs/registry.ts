import type { PageMeta } from './types';

export interface Registry {
	/** Every page, sorted by content path. */
	readonly pages: readonly PageMeta[];
	/** Exact lookup by route, such as `guides` or `guides/deploy-models/run-benchmarks`. */
	getPage(route: string): PageMeta | undefined;
	/** Exact lookup by content path, such as `guides/index.md`. */
	getPageByPath(path: string): PageMeta | undefined;
	loadHtml(path: string): Promise<string>;
}

export function createRegistry(
	metas: Iterable<PageMeta>,
	loadHtml: (path: string) => Promise<string>
): Registry {
	const pages = [...metas].sort((a, b) => a.path.localeCompare(b.path));
	const byRoute = new Map<string, PageMeta>();
	const byPath = new Map<string, PageMeta>();

	for (const page of pages) {
		const existing = byRoute.get(page.route);
		if (existing) {
			throw new Error(`${existing.path} and ${page.path} are both served at /${page.route}`);
		}
		byRoute.set(page.route, page);
		byPath.set(page.path, page);
	}

	return {
		pages,
		getPage: (route) => byRoute.get(route),
		getPageByPath: (path) => byPath.get(path),
		loadHtml: async (path) => {
			if (!byPath.has(path)) throw new Error(`unknown page ${path}`);
			return loadHtml(path);
		}
	};
}
