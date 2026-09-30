import type { NavSection, PageMeta, SearchEntry } from './types';

/**
 * Builds the search index served at /search.json. Drafts are indexed by title
 * and description only.
 */
export async function buildSearchIndex(
	pages: readonly PageMeta[],
	nav: readonly NavSection[],
	loadText: (path: string) => Promise<string>
): Promise<SearchEntry[]> {
	const sectionLabels = new Map(nav.map((section) => [section.id, section.label]));
	const groups = new Map<string, string | null>();
	for (const section of nav) {
		for (const group of section.groups) {
			for (const page of group.pages) groups.set(`${section.id}/${page}`, group.label);
		}
	}

	return Promise.all(
		pages.map(async (page): Promise<SearchEntry> => {
			const draft = page.status === 'draft';
			return {
				id: page.route,
				route: page.route,
				title: page.title,
				section: page.section,
				sectionLabel: sectionLabels.get(page.section) ?? page.section,
				group: groups.get(page.path) ?? null,
				description: page.description,
				headings: draft ? '' : page.toc.map((entry) => entry.text).join(' '),
				body: draft ? '' : await loadText(page.path),
				status: page.status,
				generated: page.generated
			};
		})
	);
}
