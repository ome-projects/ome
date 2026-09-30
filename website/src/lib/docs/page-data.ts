import { editUrl, sourceUrl } from '../config/site';
import { buildNavTree, pagePosition, type NavTree, type PagePosition } from './navigation';
import { legacyUrl, replacedPages, type RedirectEntry } from './redirects';
import type { Registry } from './registry';
import { applySinceLabels, sinceLabel, sinceState, type SinceState } from './since';
import type { NavSection, PageMeta } from './types';

/** What the doc page route returns. B1's page component renders exactly this. */
export interface DocPageData {
	title: string;
	description: string;
	page: Pick<PageMeta, 'path' | 'route' | 'section' | 'title' | 'status' | 'generated' | 'toc'>;
	/** Rendered body with since labels applied; empty for drafts. */
	html: string;
	tree: NavTree;
	position: PagePosition;
	/** Page-level since badge. */
	since: { state: SinceState; label: string } | null;
	/** For drafts, the Hugo pages the banner links to. */
	replaces: { old: string; url: string }[];
	/** Null for generated pages, which are edited through their generator. */
	editUrl: string | null;
	sourceUrl: string;
}

export interface DocPageContext {
	registry: Registry;
	nav: readonly NavSection[];
	redirects: readonly RedirectEntry[];
	/** Latest release tag, such as `v1.2.2`, or null when GitHub is unreachable. */
	latestRelease: string | null;
}

/** Builds a doc page's data. Since labels are applied here, per request, so a release needs no redeploy. */
export async function docPageData(
	page: PageMeta,
	{ registry, nav, redirects, latestRelease }: DocPageContext
): Promise<DocPageData> {
	const section = nav.find((entry) => entry.id === page.section);
	if (!section) throw new Error(`nav.ts has no section ${page.section}`);
	const tree = buildNavTree(section, registry.getPageByPath);
	const draft = page.status === 'draft';

	return {
		title: page.title,
		description: page.description,
		page: {
			path: page.path,
			route: page.route,
			section: page.section,
			title: page.title,
			status: page.status,
			generated: page.generated,
			toc: page.toc
		},
		html: draft ? '' : applySinceLabels(await registry.loadHtml(page.path), latestRelease),
		tree,
		position: pagePosition(tree, page.route),
		since: page.since
			? {
					state: sinceState(page.since, latestRelease),
					label: sinceLabel(page.since, latestRelease)
				}
			: null,
		replaces: draft
			? replacedPages(redirects, page.path).map((old) => ({ old, url: legacyUrl(old) }))
			: [],
		editUrl: page.generated ? null : editUrl(page.path),
		sourceUrl: sourceUrl(page.path)
	};
}
