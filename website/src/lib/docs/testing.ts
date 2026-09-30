import { pathToRoute, sectionOf } from './paths';
import type { PageMeta } from './types';

/** Builds page metadata for tests. */
export function testPage(path: string, overrides: Partial<PageMeta> = {}): PageMeta {
	return {
		path,
		route: pathToRoute(path),
		section: sectionOf(path),
		title: path,
		navLabel: path,
		description: `About ${path}.`,
		status: null,
		since: null,
		generated: false,
		toc: [],
		anchors: [],
		links: [],
		...overrides
	};
}
