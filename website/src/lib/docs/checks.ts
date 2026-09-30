import { SECTION_IDS } from './paths';
import type { RedirectEntry } from './redirects';
import type { NavSection, PageMeta, SearchEntry } from './types';

// Each check returns every problem it finds, so one test run reports them all.

const COMMIT = /^[0-9a-f]{7,40}$/;

/** Every section has a landing page, every nav entry exists, and every other page is listed once. */
export function checkNav(nav: readonly NavSection[], pages: readonly PageMeta[]): string[] {
	const errors: string[] = [];
	const paths = new Set(pages.map((page) => page.path));
	const listed = new Map<string, number>();

	for (const id of SECTION_IDS) {
		const count = nav.filter((section) => section.id === id).length;
		if (count !== 1) errors.push(`nav.ts must list section "${id}" once, not ${count} times`);
		if (!paths.has(`${id}/index.md`))
			errors.push(`section "${id}" has no landing page ${id}/index.md`);
	}
	for (const section of nav) {
		for (const group of section.groups) {
			for (const page of group.pages) {
				const path = `${section.id}/${page}`;
				if (page === 'index.md' || page.endsWith('/index.md')) {
					errors.push(
						`nav.ts lists ${path}; section landing pages are implicit and groups have no index page`
					);
				} else if (!paths.has(path)) {
					errors.push(`nav.ts lists ${path}, which does not exist`);
				}
				listed.set(path, (listed.get(path) ?? 0) + 1);
			}
		}
	}
	for (const page of pages) {
		if (page.path === `${page.section}/index.md`) continue;
		const count = listed.get(page.path) ?? 0;
		if (count === 0) errors.push(`${page.path} is not in nav.ts`);
		if (count > 1) errors.push(`${page.path} is listed ${count} times in nav.ts`);
	}
	return errors;
}

/** Every internal link reaches a page, and every anchor exists on its target. */
export function checkLinks(pages: readonly PageMeta[]): string[] {
	const errors: string[] = [];
	const byRoute = new Map(pages.map((page) => [page.route, page]));
	for (const page of pages) {
		for (const { route, anchor } of page.links) {
			const shown = `/${route}${anchor ? `#${anchor}` : ''}`;
			const target = byRoute.get(route);
			if (!target) {
				errors.push(`${page.path}: the link to ${shown} does not match any page`);
			} else if (anchor && !target.anchors.includes(anchor)) {
				errors.push(
					target.status === 'draft'
						? `${page.path}: the link to ${shown} points into ${target.path}, a draft with no headings yet; link to the page without an anchor`
						: `${page.path}: the link to ${shown}: ${target.path} has no id "${anchor}"`
				);
			}
		}
	}
	return errors;
}

/**
 * Old paths are unique, new paths exist, and `rewrittenFrom` is set exactly
 * when the new page is written.
 */
export function checkRedirects(
	entries: readonly RedirectEntry[],
	pages: readonly PageMeta[]
): string[] {
	const errors: string[] = [];
	const byPath = new Map(pages.map((page) => [page.path, page]));
	const seen = new Set<string>();
	for (const entry of entries) {
		const where = `redirects.json entry "${entry.old}"`;
		if (!entry.old.endsWith('.md'))
			errors.push(`${where}: "old" must be a Hugo page path ending in .md`);
		if (seen.has(entry.old)) errors.push(`${where}: "old" appears more than once`);
		seen.add(entry.old);

		const page = byPath.get(entry.new);
		if (!page) {
			errors.push(`${where}: "new" page ${entry.new} does not exist`);
		} else if (entry.rewrittenFrom === null && page.status !== 'draft') {
			errors.push(
				`${where}: ${entry.new} is written, so set "rewrittenFrom" to the 8-character hash of the last commit that changed the old page (git log -1 --abbrev=8 --format=%h HEAD -- site/content/en/docs/${entry.old})`
			);
		} else if (entry.rewrittenFrom !== null && page.status === 'draft') {
			errors.push(`${where}: ${entry.new} is a draft, so "rewrittenFrom" must be null`);
		}
		if (entry.rewrittenFrom !== null && !COMMIT.test(entry.rewrittenFrom)) {
			errors.push(`${where}: "rewrittenFrom" must be a commit SHA`);
		}
	}
	return errors;
}

/** The search index has exactly one entry per page. */
export function checkSearchIndex(
	entries: readonly SearchEntry[],
	pages: readonly PageMeta[]
): string[] {
	const errors: string[] = [];
	const routes = new Set(pages.map((page) => page.route));
	const seen = new Set<string>();
	for (const entry of entries) {
		if (!routes.has(entry.route))
			errors.push(`search entry /${entry.route} does not match any page`);
		if (seen.has(entry.route)) errors.push(`search entry /${entry.route} appears more than once`);
		seen.add(entry.route);
	}
	for (const page of pages) {
		if (!seen.has(page.route)) errors.push(`${page.path} is missing from the search index`);
	}
	return errors;
}
