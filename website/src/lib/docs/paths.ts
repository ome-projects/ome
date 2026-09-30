import type { SectionId } from './types.ts';

export const SECTION_IDS: readonly SectionId[] = [
	'getting-started',
	'guides',
	'concepts',
	'reference',
	'contributing'
];

export function isSectionId(value: string): value is SectionId {
	return (SECTION_IDS as readonly string[]).includes(value);
}

/**
 * Maps a content path to its route: `guides/x/y.md` is served at
 * `guides/x/y`, and `guides/index.md` at `guides`.
 */
export function pathToRoute(path: string): string {
	if (!path.endsWith('.md')) throw new Error(`${path}: content pages must end in .md`);
	const stem = path.slice(0, -'.md'.length);
	if (stem === 'index') throw new Error(`${path}: the content root has no index page`);
	return stem.endsWith('/index') ? stem.slice(0, -'/index'.length) : stem;
}

/** Returns the section a content path belongs to, or throws if it is outside every section. */
export function sectionOf(path: string): SectionId {
	const [first, ...rest] = path.split('/');
	if (rest.length === 0 || !isSectionId(first)) {
		throw new Error(`${path}: pages must live in a section directory (${SECTION_IDS.join(', ')})`);
	}
	return first;
}

/** Builds the URL of a route under the base path, with an optional anchor. */
export function routeHref(base: string, route: string, anchor: string | null = null): string {
	return `${base}/${route}${anchor ? `#${anchor}` : ''}`;
}

/**
 * Reports whether a request path is the base path or inside it. SvelteKit
 * only checks that a path starts with the base, so it also serves `/ome.`
 * and `/omega`.
 */
export function isUnderBase(pathname: string, base: string): boolean {
	return base === '' || pathname === base || pathname.startsWith(`${base}/`);
}
