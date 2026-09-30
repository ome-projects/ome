import type { NavSection, PageMeta, PageStatus, SectionId } from './types';

export interface NavLink {
	path: string;
	route: string;
	label: string;
	title: string;
	description: string;
	status: PageStatus | null;
}

export interface NavTreeGroup {
	label: string | null;
	preview: boolean;
	links: NavLink[];
}

/** A section's sidebar, with pages resolved from the registry. */
export interface NavTree {
	id: SectionId;
	label: string;
	landing: NavLink | null;
	groups: NavTreeGroup[];
}

export interface PagePosition {
	/** Label of the group the page is in; null for the landing page and ungrouped pages. */
	group: string | null;
	prev: NavLink | null;
	next: NavLink | null;
}

function toLink(page: PageMeta): NavLink {
	return {
		path: page.path,
		route: page.route,
		label: page.navLabel,
		title: page.title,
		description: page.description,
		status: page.status
	};
}

/**
 * Resolves a section's nav entries against the registry. Missing pages are
 * skipped here and reported by checkNav, so a bad entry never breaks the
 * sidebar at request time.
 */
export function buildNavTree(
	section: NavSection,
	lookup: (path: string) => PageMeta | undefined
): NavTree {
	const landing = lookup(`${section.id}/index.md`);
	const groups = section.groups
		.map((group) => ({
			label: group.label,
			preview: group.preview ?? false,
			links: group.pages
				.map((page) => lookup(`${section.id}/${page}`))
				.filter((page): page is PageMeta => page !== undefined)
				.map(toLink)
		}))
		.filter((group) => group.links.length > 0);
	return {
		id: section.id,
		label: section.label,
		landing: landing ? toLink(landing) : null,
		groups
	};
}

/** Finds a page in its section's reading order: the landing page, then each group in turn. */
export function pagePosition(tree: NavTree, route: string): PagePosition {
	const order = [
		...(tree.landing ? [{ link: tree.landing, group: null }] : []),
		...tree.groups.flatMap((group) => group.links.map((link) => ({ link, group: group.label })))
	];
	const index = order.findIndex((entry) => entry.link.route === route);
	if (index === -1) return { group: null, prev: null, next: null };
	return {
		group: order[index].group,
		prev: order[index - 1]?.link ?? null,
		next: order[index + 1]?.link ?? null
	};
}
