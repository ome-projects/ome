import type { NavTree } from '$lib/docs/navigation';

/**
 * Returns the labels of the sidebar groups that start closed. On a section's
 * landing page every group starts open, because the sidebar is the section's
 * map; elsewhere only the group holding the page does. In the compact layout,
 * where the sidebar sits above the article, even the landing page starts with
 * every group closed.
 */
export function closedGroups(tree: NavTree, route: string, compact: boolean): Set<string> {
	const onLanding = tree.landing?.route === route;
	const closed = new Set<string>();
	for (const group of tree.groups) {
		if (group.label === null) continue;
		const holdsPage = group.links.some((link) => link.route === route);
		if (holdsPage || (onLanding && !compact)) continue;
		closed.add(group.label);
	}
	return closed;
}

/**
 * Returns the sidebar label of the page at route, which the compact layout's
 * toggle shows after the section label. It's null on the section's landing
 * page, which the section label already names, and on pages the sidebar
 * doesn't list.
 */
export function sidebarPageLabel(tree: NavTree, route: string): string | null {
	for (const group of tree.groups) {
		const link = group.links.find((candidate) => candidate.route === route);
		if (link) return link.label;
	}
	return null;
}
