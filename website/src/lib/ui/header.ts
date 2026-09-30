/**
 * Returns how far below the top of the viewport sticky page parts, such as the
 * docs sidebar, should sit: the header's height while it shows, and zero once
 * it has slid away. An open menu keeps the header in place.
 */
export function stickyOffset(height: number, hidden: boolean, menuOpen: boolean): number {
	return hidden && !menuOpen ? 0 : height;
}

/** A section's vertical extent in viewport coordinates, as getBoundingClientRect gives it. */
export interface VerticalSpan {
	top: number;
	bottom: number;
}

/**
 * Reports whether the horizontal line at `lineY` crosses one of `sections`. The
 * home page passes the middle of the header's nav row and its dark sections, so
 * the header text turns white as a section's edge passes behind it, not as soon
 * as the section touches the bottom of the header.
 */
export function overDark(lineY: number, sections: readonly VerticalSpan[]): boolean {
	return sections.some((section) => section.top <= lineY && lineY < section.bottom);
}

/**
 * Reports whether the home header takes its on-dark colors: white text on a dark
 * backing. The open menu is a light panel under a light header bar, so the header
 * keeps its light colors while the menu is open, even over a dark section.
 */
export function headerOnDark(overDarkSection: boolean, menuOpen: boolean): boolean {
	return overDarkSection && !menuOpen;
}

/**
 * Reports whether focus moving to `next`, a focusout event's relatedTarget, leaves
 * `container`. A null target means focus went nowhere in particular, such as to
 * another window, so it doesn't count as leaving.
 */
export function focusLeaves(container: Pick<Node, 'contains'>, next: Node | null): boolean {
	return next !== null && !container.contains(next);
}

/**
 * Reports whether a header nav item is marked as the current section: on the page
 * at `href` and the pages below it. The error page belongs to no section, even at
 * an address under one, such as a guide that doesn't exist, so it marks no item.
 */
export function navItemActive(href: string, pathname: string, errorPage: boolean): boolean {
	return !errorPage && (pathname === href || pathname.startsWith(`${href}/`));
}
