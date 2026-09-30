/**
 * Returns the index of the heading being read: the last one whose top is at or
 * above offset, or the last heading once the page is scrolled to the bottom,
 * where the final headings may never reach offset. Returns -1 above the first
 * heading and when there are no headings.
 */
export function activeHeading(tops: readonly number[], offset: number, atBottom: boolean): number {
	if (tops.length === 0) return -1;
	if (atBottom) return tops.length - 1;
	let active = -1;
	for (const [index, top] of tops.entries()) {
		if (top > offset) break;
		active = index;
	}
	return active;
}
