import { describe, expect, it } from 'vitest';
import { focusLeaves, headerOnDark, navItemActive, overDark, stickyOffset } from './header';

describe('stickyOffset', () => {
	it('is the header height while the header shows', () => {
		expect(stickyOffset(84, false, false)).toBe(84);
	});

	it('drops to zero once the header slides away', () => {
		expect(stickyOffset(84, true, false)).toBe(0);
	});

	it('keeps the height while the menu holds the header open', () => {
		expect(stickyOffset(84, true, true)).toBe(84);
	});
});

describe('overDark', () => {
	// The nav row's midpoint in an 84px header.
	const line = 48;

	it('stays light while a dark section only reaches the bottom of the header', () => {
		expect(overDark(line, [{ top: 70, bottom: 1400 }])).toBe(false);
	});

	it('turns dark once the section passes behind the nav row', () => {
		expect(overDark(line, [{ top: 40, bottom: 1400 }])).toBe(true);
	});

	it('turns light again once the section has scrolled above the nav row', () => {
		expect(overDark(line, [{ top: -1400, bottom: 30 }])).toBe(false);
	});

	it('checks every section', () => {
		expect(
			overDark(line, [
				{ top: -1400, bottom: -200 },
				{ top: -200, bottom: 300 }
			])
		).toBe(true);
	});
});

describe('headerOnDark', () => {
	it('turns the header white over a dark section', () => {
		expect(headerOnDark(true, false)).toBe(true);
	});

	it('stays light elsewhere', () => {
		expect(headerOnDark(false, false)).toBe(false);
	});

	it('stays light over a dark section while the menu is open', () => {
		expect(headerOnDark(true, true)).toBe(false);
	});
});

describe('focusLeaves', () => {
	// Stand-ins for the header and the elements focus moves to.
	const inside = {} as Node;
	const outside = {} as Node;
	const header = { contains: (other: Node | null) => other === inside };

	it('is false while focus moves within the container', () => {
		expect(focusLeaves(header, inside)).toBe(false);
	});

	it('is true once focus moves to an element outside it', () => {
		expect(focusLeaves(header, outside)).toBe(true);
	});

	it('is false when focus goes nowhere, such as to another window', () => {
		expect(focusLeaves(header, null)).toBe(false);
	});
});

describe('navItemActive', () => {
	it('marks the item on the page it links to', () => {
		expect(navItemActive('/ome/guides', '/ome/guides', false)).toBe(true);
	});

	it('marks the item on the pages below it', () => {
		expect(navItemActive('/ome/guides', '/ome/guides/deploy-models/serve-from-pvc', false)).toBe(
			true
		);
	});

	it('leaves the item unmarked on a path that only starts with the same letters', () => {
		expect(navItemActive('/ome/guides', '/ome/guidestar', false)).toBe(false);
	});

	it('marks no item on the error page, even at an address under one', () => {
		expect(navItemActive('/ome/guides', '/ome/guides/nope', true)).toBe(false);
	});
});
