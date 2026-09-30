import { describe, expect, it } from 'vitest';
import { activeHeading } from './active-heading';

describe('activeHeading', () => {
	const tops = [100, 500, 900];

	it('returns -1 above the first heading', () => {
		expect(activeHeading(tops, 80, false)).toBe(-1);
	});

	it('returns the last heading at or above the offset', () => {
		expect(activeHeading(tops, 100, false)).toBe(0);
		expect(activeHeading(tops, 600, false)).toBe(1);
		expect(activeHeading(tops, 1000, false)).toBe(2);
	});

	it('returns the last heading at the bottom of the page', () => {
		expect(activeHeading(tops, 0, true)).toBe(2);
	});

	it('returns -1 without headings', () => {
		expect(activeHeading([], 100, true)).toBe(-1);
	});
});
