import { describe, expect, it } from 'vitest';
import type { SearchHit } from '$lib/docs/search-client';
import { groupHits, rowOrder } from './group-hits';

const hit = (route: string, sectionLabel: string): SearchHit => ({
	route,
	title: route,
	sectionLabel,
	group: null,
	description: '',
	status: null
});

describe('groupHits', () => {
	it('groups by section in order of first appearance, keeping rank order', () => {
		const a = hit('guides/a', 'Guides');
		const b = hit('concepts/b', 'Concepts');
		const c = hit('guides/c', 'Guides');
		expect(groupHits([a, b, c])).toEqual([
			{ label: 'Guides', hits: [a, c] },
			{ label: 'Concepts', hits: [b] }
		]);
	});

	it('returns no groups for no hits', () => {
		expect(groupHits([])).toEqual([]);
	});
});

describe('rowOrder', () => {
	it('lists hits in the order the groups show them', () => {
		const a = hit('guides/a', 'Guides');
		const b = hit('concepts/b', 'Concepts');
		const c = hit('guides/c', 'Guides');
		expect(rowOrder(groupHits([a, b, c]))).toEqual([a, c, b]);
	});
});
