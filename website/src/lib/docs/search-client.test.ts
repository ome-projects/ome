import { describe, expect, it, vi } from 'vitest';
import { createSearch, loadSearch } from './search-client';
import type { SearchEntry } from './types';

const entry = (route: string, title: string, body: string, headings = ''): SearchEntry => ({
	id: route,
	route,
	title,
	section: 'guides',
	sectionLabel: 'Guides',
	group: null,
	description: '',
	headings,
	body,
	status: null,
	generated: false
});

const entries = [
	entry('guides/pause', 'Pause and resume a rollout', 'Stop a rollout partway through.'),
	entry('guides/pvc', 'Serve models from a PVC', 'Mount a persistent volume claim.'),
	entry('concepts/storage', 'Model storage', 'Where weights live.', 'Persistent volumes')
];
const search = createSearch(entries);

describe('createSearch', () => {
	it('ranks title matches above body matches', () => {
		expect(search('rollout')[0].route).toBe('guides/pause');
	});

	it('ranks a generated page below title matches', () => {
		// Like the API reference, its type names give "rollout" many prefix matches.
		const types = [
			'Policy',
			'PolicySpec',
			'PolicyStatus',
			'Strategy',
			'Group',
			'GroupSpec',
			'Status',
			'Phase',
			'Plan',
			'Step',
			'Window',
			'Target'
		].map((name) => `Rollout${name}`);
		const reference: SearchEntry = {
			...entry(
				'reference/api',
				'API reference',
				types.map((type) => `${type} configures the rollout.`).join(' '),
				types.join(' ')
			),
			generated: true
		};
		expect(createSearch([...entries, reference])('rollout').map((hit) => hit.route)).toEqual([
			'guides/pause',
			'reference/api'
		]);
	});

	it('matches prefixes and typos', () => {
		expect(
			search('persis')
				.map((hit) => hit.route)
				.sort()
		).toEqual(['concepts/storage', 'guides/pvc']);
		expect(search('rolout')[0].route).toBe('guides/pause');
	});

	it('requires every term, then falls back to any term', () => {
		expect(search('persistent claim').map((hit) => hit.route)).toEqual(['guides/pvc']);
		expect(
			search('pause claim')
				.map((hit) => hit.route)
				.sort()
		).toEqual(['guides/pause', 'guides/pvc']);
	});

	it('returns stored fields only, and nothing for a blank query', () => {
		expect(search('weights')[0]).toEqual({
			route: 'concepts/storage',
			title: 'Model storage',
			sectionLabel: 'Guides',
			group: null,
			description: '',
			status: null
		});
		expect(search('   ')).toEqual([]);
	});
});

describe('loadSearch', () => {
	it('fetches once, and retries after a failure', async () => {
		const failing = vi.fn(async () => new Response('', { status: 503 }));
		await expect(loadSearch('/ome', failing)).rejects.toThrow(/503/);

		const working = vi.fn(async () => Response.json(entries));
		const loaded = await loadSearch('/ome', working);
		await loadSearch('/ome', working);
		expect(working).toHaveBeenCalledTimes(1);
		expect(working).toHaveBeenCalledWith('/ome/search.json');
		expect(loaded('pvc')[0].route).toBe('guides/pvc');
	});
});
