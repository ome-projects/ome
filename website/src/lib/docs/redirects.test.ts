import { describe, expect, it } from 'vitest';
import { legacyUrl, parseRedirects, replacedPages } from './redirects';

describe('parseRedirects', () => {
	it('accepts well-formed entries', () => {
		const entries = [
			{
				old: 'tasks/kubectl-ome-rollout-explain.md',
				new: 'reference/kubectl-ome/rollout.md',
				rewrittenFrom: null
			},
			{
				old: 'tasks/kubectl-ome-rollout-history.md',
				new: 'reference/kubectl-ome/rollout.md',
				rewrittenFrom: 'abc1234'
			}
		];
		expect(parseRedirects(entries)).toEqual(entries);
		expect(replacedPages(entries, 'reference/kubectl-ome/rollout.md')).toEqual([
			'tasks/kubectl-ome-rollout-explain.md',
			'tasks/kubectl-ome-rollout-history.md'
		]);
		expect(replacedPages(entries, 'guides/index.md')).toEqual([]);
	});

	it.each([
		['a non-array', {}, /must be an array/],
		['a non-object entry', ['x'], /\[0\] must be an object/],
		[
			'an unknown key',
			[{ old: 'a.md', new: 'b.md', rewrittenFrom: null, note: 'x' }],
			/unknown key "note"/
		],
		['a missing path', [{ old: 'a.md', rewrittenFrom: null }], /needs string "old" and "new"/],
		['a missing rewrittenFrom', [{ old: 'a.md', new: 'b.md' }], /needs "rewrittenFrom"/]
	])('rejects %s', (_name, value, message) => {
		expect(() => parseRedirects(value)).toThrow(message);
	});
});

describe('legacyUrl', () => {
	it.each([
		['_index.md', 'https://ome-projects.github.io/ome/docs/'],
		['concepts/_index.md', 'https://ome-projects.github.io/ome/docs/concepts/'],
		['concepts/base_model.md', 'https://ome-projects.github.io/ome/docs/concepts/base_model/'],
		[
			'tasks/run-workloads/select-accelerators.md',
			'https://ome-projects.github.io/ome/docs/tasks/run-workloads/select-accelerators/'
		]
	])('%s -> %s', (old, url) => {
		expect(legacyUrl(old)).toBe(url);
	});
});
