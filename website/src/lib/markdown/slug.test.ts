import { describe, expect, it } from 'vitest';
import { createSlugger, slugify } from './slug';

describe('slugify', () => {
	it.each([
		['Install the chart', 'install-the-chart'],
		['  kubectl ome rollout  ', 'kubectl-ome-rollout'],
		['What is OME?', 'what-is-ome'],
		['spec.modelRef', 'specmodelref'],
		['GPU/NPU support', 'gpunpu-support'],
		['snake_case stays', 'snake_case-stays']
	])('%s -> %s', (text, id) => {
		expect(slugify(text)).toBe(id);
	});
});

describe('createSlugger', () => {
	it('numbers repeats and skips claimed ids', () => {
		const slugger = createSlugger('page.md');
		slugger.claim('verify-1');
		expect(['Verify', 'Verify', 'Verify', '???'].map((text) => slugger.slug(text))).toEqual([
			'verify',
			'verify-2',
			'verify-3',
			'section'
		]);
		expect(() => slugger.claim('verify')).toThrow('page.md: duplicate heading id "verify"');
	});
});
