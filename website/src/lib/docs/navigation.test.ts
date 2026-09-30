import { describe, expect, it } from 'vitest';
import { buildNavTree, pagePosition } from './navigation';
import { testPage } from './testing';
import type { NavSection } from './types';

const section: NavSection = {
	id: 'guides',
	label: 'Guides',
	groups: [
		{ label: 'Deploy models', pages: ['deploy-models/a.md', 'deploy-models/missing.md'] },
		{ label: 'Networking', pages: ['networking/b.md'] },
		{ label: 'Empty', pages: ['empty/missing.md'] },
		{ label: 'Multi-cluster', preview: true, pages: ['multi-cluster/c.md'] }
	]
};
const pages = new Map(
	[
		'guides/index.md',
		'guides/deploy-models/a.md',
		'guides/networking/b.md',
		'guides/multi-cluster/c.md'
	].map((path) => [
		path,
		testPage(path, { navLabel: `Label ${path}`, status: path.includes('/b') ? 'draft' : null })
	])
);
const tree = buildNavTree(section, (path) => pages.get(path));

describe('buildNavTree', () => {
	it('resolves pages and drops missing pages and empty groups', () => {
		expect(tree.landing?.route).toBe('guides');
		expect(
			tree.groups.map((group) => [
				group.label,
				group.preview,
				group.links.map((link) => link.route)
			])
		).toEqual([
			['Deploy models', false, ['guides/deploy-models/a']],
			['Networking', false, ['guides/networking/b']],
			['Multi-cluster', true, ['guides/multi-cluster/c']]
		]);
		expect(tree.groups[1].links[0]).toMatchObject({
			label: 'Label guides/networking/b.md',
			status: 'draft'
		});
	});
});

describe('pagePosition', () => {
	it('walks from the landing page through each group', () => {
		expect(pagePosition(tree, 'guides')).toMatchObject({
			group: null,
			prev: null,
			next: { route: 'guides/deploy-models/a' }
		});
		expect(pagePosition(tree, 'guides/networking/b')).toMatchObject({
			group: 'Networking',
			prev: { route: 'guides/deploy-models/a' },
			next: { route: 'guides/multi-cluster/c' }
		});
		expect(pagePosition(tree, 'guides/multi-cluster/c').next).toBeNull();
		expect(pagePosition(tree, 'guides/unknown')).toEqual({ group: null, prev: null, next: null });
	});
});
