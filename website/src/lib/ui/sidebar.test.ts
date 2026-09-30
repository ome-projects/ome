import { describe, expect, it } from 'vitest';
import type { NavLink, NavTree } from '$lib/docs/navigation';
import { closedGroups, sidebarPageLabel } from './sidebar';

const link = (route: string): NavLink => ({
	path: `${route}.md`,
	route,
	label: route,
	title: route,
	description: '',
	status: null
});

const tree: NavTree = {
	id: 'guides',
	label: 'Guides',
	landing: link('guides'),
	groups: [
		{ label: null, preview: false, links: [link('guides/overview')] },
		{ label: 'Deploy models', preview: false, links: [link('guides/deploy-models/pvc')] },
		{ label: 'Networking', preview: false, links: [link('guides/networking/ingress')] },
		{ label: 'Multi-cluster', preview: true, links: [link('guides/multi-cluster/setup')] }
	]
};

describe('closedGroups', () => {
	it('opens only the group holding the page', () => {
		expect(closedGroups(tree, 'guides/networking/ingress', false)).toEqual(
			new Set(['Deploy models', 'Multi-cluster'])
		);
	});

	it('opens every group on the landing page', () => {
		expect(closedGroups(tree, 'guides', false)).toEqual(new Set());
	});

	it('closes every group on the landing page in the compact layout', () => {
		expect(closedGroups(tree, 'guides', true)).toEqual(
			new Set(['Deploy models', 'Networking', 'Multi-cluster'])
		);
	});

	it('keeps the page group open in the compact layout', () => {
		expect(closedGroups(tree, 'guides/deploy-models/pvc', true)).toEqual(
			new Set(['Networking', 'Multi-cluster'])
		);
	});

	it('never closes the unlabeled group', () => {
		expect(closedGroups(tree, 'guides/overview', true)).toEqual(
			new Set(['Deploy models', 'Networking', 'Multi-cluster'])
		);
	});
});

describe('sidebarPageLabel', () => {
	const labeled: NavTree = {
		...tree,
		groups: [
			{ label: null, preview: false, links: [{ ...link('guides/overview'), label: 'Overview' }] },
			{
				label: 'Deploy models',
				preview: false,
				links: [{ ...link('guides/deploy-models/pvc'), label: 'Serve models from a PVC' }]
			}
		]
	};

	it('names a page by its sidebar link', () => {
		expect(sidebarPageLabel(labeled, 'guides/deploy-models/pvc')).toBe('Serve models from a PVC');
	});

	it('names a page in the unlabeled group', () => {
		expect(sidebarPageLabel(labeled, 'guides/overview')).toBe('Overview');
	});

	it('adds nothing on the landing page, which the section label names', () => {
		expect(sidebarPageLabel(labeled, 'guides')).toBeNull();
	});

	it('adds nothing for a page the sidebar does not list', () => {
		expect(sidebarPageLabel(labeled, 'guides/drafts/unlisted')).toBeNull();
	});
});
