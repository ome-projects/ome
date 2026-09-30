import { describe, expect, it } from 'vitest';
import { docPageData } from './page-data';
import type { RedirectEntry } from './redirects';
import { createRegistry } from './registry';
import { testPage } from './testing';
import type { NavSection } from './types';

const nav: NavSection[] = [
	{
		id: 'guides',
		label: 'Guides',
		groups: [{ label: 'Deploy models', pages: ['deploy/a.md', 'deploy/b.md'] }]
	},
	{ id: 'reference', label: 'Reference', groups: [{ label: 'API', pages: ['api/ome.v1beta1.md'] }] }
];

const pages = [
	testPage('guides/index.md', { title: 'Guides' }),
	testPage('guides/deploy/a.md', {
		title: 'A',
		toc: [{ depth: 2, id: 'x', text: 'X' }],
		anchors: ['x'],
		links: [{ route: 'guides', anchor: null }]
	}),
	testPage('guides/deploy/b.md', { title: 'B', status: 'draft' }),
	testPage('reference/api/ome.v1beta1.md', { title: 'OME API', generated: true, since: 'v1.3' })
];

const html: Record<string, string> = {
	'guides/index.md': '<p>Landing</p>',
	'guides/deploy/a.md':
		'<h2 id="x">X<span class="doc-since" data-since="v1.3">Since v1.3</span></h2>',
	'reference/api/ome.v1beta1.md': '<p>API</p>'
};

const loaded: string[] = [];
const registry = createRegistry(pages, async (path) => {
	loaded.push(path);
	return html[path] ?? '';
});

const redirects: RedirectEntry[] = [
	{ old: 'tasks/a.md', new: 'guides/deploy/a.md', rewrittenFrom: 'abc1234' },
	{ old: 'tasks/b.md', new: 'guides/deploy/b.md', rewrittenFrom: null },
	{ old: 'tasks/b-more.md', new: 'guides/deploy/b.md', rewrittenFrom: null }
];

const context = { registry, nav, redirects, latestRelease: 'v1.2.2' };
const page = (path: string) => registry.getPageByPath(path)!;

describe('docPageData', () => {
	it('labels since badges against the latest release', async () => {
		const data = await docPageData(page('guides/deploy/a.md'), context);
		expect(data.html).toContain('class="doc-since doc-since--unreleased"');
		expect(data.html).toContain('Unreleased: coming in v1.3');
		expect(data.since).toBeNull();
	});

	it('places the page in its section', async () => {
		const data = await docPageData(page('guides/deploy/a.md'), context);
		expect(data.tree.landing?.route).toBe('guides');
		expect(data.position).toMatchObject({
			group: 'Deploy models',
			prev: { route: 'guides' },
			next: { route: 'guides/deploy/b' }
		});
	});

	it('sends only the metadata the page renders', async () => {
		const data = await docPageData(page('guides/deploy/a.md'), context);
		expect(data.page).toEqual({
			path: 'guides/deploy/a.md',
			route: 'guides/deploy/a',
			section: 'guides',
			title: 'A',
			status: null,
			generated: false,
			toc: [{ depth: 2, id: 'x', text: 'X' }]
		});
		expect(data.title).toBe('A');
		expect(data.description).toBe('About guides/deploy/a.md.');
	});

	it('links written pages to GitHub', async () => {
		const data = await docPageData(page('guides/deploy/a.md'), context);
		expect(data.editUrl).toBe(
			'https://github.com/ome-projects/ome/edit/main/website/src/lib/content/guides/deploy/a.md'
		);
		expect(data.sourceUrl).toBe(
			'https://raw.githubusercontent.com/ome-projects/ome/main/website/src/lib/content/guides/deploy/a.md'
		);
		expect(data.replaces).toEqual([]);
	});

	it('gives drafts no body and links the Hugo pages they replace', async () => {
		loaded.length = 0;
		const data = await docPageData(page('guides/deploy/b.md'), context);
		expect(data.html).toBe('');
		expect(loaded).toEqual([]);
		expect(data.replaces).toEqual([
			{ old: 'tasks/b.md', url: 'https://ome-projects.github.io/ome/docs/tasks/b/' },
			{ old: 'tasks/b-more.md', url: 'https://ome-projects.github.io/ome/docs/tasks/b-more/' }
		]);
	});

	it('hides the edit link on generated pages and labels page-level since', async () => {
		const data = await docPageData(page('reference/api/ome.v1beta1.md'), context);
		expect(data.editUrl).toBeNull();
		expect(data.sourceUrl).toContain('/reference/api/ome.v1beta1.md');
		expect(data.since).toEqual({ state: 'unreleased', label: 'Unreleased: coming in v1.3' });
	});

	it('falls back to a neutral label when the latest release is unknown', async () => {
		const data = await docPageData(page('reference/api/ome.v1beta1.md'), {
			...context,
			latestRelease: null
		});
		expect(data.since).toEqual({ state: 'unknown', label: 'Since v1.3' });
	});

	it('fails for a section missing from nav.ts', async () => {
		const orphan = testPage('concepts/x.md');
		await expect(docPageData(orphan, context)).rejects.toThrow('nav.ts has no section concepts');
	});
});
