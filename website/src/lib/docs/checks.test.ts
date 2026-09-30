import { describe, expect, it } from 'vitest';
import { checkLinks, checkNav, checkRedirects, checkSearchIndex } from './checks';
import { SECTION_IDS } from './paths';
import { testPage } from './testing';
import type { NavSection, SearchEntry } from './types';

const landings = SECTION_IDS.map((id) => testPage(`${id}/index.md`));
const emptyNav = (): NavSection[] => SECTION_IDS.map((id) => ({ id, label: id, groups: [] }));

describe('checkNav', () => {
	it('passes when every page is listed once', () => {
		const nav = emptyNav();
		nav[1].groups.push({ label: 'Deploy models', pages: ['deploy-models/a.md'] });
		expect(checkNav(nav, [...landings, testPage('guides/deploy-models/a.md')])).toEqual([]);
	});

	it('reports every problem', () => {
		const nav = emptyNav().filter((section) => section.id !== 'contributing');
		nav[1].groups.push(
			{ label: 'A', pages: ['deploy-models/a.md', 'deploy-models/gone.md', 'index.md'] },
			{ label: 'B', pages: ['deploy-models/a.md'] }
		);
		const pages = [
			...landings.filter((page) => page.section !== 'reference'),
			testPage('guides/deploy-models/a.md'),
			testPage('guides/networking/unlisted.md')
		];
		expect(checkNav(nav, pages)).toEqual([
			'section "reference" has no landing page reference/index.md',
			'nav.ts must list section "contributing" once, not 0 times',
			'nav.ts lists guides/deploy-models/gone.md, which does not exist',
			'nav.ts lists guides/index.md; section landing pages are implicit and groups have no index page',
			'guides/deploy-models/a.md is listed 2 times in nav.ts',
			'guides/networking/unlisted.md is not in nav.ts'
		]);
	});
});

describe('checkLinks', () => {
	it('checks targets and anchors', () => {
		const pages = [
			testPage('guides/a.md', {
				links: [
					{ route: 'concepts/b', anchor: 'storage' },
					{ route: 'concepts/b', anchor: 'nope' },
					{ route: 'concepts/b', anchor: null },
					{ route: 'concepts/missing', anchor: null },
					{ route: 'concepts/draft', anchor: 'x' },
					{ route: 'guides/a', anchor: 'self' }
				],
				anchors: ['self']
			}),
			testPage('concepts/b.md', { anchors: ['storage'] }),
			testPage('concepts/draft.md', { status: 'draft' })
		];
		expect(checkLinks(pages)).toEqual([
			'guides/a.md: the link to /concepts/b#nope: concepts/b.md has no id "nope"',
			'guides/a.md: the link to /concepts/missing does not match any page',
			'guides/a.md: the link to /concepts/draft#x points into concepts/draft.md, a draft with no headings yet; link to the page without an anchor'
		]);
	});
});

describe('checkRedirects', () => {
	const pages = [
		testPage('concepts/written.md'),
		testPage('concepts/draft.md', { status: 'draft' })
	];

	it('passes a consistent table', () => {
		expect(
			checkRedirects(
				[
					{
						old: 'concepts/a.md',
						new: 'concepts/written.md',
						rewrittenFrom: '0123456789abcdef0123456789abcdef01234567'
					},
					{ old: 'concepts/b.md', new: 'concepts/draft.md', rewrittenFrom: null }
				],
				pages
			)
		).toEqual([]);
	});

	it('reports every problem', () => {
		expect(
			checkRedirects(
				[
					{ old: 'concepts/a', new: 'concepts/draft.md', rewrittenFrom: null },
					{ old: 'concepts/b.md', new: 'concepts/gone.md', rewrittenFrom: null },
					{ old: 'concepts/b.md', new: 'concepts/written.md', rewrittenFrom: null },
					{ old: 'concepts/c.md', new: 'concepts/draft.md', rewrittenFrom: 'abc1234' },
					{ old: 'concepts/d.md', new: 'concepts/written.md', rewrittenFrom: 'HEAD' }
				],
				pages
			)
		).toEqual([
			'redirects.json entry "concepts/a": "old" must be a Hugo page path ending in .md',
			'redirects.json entry "concepts/b.md": "new" page concepts/gone.md does not exist',
			'redirects.json entry "concepts/b.md": "old" appears more than once',
			'redirects.json entry "concepts/b.md": concepts/written.md is written, so set "rewrittenFrom" to the 8-character hash of the last commit that changed the old page (git log -1 --abbrev=8 --format=%h HEAD -- site/content/en/docs/concepts/b.md)',
			'redirects.json entry "concepts/c.md": concepts/draft.md is a draft, so "rewrittenFrom" must be null',
			'redirects.json entry "concepts/d.md": "rewrittenFrom" must be a commit SHA'
		]);
	});
});

describe('checkSearchIndex', () => {
	it('wants one entry per page', () => {
		const entry = (route: string) => ({ route }) as SearchEntry;
		const pages = [testPage('guides/a.md'), testPage('guides/b.md')];
		expect(checkSearchIndex([entry('guides/a'), entry('guides/b')], pages)).toEqual([]);
		expect(
			checkSearchIndex([entry('guides/a'), entry('guides/a'), entry('guides/x')], pages)
		).toEqual([
			'search entry /guides/a appears more than once',
			'search entry /guides/x does not match any page',
			'guides/b.md is missing from the search index'
		]);
	});
});
