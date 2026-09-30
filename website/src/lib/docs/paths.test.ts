import { describe, expect, it } from 'vitest';
import { isSectionId, isUnderBase, pathToRoute, routeHref, sectionOf } from './paths';

describe('pathToRoute', () => {
	it.each([
		['guides/index.md', 'guides'],
		['guides/deploy-models/run-benchmarks.md', 'guides/deploy-models/run-benchmarks'],
		['concepts/models/index.md', 'concepts/models'],
		['reference/api/ome.v1beta1.md', 'reference/api/ome.v1beta1'],
		['guides/reindex.md', 'guides/reindex']
	])('serves %s at %s', (path, route) => {
		expect(pathToRoute(path)).toBe(route);
	});

	it('rejects a root index page and non-Markdown files', () => {
		expect(() => pathToRoute('index.md')).toThrow(/content root has no index page/);
		expect(() => pathToRoute('guides/diagram.svg')).toThrow(/must end in \.md/);
	});
});

describe('sectionOf', () => {
	it('returns the first directory', () => {
		expect(sectionOf('getting-started/install.md')).toBe('getting-started');
		expect(isSectionId('guides')).toBe(true);
		expect(isSectionId('blog')).toBe(false);
	});

	it('rejects pages outside a section', () => {
		expect(() => sectionOf('install.md')).toThrow(/section directory/);
		expect(() => sectionOf('blog/post.md')).toThrow(/section directory/);
	});
});

describe('routeHref', () => {
	it('prefixes the base path and adds the anchor', () => {
		expect(routeHref('/ome', 'guides')).toBe('/ome/guides');
		expect(routeHref('/ome', 'concepts/models/base-models', 'storage')).toBe(
			'/ome/concepts/models/base-models#storage'
		);
		expect(routeHref('', 'guides')).toBe('/guides');
	});
});

describe('isUnderBase', () => {
	it.each(['/ome', '/ome/', '/ome/guides', '/ome/search.json'])('accepts %s', (path) => {
		expect(isUnderBase(path, '/ome')).toBe(true);
	});

	it.each(['/ome.', '/omega', '/', '/guides'])('rejects %s', (path) => {
		expect(isUnderBase(path, '/ome')).toBe(false);
	});

	it('accepts every path when there is no base', () => {
		expect(isUnderBase('/guides', '')).toBe(true);
	});
});
