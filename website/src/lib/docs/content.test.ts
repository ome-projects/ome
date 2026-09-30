import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { join, relative, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { nav } from '$lib/config/nav';
import { renderPage, type RenderedPage } from '$lib/markdown/render';
import { checkLinks, checkNav, checkRedirects, checkSearchIndex } from './checks';
import { createRegistry } from './registry';
import { redirects } from './redirects';
import { buildSearchIndex } from './search';

// Renders every page the way the build does and runs every content check,
// so `pnpm test` reports all content errors at once.

const contentDir = fileURLToPath(new URL('../content', import.meta.url));
const staticDir = fileURLToPath(new URL('../../../static', import.meta.url));

function markdownFiles(dir: string): string[] {
	return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
		const full = join(dir, entry.name);
		if (entry.isDirectory()) return markdownFiles(full);
		return entry.name.endsWith('.md') ? [full] : [];
	});
}

const rendered = new Map<string, RenderedPage>();
const renderErrors: string[] = [];
for (const file of markdownFiles(contentDir)) {
	const path = relative(contentDir, file).split(sep).join('/');
	try {
		rendered.set(
			path,
			renderPage(readFileSync(file, 'utf8'), {
				path,
				basePath: '/ome',
				assetExists: (publicPath) => existsSync(join(staticDir, publicPath))
			})
		);
	} catch (error) {
		renderErrors.push((error as Error).message);
	}
}
const registry = createRegistry(
	[...rendered.values()].map((page) => page.meta),
	async (path) => rendered.get(path)!.html
);

describe('content', () => {
	it('renders every page', () => {
		expect(renderErrors).toEqual([]);
	});

	it('lists every page in nav.ts', () => {
		expect(checkNav(nav, registry.pages)).toEqual([]);
	});

	it('resolves every internal link and anchor', () => {
		expect(checkLinks(registry.pages)).toEqual([]);
	});

	it('keeps redirects.json consistent', () => {
		expect(checkRedirects(redirects, registry.pages)).toEqual([]);
	});

	it('indexes every page for search', async () => {
		const entries = await buildSearchIndex(
			registry.pages,
			nav,
			async (path) => rendered.get(path)!.searchText
		);
		expect(checkSearchIndex(entries, registry.pages)).toEqual([]);
	});
});
