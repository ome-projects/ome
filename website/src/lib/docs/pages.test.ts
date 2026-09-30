import { readdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { registry } from './pages';

const contentDir = fileURLToPath(new URL('../content', import.meta.url));

describe('pages', () => {
	it('registers every Markdown file through the build-time plugin', () => {
		const files = readdirSync(contentDir, { recursive: true }).filter((file) =>
			String(file).endsWith('.md')
		);
		expect(registry.pages).toHaveLength(files.length);
		expect(registry.getPage('guides')?.path).toBe('guides/index.md');
	});

	// Each HTML module goes through the Markdown plugin the first time it loads, so this
	// grows with the content: over a hundred written pages take well past vitest's 5s.
	it('loads HTML modules lazily', { timeout: 60_000 }, async () => {
		const loaded = await Promise.all(
			registry.pages.map(async (page) => ({ page, html: await registry.loadHtml(page.path) }))
		);
		for (const { page, html } of loaded) {
			expect(html === '').toBe(page.status === 'draft');
		}
	});
});
