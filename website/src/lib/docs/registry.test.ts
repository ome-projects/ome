import { describe, expect, it } from 'vitest';
import { createRegistry } from './registry';
import { testPage } from './testing';

describe('createRegistry', () => {
	const pages = [
		testPage('concepts/models/index.md'),
		testPage('concepts/index.md'),
		testPage('concepts/models/base-models.md')
	];
	const registry = createRegistry(pages, async (path) => `<p>${path}</p>`);

	it('sorts pages by path', () => {
		expect(registry.pages.map((page) => page.path)).toEqual([
			'concepts/index.md',
			'concepts/models/base-models.md',
			'concepts/models/index.md'
		]);
	});

	it('looks up a section index and a nested index separately', () => {
		expect(registry.getPage('concepts')?.path).toBe('concepts/index.md');
		expect(registry.getPage('concepts/models')?.path).toBe('concepts/models/index.md');
		expect(registry.getPage('concepts/models/base-models')?.path).toBe(
			'concepts/models/base-models.md'
		);
		expect(registry.getPage('concepts/model')).toBeUndefined();
		expect(registry.getPage('concepts/')).toBeUndefined();
		expect(registry.getPageByPath('concepts/index.md')?.route).toBe('concepts');
	});

	it('loads HTML for known pages only', async () => {
		await expect(registry.loadHtml('concepts/index.md')).resolves.toBe('<p>concepts/index.md</p>');
		await expect(registry.loadHtml('concepts/missing.md')).rejects.toThrow(/unknown page/);
	});

	it('rejects two pages served at one route', () => {
		expect(() =>
			createRegistry(
				[testPage('guides/x.md'), { ...testPage('guides/x/index.md'), route: 'guides/x' }],
				async () => ''
			)
		).toThrow('guides/x.md and guides/x/index.md are both served at /guides/x');
	});
});
