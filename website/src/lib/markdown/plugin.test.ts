import { mkdirSync, mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { beforeAll, describe, expect, it, vi } from 'vitest';
import { omeMarkdown } from './plugin';

const root = mkdtempSync(join(tmpdir(), 'ome-markdown-'));
const contentDir = join(root, 'content');
const staticDir = join(root, 'static');
const plugin = omeMarkdown({ basePath: '/ome', contentDir, staticDir });
const addWatchFile = vi.fn();
const load = (id: string) =>
	(plugin.load as (this: unknown, id: string) => string | null).call({ addWatchFile }, id);
const exported = (code: string | null) =>
	JSON.parse(code!.replace(/^export default /, '').replace(/;$/, ''));

beforeAll(() => {
	mkdirSync(join(contentDir, 'guides'), { recursive: true });
	mkdirSync(join(staticDir, 'images'), { recursive: true });
	writeFileSync(join(staticDir, 'images', 'flow.svg'), '<svg/>');
	writeFileSync(
		join(contentDir, 'guides', 'page.md'),
		'---\ntitle: Page\ndescription: A page.\n---\n\n## Hello\n\n![Flow](/images/flow.svg)\n'
	);
	writeFileSync(join(contentDir, 'guides', 'broken.md'), '---\ntitle: Broken\n---\n');
	writeFileSync(join(root, 'outside.md'), '---\ntitle: Outside\ndescription: X.\n---\n');
});

describe('omeMarkdown', () => {
	it('exports metadata, HTML and search text', () => {
		const file = join(contentDir, 'guides', 'page.md');
		expect(exported(load(`${file}?meta`))).toMatchObject({
			path: 'guides/page.md',
			route: 'guides/page'
		});
		expect(exported(load(`${file}?html`))).toContain(
			'<img src="/ome/images/flow.svg" alt="Flow" loading="lazy">'
		);
		expect(exported(load(`${file}?search`))).toBe('Hello');
		expect(addWatchFile).toHaveBeenCalledWith(file);
	});

	it('ignores other imports', () => {
		expect(load(join(contentDir, 'guides', 'page.md'))).toBeNull();
		expect(load(`${join(contentDir, 'guides', 'page.md')}?raw`)).toBeNull();
		expect(load(`${join(root, 'outside.md')}?meta`)).toBeNull();
		expect(load(`${join(contentDir, 'guides', 'x.ts')}?meta`)).toBeNull();
	});

	it('fails on renderer errors', () => {
		expect(() => load(`${join(contentDir, 'guides', 'broken.md')}?meta`)).toThrow(
			'guides/broken.md: front matter needs "description"'
		);
	});

	it('reloads the browser when a page changes', () => {
		const send = vi.fn();
		const hotUpdate = (environment: string, file: string) =>
			(plugin.hotUpdate as (this: unknown, options: { file: string }) => unknown).call(
				{ environment: { name: environment, hot: { send } } },
				{ file }
			);
		const page = join(contentDir, 'guides', 'page.md');
		expect(hotUpdate('client', page)).toEqual([]);
		expect(send).toHaveBeenCalledWith({ type: 'full-reload' });
		expect(hotUpdate('ssr', page)).toBeUndefined();
		expect(hotUpdate('client', join(root, 'outside.md'))).toBeUndefined();
		expect(hotUpdate('client', join(contentDir, 'guides', 'x.ts'))).toBeUndefined();
		expect(send).toHaveBeenCalledTimes(1);
	});
});
