import { existsSync, readFileSync } from 'node:fs';
import { isAbsolute, join, relative, sep } from 'node:path';
import type { Plugin } from 'vite';
import { renderPage, type RenderedPage } from './render.ts';

// vite.config.ts imports this module, and Vite's native config loader needs
// file extensions, so relative imports in the renderer's modules keep `.ts`.

const KINDS = ['meta', 'html', 'search'] as const;

export interface OmeMarkdownOptions {
	/** Site base path, such as `/ome`. */
	basePath: string;
	/** Absolute path of src/lib/content. */
	contentDir: string;
	/** Absolute path of static/. */
	staticDir: string;
}

/**
 * Renders content pages at build time. Importing `page.md?meta`, `?html` or
 * `?search` yields the page's metadata, HTML or search text; any renderer
 * error fails the build with the page path.
 */
export function omeMarkdown({ basePath, contentDir, staticDir }: OmeMarkdownOptions): Plugin {
	const cache = new Map<string, { source: string; page: RenderedPage }>();

	return {
		name: 'ome-markdown',
		enforce: 'pre',
		load(id) {
			const [file, query = ''] = id.split('?', 2);
			const params = new URLSearchParams(query);
			const kind = KINDS.find((k) => params.has(k));
			if (!kind || !file.endsWith('.md')) return null;
			const rel = relative(contentDir, file);
			if (rel.startsWith('..') || isAbsolute(rel)) return null;

			this.addWatchFile(file);
			const source = readFileSync(file, 'utf8');
			const path = rel.split(sep).join('/');
			let entry = cache.get(path);
			if (entry?.source !== source) {
				const page = renderPage(source, {
					path,
					basePath,
					assetExists: (publicPath) => existsSync(join(staticDir, publicPath))
				});
				entry = { source, page };
				cache.set(path, entry);
			}

			const value =
				kind === 'meta'
					? entry.page.meta
					: kind === 'html'
						? entry.page.html
						: entry.page.searchText;
			return `export default ${JSON.stringify(value)};`;
		},
		// Only server code imports pages, so an edit reloads the SSR modules
		// but never reaches the browser. Reload the browser too.
		hotUpdate({ file }) {
			if (this.environment.name !== 'client' || !file.endsWith('.md')) return;
			const rel = relative(contentDir, file);
			if (rel.startsWith('..') || isAbsolute(rel)) return;
			this.environment.hot.send({ type: 'full-reload' });
			return [];
		}
	};
}
