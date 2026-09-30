import { createRegistry } from './registry';
import type { PageMeta } from './types';

const CONTENT = '/src/lib/content/';

const metas = import.meta.glob<PageMeta>('/src/lib/content/**/*.md', {
	query: '?meta',
	import: 'default',
	eager: true
});
const html = import.meta.glob<string>('/src/lib/content/**/*.md', {
	query: '?html',
	import: 'default'
});

/** Every page in src/lib/content. Metadata is bundled; HTML loads when a page is served. */
export const registry = createRegistry(Object.values(metas), (path) => {
	const load = html[`${CONTENT}${path}`];
	if (!load) throw new Error(`no HTML module for ${path}`);
	return load();
});
