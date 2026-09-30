import { json } from '@sveltejs/kit';
import { nav } from '$lib/config/nav';
import { registry } from '$lib/docs/pages';
import { buildSearchIndex } from '$lib/docs/search';
import type { RequestHandler } from './$types';

export const prerender = true;

const CONTENT = '/src/lib/content/';

// Search text is read only here, at build time, so it stays out of the worker's page modules.
const text = import.meta.glob<string>('/src/lib/content/**/*.md', {
	query: '?search',
	import: 'default'
});

export const GET: RequestHandler = async () => {
	const entries = await buildSearchIndex(registry.pages, nav, (path) => {
		const load = text[`${CONTENT}${path}`];
		if (!load) throw new Error(`no search text for ${path}`);
		return load();
	});
	return json(entries);
};
