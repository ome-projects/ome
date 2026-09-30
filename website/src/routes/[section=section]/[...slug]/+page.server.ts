import { error } from '@sveltejs/kit';
import { nav } from '$lib/config/nav';
import { docPageData } from '$lib/docs/page-data';
import { registry } from '$lib/docs/pages';
import { redirects } from '$lib/docs/redirects';
import type { PageServerLoad } from './$types';

export const load: PageServerLoad = async ({ params, parent }) => {
	const route = params.slug ? `${params.section}/${params.slug}` : params.section;
	const page = registry.getPage(route);
	if (!page) error(404, 'Page not found');

	const { github } = await parent();
	return docPageData(page, { registry, nav, redirects, latestRelease: github.version });
};
