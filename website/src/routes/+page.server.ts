import { homeDefaults } from '$lib/config/home';
import { getContentBlocks } from '$lib/server/content';
import type { PageServerLoad } from './$types';

export const load: PageServerLoad = async ({ platform }) => {
	const keys = Object.keys(homeDefaults);
	const content = platform?.env?.DB
		? await getContentBlocks(platform.env.DB, keys, homeDefaults)
		: homeDefaults;

	return { content };
};
