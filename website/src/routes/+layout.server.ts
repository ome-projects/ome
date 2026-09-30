import { getGitHubRepoStats } from '$lib/server/github';
import type { LayoutServerLoad } from './$types';

export const load: LayoutServerLoad = async ({ platform }) => {
	const github = await getGitHubRepoStats(platform);

	return { github };
};
