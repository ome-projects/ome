/**
 * Site identity and repository location. Everything that names the repo,
 * branch or content path reads from here, so the Edit links and the GitHub
 * badge follow the site if it moves to its own repo.
 */
export const site = {
	name: 'OME',
	longName: 'Open Model Engine',
	description:
		'OME is a Kubernetes operator for serving large language models. Declare a model, and OME picks the runtime, places it on the right GPUs, and lets you roll out changes as canaries.',
	url: 'https://lightseek.org/ome',
	repo: 'ome-projects/ome',
	branch: 'main',
	contentDir: 'website/src/lib/content',
	legacyDocsUrl: 'https://ome-projects.github.io/ome/docs'
} as const;

export const repoUrl = `https://github.com/${site.repo}`;

/** GitHub editor URL for a content path such as `guides/index.md`. */
export function editUrl(path: string): string {
	return `${repoUrl}/edit/${site.branch}/${site.contentDir}/${path}`;
}

/** Raw Markdown URL for a content path. */
export function sourceUrl(path: string): string {
	return `https://raw.githubusercontent.com/${site.repo}/${site.branch}/${site.contentDir}/${path}`;
}
