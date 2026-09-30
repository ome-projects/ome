import type { ContentMap } from '$lib/server/content';

/** Hero copy. D1 can override it without a deploy; without a D1 binding, as in local development, these are used. */
export const homeDefaults: ContentMap = {
	'home.hero.title': 'The Kubernetes operator for serving LLMs in production',
	'home.hero.subtitle':
		'Declare a model. OME picks the runtime, places it on the right GPUs, and lets you roll out changes as canaries.'
};
