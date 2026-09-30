import { base } from '$app/paths';

/** Header links. Contributing is linked from the footer and the Getting Started landing page. */
export const headerNavItems = [
	{ href: `${base}/getting-started`, label: 'Getting Started' },
	{ href: `${base}/guides`, label: 'Guides' },
	{ href: `${base}/concepts`, label: 'Concepts' },
	{ href: `${base}/reference`, label: 'Reference' }
] as const;
