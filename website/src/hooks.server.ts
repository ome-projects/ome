import { base } from '$app/paths';
import type { Handle } from '@sveltejs/kit';
import { isUnderBase } from '$lib/docs/paths';

// A page served at /ome. computes its relative links and client base from
// the wrong directory, so its links leave the site and following one renders
// the layout without data. Answer such paths the way SvelteKit answers any
// other path outside the base.
export const handle: Handle = ({ event, resolve }) =>
	isUnderBase(event.url.pathname, base)
		? resolve(event)
		: new Response('Not found', { status: 404 });
