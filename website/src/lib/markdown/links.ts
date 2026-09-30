import { posix } from 'node:path';
import { site } from '../config/site.ts';
import { pathToRoute, routeHref } from '../docs/paths.ts';

const LEGACY_HOST = new URL(site.legacyDocsUrl).host;
const SCHEME = /^([a-z][a-z0-9+.-]*):/i;

export interface LinkContext {
	/** Content path of the page being rendered. */
	path: string;
	basePath: string;
}

/**
 * Checks a Markdown link and returns the href to emit. Relative `.md` links
 * resolve against the current page and become site URLs; external links pass
 * through; everything else is an error.
 */
export function resolveLink(href: string, { path, basePath }: LinkContext): string {
	if (href.startsWith('#')) return href;

	const scheme = SCHEME.exec(href)?.[1].toLowerCase();
	if (scheme === 'mailto') return href;
	if (scheme === 'http' || scheme === 'https') {
		if (new URL(href).host === LEGACY_HOST) {
			throw new Error(
				`${path}: "${href}" links to the old Hugo site; link to the new page with a relative .md path`
			);
		}
		if (href === site.url || href.startsWith(`${site.url}/`)) {
			throw new Error(
				`${path}: "${href}" is an absolute link to this site; use a relative .md path`
			);
		}
		return encodeURI(href).replace(/%25/g, '%');
	}
	if (scheme) throw new Error(`${path}: unsupported link "${href}"`);
	if (href.startsWith('/')) {
		throw new Error(
			`${path}: "${href}" is an absolute path; use a relative .md path such as ../concepts/index.md`
		);
	}

	const hash = href.indexOf('#');
	const target = hash === -1 ? href : href.slice(0, hash);
	const anchor = hash === -1 ? null : href.slice(hash + 1) || null;
	if (!target.endsWith('.md')) {
		throw new Error(
			`${path}: "${href}" must point to a .md page; link to other repository files with a full GitHub URL`
		);
	}
	const resolved = posix.normalize(posix.join(posix.dirname(path), target));
	if (resolved === '..' || resolved.startsWith('../')) {
		throw new Error(
			`${path}: "${href}" leaves src/lib/content; link to repository files with a full GitHub URL`
		);
	}
	return routeHref(basePath, pathToRoute(resolved), anchor);
}

export interface ImageContext extends LinkContext {
	/** Whether a path such as `/images/x.svg` exists in the static directory. */
	assetExists: (publicPath: string) => boolean;
}

/**
 * Checks a Markdown image source and returns the src to emit. Images must be
 * files in static/images; external images are an error, since the build
 * can't check them and they can change or disappear.
 */
export function resolveImage(src: string, { path, basePath, assetExists }: ImageContext): string {
	if (!src.startsWith('/images/')) {
		throw new Error(
			`${path}: image "${src}" must be a path under /images/ (website/static/images); external images aren't allowed`
		);
	}
	if (!assetExists(src))
		throw new Error(`${path}: image "${src}" does not exist in website/static`);
	return `${basePath}${src}`;
}
