import table from '../../../redirects.json';
import { site } from '../config/site';

/** One row of website/redirects.json, which maps each Hugo page to the page that replaces it. */
export interface RedirectEntry {
	/** Hugo page, relative to site/content/en/docs, such as `concepts/base_model.md`. */
	old: string;
	/** Replacement, relative to src/lib/content, such as `concepts/models/base-models.md`. */
	new: string;
	/** Last commit that changed the old page when the new page was written; null while it is a draft. */
	rewrittenFrom: string | null;
}

const KEYS = new Set(['old', 'new', 'rewrittenFrom']);

/** Checks the shape of the redirects table. `checkRedirects` checks its contents. */
export function parseRedirects(value: unknown): RedirectEntry[] {
	if (!Array.isArray(value)) throw new Error('redirects.json must be an array');
	return value.map((entry: unknown, index) => {
		const where = `redirects.json[${index}]`;
		if (entry === null || typeof entry !== 'object' || Array.isArray(entry)) {
			throw new Error(`${where} must be an object`);
		}
		const record = entry as Record<string, unknown>;
		for (const key of Object.keys(record)) {
			if (!KEYS.has(key)) throw new Error(`${where} has unknown key "${key}"`);
		}
		const { old, new: replacement, rewrittenFrom } = record;
		if (typeof old !== 'string' || typeof replacement !== 'string') {
			throw new Error(`${where} needs string "old" and "new" paths`);
		}
		if (rewrittenFrom !== null && typeof rewrittenFrom !== 'string') {
			throw new Error(
				`${where} needs "rewrittenFrom": a commit SHA, or null while the page is a draft`
			);
		}
		return { old, new: replacement, rewrittenFrom };
	});
}

export const redirects: readonly RedirectEntry[] = parseRedirects(table);

/** Hugo pages that a new page replaces, in table order. */
export function replacedPages(entries: readonly RedirectEntry[], path: string): string[] {
	return entries.filter((entry) => entry.new === path).map((entry) => entry.old);
}

/** URL of a Hugo page: `concepts/_index.md` was served at `.../docs/concepts/`. */
export function legacyUrl(old: string): string {
	const stem = old.replace(/(?:^|\/)_index\.md$/, '').replace(/\.md$/, '');
	return `${site.legacyDocsUrl}/${stem ? `${stem}/` : ''}`;
}
