/** Heading text to id: lowercase, punctuation dropped, spaces to hyphens. Matches SMG. */
export function slugify(text: string): string {
	return text
		.trim()
		.toLowerCase()
		.replace(/[^\w\s-]/g, '')
		.replace(/\s+/g, '-');
}

export interface Slugger {
	/** Claims an explicit id. Throws if the page already uses it. */
	claim(id: string): void;
	/** Returns a unique id for heading text, adding -1, -2 for repeats. */
	slug(text: string): string;
}

export function createSlugger(path: string): Slugger {
	const used = new Set<string>();
	const counts = new Map<string, number>();

	return {
		claim(id) {
			if (used.has(id)) throw new Error(`${path}: duplicate heading id "${id}"`);
			used.add(id);
		},
		slug(text) {
			const base = slugify(text) || 'section';
			let count = counts.get(base) ?? 0;
			let id = base;
			while (used.has(id)) {
				count += 1;
				id = `${base}-${count}`;
			}
			counts.set(base, count);
			used.add(id);
			return id;
		}
	};
}
