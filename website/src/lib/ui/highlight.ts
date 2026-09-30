/** A run of text that does or doesn't match the query. */
export interface Segment {
	text: string;
	match: boolean;
}

// MiniSearch's default tokenizer splits on this, so query terms split the same way.
const SEPARATOR = /[\n\r\p{Z}\p{P}]+/u;

/**
 * Splits text into runs, marking each place where a query term starts a word, as
 * the index's prefix search matches. Fuzzy matches aren't marked.
 */
export function highlight(text: string, query: string): Segment[] {
	if (text === '') return [];
	const terms = [...new Set(query.toLowerCase().split(SEPARATOR).filter(Boolean))]
		// Longer terms first, so "rollout" wins over "roll" at the same position.
		.sort((a, b) => b.length - a.length)
		.map(escapeRegExp);
	if (terms.length === 0) return [{ text, match: false }];

	const pattern = new RegExp(`(?<![\\p{L}\\p{N}])(?:${terms.join('|')})`, 'giu');
	const segments: Segment[] = [];
	let last = 0;
	for (const match of text.matchAll(pattern)) {
		if (match.index > last) segments.push({ text: text.slice(last, match.index), match: false });
		segments.push({ text: match[0], match: true });
		last = match.index + match[0].length;
	}
	if (last < text.length) segments.push({ text: text.slice(last), match: false });
	return segments;
}

function escapeRegExp(value: string): string {
	return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}
