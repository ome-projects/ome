const ESCAPES: Record<string, string> = {
	'&': '&amp;',
	'<': '&lt;',
	'>': '&gt;',
	'"': '&quot;',
	"'": '&#39;'
};

export function escapeHtml(value: string): string {
	return value.replace(/[&<>"']/g, (char) => ESCAPES[char]);
}

const NAMED_ENTITIES: Record<string, string> = {
	amp: '&',
	lt: '<',
	gt: '>',
	quot: '"',
	apos: "'",
	nbsp: ' '
};

export function decodeEntities(value: string): string {
	return value.replace(/&(#x[0-9a-f]+|#\d+|[a-z]+);/gi, (match, code: string) => {
		if (code[0] !== '#') return NAMED_ENTITIES[code.toLowerCase()] ?? match;
		const hex = code[1] === 'x' || code[1] === 'X';
		const point = hex ? parseInt(code.slice(2), 16) : Number(code.slice(1));
		return point > 0 && point <= 0x10ffff ? String.fromCodePoint(point) : match;
	});
}

/** Text of an inline HTML fragment: tags removed, entities decoded, whitespace collapsed. */
export function htmlToText(html: string): string {
	// Strip tags until nothing changes, so a tag split by another tag can't survive one
	// pass. The input is the site's own rendered HTML, but the fixed point is cheap.
	let text = html;
	let previous;
	do {
		previous = text;
		text = text.replace(/<[^>]*>/g, '');
	} while (text !== previous);
	return decodeEntities(text).replace(/\s+/g, ' ').trim();
}

const BLOCK_BOUNDARY =
	/<\/(?:p|div|li|h[1-6]|td|th|tr|pre|aside|details|summary|blockquote|table|ul|ol)>|<br\s*\/?>/g;

/** Text of a block HTML fragment, keeping words in adjacent blocks apart. */
export function blockHtmlToText(html: string): string {
	return htmlToText(html.replace(BLOCK_BOUNDARY, ' '));
}
