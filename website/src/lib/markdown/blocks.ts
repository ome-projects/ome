export const CALLOUT_TYPES = ['note', 'tip', 'warning', 'danger'] as const;
export type CalloutType = (typeof CALLOUT_TYPES)[number];

export type DocBlock =
	| { kind: 'prerequisites'; body: string }
	| { kind: 'cards'; items: string[] }
	| { kind: 'admonition'; type: CalloutType; title: string | null; body: string }
	| { kind: 'details'; type: CalloutType; title: string; open: boolean; body: string }
	| { kind: 'tabs'; tabs: { title: string; body: string }[] };

const PREREQUISITES_OPEN = /^<div class="prerequisites" markdown>\s*$/;
const CARDS_OPEN = /^<div class="grid cards" markdown>\s*$/;
const DIV_CLOSE = /^<\/div>\s*$/;
const ADMONITION = /^!!! (\w+)(?: "([^"]*)")?\s*$/;
const DETAILS = /^\?\?\?(\+)? (\w+)(?: "([^"]*)")?\s*$/;
const TAB = /^=== "([^"]+)"\s*$/;
/** Block syntax left over after extraction: indented, or not in a form the renderer accepts. */
const STRAY =
	/^[ \t]*(?:!!!|\?\?\?\+?)(?:\s|$)|^[ \t]*=== |^[ \t]*<div class="(?:prerequisites|grid cards)"/m;

export const BLOCK_PLACEHOLDER = /^<!--DOCBLOCK:(\d+)-->\s*$/;

/**
 * Replaces the block syntax at one nesting level of masked Markdown with
 * placeholders, and returns the blocks with their bodies dedented. Bodies
 * are rendered later, one level down.
 */
export function extractBlocks(text: string, path: string): { text: string; blocks: DocBlock[] } {
	const lines = text.split('\n');
	const out: string[] = [];
	const blocks: DocBlock[] = [];
	const place = (block: DocBlock) => {
		blocks.push(block);
		out.push('', `<!--DOCBLOCK:${blocks.length - 1}-->`, '');
	};

	let i = 0;
	while (i < lines.length) {
		const line = lines[i];
		let match: RegExpExecArray | null;

		if (PREREQUISITES_OPEN.test(line) || CARDS_OPEN.test(line)) {
			let end = i + 1;
			while (end < lines.length && !DIV_CLOSE.test(lines[end])) end++;
			if (end === lines.length) throw new Error(`${path}: "${line.trim()}" has no closing </div>`);
			const body = trimBlankLines(lines.slice(i + 1, end)).join('\n');
			if (PREREQUISITES_OPEN.test(line)) place({ kind: 'prerequisites', body });
			else place({ kind: 'cards', items: splitCards(body) });
			i = end + 1;
		} else if ((match = ADMONITION.exec(line))) {
			const type = calloutType(match[1], line, path);
			const { body, next } = collectIndented(lines, i + 1, line, path);
			const title = match[2] === undefined ? defaultTitle(type) : match[2] || null;
			place({ kind: 'admonition', type, title, body });
			i = next;
		} else if ((match = DETAILS.exec(line))) {
			const type = calloutType(match[2], line, path);
			const { body, next } = collectIndented(lines, i + 1, line, path);
			place({
				kind: 'details',
				type,
				// As for callouts, "" means no title: the summary stays empty.
				title: match[3] === undefined ? defaultTitle(type) : match[3],
				open: !!match[1],
				body
			});
			i = next;
		} else if (TAB.test(line)) {
			const tabs: { title: string; body: string }[] = [];
			let j = i;
			for (;;) {
				const header = lines[j];
				const { body, next } = collectIndented(lines, j + 1, header, path);
				tabs.push({ title: TAB.exec(header)![1], body });
				let k = next;
				while (k < lines.length && lines[k].trim() === '') k++;
				if (k < lines.length && TAB.test(lines[k])) {
					j = k;
					continue;
				}
				j = next;
				break;
			}
			place({ kind: 'tabs', tabs });
			i = j;
		} else {
			out.push(line);
			i++;
		}
	}

	const result = out.join('\n');
	const stray = STRAY.exec(result);
	if (stray) {
		throw new Error(
			`${path}: "${stray[0].trim()}" is not valid here; callouts, details and tabs start at column 0 and look like !!! note "Title", ??? tip "Title" or === "Tab"`
		);
	}
	return { text: result, blocks };
}

function calloutType(value: string, line: string, path: string): CalloutType {
	if ((CALLOUT_TYPES as readonly string[]).includes(value)) return value as CalloutType;
	throw new Error(
		`${path}: "${line.trim()}" uses an unknown type; use ${CALLOUT_TYPES.join(', ')}`
	);
}

function defaultTitle(type: CalloutType): string {
	return type[0].toUpperCase() + type.slice(1);
}

/** Collects the lines indented by four spaces after a block header, stopping at the first line that isn't. */
function collectIndented(lines: string[], start: number, header: string, path: string) {
	let end = start;
	for (let j = start; j < lines.length; j++) {
		if (lines[j].trim() === '') continue;
		if (!lines[j].startsWith('    ')) break;
		end = j + 1;
	}
	const body = trimBlankLines(
		lines.slice(start, end).map((line) => (line.trim() === '' ? '' : line.slice(4)))
	).join('\n');
	if (body === '') {
		throw new Error(`${path}: "${header.trim()}" has no body; indent its content by four spaces`);
	}
	return { body, next: end };
}

function splitCards(body: string): string[] {
	return body
		.split(/\n(?=-\s)/)
		.map((item) =>
			trimBlankLines(
				item
					.replace(/^-\s+/, '')
					.replace(/\n\s+---\s*\n/g, '\n\n')
					.split('\n')
					.map((line) => line.replace(/^ {1,4}/, ''))
			).join('\n')
		)
		.filter(Boolean);
}

function trimBlankLines(lines: string[]): string[] {
	let start = 0;
	let end = lines.length;
	while (start < end && lines[start].trim() === '') start++;
	while (end > start && lines[end - 1].trim() === '') end--;
	return lines.slice(start, end);
}
