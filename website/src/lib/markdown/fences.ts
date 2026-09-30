const FENCE_OPEN = /^(\s*)(`{3,}|~{3,})(.*)$/;
// eslint-disable-next-line no-control-regex -- NUL delimits tokens; renderPage rejects pages with NUL.
const TOKEN = /\u0000F(\d+)\u0000/g;

export interface MaskedText {
	text: string;
	/** Puts the hidden code back into any fragment of `text`. */
	restore(fragment: string): string;
}

/**
 * Hides the contents of fenced code blocks, so callout, tab and card syntax
 * inside code is never interpreted. Fence lines stay. Each non-blank inner
 * line keeps its leading whitespace, so indented bodies still dedent, and the
 * rest of the line becomes a token.
 */
export function maskFences(body: string, path: string, firstLine = 1): MaskedText {
	const stored: string[] = [];
	const lines = body.split('\n');
	const out: string[] = [];

	for (let i = 0; i < lines.length; i++) {
		const open = FENCE_OPEN.exec(lines[i]);
		// A backtick fence's info string cannot contain a backtick.
		if (!open || (open[2][0] === '`' && open[3].includes('`'))) {
			out.push(lines[i]);
			continue;
		}

		const marker = open[2];
		const closer = new RegExp(`^\\s*\\${marker[0]}{${marker.length},}\\s*$`);
		out.push(lines[i]);

		let j = i + 1;
		for (; j < lines.length && !closer.test(lines[j]); j++) {
			const line = lines[j];
			if (line.trim() === '') {
				out.push(line);
				continue;
			}
			const indent = /^\s*/.exec(line)![0];
			stored.push(line.slice(indent.length));
			out.push(`${indent}\u0000F${stored.length - 1}\u0000`);
		}
		if (j === lines.length) {
			throw new Error(`${path}: the code fence on line ${firstLine + i} is never closed`);
		}
		out.push(lines[j]);
		i = j;
	}

	return {
		text: out.join('\n'),
		restore: (fragment) => fragment.replace(TOKEN, (_match, n: string) => stored[Number(n)])
	};
}
