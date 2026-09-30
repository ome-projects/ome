import { describe, expect, it } from 'vitest';
import { highlight } from './highlight';

const marked = (text: string, query: string) =>
	highlight(text, query)
		.filter((segment) => segment.match)
		.map((segment) => segment.text);

describe('highlight', () => {
	it('marks terms case-insensitively', () => {
		expect(highlight('Serve models from a PVC', 'pvc')).toEqual([
			{ text: 'Serve models from a ', match: false },
			{ text: 'PVC', match: true }
		]);
	});

	it('marks only where a term starts a word', () => {
		expect(marked('OMENative and home', 'ome')).toEqual(['OME']);
	});

	it('splits the query on punctuation, like the index', () => {
		expect(marked('kubectl ome rollout', 'kubectl-ome')).toEqual(['kubectl', 'ome']);
		expect(highlight('a (b) c', '(b)')).toEqual([
			{ text: 'a (', match: false },
			{ text: 'b', match: true },
			{ text: ') c', match: false }
		]);
	});

	it('prefers the longest term at a position', () => {
		expect(marked('rollout', 'roll rollout')).toEqual(['rollout']);
	});

	it('escapes regular expression syntax', () => {
		expect(marked('x+y', 'x+y')).toEqual(['x+y']);
	});

	it('returns the text unmarked for an empty query', () => {
		expect(highlight('Install OME', '  ')).toEqual([{ text: 'Install OME', match: false }]);
	});

	it('returns nothing for empty text', () => {
		expect(highlight('', 'ome')).toEqual([]);
	});
});
