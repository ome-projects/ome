import { describe, expect, it } from 'vitest';
import { applySinceLabels, sinceLabel, sinceState } from './since';

describe('sinceState', () => {
	it.each([
		['v1.3', 'v1.2.2', 'unreleased'],
		['v2.0', 'v1.9.0', 'unreleased'],
		['v1.2', 'v1.2.2', 'released'],
		['v1.1', 'v1.2.2', 'released'],
		['v1.3', '1.3.0', 'released'],
		['v1.3', null, 'unknown'],
		['v1.3', 'nightly', 'unknown']
	] as const)('%s against %s is %s', (since, latest, state) => {
		expect(sinceState(since, latest)).toBe(state);
	});

	it('rejects malformed markers', () => {
		expect(() => sinceState('1.3', 'v1.2.2')).toThrow(/invalid since value/);
	});
});

describe('labels', () => {
	it('words each state', () => {
		expect(sinceLabel('v1.3', 'v1.2.2')).toBe('Unreleased: coming in v1.3');
		expect(sinceLabel('v1.3', 'v1.3.0')).toBe('New in v1.3');
		expect(sinceLabel('v1.3', null)).toBe('Since v1.3');
	});

	it('rewrites rendered badges', () => {
		const html =
			'<h2 id="a">A<span class="doc-since" data-since="v1.3">Since v1.3</span></h2><h2 id="b">B<span class="doc-since" data-since="v1.1">Since v1.1</span></h2>';
		expect(applySinceLabels(html, 'v1.2.2')).toBe(
			'<h2 id="a">A<span class="doc-since doc-since--unreleased" data-since="v1.3">Unreleased: coming in v1.3</span></h2><h2 id="b">B<span class="doc-since doc-since--released" data-since="v1.1">New in v1.1</span></h2>'
		);
		expect(applySinceLabels('<p>No badges.</p>', 'v1.2.2')).toBe('<p>No badges.</p>');
	});
});
