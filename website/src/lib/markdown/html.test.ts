import { describe, expect, it } from 'vitest';
import { blockHtmlToText, decodeEntities, escapeHtml, htmlToText } from './html';

describe('html helpers', () => {
	it('escapes and decodes', () => {
		expect(escapeHtml(`<a href="x">'&'</a>`)).toBe(
			'&lt;a href=&quot;x&quot;&gt;&#39;&amp;&#39;&lt;/a&gt;'
		);
		expect(decodeEntities('&lt;&#39;&#x41;&amp;amp;&bogus;&#0;')).toBe("<'A&amp;&bogus;&#0;");
	});

	it('extracts text', () => {
		expect(htmlToText('<code>spec</code>&nbsp;and  <em>more</em>')).toBe('spec and more');
		expect(htmlToText('<<script>script>x</script>')).not.toContain('<');
		expect(blockHtmlToText('<p>One</p><p>Two</p><ul><li>a</li><li>b</li></ul>')).toBe(
			'One Two a b'
		);
	});
});
