import { describe, expect, it } from 'vitest';
import { renderPage, type RenderOptions } from './render';

const options = (path = 'guides/deploy-models/example.md'): RenderOptions => ({
	path,
	basePath: '/ome',
	assetExists: (publicPath) => publicPath === '/images/diagram.svg'
});

const page = (body: string, frontMatter = 'title: Example\ndescription: An example page.') =>
	`---\n${frontMatter}\n---\n\n${body}`;

const render = (body: string, path?: string) => renderPage(page(body), options(path));

describe('front matter', () => {
	it('fills page metadata', () => {
		const { meta } = renderPage(
			page(
				'Body.',
				'title: Serve models from a PVC\nnavLabel: Serve from a PVC\ndescription: Mount weights.\nstatus: preview\nsince: v1.3'
			),
			options()
		);
		expect(meta).toMatchObject({
			path: 'guides/deploy-models/example.md',
			route: 'guides/deploy-models/example',
			section: 'guides',
			title: 'Serve models from a PVC',
			navLabel: 'Serve from a PVC',
			description: 'Mount weights.',
			status: 'preview',
			since: 'v1.3',
			generated: false
		});
	});

	it('uses the title as the nav label by default', () => {
		expect(render('Body.').meta.navLabel).toBe('Example');
	});

	it.each([
		['no front matter', 'Body only.', /missing front matter/],
		[
			'an unknown key',
			page('x', 'title: A\ndescription: B\nweight: 3'),
			/unknown front matter key "weight"/
		],
		['no title', page('x', 'description: B'), /needs "title"/],
		['no description', page('x', 'title: A'), /needs "description"/],
		[
			'an empty title',
			page('x', 'title: ""\ndescription: B'),
			/"title" must be a non-empty string/
		],
		[
			'an unknown status',
			page('x', 'title: A\ndescription: B\nstatus: beta'),
			/"status" must be draft or preview/
		],
		[
			'a bad since',
			page('x', 'title: A\ndescription: B\nsince: 1.3'),
			/"since" must look like v1.3/
		],
		[
			'a patch since',
			page('x', 'title: A\ndescription: B\nsince: v1.3.1'),
			/"since" must look like v1.3/
		],
		[
			'a string generated',
			page('x', 'title: A\ndescription: B\ngenerated: "yes"'),
			/"generated" must be true or false/
		],
		['a list', '---\n- a\n---\n', /must be a YAML mapping/],
		['invalid YAML', page('x', 'title: A: B\ndescription: C'), /invalid front matter/]
	])('rejects %s', (_name, source, message) => {
		expect(() => renderPage(source, options())).toThrow(message);
	});

	it('serves index.md at its directory', () => {
		expect(render('Body.', 'guides/index.md').meta.route).toBe('guides');
	});

	it('rejects pages outside a section', () => {
		expect(() => render('Body.', 'faq.md')).toThrow(/section directory/);
		expect(() => render('Body.', 'blog/post.md')).toThrow(/section directory/);
	});
});

describe('drafts', () => {
	it('renders no body', () => {
		const { meta, html, searchText } = renderPage(
			page('', 'title: A\ndescription: B\nstatus: draft'),
			options()
		);
		expect(html).toBe('');
		expect(searchText).toBe('');
		expect(meta.toc).toEqual([]);
		expect(meta.status).toBe('draft');
	});

	it('rejects a draft with a body', () => {
		expect(() =>
			renderPage(page('Text.', 'title: A\ndescription: B\nstatus: draft'), options())
		).toThrow(/draft pages have no body/);
	});
});

describe('headings', () => {
	it('slugifies text and numbers duplicates', () => {
		const { meta } = render('## Install the chart\n\n## Verify\n\n### Verify\n\n## Verify');
		expect(meta.toc.map((entry) => entry.id)).toEqual([
			'install-the-chart',
			'verify',
			'verify-1',
			'verify-2'
		]);
	});

	it('uses inline text for the id', () => {
		expect(render('## `kubectl ome rollout explain`').meta.toc[0]).toEqual({
			depth: 2,
			id: 'kubectl-ome-rollout-explain',
			text: 'kubectl ome rollout explain'
		});
	});

	it('accepts an explicit id', () => {
		const { html, meta } = render('## Storage options {#storage}');
		expect(html).toContain(
			'<h2 id="storage">Storage options<a class="doc-headerlink" href="#storage"'
		);
		expect(meta.toc[0]).toEqual({ depth: 2, id: 'storage', text: 'Storage options' });
	});

	it('adds a since badge', () => {
		const { html, meta } = render('## Pause a rollout {#pause since=v1.3}');
		expect(html).toContain(
			'<h2 id="pause">Pause a rollout<span class="doc-since" data-since="v1.3">Since v1.3</span><a class="doc-headerlink"'
		);
		expect(meta.toc[0].text).toBe('Pause a rollout');
	});

	it('avoids ids already claimed explicitly', () => {
		const { meta } = render('## Setup {#verify}\n\n## Verify');
		expect(meta.toc.map((entry) => entry.id)).toEqual(['verify', 'verify-1']);
	});

	it('rejects duplicate explicit ids', () => {
		expect(() => render('## A {#same}\n\n## B {#same}')).toThrow(/duplicate heading id "same"/);
	});

	it('rejects unknown attributes', () => {
		expect(() => render('## A {.wide}')).toThrow(/invalid attribute "\.wide"/);
		expect(() => render('## A {since=1.3}')).toThrow(/invalid attribute "since=1.3"/);
	});

	it('rejects h1', () => {
		expect(() => render('# Title')).toThrow(/sections start at ##/);
	});

	it('keeps h4 out of the table of contents', () => {
		const { html, meta } = render('## A\n\n#### Deep');
		expect(html).toContain('<h4 id="deep">');
		expect(meta.toc.map((entry) => entry.id)).toEqual(['a']);
	});

	it('assigns ids in document order across blocks', () => {
		const { meta } = render(
			'## Verify\n\n=== "Helm"\n\n    ### Verify\n\n=== "Kustomize"\n\n    ### Verify\n\n## Verify'
		);
		expect(meta.toc.map((entry) => entry.id)).toEqual([
			'verify',
			'verify-1',
			'verify-2',
			'verify-3'
		]);
	});
});

describe('code blocks', () => {
	it('highlights with a language label', () => {
		const { html } = render('```yaml\nkind: BaseModel\n```');
		expect(html).toContain(
			'<div class="doc-code"><div class="doc-code-bar"><span class="doc-code-title">YAML</span>'
		);
		expect(html).toContain(
			'<button type="button" class="doc-code-copy" aria-label="Copy to clipboard">Copy</button>'
		);
		expect(html).toContain('<code class="hljs language-yaml"><span class="hljs-attr">kind:</span>');
	});

	it('shows a title', () => {
		const { html } = render('```yaml title="model.yaml"\nkind: BaseModel\n```');
		expect(html).toContain('<span class="doc-code-title">model.yaml</span>');
	});

	it('renders output without a copy button', () => {
		const { html } = render('```output\nNAME   READY\nllama  True\n```');
		expect(html).toBe(
			'<div class="doc-code doc-code--output"><div class="doc-code-bar"><span class="doc-code-title">Output</span></div><pre class="doc-pre"><code class="language-output">NAME   READY\nllama  True</code></pre></div>\n'
		);
	});

	it('maps aliases', () => {
		expect(render('```sh\nls\n```').html).toContain('language-bash');
		expect(render('```toml\na = 1\n```').html).toContain(
			'<span class="doc-code-title">TOML</span>'
		);
		expect(render('```text\n<b>\n```').html).toContain(
			'<code class="language-text">&lt;b&gt;</code>'
		);
	});

	it('accepts check=skip on YAML only', () => {
		expect(render('```yaml check=skip\nkind: X\n```').html).toContain('language-yaml');
		expect(() => render('```bash check=skip\nls\n```')).toThrow(
			/unsupported code block attribute "check=skip"/
		);
	});

	it.each([
		['no language', '```\nls\n```', /need a language/],
		['an indented block', 'Text.\n\n    indented code', /need a language/],
		['an unknown language', '```rust\nfn main() {}\n```', /unsupported code block language "rust"/],
		['an unknown attribute', '```yaml linenums="1"\na: 1\n```', /unsupported code block attribute/],
		['an unquoted title', '```yaml title=model.yaml\na: 1\n```', /unsupported code block attribute/]
	])('rejects %s', (_name, body, message) => {
		expect(() => render(body)).toThrow(message);
	});

	it('reports an unclosed fence with its line', () => {
		expect(() => render('Text.\n\n```yaml\na: 1')).toThrow(/code fence on line 8 is never closed/);
	});

	it('keeps block syntax inside code literal', () => {
		const { html } = render('```markdown\n!!! note\n    Body\n=== "Tab"\n```');
		expect(html).not.toContain('doc-admonition');
		expect(html).not.toContain('doc-tabbed-set');
		expect(html).toContain('!!! note');
	});

	it('supports longer fences around shorter ones', () => {
		const { html } = render('````markdown\n```yaml title="a.yaml"\nx: 1\n```\n````');
		expect(html).toContain('```yaml title=&quot;a.yaml&quot;');
		expect(html.match(/class="doc-code"/g)).toHaveLength(1);
	});
});

describe('callouts', () => {
	it.each(['note', 'tip', 'warning', 'danger'])('renders %s with a default title', (type) => {
		const { html } = render(`!!! ${type}\n    Body text.`);
		const title = type[0].toUpperCase() + type.slice(1);
		expect(html).toBe(
			`<aside class="doc-admonition doc-admonition--${type}"><p class="doc-admonition-title">${title}</p><p>Body text.</p>\n</aside>\n`
		);
	});

	it('renders a custom title with inline Markdown', () => {
		expect(render('!!! warning "Do not use `latest`"\n    Body.').html).toContain(
			'<p class="doc-admonition-title">Do not use <code>latest</code></p>'
		);
	});

	it('omits an empty title', () => {
		expect(render('!!! note ""\n    Body.').html).not.toContain('doc-admonition-title');
	});

	it('rejects other types', () => {
		expect(() => render('!!! info\n    Body.')).toThrow(
			/unknown type; use note, tip, warning, danger/
		);
	});

	it('rejects a callout without an indented body', () => {
		expect(() => render('!!! note\nBody.')).toThrow(/has no body/);
	});

	it('renders code and lists inside', () => {
		const { html } = render(
			'!!! tip\n    Run:\n\n    ```bash\n    kubectl get pods\n    ```\n\n    - one\n    - two\n\nAfter.'
		);
		expect(html).toContain('<aside class="doc-admonition doc-admonition--tip">');
		expect(html).toContain('<code class="hljs language-bash">kubectl get pods</code>');
		expect(html).toContain('<li>one</li>');
		expect(html).toMatch(/<\/aside>\n<p>After.<\/p>/);
	});

	it('rejects indented callout syntax', () => {
		expect(() => render('1. Step\n\n    !!! note\n        Body.')).toThrow(/start at column 0/);
	});

	it('rejects malformed callout syntax', () => {
		expect(() => render('!!! note Title without quotes\n    Body.')).toThrow(/not valid here/);
	});
});

describe('details', () => {
	it('renders collapsed and open details', () => {
		expect(render('??? note "More"\n    Hidden.').html).toBe(
			'<details class="doc-details doc-details--note"><summary>More</summary><p>Hidden.</p>\n</details>\n'
		);
		expect(render('???+ tip\n    Shown.').html).toContain(
			'<details class="doc-details doc-details--tip" open><summary>Tip</summary>'
		);
	});

	it('leaves the summary empty for an empty title, as callouts do', () => {
		expect(render('??? note ""\n    Hidden.').html).toContain(
			'<details class="doc-details doc-details--note"><summary></summary><p>Hidden.</p>'
		);
	});
});

describe('tabs', () => {
	it('groups consecutive tabs into one set', () => {
		const { html, meta } = render(
			'=== "Helm"\n\n    ```bash\n    helm install ome\n    ```\n\n=== "Kustomize"\n\n    Apply it.\n\nAfter.'
		);
		expect(html).toContain(
			'<div class="doc-tabbed-set"><input type="radio" class="doc-tab-input" name="doc-tab-set-1" id="doc-tab-1-0" checked><input type="radio" class="doc-tab-input" name="doc-tab-set-1" id="doc-tab-1-1"><div class="doc-tab-labels"><label class="doc-tab-label" for="doc-tab-1-0">Helm</label><label class="doc-tab-label" for="doc-tab-1-1">Kustomize</label></div><div class="doc-tab-panels"><div class="doc-tab-panel"><div class="doc-code">'
		);
		expect(html).toContain(
			'<div class="doc-tab-panel"><p>Apply it.</p>\n</div></div></div>\n<p>After.</p>'
		);
		expect(meta.anchors).toEqual(expect.arrayContaining(['doc-tab-1-0', 'doc-tab-1-1']));
	});

	it('numbers tab sets per page', () => {
		const { html } = render('=== "A"\n    a\n\nText.\n\n=== "B"\n    b');
		expect(html).toContain('name="doc-tab-set-1"');
		expect(html).toContain('name="doc-tab-set-2"');
	});

	it('nests callouts in tabs', () => {
		const { html } = render('=== "A"\n\n    !!! warning\n        Careful.');
		expect(html).toContain(
			'<div class="doc-tab-panel"><aside class="doc-admonition doc-admonition--warning">'
		);
	});
});

describe('prerequisites and cards', () => {
	it('renders a Before you begin box in the table of contents', () => {
		const { html, meta } = render(
			'<div class="prerequisites" markdown>\n\n- A cluster\n- `kubectl`\n\n</div>\n\n## Step one'
		);
		expect(html).toContain(
			'<aside class="doc-prerequisites"><h2 id="before-you-begin">Before you begin<a class="doc-headerlink" href="#before-you-begin"'
		);
		expect(html).toContain('<li><code>kubectl</code></li>');
		expect(meta.toc.map((entry) => entry.id)).toEqual(['before-you-begin', 'step-one']);
	});

	it('rejects an unclosed box', () => {
		expect(() => render('<div class="prerequisites" markdown>\n\n- A')).toThrow(
			/no closing <\/div>/
		);
	});

	it('renders card grids', () => {
		const { html } = render(
			'<div class="grid cards" markdown>\n\n-   **Install**\n\n    ---\n\n    Set up OME.\n\n-   **Serve**\n\n    Deploy a model.\n\n</div>'
		);
		expect(html).toBe(
			'<div class="doc-grid"><div class="doc-card"><p><strong>Install</strong></p>\n<p>Set up OME.</p>\n</div><div class="doc-card"><p><strong>Serve</strong></p>\n<p>Deploy a model.</p>\n</div></div>\n'
		);
	});
});

describe('links', () => {
	it('resolves relative .md links to site URLs', () => {
		const { html, meta } = render(
			'See [base models](../../concepts/models/base-models.md#storage), [guides](../index.md) and [a sibling](run-benchmarks.md).'
		);
		expect(html).toContain('<a href="/ome/concepts/models/base-models#storage">base models</a>');
		expect(html).toContain('<a href="/ome/guides">guides</a>');
		expect(html).toContain('<a href="/ome/guides/deploy-models/run-benchmarks">a sibling</a>');
		expect(meta.links).toEqual([
			{ route: 'concepts/models/base-models', anchor: 'storage' },
			{ route: 'guides', anchor: null },
			{ route: 'guides/deploy-models/run-benchmarks', anchor: null }
		]);
	});

	it('records same-page anchors but not headerlinks', () => {
		const { meta } = render('## Setup\n\nJump to [verify](#verify).');
		expect(meta.links).toEqual([{ route: 'guides/deploy-models/example', anchor: 'verify' }]);
	});

	it('passes external links through', () => {
		const { html, meta } = render(
			'[Kueue](https://kueue.sigs.k8s.io/docs/) and <https://github.com/ome-projects/ome/blob/main/Makefile> and [mail](mailto:a@b.c)'
		);
		expect(html).toContain('<a href="https://kueue.sigs.k8s.io/docs/">Kueue</a>');
		expect(html).toContain('href="https://github.com/ome-projects/ome/blob/main/Makefile"');
		expect(html).toContain('href="mailto:a@b.c"');
		expect(meta.links).toEqual([]);
	});

	it.each([
		['a Hugo link', '[old](https://ome-projects.github.io/ome/docs/concepts/)', /old Hugo site/],
		[
			'an absolute self link',
			'[x](https://lightseek.org/ome/guides)',
			/absolute link to this site/
		],
		['an absolute path', '[x](/ome/guides)', /absolute path/],
		['a link out of the content', '[x](../../../../Makefile.md)', /leaves src\/lib\/content/],
		['a relative non-page link', '[x](../../../config/runtimes/)', /must point to a \.md page/],
		['a javascript link', '[x](javascript:alert(1))', /unsupported link/]
	])('rejects %s', (_name, body, message) => {
		expect(() => render(body)).toThrow(message);
	});

	it('resolves images in static/images', () => {
		expect(render('![Diagram](/images/diagram.svg)').html).toContain(
			'<img src="/ome/images/diagram.svg" alt="Diagram" loading="lazy">'
		);
		expect(() => render('![x](/images/missing.svg)')).toThrow(/does not exist/);
		expect(() => render('![x](diagram.svg)')).toThrow(/under \/images\//);
	});

	it.each(['https://example.com/diagram.png', 'http://example.com/diagram.png'])(
		'rejects the external image %s',
		(src) => {
			expect(() => render(`![Diagram](${src})`)).toThrow(/external images aren't allowed/);
		}
	);
});

describe('search text', () => {
	it('keeps prose and headings but drops code and badges', () => {
		const { searchText } = render(
			'## Install {since=v1.3}\n\nRun the `helm` command.\n\n```bash\nhelm install secret-flag\n```\n\n- one\n- two'
		);
		expect(searchText).toBe('Install Run the helm command. one two');
	});
});

describe('content patterns', () => {
	it('renders GFM tables', () => {
		const { html } = render('| Field | Type |\n| --- | --- |\n| `name` | string |');
		expect(html).toContain('<table>');
		expect(html).toContain('<td><code>name</code></td>');
	});

	it('renders fences inside list items', () => {
		const { html } = render(
			'1. Apply it:\n\n   ```bash\n   kubectl apply -f model.yaml\n   ```\n\n2. Check it.'
		);
		expect(html).toContain('<li><p>Apply it:</p>\n<div class="doc-code">');
		expect(html).toContain('<code class="hljs language-bash">kubectl apply -f model.yaml</code>');
		expect(html).toContain('<li><p>Check it.</p>');
	});

	it('renders Markdown inside HTML table cells, as the API reference does', () => {
		const { html, meta } = render(
			[
				'## `BaseModel` {#ome-io-v1beta1-BaseModel}',
				'',
				'<table class="doc-api-fields">',
				'<thead><tr><th>Field</th><th>Type</th><th>Description</th></tr></thead>',
				'<tbody>',
				'<tr><td><code>spec</code> <span class="doc-api-required">Required</span></td>',
				'<td><a href="#ome-io-v1beta1-BaseModelSpec"><code>BaseModelSpec</code></a></td>',
				'<td>',
				'',
				'Where the weights live. See [storage](#ome-io-v1beta1-BaseModel) and:',
				'',
				'- `hf://org/model`',
				'',
				'</td>',
				'</tr>',
				'</tbody>',
				'</table>'
			].join('\n')
		);
		expect(html).toContain(
			'<h2 id="ome-io-v1beta1-BaseModel"><code>BaseModel</code><a class="doc-headerlink"'
		);
		expect(html).toContain(
			'<td><p>Where the weights live. See <a href="#ome-io-v1beta1-BaseModel">storage</a> and:</p>'
		);
		expect(html).toContain('<li><code>hf://org/model</code></li>');
		expect(meta.links).toEqual([
			{ route: 'guides/deploy-models/example', anchor: 'ome-io-v1beta1-BaseModelSpec' },
			{ route: 'guides/deploy-models/example', anchor: 'ome-io-v1beta1-BaseModel' }
		]);
	});
});
