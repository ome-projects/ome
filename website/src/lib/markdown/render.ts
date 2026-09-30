import { Marked, type Tokens } from 'marked';
import { pathToRoute, sectionOf } from '../docs/paths.ts';
import type { LinkRef, PageMeta, TocEntry } from '../docs/types.ts';
import { BLOCK_PLACEHOLDER, extractBlocks, type DocBlock } from './blocks.ts';
import { maskFences } from './fences.ts';
import { parseFrontMatter, SINCE_PATTERN } from './frontmatter.ts';
import { highlight, LANGUAGES, type Language } from './highlight.ts';
import { blockHtmlToText, escapeHtml, htmlToText } from './html.ts';
import { resolveImage, resolveLink, type ImageContext } from './links.ts';
import { createSlugger } from './slug.ts';

export interface RenderOptions {
	/** Content path relative to src/lib/content, with forward slashes. */
	path: string;
	/** Site base path, such as `/ome`. */
	basePath: string;
	/** Whether a path such as `/images/x.svg` exists in the static directory. */
	assetExists: (publicPath: string) => boolean;
}

export interface RenderedPage {
	meta: PageMeta;
	html: string;
	/** Body text without code blocks, for the search index. */
	searchText: string;
}

const HEADING_ID = /^[A-Za-z0-9][\w.-]*$/;
const HEADING_ATTRIBUTES = /\s*\{([^{}]*)\}\s*$/;
const TOC_HEADING = /<h([23]) id="([^"]+)">([\s\S]*?)<\/h\1>/g;
const SINCE_BADGE = /<span class="doc-since[^"]*"[^>]*>[^<]*<\/span>/g;
const HEADERLINK = /<a class="doc-headerlink"[^>]*>[^<]*<\/a>/g;
const CODE_BLOCK = /<div class="doc-code[^"]*">[\s\S]*?<\/pre><\/div>/g;

const headerlink = (id: string) =>
	`<a class="doc-headerlink" href="#${id}" aria-label="Permanent link">¶</a>`;

/** Renders one Markdown page into its metadata, HTML and search text. */
export function renderPage(source: string, options: RenderOptions): RenderedPage {
	const { path } = options;
	const { data, body, bodyLine } = parseFrontMatter(source, path);
	const section = sectionOf(path);
	const route = pathToRoute(path);

	if (data.status === 'draft' && body.trim() !== '') {
		throw new Error(
			`${path}: draft pages have no body; remove "status: draft" once the page is written`
		);
	}
	const html = data.status === 'draft' ? '' : renderBody(body, bodyLine, options);

	const meta: PageMeta = {
		path,
		route,
		section,
		title: data.title,
		navLabel: data.navLabel ?? data.title,
		description: data.description,
		status: data.status,
		since: data.since,
		generated: data.generated,
		toc: extractToc(html),
		anchors: [...new Set([...html.matchAll(/\sid="([^"]+)"/g)].map((m) => m[1]))],
		links: extractLinks(html, route, options.basePath)
	};

	return { meta, html, searchText: searchText(html) };
}

function renderBody(body: string, firstLine: number, options: RenderOptions): string {
	const { path } = options;
	if (body.includes('\u0000')) throw new Error(`${path}: contains a NUL character`);

	const masked = maskFences(body, path, firstLine);
	const slugger = createSlugger(path);
	const imageContext: ImageContext = options;
	const levels: DocBlock[][] = [];
	let tabSets = 0;

	const marked = new Marked({
		gfm: true,
		renderer: {
			heading({ tokens, depth }) {
				let inner = this.parser.parseInline(tokens);
				if (depth === 1) {
					throw new Error(
						`${path}: "# ${htmlToText(inner)}": the title comes from front matter, so sections start at ##`
					);
				}
				let id: string | null = null;
				let since: string | null = null;
				const attributes = HEADING_ATTRIBUTES.exec(inner);
				if (attributes) {
					inner = inner.slice(0, attributes.index);
					for (const part of attributes[1].trim().split(/\s+/).filter(Boolean)) {
						if (part.startsWith('#') && HEADING_ID.test(part.slice(1))) id = part.slice(1);
						else if (part.startsWith('since=') && SINCE_PATTERN.test(part.slice(6)))
							since = part.slice(6);
						else {
							throw new Error(
								`${path}: heading "${htmlToText(inner)}" has an invalid attribute "${part}"; use {#id} and {since=v1.3}`
							);
						}
					}
				}
				if (id) slugger.claim(id);
				else id = slugger.slug(htmlToText(inner));
				const badge = since
					? `<span class="doc-since" data-since="${since}">Since ${since}</span>`
					: '';
				return `<h${depth} id="${id}">${inner}${badge}${headerlink(id)}</h${depth}>\n`;
			},
			code({ text, lang }) {
				const { language, title } = parseInfo(lang ?? '', path);
				const code = highlight(text, language);
				if (language.name === 'output') {
					return `<div class="doc-code doc-code--output"><div class="doc-code-bar"><span class="doc-code-title">${escapeHtml(title ?? language.label)}</span></div><pre class="doc-pre"><code class="language-output">${code}</code></pre></div>\n`;
				}
				const hljsClass = language.grammar ? 'hljs ' : '';
				return `<div class="doc-code"><div class="doc-code-bar"><span class="doc-code-title">${escapeHtml(title ?? language.label)}</span><button type="button" class="doc-code-copy" aria-label="Copy to clipboard">Copy</button></div><pre class="doc-pre"><code class="${hljsClass}language-${language.name}">${code}</code></pre></div>\n`;
			},
			link({ href, title, tokens }) {
				const text = this.parser.parseInline(tokens);
				const resolved = resolveLink(href, options);
				const titleAttribute = title ? ` title="${escapeHtml(title)}"` : '';
				return `<a href="${escapeHtml(resolved)}"${titleAttribute}>${text}</a>`;
			},
			image({ href, title, tokens, text }) {
				const alt = tokens ? htmlToText(this.parser.parseInline(tokens)) : text;
				const src = resolveImage(href, imageContext);
				const titleAttribute = title ? ` title="${escapeHtml(title)}"` : '';
				return `<img src="${escapeHtml(src)}" alt="${escapeHtml(alt)}"${titleAttribute} loading="lazy">`;
			},
			html(token: Tokens.HTML | Tokens.Tag) {
				const placeholder = BLOCK_PLACEHOLDER.exec(token.text);
				if (!placeholder) return token.text;
				const block = levels[levels.length - 1][Number(placeholder[1])];
				// Rendering a block runs nested parses, which rebind this.parser.
				const parser = this.parser;
				try {
					return renderBlock(block);
				} finally {
					this.parser = parser;
				}
			}
		}
	});

	const inline = (text: string) => marked.parseInline(text, { async: false }) as string;

	// Renders one nesting level: extract its blocks, restore its code, parse,
	// and render each block where its placeholder sits, in document order.
	const renderLevel = (text: string): string => {
		const { text: withPlaceholders, blocks } = extractBlocks(text, path);
		levels.push(blocks);
		try {
			return marked.parse(masked.restore(withPlaceholders), { async: false }) as string;
		} finally {
			levels.pop();
		}
	};

	const renderBlock = (block: DocBlock): string => {
		switch (block.kind) {
			case 'prerequisites':
				slugger.claim('before-you-begin');
				return `<aside class="doc-prerequisites"><h2 id="before-you-begin">Before you begin${headerlink('before-you-begin')}</h2>${renderLevel(block.body)}</aside>\n`;
			case 'cards':
				return `<div class="doc-grid">${block.items.map((item) => `<div class="doc-card">${renderLevel(item)}</div>`).join('')}</div>\n`;
			case 'admonition': {
				const title = block.title
					? `<p class="doc-admonition-title">${inline(block.title)}</p>`
					: '';
				return `<aside class="doc-admonition doc-admonition--${block.type}">${title}${renderLevel(block.body)}</aside>\n`;
			}
			case 'details':
				return `<details class="doc-details doc-details--${block.type}"${block.open ? ' open' : ''}><summary>${inline(block.title)}</summary>${renderLevel(block.body)}</details>\n`;
			case 'tabs': {
				const set = ++tabSets;
				const inputs = block.tabs
					.map(
						(_, i) =>
							`<input type="radio" class="doc-tab-input" name="doc-tab-set-${set}" id="doc-tab-${set}-${i}"${i === 0 ? ' checked' : ''}>`
					)
					.join('');
				const labels = block.tabs
					.map(
						(tab, i) =>
							`<label class="doc-tab-label" for="doc-tab-${set}-${i}">${escapeHtml(tab.title)}</label>`
					)
					.join('');
				const panels = block.tabs
					.map((tab) => `<div class="doc-tab-panel">${renderLevel(tab.body)}</div>`)
					.join('');
				return `<div class="doc-tabbed-set">${inputs}<div class="doc-tab-labels">${labels}</div><div class="doc-tab-panels">${panels}</div></div>\n`;
			}
		}
	};

	return renderLevel(masked.text);
}

/** Splits a fence info string such as `yaml title="model.yaml"` into a language and attributes. */
function parseInfo(info: string, path: string): { language: Language; title: string | null } {
	const trimmed = info.trim();
	const name = trimmed.split(/\s+/)[0];
	if (!name)
		throw new Error(`${path}: code blocks need a language, such as \`\`\`yaml or \`\`\`bash`);
	const language = LANGUAGES[name.toLowerCase()];
	if (!language) {
		throw new Error(
			`${path}: unsupported code block language "${name}"; use one of ${Object.keys(LANGUAGES).join(', ')}`
		);
	}

	let title: string | null = null;
	const attributes = trimmed.slice(name.length);
	const attribute = /\s*([\w-]+)=(?:"([^"]*)"|(\S+))/y;
	let position = 0;
	while (attributes.slice(position).trim() !== '') {
		attribute.lastIndex = position;
		const match = attribute.exec(attributes);
		if (!match) throw new Error(`${path}: cannot parse code block attributes in \`\`\`${trimmed}`);
		position = attribute.lastIndex;
		const [whole, key, quoted, bare] = match;
		if (key === 'title' && quoted) title = quoted;
		else if (key === 'check' && bare === 'skip' && language.name === 'yaml') continue;
		else
			throw new Error(
				`${path}: unsupported code block attribute "${whole.trim()}" in \`\`\`${trimmed}`
			);
	}
	return { language, title };
}

function extractToc(html: string): TocEntry[] {
	return [...html.matchAll(TOC_HEADING)].map(([, depth, id, inner]) => ({
		depth: Number(depth) as 2 | 3,
		id,
		text: htmlToText(inner.replace(SINCE_BADGE, '').replace(HEADERLINK, ''))
	}));
}

function extractLinks(html: string, route: string, basePath: string): LinkRef[] {
	const links = new Map<string, LinkRef>();
	for (const [, raw] of html.replace(HEADERLINK, '').matchAll(/\shref="([^"]*)"/g)) {
		const href = raw.replaceAll('&amp;', '&');
		let target: string;
		if (href.startsWith('#')) target = `${route}${href}`;
		else if (href.startsWith(`${basePath}/`)) target = href.slice(basePath.length + 1);
		else continue;
		const hash = target.indexOf('#');
		const link: LinkRef =
			hash === -1
				? { route: target, anchor: null }
				: { route: target.slice(0, hash), anchor: target.slice(hash + 1) || null };
		links.set(`${link.route}#${link.anchor ?? ''}`, link);
	}
	return [...links.values()];
}

function searchText(html: string): string {
	return blockHtmlToText(
		html.replace(CODE_BLOCK, ' ').replace(SINCE_BADGE, ' ').replace(HEADERLINK, '')
	);
}
