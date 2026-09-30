export type SectionId = 'getting-started' | 'guides' | 'concepts' | 'reference' | 'contributing';

export type PageStatus = 'draft' | 'preview';

export interface TocEntry {
	depth: 2 | 3;
	id: string;
	text: string;
}

/** An internal link found in a rendered page. `route` has no base path. */
export interface LinkRef {
	route: string;
	anchor: string | null;
}

/** Everything known about a page without loading its HTML. */
export interface PageMeta {
	/** Content path relative to src/lib/content, such as `guides/index.md`. */
	path: string;
	/** URL path without the base path or a leading slash, such as `guides`. */
	route: string;
	section: SectionId;
	title: string;
	/** Sidebar label: front matter `navLabel`, else `title`. */
	navLabel: string;
	description: string;
	status: PageStatus | null;
	since: string | null;
	generated: boolean;
	/** h2 and h3 headings in document order. Empty for drafts. */
	toc: TocEntry[];
	/** Every id in the rendered HTML. */
	anchors: string[];
	links: LinkRef[];
}

export interface NavGroup {
	/** Group heading in the sidebar; null for the ungrouped pages of a section. */
	label: string | null;
	/** Marks a group whose pages are all preview features. */
	preview?: boolean;
	/** Page paths relative to the section directory, in order. `index.md` is implicit. */
	pages: string[];
}

export interface NavSection {
	id: SectionId;
	label: string;
	groups: NavGroup[];
}

export interface SearchEntry {
	id: string;
	route: string;
	title: string;
	section: SectionId;
	sectionLabel: string;
	group: string | null;
	description: string;
	/** h2 and h3 heading text, joined. Empty for drafts. */
	headings: string;
	/** Body text without code blocks. Empty for drafts. */
	body: string;
	status: PageStatus | null;
	/** Marks a page generated from source, such as the API reference. */
	generated: boolean;
}
