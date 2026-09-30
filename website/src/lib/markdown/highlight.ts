import hljs from 'highlight.js/lib/core';
import bash from 'highlight.js/lib/languages/bash';
import diff from 'highlight.js/lib/languages/diff';
import dockerfile from 'highlight.js/lib/languages/dockerfile';
import go from 'highlight.js/lib/languages/go';
import ini from 'highlight.js/lib/languages/ini';
import json from 'highlight.js/lib/languages/json';
import makefile from 'highlight.js/lib/languages/makefile';
import markdown from 'highlight.js/lib/languages/markdown';
import python from 'highlight.js/lib/languages/python';
import yaml from 'highlight.js/lib/languages/yaml';
import { escapeHtml } from './html.ts';

hljs.registerLanguage('bash', bash);
hljs.registerLanguage('diff', diff);
hljs.registerLanguage('dockerfile', dockerfile);
hljs.registerLanguage('go', go);
hljs.registerLanguage('ini', ini);
hljs.registerLanguage('json', json);
hljs.registerLanguage('makefile', makefile);
hljs.registerLanguage('markdown', markdown);
hljs.registerLanguage('python', python);
hljs.registerLanguage('yaml', yaml);

export interface Language {
	/** Class suffix on the code element: `language-${name}`. */
	name: string;
	/** highlight.js grammar, or null for unhighlighted text. */
	grammar: string | null;
	/** Shown in the code bar when the block has no title. */
	label: string;
}

const lang = (name: string, grammar: string | null, label: string): Language => ({
	name,
	grammar,
	label
});

/** Every language a code fence may name, by the name used in the fence. */
export const LANGUAGES: Record<string, Language> = {
	bash: lang('bash', 'bash', 'Shell'),
	sh: lang('bash', 'bash', 'Shell'),
	shell: lang('bash', 'bash', 'Shell'),
	go: lang('go', 'go', 'Go'),
	json: lang('json', 'json', 'JSON'),
	yaml: lang('yaml', 'yaml', 'YAML'),
	yml: lang('yaml', 'yaml', 'YAML'),
	python: lang('python', 'python', 'Python'),
	dockerfile: lang('dockerfile', 'dockerfile', 'Dockerfile'),
	makefile: lang('makefile', 'makefile', 'Makefile'),
	diff: lang('diff', 'diff', 'Diff'),
	ini: lang('ini', 'ini', 'INI'),
	toml: lang('toml', 'ini', 'TOML'),
	markdown: lang('markdown', 'markdown', 'Markdown'),
	md: lang('markdown', 'markdown', 'Markdown'),
	text: lang('text', null, 'Text'),
	plaintext: lang('text', null, 'Text'),
	txt: lang('text', null, 'Text'),
	output: lang('output', null, 'Output')
};

export function highlight(code: string, language: Language): string {
	if (!language.grammar) return escapeHtml(code);
	return hljs.highlight(code, { language: language.grammar, ignoreIllegals: true }).value;
}
