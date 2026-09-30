import { sveltekit } from '@sveltejs/kit/vite';
import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vitest/config';
import svelteConfig from './svelte.config.js';
import { omeMarkdown } from './src/lib/markdown/plugin.ts';

const dir = (path: string) => fileURLToPath(new URL(path, import.meta.url));

export default defineConfig({
	plugins: [
		// Renders Markdown at build time; must run before SvelteKit sees the imports.
		omeMarkdown({
			basePath: svelteConfig.kit?.paths?.base ?? '',
			contentDir: dir('./src/lib/content'),
			staticDir: dir('./static')
		}),
		sveltekit()
	],
	test: {
		include: ['src/**/*.test.ts'],
		environment: 'node'
	}
});
