import { describe, expect, it } from 'vitest';
import { renderPage } from '../markdown/render';
import { buildSearchIndex } from './search';
import type { NavSection } from './types';

const render = (path: string, source: string) =>
	renderPage(source, { path, basePath: '/ome', assetExists: () => false });

describe('buildSearchIndex', () => {
	it('indexes prose and headings without code, and drafts by title and description', async () => {
		const written = render(
			'guides/deploy-models/pvc.md',
			'---\ntitle: Serve models from a PVC\ndescription: Mount weights from a volume.\n---\n\n## Create the claim\n\nApply the manifest.\n\n```yaml\nkind: PersistentVolumeClaim\n```\n'
		);
		const draft = render(
			'guides/index.md',
			'---\ntitle: Guides\ndescription: Task guides.\nstatus: draft\n---\n'
		);
		const texts = new Map([
			[written.meta.path, written.searchText],
			[draft.meta.path, 'not loaded for drafts']
		]);
		const nav: NavSection[] = [
			{
				id: 'guides',
				label: 'Guides',
				groups: [{ label: 'Deploy models', pages: ['deploy-models/pvc.md'] }]
			}
		];

		const entries = await buildSearchIndex([written.meta, draft.meta], nav, async (path) =>
			texts.get(path)!
		);

		expect(entries).toEqual([
			{
				id: 'guides/deploy-models/pvc',
				route: 'guides/deploy-models/pvc',
				title: 'Serve models from a PVC',
				section: 'guides',
				sectionLabel: 'Guides',
				group: 'Deploy models',
				description: 'Mount weights from a volume.',
				headings: 'Create the claim',
				body: 'Create the claim Apply the manifest.',
				status: null,
				generated: false
			},
			{
				id: 'guides',
				route: 'guides',
				title: 'Guides',
				section: 'guides',
				sectionLabel: 'Guides',
				group: null,
				description: 'Task guides.',
				headings: '',
				body: '',
				status: 'draft',
				generated: false
			}
		]);
	});

	it('marks generated pages, which search ranks lower', async () => {
		const reference = render(
			'reference/api/ome.v1beta1.md',
			'---\ntitle: OME API\ndescription: The API types.\ngenerated: true\n---\n\n## BaseModel\n\nA model.\n'
		);

		const [entry] = await buildSearchIndex([reference.meta], [], async () => reference.searchText);

		expect(entry.generated).toBe(true);
	});
});
